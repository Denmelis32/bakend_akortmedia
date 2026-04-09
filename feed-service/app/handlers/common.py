"""
COMMON v6.2.0 - ОПТИМИЗИРОВАНО для FEED SERVICE + WEBSOCKET
- 1 запрос = 1 сессия (RequestContext)
- Мониторинг сессий
- Фоновый воркер для задач
- Batch обработка
- Кэширование постов и ленты
- Реакции и лайки
- Кэширование репостов и реакций
- Кэширование комментариев
- WebSocket поддержка для real-time уведомлений
"""
import json
import hashlib
import re
import asyncio
import time
import base64
import traceback
import inspect
import gzip
import io
import os
import uuid
import logging
import threading
from typing import Dict, Any, Optional, List, Union, Tuple, Set, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from collections import OrderedDict, deque
from contextlib import asynccontextmanager

# ============================================
# ИМПОРТЫ ИНФРАСТРУКТУРЫ
# ============================================

from app.middleware.logging import logger
from app.db.pool import ydb_pool, init_db_pool  # ✅ ИСПРАВЛЕНО: ydb_pool вместо get_db_pool

# ============================================
# ПРОВЕРКА НАЛИЧИЯ ORJSON
# ============================================

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False
    logger.warning("⚠️ orjson not available, using standard json (slower)")

# ============================================
# КОНФИГУРАЦИЯ
# ============================================

@dataclass
class CommonConfig:
    """Конфигурация общих компонентов для feed сервиса"""
    RETRY_ATTEMPTS: int = int(os.environ.get('RETRY_ATTEMPTS', '3'))
    RETRY_DELAY: float = float(os.environ.get('RETRY_DELAY', '0.1'))
    RETRY_MAX_DELAY: float = float(os.environ.get('RETRY_MAX_DELAY', '2.0'))
    RETRY_BACKOFF: float = float(os.environ.get('RETRY_BACKOFF', '2.0'))
    CACHE_DEFAULT_TTL: int = int(os.environ.get('CACHE_DEFAULT_TTL', '300'))
    CACHE_USER_TTL: int = int(os.environ.get('CACHE_USER_TTL', '600'))
    CACHE_POST_TTL: int = int(os.environ.get('CACHE_POST_TTL', '300'))
    CACHE_FEED_TTL: int = int(os.environ.get('CACHE_FEED_TTL', '60'))
    CACHE_REPOST_TTL: int = int(os.environ.get('CACHE_REPOST_TTL', '30'))
    CACHE_REACTION_TTL: int = int(os.environ.get('CACHE_REACTION_TTL', '30'))
    CACHE_REACTION_USER_TTL: int = int(os.environ.get('CACHE_REACTION_USER_TTL', '10'))
    CACHE_COMMENT_TTL: int = int(os.environ.get('CACHE_COMMENT_TTL', '30'))
    QUERY_TIMEOUT: int = int(os.environ.get('QUERY_TIMEOUT', '5'))
    BATCH_SIZE: int = int(os.environ.get('BATCH_SIZE', '100'))
    RATE_LIMIT_REQUESTS: int = int(os.environ.get('RATE_LIMIT_REQUESTS', '100'))
    RATE_LIMIT_BURST: int = int(os.environ.get('RATE_LIMIT_BURST', '20'))
    COMPRESS_RESPONSE_SIZE: int = int(os.environ.get('COMPRESS_RESPONSE_SIZE', '1024'))
    SESSION_ACQUIRE_TIMEOUT: int = int(os.environ.get('SESSION_ACQUIRE_TIMEOUT', '5'))
    SESSION_WARNING_THRESHOLD: int = int(os.environ.get('SESSION_WARNING_THRESHOLD', '3'))
    
    # Настройки для воркера
    WORKER_HIGH_QUEUE_SIZE: int = int(os.environ.get('WORKER_HIGH_QUEUE_SIZE', '1000'))
    WORKER_NORMAL_QUEUE_SIZE: int = int(os.environ.get('WORKER_NORMAL_QUEUE_SIZE', '5000'))
    WORKER_LOW_QUEUE_SIZE: int = int(os.environ.get('WORKER_LOW_QUEUE_SIZE', '10000'))
    WORKER_NUM_WORKERS: int = int(os.environ.get('WORKER_NUM_WORKERS', '10'))
    WORKER_BATCH_SIZE: int = int(os.environ.get('WORKER_BATCH_SIZE', '100'))
    WORKER_RETRY_COUNT: int = int(os.environ.get('WORKER_RETRY_COUNT', '3'))
    WORKER_MAX_CONCURRENT: int = int(os.environ.get('WORKER_MAX_CONCURRENT', '20'))
    
    # Медленные запросы
    SLOW_QUERY_THRESHOLD: float = float(os.environ.get('SLOW_QUERY_THRESHOLD', '0.5'))
    
    # WebSocket настройки
    WS_HEARTBEAT_INTERVAL: int = int(os.environ.get('WS_HEARTBEAT_INTERVAL', '30'))
    WS_CONNECTION_TIMEOUT: int = int(os.environ.get('WS_CONNECTION_TIMEOUT', '300'))
    WS_MAX_CONNECTIONS_PER_USER: int = int(os.environ.get('WS_MAX_CONNECTIONS_PER_USER', '5'))

common_config = CommonConfig()

# ============================================
# IN-MEMORY КЭШ (ПАРТИЦИОНИРОВАННЫЙ LRU)
# ============================================

class LRUCache:
    """
    LRU кэш с TTL и партиционированием
    Чистый in-memory, без Redis
    """
    
    def __init__(self, maxsize: int, ttl: int, name: str = "cache", num_partitions: int = 16):
        self.maxsize = maxsize
        self.default_ttl = ttl
        self.name = name
        self.num_partitions = num_partitions
        self._partitions = [OrderedDict() for _ in range(num_partitions)]
        self._expires = [{} for _ in range(num_partitions)]
        self._locks = [asyncio.Lock() for _ in range(num_partitions)]
        self._hits = 0
        self._misses = 0
        self._stats_lock = asyncio.Lock()
        
        logger.info(f"✅ LRUCache '{name}' initialized: {num_partitions} partitions, maxsize={maxsize}, ttl={ttl}s")
    
    def _get_partition(self, key: str) -> int:
        return hash(key) % self.num_partitions
    
    async def get(self, key: str) -> Optional[Any]:
        partition = self._get_partition(key)
        async with self._locks[partition]:
            cache = self._partitions[partition]
            expires = self._expires[partition]
            
            if key in cache:
                if key in expires and expires[key] < time.time():
                    del cache[key]
                    del expires[key]
                    async with self._stats_lock:
                        self._misses += 1
                    return None
                
                value = cache.pop(key)
                cache[key] = value
                async with self._stats_lock:
                    self._hits += 1
                return value
            
            async with self._stats_lock:
                self._misses += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        partition = self._get_partition(key)
        async with self._locks[partition]:
            cache = self._partitions[partition]
            expires = self._expires[partition]
            
            if key in cache:
                del cache[key]
            
            cache[key] = value
            expires[key] = time.time() + (ttl or self.default_ttl)
            
            if len(cache) > self.maxsize // self.num_partitions:
                oldest = next(iter(cache))
                del cache[oldest]
                del expires[oldest]
    
    async def delete(self, key: str):
        partition = self._get_partition(key)
        async with self._locks[partition]:
            cache = self._partitions[partition]
            expires = self._expires[partition]
            cache.pop(key, None)
            expires.pop(key, None)
    
    async def delete_pattern(self, pattern: str):
        pattern = pattern.replace('*', '')
        for p in range(self.num_partitions):
            async with self._locks[p]:
                to_delete = [k for k in self._partitions[p].keys() if pattern in k]
                for k in to_delete:
                    self._partitions[p].pop(k, None)
                    self._expires[p].pop(k, None)
    
    async def clear(self):
        for p in range(self.num_partitions):
            async with self._locks[p]:
                self._partitions[p].clear()
                self._expires[p].clear()
        
        logger.info(f"🧹 Cache '{self.name}' cleared")
    
    async def get_stats(self) -> Dict:
        total = self._hits + self._misses
        size = sum(len(p) for p in self._partitions)
        
        return {
            'name': self.name,
            'partitions': self.num_partitions,
            'size': size,
            'maxsize': self.maxsize,
            'usage_percent': (size / self.maxsize) * 100 if self.maxsize else 0,
            'hits': self._hits,
            'misses': self._misses,
            'hit_rate': self._hits / max(total, 1)
        }


class UserCache:
    """Кэш для пользователей"""
    _cache = LRUCache(maxsize=10000, ttl=common_config.CACHE_USER_TTL, name="user")
    
    @classmethod
    async def get(cls, user_id: str) -> Optional[Dict]:
        return await cls._cache.get(f"user:{user_id}")
    
    @classmethod
    async def set(cls, user_id: str, data: Dict):
        await cls._cache.set(f"user:{user_id}", data)
    
    @classmethod
    async def invalidate(cls, user_id: str):
        await cls._cache.delete(f"user:{user_id}")


class PostCache:
    """Кэш для постов с учётом пользователя"""
    _cache = LRUCache(maxsize=5000, ttl=common_config.CACHE_POST_TTL, name="post")
    
    @classmethod
    async def get(cls, post_id: str, user_id: str) -> Optional[Dict]:
        """Получить пост для конкретного пользователя"""
        return await cls._cache.get(f"post:{post_id}:{user_id}")
    
    @classmethod
    async def set(cls, post_id: str, user_id: str, data: Dict):
        """Сохранить пост для конкретного пользователя"""
        await cls._cache.set(f"post:{post_id}:{user_id}", data)
    
    @classmethod
    async def invalidate(cls, post_id: str):
        """Удалить все кэши поста для всех пользователей"""
        await cls._cache.delete_pattern(f"post:{post_id}:*")
    
    @classmethod
    async def invalidate_all(cls):
        """Удалить весь кэш постов"""
        await cls._cache.delete_pattern("post:*")
    
    @classmethod
    async def get_stats(cls) -> Dict:
        """Получить статистику кэша"""
        return await cls._cache.get_stats()
class FeedCache:
    """Кэш для ленты"""
    _cache = LRUCache(maxsize=2000, ttl=common_config.CACHE_FEED_TTL, name="feed")
    
    @classmethod
    async def get(cls, user_id: str, feed_type: str) -> Optional[List]:
        return await cls._cache.get(f"feed:{feed_type}:{user_id}")
    
    @classmethod
    async def set(cls, user_id: str, feed_type: str, data: List, ttl: int = None):
        ttl = ttl or common_config.CACHE_FEED_TTL
        await cls._cache.set(f"feed:{feed_type}:{user_id}", data, ttl)
    
    @classmethod
    async def invalidate(cls, user_id: str, feed_type: Optional[str] = None):
        if feed_type:
            await cls._cache.delete(f"feed:{feed_type}:{user_id}")
        else:
            await cls._cache.delete_pattern(f"feed:*:{user_id}")


class ReactionCache:
    """Кэш для реакций - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
    _cache = LRUCache(maxsize=5000, ttl=common_config.CACHE_REACTION_TTL, name="reaction")
    _user_cache = LRUCache(maxsize=10000, ttl=common_config.CACHE_REACTION_USER_TTL, name="reaction_user")
    
    @classmethod
    async def get_counts(cls, entity_type: str, entity_id: str) -> Optional[List]:
        """Получить счетчики реакций из кэша"""
        return await cls._cache.get(f"reactions:counts:{entity_type}:{entity_id}")
    
    @classmethod
    async def set_counts(cls, entity_type: str, entity_id: str, counts: List):
        """Сохранить счетчики реакций в кэш"""
        await cls._cache.set(f"reactions:counts:{entity_type}:{entity_id}", counts)
    
    @classmethod
    async def get_user_reaction(cls, user_id: str, entity_type: str, entity_id: str) -> Optional[Dict]:
        """Получить реакцию пользователя из кэша"""
        return await cls._user_cache.get(f"reactions:user:{user_id}:{entity_type}:{entity_id}")
    
    @classmethod
    async def set_user_reaction(cls, user_id: str, entity_type: str, entity_id: str, reaction: Optional[Dict]):
        """Сохранить реакцию пользователя в кэш"""
        await cls._user_cache.set(f"reactions:user:{user_id}:{entity_type}:{entity_id}", reaction)
    
    @classmethod
    async def get_batch_counts(cls, entity_type: str, entity_ids: List[str]) -> Tuple[Dict[str, List], List[str]]:
        """Получить счетчики для нескольких сущностей одним запросом"""
        if not entity_ids:
            return {}, []
        
        result = {}
        uncached_ids = []
        
        # Сначала проверяем кэш
        for entity_id in entity_ids:
            cached = await cls.get_counts(entity_type, entity_id)
            if cached is not None:
                result[entity_id] = cached
            else:
                uncached_ids.append(entity_id)
        
        return result, uncached_ids
    
    @classmethod
    async def set_batch_counts(cls, entity_type: str, counts_map: Dict[str, List]):
        """Сохранить счетчики для нескольких сущностей"""
        for entity_id, counts in counts_map.items():
            await cls.set_counts(entity_type, entity_id, counts)
    
    @classmethod
    async def invalidate(cls, entity_type: str, entity_id: str, user_id: Optional[str] = None):
        """Инвалидировать кэш реакций"""
        # Инвалидируем общие счетчики
        await cls._cache.delete(f"reactions:counts:{entity_type}:{entity_id}")
        
        # Инвалидируем пользовательские реакции если указан user_id
        if user_id:
            await cls._user_cache.delete(f"reactions:user:{user_id}:{entity_type}:{entity_id}")
        else:
            # Иначе инвалидируем все пользовательские реакции для этой сущности
            await cls._user_cache.delete_pattern(f"reactions:user:*:{entity_type}:{entity_id}")
    
    @classmethod
    async def invalidate_all(cls):
        """Инвалидировать весь кэш реакций"""
        await cls._cache.delete_pattern("reactions:counts:*")
        await cls._user_cache.delete_pattern("reactions:user:*")
    
    @classmethod
    async def get_stats(cls) -> Dict:
        """Получить статистику кэша"""
        main_stats = await cls._cache.get_stats()
        user_stats = await cls._user_cache.get_stats()
        return {
            'counts_cache': main_stats,
            'user_cache': user_stats,
            'total_size': main_stats['size'] + user_stats['size']
        }


class RepostCache:
    """Кэш для репостов"""
    _cache = LRUCache(maxsize=2000, ttl=common_config.CACHE_REPOST_TTL, name="repost")
    
    @classmethod
    async def get(cls, post_id: str, cursor: Optional[str] = None) -> Optional[Dict]:
        """Получить кэш репостов поста"""
        cache_key = f"reposts:{post_id}:{cursor or 'first'}"
        return await cls._cache.get(cache_key)
    
    @classmethod
    async def set(cls, post_id: str, data: Dict, cursor: Optional[str] = None):
        """Сохранить репосты поста в кэш"""
        cache_key = f"reposts:{post_id}:{cursor or 'first'}"
        await cls._cache.set(cache_key, data)
    
    @classmethod
    async def invalidate(cls, post_id: str):
        """Инвалидировать кэш репостов поста"""
        await cls._cache.delete_pattern(f"reposts:{post_id}:*")
    
    @classmethod
    async def invalidate_all(cls):
        """Инвалидировать весь кэш репостов"""
        await cls._cache.delete_pattern("reposts:*")


class CommentCache:
    """Кэш для комментариев"""
    _cache = LRUCache(maxsize=2000, ttl=common_config.CACHE_COMMENT_TTL, name="comment")
    
    @classmethod
    async def get(cls, post_id: str, cursor: Optional[str] = None) -> Optional[Dict]:
        cache_key = f"comments:{post_id}:{cursor or 'first'}"
        return await cls._cache.get(cache_key)
    
    @classmethod
    async def set(cls, post_id: str, data: Dict, cursor: Optional[str] = None):
        cache_key = f"comments:{post_id}:{cursor or 'first'}"
        await cls._cache.set(cache_key, data)
    
    @classmethod
    async def invalidate(cls, post_id: str):
        """Удалить все кэши комментариев поста"""
        await cls._cache.delete_pattern(f"comments:{post_id}:*")

# Глобальный кэш (для обратной совместимости)
cache = LRUCache(maxsize=5000, ttl=60, name="general")

# ============================================
# WEBSOCKET МЕНЕДЖЕР
# ============================================

class WebSocketManager:
    """
    Менеджер WebSocket соединений
    Поддерживает множество соединений на одного пользователя
    """
    
    _connections: Dict[str, Tuple[str, Any]] = {}  # connection_id -> (user_id, websocket)
    _user_connections: Dict[str, Set[str]] = {}    # user_id -> set(connection_ids)
    _lock = asyncio.Lock()
    _heartbeat_task: Optional[asyncio.Task] = None
    _running = False
    
    @classmethod
    async def register(cls, connection_id: str, user_id: str, websocket) -> bool:
        """
        Зарегистрировать новое соединение
        """
        async with cls._lock:
            # Проверяем лимит соединений для пользователя
            user_conns = cls._user_connections.get(user_id, set())
            if len(user_conns) >= common_config.WS_MAX_CONNECTIONS_PER_USER:
                logger.warning(f"⚠️ User {user_id} exceeded max connections ({common_config.WS_MAX_CONNECTIONS_PER_USER})")
                return False
            
            # Регистрируем соединение
            cls._connections[connection_id] = (user_id, websocket)
            
            if user_id not in cls._user_connections:
                cls._user_connections[user_id] = set()
            cls._user_connections[user_id].add(connection_id)
            
            logger.info(f"✅ WebSocket connection registered: {connection_id} for user {user_id}")
            logger.info(f"📊 Active connections: {len(cls._connections)}, users: {len(cls._user_connections)}")
            
            return True
    
    @classmethod
    async def unregister(cls, connection_id: str):
        """
        Удалить соединение
        """
        async with cls._lock:
            if connection_id in cls._connections:
                user_id, _ = cls._connections.pop(connection_id)
                
                if user_id in cls._user_connections:
                    cls._user_connections[user_id].discard(connection_id)
                    if not cls._user_connections[user_id]:
                        del cls._user_connections[user_id]
                
                logger.info(f"✅ WebSocket connection unregistered: {connection_id}")
                logger.info(f"📊 Active connections: {len(cls._connections)}, users: {len(cls._user_connections)}")
    
    @classmethod
    async def get_user_connections(cls, user_id: str) -> List[Any]:
        """
        Получить все WebSocket соединения пользователя
        """
        async with cls._lock:
            connection_ids = cls._user_connections.get(user_id, set())
            return [cls._connections[cid][1] for cid in connection_ids if cid in cls._connections]
    
    @classmethod
    async def send_to_user(cls, user_id: str, message: Dict) -> int:
        """
        Отправить сообщение всем соединениям пользователя
        Возвращает количество успешных отправок
        """
        sent_count = 0
        connections = await cls.get_user_connections(user_id)
        
        if not connections:
            logger.debug(f"📭 No active connections for user {user_id}")
            return 0
        
        message_str = json.dumps(message, default=str, ensure_ascii=False)
        
        for ws in connections:
            try:
                await ws.send_text(message_str)
                sent_count += 1
            except Exception as e:
                logger.error(f"❌ Failed to send WS message to user {user_id}: {e}")
        
        if sent_count > 0:
            logger.debug(f"📨 Sent message to user {user_id} via {sent_count} connections")
        
        return sent_count
    
    @classmethod
    async def send_to_connection(cls, connection_id: str, message: Dict) -> bool:
        """
        Отправить сообщение конкретному соединению
        """
        async with cls._lock:
            if connection_id not in cls._connections:
                logger.warning(f"⚠️ Connection {connection_id} not found")
                return False
            
            _, ws = cls._connections[connection_id]
        
        try:
            await ws.send_text(json.dumps(message, default=str, ensure_ascii=False))
            return True
        except Exception as e:
            logger.error(f"❌ Failed to send WS message to connection {connection_id}: {e}")
            return False
    
    @classmethod
    async def broadcast(cls, message: Dict, exclude_user_id: Optional[str] = None) -> int:
        """
        Отправить сообщение всем подключенным пользователям
        """
        sent_count = 0
        message_str = json.dumps(message, default=str, ensure_ascii=False)
        
        async with cls._lock:
            connections = list(cls._connections.items())
        
        for conn_id, (user_id, ws) in connections:
            if exclude_user_id and user_id == exclude_user_id:
                continue
            
            try:
                await ws.send_text(message_str)
                sent_count += 1
            except Exception as e:
                logger.error(f"❌ Failed to broadcast to {conn_id}: {e}")
                # Не удаляем здесь, чтобы не блокировать lock
        
        logger.info(f"📢 Broadcast sent to {sent_count} connections")
        return sent_count
    
    @classmethod
    async def get_stats(cls) -> Dict:
        """
        Получить статистику соединений
        """
        async with cls._lock:
            return {
                'total_connections': len(cls._connections),
                'total_users': len(cls._user_connections),
                'connections_per_user': {
                    user_id: len(conns) 
                    for user_id, conns in cls._user_connections.items()
                }
            }
    
    @classmethod
    async def start_heartbeat(cls):
        """
        Запустить heartbeat для проверки соединений
        """
        if cls._running:
            return
        
        cls._running = True
        cls._heartbeat_task = asyncio.create_task(cls._heartbeat_loop())
        logger.info("💓 WebSocket heartbeat started")
    
    @classmethod
    async def stop_heartbeat(cls):
        """
        Остановить heartbeat
        """
        cls._running = False
        if cls._heartbeat_task:
            cls._heartbeat_task.cancel()
            try:
                await cls._heartbeat_task
            except asyncio.CancelledError:
                pass
            cls._heartbeat_task = None
        logger.info("💔 WebSocket heartbeat stopped")
    
    @classmethod
    async def _heartbeat_loop(cls):
        """
        Периодическая проверка соединений
        """
        while cls._running:
            try:
                await asyncio.sleep(common_config.WS_HEARTBEAT_INTERVAL)
                
                dead_connections = []
                
                async with cls._lock:
                    for conn_id, (user_id, ws) in list(cls._connections.items()):
                        try:
                            # Отправляем ping
                            await ws.ping()
                        except Exception:
                            dead_connections.append(conn_id)
                
                # Удаляем мертвые соединения
                for conn_id in dead_connections:
                    await cls.unregister(conn_id)
                
                if dead_connections:
                    logger.info(f"💔 Removed {len(dead_connections)} dead connections")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Heartbeat error: {e}")


# Алиас для обратной совместимости
async def send_ws(user_id: str, message: Dict) -> int:
    """
    Отправить WebSocket сообщение пользователю
    """
    return await WebSocketManager.send_to_user(user_id, message)


# ============================================
# МОНИТОРИНГ СЕССИЙ
# ============================================

class SessionMetrics:
    """Метрики для отслеживания использования сессий"""
    _active_sessions = 0
    _total_sessions_created = 0
    _sessions_created_per_request = 0
    _requests_without_context = 0
    _session_errors = 0
    _last_warning_time = 0
    _warning_cooldown = 60
    _lock = asyncio.Lock()
    
    @classmethod
    async def session_acquired(cls):
        """Сессия получена из пула"""
        async with cls._lock:
            cls._active_sessions += 1
    
    @classmethod
    async def session_released(cls):
        """Сессия возвращена в пул"""
        async with cls._lock:
            cls._active_sessions -= 1
            if cls._active_sessions < 0:
                cls._active_sessions = 0
    
    @classmethod
    async def session_created(cls, repo_name: str, stack_info: str = ""):
        """Сессия создана (не переиспользована)"""
        async with cls._lock:
            cls._total_sessions_created += 1
            cls._active_sessions += 1
            cls._sessions_created_per_request += 1
            
            current_time = time.time()
            if current_time - cls._last_warning_time > cls._warning_cooldown:
                logger.warning(
                    f"⚠️ Создание новой сессии в репозитории {repo_name}\n"
                    f"Активных сессий: {cls._active_sessions}\n"
                    f"Всего создано: {cls._total_sessions_created}"
                )
                cls._last_warning_time = current_time
    
    @classmethod
    async def request_started(cls, request_id: str):
        """Начало обработки запроса"""
        async with cls._lock:
            cls._sessions_created_per_request = 0
    
    @classmethod
    async def request_ended(cls, request_id: str, path: str):
        """Конец обработки запроса"""
        async with cls._lock:
            if cls._sessions_created_per_request > 1:
                cls._requests_without_context += 1
                logger.error(
                    f"🔥 ЗАПРОС СОЗДАЛ {cls._sessions_created_per_request} СЕССИЙ!\n"
                    f"Request ID: {request_id}\n"
                    f"Path: {path}\n"
                    f"ИСПОЛЬЗУЙТЕ: async with RequestContext() as ctx:"
                )
    
    @classmethod
    async def session_error(cls):
        """Ошибка при работе с сессией"""
        async with cls._lock:
            cls._session_errors += 1
    
    @classmethod
    async def get_stats(cls) -> Dict:
        """Получить статистику"""
        async with cls._lock:
            return {
                'active_sessions': cls._active_sessions,
                'total_sessions_created': cls._total_sessions_created,
                'requests_without_context': cls._requests_without_context,
                'session_errors': cls._session_errors
            }


class Metrics:
    """Сбор метрик производительности"""
    _counters = {
        'db_queries': 0,
        'db_errors': 0,
        'cache_hits': 0,
        'cache_misses': 0,
        'rate_limited': 0,
        'idempotency_hits': 0,
        'worker_tasks': 0,
        'worker_completed': 0,
        'worker_failed': 0,
        'compressed_responses': 0,
        'reaction_toggles': 0,
        'reaction_queries': 0,
        'ws_messages_sent': 0,
        'ws_connections': 0,
        'ws_errors': 0
    }
    _lock = asyncio.Lock()
    
    @classmethod
    async def inc_counter(cls, name: str, value: int = 1):
        async with cls._lock:
            if name in cls._counters:
                cls._counters[name] += value
    
    @classmethod
    async def get_metrics(cls) -> Dict:
        async with cls._lock:
            return cls._counters.copy()

# ============================================
# ФОНОВЫЙ ВОРКЕР ДЛЯ ЗАДАЧ
# ============================================

class BackgroundTaskQueue:
    """
    Единая очередь для всех фоновых задач с приоритетами
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not BackgroundTaskQueue._initialized:
            self.high_queue = asyncio.Queue(maxsize=common_config.WORKER_HIGH_QUEUE_SIZE)
            self.normal_queue = asyncio.Queue(maxsize=common_config.WORKER_NORMAL_QUEUE_SIZE)
            self.low_queue = asyncio.Queue(maxsize=common_config.WORKER_LOW_QUEUE_SIZE)
            
            self.workers = []
            self.num_workers = common_config.WORKER_NUM_WORKERS
            self.semaphore = asyncio.Semaphore(common_config.WORKER_MAX_CONCURRENT)
            self.running = False
            
            self._stats = {
                'high': {'submitted': 0, 'completed': 0, 'failed': 0},
                'normal': {'submitted': 0, 'completed': 0, 'failed': 0},
                'low': {'submitted': 0, 'completed': 0, 'failed': 0}
            }
            self._stats_lock = asyncio.Lock()
            BackgroundTaskQueue._initialized = True
            
            logger.info(f"✅ BackgroundTaskQueue initialized: {self.num_workers} workers")
    
    async def start(self):
        if self.running:
            logger.info("⏭️ Background worker already running")
            return
        
        logger.info("🚀 Starting background worker...")
        self.running = True
        for i in range(self.num_workers):
            worker = asyncio.create_task(self._worker_loop(i), name=f"worker-{i}")
            self.workers.append(worker)
        
        logger.info(f"✅ Background workers started: {self.num_workers}")
        logger.info(f"   - High queue size: {common_config.WORKER_HIGH_QUEUE_SIZE}")
        logger.info(f"   - Normal queue size: {common_config.WORKER_NORMAL_QUEUE_SIZE}")
        logger.info(f"   - Low queue size: {common_config.WORKER_LOW_QUEUE_SIZE}")
        logger.info(f"   - Max concurrent: {common_config.WORKER_MAX_CONCURRENT}")
    
    async def stop(self, timeout: int = 30):
        if not self.running:
            return
        
        logger.info("🛑 Stopping background worker...")
        self.running = False
        
        total_size = self.high_queue.qsize() + self.normal_queue.qsize() + self.low_queue.qsize()
        if total_size > 0:
            logger.info(f"⏳ Waiting for {total_size} tasks to complete...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        self.high_queue.join(),
                        self.normal_queue.join(),
                        self.low_queue.join()
                    ),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ Timeout waiting for queues, {total_size} tasks remaining")
        
        for worker in self.workers:
            worker.cancel()
        
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()
        logger.info("✅ Background workers stopped")
    
    async def add_high(self, func: Callable, **kwargs) -> bool:
        return await self._add_task(self.high_queue, 'high', func, **kwargs)
    
    async def add_normal(self, func: Callable, **kwargs) -> bool:
        return await self._add_task(self.normal_queue, 'normal', func, **kwargs)
    
    async def add_low(self, func: Callable, **kwargs) -> bool:
        return await self._add_task(self.low_queue, 'low', func, **kwargs)
    
    async def _add_task(self, queue: asyncio.Queue, priority: str, func: Callable, **kwargs) -> bool:
        task_id = str(uuid.uuid4())[:8]
        retries = kwargs.pop('_retries', 0)
        
        task = {
            'id': task_id,
            'func': func,
            'kwargs': kwargs,
            'priority': priority,
            'retries': retries,
            'created_at': time.time()
        }
        
        try:
            await queue.put(task)
            async with self._stats_lock:
                self._stats[priority]['submitted'] += 1
            await Metrics.inc_counter('worker_tasks')
            logger.debug(f"📦 Task {task_id} added to {priority} queue")
            return True
        except asyncio.QueueFull:
            logger.error(f"❌ Queue full for {priority} priority, task {task_id} rejected")
            return False
    
    async def _worker_loop(self, worker_id: int):
        logger.debug(f"👷 Worker {worker_id} started")
        
        while self.running:
            try:
                task = await self._get_next_task()
                if not task:
                    await asyncio.sleep(0.1)
                    continue
                
                async with self.semaphore:
                    await self._process_task(task)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Worker {worker_id} error: {e}")
                await asyncio.sleep(1)
        
        logger.debug(f"👋 Worker {worker_id} stopped")
    
    async def _get_next_task(self) -> Optional[Dict]:
        try:
            return self.high_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        
        try:
            return self.normal_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        
        try:
            return await asyncio.wait_for(self.low_queue.get(), timeout=0.1)
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            return None
    
    async def _process_task(self, task: Dict):
        task_id = task['id']
        priority = task['priority']
        func = task['func']
        kwargs = task['kwargs']
        retries = task.get('retries', 0)
        
        try:
            logger.debug(f"⚙️ Processing {priority} task {task_id}")
            start_time = time.time()
            
            if asyncio.iscoroutinefunction(func):
                result = await func(**kwargs)
            else:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, func, **kwargs)
            
            duration = time.time() - start_time
            logger.debug(f"✅ Completed {priority} task {task_id} in {duration:.2f}s")
            
            await self._task_done(task, success=True)
            
        except Exception as e:
            logger.error(f"❌ Task {task_id} failed: {e}")
            
            if retries < common_config.WORKER_RETRY_COUNT:
                logger.info(f"🔄 Retrying task {task_id} (attempt {retries + 1})")
                kwargs['_retries'] = retries + 1
                queue = self._get_queue(priority)
                await self._add_task(queue, priority, func, **kwargs)
            else:
                await self._task_done(task, success=False)
    
    async def _task_done(self, task: Dict, success: bool):
        queue = self._get_queue(task['priority'])
        queue.task_done()
        
        async with self._stats_lock:
            if success:
                self._stats[task['priority']]['completed'] += 1
                await Metrics.inc_counter('worker_completed')
            else:
                self._stats[task['priority']]['failed'] += 1
                await Metrics.inc_counter('worker_failed')
    
    def _get_queue(self, priority: str) -> asyncio.Queue:
        if priority == 'high':
            return self.high_queue
        elif priority == 'normal':
            return self.normal_queue
        else:
            return self.low_queue
    
    def get_total_size(self) -> int:
        return self.high_queue.qsize() + self.normal_queue.qsize() + self.low_queue.qsize()
    
    def get_stats(self) -> Dict:
        return {
            **self._stats,
            'queues': {
                'high': self.high_queue.qsize(),
                'normal': self.normal_queue.qsize(),
                'low': self.low_queue.qsize()
            },
            'total_size': self.get_total_size(),
            'workers': self.num_workers,
            'running': self.running,
            'max_concurrent': common_config.WORKER_MAX_CONCURRENT
        }


# Глобальный экземпляр воркера
background_worker = BackgroundTaskQueue()

# ============================================
# ДЕКОРАТОРЫ
# ============================================

def retry(max_attempts: int = common_config.RETRY_ATTEMPTS):
    """Декоратор для повторных попыток при временных ошибках"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            delay = common_config.RETRY_DELAY
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"Retry {attempt + 1}/{max_attempts} for {func.__name__}: {e}")
                        await asyncio.sleep(min(delay, common_config.RETRY_MAX_DELAY))
                        delay *= common_config.RETRY_BACKOFF
            
            raise last_error
        return wrapper
    return decorator


def rate_limit(requests: int = common_config.RATE_LIMIT_REQUESTS, window: int = 60):
    """Декоратор для ограничения частоты запросов"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, event, user, *args, **kwargs):
            if hasattr(self, '_rate_limit_check'):
                key = f"rate_limit:{user.get('user_id', 'anonymous')}:{func.__name__}"
                current = await cache.get(key) or 0
                if current >= requests:
                    raise RateLimitError("Too many requests")
                await cache.set(key, current + 1, ttl=window)
            return await func(self, event, user, *args, **kwargs)
        return wrapper
    return decorator


def measure_time(func):
    """Декоратор для измерения времени выполнения"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.time()
        try:
            return await func(*args, **kwargs)
        finally:
            duration = time.time() - start
            if duration > common_config.SLOW_QUERY_THRESHOLD:
                logger.warning(f"🐢 Slow operation {func.__name__}: {duration:.2f}s")
    return wrapper


def with_request_context(func):
    """
    Декоратор для автоматического создания RequestContext
    Использование: @with_request_context
    """
    @wraps(func)
    async def wrapper(self, event, user, *args, **kwargs):
        async with RequestContext() as ctx:
            kwargs['_request_context'] = ctx
            kwargs['session'] = ctx.session
            return await func(self, event, user, *args, **kwargs)
    return wrapper


def background_task(priority: str = 'normal'):
    """Декоратор для фоновых задач"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if priority == 'high':
                return await background_worker.add_high(func, *args, **kwargs)
            elif priority == 'normal':
                return await background_worker.add_normal(func, *args, **kwargs)
            else:
                return await background_worker.add_low(func, *args, **kwargs)
        return wrapper
    return decorator

# ============================================
# REQUEST CONTEXT (ИСПРАВЛЕННАЯ ВЕРСИЯ)
# ============================================

class RequestContext:
    """
    Контекст HTTP запроса - создает одну сессию на весь запрос
    Использование:
        async with RequestContext() as ctx:
            repo = PostRepository(ctx.session)
            result = await repo.get(...)
    """
    
    def __init__(self):
        self.session = None
        self._owns_session = False
        self._request_id = None
        self._created_at = None
        self._queries = []  # для сбора статистики запросов
    
    async def __aenter__(self):
        self._created_at = time.time()
        self._request_id = str(uuid.uuid4())[:8]
        
        # Логируем начало запроса
        logger.info(f"🚀 REQUEST START [{self._request_id}]")
        
        await SessionMetrics.request_started(self._request_id)
        
        try:
            # ✅ ИСПРАВЛЕНО: используем ydb_pool вместо get_db_pool()
            from app.db.pool import ydb_pool
            
            if not ydb_pool:
                logger.error(f"❌ RequestContext [{self._request_id}]: database pool not initialized")
                raise DatabaseError("Database pool not initialized")
            
            # Получаем сессию (синхронно)
            session = ydb_pool.get_session()
            self.session = session
            self._owns_session = True
                
            await SessionMetrics.session_acquired()
            
            # Получаем ID сессии
            session_id = 'unknown'
            if hasattr(self.session, 'session_id'):
                session_id = self.session.session_id
            elif hasattr(self.session, '_session_id'):
                session_id = self.session._session_id
            
            logger.debug(f"📊 RequestContext [{self._request_id}]: session acquired (id: {session_id})")
            
        except Exception as e:
            logger.error(f"❌ RequestContext [{self._request_id}]: error acquiring session: {e}")
            await SessionMetrics.session_error()
            raise
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._owns_session and self.session:
            try:
                # ✅ ИСПРАВЛЕНО: используем ydb_pool
                from app.db.pool import ydb_pool
                if ydb_pool:
                    ydb_pool.release_session(self.session)
                
                await SessionMetrics.session_released()
                
                duration = time.time() - self._created_at
                
                # Логируем завершение запроса со статистикой
                query_count = len(self._queries)
                slow_queries = sum(1 for q in self._queries if q['duration'] > common_config.SLOW_QUERY_THRESHOLD)
                
                if duration > 1.0:
                    logger.warning(
                        f"🏁 REQUEST END [{self._request_id}] "
                        f"duration={duration:.2f}s "
                        f"queries={query_count} "
                        f"slow={slow_queries}"
                    )
                else:
                    logger.info(
                        f"🏁 REQUEST END [{self._request_id}] "
                        f"duration={duration:.2f}s "
                        f"queries={query_count} "
                        f"slow={slow_queries}"
                    )
                
                # Если есть медленные запросы - покажем их
                if slow_queries > 0:
                    for q in self._queries:
                        if q['duration'] > common_config.SLOW_QUERY_THRESHOLD:
                            logger.warning(
                                f"🐢 SLOW QUERY [{self._request_id}] "
                                f"{q['duration']:.2f}s - {q['query'][:200]}..."
                            )
                
                logger.debug(f"📊 RequestContext [{self._request_id}]: session released after {duration:.2f}s")
                    
            except Exception as e:
                logger.error(f"❌ RequestContext [{self._request_id}]: error: {e}")
                await SessionMetrics.session_error()
            finally:
                self.session = None
                self._owns_session = False
        
        # Записываем статистику по запросу
        path = getattr(exc_val, 'path', 'unknown') if exc_val else 'unknown'
        await SessionMetrics.request_ended(self._request_id, path)
    
    def get_request_id(self) -> str:
        return self._request_id
    
    def add_query(self, query: str, duration: float):
        """Добавить информацию о выполненном запросе"""
        self._queries.append({
            'query': query,
            'duration': duration,
            'time': time.time()
        })

# ============================================
# БАЗОВЫЙ РЕПОЗИТОРИЙ (С ДОБАВЛЕННЫМИ МЕТОДАМИ)
# ============================================

class BaseRepository:
    """Базовый класс для всех репозиториев с правильным управлением сессиями"""
    
    def __init__(self, session=None):
        """
        Args:
            session: существующая сессия (из UOW или RequestContext)
                    Если None - будет создана своя сессия при execute (НЕ РЕКОМЕНДУЕТСЯ!)
        """
        self.table_name = None
        self._prepared_queries = {}
        self._field_type_cache = {}  # 👈 Кэш для типов полей
        self._batch_size = 100  # 👈 Размер батча
        self._session = session
        self._transaction = None
        self._owns_session = session is None
        self._session_id = id(session) if session else None
        
        if session is None:
            self._log_no_session_warning()
    
    def _log_no_session_warning(self):
        """Логирует предупреждение о создании репозитория без сессии"""
        try:
            stack = inspect.stack()
            caller_frame = None
            for frame in stack[2:]:
                if 'repository' not in frame.filename.lower() and 'base' not in frame.filename.lower():
                    caller_frame = frame
                    break
            
            if caller_frame:
                caller = caller_frame.function
                filename = caller_frame.filename
                lineno = caller_frame.lineno
            else:
                caller = "unknown"
                filename = "unknown"
                lineno = 0
            
            logger.warning(
                f"⚠️ РЕПОЗИТОРИЙ БЕЗ СЕССИИ: {self.__class__.__name__}\n"
                f"Создан в: {caller} ({filename}:{lineno})\n"
                f"Это приведет к созданию новой сессии на КАЖДЫЙ запрос!\n"
                f"ИСПОЛЬЗУЙТЕ: async with RequestContext() as ctx:\n"
                f"              repo = {self.__class__.__name__}(ctx.session)"
            )
        except Exception as e:
            logger.warning(f"⚠️ Репозиторий {self.__class__.__name__} создан без сессии")
    
    def set_session(self, session):
        """Установить внешнюю сессию (из UOW)"""
        self._session = session
        self._owns_session = False
        self._session_id = id(session) if session else None
    
    def set_transaction(self, transaction):
        """Установить транзакцию (из UOW)"""
        self._transaction = transaction
    
    async def _get_session(self):
        """
        Получить сессию:
        - Если есть внешняя сессия (_session) - используем её
        - Если нет - создаем новую (с предупреждением)
        """
        if self._session:
            logger.debug(f"♻️ Используем существующую сессию: {self._session_id}")
            return self._session
        
        # ✅ ИСПРАВЛЕНО: используем ydb_pool
        from app.db.pool import ydb_pool
        
        if not ydb_pool:
            raise DatabaseError("Database pool not initialized")
        
        stack_info = ''.join(traceback.format_stack()[:-1])
        await SessionMetrics.session_created(self.__class__.__name__, stack_info)
        
        for attempt in range(common_config.RETRY_ATTEMPTS):
            try:
                logger.debug(f"🆕 Попытка {attempt + 1} создания новой сессии для {self.__class__.__name__}")
                
                session = ydb_pool.get_session()
                
                if session:
                    self._session = session
                    self._owns_session = True
                    self._session_id = id(session)
                    logger.debug(f"✅ Создана новая сессия: {self._session_id}")
                    return session
                
            except Exception as e:
                logger.warning(f"❌ Ошибка получения сессии, попытка {attempt + 1}: {e}")
            
            if attempt < common_config.RETRY_ATTEMPTS - 1:
                await asyncio.sleep(common_config.RETRY_DELAY * (attempt + 1))
        
        raise DatabaseError("Не удалось получить сессию после нескольких попыток")
    
    async def _release_session(self):
        """
        Освободить сессию, если мы ей владеем
        """
        if self._owns_session and self._session:
            try:
                # ✅ ИСПРАВЛЕНО: используем ydb_pool
                from app.db.pool import ydb_pool
                
                if ydb_pool:
                    session_id = self._session_id
                    ydb_pool.release_session(self._session)
                    
                    logger.debug(f"♻️ Освобождена сессия (принадлежала репозиторию): {session_id}")
                    await SessionMetrics.session_released()
                
            except Exception as e:
                logger.error(f"❌ Ошибка освобождения сессии: {e}")
                await SessionMetrics.session_error()
            finally:
                self._session = None
                self._session_id = None
                self._owns_session = False
    
    def _escape_like(self, text: Any) -> str:
        """Экранирование спецсимволов для LIKE"""
        if text is None:
            return ''
        str_text = str(text)
        return str_text.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    
    # ============================================
    # НОВЫЕ МЕТОДЫ ДЛЯ BATCH И ТИПИЗАЦИИ
    # ============================================
    
    async def batch_get(self, keys: List[str], loader_func: Callable) -> Dict[str, Any]:
        """Batch get с объединением запросов"""
        if not keys:
            return {}
        
        # Дедупликация
        unique_keys = list(set(keys))
        
        # Разбиваем на чанки
        results = {}
        for i in range(0, len(unique_keys), self._batch_size):
            chunk = unique_keys[i:i + self._batch_size]
            chunk_results = await loader_func(chunk)
            results.update(chunk_results)
        
        return results
    
    def _get_field_type(self, field_name: str, value: Any = None) -> str:
        """Определить тип поля для DECLARE с кэшированием"""
        
        # Убираем цифровые суффиксы
        base_name = re.sub(r'_\d+$', '', field_name.replace('$', ''))
        
        # Проверяем кэш
        cache_key = f"{base_name}"
        if cache_key in self._field_type_cache:
            return self._field_type_cache[cache_key]
        
        # Определяем тип
        field_type = self._determine_field_type(base_name, value)
        
        # Сохраняем в кэш
        self._field_type_cache[cache_key] = field_type
        return field_type
    
    def _determine_field_type(self, field_name: str, value: Any = None) -> str:
        """Определить тип поля по имени и значению"""
        field_name = field_name.lower()
        
        # 👇 ЯВНО УКАЗЫВАЕМ, ЧТО ЭТО СТРОКИ!
        if field_name in ['post_id', 'user_id', 'username', 'display_name', 
                          'title', 'content', 'content_preview', 'visibility',
                          'language', 'repost_comment', 'original_post_id',
                          'original_author', 'original_post', 'hashtag_id',
                          'comment_id', 'reaction_id', 'repost_id', 'like_id',
                          'bookmark_id', 'follow_id', 'notification_id',
                          'report_id', 'mention_id', 'ip_address', 'user_agent',
                          'folder', 'notes', 'reason', 'description', 'status',
                          'entity_type', 'entity_id', 'reaction_type',
                          'follower_id', 'following_id']:
            return 'Utf8'
        
        # Timestamp поля
        if field_name in ['created_at', 'updated_at', 'published_at', 'scheduled_for', 
                          'deleted_at', 'last_login_at', 'locked_until', 'read_at', 
                          'expires_at', 'last_used_at', 'joined_at', 'left_at',
                          'banned_at', 'pinned_at', 'unpinned_at', 'last_message_at',
                          'last_active_at', 'last_read_at', 'mute_until', 'reviewed_at',
                          'username_updated_at', 'forwarded_at', 'original_date', 'blocked_at',
                          'added_at', 'last_interaction_at', 'auto_saved_at', 'completed_at']:
            return 'Timestamp'
        
        # Uint64 поля
        if field_name in ['view_duration_ms', 'row_version', 'failed_login_attempts',
                          'id', 'chat_id', 'message_id', 'event_id', 'ban_id', 'invite_id',
                          'request_id', 'target_id', 'last_message_id', 'reply_to_message_id',
                          'thread_root_id', 'forwarded_from_message_id', 'forwarded_from_chat_id',
                          'linked_chat_id', 'linked_discussion_message_id', 'linked_channel_message_id',
                          'version', 'cursor_id', 'channel_id', 'channel_message_id', 'source_channel_id',
                          'forward_count', 'edit_count', 'reply_count', 'thread_messages_count',
                          'notification_id', 'attachment_id', 'photo_id', 'contact_id', 'blocked_id',
                          'role_order', 'pin_order']:
            return 'Uint64'
        
        # Uint32 поля
        if field_name in ['likes_count', 'comments_count', 'reposts_count', 'views_count',
                          'bookmarks_count', 'replies_count', 'posts_count', 'followers_count',
                          'following_count', 'limit', 'offset', 'position_start', 'position_end',
                          'total_count', 'count', 'co_occurrence', 'retry_count', 'failure_count',
                          'members_count', 'messages_count', 'online_estimate', 'unread_count',
                          'max_members', 'slow_mode_interval', 'progress', 'used_count',
                          'max_uses', 'remaining_uses', 'width', 'height', 'duration_seconds',
                          'file_size', 'importance', 'maxsize', 'ttl', 'num_partitions',
                          'hit_rate', 'usage_percent', 'reactions_count']:  # 👈 ДОБАВЛЕНО
            return 'Uint32'
        
        # Uint8 поля
        if field_name in ['reading_time_minutes', 'max_requests', 'period', 'attempt',
                          'retries', 'batch_size', 'worker_id', 'priority']:
            return 'Uint8'
        
        # Bool поля
        if field_name in ['is_verified', 'is_deleted', 'is_edited', 'is_hidden', 'is_repost',
                          'is_read', 'is_following', 'is_liked', 'is_bookmarked', 'is_owner',
                          'is_pinned', 'show_original', 'email_verified', 'success', 'has_more',
                          'permanent', 'is_public', 'join_moderation', 'is_active', 'is_archived',
                          'is_discussion', 'comments_enabled', 'reactions_enabled', 'is_deleted_for_all',
                          'is_blocked', 'is_permanent', 'can_join', 'requires_approval',
                          'last_read_message_valid', 'show_in_profile', 'is_published',
                          'is_favorite', 'is_processed', 'is_edited', 'is_pinned',
                          'is_repost', 'is_from_channel']:
            return 'Bool'
        
        # JSON поля
        if field_name in ['media_urls', 'extra_data', 'mentions', 'hashtags', 'metadata',
                          'reactions_preview', 'context', 'settings', 'permissions',
                          'restrictions', 'payload', 'attachments', 'reactions', 'entities',
                          'edit_history', 'discussion_settings', 'comments_settings',
                          'reactions_settings', 'attachments_json', 'reactions_json',
                          'recent_reactions', 'mentions_json', 'urls', 'urls_map',
                          'reaction_counts', 'interactions', 'pagination', 'profile',
                          'statistics', 'extra', 'data', 'collections', 'do_not_disturb',
                          'private_chats', 'groups', 'channels']:
            return 'Json'
        
        # Double поля
        if field_name in ['sentiment_score', 'relevance_score', 'compression_ratio',
                          'duration', 'uptime_seconds', 'cpu_usage']:
            return 'Double'
        
        # Date поля
        if field_name in ['created_date', 'date', 'last_seen_date']:
            return 'Date'
        
        # По умолчанию - строка
        return 'Utf8'
    
    def _generate_declare(self, params: Dict) -> str:
        """Генерация DECLARE с правильными типами"""
        if not params:
            return ""
        
        # Проверяем кэш запросов
        cache_key = hashlib.md5(str(sorted(params.items())).encode()).hexdigest()
        if cache_key in self._prepared_queries:
            return self._prepared_queries[cache_key]
        
        declares = []
        seen = set()
        
        for key in params.keys():
            # Убираем индексы для определения типа
            base_key = re.sub(r'_\d+$', '', key.replace('$', ''))
            
            # 👇 КРИТИЧЕСКИ ВАЖНО: для post_id и user_id всегда Utf8!
            if base_key in ['post_id', 'user_id']:
                field_type = 'Utf8'
            else:
                if base_key in seen:
                    continue
                field_type = self._get_field_type(base_key, params[key])
            
            declares.append(f"DECLARE {key} AS {field_type};")
            seen.add(base_key)
        
        result = "\n".join(declares)
        self._prepared_queries[cache_key] = result
        return result
    
    @retry(max_attempts=common_config.RETRY_ATTEMPTS)
    @measure_time
    async def execute(self, query: str, params: Dict = None) -> List[Dict]:
        """
        Выполнить запрос с правильным управлением сессией
        Сессия освобождается ТОЛЬКО если была создана нами
        """
        session = None
        session_created_now = False
        start_time = time.time()
        
        try:
            session = await self._get_session()
            session_created_now = self._owns_session and session is not None
            
            if params:
                # Очищаем параметры
                clean_params = {}
                for k, v in params.items():
                    if isinstance(v, str):
                        if v.startswith("'") and v.endswith("'"):
                            v = v[1:-1]
                    clean_params[k] = v
                
                logger.debug(f"Executing query with {len(params)} params using session: {self._session_id}")
                
                try:
                    # prepare - синхронный!
                    prepared = session.prepare(query)
                    
                    if self._transaction:
                        result = await self._transaction.execute(prepared, clean_params)
                    else:
                        result = session.transaction().execute(prepared, clean_params, commit_tx=True)
                    
                    rows = result[0].rows if result and len(result) > 0 and hasattr(result[0], 'rows') else []
                    
                    duration = time.time() - start_time
                    if duration > common_config.SLOW_QUERY_THRESHOLD:
                        logger.warning(f"🐢 Slow query ({duration:.2f}s) in session {self._session_id}")
                    
                    await Metrics.inc_counter('db_queries')
                    
                    # Сохраняем статистику в RequestContext если есть
                    try:
                        for frame in inspect.stack():
                            if 'self' in frame.frame.f_locals:
                                obj = frame.frame.f_locals['self']
                                if isinstance(obj, RequestContext):
                                    obj.add_query(query, duration)
                                    break
                    except:
                        pass
                    
                    return rows
                    
                except Exception as e:
                    error_str = str(e).lower()
                    if ("type mismatch" in error_str or 
                        "failed to convert type" in error_str):
                        
                        logger.warning(f"Type mismatch detected, retrying: {e}")
                        self._prepared_queries = {}
                        self._field_type_cache = {}  # Сбрасываем кэш типов
                        
                        prepared = session.prepare(query)
                        
                        if self._transaction:
                            result = await self._transaction.execute(prepared, clean_params)
                        else:
                            result = session.transaction().execute(prepared, clean_params, commit_tx=True)
                        
                        rows = result[0].rows if result and len(result) > 0 and hasattr(result[0], 'rows') else []
                        return rows
                    else:
                        raise
            else:
                logger.debug(f"Executing query without params using session: {self._session_id}")
                
                if self._transaction:
                    result = await self._transaction.execute(query)
                else:
                    result = session.transaction().execute(query, commit_tx=True)
                
                rows = result[0].rows if result and len(result) > 0 and hasattr(result[0], 'rows') else []
                
                duration = time.time() - start_time
                if duration > common_config.SLOW_QUERY_THRESHOLD:
                    logger.warning(f"🐢 Slow query ({duration:.2f}s) in session {self._session_id}")
                
                await Metrics.inc_counter('db_queries')
                return rows
                
        except Exception as e:
            logger.error(f"Database error in session {self._session_id}: {e}")
            await Metrics.inc_counter('db_errors')
            await SessionMetrics.session_error()
            
            if "timeout" in str(e).lower():
                raise DatabaseError("Query timeout")
            raise DatabaseError(f"Database error: {str(e)}")
        finally:
            if session_created_now:
                await self._release_session()
    
    async def execute_many(self, query: str, params_list: List[Dict]) -> bool:
        """Выполнить несколько запросов в одном batch"""
        if not params_list:
            return True
        
        session_created_now = False
        
        try:
            session = await self._get_session()
            session_created_now = self._owns_session and session is not None
            
            for i in range(0, len(params_list), common_config.BATCH_SIZE):
                batch = params_list[i:i + common_config.BATCH_SIZE]
                for params in batch:
                    await self.execute(query, params)
            
            return True
        except Exception as e:
            logger.error(f"Batch execute error: {e}")
            await Metrics.inc_counter('db_errors')
            return False
        finally:
            if session_created_now:
                await self._release_session()
    
    async def check_session_alive(self) -> bool:
        """Проверить, жива ли сессия"""
        try:
            if not hasattr(self._session, '_session_id') or not self._session._session_id:
                return False
            
            await self.execute("SELECT 1;")
            return True
        except Exception:
            return False


class TransactionAwareRepository(BaseRepository):
    """
    Репозиторий с поддержкой транзакций (синхронная версия)
    """
    
    def __init__(self, session=None, transaction=None):
        super().__init__(session)
        self._transaction = transaction
    
    def set_transaction(self, transaction):
        self._transaction = transaction
    
    async def execute(self, query: str, params: Dict = None) -> List[Dict]:
        if self._transaction:
            if params:
                prepared = self._session.prepare(query)
                result = self._transaction.execute(prepared, params)
            else:
                result = self._transaction.execute(query)
            
            rows = []
            if result:
                for result_set in result:
                    if hasattr(result_set, 'rows') and result_set.rows:
                        for row in result_set.rows:
                            row_dict = {}
                            
                            # 1. Безопасная попытка получить _asdict, обходя проблему KeyError от YDB SDK
                            try:
                                row_dict = row._asdict()
                            except (AttributeError, KeyError, TypeError):
                                pass
                            
                            # 2. Если _asdict не сработал, пробуем альтернативные способы извлечения
                            if not row_dict:
                                if isinstance(row, dict):
                                    row_dict = row
                                elif hasattr(row, '__dict__'):
                                    row_dict = {k: v for k, v in row.__dict__.items() if not k.startswith('_')}
                                elif hasattr(row, '_fields') and hasattr(row, '_values'):
                                    # YDB namedtuple
                                    row_dict = dict(zip(row._fields, row._values))
                                else:
                                    # Последняя надежда: конвертируем в строку
                                    try:
                                        row_dict = {'raw': str(row)}
                                    except Exception:
                                        pass
                                        
                            rows.append(row_dict)
            return rows
        else:
            return await BaseRepository.execute(self, query, params)


# ============================================
# UNIT OF WORK
# ============================================

# ============================================
# UNIT OF WORK (СИНХРОННАЯ ВЕРСИЯ)
# ============================================

class UnitOfWork:
    """Unit of Work паттерн с поддержкой транзакций (синхронная версия)"""
    
    def __init__(self):
        self._session = None
        self._transaction = None
        self._repositories = []
        self._uow_id = str(uuid.uuid4())[:8]
        self._in_progress = False
    
    @classmethod
    async def from_session(cls, session):
        """Создать UOW с существующей сессией"""
        uow = cls()
        uow._session = session
        uow._transaction = session.transaction()
        uow._transaction.begin()  # ← синхронно!
        uow._in_progress = True
        logger.debug(f"📦 UOW [{uow._uow_id}]: started on existing session")
        return uow
    
    async def __aenter__(self):
        if not self._in_progress and not self._session:
            # Создаем новую сессию
            from app.db.pool import ydb_pool
            
            if not ydb_pool:
                raise DatabaseError("Database pool not initialized")
            
            self._session = ydb_pool.get_session()  # ← синхронно!
            self._transaction = self._session.transaction()
            self._transaction.begin()  # ← синхронно!
            self._in_progress = True
            logger.debug(f"📦 UOW [{self._uow_id}]: started on new session")
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                if self._transaction:
                    self._transaction.rollback()  # ← синхронно!
                    logger.debug(f"📦 UOW [{self._uow_id}]: rolled back")
            else:
                if self._transaction:
                    self._transaction.commit()  # ← синхронно!
                    logger.debug(f"📦 UOW [{self._uow_id}]: committed")
        finally:
            if self._session:
                from app.db.pool import ydb_pool
                if ydb_pool:
                    ydb_pool.release_session(self._session)  # ← синхронно!
                    logger.debug(f"📦 UOW [{self._uow_id}]: session released")
            self._session = None
            self._transaction = None
            self._in_progress = False
    
    def repository(self, repo_class: type) -> 'TransactionAwareRepository':
        """Получить репозиторий в рамках UOW"""
        if not self._in_progress:
            raise RuntimeError("Cannot get repository outside transaction")
        
        repo = repo_class(self._session)
        repo.set_transaction(self._transaction)
        return repo
    
    @asynccontextmanager
    async def savepoint(self, name: str = None):
        """Создать savepoint в транзакции"""
        if not self._in_progress:
            raise RuntimeError("Cannot create savepoint outside transaction")
        
        savepoint_name = name or f"sp_{self._uow_id}_{uuid.uuid4().hex[:6]}"
        
        logger.debug(f"📦 UOW [{self._uow_id}]: creating savepoint {savepoint_name}")
        
        try:
            self._transaction.execute(f"BEGIN TRANSACTION {savepoint_name};", {})  # ← синхронно!
            yield
            logger.debug(f"📦 UOW [{self._uow_id}]: savepoint {savepoint_name} released")
        except Exception as e:
            logger.warning(f"📦 UOW [{self._uow_id}]: rolling back to {savepoint_name}: {e}")
            self._transaction.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};", {})  # ← синхронно!
            raise
# ============================================
# МОДЕЛЬ КЛЮЧА ИДЕМПОТЕНТНОСТИ
# ============================================

@dataclass
class IdempotencyKey:
    """Модель ключа идемпотентности"""
    idempotency_key: str
    entity_type: str
    entity_id: int
    user_id: str
    created_at: datetime
    expires_at: Optional[datetime] = None

    def to_db_row(self) -> Dict[str, Any]:
        return {
            'idempotency_key': self.idempotency_key,
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'user_id': str(self.user_id),
            'created_at': to_timestamp(self.created_at),
            'expires_at': to_timestamp(self.expires_at) if self.expires_at else None
        }

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'IdempotencyKey':
        return cls(
            idempotency_key=row.get('idempotency_key'),
            entity_type=row.get('entity_type'),
            entity_id=row.get('entity_id'),
            user_id=str(row.get('user_id')),
            created_at=from_timestamp(row.get('created_at')),
            expires_at=from_timestamp(row.get('expires_at'))
        )


class IdempotencyRepository(TransactionAwareRepository):
    """Репозиторий для идемпотентности"""
    
    def __init__(self, session=None, transaction=None):
        super().__init__(session, transaction)
        self.table_name = "idempotency_keys"
        self._local_cache = {}
    
    async def check_and_create(self, idempotency_key: str, entity_type: str, 
                               entity_id: int, user_id: str) -> bool:
        cache_key = f"{idempotency_key}:{user_id}"
        if cache_key in self._local_cache:
            if time.time() - self._local_cache[cache_key] < 60:
                await Metrics.inc_counter('idempotency_hits')
                return False
        
        existing = await self.get(idempotency_key)
        if existing:
            if existing.user_id == user_id:
                await Metrics.inc_counter('idempotency_hits')
                self._local_cache[cache_key] = time.time()
                return False
            return False
        
        now = datetime.utcnow()
        expires_at = now + timedelta(hours=24)
        
        key = IdempotencyKey(
            idempotency_key=idempotency_key,
            entity_type=entity_type,
            entity_id=entity_id,
            user_id=user_id,
            created_at=now,
            expires_at=expires_at
        )
        
        success = await self.create(key)
        if success:
            self._local_cache[cache_key] = time.time()
        
        return success
    
    async def create(self, key: IdempotencyKey) -> bool:
        data = key.to_db_row()
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error creating idempotency key: {e}")
            return False
    
    async def get(self, idempotency_key: str) -> Optional[IdempotencyKey]:
        query = f"""
        DECLARE $idempotency_key AS Utf8;
        SELECT * FROM {self.table_name} WHERE idempotency_key = $idempotency_key;
        """
        params = {'$idempotency_key': idempotency_key}
        
        try:
            result = await self.execute(query, params)
            if result:
                key = IdempotencyKey.from_db_row(result[0])
                if key.expires_at and key.expires_at < datetime.utcnow():
                    await self.delete(idempotency_key)
                    return None
                return key
            return None
        except Exception as e:
            logger.error(f"Error getting idempotency key: {e}")
            return None
    
    async def delete(self, idempotency_key: str) -> bool:
        query = f"""
        DECLARE $idempotency_key AS Utf8;
        DELETE FROM {self.table_name} WHERE idempotency_key = $idempotency_key;
        """
        params = {'$idempotency_key': idempotency_key}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error deleting idempotency key: {e}")
            return False

# ============================================
# ХЕЛПЕР ДЛЯ ОТВЕТОВ
# ============================================

class ResponseHelper:
    """Хелпер для HTTP ответов с поддержкой сжатия"""
    
    MIN_COMPRESS_SIZE = common_config.COMPRESS_RESPONSE_SIZE
    
    @staticmethod
    def _dumps(data: Any) -> str:
        if HAS_ORJSON:
            return orjson.dumps(data).decode()
        return json.dumps(data, ensure_ascii=False, default=str, separators=(',', ':'))
    
    @staticmethod
    def _default_headers() -> Dict:
        return {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Idempotency-Key, X-User-ID, X-User-Role',
            'Access-Control-Expose-Headers': 'X-Request-ID, X-Response-Time',
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY',
            'X-XSS-Protection': '1; mode=block'
        }
    
    async def success(self, data: Any, status_code: int = 200, 
                      event: Dict = None, compress: bool = True) -> Dict:
        body = self._dumps({'success': True, 'data': data})
        body_bytes = body.encode('utf-8')
        
        headers = self._default_headers()
        headers['Content-Type'] = 'application/json'
        headers['X-Response-Time'] = str(int(time.time() * 1000))
        headers['Vary'] = 'Accept-Encoding'
        
        if compress and len(body_bytes) >= self.MIN_COMPRESS_SIZE and event:
            accept_encoding = event.get('headers', {}).get('accept-encoding', '')
            
            if 'gzip' in accept_encoding:
                buf = io.BytesIO()
                with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as f:
                    f.write(body_bytes)
                compressed = buf.getvalue()
                
                await Metrics.inc_counter('compressed_responses')
                
                headers['Content-Encoding'] = 'gzip'
                headers['Content-Length'] = str(len(compressed))
                
                logger.debug(f"📦 Response compressed: {len(body_bytes)} -> {len(compressed)} bytes")
                
                return {
                    'statusCode': status_code,
                    'headers': headers,
                    'body': compressed,
                    'isBase64Encoded': False
                }
        
        headers['Content-Length'] = str(len(body_bytes))
        
        return {
            'statusCode': status_code,
            'headers': headers,
            'body': body
        }
    
    async def error(self, message: str, status_code: int = 400, 
                    code: str = None, event: Dict = None) -> Dict:
        error_body = {
            'success': False,
            'error': message
        }
        if code:
            error_body['code'] = code
        
        return await self.success(error_body, status_code, event, compress=True)
    
    @staticmethod
    def options() -> Dict:
        return {
            'statusCode': 200,
            'headers': ResponseHelper._default_headers(),
            'body': ''
        }

# ============================================
# КАСТОМНЫЕ ИСКЛЮЧЕНИЯ
# ============================================

class AppError(Exception):
    def __init__(self, message: str, code: str = None, status_code: int = 400):
        self.message = message
        self.code = code
        self.status_code = status_code
        super().__init__(message)

class ValidationError(AppError):
    def __init__(self, message: str, code: str = "VALIDATION_ERROR"):
        super().__init__(message, code, 400)

class PermissionError(AppError):
    def __init__(self, message: str, code: str = "PERMISSION_DENIED"):
        super().__init__(message, code, 403)

class NotFoundError(AppError):
    def __init__(self, message: str, code: str = "NOT_FOUND"):
        super().__init__(message, code, 404)

class RateLimitError(AppError):
    def __init__(self, message: str = "Too many requests", code: str = "RATE_LIMIT"):
        super().__init__(message, code, 429)

class DatabaseError(AppError):
    def __init__(self, message: str, code: str = "DATABASE_ERROR"):
        super().__init__(message, code, 500)

# ============================================
# УТИЛИТЫ
# ============================================

UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[09a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)

def to_timestamp(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return int(dt.timestamp() * 1_000_000)

def from_timestamp(ts: Optional[int]) -> Optional[datetime]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts / 1_000_000)

def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None:
        return default
    if value == 'None' or value == '':
        return default
    try:
        result = int(value)
        if result < 0 or result > 2**64 - 1:
            logger.warning(f"Value {result} out of Uint64 range, clamping")
            result = max(0, min(result, 2**64 - 1))
        return result
    except (ValueError, TypeError):
        return default

def safe_str(value: Any, default: Optional[str] = None) -> Optional[str]:
    if value is None:
        return default
    return str(value)

def safe_b64decode(data: str) -> str:
    if not data:
        return ""
    try:
        padding = 4 - (len(data) % 4)
        if padding != 4:
            data += "=" * padding
        return base64.b64decode(data).decode('utf-8')
    except Exception as e:
        logger.warning(f"safe_b64decode failed: {e}")
        return data

def safe_b64encode(data: str) -> Optional[str]:
    if not data:
        return None
    try:
        return base64.b64encode(data.encode()).decode()
    except Exception as e:
        logger.warning(f"safe_b64encode failed: {e}")
        return data

def to_uint64(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    if value == 'None' or value == '':
        return None
    if isinstance(value, (int, float)):
        result = int(value)
        return max(0, min(result, 2**64 - 1))
    hash_input = f"uuid_{value}".encode()
    hash_result = int(hashlib.sha256(hash_input).hexdigest()[:16], 16)
    return hash_result

def validate_idempotency_key(key: Optional[str]):
    if key:
        if len(key) > 255:
            raise ValidationError("Idempotency key too long")
        if not re.match(r'^[A-Za-z0-9\-_]+$', key):
            raise ValidationError("Invalid idempotency key format")

def validate_uuid(uuid_str: str) -> bool:
    if not uuid_str:
        return False
    # Убираем дефисы для проверки
    clean = uuid_str.replace('-', '')
    if len(clean) != 32:
        return False
    try:
        int(clean, 16)
        return True
    except ValueError:
        return False

def parse_json_body(body: Any) -> Dict:
    if not body:
        return {}
    try:
        if isinstance(body, str):
            return json.loads(body)
        return body
    except json.JSONDecodeError:
        raise ValidationError("Invalid JSON body")

def time_ago(dt: datetime) -> str:
    now = datetime.utcnow()
    diff = now - dt
    seconds = diff.total_seconds()
    if seconds < 60:
        return "только что"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes} мин. назад"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours} ч. назад"
    elif seconds < 2592000:
        days = int(seconds / 86400)
        return f"{days} дн. назад"
    elif seconds < 31536000:
        months = int(seconds / 2592000)
        return f"{months} мес. назад"
    else:
        years = int(seconds / 31536000)
        return f"{years} г. назад"

def chunk_list(lst: List, chunk_size: int) -> List[List]:
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

# ============================================
# ВАЛИДАТОРЫ
# ============================================

class Validators:
    """Набор валидаторов для общих типов"""
    
    UUID_REGEX = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    
    @classmethod
    def validate_uuid(cls, value: Any, field: str = "ID") -> str:
        if not value:
            raise ValidationError(f"{field} is required")
        
        value = str(value)
        if not cls.UUID_REGEX.match(value.lower()):
            raise ValidationError(f"Invalid {field} format")
        
        return value
    
    @classmethod
    def validate_int(cls, value: Any, field: str = "ID", 
                     min_value: Optional[int] = None, 
                     max_value: Optional[int] = None) -> int:
        if value is None:
            raise ValidationError(f"{field} is required")
        
        try:
            int_value = int(value)
        except (ValueError, TypeError):
            raise ValidationError(f"{field} must be an integer")
        
        if min_value is not None and int_value < min_value:
            raise ValidationError(f"{field} must be >= {min_value}")
        
        if max_value is not None and int_value > max_value:
            raise ValidationError(f"{field} must be <= {max_value}")
        
        return int_value
    
    @classmethod
    def validate_str(cls, value: Any, field: str = "Value",
                     min_len: Optional[int] = None,
                     max_len: Optional[int] = None) -> str:
        if value is None:
            raise ValidationError(f"{field} is required")
        
        str_value = str(value).strip()
        
        if min_len is not None and len(str_value) < min_len:
            raise ValidationError(f"{field} must be at least {min_len} characters")
        
        if max_len is not None and len(str_value) > max_len:
            raise ValidationError(f"{field} must be at most {max_len} characters")
        
        return str_value
    
    @classmethod
    def validate_bool(cls, value: Any, field: str = "Value") -> bool:
        if value is None:
            raise ValidationError(f"{field} is required")
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ['true', '1', 'yes', 'on']
        
        return bool(value)
    
    @classmethod
    def validate_enum(cls, value: Any, enum_class: Enum, field: str = "Value") -> Enum:
        if value is None:
            raise ValidationError(f"{field} is required")
        
        try:
            if isinstance(value, enum_class):
                return value
            return enum_class(value)
        except (ValueError, KeyError):
            allowed = [e.value for e in enum_class]
            raise ValidationError(f"{field} must be one of: {', '.join(allowed)}")

# ============================================
# ГЛОБАЛЬНЫЕ ЭКЗЕМПЛЯРЫ
# ============================================

user_rate_limiter = None
ip_rate_limiter = None

# ============================================
# ФУНКЦИИ ДЛЯ ЗАПУСКА
# ============================================

async def initialize_common():
    """Инициализация всех компонентов"""
    logger.info("🚀 Initializing common components...")
    
    # Инициализация пула БД (синхронно)
    init_db_pool()
    logger.info("✅ Database pool initialized")
    
    # Запуск фонового воркера
    logger.info("🚀 Starting background worker...")
    try:
        if not background_worker.running:
            await background_worker.start()
            logger.info(f"✅ Background worker started successfully")
            logger.info(f"   - Workers: {background_worker.num_workers}")
            logger.info(f"   - High queue size: {common_config.WORKER_HIGH_QUEUE_SIZE}")
            logger.info(f"   - Normal queue size: {common_config.WORKER_NORMAL_QUEUE_SIZE}")
            logger.info(f"   - Low queue size: {common_config.WORKER_LOW_QUEUE_SIZE}")
            logger.info(f"   - Max concurrent: {common_config.WORKER_MAX_CONCURRENT}")
        else:
            logger.info("⏭️ Background worker already running")
    except Exception as e:
        logger.error(f"❌ Failed to start background worker: {e}", exc_info=True)
    
    # Запуск WebSocket heartbeat
    logger.info("💓 Starting WebSocket heartbeat...")
    try:
        await WebSocketManager.start_heartbeat()
        logger.info("✅ WebSocket heartbeat started")
    except Exception as e:
        logger.error(f"❌ Failed to start WebSocket heartbeat: {e}", exc_info=True)
    
    logger.info("✅ Common components initialized")
    logger.info(f"📊 Config: {common_config}")

async def shutdown_common():
    """Остановка всех компонентов"""
    logger.info("🛑 Shutting down common components...")
    
    # Остановка WebSocket heartbeat
    logger.info("💔 Stopping WebSocket heartbeat...")
    try:
        await WebSocketManager.stop_heartbeat()
        logger.info("✅ WebSocket heartbeat stopped")
    except Exception as e:
        logger.error(f"❌ Error stopping WebSocket heartbeat: {e}")
    
    # Остановка фонового воркера
    await background_worker.stop()
    
    logger.info("✅ Common components stopped")

# ============================================
# АВТОМАТИЧЕСКАЯ ИНИЦИАЛИЗАЦИЯ ДЛЯ AWS LAMBDA
# ============================================

# Флаг для однократной инициализации
_initialized = False
_init_lock = threading.Lock()

def init_sync():
    """Синхронная инициализация для Lambda (вызывается при импорте)"""
    global _initialized
    
    with _init_lock:
        if _initialized:
            return
        
        logger.info("🚀 Холодный старт - синхронная инициализация common...")
        
        # Создаем или получаем цикл событий
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Инициализируем все компоненты
        try:
            # Запускаем инициализацию (синхронно)
            init_db_pool()
            logger.info("✅ Database pool initialized (sync)")
            
            # Запускаем воркер в фоне
            async def _start_worker():
                if not background_worker.running:
                    await background_worker.start()
                    logger.info("✅ Background worker started")
            
            loop.create_task(_start_worker())
            
            # Запускаем WebSocket heartbeat в фоне
            async def _start_heartbeat():
                await WebSocketManager.start_heartbeat()
                logger.info("✅ WebSocket heartbeat started")
            
            loop.create_task(_start_heartbeat())
            
            _initialized = True
            logger.info("✅ Все компоненты common успешно инициализированы")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации common: {e}", exc_info=True)

# Вызываем инициализацию при импорте
init_sync()

# ============================================
# БАЗОВЫЙ ХЕНДЛЕР
# ============================================

class BaseHandler:
    """Базовый класс для всех хендлеров"""
    
    def __init__(self):
        self.response = ResponseHelper()
        self.validators = Validators()
        
        self._rate_limiters = {}
        self._active_requests = 0
        self._active_lock = asyncio.Lock()
    
    async def __aenter__(self):
        async with self._active_lock:
            self._active_requests += 1
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self._active_lock:
            self._active_requests -= 1
    
    def _parse_body(self, event: Dict) -> Dict:
        try:
            return parse_json_body(event.get('body'))
        except ValueError as e:
            raise ValidationError(str(e))
    
    def _get_user_id(self, event: Dict) -> Optional[str]:
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        user_id = headers.get('x-user-id')
        if user_id:
            return user_id
        
        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        claims = authorizer.get('claims', {})
        return claims.get('sub') or claims.get('username')
    
    def _get_user_role(self, event: Dict) -> str:
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        return headers.get('x-user-role', 'user')
    
    def _get_client_ip(self, event: Dict) -> str:
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        return headers.get('x-real-ip', headers.get('x-forwarded-for', 'unknown')).split(',')[0].strip()
    
    def _get_idempotency_key(self, event: Dict) -> Optional[str]:
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        key = headers.get('x-idempotency-key')
        if key:
            validate_idempotency_key(key)
        return key
    
    def _get_request_id(self, event: Dict) -> str:
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        request_id = headers.get('x-request-id')
        if not request_id:
            request_id = str(uuid.uuid4())
        return request_id
    
    def _get_query_param(self, event: Dict, param: str, default: Any = None) -> Any:
        query = event.get('queryStringParameters') or {}
        return query.get(param, default)
    
    def _get_int_query_param(self, event: Dict, param: str, default: int) -> int:
        value = self._get_query_param(event, param)
        if value is None:
            return default
        return safe_int(value) or default
    
    def _get_bool_query_param(self, event: Dict, param: str, default: bool) -> bool:
        value = self._get_query_param(event, param)
        if value is None:
            return default
        return str(value).lower() in ['true', '1', 'yes', 'on']
    
    def _get_path_param(self, event: Dict, param: str) -> Optional[str]:
        path_params = event.get('pathParameters') or {}
        return path_params.get(param)
    
    def _get_cursor(self, event: Dict) -> Optional[str]:
        cursor = self._get_query_param(event, 'cursor')
        if cursor and len(cursor) > 500:
            logger.warning(f"Cursor too long: {len(cursor)} chars, ignoring")
            return None
        return cursor
    
    async def handle_error(self, e: Exception, event: Dict) -> Dict:
        request_id = self._get_request_id(event)
        
        if isinstance(e, ValidationError):
            logger.warning(f"Validation error [{request_id}]: {e.message}")
            return await self.response.error(e.message, e.status_code, e.code, event)
        
        if isinstance(e, PermissionError):
            logger.warning(f"Permission error [{request_id}]: {e.message}")
            return await self.response.error(e.message, e.status_code, e.code, event)
        
        if isinstance(e, NotFoundError):
            logger.warning(f"Not found [{request_id}]: {e.message}")
            return await self.response.error(e.message, e.status_code, e.code, event)
        
        if isinstance(e, RateLimitError):
            logger.warning(f"Rate limit [{request_id}]: {e.message}")
            return await self.response.error(e.message, e.status_code, e.code, event)
        
        if isinstance(e, DatabaseError):
            logger.error(f"Database error [{request_id}]: {e.message}")
            return await self.response.error("Internal server error", 500, "DATABASE_ERROR", event)
        
        logger.error(f"Unexpected error [{request_id}]: {e}", exc_info=True)
        return await self.response.error("Internal server error", 500, "INTERNAL_ERROR", event)

# ============================================
# ЭКСПОРТ
# ============================================

__all__ = [
    # Конфиг
    'common_config',
    
    # Кэш
    'cache',
    'UserCache',
    'PostCache',
    'FeedCache',
    'ReactionCache',
    'RepostCache',
    'CommentCache',
    'LRUCache',
    
    # WebSocket
    'WebSocketManager',
    'send_ws',
    
    # Метрики
    'SessionMetrics',
    'Metrics',
    
    # Воркер
    'background_worker',
    'BackgroundTaskQueue',
    'background_task',
    
    # Декораторы
    'retry',
    'rate_limit',
    'measure_time',
    'with_request_context',
    'background_task',
    
    # Контекст
    'RequestContext',
    
    # Репозитории
    'BaseRepository',
    'TransactionAwareRepository',
    'IdempotencyKey',
    'IdempotencyRepository',
    
    # Unit of Work
    'UnitOfWork',
    
    # Хелперы
    'ResponseHelper',
    'BaseHandler',
    'Validators',
    
    # Утилиты
    'to_timestamp',
    'from_timestamp',
    'to_uint64',
    'safe_int',
    'safe_str',
    'safe_b64decode',
    'safe_b64encode',
    'validate_idempotency_key',
    'validate_uuid',
    'parse_json_body',
    'time_ago',
    'chunk_list',
    
    # Исключения
    'AppError',
    'ValidationError',
    'PermissionError',
    'NotFoundError',
    'RateLimitError',
    'DatabaseError',
    
    # Логгер
    'logger',
    
    # Инициализация
    'initialize_common',
    'shutdown_common'
]

