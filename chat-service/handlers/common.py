"""
COMMON v2.4 - ОПТИМИЗИРОВАНО для 5000+ пользователей + WEBSOCKET
- 1 запрос = 1 сессия
- Мониторинг сессий
- Фоновый воркер для уведомлений
- Batch обработка
- Кэширование
- WebSocket менеджер для real-time уведомлений
"""
import json
import hashlib
import re
from collections import deque
import asyncio
import time
import traceback
import inspect
import uuid
from typing import Dict, Any, Optional, List, Union, Tuple, Set, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps

# ============================================
# ИМПОРТЫ ИНФРАСТРУКТУРЫ
# ============================================

from middleware.logging import logger
from db.pool import db_pool, ydb_pool, get_db_pool 
from config import config

# Кэш
try:
    from cache.manager import cache
except ImportError:
    class SimpleCache:
        _local_cache = {}
        
        async def get(self, key):
            return self._local_cache.get(key)
        
        async def set(self, key, value, ttl=60):
            self._local_cache[key] = value
        
        async def delete(self, key):
            self._local_cache.pop(key, None)
        
        async def mget(self, keys):
            return [self._local_cache.get(k) for k in keys]
    
    cache = SimpleCache()


# ============================================
# WEBSOCKET МЕНЕДЖЕР (НОВЫЙ!)
# ============================================

import asyncio
import json
import os
from typing import Dict, List, Optional, Set, Tuple, Any
from middleware.logging import logger

class WebSocketManager:
    """Менеджер WebSocket соединений с поддержкой подписок на чаты"""
    
    _connections: Dict[str, Tuple[str, Any]] = {}      # connection_id -> (user_id, websocket)
    _user_connections: Dict[str, Set[str]] = {}        # user_id -> set(connection_ids)
    _user_subscriptions: Dict[str, Set[int]] = {}      # user_id -> set(chat_ids)
    _subscription_to_users: Dict[int, Set[str]] = {}   # chat_id -> set(user_ids)
    _lock = asyncio.Lock()
    _heartbeat_task: Optional[asyncio.Task] = None
    _running = False

    WS_HEARTBEAT_INTERVAL: int = 30
    WS_CONNECTION_TIMEOUT: int = 300
    WS_MAX_CONNECTIONS_PER_USER: int = 5

    @classmethod
    async def register(cls, connection_id: str, user_id: str, websocket) -> bool:
        """Зарегистрировать WebSocket соединение"""
        pid = os.getpid()
        logger.info(f"🔌 [WS Manager] Register in PID {pid}")
        async with cls._lock:
            user_conns = cls._user_connections.get(user_id, set())
            if len(user_conns) >= cls.WS_MAX_CONNECTIONS_PER_USER:
                logger.warning(f"⚠️ User {user_id} exceeded max connections ({cls.WS_MAX_CONNECTIONS_PER_USER})")
                return False

            cls._connections[connection_id] = (user_id, websocket)
            if user_id not in cls._user_connections:
                cls._user_connections[user_id] = set()
            cls._user_connections[user_id].add(connection_id)

            logger.info(f"✅ [WS Manager] Registered connection {connection_id} for user {user_id}")
            logger.info(f"📊 [WS Manager] _user_connections keys: {list(cls._user_connections.keys())}")
            logger.info(f"📊 [WS Manager] _connections keys: {list(cls._connections.keys())}")
            return True

    @classmethod
    async def unregister(cls, connection_id: str):
        """Удалить соединение"""
        async with cls._lock:
            if connection_id in cls._connections:
                user_id, _ = cls._connections.pop(connection_id)
                if user_id in cls._user_connections:
                    cls._user_connections[user_id].discard(connection_id)
                    if not cls._user_connections[user_id]:
                        del cls._user_connections[user_id]
                logger.info(f"✅ [WS Manager] Unregistered connection {connection_id} for user {user_id}")

    @classmethod
    async def subscribe_to_chat(cls, connection_id: str, user_id: str, chat_id: int):
        """Подписать пользователя на уведомления чата"""
        async with cls._lock:
            # Добавляем в подписки пользователя
            if user_id not in cls._user_subscriptions:
                cls._user_subscriptions[user_id] = set()
            cls._user_subscriptions[user_id].add(chat_id)
            
            # Добавляем в обратный индекс
            if chat_id not in cls._subscription_to_users:
                cls._subscription_to_users[chat_id] = set()
            cls._subscription_to_users[chat_id].add(user_id)
            
            logger.info(f"✅ User {user_id[:8]} subscribed to chat {chat_id}")

    @classmethod
    async def unsubscribe_from_chat(cls, connection_id: str, user_id: str, chat_id: int):
        """Отписать пользователя от уведомлений чата"""
        async with cls._lock:
            if user_id in cls._user_subscriptions:
                cls._user_subscriptions[user_id].discard(chat_id)
                if not cls._user_subscriptions[user_id]:
                    del cls._user_subscriptions[user_id]
            
            if chat_id in cls._subscription_to_users:
                cls._subscription_to_users[chat_id].discard(user_id)
                if not cls._subscription_to_users[chat_id]:
                    del cls._subscription_to_users[chat_id]
            
            logger.info(f"✅ User {user_id[:8]} unsubscribed from chat {chat_id}")

    @classmethod
    async def clear_user_subscriptions(cls, user_id: str):
        """Очистить все подписки пользователя"""
        async with cls._lock:
            if user_id in cls._user_subscriptions:
                chat_ids = list(cls._user_subscriptions[user_id])
                for chat_id in chat_ids:
                    if chat_id in cls._subscription_to_users:
                        cls._subscription_to_users[chat_id].discard(user_id)
                        if not cls._subscription_to_users[chat_id]:
                            del cls._subscription_to_users[chat_id]
                del cls._user_subscriptions[user_id]
                logger.info(f"✅ Cleared subscriptions for user {user_id[:8]}")

    @classmethod
    async def get_user_connections(cls, user_id: str) -> List[Any]:
        """Получить все WebSocket соединения пользователя"""
        async with cls._lock:
            connection_ids = cls._user_connections.get(user_id, set())
            return [cls._connections[cid][1] for cid in connection_ids if cid in cls._connections]

    @classmethod
    async def get_user_subscriptions(cls, user_id: str) -> Set[int]:
        """Получить подписки пользователя"""
        async with cls._lock:
            return cls._user_subscriptions.get(user_id, set())

    @classmethod
    async def get_chat_subscribers(cls, chat_id: int) -> Set[str]:
        """Получить подписчиков чата"""
        async with cls._lock:
            return cls._subscription_to_users.get(chat_id, set())

    @classmethod
    async def send_to_user(cls, user_id: str, message: Dict) -> int:
        """Отправить сообщение всем соединениям пользователя"""
        pid = os.getpid()
        logger.info(f"🔔 [WS Manager] send_to_user called for user {user_id} in PID {pid}")
        
        async with cls._lock:
            if user_id not in cls._user_connections:
                logger.info(f"⚠️ [WS Manager] No connections found for user {user_id}")
                return 0
            connection_ids = list(cls._user_connections[user_id])
            connections = [(cid, cls._connections[cid][1]) for cid in connection_ids if cid in cls._connections]

        sent_count = 0
        message_str = json.dumps(message, default=str, ensure_ascii=False)
        for cid, ws in connections:
            try:
                await ws.send_text(message_str)
                sent_count += 1
            except Exception as e:
                logger.error(f"❌ [WS Manager] Failed to send to connection {cid}: {e}")

        logger.info(f"✅ [WS Manager] Sent to {sent_count} connections for user {user_id}")
        return sent_count

    @classmethod
    async def send_to_many(cls, user_ids: List[str], message: Dict, max_concurrent: int = 50) -> int:
        """Отправить сообщение нескольким пользователям с ограничением на одновременные отправки"""
        sent_count = 0
        semaphore = asyncio.Semaphore(max_concurrent)
        message_str = json.dumps(message, default=str, ensure_ascii=False)

        async def send_one(user_id: str):
            nonlocal sent_count
            async with semaphore:
                try:
                    connections = await cls.get_user_connections(user_id)
                    for ws in connections:
                        await ws.send_text(message_str)
                        sent_count += 1
                except Exception as e:
                    logger.error(f"Failed to send to user {user_id}: {e}")

        tasks = [send_one(uid) for uid in user_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        return sent_count

    @classmethod
    async def send_to_chat(cls, chat_id: int, message: Dict, exclude_user_id: Optional[str] = None) -> int:
        """Отправить сообщение всем подписанным на чат пользователям"""
        async with cls._lock:
            user_ids = cls._subscription_to_users.get(chat_id, set())
            if exclude_user_id:
                user_ids = user_ids - {exclude_user_id}
            recipients = list(user_ids)
        
        if not recipients:
            logger.debug(f"📭 No subscribers for chat {chat_id}")
            return 0
        
        logger.info(f"📡 Sending to {len(recipients)} subscribers of chat {chat_id}")
        return await cls.send_to_many(recipients, message)

    @classmethod
    async def broadcast(cls, message: Dict, exclude_user_id: Optional[str] = None) -> int:
        """Отправить сообщение всем подключенным пользователям"""
        async with cls._lock:
            connections_snapshot = list(cls._connections.items())
        sent_count = 0
        message_str = json.dumps(message, default=str, ensure_ascii=False)
        for conn_id, (user_id, ws) in connections_snapshot:
            if exclude_user_id and user_id == exclude_user_id:
                continue
            try:
                await ws.send_text(message_str)
                sent_count += 1
            except Exception as e:
                logger.error(f"❌ [WS Manager] Failed to broadcast to {conn_id}: {e}")
        logger.info(f"📢 [WS Manager] Broadcast sent to {sent_count} connections")
        return sent_count

    @classmethod
    async def get_stats(cls) -> Dict:
        """Получить статистику соединений и подписок"""
        async with cls._lock:
            return {
                'total_connections': len(cls._connections),
                'total_users': len(cls._user_connections),
                'total_subscriptions': len(cls._user_subscriptions),
                'subscribed_chats': len(cls._subscription_to_users),
                'connections_per_user': {
                    user_id: len(conns) for user_id, conns in cls._user_connections.items()
                },
                'subscriptions_per_user': {
                    user_id: list(chats) for user_id, chats in cls._user_subscriptions.items()
                },
                'subscribers_per_chat': {
                    chat_id: len(users) for chat_id, users in cls._subscription_to_users.items()
                }
            }

    @classmethod
    async def start_heartbeat(cls):
        """Запустить проверку соединений (ping)"""
        if cls._running:
            return
        cls._running = True
        cls._heartbeat_task = asyncio.create_task(cls._heartbeat_loop())
        logger.info("💓 WebSocket heartbeat started")

    @classmethod
    async def stop_heartbeat(cls):
        """Остановить проверку соединений"""
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
        """Фоновый цикл проверки соединений"""
        while cls._running:
            try:
                await asyncio.sleep(cls.WS_HEARTBEAT_INTERVAL)
                async with cls._lock:
                    connections_snapshot = list(cls._connections.items())
                dead_connections = []
                ping_message = json.dumps({"type": "ping"})
                for conn_id, (user_id, ws) in connections_snapshot:
                    try:
                        await ws.send_text(ping_message)
                    except Exception:
                        dead_connections.append(conn_id)
                for conn_id in dead_connections:
                    await cls.unregister(conn_id)
                if dead_connections:
                    logger.info(f"💔 Removed {len(dead_connections)} dead connections")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Heartbeat error: {e}")
# ============================================
# МОНИТОРИНГ СЕССИЙ
# ============================================

class SessionMetrics:
    """Метрики для отслеживания использования сессий и транзакций"""
    _active_sessions = 0
    _total_sessions_created = 0
    _active_transactions = {}  # session_id -> count
    _transaction_history = deque(maxlen=100)  # последние 100 транзакций
    _session_stack_traces = {}  # session_id -> stack trace создания
    
    @classmethod
    def session_created(cls, repo_name: str, stack_info: str = ""):
        """Сессия создана"""
        cls._total_sessions_created += 1
        cls._active_sessions += 1
        session_id = id(session) if 'session' in locals() else None
        if session_id:
            cls._session_stack_traces[session_id] = stack_info
        
        current_time = time.time()
        if current_time - cls._last_warning_time > cls._warning_cooldown:
            logger.warning(
                f"⚠️ Создание новой сессии в {repo_name}\n"
                f"Активных сессий: {cls._active_sessions}\n"
                f"Всего создано: {cls._total_sessions_created}\n"
                f"Активных транзакций: {sum(cls._active_transactions.values())}\n"
                f"Стек: {stack_info[:500]}"
            )
            cls._last_warning_time = current_time
    
    @classmethod
    def transaction_begin(cls, session_id: int):
        """Транзакция начата"""
        cls._active_transactions[session_id] = cls._active_transactions.get(session_id, 0) + 1
        cls._transaction_history.append({
            'time': datetime.utcnow().isoformat(),
            'session_id': session_id,
            'action': 'begin',
            'active_count': cls._active_transactions[session_id]
        })
        
        if cls._active_transactions[session_id] > 5:
            logger.warning(
                f"⚠️ Слишком много активных транзакций в сессии {session_id}: "
                f"{cls._active_transactions[session_id]}"
            )
    
    @classmethod
    def transaction_end(cls, session_id: int, committed: bool, uow_id: str = ""):
        """Транзакция завершена"""
        if session_id in cls._active_transactions:
            cls._active_transactions[session_id] -= 1
            if cls._active_transactions[session_id] <= 0:
                del cls._active_transactions[session_id]
        
        cls._transaction_history.append({
            'time': datetime.utcnow().isoformat(),
            'session_id': session_id,
            'action': 'commit' if committed else 'rollback',
            'active_count': cls._active_transactions.get(session_id, 0)
        })
    
    @classmethod
    def get_stats(cls) -> Dict:
        """Получить статистику"""
        return {
            'active_sessions': cls._active_sessions,
            'total_sessions_created': cls._total_sessions_created,
            'active_transactions_by_session': dict(cls._active_transactions),
            'total_active_transactions': sum(cls._active_transactions.values()),
            'recent_transactions': list(cls._transaction_history)[-20:],
            'session_stack_traces': {k: v[:200] for k, v in cls._session_stack_traces.items()}
        }
    
    @classmethod
    def log_status(cls):
        """Вывести текущий статус"""
        stats = cls.get_stats()
        logger.info(f"📊 SESSION STATUS: {stats}")
        return stats


# ============================================
# КОНФИГУРАЦИЯ
# ============================================

@dataclass
class CommonConfig:
    """Конфигурация общих компонентов"""
    RETRY_ATTEMPTS: int = 3
    RETRY_DELAY: float = 0.1
    RETRY_MAX_DELAY: float = 2.0
    RETRY_BACKOFF: float = 2.0
    CACHE_DEFAULT_TTL: int = 300
    CACHE_PARTICIPANT_TTL: int = 600
    CACHE_CHAT_TTL: int = 300
    CACHE_CHAT_MEMBERS_TTL: int = 300
    QUERY_TIMEOUT: int = 5
    BATCH_SIZE: int = 100
    RATE_LIMIT_REQUESTS: int = 100
    RATE_LIMIT_BURST: int = 20
    COMPRESS_RESPONSE_SIZE: int = 1024
    SESSION_ACQUIRE_TIMEOUT: int = 5
    SESSION_WARNING_THRESHOLD: int = 3
    
    # Настройки для воркера уведомлений
    NOTIFICATION_WORKER_BATCH_SIZE: int = 100
    NOTIFICATION_WORKER_MAX_QUEUE: int = 10000
    NOTIFICATION_WORKER_INTERVAL: float = 0.1
    
    # WebSocket настройки
    WS_HEARTBEAT_INTERVAL: int = 30
    WS_CONNECTION_TIMEOUT: int = 300
    WS_MAX_CONNECTIONS_PER_USER: int = 5

common_config = CommonConfig()

# Применяем конфигурацию к WebSocket менеджеру
WebSocketManager.WS_HEARTBEAT_INTERVAL = common_config.WS_HEARTBEAT_INTERVAL
WebSocketManager.WS_CONNECTION_TIMEOUT = common_config.WS_CONNECTION_TIMEOUT
WebSocketManager.WS_MAX_CONNECTIONS_PER_USER = common_config.WS_MAX_CONNECTIONS_PER_USER


# ============================================
# ФОНОВЫЙ ВОРКЕР ДЛЯ УВЕДОМЛЕНИЙ
# ============================================

class NotificationWorker:
    """
    Фоновый воркер для обработки уведомлений пачками
    Запускается 1 раз при старте приложения
    
    Использование:
        await notification_worker.put({
            'type': 'new_message',
            'chat_id': 123,
            'message_id': 456,
            'sender_id': 'user123',
            'mentioned_users': ['user456'],
            'reply_to': 789
        })
    """
    
    def __init__(self, batch_size: int = None, max_queue: int = None):
        self.batch_size = batch_size or common_config.NOTIFICATION_WORKER_BATCH_SIZE
        self.max_queue = max_queue or common_config.NOTIFICATION_WORKER_MAX_QUEUE
        self.queue = asyncio.Queue(maxsize=self.max_queue)
        self.processing = False
        self._task = None
        self._stats = {
            'processed': 0,
            'errors': 0,
            'queue_size': 0,
            'batches': 0
        }
    
    async def start(self):
        """Запустить воркер"""
        if self.processing:
            return
        
        self.processing = True
        self._task = asyncio.create_task(self._process_queue())
        logger.info(f"✅ Notification worker started (batch_size={self.batch_size})")
    
    async def stop(self):
        """Остановить воркер"""
        self.processing = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Notification worker stopped")
    
    async def put(self, item: Dict):
        """Добавить задачу в очередь"""
        try:
            await self.queue.put(item)
            self._stats['queue_size'] = self.queue.qsize()
        except asyncio.QueueFull:
            logger.error(f"❌ Notification queue full! Item dropped: {item.get('type')}")
            self._stats['errors'] += 1
    
    async def _process_queue(self):
        """Основной цикл обработки очереди"""
        while self.processing:
            try:
                # Собираем пачку уведомлений
                batch = []
                
                # Берем первый элемент с ожиданием
                try:
                    first = await asyncio.wait_for(
                        self.queue.get(), 
                        timeout=1.0
                    )
                    batch.append(first)
                except asyncio.TimeoutError:
                    # Очередь пуста, продолжаем ждать
                    await asyncio.sleep(0.1)
                    continue
                
                # Пытаемся взять еще элементы без ожидания
                while len(batch) < self.batch_size:
                    try:
                        item = self.queue.get_nowait()
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break
                
                # Обрабатываем пачку
                if batch:
                    await self._process_batch(batch)
                    self._stats['processed'] += len(batch)
                    self._stats['batches'] += 1
                    self._stats['queue_size'] = self.queue.qsize()
                
                # Небольшая пауза для снижения нагрузки
                await asyncio.sleep(common_config.NOTIFICATION_WORKER_INTERVAL)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Notification worker error: {e}")
                self._stats['errors'] += 1
                await asyncio.sleep(1)
    
    async def _process_batch(self, batch: List[Dict]):
        """Обработать пачку уведомлений"""
        if not batch:
            return
        
        logger.debug(f"📦 Processing batch of {len(batch)} notifications")
        
        # Группируем по типу уведомлений
        by_type = {}
        for item in batch:
            notif_type = item.get('type', 'unknown')
            if notif_type not in by_type:
                by_type[notif_type] = []
            by_type[notif_type].append(item)
        
        # Обрабатываем каждый тип
        for notif_type, items in by_type.items():
            try:
                if notif_type == 'new_message':
                    await self._process_new_messages(items)
                elif notif_type == 'reply':
                    await self._process_replies(items)
                elif notif_type == 'mention':
                    await self._process_mentions(items)
                elif notif_type == 'join_request':
                    await self._process_join_requests(items)
                else:
                    # По одному для неизвестных типов
                    for item in items:
                        await self._process_single(item)
            except Exception as e:
                logger.error(f"❌ Error processing {notif_type} batch: {e}")
    
    async def _process_new_messages(self, items: List[Dict]):
        """Обработать пачку новых сообщений"""
        # Группируем по chat_id
        by_chat = {}
        for item in items:
            chat_id = item['chat_id']
            if chat_id not in by_chat:
                by_chat[chat_id] = []
            by_chat[chat_id].append(item)
        
        # Для каждого чата - 1 запрос к БД
        for chat_id, chat_items in by_chat.items():
            try:
                async with UnitOfWork() as uow:
                    # Получаем всех участников чата (1 запрос)
                    members = await uow.participants.list_by_chat(
                        chat_id, limit=10000, active_only=True
                    )
                    member_ids = {m.user_id for m in members}
                    
                    # Получаем настройки уведомлений для всех участников (batch)
                    settings_repo = NotificationSettingsRepository(uow._session)
                    user_ids = list(member_ids)
                    
                    # Разбиваем на подгруппы для batch запроса
                    all_settings = {}
                    for chunk in chunk_list(user_ids, 100):
                        chunk_settings = await settings_repo.get_batch(chunk)
                        all_settings.update(chunk_settings)
                    
                    # Создаем уведомления для всех сообщений пачки
                    for item in chat_items:
                        sender_id = item['sender_id']
                        mentioned_users = set(item.get('mentioned_users', []))
                        
                        for member_id in member_ids:
                            if member_id == sender_id:
                                continue
                            
                            # Проверяем настройки
                            settings = all_settings.get(member_id)
                            if not settings:
                                settings = NotificationSettings.get_default(member_id)
                            
                            # Проверяем DND и другие настройки
                            if not self._should_notify(settings, item, member_id in mentioned_users):
                                continue
                            
                            # Создаем уведомление
                            await self._create_notification(
                                uow,
                                user_id=member_id,
                                type='mention' if member_id in mentioned_users else 'new_message',
                                chat_id=chat_id,
                                sender_id=sender_id,
                                message_id=item['message_id'],
                                message_preview=item.get('preview')
                            )
                            
            except Exception as e:
                logger.error(f"❌ Error processing chat {chat_id} notifications: {e}")
    
    async def _process_replies(self, items: List[Dict]):
        """Обработать пачку ответов"""
        for item in items:
            try:
                async with UnitOfWork() as uow:
                    parent_message = await uow.messages.get(
                        item['chat_id'], 
                        item['reply_to']
                    )
                    
                    if parent_message and parent_message.sender_id != item['sender_id']:
                        await self._create_notification(
                            uow,
                            user_id=parent_message.sender_id,
                            type='reply',
                            chat_id=item['chat_id'],
                            sender_id=item['sender_id'],
                            message_id=item['message_id'],
                            message_preview=item.get('preview'),
                            data={'parent_message_id': item['reply_to']}
                        )
            except Exception as e:
                logger.error(f"❌ Error processing reply: {e}")
    
    async def _process_mentions(self, items: List[Dict]):
        """Обработать пачку упоминаний"""
        # Аналогично new_messages, но только для упомянутых
        pass
    
    async def _process_join_requests(self, items: List[Dict]):
        """Обработать пачку запросов на вступление"""
        pass
    
    async def _process_single(self, item: Dict):
        """Обработать одно уведомление (запасной вариант)"""
        try:
            async with UnitOfWork() as uow:
                await self._create_notification(uow, **item)
        except Exception as e:
            logger.error(f"❌ Error processing single notification: {e}")
    
    async def _create_notification(self, uow, **kwargs):
        """Создать уведомление в БД"""
        from handlers.message_handler import Notification, NotificationRepository
        
        notification = Notification(
            user_id=kwargs['user_id'],
            type=kwargs['type'],
            chat_id=kwargs.get('chat_id'),
            sender_id=kwargs.get('sender_id'),
            message_id=kwargs.get('message_id'),
            message_preview=kwargs.get('message_preview'),
            data=kwargs.get('data', {}),
            created_at=datetime.utcnow()
        )
        
        repo = NotificationRepository(uow._session)
        await repo.create(notification)
    
    def _should_notify(self, settings: 'NotificationSettings', item: Dict, is_mention: bool) -> bool:
        """Проверить, нужно ли отправлять уведомление"""
        # Проверка DND
        if settings.do_not_disturb.get('enabled'):
            now = datetime.utcnow().time()
            dnd_from = datetime.strptime(settings.do_not_disturb['from'], '%H:%M').time()
            dnd_to = datetime.strptime(settings.do_not_disturb['to'], '%H:%M').time()
            
            if dnd_from <= now <= dnd_to or (dnd_from > dnd_to and (now >= dnd_from or now <= dnd_to)):
                return False
        
        # Проверка по типу
        chat_type = item.get('chat_type', 'group')
        
        if chat_type == 'private':
            return settings.private_chats.get('messages') != 'none'
        elif chat_type == 'group':
            if is_mention:
                return True
            return settings.groups.get('messages') == 'all'
        elif chat_type == 'channel':
            return settings.channels.get('messages') != 'none'
        
        return True
    
    def get_stats(self) -> Dict:
        """Получить статистику воркера"""
        return {
            **self._stats,
            'queue_size': self.queue.qsize(),
            'max_queue': self.max_queue,
            'batch_size': self.batch_size,
            'is_running': self.processing
        }


# Глобальный экземпляр воркера
notification_worker = NotificationWorker()


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
    """Декоратор для rate limiting"""
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
            if duration > 1.0:
                logger.warning(f"Slow query {func.__name__}: {duration:.2f}s")
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
            return await func(self, event, user, *args, **kwargs)
    return wrapper


# ============================================
# КАСТОМНЫЕ ИСКЛЮЧЕНИЯ
# ============================================

class AppError(Exception):
    """Базовое исключение приложения"""
    def __init__(self, message: str, code: str = None, status_code: int = 400):
        self.message = message
        self.code = code
        self.status_code = status_code
        super().__init__(message)

class ValidationError(AppError):
    """Ошибка валидации"""
    def __init__(self, message: str, code: str = "VALIDATION_ERROR"):
        super().__init__(message, code, 400)

class PermissionError(AppError):
    """Ошибка доступа"""
    def __init__(self, message: str, code: str = "PERMISSION_DENIED"):
        super().__init__(message, code, 403)

class NotFoundError(AppError):
    """Ресурс не найден"""
    def __init__(self, message: str, code: str = "NOT_FOUND"):
        super().__init__(message, code, 404)

class RateLimitError(AppError):
    """Превышен лимит запросов"""
    def __init__(self, message: str = "Too many requests", code: str = "RATE_LIMIT"):
        super().__init__(message, code, 429)

class DatabaseError(AppError):
    """Ошибка базы данных"""
    def __init__(self, message: str, code: str = "DATABASE_ERROR"):
        super().__init__(message, code, 500)


# ============================================
# ВАЛИДАТОРЫ
# ============================================

class Validators:
    """Набор валидаторов для общих типов"""
    
    UUID_REGEX = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    USERNAME_REGEX = re.compile(r'^[a-zA-Z0-9_]{5,32}$')
    PHONE_REGEX = re.compile(r'^\+?[0-9]{10,15}$')
    EMAIL_REGEX = re.compile(r'^[^@]+@[^@]+\.[^@]+$')
    
    @classmethod
    def validate_uuid(cls, value: Any, field: str = "ID") -> str:
        """Проверяет, что строка является валидным UUID"""
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
        """Проверяет, что значение является целым числом в диапазоне"""
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
        """Проверяет строку на длину"""
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
        """Проверяет булево значение"""
        if value is None:
            raise ValidationError(f"{field} is required")
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ['true', '1', 'yes', 'on']
        
        return bool(value)
    
    @classmethod
    def validate_enum(cls, value: Any, enum_class: Enum, field: str = "Value") -> Enum:
        """Проверяет, что значение есть в перечислении"""
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
# МОДЕЛЬ КЛЮЧА ИДЕМПОТЕНТНОСТИ
# ============================================

@dataclass
class IdempotencyKey:
    """Модель ключа идемпотентности"""
    idempotency_key: str
    entity_type: str
    entity_id: int
    chat_id: Optional[int]
    user_id: str
    created_at: datetime
    expires_at: Optional[datetime]
    result_data: Optional[Dict] = None  # 👈 ДОБАВИТЬ

    def to_db_row(self) -> Dict[str, Any]:
        """Конвертация в строку для БД"""
        return {
            'idempotency_key': self.idempotency_key,
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'chat_id': self.chat_id,
            'user_id': str(self.user_id),
            'created_at': to_timestamp(self.created_at),
            'expires_at': to_timestamp(self.expires_at) if self.expires_at else None,
            'result_data': json.dumps(self.result_data) if self.result_data else None  # 👈 ДОБАВИТЬ
        }

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'IdempotencyKey':
        """Создание модели из строки БД"""
        return cls(
            idempotency_key=row.get('idempotency_key'),
            entity_type=row.get('entity_type'),
            entity_id=row.get('entity_id'),
            chat_id=row.get('chat_id'),
            user_id=str(row.get('user_id')),
            created_at=from_timestamp(row.get('created_at')),
            expires_at=from_timestamp(row.get('expires_at')),
            result_data=json.loads(row.get('result_data')) if row.get('result_data') else None  # 👈 ДОБАВИТЬ
        )
# ============================================
# КОНТЕКСТ ЗАПРОСА
# ============================================

class RequestContext:
    """
    Контекст HTTP запроса - создает одну сессию на весь запрос
    Использование:
        async with RequestContext() as ctx:
            repo = MessageRepository(ctx.session)
            result = await repo.get(...)
    """
    
    def __init__(self):
        self.session = None
        self._owns_session = False
        self._request_id = None
        self._created_at = None
        self._session_id = None
    
    async def __aenter__(self):
        """Вход в контекст - получаем сессию из пула с таймаутом"""
        from db.pool import get_db_pool
        pool = get_db_pool()
        
        self._created_at = time.time()
        self._request_id = str(uuid.uuid4())[:8]
        
        try:
            self.session = await asyncio.wait_for(
                pool.get_session(),
                timeout=common_config.SESSION_ACQUIRE_TIMEOUT
            )
            self._owns_session = True
            self._session_id = id(self.session)
            
            # Логируем создание сессии
            stack = ''.join(traceback.format_stack()[:-1])
            TransactionMonitor.session_created(
                source='RequestContext',
                session_id=self._session_id,
                stack=stack
            )
            
            logger.info(f"🔌 [REQ {self._request_id}] Session acquired: {self._session_id}")
            
        except asyncio.TimeoutError:
            logger.error(f"❌ [REQ {self._request_id}] Timeout acquiring session")
            raise DatabaseError("Failed to acquire database session")
        except Exception as e:
            logger.error(f"❌ [REQ {self._request_id}] Error acquiring session: {e}")
            raise
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекста - возвращаем сессию в пул"""
        if self._owns_session and self.session:
            session_id = self._session_id
            
            # Проверяем и откатываем активные транзакции
            active_tx = TransactionMonitor._active_transactions.get(session_id, 0)
            if active_tx > 0:
                logger.error(
                    f"🔥 [REQ {self._request_id}] Session {session_id} has {active_tx} active transactions! "
                    f"Attempting to rollback..."
                )
                # Пытаемся откатить все активные транзакции
                for _ in range(active_tx):
                    try:
                        await self.session.transaction().rollback()
                        TransactionMonitor.transaction_end(session_id, committed=False, uow_id="auto_rollback")
                        logger.info(f"✅ Auto-rolled back transaction in session {session_id}")
                    except Exception as e:
                        logger.error(f"Failed to rollback transaction: {e}")
            
            try:
                from db.pool import get_db_pool
                pool = get_db_pool()
                
                if pool:
                    await pool.release_session(self.session)
                    TransactionMonitor.session_released(session_id)
                    
                    duration = time.time() - self._created_at
                    logger.info(
                        f"✅ [REQ {self._request_id}] Session released: {session_id} "
                        f"after {duration:.2f}s"
                    )
                else:
                    logger.error(f"❌ [REQ {self._request_id}] Pool is None")
                    
            except Exception as e:
                logger.error(f"❌ [REQ {self._request_id}] Error releasing session: {e}")
            finally:
                self.session = None
                self._owns_session = False
                self._session_id = None
        
        # Логируем финальный статус
        TransactionMonitor.log_status()
    
    def get_request_id(self) -> str:
        """Получить ID запроса"""
        return self._request_id

# ============================================
# БАЗОВЫЙ РЕПОЗИТОРИЙ
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
        
        from db.pool import get_db_pool
        pool = get_db_pool()
        
        stack_info = ''.join(traceback.format_stack()[:-1])
        SessionMetrics.session_created(self.__class__.__name__, stack_info)
        
        for attempt in range(common_config.RETRY_ATTEMPTS):
            try:
                logger.debug(f"🆕 Попытка {attempt + 1} создания новой сессии для {self.__class__.__name__}")
                
                session = await asyncio.wait_for(
                    pool.get_session(),
                    timeout=common_config.SESSION_ACQUIRE_TIMEOUT
                )
                
                if session:
                    self._session = session
                    self._owns_session = True
                    self._session_id = id(session)
                    logger.debug(f"✅ Создана новая сессия: {self._session_id}")
                    return session
                
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Таймаут получения сессии, попытка {attempt + 1}")
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
                from db.pool import get_db_pool
                pool = get_db_pool()
                
                session_id = self._session_id
                await pool.release_session(self._session)
                
                logger.debug(f"♻️ Освобождена сессия (принадлежала репозиторию): {session_id}")
                SessionMetrics.session_released()
                
            except Exception as e:
                logger.error(f"❌ Ошибка освобождения сессии: {e}")
                SessionMetrics.session_error()
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
                clean_params = {}
                for k, v in params.items():
                    if isinstance(v, str):
                        if v.startswith("'") and v.endswith("'"):
                            v = v[1:-1]
                    clean_params[k] = v
                
                logger.debug(f"Executing query with {len(params)} params using session: {self._session_id}")
                
                try:
                    prepared = await session.prepare(query)
                    
                    if self._transaction:
                        result = await self._transaction.execute(prepared, clean_params)
                    else:
                        result = await session.transaction().execute(prepared, clean_params, commit_tx=True)
                    
                    rows = result[0].rows if result and len(result) > 0 and hasattr(result[0], 'rows') else []
                    
                    duration = time.time() - start_time
                    if duration > 1.0:
                        logger.warning(f"🐢 Медленный запрос ({duration:.2f}s) в сессии {self._session_id}")
                    
                    return rows
                    
                except Exception as e:
                    error_str = str(e).lower()
                    if ("type mismatch" in error_str or 
                        "failed to convert type" in error_str or
                        "integral type implicit bitcast" in error_str):
                        
                        logger.warning(f"Type mismatch detected, clearing query cache and retrying: {e}")
                        self._prepared_queries = {}
                        
                        prepared = await session.prepare(query)
                        
                        if self._transaction:
                            result = await self._transaction.execute(prepared, clean_params)
                        else:
                            result = await session.transaction().execute(prepared, clean_params, commit_tx=True)
                        
                        rows = result[0].rows if result and len(result) > 0 and hasattr(result[0], 'rows') else []
                        return rows
                    else:
                        raise
            else:
                logger.debug(f"Executing query without params using session: {self._session_id}")
                
                if self._transaction:
                    result = await self._transaction.execute(query)
                else:
                    result = await session.transaction().execute(query, commit_tx=True)
                
                rows = result[0].rows if result and len(result) > 0 and hasattr(result[0], 'rows') else []
                
                duration = time.time() - start_time
                if duration > 1.0:
                    logger.warning(f"🐢 Медленный запрос ({duration:.2f}s) в сессии {self._session_id}")
                
                return rows
                
        except Exception as e:
            logger.error(f"Database error in session {self._session_id}: {e}")
            SessionMetrics.session_error()
            
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
            SessionMetrics.session_error()
            return False
        finally:
            if session_created_now:
                await self._release_session()
    
   
    def _generate_declare(self, params: Dict) -> str:
        """Генерация DECLARE с кэшированием и ПРАВИЛЬНЫМ определением типов."""
        cache_key = hashlib.md5(str(sorted(params.items())).encode()).hexdigest()

        if cache_key in self._prepared_queries:
            return self._prepared_queries[cache_key]

        declares = []
        table_name = getattr(self, 'table_name', '')

        # --- СПИСКИ ПОЛЕЙ ---
        bool_fields = [
            'is_public', 'join_moderation', 'is_active', 'is_archived', 'is_deleted',
            'can_join', 'requires_approval', 'is_permanent', 'is_blocked',
            'last_read_message_valid', 'message_exists', 'has_attachments', 'is_edited',
            'is_discussion', 'comments_enabled', 'reactions_enabled',
            'is_hidden', 'is_deleted_for_all', 'show_in_profile',
            'email_verified', 'is_verified',
            'replies', 'join_requests', 'admin_alerts',
            'is_read'
        ]

        uint32_fields = [
            'max_members', 'slow_mode_interval', 'members_count', 'online_estimate',
            'unread_count', 'max_uses', 'used_count', 'remaining_uses',
            'reply_count', 'thread_messages_count', 'edit_count', 'views_count',
            'forward_count', 'file_size', 'failed_login_attempts',
            'relevance',
            'pin_order',
        ]

        uint8_fields = [
            'progress',
            'role_order'
        ]

        uint16_fields = [
            'width', 'height'
        ]

        uint64_fields = [
            'id', 'message_id', 'event_id', 'ban_id', 'invite_id',
            'target_id', 'last_message_id', 'chat_id', 'reply_to_message_id',
            'thread_root_id', 'forwarded_from_message_id',
            'forwarded_from_chat_id',
            'messages_count', 'version', 'cursor_id', 'before', 'after', 'limit',
            'contact_id', 'blocked_id', 'join_event_id', 'left_event_id',
            'last_read_message_id', 'linked_chat_id', 'linked_channel_message_id',
            'linked_discussion_message_id', 'request_id', 'offset',
            'row_version',
            'notification_id'
        ]

        json_fields = [
            'settings', 'permissions', 'restrictions', 'payload', 'metadata',
            'attachments', 'reactions', 'mentions', 'entities', 'edit_history',
            'collections', 'discussion_settings',
            'comments_settings',
            'reactions_settings',
            'attachments_json', 'reactions_json', 'recent_reactions',
            'mentions_json',
            'social_links',
            'private_chats', 'groups', 'channels', 'do_not_disturb',
            'data'
        ]

        date_fields = [
            'created_date', 'date_of_birth'
        ]

        timestamp_fields = [
            'created_at', 'updated_at', 'deleted_at', 'joined_at', 'left_at',
            'banned_at', 'expires_at', 'last_read_at', 'last_active_at',
            'last_message_at', 'saved_at', 'last_validated_at', 'blocked_at',
            'added_at', 'last_interaction_at', 'auto_saved_at', 'last_edit_at',
            'cursor_time', 'mute_until', 'deactivated_at', 'pinned_at', 'unpinned_at',
            'forwarded_at', 'reviewed_at', 'deleted_for_all_at', 'username_updated_at',
            'completed_at', 'forwarded_original_date',
            'password_updated_at', 'locked_until', 'last_login_at',
            'from_date', 'to_date'
        ]

        string_fields = [
            'sender_id', 'sender_role_at_time', 'message_type', 'content',
            'content_preview', 'delete_reason', 'status', 'type', 'subtype',
            'title', 'description', 'avatar_url', 'primary_region', 'role',
            'joined_method', 'ban_type', 'reason', 'reason_code', 'invite_code',
            'default_role', 'event_type', 'target_type', 'idempotency_key',
            'entity_type', 'first_name', 'last_name', 'phone', 'email', 'source',
            'user_id', 'created_by', 'owner_id', 'last_message_sender_id',
            'pinned_by', 'unpinned_by', 'forwarded_by', 'forward_comment',
            'forwarded_original_sender_id',
            'forwarded_original_sender_name',
            'reject_reason', 'deleted_for_all_by', 'reply_to_sender_id',
            'username', 'photo_id', 'url_original', 'url_large', 'url_medium',
            'url_small', 'caption', 'mime_type', 'error_message',
            'content_hash',
            'username', 'email', 'first_name_encrypted', 'last_name_encrypted',
            'display_name', 'avatar_url', 'cover_url', 'status', 'country_code',
            'timezone', 'bio', 'about', 'website', 'company', 'position',
            'education', 'last_login_ip', 'password_hash', 'password_algo',
            'word_0', 'word_1', 'word_2', 'word_3', 'word_4', 'word_5',
            'word_6', 'word_7', 'word_8', 'word_9',
            'search_query', 'sort_by',
            'chat_title',
            'sender_name',
            'message_preview',
            'permissions_json',
            'reactions_json',
            'mentions_json',
            'attachments_json'
        ]

        # --- ОСНОВНАЯ ЛОГИКА ОПРЕДЕЛЕНИЯ ТИПА ---
        for key, value in params.items():
            key_clean = key.lower().replace('$', '')

            # --- 1. СПЕЦИАЛЬНЫЕ СЛУЧАИ ---
            if key_clean.startswith('word_'):
                param_type = 'Utf8' + ('?' if value is None else '')
                declares.append(f"DECLARE {key} AS {param_type};")
                continue

            if table_name == 'contacts' and key_clean in ['user_id', 'contact_id']:
                param_type = 'Uint64' + ('?' if value is None else '')
                declares.append(f"DECLARE {key} AS {param_type};")
                continue

            if table_name in ['user_blocks', 'global_blocks'] and key_clean in ['user_id', 'blocked_id']:
                param_type = 'Uint64' + ('?' if value is None else '')
                declares.append(f"DECLARE {key} AS {param_type};")
                continue

            # --- 2. ОПРЕДЕЛЕНИЕ ПО ИМЕНИ ПОЛЯ ---
            if key_clean in bool_fields:
                param_type = 'Bool' + ('?' if value is None else '')
            elif key_clean in uint8_fields:
                param_type = 'Uint8' + ('?' if value is None else '')
            elif key_clean in uint16_fields:
                param_type = 'Uint16' + ('?' if value is None else '')
            elif key_clean in uint32_fields:
                param_type = 'Uint32' + ('?' if value is None else '')
            elif key_clean in uint64_fields:
                param_type = 'Uint64' + ('?' if value is None else '')
            elif key_clean in json_fields:
                param_type = 'Json' + ('?' if value is None else '')
            elif key_clean in date_fields:
                param_type = 'Date' + ('?' if value is None else '')
            elif key_clean in timestamp_fields:
                param_type = 'Timestamp' + ('?' if value is None else '')
            elif key_clean in string_fields:
                param_type = 'Utf8' + ('?' if value is None else '')
            else:
                # --- 3. FALLBACK ---
                if value is None:
                    param_type = 'Utf8?'
                elif isinstance(value, bool):
                    param_type = 'Bool'
                elif isinstance(value, int):
                    if 0 <= value <= 255:
                        param_type = 'Uint8'
                    elif 0 <= value <= 65535:
                        param_type = 'Uint16'
                    elif 0 <= value <= 4294967295:
                        param_type = 'Uint32'
                    else:
                        param_type = 'Uint64'
                elif isinstance(value, float):
                    param_type = 'Double'
                elif isinstance(value, str):
                    param_type = 'Utf8'
                elif isinstance(value, datetime):
                    param_type = 'Timestamp'
                else:
                    param_type = 'Utf8'

            declares.append(f"DECLARE {key} AS {param_type};")

        result = "\n".join(declares)
        self._prepared_queries[cache_key] = result
        return result



# ============================================
# РЕПОЗИТОРИЙ ИДЕМПОТЕНТНОСТИ
# ============================================

class IdempotencyRepository(BaseRepository):
    """Репозиторий для таблицы idempotency_keys с автоочисткой"""
    
    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "idempotency_keys"
    
    async def create(self, key: IdempotencyKey) -> bool:
        """Сохранить ключ идемпотентности"""
        data = key.to_db_row()
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns})
        VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            
            if int(time.time()) % 100 == 0:
                asyncio.create_task(self._cleanup_expired())
            
            return True
        except Exception as e:
            logger.error(f"Error creating idempotency key: {e}")
            return False
    
    async def get(self, idempotency_key: str) -> Optional[IdempotencyKey]:
        """Получить ключ идемпотентности"""
        query = f"""
        DECLARE $idempotency_key AS Utf8;
        SELECT * FROM {self.table_name}
        WHERE idempotency_key = $idempotency_key;
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
        """Удалить ключ идемпотентности"""
        query = f"""
        DECLARE $idempotency_key AS Utf8;
        DELETE FROM {self.table_name}
        WHERE idempotency_key = $idempotency_key;
        """
        
        params = {'$idempotency_key': idempotency_key}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error deleting idempotency key: {e}")
            return False
    
    async def _cleanup_expired(self):
        """Фоновая очистка истекших ключей"""
        try:
            query = f"""
            DECLARE $now AS Timestamp;
            DELETE FROM {self.table_name}
            WHERE expires_at < $now;
            """
            params = {'$now': to_timestamp(datetime.utcnow())}
            await self.execute(query, params)
            logger.info("Cleaned up expired idempotency keys")
        except Exception as e:
            logger.error(f"Error cleaning up idempotency keys: {e}")


# ============================================
# КЭШ УЧАСТНИКОВ
# ============================================

class ParticipantCache:
    """Улучшенный кэш для информации об участниках чата"""
    
    _instance = None
    _participant_repo = None
    _chat_repo = None
    _batch_size = 100
    
    @classmethod
    async def _get_participant_repo(cls, session=None):
        """Получить репозиторий участников с опциональной сессией"""
        from handlers.chat_handler import ParticipantRepository
        
        if session:
            return ParticipantRepository(session)
        else:
            logger.warning(
                f"⚠️ ParticipantCache._get_participant_repo вызван без session!\n"
                f"Это может привести к созданию множества сессий"
            )
            repo = ParticipantRepository()
            return repo
    
    @classmethod
    async def _get_chat_repo(cls, session=None):
        """Получить репозиторий чатов с опциональной сессией"""
        from handlers.chat_handler import ChatRepository
        
        if session:
            return ChatRepository(session)
        else:
            logger.warning(
                f"⚠️ ParticipantCache._get_chat_repo вызван без session!\n"
                f"Передавайте session из RequestContext"
            )
            repo = ChatRepository()
            return repo
    
    @classmethod
    async def get_participant(cls, chat_id: int, user_id: str, session=None) -> Optional[Dict]:
        """
        Получить информацию об участнике с version stamp
        Обязательно передавать session из RequestContext!
        """
        cache_key = f"participant:v2:{chat_id}:{user_id}"
        
        # Пробуем из кэша
        cached = await cache.get(cache_key)
        if cached:
            logger.debug(f"✅ Participant cache hit for {chat_id}:{user_id[:8]}")
            return cached
        
        # 👇 ВАЖНО: проверяем, передана ли сессия
        if session is None:
            logger.error(
                f"❌ ParticipantCache.get_participant вызван без session!\n"
                f"chat_id={chat_id}, user_id={user_id[:8]}\n"
                f"Это приведет к созданию отдельной сессии и возможным ошибкам!"
            )
            # Создаем временную сессию (не идеально, но лучше чем ошибка)
            from db.pool import get_db_pool
            pool = get_db_pool()
            temp_session = await pool.get_session()
            try:
                repo = await cls._get_participant_repo(temp_session)
                participant = await repo.get(chat_id, user_id)
                
                if participant:
                    # Получаем permissions если есть
                    permissions = {}
                    if hasattr(participant, 'permissions') and participant.permissions:
                        permissions = participant.permissions
                    
                    result = {
                        'role': participant.role,
                        'role_order': getattr(participant, 'role_order', 4),
                        'is_active': participant.is_active,
                        'is_blocked': participant.is_blocked,
                        'mute_until': participant.mute_until.isoformat() if participant.mute_until else None,
                        'joined_at': participant.joined_at.isoformat() if participant.joined_at else None,
                        'last_active_at': participant.last_active_at.isoformat() if participant.last_active_at else None,
                        'unread_count': participant.unread_count,
                        'is_hidden': participant.is_hidden,
                        'show_in_profile': getattr(participant, 'show_in_profile', True),
                        'permissions': permissions,
                        'version': getattr(participant, 'version', 1),
                        'cached_at': datetime.utcnow().isoformat()
                    }
                    await cache.set(cache_key, result, ttl=common_config.CACHE_PARTICIPANT_TTL)
                    logger.debug(f"✅ Participant loaded from DB (temp session) for {chat_id}:{user_id[:8]}")
                    return result
                logger.debug(f"❌ Participant not found for {chat_id}:{user_id[:8]}")
                return None
            except Exception as e:
                logger.error(f"❌ Error getting participant with temp session: {e}")
                return None
            finally:
                await pool.release_session(temp_session)
        
        # 👇 Если сессия передана - используем её (ПРАВИЛЬНЫЙ ПУТЬ)
        logger.debug(f"📦 Getting participant for {chat_id}:{user_id[:8]} with provided session")
        try:
            repo = await cls._get_participant_repo(session)
            participant = await repo.get(chat_id, user_id)
            
            if participant:
                # Получаем permissions если есть
                permissions = {}
                if hasattr(participant, 'permissions') and participant.permissions:
                    permissions = participant.permissions
                
                result = {
                    'role': participant.role,
                    'role_order': getattr(participant, 'role_order', 4),
                    'is_active': participant.is_active,
                    'is_blocked': participant.is_blocked,
                    'mute_until': participant.mute_until.isoformat() if participant.mute_until else None,
                    'joined_at': participant.joined_at.isoformat() if participant.joined_at else None,
                    'last_active_at': participant.last_active_at.isoformat() if participant.last_active_at else None,
                    'unread_count': participant.unread_count,
                    'is_hidden': participant.is_hidden,
                    'show_in_profile': getattr(participant, 'show_in_profile', True),
                    'permissions': permissions,
                    'version': getattr(participant, 'version', 1),
                    'cached_at': datetime.utcnow().isoformat()
                }
                await cache.set(cache_key, result, ttl=common_config.CACHE_PARTICIPANT_TTL)
                logger.debug(f"✅ Participant loaded from DB for {chat_id}:{user_id[:8]}")
                return result
            logger.debug(f"❌ Participant not found for {chat_id}:{user_id[:8]}")
            return None
        except Exception as e:
            logger.error(f"❌ Error getting participant: {e}")
            return None
    
    @classmethod
    async def get_batch_participants(cls, chat_id: int, user_ids: List[str], session=None) -> Dict[str, Optional[Dict]]:
        """Получить информацию о нескольких участниках за один запрос"""
        result = {}
        missing_ids = []
        cache_keys = []
        
        # Сначала проверяем кэш
        for user_id in user_ids:
            cache_key = f"participant:v2:{chat_id}:{user_id}"
            cache_keys.append(cache_key)
        
        cached_values = await cache.mget(cache_keys)
        
        for user_id, cached in zip(user_ids, cached_values):
            if cached:
                result[user_id] = cached
            else:
                missing_ids.append(user_id)
        
        # Если есть отсутствующие в кэше - идем в БД
        if missing_ids:
            # 👇 Проверяем наличие сессии
            if session is None:
                logger.error(f"❌ get_batch_participants вызван без session для {len(missing_ids)} пользователей")
                # Для отсутствующих возвращаем None
                for user_id in missing_ids:
                    result[user_id] = None
                return result
            
            repo = await cls._get_participant_repo(session)
            
            for i in range(0, len(missing_ids), cls._batch_size):
                batch_ids = missing_ids[i:i + cls._batch_size]
                participants = await repo.get_by_users(chat_id, batch_ids)
                
                for user_id, participant in participants.items():
                    if participant:
                        permissions = {}
                        if hasattr(participant, 'permissions') and participant.permissions:
                            permissions = participant.permissions
                        
                        participant_dict = {
                            'role': participant.role,
                            'role_order': getattr(participant, 'role_order', 4),
                            'is_active': participant.is_active,
                            'is_blocked': participant.is_blocked,
                            'mute_until': participant.mute_until.isoformat() if participant.mute_until else None,
                            'joined_at': participant.joined_at.isoformat() if participant.joined_at else None,
                            'last_active_at': participant.last_active_at.isoformat() if participant.last_active_at else None,
                            'unread_count': participant.unread_count,
                            'is_hidden': participant.is_hidden,
                            'show_in_profile': getattr(participant, 'show_in_profile', True),
                            'permissions': permissions,
                            'version': getattr(participant, 'version', 1)
                        }
                        result[user_id] = participant_dict
                        
                        await cache.set(
                            f"participant:v2:{chat_id}:{user_id}", 
                            participant_dict, 
                            ttl=common_config.CACHE_PARTICIPANT_TTL
                        )
                    else:
                        result[user_id] = None
        
        return result
    
    @classmethod
    async def get_chat_members(cls, chat_id: int, session=None) -> Set[str]:
        """Получить множество всех участников чата с кэшированием"""
        cache_key = f"chat:members:{chat_id}"
        
        cached = await cache.get(cache_key)
        if cached:
            return set(cached)
        
        # 👇 Проверяем наличие сессии
        if session is None:
            logger.error(f"❌ get_chat_members вызван без session для chat_id={chat_id}")
            return set()
        
        repo = await cls._get_participant_repo(session)
        participants = await repo.list_by_chat(chat_id, limit=10000, active_only=True)
        
        member_set = {p.user_id for p in participants if p.is_active}
        
        await cache.set(cache_key, list(member_set), ttl=common_config.CACHE_CHAT_MEMBERS_TTL)
        
        return member_set
    
    @classmethod
    async def check_access(cls, chat_id: int, user_id: str, session=None) -> bool:
        """Проверить доступ к чату с использованием кэша"""
        cache_key = f"chat:v2:{chat_id}"
        chat = await cache.get(cache_key)
        
        if not chat:
            # 👇 Проверяем наличие сессии
            if session is None:
                logger.error(f"❌ check_access вызван без session для chat_id={chat_id}")
                return False
            
            chat_repo = await cls._get_chat_repo(session)
            chat_obj = await chat_repo.get_by_id(chat_id)
            if chat_obj:
                chat = {
                    'is_public': chat_obj.is_public,
                    'is_deleted': chat_obj.is_deleted,
                    'type': chat_obj.type,
                    'version': chat_obj.version
                }
                await cache.set(cache_key, chat, ttl=common_config.CACHE_CHAT_TTL)
        
        if chat and chat.get('is_public') and not chat.get('is_deleted'):
            return True
        
        # 👇 Передаем session дальше
        participant = await cls.get_participant(chat_id, user_id, session)
        return participant is not None and participant.get('is_active', False)
    
    @classmethod
    async def check_permission(cls, chat_id: int, user_id: str, permission: str, session=None) -> bool:
        """Проверить наличие конкретного права"""
        # 👇 Передаем session дальше
        participant = await cls.get_participant(chat_id, user_id, session)
        if not participant:
            return False
        
        if participant.get('role') == 'owner':
            return True
        
        permissions = participant.get('permissions', {})
        return permissions.get(permission, False)
    
    @classmethod
    async def check_any_permission(cls, chat_id: int, user_id: str, permissions: List[str], session=None) -> bool:
        """Проверить наличие хотя бы одного из прав"""
        # 👇 Передаем session дальше
        participant = await cls.get_participant(chat_id, user_id, session)
        if not participant:
            return False
        
        if participant.get('role') == 'owner':
            return True
        
        user_perms = participant.get('permissions', {})
        return any(user_perms.get(p, False) for p in permissions)
    
    @classmethod
    async def is_member(cls, chat_id: int, user_id: str, session=None) -> bool:
        """Быстрая проверка, является ли пользователь участником"""
        # 👇 Передаем session дальше
        participants = await cls.get_chat_members(chat_id, session)
        return user_id in participants
    
    @classmethod
    async def invalidate(cls, chat_id: int, user_id: str):
        """Инвалидировать кэш участника"""
        await cache.delete(f"participant:v2:{chat_id}:{user_id}")
        await cache.delete(f"chat:members:{chat_id}")  # Также сбрасываем кэш участников чата
    
    @classmethod
    async def invalidate_chat(cls, chat_id: int):
        """Инвалидировать кэш чата"""
        await cache.delete(f"chat:v2:{chat_id}")
        await cache.delete(f"chat:members:{chat_id}")
    
    @classmethod
    async def get_chat_participants_set(cls, chat_id: int, session=None) -> Set[str]:
        """Получить множество всех участников чата (для обратной совместимости)"""
        return await cls.get_chat_members(chat_id, session)


# ============================================
# РЕПОЗИТОРИЙ НАСТРОЕК УВЕДОМЛЕНИЙ
# ============================================

class NotificationSettingsRepository(BaseRepository):
    """Репозиторий для настроек уведомлений"""
    
    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "notification_settings"
    
    async def get(self, user_id: str) -> Optional['NotificationSettings']:
        """Получить настройки пользователя"""
        query = f"""
        DECLARE $user_id AS Utf8;
        SELECT * FROM {self.table_name} WHERE user_id = $user_id;
        """
        params = {'$user_id': user_id}
        
        try:
            rows = await self.execute(query, params)
            if rows:
                from handlers.message_handler import NotificationSettings
                return NotificationSettings.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Failed to get notification settings: {e}")
            return None
    
    async def get_batch(self, user_ids: List[str]) -> Dict[str, 'NotificationSettings']:
        """Получить настройки для нескольких пользователей одним запросом"""
        if not user_ids:
            return {}
        
        from handlers.message_handler import NotificationSettings
        
        unions = []
        params = {}
        
        for i, user_id in enumerate(user_ids):
            param_name = f"$user_id_{i}"
            unions.append(f"SELECT * FROM {self.table_name} WHERE user_id = {param_name}")
            params[param_name] = user_id
        
        query = " UNION ALL ".join(unions) + ";"
        declare_block = self._generate_declare(params)
        query = f"{declare_block}\n{query}"
        
        try:
            rows = await self.execute(query, params)
            result = {}
            for row in rows:
                settings = NotificationSettings.from_db_row(row)
                result[settings.user_id] = settings
            return result
        except Exception as e:
            logger.error(f"Failed to get batch notification settings: {e}")
            return {}
    
    async def save(self, settings: 'NotificationSettings') -> bool:
        """Сохранить настройки"""
        data = settings.to_db_row()
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
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
            logger.error(f"Failed to save notification settings: {e}")
            return False


# ============================================
# UNIT OF WORK
# ============================================

class UnitOfWork:
    """Базовый Unit of Work паттерн с поддержкой транзакций"""
    
    def __init__(self):
        self.idempotency = None
        self._session = None
        self._transaction = None
        self._repositories = []
        self._uow_id = str(uuid.uuid4())[:8]
        self._session_provided = False
        self._session_id = None
    
    @classmethod
    async def with_session(cls, session):
        """
        Создать UOW с существующей сессией
        Использование: async with await UnitOfWork.with_session(session) as uow:
        """
        uow = cls()
        uow._session = session
        uow._session_provided = True
        uow._session_id = id(session)
        await uow.__aenter__()
        return uow
    
    def set_session(self, session):
        """Установить существующую сессию"""
        self._session = session
        self._session_id = id(session)
        # Обновляем сессию во всех репозиториях
        for repo in self._repositories:
            repo.set_session(session)
            repo.set_transaction(self._transaction)
    
    async def __aenter__(self):
        """Вход в контекстный менеджер"""
        # Если сессия уже установлена через with_session
        if self._session is not None:
            self._session_id = id(self._session)
            self._transaction = self._session.transaction()
            await self._transaction.begin()
            
            TransactionMonitor.transaction_begin(self._session_id, self._uow_id)
            
            logger.debug(f"📦 [UOW {self._uow_id}] Using existing session {self._session_id}")
            
            self.idempotency = IdempotencyRepository(self._session)
            return self
        
        # Создаем новую сессию (с retry при BadSession)
        from db.pool import get_db_pool
        import ydb
        pool = get_db_pool()

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                self._session = await asyncio.wait_for(
                    pool.get_session(),
                    timeout=common_config.SESSION_ACQUIRE_TIMEOUT
                )
                self._session_id = id(self._session)
                logger.info(f"🆕 [UOW {self._uow_id}] Created new session {self._session_id} (attempt {attempt})")

                self._transaction = self._session.transaction()
                await self._transaction.begin()

                TransactionMonitor.transaction_begin(self._session_id, self._uow_id)
                self.idempotency = IdempotencyRepository(self._session)
                return self

            except asyncio.TimeoutError:
                logger.error(f"❌ [UOW {self._uow_id}] Timeout acquiring session (attempt {attempt})")
                raise DatabaseError("Failed to acquire database session")
            except ydb.issues.BadSession as e:
                logger.warning(
                    f"⚠️ [UOW {self._uow_id}] BadSession on attempt {attempt}, "
                    f"discarding and retrying: {e}"
                )
                # Не возвращаем стухшую сессию в пул — просто берём новую
                self._session = None
                self._transaction = None
                if attempt >= max_attempts:
                    raise DatabaseError(f"Failed to acquire valid session after {max_attempts} attempts")
                await asyncio.sleep(0.05 * attempt)
                continue
            except Exception as e:
                logger.error(f"❌ [UOW {self._uow_id}] Error acquiring session (attempt {attempt}): {e}")
                raise
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекстного менеджера"""
        session_id = self._session_id
        
        try:
            if exc_type:
                if self._transaction:
                    logger.warning(
                        f"⚠️ [UOW {self._uow_id}] Rolling back transaction in session {session_id} "
                        f"due to error: {exc_type.__name__}"
                    )
                    await self._transaction.rollback()
                    TransactionMonitor.transaction_end(session_id, committed=False, uow_id=self._uow_id)
            else:
                if self._transaction:
                    logger.debug(f"✅ [UOW {self._uow_id}] Committing transaction in session {session_id}")
                    await self._transaction.commit()
                    TransactionMonitor.transaction_end(session_id, committed=True, uow_id=self._uow_id)
        except Exception as e:
            logger.error(f"❌ [UOW {self._uow_id}] Transaction error: {e}")
            # При ошибке коммита пробуем откатить
            if self._transaction:
                try:
                    await self._transaction.rollback()
                    TransactionMonitor.transaction_end(session_id, committed=False, uow_id=self._uow_id)
                    logger.info(f"✅ [UOW {self._uow_id}] Rolled back after commit error")
                except Exception as rollback_error:
                    logger.error(f"❌ [UOW {self._uow_id}] Rollback after error failed: {rollback_error}")
            raise
        finally:
            # Освобождаем сессию только если мы ее создали
            if self._session and not self._session_provided:
                from db.pool import get_db_pool
                pool = get_db_pool()
                if pool:
                    logger.debug(f"♻️ [UOW {self._uow_id}] Releasing session {session_id}")
                    await pool.release_session(self._session)
                    TransactionMonitor.session_released(session_id)
                self._session = None
                self._transaction = None
    
    async def begin_transaction(self):
        """Начать транзакцию (уже начата в __aenter__)"""
        return self._transaction
    
    async def commit(self):
        """Зафиксировать транзакцию и начать новую"""
        if self._transaction:
            await self._transaction.commit()
            TransactionMonitor.transaction_end(self._session_id, committed=True, uow_id=self._uow_id)
            logger.debug(f"✅ [UOW {self._uow_id}] Transaction explicitly committed")
            
            # Начинаем новую транзакцию
            self._transaction = self._session.transaction()
            await self._transaction.begin()
            TransactionMonitor.transaction_begin(self._session_id, self._uow_id)
            
            for repo in self._repositories:
                repo.set_transaction(self._transaction)
            
            logger.debug(f"🔄 [UOW {self._uow_id}] New transaction started")
    
    async def rollback(self):
        """Откатить транзакцию"""
        if self._transaction:
            await self._transaction.rollback()
            TransactionMonitor.transaction_end(self._session_id, committed=False, uow_id=self._uow_id)
            logger.debug(f"⚠️ [UOW {self._uow_id}] Transaction explicitly rolled back")
            self._transaction = None
    
    def register_repository(self, repo):
        """Зарегистрировать репозиторий для использования сессии"""
        self._repositories.append(repo)
        if self._session:
            repo.set_session(self._session)
            repo.set_transaction(self._transaction)
        logger.debug(f"📦 [UOW {self._uow_id}] Registered repository {repo.__class__.__name__}")
        return repo
# ============================================
# УТИЛИТЫ
# ============================================

def to_timestamp(dt: Optional[datetime]) -> Optional[int]:
    """Конвертирует datetime в timestamp (microseconds) для YDB"""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return int(dt.timestamp() * 1_000_000)

def from_timestamp(ts: Optional[int]) -> Optional[datetime]:
    """Конвертирует timestamp (microseconds) в datetime"""
    if ts is None:
        return None
    return datetime.fromtimestamp(ts / 1_000_000)

def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Безопасное преобразование в int с проверкой переполнения"""
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
    """Безопасное преобразование в str"""
    if value is None:
        return default
    return str(value)

def to_uint64(value: Optional[str]) -> Optional[int]:
    """Конвертирует UUID строку в Uint64 для YDB с контролем коллизий"""
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
    """Валидация ключа идемпотентности"""
    if key:
        if len(key) > 255:
            raise ValidationError("Idempotency key too long")
        if not re.match(r'^[A-Za-z0-9\-_]+$', key):
            raise ValidationError("Invalid idempotency key format")

def parse_json_body(body: Any) -> Dict:
    """Парсинг JSON тела запроса"""
    if not body:
        return {}
    try:
        if isinstance(body, str):
            return json.loads(body)
        return body
    except json.JSONDecodeError:
        raise ValidationError("Invalid JSON body")

def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Разбить список на чанки указанного размера"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


# ============================================
# КЛАСС ДЛЯ ОТВЕТОВ
# ============================================

class ResponseHelper:
    """Хелпер для формирования HTTP ответов с поддержкой сжатия"""
    
    def __init__(self, enable_compression: bool = True):
        self.enable_compression = enable_compression
    
    def success(self, data: Any, status_code: int = 200, headers: Dict = None, event: Dict = None) -> Dict:
        """
        Формирование успешного ответа с поддержкой сжатия по заголовку Accept-Encoding
        """
        response_headers = {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Idempotency-Key',
            'Cache-Control': 'no-cache',
        }
        
        if headers:
            response_headers.update(headers)
        
        body = json.dumps({
            'success': True,
            'data': data
        }, ensure_ascii=False, default=str)
        
        accept_encoding = ''
        if event and event.get('headers'):
            headers_dict = {k.lower(): v for k, v in event.get('headers', {}).items()}
            accept_encoding = headers_dict.get('accept-encoding', '')
        
        if ('gzip' in accept_encoding and 
            len(body) > common_config.COMPRESS_RESPONSE_SIZE and 
            self.enable_compression):
            
            import gzip
            import base64
            
            compressed = gzip.compress(body.encode('utf-8'))
            encoded = base64.b64encode(compressed).decode('ascii')
            
            response_headers['Content-Encoding'] = 'gzip'
            response_headers['Content-Type'] = 'application/json'
            
            logger.debug(f"Response compressed: {len(body)} -> {len(compressed)} bytes")
            
            return {
                'statusCode': status_code,
                'headers': response_headers,
                'body': encoded,
                'isBase64Encoded': True
            }
        
        return {
            'statusCode': status_code,
            'headers': response_headers,
            'body': body,
            'isBase64Encoded': False
        }
    
    def error(self, message: str, status_code: int = 400, code: str = None, details: Dict = None, event: Dict = None) -> Dict:
        """Формирование ответа с ошибкой (без сжатия)"""
        error_body = {
            'success': False,
            'error': {
                'message': message,
                'code': code or 'ERROR'
            }
        }
        if details:
            error_body['error']['details'] = details
        
        return {
            'statusCode': status_code,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Idempotency-Key'
            },
            'body': json.dumps(error_body, ensure_ascii=False),
            'isBase64Encoded': False
        }
    
    def options(self) -> Dict:
        """Ответ на OPTIONS запрос (CORS)"""
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS, PATCH',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Idempotency-Key, X-Request-ID',
                'Access-Control-Max-Age': '86400',
                'Access-Control-Allow-Credentials': 'true'
            },
            'body': ''
        }


# ============================================
# БАЗОВЫЙ ХЕНДЛЕР
# ============================================
# В common.py, после импортов, добавим:


class TransactionMonitor:
    """Мониторинг транзакций и сессий"""
    _active_sessions = 0
    _total_sessions = 0
    _active_transactions = {}  # session_id -> count
    _transaction_log = deque(maxlen=100)
    _session_stack = {}  # session_id -> stack trace
    _monitoring_task = None
    _running = False
    
    @classmethod
    def session_created(cls, source: str, session_id: int, stack: str = ""):
        """Сессия создана"""
        cls._active_sessions += 1
        cls._total_sessions += 1
        cls._session_stack[session_id] = stack[:500]
        
        logger.info(
            f"🔌 [MONITOR] SESSION CREATED: id={session_id}, source={source}, "
            f"active={cls._active_sessions}, total={cls._total_sessions}"
        )
    
    @classmethod
    def session_released(cls, session_id: int):
        """Сессия освобождена"""
        # Защита от отрицательных значений
        if cls._active_sessions > 0:
            cls._active_sessions -= 1
        else:
            logger.warning(f"⚠️ [MONITOR] Attempted to release session {session_id} but active_sessions is 0!")
        
        if session_id in cls._session_stack:
            del cls._session_stack[session_id]
        
        logger.info(
            f"🔌 [MONITOR] SESSION RELEASED: id={session_id}, "
            f"active={cls._active_sessions}"
        )
    
    @classmethod
    def transaction_begin(cls, session_id: int, uow_id: str = ""):
        """Транзакция начата"""
        cls._active_transactions[session_id] = cls._active_transactions.get(session_id, 0) + 1
        count = cls._active_transactions[session_id]
        
        cls._transaction_log.append({
            'time': datetime.utcnow().isoformat(),
            'session_id': session_id,
            'uow_id': uow_id,
            'action': 'BEGIN',
            'count': count
        })
        
        stack = ''.join(traceback.format_stack()[:-1])
        
        if count >= 10:
            logger.error(
                f"🔥🔥🔥 [MONITOR] MAX TRANSACTIONS REACHED! "
                f"Session {session_id} has {count} active transactions!\n"
                f"Stack:\n{stack[:1000]}"
            )
        elif count >= 5:
            logger.warning(
                f"⚠️ [MONITOR] High transaction count: session {session_id} has {count} active\n"
                f"Stack:\n{stack[:500]}"
            )
        else:
            logger.debug(
                f"🔓 [MONITOR] TX BEGIN: session={session_id}, uow={uow_id}, "
                f"active={count}"
            )
    
    @classmethod
    def transaction_end(cls, session_id: int, committed: bool, uow_id: str = ""):
        """Транзакция завершена"""
        old_count = cls._active_transactions.get(session_id, 0)
        
        if session_id in cls._active_transactions:
            cls._active_transactions[session_id] -= 1
            if cls._active_transactions[session_id] <= 0:
                del cls._active_transactions[session_id]
        
        new_count = cls._active_transactions.get(session_id, 0)
        
        cls._transaction_log.append({
            'time': datetime.utcnow().isoformat(),
            'session_id': session_id,
            'uow_id': uow_id,
            'action': 'COMMIT' if committed else 'ROLLBACK',
            'count': new_count
        })
        
        if old_count > new_count + 1:
            logger.error(
                f"🔥 [MONITOR] Transaction count mismatch! "
                f"Session {session_id} had {old_count}, now {new_count}"
            )
        
        logger.debug(
            f"🔒 [MONITOR] TX {committed and 'COMMIT' or 'ROLLBACK'}: "
            f"session={session_id}, uow={uow_id}, active={new_count}"
        )
    
    @classmethod
    def log_status(cls):
        """Логировать текущий статус"""
        total_active_tx = sum(cls._active_transactions.values())
        
        status = (
            f"📊 [MONITOR] STATUS: "
            f"sessions={cls._active_sessions}, "
            f"total_created={cls._total_sessions}, "
            f"active_tx={total_active_tx}, "
            f"tx_by_session={dict(cls._active_transactions)}"
        )
        
        if total_active_tx > 3:
            logger.warning(status)
        else:
            logger.info(status)

        # Логируем детали только при явной утечке (> 3 одновременных транзакций)
        if total_active_tx > 3:
            for sid, count in cls._active_transactions.items():
                stack = cls._session_stack.get(sid, "No stack")
                logger.warning(
                    f"  Session {sid}: {count} active transactions\n"
                    f"  Created at:\n{stack[:500]}"
                )
        
        return {
            'active_sessions': cls._active_sessions,
            'total_sessions': cls._total_sessions,
            'active_transactions': dict(cls._active_transactions),
            'total_active_tx': total_active_tx,
            'recent_transactions': list(cls._transaction_log)[-20:]
        }
    
    @classmethod
    def start_periodic_logging(cls, interval: int = 30):
        """Запустить периодическое логирование"""
        if cls._running:
            return
        
        cls._running = True
        
        async def _log_loop():
            while cls._running:
                await asyncio.sleep(interval)
                try:
                    cls.log_status()
                except Exception as e:
                    logger.error(f"Monitor error: {e}")
        
        cls._monitoring_task = asyncio.create_task(_log_loop())
        logger.info(f"✅ Transaction monitor started (interval={interval}s)")
    
    @classmethod
    def stop_periodic_logging(cls):
        """Остановить периодическое логирование"""
        cls._running = False
        if cls._monitoring_task:
            cls._monitoring_task.cancel()
            cls._monitoring_task = None
        logger.info("🛑 Transaction monitor stopped")


class BaseHandler:
    """Базовый класс для всех хендлеров с улучшенной обработкой"""
    
    def __init__(self):
        self.response = ResponseHelper()
        self.validators = Validators()
        self._request_id = None
    
    def _parse_body(self, event: Dict) -> Dict:
        """Парсинг JSON тела запроса с валидацией размера"""
        try:
            body = event.get('body')
            if not body:
                return {}
            
            if len(body) > 1024 * 1024:
                raise ValidationError("Request body too large")
            
            return parse_json_body(body)
        except ValidationError as e:
            raise e
        except Exception as e:
            raise ValidationError(f"Invalid request body: {str(e)}")
    
    def _get_idempotency_key(self, event: Dict) -> Optional[str]:
        """Получить ключ идемпотентности из заголовков"""
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        key = headers.get('x-idempotency-key')
        if key:
            validate_idempotency_key(key)
        return key
    
    def _get_request_id(self, event: Dict) -> str:
        """Получить или сгенерировать ID запроса для трекинга"""
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        request_id = headers.get('x-request-id')
        if not request_id:
            request_id = str(uuid.uuid4())
        self._request_id = request_id
        return request_id
    
    def _get_client_ip(self, event: Dict) -> str:
        """Получить реальный IP клиента"""
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        ip = (headers.get('x-real-ip') or 
              headers.get('x-forwarded-for', '').split(',')[0].strip() or 
              'unknown')
        return ip
    
    def _get_query_param(self, event: Dict, param: str, default: Any = None) -> Any:
        """Получить параметр из query string"""
        query = event.get('queryStringParameters') or {}
        return query.get(param, default)
    
    def _get_int_query_param(self, event: Dict, param: str, default: int) -> int:
        """Получить целочисленный параметр из query string"""
        value = self._get_query_param(event, param)
        if value is None:
            return default
        return safe_int(value) or default
    
    def _get_bool_query_param(self, event: Dict, param: str, default: bool) -> bool:
        """Получить булев параметр из query string"""
        value = self._get_query_param(event, param)
        if value is None:
            return default
        return str(value).lower() in ['true', '1', 'yes', 'on']
    
    def _get_cursor(self, event: Dict) -> Optional[str]:
        """Получить и валидировать курсор пагинации"""
        cursor = self._get_query_param(event, 'cursor')
        if cursor and not isinstance(cursor, str):
            logger.warning(f"Cursor is not a string: {cursor}, ignoring")
            return None
        if cursor and len(cursor) > 500:
            logger.warning(f"Cursor too long: {len(cursor)} chars, ignoring")
            return None
        return cursor
    
    def _validate_chat_id(self, chat_id: Any) -> int:
        """Валидация ID чата"""
        return self.validators.validate_int(chat_id, "chat_id", min_value=1)
    
    def _validate_user_id(self, user_id: Any) -> str:
        """Валидация ID пользователя"""
        return self.validators.validate_uuid(user_id, "user_id")
    
    def _validate_message_id(self, message_id: Any) -> int:
        """Валидация ID сообщения"""
        return self.validators.validate_int(message_id, "message_id", min_value=1)
    
    async def handle_error(self, e: Exception, event: Dict) -> Dict:
        """Централизованная обработка ошибок"""
        request_id = self._get_request_id(event)
        
        if isinstance(e, ValidationError):
            logger.warning(f"Validation error [{request_id[:8]}]: {e.message}")
            return self.response.error(e.message, e.status_code, e.code, event=event)
        
        if isinstance(e, PermissionError):
            logger.warning(f"Permission error [{request_id[:8]}]: {e.message}")
            return self.response.error(e.message, e.status_code, e.code, event=event)
        
        if isinstance(e, NotFoundError):
            logger.warning(f"Not found [{request_id[:8]}]: {e.message}")
            return self.response.error(e.message, e.status_code, e.code, event=event)
        
        if isinstance(e, RateLimitError):
            logger.warning(f"Rate limit [{request_id[:8]}]: {e.message}")
            return self.response.error(e.message, e.status_code, e.code, event=event)
        
        if isinstance(e, DatabaseError):
            logger.error(f"Database error [{request_id[:8]}]: {e.message}")
            return self.response.error("Internal server error", 500, "DATABASE_ERROR", event=event)
        
        logger.error(f"Unexpected error [{request_id[:8]}]: {e}", exc_info=True)
        return self.response.error("Internal server error", 500, "INTERNAL_ERROR", event=event)
    
    # ============================================
    # МЕТОДЫ ДЛЯ ИДЕМПОТЕНТНОСТИ
    # ============================================
    
    def _deserialize_result(self, result_data: Dict, entity_type: str):
        """
        Десериализовать результат из JSON в объект.
        Переопределяется в дочерних классах при необходимости.
        """
        if entity_type == 'message':
            from handlers.message_handler import Message
            return Message.from_dict(result_data)
        elif entity_type == 'chat':
            from handlers.chat_handler import Chat
            return Chat.from_dict(result_data)
        elif entity_type == 'contact':
            from handlers.message_handler import Contact
            # Contact может не иметь from_dict, возвращаем словарь
            return result_data
        else:
            return result_data
    
    async def _get_entity_by_id(self, entity_type: str, entity_id: int, chat_id: int, session):
        """
        Получить сущность по ID из БД.
        Переопределяется в дочерних классах при необходимости.
        """
        if entity_type == 'message':
            from handlers.message_handler import MessageRepository
            repo = MessageRepository(session)
            return await repo.get(chat_id, entity_id)
        elif entity_type == 'chat':
            from handlers.chat_handler import ChatRepository
            repo = ChatRepository(session)
            return await repo.get_by_id(entity_id)
        return None
# ============================================
# ФУНКЦИИ ДЛЯ ЗАПУСКА ВОРКЕРОВ ПРИ СТАРТЕ
# ============================================
def idempotent(entity_type: str, key_param: str = 'idempotency_key'):
    """
    Декоратор для автоматической проверки и сохранения ключей идемпотентности.
    
    Args:
        entity_type: тип сущности (message, chat, ban, role, invite, etc.)
        key_param: имя параметра в kwargs, содержащего ключ идемпотентности
    
    Usage:
        @idempotent(entity_type='message')
        async def send_message(self, ..., idempotency_key=None):
            ...
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Получаем ключ идемпотентности
            idempotency_key = kwargs.get(key_param)
            
            # Если ключа нет - просто выполняем функцию
            if not idempotency_key:
                return await func(self, *args, **kwargs)
            
            # Проверяем существующий ключ
            async with RequestContext() as ctx:
                repo = IdempotencyRepository(ctx.session)
                existing = await repo.get(idempotency_key)
                
                if existing:
                    logger.info(f"🔄 Idempotency hit: {idempotency_key[:8]} for {entity_type}")
                    
                    # Восстанавливаем результат из сохранённых данных
                    if existing.result_data:
                        # Если есть сохранённый результат в JSON, возвращаем его
                        return self._deserialize_result(existing.result_data, entity_type)
                    
                    # Если нет сохранённого результата, но есть entity_id, пытаемся получить из БД
                    if existing.entity_id:
                        return await self._get_entity_by_id(
                            entity_type, 
                            existing.entity_id, 
                            existing.chat_id,
                            ctx.session
                        )
                    
                    # Если ничего не нашли, возвращаем None
                    return None
            
            # Выполняем функцию
            result = await func(self, *args, **kwargs)
            
            # Сохраняем результат
            if result:
                async with RequestContext() as ctx:
                    repo = IdempotencyRepository(ctx.session)
                    
                    # Определяем ID сущности и chat_id
                    entity_id = None
                    chat_id = None
                    result_data = None
                    
                    # Пробуем извлечь ID из результата
                    if hasattr(result, 'id'):
                        entity_id = result.id
                    elif hasattr(result, 'message_id'):
                        entity_id = result.message_id
                    elif hasattr(result, 'chat_id'):
                        entity_id = result.chat_id
                    
                    # Пробуем извлечь chat_id
                    if hasattr(result, 'chat_id'):
                        chat_id = result.chat_id
                    elif isinstance(result, dict):
                        chat_id = result.get('chat_id')
                    
                    # Сериализуем результат в JSON
                    if hasattr(result, 'to_dict'):
                        result_data = result.to_dict()
                    elif isinstance(result, dict):
                        result_data = result
                    else:
                        result_data = {'id': entity_id}
                    
                    # Сохраняем ключ
                    key_obj = IdempotencyKey(
                        idempotency_key=idempotency_key,
                        entity_type=entity_type,
                        entity_id=entity_id,
                        chat_id=chat_id,
                        user_id=kwargs.get('user_id') or kwargs.get('user') or '',
                        created_at=datetime.utcnow(),
                        expires_at=datetime.utcnow() + timedelta(hours=24),
                        result_data=result_data
                    )
                    await repo.create(key_obj)
                    logger.info(f"💾 Idempotency saved: {idempotency_key[:8]} for {entity_type}")
            
            return result
        return wrapper
    return decorator
async def start_workers():
    """Запустить всех воркеров при старте приложения"""
    await notification_worker.start()
    await WebSocketManager.start_heartbeat()
    logger.info("🚀 All workers and heartbeat started")

async def stop_workers():
    """Остановить всех воркеров при завершении"""
    await notification_worker.stop()
    await WebSocketManager.stop_heartbeat()
    logger.info("🛑 All workers and heartbeat stopped")


# ============================================
# ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ОТПРАВКИ WS УВЕДОМЛЕНИЙ
# ============================================

async def send_ws_notification(user_id: str, notification_type: str, data: Dict) -> int:
    """Удобная обёртка для отправки WebSocket уведомлений (синхронный вызов)."""
    logger.info(f"🔔 [WS] Sending to user {user_id}, type={notification_type}")
    message = {
        'type': notification_type,
        'data': data,
        'timestamp': datetime.utcnow().isoformat()
    }
    return await WebSocketManager.send_to_user(user_id, message)

# ============================================
# ЭКСПОРТ
# ============================================

__all__ = [
    # Основные классы
    'BaseRepository', 'IdempotencyKey', 'IdempotencyRepository',
    'ParticipantCache', 'NotificationSettingsRepository',
    'UnitOfWork', 'RequestContext', 'BaseHandler',
    'ResponseHelper', 'Validators',
    
    # WebSocket
    'WebSocketManager', 'send_ws_notification',
    
    # Воркеры
    'NotificationWorker', 'notification_worker',
    'start_workers', 'stop_workers',
    
    # Декораторы
    'retry', 'rate_limit', 'measure_time', 'with_request_context',
    
    # Исключения
    'AppError', 'ValidationError', 'PermissionError', 
    'NotFoundError', 'RateLimitError', 'DatabaseError',
    
    # Конфиг
    'common_config',
    
    # Метрики
    'SessionMetrics',
    
    # Утилиты
    'to_timestamp', 'from_timestamp', 'to_uint64',
    'safe_int', 'safe_str', 'validate_idempotency_key',
    'parse_json_body', 'chunk_list',
    
    # Логгер и кэш
    'logger', 'cache',
]

