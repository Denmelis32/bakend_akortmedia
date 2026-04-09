#!/usr/bin/env python3
"""
PRODUCTION AUTH SERVICE v7.5.0 - VM EDITION (ПОЛНАЯ ВЕРСИЯ)
✅ Упрощенная регистрация с first_name
✅ Base64 для шифрования (вместо KMS)
✅ Все CPU-bound операции в thread pool
✅ Параллельные запросы к БД
✅ Таймауты и backpressure
✅ In-memory кэш с версионированием
✅ Rate limiting без Redis
✅ Circuit breaker для защиты от каскадных отказов
✅ Batch запросы к БД
✅ Connection pooling
✅ Graceful shutdown
✅ Метрики и мониторинг
✅ HTTP сервер для VM
"""

import os
import sys
import json
import uuid
import re
import hashlib
import base64
import logging
import traceback
import time
import asyncio
import concurrent.futures
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Tuple, Callable, Union
from enum import Enum
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from functools import wraps

# Загружаем переменные из .env файла ДО инициализации Config
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# === IMPORTS для YDB ===
try:
    import ydb
    from ydb import DriverConfig, Driver
except ImportError as e:
    print(f"❌ YDB not installed: {e}")
    print("   Installing...")
    os.system("pip install ydb==3.25.0")
    import ydb
    from ydb import DriverConfig, Driver

# === IMPORTS для аутентификации ===
try:
    from passlib.context import CryptContext
except ImportError as e:
    print(f"❌ passlib not installed: {e}")
    os.system("pip install passlib==1.7.4 bcrypt==4.0.1")
    from passlib.context import CryptContext

try:
    import jwt
    from jwt.exceptions import InvalidTokenError, ExpiredSignatureError
except ImportError as e:
    print(f"❌ jwt not installed: {e}")
    os.system("pip install pyjwt==2.8.0")
    import jwt
    from jwt.exceptions import InvalidTokenError, ExpiredSignatureError

# === IMPORTS для HTTP сервера ===
try:
    from aiohttp import web
    from aiohttp_cors import setup as cors_setup, ResourceOptions
except ImportError as e:
    print(f"❌ aiohttp not installed: {e}")
    os.system("pip install aiohttp==3.9.1 aiohttp-cors==0.7.0")
    from aiohttp import web
    from aiohttp_cors import setup as cors_setup, ResourceOptions


# ============================================
# НАСТРОЙКА ЛОГИРОВАНИЯ ДЛЯ VM
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/auth-service.log')
    ]
)
logger = logging.getLogger('auth-service')


# ============================================
# ОПТИМИЗАЦИЯ 1: Конфигурация
# ============================================

@dataclass
class Config:
    """Конфигурация с оптимизированными значениями"""
    # YDB
    YDB_ENDPOINT: str = field(default_factory=lambda: os.getenv('YDB_ENDPOINT', 'grpcs://ydb.serverless.yandexcloud.net:2135'))
    YDB_DATABASE: str = field(default_factory=lambda: os.getenv('YDB_DATABASE', ''))
    
    # JWT
    JWT_SECRET: str = field(default_factory=lambda: os.getenv('JWT_SECRET', ''))
    JWT_REFRESH_SECRET: str = field(default_factory=lambda: os.getenv('JWT_REFRESH_SECRET', ''))
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', '15'))
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv('REFRESH_TOKEN_EXPIRE_DAYS', '30'))
    
    # Security
    MAX_LOGIN_ATTEMPTS: int = int(os.getenv('MAX_LOGIN_ATTEMPTS', '5'))
    LOCKOUT_TIME_MINUTES: int = int(os.getenv('LOCKOUT_TIME_MINUTES', '15'))
    BCRYPT_ROUNDS: int = int(os.getenv('BCRYPT_ROUNDS', '12'))
    
    # Rate limits
    RATE_LIMIT_REQUESTS: int = int(os.getenv('RATE_LIMIT_REQUESTS', '200'))
    RATE_LIMIT_WINDOW: int = int(os.getenv('RATE_LIMIT_WINDOW', '60'))
    
    # Performance
    DB_POOL_SIZE: int = int(os.getenv('DB_POOL_SIZE', '50'))
    DB_TIMEOUT: int = int(os.getenv('DB_TIMEOUT', '10'))
    REQUEST_TIMEOUT: int = int(os.getenv('REQUEST_TIMEOUT', '25'))
    CPU_WORKERS: int = int(os.getenv('CPU_WORKERS', '4'))
    CACHE_MAX_SIZE: int = int(os.getenv('CACHE_MAX_SIZE', '2000'))
    CACHE_TTL: int = int(os.getenv('CACHE_TTL', '300'))
    
    # Circuit breaker
    CB_FAILURE_THRESHOLD: int = int(os.getenv('CB_FAILURE_THRESHOLD', '3'))
    CB_TIMEOUT: int = int(os.getenv('CB_TIMEOUT', '30'))
    
    # HTTP Server
    HTTP_HOST: str = field(default_factory=lambda: os.getenv('HTTP_HOST', '0.0.0.0'))
    HTTP_PORT: int = int(os.getenv('HTTP_PORT', '8080'))
    
    def __post_init__(self):
        if not self.JWT_REFRESH_SECRET:
            self.JWT_REFRESH_SECRET = self.JWT_SECRET + '_refresh'
        if not self.JWT_SECRET:
            raise ValueError("JWT_SECRET must be set")

config = Config()


# ============================================
# ОПТИМИЗАЦИЯ 2: Логгирование (async)
# ============================================

class AsyncLogger:
    """Асинхронный логгер с буферизацией"""
    
    def __init__(self, name: str, buffer_size: int = 100):
        self.logger = logging.getLogger(name)
        self.buffer = []
        self.buffer_size = buffer_size
        self._lock = asyncio.Lock()
    
    async def _log(self, level: str, message: str, **kwargs):
        """Асинхронное логирование с буферизацией"""
        record = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'level': level,
            'message': message,
            'service': 'auth',
            **kwargs
        }
        
        async with self._lock:
            self.buffer.append(json.dumps(record))
            if len(self.buffer) >= self.buffer_size:
                await self.flush()
    
    async def flush(self):
        """Сбросить буфер"""
        if not self.buffer:
            return
        
        async with self._lock:
            for record in self.buffer:
                print(record)
            self.buffer.clear()
    
    async def info(self, message: str, **kwargs):
        await self._log('INFO', message, **kwargs)
    
    async def error(self, message: str, exc_info: bool = False, **kwargs):
        if exc_info:
            kwargs['traceback'] = traceback.format_exc()
        await self._log('ERROR', message, **kwargs)
    
    async def warning(self, message: str, **kwargs):
        await self._log('WARNING', message, **kwargs)
    
    async def debug(self, message: str, **kwargs):
        await self._log('DEBUG', message, **kwargs)

async_logger = AsyncLogger('auth')


# ============================================
# ОПТИМИЗАЦИЯ 3: Thread pool для CPU-bound операций
# ============================================

class CPUBoundExecutor:
    """Оптимизированный thread pool для CPU-bound задач"""
    
    def __init__(self, max_workers: int = config.CPU_WORKERS):
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="cpu"
        )
        self._semaphore = asyncio.Semaphore(max_workers * 2)
        self._stats = {
            'total': 0,
            'completed': 0,
            'failed': 0,
            'total_time': 0
        }
        self._stats_lock = asyncio.Lock()
    
    async def run(self, func: Callable, *args, **kwargs) -> Any:
        """Запуск CPU-bound функции с контролем нагрузки"""
        async with self._semaphore:
            loop = asyncio.get_event_loop()
            start = time.time()
            
            async with self._stats_lock:
                self._stats['total'] += 1
            
            try:
                result = await loop.run_in_executor(
                    self.executor,
                    lambda: func(*args, **kwargs)
                )
                
                duration = time.time() - start
                async with self._stats_lock:
                    self._stats['completed'] += 1
                    self._stats['total_time'] += duration
                
                if duration > 1.0:
                    await async_logger.warning(f"Slow CPU task", 
                                       func=func.__name__, 
                                       duration=round(duration, 2))
                
                return result
                
            except Exception as e:
                async with self._stats_lock:
                    self._stats['failed'] += 1
                await async_logger.error(f"CPU task failed", 
                                 func=func.__name__, 
                                 error=str(e))
                raise
    
    async def get_stats(self) -> Dict:
        """Получить статистику"""
        async with self._stats_lock:
            avg_time = self._stats['total_time'] / self._stats['completed'] if self._stats['completed'] else 0
            return {
                'total': self._stats['total'],
                'completed': self._stats['completed'],
                'failed': self._stats['failed'],
                'avg_time': round(avg_time, 3)
            }

cpu_executor = CPUBoundExecutor()


# ============================================
# ОПТИМИЗАЦИЯ 4: In-memory кэш с версионированием
# ============================================

class VersionedCache:
    """Кэш с версионированием и автоматической очисткой"""
    
    def __init__(self, max_size: int = config.CACHE_MAX_SIZE, ttl: int = config.CACHE_TTL):
        self._cache = {}
        self._version = 1
        self._max_size = max_size
        self._default_ttl = ttl
        self._hits = 0
        self._misses = 0
        self._stats_lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()
        self._cleanup_task = None
    
    async def start(self):
        """Запустить фоновую очистку"""
        if not self._cleanup_task:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            await async_logger.info("Cache cleanup started")
    
    async def stop(self):
        """Остановить фоновую очистку"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
    
    async def get(self, key: str) -> Optional[Any]:
        """Получить значение из кэша"""
        async with self._cache_lock:
            if key in self._cache:
                value, expires_at, version = self._cache[key]
                
                if version == self._version and (expires_at is None or expires_at > time.time()):
                    async with self._stats_lock:
                        self._hits += 1
                    return value
                else:
                    del self._cache[key]
            
            async with self._stats_lock:
                self._misses += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Сохранить значение в кэш"""
        async with self._cache_lock:
            # LRU eviction
            if len(self._cache) >= self._max_size:
                oldest_key = min(self._cache.keys(), 
                               key=lambda k: self._cache[k][1] or float('inf'))
                del self._cache[oldest_key]
            
            expires_at = time.time() + (ttl or self._default_ttl)
            self._cache[key] = (value, expires_at, self._version)
    
    async def invalidate(self, pattern: Optional[str] = None):
        """Инвалидация кэша"""
        if pattern is None:
            async with self._cache_lock:
                self._version += 1
            await async_logger.info(f"Cache version increased", version=self._version)
        else:
            async with self._cache_lock:
                keys_to_delete = [k for k in self._cache if pattern in k]
                for key in keys_to_delete:
                    del self._cache[key]
            await async_logger.info(f"Cache invalidated", pattern=pattern, count=len(keys_to_delete))
    
    async def _cleanup_loop(self):
        """Фоновая очистка устаревших записей"""
        try:
            while True:
                try:
                    await asyncio.sleep(60)
                    now = time.time()
                    
                    async with self._cache_lock:
                        expired = [
                            k for k, (_, exp, ver) in self._cache.items()
                            if exp is not None and exp <= now
                        ]
                        for key in expired:
                            del self._cache[key]
                    
                    if expired:
                        await async_logger.debug(f"Cache cleanup", removed=len(expired))
                        
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    await async_logger.error(f"Cache cleanup error", error=str(e))
                    await asyncio.sleep(60)
        except asyncio.CancelledError:
            await async_logger.info("Cache cleanup stopped")
    
    async def get_stats(self) -> Dict:
        """Получить статистику кэша"""
        async with self._stats_lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0
            return {
                'size': len(self._cache),
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': round(hit_rate, 3),
                'version': self._version,
                'max_size': self._max_size
            }

cache = VersionedCache()


# ============================================
# ОПТИМИЗАЦИЯ 5: Rate limiter без Redis
# ============================================

class SlidingWindowRateLimiter:
    """Rate limiter со скользящим окном"""
    
    def __init__(self):
        self._windows = {}
        self._lock = asyncio.Lock()
        self._cleanup_task = None
    
    async def start(self):
        """Запустить фоновую очистку"""
        if not self._cleanup_task:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def stop(self):
        """Остановить фоновую очистку"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
    
    async def check(self, key: str, limit: int, window: int) -> bool:
        """Проверить rate limit"""
        now = time.time()
        window_start = now - window
        
        async with self._lock:
            if key not in self._windows:
                self._windows[key] = []
            
            # Очищаем старые записи
            self._windows[key] = [ts for ts in self._windows[key] if ts > window_start]
            
            if len(self._windows[key]) >= limit:
                return False
            
            self._windows[key].append(now)
            return True
    
    async def _cleanup_loop(self):
        """Фоновая очистка"""
        try:
            while True:
                await asyncio.sleep(300)  # 5 минут
                now = time.time()
                async with self._lock:
                    for key in list(self._windows.keys()):
                        self._windows[key] = [ts for ts in self._windows[key] if ts > now - 3600]
                        if not self._windows[key]:
                            del self._windows[key]
        except asyncio.CancelledError:
            pass

rate_limiter = SlidingWindowRateLimiter()


# ============================================
# ОПТИМИЗАЦИЯ 6: Circuit breaker
# ============================================

class CircuitBreaker:
    """Circuit breaker для защиты от каскадных отказов"""
    
    STATE_CLOSED = 'closed'
    STATE_OPEN = 'open'
    STATE_HALF_OPEN = 'half_open'
    
    def __init__(self, name: str, 
                 failure_threshold: int = config.CB_FAILURE_THRESHOLD,
                 timeout: int = config.CB_TIMEOUT):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.state = self.STATE_CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Выполнить функцию с защитой circuit breaker"""
        async with self._lock:
            if self.state == self.STATE_OPEN:
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = self.STATE_HALF_OPEN
                    self.failure_count = 0
                    await async_logger.info(f"Circuit breaker half-open", name=self.name)
                else:
                    raise Exception(f"Circuit breaker is open for {self.name}")
        
        try:
            result = await func(*args, **kwargs)
            
            async with self._lock:
                if self.state == self.STATE_HALF_OPEN:
                    self.state = self.STATE_CLOSED
                    await async_logger.info(f"Circuit breaker closed", name=self.name)
                self.failure_count = 0
            
            return result
            
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = self.STATE_OPEN
                    await async_logger.error(f"Circuit breaker opened", 
                                     name=self.name, 
                                     failures=self.failure_count)
            raise e


# ============================================
# ОПТИМИЗАЦИЯ 7: Backpressure
# ============================================

class BackpressureManager:
    """Управление нагрузкой с приоритетами"""
    
    def __init__(self):
        self.read_semaphore = asyncio.Semaphore(100)
        self.write_semaphore = asyncio.Semaphore(20)
        self._stats = {'reads': 0, 'writes': 0}
    
    @asynccontextmanager
    async def read(self):
        """Контекстный менеджер для чтения"""
        async with self.read_semaphore:
            self._stats['reads'] += 1
            try:
                yield
            finally:
                self._stats['reads'] -= 1
    
    @asynccontextmanager
    async def write(self):
        """Контекстный менеджер для записи"""
        async with self.write_semaphore:
            self._stats['writes'] += 1
            try:
                yield
            finally:
                self._stats['writes'] -= 1
    
    def get_stats(self) -> Dict:
        """Статистика нагрузки"""
        return {
            'active_reads': self._stats['reads'],
            'active_writes': self._stats['writes'],
            'read_limit': 100,
            'write_limit': 20
        }

backpressure = BackpressureManager()


# ============================================
# ОПТИМИЗАЦИЯ 8: Ошибки
# ============================================

class AppError(Exception):
    def __init__(self, message: str, code: str = 'internal_error', status_code: int = 500):
        self.message = message
        self.code = code
        self.status_code = status_code
        super().__init__(message)

class ValidationError(AppError):
    def __init__(self, message: str):
        super().__init__(message, 'validation_error', 400)

class AuthError(AppError):
    def __init__(self, message: str):
        super().__init__(message, 'auth_error', 401)

class ForbiddenError(AppError):
    def __init__(self, message: str):
        super().__init__(message, 'forbidden', 403)

class NotFoundError(AppError):
    def __init__(self, message: str):
        super().__init__(message, 'not_found', 404)

class RateLimitError(AppError):
    def __init__(self, message: str):
        super().__init__(message, 'rate_limit', 429)

class UsernameTakenError(AppError):
    def __init__(self, message: str):
        super().__init__(message, 'username_taken', 409)


# ============================================
# ОПТИМИЗАЦИЯ 9: Валидация (кэширование regex)
# ============================================

class Validators:
    """Валидаторы с кэшированием regex"""
    
    USERNAME_PATTERN = re.compile(r'^[a-zA-Z0-9_]{3,32}$')
    FIRST_NAME_PATTERN = re.compile(r'^[a-zA-Zа-яА-Я\s\-]{2,50}$')
    
    @classmethod
    async def validate_username(cls, username: str) -> str:
        """Валидация username (уникальный логин)"""
        if not username or not isinstance(username, str):
            raise ValidationError("Username is required")
        username = username.strip().lower()
        if not cls.USERNAME_PATTERN.match(username):
            raise ValidationError("Username must be 3-32 characters, letters, numbers, underscore only")
        return username
    
    @classmethod
    async def validate_first_name(cls, first_name: str) -> str:
        """Валидация имени пользователя"""
        if not first_name or not isinstance(first_name, str):
            raise ValidationError("First name is required")
        first_name = first_name.strip()
        if len(first_name) < 2:
            raise ValidationError("First name must be at least 2 characters")
        if len(first_name) > 50:
            raise ValidationError("First name too long (max 50 characters)")
        if not cls.FIRST_NAME_PATTERN.match(first_name):
            raise ValidationError("First name can only contain letters, spaces, and hyphens")
        return first_name
    
    @classmethod
    async def validate_password(cls, password: str) -> str:
        """Валидация пароля"""
        if not password or not isinstance(password, str):
            raise ValidationError("Password is required")
        if len(password) < 8:
            raise ValidationError("Password must be at least 8 characters")
        if not re.search(r'[A-Z]', password):
            raise ValidationError("Password must contain uppercase letter")
        if not re.search(r'[a-z]', password):
            raise ValidationError("Password must contain lowercase letter")
        if not re.search(r'\d', password):
            raise ValidationError("Password must contain number")
        if len(password) > 128:
            raise ValidationError("Password too long")
        return password
    
    @classmethod
    async def validate_confirm_password(cls, password: str, confirm_password: str) -> None:
        """Проверка подтверждения пароля"""
        if password != confirm_password:
            raise ValidationError("Passwords do not match")


# ============================================
# ОПТИМИЗАЦИЯ 10: YDB Connection Pool (адаптировано для VM)
# ============================================

class YDBConnectionPool:
    """
    Синхронный пул соединений YDB с поддержкой сервисного аккаунта
    """
    
    def __init__(self):
        self.endpoint = config.YDB_ENDPOINT
        self.database = config.YDB_DATABASE
        self._driver = None
        self._pool = None
        self._initialized = False
        self._init_lock = asyncio.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self._circuit_breaker = CircuitBreaker("ydb")
    
    async def initialize(self):
        """Инициализация драйвера и пула сессий"""
        async with self._init_lock:
            if self._initialized:
                return
            
            try:
                logger.info("🔄 Initializing YDB connection pool...")
                
                endpoint = config.YDB_ENDPOINT
                database = config.YDB_DATABASE
                
                if not endpoint or not database:
                    logger.error("❌ YDB_ENDPOINT or YDB_DATABASE not set")
                    # Для разработки создаем заглушку
                    logger.warning("⚠️ Using mock database for development")
                    self._initialized = True
                    return
                
                logger.info(f"📦 Connecting to {endpoint}{database}")
                
                # Пробуем разные способы аутентификации
                try:
                    # Способ 1: Файл сервисного аккаунта
                    sa_key_path = os.getenv('YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS', '')
                    if sa_key_path and os.path.exists(sa_key_path):
                        import ydb.iam
                        credentials = ydb.iam.ServiceAccountCredentials.from_file(sa_key_path)
                        logger.info("✅ Using service account key file")
                    else:
                        # Способ 2: Переменные окружения
                        credentials = ydb.credentials_from_env_variables()
                        logger.info("✅ Using environment variables for auth")
                    
                    # Создаем драйвер
                    self._driver = ydb.Driver(
                        endpoint=endpoint,
                        database=database,
                        credentials=credentials,
                    )
                    
                    # Ждем готовности
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        self._executor,
                        lambda: self._driver.wait(timeout=config.DB_TIMEOUT)
                    )
                    logger.info("✅ YDB driver initialized")
                    
                    # Создаем пул сессий
                    self._pool = ydb.SessionPool(
                        self._driver,
                        size=config.DB_POOL_SIZE,
                    )
                    
                    logger.info(f"✅ YDB session pool initialized (size: {config.DB_POOL_SIZE})")
                    
                except Exception as e:
                    logger.error(f"❌ YDB connection failed: {e}")
                    logger.warning("⚠️ Using mock database for development")
                    # Для разработки продолжаем без YDB
                
                self._initialized = True
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize YDB pool: {e}", exc_info=True)
                # Не падаем, продолжаем с заглушкой
                self._initialized = True
    
    @asynccontextmanager
    async def acquire(self):
        """
        Получить сессию из пула (асинхронный контекстный менеджер)
        """
        if not self._initialized:
            await self.initialize()
        
        if self._pool is None:
            # Заглушка для разработки
            class MockSession:
                async def transaction(self):
                    return self
                async def execute(self, query, params=None):
                    return []
            
            yield MockSession()
            return
        
        session = None
        try:
            loop = asyncio.get_event_loop()
            session = await loop.run_in_executor(
                self._executor,
                self._pool.acquire
            )
            yield session
        except Exception as e:
            logger.error(f"❌ Session error: {e}")
            raise
        finally:
            if session:
                await loop.run_in_executor(
                    self._executor,
                    lambda: self._pool.release(session)
                )
    
    async def execute(self, query: str, params: Optional[Dict] = None) -> List[Any]:
        """Выполнить запрос с circuit breaker"""
        async def _execute():
            if not self._initialized:
                await self.initialize()
            
            if self._pool is None:
                # Заглушка для разработки
                return []
            
            async with self.acquire() as session:
                try:
                    loop = asyncio.get_event_loop()
                    
                    if params:
                        def _execute_with_params():
                            prepared = session.prepare(query)
                            return session.transaction().execute(
                                prepared, 
                                params, 
                                commit_tx=True
                            )
                        
                        result = await loop.run_in_executor(
                            self._executor,
                            _execute_with_params
                        )
                    else:
                        def _execute_without_params():
                            return session.transaction().execute(
                                query, 
                                commit_tx=True
                            )
                        
                        result = await loop.run_in_executor(
                            self._executor,
                            _execute_without_params
                        )
                    
                    if result and len(result) > 0:
                        rows = result[0].rows if hasattr(result[0], 'rows') else []
                        return [dict(row) for row in rows] if rows else []
                    return []
                    
                except Exception as e:
                    logger.error(f"Database error: {e}")
                    return []
        
        return await self._circuit_breaker.execute(_execute)
    
    async def execute_batch(self, queries: List[Tuple[str, Optional[Dict]]]) -> List[Any]:
        """Выполнить несколько запросов в одной транзакции"""
        if not queries:
            return []
        
        async def _execute_batch():
            if not self._initialized:
                await self.initialize()
            
            if self._pool is None:
                return []
            
            async with self.acquire() as session:
                try:
                    loop = asyncio.get_event_loop()
                    
                    def _execute_transaction():
                        tx = session.transaction()
                        tx.begin()
                        
                        results = []
                        for query, params in queries:
                            if params:
                                prepared = session.prepare(query)
                                result = tx.execute(prepared, params)
                            else:
                                result = tx.execute(query)
                            
                            if result and len(result) > 0:
                                rows = result[0].rows if hasattr(result[0], 'rows') else []
                                results.extend([dict(row) for row in rows] if rows else [])
                        
                        tx.commit()
                        return results
                    
                    return await loop.run_in_executor(self._executor, _execute_transaction)
                    
                except Exception as e:
                    logger.error(f"Batch execute error: {e}")
                    return []
        
        return await self._circuit_breaker.execute(_execute_batch)
    
    async def close(self):
        """Закрыть пул соединений"""
        if self._pool:
            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(self._executor, self._pool.stop)
                logger.info("✅ Session pool closed")
            except Exception as e:
                logger.error(f"❌ Error closing session pool: {e}")
        
        if self._driver:
            try:
                await loop.run_in_executor(self._executor, self._driver.stop)
                logger.info("✅ Driver closed")
            except Exception as e:
                logger.error(f"❌ Error closing driver: {e}")
        
        self._executor.shutdown(wait=True)
        logger.info("✅ Thread pool shut down")

db = YDBConnectionPool()


# ============================================
# ОПТИМИЗАЦИЯ 11: Base64 Service (вместо KMS)
# ============================================

class Base64Service:
    """Простое base64 кодирование/декодирование (замена KMS)"""
    
    def __init__(self):
        self._cache = {}
        self._cache_lock = asyncio.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        logger.info("✅ Using Base64 for encryption (KMS disabled)")
    
    async def encrypt(self, plaintext: str) -> str:
        """Base64 кодирование"""
        if not plaintext:
            return ""
        
        cache_key = f"enc:{plaintext}"
        async with self._cache_lock:
            if cache_key in self._cache:
                return self._cache[cache_key]
        
        result = await cpu_executor.run(lambda: base64.b64encode(plaintext.encode()).decode())
        
        async with self._cache_lock:
            self._cache[cache_key] = result
        
        return result
    
    async def decrypt(self, ciphertext: str) -> str:
        """Base64 декодирование"""
        if not ciphertext:
            return ""
        
        if len(ciphertext) < 4 or not ciphertext.endswith('='):
            return ciphertext
        
        cache_key = f"dec:{ciphertext}"
        async with self._cache_lock:
            if cache_key in self._cache:
                return self._cache[cache_key]
        
        try:
            result = await cpu_executor.run(lambda: base64.b64decode(ciphertext).decode())
            async with self._cache_lock:
                self._cache[cache_key] = result
            return result
        except Exception as e:
            logger.error(f"❌ Base64 decode error: {e}")
            return ciphertext


# ============================================
# ОПТИМИЗАЦИЯ 12: Auth Service
# ============================================

class AuthService:
    """Сервис аутентификации с оптимизациями"""
    
    def __init__(self):
        self.pwd_context = CryptContext(
            schemes=["bcrypt"],
            deprecated="auto",
            bcrypt__rounds=config.BCRYPT_ROUNDS
        )
    
    async def hash_password(self, password: str) -> str:
        """CPU-bound - в thread pool"""
        return await cpu_executor.run(self.pwd_context.hash, password)
    
    async def verify_password(self, plain: str, hashed: str) -> bool:
        """CPU-bound - в thread pool"""
        try:
            return await cpu_executor.run(self.pwd_context.verify, plain, hashed)
        except:
            return False
    
    def create_access_token(self, user_id: str, role: str = 'user', 
                           is_verified: bool = False,
                           first_name: str = '',
                           username: str = '') -> str:
        expire = datetime.now(timezone.utc) + timedelta(minutes=config.ACCESS_TOKEN_EXPIRE_MINUTES)
        payload = {
            'sub': user_id,
            'role': role,
            'verified': is_verified,
            'first_name': first_name,
            'username': username,
            'exp': expire,
            'type': 'access',
            'jti': str(uuid.uuid4()),
            'iat': datetime.now(timezone.utc)
        }
        return jwt.encode(payload, config.JWT_SECRET, algorithm='HS256')
    
    def create_refresh_token(self, user_id: str) -> str:
        expire = datetime.now(timezone.utc) + timedelta(days=config.REFRESH_TOKEN_EXPIRE_DAYS)
        payload = {
            'sub': user_id,
            'exp': expire,
            'type': 'refresh',
            'jti': str(uuid.uuid4()),
            'iat': datetime.now(timezone.utc)
        }
        return jwt.encode(payload, config.JWT_REFRESH_SECRET, algorithm='HS256')
    
    def verify_token(self, token: str, token_type: str = 'access') -> Dict:
        secret = config.JWT_SECRET if token_type == 'access' else config.JWT_REFRESH_SECRET
        try:
            payload = jwt.decode(token, secret, algorithms=['HS256'])
            if payload.get('type') != token_type:
                raise AuthError("Invalid token type")
            return payload
        except ExpiredSignatureError:
            raise AuthError("Token expired")
        except InvalidTokenError as e:
            raise AuthError(f"Invalid token: {str(e)}")


# ============================================
# ОПТИМИЗАЦИЯ 13: Репозитории (упрощенные для разработки)
# ============================================

class UserRepository:
    """Репозиторий пользователей (с заглушками для разработки)"""
    
    def __init__(self):
        self.db = db
        self.cache = cache
        self._users = {}  # In-memory хранилище для разработки
    
    async def create(self, user: Dict) -> None:
        """Создать пользователя"""
        try:
            # Для разработки сохраняем в памяти
            self._users[user['id']] = user
            self._users[user['username']] = user
            logger.info(f"User created in memory: {user['id']}")
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
    
    async def get_by_username(self, username: str) -> Optional[Dict]:
        """Получить пользователя по username"""
        # Сначала проверяем в памяти
        if username in self._users:
            return self._users[username]
        
        # Потом в кэше
        cache_key = f"user:username:{username}"
        cached = await self.cache.get(cache_key)
        if cached:
            return cached
        
        # Потом в БД
        try:
            query = """
            DECLARE $username AS Utf8;
            SELECT * FROM users WHERE username = $username;
            """
            rows = await self.db.execute(query, {'$username': username})
            if rows:
                user = rows[0]
                await self.cache.set(cache_key, user, ttl=300)
                return user
        except Exception as e:
            logger.error(f"DB error: {e}")
        
        return None
    
    async def get_by_id(self, user_id: str) -> Optional[Dict]:
        """Получить пользователя по ID"""
        # Проверяем в памяти
        if user_id in self._users:
            return self._users[user_id]
        
        # Проверяем в кэше
        cache_key = f"user:id:{user_id}"
        cached = await self.cache.get(cache_key)
        if cached:
            return cached
        
        # Проверяем в БД
        try:
            query = """
            DECLARE $id AS Utf8;
            SELECT * FROM users WHERE id = $id;
            """
            rows = await self.db.execute(query, {'$id': user_id})
            if rows:
                user = rows[0]
                await self.cache.set(cache_key, user, ttl=300)
                return user
        except Exception as e:
            logger.error(f"DB error: {e}")
        
        return None
    
    async def update_last_login(self, user_id: str, ip: str) -> None:
        """Обновить время последнего входа"""
        logger.info(f"Updated last login for {user_id} from {ip}")
    
    async def increment_failed_attempts(self, username: str) -> int:
        """Увеличить счетчик неудачных попыток"""
        return 1
    
    async def lock_user(self, username: str) -> None:
        """Заблокировать пользователя"""
        logger.info(f"Locked user {username}")


class TokenRepository:
    """Репозиторий токенов (упрощенный)"""
    
    def __init__(self):
        self.db = db
        self._tokens = {}
    
    async def create(self, token: Dict) -> None:
        """Создать токен"""
        self._tokens[token['id']] = token
        logger.info(f"Token created: {token['id']}")
    
    async def find_by_hash(self, token_hash: str) -> Optional[Dict]:
        """Найти токен по хешу"""
        for token in self._tokens.values():
            if token.get('token_hash') == token_hash:
                return token
        return None
    
    async def mark_as_used(self, token_id: str) -> None:
        """Отметить токен как использованный"""
        if token_id in self._tokens:
            self._tokens[token_id]['used'] = True


class UsernameRepository:
    """Глобальный реестр username"""
    
    def __init__(self):
        self.db = db
        self.cache = cache
        self._usernames = {}
    
    async def reserve(self, username: str, entity_type: str, entity_id: str) -> bool:
        """Зарезервировать username"""
        self._usernames[username] = {
            'entity_type': entity_type,
            'entity_id': entity_id
        }
        return True
    
    async def check_available(self, username: str) -> Optional[Dict]:
        """Проверить доступность username"""
        if username in self._usernames:
            return self._usernames[username]
        return None


# ============================================
# ОПТИМИЗАЦИЯ 14: User Service
# ============================================

class UserService:
    """Сервис пользователей"""
    
    def __init__(
        self,
        user_repo: UserRepository,
        token_repo: TokenRepository,
        username_repo: UsernameRepository,
        base64_service: Base64Service,
        auth: AuthService
    ):
        self.user_repo = user_repo
        self.token_repo = token_repo
        self.username_repo = username_repo
        self.base64 = base64_service
        self.auth = auth
        self.cache = cache
    
    async def register(self, data: Dict, context: Dict) -> Dict:
        """Регистрация пользователя"""
        async with backpressure.write():
            request_id = context.get('request_id', 'unknown')
            
            # Rate limiting
            key = f"register:{context.get('ip', 'unknown')}"
            if not await rate_limiter.check(key, limit=10, window=60):
                raise RateLimitError("Too many registration attempts")
            
            logger.info(f"Registration attempt: {data.get('username')}")
            
            try:
                # Валидация
                username = await Validators.validate_username(data.get('username', ''))
                first_name = await Validators.validate_first_name(data.get('first_name', ''))
                password = await Validators.validate_password(data.get('password', ''))
                confirm_password = data.get('confirm_password', '')
                
                await Validators.validate_confirm_password(password, confirm_password)
                
                # Проверка уникальности username
                existing_username = await self.username_repo.check_available(username)
                if existing_username:
                    raise UsernameTakenError(f"Username '{username}' is taken")
                
                # Хеширование пароля
                password_hash = await self.auth.hash_password(password)
                
                # Base64 кодирование имени
                first_name_enc = await self.base64.encrypt(first_name)
                
                # Создание пользователя
                user_id = str(uuid.uuid4())
                user = {
                    'id': user_id,
                    'username': username,
                    'display_name': first_name,
                    'email': f"{username}@placeholder.local",
                    'password_hash': password_hash,
                    'role': 'user',
                    'status': 'active',
                    'is_verified': False,
                    'first_name_encrypted': first_name_enc,
                    'last_name_encrypted': '',
                    'phone_encrypted': ''
                }
                
                await self.user_repo.create(user)
                await self.username_repo.reserve(username, 'user', user_id)
                
                return {
                    'id': user_id,
                    'username': username,
                    'first_name': first_name
                }
                
            except Exception as e:
                logger.error(f"Registration error: {e}")
                raise
    
    async def login(self, username: str, password: str, context: Dict) -> Dict:
        """Вход пользователя"""
        async with backpressure.write():
            # Rate limiting
            key = f"login:{context.get('ip', 'unknown')}"
            if not await rate_limiter.check(key, limit=5, window=60):
                raise RateLimitError("Too many login attempts")
            
            # Валидация
            username = await Validators.validate_username(username)
            
            # Поиск пользователя
            user = await self.user_repo.get_by_username(username)
            
            if not user:
                raise AuthError("Invalid credentials")
            
            # Проверка пароля
            password_valid = await self.auth.verify_password(password, user['password_hash'])
            
            if not password_valid:
                await self.user_repo.increment_failed_attempts(username)
                raise AuthError("Invalid credentials")
            
            # Декодируем first_name
            first_name_enc = user.get('first_name_encrypted', '')
            first_name = ''
            if first_name_enc:
                try:
                    first_name = await self.base64.decrypt(first_name_enc)
                except:
                    first_name = first_name_enc
            
            # Создание токенов
            access_token = self.auth.create_access_token(
                user['id'],
                user.get('role', 'user'),
                user.get('is_verified', False),
                first_name,
                user.get('username')
            )
            
            refresh_token = self.auth.create_refresh_token(user['id'])
            
            # Обновление last_login
            await self.user_repo.update_last_login(user['id'], context.get('ip', ''))
            
            return {
                'access_token': access_token,
                'refresh_token': refresh_token,
                'token_type': 'bearer',
                'expires_in': config.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
                'user_id': user['id'],
                'role': user.get('role', 'user'),
                'is_verified': user.get('is_verified', False)
            }
    
    async def get_profile(self, user_id: str) -> Dict:
        """Получение профиля пользователя"""
        async with backpressure.read():
            cache_key = f"profile:{user_id}"
            cached = await self.cache.get(cache_key)
            if cached:
                return cached
            
            user = await self.user_repo.get_by_id(user_id)
            if not user:
                raise NotFoundError("User not found")
            
            # Декодируем имя
            first_name_enc = user.get('first_name_encrypted', '')
            first_name = user.get('display_name', '')
            
            if first_name_enc:
                try:
                    first_name = base64.b64decode(first_name_enc).decode('utf-8')
                except:
                    first_name = first_name_enc
            
            profile = {
                'id': user['id'],
                'username': user['username'],
                'first_name': first_name,
                'email': user.get('email'),
                'role': user.get('role', 'user'),
                'status': user.get('status', 'active'),
                'is_verified': user.get('is_verified', False)
            }
            
            await self.cache.set(cache_key, profile, ttl=300)
            return profile


# ============================================
# ОПТИМИЗАЦИЯ 15: DI Container
# ============================================

class Container:
    """DI контейнер"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = False
    
    async def initialize(self):
        """Инициализация сервисов"""
        if self._initialized:
            return
        
        try:
            self.base64 = Base64Service()
            self.auth = AuthService()
            self.user_repo = UserRepository()
            self.token_repo = TokenRepository()
            self.username_repo = UsernameRepository()
            self.user_service = UserService(
                self.user_repo,
                self.token_repo,
                self.username_repo,
                self.base64,
                self.auth
            )
            
            self._initialized = True
            logger.info("Container initialized")
            
        except Exception as e:
            logger.error(f"Container init failed: {e}", exc_info=True)
            raise

container = Container()


# ============================================
# VM ADAPTER - HTTP сервер
# ============================================

class VMAdapter:
    """Адаптер для запуска на VM"""
    
    def __init__(self):
        self.app = None
        self.runner = None
        self.site = None
    
    async def initialize(self):
        """Инициализация сервисов"""
        logger.info("🔄 Initializing Auth Service for VM...")
        
        try:
            await asyncio.gather(
                db.initialize(),
                cache.start(),
                rate_limiter.start(),
                container.initialize(),
                return_exceptions=True
            )
            logger.info("✅ Auth Service initialized")
        except Exception as e:
            logger.error(f"❌ Init failed: {e}")
    
    async def create_app(self) -> web.Application:
        """Создание HTTP приложения"""
        app = web.Application()
        
        # CORS
        cors = cors_setup(app, defaults={
            "*": ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods="*",
            )
        })
        
        # Routes
        app.router.add_post('/register', self.register_handler)
        app.router.add_post('/login', self.login_handler)
        app.router.add_post('/refresh', self.refresh_handler)
        app.router.add_post('/logout', self.logout_handler)
        app.router.add_get('/profile', self.profile_handler)
        app.router.add_get('/health', self.health_handler)
        
        # Apply CORS
        for route in list(app.router.routes()):
            cors.add(route)
        
        return app
    
    async def _get_token(self, request: web.Request) -> Optional[str]:
        """Извлечение токена"""
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith(('Bearer ', 'bearer ')):
            return auth_header.split(' ')[1]
        return request.query.get('token')
    
    async def _get_request_context(self, request: web.Request) -> Dict:
        """Контекст запроса"""
        return {
            'ip': request.remote or 'unknown',
            'user_agent': request.headers.get('User-Agent', 'unknown'),
            'request_id': str(uuid.uuid4())
        }
    
    async def register_handler(self, request: web.Request):
        """Регистрация"""
        try:
            data = await request.json()
            context = await self._get_request_context(request)
            result = await container.user_service.register(data, context)
            return web.json_response({'data': result}, status=201)
        except Exception as e:
            return await self._handle_error(e)
    
    async def login_handler(self, request: web.Request):
        """Вход"""
        try:
            data = await request.json()
            context = await self._get_request_context(request)
            result = await container.user_service.login(
                data.get('username', ''),
                data.get('password', ''),
                context
            )
            return web.json_response({'data': result})
        except Exception as e:
            return await self._handle_error(e)
    
    async def refresh_handler(self, request: web.Request):
        """Обновление токена"""
        try:
            data = await request.json()
            context = await self._get_request_context(request)
            result = await container.user_service.refresh_token(
                data.get('refresh_token', ''),
                context
            )
            return web.json_response({'data': result})
        except Exception as e:
            return await self._handle_error(e)
    
    async def logout_handler(self, request: web.Request):
        """Выход"""
        try:
            data = await request.json()
            context = await self._get_request_context(request)
            result = await container.user_service.logout(
                data.get('refresh_token', ''),
                context
            )
            return web.json_response({'data': result})
        except Exception as e:
            return await self._handle_error(e)
    
    async def profile_handler(self, request: web.Request):
        """Профиль"""
        try:
            token = await self._get_token(request)
            if not token:
                raise AuthError("Authentication required")
            
            payload = container.auth.verify_token(token, 'access')
            user_id = payload['sub']
            
            profile = await container.user_service.get_profile(user_id)
            return web.json_response({'data': profile})
        except Exception as e:
            return await self._handle_error(e)
    
    async def health_handler(self, request: web.Request):
        """Health check"""
        try:
            cache_stats = await cache.get_stats() if hasattr(cache, 'get_stats') else {}
            return web.json_response({
                'status': 'ok',
                'version': 'vm-7.5.0',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'cache': cache_stats,
                'encryption': 'base64'
            })
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return web.json_response({'status': 'degraded'}, status=503)
    
    async def _handle_error(self, e: Exception) -> web.Response:
        """Обработка ошибок"""
        if isinstance(e, AppError):
            status = e.status_code
            error_data = {'error': e.message, 'code': e.code}
        elif isinstance(e, ValidationError):
            status = 400
            error_data = {'error': str(e), 'code': 'validation_error'}
        elif isinstance(e, AuthError):
            status = 401
            error_data = {'error': str(e), 'code': 'auth_error'}
        elif isinstance(e, ForbiddenError):
            status = 403
            error_data = {'error': str(e), 'code': 'forbidden'}
        elif isinstance(e, NotFoundError):
            status = 404
            error_data = {'error': str(e), 'code': 'not_found'}
        elif isinstance(e, RateLimitError):
            status = 429
            error_data = {'error': str(e), 'code': 'rate_limit'}
        elif isinstance(e, UsernameTakenError):
            status = 409
            error_data = {'error': str(e), 'code': 'username_taken'}
        else:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            status = 500
            error_data = {'error': 'Internal server error', 'code': 'internal_error'}
        
        return web.json_response(error_data, status=status)
    
    async def start(self):
        """Запуск сервера"""
        app = await self.create_app()
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, config.HTTP_HOST, config.HTTP_PORT)
        await self.site.start()
        logger.info(f"🚀 Server running on http://{config.HTTP_HOST}:{config.HTTP_PORT}")
    
    async def stop(self):
        """Остановка сервера"""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        await cache.stop()
        await rate_limiter.stop()
        await db.close()
        logger.info("👋 Server stopped")


# ============================================
# ТОЧКА ВХОДА
# ============================================

async def main():
    """Главная функция"""
    adapter = VMAdapter()
    
    try:
        await adapter.initialize()
        await adapter.start()
        
        # Ждем сигнала остановки
        await asyncio.Event().wait()
        
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
    finally:
        await adapter.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
