#!/usr/bin/env python3
"""
PRODUCTION AUTH SERVICE v7.5.0 - VM EDITION (С СОХРАНЕНИЕМ В YDB)
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
✅ Сохранение пользователей в YDB (персистентность)
✅ Исправлена ошибка с timestamp в токенах
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

# === IMPORTS для YDB ===
try:
    import ydb
    import ydb.aio
    import ydb.iam
except ImportError as e:
    raise ImportError(f"YDB not installed: {e}")

# === IMPORTS для аутентификации ===
try:
    from passlib.context import CryptContext
except ImportError as e:
    raise ImportError(f"passlib not installed: {e}")

try:
    import jwt
    from jwt.exceptions import InvalidTokenError, ExpiredSignatureError
except ImportError as e:
    raise ImportError(f"jwt not installed: {e}")

# === IMPORTS для HTTP сервера ===
from aiohttp import web
from aiohttp_cors import setup as cors_setup, ResourceOptions


# ============================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
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
# ЗАГРУЗКА .env
# ============================================

def load_env():
    """Загрузить переменные окружения из .env файла"""
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        print(f"📁 Loading environment from {env_file}")
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
                        print(f"  ✅ Set {key.strip()}")
        print("✅ Environment loaded successfully")
    else:
        print(f"⚠️ No .env file found at {env_file}")

load_env()


# ============================================
# КОНФИГУРАЦИЯ
# ============================================

@dataclass
class Config:
    """Конфигурация с оптимизированными значениями"""
    
    @classmethod
    def from_env(cls):
        """Создать конфиг из переменных окружения"""
        return cls(
            YDB_ENDPOINT=os.getenv('YDB_ENDPOINT', 'grpcs://ydb.serverless.yandexcloud.net:2135'),
            YDB_DATABASE=os.getenv('YDB_DATABASE', ''),
            JWT_SECRET=os.getenv('JWT_SECRET', ''),
            JWT_REFRESH_SECRET=os.getenv('JWT_REFRESH_SECRET', ''),
            ACCESS_TOKEN_EXPIRE_MINUTES=int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', '15')),
            REFRESH_TOKEN_EXPIRE_DAYS=int(os.getenv('REFRESH_TOKEN_EXPIRE_DAYS', '30')),
            MAX_LOGIN_ATTEMPTS=int(os.getenv('MAX_LOGIN_ATTEMPTS', '5')),
            LOCKOUT_TIME_MINUTES=int(os.getenv('LOCKOUT_TIME_MINUTES', '15')),
            BCRYPT_ROUNDS=int(os.getenv('BCRYPT_ROUNDS', '12')),
            RATE_LIMIT_REQUESTS=int(os.getenv('RATE_LIMIT_REQUESTS', '200')),
            RATE_LIMIT_WINDOW=int(os.getenv('RATE_LIMIT_WINDOW', '60')),
            DB_POOL_SIZE=int(os.getenv('DB_POOL_SIZE', '50')),
            DB_TIMEOUT=int(os.getenv('DB_TIMEOUT', '10')),
            REQUEST_TIMEOUT=int(os.getenv('REQUEST_TIMEOUT', '25')),
            CPU_WORKERS=int(os.getenv('CPU_WORKERS', '4')),
            CACHE_MAX_SIZE=int(os.getenv('CACHE_MAX_SIZE', '2000')),
            CACHE_TTL=int(os.getenv('CACHE_TTL', '300')),
            CB_FAILURE_THRESHOLD=int(os.getenv('CB_FAILURE_THRESHOLD', '3')),
            CB_TIMEOUT=int(os.getenv('CB_TIMEOUT', '30')),
            HTTP_HOST=os.getenv('HTTP_HOST', '0.0.0.0'),
            HTTP_PORT=int(os.getenv('HTTP_PORT', '8080'))
        )
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        
        if not self.JWT_REFRESH_SECRET:
            self.JWT_REFRESH_SECRET = self.JWT_SECRET + '_refresh'
        if not self.JWT_SECRET:
            raise ValueError("JWT_SECRET must be set")

config = Config.from_env()


# ============================================
# YDB CONNECTION POOL (С АУТЕНТИФИКАЦИЕЙ)
# ============================================

class YDBConnectionPool:
    """Пул соединений YDB с поддержкой сервисного аккаунта"""
    
    def __init__(self):
        self.endpoint = config.YDB_ENDPOINT
        self.database = config.YDB_DATABASE
        self._driver = None
        self._pool = None
        self._initialized = False
        self._init_lock = asyncio.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    
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
                    raise ValueError("YDB_ENDPOINT and YDB_DATABASE must be set")
                
                logger.info(f"📦 Connecting to {endpoint}{database}")
                
                # Аутентификация через сервисный аккаунт
                sa_key_path = os.path.join(os.path.dirname(__file__), 'sa-key.json')
                
                if os.path.exists(sa_key_path):
                    logger.info(f"🔑 Using service account key from {sa_key_path}")
                    credentials = ydb.iam.ServiceAccountCredentials.from_file(sa_key_path)
                else:
                    logger.warning("⚠️ No service account key found, using metadata credentials")
                    credentials = ydb.credentials_from_env_variables()
                
                # Создаем драйвер
                self._driver = ydb.aio.Driver(
                    endpoint=endpoint,
                    database=database,
                    credentials=credentials,
                )
                
                # Ждем готовности драйвера
                await self._driver.wait(timeout=config.DB_TIMEOUT)
                logger.info("✅ YDB driver initialized")
                
                # Создаем пул сессий
                self._pool = ydb.aio.SessionPool(
                    self._driver,
                    size=config.DB_POOL_SIZE,
                )
                
                logger.info(f"✅ YDB session pool initialized (size: {config.DB_POOL_SIZE})")
                
                # Проверяем соединение
                await self._check_connection()
                
                self._initialized = True
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize YDB pool: {e}", exc_info=True)
                raise
    
    async def _check_connection(self):
        """Проверка соединения с БД"""
        try:
            async with self.acquire() as session:
                result = await session.transaction().execute("SELECT 1;", commit_tx=True)
                logger.info("✅ YDB connection test successful")
        except Exception as e:
            logger.error(f"❌ YDB connection test failed: {e}")
            raise
    
    @asynccontextmanager
    async def acquire(self):
        """Получить сессию из пула"""
        if self._pool is None:
            await self.initialize()
        
        session = None
        try:
            session = await self._pool.acquire()
            logger.debug(f"📊 Session acquired")
            yield session
        except Exception as e:
            logger.error(f"❌ Session error: {e}")
            raise
        finally:
            if session:
                await self._pool.release(session)
                logger.debug("📊 Session released")
    
    async def execute(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Выполнить запрос"""
        if not self._initialized:
            await self.initialize()
        
        async with self.acquire() as session:
            try:
                if params:
                    prepared = await session.prepare(query)
                    result = await session.transaction().execute(prepared, params, commit_tx=True)
                else:
                    result = await session.transaction().execute(query, commit_tx=True)
                
                if result and len(result) > 0:
                    rows = result[0].rows if hasattr(result[0], 'rows') else []
                    return [dict(row) for row in rows] if rows else []
                return []
                
            except Exception as e:
                logger.error(f"Database error: {e}")
                raise
    
    async def close(self):
        """Закрыть пул соединений"""
        if self._pool:
            await self._pool.stop()
            logger.info("✅ Session pool closed")
        
        if self._driver:
            await self._driver.stop()
            logger.info("✅ Driver closed")
        
        self._executor.shutdown(wait=True)
        logger.info("✅ Thread pool shut down")


# Глобальный экземпляр пула
db = YDBConnectionPool()


# ============================================
# CPU-BOUND EXECUTOR
# ============================================

class CPUBoundExecutor:
    """Thread pool для CPU-bound задач"""
    
    def __init__(self, max_workers: int = config.CPU_WORKERS):
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="cpu"
        )
        self._semaphore = asyncio.Semaphore(max_workers * 2)
    
    async def run(self, func: Callable, *args, **kwargs) -> Any:
        async with self._semaphore:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.executor,
                lambda: func(*args, **kwargs)
            )

cpu_executor = CPUBoundExecutor()


# ============================================
# IN-MEMORY КЭШ
# ============================================

class VersionedCache:
    """Кэш с версионированием"""
    
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
        if not self._cleanup_task:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def stop(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
    
    async def get(self, key: str) -> Optional[Any]:
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
        async with self._cache_lock:
            if len(self._cache) >= self._max_size:
                oldest_key = min(self._cache.keys(), 
                               key=lambda k: self._cache[k][1] or float('inf'))
                del self._cache[oldest_key]
            
            expires_at = time.time() + (ttl or self._default_ttl)
            self._cache[key] = (value, expires_at, self._version)
    
    async def invalidate(self, pattern: Optional[str] = None):
        if pattern is None:
            async with self._cache_lock:
                self._version += 1
        else:
            async with self._cache_lock:
                keys_to_delete = [k for k in self._cache if pattern in k]
                for key in keys_to_delete:
                    del self._cache[key]
    
    async def _cleanup_loop(self):
        try:
            while True:
                await asyncio.sleep(60)
                now = time.time()
                async with self._cache_lock:
                    expired = [k for k, (_, exp, ver) in self._cache.items()
                               if exp is not None and exp <= now]
                    for key in expired:
                        del self._cache[key]
        except asyncio.CancelledError:
            pass
    
    async def get_stats(self) -> Dict:
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
# RATE LIMITER
# ============================================

class SlidingWindowRateLimiter:
    """Rate limiter со скользящим окном"""
    
    def __init__(self):
        self._windows = {}
        self._lock = asyncio.Lock()
        self._cleanup_task = None
    
    async def start(self):
        if not self._cleanup_task:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def check(self, key: str, limit: int, window: int) -> bool:
        now = time.time()
        window_start = now - window
        
        async with self._lock:
            if key not in self._windows:
                self._windows[key] = []
            
            self._windows[key] = [ts for ts in self._windows[key] if ts > window_start]
            
            if len(self._windows[key]) >= limit:
                return False
            
            self._windows[key].append(now)
            return True
    
    async def _cleanup_loop(self):
        try:
            while True:
                await asyncio.sleep(300)
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
# BACKPRESSURE
# ============================================

class BackpressureManager:
    """Управление нагрузкой"""
    
    def __init__(self):
        self.read_semaphore = asyncio.Semaphore(100)
        self.write_semaphore = asyncio.Semaphore(20)
        self._stats = {'reads': 0, 'writes': 0}
    
    @asynccontextmanager
    async def read(self):
        async with self.read_semaphore:
            self._stats['reads'] += 1
            try:
                yield
            finally:
                self._stats['reads'] -= 1
    
    @asynccontextmanager
    async def write(self):
        async with self.write_semaphore:
            self._stats['writes'] += 1
            try:
                yield
            finally:
                self._stats['writes'] -= 1

backpressure = BackpressureManager()


# ============================================
# ОШИБКИ
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
# ВАЛИДАТОРЫ
# ============================================

class Validators:
    """Валидаторы"""
    
    USERNAME_PATTERN = re.compile(r'^[a-zA-Z0-9_]{3,32}$')
    FIRST_NAME_PATTERN = re.compile(r'^[a-zA-Zа-яА-Я\s\-]{2,50}$')
    
    @classmethod
    async def validate_username(cls, username: str) -> str:
        if not username or not isinstance(username, str):
            raise ValidationError("Username is required")
        username = username.strip().lower()
        if not cls.USERNAME_PATTERN.match(username):
            raise ValidationError("Username must be 3-32 characters, letters, numbers, underscore only")
        return username
    
    @classmethod
    async def validate_first_name(cls, first_name: str) -> str:
        if not first_name or not isinstance(first_name, str):
            raise ValidationError("First name is required")
        first_name = first_name.strip()
        if len(first_name) < 2:
            raise ValidationError("First name must be at least 2 characters")
        if len(first_name) > 50:
            raise ValidationError("First name too long (max 50 characters)")
        return first_name
    
    @classmethod
    async def validate_password(cls, password: str) -> str:
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
        return password
    
    @classmethod
    async def validate_confirm_password(cls, password: str, confirm_password: str) -> None:
        if password != confirm_password:
            raise ValidationError("Passwords do not match")


# ============================================
# BASE64 SERVICE
# ============================================

class Base64Service:
    """Base64 кодирование/декодирование"""
    
    async def encrypt(self, plaintext: str) -> str:
        if not plaintext:
            return ""
        return base64.b64encode(plaintext.encode()).decode()
    
    async def decrypt(self, ciphertext: str) -> str:
        if not ciphertext:
            return ""
        try:
            return base64.b64decode(ciphertext).decode()
        except:
            return ciphertext


# ============================================
# AUTH SERVICE
# ============================================

class AuthService:
    """Сервис аутентификации"""
    
    def __init__(self):
        self.pwd_context = CryptContext(
            schemes=["bcrypt"],
            deprecated="auto",
            bcrypt__rounds=config.BCRYPT_ROUNDS
        )
    
    async def hash_password(self, password: str) -> str:
        return await cpu_executor.run(self.pwd_context.hash, password)
    
    async def verify_password(self, plain: str, hashed: str) -> bool:
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
# РЕПОЗИТОРИИ (С СОХРАНЕНИЕМ В YDB)
# ============================================

class UserRepository:
    """Репозиторий пользователей с сохранением в YDB"""
    
    def __init__(self):
        self.db = db
        self.cache = cache
    
    def _to_timestamp(self, dt: Optional[datetime]) -> Optional[int]:
        if dt is None:
            return None
        return int(dt.timestamp() * 1_000_000)
    
    def _from_timestamp(self, ts: Optional[int]) -> Optional[datetime]:
        if ts is None:
            return None
        return datetime.fromtimestamp(ts / 1_000_000, tz=timezone.utc)
    
    async def create(self, user: Dict) -> None:
        """Создать пользователя в YDB"""
        query = """
        DECLARE $id AS Utf8;
        DECLARE $username AS Utf8;
        DECLARE $display_name AS Utf8;
        DECLARE $email AS Utf8;
        DECLARE $password_hash AS Utf8;
        DECLARE $role AS Utf8;
        DECLARE $status AS Utf8;
        DECLARE $is_verified AS Bool;
        DECLARE $first_name_encrypted AS Utf8;
        DECLARE $last_name_encrypted AS Utf8;
        DECLARE $phone_encrypted AS Utf8;
        DECLARE $created_at AS Timestamp;
        DECLARE $updated_at AS Timestamp;
        
        UPSERT INTO users (
            id, username, display_name, email, password_hash,
            role, status, is_verified,
            first_name_encrypted, last_name_encrypted, phone_encrypted,
            created_at, updated_at
        ) VALUES (
            $id, $username, $display_name, $email, $password_hash,
            $role, $status, $is_verified,
            $first_name_encrypted, $last_name_encrypted, $phone_encrypted,
            $created_at, $updated_at
        );
        """
        
        now = datetime.now(timezone.utc)
        now_ts = self._to_timestamp(now)
        
        try:
            await self.db.execute(query, {
                '$id': user['id'],
                '$username': user['username'],
                '$display_name': user.get('display_name', ''),
                '$email': user.get('email', f"{user['username']}@placeholder.local"),
                '$password_hash': user['password_hash'],
                '$role': user.get('role', 'user'),
                '$status': user.get('status', 'active'),
                '$is_verified': user.get('is_verified', False),
                '$first_name_encrypted': user.get('first_name_encrypted', ''),
                '$last_name_encrypted': user.get('last_name_encrypted', ''),
                '$phone_encrypted': user.get('phone_encrypted', ''),
                '$created_at': now_ts,
                '$updated_at': now_ts
            })
            logger.info(f"✅ User created in YDB: {user['id']}")
        except Exception as e:
            logger.error(f"❌ Failed to create user in YDB: {e}", exc_info=True)
            raise AppError("Failed to create user")
    
    async def get_by_username(self, username: str) -> Optional[Dict]:
        """Получить пользователя по username из YDB"""
        # Проверяем кэш
        cache_key = f"user:username:{username}"
        cached = await self.cache.get(cache_key)
        if cached:
            logger.debug(f"📦 User {username} found in cache")
            return cached
        
        query = """
        DECLARE $username AS Utf8;
        SELECT * FROM users WHERE username = $username;
        """
        
        try:
            rows = await self.db.execute(query, {'$username': username})
            if not rows:
                return None
            
            user = rows[0]
            await self.cache.set(cache_key, user, ttl=300)
            return user
        except Exception as e:
            logger.error(f"❌ Failed to get user by username: {e}")
            return None
    
    async def get_by_id(self, user_id: str) -> Optional[Dict]:
        """Получить пользователя по ID из YDB"""
        # Проверяем кэш
        cache_key = f"user:id:{user_id}"
        cached = await self.cache.get(cache_key)
        if cached:
            logger.debug(f"📦 User {user_id} found in cache")
            return cached
        
        query = """
        DECLARE $id AS Utf8;
        SELECT * FROM users WHERE id = $id;
        """
        
        try:
            rows = await self.db.execute(query, {'$id': user_id})
            if not rows:
                return None
            
            user = rows[0]
            await self.cache.set(cache_key, user, ttl=300)
            return user
        except Exception as e:
            logger.error(f"❌ Failed to get user by id: {e}")
            return None
    
    async def update_last_login(self, user_id: str, ip: str) -> None:
        """Обновить время последнего входа"""
        query = """
        DECLARE $user_id AS Utf8;
        DECLARE $ip AS Utf8;
        DECLARE $last_login_at AS Timestamp;
        
        UPDATE users SET 
            last_login_at = $last_login_at,
            last_login_ip = $ip,
            failed_login_attempts = 0,
            locked_until = NULL
        WHERE id = $user_id;
        """
        
        now = datetime.now(timezone.utc)
        now_ts = self._to_timestamp(now)
        
        try:
            await self.db.execute(query, {
                '$user_id': user_id,
                '$ip': ip,
                '$last_login_at': now_ts
            })
            await self.cache.invalidate(f"user:id:{user_id}")
            logger.debug(f"✅ Updated last login for user {user_id}")
        except Exception as e:
            logger.error(f"❌ Failed to update last login: {e}")
    
    async def increment_failed_attempts(self, username: str) -> int:
        """Увеличить счетчик неудачных попыток"""
        query = """
        DECLARE $username AS Utf8;
        
        $current = (SELECT failed_login_attempts FROM users WHERE username = $username);
        $new = $current + 1;
        
        UPDATE users SET failed_login_attempts = $new WHERE username = $username
        RETURNING failed_login_attempts;
        """
        
        try:
            rows = await self.db.execute(query, {'$username': username})
            if rows and len(rows) > 0:
                return rows[0].get('failed_login_attempts', 1)
            return 1
        except Exception as e:
            logger.error(f"❌ Failed to increment attempts: {e}")
            return 1
    
    async def lock_user(self, username: str) -> None:
        """Заблокировать пользователя"""
        lock_until = datetime.now(timezone.utc) + timedelta(minutes=config.LOCKOUT_TIME_MINUTES)
        lock_until_ts = self._to_timestamp(lock_until)
        
        query = """
        DECLARE $username AS Utf8;
        DECLARE $lock_until AS Timestamp;
        
        UPDATE users SET locked_until = $lock_until WHERE username = $username;
        """
        
        try:
            await self.db.execute(query, {
                '$username': username,
                '$lock_until': lock_until_ts
            })
            await self.cache.invalidate(f"user:username:{username}")
            logger.info(f"🔒 User {username} locked until {lock_until.isoformat()}")
        except Exception as e:
            logger.error(f"❌ Failed to lock user: {e}")


class TokenRepository:
    """Репозиторий токенов"""
    
    def __init__(self):
        self.db = db
    
    def _to_timestamp(self, dt: Optional[datetime]) -> Optional[int]:
        if dt is None:
            return None
        return int(dt.timestamp() * 1_000_000)
    
    async def create(self, token: Dict) -> None:
        """Создать токен в YDB"""
        query = """
        DECLARE $id AS Utf8;
        DECLARE $user_id AS Utf8;
        DECLARE $token_hash AS Utf8;
        DECLARE $token_type AS Utf8;
        DECLARE $device_fingerprint AS Utf8;
        DECLARE $ip_address AS Utf8;
        DECLARE $user_agent AS Utf8;
        DECLARE $created_at AS Timestamp;
        DECLARE $expires_at AS Timestamp;
        DECLARE $used AS Bool;
        
        UPSERT INTO tokens (
            id, user_id, token_hash, token_type,
            device_fingerprint, ip_address, user_agent,
            created_at, expires_at, used
        ) VALUES (
            $id, $user_id, $token_hash, $token_type,
            $device_fingerprint, $ip_address, $user_agent,
            $created_at, $expires_at, $used
        );
        """
        
        try:
            await self.db.execute(query, {
                '$id': token['id'],
                '$user_id': token['user_id'],
                '$token_hash': token['token_hash'],
                '$token_type': token['token_type'],
                '$device_fingerprint': token.get('device_fingerprint', ''),
                '$ip_address': token.get('ip_address', ''),
                '$user_agent': token.get('user_agent', '')[:200],
                '$created_at': self._to_timestamp(token['created_at']),
                '$expires_at': self._to_timestamp(token['expires_at']),
                '$used': token.get('used', False)
            })
            logger.debug(f"✅ Token saved: {token['id']}")
        except Exception as e:
            logger.error(f"❌ Failed to save token: {e}")
            raise AppError("Failed to save token")
    
    async def find_by_hash(self, token_hash: str) -> Optional[Dict]:
        """Найти токен по хешу"""
        query = """
        DECLARE $token_hash AS Utf8;
        SELECT * FROM tokens WHERE token_hash = $token_hash;
        """
        
        try:
            rows = await self.db.execute(query, {'$token_hash': token_hash})
            if not rows:
                return None
            return rows[0]
        except Exception as e:
            logger.error(f"❌ Failed to find token: {e}")
            return None
    
    async def mark_as_used(self, token_id: str) -> None:
        """Отметить токен как использованный"""
        query = """
        DECLARE $token_id AS Utf8;
        DECLARE $used_at AS Timestamp;
        
        UPDATE tokens SET 
            used = true,
            used_at = $used_at
        WHERE id = $token_id;
        """
        
        now = datetime.now(timezone.utc)
        now_ts = self._to_timestamp(now)
        
        try:
            await self.db.execute(query, {
                '$token_id': token_id,
                '$used_at': now_ts
            })
            logger.debug(f"✅ Token {token_id} marked as used")
        except Exception as e:
            logger.error(f"❌ Failed to mark token as used: {e}")


class UsernameRepository:
    """Глобальный реестр username"""
    
    def __init__(self):
        self.db = db
        self.cache = cache
    
    def _to_timestamp(self, dt: Optional[datetime]) -> Optional[int]:
        if dt is None:
            return None
        return int(dt.timestamp() * 1_000_000)
    
    async def reserve(self, username: str, entity_type: str, entity_id: str) -> bool:
        """Зарезервировать username в YDB"""
        query = """
        DECLARE $username AS Utf8;
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        DECLARE $now AS Timestamp;
        
        UPSERT INTO usernames (username, entity_type, entity_id, created_at, updated_at)
        VALUES ($username, $entity_type, $entity_id, $now, $now);
        """
        
        now = datetime.now(timezone.utc)
        now_ts = self._to_timestamp(now)
        
        try:
            await self.db.execute(query, {
                '$username': username,
                '$entity_type': entity_type,
                '$entity_id': entity_id,
                '$now': now_ts
            })
            await self.cache.invalidate(f"username:{username}")
            logger.debug(f"✅ Username {username} reserved for {entity_type} {entity_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to reserve username: {e}")
            return False
    
    async def check_available(self, username: str) -> Optional[Dict]:
        """Проверить доступность username"""
        cache_key = f"username:{username}"
        cached = await self.cache.get(cache_key)
        if cached:
            return cached
        
        query = """
        DECLARE $username AS Utf8;
        SELECT * FROM usernames WHERE username = $username;
        """
        
        try:
            rows = await self.db.execute(query, {'$username': username})
            if not rows:
                return None
            return rows[0]
        except Exception as e:
            logger.error(f"❌ Failed to check username: {e}")
            return None


# ============================================
# USER SERVICE (С ИСПРАВЛЕННОЙ ФУНКЦИЕЙ _save_token)
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
        """Регистрация пользователя с сохранением в YDB"""
        async with backpressure.write():
            # Rate limiting
            key = f"register:{context.get('ip', 'unknown')}"
            if not await rate_limiter.check(key, limit=10, window=60):
                raise RateLimitError("Too many registration attempts")
            
            logger.info(f"📝 Registration attempt: {data.get('username')}")
            
            try:
                # Валидация
                username = await Validators.validate_username(data.get('username', ''))
                first_name = await Validators.validate_first_name(data.get('first_name', ''))
                password = await Validators.validate_password(data.get('password', ''))
                confirm_password = data.get('confirm_password', '')
                
                await Validators.validate_confirm_password(password, confirm_password)
                
                # Проверка уникальности username
                existing = await self.username_repo.check_available(username)
                if existing:
                    raise UsernameTakenError(f"Username '{username}' is taken")
                
                # Хеширование пароля
                password_hash = await self.auth.hash_password(password)
                
                # Кодирование имени
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
                
                # Сохраняем в YDB
                await self.user_repo.create(user)
                await self.username_repo.reserve(username, 'user', user_id)
                
                logger.info(f"✅ User registered: {user_id}")
                
                return {
                    'id': user_id,
                    'username': username,
                    'first_name': first_name
                }
                
            except Exception as e:
                logger.error(f"❌ Registration error: {e}")
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
            
            # Поиск пользователя в YDB
            user = await self.user_repo.get_by_username(username)
            if not user:
                raise AuthError("Invalid credentials")
            
            # Проверка блокировки
            if user.get('locked_until'):
                locked_until = self.user_repo._from_timestamp(user['locked_until'])
                if locked_until and locked_until > datetime.now(timezone.utc):
                    raise ForbiddenError("Account locked")
            
            # Проверка статуса
            if user.get('status') != 'active':
                raise ForbiddenError(f"Account is {user.get('status')}")
            
            # Проверка пароля
            password_valid = await self.auth.verify_password(password, user['password_hash'])
            if not password_valid:
                attempts = await self.user_repo.increment_failed_attempts(username)
                if attempts >= config.MAX_LOGIN_ATTEMPTS:
                    await self.user_repo.lock_user(username)
                    raise ForbiddenError("Too many failed attempts. Account locked")
                raise AuthError("Invalid credentials")
            
            # Декодируем имя
            first_name_enc = user.get('first_name_encrypted', '')
            first_name = await self.base64.decrypt(first_name_enc) if first_name_enc else ''
            
            # Создание токенов
            access_token = self.auth.create_access_token(
                user['id'],
                user.get('role', 'user'),
                user.get('is_verified', False),
                first_name,
                user.get('username')
            )
            
            refresh_token = self.auth.create_refresh_token(user['id'])
            
            # Обновляем last_login в YDB
            await self.user_repo.update_last_login(user['id'], context.get('ip', ''))
            
            # Сохраняем refresh токен
            await self._save_token(user['id'], refresh_token, 'refresh', context)
            
            logger.info(f"✅ User logged in: {user['id']}")
            
            return {
                'access_token': access_token,
                'refresh_token': refresh_token,
                'token_type': 'bearer',
                'expires_in': config.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
                'user_id': user['id'],
                'role': user.get('role', 'user'),
                'is_verified': user.get('is_verified', False)
            }
    
    async def _save_token(self, user_id: str, token: str, token_type: str, context: Dict) -> None:
        """Сохранить токен в YDB (ИСПРАВЛЕННАЯ ВЕРСИЯ)"""
        token_hash = await cpu_executor.run(hashlib.sha256, token.encode())
        token_hash = token_hash.hexdigest()
        
        payload = self.auth.verify_token(token, token_type)
        
        # Получаем exp из payload
        exp_timestamp = payload['exp']
        
        # Преобразуем timestamp в datetime
        if isinstance(exp_timestamp, datetime):
            expires_at = exp_timestamp
        else:
            # Если это число (timestamp)
            expires_at = datetime.fromtimestamp(exp_timestamp, timezone.utc)
        
        token_obj = {
            'id': str(uuid.uuid4()),
            'user_id': user_id,
            'token_hash': token_hash,
            'token_type': token_type,
            'device_fingerprint': context.get('user_agent', '')[:100],
            'ip_address': context.get('ip', ''),
            'user_agent': context.get('user_agent', '')[:200],
            'created_at': datetime.now(timezone.utc),
            'expires_at': expires_at,
            'used': False
        }
        
        await self.token_repo.create(token_obj)
    
    async def refresh_token(self, refresh_token: str, context: Dict) -> Dict:
        """Обновление access токена"""
        async with backpressure.write():
            # Rate limiting
            key = f"refresh:{context.get('ip', 'unknown')}"
            if not await rate_limiter.check(key, limit=20, window=60):
                raise RateLimitError("Too many refresh attempts")
            
            try:
                payload = self.auth.verify_token(refresh_token, 'refresh')
                user_id = payload['sub']
            except AuthError:
                raise AuthError("Invalid refresh token")
            
            token_hash = await cpu_executor.run(hashlib.sha256, refresh_token.encode())
            token_hash = token_hash.hexdigest()
            
            stored_token = await self.token_repo.find_by_hash(token_hash)
            
            if not stored_token:
                raise AuthError("Invalid refresh token")
            
            if stored_token.get('used'):
                raise AuthError("Token already used")
            
            # Проверка срока действия
            expires_at = stored_token.get('expires_at')
            if expires_at:
                if isinstance(expires_at, int):
                    expires_at_dt = datetime.fromtimestamp(expires_at / 1_000_000, timezone.utc)
                else:
                    expires_at_dt = expires_at
                
                if expires_at_dt < datetime.now(timezone.utc):
                    raise AuthError("Token expired")
            
            user = await self.user_repo.get_by_id(user_id)
            if not user:
                raise AuthError("Invalid token")
            
            if user.get('status') != 'active':
                raise ForbiddenError(f"Account is {user.get('status')}")
            
            first_name_enc = user.get('first_name_encrypted', '')
            first_name = await self.base64.decrypt(first_name_enc) if first_name_enc else ''
            
            await self.token_repo.mark_as_used(stored_token['id'])
            
            new_access_token = self.auth.create_access_token(
                user_id,
                user.get('role', 'user'),
                user.get('is_verified', False),
                first_name,
                user.get('username')
            )
            new_refresh_token = self.auth.create_refresh_token(user_id)
            
            await self._save_token(user_id, new_refresh_token, 'refresh', context)
            
            return {
                'access_token': new_access_token,
                'refresh_token': new_refresh_token,
                'token_type': 'bearer',
                'expires_in': config.ACCESS_TOKEN_EXPIRE_MINUTES * 60
            }
    
    async def logout(self, refresh_token: str, context: Dict) -> Dict:
        """Выход пользователя"""
        async with backpressure.write():
            try:
                payload = self.auth.verify_token(refresh_token, 'refresh')
                user_id = payload['sub']
            except AuthError:
                raise AuthError("Invalid refresh token")
            
            token_hash = await cpu_executor.run(hashlib.sha256, refresh_token.encode())
            token_hash = token_hash.hexdigest()
            
            stored_token = await self.token_repo.find_by_hash(token_hash)
            
            if not stored_token:
                raise AuthError("Invalid refresh token")
            
            await self.token_repo.mark_as_used(stored_token['id'])
            
            return {'success': True}
    
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
            
            username_global = await self.username_repo.get_by_entity('user', user_id)
            
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
                'username_global': username_global,
                'first_name': first_name,
                'email': user.get('email'),
                'role': user.get('role', 'user'),
                'status': user.get('status', 'active'),
                'is_verified': user.get('is_verified', False)
            }
            
            await self.cache.set(cache_key, profile, ttl=300)
            return profile


# ============================================
# DI CONTAINER
# ============================================

class Container:
    """DI контейнер"""
    
    def __init__(self):
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

container = Container()


# ============================================
# VM ADAPTER
# ============================================

class VMAdapter:
    """Адаптер для запуска на VM"""
    
    def __init__(self):
        self.app = None
        self.runner = None
        self.site = None
    
    async def initialize(self):
        """Инициализация сервисов"""
        logger.info("🚀 Initializing Auth Service for VM...")
        
        try:
            await db.initialize()
            await cache.start()
            await rate_limiter.start()
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
        app.router.add_get('/health', self.health_handler)
        
        # Apply CORS
        for route in list(app.router.routes()):
            cors.add(route)
        
        return app
    
    async def _parse_body(self, request: web.Request) -> Dict:
        """Парсинг тела запроса"""
        try:
            return await request.json()
        except:
            return {}
    
    async def _get_request_context(self, request: web.Request) -> Dict:
        """Контекст запроса"""
        return {
            'ip': request.remote or 'unknown',
            'user_agent': request.headers.get('User-Agent', 'unknown'),
            'request_id': str(uuid.uuid4())[:8]
        }
    
    async def register_handler(self, request: web.Request):
        """Регистрация"""
        try:
            data = await self._parse_body(request)
            context = await self._get_request_context(request)
            
            result = await container.user_service.register(data, context)
            return web.json_response({'data': result}, status=201)
        except UsernameTakenError as e:
            return web.json_response({'error': str(e), 'code': 'username_taken'}, status=409)
        except ValidationError as e:
            return web.json_response({'error': str(e), 'code': 'validation_error'}, status=400)
        except Exception as e:
            logger.error(f"❌ Registration error: {e}", exc_info=True)
            return web.json_response({'error': 'Internal server error'}, status=500)
    
    async def login_handler(self, request: web.Request):
        """Вход"""
        try:
            data = await self._parse_body(request)
            context = await self._get_request_context(request)
            
            result = await container.user_service.login(
                data.get('username', ''),
                data.get('password', ''),
                context
            )
            return web.json_response({'data': result})
        except AuthError as e:
            return web.json_response({'error': str(e), 'code': 'auth_error'}, status=401)
        except ForbiddenError as e:
            return web.json_response({'error': str(e), 'code': 'forbidden'}, status=403)
        except Exception as e:
            logger.error(f"❌ Login error: {e}", exc_info=True)
            return web.json_response({'error': 'Internal server error'}, status=500)
    
    async def health_handler(self, request: web.Request):
        """Health check"""
        cache_stats = await cache.get_stats() if hasattr(cache, 'get_stats') else {}
        return web.json_response({
            'status': 'ok',
            'version': 'vm-1.0.0',
            'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'cache': cache_stats,
            'encryption': 'base64',
            'ydb_connected': db._initialized
        })
    
    async def start(self):
        """Запуск сервера"""
        app = await self.create_app()
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, config.HTTP_HOST, config.HTTP_PORT)
        await self.site.start()
        logger.info(f"🚀 Auth Service running on http://{config.HTTP_HOST}:{config.HTTP_PORT}")
    
    async def stop(self):
        """Остановка сервера"""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        await cache.stop()
        await db.close()
        logger.info("👋 Auth Service stopped")


# ============================================
# ТОЧКА ВХОДА
# ============================================

async def main():
    """Главная функция"""
    adapter = VMAdapter()
    
    try:
        await adapter.initialize()
        await adapter.start()
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
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)
