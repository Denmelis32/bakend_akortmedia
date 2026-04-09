# app.py
# Profile Service v5.0 (async YDB + session monitoring)

from dotenv import load_dotenv
load_dotenv(override=True)

import json
import os
import uuid
import re
import base64
import time
import hashlib
import bcrypt
import jwt
import logging
import traceback
import asyncio
import aiohttp
import concurrent.futures
from typing import Dict, Any, Optional, List, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator, EmailStr
import uvicorn

# === ASYNC YDB IMPORTS ===
try:
    import ydb.aio as ydb_aio
    YDB_ASYNC_AVAILABLE = True
except ImportError:
    ydb_aio = None
    YDB_ASYNC_AVAILABLE = False
    print("⚠️ ydb.aio not installed, async YDB unavailable")

# === S3 IMPORTS ===
try:
    import boto3
    from botocore.exceptions import ClientError
    S3_AVAILABLE = True
except ImportError:
    boto3 = None
    S3_AVAILABLE = False
    print("⚠️ boto3 not installed, S3 uploads disabled")

# === AIOHTTP ===
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    aiohttp = None
    AIOHTTP_AVAILABLE = False
    print("⚠️ aiohttp not installed, service calls disabled")

# ============================================
# НАСТРОЙКА ЛОГГЕРА
# ============================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================
# КОНФИГУРАЦИЯ (из переменных окружения)
# ============================================
@dataclass
class ProfileConfig:
    # YDB
    YDB_ENDPOINT: str = field(default_factory=lambda: os.environ.get('YDB_ENDPOINT', 'grpcs://ydb.serverless.yandexcloud.net:2135'))
    YDB_DATABASE: str = field(default_factory=lambda: os.environ.get('YDB_DATABASE', '/ru-central1/b1gck8tib5cffvt263ca/etnees03efof77aup3kt'))

    # JWT
    JWT_SECRET: str = field(default_factory=lambda: os.environ.get('JWT_SECRET', ''))
    JWT_ALGORITHM: str = "HS256"

    # S3
    S3_ENDPOINT: str = field(default_factory=lambda: os.environ.get('OBJECT_STORAGE_ENDPOINT', 'https://storage.yandexcloud.net'))
    S3_ACCESS_KEY: str = field(default_factory=lambda: os.environ.get('OBJECT_STORAGE_ACCESS_KEY', ''))
    S3_SECRET_KEY: str = field(default_factory=lambda: os.environ.get('OBJECT_STORAGE_SECRET_KEY', ''))
    S3_REGION: str = field(default_factory=lambda: os.environ.get('OBJECT_STORAGE_REGION', 'ru-central1'))
    S3_BUCKET: str = field(default_factory=lambda: os.environ.get('OBJECT_STORAGE_BUCKET', 'social-media-images'))
    S3_PUBLIC_URL: str = field(default_factory=lambda: os.environ.get('OBJECT_STORAGE_PUBLIC_URL', 'https://storage.yandexcloud.net/social-media-images'))

    # Feed Service API
    FEED_SERVICE_URL: str = field(default_factory=lambda: os.environ.get('FEED_SERVICE_URL', 'https://d5d8ck5m5sp3s32ve64i.a6hc9vya.apigw.yandexcloud.net'))
    FEED_SERVICE_TIMEOUT: int = 5

    # Chat Service API
    CHAT_SERVICE_URL: str = field(default_factory=lambda: os.environ.get('CHAT_SERVICE_URL', 'https://d5d8ck5m5sp3s32ve64i.a6hc9vya.apigw.yandexcloud.net'))
    CHAT_SERVICE_TIMEOUT: int = 5

    # Limits
    MAX_FIRST_NAME_LENGTH: int = 50
    MAX_LAST_NAME_LENGTH: int = 50
    MAX_USERNAME_LENGTH: int = 50
    MIN_USERNAME_LENGTH: int = 3
    MAX_BIO_LENGTH: int = 1000
    MAX_ABOUT_LENGTH: int = 2000
    MAX_SOCIAL_LINKS: int = 10
    MAX_AVATAR_SIZE_MB: int = 5
    MAX_COVER_SIZE_MB: int = 10
    ALLOWED_IMAGE_TYPES: List[str] = field(default_factory=lambda: ['jpeg', 'jpg', 'png', 'gif', 'webp'])

    # Cache
    CACHE_TTL_PROFILE: int = 300

    # Thread pool
    THREAD_POOL_WORKERS: int = 4

    # Backpressure
    MAX_CONCURRENT_REQUESTS: int = 50
    MAX_CONCURRENT_IMAGE_UPLOADS: int = 3

    # YDB pool settings
    YDB_POOL_SIZE: int = 20
    YDB_POOL_KEEP_ALIVE: int = 60
    YDB_RETRY_LIMIT: int = 3
    YDB_RETRY_BASE_DELAY: float = 0.5

    # Timeouts
    DB_TIMEOUT: int = 10
    S3_TIMEOUT: int = 15
    SERVICE_TIMEOUT: int = 5
    TOTAL_REQUEST_TIMEOUT: int = 25

profile_config = ProfileConfig()

# ============================================
# ОПТИМИЗАЦИЯ 1: Thread pool для CPU-bound операций
# ============================================
class CPUBoundExecutor:
    def __init__(self, max_workers=4):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="cpu_worker")
        self._semaphore = asyncio.Semaphore(8)
        self._stats = {'total_tasks': 0, 'completed_tasks': 0, 'failed_tasks': 0, 'total_time': 0}

    async def run(self, func, *args, **kwargs):
        async with self._semaphore:
            loop = asyncio.get_event_loop()
            start = time.time()
            self._stats['total_tasks'] += 1
            try:
                result = await asyncio.wait_for(loop.run_in_executor(self.executor, lambda: func(*args, **kwargs)), timeout=10.0)
                duration = time.time() - start
                self._stats['completed_tasks'] += 1
                self._stats['total_time'] += duration
                if duration > 1.0:
                    logger.warning(f"Slow CPU task: {func.__name__} took {duration:.2f}s")
                return result
            except asyncio.TimeoutError:
                self._stats['failed_tasks'] += 1
                logger.error(f"CPU task timeout after 10s: {func.__name__}")
                raise TimeoutError(f"CPU task {func.__name__} timed out")
            except Exception as e:
                self._stats['failed_tasks'] += 1
                logger.error(f"CPU task failed: {func.__name__} - {e}")
                raise

    def get_stats(self):
        avg_time = self._stats['total_time'] / self._stats['completed_tasks'] if self._stats['completed_tasks'] > 0 else 0
        return {'total_tasks': self._stats['total_tasks'], 'completed': self._stats['completed_tasks'], 'failed': self._stats['failed_tasks'], 'avg_time': round(avg_time, 3)}

cpu_executor = CPUBoundExecutor(max_workers=profile_config.THREAD_POOL_WORKERS)

# ============================================
# ОПТИМИЗАЦИЯ 2: Backpressure и rate limiting
# ============================================
class BackpressureManager:
    def __init__(self):
        self.read_semaphore = asyncio.Semaphore(profile_config.MAX_CONCURRENT_REQUESTS)
        self.write_semaphore = asyncio.Semaphore(profile_config.MAX_CONCURRENT_REQUESTS // 2)
        self.image_semaphore = asyncio.Semaphore(profile_config.MAX_CONCURRENT_IMAGE_UPLOADS)
        self.active_reads = 0
        self.active_writes = 0
        self.active_images = 0
        self.max_queue_size = 100
        self.request_queue = asyncio.Queue()

    @asynccontextmanager
    async def read_operation(self):
        if self.request_queue.qsize() > self.max_queue_size:
            raise Exception("Too many requests", 429)
        await self.read_semaphore.acquire()
        self.active_reads += 1
        try:
            yield
        finally:
            self.active_reads -= 1
            self.read_semaphore.release()

    @asynccontextmanager
    async def write_operation(self):
        """Контекстный менеджер для write операций"""
        if self.request_queue.qsize() > self.max_queue_size:
            raise Exception("Too many requests", 429)
        await self.write_semaphore.acquire()
        self.active_writes += 1
        try:
            yield
        finally:
            self.active_writes -= 1
            self.write_semaphore.release()

    @asynccontextmanager
    async def image_operation(self):
        await self.image_semaphore.acquire()
        self.active_images += 1
        try:
            yield
        finally:
            self.active_images -= 1
            self.image_semaphore.release()

    def get_load(self):
        return {
            'active_reads': self.active_reads,
            'active_writes': self.active_writes,
            'active_images': self.active_images,
            'queue_size': self.request_queue.qsize()
        }

backpressure = BackpressureManager()

# ============================================
# ОПТИМИЗАЦИЯ 3: In-memory кэш с версионированием
# ============================================
class VersionedCache:
    def __init__(self, max_size=1000, default_ttl=300):
        self._cache = {}
        self._version = 1
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._hits = 0
        self._misses = 0
        self._cleanup_task = None
        self._cleanup_started = False

    async def ensure_started(self):
        if not self._cleanup_started:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            self._cleanup_started = True
            logger.info("✅ Cache cleanup task started")

    async def stop(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_started = False

    async def get(self, key: str):
        if key in self._cache:
            value, expires_at, version = self._cache[key]
            if version == self._version and (expires_at is None or expires_at > time.time()):
                self._hits += 1
                return value
            else:
                del self._cache[key]
        self._misses += 1
        return None

    async def set(self, key: str, value: Any, ttl: int = None):
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][1] or float('inf'))
            del self._cache[oldest_key]
        expires_at = time.time() + (ttl or self._default_ttl)
        self._cache[key] = (value, expires_at, self._version)

    async def invalidate(self, pattern: str = None):
        if pattern is None:
            self._version += 1
            logger.info(f"Cache version increased to {self._version}")
        else:
            keys_to_delete = [k for k in self._cache if pattern in k]
            for key in keys_to_delete:
                del self._cache[key]
            logger.info(f"Invalidated {len(keys_to_delete)} keys with pattern {pattern}")

    async def _cleanup_loop(self):
        try:
            while True:
                try:
                    await asyncio.sleep(60)
                    now = time.time()
                    expired = [k for k, (_, exp, ver) in self._cache.items() if exp is not None and exp <= now]
                    for key in expired:
                        del self._cache[key]
                    if expired:
                        logger.debug(f"Cleaned up {len(expired)} expired cache entries")
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Cache cleanup error: {e}")
                    await asyncio.sleep(60)
        except asyncio.CancelledError:
            logger.info("Cache cleanup task cancelled")

    def get_stats(self):
        hit_rate = self._hits / (self._hits + self._misses) if (self._hits + self._misses) > 0 else 0
        return {'size': len(self._cache), 'hits': self._hits, 'misses': self._misses, 'hit_rate': round(hit_rate, 3), 'version': self._version}

profile_cache = VersionedCache(max_size=2000, default_ttl=300)

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (синхронные, но вызываются через executor)
# ============================================
def to_timestamp(dt: Optional[datetime]) -> Optional[int]:
    if dt is None: return None
    if dt.tzinfo is not None: dt = dt.replace(tzinfo=None)
    return int(dt.timestamp() * 1_000_000)

def from_timestamp(ts: Optional[int]) -> Optional[datetime]:
    if ts is None: return None
    return datetime.fromtimestamp(ts / 1_000_000)

def validate_uuid(uuid_str: str) -> bool:
    if not uuid_str: return False
    pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(pattern, uuid_str.lower()))

def safe_int(value: Any) -> Optional[int]:
    if value is None: return None
    try: return int(value)
    except (ValueError, TypeError): return None

def time_ago(dt: datetime) -> str:
    now = datetime.utcnow()
    diff = now - dt
    seconds = diff.total_seconds()
    if seconds < 60: return "только что"
    elif seconds < 3600: return f"{int(seconds / 60)} мин. назад"
    elif seconds < 86400: return f"{int(seconds / 3600)} ч. назад"
    elif seconds < 2592000: return f"{int(seconds / 86400)} дн. назад"
    else: return f"{int(seconds / 2592000)} мес. назад"

def safe_b64decode(data: Optional[str]) -> str:
    if not data: return ""
    try: return base64.b64decode(data).decode('utf-8')
    except: return data

def validate_email(email: str) -> bool:
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def validate_phone(phone: str) -> bool:
    if not phone: return True
    pattern = r'^[\d\s\-\+\(\)]{10,20}$'
    return bool(re.match(pattern, phone))

def validate_username(username: str) -> Tuple[bool, str]:
    if not username: return False, "Username is required"
    if len(username) < 3: return False, "Username must be at least 3 characters"
    if len(username) > 50: return False, "Username must be less than 50 characters"
    if not re.match(r'^[a-zA-Z0-9_]+$', username): return False, "Username can only contain letters, numbers, and underscore"
    return True, ""

def validate_date_of_birth(date_str: str) -> bool:
    if not date_str: return True
    try: datetime.strptime(date_str, '%Y-%m-%d'); return True
    except ValueError: return False

def validate_url(url: str) -> bool:
    if not url: return True
    pattern = r'^(https?:\/\/)?(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$'
    return bool(re.match(pattern, url))

# ============================================
# АСИНХРОННЫЙ YDB КЛИЕНТ С ПУЛОМ И МОНИТОРИНГОМ
# ============================================
class AsyncYDBConnection:
    _instance = None
    _driver = None
    _session_pool = None
    _pool_stats = {'total_sessions': 0, 'active_sessions': 0, 'idle_sessions': 0, 'acquire_wait_time': 0.0, 'acquire_count': 0}
    _stats_lock = asyncio.Lock()
    _monitor_task = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    async def initialize(self):
        """Async initialization method"""
        if self._initialized:
            return
        if not YDB_ASYNC_AVAILABLE:
            logger.error("YDB async driver not available")
            self._initialized = True
            return

        logger.info(f"Initializing async YDB driver: {profile_config.YDB_ENDPOINT}, db: {profile_config.YDB_DATABASE}")
        try:
            import ydb
            from ydb.iam import ServiceAccountCredentials
            
            sa_key_path = os.environ.get('YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS', '')
            if sa_key_path:
                credentials = ServiceAccountCredentials.from_file(sa_key_path)
            else:
                credentials = ydb.credentials_from_env_variables()

            self._driver = ydb_aio.Driver(
                endpoint=profile_config.YDB_ENDPOINT,
                database=profile_config.YDB_DATABASE,
                credentials=credentials,
                root_certificates=ydb.load_ydb_root_certificate() if 'ydb.serverless.yandexcloud.net' in profile_config.YDB_ENDPOINT else None
            )
            await self._driver.wait(fail_fast=True, timeout=15)

            # Создаём пул сессий без keep_alive_timeout
            self._session_pool = ydb_aio.SessionPool(
                self._driver,
                size=profile_config.YDB_POOL_SIZE
            )
            self._initialized = True
            logger.info("✅ Async YDB connection and session pool initialized")

            self._monitor_task = asyncio.create_task(self._monitor_pool())
        except Exception as e:
            logger.error(f"❌ Failed to initialize async YDB: {e}")
            self._initialized = True
            raise

    async def close(self):
        if self._session_pool:
            await self._session_pool.stop()
        if self._driver:
            await self._driver.stop()
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        self._initialized = False

    async def _monitor_pool(self):
        """Log session pool stats periodically"""
        while True:
            try:
                await asyncio.sleep(30)
                if self._session_pool:
                    # Get stats from pool (if available)
                    # ydb.aio.SessionPool doesn't expose detailed stats directly, so we use approximate counters
                    async with self._stats_lock:
                        stats = {
                            'pool_size': profile_config.YDB_POOL_SIZE,
                            'active': self._pool_stats['active_sessions'],
                            'idle': self._pool_stats['idle_sessions'],
                            'acquire_count': self._pool_stats['acquire_count'],
                            'avg_wait_ms': round(self._pool_stats['acquire_wait_time'] / max(1, self._pool_stats['acquire_count']) * 1000, 2)
                        }
                    logger.info(f"📊 YDB Session Pool Stats: {stats}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in pool monitor: {e}")

    async def acquire_session(self):
        """Acquire a session from the pool with timing"""
        start = time.time()
        session = await self._session_pool.acquire()
        elapsed = time.time() - start
        async with self._stats_lock:
            self._pool_stats['active_sessions'] += 1
            self._pool_stats['acquire_count'] += 1
            self._pool_stats['acquire_wait_time'] += elapsed
            if elapsed > 0.5:
                logger.warning(f"Slow session acquire: {elapsed:.3f}s")
        return session

    async def release_session(self, session):
        async with self._stats_lock:
            self._pool_stats['active_sessions'] -= 1
            self._pool_stats['idle_sessions'] += 1
        await self._session_pool.release(session)

    async def execute(self, query: str, params: Dict[str, Any] = None, retries: int = profile_config.YDB_RETRY_LIMIT) -> List[Any]:
        """Execute a query with retries and timeout"""
        if not self._initialized:
            raise Exception("YDB not initialized")
        if params is None:
            params = {}

        last_exc = None
        for attempt in range(1, retries + 1):
            session = None
            try:
                session = await self.acquire_session()
                # Prepare statement
                prepared = await session.prepare(query)
                # Execute with transaction
                tx = session.transaction()
                result = await tx.execute(prepared, params, commit_tx=True)
                await self.release_session(session)
                return result
            except Exception as e:
                last_exc = e
                if session:
                    await self.release_session(session)
                if attempt < retries:
                    wait = profile_config.YDB_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.warning(f"YDB error, retry {attempt}/{retries} in {wait}s: {e}")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"YDB error after {retries} attempts: {e}")
                    raise

        raise last_exc or Exception("Unknown YDB error")

# ============================================
# S3 КЛИЕНТ (остаётся синхронным, обёрнут в executor)
# ============================================
class S3Client:
    _instance = None
    _client = None
    _enabled = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        if not S3_AVAILABLE:
            logger.warning("boto3 not available, S3 operations disabled")
            return
        if not profile_config.S3_ACCESS_KEY or not profile_config.S3_SECRET_KEY:
            logger.warning("S3 credentials not found, avatar uploads disabled")
            return
        try:
            self._client = boto3.client('s3', endpoint_url=profile_config.S3_ENDPOINT,
                                        aws_access_key_id=profile_config.S3_ACCESS_KEY,
                                        aws_secret_access_key=profile_config.S3_SECRET_KEY,
                                        region_name=profile_config.S3_REGION)
            self._enabled = True
            logger.info(f"✅ S3 client initialized. Bucket: {profile_config.S3_BUCKET}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3 client: {e}")

    def is_enabled(self) -> bool: return self._enabled

    def upload_image(self, image_data: bytes, filename: str, content_type: str) -> Optional[str]:
        if not self._enabled:
            logger.warning("S3 not enabled, simulating upload")
            return filename
        try:
            self._client.put_object(Bucket=profile_config.S3_BUCKET, Key=filename, Body=image_data,
                                    ContentType=f'image/{content_type}', ACL='public-read')
            logger.info(f"✅ Uploaded to S3: {filename}")
            return filename
        except Exception as e:
            logger.error(f"❌ Failed to upload to S3: {e}")
            return None

    def delete_image(self, filename: str) -> bool:
        if not self._enabled:
            logger.warning("S3 not enabled, simulating delete")
            return True
        try:
            self._client.delete_object(Bucket=profile_config.S3_BUCKET, Key=filename)
            logger.info(f"✅ Deleted from S3: {filename}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to delete from S3: {e}")
            return False

    def get_public_url(self, filename: str) -> str:
        return f"{profile_config.S3_PUBLIC_URL}/{filename}"

    def generate_upload_url(self, filename: str, content_type: str = 'image/jpeg', expires_in: int = 3600) -> Optional[str]:
        if not self._enabled: return None
        try:
            url = self._client.generate_presigned_url('put_object',
                                                      Params={'Bucket': profile_config.S3_BUCKET, 'Key': filename,
                                                              'ContentType': content_type, 'ACL': 'public-read'},
                                                      ExpiresIn=expires_in)
            return url
        except Exception as e:
            logger.error(f"❌ Failed to generate presigned URL: {e}")
            return None

# ============================================
# JWT СЕРВИС (синхронный, обёрнут в executor при необходимости)
# ============================================
class JWTService:
    def __init__(self):
        self.secret = profile_config.JWT_SECRET
        self.algorithm = profile_config.JWT_ALGORITHM

    def decode_token(self, token: str) -> Optional[Dict]:
        try:
            payload = jwt.decode(token, self.secret, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            logger.error("Token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.error(f"Invalid token: {e}")
            return None

    def get_user_id_from_token(self, token: str) -> Optional[str]:
        try:
            if not token.startswith('Bearer '):
                return None
            token = token.split(' ')[1]
            payload = self.decode_token(token)
            if not payload: return None
            user_id = payload.get('user_id') or payload.get('sub')
            if not user_id: logger.error("No user_id in token payload")
            return str(user_id) if user_id else None
        except Exception as e:
            logger.error(f"Token decode error: {e}", exc_info=True)
            return None

# ============================================
# FEED SERVICE КЛИЕНТ (остаётся асинхронным)
# ============================================
class FeedServiceClient:
    def __init__(self):
        self.base_url = profile_config.FEED_SERVICE_URL
        self.timeout = profile_config.FEED_SERVICE_TIMEOUT
        self.enabled = AIOHTTP_AVAILABLE
        self._session = None
        logger.info(f"📡 Feed Service URL: {self.base_url}")

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_user_stats(self, user_id: str, token: str) -> Dict:
        if not self.enabled:
            return {'posts_count': 0, 'followers_count': 0, 'following_count': 0, 'is_following': False}
        try:
            session = await self._get_session()
            posts_task = self._get_posts_count(user_id, token, session)
            followers_task = self._get_followers_count(user_id, token, session)
            following_task = self._get_following_count(user_id, token, session)
            posts_count, followers_count, following_count = await asyncio.gather(posts_task, followers_task, following_task, return_exceptions=True)
            if isinstance(posts_count, Exception): posts_count = 0
            if isinstance(followers_count, Exception): followers_count = 0
            if isinstance(following_count, Exception): following_count = 0
            return {'posts_count': posts_count, 'followers_count': followers_count, 'following_count': following_count, 'is_following': False}
        except Exception as e:
            logger.error(f"Error calling feed_service: {e}")
            return {'posts_count': 0, 'followers_count': 0, 'following_count': 0, 'is_following': False}

    async def _get_posts_count(self, user_id: str, token: str, session) -> int:
        url = f"{self.base_url}/feed/user/posts?user_id={user_id}&limit=1"
        try:
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    posts = data.get('data', {}).get('posts', [])
                    pagination = data.get('data', {}).get('pagination', {})
                    total = pagination.get('total', len(posts))
                    return total
                else: return 0
        except: return 0

    async def _get_followers_count(self, user_id: str, token: str, session) -> int:
        url = f"{self.base_url}/feed/followers?user_id={user_id}&limit=1"
        try:
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    followers = data.get('data', {}).get('followers', [])
                    return data.get('data', {}).get('total', len(followers))
                return 0
        except:
            return 0

    async def _get_following_count(self, user_id: str, token: str, session) -> int:
        url = f"{self.base_url}/feed/following?user_id={user_id}&limit=1"
        try:
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    following = data.get('data', {}).get('following', [])
                    return data.get('data', {}).get('total', len(following))
                return 0
        except:
            return 0

    async def get_user_posts(self, user_id: str, token: str, limit: int = 20, offset: int = 0) -> Dict:
        if not self.enabled: return {'posts': [], 'total': 0}
        try:
            session = await self._get_session()
            url = f"{self.base_url}/feed/user/posts?user_id={user_id}&limit={limit}&offset={offset}"
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    posts_data = data.get('data', {})
                    posts = posts_data.get('posts', [])
                    total = posts_data.get('pagination', {}).get('total', len(posts))
                    logger.info(f"✅ Got {len(posts)} posts from feed_service")
                    return {'posts': posts, 'total': total}
                else: return {'posts': [], 'total': 0}
        except: return {'posts': [], 'total': 0}

# ============================================
# CHAT SERVICE КЛИЕНТ (остаётся асинхронным)
# ============================================
class ChatServiceClient:
    def __init__(self):
        self.base_url = profile_config.CHAT_SERVICE_URL
        self.timeout = profile_config.CHAT_SERVICE_TIMEOUT
        self.enabled = AIOHTTP_AVAILABLE
        self._session = None
        logger.info(f"💬 Chat Service URL: {self.base_url}")

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_user_public_chats(self, user_id: str, requesting_user_id: str, token: str) -> Dict:
        if not self.enabled: return {'channels': [], 'groups': []}
        try:
            session = await self._get_session()
            url = f"{self.base_url}/users/{user_id}/public-chats"
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    chats = data.get('data', {})
                    logger.info(f"✅ Got {len(chats.get('channels', []))} channels and {len(chats.get('groups', []))} groups from chat_service")
                    return chats
                else: return {'channels': [], 'groups': []}
        except: return {'channels': [], 'groups': []}

# ============================================
# РЕПОЗИТОРИЙ ПОЛЬЗОВАТЕЛЕЙ (АСИНХРОННЫЙ)
# ============================================
class UserRepository:
    def __init__(self, db: AsyncYDBConnection):
        self.db = db
        self.cache = profile_cache

    async def get_by_id(self, user_id: str) -> Optional[Dict]:
        if not validate_uuid(user_id):
            logger.warning(f"Invalid user_id format: {user_id}")
            return None
        query = """
        DECLARE $id AS Utf8;
        SELECT id, username, email, first_name_encrypted, last_name_encrypted, phone_encrypted,
               avatar_url, cover_url, role, status, email_verified, is_verified,
               country_code, timezone, date_of_birth, bio, about,
               social_links, website, company, position, education,
               created_at, updated_at, last_login_at
        FROM users WHERE id = $id;
        """
        try:
            result = await self.db.execute(query, {'$id': user_id})
            if result and result[0].rows:
                return self._row_to_dict(result[0].rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting user by ID: {e}")
            return None

    async def get_by_username(self, username: str) -> Optional[Dict]:
        query = """
        DECLARE $username AS Utf8;
        SELECT id, username, email, first_name_encrypted, last_name_encrypted, phone_encrypted,
               avatar_url, cover_url, role, status, email_verified, is_verified,
               country_code, timezone, date_of_birth, bio, about,
               social_links, website, company, position, education,
               created_at, updated_at, last_login_at
        FROM users WHERE username = $username;
        """
        try:
            result = await self.db.execute(query, {'$username': username})
            if result and result[0].rows:
                return self._row_to_dict(result[0].rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting user by username: {e}")
            return None

    async def update(self, user_id: str, updates: Dict) -> bool:
        if not updates: return True
        set_parts = []
        params = {'$id': user_id}
        declares = ["DECLARE $id AS Utf8;"]
        for key, value in updates.items():
            if value is None:
                set_parts.append(f"{key} = NULL")
            elif key == 'date_of_birth' and value:
                try:
                    date_obj = datetime.strptime(value, '%Y-%m-%d').date()
                    param_name = f"${key}"
                    params[param_name] = date_obj
                    declares.append(f"DECLARE {param_name} AS Date;")
                    set_parts.append(f"{key} = {param_name}")
                except ValueError: continue
            elif key == 'social_links' and value is not None:
                param_name = f"${key}"
                params[param_name] = json.dumps(value, ensure_ascii=False)
                declares.append(f"DECLARE {param_name} AS Json;")
                set_parts.append(f"{key} = {param_name}")
            else:
                param_name = f"${key}"
                params[param_name] = value
                declares.append(f"DECLARE {param_name} AS Utf8;")
                set_parts.append(f"{key} = {param_name}")
        set_parts.append("updated_at = CurrentUtcTimestamp()")
        declare_block = "\n".join(declares)
        query = f"{declare_block}\nUPDATE users SET {', '.join(set_parts)} WHERE id = $id;"
        try:
            await self.db.execute(query, params)
            logger.info(f"✅ User {user_id} updated")
            return True
        except Exception as e:
            logger.error(f"❌ Error updating user: {e}")
            return False

    async def get_posts_count(self, user_id: str) -> int:
        try:
            result = await self.db.execute(
                "DECLARE $uid AS Utf8; SELECT COUNT(*) AS cnt FROM feed_posts WHERE user_id = $uid AND is_deleted = false;",
                {'$uid': user_id}
            )
            return int(result[0].rows[0]['cnt']) if result and result[0].rows else 0
        except Exception as e:
            logger.error(f"posts count error: {e}")
            return 0

    async def get_followers_count(self, user_id: str) -> int:
        try:
            result = await self.db.execute(
                "DECLARE $uid AS Utf8; SELECT COUNT(*) AS cnt FROM feed_follows WHERE following_id = $uid;",
                {'$uid': user_id}
            )
            return int(result[0].rows[0]['cnt']) if result and result[0].rows else 0
        except Exception as e:
            logger.error(f"followers count error: {e}")
            return 0

    async def get_following_count(self, user_id: str) -> int:
        try:
            result = await self.db.execute(
                "DECLARE $uid AS Utf8; SELECT COUNT(*) AS cnt FROM feed_follows WHERE follower_id = $uid;",
                {'$uid': user_id}
            )
            return int(result[0].rows[0]['cnt']) if result and result[0].rows else 0
        except Exception as e:
            logger.error(f"following count error: {e}")
            return 0

    async def update_avatar(self, user_id: str, avatar_url: Optional[str]) -> bool:
        return await self.update(user_id, {'avatar_url': avatar_url})

    async def update_cover(self, user_id: str, cover_url: Optional[str]) -> bool:
        return await self.update(user_id, {'cover_url': cover_url})

    async def get_profile_visibility_settings(self, user_id: str) -> List[Dict]:
        """Получить настройки видимости только для каналов, где пользователь является владельцем или админом"""
        query = """
        DECLARE $user_id AS Utf8;
        
        SELECT 
            c.id as chat_id,
            c.title,
            c.type,
            c.is_public,
            c.username,
            p.role,
            p.show_in_profile
        FROM chat_participants p
        JOIN chats c ON c.id = p.chat_id
        WHERE p.user_id = $user_id 
          AND p.is_active = true
          AND c.type = 'channel'
          AND p.role IN ('owner', 'admin')
        ORDER BY c.title;
        """
        try:
            result = await self.db.execute(query, {'$user_id': user_id})
            if result and result[0].rows:
                return [dict(row) for row in result[0].rows]
            return []
        except Exception as e:
            logger.error(f"Failed to get profile visibility settings: {e}")
            return []

    async def update_chat_visibility(self, user_id: str, chat_id: int, show_in_profile: bool) -> bool:
        """Обновить видимость чата в профиле"""
        query = """
        DECLARE $user_id AS Utf8;
        DECLARE $chat_id AS Uint64;
        DECLARE $show_in_profile AS Bool;
        
        UPDATE chat_participants 
        SET show_in_profile = $show_in_profile, 
            version = version + 1
        WHERE user_id = $user_id AND chat_id = $chat_id;
        """
        params = {
            '$user_id': user_id,
            '$chat_id': chat_id,
            '$show_in_profile': show_in_profile
        }
        try:
            await self.db.execute(query, params)
            logger.info(f"✅ Updated visibility for user {user_id}, chat {chat_id}: show_in_profile={show_in_profile}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to update chat visibility: {e}")
            return False

    def _row_to_dict(self, row) -> Dict:
        first_name = safe_b64decode(getattr(row, 'first_name_encrypted', ''))
        last_name = safe_b64decode(getattr(row, 'last_name_encrypted', ''))
        phone = safe_b64decode(getattr(row, 'phone_encrypted', ''))
        result = {
            'id': row.id, 'username': row.username, 'email': row.email,
            'first_name': first_name, 'last_name': last_name, 'phone': phone,
            'avatar_url': getattr(row, 'avatar_url', None), 'cover_url': getattr(row, 'cover_url', None),
            'role': getattr(row, 'role', 'user'), 'status': getattr(row, 'status', 'active'),
            'email_verified': getattr(row, 'email_verified', False), 'is_verified': getattr(row, 'is_verified', False),
            'country_code': getattr(row, 'country_code', None), 'timezone': getattr(row, 'timezone', None),
            'date_of_birth': getattr(row, 'date_of_birth', None), 'bio': getattr(row, 'bio', None),
            'about': getattr(row, 'about', None), 'website': getattr(row, 'website', None),
            'company': getattr(row, 'company', None), 'position': getattr(row, 'position', None),
            'education': getattr(row, 'education', None), 'created_at': getattr(row, 'created_at', None),
            'updated_at': getattr(row, 'updated_at', None), 'last_login_at': getattr(row, 'last_login_at', None)
        }
        social_links = getattr(row, 'social_links', None)
        if social_links and isinstance(social_links, str):
            try: result['social_links'] = json.loads(social_links)
            except: result['social_links'] = []
        else: result['social_links'] = social_links or []
        if result['date_of_birth'] and hasattr(result['date_of_birth'], 'isoformat'):
            result['date_of_birth'] = result['date_of_birth'].isoformat()
        return result

# ============================================
# ВАЛИДАТОР ИЗОБРАЖЕНИЙ (асинхронный через executor)
# ============================================
class ImageValidator:
    @staticmethod
    async def validate_base64(base64_data: str, max_size_mb: int = 5) -> Tuple[bool, str, Optional[bytes], Optional[str]]:
        try:
            if not base64_data: return False, "No image data provided", None, None
            if not base64_data.startswith('data:image/'): return False, "Invalid image format. Must be data:image/*", None, None
            def _parse():
                header, encoded = base64_data.split(',', 1)
                mime_type = header.split(';')[0].split(':')[1]
                image_type = mime_type.split('/')[1].lower()
                return mime_type, image_type, encoded
            mime_type, image_type, encoded = await cpu_executor.run(_parse)
            if image_type not in profile_config.ALLOWED_IMAGE_TYPES:
                return False, f"Image type not allowed. Allowed: {', '.join(profile_config.ALLOWED_IMAGE_TYPES)}", None, None
            image_data = await cpu_executor.run(base64.b64decode, encoded)
            max_size = max_size_mb * 1024 * 1024
            if len(image_data) > max_size: return False, f"Image too large. Maximum size: {max_size_mb}MB", None, None
            # Optional: type detection
            import filetype
            def _check_type(): return filetype.guess_mime(image_data)
            detected_mime = await cpu_executor.run(_check_type)
            detected_subtype = detected_mime.split('/')[-1] if detected_mime else None
            if not detected_subtype or detected_subtype not in profile_config.ALLOWED_IMAGE_TYPES:
                return False, "Invalid or corrupted image file", None, None
            return True, "", image_data, image_type
        except Exception as e:
            logger.error(f"Image validation error: {e}")
            return False, f"Invalid image data: {str(e)}", None, None

# ============================================
# МОДЕЛИ PYDANTIC (без изменений)
# ============================================
class SocialLink(BaseModel):
    platform: str
    url: str
    title: Optional[str] = None

class ProfileUpdate(BaseModel):
    first_name: Optional[str] = Field(None, max_length=50)
    last_name: Optional[str] = Field(None, max_length=50)
    username: Optional[str] = Field(None, min_length=3, max_length=50)
    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    bio: Optional[str] = Field(None, max_length=1000)
    about: Optional[str] = Field(None, max_length=2000)
    website: Optional[str] = None
    social_links: Optional[List[SocialLink]] = Field(None, max_items=10)
    company: Optional[str] = Field(None, max_length=100)
    position: Optional[str] = Field(None, max_length=100)
    education: Optional[str] = Field(None, max_length=200)
    country_code: Optional[str] = Field(None, pattern=r'^[A-Z]{2}$')
    timezone: Optional[str] = None
    date_of_birth: Optional[str] = None

    @validator('username')
    def validate_username(cls, v):
        if v:
            valid, msg = validate_username(v)
            if not valid: raise ValueError(msg)
        return v

    @validator('phone')
    def validate_phone(cls, v):
        if v and not validate_phone(v): raise ValueError('Invalid phone number format')
        return v

    @validator('date_of_birth')
    def validate_date_of_birth(cls, v):
        if v and not validate_date_of_birth(v): raise ValueError('Date of birth must be in YYYY-MM-DD format')
        return v

    @validator('website')
    def validate_website(cls, v):
        return v

    @validator('social_links')
    def validate_social_links(cls, v):
        return v

class AvatarUpload(BaseModel):
    image: str

class CoverUpload(BaseModel):
    image: str

# ============================================
# СЕРВИС ПРОФИЛЕЙ (АСИНХРОННЫЙ)
# ============================================
class ProfileService:
    def __init__(self, db: AsyncYDBConnection, s3: S3Client, jwt: JWTService,
                 feed_client: FeedServiceClient, chat_client: ChatServiceClient):
        self.db = db
        self.s3 = s3
        self.jwt = jwt
        self.feed_client = feed_client
        self.chat_client = chat_client
        self.user_repo = UserRepository(db)
        self.cache = profile_cache
        self.stats = {'requests': 0, 'cache_hits': 0}

    async def get_profile_visibility_settings(self, user_id: str) -> Dict:
        """Получить настройки видимости для текущего пользователя"""
        async with backpressure.read_operation():
            settings = await self.user_repo.get_profile_visibility_settings(user_id)
            return {
                'settings': settings,
                'count': len(settings)
            }
    
    async def update_chat_visibility(self, user_id: str, chat_id: int, show_in_profile: bool) -> Dict:
        """Обновить видимость чата в профиле"""
        async with backpressure.write_operation():
            # Проверяем, что чат существует
            chat_query = """
            DECLARE $chat_id AS Uint64;
            SELECT id, type, is_public, username FROM chats WHERE id = $chat_id;
            """
            chat_result = await self.db.execute(chat_query, {'$chat_id': chat_id})
            if not chat_result or not chat_result[0].rows:
                raise ValueError("Chat not found")
            chat = chat_result[0].rows[0]

            # Проверяем, что пользователь является участником
            participant_query = """
            DECLARE $user_id AS Utf8;
            DECLARE $chat_id AS Uint64;
            SELECT role FROM chat_participants 
            WHERE user_id = $user_id AND chat_id = $chat_id AND is_active = true;
            """
            participant_result = await self.db.execute(participant_query, {
                '$user_id': user_id,
                '$chat_id': chat_id
            })
            if not participant_result or not participant_result[0].rows:
                raise PermissionError("You are not a member of this chat")
            participant = participant_result[0].rows[0]

            # Проверяем права (только владелец или админ)
            if participant.get('role') not in ['owner', 'admin']:
                raise PermissionError("Only owner and admin can change visibility settings")

            # Для публичных чатов проверяем наличие username
            if show_in_profile and chat.get('is_public') and not chat.get('username'):
                raise ValidationError("Public chats must have a username to be shown in profile")

            # Обновляем видимость
            success = await self.user_repo.update_chat_visibility(user_id, chat_id, show_in_profile)
            if not success:
                raise Exception("Failed to update visibility")

            # Инвалидируем кэш профиля
            await self.cache.invalidate(f"profile:*:{user_id}")

            return {
                'chat_id': chat_id,
                'show_in_profile': show_in_profile,
                'message': f"Chat visibility set to {show_in_profile}"
            }

    async def get_profile(self, current_user_id: str, target_user_id: Optional[str] = None,
                          username: Optional[str] = None, token: Optional[str] = None) -> Dict:
        async with backpressure.read_operation():
            self.stats['requests'] += 1
            if target_user_id:
                cache_key = f"profile:id:{target_user_id}:{current_user_id}"
            elif username:
                cache_key = f"profile:username:{username}:{current_user_id}"
            else:
                cache_key = f"profile:me:{current_user_id}"
            cached = await self.cache.get(cache_key)
            if cached:
                self.stats['cache_hits'] += 1
                return cached
            if target_user_id:
                user = await self.user_repo.get_by_id(target_user_id)
            elif username:
                user = await self.user_repo.get_by_username(username)
            else:
                user = await self.user_repo.get_by_id(current_user_id)
            if not user: raise ValueError("User not found")
            if user.get('status') != 'active' and str(user['id']) != str(current_user_id):
                raise ValueError("User not found")
            try:
                public_chats = await self.chat_client.get_user_public_chats(user['id'], current_user_id, token)
            except Exception:
                public_chats = {'channels': [], 'groups': []}
            posts_count = await self.user_repo.get_posts_count(user['id'])
            followers_count = await self.user_repo.get_followers_count(user['id'])
            following_count = await self.user_repo.get_following_count(user['id'])
            stats = {'posts_count': posts_count, 'followers_count': followers_count, 'following_count': following_count, 'is_following': False}
            full_name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            if not full_name: full_name = user.get('username', '')
            avatar_url = user.get('avatar_url')
            if avatar_url and avatar_url.startswith('avatars/'):
                avatar_url = f"{profile_config.S3_PUBLIC_URL}/{avatar_url}"
            cover_url = user.get('cover_url')
            if cover_url and cover_url.startswith('covers/'):
                cover_url = f"{profile_config.S3_PUBLIC_URL}/{cover_url}"
            created_at = user.get('created_at')
            if created_at and hasattr(created_at, 'isoformat'):
                created_at = created_at.isoformat() + 'Z'
            result = {
                'id': user['id'], 'username': user['username'],
                'first_name': user.get('first_name', ''), 'last_name': user.get('last_name', ''),
                'full_name': full_name, 'avatar_url': avatar_url, 'cover_url': cover_url,
                'bio': user.get('bio'), 'about': user.get('about'), 'website': user.get('website'),
                'social_links': user.get('social_links', []), 'company': user.get('company'),
                'position': user.get('position'), 'education': user.get('education'),
                'country_code': user.get('country_code'), 'is_verified': user.get('is_verified', False),
                'created_at': created_at,
                'posts_count': stats.get('posts_count', 0), 'followers_count': stats.get('followers_count', 0),
                'following_count': stats.get('following_count', 0), 'is_following': stats.get('is_following', False),
                'channels': public_chats.get('channels', []), 'groups': public_chats.get('groups', []),
                'is_owner': str(user['id']) == str(current_user_id)
            }
            await self.cache.set(cache_key, result, ttl=profile_config.CACHE_TTL_PROFILE)
            return result

    async def get_user_posts(self, current_user_id: str, target_user_id: str,
                             token: str, limit: int = 20, offset: int = 0) -> Dict:
        async with backpressure.read_operation():
            cache_key = f"posts:{target_user_id}:{limit}:{offset}"
            cached = await self.cache.get(cache_key)
            if cached:
                self.stats['cache_hits'] += 1
                return cached
            user = await self.user_repo.get_by_id(target_user_id)
            if not user: raise ValueError("User not found")
            if user.get('status') != 'active' and str(user['id']) != str(current_user_id):
                raise ValueError("User not found")
            result = await self.feed_client.get_user_posts(target_user_id, token, limit, offset)
            formatted = {'posts': result.get('posts', []), 'total': result.get('total', 0),
                         'limit': limit, 'offset': offset,
                         'has_more': (offset + limit) < result.get('total', 0)}
            await self.cache.set(cache_key, formatted, ttl=60)
            return formatted

    async def update_profile(self, user_id: str, data: ProfileUpdate) -> Dict:
        async with backpressure.read_operation():
            updates = data.dict(exclude_unset=True, exclude_none=True)
            if 'social_links' in updates:
                updates['social_links'] = [link.dict() for link in updates['social_links']]
            if not updates:
                return await self.get_profile(user_id, user_id, token=None)
            if 'username' in updates:
                existing = await self.user_repo.get_by_username(updates['username'])
                if existing and str(existing['id']) != user_id:
                    raise ValueError("Username already taken")
            # Переименовываем поля в имена колонок YDB (зашифрованные)
            encrypted_map = {'first_name': 'first_name_encrypted', 'last_name': 'last_name_encrypted', 'phone': 'phone_encrypted'}
            for field, col in encrypted_map.items():
                if field in updates:
                    val = updates.pop(field)
                    updates[col] = base64.b64encode(val.encode()).decode() if val else None
            success = await self.user_repo.update(user_id, updates)
            if not success: raise Exception("Failed to update profile")
            await self.cache.invalidate(user_id)
            return await self.get_profile(user_id, user_id, token=None)

    async def upload_avatar(self, user_id: str, image_data: str) -> Dict:
        async with backpressure.image_operation():
            if not self.s3.is_enabled(): raise Exception("Image storage service unavailable")
            is_valid, error, img_data, img_type = await ImageValidator.validate_base64(image_data, profile_config.MAX_AVATAR_SIZE_MB)
            if not is_valid: raise ValueError(error)
            user = await self.user_repo.get_by_id(user_id)
            if user and user.get('avatar_url'):
                old_avatar = user['avatar_url']
                if old_avatar and old_avatar.startswith('avatars/'):
                    await cpu_executor.run(self.s3.delete_image, old_avatar)
            timestamp = int(datetime.utcnow().timestamp())
            extension = 'png' if img_type == 'png' else 'jpg'
            filename = f"avatars/{user_id}_{timestamp}.{extension}"
            uploaded = await cpu_executor.run(self.s3.upload_image, img_data, filename, img_type)
            if not uploaded: raise Exception("Failed to upload image")
            success = await self.user_repo.update_avatar(user_id, filename)
            if not success:
                await cpu_executor.run(self.s3.delete_image, filename)
                raise Exception("Failed to update avatar in database")
            await self.cache.invalidate(user_id)
            avatar_url = self.s3.get_public_url(filename)
            return {'avatar_url': avatar_url, 'filename': filename}

    async def upload_cover(self, user_id: str, image_data: str) -> Dict:
        async with backpressure.image_operation():
            if not self.s3.is_enabled(): raise Exception("Image storage service unavailable")
            is_valid, error, img_data, img_type = await ImageValidator.validate_base64(image_data, profile_config.MAX_COVER_SIZE_MB)
            if not is_valid: raise ValueError(error)
            user = await self.user_repo.get_by_id(user_id)
            if user and user.get('cover_url'):
                old_cover = user['cover_url']
                if old_cover and old_cover.startswith('covers/'):
                    await cpu_executor.run(self.s3.delete_image, old_cover)
            timestamp = int(datetime.utcnow().timestamp())
            extension = 'png' if img_type == 'png' else 'jpg'
            filename = f"covers/{user_id}_{timestamp}.{extension}"
            uploaded = await cpu_executor.run(self.s3.upload_image, img_data, filename, img_type)
            if not uploaded: raise Exception("Failed to upload image")
            success = await self.user_repo.update_cover(user_id, filename)
            if not success:
                await cpu_executor.run(self.s3.delete_image, filename)
                raise Exception("Failed to update cover in database")
            await self.cache.invalidate(user_id)
            cover_url = self.s3.get_public_url(filename)
            return {'cover_url': cover_url, 'filename': filename}

    async def delete_avatar(self, user_id: str) -> Dict:
        async with backpressure.read_operation():
            user = await self.user_repo.get_by_id(user_id)
            if not user: raise ValueError("User not found")
            old_avatar = user.get('avatar_url')
            if old_avatar and old_avatar.startswith('avatars/') and self.s3.is_enabled():
                await cpu_executor.run(self.s3.delete_image, old_avatar)
            success = await self.user_repo.update_avatar(user_id, None)
            if not success: raise Exception("Failed to delete avatar")
            await self.cache.invalidate(user_id)
            return {'success': True}

    async def delete_cover(self, user_id: str) -> Dict:
        async with backpressure.read_operation():
            user = await self.user_repo.get_by_id(user_id)
            if not user: raise ValueError("User not found")
            old_cover = user.get('cover_url')
            if old_cover and old_cover.startswith('covers/') and self.s3.is_enabled():
                await cpu_executor.run(self.s3.delete_image, old_cover)
            success = await self.user_repo.update_cover(user_id, None)
            if not success: raise Exception("Failed to delete cover")
            await self.cache.invalidate(user_id)
            return {'success': True}

    async def generate_avatar_upload_url(self, user_id: str) -> Dict:
        if not self.s3.is_enabled(): raise Exception("Image storage service unavailable")
        timestamp = int(datetime.utcnow().timestamp())
        filename = f"avatars/{user_id}_{timestamp}.jpg"
        upload_url = await cpu_executor.run(self.s3.generate_upload_url, filename, 'image/jpeg', 3600)
        if not upload_url: raise Exception("Failed to generate upload URL")
        public_url = self.s3.get_public_url(filename)
        return {'upload_url': upload_url, 'public_url': public_url, 'filename': filename, 'expires_in': 3600}

    async def generate_cover_upload_url(self, user_id: str) -> Dict:
        if not self.s3.is_enabled(): raise Exception("Image storage service unavailable")
        timestamp = int(datetime.utcnow().timestamp())
        filename = f"covers/{user_id}_{timestamp}.jpg"
        upload_url = await cpu_executor.run(self.s3.generate_upload_url, filename, 'image/jpeg', 3600)
        if not upload_url: raise Exception("Failed to generate upload URL")
        public_url = self.s3.get_public_url(filename)
        return {'upload_url': upload_url, 'public_url': public_url, 'filename': filename, 'expires_in': 3600}

# ============================================
# FASTAPI ПРИЛОЖЕНИЕ
# ============================================
# Глобальные переменные для сервисов, инициализируемые в lifespan
db: Optional[AsyncYDBConnection] = None
s3: Optional[S3Client] = None
jwt_service: Optional[JWTService] = None
feed_client: Optional[FeedServiceClient] = None
chat_client: Optional[ChatServiceClient] = None
profile_service: Optional[ProfileService] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация сервисов
    global db, s3, jwt_service, feed_client, chat_client, profile_service
    logger.info("Initializing services...")
    try:
        db = AsyncYDBConnection()
        await db.initialize()
    except Exception as e:
        logger.error(f"❌ YDB init failed: {e}")
        db = None
    try:
        s3 = S3Client()
    except Exception as e:
        logger.error(f"❌ S3 init failed: {e}")
        s3 = None
    jwt_service = JWTService()
    feed_client = FeedServiceClient()
    chat_client = ChatServiceClient()
    if db is not None:
        profile_service = ProfileService(db, s3, jwt_service, feed_client, chat_client)
    else:
        logger.error("❌ profile_service not started — YDB unavailable")
    await profile_cache.ensure_started()
    logger.info("✅ Services initialized (YDB: %s)", "OK" if db is not None else "FAILED")
    yield
    # Очистка
    logger.info("Shutting down services...")
    await profile_cache.stop()
    if feed_client:
        await feed_client.close()
    if chat_client:
        await chat_client.close()
    if db:
        await db.close()
    logger.info("✅ Shutdown complete")

app = FastAPI(title="Profile Service", version="5.0.0", lifespan=lifespan)

from fastapi.exceptions import RequestValidationError
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    def _make_serializable(obj):
        if isinstance(obj, dict):
            return {k: _make_serializable(v) for k, v in obj.items() if k != "url"}
        if isinstance(obj, list):
            return [_make_serializable(i) for i in obj]
        if isinstance(obj, Exception):
            return str(obj)
        return obj

    errors = _make_serializable(exc.errors())
    logger.error(f"422 Validation error on {request.method} {request.url.path}: {errors}")
    return JSONResponse(status_code=422, content={"detail": errors})

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Вспомогательные функции для извлечения параметров
def get_token(request: Request) -> Optional[str]:
    auth = request.headers.get("Authorization")
    if auth and auth.startswith("Bearer "):
        return auth
    return None

def get_current_user_id(request: Request) -> str:
    """Получить user_id из токена"""
    logger.info("🔍 [AUTH] Getting current user ID")
    
    if profile_service is None:
        logger.error("❌ [AUTH] Profile service is None")
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")
    
    auth_header = request.headers.get("Authorization")
    logger.info(f"🔍 [AUTH] Authorization header: {auth_header[:50] if auth_header else 'None'}...")
    
    if not auth_header or not auth_header.startswith("Bearer "):
        logger.warning("❌ [AUTH] Missing or invalid Authorization header")
        raise HTTPException(status_code=401, detail="Missing authorization token")
    
    token = auth_header.split(" ")[1]
    logger.info(f"🔍 [AUTH] Token length: {len(token)}")
    
    # Декодируем токен напрямую через jwt
    try:
        import jwt
        logger.info("🔍 [AUTH] Decoding token...")
        payload = jwt.decode(token, profile_config.JWT_SECRET, algorithms=["HS256"])
        logger.info(f"🔍 [AUTH] Payload: {payload}")
        
        user_id = payload.get('user_id') or payload.get('sub')
        logger.info(f"🔍 [AUTH] Extracted user_id: {user_id}, type: {type(user_id)}")
        
        if not user_id:
            logger.error("❌ [AUTH] No user_id in token payload")
            raise HTTPException(status_code=401, detail="No user_id in token")
        
        # Возвращаем как есть, без проверки UUID
        logger.info(f"✅ [AUTH] Authenticated as user: {user_id}")
        return str(user_id)
        
    except jwt.ExpiredSignatureError:
        logger.error("❌ [AUTH] Token expired")
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as e:
        logger.error(f"❌ [AUTH] Invalid token: {e}")
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")
    except Exception as e:
        logger.error(f"❌ [AUTH] Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=401, detail=f"Authentication error: {str(e)}")

# ============================================
# МАРШРУТЫ
# ============================================
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "profile-service",
        "version": "5.0.0",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "s3_enabled": s3.is_enabled() if s3 else False
    }

@app.get("/info")
@app.get("/")
async def info():
    return {
        "service": "Profile Service",
        "version": "5.0.0",
        "endpoints": [
            "GET /health - Health check",
            "GET /profile - Get own profile",
            "GET /profile/{userId} - Get user profile by ID",
            "GET /u/{username} - Get user profile by username",
            "GET /profile/{userId}/posts - Get user posts",
            "GET /u/{username}/posts - Get user posts by username",
            "PATCH /profile - Update profile",
            "POST /profile/avatar/upload - Upload avatar (Base64)",
            "POST /profile/cover/upload - Upload cover (Base64)",
            "DELETE /profile/avatar - Delete avatar",
            "DELETE /profile/cover - Delete cover",
            "GET /profile/avatar/upload-url - Get presigned URL for avatar",
            "GET /profile/cover/upload-url - Get presigned URL for cover"
        ]
    }

@app.get("/profile")
async def get_my_profile(user_id: str = Depends(get_current_user_id)):
    try:
        result = await profile_service.get_profile(user_id, user_id, token=user_id)
        return {"profile": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/profile/visibility")
async def get_profile_visibility(user_id: str = Depends(get_current_user_id)):
    """Получить настройки видимости чатов в профиле"""
    logger.info(f"📋 [VISIBILITY] Getting profile visibility for user: {user_id}")
    try:
        result = await profile_service.get_profile_visibility_settings(user_id)
        logger.info(f"✅ [VISIBILITY] Got {result.get('count', 0)} visibility settings")
        return result
    except Exception as e:
        logger.error(f"❌ [VISIBILITY] Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/profile/visibility/{chat_id}")
async def update_chat_visibility(
    chat_id: int, 
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    """Обновить видимость чата в профиле"""
    try:
        body = await request.json()
        show_in_profile = body.get('show_in_profile', False)
        
        if not isinstance(show_in_profile, bool):
            raise HTTPException(status_code=400, detail="show_in_profile must be boolean")
        
        result = await profile_service.update_chat_visibility(user_id, chat_id, show_in_profile)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating chat visibility: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/profile/{userId}")
async def get_profile_by_id(userId: str, user_id: str = Depends(get_current_user_id)):
    try:
        if not validate_uuid(userId):
            raise HTTPException(status_code=400, detail="Invalid user ID format")
        result = await profile_service.get_profile(user_id, userId, token=user_id)
        return {"profile": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/u/{username}")
async def get_profile_by_username(username: str, user_id: str = Depends(get_current_user_id)):
    try:
        result = await profile_service.get_profile(user_id, None, username, token=user_id)
        return {"profile": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/profile/{userId}/posts")
async def get_user_posts(userId: str, limit: int = 20, offset: int = 0, request: Request = None, user_id: str = Depends(get_current_user_id)):
    try:
        if not validate_uuid(userId):
            raise HTTPException(status_code=400, detail="Invalid user ID format")
        limit = max(1, min(100, limit))
        offset = max(0, offset)
        auth = request.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else auth
        result = await profile_service.get_user_posts(user_id, userId, token, limit, offset)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/u/{username}/posts")
async def get_user_posts_by_username(username: str, limit: int = 20, offset: int = 0, request: Request = None, user_id: str = Depends(get_current_user_id)):
    try:
        user = await profile_service.user_repo.get_by_username(username)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        limit = max(1, min(100, limit))
        offset = max(0, offset)
        auth = request.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else auth
        result = await profile_service.get_user_posts(user_id, user['id'], token, limit, offset)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.patch("/profile")
async def update_profile(update_data: ProfileUpdate, user_id: str = Depends(get_current_user_id)):
    try:
        result = await profile_service.update_profile(user_id, update_data)
        return {"profile": result, "message": "Profile updated successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/profile/avatar/upload")
async def upload_avatar(upload: AvatarUpload, user_id: str = Depends(get_current_user_id)):
    try:
        result = await profile_service.upload_avatar(user_id, upload.image)
        return {"message": "Avatar uploaded successfully", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/profile/avatar")
async def delete_avatar(user_id: str = Depends(get_current_user_id)):
    try:
        result = await profile_service.delete_avatar(user_id)
        return {"message": "Avatar deleted successfully", "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/profile/avatar/upload-url")
async def avatar_upload_url(user_id: str = Depends(get_current_user_id)):
    try:
        result = await profile_service.generate_avatar_upload_url(user_id)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/profile/cover/upload")
async def upload_cover(upload: CoverUpload, user_id: str = Depends(get_current_user_id)):
    try:
        result = await profile_service.upload_cover(user_id, upload.image)
        return {"message": "Cover uploaded successfully", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/profile/cover")
async def delete_cover(user_id: str = Depends(get_current_user_id)):
    try:
        result = await profile_service.delete_cover(user_id)
        return {"message": "Cover deleted successfully", "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/profile/cover/upload-url")
async def cover_upload_url(user_id: str = Depends(get_current_user_id)):
    try:
        result = await profile_service.generate_cover_upload_url(user_id)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ============================================
# ЗАПУСК (для локального тестирования)
# ============================================
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8003, reload=True)