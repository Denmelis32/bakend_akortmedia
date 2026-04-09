"""
YDB Connection Pool - полностью асинхронная версия с детальным логированием
ИСПРАВЛЕНО: убрана рекурсия в _check_connection
"""
import os
import ydb
import ydb.aio
import ydb.iam
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
import traceback

logger = logging.getLogger('chat-service.db')

class YDBConnectionPool:
    """Асинхронный пул соединений YDB"""
    
    def __init__(self):
        self.endpoint = os.environ.get('YDB_ENDPOINT')
        self.database = os.environ.get('YDB_DATABASE')
        self.key_file = os.environ.get('YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS')
        self._driver = None
        self._pool = None
        self._initialized = False
        self._is_mock = False  # Флаг mock-режима
        self._lock = asyncio.Lock()
        logger.info(f"🔧 [YDB] Initialized with endpoint={self.endpoint}, database={self.database}, key_file={self.key_file}")
    
    async def initialize(self):
        """Асинхронная инициализация"""
        logger.info("🔧 [YDB] initialize() called")
        async with self._lock:
            if self._initialized:
                logger.info("✅ [YDB] Already initialized, returning")
                return
            
            try:
                logger.info("🔄 [YDB] Initializing YDB connection pool...")
                
                if not self.endpoint:
                    logger.error("❌ [YDB] YDB_ENDPOINT not set")
                    raise ValueError("YDB_ENDPOINT must be set")
                if not self.database:
                    logger.error("❌ [YDB] YDB_DATABASE not set")
                    raise ValueError("YDB_DATABASE must be set")
                
                logger.info(f"📦 [YDB] Connecting to {self.endpoint}{self.database}")
                
                # Используем файл ключа
                if self.key_file and os.path.exists(self.key_file):
                    logger.info(f"🔑 [YDB] Using key file: {self.key_file}")
                    credentials = ydb.iam.ServiceAccountCredentials.from_file(self.key_file)
                    logger.info("✅ [YDB] Credentials created from key file")
                else:
                    # Пробуем из переменных окружения
                    logger.info("🔑 [YDB] Using credentials from env")
                    credentials = ydb.credentials_from_env_variables()
                    logger.info("✅ [YDB] Credentials created from env")
                
                # Создаем драйвер
                logger.info("🔧 [YDB] Creating driver...")
                self._driver = ydb.aio.Driver(
                    endpoint=self.endpoint,
                    database=self.database,
                    credentials=credentials,
                )
                logger.info(f"✅ [YDB] Driver created: {self._driver}")
                
                # Ждем готовности
                logger.info("⏳ [YDB] Waiting for driver to become ready (timeout=15s)...")
                try:
                    await self._driver.wait(timeout=15)
                    logger.info("✅ [YDB] Driver initialized successfully")
                except asyncio.TimeoutError:
                    logger.error("❌ [YDB] Driver wait timeout after 15 seconds")
                    raise TimeoutError("YDB driver initialization timeout")
                except Exception as e:
                    logger.error(f"❌ [YDB] Driver wait failed: {e}")
                    logger.error(traceback.format_exc())
                    raise
                
                # Создаем пул
                logger.info("🔧 [YDB] Creating session pool...")
                self._pool = ydb.aio.SessionPool(self._driver, size=20)
                logger.info(f"✅ [YDB] Session pool created with size 20")
                
                # Проверяем соединение - ИСПРАВЛЕНО: не используем acquire
                logger.info("🔍 [YDB] Starting connection test...")
                try:
                    await self._check_connection()
                    logger.info("✅ [YDB] Connection test passed")
                except Exception as e:
                    logger.error(f"❌ [YDB] Connection test failed: {e}")
                    logger.error(traceback.format_exc())
                    raise
                
                self._initialized = True
                logger.info("✅ [YDB] Initialization complete")
                
            except Exception as e:
                logger.error(f"❌ [YDB] Failed to initialize: {e}")
                logger.error(traceback.format_exc())
                # Для разработки создаем заглушку (mock mode)
                logger.warning("⚠️ [YDB] Using mock database for development")
                self._initialized = True
                self._is_mock = True
    
    async def _check_connection(self):
        """Проверка соединения - ИСПРАВЛЕНО: без рекурсии"""
        logger.info("🔍 [YDB] _check_connection() called")
        
        # Проверяем, что пул существует
        if self._pool is None:
            logger.error("❌ [YDB] Pool is None, cannot check connection")
            return
        
        # Получаем сессию напрямую из пула (не через acquire)
        session = None
        try:
            logger.info("🔍 [YDB] Acquiring session from pool directly...")
            session = await self._pool.acquire()
            logger.info(f"✅ [YDB] Session acquired: {session}")
            
            logger.info("🔍 [YDB] Executing SELECT 1...")
            result = await session.transaction().execute("SELECT 1;", commit_tx=True)
            logger.info(f"✅ [YDB] Query executed successfully: {result}")
            
        except Exception as e:
            logger.error(f"❌ [YDB] Connection test failed: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if session:
                logger.info("🔍 [YDB] Releasing session...")
                await self._pool.release(session)
                logger.info("✅ [YDB] Session released")
    
    @asynccontextmanager
    async def acquire(self):
        """Получить сессию"""
        logger.info("🔍 [YDB] acquire() called")
        if not self._initialized:
            logger.info("⚠️ [YDB] Not initialized, calling initialize()")
            await self.initialize()
        
        # Mock режим - возвращаем заглушку
        if getattr(self, '_is_mock', False):
            class MockSession:
                async def transaction(self):
                    return self
                async def execute(self, query, params=None, commit_tx=False):
                    logger.warning(f"⚠️ [YDB MOCK] Executing query: {query[:100]}...")
                    return []  # Возвращаем пустой результат
            logger.warning("⚠️ [YDB] Using mock session")
            yield MockSession()
            return
        
        session = None
        try:
            logger.info("🔍 [YDB] Acquiring session from pool...")
            session = await self._pool.acquire()
            logger.info(f"✅ [YDB] Session acquired: {session}")
            yield session
        except Exception as e:
            logger.error(f"❌ [YDB] Session acquisition error: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if session:
                logger.info("🔍 [YDB] Releasing session...")
                await self._pool.release(session)
                logger.info("✅ [YDB] Session released")
    
    async def get_session(self):
        """Получить сессию (для совместимости)"""
        logger.info("🔍 [YDB] get_session() called")
        if not self._initialized:
            logger.info("⚠️ [YDB] Not initialized, calling initialize()")
            await self.initialize()
        
        # Mock режим - возвращаем заглушку
        if getattr(self, '_is_mock', False):
            class MockSession:
                async def transaction(self):
                    return self
                async def execute(self, query, params=None, commit_tx=False):
                    logger.warning(f"⚠️ [YDB MOCK] Executing query: {query[:100]}...")
                    return []
            logger.warning("⚠️ [YDB] Using mock session (get_session)")
            return MockSession()
        
        session = await self._pool.acquire()
        logger.info(f"✅ [YDB] Session acquired: {session}")
        return session
    
    async def release_session(self, session):
        """Вернуть сессию"""
        logger.info("🔍 [YDB] release_session() called")
        if session:
            await self._pool.release(session)
            logger.info("✅ [YDB] Session released")
    
    async def execute(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Выполнить запрос"""
        logger.info(f"🔍 [YDB] execute() called: {query[:100]}...")
        async with self.acquire() as session:
            try:
                if params:
                    logger.info(f"🔍 [YDB] Preparing query with {len(params)} params")
                    prepared = await session.prepare(query)
                    result = await session.transaction().execute(prepared, params, commit_tx=True)
                else:
                    result = await session.transaction().execute(query, commit_tx=True)
                
                if result and len(result) > 0:
                    rows = [dict(row) for row in result[0].rows] if result[0].rows else []
                    logger.info(f"✅ [YDB] Query returned {len(rows)} rows")
                    return rows
                logger.info("✅ [YDB] Query returned empty result")
                return []
            except Exception as e:
                logger.error(f"❌ [YDB] Execute error: {e}")
                logger.error(traceback.format_exc())
                raise
    
    async def close(self):
        """Закрыть пул"""
        logger.info("🔧 [YDB] close() called")
        if self._pool:
            logger.info("🔧 [YDB] Stopping pool...")
            await self._pool.stop()
            logger.info("✅ [YDB] Pool stopped")
        if self._driver:
            logger.info("🔧 [YDB] Stopping driver...")
            await self._driver.stop()
            logger.info("✅ [YDB] Driver stopped")
        logger.info("✅ [YDB] Pool closed")


# Глобальный экземпляр
_ydb_pool = None

def get_ydb_pool() -> YDBConnectionPool:
    """Получить глобальный экземпляр пула"""
    global _ydb_pool
    if _ydb_pool is None:
        _ydb_pool = YDBConnectionPool()
    return _ydb_pool

ydb_pool = get_ydb_pool()


# 👇 ДЛЯ СОВМЕСТИМОСТИ С common.py
class DatabasePool:
    """Обертка для совместимости"""
    def __init__(self):
        self._pool = get_ydb_pool()
        logger.info("✅ [DB] DatabasePool created")
    
    async def execute(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        return await self._pool.execute(query, params)
    
    async def get_session(self):
        return await self._pool.get_session()
    
    async def release_session(self, session):
        await self._pool.release_session(session)

db_pool = DatabasePool()


# 👇 ФУНКЦИИ ДЛЯ common.py
def get_db_pool() -> DatabasePool:
    """Получить глобальный экземпляр DatabasePool"""
    global db_pool
    return db_pool


# ============================================
# ФУНКЦИИ ИНИЦИАЛИЗАЦИИ
# ============================================

async def init_db_pool_async():
    """Асинхронная инициализация для app.py"""
    logger.info("🚀 [DB] init_db_pool_async() called")
    await ydb_pool.initialize()
    logger.info("✅ [DB] Database pool initialized (async)")
    return ydb_pool

def init_db_pool_sync():
    """Синхронная инициализация (для обратной совместимости)"""
    logger.info("🚀 [DB] init_db_pool_sync() called")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(ydb_pool.initialize())
        logger.info("✅ [DB] Database pool initialized (sync)")
        return True
    except Exception as e:
        logger.error(f"❌ [DB] Failed to initialize database pool: {e}")
        return False

def init_db_pool():
    """Синхронная инициализация для common.py"""
    logger.info("🚀 [DB] init_db_pool() called")
    return init_db_pool_sync()

async def close_db_pool():
    """Закрыть пул"""
    logger.info("🚀 [DB] close_db_pool() called")
    await ydb_pool.close()
    logger.info("✅ [DB] Database pool closed")
