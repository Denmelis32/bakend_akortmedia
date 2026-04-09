"""
Connection pool wrapper with context manager
Синхронная версия для AWS Lambda
"""
from contextlib import contextmanager
from typing import Optional, Dict, Any, List, Iterator
import ydb
import os
from app.middleware.logging import logger
import traceback


class YDBPool:
    """
    Синхронный пул соединений YDB для AWS Lambda
    """
    _instance = None
    _driver: Optional[ydb.Driver] = None
    _pool: Optional[ydb.SessionPool] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize(self):
        """Инициализация драйвера и пула сессий (синхронно)"""
        if self._driver is not None:
            return
        
        try:
            logger.info("🔄 Initializing YDB connection pool (sync)...")
            
            # Получаем параметры из окружения
            endpoint = os.environ.get('YDB_ENDPOINT')
            database = os.environ.get('YDB_DATABASE')
            key_file = os.environ.get('YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS')
            
            logger.info(f"📦 Endpoint from env: {endpoint}")
            logger.info(f"📦 Database from env: {database}")
            logger.info(f"📦 Key file from env: {key_file}")
            
            # Проверяем все переменные окружения YDB
            all_env = {k: v for k, v in os.environ.items() if 'YDB' in k}
            logger.info(f"📦 All YDB env vars: {all_env}")
            
            if not endpoint:
                raise ValueError("YDB_ENDPOINT not set")
            if not database:
                raise ValueError("YDB_DATABASE not set")
            
            # Проверяем, существует ли файл ключа
            if key_file:
                if os.path.exists(key_file):
                    logger.info(f"✅ Key file exists at: {key_file}")
                    # Покажем первые 100 символов ключа для проверки
                    try:
                        with open(key_file, 'r') as f:
                            content = f.read().strip()
                            logger.info(f"📄 Key file preview: {content[:100]}...")
                    except Exception as e:
                        logger.error(f"❌ Cannot read key file: {e}")
                else:
                    logger.error(f"❌ Key file not found: {key_file}")
            else:
                logger.warning("⚠️ YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS not set")
            
            logger.info(f"📦 Connecting to {endpoint}{database}")
            
            # Создаем синхронный драйвер
            self._driver = ydb.Driver(
                endpoint=endpoint,
                database=database,
                credentials=ydb.credentials_from_env_variables(),
            )
            
            # Ждем готовности драйвера
            logger.info("⏳ Waiting for driver to become ready...")
            self._driver.wait(timeout=30)
            logger.info("✅ YDB driver initialized")
            
            # Создаем пул сессий
            self._pool = ydb.SessionPool(
                self._driver,
                size=50,
            )
            
            logger.info(f"✅ YDB session pool initialized (size: 50)")
            
            # Проверяем соединение
            self._check_connection()
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize YDB pool: {e}")
            logger.error(traceback.format_exc())
            raise
    
    def _check_connection(self):
        """Проверка соединения с БД"""
        try:
            with self.acquire() as session:
                result = session.transaction().execute("SELECT 1;", commit_tx=True)
                logger.info(f"✅ YDB connection test successful: {result}")
        except Exception as e:
            logger.error(f"❌ YDB connection test failed: {e}")
            raise
    
    @contextmanager
    def acquire(self) -> Iterator[ydb.Session]:
        """
        Получить сессию из пула (синхронный контекстный менеджер)
        """
        if self._pool is None:
            self.initialize()
        
        session = None
        try:
            session = self._pool.acquire()
            logger.debug(f"📊 Session acquired")
            yield session
        except Exception as e:
            logger.error(f"❌ Session error: {e}")
            raise
        finally:
            if session:
                self._pool.release(session)
                logger.debug("📊 Session released")
    
    def get_session(self) -> ydb.Session:
        """Получить сессию из пула"""
        if self._pool is None:
            self.initialize()
        return self._pool.acquire()
    
    def release_session(self, session):
        """Вернуть сессию в пул"""
        if self._pool and session:
            self._pool.release(session)
    
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """
        Выполнить запрос и вернуть результаты (синхронно)
        """
        import time
        start_time = time.time()
        
        with self.acquire() as session:
            try:
                logger.debug(f"\n🔍 YDB EXECUTE: {query[:200]}...")
                
                if params:
                    prepared_query = session.prepare(query)
                    result = session.transaction().execute(
                        prepared_query, 
                        params, 
                        commit_tx=True
                    )
                else:
                    result = session.transaction().execute(
                        query, 
                        commit_tx=True
                    )
                
                duration = time.time() - start_time
                if duration > 0.5:
                    logger.warning(f"🐢 Slow query ({duration:.3f}s): {query[:200]}")
                
                if result and len(result) > 0:
                    return result[0].rows if hasattr(result[0], 'rows') else []
                return []
                
            except Exception as e:
                logger.error(f"❌ Database error: {e}")
                logger.error(traceback.format_exc())
                raise
    
    def close(self):
        """Закрыть пул соединений"""
        if self._pool:
            self._pool.stop()
            logger.info("✅ Session pool closed")
        
        if self._driver:
            self._driver.stop()
            logger.info("✅ Driver closed")


# Глобальный экземпляр пула
_ydb_pool = None


def get_ydb_pool() -> YDBPool:
    """Получить глобальный экземпляр пула"""
    global _ydb_pool
    if _ydb_pool is None:
        _ydb_pool = YDBPool()
    return _ydb_pool


# Для совместимости
def get_db_pool() -> YDBPool:
    return get_ydb_pool()


# Глобальный экземпляр
ydb_pool = get_ydb_pool()


def init_db_pool():
    """Инициализировать пул соединений (синхронно)"""
    ydb_pool.initialize()
    return ydb_pool
