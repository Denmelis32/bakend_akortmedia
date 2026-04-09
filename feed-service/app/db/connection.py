"""
YDB connection management
"""
from app.config import config
from app.middleware.logging import logger
import ydb
import ydb.aio
from .pool import ydb_pool


class YDBConnection:
    """
    Класс для управления подключением к YDB
    Использует глобальный пул соединений
    """
    
    def __init__(self):
        # Инициализируем пул при создании
        self._pool = ydb_pool
        logger.info("✅ YDBConnection initialized")
    
    async def initialize(self):
        """Явная инициализация (вызывается при старте)"""
        await self._pool.initialize()
    
    async def execute(self, query: str, params: dict = None) -> list:
        """Выполнить запрос"""
        return await self._pool.execute(query, params)
    
    async def execute_many(self, query: str, params_list: list) -> bool:
        """Выполнить множество запросов"""
        return await self._pool.execute_many(query, params_list)
    
    async def close(self):
        """Закрыть соединение"""
        await self._pool.close()


# Для обратной совместимости
def get_connection():
    """Получить соединение (для обратной совместимости)"""
    return YDBConnection()
