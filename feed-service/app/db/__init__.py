"""
Database package
Инициализация и экспорт компонентов БД
"""
from .pool import ydb_pool, init_db_pool, YDBPool
from .connection import YDBConnection, get_connection

# Для обратной совместимости - добавляем алиас db_pool
db_pool = ydb_pool  # ✅ теперь можно импортировать и как db_pool

# Экспортируем всё для удобства
__all__ = [
    # Из pool.py
    'ydb_pool',
    'db_pool',  # добавили алиас
    'init_db_pool',
    'YDBPool',
    
    # Из connection.py
    'YDBConnection',
    'get_connection'
]
