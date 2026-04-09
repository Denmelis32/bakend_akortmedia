"""
Утилиты для кэширования
LRU кэш с ограничением по времени и размеру
"""
import time
import threading
from typing import Any, Optional, Dict, Tuple
from functools import wraps
from collections import OrderedDict


class LRUCache:
    """
    LRU (Least Recently Used) кэш с ограничением по времени и размеру
    
    Args:
        max_size: Максимальное количество элементов в кэше
        ttl: Время жизни элемента в секундах (None = бесконечно)
    """
    
    def __init__(self, max_size: int = 1000, ttl: Optional[float] = None):
        self._cache: OrderedDict = OrderedDict()
        self._timestamps: Dict[str, float] = {}
        self._max_size = max_size
        self._ttl = ttl
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        """
        Получить значение из кэша
        
        Returns:
            Значение или None если ключ не найден или истёк TTL
        """
        with self._lock:
            # Проверяем существование ключа
            if key not in self._cache:
                self._misses += 1
                return None
            
            # Проверяем TTL
            if self._ttl is not None:
                age = time.time() - self._timestamps[key]
                if age > self._ttl:
                    self._remove(key)
                    self._misses += 1
                    return None
            
            # Перемещаем в конец (самый свежий)
            self._cache.move_to_end(key)
            self._hits += 1
            return self._cache[key]
    
    def set(self, key: str, value: Any) -> None:
        """
        Установить значение в кэш
        """
        with self._lock:
            # Если ключ уже существует, удаляем старое значение
            if key in self._cache:
                self._cache.move_to_end(key)
                self._cache[key] = value
            else:
                # Добавляем новый элемент
                self._cache[key] = value
                self._timestamps[key] = time.time()
                
                # Удаляем старые элементы если превышен размер
                while len(self._cache) > self._max_size:
                    oldest_key = next(iter(self._cache))
                    self._remove(oldest_key)
    
    def _remove(self, key: str) -> None:
        """Удалить элемент из кэша"""
        if key in self._cache:
            del self._cache[key]
        if key in self._timestamps:
            del self._timestamps[key]
    
    def delete(self, key: str) -> bool:
        """
        Удалить элемент из кэша
        
        Returns:
            True если элемент был удалён, False если не найден
        """
        with self._lock:
            if key in self._cache:
                self._remove(key)
                return True
            return False
    
    def clear(self) -> None:
        """Очистить весь кэш"""
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()
            self._hits = 0
            self._misses = 0
    
    def stats(self) -> Dict[str, Any]:
        """Получить статистику кэша"""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0.0
            return {
                'size': len(self._cache),
                'max_size': self._max_size,
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': round(hit_rate, 2),
                'ttl': self._ttl
            }
    
    def cleanup_expired(self) -> int:
        """
        Очистить просроченные элементы
        
        Returns:
            Количество удалённых элементов
        """
        if self._ttl is None:
            return 0
        
        with self._lock:
            now = time.time()
            expired_keys = [
                key for key, ts in self._timestamps.items()
                if now - ts > self._ttl
            ]
            
            for key in expired_keys:
                self._remove(key)
            
            return len(expired_keys)
    
    def __len__(self) -> int:
        return len(self._cache)
    
    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None


def cached(cache: LRUCache, key_prefix: str = ''):
    """
    Декоратор для кэширования результатов функции
    
    Args:
        cache: Экземпляр LRUCache для использования
        key_prefix: Префикс для ключей кэша
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Формируем ключ кэша из аргументов
            key_parts = [key_prefix, func.__name__]
            key_parts.extend(str(arg) for arg in args)
            key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
            cache_key = ':'.join(key_parts)
            
            # Пробуем получить из кэша
            result = cache.get(cache_key)
            if result is not None:
                return result
            
            # Вызываем функцию и сохраняем результат
            result = func(*args, **kwargs)
            cache.set(cache_key, result)
            return result
        
        return wrapper
    return decorator


# Глобальный кэш по умолчанию
default_cache = LRUCache(max_size=1000, ttl=300)
