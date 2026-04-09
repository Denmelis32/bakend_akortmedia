"""
Конвертеры для работы с YDB типами данных
YDB использует микросекундные timestamp и специфичные форматы
"""
import json
from datetime import datetime, timezone
from typing import Any, Optional, Dict, List, Union


class YDBConverter:
    """Конвертация между Python и YDB типами"""
    
    @staticmethod
    def to_timestamp(dt: Optional[datetime]) -> Optional[int]:
        """
        Конвертировать datetime в YDB timestamp (микросекунды)
        YDB хранит timestamp как микросекунды с эпохи
        """
        if dt is None:
            return None
        if isinstance(dt, (int, float)):
            return int(dt)
        return int(dt.timestamp() * 1_000_000)
    
    @staticmethod
    def from_timestamp(ts: Optional[int]) -> Optional[datetime]:
        """
        Конвертировать YDB timestamp (микросекунды) в datetime
        """
        if ts is None:
            return None
        return datetime.fromtimestamp(ts / 1_000_000, tz=timezone.utc)
    
    @staticmethod
    def to_json(data: Optional[Any]) -> Optional[str]:
        """
        Конвертировать любой объект в JSON строку для YDB Json типа
        """
        if data is None:
            return None
        try:
            return json.dumps(data, ensure_ascii=False, default=str)
        except Exception:
            return '{}'
    
    @staticmethod
    def from_json(json_str: Optional[str]) -> Any:
        """
        Конвертировать JSON строку из YDB в Python объект
        """
        if json_str is None or json_str == '':
            return {} if json_str == '' else None
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return {}
        except Exception:
            return {}
    
    @staticmethod
    def to_date_int(dt: Optional[datetime]) -> Optional[int]:
        """
        Конвертировать datetime в YDB Date (целое число YYYYMMDD)
        """
        if dt is None:
            return None
        return int(dt.strftime('%Y%m%d'))
    
    @staticmethod
    def from_date_int(date_int: Optional[int]) -> Optional[datetime]:
        """
        Конвертировать YDB Date (YYYYMMDD) в datetime
        """
        if date_int is None:
            return None
        try:
            date_str = str(date_int)
            return datetime.strptime(date_str, '%Y%m%d').replace(tzinfo=timezone.utc)
        except:
            return None
    
    @staticmethod
    def to_uint64(value: Optional[Union[str, int]]) -> Optional[int]:
        """
        Конвертировать строку или число в Uint64 для БД
        
        Args:
            value: строка или число (например, ID пользователя)
            
        Returns:
            int: число для сохранения в БД или None
        """
        if value is None:
            return None
        try:
            # Если это строка, пробуем распарсить как int
            if isinstance(value, str):
                # Убираем возможные кавычки и пробелы
                value = value.strip().strip('"\'')
            return int(value)
        except (ValueError, TypeError) as e:
            print(f"⚠️ to_uint64 failed for '{value}': {e}")
            return None
    
    @staticmethod
    def from_uint64(value: Optional[int]) -> Optional[str]:
        """
        Конвертировать Uint64 из БД в строку
        
        Args:
            value: число из БД
            
        Returns:
            str: строковое представление числа или None
        """
        if value is None:
            return None
        return str(value)
    
    @staticmethod
    def to_uint32(value: Optional[int]) -> Optional[int]:
        """
        Конвертировать число в Uint32 с проверкой диапазона
        
        Args:
            value: число для конвертации
            
        Returns:
            int: число в диапазоне 0..4294967295 или None
        """
        if value is None:
            return None
        # Uint32 диапазон: 0 до 4294967295
        if value < 0:
            return 0
        if value > 4294967295:
            return 4294967295
        return value
    
    @staticmethod
    def from_uint32(value: Optional[int]) -> Optional[int]:
        """
        Конвертировать Uint32 из БД
        """
        return value
    
    @staticmethod
    def to_bool(value: Optional[Union[bool, int, str]]) -> Optional[bool]:
        """
        Конвертировать различные типы в bool
        """
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return bool(value)
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value)
    
    @staticmethod
    def from_bool(value: Optional[bool]) -> Optional[bool]:
        """
        Конвертировать bool из БД
        """
        return value


# Глобальный экземпляр для использования во всем приложении
converter = YDBConverter()


# Дополнительные утилиты для работы с датами
def utc_now() -> datetime:
    """Текущее время в UTC"""
    return datetime.now(timezone.utc)


def utc_now_timestamp() -> int:
    """Текущее время в микросекундах (YDB timestamp)"""
    return converter.to_timestamp(utc_now())


def format_datetime_iso(dt: Optional[datetime]) -> Optional[str]:
    """Форматировать datetime в ISO строку для API ответов"""
    if dt is None:
        return None
    return dt.isoformat()


def parse_datetime_iso(dt_str: Optional[str]) -> Optional[datetime]:
    """Распарсить ISO строку из API запроса в datetime"""
    if dt_str is None:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except:
        return None
