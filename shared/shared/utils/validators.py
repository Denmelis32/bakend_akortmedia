"""
Валидаторы для входных данных
Проверка и очистка пользовательских данных
"""
import re
from typing import Any, Dict, List, Optional, Tuple, Callable
from shared.shared.utils.errors import ValidationError


class Validator:
    """Базовый класс для валидаторов"""
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Проверить корректность email"""
        if not email or not isinstance(email, str):
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def validate_username(username: str) -> Tuple[bool, Optional[str]]:
        """
        Проверить корректность username
        
        Returns:
            (is_valid, error_message)
        """
        if not username or not isinstance(username, str):
            return False, "Username is required"
        
        if len(username) < 3:
            return False, "Username must be at least 3 characters"
        
        if len(username) > 50:
            return False, "Username must be no more than 50 characters"
        
        if not re.match(r'^[a-zA-Z0-9_]+$', username):
            return False, "Username can only contain letters, numbers and underscores"
        
        return True, None
    
    @staticmethod
    def validate_password(password: str) -> Tuple[bool, Optional[str]]:
        """
        Проверить корректность пароля
        
        Returns:
            (is_valid, error_message)
        """
        if not password or not isinstance(password, str):
            return False, "Password is required"
        
        if len(password) < 8:
            return False, "Password must be at least 8 characters"
        
        if len(password) > 128:
            return False, "Password must be no more than 128 characters"
        
        # Проверяем наличие хотя бы одной цифры и буквы
        has_letter = any(c.isalpha() for c in password)
        has_digit = any(c.isdigit() for c in password)
        
        if not has_letter or not has_digit:
            return False, "Password must contain at least one letter and one digit"
        
        return True, None
    
    @staticmethod
    def validate_uuid(uuid_str: str) -> bool:
        """Проверить корректность UUID"""
        if not uuid_str or not isinstance(uuid_str, str):
            return False
        pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        return bool(re.match(pattern, uuid_str.lower()))
    
    @staticmethod
    def sanitize_string(value: str, max_length: int = 1000) -> str:
        """
        Очистить строку от опасных символов
        
        Args:
            value: Строка для очистки
            max_length: Максимальная длина
        
        Returns:
            Очищенная строка
        """
        if not value or not isinstance(value, str):
            return ""
        
        # Обрезаем до максимальной длины
        value = value[:max_length]
        
        # Удаляем control symbols кроме newline и tab
        value = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', value)
        
        return value.strip()
    
    @staticmethod
    def validate_pagination(page: Any, page_size: Any) -> Tuple[int, int]:
        """
        Проверить и нормализовать параметры пагинации
        
        Returns:
            (page, page_size) - нормализованные значения
        """
        try:
            page = int(page) if page is not None else 0
            page_size = int(page_size) if page_size is not None else 50
        except (ValueError, TypeError):
            raise ValidationError("Invalid pagination parameters")
        
        # Ограничиваем значения
        page = max(0, page)
        page_size = min(max(1, page_size), 100)  # Max 100 items per page
        
        return page, page_size
    
    @staticmethod
    def required(value: Any, field_name: str = "Field") -> Any:
        """Проверить что поле обязательно"""
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValidationError(f"{field_name} is required")
        return value
    
    @staticmethod
    def min_length(value: str, min_len: int, field_name: str = "Field") -> str:
        """Проверить минимальную длину строки"""
        if len(value) < min_len:
            raise ValidationError(f"{field_name} must be at least {min_len} characters")
        return value
    
    @staticmethod
    def max_length(value: str, max_len: int, field_name: str = "Field") -> str:
        """Проверить максимальную длину строки"""
        if len(value) > max_len:
            raise ValidationError(f"{field_name} must be no more than {max_len} characters")
        return value
    
    @staticmethod
    def in_range(value: Any, min_val: Any, max_val: Any, field_name: str = "Field") -> Any:
        """Проверить что значение в диапазоне"""
        if value < min_val or value > max_val:
            raise ValidationError(f"{field_name} must be between {min_val} and {max_val}")
        return value
    
    @staticmethod
    def one_of(value: Any, allowed_values: List[Any], field_name: str = "Field") -> Any:
        """Проверить что значение в списке разрешённых"""
        if value not in allowed_values:
            raise ValidationError(f"{field_name} must be one of: {', '.join(map(str, allowed_values))}")
        return value


# Глобальный экземпляр
validator = Validator()
