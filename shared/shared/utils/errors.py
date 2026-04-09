"""
Кастомные ошибки для всего приложения
Стандартизированные коды ошибок для API
"""
from typing import Optional, Dict, Any


class AppError(Exception):
    """Базовый класс для всех ошибок приложения"""
    
    def __init__(self, message: str, code: str = 'internal_error', status_code: int = 500):
        self.message = message
        self.code = code
        self.status_code = status_code
        super().__init__(message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертировать в dict для JSON ответа"""
        return {
            'code': self.code,
            'message': self.message
        }


class ValidationError(AppError):
    """Ошибка валидации входных данных (400)"""
    
    def __init__(self, message: str):
        super().__init__(message, 'validation_error', 400)


class AuthError(AppError):
    """Ошибка аутентификации (401)"""
    
    def __init__(self, message: str = 'Authentication required'):
        super().__init__(message, 'auth_error', 401)


class ForbiddenError(AppError):
    """Ошибка доступа (403)"""
    
    def __init__(self, message: str = 'Access denied'):
        super().__init__(message, 'forbidden', 403)


class PermissionError(AppError):
    """Ошибка прав доступа (403)"""
    
    def __init__(self, message: str = 'Permission denied'):
        super().__init__(message, 'permission_denied', 403)


class NotFoundError(AppError):
    """Ресурс не найден (404)"""
    
    def __init__(self, message: str = 'Resource not found'):
        super().__init__(message, 'not_found', 404)


class RateLimitError(AppError):
    """Превышен лимит запросов (429)"""
    
    def __init__(self, message: str = 'Too many requests'):
        super().__init__(message, 'rate_limit', 429)


class ConflictError(AppError):
    """Конфликт данных (409)"""
    
    def __init__(self, message: str = 'Resource already exists'):
        super().__init__(message, 'conflict', 409)


class ServiceUnavailableError(AppError):
    """Сервис временно недоступен (503)"""
    
    def __init__(self, message: str = 'Service temporarily unavailable'):
        super().__init__(message, 'service_unavailable', 503)


def error_handler(e: Exception) -> Dict[str, Any]:
    """
    Обработчик ошибок для формирования ответа
    Возвращает структуру для response.error
    """
    if isinstance(e, AppError):
        return {
            'statusCode': e.status_code,
            'error': e.to_dict()
        }
    
    # Неожиданная ошибка
    return {
        'statusCode': 500,
        'error': {
            'code': 'internal_error',
            'message': 'Internal server error'
        }
    }
