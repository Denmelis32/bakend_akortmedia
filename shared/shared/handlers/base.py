"""
Базовый класс для всех хендлеров
Предоставляет общую функциональность для обработки запросов
"""
from typing import Dict, Any, Optional, Callable
from functools import wraps
import logging
import json

from shared.shared.utils.response import response
from shared.shared.utils.errors import AppError, AuthError
from shared.shared.middleware.auth import AuthMiddleware

logger = logging.getLogger(__name__)


class BaseHandler:
    """
    Базовый класс для всех хендлеров
    Предоставляет общие методы для обработки запросов
    """
    
    def __init__(self, auth_middleware: Optional[AuthMiddleware] = None):
        self.auth = auth_middleware or AuthMiddleware()
    
    def handle_request(self, event: Dict) -> Dict:
        """
        Обработать входящий запрос
        Должен быть переопределён в наследниках
        """
        raise NotImplementedError("Subclasses must implement handle_request")
    
    def authenticate(self, event: Dict) -> Dict[str, Any]:
        """
        Аутентифицировать пользователя из запроса
        Возвращает объект пользователя или выбрасывает AuthError
        """
        return self.auth.get_user_from_request(event)
    
    def require_auth(self, func: Callable) -> Callable:
        """
        Декоратор для требующих аутентификации методов
        """
        @wraps(func)
        def wrapper(event: Dict, *args, **kwargs) -> Dict:
            try:
                user = self.authenticate(event)
                kwargs['user'] = user
                return func(event, *args, **kwargs)
            except AuthError as e:
                logger.warning(f"Authentication failed: {e.message}")
                return response.error(e.message, e.code, e.status_code)
            except Exception as e:
                logger.exception(f"Unexpected error during authentication: {e}")
                return response.error("Authentication failed", "auth_error", 500)
        return wrapper
    
    def require_role(self, required_role: str) -> Callable:
        """
        Декоратор для проверки роли пользователя
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            @self.require_auth
            def wrapper(event: Dict, *args, **kwargs) -> Dict:
                user = kwargs.get('user')
                try:
                    self.auth.require_role(user, required_role)
                    return func(event, *args, **kwargs)
                except AppError as e:
                    return response.error(e.message, e.code, e.status_code)
            return wrapper
        return decorator
    
    def safe_handler(self, func: Callable) -> Callable:
        """
        Декоратор для безопасной обработки ошибок
        Автоматически перехватывает исключения и формирует ответ
        """
        @wraps(func)
        def wrapper(event: Dict, *args, **kwargs) -> Dict:
            try:
                return func(event, *args, **kwargs)
            except AppError as e:
                logger.warning(f"App error: {e.message} (code: {e.code})")
                return response.error(e.message, e.code, e.status_code)
            except Exception as e:
                logger.exception(f"Unexpected error: {e}")
                return response.error("Internal server error", "internal_error", 500)
        return wrapper
    
    def get_query_param(self, event: Dict, name: str, default: Any = None) -> Any:
        """Получить параметр из query string"""
        params = event.get('queryStringParameters', {}) or {}
        return params.get(name, default)
    
    def get_body(self, event: Dict) -> Optional[Dict]:
        """Получить и распарсить тело запроса"""
        body = event.get('body')
        if not body:
            return None
        
        try:
            # Проверяем если body уже dict (может быть при тестировании)
            if isinstance(body, dict):
                return body
            return json.loads(body) if isinstance(body, str) else None
        except (json.JSONDecodeError, SyntaxError, ValueError):
            return None
    
    def get_path_param(self, event: Dict, name: str, default: Any = None) -> Any:
        """Получить параметр из пути"""
        params = event.get('pathParameters', {}) or {}
        return params.get(name, default)
    
    def get_header(self, event: Dict, name: str, default: Any = None) -> Any:
        """Получить заголовок запроса"""
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        return headers.get(name.lower(), default)
