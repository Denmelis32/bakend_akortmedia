"""
Аутентификация через JWT из существующей auth системы
Проверяет токены и извлекает информацию о пользователе
"""
import os
import jwt
import logging
from typing import Dict, Any, Optional
from shared.shared.utils.errors import AuthError, ForbiddenError

logger = logging.getLogger(__name__)


class AuthMiddleware:
    """
    Проверка JWT токенов и извлечение пользователя
    Интеграция с существующей auth системой
    """
    
    def __init__(self, jwt_secret: Optional[str] = None):
        self.jwt_secret = jwt_secret or os.environ.get('JWT_SECRET', '')
        self.jwt_algorithm = 'HS256'
        logger.info("✅ AuthMiddleware initialized")
    
    def verify_token(self, token: str) -> Dict[str, Any]:
        """
        Проверить токен и вернуть payload
        Выбрасывает AuthError если токен невалидный
        """
        try:
            payload = jwt.decode(
                token,
                self.jwt_secret,
                algorithms=[self.jwt_algorithm]
            )
            
            # Проверяем что это access token
            if payload.get('type') != 'access':
                logger.warning(f"Invalid token type: {payload.get('type')}")
                raise AuthError("Invalid token type")
            
            return payload
            
        except jwt.ExpiredSignatureError:
            raise AuthError("Token expired")
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {e}")
            raise AuthError(f"Invalid token: {str(e)}")
    
    def get_user_from_request(self, event: Dict) -> Dict[str, Any]:
        """
        Извлечь пользователя из запроса
        Ищет токен в Authorization header
        Возвращает dict с информацией о пользователе
        """
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        
        # Ищем токен в Authorization header
        auth_header = headers.get('authorization', '')
        
        if not auth_header:
            # Проверяем query параметры (для WebSocket или особых случаев)
            query = event.get('queryStringParameters', {}) or {}
            token = query.get('token')
            if token:
                auth_header = f"Bearer {token}"
        
        if not auth_header:
            raise AuthError("Authorization header required")
        
        # Парсим Bearer token
        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != 'bearer':
            raise AuthError("Invalid authorization header format. Use: Bearer <token>")
        
        token = parts[1]
        payload = self.verify_token(token)
        
        # Формируем username если его нет
        user_id = payload.get('sub')
        username = payload.get('username')
        if not username:
            username = f"user_{user_id[:8]}" if user_id else "unknown"
        
        # Формируем display_name из first_name или username
        first_name = payload.get('first_name', '')
        display_name = first_name if first_name else username
        
        user = {
            'user_id': user_id,
            'username': username,
            'first_name': first_name,
            'display_name': display_name,
            'role': payload.get('role', 'user'),
            'is_verified': payload.get('verified', False),
            'token_id': payload.get('jti'),
            'token_exp': payload.get('exp'),
        }
        
        # Валидируем наличие user_id
        if not user['user_id']:
            raise AuthError("Token missing user_id (sub claim)")
        
        logger.debug(f"✅ User authenticated: {user['user_id']} ({user['display_name']})")
        return user
    
    def require_role(self, user: Dict, required_role: str = 'admin') -> None:
        """
        Проверить что пользователь имеет необходимую роль
        Выбрасывает ForbiddenError если роль не подходит
        """
        if user.get('role') != required_role:
            logger.warning(
                f"Access denied: user {user.get('user_id')} "
                f"has role {user.get('role')}, required {required_role}"
            )
            raise ForbiddenError(f"{required_role.capitalize()} access required")
    
    def is_admin(self, user: Dict) -> bool:
        """Проверить является ли пользователь админом"""
        return user.get('role') == 'admin'
    
    def get_user_id(self, user: Dict) -> Optional[str]:
        """Получить ID пользователя из объекта user"""
        return user.get('user_id')
    
    def get_display_name(self, user: Dict) -> str:
        """Получить отображаемое имя пользователя"""
        return user.get('display_name', user.get('username', 'Unknown'))


# Глобальный экземпляр для использования во всем приложении
auth = AuthMiddleware()
