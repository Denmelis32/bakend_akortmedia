"""
Формирование HTTP ответов для Yandex Cloud Function
Соответствует формату, ожидаемому API Gateway
"""
import json
from typing import Any, Optional, Dict, List
from datetime import datetime


class ResponseBuilder:
    """Построитель HTTP ответов"""
    
    @staticmethod
    def _default_headers() -> Dict[str, str]:
        """Заголовки по умолчанию для всех ответов"""
        return {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Idempotency-Key',
            'Access-Control-Expose-Headers': 'Content-Type, X-Request-Id',
            'Cache-Control': 'no-store',
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY'
        }
    
    @staticmethod
    def success(data: Any = None, status_code: int = 200, 
                headers: Optional[Dict] = None) -> Dict:
        """
        Успешный ответ
        Формат: { "success": true, "data": ... }
        """
        body = {'success': True}
        if data is not None:
            body['data'] = data
        
        response_headers = ResponseBuilder._default_headers()
        if headers:
            response_headers.update(headers)
        
        return {
            'statusCode': status_code,
            'headers': response_headers,
            'body': json.dumps(body, default=str)
        }
    
    @staticmethod
    def error(message: str, code: str = 'error', status_code: int = 400,
              headers: Optional[Dict] = None) -> Dict:
        """
        Ответ с ошибкой
        Формат: { "success": false, "error": { "code": "...", "message": "..." } }
        """
        body = {
            'success': False,
            'error': {
                'code': code,
                'message': message
            }
        }
        
        response_headers = ResponseBuilder._default_headers()
        if headers:
            response_headers.update(headers)
        
        return {
            'statusCode': status_code,
            'headers': response_headers,
            'body': json.dumps(body, default=str)
        }
    
    @staticmethod
    def paginated(items: List, total: int, page: int = 0, 
                  page_size: int = 50, **kwargs) -> Dict:
        """
        Пагинированный ответ
        Формат: { "success": true, "data": { "items": [...], "pagination": {...} } }
        """
        data = {
            'items': items,
            'pagination': {
                'total': total,
                'page': page,
                'page_size': page_size,
                'total_pages': (total + page_size - 1) // page_size if page_size > 0 else 0
            }
        }
        
        # Добавляем дополнительные поля если есть
        data.update(kwargs)
        
        return ResponseBuilder.success(data=data)
    
    @staticmethod
    def options() -> Dict:
        """Ответ на OPTIONS запрос (CORS preflight)"""
        return {
            'statusCode': 200,
            'headers': ResponseBuilder._default_headers(),
            'body': ''
        }
    
    @staticmethod
    def redirect(url: str, status_code: int = 302) -> Dict:
        """Редирект"""
        return {
            'statusCode': status_code,
            'headers': {
                'Location': url,
                **ResponseBuilder._default_headers()
            },
            'body': ''
        }


# Глобальный экземпляр для использования во всем приложении
response = ResponseBuilder()
