"""
Configuration management for Chat Service
Все переменные окружения из существующей системы
"""
import os
from typing import Optional
from datetime import timedelta

class Config:
    """
    Конфигурация приложения
    Все значения берутся из переменных окружения
    """
    
    def __init__(self):
        # YDB Configuration
        self.YDB_ENDPOINT = os.getenv('YDB_ENDPOINT', 'grpcs://ydb.serverless.yandexcloud.net:2135')
        self.YDB_DATABASE = os.getenv('YDB_DATABASE', '/ru-central1/b1gck8tib5cffvt263ca/etnees03efof77aup3kt')
        
        # Object Storage Configuration
        self.OBJECT_STORAGE_ENDPOINT = os.getenv('OBJECT_STORAGE_ENDPOINT', 'https://storage.yandexcloud.net')
        self.OBJECT_STORAGE_BUCKET = os.getenv('OBJECT_STORAGE_BUCKET', 'social-media-images')
        self.OBJECT_STORAGE_ACCESS_KEY = os.getenv('OBJECT_STORAGE_ACCESS_KEY', '')
        self.OBJECT_STORAGE_SECRET_KEY = os.getenv('OBJECT_STORAGE_SECRET_KEY', '')
        self.OBJECT_STORAGE_REGION = os.getenv('OBJECT_STORAGE_REGION', 'ru-central1')
        self.OBJECT_STORAGE_PUBLIC_URL = os.getenv('OBJECT_STORAGE_PUBLIC_URL', 'https://storage.yandexcloud.net/social-media-images')
        
        # JWT Configuration
        self.JWT_SECRET = os.getenv('JWT_SECRET', '')
        
        # Service Configuration
        self.DEFAULT_REGION = 'ru-central1'
        self.MAX_MEMBERS_DEFAULT = 100
        self.SLOW_MODE_DEFAULT = 0
        self.MESSAGE_TTL_DAYS = 365
        self.EVENT_TTL_DAYS = 90
        
        # Rate Limits
        self.RATE_LIMIT_CHAT_CREATE = 10  # per minute
        self.RATE_LIMIT_MESSAGE_SEND = 60  # per minute
        self.RATE_LIMIT_ATTACHMENT_UPLOAD = 20  # per minute
        self.RATE_LIMIT_AVATAR_UPLOAD = 10  # per minute 👈 НОВОЕ
        
        # Pagination
        self.DEFAULT_PAGE_SIZE = 50
        self.MAX_PAGE_SIZE = 200
        
        # Cache TTL
        self.CHAT_CACHE_TTL_SECONDS = 300  # 5 minutes
        self.PROFILE_CACHE_TTL_SECONDS = 300  # 5 minutes
        self.AVATAR_CACHE_TTL_SECONDS = 3600  # 1 hour 👈 НОВОЕ
        
        # 👇 НОВЫЙ БЛОК: Настройки для изображений
        self.MAX_AVATAR_SIZE = 5 * 1024 * 1024  # 5 MB
        self.MAX_AVATAR_WIDTH = 1024  # пикселей
        self.MAX_AVATAR_HEIGHT = 1024  # пикселей
        self.ALLOWED_AVATAR_FORMATS = ['image/jpeg', 'image/png', 'image/webp', 'image/gif']
        self.AVATAR_SIZES = {
            'original': None,  # оригинальный размер
            'large': (512, 512),  # большой
            'medium': (256, 256),  # средний
            'small': (128, 128),  # маленький
            'thumbnail': (64, 64)  # миниатюра
        }
        
        # Проверка обязательных переменных
        self._validate()
    
    def _validate(self):
        """Проверка обязательных переменных"""
        required_vars = [
            ('YDB_ENDPOINT', self.YDB_ENDPOINT),
            ('YDB_DATABASE', self.YDB_DATABASE),
            ('JWT_SECRET', self.JWT_SECRET),
            ('OBJECT_STORAGE_ACCESS_KEY', self.OBJECT_STORAGE_ACCESS_KEY),
            ('OBJECT_STORAGE_SECRET_KEY', self.OBJECT_STORAGE_SECRET_KEY),
        ]
        
        missing = []
        for name, value in required_vars:
            if not value or value == 'your-secret-key-change-in-production':
                missing.append(name)
        
        if missing:
            import warnings
            warnings.warn(f"Missing required environment variables: {missing}")
    
    def __repr__(self):
        return f"Config(region={self.DEFAULT_REGION})"
    
    @property
    def object_storage_url(self) -> str:
        """Получить базовый URL для Object Storage"""
        return f"{self.OBJECT_STORAGE_ENDPOINT}/{self.OBJECT_STORAGE_BUCKET}"
    
    def get_attachment_url(self, chat_id: int, filename: str) -> str:
        """Сформировать URL для вложения"""
        return f"{self.object_storage_url}/chats/{chat_id}/attachments/{filename}"
    
    def get_avatar_url(self, chat_id: int, filename: str, size: str = 'medium') -> str:
        """
        Сформировать URL для аватара чата с указанием размера
        Пример: /avatars/123/medium/abc123.jpg
        """
        if size not in self.AVATAR_SIZES:
            size = 'medium'
        return f"{self.object_storage_url}/chats/{chat_id}/avatars/{size}/{filename}"
    
    def get_avatar_upload_path(self, chat_id: int, filename: str, size: str = 'original') -> str:
        """Получить путь для загрузки аватара в Object Storage"""
        return f"chats/{chat_id}/avatars/{size}/{filename}"


# Глобальный экземпляр конфигурации
config = Config()
