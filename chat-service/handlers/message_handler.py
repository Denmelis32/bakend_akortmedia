from config.config import config
"""
MESSAGE HANDLER v4.1 - ФИНАЛЬНАЯ ОПТИМИЗИРОВАННАЯ ВЕРСИЯ
- 1 HTTP запрос = 1 сессия через RequestContext
- Все методы хендлера используют RequestContext
- Все методы сервиса принимают session и передают в UOW
- Полностью сохранен весь функционал (40+ методов)
"""
import json
import re
import base64
import hashlib
import time
import uuid
import asyncio
import aiohttp
from utils.storage import storage
from PIL import Image
from enum import Enum
import secrets
from collections import deque, Counter
from typing import Dict, Any, Optional, List, Tuple, TYPE_CHECKING, Set,Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from utils.errors import AuthError
from middleware.auth import auth
from handlers.common import (
    BaseRepository, IdempotencyKey, IdempotencyRepository,
    ParticipantCache, to_timestamp, from_timestamp, to_uint64,
    safe_int, validate_idempotency_key, ResponseHelper, DatabaseError,
    BaseHandler, UnitOfWork, logger, cache, chunk_list,
    ValidationError, PermissionError, NotFoundError, RateLimitError,
    Validators, retry, rate_limit, measure_time, common_config,
    notification_worker, RequestContext,
    # 👇 НОВЫЕ ИМПОРТЫ ДЛЯ WEBSOCKET
    WebSocketManager, send_ws_notification, start_workers, stop_workers,idempotent 
)


from handlers.chat_handler import ChatRepository, ParticipantRepository
# ============================================
# КОНФИГУРАЦИЯ СООБЩЕНИЙ
# ============================================

@dataclass
class MessageConfig:
    """Конфигурация для сообщений"""
    MAX_CONTENT_LENGTH: int = 10000
    EDIT_TIME_LIMIT_HOURS: int = 24
    MAX_ATTACHMENTS: int = 10
    MAX_MENTIONS: int = 50
    MAX_BATCH_SIZE: int = 100
    MAX_PHOTOS_PER_MESSAGE: int = 10
    MAX_PHOTO_SIZE_MB: int = 20
    MAX_FORWARD_DEPTH: int = 5
    CACHE_TTL_PARTICIPANT: int = 600  # 10 минут
    CACHE_TTL_MESSAGE: int = 600  # 10 минут
    CACHE_TTL_PHOTO: int = 86400  # 24 часа
    ALLOWED_MESSAGE_TYPES: List[str] = field(default_factory=lambda: [
        'text', 'image', 'video', 'audio', 'file', 'location', 'contact', 'poll', 'album'
    ])
    ALLOWED_REACTIONS: List[str] = field(default_factory=lambda: [
        '👍', '❤️', '😊', '🎉', '😢', '😡', '👎', '🔥', '✅', '⭐'
    ])

message_config = MessageConfig()


# ============================================
# МОДЕЛИ СООБЩЕНИЙ (оптимизированные)
# ============================================

@dataclass
class WebSocketConfig:
    """Конфигурация для WebSocket сервера"""
    SERVER_URL: str = "http://10.130.0.7:8080"
    API_KEY: str = "ws-server-secret-key-2026"
    TIMEOUT: int = 10  # секунд

websocket_config = WebSocketConfig()

# ============================================
# IN-MEMORY CACHE (БЕЗ REDIS)
# ============================================

class MemoryCache:
    """Простой in-memory кэш для идемпотентности"""
    
    def __init__(self, max_size=10000, default_ttl=3600):
        self._cache = {}  # key -> (value, expires_at)
        self._max_size = max_size
        self._default_ttl = default_ttl
    
    async def get(self, key):
        """Получить значение"""
        if key in self._cache:
            value, expires = self._cache[key]
            if expires > time.time():
                return value
            else:
                del self._cache[key]
        return None
    
    async def set(self, key, value, ttl=None):
        """Установить значение"""
        # Проверка размера
        if len(self._cache) >= self._max_size:
            # Удаляем 20% самых старых
            sorted_items = sorted(self._cache.items(), key=lambda x: x[1][1])
            to_remove = len(self._cache) // 5
            for k, _ in sorted_items[:to_remove]:
                del self._cache[k]
        
        expires = time.time() + (ttl or self._default_ttl)
        self._cache[key] = (value, expires)
    
    async def delete(self, key):
        """Удалить ключ"""
        self._cache.pop(key, None)

# Глобальный экземпляр
memory_cache = MemoryCache(max_size=10000, default_ttl=3600)
class MessageType(str, Enum):
    TEXT = "text"
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    FILE = "file"
    LOCATION = "location"
    CONTACT = "contact"
    POLL = "poll"
    ALBUM = "album"


class PhotoStatus(str, Enum):
    PENDING = "pending"
    UPLOADING = "uploading"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Message:
    """Модель сообщения - оптимизированная версия с __slots__"""

    __slots__ = [
        'chat_id', 'message_id', 'sender_id', 'sender_role_at_time',
        'message_type', 'content', 'content_preview', 'reactions_json',
        'recent_reactions', 'mentions_json', 'mentions',
        'attachments_json', 'has_attachments', 'entities', 'reply_to_message_id',
        'reply_to_sender_id', 'reply_count', 'thread_root_id',
        'thread_messages_count', 'forwarded_from_message_id',
        'forwarded_from_chat_id', 'forwarded_by', 'forwarded_at',
        'forward_comment', 'forward_count', 'forwarded_original_sender_id',
        'forwarded_original_date', 'forwarded_original_sender_name',
        'linked_channel_message_id', 'linked_discussion_message_id',
        'reply_to_info', 'is_deleted', 'deleted_at', 'delete_reason',
        'is_edited', 'edit_count', 'last_edit_at', 'views_count',
        'created_at', 'version', 'edit_history', 'forwarded_from'
    ]

    def __init__(self, **kwargs):
        logger.info(f"🔨 Message.__init__ called for message_id: {kwargs.get('message_id')}")
        
        for key in self.__slots__:
            value = kwargs.get(key)
            setattr(self, key, value)
            
            # Логируем важные поля
            if key in ['message_id', 'mentions', 'mentions_json']:
                logger.info(f"  - {key}: {value}")

        # 👇 ЗНАЧЕНИЯ ПО УМОЛЧАНИЮ ДЛЯ ВСЕХ ПОЛЕЙ
        now = datetime.utcnow()
        
        # Дата создания
        if self.created_at is None:
            self.created_at = now
            logger.info(f"  - created_at set to default: {self.created_at}")
        
        # JSON поля
        if self.reactions_json is None:
            self.reactions_json = {}
        if self.recent_reactions is None:
            self.recent_reactions = []
        if self.mentions_json is None:
            self.mentions_json = []
        if self.mentions is None:
            self.mentions = []
        if self.attachments_json is None:
            self.attachments_json = []
        if self.entities is None:
            self.entities = {}
        if self.edit_history is None:
            self.edit_history = []
        if self.forwarded_from is None:
            self.forwarded_from = None

        # 👇 БУЛЕВЫ ПОЛЯ
        if self.is_deleted is None:
            self.is_deleted = False
        if self.is_edited is None:
            self.is_edited = False
        if self.has_attachments is None:
            self.has_attachments = False

        # 👇 ЧИСЛОВЫЕ ПОЛЯ (счетчики)
        if self.edit_count is None:
            self.edit_count = 0
        if self.reply_count is None:
            self.reply_count = 0
        if self.thread_messages_count is None:
            self.thread_messages_count = 0
        if self.views_count is None:
            self.views_count = 0
        if self.forward_count is None:
            self.forward_count = 0
        if self.version is None:
            self.version = 1

        # 👇 ДАТЫ (кроме created_at)
        if self.last_edit_at is None:
            self.last_edit_at = None
        if self.deleted_at is None:
            self.deleted_at = None
        if self.forwarded_at is None:
            self.forwarded_at = None
        if self.forwarded_original_date is None:
            self.forwarded_original_date = None

        # 👇 ID ПОЛЯ
        if self.reply_to_message_id is None:
            self.reply_to_message_id = None
        if self.reply_to_sender_id is None:
            self.reply_to_sender_id = None
        if self.thread_root_id is None:
            self.thread_root_id = None
        if self.forwarded_from_message_id is None:
            self.forwarded_from_message_id = None
        if self.forwarded_from_chat_id is None:
            self.forwarded_from_chat_id = None
        if self.forwarded_by is None:
            self.forwarded_by = None
        if self.forwarded_original_sender_id is None:
            self.forwarded_original_sender_id = None
        if self.forwarded_original_sender_name is None:
            self.forwarded_original_sender_name = None
        if self.linked_channel_message_id is None:
            self.linked_channel_message_id = None
        if self.linked_discussion_message_id is None:
            self.linked_discussion_message_id = None

        # 👇 СТРОКОВЫЕ ПОЛЯ
        if self.sender_role_at_time is None:
            self.sender_role_at_time = None
        if self.content is None:
            self.content = None
        if self.content_preview is None:
            self.content_preview = None
        if self.delete_reason is None:
            self.delete_reason = None
        if self.forward_comment is None:
            self.forward_comment = None

        logger.info(f"✅ Message.__init__ completed for {self.message_id}")

    def to_dict(self, user_role: str = 'member', chat_type: str = 'group',
                discussion_chat_id: Optional[int] = None) -> Dict:
        """Для API ответов с учетом типа чата и роли пользователя"""
        logger.info(f"📝 Message.to_dict called for message {self.message_id}")
        
        result = {
            'id': str(self.message_id),
            'chat_id': str(self.chat_id),
            'sender_id': self.sender_id,
            'type': self.message_type,
            'content': self.content,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'is_deleted': self.is_deleted,
            'is_edited': self.is_edited,
            'edit_count': self.edit_count,
            'last_edit_at': self.last_edit_at.isoformat() if self.last_edit_at else None,
            'reply_to': str(self.reply_to_message_id) if self.reply_to_message_id else None,
            'reply_count': self.reply_count,
            'thread_root_id': str(self.thread_root_id) if self.thread_root_id else None,
            'thread_messages_count': self.thread_messages_count,
            'has_attachments': self.has_attachments,
            'views': self.views_count,
            'version': self.version,
            'forward_count': self.forward_count,
            'reactions': self.reactions_json if self.reactions_json is not None else {},
            'recent_reactions': self.recent_reactions or [],
            'mentions': self.mentions or []
        }

        # Добавляем attachments
        if self.attachments_json:
            result['attachments'] = self.attachments_json
            photos = [a for a in self.attachments_json if a.get('type') == 'photo']
            if photos:
                result['photos'] = photos
                result['photos_count'] = len(photos)

        # Информация об обсуждении для каналов
        if chat_type == 'channel' and discussion_chat_id:
            result['discussion'] = {
                'chat_id': discussion_chat_id,
                'reply_count': self.reply_count,
                'has_comments': self.reply_count > 0
            }

        # Информация о пересылке
        if self.forwarded_from_message_id:
            result['forwarded_from'] = {
                'chat_id': str(self.forwarded_from_chat_id) if self.forwarded_from_chat_id else None,
                'message_id': str(self.forwarded_from_message_id),
                'original_sender_id': self.forwarded_original_sender_id,
                'original_sender_name': self.forwarded_original_sender_name,
                'original_date': self.forwarded_original_date.isoformat() if self.forwarded_original_date else None,
                'forwarded_by': self.forwarded_by,
                'forwarded_at': self.forwarded_at.isoformat() if self.forwarded_at else None,
                'comment': self.forward_comment
            }
        elif hasattr(self, 'forwarded_from') and self.forwarded_from:
            result['forwarded_from'] = self.forwarded_from

        # Информация об ответе
        if self.reply_to_info:
            result['reply_to_info'] = self.reply_to_info

        return result

    @classmethod
    def from_dict(cls, data: Dict) -> 'Message':
        """
        Создание сообщения из словаря (для кэша)
        """
        logger.info(f"📖 Message.from_dict called for message_id: {data.get('id')}")
        
        # Преобразуем id обратно в int
        message_id = None
        if data.get('id'):
            try:
                message_id = int(data['id'])
            except (ValueError, TypeError):
                message_id = data.get('id')
        
        chat_id = None
        if data.get('chat_id'):
            try:
                chat_id = int(data['chat_id'])
            except (ValueError, TypeError):
                chat_id = data.get('chat_id')
        
        # Преобразуем строковые даты обратно в datetime
        created_at = None
        if data.get('created_at'):
            try:
                created_at = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00'))
            except:
                created_at = None
        
        last_edit_at = None
        if data.get('last_edit_at'):
            try:
                last_edit_at = datetime.fromisoformat(data['last_edit_at'].replace('Z', '+00:00'))
            except:
                last_edit_at = None
        
        deleted_at = None
        if data.get('deleted_at'):
            try:
                deleted_at = datetime.fromisoformat(data['deleted_at'].replace('Z', '+00:00'))
            except:
                deleted_at = None
        
        forwarded_at = None
        if data.get('forwarded_at'):
            try:
                forwarded_at = datetime.fromisoformat(data['forwarded_at'].replace('Z', '+00:00'))
            except:
                forwarded_at = None
        
        forwarded_original_date = None
        if data.get('forwarded_original_date'):
            try:
                forwarded_original_date = datetime.fromisoformat(data['forwarded_original_date'].replace('Z', '+00:00'))
            except:
                forwarded_original_date = None
        
        # Преобразуем reply_to обратно в int
        reply_to = None
        if data.get('reply_to'):
            try:
                reply_to = int(data['reply_to'])
            except (ValueError, TypeError):
                reply_to = data.get('reply_to')
        
        thread_root_id = None
        if data.get('thread_root_id'):
            try:
                thread_root_id = int(data['thread_root_id'])
            except (ValueError, TypeError):
                thread_root_id = data.get('thread_root_id')
        
        # Создаем экземпляр сообщения
        message = cls(
            chat_id=chat_id,
            message_id=message_id,
            sender_id=data.get('sender_id'),
            sender_role_at_time=data.get('sender_role_at_time'),
            message_type=data.get('type', 'text'),
            content=data.get('content'),
            content_preview=data.get('content_preview'),
            reactions_json=data.get('reactions', {}),
            recent_reactions=data.get('recent_reactions', []),
            mentions_json=data.get('mentions', []),
            mentions=data.get('mentions', []),
            attachments_json=data.get('attachments', []),
            has_attachments=data.get('has_attachments', False),
            entities=data.get('entities', {}),
            reply_to_message_id=reply_to,
            reply_to_sender_id=data.get('reply_to_sender_id'),
            reply_count=data.get('reply_count', 0),
            thread_root_id=thread_root_id,
            thread_messages_count=data.get('thread_messages_count', 0),
            forwarded_from_message_id=data.get('forwarded_from_message_id'),
            forwarded_from_chat_id=data.get('forwarded_from_chat_id'),
            forwarded_by=data.get('forwarded_by'),
            forwarded_at=forwarded_at,
            forward_comment=data.get('forward_comment'),
            forward_count=data.get('forward_count', 0),
            forwarded_original_sender_id=data.get('forwarded_original_sender_id'),
            forwarded_original_date=forwarded_original_date,
            forwarded_original_sender_name=data.get('forwarded_original_sender_name'),
            linked_channel_message_id=data.get('linked_channel_message_id'),
            linked_discussion_message_id=data.get('linked_discussion_message_id'),
            is_deleted=data.get('is_deleted', False),
            deleted_at=deleted_at,
            delete_reason=data.get('delete_reason'),
            is_edited=data.get('is_edited', False),
            edit_count=data.get('edit_count', 0),
            last_edit_at=last_edit_at,
            views_count=data.get('views', 0),
            created_at=created_at,
            version=data.get('version', 1),
            edit_history=data.get('edit_history', []),
            forwarded_from=data.get('forwarded_from')
        )
        
        logger.info(f"✅ Message.from_dict completed for {message.message_id}")
        return message

    def to_db_row(self) -> Dict:
        """Для сохранения в БД"""
        logger.info(f"💾 Message.to_db_row called for {self.message_id}")
        
        # 👇 УБЕДИМСЯ, ЧТО ВСЕ ПОЛЯ ИМЕЮТ ЗНАЧЕНИЯ
        edit_count_value = int(self.edit_count) if self.edit_count is not None else 0
        reply_count_value = int(self.reply_count) if self.reply_count is not None else 0
        thread_messages_count_value = int(self.thread_messages_count) if self.thread_messages_count is not None else 0
        views_count_value = int(self.views_count) if self.views_count is not None else 0
        version_value = int(self.version) if self.version is not None else 1
        forward_count_value = int(self.forward_count) if self.forward_count is not None else 0

        # Вычисляем created_date
        if self.created_at:
            epoch = datetime(1970, 1, 1)
            created_date_value = (self.created_at.date() - epoch.date()).days
        else:
            now = datetime.utcnow()
            epoch = datetime(1970, 1, 1)
            created_date_value = (now.date() - epoch.date()).days

        # Сериализуем JSON поля
        reactions_json_value = json.dumps(self.reactions_json, ensure_ascii=False) if self.reactions_json else None
        recent_reactions_value = json.dumps(self.recent_reactions, ensure_ascii=False) if self.recent_reactions else None
        mentions_json_value = json.dumps(self.mentions_json, ensure_ascii=False) if self.mentions_json else None
        attachments_json_value = json.dumps(self.attachments_json, ensure_ascii=False) if self.attachments_json else None
        entities_value = json.dumps(self.entities, ensure_ascii=False) if self.entities else None
        edit_history_value = json.dumps(self.edit_history, ensure_ascii=False) if self.edit_history else None

        row = {
            'chat_id': self.chat_id,
            'message_id': self.message_id,
            'created_at': to_timestamp(self.created_at),
            'created_date': created_date_value,
            'sender_id': str(self.sender_id) if self.sender_id else None,
            'sender_role_at_time': self.sender_role_at_time,
            'message_type': self.message_type or 'text',
            'content': self.content,
            'content_preview': self.content_preview or (self.content[:200] if self.content else None),
            'content_search_vector': None,
            'reactions_json': reactions_json_value,
            'recent_reactions': recent_reactions_value,
            'mentions_json': mentions_json_value,
            'attachments_json': attachments_json_value,
            'has_attachments': self.has_attachments,
            'entities': entities_value,
            'reply_to_message_id': self.reply_to_message_id,
            'reply_to_sender_id': str(self.reply_to_sender_id) if self.reply_to_sender_id else None,
            'reply_count': reply_count_value,
            'thread_root_id': self.thread_root_id,
            'thread_messages_count': thread_messages_count_value,
            'forwarded_from_message_id': self.forwarded_from_message_id,
            'forwarded_from_chat_id': self.forwarded_from_chat_id,
            'is_deleted': self.is_deleted,
            'deleted_at': to_timestamp(self.deleted_at) if self.deleted_at else None,
            'delete_reason': self.delete_reason,
            'is_edited': self.is_edited,
            'edit_count': edit_count_value,
            'last_edit_at': to_timestamp(self.last_edit_at) if self.last_edit_at else None,
            'views_count': views_count_value,
            'version': version_value,
            'attachments': None,  # deprecated
            'edit_history': edit_history_value,
            'forwarded_by': str(self.forwarded_by) if self.forwarded_by else None,
            'forwarded_at': to_timestamp(self.forwarded_at) if self.forwarded_at else None,
            'forward_comment': self.forward_comment,
            'forward_count': forward_count_value,
            'forwarded_original_sender_id': str(self.forwarded_original_sender_id) if self.forwarded_original_sender_id else None,
            'forwarded_original_date': to_timestamp(self.forwarded_original_date) if self.forwarded_original_date else None,
            'forwarded_original_sender_name': self.forwarded_original_sender_name,
            'linked_channel_message_id': self.linked_channel_message_id,
            'linked_discussion_message_id': self.linked_discussion_message_id,
        }
        
        logger.info(f"✅ Message.to_db_row completed for {self.message_id}")
        return row

    @classmethod
    def from_db_row(cls, row: Dict) -> 'Message':
        """Создание сообщения из строки БД"""
        logger.info(f"📖 Message.from_db_row called")
        
        # Парсим mentions_json в список
        mentions = []
        if row.get('mentions_json'):
            try:
                mentions = json.loads(row.get('mentions_json'))
                logger.info(f"  ✅ Parsed mentions: {mentions}")
            except Exception as e:
                logger.error(f"  ❌ Failed to parse mentions_json: {e}")
        
        message = cls(
            chat_id=int(row.get('chat_id', 0)),
            message_id=int(row.get('message_id', 0)),
            sender_id=row.get('sender_id'),
            sender_role_at_time=row.get('sender_role_at_time'),
            message_type=row.get('message_type', 'text'),
            content=row.get('content'),
            content_preview=row.get('content_preview'),
            reactions_json=json.loads(row.get('reactions_json')) if row.get('reactions_json') else {},
            recent_reactions=json.loads(row.get('recent_reactions')) if row.get('recent_reactions') else [],
            mentions_json=mentions,
            mentions=mentions,
            attachments_json=json.loads(row.get('attachments_json')) if row.get('attachments_json') else [],
            has_attachments=row.get('has_attachments', False),
            entities=json.loads(row.get('entities')) if row.get('entities') else {},
            reply_to_message_id=int(row.get('reply_to_message_id')) if row.get('reply_to_message_id') else None,
            reply_to_sender_id=row.get('reply_to_sender_id'),
            reply_count=row.get('reply_count', 0),
            thread_root_id=int(row.get('thread_root_id')) if row.get('thread_root_id') else None,
            thread_messages_count=row.get('thread_messages_count', 0),
            forwarded_from_message_id=int(row.get('forwarded_from_message_id')) if row.get('forwarded_from_message_id') else None,
            forwarded_from_chat_id=int(row.get('forwarded_from_chat_id')) if row.get('forwarded_from_chat_id') else None,
            forwarded_by=row.get('forwarded_by'),
            forwarded_at=from_timestamp(row.get('forwarded_at')),
            forward_comment=row.get('forward_comment'),
            forward_count=row.get('forward_count', 0),
            forwarded_original_sender_id=row.get('forwarded_original_sender_id'),
            forwarded_original_date=from_timestamp(row.get('forwarded_original_date')),
            forwarded_original_sender_name=row.get('forwarded_original_sender_name'),
            linked_channel_message_id=row.get('linked_channel_message_id'),
            linked_discussion_message_id=row.get('linked_discussion_message_id'),
            is_deleted=row.get('is_deleted', False),
            deleted_at=from_timestamp(row.get('deleted_at')),
            delete_reason=row.get('delete_reason'),
            is_edited=row.get('is_edited', False),
            edit_count=row.get('edit_count', 0),
            last_edit_at=from_timestamp(row.get('last_edit_at')),
            views_count=row.get('views_count', 0),
            created_at=from_timestamp(row.get('created_at')),
            version=row.get('version', 1),
            edit_history=json.loads(row.get('edit_history')) if row.get('edit_history') else [],
            forwarded_from=None,
        )
        
        logger.info(f"✅ Message.from_db_row completed for {message.message_id}")
        return message

@dataclass
class MessageSearchResult:
    """Модель результата поиска сообщения"""
    __slots__ = [
        'message', 'chat_id', 'chat_title', 'chat_type',
        'sender_name', 'match_preview', 'match_score',
        'reply_to_message_id', 'reply_to_content'
    ]

    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))

    def to_dict(self) -> Dict:
        """Конвертация в словарь для API"""
        result = {
            'message': self.message.to_dict() if self.message else None,
            'chat_id': self.chat_id,
            'chat_title': self.chat_title,
            'chat_type': self.chat_type,
            'sender_name': self.sender_name,
            'match_preview': self.match_preview,
            'match_score': self.match_score,
        }

        if self.reply_to_message_id:
            result['reply_to'] = {
                'message_id': self.reply_to_message_id,
                'content_preview': self.reply_to_content
            }

        return {k: v for k, v in result.items() if v is not None}


@dataclass
class PhotoAttachment:
    """Модель фото с отслеживанием статуса"""

    __slots__ = [
        'photo_id', 'chat_id', 'user_id', 'status', 'progress',
        'url_original', 'url_small', 'url_medium', 'url_large',
        'caption', 'width', 'height', 'file_size', 'mime_type',
        'error_message', 'message_id', 'created_at', 'updated_at',
        'completed_at', 'content_hash'
    ]

    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))

        # Значения по умолчанию
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
        if self.progress is None:
            self.progress = 0
        if self.status is None:
            self.status = PhotoStatus.PENDING
        if self.content_hash is None:
            # Генерируем временный хеш, потом заменится на реальный
            self.content_hash = hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()

    def to_dict(self) -> Dict:
        """Для API ответов"""
        return {
            'photo_id': self.photo_id,
            'chat_id': self.chat_id,
            'status': self.status.value,
            'progress': self.progress,
            'urls': {
                'original': self.url_original,
                'small': self.url_small,
                'medium': self.url_medium,
                'large': self.url_large
            },
            'caption': self.caption,
            'width': self.width,
            'height': self.height,
            'file_size': self.file_size,
            'mime_type': self.mime_type,
            'error': self.error_message,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'completed_at': self.completed_at.isoformat() if self.completed_at else None
        }

    def to_db_row(self) -> Dict:
        """Для сохранения в БД"""
        return {
            'photo_id': self.photo_id,
            'chat_id': self.chat_id,
            'user_id': self.user_id,
            'message_id': self.message_id,
            'status': self.status.value,
            'progress': self.progress,
            'url_original': self.url_original,
            'url_small': self.url_small,
            'url_medium': self.url_medium,
            'url_large': self.url_large,
            'caption': self.caption,
            'width': self.width,
            'height': self.height,
            'file_size': self.file_size,
            'mime_type': self.mime_type,
            'error_message': self.error_message,
            'content_hash': self.content_hash,
            'created_at': to_timestamp(self.created_at),
            'updated_at': to_timestamp(self.updated_at),
            'completed_at': to_timestamp(self.completed_at) if self.completed_at else None
        }

    @classmethod
    def from_db_row(cls, row: Dict) -> 'PhotoAttachment':
        """Создание из строки БД"""
        return cls(
            photo_id=row.get('photo_id'),
            chat_id=row.get('chat_id'),
            user_id=row.get('user_id'),
            message_id=row.get('message_id'),
            status=PhotoStatus(row.get('status', 'pending')),
            progress=row.get('progress', 0),
            url_original=row.get('url_original'),
            url_small=row.get('url_small'),
            url_medium=row.get('url_medium'),
            url_large=row.get('url_large'),
            caption=row.get('caption'),
            width=row.get('width'),
            height=row.get('height'),
            file_size=row.get('file_size', 0),
            mime_type=row.get('mime_type', 'image/jpeg'),
            error_message=row.get('error_message'),
            content_hash=row.get('content_hash'),
            created_at=from_timestamp(row.get('created_at')),
            updated_at=from_timestamp(row.get('updated_at')),
            completed_at=from_timestamp(row.get('completed_at'))
        )


class Attachment:
    """Модель вложения"""

    __slots__ = [
        'attachment_id', 'message_id', 'chat_id', 'type', 'url',
        'preview_url', 'file_name', 'file_size', 'mime_type',
        'width', 'height', 'duration', 'is_processed',
        'processing_error', 'uploaded_by', 'uploaded_at', 'metadata'
    ]

    def __init__(
        self,
        attachment_id: Optional[str] = None,
        message_id: Optional[int] = None,
        chat_id: Optional[int] = None,
        type: str = 'file',
        url: Optional[str] = None,
        preview_url: Optional[str] = None,
        file_name: Optional[str] = None,
        file_size: Optional[int] = None,
        mime_type: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        duration: Optional[int] = None,
        is_processed: bool = False,
        processing_error: Optional[str] = None,
        uploaded_by: Optional[str] = None,
        uploaded_at: Optional[datetime] = None,
        metadata: Optional[Dict] = None,
    ):
        self.attachment_id = attachment_id or str(uuid.uuid4())
        self.message_id = message_id
        self.chat_id = chat_id
        self.type = type
        self.url = url
        self.preview_url = preview_url
        self.file_name = file_name
        self.file_size = file_size
        self.mime_type = mime_type
        self.width = width
        self.height = height
        self.duration = duration
        self.is_processed = is_processed
        self.processing_error = processing_error
        self.uploaded_by = uploaded_by
        self.uploaded_at = uploaded_at or datetime.utcnow()
        self.metadata = metadata or {}

    def to_dict(self) -> Dict:
        return {
            'attachment_id': self.attachment_id,
            'message_id': str(self.message_id) if self.message_id else None,
            'chat_id': str(self.chat_id) if self.chat_id else None,
            'type': self.type,
            'url': self.url,
            'preview_url': self.preview_url,
            'file_name': self.file_name,
            'file_size': self.file_size,
            'mime_type': self.mime_type,
            'width': self.width,
            'height': self.height,
            'duration': self.duration,
            'is_processed': self.is_processed,
            'uploaded_by': self.uploaded_by,
            'uploaded_at': self.uploaded_at.isoformat() if self.uploaded_at else None,
            'metadata': self.metadata
        }

    def to_db_row(self) -> Dict:
        return {
            'attachment_id': self.attachment_id,
            'message_id': self.message_id,
            'chat_id': self.chat_id,
            'type': self.type,
            'url': self.url,
            'preview_url': self.preview_url,
            'file_name': self.file_name,
            'file_size': self.file_size,
            'mime_type': self.mime_type,
            'width': self.width,
            'height': self.height,
            'duration': self.duration,
            'is_processed': self.is_processed,
            'processing_error': self.processing_error,
            'uploaded_by': self.uploaded_by,
            'uploaded_at': to_timestamp(self.uploaded_at),
            'metadata': json.dumps(self.metadata) if self.metadata else None,
        }

    @classmethod
    def from_db_row(cls, row: Dict) -> 'Attachment':
        return cls(
            attachment_id=row.get('attachment_id'),
            message_id=row.get('message_id'),
            chat_id=row.get('chat_id'),
            type=row.get('type', 'file'),
            url=row.get('url'),
            preview_url=row.get('preview_url'),
            file_name=row.get('file_name'),
            file_size=row.get('file_size'),
            mime_type=row.get('mime_type'),
            width=row.get('width'),
            height=row.get('height'),
            duration=row.get('duration'),
            is_processed=row.get('is_processed', False),
            processing_error=row.get('processing_error'),
            uploaded_by=row.get('uploaded_by'),
            uploaded_at=from_timestamp(row.get('uploaded_at')),
            metadata=json.loads(row.get('metadata')) if row.get('metadata') else None,
        )


@dataclass
class MessageReaction:
    """Модель реакции на сообщение"""
    chat_id: int
    message_id: int
    user_id: str
    reaction: str
    created_at: datetime

    def to_dict(self) -> Dict:
        return {
            'chat_id': self.chat_id,
            'message_id': self.message_id,
            'user_id': self.user_id,
            'reaction': self.reaction,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

    def to_db_row(self) -> Dict:
        return {
            'chat_id': self.chat_id,
            'message_id': self.message_id,
            'user_id': self.user_id,
            'reaction': self.reaction,
            'created_at': to_timestamp(self.created_at)
        }

    @classmethod
    def from_db_row(cls, row: Dict) -> 'MessageReaction':
        return cls(
            chat_id=row.get('chat_id'),
            message_id=row.get('message_id'),
            user_id=row.get('user_id'),
            reaction=row.get('reaction'),
            created_at=from_timestamp(row.get('created_at'))
        )


class Draft:
    """Модель черновика - оптимизированная"""

    __slots__ = ['chat_id', 'user_id', 'content', 'attachments',
                 'reply_to_message_id', 'entities', 'auto_save_content',
                 'auto_saved_at', 'updated_at', 'version']

    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))

        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
        if self.attachments is None:
            self.attachments = []
        if self.entities is None:
            self.entities = {}

    def to_dict(self) -> Dict:
        return {
            'chat_id': str(self.chat_id),
            'user_id': self.user_id,
            'content': self.content,
            'attachments': self.attachments,
            'reply_to': str(self.reply_to_message_id) if self.reply_to_message_id else None,
            'entities': self.entities,
            'auto_save_content': self.auto_save_content,
            'auto_saved_at': self.auto_saved_at.isoformat() if self.auto_saved_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'version': self.version
        }

    def to_db_row(self) -> Dict:
        chat_id_value = int(self.chat_id) if self.chat_id else 0
        reply_to_value = int(self.reply_to_message_id) if self.reply_to_message_id else None
        version_value = int(self.version) if self.version else 0

        return {
            'chat_id': chat_id_value,
            'user_id': str(self.user_id) if self.user_id else None,
            'content': self.content,
            'attachments': json.dumps(self.attachments) if self.attachments else None,
            'reply_to_message_id': reply_to_value,
            'entities': json.dumps(self.entities) if self.entities else None,
            'auto_save_content': self.auto_save_content,
            'auto_saved_at': to_timestamp(self.auto_saved_at) if self.auto_saved_at else None,
            'updated_at': to_timestamp(self.updated_at),
            'version': version_value,
        }

    @classmethod
    def from_db_row(cls, row: Dict) -> 'Draft':
        return cls(
            chat_id=row.get('chat_id'),
            user_id=row.get('user_id'),
            content=row.get('content'),
            attachments=json.loads(row.get('attachments')) if row.get('attachments') else None,
            reply_to_message_id=row.get('reply_to_message_id'),
            entities=json.loads(row.get('entities')) if row.get('entities') else None,
            auto_save_content=row.get('auto_save_content'),
            auto_saved_at=from_timestamp(row.get('auto_saved_at')),
            updated_at=from_timestamp(row.get('updated_at')),
            version=row.get('version', 0),
        )


class SavedMessage:
    """Модель сохраненного сообщения"""

    __slots__ = ['user_id', 'message_id', 'chat_id', 'saved_at', 'notes',
                 'collections', 'importance', 'message_exists', 'last_validated_at']

    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))

        if self.saved_at is None:
            self.saved_at = datetime.utcnow()
        if self.collections is None:
            self.collections = []
        self.importance = min(int(self.importance or 5), 255)

    def to_dict(self) -> Dict:
        return {
            'user_id': self.user_id,
            'message_id': str(self.message_id),
            'chat_id': str(self.chat_id),
            'saved_at': self.saved_at.isoformat() if self.saved_at else None,
            'notes': self.notes,
            'collections': self.collections,
            'importance': self.importance,
            'message_exists': self.message_exists
        }

    def to_db_row(self) -> Dict:
        return {
            'user_id': self.user_id,
            'message_id': self.message_id,
            'chat_id': self.chat_id,
            'saved_at': to_timestamp(self.saved_at),
            'notes': self.notes,
            'collections': json.dumps(self.collections) if self.collections else None,
            'importance': self.importance,
            'message_exists': self.message_exists,
            'last_validated_at': to_timestamp(self.last_validated_at) if self.last_validated_at else None,
        }

    @classmethod
    def from_db_row(cls, row: Dict) -> 'SavedMessage':
        return cls(
            user_id=str(row.get('user_id')),
            message_id=row.get('message_id'),
            chat_id=row.get('chat_id'),
            saved_at=from_timestamp(row.get('saved_at')),
            notes=row.get('notes'),
            collections=json.loads(row.get('collections')) if row.get('collections') else None,
            importance=row.get('importance', 5),
            message_exists=row.get('message_exists', True),
            last_validated_at=from_timestamp(row.get('last_validated_at')),
        )


@dataclass
class NotificationSettings:
    """Настройки уведомлений пользователя"""
    __slots__ = [
        'user_id', 'private_chats', 'groups', 'channels',
        'replies', 'join_requests', 'admin_alerts',
        'do_not_disturb', 'updated_at'
    ]

    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))

        if self.updated_at is None:
            self.updated_at = datetime.utcnow()

        # Убеждаемся, что булевы поля действительно булевы
        if self.replies is not None:
            self.replies = bool(self.replies)
        if self.join_requests is not None:
            self.join_requests = bool(self.join_requests)
        if self.admin_alerts is not None:
            self.admin_alerts = bool(self.admin_alerts)

    @classmethod
    def get_default(cls, user_id: str) -> 'NotificationSettings':
        """Настройки по умолчанию"""
        return cls(
            user_id=user_id,
            private_chats={'messages': 'all', 'sound': True, 'vibrate': True},
            groups={'messages': 'mentions', 'sound': True, 'vibrate': True},
            channels={'messages': 'none', 'sound': False, 'vibrate': False},
            replies=True,
            join_requests=True,
            admin_alerts=True,
            do_not_disturb={'enabled': False, 'from': '23:00', 'to': '08:00'},
            updated_at=datetime.utcnow()
        )

    def to_dict(self) -> Dict:
        return {
            'private_chats': self.private_chats,
            'groups': self.groups,
            'channels': self.channels,
            'replies': self.replies,
            'join_requests': self.join_requests,
            'admin_alerts': self.admin_alerts,
            'do_not_disturb': self.do_not_disturb,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

    def to_db_row(self) -> Dict:
        return {
            'user_id': self.user_id,
            'private_chats': json.dumps(self.private_chats) if self.private_chats else None,
            'groups': json.dumps(self.groups) if self.groups else None,
            'channels': json.dumps(self.channels) if self.channels else None,
            'replies': self.replies,
            'join_requests': self.join_requests,
            'admin_alerts': self.admin_alerts,
            'do_not_disturb': json.dumps(self.do_not_disturb) if self.do_not_disturb else None,
            'updated_at': to_timestamp(self.updated_at)
        }

    @classmethod
    def from_db_row(cls, row: Dict) -> 'NotificationSettings':
        return cls(
            user_id=row.get('user_id'),
            private_chats=json.loads(row.get('private_chats')) if row.get('private_chats') else {'messages': 'all'},
            groups=json.loads(row.get('groups')) if row.get('groups') else {'messages': 'mentions'},
            channels=json.loads(row.get('channels')) if row.get('channels') else {'messages': 'none'},
            replies=row.get('replies', True),
            join_requests=row.get('join_requests', True),
            admin_alerts=row.get('admin_alerts', True),
            do_not_disturb=json.loads(row.get('do_not_disturb')) if row.get('do_not_disturb') else {'enabled': False},
            updated_at=from_timestamp(row.get('updated_at'))
        )


class Contact:
    """Модель контакта"""

    __slots__ = ['user_id', 'contact_id', 'first_name', 'last_name', 'phone',
                 'added_at', 'source', 'is_favorite', 'is_blocked',
                 'last_interaction_at', 'last_message_preview']

    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))

        if self.added_at is None:
            self.added_at = datetime.utcnow()

    def to_dict(self) -> Dict:
        return {
            'user_id': self.user_id,
            'contact_id': self.contact_id,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'phone': self.phone,
            'added_at': self.added_at.isoformat() if self.added_at else None,
            'source': self.source,
            'is_favorite': self.is_favorite,
            'is_blocked': self.is_blocked,
            'last_interaction_at': self.last_interaction_at.isoformat() if self.last_interaction_at else None,
            'last_message_preview': self.last_message_preview
        }

    def to_db_row(self) -> Dict:
        user_id_value = to_uint64(self.user_id) if self.user_id else 0
        contact_id_value = to_uint64(self.contact_id) if self.contact_id else 0

        return {
            'user_id': user_id_value,
            'contact_id': contact_id_value,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'phone': self.phone,
            'added_at': to_timestamp(self.added_at),
            'source': self.source,
            'is_favorite': self.is_favorite,
            'is_blocked': self.is_blocked,
            'last_interaction_at': to_timestamp(self.last_interaction_at) if self.last_interaction_at else None,
            'last_message_preview': self.last_message_preview,
        }

    @classmethod
    def from_db_row(cls, row: Dict) -> 'Contact':
        return cls(
            user_id=str(row.get('user_id')),
            contact_id=str(row.get('contact_id')),
            first_name=row.get('first_name'),
            last_name=row.get('last_name'),
            phone=row.get('phone'),
            added_at=from_timestamp(row.get('added_at')),
            source=row.get('source', 'manual'),
            is_favorite=row.get('is_favorite', False),
            is_blocked=row.get('is_blocked', False),
            last_interaction_at=from_timestamp(row.get('last_interaction_at')),
            last_message_preview=row.get('last_message_preview'),
        )


@dataclass
class Notification:
    """Модель уведомления"""
    __slots__ = [
        'notification_id', 'user_id', 'type', 'chat_id', 'chat_title',
        'sender_id', 'sender_name', 'message_id', 'message_preview',
        'data', 'is_read', 'created_at', 'expires_at'
    ]

    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))

        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.expires_at is None:
            self.expires_at = datetime.utcnow() + timedelta(days=30)
        if self.is_read is None:
            self.is_read = False
        if self.notification_id is None:
            # Генерируем ID как в других моделях
            self.notification_id = int(time.time() * 1000) ^ (secrets.randbits(32))

    def to_dict(self) -> Dict:
        """Для API ответов"""
        return {
            'id': str(self.notification_id),
            'type': self.type,
            'chat_id': str(self.chat_id) if self.chat_id else None,
            'chat_title': self.chat_title,
            'sender_id': self.sender_id,
            'sender_name': self.sender_name,
            'message_id': str(self.message_id) if self.message_id else None,
            'message_preview': self.message_preview,
            'data': self.data or {},
            'is_read': self.is_read,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

    def to_db_row(self) -> Dict:
        """Для сохранения в БД"""
        data_value = json.dumps(self.data) if self.data else None

        return {
            'notification_id': self.notification_id,
            'user_id': self.user_id,
            'type': self.type,
            'chat_id': self.chat_id,
            'chat_title': self.chat_title,
            'sender_id': self.sender_id,
            'sender_name': self.sender_name,
            'message_id': self.message_id,
            'message_preview': self.message_preview,
            'data': data_value,
            'is_read': self.is_read,
            'created_at': to_timestamp(self.created_at),
            'expires_at': to_timestamp(self.expires_at)
        }

    @classmethod
    def from_db_row(cls, row: Dict) -> 'Notification':
        """Создание из строки БД"""
        return cls(
            notification_id=row.get('notification_id'),
            user_id=row.get('user_id'),
            type=row.get('type'),
            chat_id=row.get('chat_id'),
            chat_title=row.get('chat_title'),
            sender_id=row.get('sender_id'),
            sender_name=row.get('sender_name'),
            message_id=row.get('message_id'),
            message_preview=row.get('message_preview'),
            data=json.loads(row.get('data')) if row.get('data') else {},
            is_read=row.get('is_read', False),
            created_at=from_timestamp(row.get('created_at')),
            expires_at=from_timestamp(row.get('expires_at'))
        )


class Block:
    """Модель личной блокировки пользователя"""

    __slots__ = ['user_id', 'blocked_id', 'blocked_at', 'reason', 'expires_at']

    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))

        if self.blocked_at is None:
            self.blocked_at = datetime.utcnow()

    def to_dict(self) -> Dict:
        return {
            'user_id': self.user_id,
            'blocked_id': self.blocked_id,
            'blocked_at': self.blocked_at.isoformat() if self.blocked_at else None,
            'reason': self.reason,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None
        }

    def to_db_row(self) -> Dict:
        """Конвертация в строку для БД (маппинг полей)"""
        return {
            'blocker_id': self.user_id,        # user_id -> blocker_id
            'blocked_id': self.blocked_id,      # blocked_id остается
            'created_at': to_timestamp(self.blocked_at),  # blocked_at -> created_at
            'reason': self.reason,
            'expires_at': to_timestamp(self.expires_at) if self.expires_at else None,
        }

    @classmethod
    def from_db_row(cls, row: Dict) -> 'Block':
        """Создание модели из строки БД (обратный маппинг)"""
        return cls(
            user_id=row.get('blocker_id'),      # blocker_id -> user_id
            blocked_id=row.get('blocked_id'),
            blocked_at=from_timestamp(row.get('created_at')),  # created_at -> blocked_at
            reason=row.get('reason'),
            expires_at=from_timestamp(row.get('expires_at')),
        )


# ============================================
# ФОНОВЫЕ ЗАДАЧИ
# ============================================

class BackgroundTaskQueue:
    """Очередь для управления фоновыми задачами"""

    def __init__(self, max_size=1000):
        self.queue = deque(maxlen=max_size)
        self.processing = False
        self._lock = asyncio.Lock()

    async def add_task(self, coro):
        """Добавить задачу в очередь"""
        async with self._lock:
            self.queue.append(coro)
            if not self.processing:
                asyncio.create_task(self._process_queue())

    async def _process_queue(self):
        """Обработать очередь задач"""
        self.processing = True
        try:
            while self.queue:
                coro = self.queue.popleft()
                try:
                    await coro
                except Exception as e:
                    logger.error(f"Background task error: {e}")
                await asyncio.sleep(0.01)
        finally:
            self.processing = False


background_tasks = BackgroundTaskQueue()


# ============================================
# ОПТИМИЗИРОВАННЫЕ РЕПОЗИТОРИИ
# ============================================

class MessageRepository(BaseRepository):
    """Репозиторий для работы с сообщениями - оптимизированная версия"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "messages"

    @retry(max_attempts=3)
    async def create(self, message: Message) -> Optional[Message]:
        """Создать сообщение"""
        data = message.to_db_row()
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

        query = f"""
        {declare_block}
        INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f"${k}": v for k, v in data.items()}

        try:
            await self.execute(query, params)
            await cache.delete(f"message:{message.chat_id}:{message.message_id}")
            return message
        except Exception as e:
            logger.error(f"Error creating message: {e}")
            return None

    async def get_many(self, chat_id: int, message_ids: List[int]) -> Dict[int, Message]:
        """Получить несколько сообщений одним запросом"""
        if not message_ids:
            return {}

        # Используем UNION ALL для YDB
        unions = []
        params = {'$chat_id': chat_id}

        for i, msg_id in enumerate(message_ids):
            param_name = f"$msg_id_{i}"
            unions.append(f"SELECT * FROM {self.table_name} WHERE chat_id = $chat_id AND message_id = {param_name}")
            params[param_name] = msg_id

        query = " UNION ALL ".join(unions) + ";"
        declare_block = self._generate_declare(params)
        query = f"{declare_block}\n{query}"

        try:
            rows = await self.execute(query, params)
            result = {}
            for row in rows:
                msg = Message.from_db_row(row)
                result[msg.message_id] = msg
            return result
        except Exception as e:
            logger.error(f"Error in get_many: {e}")
            return {}

    async def get(self, chat_id: int, message_id: int, use_cache: bool = True) -> Optional[Message]:
        """Получить сообщение с поддержкой кэша"""
        logger.info(f"🔍 Getting message: chat_id={chat_id}, message_id={message_id}, use_cache={use_cache}")

        if use_cache:
            cache_key = f"message:{chat_id}:{message_id}"
            cached = await cache.get(cache_key)
            if cached:
                logger.info(f"✅ Cache hit for message {message_id}")
                if hasattr(Message, 'from_dict'):
                    return Message.from_dict(cached)
                else:
                    logger.warning(f"⚠️ Message.from_dict not available, returning None")
                    return None

        query = f"""
        DECLARE $chat_id AS Uint64; DECLARE $message_id AS Uint64;
        SELECT * FROM {self.table_name} WHERE chat_id = $chat_id AND message_id = $message_id;
        """
        params = {'$chat_id': chat_id, '$message_id': message_id}

        logger.info(f"📡 Executing query for message {message_id}")
        try:
            rows = await self.execute(query, params)
            if rows:
                logger.info(f"✅ Found message {message_id} in database")
                message = Message.from_db_row(rows[0])
                if use_cache:
                    logger.info(f"💾 Caching message {message_id}")
                    await cache.set(cache_key, message.to_dict(),
                                   ttl=message_config.CACHE_TTL_MESSAGE)
                return message
            else:
                logger.warning(f"❌ Message {message_id} not found in database")
                return None
        except Exception as e:
            logger.error(f"🔥 Error getting message {message_id}: {e}", exc_info=True)
            return None

    async def list_by_chat(
        self,
        chat_id: int,
        limit: int = 50,
        cursor: Optional[str] = None,
        before: Optional[int] = None,
        after: Optional[int] = None,
        include_deleted: bool = False,
        message_type: Optional[str] = None,
        sender_id: Optional[str] = None
    ) -> Tuple[List[Message], Optional[str]]:
        """Получить сообщения чата с пагинацией и фильтрацией"""

        limit = min(limit, message_config.MAX_BATCH_SIZE)

        conditions = ["chat_id = $chat_id"]
        params = {'$chat_id': chat_id, '$limit': limit + 1}

        if not include_deleted:
            conditions.append("(is_deleted = false OR is_deleted IS NULL)")

        if message_type:
            conditions.append("message_type = $message_type")
            params['$message_type'] = message_type

        if sender_id:
            conditions.append("sender_id = $sender_id")
            params['$sender_id'] = sender_id

        # Обработка курсора
        if cursor:
            try:
                cursor_time, cursor_id = cursor.split(':', 1)
                cursor_datetime = datetime.fromisoformat(cursor_time.replace('Z', ''))
                if cursor_datetime.tzinfo:
                    cursor_datetime = cursor_datetime.replace(tzinfo=None)

                conditions.append("(created_at, message_id) < ($cursor_time, $cursor_id)")
                params['$cursor_time'] = cursor_datetime
                params['$cursor_id'] = int(cursor_id)
            except Exception as e:
                logger.error(f"Error parsing cursor: {e}")

        if before:
            conditions.append("message_id < $before")
            params['$before'] = int(before)

        if after:
            conditions.append("message_id > $after")
            params['$after'] = int(after)

        where_clause = " AND ".join(conditions)
        declare_block = self._generate_declare(params)

        query = f"""
        {declare_block}
        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY created_at DESC, message_id DESC
        LIMIT $limit;
        """

        try:
            rows = await self.execute(query, params)

            has_next = len(rows) > limit
            if has_next:
                rows = rows[:limit]

            messages = [Message.from_db_row(row) for row in rows]

            next_cursor = None
            if has_next and messages:
                last = messages[-1]
                if last.created_at:
                    created_at_naive = last.created_at
                    if created_at_naive.tzinfo:
                        created_at_naive = created_at_naive.replace(tzinfo=None)
                    next_cursor = f"{created_at_naive.isoformat()}:{last.message_id}"

            return messages, next_cursor
        except Exception as e:
            logger.error(f"Error listing messages: {e}")
            return [], None

    async def list_by_thread(
        self,
        thread_root_id: int,
        limit: int = 50,
        cursor: Optional[str] = None
    ) -> Tuple[List[Message], Optional[str]]:
        """Получить сообщения треда"""

        limit = min(limit, message_config.MAX_BATCH_SIZE)

        conditions = ["thread_root_id = $thread_root_id AND is_deleted = false"]
        params = {'$thread_root_id': thread_root_id, '$limit': limit + 1}

        if cursor:
            try:
                cursor_time, cursor_id = cursor.split(':', 1)
                cursor_datetime = datetime.fromisoformat(cursor_time.replace('Z', ''))
                if cursor_datetime.tzinfo:
                    cursor_datetime = cursor_datetime.replace(tzinfo=None)

                conditions.append("(created_at, message_id) > ($cursor_time, $cursor_id)")
                params['$cursor_time'] = cursor_datetime
                params['$cursor_id'] = int(cursor_id)
            except Exception as e:
                logger.error(f"Error parsing thread cursor: {e}")

        where_clause = " AND ".join(conditions)
        declare_block = self._generate_declare(params)

        query = f"""
        {declare_block}
        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY created_at ASC, message_id ASC
        LIMIT $limit;
        """

        try:
            rows = await self.execute(query, params)

            has_next = len(rows) > limit
            if has_next:
                rows = rows[:limit]

            messages = [Message.from_db_row(row) for row in rows]

            next_cursor = None
            if has_next and messages:
                last = messages[-1]
                if last.created_at:
                    created_at_naive = last.created_at
                    if created_at_naive.tzinfo:
                        created_at_naive = created_at_naive.replace(tzinfo=None)
                    next_cursor = f"{created_at_naive.isoformat()}:{last.message_id}"

            return messages, next_cursor
        except Exception as e:
            logger.error(f"Error listing thread messages: {e}")
            return [], None

    async def update(self, message: Message) -> bool:
        """Обновить сообщение"""
        data = message.to_db_row()
        chat_id = data.pop('chat_id')
        message_id = data.pop('message_id')

        updatable_fields = [
            'content', 'content_preview', 'is_edited', 'edit_count', 'last_edit_at',
            'reactions_json', 'recent_reactions', 'mentions_json', 'attachments_json',
            'has_attachments', 'entities', 'reply_count', 'thread_messages_count',
            'is_deleted', 'deleted_at', 'delete_reason', 'views_count', 'version',
            'edit_history', 'linked_channel_message_id', 'linked_discussion_message_id'
        ]

        set_parts = []
        update_params = {'$chat_id': chat_id, '$message_id': message_id}

        for field in updatable_fields:
            if field in data:
                param_name = f"${field}"
                set_parts.append(f"{field} = {param_name}")
                update_params[param_name] = data[field]

        if not set_parts:
            return True

        set_clause = ", ".join(set_parts)
        declare_block = self._generate_declare(update_params)

        query = f"""
        {declare_block}
        UPDATE {self.table_name} SET {set_clause}
        WHERE chat_id = $chat_id AND message_id = $message_id;
        """

        try:
            await self.execute(query, update_params)
            await cache.delete(f"message:{chat_id}:{message_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating message: {e}")
            return False

    async def delete(self, chat_id: int, message_id: int, permanent: bool = False) -> bool:
        """Удалить сообщение"""
        if permanent:
            query = f"""
            DECLARE $chat_id AS Uint64;
            DECLARE $message_id AS Uint64;

            DELETE FROM {self.table_name}
            WHERE chat_id = $chat_id AND message_id = $message_id;
            """
            params = {'$chat_id': chat_id, '$message_id': message_id}
        else:
            query = f"""
            DECLARE $chat_id AS Uint64;
            DECLARE $message_id AS Uint64;
            DECLARE $deleted_at AS Timestamp;

            UPDATE {self.table_name}
            SET is_deleted = true,
                deleted_at = $deleted_at
            WHERE chat_id = $chat_id AND message_id = $message_id;
            """
            params = {
                '$chat_id': chat_id,
                '$message_id': message_id,
                '$deleted_at': to_timestamp(datetime.utcnow())
            }

        try:
            await self.execute(query, params)
            await cache.delete(f"message:{chat_id}:{message_id}")
            logger.info(f"✅ Message {message_id} deleted (permanent={permanent})")
            return True
        except Exception as e:
            logger.error(f"❌ Error deleting message: {e}")
            return False

    async def increment_view(self, chat_id: int, message_id: int) -> bool:
        """Увеличить счетчик просмотров"""
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $message_id AS Uint64;

        UPDATE {self.table_name}
        SET views_count = views_count + CAST(1 AS Uint32)
        WHERE chat_id = $chat_id AND message_id = $message_id;
        """
        params = {'$chat_id': chat_id, '$message_id': message_id}

        try:
            await self.execute(query, params)
            await cache.delete(f"message:{chat_id}:{message_id}")
            return True
        except Exception as e:
            logger.error(f"Error incrementing view: {e}")
            return False

    async def increment_reply_count(self, chat_id: int, message_id: int) -> bool:
        """Увеличить счетчик ответов"""
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $message_id AS Uint64;

        UPDATE {self.table_name}
        SET reply_count = reply_count + CAST(1 AS Uint32)
        WHERE chat_id = $chat_id AND message_id = $message_id;
        """
        params = {'$chat_id': chat_id, '$message_id': message_id}

        try:
            await self.execute(query, params)
            await cache.delete(f"message:{chat_id}:{message_id}")
            return True
        except Exception as e:
            logger.error(f"Error incrementing reply count: {e}")
            return False

    async def count_all_replies(self, chat_id: int, root_message_id: int) -> int:
        """Подсчитать ВСЕ ответы на сообщение"""
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $root_id AS Uint64;

        $level1 = SELECT message_id FROM {self.table_name}
                  WHERE chat_id = $chat_id AND reply_to_message_id = $root_id AND is_deleted = false;

        SELECT COUNT(*) as total FROM (
            SELECT message_id FROM {self.table_name}
            WHERE chat_id = $chat_id AND reply_to_message_id = $root_id AND is_deleted = false
            UNION ALL
            SELECT message_id FROM {self.table_name}
            WHERE chat_id = $chat_id AND reply_to_message_id IN $level1 AND is_deleted = false
        );
        """

        params = {'$chat_id': chat_id, '$root_id': root_message_id}

        try:
            rows = await self.execute(query, params)
            if rows and len(rows) > 0:
                return rows[0].get('total', 0)
            return 0
        except Exception as e:
            logger.error(f"Error counting replies: {e}")
            return 0

    async def search(self, chat_id: int, query_text: str, limit: int = 50) -> List[Message]:
        """Поиск по сообщениям"""
        search_pattern = f'%{self._escape_like(query_text)}%'

        query = f"""
        DECLARE $chat_id AS Uint64; DECLARE $search AS Utf8; DECLARE $limit AS Uint64;
        SELECT * FROM {self.table_name}
        WHERE chat_id = $chat_id AND is_deleted = false AND content LIKE $search
        ORDER BY created_at DESC LIMIT $limit;
        """
        params = {'$chat_id': chat_id, '$search': search_pattern, '$limit': limit}

        try:
            rows = await self.execute(query, params)
            return [Message.from_db_row(row) for row in rows]
        except Exception as e:
            logger.error(f"Error searching messages: {e}")
            return []


class PhotoUploadRepository(BaseRepository):
    """Репозиторий для отслеживания загрузки фото"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "photo_uploads"

    async def create(self, photo: PhotoAttachment) -> bool:
        """Создать запись о загрузке фото"""
        data = photo.to_db_row()

        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

        query = f"""
        {declare_block}
        INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f"${k}": v for k, v in data.items()}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to create photo record: {e}")
            return False

    async def update(self, photo: PhotoAttachment) -> bool:
        """Обновить статус фото"""
        data = photo.to_db_row()
        photo_id = data.pop('photo_id')

        set_parts = [f"{key} = ${key}" for key in data.keys()]
        set_clause = ", ".join(set_parts)

        params = {'$photo_id': photo_id}
        for k, v in data.items():
            params[f'${k}'] = v

        declare_block = self._generate_declare(params)

        query = f"""
        {declare_block}
        UPDATE {self.table_name} SET {set_clause}
        WHERE photo_id = $photo_id;
        """

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to update photo: {e}")
            return False

    async def get(self, photo_id: str) -> Optional[PhotoAttachment]:
        """Получить информацию о фото"""
        query = f"""
        DECLARE $photo_id AS Utf8;
        SELECT * FROM {self.table_name} WHERE photo_id = $photo_id;
        """
        params = {'$photo_id': photo_id}

        try:
            rows = await self.execute(query, params)
            if rows:
                return PhotoAttachment.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Failed to get photo: {e}")
            return None

    async def get_many(self, photo_ids: List[str]) -> Dict[str, PhotoAttachment]:
        """Получить несколько фото одним запросом (batch)"""
        if not photo_ids:
            return {}

        unions = []
        params = {}

        for i, photo_id in enumerate(photo_ids):
            param_name = f"$photo_id_{i}"
            unions.append(f"SELECT * FROM {self.table_name} WHERE photo_id = {param_name}")
            params[param_name] = photo_id

        query = " UNION ALL ".join(unions) + ";"
        declare_block = self._generate_declare(params)
        query = f"{declare_block}\n{query}"

        try:
            rows = await self.execute(query, params)
            result = {}
            for row in rows:
                photo = PhotoAttachment.from_db_row(row)
                result[photo.photo_id] = photo
            return result
        except Exception as e:
            logger.error(f"Error in get_many photos: {e}")
            return {}

    async def get_by_hash(self, content_hash: str) -> Optional[PhotoAttachment]:
        """Получить фото по хешу содержимого (для кэша)"""
        query = f"""
        DECLARE $content_hash AS Utf8;
        SELECT * FROM {self.table_name}
        WHERE content_hash = $content_hash AND status = 'completed'
        LIMIT 1;
        """
        params = {'$content_hash': content_hash}

        try:
            rows = await self.execute(query, params)
            if rows:
                return PhotoAttachment.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Failed to get photo by hash: {e}")
            return None

    async def list_by_chat(self, chat_id: int, limit: int = 50, status: Optional[str] = None) -> List[PhotoAttachment]:
        """Получить все фото чата с фильтром по статусу"""
        conditions = ["chat_id = $chat_id"]
        params = {'$chat_id': chat_id, '$limit': limit}

        if status:
            conditions.append("status = $status")
            params['$status'] = status

        where_clause = " AND ".join(conditions)

        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $limit AS Uint64;
        {self._generate_declare(params) if status else ''}

        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT $limit;
        """

        try:
            rows = await self.execute(query, params)
            return [PhotoAttachment.from_db_row(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to list photos: {e}")
            return []


class NotificationRepository(BaseRepository):
    """Репозиторий для работы с уведомлениями"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "notifications"

    async def create(self, notification: Notification) -> Optional[Notification]:
        """Создать уведомление"""
        try:
            data = notification.to_db_row()
            logger.debug(f"📝 Creating notification with data: {data}")

            columns = ", ".join(data.keys())
            placeholders = ", ".join([f"${key}" for key in data.keys()])
            declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

            query = f"""
            {declare_block}
            INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
            """

            params = {f"${k}": v for k, v in data.items()}

            await self.execute(query, params)
            logger.info(f"✅ Notification created successfully: {notification.notification_id}")
            return notification

        except Exception as e:
            logger.error(f"❌ Failed to create notification: {e}")
            return None

    async def get_by_user(
        self,
        user_id: str,
        limit: int = 50,
        offset: int = 0,
        unread_only: bool = False
    ) -> Tuple[List[Notification], int]:
        """Получить уведомления пользователя"""
        conditions = ["user_id = $user_id"]
        params = {'$user_id': user_id, '$limit': limit, '$offset': offset}

        if unread_only:
            conditions.append("is_read = false")

        where_clause = " AND ".join(conditions)

        # Сначала получаем общее количество
        count_query = f"""
        DECLARE $user_id AS Utf8;
        SELECT COUNT(*) as total FROM {self.table_name}
        WHERE {where_clause};
        """

        count_result = await self.execute(count_query, {'$user_id': user_id})
        total = count_result[0]['total'] if count_result else 0

        # Получаем данные
        data_query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;

        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT $limit OFFSET $offset;
        """

        rows = await self.execute(data_query, params)
        notifications = [Notification.from_db_row(row) for row in rows] if rows else []

        return notifications, total

    async def mark_as_read(self, notification_id: int, user_id: str) -> bool:
        """Отметить уведомление как прочитанное"""
        query = f"""
        DECLARE $notification_id AS Uint64;
        DECLARE $user_id AS Utf8;

        UPDATE {self.table_name}
        SET is_read = true
        WHERE notification_id = $notification_id AND user_id = $user_id;
        """

        params = {
            '$notification_id': notification_id,
            '$user_id': user_id
        }

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to mark notification as read: {e}")
            return False

    async def mark_all_as_read(self, user_id: str) -> int:
        """Отметить все уведомления как прочитанные"""
        query = f"""
        DECLARE $user_id AS Utf8;

        UPDATE {self.table_name}
        SET is_read = true
        WHERE user_id = $user_id AND is_read = false;
        """

        params = {'$user_id': user_id}

        try:
            await self.execute(query, params)
            # YDB не возвращает количество, поэтому сначала получим count
            count_query = f"""
            DECLARE $user_id AS Utf8;
            SELECT COUNT(*) as total FROM {self.table_name}
            WHERE user_id = $user_id AND is_read = false;
            """
            count_result = await self.execute(count_query, {'$user_id': user_id})
            return count_result[0]['total'] if count_result else 0
        except Exception as e:
            logger.error(f"Failed to mark all as read: {e}")
            return 0

    async def delete(self, notification_id: int, user_id: str) -> bool:
        """Удалить уведомление"""
        query = f"""
        DECLARE $notification_id AS Uint64;
        DECLARE $user_id AS Utf8;

        DELETE FROM {self.table_name}
        WHERE notification_id = $notification_id AND user_id = $user_id;
        """

        params = {
            '$notification_id': notification_id,
            '$user_id': user_id
        }

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to delete notification: {e}")
            return False

    async def get_unread_count(self, user_id: str) -> int:
        """Получить количество непрочитанных уведомлений"""
        query = f"""
        DECLARE $user_id AS Utf8;

        SELECT COUNT(*) as total FROM {self.table_name}
        WHERE user_id = $user_id AND is_read = false;
        """

        params = {'$user_id': user_id}

        try:
            result = await self.execute(query, params)
            return result[0]['total'] if result else 0
        except Exception as e:
            logger.error(f"Failed to get unread count: {e}")
            return 0

    async def cleanup_expired(self) -> int:
        """Удалить устаревшие уведомления"""
        query = f"""
        DECLARE $now AS Timestamp;

        DELETE FROM {self.table_name}
        WHERE expires_at < $now;
        """

        params = {'$now': to_timestamp(datetime.utcnow())}

        try:
            await self.execute(query, params)
            return 0  # YDB не возвращает количество
        except Exception as e:
            logger.error(f"Failed to cleanup expired notifications: {e}")
            return 0


class MessageReactionRepository(BaseRepository):
    """Репозиторий для работы с реакциями - с batch операциями"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "message_reactions"

    async def add_batch(self, reactions: List[MessageReaction]) -> bool:
        """Добавить несколько реакций одним запросом"""
        if not reactions:
            return True

        all_data = []
        for reaction in reactions:
            data = reaction.to_db_row()
            all_data.append(data)

        if all_data:
            success = await self.execute_many(
                f"UPSERT INTO {self.table_name} ({', '.join(all_data[0].keys())}) VALUES ({', '.join(['$' + k for k in all_data[0].keys()])})",
                all_data
            )
            return success

        return False

    async def add(self, reaction: MessageReaction) -> bool:
        """Добавить реакцию"""
        data = reaction.to_db_row()

        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f"${k}": v for k, v in data.items()}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"❌ Failed to add reaction: {e}")
            return False

    async def remove_batch(self, reactions: List[Tuple[int, int, str, str]]) -> bool:
        """Удалить несколько реакций одним запросом"""
        if not reactions:
            return True

        conditions = []
        params = {}

        for i, (chat_id, message_id, user_id, reaction) in enumerate(reactions):
            cond = f"(chat_id = $chat_id_{i} AND message_id = $message_id_{i} AND user_id = $user_id_{i} AND reaction = $reaction_{i})"
            conditions.append(cond)
            params[f"$chat_id_{i}"] = chat_id
            params[f"$message_id_{i}"] = message_id
            params[f"$user_id_{i}"] = user_id
            params[f"$reaction_{i}"] = reaction

        where_clause = " OR ".join(conditions)
        declare_block = self._generate_declare(params)

        query = f"""
        {declare_block}
        DELETE FROM {self.table_name}
        WHERE {where_clause};
        """

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to remove reactions batch: {e}")
            return False

    async def remove(self, chat_id: int, message_id: int, user_id: str, reaction: str) -> bool:
        """Удалить реакцию"""
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $message_id AS Uint64;
        DECLARE $user_id AS Utf8;
        DECLARE $reaction AS Utf8;

        DELETE FROM {self.table_name}
        WHERE chat_id = $chat_id
          AND message_id = $message_id
          AND user_id = $user_id
          AND reaction = $reaction;
        """

        params = {
            '$chat_id': chat_id,
            '$message_id': message_id,
            '$user_id': str(user_id),
            '$reaction': reaction
        }

        try:
            await self.execute(query, params)
            logger.debug(f"✅ Reaction removed: {user_id} removed {reaction} from message {message_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to remove reaction: {e}")
            return False

    async def get_stats_batch(self, items: List[Tuple[int, int]]) -> Dict[Tuple[int, int], Dict[str, int]]:
        """Получить статистику реакций для нескольких сообщений одним запросом"""
        if not items:
            return {}

        conditions = []
        params = {}

        for i, (chat_id, message_id) in enumerate(items):
            cond = f"(chat_id = $chat_id_{i} AND message_id = $message_id_{i})"
            conditions.append(cond)
            params[f"$chat_id_{i}"] = chat_id
            params[f"$message_id_{i}"] = message_id

        where_clause = " OR ".join(conditions)
        declare_block = self._generate_declare(params)

        query = f"""
        {declare_block}
        SELECT chat_id, message_id, reaction, COUNT(*) as count
        FROM {self.table_name}
        WHERE {where_clause}
        GROUP BY chat_id, message_id, reaction;
        """

        try:
            rows = await self.execute(query, params)
            result = {}
            for row in rows:
                key = (row['chat_id'], row['message_id'])
                if key not in result:
                    result[key] = {}
                result[key][row['reaction']] = row['count']
            return result
        except Exception as e:
            logger.error(f"Failed to get reaction stats batch: {e}")
            return {}

    async def get_users_by_reaction(self, chat_id: int, message_id: int, reaction: str, limit: int = 100) -> List[str]:
        """Получить список пользователей, поставивших конкретную реакцию"""
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $message_id AS Uint64;
        DECLARE $reaction AS Utf8;
        DECLARE $limit AS Uint64;

        SELECT user_id FROM {self.table_name}
        WHERE chat_id = $chat_id
          AND message_id = $message_id
          AND reaction = $reaction
        ORDER BY created_at DESC
        LIMIT $limit;
        """

        params = {
            '$chat_id': chat_id,
            '$message_id': message_id,
            '$reaction': reaction,
            '$limit': limit
        }

        try:
            rows = await self.execute(query, params)
            users = [row['user_id'] for row in rows] if rows else []
            logger.debug(f"Found {len(users)} users for reaction {reaction} on message {message_id}")
            return users
        except Exception as e:
            logger.error(f"❌ Failed to get users by reaction: {e}")
            return []

    async def get_user_reactions(self, chat_id: int, message_id: int, user_id: str) -> List[str]:
        """Получить список реакций пользователя"""
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $message_id AS Uint64;
        DECLARE $user_id AS Utf8;

        SELECT reaction FROM {self.table_name}
        WHERE chat_id = $chat_id
          AND message_id = $message_id
          AND user_id = $user_id
        ORDER BY created_at DESC;
        """

        params = {
            '$chat_id': chat_id,
            '$message_id': message_id,
            '$user_id': user_id
        }

        try:
            rows = await self.execute(query, params)
            return [row['reaction'] for row in rows]
        except Exception as e:
            logger.error(f"Failed to get user reactions: {e}")
            return []

    async def get_top_messages(self, chat_id: int, limit: int = 10) -> List[Dict]:
        """Получить топ сообщений по реакциям"""
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $limit AS Uint64;

        SELECT
            message_id,
            COUNT(*) as total_reactions,
            COUNT(DISTINCT user_id) as unique_users
        FROM {self.table_name}
        WHERE chat_id = $chat_id
        GROUP BY message_id
        ORDER BY total_reactions DESC
        LIMIT $limit;
        """

        params = {'$chat_id': chat_id, '$limit': limit}

        try:
            rows = await self.execute(query, params)
            return [
                {
                    'message_id': row['message_id'],
                    'total_reactions': row['total_reactions'],
                    'unique_users': row['unique_users']
                }
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Failed to get top messages: {e}")
            return []


class AttachmentRepository(BaseRepository):
    """Репозиторий для работы с вложениями"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "message_attachments"

    async def create(self, attachment: Attachment) -> Optional[Attachment]:
        data = attachment.to_db_row()

        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

        query = f"""
        {declare_block}
        INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f"${k}": v for k, v in data.items()}

        try:
            await self.execute(query, params)
            return attachment
        except Exception as e:
            logger.error(f"Error creating attachment: {e}")
            return None

    async def get(self, attachment_id: str) -> Optional[Attachment]:
        query = f"DECLARE $attachment_id AS Utf8; SELECT * FROM {self.table_name} WHERE attachment_id = $attachment_id;"
        params = {'$attachment_id': attachment_id}

        try:
            rows = await self.execute(query, params)
            if rows:
                return Attachment.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting attachment: {e}")
            return None

    async def list_by_message(self, message_id: int) -> List[Attachment]:
        query = f"DECLARE $message_id AS Uint64; SELECT * FROM {self.table_name} WHERE message_id = $message_id ORDER BY uploaded_at;"
        params = {'$message_id': message_id}

        try:
            rows = await self.execute(query, params)
            return [Attachment.from_db_row(row) for row in rows]
        except Exception as e:
            logger.error(f"Error listing attachments: {e}")
            return []

    async def update_message_id(self, attachment_id: str, message_id: int) -> bool:
        query = f"""
        DECLARE $attachment_id AS Utf8; DECLARE $message_id AS Uint64;
        UPDATE {self.table_name} SET message_id = $message_id WHERE attachment_id = $attachment_id;
        """
        params = {'$attachment_id': attachment_id, '$message_id': message_id}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error updating attachment message_id: {e}")
            return False

    async def delete(self, attachment_id: str) -> bool:
        query = f"DECLARE $attachment_id AS Utf8; DELETE FROM {self.table_name} WHERE attachment_id = $attachment_id;"
        params = {'$attachment_id': attachment_id}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error deleting attachment: {e}")
            return False


class DraftRepository(BaseRepository):
    """Репозиторий для работы с черновиками"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "message_drafts"

    async def save(self, draft: Draft) -> bool:
        """Сохранить черновик"""
        data = draft.to_db_row()

        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f"${k}": v for k, v in data.items()}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error saving draft: {e}")
            return False

    async def get(self, chat_id: int, user_id: str) -> Optional[Draft]:
        """Получить черновик"""
        query = f"""
        DECLARE $chat_id AS Uint64; DECLARE $user_id AS Utf8;
        SELECT * FROM {self.table_name} WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {'$chat_id': chat_id, '$user_id': user_id}

        try:
            rows = await self.execute(query, params)
            if rows:
                return Draft.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting draft: {e}")
            return None

    async def delete(self, chat_id: int, user_id: str) -> bool:
        """Удалить черновик"""
        query = f"""
        DECLARE $chat_id AS Uint64; DECLARE $user_id AS Utf8;
        DELETE FROM {self.table_name} WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {'$chat_id': chat_id, '$user_id': user_id}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error deleting draft: {e}")
            return False

    async def list_by_user(self, user_id: str, limit: int = 50, offset: int = 0) -> List[Draft]:
        """Получить все черновики пользователя"""
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;

        SELECT * FROM {self.table_name}
        WHERE user_id = $user_id
        ORDER BY updated_at DESC
        LIMIT $limit OFFSET $offset;
        """

        params = {'$user_id': user_id, '$limit': limit, '$offset': offset}

        try:
            rows = await self.execute(query, params)
            return [Draft.from_db_row(row) for row in rows] if rows else []
        except Exception as e:
            logger.error(f"Error listing drafts: {e}")
            return []


class MessageSearchRepository(BaseRepository):
    """Репозиторий для расширенного поиска сообщений"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "messages"

    def _escape_like(self, text: str) -> str:
        """Экранирование спецсимволов для LIKE"""
        return text.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

    def _split_query_into_words(self, query: str) -> List[str]:
        """Разбивает запрос на отдельные слова"""
        # Удаляем лишние пробелы и разбиваем
        words = re.findall(r'\w+', query.lower())
        # Оставляем слова длиннее 2 символов
        return [w for w in words if len(w) > 2]
    def _create_match_preview(self, content: Optional[str], search_words: List[str]) -> Optional[str]:
        """Создает превью с подсветкой найденных слов"""
        if not content:
            return None
        
        # Находим первое вхождение любого из слов
        content_lower = content.lower()
        first_pos = None
        matched_word = None
        
        for word in search_words:
            pos = content_lower.find(word.lower())
            if pos != -1 and (first_pos is None or pos < first_pos):
                first_pos = pos
                matched_word = word
        
        if first_pos is None:
            # Если слова не найдены, возвращаем начало
            return content[:200] + '...' if len(content) > 200 else content
        
        # Берем контекст вокруг найденного слова
        start = max(0, first_pos - 50)
        end = min(len(content), first_pos + 100)
        
        preview = content[start:end]
        
        # Добавляем многоточия если нужно
        if start > 0:
            preview = '...' + preview
        if end < len(content):
            preview = preview + '...'
        
        return preview
    async def search_messages(
        self,
        user_id: str,
        query: str,
        chat_id: Optional[int] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        sender_id: Optional[str] = None,
        message_type: Optional[str] = None,
        has_attachments: Optional[bool] = None,
        limit: int = 50,
        offset: int = 0,
        sort_by: str = 'relevance'
    ) -> Tuple[List[Dict], int]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - поиск по словам через массив
        """
        start_time = time.time()
        
        # Разбиваем запрос на слова
        search_words = [w.lower() for w in query.split() if len(w) > 2]
        if not search_words:
            return [], 0
        
        logger.info(f"🔍 Searching messages for: {search_words}")
        
        async with RequestContext() as ctx:
            # 👇 ПОЛУЧАЕМ ВСЕ ЧАТЫ ПОЛЬЗОВАТЕЛЯ (ОДИН РАЗ)
            from handlers.chat_handler import ParticipantRepository
            participant_repo = ParticipantRepository(ctx.session)
            participants = await participant_repo.list_by_user(user_id, limit=1000)
            user_chat_ids = [p.chat_id for p in participants[0]] if participants else []
            
            if not user_chat_ids:
                return [], 0
            
            # 👇 БАЗОВЫЕ УСЛОВИЯ
            conditions = ["is_deleted = false"]
            params = {}
            
            # Фильтр по чатам пользователя
            conditions.append(f"chat_id IN ({','.join([f'$chat_{i}' for i in range(len(user_chat_ids))])})")
            for i, cid in enumerate(user_chat_ids):
                params[f'$chat_{i}'] = cid
            
            # Дополнительные фильтры
            if chat_id:
                conditions.append("chat_id = $chat_id")
                params['$chat_id'] = chat_id
            
            if from_date:
                conditions.append("created_at >= $from_date")
                params['$from_date'] = to_timestamp(from_date)
            
            if to_date:
                conditions.append("created_at <= $to_date")
                params['$to_date'] = to_timestamp(to_date)
            
            if sender_id:
                conditions.append("sender_id = $sender_id")
                params['$sender_id'] = sender_id
            
            if message_type:
                conditions.append("message_type = $message_type")
                params['$message_type'] = message_type
            
            if has_attachments is not None:
                conditions.append("has_attachments = $has_attachments")
                params['$has_attachments'] = has_attachments
            
            # 👇 ПОИСК ПО СЛОВАМ ЧЕРЕЗ МАССИВ (если есть колонка content_words)
            # Альтернатива: создать колонку content_words Array<Utf8> и заполнять при сохранении
            
            word_conditions = []
            for i, word in enumerate(search_words):
                # Используем позиционные параметры для YDB
                param_name = f"$word_{i}"
                word_conditions.append(f"content LIKE {param_name}")
                params[param_name] = f'%{word}%'
            
            if word_conditions:
                conditions.append(f"({' OR '.join(word_conditions)})")
            
            # 👇 СОРТИРОВКА
            order_by = "created_at DESC"
            if sort_by == 'relevance':
                # Простая релевантность: чем больше слов найдено, тем выше
                relevance = " + ".join([f"CASE WHEN content LIKE $word_{i} THEN 1 ELSE 0 END" for i in range(len(search_words))])
                order_by = f"{relevance} DESC, created_at DESC"
            
            # 👇 ОСНОВНОЙ ЗАПРОС
            query_text = f"""
            DECLARE $limit AS Uint64;
            DECLARE $offset AS Uint64;
            {self._generate_declare(params)}
            
            SELECT *
            FROM `messages`
            WHERE {' AND '.join(conditions)}
            ORDER BY {order_by}
            LIMIT $limit OFFSET $offset;
            """
            
            params['$limit'] = limit
            params['$offset'] = offset
            
            rows = await self.execute(query_text, params)
            
            # Преобразуем в сообщения
            messages = []
            for row in rows:
                msg = Message.from_db_row(row)
                
                # Подсчитываем релевантность
                relevance = 0
                content_lower = (msg.content or '').lower()
                for word in search_words:
                    if word in content_lower:
                        relevance += 1
                
                # Создаем превью с подсветкой
                preview = self._create_match_preview(msg.content, search_words)
                
                messages.append({
                    'message': msg,
                    'relevance': relevance,
                    'match_preview': preview,
                    'chat_id': msg.chat_id,
                    'sender_id': msg.sender_id,
                    'created_at': msg.created_at
                })
            
            # 👇 ПОДСЧЕТ ОБЩЕГО КОЛИЧЕСТВА
            count_query = f"""
            {self._generate_declare(params)}
            SELECT COUNT(*) as total
            FROM `messages`
            WHERE {' AND '.join(conditions)};
            """
            
            # Убираем limit/offset из params для count
            count_params = {k:v for k,v in params.items() if k not in ['$limit', '$offset']}
            count_result = await self.execute(count_query, count_params)
            total = count_result[0]['total'] if count_result else 0
            
            duration = time.time() - start_time
            logger.info(f"✅ Found {len(messages)} messages in {duration*1000:.1f}ms")
            
            return messages, total

    def _create_match_preview(self, content: Optional[str], search_words: List[str]) -> Optional[str]:
        """Создает превью с подсветкой найденных слов"""
        if not content:
            return None

        # Находим первое вхождение любого из слов
        content_lower = content.lower()
        first_pos = None
        matched_word = None

        for word in search_words:
            pos = content_lower.find(word.lower())
            if pos != -1 and (first_pos is None or pos < first_pos):
                first_pos = pos
                matched_word = word

        if first_pos is None:
            # Если слова не найдены (маловероятно), возвращаем начало
            preview = content[:200] + '...' if len(content) > 200 else content
            return preview

        # Берем контекст вокруг найденного слова
        start = max(0, first_pos - 50)
        end = min(len(content), first_pos + 100)

        preview = content[start:end]

        # Добавляем многоточия если нужно
        if start > 0:
            preview = '...' + preview
        if end < len(content):
            preview = preview + '...'

        return preview


class SavedMessageRepository(BaseRepository):
    """Репозиторий для работы с сохраненными сообщениями"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "saved_messages"

    async def save(self, saved: SavedMessage) -> bool:
        """Сохранить сообщение"""
        data = saved.to_db_row()

        if 'user_id' in data and data['user_id']:
            data['user_id'] = to_uint64(data['user_id'])

        if 'importance' in data and data['importance'] is not None:
            data['importance'] = int(data['importance']) & 0xFF

        if 'collections' in data and isinstance(data['collections'], list):
            data['collections'] = json.dumps(data['collections'])

        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f"${k}": v for k, v in data.items()}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error saving message: {e}")
            return False

    async def get(self, user_id: str, message_id: int) -> Optional[SavedMessage]:
        """Получить сохраненное сообщение"""
        user_id_value = to_uint64(user_id) if user_id else 0

        query = f"""
        DECLARE $user_id AS Uint64;
        DECLARE $message_id AS Uint64;

        SELECT * FROM {self.table_name}
        WHERE user_id = $user_id AND message_id = $message_id;
        """
        params = {'$user_id': user_id_value, '$message_id': message_id}

        try:
            rows = await self.execute(query, params)
            if rows:
                return SavedMessage.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting saved message: {e}")
            return None

    async def delete(self, user_id: str, message_id: int) -> bool:
        """Удалить из сохраненных"""
        user_id_value = to_uint64(user_id) if user_id else 0

        query = f"""
        DECLARE $user_id AS Uint64;
        DECLARE $message_id AS Uint64;

        DELETE FROM {self.table_name}
        WHERE user_id = $user_id AND message_id = $message_id;
        """
        params = {'$user_id': user_id_value, '$message_id': message_id}

        try:
            rows = await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error deleting saved message: {e}")
            return False

    async def list_by_user(
        self,
        user_id: str,
        collection: Optional[str] = None,
        limit: int = 50,
        cursor: Optional[str] = None
    ) -> Tuple[List[SavedMessage], Optional[str]]:
        """Получить сохраненные сообщения пользователя"""

        limit = min(limit, 100)

        user_id_value = to_uint64(user_id) if user_id else 0

        conditions = ["user_id = $user_id"]
        params = {'$user_id': user_id_value, '$limit': limit + 1}

        if collection:
            collection_str = str(collection)
            conditions.append("collections LIKE $collection")
            params['$collection'] = f'%{self._escape_like(collection_str)}%'

        if cursor:
            try:
                cursor_time, cursor_id = cursor.split(':', 1)
                cursor_datetime = datetime.fromisoformat(cursor_time)
                if cursor_datetime.tzinfo:
                    cursor_datetime = cursor_datetime.replace(tzinfo=None)

                conditions.append("(saved_at, message_id) < ($cursor_time, $cursor_id)")
                params['$cursor_time'] = to_timestamp(cursor_datetime)
                params['$cursor_id'] = int(cursor_id)
            except Exception as e:
                logger.error(f"Error parsing cursor in saved messages: {e}")

        where_clause = " AND ".join(conditions)
        declare_block = self._generate_declare(params)

        query = f"""
        {declare_block}
        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY saved_at DESC, message_id DESC
        LIMIT $limit;
        """

        try:
            rows = await self.execute(query, params)

            has_next = len(rows) > limit
            if has_next:
                rows = rows[:limit]

            saved = [SavedMessage.from_db_row(row) for row in rows]

            next_cursor = None
            if has_next and saved:
                last = saved[-1]
                if last.saved_at:
                    saved_at_naive = last.saved_at
                    if saved_at_naive.tzinfo:
                        saved_at_naive = saved_at_naive.replace(tzinfo=None)
                    next_cursor = f"{saved_at_naive.isoformat()}:{last.message_id}"

            return saved, next_cursor
        except Exception as e:
            logger.error(f"Error listing saved messages: {e}")
            return [], None


class ContactRepository(BaseRepository):
    """Репозиторий для работы с контактами"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "contacts"

    async def create(self, contact: Contact) -> bool:
        """Создать новый контакт"""
        data = contact.to_db_row()

        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f"${k}": v for k, v in data.items()}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error creating contact: {e}")
            return False

    async def get(self, user_id: str, contact_id: str) -> Optional[Contact]:
        """Получить контакт"""
        user_id_value = to_uint64(user_id) if user_id else 0
        contact_id_value = to_uint64(contact_id) if contact_id else 0

        query = f"""
        DECLARE $user_id AS Uint64;
        DECLARE $contact_id AS Uint64;

        SELECT * FROM {self.table_name}
        WHERE user_id = $user_id AND contact_id = $contact_id;
        """
        params = {'$user_id': user_id_value, '$contact_id': contact_id_value}

        try:
            rows = await self.execute(query, params)
            if rows:
                return Contact.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting contact: {e}")
            return None

    async def update(self, contact: Contact) -> bool:
        """Обновить контакт"""
        data = contact.to_db_row()
        user_id = data.pop('user_id')
        contact_id = data.pop('contact_id')

        if not isinstance(user_id, int):
            user_id = to_uint64(str(user_id))
        if not isinstance(contact_id, int):
            contact_id = to_uint64(str(contact_id))

        set_parts = [f"{key} = ${key}" for key in data.keys()]
        set_clause = ", ".join(set_parts)

        params = {'$user_id': user_id, '$contact_id': contact_id}
        for k, v in data.items():
            params[f'${k}'] = v

        declare_block = self._generate_declare(params)

        query = f"""
        {declare_block}
        UPDATE {self.table_name} SET {set_clause}
        WHERE user_id = $user_id AND contact_id = $contact_id;
        """

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error updating contact: {e}")
            return False

    async def delete(self, user_id: str, contact_id: str) -> bool:
        """Удалить контакт"""
        user_id_value = to_uint64(user_id) if user_id else 0
        contact_id_value = to_uint64(contact_id) if contact_id else 0

        query = f"""
        DECLARE $user_id AS Uint64;
        DECLARE $contact_id AS Uint64;

        DELETE FROM {self.table_name}
        WHERE user_id = $user_id AND contact_id = $contact_id;
        """
        params = {'$user_id': user_id_value, '$contact_id': contact_id_value}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error deleting contact: {e}")
            return False

    async def list_by_user(
        self,
        user_id: str,
        favorites_only: bool = False,
        limit: int = 50,
        cursor: Optional[str] = None
    ) -> Tuple[List[Contact], Optional[str]]:
        """Получить список контактов пользователя"""

        limit = min(limit, 100)

        user_id_value = to_uint64(user_id) if user_id else 0

        conditions = ["user_id = $user_id"]
        params = {'$user_id': user_id_value, '$limit': limit + 1}

        if favorites_only:
            conditions.append("is_favorite = true")

        if cursor:
            try:
                cursor_time, cursor_id = cursor.split(':', 1)
                cursor_datetime = datetime.fromisoformat(cursor_time)
                if cursor_datetime.tzinfo:
                    cursor_datetime = cursor_datetime.replace(tzinfo=None)

                conditions.append("(added_at, contact_id) < ($cursor_time, $cursor_id)")
                params['$cursor_time'] = to_timestamp(cursor_datetime)
                params['$cursor_id'] = int(cursor_id)
            except Exception as e:
                logger.error(f"Error parsing cursor in contacts: {e}")

        where_clause = " AND ".join(conditions)
        declare_block = self._generate_declare(params)

        query = f"""
        {declare_block}
        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY added_at DESC, contact_id DESC
        LIMIT $limit;
        """

        try:
            rows = await self.execute(query, params)

            has_next = len(rows) > limit
            if has_next:
                rows = rows[:limit]

            contacts = [Contact.from_db_row(row) for row in rows]

            next_cursor = None
            if has_next and contacts:
                last = contacts[-1]
                if last.added_at:
                    added_at_naive = last.added_at
                    if added_at_naive.tzinfo:
                        added_at_naive = added_at_naive.replace(tzinfo=None)
                    next_cursor = f"{added_at_naive.isoformat()}:{last.contact_id}"

            return contacts, next_cursor
        except Exception as e:
            logger.error(f"Error listing contacts: {e}")
            return [], None

    async def search(self, user_id: str, query_text: str) -> List[Contact]:
        """Поиск по контактам"""
        user_id_value = to_uint64(user_id) if user_id else 0
        escaped_query = self._escape_like(query_text)
        search_pattern = f'%{escaped_query}%'

        query = f"""
        DECLARE $user_id AS Uint64;
        DECLARE $search AS Utf8;

        SELECT * FROM {self.table_name}
        WHERE user_id = $user_id
          AND (first_name LIKE $search OR last_name LIKE $search OR phone LIKE $search)
        ORDER BY is_favorite DESC, last_interaction_at DESC
        LIMIT 50;
        """
        params = {'$user_id': user_id_value, '$search': search_pattern}

        try:
            rows = await self.execute(query, params)
            return [Contact.from_db_row(row) for row in rows]
        except Exception as e:
            logger.error(f"Error searching contacts: {e}")
            return []

    async def update_last_interaction(self, user_id: str, contact_id: str, message_preview: str) -> bool:
        """Обновить время последнего взаимодействия"""
        user_id_value = to_uint64(user_id) if user_id else 0
        contact_id_value = to_uint64(contact_id) if contact_id else 0

        query = f"""
        DECLARE $user_id AS Uint64;
        DECLARE $contact_id AS Uint64;
        DECLARE $now AS Timestamp;
        DECLARE $preview AS Utf8;

        UPDATE {self.table_name}
        SET last_interaction_at = $now, last_message_preview = $preview
        WHERE user_id = $user_id AND contact_id = $contact_id;
        """
        params = {
            '$user_id': user_id_value,
            '$contact_id': contact_id_value,
            '$now': to_timestamp(datetime.utcnow()),
            '$preview': message_preview
        }

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error updating last interaction: {e}")
            return False


class BlockRepository(BaseRepository):
    """Репозиторий для таблицы user_blocks"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "user_blocks"

    async def create(self, block: Block) -> bool:
        """Создать блокировку"""
        data = block.to_db_row()

        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f"${k}": v for k, v in data.items()}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error creating block: {e}")
            return False

    async def get(self, user_id: str, blocked_id: str) -> Optional[Block]:
        """Получить блокировку"""
        query = f"""
        DECLARE $blocker_id AS Utf8;
        DECLARE $blocked_id AS Utf8;

        SELECT * FROM {self.table_name}
        WHERE blocker_id = $blocker_id AND blocked_id = $blocked_id;
        """
        params = {'$blocker_id': user_id, '$blocked_id': blocked_id}

        try:
            rows = await self.execute(query, params)
            if rows:
                return Block.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting block: {e}")
            return None

    async def delete(self, user_id: str, blocked_id: str) -> bool:
        """Удалить блокировку"""
        query = f"""
        DECLARE $blocker_id AS Utf8;
        DECLARE $blocked_id AS Utf8;

        DELETE FROM {self.table_name}
        WHERE blocker_id = $blocker_id AND blocked_id = $blocked_id;
        """
        params = {'$blocker_id': user_id, '$blocked_id': blocked_id}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error deleting block: {e}")
            return False

    async def list_by_user(self, user_id: str) -> List[Block]:
        """Получить список заблокированных пользователем"""
        query = f"""
        DECLARE $blocker_id AS Utf8;

        SELECT * FROM {self.table_name}
        WHERE blocker_id = $blocker_id
        ORDER BY created_at DESC;
        """
        params = {'$blocker_id': user_id}

        try:
            rows = await self.execute(query, params)
            return [Block.from_db_row(row) for row in rows]
        except Exception as e:
            logger.error(f"Error listing blocks: {e}")
            return []


class NotificationSettingsRepository(BaseRepository):
    """Репозиторий для настроек уведомлений"""

    def __init__(self, session=None):
        super().__init__(session)
        self.table_name = "notification_settings"

    async def get(self, user_id: str) -> Optional[NotificationSettings]:
        """Получить настройки пользователя"""
        query = f"""
        DECLARE $user_id AS Utf8;
        SELECT * FROM {self.table_name} WHERE user_id = $user_id;
        """
        params = {'$user_id': user_id}

        try:
            rows = await self.execute(query, params)
            if rows:
                return NotificationSettings.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Failed to get notification settings: {e}")
            return None

    async def save(self, settings: NotificationSettings) -> bool:
        """Сохранить настройки"""
        data = settings.to_db_row()

        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})

        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f"${k}": v for k, v in data.items()}

        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to save notification settings: {e}")
            return False


# ============================================
# UNIT OF WORK ДЛЯ СООБЩЕНИЙ
# ============================================

class MessageUnitOfWork(UnitOfWork):
    """Unit of Work для сообщений - координирует все репозитории"""
    
    def __init__(self):
        super().__init__()
        # 👇 Ленивая инициализация всех репозиториев
        self._messages = None
        self._photos = None
        self._reactions = None
        self._attachments = None
        self._drafts = None
        self._saved = None
        self._contacts = None
        self._blocks = None
        self._chats = None
        self._participants = None
        self._mentions = None
        self._notifications = None
        self._notification_settings = None

    @property
    def messages(self):
        """Репозиторий сообщений"""
        if self._messages is None:
            from handlers.message_handler import MessageRepository
            self._messages = self.register_repository(MessageRepository(self._session))
        return self._messages

    @property
    def photos(self):
        """Репозиторий фото"""
        if self._photos is None:
            from handlers.message_handler import PhotoUploadRepository
            self._photos = self.register_repository(PhotoUploadRepository(self._session))
        return self._photos

    @property
    def reactions(self):
        """Репозиторий реакций"""
        if self._reactions is None:
            from handlers.message_handler import MessageReactionRepository
            self._reactions = self.register_repository(MessageReactionRepository(self._session))
        return self._reactions

    @property
    def attachments(self):
        """Репозиторий вложений"""
        if self._attachments is None:
            from handlers.message_handler import AttachmentRepository
            self._attachments = self.register_repository(AttachmentRepository(self._session))
        return self._attachments

    @property
    def drafts(self):
        """Репозиторий черновиков"""
        if self._drafts is None:
            from handlers.message_handler import DraftRepository
            self._drafts = self.register_repository(DraftRepository(self._session))
        return self._drafts

    @property
    def saved(self):
        """Репозиторий сохраненных сообщений"""
        if self._saved is None:
            from handlers.message_handler import SavedMessageRepository
            self._saved = self.register_repository(SavedMessageRepository(self._session))
        return self._saved

    @property
    def contacts(self):
        """Репозиторий контактов"""
        if self._contacts is None:
            from handlers.message_handler import ContactRepository
            self._contacts = self.register_repository(ContactRepository(self._session))
        return self._contacts

    @property
    def blocks(self):
        """Репозиторий блокировок"""
        if self._blocks is None:
            from handlers.message_handler import BlockRepository
            self._blocks = self.register_repository(BlockRepository(self._session))
        return self._blocks

    @property
    def chats(self):
        """Репозиторий чатов"""
        if self._chats is None:
            from handlers.chat_handler import ChatRepository
            self._chats = self.register_repository(ChatRepository(self._session))
        return self._chats

    @property
    def participants(self):
        """Репозиторий участников"""
        if self._participants is None:
            from handlers.chat_handler import ParticipantRepository
            self._participants = self.register_repository(ParticipantRepository(self._session))
        return self._participants

    @property
    def notifications(self):
        """Репозиторий уведомлений"""
        if self._notifications is None:
            from handlers.message_handler import NotificationRepository
            self._notifications = self.register_repository(NotificationRepository(self._session))
        return self._notifications

    @property
    def notification_settings(self):
        """Репозиторий настроек уведомлений"""
        if self._notification_settings is None:
            from handlers.common import NotificationSettingsRepository
            self._notification_settings = self.register_repository(NotificationSettingsRepository(self._session))
        return self._notification_settings
# ============================================
# ВАЛИДАТОРЫ
# ============================================

class MessageValidator:
    """Валидатор для сообщений"""

    @classmethod
    def validate_send(
        cls,
        content: Optional[str],
        message_type: str,
        attachments: Optional[List],
        reply_to: Optional[str]
    ):
        if message_type not in message_config.ALLOWED_MESSAGE_TYPES:
            raise ValidationError(f"Invalid message type. Allowed: {', '.join(message_config.ALLOWED_MESSAGE_TYPES)}")

        if not content and not attachments:
            raise ValidationError("Either content or attachments is required")

        if message_type == 'text' and content:
            if len(content) > message_config.MAX_CONTENT_LENGTH:
                raise ValidationError(f"Message too long. Max length: {message_config.MAX_CONTENT_LENGTH}")

        if attachments:
            if len(attachments) > message_config.MAX_ATTACHMENTS:
                raise ValidationError(f"Too many attachments. Max: {message_config.MAX_ATTACHMENTS}")

    @classmethod
    def validate_edit(cls, new_content: str):
        if not new_content or not new_content.strip():
            raise ValidationError("Message content cannot be empty")

        if len(new_content) > message_config.MAX_CONTENT_LENGTH:
            raise ValidationError(f"Message too long. Max length: {message_config.MAX_CONTENT_LENGTH}")

    @classmethod
    def validate_reaction(cls, reaction: str):
        if not reaction:
            raise ValidationError("Reaction cannot be empty")

        if reaction not in message_config.ALLOWED_REACTIONS:
            raise ValidationError(f"Invalid reaction. Allowed: {', '.join(message_config.ALLOWED_REACTIONS)}")

    @classmethod
    def validate_mentions(cls, mentions: List[str]):
        if len(mentions) > message_config.MAX_MENTIONS:
            raise ValidationError(f"Too many mentions. Max: {message_config.MAX_MENTIONS}")

        for user_id in mentions:
            if not user_id or not isinstance(user_id, str):
                raise ValidationError(f"Invalid user_id format: {user_id}")


class AttachmentValidator:
    """Валидатор для вложений"""

    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB
    MAX_IMAGE_SIZE = 20 * 1024 * 1024   # 20 MB
    MAX_VIDEO_SIZE = 100 * 1024 * 1024  # 100 MB
    MAX_AUDIO_SIZE = 20 * 1024 * 1024   # 20 MB
    MAX_AUDIO_DURATION = 5 * 60  # 5 минут

    ALLOWED_IMAGE_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/svg+xml']
    ALLOWED_VIDEO_TYPES = ['video/mp4', 'video/webm', 'video/quicktime']
    ALLOWED_AUDIO_TYPES = ['audio/mpeg', 'audio/ogg', 'audio/wav', 'audio/aac', 'audio/opus']
    ALLOWED_DOCUMENT_TYPES = [
        'application/pdf', 'application/msword',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/vnd.ms-excel',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'text/plain', 'text/csv'
    ]

    @classmethod
    def validate_upload(cls, file_size: int, mime_type: str, file_type: str, duration: Optional[int] = None):
        if file_size > cls.MAX_FILE_SIZE:
            raise ValidationError(f"File too large. Max size: {cls.MAX_FILE_SIZE // (1024*1024)} MB")

        if file_type == 'image':
            if file_size > cls.MAX_IMAGE_SIZE:
                raise ValidationError(f"Image too large. Max size: {cls.MAX_IMAGE_SIZE // (1024*1024)} MB")
            if mime_type not in cls.ALLOWED_IMAGE_TYPES:
                raise ValidationError(f"Unsupported image type. Allowed: {', '.join(cls.ALLOWED_IMAGE_TYPES)}")

        elif file_type == 'video':
            if file_size > cls.MAX_VIDEO_SIZE:
                raise ValidationError(f"Video too large. Max size: {cls.MAX_VIDEO_SIZE // (1024*1024)} MB")
            if mime_type not in cls.ALLOWED_VIDEO_TYPES:
                raise ValidationError(f"Unsupported video type. Allowed: {', '.join(cls.ALLOWED_VIDEO_TYPES)}")

        elif file_type == 'audio':
            if file_size > cls.MAX_AUDIO_SIZE:
                raise ValidationError(f"Audio too large. Max size: {cls.MAX_AUDIO_SIZE // (1024*1024)} MB")
            if mime_type not in cls.ALLOWED_AUDIO_TYPES:
                raise ValidationError(f"Unsupported audio type. Allowed: {', '.join(cls.ALLOWED_AUDIO_TYPES)}")
            if duration and duration > cls.MAX_AUDIO_DURATION:
                raise ValidationError(f"Audio too long. Max duration: {cls.MAX_AUDIO_DURATION // 60} minutes")

        elif file_type == 'file':
            if mime_type not in cls.ALLOWED_DOCUMENT_TYPES:
                raise ValidationError(f"Unsupported document type")


class ContactValidator:
    """Валидатор для контактов"""

    @classmethod
    def validate_contact_id(cls, contact_id: str):
        if not contact_id or not isinstance(contact_id, str):
            raise ValidationError("Invalid contact ID")

    @classmethod
    def validate_phone(cls, phone: str):
        if not phone:
            return

        import re
        cleaned = re.sub(r'[\s\-\(\)]', '', phone)
        if not re.match(r'^\+?[0-9]{10,15}$', cleaned):
            raise ValidationError("Invalid phone number format")

    @classmethod
    def validate_email(cls, email: str):
        if not email:
            return

        import re
        if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
            raise ValidationError("Invalid email format")


class PhotoValidator:
    """Валидатор для фото"""

    @classmethod
    def validate_upload(cls, photo_data: str, idx: int):
        if not photo_data:
            raise ValidationError(f"Photo {idx+1}: missing data")

        if not photo_data.startswith('data:image'):
            raise ValidationError(f"Photo {idx+1}: must be base64 image")

        # Проверяем размер (приблизительно)
        base64_length = len(photo_data.split(',')[-1] if ',' in photo_data else photo_data)
        approx_size = (base64_length * 3) / 4

        if approx_size > message_config.MAX_PHOTO_SIZE_MB * 1024 * 1024:
            raise ValidationError(f"Photo {idx+1}: too large. Max {message_config.MAX_PHOTO_SIZE_MB}MB")


# ============================================
# СЕРВИС СООБЩЕНИЙ (ПОЛНЫЙ)
# ============================================
# ============================================
# СЕРВИС СООБЩЕНИЙ (ПОЛНЫЙ - ИСПРАВЛЕННЫЙ)
# ============================================

class MessageService:
    """Сервис для работы с сообщениями - полная оптимизированная версия"""

    def __init__(self):
        self.participant_cache = ParticipantCache()
        logger.info("✅ MessageService initialized (full optimized version)")

        # Хранилище для статусов "печатает" (в памяти)
        self._typing_status = {}  # {chat_id: {user_id: expiry_timestamp}}
        self._typing_lock = asyncio.Lock()
        self.typing_ttl = 10  # 10 секунд
        self._typing_cleanup_task = None
        self._typing_cleanup_started = False

        # Хранилище для online статусов
        self._online_users = {}      # user_id -> expiry_timestamp
        self._user_sessions = {}     # user_id -> количество активных сессий
        self._online_lock = asyncio.Lock()
        self.online_ttl = 60  # 60 секунд
        self._online_cleanup_task = None
        self._online_cleanup_started = False
    async def _notify_websocket(self, chat_id: int, event_type: str, data: Dict,
                                exclude_user_id: Optional[str] = None, session=None):
        """Отправить WebSocket уведомление всем подписанным на чат пользователям"""
        logger.info(f"🔔 [NOTIFY] Called: chat={chat_id}, event={event_type}, exclude={exclude_user_id}")
        try:
            # Отправляем всем подписанным пользователям через WebSocketManager
            sent = await WebSocketManager.send_to_chat(
                chat_id,
                {
                    'type': event_type,
                    'data': data,
                    'timestamp': datetime.utcnow().isoformat()
                },
                exclude_user_id=exclude_user_id
            )
            logger.info(f"🔔 [NOTIFY] Notifications sent to {sent} users for chat {chat_id}")
        except Exception as e:
            logger.error(f"🔔 [NOTIFY] Error: {e}", exc_info=True)

    async def _notify_typing(self, chat_id: int, user_id: str, is_typing: bool):
        """Отправить уведомление о статусе печатания всем подписанным на чат"""
        try:
            await WebSocketManager.send_to_chat(
                chat_id,
                {
                    'type': 'typing_status',
                    'data': {
                        'chat_id': chat_id,
                        'user_id': user_id,
                        'is_typing': is_typing,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                },
                exclude_user_id=user_id
            )
        except Exception as e:
            logger.error(f"❌ Failed to send typing notification: {e}")

    async def _notify_user_status(self, user_id: str, is_online: bool):
        """Уведомить о смене статуса пользователя всех, кто подписан на чаты с ним"""
        try:
            async with RequestContext() as ctx:
                # Получаем все чаты пользователя
                from handlers.chat_handler import ParticipantRepository
                participant_repo = ParticipantRepository(ctx.session)
                participants = await participant_repo.list_by_user(user_id, limit=1000)
                
                if isinstance(participants, tuple):
                    participants = participants[0]
                
                # Для каждого чата отправляем уведомление подписанным
                for p in participants:
                    await WebSocketManager.send_to_chat(
                        p.chat_id,
                        {
                            'type': 'user_status',
                            'data': {
                                'user_id': user_id,
                                'is_online': is_online,
                                'chat_id': p.chat_id,
                                'timestamp': datetime.utcnow().isoformat()
                            }
                        },
                        exclude_user_id=user_id
                    )
        except Exception as e:
            logger.error(f"❌ Failed to notify user status: {e}")
    # ========== МЕТОДЫ ДЛЯ УПРАВЛЕНИЯ ФОНОВЫМИ ЗАДАЧАМИ ==========
    async def _update_unread_counts_batch_with_session(
        self, 
        chat_id: int, 
        sender_id: str, 
        chat_type: str, 
        session
    ):
        """Batch обновление непрочитанных сообщений с переданной сессией"""
        if chat_type == 'channel':
            return
        
        participant_repo = ParticipantRepository(session)
        members = await participant_repo.list_by_chat(chat_id, limit=10000, active_only=True)
        
        for member in members:
            if member.user_id != sender_id and member.is_active:
                await participant_repo.increment_unread(chat_id, member.user_id)
                
                # Для личных диалогов - показываем скрытый
                if member.is_hidden:
                    await participant_repo.show_dialog(chat_id, member.user_id)
    async def get_message_with_sender(self, chat_id: int, message_id: int, user_id: str, session=None) -> Dict:
        """
        Получить сообщение с данными отправителя (включая аватар)
        """
        # Проверка доступа
        has_access = await self.participant_cache.is_member(chat_id, user_id, session=session)
        if not has_access:
            raise PermissionError("You don't have access to this chat")
        
        async with await MessageUnitOfWork.with_session(session) as uow:
            # Получаем сообщение
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                raise NotFoundError(f"Message {message_id} not found")
            
            if message.is_deleted:
                raise NotFoundError("Message has been deleted")
            
            # Получаем информацию об ответе
            if message.reply_to_message_id:
                reply_message = await uow.messages.get(chat_id, message.reply_to_message_id)
                if reply_message:
                    message.reply_to_info = {
                        'id': reply_message.message_id,
                        'sender_id': reply_message.sender_id,
                        'content_preview': reply_message.content[:150] + '...' if reply_message.content and len(reply_message.content) > 150 else reply_message.content,
                        'created_at': reply_message.created_at.isoformat() if reply_message.created_at else None,
                        'is_deleted': reply_message.is_deleted,
                        'message_type': reply_message.message_type
                    }
            
            # Получаем данные отправителя с аватаром
            message_dict = message.to_dict()
            
            if message.sender_id:
                users_query = """
                DECLARE $user_id AS Utf8;
                SELECT username, first_name_encrypted, avatar_url
                FROM `users`
                WHERE id = $user_id;
                """
                
                users_result = await uow._transaction.execute(
                    await uow._session.prepare(users_query),
                    {'$user_id': message.sender_id},
                    commit_tx=False
                )
                
                if users_result and users_result[0].rows:
                    row = users_result[0].rows[0]
                    first_name_enc = row.get('first_name_encrypted', '')
                    first_name = ''
                    if first_name_enc:
                        try:
                            import base64
                            first_name = base64.b64decode(first_name_enc).decode('utf-8')
                        except Exception as e:
                            logger.error(f"Base64 decode error: {e}")
                            first_name = first_name_enc
                    
                    # 👇 ИСПРАВЛЕНО: используем OBJECT_STORAGE_PUBLIC_URL
                    avatar_url = row.get('avatar_url')
                    if avatar_url and not avatar_url.startswith('http'):
                        from config.config import config
                        avatar_url = f"{config.OBJECT_STORAGE_PUBLIC_URL}/{avatar_url}"
                    
                    message_dict['sender'] = {
                        'username': row.get('username'),
                        'first_name': first_name,
                        'avatar_url': avatar_url
                    }
            
            # Увеличиваем счетчик просмотров в фоне
            if not message.is_deleted:
                asyncio.create_task(self._safe_increment_view(chat_id, message_id))
            
            return message_dict

    async def _filter_existing_users(self, user_ids: List[str]) -> List[str]:
        """Проверить, какие пользователи существуют в системе"""
        if not user_ids:
            return []
        
        async with RequestContext() as ctx:
            query = """
            DECLARE $user_ids AS List<Utf8>;
            SELECT id FROM `users`
            WHERE id IN $user_ids AND status = 'active';
            """
            result = await ctx.session.transaction().execute(
                await ctx.session.prepare(query),
                {'$user_ids': user_ids},
                commit_tx=True
            )
            
            existing = [row['id'] for row in result[0].rows] if result and result[0].rows else []
            return existing
    
    async def _filter_chat_members(self, chat_id: int, user_ids: List[str]) -> List[str]:
        """Проверить, какие пользователи являются участниками чата"""
        if not user_ids:
            return []
        
        async with RequestContext() as ctx:
            query = """
            DECLARE $chat_id AS Uint64;
            DECLARE $user_ids AS List<Utf8>;
            SELECT user_id FROM `chat_participants`
            WHERE chat_id = $chat_id AND user_id IN $user_ids AND is_active = true;
            """
            result = await ctx.session.transaction().execute(
                await ctx.session.prepare(query),
                {'$chat_id': chat_id, '$user_ids': user_ids},
                commit_tx=True
            )
            
            members = [row['user_id'] for row in result[0].rows] if result and result[0].rows else []
            return members
    
    async def _send_mention_notifications(self, chat_id: int, message_id: int, 
                                          mentioned_users: List[str], sender_id: str):
        """Отправить уведомления упомянутым пользователям"""
        if not mentioned_users:
            return
        
        # Получаем информацию о чате для уведомлений
        async with RequestContext() as ctx:
            chat_query = """
            DECLARE $chat_id AS Uint64;
            SELECT title FROM `chats` WHERE id = $chat_id;
            """
            chat_result = await ctx.session.transaction().execute(
                await ctx.session.prepare(chat_query),
                {'$chat_id': chat_id},
                commit_tx=True
            )
            chat_title = chat_result[0].rows[0]['title'] if chat_result and chat_result[0].rows else f"Chat {chat_id}"
        
        from handlers.message_handler import Notification, NotificationRepository
        
        async with MessageUnitOfWork() as uow:
            for user_id in mentioned_users:
                # Не отправляем уведомление самому себе
                if user_id == sender_id:
                    continue
                
                notification = Notification(
                    user_id=user_id,
                    type='mention',
                    chat_id=chat_id,
                    sender_id=sender_id,
                    message_id=message_id,
                    message_preview=None,  # можно добавить preview
                    data={
                        'chat_title': chat_title,
                        'mentioned_by': sender_id
                    },
                    created_at=datetime.utcnow()
                )
                
                await uow.notifications.create(notification)
    
  
    
    async def _ensure_typing_cleanup(self):
        """Запустить фоновую задачу очистки typing статусов если еще не запущена"""
        if not self._typing_cleanup_started:
            self._typing_cleanup_started = True
            self._typing_cleanup_task = asyncio.create_task(self._cleanup_typing_status())
            logger.info("✅ Typing cleanup task started")

    async def _ensure_online_cleanup(self):
        """Запустить фоновую задачу очистки online статусов если еще не запущена"""
        if not self._online_cleanup_started:
            self._online_cleanup_started = True
            self._online_cleanup_task = asyncio.create_task(self._cleanup_online_status())
            logger.info("✅ Online cleanup task started")

    async def _cleanup_typing_status(self):
        """Фоновая очистка истекших статусов печатания"""
        try:
            while True:
                await asyncio.sleep(5)
                try:
                    async with self._typing_lock:
                        now = time.time()
                        for chat_id in list(self._typing_status.keys()):
                            chat_users = self._typing_status[chat_id]
                            expired = [uid for uid, exp in chat_users.items() if exp < now]
                            for uid in expired:
                                del chat_users[uid]
                                await self._notify_typing(chat_id, uid, False)
                            if not chat_users:
                                del self._typing_status[chat_id]
                except Exception as e:
                    logger.error(f"Error cleaning typing status: {e}")
        except asyncio.CancelledError:
            logger.info("Typing cleanup task cancelled")
            raise

    async def _cleanup_online_status(self):
        """Фоновая очистка истекших online статусов"""
        try:
            while True:
                await asyncio.sleep(10)
                try:
                    async with self._online_lock:
                        now = time.time()
                        expired = []

                        for user_id, expiry in self._online_users.items():
                            if expiry < now:
                                expired.append(user_id)

                        for user_id in expired:
                            del self._online_users[user_id]
                            if user_id in self._user_sessions:
                                del self._user_sessions[user_id]
                            logger.debug(f"User {user_id[:8]} went offline (expired)")
                            
                            asyncio.create_task(self._notify_user_status(user_id, False))
                except Exception as e:
                    logger.error(f"Error cleaning online status: {e}")
        except asyncio.CancelledError:
            logger.info("Online cleanup task cancelled")
            raise

    async def shutdown(self):
        """Остановить фоновые задачи при завершении"""
        if self._typing_cleanup_task and not self._typing_cleanup_task.done():
            self._typing_cleanup_task.cancel()
            try:
                await self._typing_cleanup_task
            except asyncio.CancelledError:
                pass

        if self._online_cleanup_task and not self._online_cleanup_task.done():
            self._online_cleanup_task.cancel()
            try:
                await self._online_cleanup_task
            except asyncio.CancelledError:
                pass

    # ========== МЕТОДЫ ДЛЯ ONLINE СТАТУСА ==========

    async def user_connected(self, user_id: str, session_id: str):
        """Пользователь подключился (открыл WebSocket или приложение)"""
        try:
            await self._ensure_online_cleanup()

            async with self._online_lock:
                if user_id not in self._user_sessions:
                    self._user_sessions[user_id] = 0
                self._user_sessions[user_id] += 1

                was_offline = user_id not in self._online_users
                self._online_users[user_id] = time.time() + self.online_ttl

                logger.info(f"✅ User {user_id[:8]} connected (sessions: {self._user_sessions[user_id]})")
                
                if was_offline:
                    await self._notify_user_status(user_id, True)

        except Exception as e:
            logger.error(f"Error in user_connected: {e}")

    async def user_disconnected(self, user_id: str, session_id: str):
        """Пользователь отключился"""
        try:
            await self._ensure_online_cleanup()

            async with self._online_lock:
                if user_id in self._user_sessions:
                    self._user_sessions[user_id] -= 1

                    if self._user_sessions[user_id] <= 0:
                        del self._user_sessions[user_id]
                        if user_id in self._online_users:
                            del self._online_users[user_id]

                        logger.info(f"✅ User {user_id[:8]} disconnected (all sessions closed)")
                        
                        await self._notify_user_status(user_id, False)
                    else:
                        logger.info(f"✅ User {user_id[:8]} disconnected (remaining sessions: {self._user_sessions[user_id]})")

        except Exception as e:
            logger.error(f"Error in user_disconnected: {e}")

    async def update_user_activity(self, user_id: str):
        """Обновить активность пользователя (вызывать при любом действии)"""
        try:
            await self._ensure_online_cleanup()

            async with self._online_lock:
                was_offline = user_id not in self._online_users
                
                if user_id in self._user_sessions:
                    self._online_users[user_id] = time.time() + self.online_ttl
                    
                    if was_offline:
                        await self._notify_user_status(user_id, True)
                        
        except Exception as e:
            logger.error(f"Error updating user activity: {e}")

    async def get_online_users(self, chat_id: int, user_id: str) -> List[str]:
        """Получить список онлайн пользователей в чате"""
        try:
            await self._ensure_online_cleanup()

            # Проверяем доступ к чату
            has_access = await self.participant_cache.is_member(chat_id, user_id)
            if not has_access:
                return []

            async with MessageUnitOfWork() as uow:
                # Получаем всех участников чата
                members = await uow.participants.list_by_chat(chat_id, limit=10000)

                online_users = []
                now = time.time()

                async with self._online_lock:
                    for member in members:
                        if member.user_id != user_id:  # Не показываем самого себя
                            expiry = self._online_users.get(member.user_id)
                            if expiry and expiry > now:
                                online_users.append(member.user_id)

                return online_users

        except Exception as e:
            logger.error(f"Error getting online users: {e}")
            return []

    # ========== МЕТОДЫ ДЛЯ "ПЕЧАТАЕТ..." ==========

    async def set_typing(self, chat_id: int, user_id: str, session=None):
        """Отметить что пользователь печатает"""
        try:
            await self._ensure_typing_cleanup()

            has_access = await self.participant_cache.is_member(chat_id, user_id, session=session)
            if not has_access:
                logger.debug(f"User {user_id[:8]} has no access to chat {chat_id}")
                return

            was_typing = False
            async with self._typing_lock:
                if chat_id not in self._typing_status:
                    self._typing_status[chat_id] = {}
                else:
                    was_typing = user_id in self._typing_status[chat_id]

                expiry = time.time() + self.typing_ttl
                self._typing_status[chat_id][user_id] = expiry
                
                if not was_typing:
                    await self._notify_typing(chat_id, user_id, True)
                    
                logger.debug(f"User {user_id[:8]} typing in chat {chat_id}")

        except Exception as e:
            logger.error(f"Error setting typing status: {e}")

    async def stop_typing(self, chat_id: int, user_id: str):
        """Пользователь перестал печатать"""
        try:
            await self._ensure_typing_cleanup()

            async with self._typing_lock:
                if chat_id in self._typing_status:
                    if user_id in self._typing_status[chat_id]:
                        del self._typing_status[chat_id][user_id]
                        logger.debug(f"User {user_id} stopped typing in chat {chat_id}")
                        # Уведомляем остальных участников что печатание остановлено
                        await self._notify_typing(chat_id, user_id, False)

                    if not self._typing_status[chat_id]:
                        del self._typing_status[chat_id]

        except Exception as e:
            logger.error(f"Error stopping typing status: {e}")

    async def get_typing_users(self, chat_id: int, user_id: str) -> List[str]:
        """Получить список печатающих пользователей (для API)"""
        try:
            await self._ensure_typing_cleanup()

            # Проверяем доступ
            has_access = await self.participant_cache.is_member(chat_id, user_id)
            if not has_access:
                return []

            async with self._typing_lock:
                if chat_id not in self._typing_status:
                    return []

                now = time.time()
                users = []
                expired = []

                for uid, expiry in self._typing_status[chat_id].items():
                    if expiry > now:
                        users.append(uid)
                    else:
                        expired.append(uid)

                # Очищаем истекшие
                for uid in expired:
                    del self._typing_status[chat_id][uid]

                if not self._typing_status[chat_id]:
                    del self._typing_status[chat_id]

                # Не показываем самого себя
                return [u for u in users if u != user_id]

        except Exception as e:
            logger.error(f"Error getting typing users: {e}")
            return []

    # ========== МЕТОДЫ ДЛЯ ОТПРАВКИ УВЕДОМЛЕНИЙ ЧЕРЕЗ ВОРКЕР ==========

    def _queue_notification(self, notif_type: str, data: Dict):
        """
        Отправить уведомление в очередь фонового воркера
        (неблокирующий вызов)
        """
        try:
            item = {
                'type': notif_type,
                **data
            }
            asyncio.create_task(notification_worker.put(item))
            logger.debug(f"📨 Notification queued: {notif_type}")
        except Exception as e:
            logger.error(f"❌ Failed to queue notification: {e}")

    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========

    async def _notify_external_websocket(self, chat_id: int, message_data: Dict,
                                message_type: str = "new_message",
                                exclude_user_id: Optional[str] = None):
        """Уведомить WebSocket сервер о новом событии"""
        logger.info(f"📤 Attempting to notify WebSocket for chat {chat_id}, type: {message_type}")

        from config.config import config
        websocket_url = config.WEBSOCKET_URL
        api_key = config.WEBSOCKET_API_KEY

        logger.info(f"📤 WebSocket URL: {websocket_url}/api/broadcast")

        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "chatId": chat_id,
                    "message": message_data,
                    "type": message_type,
                    "excludeUserId": exclude_user_id
                }

                async with session.post(
                    f"{websocket_url}/api/broadcast",
                    json=payload,
                    headers={"X-API-Key": api_key},
                    timeout=10
                ) as response:
                    if response.status != 200:
                        logger.error(f"❌ WebSocket notification failed: {response.status}")
                    else:
                        logger.info(f"✅ WebSocket notification sent to chat {chat_id}")
        except asyncio.TimeoutError:
            logger.error(f"❌ WebSocket notification TIMEOUT for chat {chat_id}")
        except aiohttp.ClientConnectorError as e:
            logger.error(f"❌ Cannot connect to WebSocket server: {e}")
        except Exception as e:
            logger.error(f"❌ Failed to notify WebSocket: {e}", exc_info=True)

    async def _check_idempotency(self, idempotency_key: str, chat_id: int) -> Optional[Message]:
        """Проверить ключ идемпотентности"""
        async with MessageUnitOfWork() as uow:
            existing_key = await uow.idempotency.get(idempotency_key)
            if existing_key:
                return await uow.messages.get(chat_id, int(existing_key.entity_id))
        return None

    async def _get_chat_and_participant(self, chat_id: int, user_id: str, session = None) -> Tuple:
        """Получить чат и участника"""
        async with await MessageUnitOfWork.with_session(session) as uow:
            chat = await uow.chats.get_by_id(chat_id)
            if not chat or chat.is_deleted:
                raise NotFoundError(f"Chat {chat_id} not found")

            participant = await self.participant_cache.get_participant(
                chat_id, 
                user_id, 
                session=session  # 👈 передаем сессию!
            )
            if not participant:
                raise PermissionError("You are not a member of this chat")

            return chat, participant

    async def _check_channel_permissions(self, chat, participant, reply_to, thread_root_id):
        """Проверить права для канала"""
        if chat.type != 'channel':
            return

        if chat.linked_chat_id:
            if not participant or participant.get('role') not in ['owner', 'admin']:
                raise PermissionError("Only admins can post in channels with discussion")
        elif chat.comments_enabled:
            if reply_to or thread_root_id:
                if not participant:
                    raise PermissionError("You must be a member to comment")
                if participant.get('is_blocked'):
                    raise PermissionError("You are blocked in this chat")
                if chat.comments_settings:
                    who_can_comment = chat.comments_settings.get('who_can_comment', 'all')
                    if who_can_comment == 'admins' and participant.get('role') not in ['owner', 'admin']:
                        raise PermissionError("Only admins can comment")
            else:
                if not participant or participant.get('role') not in ['owner', 'admin']:
                    raise PermissionError("Only admins can create new posts in channels")
        else:
            if not participant or participant.get('role') not in ['owner', 'admin']:
                raise PermissionError("Only admins can post in channels")

    async def _collect_attachments(self, chat_id: int, attachments: Optional[List],
                                   photo_ids: Optional[List[str]], photos: Optional[List],
                                   user_id: str) -> Tuple[List, Optional[str]]:
        """Собрать все вложения для сообщения"""
        all_attachments = attachments or []
        message_type = 'text'

        # Добавляем фото из photo_ids
        if photo_ids:
            async with MessageUnitOfWork() as uow:
                photos_map = await uow.photos.get_many(photo_ids)
                for photo_id in photo_ids:
                    photo = photos_map.get(photo_id)
                    if photo and photo.status == PhotoStatus.COMPLETED:
                        all_attachments.append({
                            'photo_id': photo_id,
                            'type': 'photo',
                            'caption': photo.caption or '',
                            'urls': {
                                'original': photo.url_original,
                                'large': photo.url_large,
                                'medium': photo.url_medium,
                                'small': photo.url_small
                            },
                            'width': photo.width,
                            'height': photo.height,
                            'file_size': photo.file_size,
                            'mime_type': photo.mime_type
                        })

        # Создаем новые фото
        if photos:
            uploaded_photos = await self.upload_message_photos(chat_id, user_id, photos)
            for uploaded in uploaded_photos:
                all_attachments.append({
                    'photo_id': uploaded['photo_id'],
                    'type': 'photo',
                    'caption': '',
                    'status': 'pending'
                })

            if photos and not photo_ids:
                message_type = 'image' if len(photos) == 1 else 'album'

        return all_attachments, message_type

    async def _extract_mentions(self, content: Optional[str], mentions: Optional[List]) -> List[str]:
        """Извлечь упоминания из текста"""
        mention_list = mentions or []
        if content:
            mention_matches = re.findall(r'@([a-f0-9-]{36})', content)
            mention_list.extend([m for m in mention_matches if m not in mention_list])
        return mention_list

    async def _update_message_counts(self, uow, chat_id: int, message_id: int,
                                     reply_to: Optional[int], thread_root_id: Optional[int],
                                     chat_type: str, user_id: str, preview: str):
        """Обновить счетчики после отправки сообщения"""
        if reply_to:
            await uow.messages.increment_reply_count(chat_id, reply_to)

        if thread_root_id:
            await uow.messages.increment_thread_count(chat_id, thread_root_id)

        await uow.chats.update_last_message(
            chat_id=chat_id,
            message_id=message_id,
            preview=preview,
            sender_id=user_id,
            at=datetime.utcnow()
        )

        await uow.participants.update_activity(chat_id, user_id)

    
    async def _batch_increment_unread(self, uow, chat_id: int, sender_id: str, members: List):
        """Инкрементировать unread_count для пачки пользователей"""
        for member in members:
            if member.user_id != sender_id and member.is_active:
                await uow.participants.increment_unread(chat_id, member.user_id)
                # Обновляем last_active_at чтобы чат поднялся вверх списка у получателей
                await uow.participants.update_activity(chat_id, member.user_id)

                # Для личных диалогов - показываем скрытый
                if member.is_hidden:
                    await uow.participants.show_dialog(chat_id, member.user_id)

    async def _save_idempotency_key(self, uow, key: str, message_id: int, chat_id: int, user_id: str):
        """Сохранить ключ идемпотентности"""
        key_obj = IdempotencyKey(
            idempotency_key=key,
            entity_type="message",
            entity_id=int(message_id),
            chat_id=chat_id,
            user_id=user_id,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=24)
        )
        await uow.idempotency.create(key_obj)

    async def _attach_reply_previews_batch(self, chat_id: int, messages: List[Message], session=None):
        """
        Batch получение превью ответов
        """
        if not messages:
            return
            
        # Собираем ID сообщений, на которые есть ответы
        reply_ids = set()
        for msg in messages:
            if msg.reply_to_message_id:
                reply_ids.add(msg.reply_to_message_id)

        if not reply_ids:
            return

        # Получаем все сообщения-ответы одним запросом
        if session:
            # Используем переданную сессию
            msg_repo = MessageRepository(session)
            reply_messages_map = await msg_repo.get_many(chat_id, list(reply_ids))
        else:
            # Создаем новую сессию (не рекомендуется, но для обратной совместимости)
            async with MessageUnitOfWork() as uow:
                msg_repo = MessageRepository(uow._session)
                reply_messages_map = await msg_repo.get_many(chat_id, list(reply_ids))

        # Добавляем информацию об ответах
        for msg in messages:
            if msg.reply_to_message_id and msg.reply_to_message_id in reply_messages_map:
                reply_msg = reply_messages_map[msg.reply_to_message_id]
                msg.reply_to_info = {
                    'id': reply_msg.message_id,
                    'sender_id': reply_msg.sender_id,
                    'content_preview': reply_msg.content[:150] + '...' if reply_msg.content and len(reply_msg.content) > 150 else reply_msg.content,
                    'created_at': reply_msg.created_at.isoformat() if reply_msg.created_at else None,
                    'is_deleted': reply_msg.is_deleted,
                    'message_type': reply_msg.message_type
                }
                if reply_msg.is_deleted:
                    msg.reply_to_info['content_preview'] = '[Message deleted]'

    async def _safe_increment_view(self, chat_id: int, message_id: int):
        """Безопасно увеличить счетчик просмотров (в фоне)"""
        try:
            async with MessageUnitOfWork() as uow:
                await uow.messages.increment_view(chat_id, message_id)
        except Exception as e:
            logger.error(f"Failed to increment view for message {message_id}: {e}")

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ФОТО ==========

    async def upload_message_photos(self, chat_id: int, user_id: str, photos: List[Dict]) -> List[Dict]:
        """
        Загрузить несколько фото для сообщения
        Создает записи в БД и запускает фоновую обработку
        Возвращает список photo_id для отслеживания статуса
        """
        logger.info(f"📸 Starting upload of {len(photos)} photos for chat {chat_id}")

        # Проверяем доступ к чату (через RequestContext с сессией)
        async with RequestContext() as access_ctx:
            has_access = await self.participant_cache.is_member(
                chat_id, user_id, session=access_ctx.session
            )
        if not has_access:
            raise PermissionError("You don't have access to this chat")

        # Валидация количества фото
        if len(photos) > message_config.MAX_PHOTOS_PER_MESSAGE:
            raise ValidationError(f"Maximum {message_config.MAX_PHOTOS_PER_MESSAGE} photos per message")

        uploaded_photos = []

        async with MessageUnitOfWork() as uow:
            for idx, photo_data in enumerate(photos):
                # Валидация каждого фото
                if not photo_data.get('photo'):
                    raise ValidationError(f"Photo {idx+1}: missing image data")

                if not photo_data.get('photo').startswith('data:image'):
                    raise ValidationError(f"Photo {idx+1}: must be base64 image")

                # Проверка размера (приблизительно)
                base64_data = photo_data['photo'].split(',')[-1] if ',' in photo_data['photo'] else photo_data['photo']
                approx_size = (len(base64_data) * 3) / 4
                if approx_size > message_config.MAX_PHOTO_SIZE_MB * 1024 * 1024:
                    raise ValidationError(f"Photo {idx+1}: too large. Max {message_config.MAX_PHOTO_SIZE_MB}MB")

                photo_id = str(uuid.uuid4())

                # Создаем запись в БД
                photo = PhotoAttachment(
                    photo_id=photo_id,
                    chat_id=chat_id,
                    user_id=user_id,
                    status=PhotoStatus.PENDING,
                    progress=0,
                    caption=photo_data.get('caption', ''),
                    mime_type=photo_data.get('mime_type', 'image/jpeg')
                )

                success = await uow.photos.create(photo)
                if not success:
                    logger.error(f"❌ Failed to create photo record for {photo_id}")
                    continue

                uploaded_photos.append({
                    'photo_id': photo_id,
                    'status': 'pending',
                    'progress': 0
                })

                # Запускаем фоновую обработку
                asyncio.create_task(self._process_photo_upload(
                    photo_id=photo_id,
                    chat_id=chat_id,
                    user_id=user_id,
                    image_data=photo_data.get('photo'),
                    caption=photo_data.get('caption', '')
                ))

                logger.info(f"⏳ Photo {photo_id} queued for processing")

        return uploaded_photos

    async def upload_message_photos_sync(self, chat_id: int, user_id: str, photos: List[Dict]) -> List[Dict]:
        """
        Загрузить фото синхронно — сразу в Object Storage, возвращает URL.
        Работает как загрузка аватара в профиле.
        """
        async with RequestContext() as access_ctx:
            has_access = await self.participant_cache.is_member(
                chat_id, user_id, session=access_ctx.session
            )
        if not has_access:
            raise PermissionError("You don't have access to this chat")

        if len(photos) > message_config.MAX_PHOTOS_PER_MESSAGE:
            raise ValidationError(f"Maximum {message_config.MAX_PHOTOS_PER_MESSAGE} photos per message")

        uploaded = []
        loop = asyncio.get_event_loop()

        for idx, photo_data in enumerate(photos):
            raw = photo_data.get('photo', '')
            if not raw:
                raise ValidationError(f"Photo {idx+1}: missing image data")
            if not raw.startswith('data:image'):
                raise ValidationError(f"Photo {idx+1}: must be base64 data URL")

            if ',' in raw:
                header, b64 = raw.split(',', 1)
                content_type = header.split(';')[0].replace('data:', '')
            else:
                b64, content_type = raw, 'image/jpeg'

            try:
                image_bytes = base64.b64decode(b64)
            except Exception:
                raise ValidationError(f"Photo {idx+1}: invalid base64")

            if not image_bytes:
                raise ValidationError(f"Photo {idx+1}: empty image")

            if len(image_bytes) > message_config.MAX_PHOTO_SIZE_MB * 1024 * 1024:
                raise ValidationError(f"Photo {idx+1}: too large (max {message_config.MAX_PHOTO_SIZE_MB}MB)")

            photo_id = str(uuid.uuid4())
            ext = content_type.split('/')[-1] if '/' in content_type else 'jpeg'
            filename = f"photo_{photo_id}.{ext}"

            # Загружаем в S3 синхронно через executor (boto3 блокирует поток)
            _bytes, _ct, _fn, _cid, _uid = image_bytes, content_type, filename, chat_id, user_id
            upload_result = await loop.run_in_executor(
                None,
                lambda: storage.upload_file(
                    file_data=_bytes,
                    content_type=_ct,
                    filename=_fn,
                    chat_id=_cid,
                    user_id=_uid,
                )
            )
            photo_url = upload_result['url']

            # Создаём запись в БД со статусом completed
            async with MessageUnitOfWork() as uow:
                photo = PhotoAttachment(
                    photo_id=photo_id,
                    chat_id=chat_id,
                    user_id=user_id,
                    status=PhotoStatus.COMPLETED,
                    progress=100,
                    caption=photo_data.get('caption', ''),
                    mime_type=content_type,
                    url_original=photo_url,
                    url_large=photo_url,
                    url_medium=photo_url,
                    url_small=photo_url,
                    file_size=len(image_bytes),
                    completed_at=datetime.utcnow(),
                )
                await uow.photos.create(photo)

            uploaded.append({
                'photo_id': photo_id,
                'url': photo_url,
                'status': 'completed',
                'progress': 100,
            })
            logger.info(f"✅ Photo {photo_id} uploaded sync → {photo_url}")

        return uploaded

    async def _process_photo_upload(self, photo_id: str, chat_id: int, user_id: str,
                                    image_data: str, caption: str):
        """
        Фоновая обработка фото с кэшированием
        """
        logger.info(f"🔄 Processing photo {photo_id}")

        async with MessageUnitOfWork() as uow:
            try:
                # 1. Обновляем статус на UPLOADING
                photo = await uow.photos.get(photo_id)
                if not photo:
                    logger.error(f"❌ Photo {photo_id} not found in database")
                    return

                # Проверяем, не начал ли кто-то уже обрабатывать это фото
                if photo.status != PhotoStatus.PENDING:
                    logger.warning(f"⚠️ Photo {photo_id} already being processed (status: {photo.status})")
                    return

                photo.status = PhotoStatus.UPLOADING
                photo.progress = 10
                await uow.photos.update(photo)

                # 2. Декодируем base64 с проверкой
                logger.info(f"📸 Decoding base64 image for photo {photo_id}")

                # Проверяем, есть ли префикс data:image
                if ',' in image_data:
                    header, base64_data = image_data.split(',', 1)
                    content_type = header.split(';')[0].replace('data:', '')
                    logger.info(f"Content-Type from header: {content_type}")
                else:
                    base64_data = image_data
                    content_type = 'image/jpeg'
                    logger.info("No header found, using default image/jpeg")

                # Декодируем base64
                try:
                    image_bytes = base64.b64decode(base64_data)
                    logger.info(f"✅ Decoded {len(image_bytes)} bytes for photo {photo_id}")
                except Exception as e:
                    logger.error(f"❌ Base64 decode failed: {e}")
                    photo.status = PhotoStatus.FAILED
                    photo.error_message = f"Base64 decode failed: {str(e)}"
                    photo.updated_at = datetime.utcnow()
                    await uow.photos.update(photo)
                    return

                # Проверяем, что байты не пустые
                if len(image_bytes) == 0:
                    logger.error(f"❌ Decoded image is empty for photo {photo_id}")
                    photo.status = PhotoStatus.FAILED
                    photo.error_message = "Decoded image is empty"
                    photo.updated_at = datetime.utcnow()
                    await uow.photos.update(photo)
                    return

                photo.progress = 30
                photo.file_size = len(image_bytes)
                photo.mime_type = content_type
                await uow.photos.update(photo)

                # 3. Вычисляем хеш содержимого для кэша
                content_hash = hashlib.md5(image_bytes).hexdigest()
                photo.content_hash = content_hash
                await uow.photos.update(photo)

                # 4. Проверяем, есть ли уже обработанное фото с таким хешем
                cached_photo = await uow.photos.get_by_hash(content_hash)

                if cached_photo and cached_photo.status == PhotoStatus.COMPLETED:
                    # Если нашли в кэше - используем готовые URL
                    logger.info(f"✅ Photo cache hit for {photo_id}")
                    photo.url_original = cached_photo.url_original
                    photo.url_large = cached_photo.url_large
                    photo.url_medium = cached_photo.url_medium
                    photo.url_small = cached_photo.url_small
                    photo.width = cached_photo.width
                    photo.height = cached_photo.height
                    photo.status = PhotoStatus.COMPLETED
                    photo.progress = 100
                    photo.completed_at = datetime.utcnow()
                    photo.updated_at = datetime.utcnow()

                    await uow.photos.update(photo)
                    logger.info(f"✅ Photo {photo_id} loaded from cache")

                    # Если фото уже привязано к сообщению, обновляем attachments
                    await self._update_message_attachments(photo)
                    return

                # 5. Если нет в кэше - обрабатываем
                logger.info(f"📦 Photo cache miss for {photo_id}")
                photo.status = PhotoStatus.PROCESSING
                photo.progress = 50
                await uow.photos.update(photo)

                # Обрабатываем изображение (создаем разные размеры)
                try:
                    processed = await self._process_image(image_bytes, photo_id)
                except Exception as e:
                    logger.error(f"❌ Image processing failed: {e}")
                    photo.status = PhotoStatus.FAILED
                    photo.error_message = f"Image processing failed: {str(e)}"
                    photo.updated_at = datetime.utcnow()
                    await uow.photos.update(photo)
                    return

                # 6. Загружаем все версии в Object Storage через storage сервис
                urls = {}
                sizes_order = ['small', 'medium', 'large', 'original']

                for i, size_name in enumerate(sizes_order):
                    if size_name not in processed:
                        logger.warning(f"⚠️ Size {size_name} not in processed result")
                        continue

                    size_data = processed[size_name]
                    try:
                        filename = f"photo_{photo_id}_{size_name}.jpg"

                        result = storage.upload_file(
                            file_data=size_data['bytes'],
                            content_type='image/jpeg',
                            filename=filename,
                            chat_id=chat_id,
                            user_id=user_id,
                            metadata={
                                'photo_id': photo_id,
                                'size': size_name,
                                'width': str(size_data['width']),
                                'height': str(size_data['height']),
                                'original_filename': filename,
                                'processed_at': datetime.utcnow().isoformat()
                            }
                        )

                        urls[size_name] = result['url']

                        # Обновляем прогресс
                        photo.progress = 50 + (int(50 * (i + 1) / len(sizes_order)))
                        await uow.photos.update(photo)
                        logger.info(f"✅ Uploaded {size_name} for photo {photo_id}: {result['url']}")

                    except Exception as e:
                        logger.error(f"❌ Failed to upload {size_name}: {e}")
                        photo.status = PhotoStatus.FAILED
                        photo.error_message = f"Upload failed for {size_name}: {str(e)}"
                        photo.updated_at = datetime.utcnow()
                        await uow.photos.update(photo)
                        return

                # 7. Сохраняем все URL
                photo.url_original = urls.get('original')
                photo.url_large = urls.get('large')
                photo.url_medium = urls.get('medium')
                photo.url_small = urls.get('small')
                photo.width = processed.get('original', {}).get('width')
                photo.height = processed.get('original', {}).get('height')
                photo.status = PhotoStatus.COMPLETED
                photo.progress = 100
                photo.completed_at = datetime.utcnow()
                photo.updated_at = datetime.utcnow()

                await uow.photos.update(photo)

                logger.info(f"✅ Photo {photo_id} processed and uploaded successfully")

                # Если фото уже привязано к сообщению, обновляем attachments
                await self._update_message_attachments(photo)

            except Exception as e:
                logger.error(f"❌ Photo processing failed: {e}", exc_info=True)

                # Обновляем статус на FAILED
                try:
                    photo = await uow.photos.get(photo_id)
                    if photo:
                        photo.status = PhotoStatus.FAILED
                        photo.error_message = str(e)
                        photo.updated_at = datetime.utcnow()
                        await uow.photos.update(photo)
                except Exception as update_error:
                    logger.error(f"❌ Failed to update photo status: {update_error}")

    async def _process_image(self, image_bytes: bytes, photo_id: str) -> Dict:
        """
        Обработать изображение:
        - Сжать оригинал
        - Создать превью разных размеров
        """
        try:
            from PIL import Image
            import io

            logger.info(f"🖼️ Processing image for photo {photo_id}, size: {len(image_bytes)} bytes")

            img = Image.open(io.BytesIO(image_bytes))
            original_width, original_height = img.size

            # Конвертируем в RGB если нужно
            if img.mode in ('RGBA', 'P'):
                img = img.convert('RGB')

            results = {}

            # Размеры для превью
            sizes = {
                'small': (100, 100),
                'medium': (400, 400),
                'large': (1024, 1024),
                'original': (original_width, original_height)
            }

            for size_name, max_size in sizes.items():
                img_copy = img.copy()

                # Уменьшаем если нужно
                if size_name != 'original' and (img_copy.width > max_size[0] or img_copy.height > max_size[1]):
                    img_copy.thumbnail(max_size, Image.Resampling.LANCZOS)

                # Сохраняем
                output = io.BytesIO()

                # Качество: меньше для превью, больше для оригинала
                quality = 75 if size_name in ['small', 'medium'] else 85

                img_copy.save(output, format='JPEG', quality=quality, optimize=True)
                results[size_name] = {
                    'bytes': output.getvalue(),
                    'width': img_copy.width,
                    'height': img_copy.height
                }

                logger.info(f"📸 Created {size_name}: {img_copy.width}x{img_copy.height}, {len(output.getvalue())} bytes")

            return {
                'original': results['original'],
                'large': results['large'],
                'medium': results['medium'],
                'small': results['small']
            }

        except Exception as e:
            logger.error(f"❌ Image processing failed: {e}")
            raise

    async def _update_message_attachments(self, photo: PhotoAttachment):
        """Обновить attachments в сообщении после завершения обработки фото"""
        if not photo.message_id:
            return

        async with MessageUnitOfWork() as uow:
            message = await uow.messages.get(photo.chat_id, photo.message_id)
            if not message or not message.attachments_json:
                return

            updated = False
            for attachment in message.attachments_json:
                if attachment.get('type') == 'photo' and attachment.get('photo_id') == photo.photo_id:
                    attachment['status'] = 'completed'
                    attachment['urls'] = {
                        'original': photo.url_original,
                        'large': photo.url_large,
                        'medium': photo.url_medium,
                        'small': photo.url_small
                    }
                    attachment['width'] = photo.width
                    attachment['height'] = photo.height
                    attachment['file_size'] = photo.file_size
                    updated = True
                    break

            if updated:
                await uow.messages.update(message)
                logger.info(f"✅ Updated message {message.message_id} with photo {photo.photo_id} URLs")

    async def get_photo_status(self, photo_id: str, user_id: str, session = None) -> PhotoAttachment:
        """Получить статус загрузки фото"""
        async with await MessageUnitOfWork.with_session(session) as uow:
            photo = await uow.photos.get(photo_id)
            if not photo:
                raise NotFoundError(f"Photo {photo_id} not found")

            # Проверяем доступ
            if photo.user_id != user_id:
                has_access = await self.participant_cache.is_member(photo.chat_id, user_id)
                if not has_access:
                    raise PermissionError("You don't have access to this photo")

            return photo

    async def list_chat_photos(self, chat_id: int, user_id: str, limit: int = 50,
                               status: Optional[str] = None, session = None) -> List[PhotoAttachment]:
        """Получить все фото чата"""
        # Проверяем доступ
        has_access = await self.participant_cache.is_member(chat_id, user_id)
        if not has_access:
            raise PermissionError("You don't have access to this chat")

        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.photos.list_by_chat(chat_id, limit, status)

    async def get_message_photos(self, chat_id: int, message_id: int, user_id: str, session = None) -> List[Dict]:
        """Получить все фото сообщения с актуальными URL"""
        # Проверяем доступ
        has_access = await self.participant_cache.is_member(chat_id, user_id)
        if not has_access:
            raise PermissionError("You don't have access to this chat")

        async with await MessageUnitOfWork.with_session(session) as uow:
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                raise NotFoundError(f"Message {message_id} not found")

            photos = []
            if message.attachments_json:
                # Собираем все photo_id из attachments
                photo_ids = []
                for attachment in message.attachments_json:
                    if attachment.get('type') == 'photo' and attachment.get('photo_id'):
                        photo_ids.append(attachment['photo_id'])

                # Получаем актуальную информацию о фото
                if photo_ids:
                    photos_map = await uow.photos.get_many(photo_ids)
                    for photo_id in photo_ids:
                        photo = photos_map.get(photo_id)
                        if photo:
                            photos.append(photo.to_dict())

            return photos

    async def delete_photo(self, photo_id: str, user_id: str, session = None) -> bool:
        """Удалить фото и файлы из storage"""
        async with await MessageUnitOfWork.with_session(session) as uow:
            photo = await uow.photos.get(photo_id)
            if not photo:
                raise NotFoundError(f"Photo {photo_id} not found")

            # Проверяем права
            if photo.user_id != user_id:
                has_access = await self.participant_cache.is_member(photo.chat_id, user_id)
                if not has_access:
                    raise PermissionError("You don't have access to this photo")

            # Удаляем файлы из storage
            for url in [photo.url_original, photo.url_large, photo.url_medium, photo.url_small]:
                if url:
                    storage.delete_file(url)

            # Если фото привязано к сообщению, обновляем attachments
            if photo.message_id:
                message = await uow.messages.get(photo.chat_id, photo.message_id)
                if message and message.attachments_json:
                    message.attachments_json = [
                        a for a in message.attachments_json
                        if not (a.get('type') == 'photo' and a.get('photo_id') == photo_id)
                    ]
                    message.has_attachments = bool(message.attachments_json)
                    await uow.messages.update(message)

            # Помечаем как удаленное
            photo.status = PhotoStatus.FAILED
            photo.error_message = "Deleted by user"
            await uow.photos.update(photo)

            logger.info(f"✅ Photo {photo_id} deleted")
            return True

    async def get_photo_info(self, photo_id: str, user_id: str, session = None) -> Dict:
        """Получить информацию о фото с данными из storage"""
        async with await MessageUnitOfWork.with_session(session) as uow:
            photo = await uow.photos.get(photo_id)
            if not photo:
                raise NotFoundError(f"Photo {photo_id} not found")

            # Проверяем доступ
            if photo.user_id != user_id:
                has_access = await self.participant_cache.is_member(photo.chat_id, user_id)
                if not has_access:
                    raise PermissionError("You don't have access to this photo")

            result = photo.to_dict()

            # Добавляем информацию из storage для каждого размера
            for size in ['original', 'large', 'medium', 'small']:
                url = result['urls'].get(size)
                if url and storage:
                    # Извлекаем ключ из URL
                    key = url.replace(f"{storage.public_url}/", "")
                    file_info = storage.get_file_info(key)
                    if file_info:
                        if 'storage_info' not in result:
                            result['storage_info'] = {}
                        result['storage_info'][size] = file_info

            return result

    # ========== ОСНОВНЫЕ МЕТОДЫ ==========

   
    async def send_private_message(
        self,
        recipient_id: str,
        user_id: str,
        content: Optional[str] = None,
        message_type: str = "text",
        attachments: Optional[List[Dict]] = None,
        idempotency_key: Optional[str] = None,
        session = None
    ) -> Message:
        """Отправить личное сообщение с WebSocket уведомлением"""

        if user_id == recipient_id:
            raise ValidationError("Cannot send message to yourself")

        MessageValidator.validate_send(content, message_type, attachments, None)

        async with await MessageUnitOfWork.with_session(session) as uow:
            # Проверка идемпотентности
            if idempotency_key:
                existing_key = await uow.idempotency.get(idempotency_key)
                if existing_key:
                    existing_msg = await uow.messages.get(
                        int(existing_key.chat_id),
                        int(existing_key.entity_id)
                    )
                    if existing_msg:
                        return existing_msg

            # Сортируем ID пользователей для детерминированного ключа
            user_ids = sorted([user_id, recipient_id])
            user1, user2 = user_ids[0], user_ids[1]
            
            # Генерируем ID чата заранее
            chat_id = uuid.uuid4().int & (2**64 - 1)
            now = datetime.utcnow()
            
            session_obj = uow._session
            
            # 1. Сначала пытаемся найти существующий чат
            find_query = """
            DECLARE $user1 AS Utf8;
            DECLARE $user2 AS Utf8;
            
            SELECT id 
            FROM `chats` 
            WHERE type = 'private' 
              AND user1_id = $user1 
              AND user2_id = $user2 
              AND is_deleted = false
            LIMIT 1;
            """
            
            find_result = await session_obj.transaction().execute(
                await session_obj.prepare(find_query),
                {'$user1': user1, '$user2': user2},
                commit_tx=True
            )
            
            existing_chat_id = find_result[0].rows[0]['id'] if find_result and find_result[0].rows else None
            
            # 2. Если чат найден - используем его
            if existing_chat_id:
                logger.info(f"✅ Found existing private chat {existing_chat_id}")
                final_chat_id = existing_chat_id
            else:
                # 3. Если не найден - создаем новый
                logger.info(f"🚀 Creating new private chat between {user1[:8]} and {user2[:8]}")
                
                title = f"Dialog between {user1[:8]} and {user2[:8]}"
                
                create_query = """
                DECLARE $chat_id AS Uint64;
                DECLARE $user1 AS Utf8;
                DECLARE $user2 AS Utf8;
                DECLARE $now AS Timestamp;
                DECLARE $title AS Utf8;
                
                INSERT INTO `chats` (
                    id, type, created_by, owner_id, members_count, 
                    created_at, user1_id, user2_id, title, is_active,
                    is_public, join_moderation, max_members, slow_mode_interval,
                    is_archived, is_deleted, is_discussion, comments_enabled,
                    reactions_enabled, is_deleted_for_all, messages_count,
                    online_estimate, views_count, version, primary_region, status
                ) VALUES (
                    $chat_id, 
                    'private', 
                    $user1, 
                    $user1, 
                    2,
                    $now, 
                    $user1, 
                    $user2, 
                    $title,
                    true, 
                    false, 
                    false, 
                    2, 
                    0,
                    false, 
                    false, 
                    false, 
                    false,
                    true, 
                    false, 
                    0,
                    0, 
                    0, 
                    1, 
                    'ru-central1', 
                    'active'
                );
                
                UPSERT INTO `chat_participants` (
                    chat_id, user_id, role, joined_at, is_active, 
                    unread_count, region, is_hidden, show_in_profile,
                    role_order
                ) VALUES 
                    ($chat_id, $user1, 'owner', $now, true, 0, 'ru-central1', false, true, 1),
                    ($chat_id, $user2, 'member', $now, true, 0, 'ru-central1', false, true, 4);
                
                SELECT $chat_id as chat_id;
                """
                
                params = {
                    '$chat_id': chat_id,
                    '$user1': user1,
                    '$user2': user2,
                    '$now': to_timestamp(now),
                    '$title': title
                }
                
                create_result = await session_obj.transaction().execute(
                    await session_obj.prepare(create_query),
                    params,
                    commit_tx=True
                )
                
                final_chat_id = create_result[0].rows[0]['chat_id'] if create_result and create_result[0].rows else None
                if not final_chat_id:
                    raise DatabaseError("Failed to create private chat")
                
                logger.info(f"✅ Created new private chat {final_chat_id}")

            # Отправляем сообщение
            message = await self.send_message(
                chat_id=final_chat_id,
                user_id=user_id,
                content=content,
                message_type=message_type,
                attachments=attachments,
                idempotency_key=idempotency_key,
                session=session_obj
            )

            return message
    
    @retry(max_attempts=3)
    async def send_message(
        self,
        chat_id: int,
        user_id: str,
        content: Optional[str] = None,
        message_type: str = "text",
        reply_to: Optional[int] = None,
        thread_root_id: Optional[int] = None,
        attachments: Optional[List[Dict]] = None,
        photos: Optional[List[Dict]] = None,
        photo_ids: Optional[Union[List[str], str]] = None,
        mentions: Optional[List[str]] = None,
        entities: Optional[Dict] = None,
        idempotency_key: Optional[str] = None,
        client_ip: Optional[str] = None,
        session = None
    ) -> Message:
        """
        Отправить сообщение с WebSocket уведомлением и поддержкой идемпотентности
        """
        from handlers.message_handler import Message
        
        start_time = time.time()
        logger.info(f"🚀 [SEND_MESSAGE] START: chat_id={chat_id}, user_id={user_id[:8]}, idempotency_key={idempotency_key[:8] if idempotency_key else None}")
        
        # ========== 1. ПРОВЕРКА ИДЕМПОТЕНТНОСТИ (с кэшем) ==========
        if idempotency_key:
            logger.info(f"🔍 [IDEM] Checking idempotency for key: {idempotency_key[:8]}")
            async with RequestContext() as ctx:
                idem_repo = IdempotencyRepository(ctx.session)
                existing_key = await idem_repo.get(idempotency_key)
                if existing_key:
                    logger.info(f"🔍 [IDEM] Existing key found: entity_id={existing_key.entity_id}, has_result_data={existing_key.result_data is not None}")
                    
                    # 1.1 Если есть сохранённый результат в JSON – используем его
                    if existing_key.result_data:
                        logger.info(f"🔄 [IDEM] Idempotency hit (cached): {idempotency_key[:8]}")
                        return Message.from_dict(existing_key.result_data)
                    
                    # 1.2 Иначе ищем в БД по ID (fallback)
                    if existing_key.entity_type == 'message' and existing_key.entity_id:
                        logger.info(f"🔄 [IDEM] Idempotency hit (DB): {idempotency_key[:8]}")
                        msg_repo = MessageRepository(ctx.session)
                        existing_msg = await msg_repo.get(chat_id, int(existing_key.entity_id))
                        if existing_msg:
                            logger.info(f"✅ [IDEM] Existing message found in DB: {existing_msg.message_id}")
                            return existing_msg
                        else:
                            logger.warning(f"⚠️ [IDEM] Existing key found but message not found in DB! entity_id={existing_key.entity_id}")
                else:
                    logger.info(f"🔍 [IDEM] No existing key found for {idempotency_key[:8]}")
        
        # ========== 2. ОСНОВНАЯ ЛОГИКА ==========
        logger.info(f"📝 [SEND_MESSAGE] Creating new message...")
        
        async with RequestContext() as ctx:
            session_obj = ctx.session
            
            # ===== 2.1 Нормализация photo_ids =====
            normalized_photo_ids = None
            if photo_ids is not None:
                if isinstance(photo_ids, str):
                    try:
                        normalized_photo_ids = json.loads(photo_ids)
                        if not isinstance(normalized_photo_ids, list):
                            normalized_photo_ids = [photo_ids]
                    except:
                        normalized_photo_ids = [pid.strip() for pid in photo_ids.split(',') if pid.strip()]
                elif isinstance(photo_ids, list):
                    normalized_photo_ids = photo_ids
                else:
                    normalized_photo_ids = [str(photo_ids)]
            
            # ===== 2.2 Подготовка данных =====
            message_id = uuid.uuid4().int & (2**64 - 1)
            now = datetime.utcnow()
            logger.info(f"📝 [SEND_MESSAGE] Generated message_id={message_id}")
            
            content_preview = None
            if content:
                content_preview = content[:200] + '...' if len(content) > 200 else content
            elif attachments:
                content_preview = f"[{len(attachments)} attachment(s)]"
            elif normalized_photo_ids:
                content_preview = f"[{len(normalized_photo_ids)} photo(s)]"
            elif photos:
                content_preview = f"[{len(photos)} photo(s)]"
            
            # ===== 2.3 Извлечение упоминаний =====
            mention_list = mentions or []
            if content:
                import re
                mention_matches = re.findall(r'@([a-f0-9-]{36})', content)
                for match in mention_matches:
                    if match not in mention_list:
                        mention_list.append(match)
            
            # ===== 2.4 Получение информации о чате (с кэшем) =====
            chat = None
            cached_chat = await cache.get(f"chat:{chat_id}")
            if cached_chat is not None and not isinstance(cached_chat, dict):
                chat = {'type': cached_chat.type, 'linked_chat_id': cached_chat.linked_chat_id,
                        'comments_enabled': cached_chat.comments_enabled, 'title': cached_chat.title}
            if not chat:
                chat_result = await session_obj.transaction().execute(
                    await session_obj.prepare(
                        "DECLARE $chat_id AS Uint64; "
                        "SELECT type, linked_chat_id, comments_enabled, title "
                        "FROM `chats` WHERE id = $chat_id AND is_deleted = false;"
                    ),
                    {'$chat_id': chat_id}, commit_tx=True
                )
                if not chat_result or not chat_result[0].rows:
                    raise NotFoundError(f"Chat {chat_id} not found")
                chat = chat_result[0].rows[0]
            chat_title = chat.get('title', f"Chat {chat_id}")

            # ===== 2.5 Получение информации об участнике =====
            participant_result = await session_obj.transaction().execute(
                await session_obj.prepare(
                    "DECLARE $chat_id AS Uint64; DECLARE $user_id AS Utf8; "
                    "SELECT role, is_blocked, is_active FROM `chat_participants` "
                    "WHERE chat_id = $chat_id AND user_id = $user_id;"
                ),
                {'$chat_id': chat_id, '$user_id': user_id}, commit_tx=True
            )
            if not participant_result or not participant_result[0].rows:
                raise PermissionError("Not a member")
            participant = participant_result[0].rows[0]
            if participant.get('is_blocked'):
                raise PermissionError("User is blocked")
            
            # ===== 2.6 Проверка reply_to =====
            if reply_to:
                parent_query = """
                DECLARE $chat_id AS Uint64;
                DECLARE $msg_id AS Uint64;
                SELECT is_deleted FROM `messages`
                WHERE chat_id = $chat_id AND message_id = $msg_id;
                """
                parent_result = await session_obj.transaction().execute(
                    await session_obj.prepare(parent_query),
                    {'$chat_id': chat_id, '$msg_id': reply_to},
                    commit_tx=True
                )
                if not parent_result or not parent_result[0].rows:
                    raise NotFoundError(f"Parent message {reply_to} not found")
            
            # ===== 2.7 Подготовка attachments =====
            all_attachments = attachments or []
            if normalized_photo_ids:
                for photo_id in normalized_photo_ids:
                    all_attachments.append({
                        'photo_id': photo_id,
                        'type': 'photo',
                        'status': 'pending'
                    })
            
            # ===== 2.8 ПРОВЕРКА УПОМИНАНИЙ =====
            valid_users = []
            if mention_list:
                user_check_query = """
                DECLARE $user_ids AS List<Utf8>;
                SELECT id FROM `users`
                WHERE id IN $user_ids AND status = 'active';
                """
                user_result = await session_obj.transaction().execute(
                    await session_obj.prepare(user_check_query),
                    {'$user_ids': mention_list},
                    commit_tx=True
                )
                existing_users = [row['id'] for row in user_result[0].rows] if user_result and user_result[0].rows else []
                
                if existing_users:
                    member_check_query = """
                    DECLARE $chat_id AS Uint64;
                    DECLARE $user_ids AS List<Utf8>;
                    SELECT user_id FROM `chat_participants`
                    WHERE chat_id = $chat_id AND user_id IN $user_ids AND is_active = true;
                    """
                    member_result = await session_obj.transaction().execute(
                        await session_obj.prepare(member_check_query),
                        {'$chat_id': chat_id, '$user_ids': existing_users},
                        commit_tx=True
                    )
                    valid_users = [row['user_id'] for row in member_result[0].rows] if member_result and member_result[0].rows else []
            
            # ===== 2.9 НАЧИНАЕМ ТРАНЗАКЦИЮ =====
            tx = session_obj.transaction()
            await tx.begin()
            logger.info(f"🔓 [TX] Transaction started for message {message_id}")
            
            try:
                # ===== 2.10 Вставка сообщения =====
                epoch = datetime(1970, 1, 1)
                created_date_value = (now.date() - epoch.date()).days
                mentions_json = json.dumps(mention_list) if mention_list else None
                
                insert_query = """
                DECLARE $chat_id AS Uint64;
                DECLARE $message_id AS Uint64;
                DECLARE $user_id AS Utf8;
                DECLARE $user_role AS Utf8?;
                DECLARE $type AS Utf8;
                DECLARE $content AS Utf8?;
                DECLARE $preview AS Utf8?;
                DECLARE $attachments AS Json?;
                DECLARE $mentions AS Json?;
                DECLARE $reply_to AS Uint64?;
                DECLARE $thread_root AS Uint64?;
                DECLARE $created_date AS Date;
                
                INSERT INTO `messages` (
                    chat_id, message_id, sender_id, sender_role_at_time,
                    message_type, content, content_preview,
                    attachments_json, has_attachments,
                    mentions_json,
                    reply_to_message_id, thread_root_id,
                    created_at, created_date, version
                ) VALUES (
                    $chat_id, $message_id, $user_id, $user_role,
                    $type, $content, $preview,
                    $attachments, $attachments IS NOT NULL,
                    $mentions,
                    $reply_to, $thread_root,
                    CurrentUtcTimestamp(), $created_date, 1
                );
                
                UPDATE `chats` SET
                    last_message_id = $message_id,
                    last_message_at = CurrentUtcTimestamp(),
                    last_message_sender_id = $user_id,
                    last_message_preview = $preview,
                    messages_count = messages_count + 1
                WHERE id = $chat_id;
                
                UPDATE `chat_participants`
                SET unread_count = unread_count + CAST(1 AS Uint32)
                WHERE chat_id = $chat_id
                  AND user_id != $user_id
                  AND is_active = true;
                """
                
                insert_params = {
                    '$chat_id': chat_id,
                    '$message_id': message_id,
                    '$user_id': user_id,
                    '$user_role': participant.get('role'),
                    '$type': message_type,
                    '$content': content,
                    '$preview': content_preview,
                    '$attachments': json.dumps(all_attachments) if all_attachments else None,
                    '$mentions': mentions_json,
                    '$reply_to': reply_to,
                    '$thread_root': thread_root_id,
                    '$created_date': created_date_value
                }
                
                await tx.execute(
                    await session_obj.prepare(insert_query),
                    insert_params
                )
                logger.info(f"✅ [DB] Message inserted: {message_id}")
                
                # ===== 2.11 Обновление счетчика ответов =====
                if reply_to:
                    reply_query = """
                    DECLARE $chat_id AS Uint64;
                    DECLARE $message_id AS Uint64;
                    
                    UPDATE `messages`
                    SET reply_count = reply_count + CAST(1 AS Uint32)
                    WHERE chat_id = $chat_id AND message_id = $message_id;
                    """
                    await tx.execute(
                        await session_obj.prepare(reply_query),
                        {'$chat_id': chat_id, '$message_id': reply_to}
                    )
                    logger.info(f"✅ [DB] Reply count incremented for {reply_to}")
                
                # ===== 2.12 Привязка фото =====
                if normalized_photo_ids:
                    photo_query = """
                    DECLARE $message_id AS Uint64;
                    DECLARE $photo_ids AS List<Utf8>;
                    UPDATE `photo_uploads`
                    SET message_id = $message_id
                    WHERE photo_id IN $photo_ids;
                    """
                    await tx.execute(
                        await session_obj.prepare(photo_query),
                        {'$message_id': message_id, '$photo_ids': normalized_photo_ids}
                    )
                    logger.info(f"✅ [DB] Photos linked: {normalized_photo_ids}")
                
                # ===== 2.13 Удаление черновика =====
                draft_query = """
                DECLARE $chat_id AS Uint64;
                DECLARE $user_id AS Utf8;
                DELETE FROM `message_drafts`
                WHERE chat_id = $chat_id AND user_id = $user_id;
                """
                await tx.execute(
                    await session_obj.prepare(draft_query),
                    {'$chat_id': chat_id, '$user_id': user_id}
                )
                
                # ===== 2.14 СОЗДАНИЕ ОБЪЕКТА СООБЩЕНИЯ =====
                message = Message(
                    chat_id=chat_id,
                    message_id=message_id,
                    sender_id=user_id,
                    sender_role_at_time=participant.get('role'),
                    message_type=message_type,
                    content=content,
                    content_preview=content_preview,
                    attachments_json=all_attachments,
                    has_attachments=bool(all_attachments),
                    mentions_json=mention_list,
                    mentions=mention_list,
                    reply_to_message_id=reply_to,
                    thread_root_id=thread_root_id,
                    created_at=now,
                    version=1
                )
                logger.info(f"📦 [OBJ] Message object created: {message_id}")
                
                # ===== 2.15 Сохранение ключа идемпотентности с result_data =====
                if idempotency_key:
                    # Сериализуем сообщение в JSON для кэша
                    message_dict = message.to_dict()
                    logger.info(f"💾 [IDEM] Saving idempotency key: {idempotency_key[:8]}, result_data size={len(json.dumps(message_dict))} bytes")
                    
                    key_query = """
                    DECLARE $key AS Utf8;
                    DECLARE $entity_type AS Utf8;
                    DECLARE $entity_id AS Uint64;
                    DECLARE $chat_id AS Uint64;
                    DECLARE $user_id AS Utf8;
                    DECLARE $created_at AS Timestamp;
                    DECLARE $expires_at AS Timestamp;
                    DECLARE $result_data AS Json?;
                    
                    UPSERT INTO `idempotency_keys` 
                    (idempotency_key, entity_type, entity_id, chat_id, user_id, created_at, expires_at, result_data)
                    VALUES ($key, $entity_type, $entity_id, $chat_id, $user_id, $created_at, $expires_at, $result_data);
                    """
                    
                    await tx.execute(
                        await session_obj.prepare(key_query),
                        {
                            '$key': idempotency_key,
                            '$entity_type': 'message',
                            '$entity_id': message_id,
                            '$chat_id': chat_id,
                            '$user_id': user_id,
                            '$created_at': to_timestamp(now),
                            '$expires_at': to_timestamp(now + timedelta(hours=24)),
                            '$result_data': json.dumps(message_dict)
                        }
                    )
                    logger.info(f"✅ [IDEM] Idempotency key saved with result_data for {idempotency_key[:8]}")
                else:
                    logger.info(f"ℹ️ [IDEM] No idempotency key provided, skipping save")
                
                # ===== 2.16 КОММИТ ТРАНЗАКЦИИ =====
                await tx.commit()
                logger.info(f"✅ [TX] Transaction committed for message {message_id}")

                # ===== 2.16.1 ОБНОВЛЕНИЕ LAST MESSAGE (после коммита, отдельная TX) =====
                try:
                    chat_repo = ChatRepository(session_obj)
                    await chat_repo.update_last_message(
                        chat_id=chat_id,
                        message_id=message_id,
                        preview=content_preview,
                        sender_id=user_id,
                        at=now
                    )
                    logger.info(f"✅ [LAST_MSG] Updated last_message for chat {chat_id}")
                except Exception as e:
                    logger.error(f"❌ [LAST_MSG] Failed to update last_message for chat {chat_id}: {e}")

                duration = time.time() - start_time
                logger.info(f"✅✅✅ Message {message_id} sent in {duration*1000:.1f}ms")

                # ===== 2.17 WEBSOCKET УВЕДОМЛЕНИЕ =====
                asyncio.create_task(
                    self._notify_websocket(
                        chat_id=chat_id,
                        event_type='new_message',
                        data={
                            'message': message.to_dict(),
                            'chat_id': chat_id,
                            'sender_id': user_id,
                            'timestamp': now.isoformat()
                        },
                        exclude_user_id=user_id,
                        session=session_obj
                    )
                )
                
                return message
                
            except Exception as e:
                await tx.rollback()
                logger.error(f"❌ [TX] Transaction rolled back for message {message_id}: {e}", exc_info=True)
                raise

    def _generate_preview(self, content, attachments, photo_ids, photos):
        """Сгенерировать превью для сообщения"""
        if content:
            return content[:200] + '...' if len(content) > 200 else content
        elif attachments:
            return f"[{len(attachments)} attachment(s)]"
        elif photo_ids:
            return f"[{len(photo_ids)} photo(s)]"
        elif photos:
            return f"[{len(photos)} photo(s)]"
        return None
    
    def _extract_mentions_sync(self, content, mentions):
        """Синхронное извлечение упоминаний"""
        mention_list = mentions or []
        if content:
            import re
            mention_matches = re.findall(r'@([a-f0-9-]{36})', content)
            mention_list.extend([m for m in mention_matches if m not in mention_list])
        return mention_list
    
    async def _prepare_attachments(self, chat_id, attachments, photo_ids, photos, user_id, session):
        """Подготовка вложений"""
        all_attachments = attachments or []
        
        if photo_ids and photos:
            # Комбинируем существующие photo_ids с новыми photos
            photo_repo = PhotoUploadRepository(session)
            photos_map = await photo_repo.get_many(photo_ids)
            for photo_id in photo_ids:
                photo = photos_map.get(photo_id)
                if photo and photo.status == 'completed':
                    all_attachments.append({
                        'photo_id': photo_id,
                        'type': 'photo',
                        'caption': photo.caption or '',
                        'urls': {
                            'original': photo.url_original,
                            'large': photo.url_large,
                            'medium': photo.url_medium,
                            'small': photo.url_small
                        }
                    })
        
        if photos:
            # Новые фото - создаем записи и добавляем в attachments
            for photo_data in photos:
                photo_id = str(uuid.uuid4())
                photo = PhotoAttachment(
                    photo_id=photo_id,
                    chat_id=chat_id,
                    user_id=user_id,
                    status='pending',
                    caption=photo_data.get('caption', '')
                )
                photo_repo = PhotoUploadRepository(session)
                await photo_repo.create(photo)
                
                all_attachments.append({
                    'photo_id': photo_id,
                    'type': 'photo',
                    'status': 'pending',
                    'caption': photo_data.get('caption', '')
                })
                
                # Запускаем фоновую обработку
                asyncio.create_task(self._process_photo_upload(
                    photo_id=photo_id,
                    chat_id=chat_id,
                    user_id=user_id,
                    image_data=photo_data.get('photo'),
                    caption=photo_data.get('caption', '')
                ))
        
        return all_attachments
    
    async def _link_photos_to_message(self, photo_ids, message_id, session):
        """Привязать фото к сообщению"""
        photo_repo = PhotoUploadRepository(session)
        for photo_id in photo_ids:
            photo = await photo_repo.get(photo_id)
            if photo:
                photo.message_id = message_id
                await photo_repo.update(photo)

    async def _send_message_in_uow(
        self,
        uow,
        chat_id: int,
        user_id: str,
        content: Optional[str] = None,
        message_type: str = "text",
        attachments: Optional[List[Dict]] = None,
        idempotency_key: Optional[str] = None
    ) -> Message:
        """Внутренний метод для отправки сообщения в рамках существующего UOW"""
        # Получаем чат и участника
        chat, participant = await self._get_chat_and_participant(
            chat_id, 
            user_id, 
            session=uow._session  # 👈 ВАЖНО: передаем сессию!
        )

        now = datetime.utcnow()
        message_id = uuid.uuid4().int & (2**64 - 1)

        content_preview = None
        if content:
            content_preview = content[:200] if len(content) > 200 else content
        elif attachments:
            content_preview = f"[{len(attachments)} attachment(s)]"

        message = Message(
            chat_id=chat_id,
            message_id=message_id,
            sender_id=user_id,
            sender_role_at_time=participant.get('role'),
            message_type=message_type,
            content=content,
            content_preview=content_preview,
            attachments_json=attachments or [],
            has_attachments=bool(attachments),
            created_at=now
        )

        result = await uow.messages.create(message)
        if not result:
            raise DatabaseError("Failed to send message")

        preview = content[:100] + "..." if content and len(content) > 100 else content
        if not preview and attachments:
            preview = f"[{len(attachments)} attachment(s)]"

        await uow.chats.update_last_message(
            chat_id=chat_id,
            message_id=message_id,
            preview=preview,
            sender_id=user_id,
            at=now
        )

        await uow.participants.update_activity(chat_id, user_id)

        if idempotency_key:
            await self._save_idempotency_key(uow, idempotency_key, message_id, chat_id, user_id)

        return message


    async def get_chat_messages(
        self,
        chat_id: int,
        user_id: str,
        limit: int = 50,
        cursor: Optional[str] = None,
        before: Optional[int] = None,
        after: Optional[int] = None,
        message_type: Optional[str] = None,
        sender_id: Optional[str] = None,
        include_reply_preview: bool = True,
        session = None
    ) -> Tuple[List[Message], Optional[str], str, str, Optional[int]]:
        """
        УПРОЩЕННАЯ ВЕРСИЯ - без вложенных запросов
        """
        logger.info(f"📋 Getting messages for chat {chat_id} (limit={limit})")
        start_time = time.time()
        
        try:
            async with await MessageUnitOfWork.with_session(session) as uow:
                # Получаем информацию о чате
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")

                # Публичный канал — сообщения доступны всем без членства
                is_public_channel = chat.type == 'channel' and chat.is_public
                if not is_public_channel:
                    has_access = await self.participant_cache.is_member(
                        chat_id,
                        user_id,
                        session=session
                    )
                    if not has_access:
                        raise PermissionError("You don't have access to this chat")
                
                # 👇 ПРОСТОЙ ЗАПРОС
                query = """
                DECLARE $chat_id AS Uint64;
                DECLARE $limit AS Uint64;
                DECLARE $cursor_time AS Timestamp?;
                DECLARE $cursor_id AS Uint64?;
                DECLARE $before AS Uint64?;
                DECLARE $after AS Uint64?;
                DECLARE $message_type AS Utf8?;
                DECLARE $sender_id AS Utf8?;
                
                SELECT *
                FROM `messages`
                WHERE chat_id = $chat_id 
                    AND is_deleted = false
                    AND ($message_type IS NULL OR message_type = $message_type)
                    AND ($sender_id IS NULL OR sender_id = $sender_id)
                    AND ($before IS NULL OR message_id < $before)
                    AND ($after IS NULL OR message_id > $after)
                    AND ($cursor_time IS NULL 
                         OR created_at < $cursor_time 
                         OR (created_at = $cursor_time AND message_id < $cursor_id))
                ORDER BY created_at DESC, message_id DESC
                LIMIT $limit;
                """
                
                params = {
                    '$chat_id': chat_id,
                    '$limit': limit + 1,
                    '$cursor_time': None,
                    '$cursor_id': None,
                    '$before': before,
                    '$after': after,
                    '$message_type': message_type,
                    '$sender_id': sender_id
                }
                
                if cursor:
                    try:
                        cursor_time_str, cursor_id_str = cursor.split(':', 1)
                        cursor_time = datetime.fromisoformat(cursor_time_str)
                        if cursor_time.tzinfo:
                            cursor_time = cursor_time.replace(tzinfo=None)
                        params['$cursor_time'] = to_timestamp(cursor_time)
                        params['$cursor_id'] = int(cursor_id_str)
                    except Exception as e:
                        logger.error(f"Error parsing cursor: {e}")
                
                session_obj = uow._session
                result = await session_obj.transaction().execute(
                    await session_obj.prepare(query),
                    params,
                    commit_tx=True
                )
                
                rows = result[0].rows if result else []
                
                has_next = len(rows) > limit
                if has_next:
                    rows = rows[:limit]
                
                # Преобразуем строки в сообщения
                messages = []
                for row in rows:
                    message = Message.from_db_row(row)
                    messages.append(message)
                
                # 👇 ОТДЕЛЬНО получаем информацию об ответах (если нужно)
                if include_reply_preview and messages:
                    await self._attach_reply_previews_batch(chat_id, messages, uow._session)
                
                # Получаем информацию об участнике
                participant = await self.participant_cache.get_participant(
                    chat_id, 
                    user_id, 
                    session=session
                )
                user_role = participant.get('role') if participant else 'member'
                
                # Формируем следующий курсор
                next_cursor = None
                if has_next and messages:
                    last = messages[-1]
                    if last.created_at:
                        created_at_naive = last.created_at
                        if created_at_naive.tzinfo:
                            created_at_naive = created_at_naive.replace(tzinfo=None)
                        next_cursor = f"{created_at_naive.isoformat()}:{last.message_id}"
                
                discussion_chat_id = chat.linked_chat_id if chat.type == 'channel' else None
                
                duration = time.time() - start_time
                logger.info(f"✅ Got {len(messages)} messages in {duration*1000:.1f}ms")
                
                return messages, next_cursor, chat.type, user_role, discussion_chat_id
                
        except Exception as e:
            logger.error(f"❌ Error getting chat messages: {e}")
            return [], None, 'unknown', 'member', None

    async def get_message(self, chat_id: int, message_id: int, user_id: str, 
                          include_deleted: bool = False, session = None) -> Message:
        """Получить сообщение"""

        # 👇 ИСПРАВЛЕНО: передаем сессию в participant_cache
        has_access = await self.participant_cache.is_member(
            chat_id, 
            user_id, 
            session=session
        )
        if not has_access:
            raise PermissionError("You don't have access to this chat")

        # 👇 ИСПРАВЛЕНО: используем UOW с той же сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                raise NotFoundError(f"Message {message_id} not found")

            if message.is_deleted and not include_deleted:
                raise NotFoundError("Message has been deleted")

        if not message.is_deleted:
            await background_tasks.add_task(self._safe_increment_view(chat_id, message_id))

        return message

    async def get_message_with_reply(self, chat_id: int, message_id: int, user_id: str, 
                                      include_deleted: bool = False, session = None) -> Message:
        """Получить сообщение с информацией об ответе"""

        # 👇 ИСПРАВЛЕНО: передаем сессию в participant_cache
        has_access = await self.participant_cache.is_member(
            chat_id, 
            user_id, 
            session=session
        )
        if not has_access:
            raise PermissionError("You don't have access to this chat")

        # 👇 ИСПРАВЛЕНО: используем UOW с той же сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                raise NotFoundError(f"Message {message_id} not found")

            if message.is_deleted and not include_deleted:
                raise NotFoundError("Message has been deleted")

            if message.reply_to_message_id:
                reply_message = await uow.messages.get(chat_id, message.reply_to_message_id)
                if reply_message:
                    message.reply_to_info = {
                        'id': reply_message.message_id,
                        'sender_id': reply_message.sender_id,
                        'content_preview': reply_message.content[:150] + '...' if reply_message.content and len(reply_message.content) > 150 else reply_message.content,
                        'created_at': reply_message.created_at.isoformat() if reply_message.created_at else None,
                        'is_deleted': reply_message.is_deleted,
                        'message_type': reply_message.message_type
                    }
                    if reply_message.is_deleted:
                        message.reply_to_info['content_preview'] = '[Message deleted]'
                else:
                    message.reply_to_info = {
                        'id': message.reply_to_message_id,
                        'is_deleted': True,
                        'content_preview': '[Message not found]'
                    }

        if not message.is_deleted:
            await background_tasks.add_task(self._safe_increment_view(chat_id, message_id))

        return message

    async def get_thread_messages(
        self,
        chat_id: int,
        thread_root_id: int,
        user_id: str,
        limit: int = 50,
        cursor: Optional[str] = None,
        session = None
    ) -> Tuple[List[Message], Optional[str]]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - использует индекс idx_messages_thread
        """
        logger.info(f"📋 Getting thread messages for root {thread_root_id} in chat {chat_id}")
        start_time = time.time()
        
        try:
            # Проверяем доступ
            has_access = await self.participant_cache.is_member(
                chat_id, 
                user_id, 
                session=session
            )
            if not has_access:
                raise PermissionError("You don't have access to this chat")
            
            limit = min(limit, 100)
            
            async with await MessageUnitOfWork.with_session(session) as uow:
                session_obj = uow._session
                
                # 👇 ИСПОЛЬЗУЕМ ИНДЕКС idx_messages_thread
                query = """
                DECLARE $thread_root_id AS Uint64;
                DECLARE $limit AS Uint64;
                DECLARE $cursor_time AS Timestamp?;
                DECLARE $cursor_id AS Uint64?;
                
                SELECT *
                FROM `messages`
                WHERE thread_root_id = $thread_root_id 
                  AND is_deleted = false
                  AND ($cursor_time IS NULL 
                       OR created_at > $cursor_time 
                       OR (created_at = $cursor_time AND message_id > $cursor_id))
                ORDER BY created_at ASC, message_id ASC
                LIMIT $limit;
                """
                
                params = {
                    '$thread_root_id': thread_root_id,
                    '$limit': limit + 1,
                    '$cursor_time': None,
                    '$cursor_id': None
                }
                
                if cursor:
                    try:
                        cursor_time_str, cursor_id_str = cursor.split(':', 1)
                        cursor_time = datetime.fromisoformat(cursor_time_str)
                        if cursor_time.tzinfo:
                            cursor_time = cursor_time.replace(tzinfo=None)
                        params['$cursor_time'] = to_timestamp(cursor_time)
                        params['$cursor_id'] = int(cursor_id_str)
                        logger.info(f"📌 Using cursor: {cursor_time_str} / {cursor_id_str}")
                    except Exception as e:
                        logger.error(f"Error parsing cursor: {e}")
                
                prepared_query = await session_obj.prepare(query)
                result = await session_obj.transaction().execute(
                    prepared_query,
                    params,
                    commit_tx=True
                )
                
                rows = result[0].rows if result and len(result) > 0 else []
                
                has_next = len(rows) > limit
                if has_next:
                    rows = rows[:limit]
                
                messages = [Message.from_db_row(row) for row in rows]
                
                # Формируем следующий курсор
                next_cursor = None
                if has_next and messages:
                    last = messages[-1]
                    if last.created_at:
                        created_at_naive = last.created_at
                        if created_at_naive.tzinfo:
                            created_at_naive = created_at_naive.replace(tzinfo=None)
                        next_cursor = f"{created_at_naive.isoformat()}:{last.message_id}"
                
                duration = time.time() - start_time
                logger.info(f"✅ Got {len(messages)} thread messages in {duration*1000:.1f}ms")
                
                return messages, next_cursor
                
        except Exception as e:
            logger.error(f"❌ Error getting thread messages: {e}")
            return [], None

    async def edit_message(
        self,
        chat_id: int,
        message_id: int,
        user_id: str,
        new_content: Optional[str] = None,
        new_photos: Optional[List[Dict]] = None,
        new_photo_ids: Optional[List[str]] = None,
        entities: Optional[Dict] = None,
        session = None
    ) -> Message:
        """Редактировать сообщение с WebSocket уведомлением"""

        # Валидация входных параметров
        if new_content is None and new_photos is None and new_photo_ids is None and entities is None:
            raise ValidationError("Nothing to update")

        if new_photo_ids is not None and not isinstance(new_photo_ids, list):
            raise ValidationError("new_photo_ids must be a list")

        async with await MessageUnitOfWork.with_session(session) as uow:
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                raise NotFoundError(f"Message {message_id} not found")

            if message.is_deleted:
                raise ValidationError("Cannot edit deleted message")

            if message.sender_id != user_id:
                is_admin = await self.participant_cache.check_permission(
                    chat_id, user_id, ['owner', 'admin']
                )
                if not is_admin:
                    raise PermissionError("You can only edit your own messages")

            now = datetime.utcnow()
            msg_time = message.created_at
            if msg_time.tzinfo:
                msg_time = msg_time.replace(tzinfo=None)

            if msg_time < now - timedelta(hours=message_config.EDIT_TIME_LIMIT_HOURS):
                raise PermissionError(f"Cannot edit messages older than {message_config.EDIT_TIME_LIMIT_HOURS} hours")

            if not message.edit_history:
                message.edit_history = []

            message.edit_history.append({
                'old_content': message.content,
                'old_attachments': message.attachments_json,
                'old_entities': message.entities,
                'edited_at': now.isoformat(),
                'edited_by': user_id
            })

            if new_content is not None:
                message.content = new_content
                message.content_preview = new_content[:200] if new_content else None
                message.is_edited = True
                message.edit_count += 1
                message.last_edit_at = now

            if new_photo_ids == []:
                if message.attachments_json:
                    message.attachments_json = [
                        a for a in message.attachments_json
                        if a.get('type') != 'photo'
                    ]
                message.has_attachments = bool(message.attachments_json)
                if not message.attachments_json:
                    message.message_type = 'text'

            elif new_photo_ids and len(new_photo_ids) > 0:
                photos_map = await uow.photos.get_many(new_photo_ids)

                new_photo_attachments = []
                for photo_id in new_photo_ids:
                    photo = photos_map.get(photo_id)
                    if photo and photo.status == PhotoStatus.COMPLETED:
                        new_photo_attachments.append({
                            'photo_id': photo_id,
                            'type': 'photo',
                            'caption': photo.caption or '',
                            'urls': {
                                'original': photo.url_original,
                                'large': photo.url_large,
                                'medium': photo.url_medium,
                                'small': photo.url_small
                            },
                            'width': photo.width,
                            'height': photo.height,
                            'file_size': photo.file_size,
                            'mime_type': photo.mime_type
                        })

                        if not photo.message_id:
                            photo.message_id = message_id
                            await uow.photos.update(photo)

                if message.attachments_json:
                    non_photo_attachments = [
                        a for a in message.attachments_json
                        if a.get('type') != 'photo'
                    ]
                    message.attachments_json = non_photo_attachments + new_photo_attachments
                else:
                    message.attachments_json = new_photo_attachments

                message.has_attachments = bool(message.attachments_json)

                if new_photo_attachments:
                    if len(new_photo_attachments) == 1 and not non_photo_attachments:
                        message.message_type = 'image'
                    elif len(new_photo_attachments) > 1 and not non_photo_attachments:
                        message.message_type = 'album'

            if new_photos:
                uploaded_photos = await self.upload_message_photos(chat_id, user_id, new_photos)
                await asyncio.sleep(1)

                new_photo_attachments = []
                for uploaded in uploaded_photos:
                    photo = await uow.photos.get(uploaded['photo_id'])
                    if photo and photo.status == PhotoStatus.COMPLETED:
                        new_photo_attachments.append({
                            'photo_id': photo.photo_id,
                            'type': 'photo',
                            'caption': photo.caption or '',
                            'urls': {
                                'original': photo.url_original,
                                'large': photo.url_large,
                                'medium': photo.url_medium,
                                'small': photo.url_small
                            },
                            'width': photo.width,
                            'height': photo.height,
                            'file_size': photo.file_size,
                            'mime_type': photo.mime_type
                        })

                        photo.message_id = message_id
                        await uow.photos.update(photo)

                if new_photo_attachments:
                    if not message.attachments_json:
                        message.attachments_json = []
                    message.attachments_json.extend(new_photo_attachments)
                    message.has_attachments = True

            if entities:
                if not message.entities:
                    message.entities = {}
                message.entities.update(entities)

            success = await uow.messages.update(message)
            if not success:
                raise DatabaseError("Failed to edit message")

        updated_message = await self.get_message_with_reply(chat_id, message_id, user_id)

        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_websocket(
                chat_id=chat_id,
                event_type="message_edited",
                data={
                    'message': updated_message.to_dict(),
                    'chat_id': chat_id,
                    'message_id': message_id,
                    'edited_by': user_id,
                    'timestamp': now.isoformat()
                },
                exclude_user_id=user_id
            )
        )

        return updated_message

    async def delete_message(
        self,
        chat_id: int,
        message_id: int,
        user_id: str,
        permanent: bool = False,
        reason: Optional[str] = None,
        session = None
    ) -> Dict:
        """Удалить сообщение с WebSocket уведомлением"""

        async with await MessageUnitOfWork.with_session(session) as uow:
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                raise NotFoundError(f"Message {message_id} not found")

            can_delete = message.sender_id == user_id
            if not can_delete:
                is_admin = await self.participant_cache.check_permission(
                    chat_id, user_id, ['owner', 'admin']
                )
                can_delete = is_admin

            if not can_delete:
                raise PermissionError("You don't have permission to delete this message")

            from handlers.chat_handler import ChatUnitOfWork
            async with ChatUnitOfWork() as chat_uow:
                pinned = await chat_uow.pinned.get_active(chat_id)
                if pinned and pinned.message_id == message_id:
                    await chat_uow.pinned.unpin_by_id(message_id, chat_id, user_id)

            if message.attachments_json:
                for attachment in message.attachments_json:
                    if attachment.get('type') == 'photo' and attachment.get('photo_id'):
                        photo = await uow.photos.get(attachment['photo_id'])
                        if photo:
                            photo.message_id = None
                            await uow.photos.update(photo)

            success = await uow.messages.delete(chat_id, message_id, permanent)
            if not success:
                raise DatabaseError("Failed to delete message")

            if not permanent:
                message.is_deleted = True
                message.deleted_at = datetime.utcnow()
                message.delete_reason = reason
                await uow.messages.update(message)

        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_websocket(
                chat_id=chat_id,
                event_type="message_deleted",
                data={
                    "message_id": str(message_id),
                    "chat_id": str(chat_id),
                    "deleted_by": user_id,
                    "reason": reason,
                    "permanent": permanent,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        )

        return {
            'deleted': True,
            'message_id': str(message_id),
            'permanent': permanent,
            'deleted_by': user_id
        }

    async def forward_message(
        self,
        source_chat_id: int,
        message_id: int,
        user_id: str,
        target_chat_id: int,
        comment: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        session = None
    ) -> Message:
        """Переслать сообщение с WebSocket уведомлением"""

        has_source_access = await self.participant_cache.is_member(source_chat_id, user_id, session=session)
        if not has_source_access:
            raise PermissionError("You don't have access to the source chat")

        has_target_access = await self.participant_cache.is_member(target_chat_id, user_id, session=session)
        if not has_target_access:
            raise PermissionError("You don't have access to the target chat")

        async with await MessageUnitOfWork.with_session(session) as uow:
            if idempotency_key:
                existing_key = await uow.idempotency.get(idempotency_key)
                if existing_key:
                    existing_msg = await uow.messages.get(target_chat_id, int(existing_key.entity_id))
                    if existing_msg:
                        return existing_msg

            source_message = await uow.messages.get(source_chat_id, message_id)
            if not source_message or source_message.is_deleted:
                raise NotFoundError(f"Message {message_id} not found")

            target_chat = await uow.chats.get_by_id(target_chat_id)
            if not target_chat or target_chat.is_deleted:
                raise NotFoundError(f"Target chat {target_chat_id} not found")

            target_participant = await uow.participants.get(target_chat_id, user_id)
            if target_participant and target_participant.is_blocked:
                raise PermissionError("You are blocked in the target chat")

            now = datetime.utcnow()
            new_message_id = uuid.uuid4().int & (2**64 - 1)

            original_sender_name = source_message.sender_id[:8] if source_message.sender_id else "Unknown"

            final_content = comment
            if comment and source_message.content:
                final_content = f"{comment}\n\n---\n{source_message.content}"
            elif not comment and source_message.content:
                final_content = source_message.content

            source_message.forward_count += 1
            await uow.messages.update(source_message)

            forwarded_message = Message(
                chat_id=target_chat_id,
                message_id=new_message_id,
                sender_id=user_id,
                sender_role_at_time=target_participant.role if target_participant else None,
                message_type=source_message.message_type,
                content=final_content,
                content_preview=final_content[:200] if final_content else None,
                mentions_json=[],
                attachments_json=source_message.attachments_json,
                has_attachments=source_message.has_attachments,
                entities=source_message.entities,
                forwarded_from_message_id=message_id,
                forwarded_from_chat_id=source_chat_id,
                forwarded_by=user_id,
                forwarded_at=now,
                forward_comment=comment,
                forward_count=0,
                forwarded_original_sender_id=source_message.sender_id,
                forwarded_original_date=source_message.created_at,
                forwarded_original_sender_name=original_sender_name,
                created_at=now
            )

            result = await uow.messages.create(forwarded_message)
            if not result:
                raise DatabaseError("Failed to forward message")

            preview = final_content[:100] + "..." if final_content and len(final_content) > 100 else final_content
            if not preview and source_message.attachments_json:
                preview = f"[Forwarded: {source_message.attachments_json[0].get('type', 'attachment')}]"
            else:
                preview = "[Forwarded message]"

            await uow.chats.update_last_message(
                chat_id=target_chat_id,
                message_id=new_message_id,
                preview=preview,
                sender_id=user_id,
                at=now
            )

            await uow.participants.update_activity(target_chat_id, user_id)
            
            if target_chat.type != 'channel':
                members = await uow.participants.list_by_chat(target_chat_id, limit=10000, active_only=True)
                await self._batch_increment_unread(uow, target_chat_id, user_id, members)

            if idempotency_key:
                await self._save_idempotency_key(uow, idempotency_key, new_message_id, target_chat_id, user_id)

        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_websocket(
                chat_id=target_chat_id,
                event_type="message_forwarded",
                data={
                    'message': forwarded_message.to_dict(),
                    'source_chat_id': source_chat_id,
                    'source_message_id': message_id,
                    'forwarded_by': user_id,
                    'timestamp': now.isoformat()
                }
            )
        )

        return forwarded_message

    async def add_reaction(
        self,
        chat_id: int,
        message_id: int,
        user_id: str,
        reaction: str,
        session = None
    ) -> Message:
        """Добавить реакцию с WebSocket уведомлением"""

        logger.info(f"🎯 Adding reaction {reaction} to message {message_id} in chat {chat_id} by user {user_id}")

        has_access = await self.participant_cache.is_member(chat_id, user_id, session=session)
        if not has_access:
            logger.warning(f"⛔ User {user_id} has no access to chat {chat_id}")
            raise PermissionError("You don't have access to this chat")

        MessageValidator.validate_reaction(reaction)

        async with await MessageUnitOfWork.with_session(session) as uow:
            logger.info(f"📦 Getting message {message_id} from database")
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                logger.error(f"❌ Message {message_id} not found in database")
                raise NotFoundError(f"Message {message_id} not found")

            if message.is_deleted:
                logger.warning(f"🗑️ Message {message_id} is deleted")
                raise NotFoundError("Message has been deleted")

            logger.info(f"✅ Message {message_id} found, getting chat info")
            chat = await uow.chats.get_by_id(chat_id)

            if chat and chat.reactions_settings:
                settings = chat.reactions_settings
                logger.info(f"⚙️ Reaction settings: {settings}")

                if settings.get('allowed_reactions') and reaction not in settings['allowed_reactions']:
                    logger.warning(f"⛔ Reaction {reaction} not allowed in chat {chat_id}")
                    raise ValidationError(f"Reaction '{reaction}' is not allowed")

            logger.info(f"🔍 Checking if user already has this reaction")
            user_reactions = await uow.reactions.get_user_reactions(chat_id, message_id, user_id)
            logger.info(f"📊 User already has reactions: {user_reactions}")

            if reaction in user_reactions:
                logger.info(f"🔄 User already has reaction {reaction}, removing (double-tap)")

                await uow.reactions.remove(chat_id, message_id, user_id, reaction)

                if message.reactions_json and reaction in message.reactions_json:
                    if message.reactions_json[reaction] > 1:
                        message.reactions_json[reaction] -= 1
                    else:
                        del message.reactions_json[reaction]

                if message.recent_reactions:
                    message.recent_reactions = [
                        r for r in message.recent_reactions
                        if not (r.get('user_id') == user_id and r.get('reaction') == reaction)
                    ]

                logger.info(f"💾 Saving updated message after removal")
                await uow.messages.update(message)

                if chat and (chat.type == 'channel' or chat.is_discussion):
                    logger.info(f"🔄 Syncing reaction removal to discussion")
                    await self._sync_reaction_json_only(chat_id, message_id, user_id, reaction, is_add=False)

                logger.info(f"✅ Reaction removed successfully (double-tap)")
                
                # WEBSOCKET УВЕДОМЛЕНИЕ
                asyncio.create_task(
                    self._notify_websocket(
                        chat_id=chat_id,
                        event_type="reaction_removed",
                        data={
                            'chat_id': chat_id,
                            'message_id': message_id,
                            'user_id': user_id,
                            'reaction': reaction,
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    )
                )
                
                return message

            if chat and chat.reactions_settings:
                settings = chat.reactions_settings

                if settings.get('max_per_user', 1) == 1 and user_reactions:
                    logger.info(f"⚠️ User already has {len(user_reactions)} reactions, replacing")

                    reactions_to_remove = [(chat_id, message_id, user_id, r) for r in user_reactions]
                    await uow.reactions.remove_batch(reactions_to_remove)

                    if not message.reactions_json:
                        message.reactions_json = {}
                    for old_reaction in user_reactions:
                        if old_reaction in message.reactions_json:
                            if message.reactions_json[old_reaction] > 1:
                                message.reactions_json[old_reaction] -= 1
                            else:
                                del message.reactions_json[old_reaction]

                if not settings.get('allow_multiple', False) and user_reactions:
                    logger.warning(f"⛔ User already has reactions and multiple not allowed")
                    raise PermissionError("You can only add one reaction per message")

            logger.info(f"➕ Adding reaction to database")
            reaction_obj = MessageReaction(
                chat_id=chat_id,
                message_id=message_id,
                user_id=user_id,
                reaction=reaction,
                created_at=datetime.utcnow()
            )
            await uow.reactions.add(reaction_obj)

            logger.info(f"🔄 Updating message reactions_json")
            if not message.reactions_json:
                message.reactions_json = {}

            message.reactions_json[reaction] = message.reactions_json.get(reaction, 0) + 1
            logger.info(f"📊 New reactions state: {message.reactions_json}")

            if not message.recent_reactions:
                message.recent_reactions = []

            message.recent_reactions = [r for r in message.recent_reactions if r.get('user_id') != user_id]

            message.recent_reactions.insert(0, {
                'user_id': user_id,
                'reaction': reaction,
                'added_at': datetime.utcnow().isoformat()
            })

            message.recent_reactions = message.recent_reactions[:10]

            logger.info(f"💾 Saving updated message")
            await uow.messages.update(message)

            if chat and (chat.type == 'channel' or chat.is_discussion):
                logger.info(f"🔄 Syncing reaction to discussion")
                await self._sync_reaction_json_only(chat_id, message_id, user_id, reaction, is_add=True)

            logger.info(f"✅ Reaction added successfully")

            # WEBSOCKET УВЕДОМЛЕНИЕ
            asyncio.create_task(
                self._notify_websocket(
                    chat_id=chat_id,
                    event_type="reaction_added",
                    data={
                        'chat_id': chat_id,
                        'message_id': message_id,
                        'user_id': user_id,
                        'reaction': reaction,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                )
            )

            return message

    async def remove_reaction(
        self,
        chat_id: int,
        message_id: int,
        user_id: str,
        reaction: str,
        session = None
    ) -> Message:
        """Удалить реакцию с WebSocket уведомлением"""

        logger.info(f"🗑️ Removing reaction {reaction} from message {message_id} in chat {chat_id} by user {user_id}")

        has_access = await self.participant_cache.is_member(chat_id, user_id, session=session)
        if not has_access:
            logger.warning(f"⛔ User {user_id} has no access to chat {chat_id}")
            raise PermissionError("You don't have access to this chat")

        async with await MessageUnitOfWork.with_session(session) as uow:
            logger.info(f"📦 Getting message {message_id} from database")
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                logger.error(f"❌ Message {message_id} not found in database")
                raise NotFoundError(f"Message {message_id} not found")

            if message.is_deleted:
                logger.warning(f"🗑️ Message {message_id} is deleted")
                raise NotFoundError("Message has been deleted")

            logger.info(f"✅ Message {message_id} found, getting chat info")
            chat = await uow.chats.get_by_id(chat_id)

            logger.info(f"🔍 Removing reaction from database")
            await uow.reactions.remove(chat_id, message_id, user_id, reaction)

            logger.info(f"🔄 Updating message reactions_json")
            if message.reactions_json and reaction in message.reactions_json:
                if message.reactions_json[reaction] > 1:
                    message.reactions_json[reaction] -= 1
                else:
                    del message.reactions_json[reaction]

            if message.recent_reactions:
                message.recent_reactions = [
                    r for r in message.recent_reactions
                    if not (r.get('user_id') == user_id and r.get('reaction') == reaction)
                ]

            logger.info(f"📊 New reactions state: {message.reactions_json}")
            logger.info(f"📊 New recent_reactions state: {message.recent_reactions}")

            logger.info(f"💾 Saving updated message")
            await uow.messages.update(message)

            if chat and (chat.type == 'channel' or chat.is_discussion):
                logger.info(f"🔄 Syncing reaction removal to discussion")
                await self._sync_reaction_json_only(chat_id, message_id, user_id, reaction, is_add=False)

            logger.info(f"✅ Reaction removed successfully")

            # WEBSOCKET УВЕДОМЛЕНИЕ
            asyncio.create_task(
                self._notify_websocket(
                    chat_id=chat_id,
                    event_type="reaction_removed",
                    data={
                        'chat_id': chat_id,
                        'message_id': message_id,
                        'user_id': user_id,
                        'reaction': reaction,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                )
            )

        return await self.get_message_with_reply(chat_id, message_id, user_id)

    async def get_reaction_users(
        self,
        chat_id: int,
        message_id: int,
        reaction: str,
        user_id: str,
        limit: int = 100,
        session = None
    ) -> List[str]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - использует индекс
        """
        logger.info(f"👥 Getting users for reaction {reaction} on message {message_id}")
        start_time = time.time()
        
        try:
            # Проверяем доступ
            has_access = await self.participant_cache.is_member(
                chat_id, 
                user_id, 
                session=session
            )
            if not has_access:
                raise PermissionError("You don't have access to this chat")
            
            async with await MessageUnitOfWork.with_session(session) as uow:
                # 👇 Используем метод репозитория (уже оптимизирован)
                users = await uow.reactions.get_users_by_reaction(
                    chat_id, message_id, reaction, limit
                )
                
                duration = time.time() - start_time
                logger.info(f"✅ Found {len(users)} users in {duration*1000:.1f}ms")
                
                return users
                
        except Exception as e:
            logger.error(f"❌ Error getting reaction users: {e}")
            return []

    async def get_top_messages_by_reactions(
        self,
        chat_id: int,
        user_id: str,
        limit: int = 10,
        session = None
    ) -> List[Dict]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - использует индекс idx_reactions_top
        """
        logger.info(f"📊 Getting top messages by reactions in chat {chat_id}")
        start_time = time.time()
        
        try:
            # Проверяем доступ
            has_access = await self.participant_cache.is_member(
                chat_id, 
                user_id, 
                session=session
            )
            if not has_access:
                raise PermissionError("You don't have access to this chat")
            
            limit = min(limit, 50)
            
            async with await MessageUnitOfWork.with_session(session) as uow:
                # 👇 ПРЯМОЙ SQL С ИСПОЛЬЗОВАНИЕМ ИНДЕКСА
                query = """
                DECLARE $chat_id AS Uint64;
                DECLARE $limit AS Uint64;
                
                SELECT 
                    message_id,
                    COUNT(*) as total_reactions,
                    COUNT(DISTINCT user_id) as unique_users
                FROM `message_reactions`
                WHERE chat_id = $chat_id
                GROUP BY message_id
                ORDER BY total_reactions DESC
                LIMIT $limit;
                """
                
                params = {
                    '$chat_id': chat_id,
                    '$limit': limit
                }
                
                prepared_query = await uow._session.prepare(query)
                result = await uow._transaction.execute(
                    prepared_query,
                    params,
                    commit_tx=False
                )
                
                rows = result[0].rows if result and len(result) > 0 else []
                
                # Получаем информацию о сообщениях
                result_list = []
                for row in rows:
                    message = await uow.messages.get(chat_id, row['message_id'])
                    if message and not message.is_deleted:
                        result_list.append({
                            'message_id': row['message_id'],
                            'total_reactions': row['total_reactions'],
                            'unique_users': row['unique_users'],
                            'content_preview': message.content[:100] if message.content else None,
                            'sender_id': message.sender_id,
                            'created_at': message.created_at.isoformat() if message.created_at else None
                        })
                
                duration = time.time() - start_time
                logger.info(f"✅ Got top {len(result_list)} messages in {duration*1000:.1f}ms")
                
                return result_list
                
        except Exception as e:
            logger.error(f"❌ Error getting top messages: {e}")
            return []
       
  
    async def save_draft(self, chat_id: int, user_id: str, content: Optional[str] = None,
                        attachments: Optional[List] = None, reply_to: Optional[int] = None,
                        entities: Optional[Dict] = None, session = None) -> Draft:
        """Сохранить черновик"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            draft = Draft(
                chat_id=chat_id,
                user_id=user_id,
                content=content,
                attachments=attachments,
                reply_to_message_id=reply_to,
                entities=entities or {},
                updated_at=datetime.utcnow()
            )

            success = await uow.drafts.save(draft)
            if not success:
                raise DatabaseError("Failed to save draft")

            return draft

    async def get_draft(self, chat_id: int, user_id: str, session = None) -> Optional[Draft]:
        """Получить черновик"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.drafts.get(chat_id, user_id)

    async def delete_draft(self, chat_id: int, user_id: str, session = None) -> bool:
        """Удалить черновик"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.drafts.delete(chat_id, user_id)

    async def get_all_drafts(self, user_id: str, limit: int = 50, offset: int = 0, session = None) -> List[Draft]:
        """Получить все черновики пользователя"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.drafts.list_by_user(user_id, limit, offset)

    async def save_message(self, user_id: str, message_id: int, chat_id: int,
                          notes: Optional[str] = None, collections: Optional[List] = None,
                          importance: int = 5, session = None) -> SavedMessage:
        """Сохранить сообщение"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                raise NotFoundError(f"Message {message_id} not found")

            existing = await uow.saved.get(user_id, message_id)
            if existing:
                return existing

            saved = SavedMessage(
                user_id=user_id,
                message_id=message_id,
                chat_id=chat_id,
                saved_at=datetime.utcnow(),
                notes=notes,
                collections=collections,
                importance=importance
            )

            success = await uow.saved.save(saved)
            if not success:
                raise DatabaseError("Failed to save message")

            if saved.message_exists:
                message_obj = await uow.messages.get(chat_id, message_id)
                if not message_obj or message_obj.is_deleted:
                    saved.message_exists = False
                    await background_tasks.add_task(uow.saved.save(saved))

            return saved

    async def unsave_message(self, user_id: str, message_id: int, session = None) -> bool:
        """Удалить из сохраненных"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.saved.delete(user_id, message_id)

    async def get_saved_messages(self, user_id: str, collection: Optional[str] = None,
                                limit: int = 50, cursor: Optional[str] = None, session = None) -> Tuple[List[SavedMessage], Optional[str]]:
        """Получить сохраненные сообщения"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            saved, next_cursor = await uow.saved.list_by_user(
                user_id=user_id,
                collection=collection,
                limit=limit,
                cursor=cursor
            )

            for item in saved:
                if item.message_exists:
                    message = await uow.messages.get(item.chat_id, item.message_id)
                    if not message or message.is_deleted:
                        item.message_exists = False
                        await background_tasks.add_task(uow.saved.save(item))

            return saved, next_cursor

    async def upload_attachment(self, chat_id: int, user_id: str, filename: str,
                               mime_type: str, file_type: str, file_data_b64: str,
                               metadata: Optional[Dict] = None, duration: Optional[int] = None,
                               session = None) -> Attachment:
        """Загрузить вложение"""

        # 👇 ИСПРАВЛЕНО: передаем сессию в participant_cache
        has_access = await self.participant_cache.is_member(chat_id, user_id, session=session)
        if not has_access:
            raise PermissionError("You don't have access to this chat")

        try:
            file_data = base64.b64decode(file_data_b64)
        except:
            raise ValidationError("Invalid base64 file_data")

        AttachmentValidator.validate_upload(len(file_data), mime_type, file_type, duration)

        timestamp = int(time.time())
        file_hash = hashlib.md5(file_data).hexdigest()[:8]
        ext = filename.split('.')[-1] if '.' in filename else 'bin'
        safe_filename = f"{timestamp}_{file_hash}.{ext}"

        from config.config import config
        file_url = f"https://storage.yandexcloud.net/{config.OBJECT_STORAGE_BUCKET}/chats/{chat_id}/{safe_filename}"

        attachment = Attachment(
            chat_id=chat_id,
            type=file_type,
            url=file_url,
            file_name=filename,
            file_size=len(file_data),
            mime_type=mime_type,
            uploaded_by=user_id,
            uploaded_at=datetime.utcnow(),
            metadata=metadata or {},
            duration=duration
        )

        if file_type == 'image' and metadata:
            attachment.width = metadata.get('width')
            attachment.height = metadata.get('height')

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            result = await uow.attachments.create(attachment)
            if not result:
                raise DatabaseError("Failed to upload attachment")

            return result

    async def get_attachment(self, attachment_id: str, user_id: str, session = None) -> Attachment:
        """Получить вложение"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            attachment = await uow.attachments.get(attachment_id)
            if not attachment:
                raise NotFoundError(f"Attachment {attachment_id} not found")

            has_access = await self.participant_cache.check_access(attachment.chat_id, user_id, session=session)
            if not has_access:
                raise PermissionError("You don't have access to this attachment")

            return attachment

    async def get_contacts(self, user_id: str, favorites_only: bool = False,
                          limit: int = 50, cursor: Optional[str] = None, session = None) -> Tuple[List[Contact], Optional[str]]:
        """Получить контакты"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.contacts.list_by_user(
                user_id=user_id,
                favorites_only=favorites_only,
                limit=limit,
                cursor=cursor
            )

    async def add_contact(self, user_id: str, contact_id: str, first_name: Optional[str] = None,
                         last_name: Optional[str] = None, phone: Optional[str] = None,
                         is_favorite: bool = False, session = None) -> Contact:
        """Добавить контакт"""

        ContactValidator.validate_contact_id(contact_id)
        if phone:
            ContactValidator.validate_phone(phone)

        if user_id == contact_id:
            raise ValidationError("Cannot add yourself as contact")

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            existing = await uow.contacts.get(user_id, contact_id)
            if existing:
                return existing

            contact = Contact(
                user_id=user_id,
                contact_id=contact_id,
                first_name=first_name,
                last_name=last_name,
                phone=phone,
                added_at=datetime.utcnow(),
                source='manual',
                is_favorite=is_favorite,
                is_blocked=False
            )

            success = await uow.contacts.create(contact)
            if not success:
                raise DatabaseError("Failed to add contact")

            return contact

    async def update_contact(self, user_id: str, contact_id: str, updates: Dict, session = None) -> Contact:
        """Обновить контакт"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            contact = await uow.contacts.get(user_id, contact_id)
            if not contact:
                raise NotFoundError("Contact not found")

            for key, value in updates.items():
                if hasattr(contact, key):
                    setattr(contact, key, value)

            if 'phone' in updates:
                ContactValidator.validate_phone(updates['phone'])

            success = await uow.contacts.update(contact)
            if not success:
                raise DatabaseError("Failed to update contact")

            return contact

    async def delete_contact(self, user_id: str, contact_id: str, session = None) -> bool:
        """Удалить контакт"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.contacts.delete(user_id, contact_id)

    async def search_contacts(self, user_id: str, query: str, session = None) -> List[Contact]:
        """Поиск по контактам"""

        if len(query) < 2:
            raise ValidationError("Search query must be at least 2 characters")

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.contacts.search(user_id, query)

    async def block_user(self, user_id: str, blocked_id: str, reason: Optional[str] = None, session = None) -> Block:
        """Заблокировать пользователя"""

        if user_id == blocked_id:
            raise ValidationError("Cannot block yourself")

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            existing = await uow.blocks.get(user_id, blocked_id)
            if existing:
                return existing

            block = Block(
                user_id=user_id,
                blocked_id=blocked_id,
                blocked_at=datetime.utcnow(),
                reason=reason
            )

            success = await uow.blocks.create(block)
            if not success:
                raise DatabaseError("Failed to block user")

            contact = await uow.contacts.get(user_id, blocked_id)
            if contact:
                await uow.contacts.delete(user_id, blocked_id)

            return block

    async def unblock_user(self, user_id: str, blocked_id: str, session = None) -> bool:
        """Разблокировать пользователя"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.blocks.delete(user_id, blocked_id)

    async def get_blocked_users(self, user_id: str, session = None) -> List[Block]:
        """Получить список заблокированных"""

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            return await uow.blocks.list_by_user(user_id)

    async def get_user_stats(self, user_id: str, session = None) -> Dict:
        """
        РАБОЧАЯ ВЕРСИЯ - 4 отдельных запроса, результат кэшируется на 60с
        """
        cache_key = f"user_stats:{user_id}"
        cached = await cache.get(cache_key)
        if cached is not None:
            return cached

        logger.info(f"📊 Getting stats for user {user_id}")
        start_time = time.time()
        
        try:
            async with await MessageUnitOfWork.with_session(session) as uow:
                session_obj = uow._session
                
                # 👇 1. Статистика чатов
                chats_query = """
                DECLARE $user_id AS Utf8;
                SELECT 
                    COUNT(*) as total_chats,
                    MIN(joined_at) as first_joined,
                    MAX(last_active_at) as last_active
                FROM `chat_participants`
                WHERE user_id = $user_id AND is_active = true;
                """
                chats_result = await session_obj.transaction().execute(
                    await session_obj.prepare(chats_query),
                    {'$user_id': user_id},
                    commit_tx=True
                )
                chats_row = chats_result[0].rows[0] if chats_result and chats_result[0].rows else {}
                
                # 👇 2. Контакты
                contacts_query = """
                DECLARE $user_id AS Uint64;
                SELECT COUNT(*) as total_contacts
                FROM `contacts`
                WHERE user_id = $user_id;
                """
                contacts_result = await session_obj.transaction().execute(
                    await session_obj.prepare(contacts_query),
                    {'$user_id': to_uint64(user_id)},
                    commit_tx=True
                )
                contacts_count = contacts_result[0].rows[0]['total_contacts'] if contacts_result and contacts_result[0].rows else 0
                
                # 👇 3. Блокировки
                blocks_query = """
                DECLARE $user_id AS Utf8;
                SELECT COUNT(*) as total_blocks
                FROM `user_blocks`
                WHERE blocker_id = $user_id;
                """
                blocks_result = await session_obj.transaction().execute(
                    await session_obj.prepare(blocks_query),
                    {'$user_id': user_id},
                    commit_tx=True
                )
                blocks_count = blocks_result[0].rows[0]['total_blocks'] if blocks_result and blocks_result[0].rows else 0
                
                # 👇 4. Сохраненные
                saved_query = """
                DECLARE $user_id AS Uint64;
                SELECT COUNT(*) as total_saved
                FROM `saved_messages`
                WHERE user_id = $user_id;
                """
                saved_result = await session_obj.transaction().execute(
                    await session_obj.prepare(saved_query),
                    {'$user_id': to_uint64(user_id)},
                    commit_tx=True
                )
                saved_count = saved_result[0].rows[0]['total_saved'] if saved_result and saved_result[0].rows else 0
                
                result_data = {
                    'user_id': user_id,
                    'total_chats': chats_row.get('total_chats', 0),
                    'joined_at': from_timestamp(chats_row.get('first_joined')).isoformat() if chats_row.get('first_joined') else None,
                    'last_active': from_timestamp(chats_row.get('last_active')).isoformat() if chats_row.get('last_active') else None,
                    'contacts_count': contacts_count,
                    'blocked_count': blocks_count,
                    'saved_messages_count': saved_count
                }
                
                duration = time.time() - start_time
                logger.info(f"✅ Got user stats in {duration*1000:.1f}ms")

                await cache.set(cache_key, result_data, ttl=60)
                return result_data
                
        except Exception as e:
            logger.error(f"❌ Error getting user stats: {e}")
            return {
                'user_id': user_id,
                'total_chats': 0,
                'joined_at': None,
                'last_active': None,
                'contacts_count': 0,
                'blocked_count': 0,
                'saved_messages_count': 0
            }

    async def get_original_message(
        self,
        chat_id: int,
        message_id: int,
        user_id: str,
        session = None  # 👈 добавляем параметр session
    ) -> Dict:
        """Получить оригинал пересланного сообщения"""

        # 👇 ИСПРАВЛЕНО: передаем сессию
        forwarded_message = await self.get_message_with_reply(chat_id, message_id, user_id, session=session)

        if not forwarded_message.forwarded_from_message_id:
            raise ValidationError("Message is not forwarded")

        has_access = await self.participant_cache.check_access(
            forwarded_message.forwarded_from_chat_id,
            user_id,
            session=session
        )
        if not has_access:
            raise PermissionError("You don't have access to the original chat")

        # 👇 ИСПРАВЛЕНО: передаем сессию
        original_message = await self.get_message_with_reply(
            forwarded_message.forwarded_from_chat_id,
            forwarded_message.forwarded_from_message_id,
            user_id,
            session=session
        )

        # 👇 ИСПРАВЛЕНО: используем UOW с переданной сессией
        async with await MessageUnitOfWork.with_session(session) as uow:
            source_chat = await uow.chats.get_by_id(forwarded_message.forwarded_from_chat_id)
            target_chat = await uow.chats.get_by_id(chat_id)

        return {
            'original': {
                'message': original_message.to_dict(),
                'chat': {
                    'id': source_chat.id if source_chat else None,
                    'title': source_chat.title if source_chat else None,
                    'type': source_chat.type if source_chat else None
                }
            },
            'forwarded': {
                'message': forwarded_message.to_dict(),
                'chat': {
                    'id': target_chat.id if target_chat else None,
                    'title': target_chat.title if target_chat else None,
                    'type': target_chat.type if target_chat else None
                },
                'forwarded_by': forwarded_message.forwarded_by,
                'forwarded_at': forwarded_message.forwarded_at.isoformat() if forwarded_message.forwarded_at else None,
                'comment': forwarded_message.forward_comment
            }
        }

    async def _attach_original_previews_batch(self, chat_id: int, messages: List[Message], session):
        """
        Batch получение информации об оригинальных сообщениях
        """
        if not messages:
            return
            
        # Собираем ID оригинальных сообщений
        original_ids = set()
        for msg in messages:
            if msg.forwarded_from_message_id:
                original_ids.add((msg.forwarded_from_chat_id, msg.forwarded_from_message_id))

        if not original_ids:
            return

        # Группируем по chat_id для batch запроса
        by_chat = {}
        for orig_chat_id, orig_msg_id in original_ids:
            if orig_chat_id not in by_chat:
                by_chat[orig_chat_id] = []
            by_chat[orig_chat_id].append(orig_msg_id)

        # Получаем все оригинальные сообщения
        msg_repo = MessageRepository(session)
        original_messages = {}
        
        for orig_chat_id, msg_ids in by_chat.items():
            msgs_map = await msg_repo.get_many(orig_chat_id, msg_ids)
            original_messages.update(msgs_map)

        # Добавляем информацию к пересланным сообщениям
        for msg in messages:
            if msg.forwarded_from_message_id:
                orig_msg = original_messages.get(msg.forwarded_from_message_id)
                if orig_msg:
                    msg.forwarded_original_info = {
                        'content_preview': orig_msg.content[:200] if orig_msg.content else None,
                        'sender_id': orig_msg.sender_id,
                        'created_at': orig_msg.created_at.isoformat() if orig_msg.created_at else None,
                        'is_deleted': orig_msg.is_deleted
                    }

    async def get_forwarded_messages(
        self,
        chat_id: int,
        user_id: str,
        limit: int = 50,
        cursor: Optional[str] = None,
        session = None
    ) -> Tuple[List[Message], Optional[str]]:
        """
        ПОЛУЧИТЬ ПЕРЕСЛАННЫЕ СООБЩЕНИЯ - ИСПРАВЛЕННАЯ ВЕРСИЯ
        """
        logger.info("=" * 60)
        logger.info(f"🔍 GET FORWARDED MESSAGES - START")
        logger.info("=" * 60)
        logger.info(f"📋 INPUT PARAMETERS:")
        logger.info(f"  - chat_id: {chat_id} (type: {type(chat_id)})")
        logger.info(f"  - user_id: {user_id}")
        logger.info(f"  - limit: {limit}")
        logger.info(f"  - cursor: {cursor}")
        logger.info(f"  - session provided: {session is not None}")
        
        start_time = time.time()
        
        try:
            # Проверяем доступ
            logger.info("🔐 Checking access...")
            has_access = await self.participant_cache.is_member(
                chat_id, 
                user_id, 
                session=session
            )
            logger.info(f"  - has_access: {has_access}")
            
            if not has_access:
                logger.warning(f"⛔ User {user_id} has no access to chat {chat_id}")
                raise PermissionError("You don't have access to this chat")
            
            logger.info(f"✅ Access granted for user {user_id}")
            
            limit = min(limit, 100)
            logger.info(f"📊 Adjusted limit: {limit}")
            
            async with await MessageUnitOfWork.with_session(session) as uow:
                logger.info(f"🔄 UOW session created")
                session_obj = uow._session
                
                # ========== ДИАГНОСТИКА 1: ВСЕ СООБЩЕНИЯ В ЧАТЕ ==========
                logger.info("=" * 40)
                logger.info("🔍 DIAGNOSTIC: ALL messages in chat")
                logger.info("=" * 40)
                
                debug_query = """
                DECLARE $chat_id AS Uint64;
                SELECT 
                    message_id,
                    content,
                    forwarded_from_message_id,
                    is_deleted
                FROM `messages`
                WHERE chat_id = $chat_id
                ORDER BY created_at DESC
                LIMIT 20;
                """
                
                prepared_debug = await session_obj.prepare(debug_query)
                debug_result = await session_obj.transaction().execute(
                    prepared_debug,
                    {'$chat_id': chat_id},
                    commit_tx=True
                )
                
                if debug_result and debug_result[0].rows:
                    rows = debug_result[0].rows
                    logger.info(f"📋 Found {len(rows)} messages in chat {chat_id}:")
                    
                    for i, row in enumerate(rows):
                        fwd_from = row.get('forwarded_from_message_id')
                        is_deleted = row.get('is_deleted')
                        logger.info(f"  Message {i}:")
                        logger.info(f"    - message_id: {row.get('message_id')}")
                        logger.info(f"    - content: {str(row.get('content', ''))[:30]}...")
                        logger.info(f"    - forwarded_from_message_id: {fwd_from}")
                        logger.info(f"    - is_deleted: {is_deleted}")
                        
                        if fwd_from is not None:
                            logger.info(f"    ✅ SHOULD be included!")
                
                # ========== ОСНОВНОЙ ЗАПРОС (БЕЗ УСЛОВИЯ is_deleted) ==========
                logger.info("=" * 40)
                logger.info("🔍 MAIN QUERY: forwarded messages")
                logger.info("=" * 40)
                
                query = """
                DECLARE $chat_id AS Uint64;
                DECLARE $limit AS Uint64;
                DECLARE $cursor_time AS Timestamp?;
                DECLARE $cursor_id AS Uint64?;
                
                SELECT *
                FROM `messages`
                WHERE chat_id = $chat_id 
                  AND forwarded_from_message_id IS NOT NULL
                  AND ($cursor_time IS NULL 
                       OR created_at < $cursor_time 
                       OR (created_at = $cursor_time AND message_id < $cursor_id))
                ORDER BY created_at DESC, message_id DESC
                LIMIT $limit;
                """
                
                logger.info(f"📝 Query template prepared")
                
                params = {
                    '$chat_id': chat_id,
                    '$limit': limit + 1,
                    '$cursor_time': None,
                    '$cursor_id': None
                }
                
                if cursor:
                    try:
                        cursor_time_str, cursor_id_str = cursor.split(':', 1)
                        logger.info(f"📌 Parsing cursor: '{cursor_time_str}' / '{cursor_id_str}'")
                        cursor_time = datetime.fromisoformat(cursor_time_str)
                        if cursor_time.tzinfo:
                            cursor_time = cursor_time.replace(tzinfo=None)
                        params['$cursor_time'] = to_timestamp(cursor_time)
                        params['$cursor_id'] = int(cursor_id_str)
                        logger.info(f"✅ Cursor parsed: time={cursor_time}, id={cursor_id_str}")
                    except Exception as e:
                        logger.error(f"❌ Error parsing cursor: {e}")
                
                logger.info(f"📦 Query params: {params}")
                logger.info("🚀 Executing main query...")
                
                prepared_query = await session_obj.prepare(query)
                result = await session_obj.transaction().execute(
                    prepared_query,
                    params,
                    commit_tx=True
                )
                
                logger.info(f"📊 Result type: {type(result)}")
                logger.info(f"📊 Result length: {len(result) if result else 0}")
                
                rows = result[0].rows if result and len(result) > 0 else []
                logger.info(f"🔍 Main query returned {len(rows)} rows")
                
                # ========== ОБРАБОТКА РЕЗУЛЬТАТОВ ==========
                has_next = len(rows) > limit
                if has_next:
                    rows = rows[:limit]
                    logger.info(f"📌 Has next page, truncating to {limit}")
                
                messages = []
                valid_count = 0
                
                logger.info(f"🔄 Processing {len(rows)} rows...")
                for i, row in enumerate(rows):
                    logger.info(f"  Row {i}:")
                    logger.info(f"    - message_id: {row.get('message_id')}")
                    logger.info(f"    - forwarded_from_message_id: {row.get('forwarded_from_message_id')}")
                    
                    message = Message.from_db_row(row)
                    
                    if message.forwarded_from_message_id:
                        valid_count += 1
                        logger.info(f"    ✅ Has forwarded_from_message_id = {message.forwarded_from_message_id}")
                        
                        # Добавляем информацию о пересылке
                        message.forwarded_from = {
                            'chat_id': str(message.forwarded_from_chat_id) if message.forwarded_from_chat_id else None,
                            'message_id': str(message.forwarded_from_message_id),
                            'original_sender_id': message.forwarded_original_sender_id,
                            'original_sender_name': message.forwarded_original_sender_name,
                            'original_date': message.forwarded_original_date.isoformat() if message.forwarded_original_date else None,
                            'forwarded_by': message.forwarded_by,
                            'forwarded_at': message.forwarded_at.isoformat() if message.forwarded_at else None,
                            'comment': message.forward_comment
                        }
                    
                    messages.append(message)
                
                logger.info(f"📊 FINAL SUMMARY: {valid_count} forwarded messages out of {len(rows)}")
                
                # Формируем следующий курсор
                next_cursor = None
                if has_next and messages:
                    last = messages[-1]
                    if last.created_at:
                        created_at_naive = last.created_at
                        if created_at_naive.tzinfo:
                            created_at_naive = created_at_naive.replace(tzinfo=None)
                        next_cursor = f"{created_at_naive.isoformat()}:{last.message_id}"
                        logger.info(f"📌 Next cursor: {next_cursor}")
                
                duration = time.time() - start_time
                logger.info(f"⏱️ Total execution time: {duration*1000:.1f}ms")
                logger.info("=" * 60)
                logger.info(f"🔍 GET FORWARDED MESSAGES - END")
                logger.info("=" * 60)
                
                return messages, next_cursor
                
        except Exception as e:
            logger.error(f"❌ Error getting forwarded messages: {e}")
            logger.error("Full traceback:", exc_info=True)
            logger.info("=" * 60)
            logger.info(f"🔍 GET FORWARDED MESSAGES - END (with error)")
            logger.info("=" * 60)
            return [], None

    async def get_forwarding_info(
        self,
        chat_id: int,
        message_id: int,
        user_id: str,
        session = None  # 👈 добавляем параметр session
    ) -> Dict:
        """Получить информацию о пересылке"""

        # 👇 ИСПРАВЛЕНО: передаем сессию в get_message
        message = await self.get_message(
            chat_id, 
            message_id, 
            user_id,
            session=session  # 👈 передаем сессию!
        )

        if not message.forwarded_from_message_id:
            return {
                'is_forwarded': False,
                'message': message.to_dict()
            }

        async with await MessageUnitOfWork.with_session(session) as uow:
            original_message = await uow.messages.get(
                message.forwarded_from_chat_id,
                message.forwarded_from_message_id
            )

            source_chat = await uow.chats.get_by_id(message.forwarded_from_chat_id)
            target_chat = await uow.chats.get_by_id(chat_id)

        forward_chain = []
        current = message
        max_depth = 5

        while current and current.forwarded_from_message_id and max_depth > 0:
            forward_chain.append({
                'message_id': current.message_id,
                'chat_id': current.chat_id,
                'forwarded_by': current.forwarded_by,
                'forwarded_at': current.forwarded_at.isoformat() if current.forwarded_at else None
            })

            if current.forwarded_from_message_id:
                async with await MessageUnitOfWork.with_session(session) as uow:
                    current = await uow.messages.get(
                        current.forwarded_from_chat_id,
                        current.forwarded_from_message_id
                    )
            max_depth -= 1

        return {
            'is_forwarded': True,
            'forward_count': message.forward_count,
            'original': {
                'message_id': message.forwarded_from_message_id,
                'chat_id': message.forwarded_from_chat_id,
                'chat_title': source_chat.title if source_chat else None,
                'sender_id': message.forwarded_original_sender_id,
                'sender_name': message.forwarded_original_sender_name,
                'created_at': message.forwarded_original_date.isoformat() if message.forwarded_original_date else None
            } if original_message else None,
            'forwarded_by': message.forwarded_by,
            'forwarded_at': message.forwarded_at.isoformat() if message.forwarded_at else None,
            'comment': message.forward_comment,
            'forward_chain': forward_chain,
            'message': message.to_dict()
        }

    async def sync_reaction_to_discussion(self, chat_id: int, message_id: int, user_id: str,
                                         reaction: str, is_add: bool = True, session = None) -> None:
        """Синхронизировать реакцию между каналом и обсуждением"""

        async with await MessageUnitOfWork.with_session(session) as uow:
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                return

            chat = await uow.chats.get_by_id(chat_id)
            if not chat:
                return

            target_chat_id = None
            target_message_id = None

            if chat.type == 'channel' and chat.linked_chat_id:
                target_chat_id = chat.linked_chat_id

                discussion_messages, _ = await uow.messages.list_by_chat(target_chat_id, limit=100)
                for msg in discussion_messages:
                    if (msg.forwarded_from_chat_id == chat_id and
                        msg.forwarded_from_message_id == message_id):
                        target_message_id = msg.message_id
                        break

            elif chat.type == 'group' and chat.is_discussion:
                if message.forwarded_from_chat_id and message.forwarded_from_message_id:
                    target_chat_id = message.forwarded_from_chat_id
                    target_message_id = message.forwarded_from_message_id

            if not target_chat_id or not target_message_id:
                return

            reaction_obj = MessageReaction(
                chat_id=target_chat_id,
                message_id=target_message_id,
                user_id=user_id,
                reaction=reaction,
                created_at=datetime.utcnow()
            )

            if is_add:
                await uow.reactions.add(reaction_obj)
            else:
                await uow.reactions.remove(target_chat_id, target_message_id, user_id, reaction)

            target_message = await uow.messages.get(target_chat_id, target_message_id)
            if target_message:
                if is_add:
                    if not target_message.reactions_json:
                        target_message.reactions_json = {}
                    target_message.reactions_json[reaction] = target_message.reactions_json.get(reaction, 0) + 1
                else:
                    if target_message.reactions_json and reaction in target_message.reactions_json:
                        if target_message.reactions_json[reaction] > 1:
                            target_message.reactions_json[reaction] -= 1
                        else:
                            del target_message.reactions_json[reaction]

                await uow.messages.update(target_message)

    async def _sync_reaction_json_only(self, chat_id: int, message_id: int, user_id: str,
                                      reaction: str, is_add: bool = True, session = None) -> None:
        """Синхронизировать только JSON поле реакции"""

        async with await MessageUnitOfWork.with_session(session) as uow:
            message = await uow.messages.get(chat_id, message_id)
            if not message:
                return

            chat = await uow.chats.get_by_id(chat_id)
            if not chat:
                return

            target_chat_id = None
            target_message_id = None

            if chat.type == 'channel' and chat.linked_chat_id:
                target_chat_id = chat.linked_chat_id

                discussion_messages, _ = await uow.messages.list_by_chat(target_chat_id, limit=100)
                for msg in discussion_messages:
                    if (msg.forwarded_from_chat_id == chat_id and
                        msg.forwarded_from_message_id == message_id):
                        target_message_id = msg.message_id
                        break

            elif chat.type == 'group' and chat.is_discussion:
                if message.forwarded_from_chat_id and message.forwarded_from_message_id:
                    target_chat_id = message.forwarded_from_chat_id
                    target_message_id = message.forwarded_from_message_id

            if not target_chat_id or not target_message_id:
                return

            target_message = await uow.messages.get(target_chat_id, target_message_id)
            if not target_message:
                return

            if is_add:
                if not target_message.reactions_json:
                    target_message.reactions_json = {}
                target_message.reactions_json[reaction] = target_message.reactions_json.get(reaction, 0) + 1
            else:
                if target_message.reactions_json and reaction in target_message.reactions_json:
                    if target_message.reactions_json[reaction] > 1:
                        target_message.reactions_json[reaction] -= 1
                    else:
                        del target_message.reactions_json[reaction]

            await uow.messages.update(target_message)

    async def sync_reply_count_to_channel(self, discussion_message_id: int, chat_id: int, session = None) -> None:
        """Синхронизировать счетчик комментариев в канал"""

        async with await MessageUnitOfWork.with_session(session) as uow:
            discussion_message = await uow.messages.get(chat_id, discussion_message_id)
            if not discussion_message:
                return

            if not discussion_message.linked_channel_message_id:
                return

            channel_message_id = discussion_message.linked_channel_message_id

            channel_id = None
            if discussion_message.forwarded_from_chat_id:
                channel_id = discussion_message.forwarded_from_chat_id
            else:
                chat = await uow.chats.get_by_id(chat_id)
                if chat and chat.is_discussion:
                    channel = await uow.chats.get_channel_by_discussion(chat_id)
                    if channel:
                        channel_id = channel.id
                    else:
                        return
                else:
                    return

            channel_message = await uow.messages.get(channel_id, channel_message_id)
            if not channel_message:
                return

            total_reply_count = await uow.messages.count_all_replies(chat_id, discussion_message_id)

            if channel_message.reply_count != total_reply_count:
                channel_message.reply_count = total_reply_count
                await uow.messages.update(channel_message)

    async def create_discussion_post(self, channel_id: int, message_id: int, user_id: str, session = None) -> Optional[Message]:
        """Создать пост в обсуждении"""

        async with await MessageUnitOfWork.with_session(session) as uow:
            channel = await uow.chats.get_by_id(channel_id)
            if not channel or not channel.linked_chat_id:
                return None

            channel_message = await uow.messages.get(channel_id, message_id)
            if not channel_message:
                return None

            discussion_chat = await uow.chats.get_by_id(channel.linked_chat_id)
            if not discussion_chat:
                return None

            settings = discussion_chat.discussion_settings or {}
            if not settings.get('auto_post', True):
                return None

            now = datetime.utcnow()
            discussion_message_id = uuid.uuid4().int & (2**64 - 1)

            content = f"📢 **Новый пост в канале**\n\n{channel_message.content}\n\n[Перейти к посту](https://t.me/c/{channel_id}/{message_id})"

            discussion_message = Message(
                chat_id=discussion_chat.id,
                message_id=discussion_message_id,
                sender_id=user_id,
                sender_role_at_time=None,
                message_type=channel_message.message_type,
                content=content,
                content_preview=channel_message.content_preview,
                mentions_json=[],
                attachments_json=channel_message.attachments_json,
                has_attachments=channel_message.has_attachments,
                entities=channel_message.entities,
                reply_to_message_id=None,
                thread_root_id=None,
                created_at=now,
                forwarded_from_message_id=message_id,
                forwarded_from_chat_id=channel_id,
                linked_channel_message_id=message_id
            )

            result = await uow.messages.create(discussion_message)
            if not result:
                return None

            channel_message.linked_discussion_message_id = discussion_message_id
            await uow.messages.update(channel_message)

            preview = channel_message.content_preview or "[New post]"
            await uow.chats.update_last_message(
                chat_id=discussion_chat.id,
                message_id=discussion_message_id,
                preview=preview,
                sender_id=user_id,
                at=now
            )

            return discussion_message

    async def get_discussion_comments(self, channel_id: int, channel_message_id: int, user_id: str, session = None) -> List[Message]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - один запрос вместо нескольких
        """
        chat_repo = ChatRepository(session)
        msg_repo = MessageRepository(session)
        
        # 👇 ОДИН JOIN-ЗАПРОС
        query = """
        DECLARE $channel_id AS Uint64;
        DECLARE $channel_message_id AS Uint64;
        
        $discussion = (
            SELECT linked_chat_id 
            FROM `chats` 
            WHERE id = $channel_id AND linked_chat_id IS NOT NULL
        );
        
        $discussion_post = (
            SELECT message_id 
            FROM `messages` 
            WHERE chat_id IN $discussion 
              AND forwarded_from_chat_id = $channel_id 
              AND forwarded_from_message_id = $channel_message_id
            LIMIT 1
        );
        
        SELECT m.* 
        FROM `messages` m
        WHERE m.chat_id IN $discussion 
          AND m.reply_to_message_id IN $discussion_post
          AND m.is_deleted = false
        ORDER BY m.created_at ASC;
        """
        
        rows = await msg_repo.execute(query, {
            '$channel_id': channel_id,
            '$channel_message_id': channel_message_id
        })
        
        return [Message.from_db_row(row) for row in rows]


class MessageHandler(BaseHandler):
    """Обработчик HTTP запросов для сообщений"""

    def __init__(self):
        super().__init__()
        self.service = MessageService()
        logger.info("✅ MessageHandler initialized (full optimized version)")

    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========
    
    async def _enrich_messages_with_senders(self, messages: List[Message], session) -> List[Dict]:
        """
        Обогащает сообщения данными об отправителях (username, first_name, avatar_url)
        """
        logger.info(f"🔍 _enrich_messages_with_senders called with {len(messages)} messages")
        
        if not messages:
            logger.info("  📭 No messages to enrich")
            return []
        
        # Собираем уникальные ID отправителей
        sender_ids = list(set(m.sender_id for m in messages if m.sender_id))
        logger.info(f"  📋 Found sender IDs: {sender_ids}")
        
        if not sender_ids:
            logger.info("  📭 No sender IDs found")
            return [m.to_dict() for m in messages]
        
        # Получаем данные пользователей одним запросом (с avatar_url)
        users_query = """
        DECLARE $user_ids AS List<Utf8>;
        SELECT id, username, first_name_encrypted, avatar_url
        FROM `users`
        WHERE id IN $user_ids;
        """
        
        try:
            logger.info(f"  🔍 Executing users query for {len(sender_ids)} users")
            users_result = await session.transaction().execute(
                await session.prepare(users_query),
                {'$user_ids': sender_ids},
                commit_tx=True
            )
            
            logger.info(f"  📊 users_result type: {type(users_result)}")
            if users_result and len(users_result) > 0:
                rows_count = len(users_result[0].rows) if users_result[0].rows else 0
                logger.info(f"  📊 users_result[0].rows: {rows_count} rows")
            else:
                logger.info("  ⚠️ users_result is empty or None")
            
            # Создаем словарь user_id -> данные пользователя
            users_map = {}
            if users_result and users_result[0].rows:
                for row in users_result[0].rows:
                    # Получаем зашифрованное имя
                    first_name_enc = row.get('first_name_encrypted', '')
                    
                    # Декодируем из base64
                    first_name = ''
                    if first_name_enc:
                        try:
                            import base64
                            first_name = base64.b64decode(first_name_enc).decode('utf-8')
                            logger.info(f"    ✅ Decoded first_name: {first_name}")
                        except Exception as e:
                            logger.error(f"    ❌ Base64 decode error: {e}")
                            first_name = first_name_enc
                    
                    # Получаем URL аватара
                    avatar_url = row.get('avatar_url')
                    if avatar_url and not avatar_url.startswith('http'):
                        from config.config import config
                        avatar_url = f"{config.OBJECT_STORAGE_PUBLIC_URL}/{avatar_url}"
                    
                    users_map[row['id']] = {
                        'username': row.get('username'),
                        'first_name': first_name,
                        'avatar_url': avatar_url
                    }
                logger.info(f"  ✅ Created users_map with {len(users_map)} entries")
                
                # Логируем первые несколько записей для отладки
                for i, (uid, data) in enumerate(list(users_map.items())[:3]):
                    logger.info(f"    User {i+1}: {uid} -> {data}")
            else:
                logger.warning("  ⚠️ No users found in database")
            
            # Обогащаем сообщения
            enriched = []
            for i, msg in enumerate(messages):
                msg_dict = msg.to_dict()
                logger.info(f"  📝 Processing message {i+1}/{len(messages)}: id={msg.message_id}, sender_id={msg.sender_id}")
                
                if msg.sender_id and msg.sender_id in users_map:
                    msg_dict['sender'] = users_map[msg.sender_id]
                    logger.info(f"    ✅ Added sender data for {msg.sender_id}")
                else:
                    logger.info(f"    ⚠️ No sender data found for {msg.sender_id}")
                
                enriched.append(msg_dict)
            
            logger.info(f"  ✅ Enriched {len(enriched)} messages")
            return enriched
            
        except Exception as e:
            logger.error(f"❌ Error enriching messages with senders: {e}", exc_info=True)
            # В случае ошибки возвращаем сообщения без данных отправителей
            return [m.to_dict() for m in messages]
    
    async def _enrich_message_with_sender(self, message: Message, session) -> Dict:
        """
        Обогащает одно сообщение данными об отправителе (username, first_name, avatar_url)
        """
        if not message or not message.sender_id:
            return message.to_dict() if message else {}
        
        logger.info(f"🔍 _enrich_message_with_sender called for message {message.message_id}")
        
        # Получаем данные отправителя с avatar_url
        users_query = """
        DECLARE $user_id AS Utf8;
        SELECT username, first_name_encrypted, avatar_url
        FROM `users`
        WHERE id = $user_id;
        """
        
        try:
            users_result = await session.transaction().execute(
                await session.prepare(users_query),
                {'$user_id': message.sender_id},
                commit_tx=True
            )
            
            message_dict = message.to_dict()
            
            if users_result and users_result[0].rows:
                row = users_result[0].rows[0]
                first_name_enc = row.get('first_name_encrypted', '')
                first_name = ''
                if first_name_enc:
                    try:
                        import base64
                        first_name = base64.b64decode(first_name_enc).decode('utf-8')
                        logger.info(f"    ✅ Decoded first_name: {first_name}")
                    except Exception as e:
                        logger.error(f"    ❌ Base64 decode error: {e}")
                        first_name = first_name_enc
                
                # Получаем URL аватара
                avatar_url = row.get('avatar_url')
                if avatar_url and not avatar_url.startswith('http'):
                    from config.config import config
                    avatar_url = f"{config.OBJECT_STORAGE_PUBLIC_URL}/{avatar_url}"
                
                message_dict['sender'] = {
                    'username': row.get('username'),
                    'first_name': first_name,
                    'avatar_url': avatar_url
                }
                logger.info(f"    ✅ Added sender data with avatar")
            
            return message_dict
            
        except Exception as e:
            logger.error(f"❌ Error enriching message with sender: {e}", exc_info=True)
            return message.to_dict()
    
    async def _check_common_chat(self, user_id: str, target_user_id: str) -> bool:
        """Проверить, есть ли у пользователей общий чат"""
        try:
            async with RequestContext() as ctx:
                from handlers.chat_handler import ParticipantRepository
                participant_repo = ParticipantRepository(ctx.session)

                # Получаем чаты первого пользователя
                user_chats = await participant_repo.list_by_user(user_id, limit=1000)

                for p in user_chats:
                    # Проверяем, есть ли второй пользователь в этом чате
                    other = await participant_repo.get(p.chat_id, target_user_id)
                    if other:
                        return True

            return False
        except Exception as e:
            logger.error(f"Error checking common chat: {e}")
            return False

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_notifications(self, event: Dict, user: Dict) -> Dict:
        """
        GET /notifications - Получить уведомления пользователя

        Параметры:
            limit: количество (по умолчанию 50, макс 100)
            offset: смещение
            unread_only: только непрочитанные (true/false)
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            limit = self._get_int_query_param(event, 'limit', 50)
            offset = self._get_int_query_param(event, 'offset', 0)
            unread_only = self._get_bool_query_param(event, 'unread_only', False)

            limit = min(limit, 100)

            # 👇 ИСПОЛЬЗУЕМ RequestContext ДЛЯ ОДНОЙ СЕССИИ
            async with RequestContext() as ctx:
                repo = NotificationRepository(ctx.session)
                notifications, total = await repo.get_by_user(
                    user_id=user_id,
                    limit=limit,
                    offset=offset,
                    unread_only=unread_only
                )

                # Получаем количество непрочитанных
                unread_count = await repo.get_unread_count(user_id)

            return self.response.success({
                'notifications': [n.to_dict() for n in notifications],
                'count': len(notifications),
                'total': total,
                'unread_count': unread_count,
                'limit': limit,
                'offset': offset
            }, 200)

        except ValidationError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error getting notifications: {e}")
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_unread_count(self, event: Dict, user: Dict) -> Dict:
        """
        GET /notifications/unread-count - Получить количество непрочитанных
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПОЛЬЗУЕМ RequestContext ДЛЯ ОДНОЙ СЕССИИ
            async with RequestContext() as ctx:
                repo = NotificationRepository(ctx.session)
                count = await repo.get_unread_count(user_id)

            return self.response.success({
                'unread_count': count
            }, 200)

        except ValidationError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error getting unread count: {e}")
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_mark_notification_read(self, event: Dict, user: Dict, notification_id: int) -> Dict:
        """
        POST /notifications/{notificationId}/read - Отметить уведомление как прочитанное
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            notification_id = self._validate_message_id(notification_id)

            # 👇 ИСПОЛЬЗУЕМ RequestContext ДЛЯ ОДНОЙ СЕССИИ
            async with RequestContext() as ctx:
                repo = NotificationRepository(ctx.session)
                success = await repo.mark_as_read(notification_id, user_id)

                if not success:
                    return self.response.error(
                        message="Notification not found",
                        code="not_found",
                        status_code=404,
                        event=event
                    )

                # Получаем обновленный счетчик
                unread_count = await repo.get_unread_count(user_id)

            return self.response.success({
                'marked_read': True,
                'notification_id': notification_id,
                'unread_count': unread_count
            }, 200)

        except ValidationError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error marking notification as read: {e}")
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_mark_all_read(self, event: Dict, user: Dict) -> Dict:
        """
        POST /notifications/read-all - Отметить все как прочитанные
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПОЛЬЗУЕМ RequestContext ДЛЯ ОДНОЙ СЕССИИ
            async with RequestContext() as ctx:
                repo = NotificationRepository(ctx.session)
                marked_count = await repo.mark_all_as_read(user_id)

            return self.response.success({
                'marked_all': True,
                'count': marked_count
            }, 200)

        except ValidationError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error marking all as read: {e}")
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_delete_notification(self, event: Dict, user: Dict, notification_id: int) -> Dict:
        """
        DELETE /notifications/{notificationId} - Удалить уведомление
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            notification_id = self._validate_message_id(notification_id)

            # 👇 ИСПОЛЬЗУЕМ RequestContext ДЛЯ ОДНОЙ СЕССИИ
            async with RequestContext() as ctx:
                repo = NotificationRepository(ctx.session)
                success = await repo.delete(notification_id, user_id)

                if not success:
                    return self.response.error(
                        message="Notification not found",
                        code="not_found",
                        status_code=404,
                        event=event
                    )

            return self.response.success({
                'deleted': True,
                'notification_id': notification_id
            }, 200)

        except ValidationError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error deleting notification: {e}")
            return await self.handle_error(e, event)

    # ========== НАСТРОЙКИ УВЕДОМЛЕНИЙ ==========

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_notification_settings(self, event: Dict, user: Dict) -> Dict:
        """
        GET /users/me/notification-settings - Получить настройки уведомлений
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПОЛЬЗУЕМ RequestContext ДЛЯ ОДНОЙ СЕССИИ
            async with RequestContext() as ctx:
                repo = NotificationSettingsRepository(ctx.session)
                settings = await repo.get(user_id)

                if not settings:
                    settings = NotificationSettings.get_default(user_id)

            return self.response.success(settings.to_dict(), 200)

        except ValidationError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error getting notification settings: {e}")
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_update_notification_settings(self, event: Dict, user: Dict) -> Dict:
        """
        PUT /users/me/notification-settings - Обновить настройки уведомлений
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            body = self._parse_body(event)

            # 👇 ИСПОЛЬЗУЕМ RequestContext ДЛЯ ОДНОЙ СЕССИИ
            async with RequestContext() as ctx:
                # Получаем текущие настройки
                repo = NotificationSettingsRepository(ctx.session)
                settings = await repo.get(user_id)

                if not settings:
                    settings = NotificationSettings.get_default(user_id)

                # Обновляем поля
                if 'private_chats' in body:
                    settings.private_chats = body['private_chats']
                if 'groups' in body:
                    settings.groups = body['groups']
                if 'channels' in body:
                    settings.channels = body['channels']
                if 'replies' in body:
                    settings.replies = body['replies']
                if 'join_requests' in body:
                    settings.join_requests = body['join_requests']
                if 'admin_alerts' in body:
                    settings.admin_alerts = body['admin_alerts']
                if 'do_not_disturb' in body:
                    settings.do_not_disturb = body['do_not_disturb']

                settings.updated_at = datetime.utcnow()

                # Сохраняем
                success = await repo.save(settings)
                if not success:
                    raise DatabaseError("Failed to save notification settings")

            # Инвалидируем кэш
            await cache.delete(f"notify_settings:{user_id}")

            return self.response.success(settings.to_dict(), 200)

        except ValidationError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error updating notification settings: {e}")
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_search_messages(self, event: Dict, user: Dict) -> Dict:
        """
        GET /messages/search - Расширенный поиск по сообщениям

        Параметры запроса:
            q: поисковый запрос (обязательный, мин. 2 символа)
            chat_id: ID чата для поиска (опционально)
            from_date: начальная дата (ISO формат, опционально)
            to_date: конечная дата (ISO формат, опционально)
            sender_id: ID отправителя (опционально)
            type: тип сообщения (text, image, etc) (опционально)
            has_attachments: только с вложениями (true/false) (опционально)
            limit: количество результатов (по умолчанию 50, макс. 100)
            offset: смещение для пагинации (по умолчанию 0)
            sort: сортировка (relevance, date, chat) (по умолчанию relevance)
        """
        try:
            # Получаем параметры запроса
            query = self._get_query_param(event, 'q')
            if not query:
                return self.response.error(
                    message="Search query 'q' is required",
                    code="validation_error",
                    status_code=400,
                    event=event
                )

            # Получаем опциональные параметры
            chat_id_str = self._get_query_param(event, 'chat_id')
            chat_id = safe_int(chat_id_str) if chat_id_str else None

            from_date = self._get_query_param(event, 'from_date')
            to_date = self._get_query_param(event, 'to_date')
            sender_id = self._get_query_param(event, 'sender_id')
            message_type = self._get_query_param(event, 'type')

            has_attachments_str = self._get_query_param(event, 'has_attachments')
            has_attachments = None
            if has_attachments_str:
                has_attachments = has_attachments_str.lower() in ['true', '1', 'yes']

            limit = self._get_int_query_param(event, 'limit', 50)
            offset = self._get_int_query_param(event, 'offset', 0)
            sort_by = self._get_query_param(event, 'sort', 'relevance')

            # Ограничиваем лимит
            limit = min(limit, 100)

            # Валидируем сортировку
            if sort_by not in ['relevance', 'date', 'chat']:
                sort_by = 'relevance'

            # 👇 ИСПОЛЬЗУЕМ RequestContext ДЛЯ ОДНОЙ СЕССИИ
            async with RequestContext() as ctx:
                # Создаем репозиторий для поиска с сессией
                search_repo = MessageSearchRepository(ctx.session)

                # Конвертируем даты
                from_datetime = None
                if from_date:
                    try:
                        from_datetime = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
                    except:
                        raise ValidationError("Invalid from_date format")

                to_datetime = None
                if to_date:
                    try:
                        to_datetime = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
                    except:
                        raise ValidationError("Invalid to_date format")

                # Выполняем поиск
                messages, total = await search_repo.search_messages(
                    user_id=user['user_id'],
                    query=query,
                    chat_id=chat_id,
                    from_date=from_datetime,
                    to_date=to_datetime,
                    sender_id=sender_id,
                    message_type=message_type,
                    has_attachments=has_attachments,
                    limit=limit,
                    offset=offset,
                    sort_by=sort_by
                )

            # Обогащаем результаты информацией о чатах и отправителях
            results = []

            for item in messages:
                message = item['message']
                chat_id = message.chat_id

                # Получаем информацию о чате
                async with RequestContext() as ctx:
                    from handlers.chat_handler import ChatRepository
                    chat_repo = ChatRepository(ctx.session)
                    chat = await chat_repo.get_by_id(chat_id)

                    chat_title = chat.title if chat else f"Chat {chat_id}"
                    chat_type = chat.type if chat else 'unknown'

                # Получаем данные отправителя
                sender_data = {}
                if message.sender_id:
                    async with RequestContext() as ctx:
                        users_query = """
                        DECLARE $user_id AS Utf8;
                        SELECT username, first_name_encrypted, avatar_url
                        FROM `users`
                        WHERE id = $user_id;
                        """
                        
                        users_result = await ctx.session.transaction().execute(
                            await ctx.session.prepare(users_query),
                            {'$user_id': message.sender_id},
                            commit_tx=True
                        )
                        
                        if users_result and users_result[0].rows:
                            row = users_result[0].rows[0]
                            first_name_enc = row.get('first_name_encrypted', '')
                            first_name = ''
                            if first_name_enc:
                                try:
                                    import base64
                                    first_name = base64.b64decode(first_name_enc).decode('utf-8')
                                except:
                                    first_name = first_name_enc
                            
                            avatar_url = row.get('avatar_url')
                            if avatar_url and not avatar_url.startswith('http'):
                                from config.config import config
                                avatar_url = f"{config.OBJECT_STORAGE_PUBLIC_URL}/{avatar_url}"
                            
                            sender_data = {
                                'username': row.get('username'),
                                'first_name': first_name,
                                'avatar_url': avatar_url
                            }

                # Создаем результат
                result = {
                    'message': message.to_dict(),
                    'chat_id': chat_id,
                    'chat_title': chat_title,
                    'chat_type': chat_type,
                    'sender': sender_data,
                    'match_preview': item['match_preview'],
                    'match_score': item['relevance'],
                    'created_at': message.created_at.isoformat() if message.created_at else None
                }

                # Добавляем информацию об ответе если есть
                if message.reply_to_message_id:
                    result['reply_to'] = {
                        'message_id': message.reply_to_message_id,
                        'content_preview': None  # Можно добавить позже
                    }

                results.append(result)

            # Формируем ответ
            return self.response.success({
                'results': results,
                'count': len(results),
                'total': total,
                'query': query,
                'filters': {
                    'chat_id': chat_id,
                    'from_date': from_date,
                    'to_date': to_date,
                    'sender_id': sender_id,
                    'type': message_type,
                    'has_attachments': has_attachments
                },
                'pagination': {
                    'limit': limit,
                    'offset': offset,
                    'has_more': offset + len(results) < total
                },
                'sort_by': sort_by
            }, 200)

        except ValidationError as e:
            return await self.handle_error(e, event)
        except PermissionError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error in search_messages: {e}", exc_info=True)
            return await self.handle_error(e, event)

    @rate_limit(requests=10, window=60)
    @measure_time
    async def handle_publish_to_feed(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """POST /chats/{chatId}/messages/{messageId}/publish-to-feed - опубликовать сообщение канала в ленту"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            logger.info(f"📰 Publishing message {message_id} from chat {chat_id} to feed")

            async with RequestContext() as ctx:
                message = await self.service.get_message(chat_id, message_id, user_id, session=ctx.session)

                from handlers.chat_handler import ChatRepository
                chat_repo = ChatRepository(ctx.session)
                chat = await chat_repo.get_by_id(chat_id)
                if not chat:
                    return self.response.error("Chat not found", 404, event=event)

                if chat.type != 'channel':
                    return self.response.error("Only channels can publish to feed", 400, event=event)

                participant = await self.service.participant_cache.get_participant(chat_id, user_id, session=ctx.session)
                if not participant or participant.get('role') not in ['owner', 'admin']:
                    return self.response.error("Only owner and admin can publish to feed", 403, event=event)

                # Вызываем feed-service через HTTP
                import aiohttp
                import os
                feed_base = os.environ.get("FEED_SERVICE_URL", "http://localhost:8002")
                event_headers = event.get('headers', {})
                auth_header = (
                    event_headers.get('authorization') or
                    event_headers.get('Authorization') or
                    ''
                )
                headers = {"Authorization": auth_header, "Content-Type": "application/json"}

                try:
                    async with aiohttp.ClientSession() as http:
                        async with http.post(
                            f"{feed_base}/feed/channels/{chat_id}/messages/{message_id}/publish",
                            json={},
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            body = await resp.json()
                            if resp.status not in (200, 201):
                                logger.error(f"❌ Feed service returned {resp.status}: {body}")
                                return self.response.error(
                                    body.get('error', {}).get('message', 'Feed service error'),
                                    resp.status, event=event
                                )
                            logger.info(f"✅ Post published to feed from channel message {message_id}")
                            return self.response.success(body.get('data', body), 201, event=event)
                except aiohttp.ClientConnectorError:
                    logger.error("❌ Feed service unreachable")
                    return self.response.error("Feed service not available", 503, event=event)
                except Exception as e:
                    logger.error(f"❌ Feed service error: {e}")
                    return self.response.error(f"Feed service error: {str(e)}", 500, event=event)

        except Exception as e:
            logger.error(f"Error publishing to feed: {e}", exc_info=True)
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_user_connected(self, event: Dict, user: Dict) -> Dict:
        """POST /users/me/connected - Пользователь подключился"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            body = self._parse_body(event)
            session_id = body.get('session_id', str(uuid.uuid4()))

            await self.service.user_connected(user_id, session_id)

            return self.response.success(None, 204)

        except Exception as e:
            logger.error(f"Error in user_connected: {e}")
            return self.response.success(None, 204)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_user_disconnected(self, event: Dict, user: Dict) -> Dict:
        """POST /users/me/disconnected - Пользователь отключился"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            body = self._parse_body(event)
            session_id = body.get('session_id')

            if session_id:
                await self.service.user_disconnected(user_id, session_id)

            return self.response.success(None, 204)

        except Exception as e:
            logger.error(f"Error in user_disconnected: {e}")
            return self.response.success(None, 204)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_online_users(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/online - Кто онлайн в чате"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПОЛЬЗУЕМ RequestContext
            async with RequestContext() as ctx:
                online_users = await self.service.get_online_users(chat_id, user_id)

            return self.response.success({
                'chat_id': chat_id,
                'online_users': online_users,
                'count': len(online_users)
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_check_online(self, event: Dict, user: Dict, target_user_id: str) -> Dict:
        """GET /users/{userId}/online - Проверить онлайн статус пользователя"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            target_user_id = self._validate_user_id(target_user_id)

            # 👇 ИСПОЛЬЗУЕМ RequestContext
            async with RequestContext() as ctx:
                # Проверяем, есть ли у пользователя общий чат с target_user_id
                # (чтобы нельзя было следить за любым пользователем)
                has_common_chat = await self._check_common_chat(user_id, target_user_id)
                if not has_common_chat:
                    raise PermissionError("You don't have a common chat with this user")

                is_online = await self.service.is_user_online(target_user_id)

            return self.response.success({
                'user_id': target_user_id,
                'is_online': is_online,
                'last_seen': None  # Можно добавить из БД если нужно
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_typing_start(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/typing/start - Начать печатать"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            await self.service.set_typing(chat_id, user_id)

            # Возвращаем 204 No Content (успешно, но ничего не возвращаем)
            return self.response.success(None, 204)

        except Exception as e:
            # Даже при ошибке возвращаем 204, чтобы не бесить клиента
            logger.error(f"Error in typing start: {e}")
            return self.response.success(None, 204)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_typing_stop(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/typing/stop - Перестать печатать"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            await self.service.stop_typing(chat_id, user_id)

            return self.response.success(None, 204)

        except Exception as e:
            logger.error(f"Error in typing stop: {e}")
            return self.response.success(None, 204)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_typing_users(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/typing - Кто сейчас печатает"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПОЛЬЗУЕМ RequestContext
            async with RequestContext() as ctx:
                typing_users = await self.service.get_typing_users(chat_id, user_id)

            return self.response.success({
                'chat_id': chat_id,
                'typing_users': typing_users,
                'count': len(typing_users)
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=1000, window=60)
    @measure_time
    async def handle_validate_token(self, event: Dict, user: Dict) -> Dict:
        """POST /validate - Проверить токен для WebSocket"""
        try:
            body = self._parse_body(event)
            token = body.get('token')

            if not token:
                return self.response.success({'valid': False})

            try:
                # Проверяем токен через существующий middleware auth
                payload = auth.verify_token(token)

                if payload and payload.get('sub'):
                    return self.response.success({
                        'valid': True,
                        'user': {
                            'user_id': payload.get('sub'),
                            'role': payload.get('role', 'user'),
                            'exp': payload.get('exp')
                        }
                    })
            except Exception as e:
                # Ловим любую ошибку от auth.verify_token
                logger.error(f"Token validation error: {e}")
                return self.response.success({'valid': False, 'error': str(e)})

            return self.response.success({'valid': False})

        except Exception as e:
            logger.error(f"Token validation error: {e}")
            return self.response.success({'valid': False})

    @rate_limit(requests=100, window=60)
    @measure_time
    @idempotent(entity_type='message')  # 👈 ДОБАВИТЬ ЭТУ СТРОКУ
    async def handle_send_message(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """
        POST /chats/{chatId}/messages - Отправить сообщение
        """
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            body = self._parse_body(event)
            
            content = body.get('content')
            message_type = body.get('type', 'text')
            reply_to = safe_int(body.get('reply_to'))
            thread_root_id = safe_int(body.get('thread_root_id'))
            attachments = body.get('attachments', [])
            photos = body.get('photos')
            photo_ids = body.get('photo_ids')
            mentions = body.get('mentions')
            entities = body.get('entities')
            idempotency_key = self._get_idempotency_key(event)  # 👈 УЖЕ ЕСТЬ
            
            async with RequestContext() as ctx:
                result = await self.service.send_message(
                    chat_id=chat_id,
                    user_id=user_id,
                    content=content,
                    message_type=message_type,
                    reply_to=reply_to,
                    thread_root_id=thread_root_id,
                    attachments=attachments,
                    photos=photos,
                    photo_ids=photo_ids,
                    mentions=mentions,
                    entities=entities,
                    idempotency_key=idempotency_key,  # 👈 УЖЕ ПЕРЕДАЁТСЯ
                    client_ip=self._get_client_ip(event),
                    session=ctx.session
                )
                
                logger.info(f"✅ [REQ] Message sent: {result.message_id}")
                return self.response.success(result.to_dict(), 201)
                
        except Exception as e:
            logger.error(f"❌ Send message error: {e}", exc_info=True)
            return await self.handle_error(e, event)
    

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_send_private_message(self, event: Dict, user: Dict) -> Dict:
        """POST /messages/private - Отправить личное сообщение"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            body = self._parse_body(event)
            recipient_id = body.get('recipient_id')

            if not recipient_id:
                raise ValidationError("recipient_id is required")

            recipient_id = self._validate_user_id(recipient_id)
            
            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.send_private_message(
                    recipient_id=recipient_id,
                    user_id=user_id,
                    content=body.get('content'),
                    message_type=body.get('type', 'text'),
                    attachments=body.get('attachments'),
                    idempotency_key=self._get_idempotency_key(event),
                    session=ctx.session  # 👈 ВАЖНО: передаем сессию!
                )
                
                result_dict = result.to_dict()
                
                # Получаем данные отправителя с аватаром
                if result.sender_id:
                    users_query = """
                    DECLARE $user_id AS Utf8;
                    SELECT username, first_name_encrypted, avatar_url
                    FROM `users`
                    WHERE id = $user_id;
                    """
                    
                    users_result = await ctx.session.transaction().execute(
                        await ctx.session.prepare(users_query),
                        {'$user_id': result.sender_id},
                        commit_tx=True
                    )
                    
                    if users_result and users_result[0].rows:
                        row = users_result[0].rows[0]
                        first_name_enc = row.get('first_name_encrypted', '')
                        first_name = ''
                        if first_name_enc:
                            try:
                                import base64
                                first_name = base64.b64decode(first_name_enc).decode('utf-8')
                            except:
                                first_name = first_name_enc
                        
                        avatar_url = row.get('avatar_url')
                        if avatar_url and not avatar_url.startswith('http'):
                            from config.config import config
                            avatar_url = f"{config.OBJECT_STORAGE_PUBLIC_URL}/{avatar_url}"
                        
                        result_dict['sender'] = {
                            'username': row.get('username'),
                            'first_name': first_name,
                            'avatar_url': avatar_url
                        }

                return self.response.success(result_dict, 201)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @measure_time
    async def handle_get_chat_messages(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] GET messages for chat {chat_id}")
        
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            limit = self._get_int_query_param(event, 'limit', 50)
            cursor = self._get_cursor(event)
            before = safe_int(self._get_query_param(event, 'before'))
            after = safe_int(self._get_query_param(event, 'after'))
            message_type = self._get_query_param(event, 'type')
            sender_id = self._get_query_param(event, 'sender_id')
            include_reply_preview = self._get_bool_query_param(event, 'include_reply_preview', True)
            
            async with RequestContext() as ctx:
                messages, next_cursor, chat_type, user_role, discussion_chat_id = await self.service.get_chat_messages(
                    chat_id=chat_id,
                    user_id=user_id,
                    limit=limit,
                    cursor=cursor,
                    before=before,
                    after=after,
                    message_type=message_type,
                    sender_id=sender_id,
                    include_reply_preview=include_reply_preview,
                    session=ctx.session
                )
                
                enriched_messages = await self._enrich_messages_with_senders(messages, ctx.session)
                
                logger.info(f"✅ [REQ {request_id}] Returned {len(messages)} messages")
                return self.response.success({
                    'messages': enriched_messages,
                    'count': len(messages),
                    'next_cursor': next_cursor,
                    'chat_id': chat_id
                })
                
        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Get messages error: {e}", exc_info=True)
            return await self.handle_error(e, event)

    @measure_time
    async def handle_get_message(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] GET message {message_id} in chat {chat_id}")
        
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            async with RequestContext() as ctx:
                result = await self.service.get_message_with_sender(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    session=ctx.session
                )
                
                logger.info(f"✅ [REQ {request_id}] Message found: {message_id}")
                return self.response.success(result)
                
        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Get message error: {e}", exc_info=True)
            return await self.handle_error(e, event)
    

    @measure_time
    async def handle_edit_message(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] EDIT message {message_id} in chat {chat_id}")
        
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            body = self._parse_body(event)
            
            async with RequestContext() as ctx:
                result = await self.service.edit_message(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    new_content=body.get('content'),
                    new_photos=body.get('photos'),
                    new_photo_ids=body.get('photo_ids'),
                    entities=body.get('entities'),
                    session=ctx.session
                )
                
                logger.info(f"✅ [REQ {request_id}] Message edited: {message_id}")
                return self.response.success(result.to_dict())
                
        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Edit message error: {e}", exc_info=True)
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_delete_message(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """DELETE /chats/{chatId}/messages/{messageId} - Удалить сообщение"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            permanent = self._get_bool_query_param(event, 'permanent', False)
            reason = self._get_query_param(event, 'reason')

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.delete_message(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    permanent=permanent,
                    reason=reason,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success(result)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @measure_time
    async def handle_forward_message(self, event: Dict, user: Dict, source_chat_id: int, message_id: int) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] FORWARD message {message_id} from chat {source_chat_id}")
        
        try:
            source_chat_id = self._validate_chat_id(source_chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            body = self._parse_body(event)
            target_chat_id = body.get('target_chat_id')
            comment = body.get('comment')
            
            if not target_chat_id:
                raise ValidationError("target_chat_id is required")
            
            target_chat_id = self._validate_chat_id(target_chat_id)
            
            async with RequestContext() as ctx:
                result = await self.service.forward_message(
                    source_chat_id=source_chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    target_chat_id=target_chat_id,
                    comment=comment,
                    idempotency_key=self._get_idempotency_key(event),
                    session=ctx.session
                )
                
                logger.info(f"✅ [REQ {request_id}] Message forwarded to {target_chat_id}")
                return self.response.success(result.to_dict(), 201)
                
        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Forward message error: {e}", exc_info=True)
            return await self.handle_error(e, event)
    

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_add_reaction(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """POST /chats/{chatId}/messages/{messageId}/reactions - Добавить реакцию"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            body = self._parse_body(event)
            reaction = body.get('reaction')

            if not reaction:
                raise ValidationError("Reaction is required")

            # 👇 ИСПРАВЛЕНО: используем RequestContext для всего запроса
            async with RequestContext() as ctx:
                result = await self.service.add_reaction(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    reaction=reaction,
                    session=ctx.session  # 👈 ВАЖНО: передаем сессию!
                )

                result_dict = result.to_dict()
                
                # Получаем данные отправителя с аватаром
                if result.sender_id:
                    users_query = """
                    DECLARE $user_id AS Utf8;
                    SELECT username, first_name_encrypted, avatar_url
                    FROM `users`
                    WHERE id = $user_id;
                    """
                    
                    users_result = await ctx.session.transaction().execute(
                        await ctx.session.prepare(users_query),
                        {'$user_id': result.sender_id},
                        commit_tx=True
                    )
                    
                    if users_result and users_result[0].rows:
                        row = users_result[0].rows[0]
                        first_name_enc = row.get('first_name_encrypted', '')
                        first_name = ''
                        if first_name_enc:
                            try:
                                import base64
                                first_name = base64.b64decode(first_name_enc).decode('utf-8')
                            except:
                                first_name = first_name_enc
                        
                        avatar_url = row.get('avatar_url')
                        if avatar_url and not avatar_url.startswith('http'):
                            from config.config import config
                            avatar_url = f"{config.OBJECT_STORAGE_PUBLIC_URL}/{avatar_url}"
                        
                        result_dict['sender'] = {
                            'username': row.get('username'),
                            'first_name': first_name,
                            'avatar_url': avatar_url
                        }

                return self.response.success(result_dict)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_remove_reaction(self, event: Dict, user: Dict, chat_id: int, message_id: int, reaction: str) -> Dict:
        """DELETE /chats/{chatId}/messages/{messageId}/reactions/{reaction} - Удалить реакцию"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.remove_reaction(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    reaction=reaction,
                    session=ctx.session  # 👈 передаем сессию!
                )
                
                result_dict = result.to_dict()
                
                # Получаем данные отправителя с аватаром
                if result.sender_id:
                    users_query = """
                    DECLARE $user_id AS Utf8;
                    SELECT username, first_name_encrypted, avatar_url
                    FROM `users`
                    WHERE id = $user_id;
                    """
                    
                    users_result = await ctx.session.transaction().execute(
                        await ctx.session.prepare(users_query),
                        {'$user_id': result.sender_id},
                        commit_tx=True
                    )
                    
                    if users_result and users_result[0].rows:
                        row = users_result[0].rows[0]
                        first_name_enc = row.get('first_name_encrypted', '')
                        first_name = ''
                        if first_name_enc:
                            try:
                                import base64
                                first_name = base64.b64decode(first_name_enc).decode('utf-8')
                            except:
                                first_name = first_name_enc
                        
                        avatar_url = row.get('avatar_url')
                        if avatar_url and not avatar_url.startswith('http'):
                            from config.config import config
                            avatar_url = f"{config.OBJECT_STORAGE_PUBLIC_URL}/{avatar_url}"
                        
                        result_dict['sender'] = {
                            'username': row.get('username'),
                            'first_name': first_name,
                            'avatar_url': avatar_url
                        }

            return self.response.success(result_dict)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_reaction_users(self, event: Dict, user: Dict, chat_id: int, message_id: int, reaction: str) -> Dict:
        """GET /chats/{chatId}/messages/{messageId}/reactions/{reaction}/users - Список пользователей"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            limit = self._get_int_query_param(event, 'limit', 100)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                users = await self.service.get_reaction_users(
                    chat_id=chat_id,
                    message_id=message_id,
                    reaction=reaction,
                    user_id=user_id,
                    limit=limit,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success({
                'reaction': reaction,
                'users': users,
                'count': len(users),
                'message_id': message_id,
                'chat_id': chat_id
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_top_messages(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/messages/top - Топ сообщений по реакциям"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            limit = self._get_int_query_param(event, 'limit', 10)
            limit = min(limit, 50)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                top_messages = await self.service.get_top_messages_by_reactions(
                    chat_id=chat_id,
                    user_id=user_id,
                    limit=limit,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success({
                'messages': top_messages,
                'count': len(top_messages),
                'chat_id': chat_id
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_my_reactions(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """GET /chats/{chatId}/messages/{messageId}/reactions/me - Мои реакции"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            async with RequestContext() as ctx:
                repo = MessageReactionRepository(ctx.session)
                reactions = await repo.get_user_reactions(chat_id, message_id, user_id)

                return self.response.success({
                    'message_id': message_id,
                    'chat_id': chat_id,
                    'reactions': reactions,
                    'count': len(reactions)
                })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_thread_messages(self, event: Dict, user: Dict, chat_id: int, thread_root_id: int) -> Dict:
        """GET /chats/{chatId}/threads/{threadRootId} - Получить сообщения треда"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            thread_root_id = self._validate_message_id(thread_root_id)
            user_id = self._validate_user_id(user.get('user_id'))

            limit = self._get_int_query_param(event, 'limit', 50)
            cursor = self._get_cursor(event)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                messages, next_cursor = await self.service.get_thread_messages(
                    chat_id=chat_id,
                    thread_root_id=thread_root_id,
                    user_id=user_id,
                    limit=limit,
                    cursor=cursor,
                    session=ctx.session  # 👈 ВАЖНО: передаем сессию!
                )

                # 👇 Обогащаем сообщения данными отправителей (с аватарами)
                enriched_messages = await self._enrich_messages_with_senders(messages, ctx.session)

                return self.response.success({
                    'messages': enriched_messages,
                    'count': len(messages),
                    'next_cursor': next_cursor,
                    'thread_root_id': thread_root_id
                })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_save_draft(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/drafts - Сохранить черновик"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            body = self._parse_body(event)

            reply_to = body.get('reply_to')
            if reply_to is not None:
                reply_to = safe_int(reply_to)
                if reply_to is None:
                    raise ValidationError("Invalid reply_to format")

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.save_draft(
                    chat_id=chat_id,
                    user_id=user_id,
                    content=body.get('content'),
                    attachments=body.get('attachments'),
                    reply_to=reply_to,
                    entities=body.get('entities', {}),
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success(result.to_dict())

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_draft(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/drafts - Получить черновик"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                draft = await self.service.get_draft(
                    chat_id, 
                    user_id,
                    session=ctx.session  # 👈 передаем сессию!
                )

            if not draft:
                return self.response.success(None)

            return self.response.success(draft.to_dict())

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_delete_draft(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """DELETE /chats/{chatId}/drafts - Удалить черновик"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                success = await self.service.delete_draft(
                    chat_id, 
                    user_id,
                    session=ctx.session  # 👈 передаем сессию!
                )
                
            if not success:
                raise NotFoundError("Draft not found")

            return self.response.success({
                'deleted': True,
                'chat_id': chat_id,
                'user_id': user_id
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_all_drafts(self, event: Dict, user: Dict) -> Dict:
        """GET /users/me/drafts - Получить все черновики"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            limit = self._get_int_query_param(event, 'limit', 50)
            offset = self._get_int_query_param(event, 'offset', 0)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                drafts = await self.service.get_all_drafts(
                    user_id, 
                    limit, 
                    offset,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success({
                'drafts': [d.to_dict() for d in drafts],
                'count': len(drafts)
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_upload_photos(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/photos - Загрузить фото"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            body = self._parse_body(event)
            photos = body.get('photos', [])

            if not photos:
                raise ValidationError("photos array is required")

            if len(photos) > message_config.MAX_PHOTOS_PER_MESSAGE:
                raise ValidationError(f"Maximum {message_config.MAX_PHOTOS_PER_MESSAGE} photos per request")

            result = await self.service.upload_message_photos_sync(
                chat_id=chat_id,
                user_id=user_id,
                photos=photos,
            )

            return self.response.success({
                'photos': result,
                'count': len(result),
                'chat_id': chat_id
            }, 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_photo_status(self, event: Dict, user: Dict, photo_id: str) -> Dict:
        """GET /photos/{photoId}/status - Статус загрузки фото"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            async with RequestContext() as ctx:
                repo = PhotoUploadRepository(ctx.session)
                photo = await repo.get(photo_id)

                if not photo:
                    raise NotFoundError("Photo not found")

                if photo.user_id != user_id:
                    has_access = await self.service.participant_cache.check_access(
                        photo.chat_id, user_id
                    )
                    if not has_access:
                        raise PermissionError("Access denied")

                return self.response.success(photo.to_dict(), 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_list_chat_photos(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/photos - Список фото чата"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            has_access = await self.service.participant_cache.check_access(chat_id, user_id)
            if not has_access:
                raise PermissionError("Access denied")

            limit = self._get_int_query_param(event, 'limit', 50)
            status = self._get_query_param(event, 'status')

            async with RequestContext() as ctx:
                repo = PhotoUploadRepository(ctx.session)
                photos = await repo.list_by_chat(chat_id, limit)

                if status:
                    photos = [p for p in photos if p.status.value == status]

                return self.response.success({
                    'photos': [p.to_dict() for p in photos],
                    'count': len(photos)
                }, 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_message_photos(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """GET /chats/{chatId}/messages/{messageId}/photos - Фото сообщения"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                message = await self.service.get_message(
                    chat_id, 
                    message_id, 
                    user_id,
                    session=ctx.session  # 👈 передаем сессию!
                )

                photos = []
                if message.attachments_json:
                    photos = [a for a in message.attachments_json if a.get('type') == 'photo']

                repo = PhotoUploadRepository(ctx.session)
                for photo in photos:
                    if photo.get('photo_id'):
                        photo_info = await repo.get(photo['photo_id'])
                        if photo_info:
                            photo['status'] = photo_info.status.value
                            photo['urls'] = {
                                'small': photo_info.url_small,
                                'medium': photo_info.url_medium,
                                'large': photo_info.url_large,
                                'original': photo_info.url_original
                            }

            return self.response.success({
                'message_id': message_id,
                'chat_id': chat_id,
                'photos': photos,
                'count': len(photos)
            }, 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_photos_batch(self, event: Dict, user: Dict) -> Dict:
        """
        GET /photos/batch - Получить статусы нескольких фото

        Параметры запроса:
            ids (string): Список ID фото через запятую (макс. 100)

        Возвращает:
            {
                "success": true,
                "data": {
                    "photos": [
                        {
                            "photo_id": "uuid",
                            "status": "completed",
                            "urls": {...},
                            ...
                        }
                    ],
                    "count": 1
                }
            }
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            # Получаем список ID из query параметра
            query_params = event.get('queryStringParameters') or {}
            ids_param = query_params.get('ids', '')

            if not ids_param:
                return self.response.error(
                    message="ids parameter is required",
                    code="validation_error",
                    status_code=400,
                    event=event
                )

            # Разбираем IDs (разделены запятыми)
            photo_ids = [pid.strip() for pid in ids_param.split(',') if pid.strip()]

            if len(photo_ids) > 100:
                return self.response.error(
                    message="Too many IDs. Max 100 allowed",
                    code="validation_error",
                    status_code=400,
                    event=event
                )

            logger.info(f"📸 Batch photo request for {len(photo_ids)} photos: {photo_ids}")

            async with RequestContext() as ctx:
                repo = PhotoUploadRepository(ctx.session)
                # Получаем все фото одним запросом
                photos_map = await repo.get_many(photo_ids)

                result = []
                for photo_id in photo_ids:
                    photo = photos_map.get(photo_id)
                    if photo:
                        # Проверяем доступ к чату
                        has_access = await self.service.participant_cache.is_member(
                            photo.chat_id, 
                            user_id,
                            session=ctx.session
                        )
                        if has_access:
                            result.append(photo.to_dict())
                        else:
                            result.append({
                                'photo_id': photo_id,
                                'status': 'access_denied',
                                'error': 'You do not have access to this photo',
                                'chat_id': photo.chat_id
                            })
                    else:
                        # Фото не найдено
                        result.append({
                            'photo_id': photo_id,
                            'status': 'not_found',
                            'error': 'Photo not found'
                        })

                logger.info(f"✅ Batch photo response: {len([r for r in result if r.get('status') == 'completed'])} completed, "
                           f"{len([r for r in result if r.get('status') == 'not_found'])} not found")

                return self.response.success({
                    'photos': result,
                    'count': len(result)
                }, 200)

        except ValidationError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"❌ Error in batch photos: {e}", exc_info=True)
            return self.response.error(
                message="Internal server error",
                code="internal_error",
                status_code=500,
                event=event
            )

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_save_message(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """POST /chats/{chatId}/messages/{messageId}/save - Сохранить сообщение"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            body = self._parse_body(event)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.save_message(
                    user_id=user_id,
                    message_id=message_id,
                    chat_id=chat_id,
                    notes=body.get('notes'),
                    collections=body.get('collections'),
                    importance=body.get('importance', 5),
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success(result.to_dict(), 201)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_unsave_message(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """DELETE /chats/{chatId}/messages/{messageId}/save - Удалить из сохраненных"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                success = await self.service.unsave_message(
                    user_id, 
                    message_id,
                    session=ctx.session  # 👈 передаем сессию!
                )
                
            if not success:
                raise NotFoundError("Saved message not found")

            return self.response.success({
                'removed': True,
                'message_id': message_id
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_saved_messages(self, event: Dict, user: Dict) -> Dict:
        """GET /users/me/saved - Получить сохраненные сообщения"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            collection = self._get_query_param(event, 'collection')
            limit = self._get_int_query_param(event, 'limit', 50)
            cursor = self._get_cursor(event)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                saved, next_cursor = await self.service.get_saved_messages(
                    user_id=user_id,
                    collection=collection,
                    limit=limit,
                    cursor=cursor,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success({
                'saved_messages': [s.to_dict() for s in saved],
                'count': len(saved),
                'next_cursor': next_cursor
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_upload_attachment(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/attachments - Загрузить вложение"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            body = self._parse_body(event)

            filename = body.get('filename')
            mime_type = body.get('mime_type')
            file_type = body.get('file_type', 'file')
            file_data_b64 = body.get('file_data')
            metadata = body.get('metadata')
            duration = body.get('duration') if file_type == 'audio' else None

            if not filename or not mime_type or not file_data_b64:
                raise ValidationError("filename, mime_type and file_data are required")

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.upload_attachment(
                    chat_id=chat_id,
                    user_id=user_id,
                    filename=filename,
                    mime_type=mime_type,
                    file_type=file_type,
                    file_data_b64=file_data_b64,
                    metadata=metadata,
                    duration=duration,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success(result.to_dict(), 201)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_attachment(self, event: Dict, user: Dict, chat_id: int, attachment_id: str) -> Dict:
        """GET /chats/{chatId}/attachments/{attachmentId} - Получить вложение"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                attachment = await self.service.get_attachment(
                    attachment_id, 
                    user_id,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success(attachment.to_dict())

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_contacts(self, event: Dict, user: Dict) -> Dict:
        """GET /users/me/contacts - Получить контакты"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            favorites_only = self._get_bool_query_param(event, 'favorites_only', False)
            limit = self._get_int_query_param(event, 'limit', 50)
            cursor = self._get_cursor(event)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                contacts, next_cursor = await self.service.get_contacts(
                    user_id=user_id,
                    favorites_only=favorites_only,
                    limit=limit,
                    cursor=cursor,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success({
                'contacts': [c.to_dict() for c in contacts],
                'count': len(contacts),
                'next_cursor': next_cursor
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_add_contact(self, event: Dict, user: Dict) -> Dict:
        """POST /users/me/contacts - Добавить контакт"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            body = self._parse_body(event)
            contact_id = body.get('contact_id')

            if not contact_id:
                raise ValidationError("contact_id is required")

            contact_id = self._validate_user_id(contact_id)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.add_contact(
                    user_id=user_id,
                    contact_id=contact_id,
                    first_name=body.get('first_name'),
                    last_name=body.get('last_name'),
                    phone=body.get('phone'),
                    is_favorite=body.get('is_favorite', False),
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success(result.to_dict(), 201)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_update_contact(self, event: Dict, user: Dict, contact_id: str) -> Dict:
        """PUT /users/me/contacts/{contactId} - Обновить контакт"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            contact_id = self._validate_user_id(contact_id)

            body = self._parse_body(event)

            updates = {}
            if 'first_name' in body:
                updates['first_name'] = body['first_name']
            if 'last_name' in body:
                updates['last_name'] = body['last_name']
            if 'phone' in body:
                updates['phone'] = body['phone']
            if 'is_favorite' in body:
                updates['is_favorite'] = body['is_favorite']

            if not updates:
                raise ValidationError("No fields to update")

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.update_contact(
                    user_id=user_id,
                    contact_id=contact_id,
                    updates=updates,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success(result.to_dict())

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_delete_contact(self, event: Dict, user: Dict, contact_id: str) -> Dict:
        """DELETE /users/me/contacts/{contactId} - Удалить контакт"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            contact_id = self._validate_user_id(contact_id)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                success = await self.service.delete_contact(
                    user_id, 
                    contact_id,
                    session=ctx.session  # 👈 передаем сессию!
                )
                
            if not success:
                raise NotFoundError("Contact not found")

            return self.response.success({
                'deleted': True,
                'contact_id': contact_id
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_search_contacts(self, event: Dict, user: Dict) -> Dict:
        """GET /users/me/contacts/search - Поиск по контактам"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            query = self._get_query_param(event, 'q')

            if not query:
                raise ValidationError("Search query 'q' is required")

            if len(query) < 2:
                raise ValidationError("Search query must be at least 2 characters")

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                contacts = await self.service.search_contacts(
                    user_id, 
                    query,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success({
                'contacts': [c.to_dict() for c in contacts],
                'count': len(contacts),
                'query': query
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_block_user(self, event: Dict, user: Dict) -> Dict:
        """POST /users/me/blocks - Заблокировать пользователя"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            body = self._parse_body(event)
            blocked_id = body.get('blocked_id')

            if not blocked_id:
                raise ValidationError("blocked_id is required")

            blocked_id = self._validate_user_id(blocked_id)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.block_user(
                    user_id=user_id,
                    blocked_id=blocked_id,
                    reason=body.get('reason'),
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success(result.to_dict(), 201)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_unblock_user(self, event: Dict, user: Dict, blocked_id: str) -> Dict:
        """DELETE /users/me/blocks/{blockedId} - Разблокировать пользователя"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            blocked_id = self._validate_user_id(blocked_id)

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                success = await self.service.unblock_user(
                    user_id, 
                    blocked_id,
                    session=ctx.session  # 👈 передаем сессию!
                )
                
            if not success:
                raise NotFoundError("Block not found")

            return self.response.success({
                'unblocked': True,
                'user_id': blocked_id
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_blocked_users(self, event: Dict, user: Dict) -> Dict:
        """GET /users/me/blocks - Получить список заблокированных"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                blocks = await self.service.get_blocked_users(
                    user_id,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success({
                'blocks': [b.to_dict() for b in blocks],
                'count': len(blocks)
            })

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_user_stats(self, event: Dict, user: Dict) -> Dict:
        """GET /users/me/stats - Получить статистику пользователя"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                stats = await self.service.get_user_stats(
                    user_id,
                    session=ctx.session  # 👈 передаем сессию!
                )

            return self.response.success(stats)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_original_message(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """GET /chats/{chatId}/messages/{messageId}/original - Оригинал пересланного"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.get_original_message(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    session=ctx.session  # 👈 передаем сессию!
                )
                
                # Обогащаем сообщение данными отправителя (с аватаром)
                if 'original' in result and 'message' in result['original']:
                    message = result['original']['message']
                    if message and message.get('sender_id'):
                        users_query = """
                        DECLARE $user_id AS Utf8;
                        SELECT username, first_name_encrypted, avatar_url
                        FROM `users`
                        WHERE id = $user_id;
                        """
                        
                        users_result = await ctx.session.transaction().execute(
                            await ctx.session.prepare(users_query),
                            {'$user_id': message['sender_id']},
                            commit_tx=True
                        )
                        
                        if users_result and users_result[0].rows:
                            row = users_result[0].rows[0]
                            first_name_enc = row.get('first_name_encrypted', '')
                            first_name = ''
                            if first_name_enc:
                                try:
                                    import base64
                                    first_name = base64.b64decode(first_name_enc).decode('utf-8')
                                except:
                                    first_name = first_name_enc
                            
                            avatar_url = row.get('avatar_url')
                            if avatar_url and not avatar_url.startswith('http'):
                                from config.config import config
                                avatar_url = f"{config.OBJECT_STORAGE_PUBLIC_URL}/{avatar_url}"
                            
                            result['original']['sender'] = {
                                'username': row.get('username'),
                                'first_name': first_name,
                                'avatar_url': avatar_url
                            }

            return self.response.success(result, 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @measure_time
    async def handle_get_forwarded_messages(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] GET forwarded messages in chat {chat_id}")
        
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            limit = self._get_int_query_param(event, 'limit', 50)
            cursor = self._get_cursor(event)
            
            async with RequestContext() as ctx:
                messages, next_cursor = await self.service.get_forwarded_messages(
                    chat_id=chat_id,
                    user_id=user_id,
                    limit=limit,
                    cursor=cursor,
                    session=ctx.session
                )
                
                logger.info(f"✅ [REQ {request_id}] Returned {len(messages)} forwarded messages")
                return self.response.success({
                    'messages': [m.to_dict() for m in messages],
                    'count': len(messages),
                    'next_cursor': next_cursor
                }, 200)
                
        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Get forwarded messages error: {e}", exc_info=True)
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_forwarding_info(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """GET /chats/{chatId}/messages/{messageId}/forward-info - Информация о пересылке"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            # 👇 ИСПРАВЛЕНО: используем RequestContext
            async with RequestContext() as ctx:
                result = await self.service.get_forwarding_info(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    session=ctx.session  # 👈 ВАЖНО: передаем сессию!
                )

                # Обогащаем сообщение данными отправителя (с аватаром)
                if 'message' in result and result['message'].get('sender_id'):
                    sender_id = result['message']['sender_id']
                    users_query = """
                    DECLARE $user_id AS Utf8;
                    SELECT username, first_name_encrypted, avatar_url
                    FROM `users`
                    WHERE id = $user_id;
                    """
                    
                    users_result = await ctx.session.transaction().execute(
                        await ctx.session.prepare(users_query),
                        {'$user_id': sender_id},
                        commit_tx=True
                    )
                    
                    if users_result and users_result[0].rows:
                        row = users_result[0].rows[0]
                        first_name_enc = row.get('first_name_encrypted', '')
                        first_name = ''
                        if first_name_enc:
                            try:
                                import base64
                                first_name = base64.b64decode(first_name_enc).decode('utf-8')
                            except:
                                first_name = first_name_enc
                        
                        avatar_url = row.get('avatar_url')
                        if avatar_url and not avatar_url.startswith('http'):
                            from config.config import config
                            avatar_url = f"{config.OBJECT_STORAGE_PUBLIC_URL}/{avatar_url}"
                        
                        result['message']['sender'] = {
                            'username': row.get('username'),
                            'first_name': first_name,
                            'avatar_url': avatar_url
                        }

                return self.response.success(result, 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_message_discussion(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """GET /chats/{chatId}/messages/{messageId}/discussion - Информация об обсуждении"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            async with RequestContext() as ctx:
                from handlers.chat_handler import ChatRepository
                chat_repo = ChatRepository(ctx.session)
                channel = await chat_repo.get_by_id(chat_id)
                if not channel or channel.type != 'channel':
                    return self.response.error("Chat is not a channel", 400)

                if not channel.linked_chat_id:
                    return self.response.error("Channel has no discussion chat", 404)

                return self.response.success({
                    'channel_id': chat_id,
                    'channel_message_id': message_id,
                    'discussion_chat_id': channel.linked_chat_id,
                    'discussion_message_id': None,
                    'redirect_to': f"/chats/{channel.linked_chat_id}/messages"
                }, 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)


# ============================================
# ЭКСПОРТ
# ============================================

message_handler = MessageHandler()

