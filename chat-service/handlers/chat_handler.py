"""
CHAT HANDLER v3.1 - ПОЛНАЯ оптимизированная версия для 5000+ пользователей
ВСЕ РЕПОЗИТОРИИ ПРИНИМАЮТ session В КОНСТРУКТОРЕ
ВСЕ МЕТОДЫ ХЕНДЛЕРА ИСПОЛЬЗУЮТ RequestContext
"""
import json
import secrets
import string
import time
import hashlib
import re
import asyncio
import uuid
import base64
from typing import Dict, Any, Optional, List, Union, Tuple, TYPE_CHECKING
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum 
from typing import Protocol
from datetime import datetime
from typing import Optional, Dict, Any

class MessageProtocol(Protocol):
    """Протокол сообщения для избежания циклических импортов"""
    message_id: int
    chat_id: int
    sender_id: str
    content: Optional[str]
    created_at: datetime
    message_type: str
    is_deleted: bool
    has_attachments: bool
    linked_discussion_message_id: Optional[int]
    
    def to_dict(self) -> Dict[str, Any]: ...

# Используем протокол вместо прямого импорта
Message = MessageProtocol
    
from handlers.common import (
    BaseRepository, IdempotencyKey, IdempotencyRepository,
    ParticipantCache, to_timestamp, from_timestamp, to_uint64,
    safe_int, validate_idempotency_key, ResponseHelper,
    BaseHandler, UnitOfWork, logger, cache, chunk_list,
    ValidationError, PermissionError, NotFoundError, RateLimitError, DatabaseError,
    Validators, retry, rate_limit, measure_time, common_config,
    RequestContext,
    # 👇 НОВЫЕ ИМПОРТЫ ДЛЯ WEBSOCKET
    WebSocketManager, send_ws_notification, start_workers, stop_workers
)
from utils.storage import storage

# 👇 ИСПРАВЛЯЕМ ЦИКЛИЧЕСКИЙ ИМПОРТ



# ============================================
# КОНФИГУРАЦИЯ ЧАТОВ
# ============================================
class BanValidator:
    """Валидатор для банов"""
    
    @classmethod
    def validate_ban(cls, ban_type: str, duration_minutes: Optional[int], permanent: bool):
        """Валидация бана"""
        if ban_type not in ['ban', 'mute', 'kick', 'warning']:
            raise ValidationError(f"Invalid ban type. Allowed: ban, mute, kick, warning")
        
        # Kick - особый случай, не требует duration или permanent
        if ban_type == 'kick':
            if permanent:
                raise ValidationError("Kick cannot be permanent")
            if duration_minutes:
                raise ValidationError("Kick cannot have duration")
            return  # ✅ Kick валиден без параметров
        
        # Для остальных типов (ban, mute, warning)
        if permanent and duration_minutes:
            raise ValidationError("Cannot set both permanent and duration")
        
        if not permanent and not duration_minutes:
            raise ValidationError("Either permanent or duration_minutes must be set")
        
        if duration_minutes:
            if duration_minutes < 1:
                raise ValidationError("Duration must be at least 1 minute")
            if duration_minutes > 43200:  # 30 дней
                raise ValidationError("Duration cannot exceed 30 days (43200 minutes)")
    
    @classmethod
    def validate_unban(cls, ban_id: int):
        """Валидация разбана"""
        if not ban_id or ban_id <= 0:
            raise ValidationError("Invalid ban ID")
class ParticipantValidator:
    """Валидатор для участников чата"""
    
    @classmethod
    def validate_role_change(cls, current_user_role: str, target_user_role: str, new_role: str):
        """Валидация смены роли"""
        role_level = {'owner': 4, 'admin': 3, 'moderator': 2, 'member': 1}
        
        # Проверяем существование ролей
        if new_role not in role_level:
            raise ValidationError(f"Invalid role: {new_role}")
        
        # Проверяем права текущего пользователя
        if current_user_role not in ['owner', 'admin']:
            raise ValidationError("You don't have permission to change roles")
        
        # Owner может менять любые роли
        if current_user_role == 'owner':
            return
        
        # Admin не может менять роль owner'а
        if target_user_role == 'owner':
            raise ValidationError("Cannot change owner's role")
        
        # Admin не может назначать других админов (только если разрешено)
        if new_role == 'admin' and current_user_role == 'admin':
            raise ValidationError("Only owner can assign admin role")
        
        # Admin не может менять роль другого админа
        if target_user_role == 'admin' and current_user_role == 'admin':
            raise ValidationError("Admins cannot modify other admins")
    
    @classmethod
    def validate_ban(cls, ban_type: str, duration_minutes: Optional[int], permanent: bool):
        """Валидация бана"""
        if ban_type not in chat_config.ALLOWED_BAN_TYPES:
            raise ValidationError(f"Invalid ban type. Allowed: {', '.join(chat_config.ALLOWED_BAN_TYPES)}")
        
        if permanent and duration_minutes:
            raise ValidationError("Cannot set both permanent and duration")
        
        if not permanent and not duration_minutes:
            raise ValidationError("Either permanent or duration_minutes must be set")
        
        if duration_minutes and (duration_minutes < 1 or duration_minutes > 43200):  # 30 дней макс
            raise ValidationError("Duration must be between 1 minute and 30 days")
    
    @classmethod
    def validate_invite(cls, expires_in_hours: int, max_uses: int, default_role: str):
        """Валидация приглашения"""
        if expires_in_hours < 0 or expires_in_hours > 720:  # 30 дней
            raise ValidationError("Expires in hours must be between 0 and 720")
        
        if max_uses < 0 or max_uses > 100:
            raise ValidationError("Max uses must be between 0 and 100")
        
        if default_role not in ['member', 'admin']:
            raise ValidationError("Default role must be 'member' or 'admin'")
class InviteValidator:
    """Валидатор для приглашений"""
    
    @classmethod
    def validate_create(cls, expires_in_hours: int, max_uses: int, default_role: str):
        """Валидация создания приглашения"""
        if expires_in_hours < 0:
            raise ValidationError("Expires in hours must be positive")
        
        if expires_in_hours > 720:  # 30 дней
            raise ValidationError("Expires in hours cannot exceed 720 (30 days)")
        
        if max_uses < 0:
            raise ValidationError("Max uses must be positive")
        
        if max_uses > chat_config.MAX_INVITE_USES:
            raise ValidationError(f"Max uses cannot exceed {chat_config.MAX_INVITE_USES}")
        
        if default_role not in ['member', 'admin']:
            raise ValidationError("Default role must be 'member' or 'admin'")
class ChatValidator:
    """Валидатор для чатов"""
    
    @classmethod
    def validate_create(cls, title: Optional[str], chat_type: str, max_members: Optional[int]):
        """Валидация создания чата"""
        if not title or not title.strip():
            raise ValidationError("Chat title is required")
        
        if len(title) > chat_config.MAX_CHAT_TITLE_LENGTH:
            raise ValidationError(f"Chat title too long. Max length: {chat_config.MAX_CHAT_TITLE_LENGTH}")
        
        if chat_type not in chat_config.ALLOWED_CHAT_TYPES:
            raise ValidationError(f"Invalid chat type. Allowed: {', '.join(chat_config.ALLOWED_CHAT_TYPES)}")
        
        if max_members and max_members > chat_config.MAX_MEMBERS_PREMIUM:
            raise ValidationError(f"Max members cannot exceed {chat_config.MAX_MEMBERS_PREMIUM}")
    
    @classmethod
    def validate_update(cls, updates: Dict):
        """Валидация обновления чата"""
        allowed_fields = ['title', 'description', 'avatar_url', 'username', 
                         'is_public', 'join_moderation', 'max_members', 
                         'slow_mode_interval', 'settings']
        
        for field in updates.keys():
            if field not in allowed_fields:
                raise ValidationError(f"Cannot update field: {field}")
        
        if 'title' in updates and len(updates['title']) > chat_config.MAX_CHAT_TITLE_LENGTH:
            raise ValidationError(f"Title too long. Max length: {chat_config.MAX_CHAT_TITLE_LENGTH}")
        
        if 'description' in updates and len(updates['description']) > chat_config.MAX_CHAT_DESCRIPTION_LENGTH:
            raise ValidationError(f"Description too long. Max length: {chat_config.MAX_CHAT_DESCRIPTION_LENGTH}")
@dataclass
class ChatConfig:
    """Конфигурация для чатов"""
    MAX_CHAT_TITLE_LENGTH: int = 255
    MAX_CHAT_DESCRIPTION_LENGTH: int = 5000
    MAX_MEMBERS_DEFAULT: int = 100
    MAX_MEMBERS_PREMIUM: int = 10000
    MAX_BATCH_SIZE: int = 100
    CACHE_TTL_CHAT: int = 300
    CACHE_TTL_PARTICIPANT: int = 600  # 10 минут
    INVITE_CODE_LENGTH: int = 16
    INVITE_EXPIRY_HOURS: int = 24
    MAX_INVITE_USES: int = 100
    SLOW_MODE_INTERVALS: List[int] = field(default_factory=lambda: [0, 5, 10, 30, 60, 300, 600, 1800, 3600])
    ALLOWED_CHAT_TYPES: List[str] = field(default_factory=lambda: ['private', 'group', 'channel'])
    ALLOWED_PARTICIPANT_ROLES: List[str] = field(default_factory=lambda: ['owner', 'admin', 'moderator', 'member'])
    ALLOWED_BAN_TYPES: List[str] = field(default_factory=lambda: ['ban', 'mute', 'kick', 'warning'])
    DEFAULT_REGION: str = "ru-central1"

chat_config = ChatConfig()


# ============================================
# МОДЕЛИ ЧАТОВ (оптимизированные с __slots__)
# ============================================

class ChatType(str, Enum):
    PRIVATE = "private"
    GROUP = "group"
    CHANNEL = "channel"

class ChatStatus(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"
    BANNED = "banned"

class ParticipantRole(str, Enum):
    OWNER = "owner"
    ADMIN = "admin"
    MODERATOR = "moderator"
    MEMBER = "member"

class BanType(str, Enum):
    BAN = "ban"
    MUTE = "mute"
    KICK = "kick"
    WARNING = "warning"

class EventType(str, Enum):
    CHAT_CREATED = "chat_created"
    CHAT_UPDATED = "chat_updated"
    CHAT_DELETED = "chat_deleted"
    CHAT_ARCHIVED = "chat_archived"
    CHAT_UNARCHIVED = "chat_unarchived"
    USER_JOINED = "user_joined"
    USER_LEFT = "user_left"
    USER_KICKED = "user_kicked"
    USER_BANNED = "user_banned"
    USER_UNBANNED = "user_unbanned"
    ROLE_CHANGED = "role_changed"
    OWNERSHIP_TRANSFERRED = "ownership_transferred"
    INVITE_CREATED = "invite_created"
    INVITE_REVOKED = "invite_revoked"


@dataclass
class Chat:
    """Модель чата - оптимизированная с __slots__"""
    
    __slots__ = [
        'id', 'type', 'title', 'owner_id', 'created_by', 'created_at',
        'subtype', 'description', 'avatar_url', 'updated_at', 'deleted_at',
        'username', 'username_updated_at', 'is_public', 'join_moderation',
        'is_active', 'is_archived', 'is_deleted', 'is_discussion',
        'comments_enabled', 'reactions_enabled', 'is_deleted_for_all',
        'max_members', 'slow_mode_interval', 'members_count', 'messages_count',
        'online_estimate', 'views_count', 'version', 'last_message_id',
        'last_message_at', 'last_message_preview', 'last_message_sender_id',
        'last_message_sender',
        'settings', 'discussion_settings', 'comments_settings', 'reactions_settings',
        'linked_chat_id', 'primary_region', 'status', 'deleted_for_all_at',
        'deleted_for_all_by', '_participant_info',
        'user1_id', 'user2_id',
    ]
    
    def __init__(self, **kwargs):
        # Инициализируем все поля из kwargs
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))
        
        # Значения по умолчанию
        now = datetime.utcnow()
        
        # Числовые поля
        if self.max_members is None:
            self.max_members = chat_config.MAX_MEMBERS_DEFAULT
        if self.members_count is None:
            self.members_count = 0
        if self.messages_count is None:
            self.messages_count = 0
        if self.online_estimate is None:
            self.online_estimate = 0
        if self.views_count is None:
            self.views_count = 0
        if self.version is None:
            self.version = 1
        if self.slow_mode_interval is None:
            self.slow_mode_interval = 0
        
        # Строковые поля
        if self.primary_region is None:
            self.primary_region = chat_config.DEFAULT_REGION
        if self.status is None:
            self.status = ChatStatus.ACTIVE.value
        if self.type is None:
            self.type = 'group'
        if self.title is None:
            self.title = ''
        if self.owner_id is None:
            self.owner_id = ''
        if self.created_by is None:
            self.created_by = ''
        
        # Поля для приватных чатов
        if not hasattr(self, 'user1_id'):
            self.user1_id = None
        if not hasattr(self, 'user2_id'):
            self.user2_id = None
        
        # Булевы поля
        if self.is_public is None:
            self.is_public = False
        if self.join_moderation is None:
            self.join_moderation = False
        if self.is_active is None:
            self.is_active = True
        if self.is_archived is None:
            self.is_archived = False
        if self.is_deleted is None:
            self.is_deleted = False
        if self.is_discussion is None:
            self.is_discussion = False
        if self.comments_enabled is None:
            self.comments_enabled = False
        if self.reactions_enabled is None:
            self.reactions_enabled = True
        if self.is_deleted_for_all is None:
            self.is_deleted_for_all = False
        
        # Дата создания
        if self.created_at is None:
            self.created_at = now
        
        # JSON поля
        if self.settings is None:
            self.settings = {}
        if self.discussion_settings is None:
            self.discussion_settings = {}
        if self.comments_settings is None:
            self.comments_settings = {}
        if self.reactions_settings is None:
            self.reactions_settings = {}
        
        # Поле для информации об участнике
        if self._participant_info is None:
            self._participant_info = None
        
        # Поле для данных отправителя последнего сообщения
        if not hasattr(self, 'last_message_sender'):
            self.last_message_sender = None

    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь для API - с данными отправителя"""
        logger.debug(f"🔍 Chat.to_dict called for chat id: {self.id}")
        
        # Базовый результат
        result = {
            'id': str(self.id),
            'type': self.type,
            'subtype': self.subtype,
            'title': self.title,
            'description': self.description,
            'avatar_url': self.avatar_url,
            'owner_id': str(self.owner_id) if self.owner_id else None,
            'created_by': str(self.created_by) if self.created_by else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'deleted_at': self.deleted_at.isoformat() if self.deleted_at else None,
            'username': self.username,
            'username_updated_at': self.username_updated_at.isoformat() if self.username_updated_at else None,
            'link': f"https://t.me/{self.username}" if self.username else None,
            'is_public': self.is_public,
            'join_moderation': self.join_moderation,
            'is_active': self.is_active,
            'is_archived': self.is_archived,
            'is_deleted': self.is_deleted,
            'max_members': self.max_members,
            'slow_mode_interval': self.slow_mode_interval,
            'members_count': self.members_count,
            'messages_count': self.messages_count,
            'online_estimate': self.online_estimate,
            'views_count': self.views_count,
            'version': self.version,
            'last_message_id': str(self.last_message_id) if self.last_message_id else None,
            'last_message_at': self.last_message_at.isoformat() if self.last_message_at else None,
            'last_message_preview': self.last_message_preview,
            'last_message_sender_id': str(self.last_message_sender_id) if self.last_message_sender_id else None,
            'last_message_sender': self.last_message_sender,
            'settings': self.settings,
            'primary_region': self.primary_region,
            'status': self.status,
            'linked_chat_id': str(self.linked_chat_id) if self.linked_chat_id else None,
            'is_discussion': self.is_discussion,
            'discussion_settings': self.discussion_settings,
            'comments_enabled': self.comments_enabled,
            'comments_settings': self.comments_settings,
            'reactions_enabled': self.reactions_enabled,
            'reactions_settings': self.reactions_settings,
            'is_deleted_for_all': self.is_deleted_for_all,
            'deleted_for_all_at': self.deleted_for_all_at.isoformat() if self.deleted_for_all_at else None,
            'deleted_for_all_by': self.deleted_for_all_by,
            'user1_id': self.user1_id,
            'user2_id': self.user2_id,
        }
        
        # 👇 Для приватных чатов - подставляем имя собеседника в title
        if self.type == 'private' and self._participant_info:
            try:
                # Получаем ID текущего пользователя
                current_user_id = self._participant_info.user_id if hasattr(self._participant_info, 'user_id') else self._participant_info.get('user_id')
                
                if current_user_id:
                    # Определяем ID собеседника
                    if self.user1_id == current_user_id:
                        partner_id = self.user2_id
                    else:
                        partner_id = self.user1_id
                    
                    # Вместо title показываем имя собеседника
                    # Имя будет добавлено позже в _enrich_chat_with_partner_name
                    # Пока оставляем как есть, но добавляем partner_id в результат
                    if partner_id:
                        result['partner_id'] = partner_id
            except Exception as e:
                logger.debug(f"Error setting partner_id: {e}")
        
        # 👇 Добавляем participant информацию
        if self._participant_info is not None:
            if hasattr(self._participant_info, 'role'):
                logger.debug(f"✅ Adding participant info for chat {self.id}: role={self._participant_info.role}")
                result['participant'] = self._participant_info.to_dict()
            elif isinstance(self._participant_info, dict):
                logger.debug(f"✅ Adding participant info (dict) for chat {self.id}: role={self._participant_info.get('role')}")
                result['participant'] = self._participant_info
            else:
                logger.warning(f"⚠️ Unknown participant info type for chat {self.id}: {type(self._participant_info)}")
                result['participant'] = None
        else:
            logger.debug(f"⚠️ No participant info for chat {self.id}")
            result['participant'] = None
        
        # Убираем None значения для чистоты ответа
        result = {k: v for k, v in result.items() if v is not None}
        
        logger.debug(f"🔍 Chat.to_dict result for {self.id}: id={result.get('id')}, title={result.get('title')}")
        return result

    def to_db_row(self) -> Dict[str, Any]:
        """Конвертация в строку для БД"""
        # Обработка BOOL полей
        bool_fields = {
            'is_public': self.is_public,
            'join_moderation': self.join_moderation,
            'is_active': self.is_active,
            'is_archived': self.is_archived,
            'is_deleted': self.is_deleted,
            'is_discussion': self.is_discussion,
            'comments_enabled': self.comments_enabled,
            'reactions_enabled': self.reactions_enabled,
            'is_deleted_for_all': self.is_deleted_for_all,
        }
        
        for field_name, value in bool_fields.items():
            if isinstance(value, str):
                bool_fields[field_name] = value.lower() in ['true', '1', 'yes', 'on']
            else:
                bool_fields[field_name] = bool(value)
        
        # Обработка числовых полей
        numeric_fields = {
            'max_members': self.max_members,
            'slow_mode_interval': self.slow_mode_interval,
            'members_count': self.members_count,
            'messages_count': self.messages_count,
            'online_estimate': self.online_estimate,
            'views_count': self.views_count,
            'version': self.version,
            'linked_chat_id': self.linked_chat_id,
            'last_message_id': self.last_message_id,
        }
        
        numeric_values = {}
        for field_name, value in numeric_fields.items():
            if value is not None:
                try:
                    numeric_values[field_name] = int(value)
                except (ValueError, TypeError):
                    numeric_values[field_name] = 0
            else:
                if field_name in ['linked_chat_id', 'last_message_id']:
                    numeric_values[field_name] = None
                else:
                    numeric_values[field_name] = 0
        
        # Строковые поля
        title_value = self.title if self.title is not None else ""
        type_value = self.type if self.type is not None else "group"
        owner_id_value = str(self.owner_id) if self.owner_id else ""
        created_by_value = str(self.created_by) if self.created_by else ""
        primary_region_value = self.primary_region if self.primary_region is not None else chat_config.DEFAULT_REGION
        status_value = self.status if self.status is not None else ChatStatus.ACTIVE.value
        
        # Поля для приватных чатов
        user1_id_value = str(self.user1_id) if self.user1_id else None
        user2_id_value = str(self.user2_id) if self.user2_id else None
        
        # Опциональные строковые поля
        description_value = self.description if self.description is not None else None
        avatar_url_value = self.avatar_url if self.avatar_url is not None else None
        username_value = self.username if self.username is not None else None
        subtype_value = self.subtype if self.subtype is not None else None
        last_message_preview_value = self.last_message_preview if self.last_message_preview is not None else None
        last_message_sender_id_value = str(self.last_message_sender_id) if self.last_message_sender_id else None
        deleted_for_all_by_value = str(self.deleted_for_all_by) if self.deleted_for_all_by else None
        
        # Сериализация JSON полей
        settings_value = json.dumps(self.settings, ensure_ascii=False) if self.settings else None
        discussion_settings_value = json.dumps(self.discussion_settings, ensure_ascii=False) if self.discussion_settings else None
        comments_settings_value = json.dumps(self.comments_settings, ensure_ascii=False) if self.comments_settings else None
        reactions_settings_value = json.dumps(self.reactions_settings, ensure_ascii=False) if self.reactions_settings else None
        
        return {
            'id': self.id,
            'type': type_value,
            'title': title_value,
            'owner_id': owner_id_value,
            'created_by': created_by_value,
            'created_at': to_timestamp(self.created_at),
            'is_public': bool_fields['is_public'],
            'join_moderation': bool_fields['join_moderation'],
            'is_active': bool_fields['is_active'],
            'is_archived': bool_fields['is_archived'],
            'is_deleted': bool_fields['is_deleted'],
            'is_discussion': bool_fields['is_discussion'],
            'comments_enabled': bool_fields['comments_enabled'],
            'reactions_enabled': bool_fields['reactions_enabled'],
            'is_deleted_for_all': bool_fields['is_deleted_for_all'],
            'max_members': numeric_values['max_members'],
            'slow_mode_interval': numeric_values['slow_mode_interval'],
            'members_count': numeric_values['members_count'],
            'messages_count': numeric_values['messages_count'],
            'online_estimate': numeric_values['online_estimate'],
            'views_count': numeric_values['views_count'],
            'version': numeric_values['version'],
            'primary_region': primary_region_value,
            'status': status_value,
            'user1_id': user1_id_value,
            'user2_id': user2_id_value,
            'subtype': subtype_value,
            'description': description_value,
            'avatar_url': avatar_url_value,
            'updated_at': to_timestamp(self.updated_at) if self.updated_at else None,
            'deleted_at': to_timestamp(self.deleted_at) if self.deleted_at else None,
            'username': username_value,
            'username_updated_at': to_timestamp(self.username_updated_at) if self.username_updated_at else None,
            'linked_chat_id': numeric_values['linked_chat_id'],
            'last_message_id': numeric_values['last_message_id'],
            'last_message_at': to_timestamp(self.last_message_at) if self.last_message_at else None,
            'last_message_preview': last_message_preview_value,
            'last_message_sender_id': last_message_sender_id_value,
            'deleted_for_all_by': deleted_for_all_by_value,
            'deleted_for_all_at': to_timestamp(self.deleted_for_all_at) if self.deleted_for_all_at else None,
            'settings': settings_value,
            'discussion_settings': discussion_settings_value,
            'comments_settings': comments_settings_value,
            'reactions_settings': reactions_settings_value,
        }

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Chat':
        """Создание модели из строки БД"""
        return cls(
            id=row.get('id'),
            type=row.get('type', 'group'),
            subtype=row.get('subtype'),
            title=row.get('title'),
            description=row.get('description'),
            avatar_url=row.get('avatar_url'),
            owner_id=str(row.get('owner_id')) if row.get('owner_id') else None,
            created_by=str(row.get('created_by')) if row.get('created_by') else None,
            created_at=from_timestamp(row.get('created_at')),
            updated_at=from_timestamp(row.get('updated_at')),
            deleted_at=from_timestamp(row.get('deleted_at')),
            username=row.get('username'),
            username_updated_at=from_timestamp(row.get('username_updated_at')),
            is_public=row.get('is_public', False),
            join_moderation=row.get('join_moderation', False),
            is_active=row.get('is_active', True),
            is_archived=row.get('is_archived', False),
            is_deleted=row.get('is_deleted', False),
            is_discussion=row.get('is_discussion', False),
            comments_enabled=row.get('comments_enabled', False),
            reactions_enabled=row.get('reactions_enabled', True),
            is_deleted_for_all=row.get('is_deleted_for_all', False),
            max_members=row.get('max_members', 100),
            slow_mode_interval=row.get('slow_mode_interval', 0),
            members_count=row.get('members_count', 0),
            messages_count=row.get('messages_count', 0),
            online_estimate=row.get('online_estimate', 0),
            views_count=row.get('views_count', 0),
            version=row.get('version', 1),
            linked_chat_id=row.get('linked_chat_id'),
            last_message_id=row.get('last_message_id'),
            last_message_at=from_timestamp(row.get('last_message_at')),
            deleted_for_all_at=from_timestamp(row.get('deleted_for_all_at')),
            last_message_preview=row.get('last_message_preview'),
            last_message_sender_id=str(row.get('last_message_sender_id')) if row.get('last_message_sender_id') else None,
            last_message_sender=None,
            primary_region=row.get('primary_region', 'ru-central1'),
            status=row.get('status', 'active'),
            deleted_for_all_by=row.get('deleted_for_all_by'),
            settings=json.loads(row.get('settings')) if row.get('settings') else None,
            discussion_settings=json.loads(row.get('discussion_settings')) if row.get('discussion_settings') else None,
            comments_settings=json.loads(row.get('comments_settings')) if row.get('comments_settings') else None,
            reactions_settings=json.loads(row.get('reactions_settings')) if row.get('reactions_settings') else None,
            user1_id=str(row.get('user1_id')) if row.get('user1_id') else None,
            user2_id=str(row.get('user2_id')) if row.get('user2_id') else None,
            _participant_info=None,
        )
@dataclass
class JoinRequest:
    """Модель заявки на вступление"""
    __slots__ = ['request_id', 'chat_id', 'user_id', 'status', 'created_at',
                 'reviewed_by', 'reviewed_at', 'reject_reason', 'invite_code']
    
    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': str(self.request_id),
            'chat_id': str(self.chat_id),
            'user_id': self.user_id,
            'status': self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'reviewed_by': self.reviewed_by,
            'reviewed_at': self.reviewed_at.isoformat() if self.reviewed_at else None,
            'reject_reason': self.reject_reason,
            'invite_code': self.invite_code
        }
    
    def to_db_row(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'chat_id': self.chat_id,
            'user_id': str(self.user_id) if self.user_id else None,
            'status': self.status,
            'created_at': to_timestamp(self.created_at),
            'reviewed_by': str(self.reviewed_by) if self.reviewed_by else None,
            'reviewed_at': to_timestamp(self.reviewed_at) if self.reviewed_at else None,
            'reject_reason': self.reject_reason,
            'invite_code': self.invite_code
        }
    
    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'JoinRequest':
        return cls(
            request_id=row.get('request_id'),
            chat_id=row.get('chat_id'),
            user_id=str(row.get('user_id')) if row.get('user_id') else None,
            status=row.get('status'),
            created_at=from_timestamp(row.get('created_at')),
            reviewed_by=str(row.get('reviewed_by')) if row.get('reviewed_by') else None,
            reviewed_at=from_timestamp(row.get('reviewed_at')),
            reject_reason=row.get('reject_reason'),
            invite_code=row.get('invite_code')
        )


@dataclass
class PinnedMessage:
    """Модель закрепленного сообщения"""
    __slots__ = [
        'chat_id', 'message_id', 'pinned_by', 'pinned_at', 
        'unpinned_at', 'unpinned_by', 'is_active', 'version', 
        'created_at', 'pin_order'
    ]
    
    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))
        
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.version is None:
            self.version = 1
        if self.pin_order is None:
            self.pin_order = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'chat_id': self.chat_id,
            'message_id': self.message_id,
            'pinned_by': self.pinned_by,
            'pinned_at': self.pinned_at.isoformat() if self.pinned_at else None,
            'is_active': self.is_active,
            'order': self.pin_order
        }
    
    def to_db_row(self) -> Dict[str, Any]:
        return {
            'chat_id': self.chat_id,
            'message_id': self.message_id,
            'pinned_by': str(self.pinned_by) if self.pinned_by else None,
            'pinned_at': to_timestamp(self.pinned_at),
            'unpinned_at': to_timestamp(self.unpinned_at) if self.unpinned_at else None,
            'unpinned_by': str(self.unpinned_by) if self.unpinned_by else None,
            'is_active': self.is_active,
            'version': self.version,
            'created_at': to_timestamp(self.created_at),
            'pin_order': self.pin_order
        }
    
    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'PinnedMessage':
        return cls(
            chat_id=row.get('chat_id'),
            message_id=row.get('message_id'),
            pinned_by=str(row.get('pinned_by')) if row.get('pinned_by') else None,
            pinned_at=from_timestamp(row.get('pinned_at')),
            unpinned_at=from_timestamp(row.get('unpinned_at')),
            unpinned_by=str(row.get('unpinned_by')) if row.get('unpinned_by') else None,
            is_active=row.get('is_active', True),
            version=row.get('version', 1),
            created_at=from_timestamp(row.get('created_at')),
            pin_order=row.get('pin_order', 0)
        )


@dataclass
class ChatParticipant:
    """Модель участника чата с role_order для оптимизированной сортировки"""
    __slots__ = [
        'chat_id', 'user_id', 'role', 'role_order',
        'permissions_json', 'joined_at', 'joined_method', 'join_event_id',
        'is_active', 'left_at', 'left_event_id', 'mute_until', 'is_blocked',
        'last_read_at', 'last_read_message_id', 'last_read_message_valid',
        'last_active_at', 'unread_count', 'version', 'region',
        'is_hidden', 'show_in_profile'
    ]
    
    def __init__(self, **kwargs):
        # Инициализируем все поля из kwargs
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))
        
        # Значения по умолчанию
        if self.unread_count is None:
            self.unread_count = 0
        if self.version is None:
            self.version = 1
        if self.region is None:
            self.region = chat_config.DEFAULT_REGION
        if self.show_in_profile is None:
            self.show_in_profile = True
        
        # 👇 Устанавливаем role_order на основе роли, если не задан
        # role_order должен быть Uint8 NOT NULL в БД, поэтому всегда должен быть значение
        if self.role_order is None:
            role_map = {'owner': 1, 'admin': 2, 'moderator': 3, 'member': 4}
            self.role_order = role_map.get(self.role, 4)
        else:
            # Убеждаемся, что значение в диапазоне Uint8
            self.role_order = int(self.role_order) & 0xFF
        
        # Устанавливаем permissions по умолчанию, если нет
        if self.permissions_json is None and self.role:
            default_perms = RolePermissions.get_default_permissions(self.role)
            self.permissions_json = default_perms.to_dict()

    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь для API"""
        return {
            'chat_id': str(self.chat_id),  # ✅ ИСПРАВЛЕНО: число → строка
            'user_id': str(self.user_id),  # уже строка
            'role': self.role,
            'role_order': self.role_order,
            'permissions': self.permissions_json,
            'joined_at': self.joined_at.isoformat() if self.joined_at else None,
            'is_active': self.is_active,
            'is_blocked': self.is_blocked,
            'mute_until': self.mute_until.isoformat() if self.mute_until else None,
            'last_active_at': self.last_active_at.isoformat() if self.last_active_at else None,
            'unread_count': self.unread_count,
            'last_read_message_id': str(self.last_read_message_id) if self.last_read_message_id else None,  # ✅ ИСПРАВЛЕНО
            'region': self.region,
            'is_hidden': self.is_hidden,
            'show_in_profile': self.show_in_profile
        }

    def to_db_row(self) -> Dict:
        """Конвертация в строку для БД - ИСПРАВЛЕНО для Uint8 NOT NULL"""
        
        # role_order должен быть Uint8 NOT NULL - всегда есть значение
        # (уже установлено в __init__, но на всякий случай проверим)
        if self.role_order is None:
            # Вычисляем на основе роли
            role_map = {'owner': 1, 'admin': 2, 'moderator': 3, 'member': 4}
            role_order_value = role_map.get(self.role, 4)
        else:
            role_order_value = self.role_order
        
        # Приводим к диапазону Uint8 (0-255)
        role_order_value = int(role_order_value) & 0xFF
        
        # Обработка permissions
        permissions_value = None
        if self.permissions_json:
            permissions_value = json.dumps(self.permissions_json)
        
        return {
            'chat_id': self.chat_id,
            'user_id': str(self.user_id),
            'role': self.role,
            'role_order': role_order_value,  # 👈 ТЕПЕРЬ ЭТО Uint8 NOT NULL (0-255)
            'permissions': permissions_value,
            'joined_at': to_timestamp(self.joined_at),
            'joined_method': self.joined_method,
            'join_event_id': self.join_event_id,
            'is_active': self.is_active if self.is_active is not None else True,
            'left_at': to_timestamp(self.left_at) if self.left_at else None,
            'left_event_id': self.left_event_id,
            'mute_until': to_timestamp(self.mute_until) if self.mute_until else None,
            'is_blocked': self.is_blocked if self.is_blocked is not None else False,
            'last_read_at': to_timestamp(self.last_read_at) if self.last_read_at else None,
            'last_read_message_id': self.last_read_message_id,
            'last_read_message_valid': self.last_read_message_valid if self.last_read_message_valid is not None else False,
            'last_active_at': to_timestamp(self.last_active_at) if self.last_active_at else None,
            'unread_count': self.unread_count if self.unread_count is not None else 0,
            'version': self.version if self.version is not None else 1,
            'region': self.region if self.region is not None else chat_config.DEFAULT_REGION,
            'is_hidden': self.is_hidden if self.is_hidden is not None else False,
            'show_in_profile': self.show_in_profile if self.show_in_profile is not None else True
        }

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChatParticipant':
        """Создание модели из строки БД"""
        return cls(
            chat_id=row.get('chat_id'),
            user_id=str(row.get('user_id')),
            role=row.get('role', 'member'),
            role_order=row.get('role_order'),
            permissions_json=json.loads(row.get('permissions')) if row.get('permissions') else None,
            joined_at=from_timestamp(row.get('joined_at')),
            joined_method=row.get('joined_method', 'join'),
            join_event_id=row.get('join_event_id'),
            is_active=row.get('is_active', True),
            left_at=from_timestamp(row.get('left_at')),
            left_event_id=row.get('left_event_id'),
            mute_until=from_timestamp(row.get('mute_until')),
            is_blocked=row.get('is_blocked', False),
            last_read_at=from_timestamp(row.get('last_read_at')),
            last_read_message_id=row.get('last_read_message_id'),
            last_read_message_valid=row.get('last_read_message_valid', False),
            last_active_at=from_timestamp(row.get('last_active_at')),
            unread_count=row.get('unread_count', 0),
            version=row.get('version', 1),
            region=row.get('region', chat_config.DEFAULT_REGION),
            is_hidden=row.get('is_hidden', False),
            show_in_profile=row.get('show_in_profile', True)
        )

@dataclass
class RolePermissions:
    """Модель прав для роли"""
    __slots__ = [
        'can_delete_messages', 'can_ban_users', 'can_pin_messages',
        'can_change_info', 'can_invite_users', 'can_promote_members',
        'can_post_messages', 'can_edit_messages', 'can_view_admins'
    ]
    
    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key, False))
    
    def to_dict(self) -> Dict:
        return {
            'can_delete_messages': self.can_delete_messages,
            'can_ban_users': self.can_ban_users,
            'can_pin_messages': self.can_pin_messages,
            'can_change_info': self.can_change_info,
            'can_invite_users': self.can_invite_users,
            'can_promote_members': self.can_promote_members,
            'can_post_messages': self.can_post_messages,
            'can_edit_messages': self.can_edit_messages,
            'can_view_admins': self.can_view_admins
        }
    
    @classmethod
    def get_default_permissions(cls, role: str) -> 'RolePermissions':
        if role == 'owner':
            return cls(
                can_delete_messages=True,
                can_ban_users=True,
                can_pin_messages=True,
                can_change_info=True,
                can_invite_users=True,
                can_promote_members=True,
                can_post_messages=True,
                can_edit_messages=True,
                can_view_admins=True
            )
        elif role == 'admin':
            return cls(
                can_delete_messages=True,
                can_ban_users=True,
                can_pin_messages=True,
                can_change_info=True,
                can_invite_users=True,
                can_promote_members=False,
                can_post_messages=True,
                can_edit_messages=True,
                can_view_admins=True
            )
        elif role == 'moderator':
            return cls(
                can_delete_messages=True,
                can_ban_users=False,
                can_pin_messages=False,
                can_change_info=False,
                can_invite_users=True,
                can_promote_members=False,
                can_post_messages=True,
                can_edit_messages=False,
                can_view_admins=False
            )
        else:
            return cls(
                can_delete_messages=False,
                can_ban_users=False,
                can_pin_messages=False,
                can_change_info=False,
                can_invite_users=False,
                can_promote_members=False,
                can_post_messages=True,
                can_edit_messages=False,
                can_view_admins=False
            )


@dataclass
class ChatBan:
    """Модель бана"""
    __slots__ = [
        'ban_id', 'chat_id', 'user_id', 'banned_by', 'ban_type',
        'reason', 'reason_code', 'restrictions', 'banned_at',
        'expires_at', 'is_permanent', 'event_id', 'is_active', 'region'
    ]
    
    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))
        
        if self.region is None:
            self.region = chat_config.DEFAULT_REGION

    def to_dict(self) -> Dict[str, Any]:
        return {
            'ban_id': self.ban_id,
            'chat_id': self.chat_id,
            'user_id': self.user_id,
            'banned_by': self.banned_by,
            'type': self.ban_type,
            'reason': self.reason,
            'banned_at': self.banned_at.isoformat() if self.banned_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'is_permanent': self.is_permanent,
            'is_active': self.is_active
        }

    def to_db_row(self) -> Dict[str, Any]:
        return {
            'ban_id': self.ban_id,
            'chat_id': self.chat_id,
            'user_id': str(self.user_id) if self.user_id else None,
            'banned_by': str(self.banned_by) if self.banned_by else None,
            'ban_type': self.ban_type,
            'reason': self.reason,
            'reason_code': self.reason_code,
            'restrictions': json.dumps(self.restrictions) if self.restrictions else None,
            'banned_at': to_timestamp(self.banned_at),
            'expires_at': to_timestamp(self.expires_at) if self.expires_at else None,
            'is_permanent': self.is_permanent,
            'event_id': self.event_id,
            'is_active': self.is_active,
            'region': self.region
        }

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChatBan':
        return cls(
            ban_id=row.get('ban_id'),
            chat_id=row.get('chat_id'),
            user_id=str(row.get('user_id')),
            banned_by=str(row.get('banned_by')),
            ban_type=row.get('ban_type'),
            reason=row.get('reason'),
            reason_code=row.get('reason_code'),
            restrictions=json.loads(row.get('restrictions')) if row.get('restrictions') else None,
            banned_at=from_timestamp(row.get('banned_at')),
            expires_at=from_timestamp(row.get('expires_at')),
            is_permanent=row.get('is_permanent', False),
            event_id=row.get('event_id'),
            is_active=row.get('is_active', True),
            region=row.get('region', chat_config.DEFAULT_REGION)
        )


@dataclass
class ChatEvent:
    """Модель события"""
    __slots__ = [
        'event_id', 'chat_id', 'event_type', 'user_id', 'user_role_at_time',
        'target_id', 'target_type', 'payload', 'created_at',
        'created_date', 'idempotency_key', 'region'
    ]
    
    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))
        
        if self.region is None:
            self.region = chat_config.DEFAULT_REGION

    def to_dict(self) -> Dict[str, Any]:
        return {
            'event_id': self.event_id,
            'chat_id': self.chat_id,
            'event_type': self.event_type,
            'user_id': self.user_id,
            'user_role_at_time': self.user_role_at_time,
            'target_id': self.target_id,
            'target_type': self.target_type,
            'payload': self.payload,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

    def to_db_row(self) -> Dict[str, Any]:
        created_date_value = None
        if self.created_date:
            date_str = str(self.created_date)
            if len(date_str) == 8:
                year = int(date_str[0:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                dt = datetime(year, month, day)
                epoch = datetime(1970, 1, 1)
                created_date_value = (dt - epoch).days
            else:
                created_date_value = int(self.created_date)
        else:
            now = datetime.utcnow()
            epoch = datetime(1970, 1, 1)
            created_date_value = (now.date() - epoch.date()).days
        
        payload_value = None
        if self.payload:
            payload_value = json.dumps(self.payload, ensure_ascii=False)
        
        return {
            'event_id': self.event_id,
            'chat_id': int(self.chat_id) if self.chat_id else None,
            'event_type': self.event_type,
            'user_id': str(self.user_id) if self.user_id else None,
            'user_role_at_time': self.user_role_at_time,
            'target_id': self.target_id,
            'target_type': self.target_type,
            'payload': payload_value,
            'created_at': to_timestamp(self.created_at),
            'created_date': created_date_value,
            'idempotency_key': self.idempotency_key,
            'region': self.region
        }

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChatEvent':
        created_at = from_timestamp(row.get('created_at'))
        
        created_date_days = row.get('created_date')
        if created_date_days is not None:
            epoch = datetime(1970, 1, 1)
            created_date_dt = epoch + timedelta(days=created_date_days)
            created_date = int(created_date_dt.strftime('%Y%m%d'))
        else:
            created_date = None
        
        return cls(
            event_id=row.get('event_id'),
            chat_id=row.get('chat_id'),
            event_type=row.get('event_type'),
            user_id=str(row.get('user_id')) if row.get('user_id') else None,
            user_role_at_time=row.get('user_role_at_time'),
            target_id=row.get('target_id'),
            target_type=row.get('target_type'),
            payload=json.loads(row.get('payload')) if row.get('payload') else {},
            created_at=created_at,
            created_date=created_date,
            idempotency_key=row.get('idempotency_key'),
            region=row.get('region', 'ru-central1')
        )


@dataclass
class UserSearchResult:
    """Модель результата поиска пользователя"""
    __slots__ = [
        'id', 'username', 'email', 'first_name', 'last_name', 
        'display_name', 'avatar_url', 'status', 'last_login_at',
        'is_verified', 'country_code', 'bio'
    ]
    
    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            'id': self.id,
            'username': self.username,
            'email': self.email if self.email else None,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'display_name': self.display_name,
            'avatar_url': self.avatar_url,
            'status': self.status,
            'last_login_at': self.last_login_at.isoformat() if self.last_login_at else None,
            'is_verified': self.is_verified,
            'country_code': self.country_code,
            'bio': self.bio,
        }
        return {k: v for k, v in result.items() if v is not None}
    
    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'UserSearchResult':
        return cls(
            id=row.get('id'),
            username=row.get('username'),
            email=row.get('email'),
            first_name=row.get('first_name_encrypted'),
            last_name=row.get('last_name_encrypted'),
            display_name=row.get('display_name'),
            avatar_url=row.get('avatar_url'),
            status=row.get('status'),
            last_login_at=from_timestamp(row.get('last_login_at')),
            is_verified=row.get('is_verified', False),
            country_code=row.get('country_code'),
            bio=row.get('bio')
        )


@dataclass
class ChatInvite:
    """Модель приглашения"""
    __slots__ = [
        'invite_id', 'chat_id', 'invite_code', 'created_by', 'created_at',
        'expires_at', 'max_uses', 'used_count', 'remaining_uses',
        'can_join', 'requires_approval', 'default_role', 'is_active',
        'deactivated_at', 'region'
    ]
    
    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))
        
        if self.region is None:
            self.region = chat_config.DEFAULT_REGION

    def to_dict(self) -> Dict[str, Any]:
        return {
            'invite_id': str(self.invite_id) if self.invite_id else None,
            'chat_id': str(self.chat_id) if self.chat_id else None,
            'invite_code': self.invite_code,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'max_uses': self.max_uses,
            'used_count': self.used_count,
            'remaining_uses': self.remaining_uses,
            'can_join': self.can_join,
            'requires_approval': self.requires_approval,
            'default_role': self.default_role,
            'is_active': self.is_active
        }

    def to_db_row(self) -> Dict[str, Any]:
        return {
            'invite_id': self.invite_id,
            'chat_id': self.chat_id,
            'invite_code': self.invite_code,
            'created_by': str(self.created_by) if self.created_by else None,
            'created_at': to_timestamp(self.created_at),
            'expires_at': to_timestamp(self.expires_at) if self.expires_at else None,
            'max_uses': self.max_uses,
            'used_count': self.used_count,
            'remaining_uses': self.remaining_uses,
            'can_join': self.can_join,
            'requires_approval': self.requires_approval,
            'default_role': self.default_role,
            'is_active': self.is_active,
            'deactivated_at': to_timestamp(self.deactivated_at) if self.deactivated_at else None,
            'region': self.region
        }

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChatInvite':
        return cls(
            invite_id=row.get('invite_id'),
            chat_id=row.get('chat_id'),
            invite_code=row.get('invite_code'),
            created_by=str(row.get('created_by')) if row.get('created_by') else None,
            created_at=from_timestamp(row.get('created_at')),
            expires_at=from_timestamp(row.get('expires_at')),
            max_uses=row.get('max_uses', 0),
            used_count=row.get('used_count', 0),
            remaining_uses=row.get('remaining_uses', row.get('max_uses', 0)),
            can_join=row.get('can_join', True),
            requires_approval=row.get('requires_approval', False),
            default_role=row.get('default_role', 'member'),
            is_active=row.get('is_active', True),
            deactivated_at=from_timestamp(row.get('deactivated_at')),
            region=row.get('region', chat_config.DEFAULT_REGION)
        )


# ============================================
# ИСПРАВЛЕННЫЕ РЕПОЗИТОРИИ ЧАТОВ (ВСЕ ПРИНИМАЮТ session)
# ============================================

class ChatRepository(BaseRepository):
    """Репозиторий для таблицы chats"""
    
    def __init__(self, session=None):  # 👈 ИСПРАВЛЕНО
        super().__init__(session)
        self.table_name = "chats"

    def _escape_like(self, text: str) -> str:
        return text.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    
    @retry(max_attempts=3)
    async def create(self, chat: Chat) -> Optional[Chat]:
        """Создать новый чат"""
        data = chat.to_db_row()
        
        if data.get('id') is None:
            data['id'] = int(time.time() * 1000) ^ (secrets.randbits(32))
        
        # Для приватных чатов сразу устанавливаем members_count = 2
        if chat.type == 'private' and chat.max_members == 2:
            data['members_count'] = 2
            logger.info(f"📊 [CREATE] Setting members_count=2 for private chat {chat.id}")
        
        logger.info(f"📊 [CREATE] Chat type={chat.type}, members_count={data.get('members_count')}, title={chat.title}")
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        INSERT INTO {self.table_name} ({columns})
        VALUES ({placeholders})
        RETURNING id;
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                chat_id = result[0]['id']
                logger.info(f"📊 [CREATE] Chat {chat_id} created with members_count={data.get('members_count')}")
                # После создания чата увеличиваем счетчик участников
                await self.increment_members(chat_id)
                return await self.get_by_id(chat_id)
            return None
        except Exception as e:
            logger.error(f"Error creating chat: {e}")
            return None
    
    async def get_many(self, chat_ids: List[int]) -> Dict[int, Chat]:
        """Получить несколько чатов по ID — сначала из кэша, остаток из YDB"""
        if not chat_ids:
            return {}

        result = {}
        missing_ids = []

        for chat_id in chat_ids:
            cached = await cache.get(f"chat:{chat_id}")
            if cached is not None:
                result[chat_id] = cached if not isinstance(cached, dict) else Chat.from_db_row(cached)
            else:
                missing_ids.append(chat_id)

        if not missing_ids:
            return result

        unions = []
        params = {}
        for i, chat_id in enumerate(missing_ids):
            param_name = f"$id_{i}"
            unions.append(f"SELECT * FROM {self.table_name} WHERE id = {param_name}")
            params[param_name] = chat_id

        query = " UNION ALL ".join(unions) + ";"
        declare_block = self._generate_declare(params)
        query = f"{declare_block}\n{query}"

        try:
            rows = await self.execute(query, params)
            for row in rows:
                chat = Chat.from_db_row(row)
                result[chat.id] = chat
                await cache.set(f"chat:{chat.id}", chat, ttl=60)
            return result
        except Exception as e:
            logger.error(f"Error in get_many: {e}")
            return result
    
    async def get_by_id(self, chat_id: int) -> Optional[Chat]:
        """Получить чат по ID"""
        cache_key = f"chat:{chat_id}"
        cached = await cache.get(cache_key)
        if cached is not None:
            return Chat.from_db_row(cached) if isinstance(cached, dict) else cached

        query = f"""
        DECLARE $id AS Uint64;
        SELECT * FROM {self.table_name} WHERE id = $id;
        """
        params = {'$id': chat_id}

        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                chat = Chat.from_db_row(result[0])
                logger.info(f"📊 [GET_BY_ID] Chat {chat_id}: members_count={chat.members_count}, type={chat.type}, title={chat.title}")
                await cache.set(cache_key, chat, ttl=60)
                return chat
            return None
        except Exception as e:
            logger.error(f"Error getting chat: {e}")
            return None
    
    async def get_by_username(self, username: str) -> Optional[Chat]:
        """Найти чат по username"""
        query = f"""
        DECLARE $username AS Utf8;
        SELECT * FROM {self.table_name} VIEW idx_chats_username
        WHERE username = $username;
        """
        params = {'$username': username}
        
        try:
            rows = await self.execute(query, params)
            if rows:
                return Chat.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting chat by username: {e}")
            return None
    
    async def get_channel_by_discussion(self, discussion_chat_id: int) -> Optional[Chat]:
        """Найти канал по чату обсуждения"""
        query = f"""
        DECLARE $linked_chat_id AS Uint64;
        SELECT * FROM {self.table_name} VIEW idx_chats_discussion
        WHERE is_discussion = true AND linked_chat_id = $linked_chat_id AND is_deleted = false
        LIMIT 1;
        """
        params = {'$linked_chat_id': discussion_chat_id}
        
        try:
            rows = await self.execute(query, params)
            if rows:
                return Chat.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Error finding channel by discussion chat: {e}")
            return None
    
    async def list_discussion_chats(self, limit: int = 50, offset: int = 0) -> List[Chat]:
        """Список чатов-обсуждений"""
        query = f"""
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE is_discussion = true AND is_deleted = false
        ORDER BY created_at DESC
        LIMIT $limit OFFSET $offset;
        """
        params = {'$limit': limit, '$offset': offset}
        
        try:
            rows = await self.execute(query, params)
            return [Chat.from_db_row(row) for row in rows] if rows else []
        except Exception as e:
            logger.error(f"Error listing discussion chats: {e}")
            return []
    
    async def update(self, chat: Chat) -> bool:
        """Обновить чат"""
        data = chat.to_db_row()
        chat_id = data.pop('id')
        data.pop('primary_region', None)
        
        set_parts = [f"{key} = ${key}" for key in data.keys()]
        set_clause = ", ".join(set_parts)
        
        query = f"""
        DECLARE $id AS Uint64;
        {self._generate_declare({f"${k}": v for k, v in data.items()})}
        UPDATE {self.table_name} SET {set_clause} WHERE id = $id;
        """
        
        params = {'$id': chat_id}
        for k, v in data.items():
            params[f'${k}'] = v
        
        try:
            await self.execute(query, params)
            await cache.delete(f"chat:{chat_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating chat: {e}")
            return False
    
    async def delete(self, chat_id: int, permanent: bool = False) -> bool:
        """Удалить чат"""
        if permanent:
            query = f"DECLARE $id AS Uint64; DELETE FROM {self.table_name} WHERE id = $id;"
            params = {'$id': chat_id}
        else:
            query = f"""
            DECLARE $id AS Uint64; DECLARE $deleted_at AS Timestamp;
            UPDATE {self.table_name} SET is_deleted = true, deleted_at = $deleted_at WHERE id = $id;
            """
            params = {'$id': chat_id, '$deleted_at': to_timestamp(datetime.utcnow())}
        
        try:
            await self.execute(query, params)
            await cache.delete(f"chat:{chat_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting chat: {e}")
            return False
    
    async def list_public(self, limit: int = 50, cursor: Optional[str] = None) -> Tuple[List[Chat], Optional[str]]:
        """Получить список публичных чатов"""
        limit = min(limit, 100)
        params = {'$limit': limit + 1}
        
        conditions = ["is_public = true AND is_deleted = false"]
        
        if cursor:
            try:
                parts = cursor.split(':', 2)
                
                if len(parts) == 3:
                    members_count, created_at_str, chat_id_str = parts
                    if chat_id_str.isdigit():
                        cursor_datetime = datetime.fromisoformat(created_at_str)
                        if cursor_datetime.tzinfo:
                            cursor_datetime = cursor_datetime.replace(tzinfo=None)
                        
                        conditions.append(
                            "(members_count, created_at, id) < ($cursor_members, $cursor_time, $cursor_id)"
                        )
                        params['$cursor_members'] = int(members_count)
                        params['$cursor_time'] = cursor_datetime
                        params['$cursor_id'] = int(chat_id_str)
                
                elif len(parts) == 2:
                    created_at_str, chat_id_str = parts
                    if chat_id_str.isdigit():
                        cursor_datetime = datetime.fromisoformat(created_at_str)
                        if cursor_datetime.tzinfo:
                            cursor_datetime = cursor_datetime.replace(tzinfo=None)
                        
                        conditions.append("(created_at, id) < ($cursor_time, $cursor_id)")
                        params['$cursor_time'] = cursor_datetime
                        params['$cursor_id'] = int(chat_id_str)
            except Exception as e:
                logger.error(f"Error parsing cursor: {e}")
        
        where_clause = " AND ".join(conditions)
        declare_block = self._generate_declare(params)
        
        query = f"""
        {declare_block}
        SELECT * FROM {self.table_name} VIEW idx_chats_public_v2
        WHERE {where_clause}
        ORDER BY members_count DESC, created_at DESC, id DESC
        LIMIT $limit;
        """

        try:
            rows = await self.execute(query, params)

            has_next = len(rows) > limit
            if has_next:
                rows = rows[:limit]

            chats = [Chat.from_db_row(row) for row in rows] if rows else []

            next_cursor = None
            if has_next and chats:
                last = chats[-1]
                if last.created_at:
                    created_at_naive = last.created_at
                    if created_at_naive.tzinfo:
                        created_at_naive = created_at_naive.replace(tzinfo=None)
                    next_cursor = f"{last.members_count}:{created_at_naive.isoformat()}:{last.id}"

            return chats, next_cursor
        except Exception as e:
            logger.error(f"Error listing public chats: {e}")
            return [], None
    
    async def search_by_title(self, query_text: str, limit: int = 20, cursor: Optional[str] = None) -> Tuple[List[Chat], Optional[str]]:
        """Поиск чатов по названию"""
        limit = min(limit, 50)
        escaped = self._escape_like(query_text)
        search_pattern = f'%{escaped}%'
        
        params = {'$search': search_pattern, '$limit': limit + 1}
        conditions = ["title LIKE $search AND is_deleted = false"]
        
        if cursor:
            try:
                parts = cursor.split(':', 2)
                if len(parts) == 3:
                    members_count, created_at_str, chat_id_str = parts
                    if chat_id_str.isdigit():
                        cursor_datetime = datetime.fromisoformat(created_at_str)
                        if cursor_datetime.tzinfo:
                            cursor_datetime = cursor_datetime.replace(tzinfo=None)
                        
                        conditions.append(
                            "(members_count, created_at, id) < ($cursor_members, $cursor_time, $cursor_id)"
                        )
                        params['$cursor_members'] = int(members_count)
                        params['$cursor_time'] = cursor_datetime
                        params['$cursor_id'] = int(chat_id_str)
            except Exception as e:
                logger.error(f"Error parsing cursor: {e}")
        
        where_clause = " AND ".join(conditions)
        declare_block = self._generate_declare(params)
        
        query = f"""
        {declare_block}
        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY members_count DESC, created_at DESC, id DESC
        LIMIT $limit;
        """
        
        try:
            rows = await self.execute(query, params)
            
            has_next = len(rows) > limit
            if has_next:
                rows = rows[:limit]
            
            chats = [Chat.from_db_row(row) for row in rows] if rows else []
            
            next_cursor = None
            if has_next and chats:
                last = chats[-1]
                next_cursor = f"{last.members_count}:{last.created_at.isoformat()}:{last.id}"
            
            return chats, next_cursor
        except Exception as e:
            logger.error(f"Error searching chats: {e}")
            return [], None
    
    async def increment_members(self, chat_id: int) -> bool:
        """Увеличить счетчик участников"""
        logger.info(f"📊 [INCREMENT] Increasing members_count for chat {chat_id}")
        
        query = f"""
        DECLARE $id AS Uint64;
        UPDATE {self.table_name} 
        SET members_count = members_count + CAST(1 AS Uint32), 
            version = version + 1 
        WHERE id = $id;
        """
        params = {'$id': chat_id}
        
        try:
            await self.execute(query, params)
            await cache.delete(f"chat:{chat_id}")
            
            # Проверяем новое значение
            chat = await self.get_by_id(chat_id)
            if chat:
                logger.info(f"📊 [INCREMENT] Chat {chat_id} new members_count: {chat.members_count}")
            else:
                logger.warning(f"📊 [INCREMENT] Could not verify members_count for chat {chat_id}")
            
            return True
        except Exception as e:
            logger.error(f"Error incrementing members: {e}")
            return False
    
    async def decrement_members(self, chat_id: int) -> bool:
        """Уменьшить счетчик участников"""
        query = f"""
        DECLARE $id AS Uint64;
        UPDATE {self.table_name} 
        SET members_count = members_count - CAST(1 AS Uint32), 
            version = version + 1 
        WHERE id = $id AND members_count > 0;
        """
        params = {'$id': chat_id}
        
        try:
            await self.execute(query, params)
            await cache.delete(f"chat:{chat_id}")
            return True
        except Exception as e:
            logger.error(f"Error decrementing members: {e}")
            return False
    
    async def update_last_message(self, chat_id: int, message_id: int, 
                                   preview: str, sender_id: str, at: datetime) -> bool:
        """Обновить последнее сообщение в чате"""
        query = f"""
        DECLARE $id AS Uint64; DECLARE $last_message_id AS Uint64;
        DECLARE $last_message_preview AS Utf8; DECLARE $last_message_sender_id AS Utf8;
        DECLARE $last_message_at AS Timestamp;
        
        UPDATE {self.table_name}
        SET last_message_id = $last_message_id,
            last_message_preview = $last_message_preview,
            last_message_sender_id = $last_message_sender_id,
            last_message_at = $last_message_at,
            messages_count = messages_count + 1,
            version = version + 1
        WHERE id = $id;
        """
        
        params = {
            '$id': chat_id,
            '$last_message_id': message_id,
            '$last_message_preview': preview[:200] if preview else None,
            '$last_message_sender_id': str(sender_id),
            '$last_message_at': to_timestamp(at)
        }
        
        try:
            await self.execute(query, params)
            await cache.delete(f"chat:{chat_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating last message: {e}")
            return False


class UserSearchRepository(BaseRepository):
    """Репозиторий для поиска пользователей"""
    
    def __init__(self, session=None):  # 👈 ИСПРАВЛЕНО
        super().__init__(session)
        self.table_name = "users"
    
    def _escape_like(self, text: str) -> str:
        return text.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    
    async def search_users(
        self, 
        query: str, 
        limit: int = 20, 
        offset: int = 0,
        current_user_id: Optional[str] = None
    ) -> Tuple[List[UserSearchResult], int]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - поиск по частям с приоритетами
        """
        if len(query) < 2:
            return [], 0
        
        clean_query = self._escape_like(query)
        start_time = time.time()
        
        async with RequestContext() as ctx:
            session = ctx.session
            results = []
            
            # 👇 1. СНАЧАЛА ТОЧНЫЕ СОВПАДЕНИЯ (username)
            if len(results) < limit:
                exact_query = """
                DECLARE $username AS Utf8;
                DECLARE $current_user AS Utf8?;
                DECLARE $limit AS Uint64;
                
                SELECT 
                    id, username, email, 
                    first_name_encrypted as first_name,
                    last_name_encrypted as last_name,
                    display_name, avatar_url, status,
                    last_login_at, is_verified, country_code, bio,
                    1 as priority
                FROM `users`
                WHERE username = $username 
                  AND status = 'active'
                  AND ($current_user IS NULL OR id != $current_user)
                LIMIT $limit;
                """
                
                exact_params = {
                    '$username': query.lower(),
                    '$current_user': current_user_id,
                    '$limit': limit
                }
                
                exact_result = await session.transaction().execute(
                    await session.prepare(exact_query),
                    exact_params,
                    commit_tx=True
                )
                
                if exact_result and exact_result[0].rows:
                    for row in exact_result[0].rows:
                        results.append(UserSearchResult.from_db_row(row))
            
            # 👇 2. ПОТОМ НАЧИНАЕТСЯ С ЗАПРОСА (username)
            if len(results) < limit:
                starts_query = """
                DECLARE $prefix AS Utf8;
                DECLARE $current_user AS Utf8?;
                DECLARE $limit AS Uint64;
                
                SELECT 
                    id, username, email, 
                    first_name_encrypted as first_name,
                    last_name_encrypted as last_name,
                    display_name, avatar_url, status,
                    last_login_at, is_verified, country_code, bio,
                    2 as priority
                FROM `users`
                WHERE username LIKE $prefix 
                  AND status = 'active'
                  AND ($current_user IS NULL OR id != $current_user)
                ORDER BY 
                    CASE 
                        WHEN username LIKE $prefix THEN 1
                        ELSE 2
                    END,
                    last_login_at DESC
                LIMIT $limit;
                """
                
                starts_params = {
                    '$prefix': f'{clean_query}%',
                    '$current_user': current_user_id,
                    '$limit': limit - len(results)
                }
                
                starts_result = await session.transaction().execute(
                    await session.prepare(starts_query),
                    starts_params,
                    commit_tx=True
                )
                
                if starts_result and starts_result[0].rows:
                    for row in starts_result[0].rows:
                        if not any(r.username == row.get('username') for r in results):
                            results.append(UserSearchResult.from_db_row(row))
                            if len(results) >= limit:
                                break
            
            # 👇 3. ЗАТЕМ СОДЕРЖИТ (username)
            if len(results) < limit:
                contains_query = """
                DECLARE $search AS Utf8;
                DECLARE $current_user AS Utf8?;
                DECLARE $limit AS Uint64;
                
                SELECT 
                    id, username, email, 
                    first_name_encrypted as first_name,
                    last_name_encrypted as last_name,
                    display_name, avatar_url, status,
                    last_login_at, is_verified, country_code, bio,
                    3 as priority
                FROM `users`
                WHERE username LIKE $search 
                  AND status = 'active'
                  AND ($current_user IS NULL OR id != $current_user)
                ORDER BY last_login_at DESC
                LIMIT $limit;
                """
                
                contains_params = {
                    '$search': f'%{clean_query}%',
                    '$current_user': current_user_id,
                    '$limit': limit - len(results)
                }
                
                contains_result = await session.transaction().execute(
                    await session.prepare(contains_query),
                    contains_params,
                    commit_tx=True
                )
                
                if contains_result and contains_result[0].rows:
                    for row in contains_result[0].rows:
                        if not any(r.username == row.get('username') for r in results):
                            results.append(UserSearchResult.from_db_row(row))
                            if len(results) >= limit:
                                break
            
            # 👇 4. ПОИСК ПО display_name
            if len(results) < limit:
                display_query = """
                DECLARE $search AS Utf8;
                DECLARE $current_user AS Utf8?;
                DECLARE $limit AS Uint64;
                
                SELECT 
                    id, username, email, 
                    first_name_encrypted as first_name,
                    last_name_encrypted as last_name,
                    display_name, avatar_url, status,
                    last_login_at, is_verified, country_code, bio,
                    4 as priority
                FROM `users`
                WHERE display_name LIKE $search 
                  AND status = 'active'
                  AND ($current_user IS NULL OR id != $current_user)
                ORDER BY last_login_at DESC
                LIMIT $limit;
                """
                
                display_params = {
                    '$search': f'%{clean_query}%',
                    '$current_user': current_user_id,
                    '$limit': limit - len(results)
                }
                
                display_result = await session.transaction().execute(
                    await session.prepare(display_query),
                    display_params,
                    commit_tx=True
                )
                
                if display_result and display_result[0].rows:
                    for row in display_result[0].rows:
                        if not any(r.username == row.get('username') for r in results):
                            results.append(UserSearchResult.from_db_row(row))
                            if len(results) >= limit:
                                break
            
            duration = time.time() - start_time
            logger.info(f"✅ Found {len(results)} users in {duration*1000:.1f}ms")
            
            return results, len(results)

class PinnedMessageRepository(BaseRepository):
    """Репозиторий для закрепленных сообщений"""
    
    def __init__(self, session=None):  # 👈 ИСПРАВЛЕНО
        super().__init__(session)
        self.table_name = "pinned_messages"
        self.MAX_PINNED = 5
    
    async def pin(self, chat_id: int, message_id: int, user_id: str) -> Optional[PinnedMessage]:
        now = datetime.utcnow()
        
        check_query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $message_id AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE chat_id = $chat_id AND message_id = $message_id AND is_active = true;
        """
        
        check_params = {'$chat_id': chat_id, '$message_id': message_id}
        existing = await self.execute(check_query, check_params)
        
        if existing:
            logger.warning(f"Message {message_id} is already pinned")
            return PinnedMessage.from_db_row(existing[0])
        
        reactivate_query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $message_id AS Uint64;
        DECLARE $now AS Timestamp;
        DECLARE $pinned_by AS Utf8;
        
        UPDATE {self.table_name}
        SET 
            is_active = true,
            pinned_at = $now,
            pinned_by = $pinned_by,
            unpinned_at = NULL,
            unpinned_by = NULL,
            version = version + 1
        WHERE chat_id = $chat_id AND message_id = $message_id AND is_active = false;
        """
        
        reactivate_params = {
            '$chat_id': chat_id,
            '$message_id': message_id,
            '$now': to_timestamp(now),
            '$pinned_by': user_id
        }
        
        await self.execute(reactivate_query, reactivate_params)
        
        updated = await self.execute(check_query, check_params)
        if updated:
            logger.info(f"✅ Reactivated pinned message {message_id}")
            return PinnedMessage.from_db_row(updated[0])
        
        active_pins = await self.get_active_list(chat_id)
        
        if len(active_pins) >= self.MAX_PINNED:
            oldest = active_pins[-1]
            await self.unpin_by_id(oldest.message_id, chat_id, user_id, keep_history=True)
            active_pins = await self.get_active_list(chat_id)
        
        max_order = 0
        if active_pins:
            max_order = max(p.pin_order for p in active_pins)
        
        pinned = PinnedMessage(
            chat_id=chat_id,
            message_id=message_id,
            pinned_by=user_id,
            pinned_at=now,
            is_active=True,
            pin_order=max_order + 1,
            version=1,
            created_at=now
        )
        
        data = pinned.to_db_row()
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        insert_query = f"""
        {declare_block}
        INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(insert_query, params)
            logger.info(f"✅ Created new pinned message {message_id}")
            return pinned
        except Exception as e:
            logger.error(f"Failed to pin message: {e}")
            return None
    
    async def unpin_by_id(self, message_id: int, chat_id: int, user_id: str, keep_history: bool = True) -> bool:
        now = datetime.utcnow()
        
        if keep_history:
            query = f"""
            DECLARE $chat_id AS Uint64;
            DECLARE $message_id AS Uint64;
            DECLARE $now AS Timestamp;
            DECLARE $unpinned_by AS Utf8;
            
            UPDATE {self.table_name}
            SET 
                is_active = false,
                unpinned_at = $now,
                unpinned_by = $unpinned_by,
                version = version + 1
            WHERE chat_id = $chat_id AND message_id = $message_id AND is_active = true;
            """
        else:
            query = f"""
            DECLARE $chat_id AS Uint64;
            DECLARE $message_id AS Uint64;
            
            DELETE FROM {self.table_name}
            WHERE chat_id = $chat_id AND message_id = $message_id;
            """
        
        params = {
            '$chat_id': chat_id,
            '$message_id': message_id,
            '$now': to_timestamp(now),
            '$unpinned_by': user_id
        }
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to unpin message {message_id}: {e}")
            return False
    
    async def unpin_all(self, chat_id: int, user_id: str) -> bool:
        now = datetime.utcnow()
        
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $now AS Timestamp;
        DECLARE $unpinned_by AS Utf8;
        
        UPDATE {self.table_name}
        SET 
            is_active = false,
            unpinned_at = $now,
            unpinned_by = $unpinned_by,
            version = version + 1
        WHERE chat_id = $chat_id AND is_active = true;
        """
        
        params = {'$chat_id': chat_id, '$now': to_timestamp(now), '$unpinned_by': user_id}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to unpin all: {e}")
            return False
    
    async def get_active_list(self, chat_id: int) -> List[PinnedMessage]:
        query = f"""
        DECLARE $chat_id AS Uint64;
        SELECT * FROM {self.table_name}
        WHERE chat_id = $chat_id AND is_active = true
        ORDER BY pin_order DESC, pinned_at DESC;
        """
        params = {'$chat_id': chat_id}
        
        try:
            rows = await self.execute(query, params)
            return [PinnedMessage.from_db_row(row) for row in rows] if rows else []
        except Exception as e:
            logger.error(f"Failed to get active pins: {e}")
            return []
    
    async def get_active(self, chat_id: int) -> Optional[PinnedMessage]:
        pins = await self.get_active_list(chat_id)
        return pins[0] if pins else None
    
    async def get_history(self, chat_id: int, limit: int = 50) -> List[PinnedMessage]:
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $limit AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE chat_id = $chat_id
        ORDER BY pinned_at DESC
        LIMIT $limit;
        """
        params = {'$chat_id': chat_id, '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            return [PinnedMessage.from_db_row(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get pin history: {e}")
            return []
    
    async def reorder(self, chat_id: int, message_ids: List[int], user_id: str) -> bool:
        active_pins = await self.get_active_list(chat_id)
        active_ids = [p.message_id for p in active_pins]
        
        if set(message_ids) != set(active_ids):
            logger.error(f"Message IDs mismatch: requested {message_ids}, active {active_ids}")
            return False
        
        for idx, message_id in enumerate(message_ids):
            new_order = len(message_ids) - idx
            
            query = f"""
            DECLARE $chat_id AS Uint64;
            DECLARE $message_id AS Uint64;
            DECLARE $new_order AS Uint32;
            
            UPDATE {self.table_name}
            SET pin_order = $new_order, version = version + 1
            WHERE chat_id = $chat_id AND message_id = $message_id AND is_active = true;
            """
            
            params = {
                '$chat_id': chat_id,
                '$message_id': message_id,
                '$new_order': new_order
            }
            
            try:
                await self.execute(query, params)
            except Exception as e:
                logger.error(f"Failed to reorder message {message_id}: {e}")
                return False
        
        return True


class ParticipantRepository(BaseRepository):
    """Репозиторий для таблицы chat_participants"""
    
    def __init__(self, session=None):  # 👈 ИСПРАВЛЕНО
        super().__init__(session)
        self.table_name = "chat_participants"
    async def list_by_chat_with_cursor(
        self,
        chat_id: int,
        limit: int = 100,
        cursor: Optional[str] = None,
        active_only: bool = True
    ) -> Tuple[List[ChatParticipant], Optional[str]]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - использует role_order
        Сортировка: owner -> admin -> moderator -> member, затем по дате вступления
        """
        limit = min(limit, 200)
        
        conditions = ["chat_id = $chat_id"]
        params = {'$chat_id': chat_id, '$limit': limit + 1}
        
        if active_only:
            conditions.append("is_active = true")
        
        # 👇 ТЕПЕРЬ СОРТИРУЕМ ПО role_order (индексированное поле)
        if cursor:
            try:
                # Формат курсора: "role_order:joined_at:user_id"
                role_val, joined_at_str, user_id_str = cursor.split(':', 2)
                role_order_val = int(role_val)
                joined_at = datetime.fromisoformat(joined_at_str)
                if joined_at.tzinfo:
                    joined_at = joined_at.replace(tzinfo=None)
                
                conditions.append(
                    "(role_order, joined_at, user_id) > ($cursor_role, $cursor_joined, $cursor_user)"
                )
                params['$cursor_role'] = role_order_val
                params['$cursor_joined'] = to_timestamp(joined_at)
                params['$cursor_user'] = user_id_str
            except Exception as e:
                logger.error(f"Error parsing members cursor: {e}")
        
        where_clause = " AND ".join(conditions)
        declare_block = self._generate_declare(params)
        
        query = f"""
        {declare_block}
        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY role_order ASC, joined_at ASC, user_id ASC
        LIMIT $limit;
        """
        
        try:
            rows = await self.execute(query, params)
            
            has_next = len(rows) > limit
            if has_next:
                rows = rows[:limit]
            
            members = [ChatParticipant.from_db_row(row) for row in rows] if rows else []
            
            next_cursor = None
            if has_next and members:
                last = members[-1]
                if last.joined_at:
                    joined_at_naive = last.joined_at
                    if joined_at_naive.tzinfo:
                        joined_at_naive = joined_at_naive.replace(tzinfo=None)
                    next_cursor = f"{last.role_order}:{joined_at_naive.isoformat()}:{last.user_id}"
            
            logger.info(f"👥 Found {len(members)} members for chat {chat_id} (has_next: {has_next})")
            return members, next_cursor
            
        except Exception as e:
            logger.error(f"Error listing chat members with cursor: {e}")
            return [], None
    async def create(self, participant: ChatParticipant) -> bool:
        data = participant.to_db_row()
        
        if 'user_id' in data and data['user_id']:
            data['user_id'] = str(data['user_id'])
        
        if 'unread_count' in data and data['unread_count'] is not None:
            data['unread_count'] = int(data['unread_count']) & 0xFFFFFFFF
        
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
            from handlers.common import cache
            await cache.delete(f"participant:v2:{participant.chat_id}:{participant.user_id}")
            return True
        except Exception as e:
            logger.error(f"Error creating participant: {e}")
            return False
    
    async def get(self, chat_id: int, user_id: str) -> Optional[ChatParticipant]:
        query = f"""
        DECLARE $chat_id AS Uint64; DECLARE $user_id AS Utf8;
        SELECT * FROM {self.table_name} WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {'$chat_id': chat_id, '$user_id': str(user_id)}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                return ChatParticipant.from_db_row(result[0])
            return None
        except Exception as e:
            logger.error(f"Error getting participant: {e}")
            return None
    
    async def get_many(self, chat_id: int, user_ids: List[str]) -> Dict[str, Optional[ChatParticipant]]:
        """
        ПОЛУЧИТЬ НЕСКОЛЬКИХ УЧАСТНИКОВ ЗА ОДИН ЗАПРОС
        Исправляет N+1 проблему
        """
        if not user_ids:
            return {}
        
        # Разбиваем на чанки по 100 (ограничение YDB)
        from handlers.common import chunk_list
        result = {}
        
        for chunk in chunk_list(user_ids, 100):
            conditions = []
            params = {'$chat_id': chat_id}
            
            for i, user_id in enumerate(chunk):
                param_name = f"$user_id_{i}"
                conditions.append(f"user_id = {param_name}")
                params[param_name] = str(user_id)
            
            where_clause = f"chat_id = $chat_id AND ({' OR '.join(conditions)})"
            declare_block = self._generate_declare(params)
            
            query = f"""
            {declare_block}
            SELECT * FROM {self.table_name}
            WHERE {where_clause};
            """
            
            try:
                rows = await self.execute(query, params)
                for row in rows:
                    participant = ChatParticipant.from_db_row(row)
                    result[participant.user_id] = participant
            except Exception as e:
                logger.error(f"Error in get_many chunk: {e}")
                # Продолжаем с другими чанками
        
        # Для пользователей, которых не нашли, ставим None
        for user_id in user_ids:
            if user_id not in result:
                result[user_id] = None
        
        return result
    
    async def update(self, participant: ChatParticipant) -> bool:
        data = participant.to_db_row()
        chat_id = data.pop('chat_id')
        user_id = data.pop('user_id')
        
        set_parts = [f"{key} = ${key}" for key in data.keys()]
        set_clause = ", ".join(set_parts)
        
        query = f"""
        DECLARE $chat_id AS Uint64; DECLARE $user_id AS Utf8;
        {self._generate_declare({f"${k}": v for k, v in data.items()})}
        UPDATE {self.table_name} SET {set_clause} WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        
        params = {'$chat_id': chat_id, '$user_id': str(user_id)}
        for k, v in data.items():
            params[f'${k}'] = v
        
        try:
            await self.execute(query, params)
            from handlers.common import cache
            await cache.delete(f"participant:v2:{chat_id}:{user_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating participant: {e}")
            return False
    
    async def delete(self, chat_id: int, user_id: str) -> bool:
        query = f"""
        DECLARE $chat_id AS Uint64; DECLARE $user_id AS Utf8;
        DELETE FROM {self.table_name} WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {'$chat_id': chat_id, '$user_id': str(user_id)}
        
        try:
            await self.execute(query, params)
            from handlers.common import cache
            await cache.delete(f"participant:v2:{chat_id}:{user_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting participant: {e}")
            return False
    
    async def list_by_chat(
        self, 
        chat_id: int, 
        limit: int = 100, 
        offset: int = 0,
        active_only: bool = True
    ) -> List[ChatParticipant]:
        conditions = ["chat_id = $chat_id"]
        params = {'$chat_id': chat_id, '$limit': limit, '$offset': offset}
        
        if active_only:
            conditions.append("is_active = true")
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY 
            CASE role 
                WHEN 'owner' THEN 1 
                WHEN 'admin' THEN 2 
                WHEN 'moderator' THEN 3 
                ELSE 4 
            END,
            joined_at ASC
        LIMIT $limit OFFSET $offset;
        """
        
        try:
            result = await self.execute(query, params)
            return [ChatParticipant.from_db_row(row) for row in result] if result else []
        except Exception as e:
            logger.error(f"Error listing participants by chat: {e}")
            return []
    
    async def list_by_user_with_chats(
        self, 
        user_id: str, 
        limit: int = 50, 
        offset: int = 0, 
        include_hidden: bool = False
    ) -> Tuple[List[Tuple[ChatParticipant, Chat]], Optional[str]]:
        conditions = ["p.user_id = $user_id AND p.is_active = true"]
        params = {'$user_id': str(user_id), '$limit': limit + 1, '$offset': offset}
        
        if not include_hidden:
            conditions.append("(p.is_hidden = false OR p.is_hidden IS NULL)")
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT 
            p.chat_id, p.user_id, p.role, p.permissions, p.joined_at,
            p.joined_method, p.join_event_id, p.is_active, p.left_at,
            p.left_event_id, p.mute_until, p.is_blocked, p.last_read_at,
            p.last_read_message_id, p.last_read_message_valid, p.last_active_at,
            p.unread_count, p.version, p.region, p.is_hidden, p.show_in_profile,
            c.*
        FROM {self.table_name} p
        JOIN chats c ON c.id = p.chat_id
        WHERE {where_clause}
        ORDER BY p.last_active_at DESC, p.chat_id DESC
        LIMIT $limit OFFSET $offset;
        """
        
        try:
            rows = await self.execute(query, params)
            
            has_next = len(rows) > limit
            if has_next:
                rows = rows[:limit]
            
            result = []
            for row in rows:
                participant_data = {k: row[k] for k in row.keys() if k.startswith(('p.', 'chat_id', 'user_id', 'role', 'permissions', 'joined_at', 'joined_method', 'join_event_id', 'is_active', 'left_at', 'left_event_id', 'mute_until', 'is_blocked', 'last_read_at', 'last_read_message_id', 'last_read_message_valid', 'last_active_at', 'unread_count', 'version', 'region', 'is_hidden', 'show_in_profile'))}
                chat_data = {k: row[k] for k in row.keys() if not k.startswith(('p.', 'chat_id', 'user_id', 'role', 'permissions', 'joined_at', 'joined_method', 'join_event_id', 'is_active', 'left_at', 'left_event_id', 'mute_until', 'is_blocked', 'last_read_at', 'last_read_message_id', 'last_read_message_valid', 'last_active_at', 'unread_count', 'version', 'region', 'is_hidden', 'show_in_profile'))}
                
                participant = ChatParticipant.from_db_row(participant_data)
                chat = Chat.from_db_row(chat_data)
                result.append((participant, chat))
            
            next_cursor = None
            if has_next and result:
                last_participant, last_chat = result[-1]
                if last_participant.last_active_at:
                    next_cursor = f"{last_participant.last_active_at.isoformat()}:{last_chat.id}"
            
            return result, next_cursor
        except Exception as e:
            logger.error(f"Error listing user chats with join: {e}")
            return [], None
    
    async def list_by_user(
        self, 
        user_id: str, 
        limit: int = 50, 
        cursor: Optional[str] = None,
        include_hidden: bool = False
    ) -> Tuple[List[ChatParticipant], Optional[str]]:
        """
        Получить список чатов пользователя с пагинацией по курсору
        
        Args:
            user_id: ID пользователя
            limit: максимальное количество результатов
            cursor: курсор для пагинации (формат: "last_active_at:chat_id")
            include_hidden: включать скрытые диалоги
            
        Returns:
            Tuple[список участников, следующий курсор]
        """
        limit = min(limit, 100)  # Ограничиваем максимальный размер страницы
        
        conditions = ["user_id = $user_id AND is_active = true"]
        params = {'$user_id': str(user_id), '$limit': limit + 1}
        
        if not include_hidden:
            conditions.append("(is_hidden = false OR is_hidden IS NULL)")
        
        # 👇 Обработка курсора для пагинации
        if cursor:
            try:
                # Формат курсора: "last_active_at:chat_id"
                last_active_at_str, chat_id_str = cursor.split(':', 1)
                last_active_at = datetime.fromisoformat(last_active_at_str)
                if last_active_at.tzinfo:
                    last_active_at = last_active_at.replace(tzinfo=None)
                
                conditions.append(
                    "(last_active_at, chat_id) < ($cursor_time, $cursor_id)"
                )
                params['$cursor_time'] = to_timestamp(last_active_at)
                params['$cursor_id'] = int(chat_id_str)
            except Exception as e:
                logger.error(f"Error parsing cursor: {e}")
        
        where_clause = " AND ".join(conditions)
        declare_block = self._generate_declare(params)
        
        query = f"""
        {declare_block}
        SELECT * FROM {self.table_name} VIEW idx_user_active_time
        WHERE {where_clause}
        ORDER BY last_active_at DESC, chat_id DESC
        LIMIT $limit;
        """
        
        try:
            rows = await self.execute(query, params)
            
            # 👇 Проверяем, есть ли следующая страница
            has_next = len(rows) > limit
            if has_next:
                rows = rows[:limit]
            
            participants = [ChatParticipant.from_db_row(row) for row in rows] if rows else []
            
            # 👇 Формируем следующий курсор
            next_cursor = None
            if has_next and participants:
                last = participants[-1]
                if last.last_active_at:
                    last_active_at_naive = last.last_active_at
                    if last_active_at_naive.tzinfo:
                        last_active_at_naive = last_active_at_naive.replace(tzinfo=None)
                    next_cursor = f"{last_active_at_naive.isoformat()}:{last.chat_id}"
            
            logger.info(f"📊 Found {len(participants)} chats for user (has_next: {has_next})")
            return participants, next_cursor
            
        except Exception as e:
            logger.error(f"Error listing user chats: {e}")
            return [], None
    
    async def update_last_read(self, chat_id: int, user_id: str, message_id: int) -> bool:
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $user_id AS Utf8;
        DECLARE $message_id AS Uint64;
        DECLARE $now AS Timestamp;
        
        UPDATE {self.table_name}
        SET last_read_at = $now, 
            last_read_message_id = $message_id,
            last_read_message_valid = true, 
            unread_count = CAST(0 AS Uint32),
            version = version + CAST(1 AS Uint64)
        WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {
            '$chat_id': chat_id,
            '$user_id': str(user_id),
            '$message_id': message_id,
            '$now': to_timestamp(datetime.utcnow())
        }
        
        try:
            await self.execute(query, params)
            from handlers.common import cache
            await cache.delete(f"participant:v2:{chat_id}:{user_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating last read: {e}")
            return False

    async def reset_unread(self, chat_id: int, user_id: str) -> bool:
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $user_id AS Utf8;
        
        UPDATE {self.table_name}
        SET unread_count = CAST(0 AS Uint32),
            version = version + CAST(1 AS Uint64)
        WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {'$chat_id': chat_id, '$user_id': str(user_id)}
        
        try:
            await self.execute(query, params)
            from handlers.common import cache
            await cache.delete(f"participant:v2:{chat_id}:{user_id}")
            return True
        except Exception as e:
            logger.error(f"Error resetting unread: {e}")
            return False

    async def increment_unread(self, chat_id: int, user_id: str) -> bool:
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $user_id AS Utf8;
        
        UPDATE {self.table_name} 
        SET unread_count = unread_count + CAST(1 AS Uint32), 
            version = version + CAST(1 AS Uint64)
        WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {'$chat_id': chat_id, '$user_id': str(user_id)}
        
        try:
            await self.execute(query, params)
            from handlers.common import cache
            await cache.delete(f"participant:v2:{chat_id}:{user_id}")
            return True
        except Exception as e:
            logger.error(f"Error incrementing unread: {e}")
            return False
    
    async def update_activity(self, chat_id: int, user_id: str) -> bool:
        query = f"""
        DECLARE $chat_id AS Uint64; DECLARE $user_id AS Utf8; DECLARE $now AS Timestamp;
        UPDATE {self.table_name} SET last_active_at = $now, version = version + 1
        WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {
            '$chat_id': chat_id,
            '$user_id': str(user_id),
            '$now': to_timestamp(datetime.utcnow())
        }
        
        try:
            await self.execute(query, params)
            from handlers.common import cache
            await cache.delete(f"participant:v2:{chat_id}:{user_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating activity: {e}")
            return False
    
    async def hide_dialog(self, chat_id: int, user_id: str) -> bool:
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $user_id AS Utf8;
        
        UPDATE {self.table_name}
        SET is_hidden = true, version = version + 1
        WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {'$chat_id': chat_id, '$user_id': str(user_id)}
        
        try:
            await self.execute(query, params)
            from handlers.common import cache
            await cache.delete(f"participant:v2:{chat_id}:{user_id}")
            return True
        except Exception as e:
            logger.error(f"Error hiding dialog: {e}")
            return False
    
    async def show_dialog(self, chat_id: int, user_id: str) -> bool:
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $user_id AS Utf8;
        
        UPDATE {self.table_name}
        SET is_hidden = false, version = version + 1
        WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {'$chat_id': chat_id, '$user_id': str(user_id)}
        
        try:
            await self.execute(query, params)
            from handlers.common import cache
            await cache.delete(f"participant:v2:{chat_id}:{user_id}")
            return True
        except Exception as e:
            logger.error(f"Error showing dialog: {e}")
            return False
    
    async def hide_for_all(self, chat_id: int) -> bool:
        query = f"""
        DECLARE $chat_id AS Uint64;
        
        UPDATE {self.table_name}
        SET is_hidden = true, version = version + 1
        WHERE chat_id = $chat_id;
        """
        params = {'$chat_id': chat_id}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error hiding dialog for all: {e}")
            return False
    
    async def get_hidden_count(self, user_id: str) -> int:
        query = f"""
        DECLARE $user_id AS Utf8;
        
        SELECT COUNT(*) as count FROM {self.table_name}
        WHERE user_id = $user_id AND is_hidden = true AND is_active = true;
        """
        params = {'$user_id': str(user_id)}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                return result[0]['count']
            return 0
        except Exception as e:
            logger.error(f"Error getting hidden count: {e}")
            return 0


class BanRepository(BaseRepository):
    """Репозиторий для таблицы chat_bans"""
    
    def __init__(self, session=None):  # 👈 ИСПРАВЛЕНО
        super().__init__(session)
        self.table_name = "chat_bans"
    
    async def create(self, ban: ChatBan) -> Optional[ChatBan]:
        data = ban.to_db_row()
        
        if 'ban_id' not in data or data['ban_id'] is None:
            data['ban_id'] = int(time.time() * 1000) ^ (secrets.randbits(32))
        
        if 'event_id' not in data or data['event_id'] is None:
            data['event_id'] = int(time.time() * 1000) ^ (secrets.randbits(32))
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})
        RETURNING ban_id;
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                ban_id = result[0]['ban_id']
                return await self.get(ban_id)
            return None
        except Exception as e:
            logger.error(f"Error creating ban: {e}")
            return None
    
    async def get(self, ban_id: int) -> Optional[ChatBan]:
        query = f"DECLARE $ban_id AS Uint64; SELECT * FROM {self.table_name} WHERE ban_id = $ban_id;"
        params = {'$ban_id': ban_id}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                return ChatBan.from_db_row(result[0])
            return None
        except Exception as e:
            logger.error(f"Error getting ban: {e}")
            return None
    
    async def get_active(self, chat_id: int, user_id: str) -> Optional[ChatBan]:
        query = f"""
        DECLARE $chat_id AS Uint64; DECLARE $user_id AS Utf8; DECLARE $now AS Timestamp;
        SELECT * FROM {self.table_name}
        WHERE chat_id = $chat_id AND user_id = $user_id AND is_active = true
          AND (is_permanent = true OR expires_at > $now)
        ORDER BY banned_at DESC LIMIT 1;
        """
        params = {
            '$chat_id': chat_id,
            '$user_id': str(user_id),
            '$now': to_timestamp(datetime.utcnow())
        }
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                return ChatBan.from_db_row(result[0])
            return None
        except Exception as e:
            logger.error(f"Error getting active ban: {e}")
            return None
    
    async def deactivate(self, ban_id: int) -> bool:
        query = f"DECLARE $ban_id AS Uint64; UPDATE {self.table_name} SET is_active = false WHERE ban_id = $ban_id;"
        params = {'$ban_id': ban_id}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error deactivating ban: {e}")
            return False
    
    async def list_by_chat(
        self, 
        chat_id: int, 
        active_only: bool = True,
        limit: int = 50, 
        offset: int = 0
    ) -> List[ChatBan]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - использует составные индексы
        """
        if active_only:
            # 👇 ИСПОЛЬЗУЕТ ИНДЕКС idx_bans_active
            query = f"""
            DECLARE $chat_id AS Uint64;
            DECLARE $limit AS Uint64;
            DECLARE $offset AS Uint64;
            DECLARE $now AS Timestamp;
            
            SELECT * FROM {self.table_name}
            WHERE chat_id = $chat_id 
              AND is_active = true
              AND (is_permanent = true OR expires_at > $now)
            ORDER BY banned_at DESC
            LIMIT $limit OFFSET $offset;
            """
            params = {
                '$chat_id': chat_id,
                '$limit': limit,
                '$offset': offset,
                '$now': to_timestamp(datetime.utcnow())
            }
        else:
            # 👇 ИСПОЛЬЗУЕТ ИНДЕКС idx_bans_chat_date
            query = f"""
            DECLARE $chat_id AS Uint64;
            DECLARE $limit AS Uint64;
            DECLARE $offset AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE chat_id = $chat_id
            ORDER BY banned_at DESC
            LIMIT $limit OFFSET $offset;
            """
            params = {'$chat_id': chat_id, '$limit': limit, '$offset': offset}
        
        try:
            result = await self.execute(query, params)
            return [ChatBan.from_db_row(row) for row in result] if result else []
        except Exception as e:
            logger.error(f"Error listing bans: {e}")
            return []


class JoinRequestRepository(BaseRepository):
    """Репозиторий для заявок на вступление"""
    
    def __init__(self, session=None):  # 👈 ИСПРАВЛЕНО
        super().__init__(session)
        self.table_name = "join_requests"
    
    async def create(self, request: JoinRequest) -> Optional[JoinRequest]:
        data = request.to_db_row()
        
        if data.get('request_id') is None:
            data['request_id'] = int(time.time() * 1000) ^ (secrets.randbits(32))
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})
        RETURNING request_id;
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                request_id = result[0]['request_id']
                return await self.get(request_id)
            return None
        except Exception as e:
            logger.error(f"Error creating join request: {e}")
            return None
    
    async def get(self, request_id: int) -> Optional[JoinRequest]:
        query = f"DECLARE $request_id AS Uint64; SELECT * FROM {self.table_name} WHERE request_id = $request_id;"
        params = {'$request_id': request_id}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                return JoinRequest.from_db_row(result[0])
            return None
        except Exception as e:
            logger.error(f"Error getting join request: {e}")
            return None
    
    async def get_pending(self, chat_id: int, user_id: str) -> Optional[JoinRequest]:
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $user_id AS Utf8;
        
        SELECT * FROM {self.table_name}
        WHERE chat_id = $chat_id AND user_id = $user_id AND status = 'pending'
        LIMIT 1;
        """
        params = {'$chat_id': chat_id, '$user_id': str(user_id)}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                return JoinRequest.from_db_row(result[0])
            return None
        except Exception as e:
            logger.error(f"Error getting pending request: {e}")
            return None
    
    async def list_by_chat(
        self, 
        chat_id: int, 
        status: Optional[str] = None, 
        limit: int = 50, 
        offset: int = 0
    ) -> List[JoinRequest]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - использует составной индекс
        """
        conditions = ["chat_id = $chat_id"]
        params = {'$chat_id': chat_id, '$limit': limit, '$offset': offset}
        
        if status:
            conditions.append("status = $status")
            params['$status'] = status
        
        where_clause = " AND ".join(conditions)
        
        # 👇 ПРОСТОЙ ЗАПРОС, ИНДЕКС СДЕЛАЕТ ВСЁ
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        {self._generate_declare(params) if status else ''}
        
        SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT $limit OFFSET $offset;
        """
        
        try:
            result = await self.execute(query, params)
            return [JoinRequest.from_db_row(row) for row in result] if result else []
        except Exception as e:
            logger.error(f"Error listing join requests: {e}")
            return []
    
    async def update_status(self, request_id: int, status: str, 
                            reviewed_by: str, reject_reason: Optional[str] = None) -> bool:
        now = datetime.utcnow()
        
        params = {
            '$request_id': request_id,
            '$status': status,
            '$reviewed_by': reviewed_by,
            '$reviewed_at': to_timestamp(now),
            '$reject_reason': reject_reason
        }
        
        query = f"""
        DECLARE $request_id AS Uint64;
        DECLARE $status AS Utf8;
        DECLARE $reviewed_by AS Utf8;
        DECLARE $reviewed_at AS Timestamp;
        DECLARE $reject_reason AS Utf8?;
        
        UPDATE {self.table_name}
        SET 
            status = $status,
            reviewed_by = $reviewed_by,
            reviewed_at = $reviewed_at,
            reject_reason = $reject_reason
        WHERE request_id = $request_id;
        """
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error updating request status: {e}")
            return False
    
    async def list_by_user(self, user_id: str, limit: int = 50, offset: int = 0) -> List[JoinRequest]:
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE user_id = $user_id
        ORDER BY created_at DESC
        LIMIT $limit OFFSET $offset;
        """
        params = {'$user_id': str(user_id), '$limit': limit, '$offset': offset}
        
        try:
            result = await self.execute(query, params)
            return [JoinRequest.from_db_row(row) for row in result] if result else []
        except Exception as e:
            logger.error(f"Error listing user requests: {e}")
            return []


class UsernameRepository(BaseRepository):
    """Репозиторий для глобального реестра username"""
    
    def __init__(self, session=None):  # 👈 ИСПРАВЛЕНО
        super().__init__(session)
        self.table_name = "usernames"
    
    async def check_available(self, username: str) -> Optional[Dict]:
        query = f"""
        DECLARE $username AS Utf8;
        SELECT * FROM {self.table_name} WHERE username = $username;
        """
        params = {'$username': username}
        
        try:
            rows = await self.execute(query, params)
            if rows:
                row = rows[0]
                return {
                    'username': row['username'],
                    'entity_type': row['entity_type'],
                    'entity_id': row['entity_id'],
                    'created_at': row['created_at'],
                    'updated_at': row.get('updated_at')
                }
            return None
        except Exception as e:
            logger.error(f"Failed to check username: {e}")
            return None
    
    async def reserve(self, username: str, entity_type: str, entity_id: str) -> bool:
        query = f"""
        DECLARE $username AS Utf8;
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        DECLARE $now AS Timestamp;
        
        UPSERT INTO {self.table_name} (username, entity_type, entity_id, created_at, updated_at)
        VALUES ($username, $entity_type, $entity_id, $now, $now);
        """
        
        params = {
            '$username': username,
            '$entity_type': entity_type,
            '$entity_id': entity_id,
            '$now': to_timestamp(datetime.utcnow())
        }
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to reserve username: {e}")
            return False
    
    async def release(self, username: str) -> bool:
        query = f"""
        DECLARE $username AS Utf8;
        DELETE FROM {self.table_name} WHERE username = $username;
        """
        params = {'$username': username}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to release username: {e}")
            return False


class InviteRepository(BaseRepository):
    """Репозиторий для таблицы chat_invites"""
    
    def __init__(self, session=None):  # 👈 ИСПРАВЛЕНО
        super().__init__(session)
        self.table_name = "chat_invites"
    
    def _generate_invite_code(self) -> str:
        alphabet = string.ascii_uppercase + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(chat_config.INVITE_CODE_LENGTH))
    
    async def create(self, invite: ChatInvite) -> Optional[ChatInvite]:
        data = invite.to_db_row()
        
        if 'invite_id' not in data or data['invite_id'] is None:
            data['invite_id'] = int(time.time() * 1000) ^ (secrets.randbits(32))
        
        if 'remaining_uses' not in data or data['remaining_uses'] is None:
            data['remaining_uses'] = data.get('max_uses', 0)
        
        if 'created_by' in data and data['created_by']:
            data['created_by'] = str(data['created_by'])
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})
        RETURNING invite_id;
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                invite_id = result[0]['invite_id']
                return await self.get(invite_id)
            return None
        except Exception as e:
            logger.error(f"Error creating invite: {e}")
            return None
    
    async def get(self, invite_id: int) -> Optional[ChatInvite]:
        query = f"DECLARE $invite_id AS Uint64; SELECT * FROM {self.table_name} WHERE invite_id = $invite_id;"
        params = {'$invite_id': invite_id}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                return ChatInvite.from_db_row(result[0])
            return None
        except Exception as e:
            logger.error(f"Error getting invite: {e}")
            return None
    
    async def get_by_code(self, invite_code: str) -> Optional[ChatInvite]:
        query = f"""
        DECLARE $invite_code AS Utf8;
        SELECT * FROM {self.table_name}
        WHERE invite_code = $invite_code;
        """
        params = {'$invite_code': invite_code}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                return ChatInvite.from_db_row(result[0])
            return None
        except Exception as e:
            logger.error(f"Error getting invite by code: {e}")
            return None
    
    async def increment_used(self, invite_id: int) -> bool:
        """
        Увеличить счетчик использований приглашения
        """
        query = f"""
        DECLARE $invite_id AS Uint64;
        
        UPDATE {self.table_name}
        SET 
            used_count = used_count + CAST(1 AS Uint32),
            remaining_uses = remaining_uses - CAST(1 AS Uint32)
        WHERE invite_id = $invite_id AND remaining_uses > CAST(0 AS Uint32);
        """
        params = {'$invite_id': invite_id}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error incrementing invite uses: {e}")
            return False
    
    async def list_by_chat(
        self, 
        chat_id: int, 
        active_only: bool = True,
        limit: int = 50, 
        offset: int = 0
    ) -> List[ChatInvite]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - использует составные индексы
        """
        if active_only:
            query = """
            DECLARE $chat_id AS Uint64;
            DECLARE $limit AS Uint64;
            DECLARE $offset AS Uint64;
            DECLARE $now AS Timestamp;
            
            SELECT *
            FROM `chat_invites`
            WHERE chat_id = $chat_id 
              AND is_active = true
              AND (expires_at IS NULL OR expires_at > $now)
            ORDER BY created_at DESC
            LIMIT $limit OFFSET $offset;
            """
            params = {
                '$chat_id': chat_id,
                '$limit': limit,
                '$offset': offset,
                '$now': to_timestamp(datetime.utcnow())
            }
        else:
            query = """
            DECLARE $chat_id AS Uint64;
            DECLARE $limit AS Uint64;
            DECLARE $offset AS Uint64;
            
            SELECT *
            FROM `chat_invites`
            WHERE chat_id = $chat_id
            ORDER BY created_at DESC
            LIMIT $limit OFFSET $offset;
            """
            params = {'$chat_id': chat_id, '$limit': limit, '$offset': offset}
        
        try:
            result = await self.execute(query, params)
            return [ChatInvite.from_db_row(row) for row in result] if result else []
        except Exception as e:
            logger.error(f"Error listing invites: {e}")
            return []


class EventRepository(BaseRepository):
    """Репозиторий для таблицы chat_events"""
    
    def __init__(self, session=None):  # 👈 ИСПРАВЛЕНО
        super().__init__(session)
        self.table_name = "chat_events"
    
    async def create(self, event: ChatEvent) -> Optional[int]:
        data = event.to_db_row()
        data.pop('event_id', None)
        
        if 'event_id' not in data or data['event_id'] is None:
            data['event_id'] = int(time.time() * 1000) ^ (secrets.randbits(32))
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${key}" for key in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})
        RETURNING event_id;
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                return result[0]['event_id']
            return None
        except Exception as e:
            logger.error(f"Error creating event: {e}")
            return None

    async def list_by_chat(
        self, 
        chat_id: int, 
        limit: int = 50, 
        offset: int = 0,
        event_type: Optional[str] = None,
        user_id: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> List[ChatEvent]:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ - использует составные индексы
        """
        conditions = ["chat_id = $chat_id"]
        params = {'$chat_id': chat_id, '$limit': limit, '$offset': offset}
        
        # Добавляем фильтры (используют индексы)
        if event_type:
            conditions.append("event_type = $event_type")
            params['$event_type'] = event_type
        
        if user_id:
            conditions.append("user_id = $user_id")
            params['$user_id'] = str(user_id)
        
        if from_date:
            try:
                from_datetime = datetime.fromisoformat(from_date)
                conditions.append("created_at >= $from_date")
                params['$from_date'] = to_timestamp(from_datetime)
            except:
                pass
        
        if to_date:
            try:
                to_datetime = datetime.fromisoformat(to_date)
                conditions.append("created_at <= $to_date")
                params['$to_date'] = to_timestamp(to_datetime)
            except:
                pass
        
        where_clause = " AND ".join(conditions)
        
        # 👇 ИСПОЛЬЗУЕМ ПОДСКАЗКИ ДЛЯ ОПТИМИЗАТОРА
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        {self._generate_declare(params)}
        
        $events = SELECT * FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT $limit OFFSET $offset;
        
        SELECT * FROM $events
        ORDER BY created_at DESC;
        """
        
        try:
            result = await self.execute(query, params)
            return [ChatEvent.from_db_row(row) for row in result] if result else []
        except Exception as e:
            logger.error(f"Error listing events: {e}")
            return []


# ============================================
# ИСПРАВЛЕННЫЙ UNIT OF WORK
# ============================================

class ChatUnitOfWork(UnitOfWork):
    def __init__(self):
        super().__init__()
        # 👇 ИСПРАВЛЕНО: создаем lazy свойства
        self._chats = None
        self._participants = None
        self._bans = None
        self._invites = None
        self._events = None
        self._pinned = None
        self._join_requests = None
        self._usernames = None

    @property
    def chats(self):
        if self._chats is None:
            self._chats = self.register_repository(ChatRepository(self._session))
        return self._chats

    @property
    def participants(self):
        if self._participants is None:
            self._participants = self.register_repository(ParticipantRepository(self._session))
        return self._participants

    @property
    def bans(self):
        if self._bans is None:
            self._bans = self.register_repository(BanRepository(self._session))
        return self._bans

    @property
    def invites(self):
        if self._invites is None:
            self._invites = self.register_repository(InviteRepository(self._session))
        return self._invites

    @property
    def events(self):
        if self._events is None:
            self._events = self.register_repository(EventRepository(self._session))
        return self._events

    @property
    def pinned(self):
        if self._pinned is None:
            self._pinned = self.register_repository(PinnedMessageRepository(self._session))
        return self._pinned

    @property
    def join_requests(self):
        if self._join_requests is None:
            self._join_requests = self.register_repository(JoinRequestRepository(self._session))
        return self._join_requests

    @property
    def usernames(self):
        if self._usernames is None:
            self._usernames = self.register_repository(UsernameRepository(self._session))
        return self._usernames


# ============================================
# ИСПРАВЛЕННЫЙ СЕРВИС
# ============================================

class ChatService:
    """Сервис для работы с чатами - оптимизированная версия"""
    
    def __init__(self):
        self.participant_cache = ParticipantCache()
        self._online_estimates = {}
        logger.info("✅ ChatService initialized (optimized)")
    
    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========
    



    async def _notify_chat_event(self, chat_id: int, event_type: str, data: Dict,
                                 exclude_user_id: Optional[str] = None, session = None):
        """
        Отправить WebSocket уведомление о событии в чате всем участникам
        """
        try:
            # Используем переданную сессию или создаем новую
            if session:
                from handlers.chat_handler import ParticipantRepository
                participant_repo = ParticipantRepository(session)
                members = await participant_repo.list_by_chat(chat_id, limit=10000, active_only=True)
                
                for member in members:
                    if exclude_user_id and member.user_id == exclude_user_id:
                        continue
                    
                    asyncio.create_task(
                        send_ws_notification(
                            user_id=member.user_id,
                            notification_type=event_type,
                            data=data
                        )
                    )
            else:
                async with RequestContext() as ctx:
                    from handlers.chat_handler import ParticipantRepository
                    participant_repo = ParticipantRepository(ctx.session)
                    members = await participant_repo.list_by_chat(chat_id, limit=10000, active_only=True)
                    
                    for member in members:
                        if exclude_user_id and member.user_id == exclude_user_id:
                            continue
                        
                        asyncio.create_task(
                            send_ws_notification(
                                user_id=member.user_id,
                                notification_type=event_type,
                                data=data
                            )
                        )
            
            logger.debug(f"📤 Chat event {event_type} notifications queued for {len(members)} users")
                
        except Exception as e:
            logger.error(f"❌ Failed to send chat event notifications: {e}")

    async def _notify_user_chat_event(self, user_id: str, event_type: str, data: Dict, session=None):
        """Отправить WebSocket уведомление конкретному пользователю"""
        try:
            await WebSocketManager.send_to_user(
                user_id,
                {
                    'type': event_type,
                    'data': data,
                    'timestamp': datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            logger.error(f"❌ Failed to send user notification: {e}")
    
    async def get_chat_stats(self, chat_id: int, user_id: str, session = None) -> Dict:
        """
        ОПТИМИЗИРОВАННАЯ ВЕРСИЯ 2.0 - 3 запроса вместо 5 (с ELSE 0)
        """
        # 👇 КЛЮЧ КЭША (TTL 5 минут)
        cache_key = f"chat_stats:{chat_id}"
        cached = await cache.get(cache_key)
        if cached:
            logger.info(f"✅ Stats cache hit for chat {chat_id}")
            return cached
        
        # Используем переданную сессию или создаем новую
        if session:
            # Проверяем доступ
            has_access = await self.participant_cache.is_member(
                chat_id, 
                user_id, 
                session=session
            )
            if not has_access:
                raise PermissionError("You don't have access to this chat")
            
            # Получаем чат
            chat_repo = ChatRepository(session)
            chat = await chat_repo.get_by_id(chat_id)
            if not chat:
                raise NotFoundError(f"Chat {chat_id} not found")
            
            now = datetime.utcnow()
            thirty_days_ago = now - timedelta(days=30)
            
            # 👇 1. ОБЪЕДИНЕННЫЙ ЗАПРОС (с ELSE 0!)
            messages_query = """
            DECLARE $chat_id AS Uint64;
            DECLARE $days30 AS Timestamp;
            
            SELECT
                COUNT(*) as total_messages,
                COUNT(CASE WHEN created_at >= $days30 THEN 1 ELSE 0 END) as recent_messages,
                COUNT(DISTINCT sender_id) as unique_senders_30d
            FROM `messages`
            WHERE chat_id = $chat_id AND is_deleted = false;
            """
            
            messages_result = await session.transaction().execute(
                await session.prepare(messages_query),
                {'$chat_id': chat_id, '$days30': to_timestamp(thirty_days_ago)},
                commit_tx=True
            )
            
            messages_stats = messages_result[0].rows[0] if messages_result and messages_result[0].rows else {}
            
            # 👇 2. КОЛИЧЕСТВО УЧАСТНИКОВ
            members_query = """
            DECLARE $chat_id AS Uint64;
            SELECT COUNT(*) as total_members
            FROM `chat_participants`
            WHERE chat_id = $chat_id AND is_active = true;
            """
            
            members_result = await session.transaction().execute(
                await session.prepare(members_query),
                {'$chat_id': chat_id},
                commit_tx=True
            )
            
            total_members = members_result[0].rows[0]['total_members'] if members_result and members_result[0].rows else 0
            
            # 👇 3. ТОП ОТПРАВИТЕЛЕЙ
            top_query = """
            DECLARE $chat_id AS Uint64;
            SELECT sender_id, COUNT(*) as count
            FROM `messages`
            WHERE chat_id = $chat_id AND is_deleted = false
            GROUP BY sender_id
            ORDER BY count DESC
            LIMIT 10;
            """
            
            top_result = await session.transaction().execute(
                await session.prepare(top_query),
                {'$chat_id': chat_id},
                commit_tx=True
            )
            
            top_senders = []
            if top_result and top_result[0].rows:
                top_senders = [
                    {'user_id': row['sender_id'], 'count': row['count']}
                    for row in top_result[0].rows
                ]
            
            # 👇 Онлайн оценка (из кэша)
            online_estimate = await self._get_online_estimate(chat_id)
            
            result = {
                'chat_id': str(chat_id),
                'title': chat.title,
                'type': chat.type,
                'total_messages': messages_stats.get('total_messages', 0),
                'recent_messages_30d': messages_stats.get('recent_messages', 0),
                'total_members': total_members,
                'online_estimate': online_estimate,
                'unique_senders_30d': messages_stats.get('unique_senders_30d', 0),
                'top_senders': top_senders,
                'created_at': chat.created_at.isoformat() if chat.created_at else None,
                'last_message_at': chat.last_message_at.isoformat() if chat.last_message_at else None
            }
        else:
            async with RequestContext() as ctx:
                # Проверяем доступ
                has_access = await self.participant_cache.is_member(
                    chat_id, 
                    user_id, 
                    session=ctx.session
                )
                if not has_access:
                    raise PermissionError("You don't have access to this chat")
                
                # Получаем чат
                chat_repo = ChatRepository(ctx.session)
                chat = await chat_repo.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                now = datetime.utcnow()
                thirty_days_ago = now - timedelta(days=30)
                
                # 👇 1. ОБЪЕДИНЕННЫЙ ЗАПРОС (с ELSE 0!)
                messages_query = """
                DECLARE $chat_id AS Uint64;
                DECLARE $days30 AS Timestamp;
                
                SELECT
                    COUNT(*) as total_messages,
                    COUNT(CASE WHEN created_at >= $days30 THEN 1 ELSE 0 END) as recent_messages,
                    COUNT(DISTINCT sender_id) as unique_senders_30d
                FROM `messages`
                WHERE chat_id = $chat_id AND is_deleted = false;
                """
                
                messages_result = await ctx.session.transaction().execute(
                    await ctx.session.prepare(messages_query),
                    {'$chat_id': chat_id, '$days30': to_timestamp(thirty_days_ago)},
                    commit_tx=True
                )
                
                messages_stats = messages_result[0].rows[0] if messages_result and messages_result[0].rows else {}
                
                # 👇 2. КОЛИЧЕСТВО УЧАСТНИКОВ
                members_query = """
                DECLARE $chat_id AS Uint64;
                SELECT COUNT(*) as total_members
                FROM `chat_participants`
                WHERE chat_id = $chat_id AND is_active = true;
                """
                
                members_result = await ctx.session.transaction().execute(
                    await ctx.session.prepare(members_query),
                    {'$chat_id': chat_id},
                    commit_tx=True
                )
                
                total_members = members_result[0].rows[0]['total_members'] if members_result and members_result[0].rows else 0
                
                # 👇 3. ТОП ОТПРАВИТЕЛЕЙ
                top_query = """
                DECLARE $chat_id AS Uint64;
                SELECT sender_id, COUNT(*) as count
                FROM `messages`
                WHERE chat_id = $chat_id AND is_deleted = false
                GROUP BY sender_id
                ORDER BY count DESC
                LIMIT 10;
                """
                
                top_result = await ctx.session.transaction().execute(
                    await ctx.session.prepare(top_query),
                    {'$chat_id': chat_id},
                    commit_tx=True
                )
                
                top_senders = []
                if top_result and top_result[0].rows:
                    top_senders = [
                        {'user_id': row['sender_id'], 'count': row['count']}
                        for row in top_result[0].rows
                    ]
                
                # 👇 Онлайн оценка (из кэша)
                online_estimate = await self._get_online_estimate(chat_id)
                
                result = {
                    'chat_id': str(chat_id),
                    'title': chat.title,
                    'type': chat.type,
                    'total_messages': messages_stats.get('total_messages', 0),
                    'recent_messages_30d': messages_stats.get('recent_messages', 0),
                    'total_members': total_members,
                    'online_estimate': online_estimate,
                    'unique_senders_30d': messages_stats.get('unique_senders_30d', 0),
                    'top_senders': top_senders,
                    'created_at': chat.created_at.isoformat() if chat.created_at else None,
                    'last_message_at': chat.last_message_at.isoformat() if chat.last_message_at else None
                }
        
        # 👇 КЭШИРУЕМ НА 5 МИНУТ
        await cache.set(cache_key, result, ttl=300)
        
        return result

    async def search_users(self, query: str, user_id: str, limit: int = 20, offset: int = 0, session=None) -> Tuple[List[UserSearchResult], int]:
        """Поиск пользователей"""
        logger.info(f"🔍 Searching users with query: '{query}'")
        
        if len(query) < 2:
            raise ValidationError("Search query must be at least 2 characters")
        
        if session:
            repo = UserSearchRepository(session)
            users, total = await repo.search_users(
                query=query,
                limit=limit,
                offset=offset,
                current_user_id=user_id
            )
        else:
            async with RequestContext() as ctx:
                repo = UserSearchRepository(ctx.session)
                users, total = await repo.search_users(
                    query=query,
                    limit=limit,
                    offset=offset,
                    current_user_id=user_id
                )
        
        logger.info(f"✅ Found {len(users)} users (total: {total})")
        return users, total
    
    async def _check_idempotency(self, idempotency_key: str, chat_id: int, session=None) -> Optional[Chat]:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                existing_key = await uow.idempotency.get(idempotency_key)
                if existing_key:
                    return await uow.chats.get_by_id(existing_key.entity_id)
        else:
            async with ChatUnitOfWork() as uow:
                existing_key = await uow.idempotency.get(idempotency_key)
                if existing_key:
                    return await uow.chats.get_by_id(existing_key.entity_id)
        return None
    
    async def _save_idempotency_key(self, uow, key: str, chat_id: int, user_id: str):
        key_obj = IdempotencyKey(
            idempotency_key=key,
            entity_type="chat",
            entity_id=chat_id % (2**64),
            chat_id=chat_id,
            user_id=user_id,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=24)
        )
        await uow.idempotency.create(key_obj)
    
    async def _get_online_estimate(self, chat_id: int) -> int:
        """Быстрая оценка онлайн пользователей (с кэшем)"""
        cache_key = f"online_estimate:{chat_id}"
        
        # Проверяем кэш
        cached = await cache.get(cache_key)
        if cached is not None:
            return cached
        
        try:
            active_since = datetime.utcnow() - timedelta(minutes=15)
            
            query = """
            DECLARE $chat_id AS Uint64;
            DECLARE $since AS Timestamp;
            
            SELECT COUNT(*) as online
            FROM `chat_participants`
            WHERE chat_id = $chat_id 
              AND is_active = true
              AND last_active_at >= $since;
            """
            
            async with RequestContext() as ctx:
                result = await ctx.session.transaction().execute(
                    await ctx.session.prepare(query),
                    {
                        '$chat_id': chat_id,
                        '$since': to_timestamp(active_since)
                    },
                    commit_tx=True
                )
                
                online = result[0].rows[0]['online'] if result and result[0].rows else 0
            
            # Кэшируем на 5 минут
            await cache.set(cache_key, online, ttl=300)
            return online
            
        except Exception as e:
            logger.error(f"Error getting online estimate: {e}")
            return 0
    
    # ========== ОСНОВНЫЕ МЕТОДЫ ==========

    async def get_user_permissions(self, chat_id: int, user_id: str, session = None) -> Dict:
        """Получить права пользователя в чате"""
        participant = await self.participant_cache.get_participant(
            chat_id, 
            user_id, 
            session=session
        )
        
        if not participant:
            if session:
                async with await ChatUnitOfWork.with_session(session) as uow:
                    chat = await uow.chats.get_by_id(chat_id)
                    if chat and chat.is_public:
                        return {
                            'chat_id': chat_id,
                            'user_id': user_id,
                            'role': 'visitor',
                            'permissions': {
                                'can_delete_messages': False,
                                'can_ban_users': False,
                                'can_pin_messages': False,
                                'can_change_info': False,
                                'can_invite_users': False,
                                'can_promote_members': False,
                                'can_post_messages': chat.type == 'channel',
                                'can_edit_messages': False,
                                'can_view_admins': True
                            }
                        }
            else:
                async with ChatUnitOfWork() as uow:
                    chat = await uow.chats.get_by_id(chat_id)
                    if chat and chat.is_public:
                        return {
                            'chat_id': chat_id,
                            'user_id': user_id,
                            'role': 'visitor',
                            'permissions': {
                                'can_delete_messages': False,
                                'can_ban_users': False,
                                'can_pin_messages': False,
                                'can_change_info': False,
                                'can_invite_users': False,
                                'can_promote_members': False,
                                'can_post_messages': chat.type == 'channel',
                                'can_edit_messages': False,
                                'can_view_admins': True
                            }
                        }
            raise PermissionError("You are not a member of this chat")
        
        return {
            'chat_id': chat_id,
            'user_id': user_id,
            'role': participant.get('role'),
            'permissions': participant.get('permissions', {})
        }
    
    async def update_member_permissions(self, chat_id: int, admin_id: str, target_user_id: str, 
                                       permissions: Dict[str, bool], session=None) -> Dict:
        logger.info(f"🔧 Updating permissions for user {target_user_id} in chat {chat_id}")
        
        allowed_permissions = [
            'can_delete_messages', 'can_ban_users', 'can_pin_messages',
            'can_change_info', 'can_invite_users', 'can_promote_members',
            'can_post_messages', 'can_edit_messages', 'can_view_admins'
        ]
        
        for perm, value in permissions.items():
            if perm not in allowed_permissions:
                raise ValidationError(f"Invalid permission: {perm}")
            if not isinstance(value, bool):
                raise ValidationError(f"Permission {perm} must be boolean")
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can update permissions")
                
                target = await uow.participants.get(chat_id, target_user_id)
                if not target:
                    raise NotFoundError(f"User {target_user_id} is not a member")
                
                if admin.role == 'admin' and target.role in ['owner', 'admin']:
                    raise PermissionError("Cannot update permissions of owners or other admins")
                
                old_permissions = target.permissions_json
                target.permissions_json = permissions
                await uow.participants.update(target)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="permissions_updated",
                    user_id=admin_id,
                    user_role_at_time=admin.role,
                    target_id=None,
                    target_type=None,
                    payload={
                        'target_user_id': target_user_id,
                        'old_permissions': old_permissions,
                        'new_permissions': permissions
                    },
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can update permissions")
                
                target = await uow.participants.get(chat_id, target_user_id)
                if not target:
                    raise NotFoundError(f"User {target_user_id} is not a member")
                
                if admin.role == 'admin' and target.role in ['owner', 'admin']:
                    raise PermissionError("Cannot update permissions of owners or other admins")
                
                old_permissions = target.permissions_json
                target.permissions_json = permissions
                await uow.participants.update(target)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="permissions_updated",
                    user_id=admin_id,
                    user_role_at_time=admin.role,
                    target_id=None,
                    target_type=None,
                    payload={
                        'target_user_id': target_user_id,
                        'old_permissions': old_permissions,
                        'new_permissions': permissions
                    },
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        await self.participant_cache.invalidate(chat_id, target_user_id)
        
        logger.info(f"✅ Permissions updated for user {target_user_id}")
        
        return {
            'chat_id': chat_id,
            'user_id': target_user_id,
            'role': target.role,
            'permissions': permissions,
            'updated_by': admin_id
        }
    
    async def reset_member_permissions(self, chat_id: int, admin_id: str, target_user_id: str, session=None) -> Dict:
        logger.info(f"🔄 Resetting permissions for user {target_user_id} in chat {chat_id}")
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can reset permissions")
                
                target = await uow.participants.get(chat_id, target_user_id)
                if not target:
                    raise NotFoundError(f"User {target_user_id} is not a member")
                
                if admin.role == 'admin' and target.role in ['owner', 'admin']:
                    raise PermissionError("Cannot reset permissions of owners or other admins")
                
                default_perms = RolePermissions.get_default_permissions(target.role)
                target.permissions_json = default_perms.to_dict()
                
                await uow.participants.update(target)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="permissions_reset",
                    user_id=admin_id,
                    user_role_at_time=admin.role,
                    target_id=None,
                    target_type=None,
                    payload={
                        'target_user_id': target_user_id,
                        'role': target.role
                    },
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can reset permissions")
                
                target = await uow.participants.get(chat_id, target_user_id)
                if not target:
                    raise NotFoundError(f"User {target_user_id} is not a member")
                
                if admin.role == 'admin' and target.role in ['owner', 'admin']:
                    raise PermissionError("Cannot reset permissions of owners or other admins")
                
                default_perms = RolePermissions.get_default_permissions(target.role)
                target.permissions_json = default_perms.to_dict()
                
                await uow.participants.update(target)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="permissions_reset",
                    user_id=admin_id,
                    user_role_at_time=admin.role,
                    target_id=None,
                    target_type=None,
                    payload={
                        'target_user_id': target_user_id,
                        'role': target.role
                    },
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        await self.participant_cache.invalidate(chat_id, target_user_id)
        
        logger.info(f"✅ Permissions reset for user {target_user_id}")
        
        return {
            'chat_id': chat_id,
            'user_id': target_user_id,
            'role': target.role,
            'permissions': target.permissions_json,
            'reset_by': admin_id,
            'message': f"Permissions reset to default for role {target.role}"
        }
    
    async def upload_chat_avatar(self, chat_id: int, user_id: str, image_data: str, session=None) -> Optional[str]:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can upload avatar")
        else:
            async with ChatUnitOfWork() as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can upload avatar")
        
        try:
            if ',' in image_data:
                header, base64_data = image_data.split(',', 1)
                content_type = header.split(';')[0].replace('data:', '')
                if 'jpeg' in content_type or 'jpg' in content_type:
                    extension = 'jpg'
                elif 'png' in content_type:
                    extension = 'png'
                elif 'gif' in content_type:
                    extension = 'gif'
                elif 'webp' in content_type:
                    extension = 'webp'
                else:
                    extension = 'jpg'
            else:
                base64_data = image_data
                content_type = 'image/jpeg'
                extension = 'jpg'
            
            image_bytes = base64.b64decode(base64_data)
            
            if len(image_bytes) > 5 * 1024 * 1024:
                raise ValidationError("Image too large. Max size: 5 MB")
            
            file_hash = hashlib.md5(image_bytes).hexdigest()[:16]
            timestamp = int(time.time())
            filename = f"avatar_{timestamp}_{file_hash}.{extension}"
            
            avatar_url = f"https://storage.example.com/chats/{chat_id}/avatars/{filename}"
            
            if session:
                async with await ChatUnitOfWork.with_session(session) as uow:
                    chat = await uow.chats.get_by_id(chat_id)
                    if chat:
                        chat.avatar_url = avatar_url
                        chat.updated_at = datetime.utcnow()
                        await uow.chats.update(chat)
            else:
                async with ChatUnitOfWork() as uow:
                    chat = await uow.chats.get_by_id(chat_id)
                    if chat:
                        chat.avatar_url = avatar_url
                        chat.updated_at = datetime.utcnow()
                        await uow.chats.update(chat)
            
            return avatar_url
        except Exception as e:
            logger.error(f"Failed to upload avatar: {e}")
            raise
    
    async def get_chat_by_username(self, username: str, user_id: str, session=None) -> Chat:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_username(username)
                if not chat:
                    raise NotFoundError(f"Chat with username '{username}' not found")
                
                if not chat.is_public:
                    participant = await uow.participants.get(chat.id, user_id)
                    if not participant:
                        raise PermissionError("You don't have access to this chat")
                
                return chat
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_username(username)
                if not chat:
                    raise NotFoundError(f"Chat with username '{username}' not found")
                
                if not chat.is_public:
                    participant = await uow.participants.get(chat.id, user_id)
                    if not participant:
                        raise PermissionError("You don't have access to this chat")
                
                return chat
    
    async def get_user_public_chats(self, target_user_id: str, requesting_user_id: str, session=None) -> Dict:
        """Получить публичные чаты пользователя для отображения в профиле"""
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                participants, _ = await uow.participants.list_by_user(target_user_id, limit=1000)
                
                channels = []
                groups = []
                
                for p in participants:
                    # p уже объект ChatParticipant, но если это словарь - конвертируем
                    if isinstance(p, dict):
                        p = ChatParticipant.from_db_row(p)
                    
                    if not p.show_in_profile:
                        continue
                    
                    chat = await uow.chats.get_by_id(p.chat_id)
                    
                    if not chat or chat.is_deleted:
                        continue
                    
                    if not chat.is_public or not chat.username:
                        continue
                    
                    if p.role not in ['owner', 'admin']:
                        continue
                    
                    chat_info = {
                        'id': chat.id,
                        'title': chat.title,
                        'username': chat.username,
                        'link': f"https://t.me/{chat.username}",
                        'type': chat.type,
                        'avatar_url': chat.avatar_url,
                        'members_count': chat.members_count,
                        'is_verified': False
                    }
                    
                    if chat.type == 'channel':
                        chat_info['subscribers_count'] = chat.members_count
                        channels.append(chat_info)
                    elif chat.type == 'group':
                        groups.append(chat_info)
        else:
            async with ChatUnitOfWork() as uow:
                participants, _ = await uow.participants.list_by_user(target_user_id, limit=1000)
                
                channels = []
                groups = []
                
                for p in participants:
                    if isinstance(p, dict):
                        p = ChatParticipant.from_db_row(p)
                    
                    if not p.show_in_profile:
                        continue
                    
                    chat = await uow.chats.get_by_id(p.chat_id)
                    
                    if not chat or chat.is_deleted:
                        continue
                    
                    if not chat.is_public or not chat.username:
                        continue
                    
                    if p.role not in ['owner', 'admin']:
                        continue
                    
                    chat_info = {
                        'id': chat.id,
                        'title': chat.title,
                        'username': chat.username,
                        'link': f"https://t.me/{chat.username}",
                        'type': chat.type,
                        'avatar_url': chat.avatar_url,
                        'members_count': chat.members_count,
                        'is_verified': False
                    }
                    
                    if chat.type == 'channel':
                        chat_info['subscribers_count'] = chat.members_count
                        channels.append(chat_info)
                    elif chat.type == 'group':
                        groups.append(chat_info)
        
        return {
            'channels': channels,
            'groups': groups
        }
    
    async def mark_as_read(self, chat_id: int, user_id: str, message_id: int, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("You are not a member of this chat")
                
                from handlers.message_handler import MessageRepository
                msg_repo = MessageRepository(uow._session)
                message = await msg_repo.get(chat_id, message_id)
                if not message:
                    raise NotFoundError(f"Message {message_id} not found")
                
                success = await uow.participants.update_last_read(chat_id, user_id, message_id)
                if not success:
                    raise DatabaseError("Failed to mark messages as read")
                
                await self.participant_cache.invalidate(chat_id, user_id)
                
                return {
                    'success': True,
                    'chat_id': chat_id,
                    'message_id': message_id,
                    'unread_count': 0,
                    'marked_at': datetime.utcnow().isoformat()
                }
        else:
            async with ChatUnitOfWork() as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("You are not a member of this chat")
                
                from handlers.message_handler import MessageRepository
                msg_repo = MessageRepository(uow._session)
                message = await msg_repo.get(chat_id, message_id)
                if not message:
                    raise NotFoundError(f"Message {message_id} not found")
                
                success = await uow.participants.update_last_read(chat_id, user_id, message_id)
                if not success:
                    raise DatabaseError("Failed to mark messages as read")
                
                await self.participant_cache.invalidate(chat_id, user_id)
                
                return {
                    'success': True,
                    'chat_id': chat_id,
                    'message_id': message_id,
                    'unread_count': 0,
                    'marked_at': datetime.utcnow().isoformat()
                }
    
    async def set_username(self, chat_id: int, user_id: str, username: Optional[str], session=None) -> Chat:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can set username")
                
                if username:
                    username = username.lstrip('@').strip().lower()
                    
                    if not re.match(r'^[a-zA-Z0-9_]{5,32}$', username):
                        raise ValidationError("Username must be 5-32 characters, letters, numbers, underscore only")
                    
                    existing = await uow.usernames.check_available(username)
                    if existing:
                        if existing['entity_type'] != chat.type or existing['entity_id'] != str(chat.id):
                            raise ValidationError(f"Username '{username}' is already taken")
                else:
                    username = None
                
                old_username = chat.username
                
                if old_username:
                    await uow.usernames.release(old_username)
                
                chat.username = username
                chat.username_updated_at = datetime.utcnow()
                
                await uow.chats.update(chat)
                
                if username:
                    success = await uow.usernames.reserve(
                        username=username,
                        entity_type=chat.type,
                        entity_id=str(chat.id)
                    )
                    if not success:
                        chat.username = old_username
                        await uow.chats.update(chat)
                        raise DatabaseError(f"Failed to reserve username '{username}'")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="username_changed",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={
                        'old_username': old_username,
                        'new_username': username
                    },
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                return chat
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can set username")
                
                if username:
                    username = username.lstrip('@').strip().lower()
                    
                    if not re.match(r'^[a-zA-Z0-9_]{5,32}$', username):
                        raise ValidationError("Username must be 5-32 characters, letters, numbers, underscore only")
                    
                    existing = await uow.usernames.check_available(username)
                    if existing:
                        if existing['entity_type'] != chat.type or existing['entity_id'] != str(chat.id):
                            raise ValidationError(f"Username '{username}' is already taken")
                else:
                    username = None
                
                old_username = chat.username
                
                if old_username:
                    await uow.usernames.release(old_username)
                
                chat.username = username
                chat.username_updated_at = datetime.utcnow()
                
                await uow.chats.update(chat)
                
                if username:
                    success = await uow.usernames.reserve(
                        username=username,
                        entity_type=chat.type,
                        entity_id=str(chat.id)
                    )
                    if not success:
                        chat.username = old_username
                        await uow.chats.update(chat)
                        raise DatabaseError(f"Failed to reserve username '{username}'")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="username_changed",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={
                        'old_username': old_username,
                        'new_username': username
                    },
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                return chat
    
    async def get_unread_count(self, chat_id: int, user_id: str, session=None) -> int:
        query = f"""
        DECLARE $chat_id AS Uint64;
        DECLARE $user_id AS Utf8;
        
        SELECT unread_count FROM chat_participants
        WHERE chat_id = $chat_id AND user_id = $user_id;
        """
        params = {'$chat_id': chat_id, '$user_id': str(user_id)}
        
        try:
            if session:
                repo = ParticipantRepository(session)
                result = await repo.execute(query, params)
                if result and len(result) > 0:
                    return result[0].get('unread_count', 0)
            else:
                async with RequestContext() as ctx:
                    repo = ParticipantRepository(ctx.session)
                    result = await repo.execute(query, params)
                    if result and len(result) > 0:
                        return result[0].get('unread_count', 0)
            return 0
        except Exception as e:
            logger.error(f"Error getting unread count: {e}")
            return 0

    async def get_all_unread_counts(self, user_id: str, session=None) -> List[Dict]:
        """
        Получить количество непрочитанных сообщений во всех чатах пользователя
        """
        logger.info(f"📊 Getting unread counts for user {user_id}")
        start_time = time.time()
        
        try:
            if session:
                participant_repo = ParticipantRepository(session)
                chat_repo = ChatRepository(session)
                
                participants_result = await participant_repo.list_by_user(user_id, limit=1000)
                
                if isinstance(participants_result, tuple):
                    participants = participants_result[0]
                    logger.info(f"📊 Found {len(participants)} chats for user")
                else:
                    participants = participants_result or []
                    logger.info(f"📊 Found {len(participants)} chats for user")
                
                if not participants:
                    return []
                
                chat_ids = []
                unread_map = {}
                
                for p in participants:
                    if hasattr(p, 'unread_count') and p.unread_count and p.unread_count > 0:
                        chat_ids.append(p.chat_id)
                        unread_map[p.chat_id] = p.unread_count
                
                if not chat_ids:
                    return []
                
                chats_map = await chat_repo.get_many(chat_ids)
                
                result = []
                total_unread = 0
                
                for chat_id in chat_ids:
                    chat = chats_map.get(chat_id)
                    if chat and not chat.is_deleted:
                        unread_count = unread_map.get(chat_id, 0)
                        chat_info = {
                            'chat_id': chat_id,
                            'chat_title': chat.title,
                            'chat_type': chat.type,
                            'unread_count': unread_count,
                            'last_message_id': chat.last_message_id,
                            'last_message_preview': chat.last_message_preview,
                            'last_message_at': chat.last_message_at.isoformat() if chat.last_message_at else None,
                            'last_message_sender_id': chat.last_message_sender_id
                        }
                        result.append(chat_info)
                        total_unread += unread_count
            else:
                async with RequestContext() as ctx:
                    participant_repo = ParticipantRepository(ctx.session)
                    chat_repo = ChatRepository(ctx.session)
                    
                    participants_result = await participant_repo.list_by_user(user_id, limit=1000)
                    
                    if isinstance(participants_result, tuple):
                        participants = participants_result[0]
                        logger.info(f"📊 Found {len(participants)} chats for user")
                    else:
                        participants = participants_result or []
                        logger.info(f"📊 Found {len(participants)} chats for user")
                    
                    if not participants:
                        return []
                    
                    chat_ids = []
                    unread_map = {}
                    
                    for p in participants:
                        if hasattr(p, 'unread_count') and p.unread_count and p.unread_count > 0:
                            chat_ids.append(p.chat_id)
                            unread_map[p.chat_id] = p.unread_count
                    
                    if not chat_ids:
                        return []
                    
                    chats_map = await chat_repo.get_many(chat_ids)
                    
                    result = []
                    total_unread = 0
                    
                    for chat_id in chat_ids:
                        chat = chats_map.get(chat_id)
                        if chat and not chat.is_deleted:
                            unread_count = unread_map.get(chat_id, 0)
                            chat_info = {
                                'chat_id': chat_id,
                                'chat_title': chat.title,
                                'chat_type': chat.type,
                                'unread_count': unread_count,
                                'last_message_id': chat.last_message_id,
                                'last_message_preview': chat.last_message_preview,
                                'last_message_at': chat.last_message_at.isoformat() if chat.last_message_at else None,
                                'last_message_sender_id': chat.last_message_sender_id
                            }
                            result.append(chat_info)
                            total_unread += unread_count
            
            duration = time.time() - start_time
            logger.info(f"✅ Got unread counts for {len(result)} chats in {duration*1000:.1f}ms, total unread: {total_unread}")
            
            return result
                
        except Exception as e:
            logger.error(f"❌ Error getting unread counts: {e}")
            return []

    async def mark_all_as_read(self, chat_id: int, user_id: str, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("You are not a member of this chat")
                
                chat = await uow.chats.get_by_id(chat_id)
                if not chat or not chat.last_message_id:
                    return {
                        'success': True,
                        'chat_id': chat_id,
                        'message_id': None,
                        'unread_count': 0
                    }
                
                success = await uow.participants.update_last_read(chat_id, user_id, chat.last_message_id)
                if not success:
                    raise DatabaseError("Failed to mark messages as read")
                
                await self.participant_cache.invalidate(chat_id, user_id)
                
                return {
                    'success': True,
                    'chat_id': chat_id,
                    'message_id': chat.last_message_id,
                    'unread_count': 0,
                    'marked_at': datetime.utcnow().isoformat()
                }
        else:
            async with ChatUnitOfWork() as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("You are not a member of this chat")
                
                chat = await uow.chats.get_by_id(chat_id)
                if not chat or not chat.last_message_id:
                    return {
                        'success': True,
                        'chat_id': chat_id,
                        'message_id': None,
                        'unread_count': 0
                    }
                
                success = await uow.participants.update_last_read(chat_id, user_id, chat.last_message_id)
                if not success:
                    raise DatabaseError("Failed to mark messages as read")
                
                await self.participant_cache.invalidate(chat_id, user_id)
                
                return {
                    'success': True,
                    'chat_id': chat_id,
                    'message_id': chat.last_message_id,
                    'unread_count': 0,
                    'marked_at': datetime.utcnow().isoformat()
                }
    
    
    async def get_or_create_private_chat(self, user_id: str, recipient_id: str, session=None) -> Chat:
        """Получить существующий приватный чат или создать новый."""
        if user_id == recipient_id:
            raise ValidationError("Cannot create chat with yourself")
        
        async with await ChatUnitOfWork.with_session(session) as uow:
            # Ищем существующий чат
            existing = await self._find_private_chat(uow, user_id, recipient_id)
            if existing:
                logger.info(f"📦 Returning existing private chat {existing.id}")
                
                # Получаем имя собеседника для title
                partner_name = await self._get_partner_name(existing, user_id)
                if partner_name:
                    existing.title = partner_name
                
                return existing
            
            # Создаём новый чат
            logger.info(f"🆕 Creating new private chat between {user_id[:8]} and {recipient_id[:8]}")
            chat = await self.create_private_chat(user_id, recipient_id, session=session)
            
            # Получаем имя собеседника для title
            partner_name = await self._get_partner_name(chat, user_id)
            if partner_name:
                chat.title = partner_name
            
            return chat
    
    async def _find_private_chat(self, uow, user1_id: str, user2_id: str) -> Optional[Chat]:
        """Найти существующий приватный чат между двумя пользователями"""
        # Ищем в обоих направлениях
        query = """
        DECLARE $user1 AS Utf8;
        DECLARE $user2 AS Utf8;
        
        SELECT id FROM `chats`
        WHERE type = 'private' 
          AND (
            (user1_id = $user1 AND user2_id = $user2)
            OR (user1_id = $user2 AND user2_id = $user1)
          )
          AND is_deleted = false
        LIMIT 1;
        """
        
        params = {
            '$user1': user1_id,
            '$user2': user2_id
        }
        
        try:
            # Используем сессию из UOW для выполнения запроса
            session = uow._session
            prepared_query = await session.prepare(query)
            result = await session.transaction().execute(
                prepared_query,
                params,
                commit_tx=True
            )
            
            if result and len(result) > 0 and result[0].rows:
                chat_id = result[0].rows[0]['id']
                logger.info(f"✅ Found existing private chat {chat_id} between {user1_id[:8]} and {user2_id[:8]}")
                return await uow.chats.get_by_id(chat_id)
            return None
        except Exception as e:
            logger.error(f"❌ Error finding private chat: {e}")
            return None

    async def create_private_chat(self, user1_id: str, user2_id: str, session=None) -> Chat:
        """Создать приватный чат с защитой от дублирования"""
        if user1_id == user2_id:
            raise ValidationError("Cannot create chat with yourself")
        
        # Канонический порядок
        u1, u2 = sorted([user1_id, user2_id])
        
        async with await ChatUnitOfWork.with_session(session) as uow:
            # Проверяем существование
            existing = await self._find_private_chat(uow, user1_id, user2_id)
            if existing:
                return existing
            
            # Создаём новый чат
            chat_id = uuid.uuid4().int & (2**64 - 1)
            now = datetime.utcnow()
            
            chat = Chat(
                id=chat_id,
                type='private',
                title=f"Dialog between {user1_id[:8]} and {user2_id[:8]}",
                owner_id=user1_id,
                created_by=user1_id,
                created_at=now,
                updated_at=now,
                is_public=False,
                join_moderation=False,
                max_members=2,
                slow_mode_interval=0,
                is_active=True,
                is_archived=False,
                is_deleted=False,
                settings={},
                members_count=2,
                messages_count=0,
                online_estimate=0,
                views_count=0,
                version=1,
                primary_region=chat_config.DEFAULT_REGION,
                status='active',
                user1_id=u1,
                user2_id=u2
            )
            
            # Сохраняем чат
            result = await uow.chats.create(chat)
            if not result:
                raise DatabaseError("Failed to create private chat")
            
            # Создаём участников
            participant1 = ChatParticipant(
                chat_id=result.id,
                user_id=user1_id,
                role='owner',
                joined_at=now,
                joined_method='create',
                is_active=True,
                unread_count=0,
                version=1,
                region=chat_config.DEFAULT_REGION,
                is_hidden=False
            )
            
            participant2 = ChatParticipant(
                chat_id=result.id,
                user_id=user2_id,
                role='member',
                joined_at=now,
                joined_method='auto',
                is_active=True,
                unread_count=0,
                version=1,
                region=chat_config.DEFAULT_REGION,
                is_hidden=False
            )
            
            await uow.participants.create(participant1)
            await uow.participants.create(participant2)
            
            # Создаём событие
            event = ChatEvent(
                event_id=None,
                chat_id=result.id,
                event_type="private_chat_created",
                user_id=user1_id,
                user_role_at_time='owner',
                payload={'other_user': user2_id},
                created_at=now,
                created_date=int(now.strftime('%Y%m%d')),
                region=chat_config.DEFAULT_REGION
            )
            await uow.events.create(event)
            
            return result
    
    async def restore_dialog(self, chat_id: int, user_id: str, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                if chat.type != 'private':
                    raise ValidationError("This operation is only for private chats")
                
                if chat.is_deleted:
                    raise ValidationError("This chat was deleted for all users")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("You are not a member of this chat")
                
                success = await uow.participants.show_dialog(chat_id, user_id)
                if not success:
                    raise DatabaseError("Failed to restore dialog")
                
                return {
                    'restored': True,
                    'chat_id': chat_id,
                    'hidden': False
                }
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                if chat.type != 'private':
                    raise ValidationError("This operation is only for private chats")
                
                if chat.is_deleted:
                    raise ValidationError("This chat was deleted for all users")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("You are not a member of this chat")
                
                success = await uow.participants.show_dialog(chat_id, user_id)
                if not success:
                    raise DatabaseError("Failed to restore dialog")
                
                return {
                    'restored': True,
                    'chat_id': chat_id,
                    'hidden': False
                }
    
    async def get_hidden_dialogs(self, user_id: str, limit: int = 50, cursor: Optional[str] = None,
                                session=None) -> Tuple[List[Chat], Optional[str]]:
        """
        Получить скрытые диалоги с пагинацией
        """
        logger.info(f"📁 Getting hidden dialogs for user {user_id}")
        
        # Всегда используем переданную сессию или создаем новую
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                participants, next_cursor = await uow.participants.list_by_user(
                    user_id=user_id,
                    limit=limit,
                    cursor=cursor,
                    include_hidden=True
                )
                
                hidden_participants = [p for p in participants if p.is_hidden]
                
                chats = []
                for p in hidden_participants:
                    chat = await uow.chats.get_by_id(p.chat_id)
                    if chat and chat.type == 'private' and not chat.is_deleted:
                        chat._participant_info = p
                        chats.append(chat)
        else:
            async with ChatUnitOfWork() as uow:
                participants, next_cursor = await uow.participants.list_by_user(
                    user_id=user_id,
                    limit=limit,
                    cursor=cursor,
                    include_hidden=True
                )
                
                hidden_participants = [p for p in participants if p.is_hidden]
                
                chats = []
                for p in hidden_participants:
                    chat = await uow.chats.get_by_id(p.chat_id)
                    if chat and chat.type == 'private' and not chat.is_deleted:
                        chat._participant_info = p
                        chats.append(chat)
        
        logger.info(f"✅ Found {len(chats)} hidden dialogs")
        return chats, next_cursor
    
    async def delete_dialog(self, chat_id: int, user_id: str, delete_for_all: bool = False, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                if chat.type != 'private':
                    raise ValidationError("This operation is only for private chats")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("You are not a member of this chat")
                
                if not delete_for_all:
                    success = await uow.participants.hide_dialog(chat_id, user_id)
                    if not success:
                        raise DatabaseError("Failed to hide dialog")
                    
                    return {
                        'deleted': True,
                        'chat_id': chat_id,
                        'delete_for_all': False,
                        'hidden': True
                    }
                else:
                    now = datetime.utcnow()
                    
                    chat.is_deleted = True
                    chat.deleted_at = now
                    chat.deleted_for_all_by = user_id
                    chat.deleted_for_all_at = now
                    await uow.chats.update(chat)
                    
                    await uow.participants.hide_for_all(chat_id)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type="dialog_deleted_for_all",
                        user_id=user_id,
                        user_role_at_time=participant.role,
                        target_id=None,
                        target_type=None,
                        payload={},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    return {
                        'deleted': True,
                        'chat_id': chat_id,
                        'delete_for_all': True,
                        'deleted_at': now.isoformat(),
                        'deleted_by': user_id
                    }
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                if chat.type != 'private':
                    raise ValidationError("This operation is only for private chats")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("You are not a member of this chat")
                
                if not delete_for_all:
                    success = await uow.participants.hide_dialog(chat_id, user_id)
                    if not success:
                        raise DatabaseError("Failed to hide dialog")
                    
                    return {
                        'deleted': True,
                        'chat_id': chat_id,
                        'delete_for_all': False,
                        'hidden': True
                    }
                else:
                    now = datetime.utcnow()
                    
                    chat.is_deleted = True
                    chat.deleted_at = now
                    chat.deleted_for_all_by = user_id
                    chat.deleted_for_all_at = now
                    await uow.chats.update(chat)
                    
                    await uow.participants.hide_for_all(chat_id)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type="dialog_deleted_for_all",
                        user_id=user_id,
                        user_role_at_time=participant.role,
                        target_id=None,
                        target_type=None,
                        payload={},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    return {
                        'deleted': True,
                        'chat_id': chat_id,
                        'delete_for_all': True,
                        'deleted_at': now.isoformat(),
                        'deleted_by': user_id
                    }
    
    async def create_join_request(self, chat_id: int, user_id: str, invite_code: Optional[str] = None, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                existing = await uow.participants.get(chat_id, user_id)
                if existing:
                    return {'already_member': True, 'chat_id': chat_id, 'role': existing.role}
                
                members = await uow.participants.list_by_chat(chat_id, limit=1)
                if len(members) >= chat.max_members:
                    raise PermissionError("Chat has reached maximum number of members")
                
                if not chat.join_moderation:
                    now = datetime.utcnow()
                    participant = ChatParticipant(
                        chat_id=chat_id,
                        user_id=user_id,
                        role='member',
                        permissions=None,
                        joined_at=now,
                        joined_method='auto',
                        join_event_id=None,
                        is_active=True,
                        left_at=None,
                        left_event_id=None,
                        mute_until=None,
                        is_blocked=False,
                        last_read_at=None,
                        last_read_message_id=None,
                        last_read_message_valid=False,
                        last_active_at=None,
                        unread_count=0,
                        version=1,
                        region=chat_config.DEFAULT_REGION,
                        is_hidden=False
                    )
                    
                    success = await uow.participants.create(participant)
                    if not success:
                        raise DatabaseError("Failed to auto-join chat")
                    
                    await uow.chats.increment_members(chat_id)
                    
                    await cache.delete(f"chat:{chat_id}")
                    await self.participant_cache.invalidate(chat_id, user_id)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type="user_joined",
                        user_id=user_id,
                        user_role_at_time='member',
                        target_id=None,
                        target_type=None,
                        payload={'method': 'auto'},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    return {
                        'auto_joined': True,
                        'chat_id': chat_id,
                        'role': 'member',
                        'joined_at': now.isoformat()
                    }
                
                pending = await uow.join_requests.get_pending(chat_id, user_id)
                if pending:
                    return {
                        'already_pending': True,
                        'request_id': pending.request_id,
                        'created_at': pending.created_at.isoformat()
                    }
                
                now = datetime.utcnow()
                request = JoinRequest(
                    request_id=None,
                    chat_id=chat_id,
                    user_id=user_id,
                    status='pending',
                    created_at=now,
                    invite_code=invite_code
                )
                
                result = await uow.join_requests.create(request)
                if not result:
                    raise DatabaseError("Failed to create join request")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="join_request_created",
                    user_id=user_id,
                    user_role_at_time=None,
                    target_id=result.request_id,
                    target_type='join_request',
                    payload={'invite_code': invite_code},
                    created_at=now,
                    created_date=int(now.strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                # Уведомление админов о новой заявке
                asyncio.create_task(self._notify_join_request_created(
                    chat_id=chat_id,
                    request_id=result.request_id,
                    user_id=user_id,
                    chat_title=chat.title,
                    session=session
                ))
                
                return {
                    'success': True,
                    'request_id': result.request_id,
                    'status': 'pending',
                    'chat_id': chat_id,
                    'created_at': now.isoformat()
                }
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                existing = await uow.participants.get(chat_id, user_id)
                if existing:
                    return {'already_member': True, 'chat_id': chat_id, 'role': existing.role}
                
                members = await uow.participants.list_by_chat(chat_id, limit=1)
                if len(members) >= chat.max_members:
                    raise PermissionError("Chat has reached maximum number of members")
                
                if not chat.join_moderation:
                    now = datetime.utcnow()
                    participant = ChatParticipant(
                        chat_id=chat_id,
                        user_id=user_id,
                        role='member',
                        permissions=None,
                        joined_at=now,
                        joined_method='auto',
                        join_event_id=None,
                        is_active=True,
                        left_at=None,
                        left_event_id=None,
                        mute_until=None,
                        is_blocked=False,
                        last_read_at=None,
                        last_read_message_id=None,
                        last_read_message_valid=False,
                        last_active_at=None,
                        unread_count=0,
                        version=1,
                        region=chat_config.DEFAULT_REGION,
                        is_hidden=False
                    )
                    
                    success = await uow.participants.create(participant)
                    if not success:
                        raise DatabaseError("Failed to auto-join chat")
                    
                    await uow.chats.increment_members(chat_id)
                    
                    await cache.delete(f"chat:{chat_id}")
                    await self.participant_cache.invalidate(chat_id, user_id)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type="user_joined",
                        user_id=user_id,
                        user_role_at_time='member',
                        target_id=None,
                        target_type=None,
                        payload={'method': 'auto'},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    return {
                        'auto_joined': True,
                        'chat_id': chat_id,
                        'role': 'member',
                        'joined_at': now.isoformat()
                    }
                
                pending = await uow.join_requests.get_pending(chat_id, user_id)
                if pending:
                    return {
                        'already_pending': True,
                        'request_id': pending.request_id,
                        'created_at': pending.created_at.isoformat()
                    }
                
                now = datetime.utcnow()
                request = JoinRequest(
                    request_id=None,
                    chat_id=chat_id,
                    user_id=user_id,
                    status='pending',
                    created_at=now,
                    invite_code=invite_code
                )
                
                result = await uow.join_requests.create(request)
                if not result:
                    raise DatabaseError("Failed to create join request")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="join_request_created",
                    user_id=user_id,
                    user_role_at_time=None,
                    target_id=result.request_id,
                    target_type='join_request',
                    payload={'invite_code': invite_code},
                    created_at=now,
                    created_date=int(now.strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                # Уведомление админов о новой заявке
                asyncio.create_task(self._notify_join_request_created(
                    chat_id=chat_id,
                    request_id=result.request_id,
                    user_id=user_id,
                    chat_title=chat.title,
                    session=session
                ))
                
                return {
                    'success': True,
                    'request_id': result.request_id,
                    'status': 'pending',
                    'chat_id': chat_id,
                    'created_at': now.isoformat()
                }
    
    async def _notify_join_request_created(self, chat_id: int, request_id: int, user_id: str,
                                          chat_title: Optional[str], session=None):
        """Уведомить админов о новой заявке (только подписанных на админские уведомления)"""
        try:
            # Получаем админов чата
            async with await ChatUnitOfWork.with_session(session) as uow:
                members = await uow.participants.list_by_chat(chat_id, limit=10000, active_only=True)
                admins = [m.user_id for m in members if m.role in ['owner', 'admin']]
                
                if not admins:
                    return
                
                user_name = user_id[:8]
                
                for admin_id in admins:
                    await WebSocketManager.send_to_user(
                        admin_id,
                        {
                            'type': 'join_request',
                            'data': {
                                'request_id': request_id,
                                'user_id': user_id,
                                'user_name': user_name,
                                'chat_id': chat_id,
                                'chat_title': chat_title or f"Chat {chat_id}"
                            },
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    )
        except Exception as e:
            logger.error(f"Error sending join request notification: {e}")
    
    async def _notify_request_approved(self, chat_id: int, user_id: str, approved_by: str, session=None):
        """Уведомить пользователя о том, что заявка одобрена"""
        try:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                chat_title = chat.title or f"Chat {chat_id}"
                admin_name = approved_by[:8]
                
                await WebSocketManager.send_to_user(
                    user_id,
                    {
                        'type': 'request_approved',
                        'data': {
                            'chat_id': chat_id,
                            'chat_title': chat_title,
                            'approved_by': approved_by,
                            'approved_by_name': admin_name,
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    }
                )
        except Exception as e:
            logger.error(f"Error sending request approved notification: {e}")
    
    async def approve_join_request(self, chat_id: int, request_id: int, admin_id: str, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can approve join requests")
                
                request = await uow.join_requests.get(request_id)
                if not request or request.chat_id != chat_id:
                    raise NotFoundError(f"Join request {request_id} not found")
                
                if request.status != 'pending':
                    raise ValidationError(f"Request already {request.status}")
                
                success = await uow.join_requests.update_status(
                    request_id=request_id,
                    status='approved',
                    reviewed_by=admin_id
                )
                if not success:
                    raise DatabaseError("Failed to update request status")
                
                now = datetime.utcnow()
                participant = ChatParticipant(
                    chat_id=chat_id,
                    user_id=request.user_id,
                    role='member',
                    permissions=None,
                    joined_at=now,
                    joined_method='join_request',
                    join_event_id=None,
                    is_active=True,
                    left_at=None,
                    left_event_id=None,
                    mute_until=None,
                    is_blocked=False,
                    last_read_at=None,
                    last_read_message_id=None,
                    last_read_message_valid=False,
                    last_active_at=None,
                    unread_count=0,
                    version=1,
                    region=chat_config.DEFAULT_REGION
                )
                
                await uow.participants.create(participant)
                await uow.chats.increment_members(chat_id)
                
                await self.participant_cache.invalidate_chat(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="join_request_approved",
                    user_id=admin_id,
                    user_role_at_time=admin.role,
                    target_id=request_id,
                    target_type='join_request',
                    payload={'user_id': request.user_id},
                    created_at=now,
                    created_date=int(now.strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                await self.participant_cache.invalidate(chat_id, request.user_id)
                
                # Уведомление пользователя о том, что заявка одобрена
                asyncio.create_task(self._notify_request_approved(
                    chat_id=chat_id,
                    user_id=request.user_id,
                    approved_by=admin_id,
                    session=session
                ))
                
                return {
                    'approved': True,
                    'request_id': request_id,
                    'user_id': request.user_id,
                    'chat_id': chat_id
                }
        else:
            async with ChatUnitOfWork() as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can approve join requests")
                
                request = await uow.join_requests.get(request_id)
                if not request or request.chat_id != chat_id:
                    raise NotFoundError(f"Join request {request_id} not found")
                
                if request.status != 'pending':
                    raise ValidationError(f"Request already {request.status}")
                
                success = await uow.join_requests.update_status(
                    request_id=request_id,
                    status='approved',
                    reviewed_by=admin_id
                )
                if not success:
                    raise DatabaseError("Failed to update request status")
                
                now = datetime.utcnow()
                participant = ChatParticipant(
                    chat_id=chat_id,
                    user_id=request.user_id,
                    role='member',
                    permissions=None,
                    joined_at=now,
                    joined_method='join_request',
                    join_event_id=None,
                    is_active=True,
                    left_at=None,
                    left_event_id=None,
                    mute_until=None,
                    is_blocked=False,
                    last_read_at=None,
                    last_read_message_id=None,
                    last_read_message_valid=False,
                    last_active_at=None,
                    unread_count=0,
                    version=1,
                    region=chat_config.DEFAULT_REGION
                )
                
                await uow.participants.create(participant)
                await uow.chats.increment_members(chat_id)
                
                await self.participant_cache.invalidate_chat(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="join_request_approved",
                    user_id=admin_id,
                    user_role_at_time=admin.role,
                    target_id=request_id,
                    target_type='join_request',
                    payload={'user_id': request.user_id},
                    created_at=now,
                    created_date=int(now.strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                await self.participant_cache.invalidate(chat_id, request.user_id)
                
                # Уведомление пользователя о том, что заявка одобрена
                asyncio.create_task(self._notify_request_approved(
                    chat_id=chat_id,
                    user_id=request.user_id,
                    approved_by=admin_id,
                    session=session
                ))
                
                return {
                    'approved': True,
                    'request_id': request_id,
                    'user_id': request.user_id,
                    'chat_id': chat_id
                }
    
    async def reject_join_request(self, chat_id: int, request_id: int, admin_id: str, 
                                 reason: Optional[str] = None, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can reject join requests")
                
                request = await uow.join_requests.get(request_id)
                if not request or request.chat_id != chat_id:
                    raise NotFoundError(f"Join request {request_id} not found")
                
                if request.status != 'pending':
                    raise ValidationError(f"Request already {request.status}")
                
                success = await uow.join_requests.update_status(
                    request_id=request_id,
                    status='rejected',
                    reviewed_by=admin_id,
                    reject_reason=reason
                )
                if not success:
                    raise DatabaseError("Failed to update request status")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="join_request_rejected",
                    user_id=admin_id,
                    user_role_at_time=admin.role,
                    target_id=request_id,
                    target_type='join_request',
                    payload={'user_id': request.user_id, 'reason': reason},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can reject join requests")
                
                request = await uow.join_requests.get(request_id)
                if not request or request.chat_id != chat_id:
                    raise NotFoundError(f"Join request {request_id} not found")
                
                if request.status != 'pending':
                    raise ValidationError(f"Request already {request.status}")
                
                success = await uow.join_requests.update_status(
                    request_id=request_id,
                    status='rejected',
                    reviewed_by=admin_id,
                    reject_reason=reason
                )
                if not success:
                    raise DatabaseError("Failed to update request status")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="join_request_rejected",
                    user_id=admin_id,
                    user_role_at_time=admin.role,
                    target_id=request_id,
                    target_type='join_request',
                    payload={'user_id': request.user_id, 'reason': reason},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        return {
            'rejected': True,
            'request_id': request_id,
            'user_id': request.user_id,
            'chat_id': chat_id,
            'reason': reason
        }
    
    async def list_join_requests(self, chat_id: int, admin_id: str, status: Optional[str] = None,
                                limit: int = 50, offset: int = 0, session=None) -> List[JoinRequest]:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can view join requests")
                
                return await uow.join_requests.list_by_chat(
                    chat_id=chat_id,
                    status=status,
                    limit=limit,
                    offset=offset
                )
        else:
            async with ChatUnitOfWork() as uow:
                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can view join requests")
                
                return await uow.join_requests.list_by_chat(
                    chat_id=chat_id,
                    status=status,
                    limit=limit,
                    offset=offset
                )
    
    async def get_my_join_requests(self, user_id: str, limit: int = 50, offset: int = 0, session=None) -> List[JoinRequest]:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                return await uow.join_requests.list_by_user(user_id, limit, offset)
        else:
            async with ChatUnitOfWork() as uow:
                return await uow.join_requests.list_by_user(user_id, limit, offset)
    
    async def enable_reactions(self, channel_id: int, user_id: str, settings: Optional[Dict] = None, session=None) -> Chat:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can enable reactions")
                
                if channel.linked_chat_id:
                    raise ValidationError("Channel already has a linked discussion chat")
                
                channel.reactions_enabled = True
                channel.reactions_settings = settings or {
                    'allowed_reactions': ['👍', '❤️', '😊', '🎉', '😢', '😡', '👎', '🔥', '✅', '⭐'],
                    'max_per_user': 1,
                    'allow_custom': False,
                    'allow_multiple': False
                }
                
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="reactions_enabled",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={'settings': channel.reactions_settings},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can enable reactions")
                
                if channel.linked_chat_id:
                    raise ValidationError("Channel already has a linked discussion chat")
                
                channel.reactions_enabled = True
                channel.reactions_settings = settings or {
                    'allowed_reactions': ['👍', '❤️', '😊', '🎉', '😢', '😡', '👎', '🔥', '✅', '⭐'],
                    'max_per_user': 1,
                    'allow_custom': False,
                    'allow_multiple': False
                }
                
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="reactions_enabled",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={'settings': channel.reactions_settings},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        return channel
    
    async def disable_reactions(self, channel_id: int, user_id: str, session=None) -> Chat:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can disable reactions")
                
                channel.reactions_enabled = False
                channel.reactions_settings = None
                
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="reactions_disabled",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can disable reactions")
                
                channel.reactions_enabled = False
                channel.reactions_settings = None
                
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="reactions_disabled",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        return channel
    
    async def get_reactions_settings(self, channel_id: int, user_id: str, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                has_access = await self.participant_cache.check_access(channel_id, user_id, session=session)
                if not has_access:
                    raise PermissionError("You don't have access to this channel")
        else:
            async with ChatUnitOfWork() as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                has_access = await self.participant_cache.check_access(channel_id, user_id)
                if not has_access:
                    raise PermissionError("You don't have access to this channel")
        
        return {
            'enabled': channel.reactions_enabled,
            'settings': channel.reactions_settings
        }
    
    async def enable_comments(self, channel_id: int, user_id: str, settings: Optional[Dict] = None, session=None) -> Chat:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can enable comments")
                
                if channel.linked_chat_id:
                    raise ValidationError("Channel already has a linked discussion chat")
                
                channel.comments_enabled = True
                channel.comments_settings = settings or {
                    'who_can_comment': 'all',
                    'pre_moderation': False,
                    'allow_replies': True,
                    'max_depth': 3
                }
                
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="comments_enabled",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={'settings': channel.comments_settings},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can enable comments")
                
                if channel.linked_chat_id:
                    raise ValidationError("Channel already has a linked discussion chat")
                
                channel.comments_enabled = True
                channel.comments_settings = settings or {
                    'who_can_comment': 'all',
                    'pre_moderation': False,
                    'allow_replies': True,
                    'max_depth': 3
                }
                
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="comments_enabled",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={'settings': channel.comments_settings},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        return channel
    
    async def disable_comments(self, channel_id: int, user_id: str, session=None) -> Chat:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can disable comments")
                
                channel.comments_enabled = False
                channel.comments_settings = None
                
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="comments_disabled",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can disable comments")
                
                channel.comments_enabled = False
                channel.comments_settings = None
                
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="comments_disabled",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        return channel
    
    async def create_chat(self, chat: Chat, idempotency_key: Optional[str] = None, session=None) -> Chat:
        """Создать чат (группу или канал)"""
        ChatValidator.validate_create(chat.title, chat.type, chat.max_members)
        
        if chat.is_public and not chat.username:
            raise ValidationError("Public chats and channels must have a username")
        
        if chat.username:
            if not re.match(r'^[a-zA-Z0-9_]{5,32}$', chat.username):
                raise ValidationError("Username must be 5-32 characters, letters, numbers, underscore only")
            
            if session:
                async with await ChatUnitOfWork.with_session(session) as check_uow:
                    existing = await check_uow.usernames.check_available(chat.username)
                    if existing:
                        raise ValidationError(f"Username '{chat.username}' is already taken")
            else:
                async with ChatUnitOfWork() as check_uow:
                    existing = await check_uow.usernames.check_available(chat.username)
                    if existing:
                        raise ValidationError(f"Username '{chat.username}' is already taken")
        
        if idempotency_key:
            existing_chat = await self._check_idempotency(idempotency_key, chat.id, session)
            if existing_chat:
                return existing_chat
        
        now = datetime.utcnow()
        chat.id = None
        chat.created_at = now
        chat.updated_at = now
        chat.members_count = 1  # Устанавливаем 1 для владельца
        chat.version = 1
        chat.status = ChatStatus.ACTIVE.value
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                await uow.begin_transaction()
                
                try:
                    result = await uow.chats.create(chat)
                    if not result:
                        raise DatabaseError("Failed to create chat")
                    
                    if chat.username:
                        success = await uow.usernames.reserve(
                            username=chat.username,
                            entity_type=chat.type,
                            entity_id=str(result.id)
                        )
                        if not success:
                            raise DatabaseError(f"Failed to reserve username '{chat.username}'")
                    
                    participant = ChatParticipant(
                        chat_id=result.id,
                        user_id=chat.owner_id,
                        role=ParticipantRole.OWNER.value,
                        joined_at=now,
                        joined_method='create',
                        is_active=True,
                        unread_count=0,
                        version=1,
                        region=chat_config.DEFAULT_REGION
                    )
                    
                    await uow.participants.create(participant)
                    
                    # Увеличиваем счетчик участников для владельца
                    await uow.chats.increment_members(result.id)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=result.id,
                        event_type=EventType.CHAT_CREATED.value,
                        user_id=chat.owner_id,
                        user_role_at_time=ParticipantRole.OWNER.value,
                        payload={'title': chat.title, 'type': chat.type, 'username': chat.username},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=idempotency_key,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    if idempotency_key and result.id:
                        await self._save_idempotency_key(uow, idempotency_key, result.id, chat.owner_id)
                    
                    logger.info(f"Chat created successfully: {result.id} with username: {chat.username}")
                    
                    # WebSocket уведомление
                    asyncio.create_task(
                        self._notify_user_chat_event(
                            user_id=chat.owner_id,
                            event_type='chat_created',
                            data={
                                'chat': result.to_dict(),
                                'timestamp': now.isoformat()
                            },
                            session=session
                        )
                    )
                    
                    return result
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"Error creating chat: {e}")
                    raise
        else:
            async with ChatUnitOfWork() as uow:
                await uow.begin_transaction()
                
                try:
                    result = await uow.chats.create(chat)
                    if not result:
                        raise DatabaseError("Failed to create chat")
                    
                    if chat.username:
                        success = await uow.usernames.reserve(
                            username=chat.username,
                            entity_type=chat.type,
                            entity_id=str(result.id)
                        )
                        if not success:
                            raise DatabaseError(f"Failed to reserve username '{chat.username}'")
                    
                    participant = ChatParticipant(
                        chat_id=result.id,
                        user_id=chat.owner_id,
                        role=ParticipantRole.OWNER.value,
                        joined_at=now,
                        joined_method='create',
                        is_active=True,
                        unread_count=0,
                        version=1,
                        region=chat_config.DEFAULT_REGION
                    )
                    
                    await uow.participants.create(participant)
                    
                    # Увеличиваем счетчик участников для владельца
                    await uow.chats.increment_members(result.id)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=result.id,
                        event_type=EventType.CHAT_CREATED.value,
                        user_id=chat.owner_id,
                        user_role_at_time=ParticipantRole.OWNER.value,
                        payload={'title': chat.title, 'type': chat.type, 'username': chat.username},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=idempotency_key,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    if idempotency_key and result.id:
                        await self._save_idempotency_key(uow, idempotency_key, result.id, chat.owner_id)
                    
                    logger.info(f"Chat created successfully: {result.id} with username: {chat.username}")
                    
                    asyncio.create_task(
                        self._notify_user_chat_event(
                            user_id=chat.owner_id,
                            event_type='chat_created',
                            data={
                                'chat': result.to_dict(),
                                'timestamp': now.isoformat()
                            }
                        )
                    )
                    
                    return result
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"Error creating chat: {e}")
                    raise
    
    async def get_chat(self, chat_id: int, user_id: str, session=None) -> Chat:
        """Получить чат по ID с информацией об участнике"""
        logger.info(f"🚀 ENTERING get_chat for chat {chat_id}, user {user_id}")
        
        if session:
            logger.info(f"📦 Using provided session for chat {chat_id}")
            async with await ChatUnitOfWork.with_session(session) as uow:
                logger.info(f"📦 Getting chat from DB for id {chat_id}")
                chat = await uow.chats.get_by_id(chat_id)
                logger.info(f"📦 Chat from DB result: {chat.id if chat else 'None'}")
                
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                logger.info(f"📦 Chat.is_public: {chat.is_public}")
                
                # 👇 ВСЕГДА проверяем, является ли пользователь участником
                logger.info(f"🔍 Getting participant for user {user_id}")
                participant = await uow.participants.get(chat_id, user_id)
                logger.info(f"🔍 Participant result: {participant}")
                
                if participant:
                    logger.info(f"✅ Setting participant info for chat {chat_id}, role={participant.role}")
                    chat._participant_info = participant
                else:
                    logger.info(f"ℹ️ User {user_id} is not a participant of chat {chat_id}")
                    
                    # Для публичных чатов это нормально, для приватных - ошибка доступа
                    if not chat.is_public:
                        raise PermissionError("You don't have access to this chat")
                
                return chat
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                participant = await uow.participants.get(chat_id, user_id)
                if participant:
                    chat._participant_info = participant
                else:
                    if not chat.is_public:
                        raise PermissionError("You don't have access to this chat")
                return chat

    async def update_chat(self, chat_id: int, user_id: str, updates: Dict, session=None) -> Chat:
        """Обновить чат с WebSocket уведомлением"""
        ChatValidator.validate_update(updates)
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("You don't have permission to update this chat")
                
                old_chat_dict = chat.to_dict()
                
                for key, value in updates.items():
                    if hasattr(chat, key):
                        setattr(chat, key, value)
                
                chat.updated_at = datetime.utcnow()
                
                success = await uow.chats.update(chat)
                if not success:
                    raise DatabaseError("Failed to update chat")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.CHAT_UPDATED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={'updates': updates},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("You don't have permission to update this chat")
                
                old_chat_dict = chat.to_dict()
                
                for key, value in updates.items():
                    if hasattr(chat, key):
                        setattr(chat, key, value)
                
                chat.updated_at = datetime.utcnow()
                
                success = await uow.chats.update(chat)
                if not success:
                    raise DatabaseError("Failed to update chat")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.CHAT_UPDATED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={'updates': updates},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='chat_updated',
                data={
                    'chat_id': chat_id,
                    'updates': updates,
                    'old_chat': old_chat_dict,
                    'updated_by': user_id,
                    'timestamp': datetime.utcnow().isoformat()
                },
                exclude_user_id=user_id,
                session=session
            )
        )
        
        return chat
    
    async def delete_chat(self, chat_id: int, user_id: str, permanent: bool = False, session=None) -> Dict:
        """Удалить чат с WebSocket уведомлением"""
        logger.info(f"🗑️ Deleting chat {chat_id} by user {user_id}, permanent={permanent}")
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                await uow.begin_transaction()
                try:
                    chat = await uow.chats.get_by_id(chat_id)
                    if not chat:
                        raise NotFoundError(f"Chat {chat_id} not found")
                    
                    participant = await uow.participants.get(chat_id, user_id)
                    if not participant or participant.role != ParticipantRole.OWNER.value:
                        raise PermissionError("Only chat owner can delete the chat")
                    
                    if chat.username:
                        await uow.usernames.release(chat.username)
                        logger.info(f"Released username '{chat.username}'")
                    
                    await uow.chats.delete(chat_id, permanent)
                    
                    if permanent:
                        members = await uow.participants.list_by_chat(chat_id, limit=10000)
                        for member in members:
                            await uow.participants.delete(chat_id, member.user_id)
                    else:
                        members = await uow.participants.list_by_chat(chat_id, limit=10000)
                        for member in members:
                            member.is_active = False
                            member.left_at = datetime.utcnow()
                            await uow.participants.update(member)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type=EventType.CHAT_DELETED.value,
                        user_id=user_id,
                        user_role_at_time=participant.role,
                        target_id=None,
                        target_type=None,
                        payload={'permanent': permanent},
                        created_at=datetime.utcnow(),
                        created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    await uow.commit()
                    
                    await self.participant_cache.invalidate_chat(chat_id)
                    
                    logger.info(f"✅ Chat {chat_id} deleted successfully")
                    
                    # WEBSOCKET УВЕДОМЛЕНИЕ
                    asyncio.create_task(
                        self._notify_chat_event(
                            chat_id=chat_id,
                            event_type='chat_deleted',
                            data={
                                'chat_id': chat_id,
                                'deleted_by': user_id,
                                'permanent': permanent,
                                'timestamp': datetime.utcnow().isoformat()
                            },
                            session=session
                        )
                    )
                    
                    return {
                        'deleted': True,
                        'permanent': permanent,
                        'chat_id': chat_id,
                        'username_freed': chat.username is not None
                    }
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"❌ Failed to delete chat: {e}")
                    raise
        else:
            async with ChatUnitOfWork() as uow:
                await uow.begin_transaction()
                try:
                    chat = await uow.chats.get_by_id(chat_id)
                    if not chat:
                        raise NotFoundError(f"Chat {chat_id} not found")
                    
                    participant = await uow.participants.get(chat_id, user_id)
                    if not participant or participant.role != ParticipantRole.OWNER.value:
                        raise PermissionError("Only chat owner can delete the chat")
                    
                    if chat.username:
                        await uow.usernames.release(chat.username)
                        logger.info(f"Released username '{chat.username}'")
                    
                    await uow.chats.delete(chat_id, permanent)
                    
                    if permanent:
                        members = await uow.participants.list_by_chat(chat_id, limit=10000)
                        for member in members:
                            await uow.participants.delete(chat_id, member.user_id)
                    else:
                        members = await uow.participants.list_by_chat(chat_id, limit=10000)
                        for member in members:
                            member.is_active = False
                            member.left_at = datetime.utcnow()
                            await uow.participants.update(member)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type=EventType.CHAT_DELETED.value,
                        user_id=user_id,
                        user_role_at_time=participant.role,
                        target_id=None,
                        target_type=None,
                        payload={'permanent': permanent},
                        created_at=datetime.utcnow(),
                        created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    await uow.commit()
                    
                    await self.participant_cache.invalidate_chat(chat_id)
                    
                    logger.info(f"✅ Chat {chat_id} deleted successfully")
                    
                    # WEBSOCKET УВЕДОМЛЕНИЕ
                    asyncio.create_task(
                        self._notify_chat_event(
                            chat_id=chat_id,
                            event_type='chat_deleted',
                            data={
                                'chat_id': chat_id,
                                'deleted_by': user_id,
                                'permanent': permanent,
                                'timestamp': datetime.utcnow().isoformat()
                            }
                        )
                    )
                    
                    return {
                        'deleted': True,
                        'permanent': permanent,
                        'chat_id': chat_id,
                        'username_freed': chat.username is not None
                    }
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"❌ Failed to delete chat: {e}")
                    raise
    
    async def link_discussion_chat(self, channel_id: int, discussion_chat_id: int, user_id: str, 
                                  settings: Optional[Dict] = None, session=None) -> Chat:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can link discussion chat")
                
                discussion_chat = await uow.chats.get_by_id(discussion_chat_id)
                if not discussion_chat:
                    raise NotFoundError(f"Discussion chat {discussion_chat_id} not found")
                
                if discussion_chat.type not in ['group', 'supergroup']:
                    raise ValidationError("Discussion chat must be a group")
                
                discussion_participant = await uow.participants.get(discussion_chat_id, user_id)
                if not discussion_participant:
                    raise PermissionError("You must be a member of the discussion chat")
                
                if discussion_chat.is_discussion:
                    raise ValidationError("This chat is already linked to another channel")
                
                channel.linked_chat_id = discussion_chat_id
                discussion_chat.is_discussion = True
                discussion_chat.discussion_settings = settings or {
                    'auto_post': True,
                    'allow_comments': True,
                    'pre_moderation': False,
                    'who_can_comment': 'all',
                    'show_preview': True
                }
                
                await uow.chats.update(channel)
                await uow.chats.update(discussion_chat)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="discussion_linked",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=discussion_chat_id,
                    target_type="chat",
                    payload={'discussion_chat_id': discussion_chat_id, 'settings': discussion_chat.discussion_settings},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can link discussion chat")
                
                discussion_chat = await uow.chats.get_by_id(discussion_chat_id)
                if not discussion_chat:
                    raise NotFoundError(f"Discussion chat {discussion_chat_id} not found")
                
                if discussion_chat.type not in ['group', 'supergroup']:
                    raise ValidationError("Discussion chat must be a group")
                
                discussion_participant = await uow.participants.get(discussion_chat_id, user_id)
                if not discussion_participant:
                    raise PermissionError("You must be a member of the discussion chat")
                
                if discussion_chat.is_discussion:
                    raise ValidationError("This chat is already linked to another channel")
                
                channel.linked_chat_id = discussion_chat_id
                discussion_chat.is_discussion = True
                discussion_chat.discussion_settings = settings or {
                    'auto_post': True,
                    'allow_comments': True,
                    'pre_moderation': False,
                    'who_can_comment': 'all',
                    'show_preview': True
                }
                
                await uow.chats.update(channel)
                await uow.chats.update(discussion_chat)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="discussion_linked",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=discussion_chat_id,
                    target_type="chat",
                    payload={'discussion_chat_id': discussion_chat_id, 'settings': discussion_chat.discussion_settings},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        return channel
    
    async def unlink_discussion_chat(self, channel_id: int, user_id: str, session=None) -> Chat:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can unlink discussion chat")
                
                if not channel.linked_chat_id:
                    raise ValidationError("Channel has no linked discussion chat")
                
                discussion_chat = await uow.chats.get_by_id(channel.linked_chat_id)
                if discussion_chat:
                    discussion_chat.is_discussion = False
                    discussion_chat.discussion_settings = None
                    await uow.chats.update(discussion_chat)
                
                old_linked_chat_id = channel.linked_chat_id
                channel.linked_chat_id = None
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="discussion_unlinked",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=old_linked_chat_id,
                    target_type="chat",
                    payload={'previous_discussion_chat_id': old_linked_chat_id},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can unlink discussion chat")
                
                if not channel.linked_chat_id:
                    raise ValidationError("Channel has no linked discussion chat")
                
                discussion_chat = await uow.chats.get_by_id(channel.linked_chat_id)
                if discussion_chat:
                    discussion_chat.is_discussion = False
                    discussion_chat.discussion_settings = None
                    await uow.chats.update(discussion_chat)
                
                old_linked_chat_id = channel.linked_chat_id
                channel.linked_chat_id = None
                await uow.chats.update(channel)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="discussion_unlinked",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=old_linked_chat_id,
                    target_type="chat",
                    payload={'previous_discussion_chat_id': old_linked_chat_id},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        return channel
    
    async def create_discussion_post(self, channel_id: int, message_id: int, user_id: str, session=None) -> Optional['Message']:
        from handlers.message_handler import Message, MessageUnitOfWork as MessageUOW
        
        if session:
            async with await MessageUOW.with_session(session) as uow:
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
        else:
            async with MessageUOW() as uow:
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
    
    async def get_discussion_chat(self, channel_id: int, user_id: str, session=None) -> Optional[Chat]:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                has_access = await self.participant_cache.check_access(channel_id, user_id, session=session)
                if not has_access:
                    raise PermissionError("You don't have access to this channel")
                
                if not channel.linked_chat_id:
                    return None
                
                return await uow.chats.get_by_id(channel.linked_chat_id)
        else:
            async with ChatUnitOfWork() as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                has_access = await self.participant_cache.check_access(channel_id, user_id)
                if not has_access:
                    raise PermissionError("You don't have access to this channel")
                
                if not channel.linked_chat_id:
                    return None
                
                return await uow.chats.get_by_id(channel.linked_chat_id)
    
    async def update_discussion_settings(self, channel_id: int, user_id: str, settings: Dict, session=None) -> Chat:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can update discussion settings")
                
                if not channel.linked_chat_id:
                    raise ValidationError("Channel has no linked discussion chat")
                
                discussion_chat = await uow.chats.get_by_id(channel.linked_chat_id)
                if not discussion_chat:
                    raise NotFoundError("Linked discussion chat not found")
                
                current_settings = discussion_chat.discussion_settings or {}
                current_settings.update(settings)
                discussion_chat.discussion_settings = current_settings
                
                await uow.chats.update(discussion_chat)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="discussion_settings_updated",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=channel.linked_chat_id,
                    target_type="chat",
                    payload={'settings': settings},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                channel = await uow.chats.get_by_id(channel_id)
                if not channel:
                    raise NotFoundError(f"Channel {channel_id} not found")
                
                if channel.type != 'channel':
                    raise ValidationError(f"Chat {channel_id} is not a channel")
                
                participant = await uow.participants.get(channel_id, user_id)
                if not participant or participant.role not in ['owner', 'admin']:
                    raise PermissionError("Only owner and admin can update discussion settings")
                
                if not channel.linked_chat_id:
                    raise ValidationError("Channel has no linked discussion chat")
                
                discussion_chat = await uow.chats.get_by_id(channel.linked_chat_id)
                if not discussion_chat:
                    raise NotFoundError("Linked discussion chat not found")
                
                current_settings = discussion_chat.discussion_settings or {}
                current_settings.update(settings)
                discussion_chat.discussion_settings = current_settings
                
                await uow.chats.update(discussion_chat)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=channel_id,
                    event_type="discussion_settings_updated",
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=channel.linked_chat_id,
                    target_type="chat",
                    payload={'settings': settings},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        return discussion_chat
    
    async def archive_chat(self, chat_id: int, user_id: str, session=None) -> Chat:
        """Архивировать чат с WebSocket уведомлением"""
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("You don't have permission to archive this chat")
                
                chat.is_archived = True
                chat.status = ChatStatus.ARCHIVED.value
                chat.updated_at = datetime.utcnow()
                
                success = await uow.chats.update(chat)
                if not success:
                    raise DatabaseError("Failed to archive chat")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.CHAT_ARCHIVED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("You don't have permission to archive this chat")
                
                chat.is_archived = True
                chat.status = ChatStatus.ARCHIVED.value
                chat.updated_at = datetime.utcnow()
                
                success = await uow.chats.update(chat)
                if not success:
                    raise DatabaseError("Failed to archive chat")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.CHAT_ARCHIVED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='chat_archived',
                data={
                    'chat_id': chat_id,
                    'archived_by': user_id,
                    'timestamp': datetime.utcnow().isoformat()
                },
                session=session
            )
        )
        
        return chat
    
    async def unarchive_chat(self, chat_id: int, user_id: str, session=None) -> Chat:
        """Разархивировать чат с WebSocket уведомлением"""
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("You don't have permission to unarchive this chat")
                
                chat.is_archived = False
                chat.status = ChatStatus.ACTIVE.value
                chat.updated_at = datetime.utcnow()
                
                success = await uow.chats.update(chat)
                if not success:
                    raise DatabaseError("Failed to unarchive chat")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.CHAT_UNARCHIVED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("You don't have permission to unarchive this chat")
                
                chat.is_archived = False
                chat.status = ChatStatus.ACTIVE.value
                chat.updated_at = datetime.utcnow()
                
                success = await uow.chats.update(chat)
                if not success:
                    raise DatabaseError("Failed to unarchive chat")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.CHAT_UNARCHIVED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='chat_unarchived',
                data={
                    'chat_id': chat_id,
                    'unarchived_by': user_id,
                    'timestamp': datetime.utcnow().isoformat()
                },
                session=session
            )
        )
        
        return chat
    
    async def admin_add_member(self, chat_id: int, admin_id: str, target_user_id: str, session=None):
        """Добавить участника в чат от имени владельца/администратора"""
        async with ChatUnitOfWork() as uow:
            await uow.begin_transaction()
            try:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")

                admin = await uow.participants.get(chat_id, admin_id)
                if not admin or admin.role not in ('owner', 'admin'):
                    raise PermissionError("Only owner or admin can add members")

                existing = await uow.participants.get(chat_id, target_user_id)
                if existing:
                    return existing

                if chat.members_count >= chat.max_members:
                    raise PermissionError("Chat has reached maximum number of members")

                now = datetime.utcnow()
                participant = ChatParticipant(
                    chat_id=chat_id,
                    user_id=target_user_id,
                    role=ParticipantRole.MEMBER.value,
                    joined_at=now,
                    joined_method='added',
                    is_active=True,
                    unread_count=0,
                    version=1,
                    region=chat_config.DEFAULT_REGION,
                    show_in_profile=True
                )
                await uow.participants.create(participant)
                await uow.chats.increment_members(chat_id)

                event_obj = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.USER_JOINED.value,
                    user_id=target_user_id,
                    user_role_at_time=ParticipantRole.MEMBER.value,
                    payload={'method': 'added', 'added_by': admin_id},
                    created_at=now,
                    created_date=int(now.strftime('%Y%m%d')),
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event_obj)
                await uow.commit()

                await self.participant_cache.invalidate_chat(chat_id)
                await self.participant_cache.invalidate(chat_id, target_user_id)

                logger.info(f"✅ User {target_user_id[:8]} added to chat {chat_id} by {admin_id[:8]}")
                return participant
            except Exception as e:
                await uow.rollback()
                logger.error(f"❌ Failed to add member: {e}")
                raise

    async def join_chat(self, chat_id: int, user_id: str, invite_code: Optional[str] = None,
                        idempotency_key: Optional[str] = None, session=None) -> ChatParticipant:
        """Присоединиться к чату"""
        logger.info(f"🚪 User {user_id} joining chat {chat_id}")
        
        if idempotency_key:
            if session:
                async with await ChatUnitOfWork.with_session(session) as uow:
                    existing_key = await uow.idempotency.get(idempotency_key)
                    if existing_key:
                        existing = await uow.participants.get(chat_id, user_id)
                        if existing:
                            return existing
            else:
                async with ChatUnitOfWork() as uow:
                    existing_key = await uow.idempotency.get(idempotency_key)
                    if existing_key:
                        existing = await uow.participants.get(chat_id, user_id)
                        if existing:
                            return existing
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                await uow.begin_transaction()
                try:
                    chat = await uow.chats.get_by_id(chat_id)
                    if not chat:
                        raise NotFoundError(f"Chat {chat_id} not found")
                    
                    if chat.status != ChatStatus.ACTIVE.value:
                        raise PermissionError("Chat is not active")
                    
                    existing = await uow.participants.get(chat_id, user_id)
                    if existing:
                        return existing
                    
                    # Проверяем лимит участников
                    if chat.members_count >= chat.max_members:
                        raise PermissionError("Chat has reached maximum number of members")
                    
                    default_role = ParticipantRole.MEMBER.value
                    
                    # Проверка инвайта для приватных чатов
                    if not chat.is_public:
                        if not invite_code:
                            raise PermissionError("This chat is private. Invite code required.")
                        
                        invite = await uow.invites.get_by_code(invite_code)
                        if not invite or invite.chat_id != chat_id:
                            raise PermissionError("Invalid invite code")
                        
                        if not invite.is_active:
                            raise PermissionError("Invite code is no longer active")
                        
                        if invite.expires_at and invite.expires_at < datetime.utcnow():
                            raise PermissionError("Invite code has expired")
                        
                        if invite.max_uses > 0 and invite.used_count >= invite.max_uses:
                            raise PermissionError("Invite code has reached maximum uses")
                        
                        await uow.invites.increment_used(invite.invite_id)
                        default_role = invite.default_role
                    
                    now = datetime.utcnow()
                    participant = ChatParticipant(
                        chat_id=chat_id,
                        user_id=user_id,
                        role=default_role,
                        joined_at=now,
                        joined_method='invite' if invite_code else 'join',
                        is_active=True,
                        unread_count=0,
                        version=1,
                        region=chat_config.DEFAULT_REGION,
                        show_in_profile=True
                    )
                    
                    await uow.participants.create(participant)
                    
                    # Увеличиваем счетчик участников
                    await uow.chats.increment_members(chat_id)
                    
                    if idempotency_key:
                        key_obj = IdempotencyKey(
                            idempotency_key=idempotency_key,
                            entity_type="join",
                            entity_id=chat_id % (2**64),
                            chat_id=chat_id,
                            user_id=user_id,
                            created_at=now,
                            expires_at=now + timedelta(hours=24)
                        )
                        await uow.idempotency.create(key_obj)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type=EventType.USER_JOINED.value,
                        user_id=user_id,
                        user_role_at_time=default_role,
                        payload={'method': participant.joined_method, 'invite_code': invite_code},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=idempotency_key,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    await uow.commit()
                    
                    await self.participant_cache.invalidate_chat(chat_id)
                    await self.participant_cache.invalidate(chat_id, user_id)
                    
                    logger.info(f"✅ User {user_id} joined chat {chat_id}")
                    
                    return participant
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"❌ Failed to join chat: {e}")
                    raise
        else:
            # Аналогично для случая без session
            async with ChatUnitOfWork() as uow:
                await uow.begin_transaction()
                try:
                    chat = await uow.chats.get_by_id(chat_id)
                    if not chat:
                        raise NotFoundError(f"Chat {chat_id} not found")
                    
                    if chat.status != ChatStatus.ACTIVE.value:
                        raise PermissionError("Chat is not active")
                    
                    existing = await uow.participants.get(chat_id, user_id)
                    if existing:
                        return existing
                    
                    if chat.members_count >= chat.max_members:
                        raise PermissionError("Chat has reached maximum number of members")
                    
                    default_role = ParticipantRole.MEMBER.value
                    
                    if not chat.is_public:
                        if not invite_code:
                            raise PermissionError("This chat is private. Invite code required.")
                        
                        invite = await uow.invites.get_by_code(invite_code)
                        if not invite or invite.chat_id != chat_id:
                            raise PermissionError("Invalid invite code")
                        
                        if not invite.is_active:
                            raise PermissionError("Invite code is no longer active")
                        
                        if invite.expires_at and invite.expires_at < datetime.utcnow():
                            raise PermissionError("Invite code has expired")
                        
                        if invite.max_uses > 0 and invite.used_count >= invite.max_uses:
                            raise PermissionError("Invite code has reached maximum uses")
                        
                        await uow.invites.increment_used(invite.invite_id)
                        default_role = invite.default_role
                    
                    now = datetime.utcnow()
                    participant = ChatParticipant(
                        chat_id=chat_id,
                        user_id=user_id,
                        role=default_role,
                        joined_at=now,
                        joined_method='invite' if invite_code else 'join',
                        is_active=True,
                        unread_count=0,
                        version=1,
                        region=chat_config.DEFAULT_REGION,
                        show_in_profile=True
                    )
                    
                    await uow.participants.create(participant)
                    
                    # Увеличиваем счетчик участников
                    await uow.chats.increment_members(chat_id)
                    
                    if idempotency_key:
                        key_obj = IdempotencyKey(
                            idempotency_key=idempotency_key,
                            entity_type="join",
                            entity_id=chat_id % (2**64),
                            chat_id=chat_id,
                            user_id=user_id,
                            created_at=now,
                            expires_at=now + timedelta(hours=24)
                        )
                        await uow.idempotency.create(key_obj)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type=EventType.USER_JOINED.value,
                        user_id=user_id,
                        user_role_at_time=default_role,
                        payload={'method': participant.joined_method, 'invite_code': invite_code},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=idempotency_key,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    await uow.commit()
                    
                    await self.participant_cache.invalidate_chat(chat_id)
                    await self.participant_cache.invalidate(chat_id, user_id)
                    
                    logger.info(f"✅ User {user_id} joined chat {chat_id}")
                    
                    return participant
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"❌ Failed to join chat: {e}")
                    raise
    
    async def leave_chat(self, chat_id: int, user_id: str, session=None) -> Dict:
        """Покинуть чат с WebSocket уведомлением"""
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise ValidationError("You are not a member of this chat")
                
                if participant.role == ParticipantRole.OWNER.value:
                    members = await uow.participants.list_by_chat(chat_id, limit=2)
                    if len(members) > 1:
                        raise PermissionError("You are the owner of this chat. Transfer ownership before leaving.")
                    else:
                        return await self.delete_chat(chat_id, user_id, permanent=False, session=session)
                
                success = await uow.participants.delete(chat_id, user_id)
                if not success:
                    raise DatabaseError("Failed to leave chat")
                
                await uow.chats.decrement_members(chat_id)
                
                await self.participant_cache.invalidate_chat(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.USER_LEFT.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                await self.participant_cache.invalidate(chat_id, user_id)
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise ValidationError("You are not a member of this chat")
                
                if participant.role == ParticipantRole.OWNER.value:
                    members = await uow.participants.list_by_chat(chat_id, limit=2)
                    if len(members) > 1:
                        raise PermissionError("You are the owner of this chat. Transfer ownership before leaving.")
                    else:
                        return await self.delete_chat(chat_id, user_id, permanent=False)
                
                success = await uow.participants.delete(chat_id, user_id)
                if not success:
                    raise DatabaseError("Failed to leave chat")
                
                await uow.chats.decrement_members(chat_id)
                
                await self.participant_cache.invalidate_chat(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.USER_LEFT.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=None,
                    target_type=None,
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                await self.participant_cache.invalidate(chat_id, user_id)
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='user_left',
                data={
                    'chat_id': chat_id,
                    'user_id': user_id,
                    'timestamp': datetime.utcnow().isoformat()
                },
                session=session
            )
        )
        
        return {'left': True}
    
    async def change_role(self, chat_id: int, user_id: str, target_user_id: str, new_role: str, session=None) -> Dict:
        """Изменить роль с WebSocket уведомлением"""
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                current = await uow.participants.get(chat_id, user_id)
                if not current:
                    raise PermissionError("You are not a member of this chat")
                
                target = await uow.participants.get(chat_id, target_user_id)
                if not target:
                    raise NotFoundError(f"User {target_user_id} is not a member")
                
                if target_user_id == user_id and current.role == ParticipantRole.OWNER.value:
                    raise PermissionError("Owner cannot change their own role. Transfer ownership first.")
                
                ParticipantValidator.validate_role_change(current.role, target.role, new_role)
                
                old_role = target.role
                target.role = new_role
                success = await uow.participants.update(target)
                if not success:
                    raise DatabaseError("Failed to update role")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.ROLE_CHANGED.value,
                    user_id=user_id,
                    user_role_at_time=current.role,
                    target_id=None,
                    target_type=None,
                    payload={
                        'target_user_id': target_user_id,
                        'old_role': old_role,
                        'new_role': new_role
                    },
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                await self.participant_cache.invalidate(chat_id, target_user_id)
        else:
            async with ChatUnitOfWork() as uow:
                current = await uow.participants.get(chat_id, user_id)
                if not current:
                    raise PermissionError("You are not a member of this chat")
                
                target = await uow.participants.get(chat_id, target_user_id)
                if not target:
                    raise NotFoundError(f"User {target_user_id} is not a member")
                
                if target_user_id == user_id and current.role == ParticipantRole.OWNER.value:
                    raise PermissionError("Owner cannot change their own role. Transfer ownership first.")
                
                ParticipantValidator.validate_role_change(current.role, target.role, new_role)
                
                old_role = target.role
                target.role = new_role
                success = await uow.participants.update(target)
                if not success:
                    raise DatabaseError("Failed to update role")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.ROLE_CHANGED.value,
                    user_id=user_id,
                    user_role_at_time=current.role,
                    target_id=None,
                    target_type=None,
                    payload={
                        'target_user_id': target_user_id,
                        'old_role': old_role,
                        'new_role': new_role
                    },
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                await self.participant_cache.invalidate(chat_id, target_user_id)
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='role_changed',
                data={
                    'chat_id': chat_id,
                    'target_user_id': target_user_id,
                    'old_role': old_role,
                    'new_role': new_role,
                    'changed_by': user_id,
                    'timestamp': datetime.utcnow().isoformat()
                },
                session=session
            )
        )
        
        return {
            'user_id': target_user_id,
            'old_role': old_role,
            'new_role': new_role,
            'changed_by': user_id
        }
    
    async def join_by_link(self, invite_code: str, user_id: str, idempotency_key: Optional[str] = None, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                invite = await uow.invites.get_by_code(invite_code)
                if not invite:
                    raise NotFoundError("Invite not found")
                
                if not invite.is_active:
                    raise PermissionError("Invite is no longer active")
                
                if invite.expires_at and invite.expires_at < datetime.utcnow():
                    raise PermissionError("Invite has expired")
                
                if invite.max_uses > 0 and invite.used_count >= invite.max_uses:
                    raise PermissionError("Invite has reached maximum uses")
                
                chat = await uow.chats.get_by_id(invite.chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {invite.chat_id} not found")
                
                if chat.status != ChatStatus.ACTIVE.value:
                    raise PermissionError("Chat is not active")
                
                existing = await uow.participants.get(invite.chat_id, user_id)
                if existing:
                    return {
                        'already_member': True,
                        'chat_id': str(invite.chat_id),
                        'chat_title': chat.title
                    }
                
                members = await uow.participants.list_by_chat(invite.chat_id, limit=1)
                if len(members) >= chat.max_members:
                    raise PermissionError("Chat has reached maximum number of members")
                
                now = datetime.utcnow()
                participant = ChatParticipant(
                    chat_id=invite.chat_id,
                    user_id=user_id,
                    role=invite.default_role,
                    permissions=None,
                    joined_at=now,
                    joined_method='invite_link',
                    join_event_id=None,
                    is_active=True,
                    left_at=None,
                    left_event_id=None,
                    mute_until=None,
                    is_blocked=False,
                    last_read_at=None,
                    last_read_message_id=None,
                    last_read_message_valid=False,
                    last_active_at=None,
                    unread_count=0,
                    version=1,
                    region=chat_config.DEFAULT_REGION
                )
                
                success = await uow.participants.create(participant)
                if not success:
                    raise DatabaseError("Failed to join chat")
                
                await uow.chats.increment_members(invite.chat_id)
                await uow.invites.increment_used(invite.invite_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=invite.chat_id,
                    event_type=EventType.USER_JOINED.value,
                    user_id=user_id,
                    user_role_at_time=invite.default_role,
                    target_id=None,
                    target_type=None,
                    payload={'method': 'invite_link', 'invite_code': invite_code},
                    created_at=now,
                    created_date=int(now.strftime('%Y%m%d')),
                    idempotency_key=idempotency_key,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                await self.participant_cache.invalidate(invite.chat_id, user_id)
        else:
            async with ChatUnitOfWork() as uow:
                invite = await uow.invites.get_by_code(invite_code)
                if not invite:
                    raise NotFoundError("Invite not found")
                
                if not invite.is_active:
                    raise PermissionError("Invite is no longer active")
                
                if invite.expires_at and invite.expires_at < datetime.utcnow():
                    raise PermissionError("Invite has expired")
                
                if invite.max_uses > 0 and invite.used_count >= invite.max_uses:
                    raise PermissionError("Invite has reached maximum uses")
                
                chat = await uow.chats.get_by_id(invite.chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {invite.chat_id} not found")
                
                if chat.status != ChatStatus.ACTIVE.value:
                    raise PermissionError("Chat is not active")
                
                existing = await uow.participants.get(invite.chat_id, user_id)
                if existing:
                    return {
                        'already_member': True,
                        'chat_id': str(invite.chat_id),
                        'chat_title': chat.title
                    }
                
                members = await uow.participants.list_by_chat(invite.chat_id, limit=1)
                if len(members) >= chat.max_members:
                    raise PermissionError("Chat has reached maximum number of members")
                
                now = datetime.utcnow()
                participant = ChatParticipant(
                    chat_id=invite.chat_id,
                    user_id=user_id,
                    role=invite.default_role,
                    permissions=None,
                    joined_at=now,
                    joined_method='invite_link',
                    join_event_id=None,
                    is_active=True,
                    left_at=None,
                    left_event_id=None,
                    mute_until=None,
                    is_blocked=False,
                    last_read_at=None,
                    last_read_message_id=None,
                    last_read_message_valid=False,
                    last_active_at=None,
                    unread_count=0,
                    version=1,
                    region=chat_config.DEFAULT_REGION
                )
                
                success = await uow.participants.create(participant)
                if not success:
                    raise DatabaseError("Failed to join chat")
                
                await uow.chats.increment_members(invite.chat_id)
                await uow.invites.increment_used(invite.invite_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=invite.chat_id,
                    event_type=EventType.USER_JOINED.value,
                    user_id=user_id,
                    user_role_at_time=invite.default_role,
                    target_id=None,
                    target_type=None,
                    payload={'method': 'invite_link', 'invite_code': invite_code},
                    created_at=now,
                    created_date=int(now.strftime('%Y%m%d')),
                    idempotency_key=idempotency_key,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
                
                await self.participant_cache.invalidate(invite.chat_id, user_id)
        
        return {
            'success': True,
            'chat_id': str(invite.chat_id),
            'chat_title': chat.title,
            'role': invite.default_role
        }
    
    async def transfer_ownership(self, chat_id: int, user_id: str, new_owner_id: str, session=None) -> Dict:
        """Передать права с WebSocket уведомлением"""
        logger.info(f"👑 Transferring ownership from {user_id} to {new_owner_id} in chat {chat_id}")
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                await uow.begin_transaction()
                try:
                    chat = await uow.chats.get_by_id(chat_id)
                    if not chat:
                        raise NotFoundError(f"Chat {chat_id} not found")
                    
                    current = await uow.participants.get(chat_id, user_id)
                    if not current or current.role != ParticipantRole.OWNER.value:
                        raise PermissionError("Only owner can transfer ownership")
                    
                    new_owner = await uow.participants.get(chat_id, new_owner_id)
                    if not new_owner:
                        raise NotFoundError(f"User {new_owner_id} is not a member")
                    
                    old_role = current.role
                    current.role = ParticipantRole.ADMIN.value
                    await uow.participants.update(current)
                    
                    new_owner.role = ParticipantRole.OWNER.value
                    await uow.participants.update(new_owner)
                    
                    chat.owner_id = new_owner_id
                    chat.updated_at = datetime.utcnow()
                    await uow.chats.update(chat)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type=EventType.OWNERSHIP_TRANSFERRED.value,
                        user_id=user_id,
                        user_role_at_time=current.role,
                        target_id=None,
                        target_type=None,
                        payload={'old_owner': user_id, 'new_owner': new_owner_id},
                        created_at=datetime.utcnow(),
                        created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    await uow.commit()
                    
                    await self.participant_cache.invalidate(chat_id, user_id)
                    await self.participant_cache.invalidate(chat_id, new_owner_id)
                    
                    logger.info(f"✅ Ownership transferred from {user_id} to {new_owner_id}")
                    
                    # WEBSOCKET УВЕДОМЛЕНИЕ
                    asyncio.create_task(
                        self._notify_chat_event(
                            chat_id=chat_id,
                            event_type='ownership_transferred',
                            data={
                                'chat_id': chat_id,
                                'old_owner': user_id,
                                'new_owner': new_owner_id,
                                'timestamp': datetime.utcnow().isoformat()
                            },
                            session=session
                        )
                    )
                    
                    return {
                        'chat_id': chat_id,
                        'old_owner': user_id,
                        'new_owner': new_owner_id,
                        'old_role': old_role,
                        'new_role': new_owner.role
                    }
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"❌ Failed to transfer ownership: {e}")
                    raise
        else:
            async with ChatUnitOfWork() as uow:
                await uow.begin_transaction()
                try:
                    chat = await uow.chats.get_by_id(chat_id)
                    if not chat:
                        raise NotFoundError(f"Chat {chat_id} not found")
                    
                    current = await uow.participants.get(chat_id, user_id)
                    if not current or current.role != ParticipantRole.OWNER.value:
                        raise PermissionError("Only owner can transfer ownership")
                    
                    new_owner = await uow.participants.get(chat_id, new_owner_id)
                    if not new_owner:
                        raise NotFoundError(f"User {new_owner_id} is not a member")
                    
                    old_role = current.role
                    current.role = ParticipantRole.ADMIN.value
                    await uow.participants.update(current)
                    
                    new_owner.role = ParticipantRole.OWNER.value
                    await uow.participants.update(new_owner)
                    
                    chat.owner_id = new_owner_id
                    chat.updated_at = datetime.utcnow()
                    await uow.chats.update(chat)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type=EventType.OWNERSHIP_TRANSFERRED.value,
                        user_id=user_id,
                        user_role_at_time=current.role,
                        target_id=None,
                        target_type=None,
                        payload={'old_owner': user_id, 'new_owner': new_owner_id},
                        created_at=datetime.utcnow(),
                        created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    await uow.commit()
                    
                    await self.participant_cache.invalidate(chat_id, user_id)
                    await self.participant_cache.invalidate(chat_id, new_owner_id)
                    
                    logger.info(f"✅ Ownership transferred from {user_id} to {new_owner_id}")
                    
                    # WEBSOCKET УВЕДОМЛЕНИЕ
                    asyncio.create_task(
                        self._notify_chat_event(
                            chat_id=chat_id,
                            event_type='ownership_transferred',
                            data={
                                'chat_id': chat_id,
                                'old_owner': user_id,
                                'new_owner': new_owner_id,
                                'timestamp': datetime.utcnow().isoformat()
                            }
                        )
                    )
                    
                    return {
                        'chat_id': chat_id,
                        'old_owner': user_id,
                        'new_owner': new_owner_id,
                        'old_role': old_role,
                        'new_role': new_owner.role
                    }
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"❌ Failed to transfer ownership: {e}")
                    raise
    
    async def ban_user(self, chat_id: int, user_id: str, target_user_id: str,
                       ban_type: str = 'ban', reason: Optional[str] = None,
                       duration_minutes: Optional[int] = None, permanent: bool = False, session=None) -> ChatBan:
        """Забанить пользователя с WebSocket уведомлением"""
        logger.info(f"🚫 Banning {target_user_id} from chat {chat_id}")
        
        BanValidator.validate_ban(ban_type, duration_minutes, permanent)
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                await uow.begin_transaction()
                try:
                    chat = await uow.chats.get_by_id(chat_id)
                    if not chat:
                        raise NotFoundError(f"Chat {chat_id} not found")
                    
                    current = await uow.participants.get(chat_id, user_id)
                    if not current or current.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                        raise PermissionError("No permission to ban users")
                    
                    target = await uow.participants.get(chat_id, target_user_id)
                    if not target:
                        active_ban = await uow.bans.get_active(chat_id, target_user_id)
                        if active_ban:
                            raise ValidationError("User is already banned")
                        raise NotFoundError(f"User {target_user_id} is not a member")
                    
                    role_level = {'owner': 4, 'admin': 3, 'moderator': 2, 'member': 1}
                    if role_level.get(target.role, 0) >= role_level.get(current.role, 0):
                        raise PermissionError("Cannot ban users with equal or higher role")
                    
                    now = datetime.utcnow()
                    ban = ChatBan(
                        ban_id=None,
                        chat_id=chat_id,
                        user_id=target_user_id,
                        banned_by=user_id,
                        ban_type=ban_type,
                        reason=reason,
                        reason_code=None,
                        restrictions=None,
                        banned_at=now,
                        expires_at=now + timedelta(minutes=duration_minutes) if duration_minutes else None,
                        is_permanent=permanent,
                        event_id=None,
                        is_active=True,
                        region=chat_config.DEFAULT_REGION
                    )
                    
                    result = await uow.bans.create(ban)
                    if not result:
                        raise DatabaseError("Failed to create ban")
                    
                    if ban_type in ['ban', 'kick']:
                        await uow.participants.delete(chat_id, target_user_id)
                        await uow.chats.decrement_members(chat_id)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type=f"user_{ban_type}d",
                        user_id=user_id,
                        user_role_at_time=current.role,
                        target_id=result.ban_id,
                        target_type='ban',
                        payload={'target_user_id': target_user_id, 'ban_id': result.ban_id, 'reason': reason},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    await uow.commit()
                    
                    await self.participant_cache.invalidate(chat_id, target_user_id)
                    if ban_type in ['ban', 'kick']:
                        await self.participant_cache.invalidate_chat(chat_id)
                    
                    logger.info(f"✅ User {target_user_id} banned from chat {chat_id}")
                    
                    # WEBSOCKET УВЕДОМЛЕНИЕ
                    asyncio.create_task(
                        self._notify_chat_event(
                            chat_id=chat_id,
                            event_type=f'user_{ban_type}d',
                            data={
                                'chat_id': chat_id,
                                'target_user_id': target_user_id,
                                'banned_by': user_id,
                                'ban_type': ban_type,
                                'ban_id': result.ban_id,
                                'reason': reason,
                                'duration_minutes': duration_minutes,
                                'permanent': permanent,
                                'timestamp': now.isoformat()
                            },
                            session=session
                        )
                    )
                    
                    return result
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"❌ Failed to ban user: {e}")
                    raise
        else:
            async with ChatUnitOfWork() as uow:
                await uow.begin_transaction()
                try:
                    chat = await uow.chats.get_by_id(chat_id)
                    if not chat:
                        raise NotFoundError(f"Chat {chat_id} not found")
                    
                    current = await uow.participants.get(chat_id, user_id)
                    if not current or current.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                        raise PermissionError("No permission to ban users")
                    
                    target = await uow.participants.get(chat_id, target_user_id)
                    if not target:
                        active_ban = await uow.bans.get_active(chat_id, target_user_id)
                        if active_ban:
                            raise ValidationError("User is already banned")
                        raise NotFoundError(f"User {target_user_id} is not a member")
                    
                    role_level = {'owner': 4, 'admin': 3, 'moderator': 2, 'member': 1}
                    if role_level.get(target.role, 0) >= role_level.get(current.role, 0):
                        raise PermissionError("Cannot ban users with equal or higher role")
                    
                    now = datetime.utcnow()
                    ban = ChatBan(
                        ban_id=None,
                        chat_id=chat_id,
                        user_id=target_user_id,
                        banned_by=user_id,
                        ban_type=ban_type,
                        reason=reason,
                        reason_code=None,
                        restrictions=None,
                        banned_at=now,
                        expires_at=now + timedelta(minutes=duration_minutes) if duration_minutes else None,
                        is_permanent=permanent,
                        event_id=None,
                        is_active=True,
                        region=chat_config.DEFAULT_REGION
                    )
                    
                    result = await uow.bans.create(ban)
                    if not result:
                        raise DatabaseError("Failed to create ban")
                    
                    if ban_type in ['ban', 'kick']:
                        await uow.participants.delete(chat_id, target_user_id)
                        await uow.chats.decrement_members(chat_id)
                    
                    event = ChatEvent(
                        event_id=None,
                        chat_id=chat_id,
                        event_type=f"user_{ban_type}d",
                        user_id=user_id,
                        user_role_at_time=current.role,
                        target_id=result.ban_id,
                        target_type='ban',
                        payload={'target_user_id': target_user_id, 'ban_id': result.ban_id, 'reason': reason},
                        created_at=now,
                        created_date=int(now.strftime('%Y%m%d')),
                        idempotency_key=None,
                        region=chat_config.DEFAULT_REGION
                    )
                    await uow.events.create(event)
                    
                    await uow.commit()
                    
                    await self.participant_cache.invalidate(chat_id, target_user_id)
                    if ban_type in ['ban', 'kick']:
                        await self.participant_cache.invalidate_chat(chat_id)
                    
                    logger.info(f"✅ User {target_user_id} banned from chat {chat_id}")
                    
                    # WEBSOCKET УВЕДОМЛЕНИЕ
                    asyncio.create_task(
                        self._notify_chat_event(
                            chat_id=chat_id,
                            event_type=f'user_{ban_type}d',
                            data={
                                'chat_id': chat_id,
                                'target_user_id': target_user_id,
                                'banned_by': user_id,
                                'ban_type': ban_type,
                                'ban_id': result.ban_id,
                                'reason': reason,
                                'duration_minutes': duration_minutes,
                                'permanent': permanent,
                                'timestamp': now.isoformat()
                            }
                        )
                    )
                    
                    return result
                    
                except Exception as e:
                    await uow.rollback()
                    logger.error(f"❌ Failed to ban user: {e}")
                    raise
    
    async def unban_user(self, chat_id: int, user_id: str, ban_id: int, session=None) -> Dict:
        """Разбанить пользователя с WebSocket уведомлением"""
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                current = await uow.participants.get(chat_id, user_id)
                if not current or current.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("No permission to unban users")
                
                ban = await uow.bans.get(ban_id)
                if not ban or ban.chat_id != chat_id:
                    raise NotFoundError(f"Ban {ban_id} not found")
                
                await uow.bans.deactivate(ban_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.USER_UNBANNED.value,
                    user_id=user_id,
                    user_role_at_time=current.role,
                    target_id=ban_id,
                    target_type='ban',
                    payload={'target_user_id': ban.user_id},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                current = await uow.participants.get(chat_id, user_id)
                if not current or current.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("No permission to unban users")
                
                ban = await uow.bans.get(ban_id)
                if not ban or ban.chat_id != chat_id:
                    raise NotFoundError(f"Ban {ban_id} not found")
                
                await uow.bans.deactivate(ban_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.USER_UNBANNED.value,
                    user_id=user_id,
                    user_role_at_time=current.role,
                    target_id=ban_id,
                    target_type='ban',
                    payload={'target_user_id': ban.user_id},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='user_unbanned',
                data={
                    'chat_id': chat_id,
                    'user_id': ban.user_id,
                    'unbanned_by': user_id,
                    'ban_id': ban_id,
                    'timestamp': datetime.utcnow().isoformat()
                },
                session=session
            )
        )
        
        return {'user_id': ban.user_id, 'unbanned_by': user_id, 'previous_ban_id': ban_id}
    
    async def create_invite(self, chat_id: int, user_id: str,
                           expires_in_hours: int = 24, max_uses: int = 0,
                           requires_approval: bool = False, default_role: str = 'member', session=None) -> Dict:
        """Создать приглашение с WebSocket уведомлением"""
        InviteValidator.validate_create(expires_in_hours, max_uses, default_role)
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("No permission to create invites")
                
                now = datetime.utcnow()
                
                invite_code = uow.invites._generate_invite_code()
                
                invite = ChatInvite(
                    invite_id=None,
                    chat_id=chat_id,
                    invite_code=invite_code,
                    created_by=user_id,
                    created_at=now,
                    expires_at=now + timedelta(hours=expires_in_hours) if expires_in_hours > 0 else None,
                    max_uses=max_uses,
                    used_count=0,
                    remaining_uses=max_uses,
                    can_join=True,
                    requires_approval=requires_approval,
                    default_role=default_role,
                    is_active=True,
                    deactivated_at=None,
                    region=chat_config.DEFAULT_REGION
                )
                
                result = await uow.invites.create(invite)
                if not result:
                    raise DatabaseError("Failed to create invite")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.INVITE_CREATED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=result.invite_id,
                    target_type='invite',
                    payload={},
                    created_at=now,
                    created_date=int(now.strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                chat = await uow.chats.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError(f"Chat {chat_id} not found")
                
                participant = await uow.participants.get(chat_id, user_id)
                if not participant:
                    raise PermissionError("No permission to create invites")
                
                now = datetime.utcnow()
                
                invite_code = uow.invites._generate_invite_code()
                
                invite = ChatInvite(
                    invite_id=None,
                    chat_id=chat_id,
                    invite_code=invite_code,
                    created_by=user_id,
                    created_at=now,
                    expires_at=now + timedelta(hours=expires_in_hours) if expires_in_hours > 0 else None,
                    max_uses=max_uses,
                    used_count=0,
                    remaining_uses=max_uses,
                    can_join=True,
                    requires_approval=requires_approval,
                    default_role=default_role,
                    is_active=True,
                    deactivated_at=None,
                    region=chat_config.DEFAULT_REGION
                )
                
                result = await uow.invites.create(invite)
                if not result:
                    raise DatabaseError("Failed to create invite")
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.INVITE_CREATED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=result.invite_id,
                    target_type='invite',
                    payload={},
                    created_at=now,
                    created_date=int(now.strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        base_url = "https://akortmedia.com/join"
        invite_link = f"{base_url}/{result.invite_code}"
        
        result_dict = result.to_dict()
        result_dict['invite_link'] = invite_link
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='invite_created',
                data={
                    'chat_id': chat_id,
                    'invite': result_dict,
                    'created_by': user_id,
                    'timestamp': now.isoformat()
                },
                session=session
            )
        )
        
        return result_dict
    
    async def get_invite(self, invite_code: str, session=None) -> ChatInvite:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                invite = await uow.invites.get_by_code(invite_code)
                if not invite:
                    raise NotFoundError("Invite not found")
                
                if not invite.is_active:
                    raise PermissionError("Invite is no longer active")
                
                if invite.expires_at and invite.expires_at < datetime.utcnow():
                    raise PermissionError("Invite has expired")
                
                if invite.max_uses > 0 and invite.used_count >= invite.max_uses:
                    raise PermissionError("Invite has reached maximum uses")
        else:
            async with ChatUnitOfWork() as uow:
                invite = await uow.invites.get_by_code(invite_code)
                if not invite:
                    raise NotFoundError("Invite not found")
                
                if not invite.is_active:
                    raise PermissionError("Invite is no longer active")
                
                if invite.expires_at and invite.expires_at < datetime.utcnow():
                    raise PermissionError("Invite has expired")
                
                if invite.max_uses > 0 and invite.used_count >= invite.max_uses:
                    raise PermissionError("Invite has reached maximum uses")
        
        return invite
    
    async def revoke_invite(self, chat_id: int, user_id: str, invite_id: int, session=None) -> Dict:
        """Отозвать приглашение с WebSocket уведомлением"""
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("No permission to revoke invites")
                
                invite = await uow.invites.get(invite_id)
                if not invite or invite.chat_id != chat_id:
                    raise NotFoundError(f"Invite {invite_id} not found")
                
                await uow.invites.deactivate(invite_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.INVITE_REVOKED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=invite_id,
                    target_type='invite',
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("No permission to revoke invites")
                
                invite = await uow.invites.get(invite_id)
                if not invite or invite.chat_id != chat_id:
                    raise NotFoundError(f"Invite {invite_id} not found")
                
                await uow.invites.deactivate(invite_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type=EventType.INVITE_REVOKED.value,
                    user_id=user_id,
                    user_role_at_time=participant.role,
                    target_id=invite_id,
                    target_type='invite',
                    payload={},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='invite_revoked',
                data={
                    'chat_id': chat_id,
                    'invite_id': invite_id,
                    'revoked_by': user_id,
                    'timestamp': datetime.utcnow().isoformat()
                },
                session=session
            )
        )
        
        return {'revoked': True, 'invite_id': invite_id}
    
    async def list_invites(self, chat_id: int, user_id: str,
                           active_only: bool = True, limit: int = 50, offset: int = 0, session=None) -> List[ChatInvite]:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("No permission to view invites")
                
                return await uow.invites.list_by_chat(chat_id, active_only, limit, offset)
        else:
            async with ChatUnitOfWork() as uow:
                participant = await uow.participants.get(chat_id, user_id)
                if not participant or participant.role not in [ParticipantRole.OWNER.value, ParticipantRole.ADMIN.value]:
                    raise PermissionError("No permission to view invites")
                
                return await uow.invites.list_by_chat(chat_id, active_only, limit, offset)
    
    async def _get_partner_name(self, chat: Chat, current_user_id: str) -> str:
        """Получить имя собеседника для приватного чата"""
        if chat.type != 'private':
            return chat.title
        
        # Определяем ID собеседника
        if chat.user1_id == current_user_id:
            partner_id = chat.user2_id
        else:
            partner_id = chat.user1_id
        
        if not partner_id:
            return chat.title
        
        # Пытаемся получить из кэша
        cache_key = f"user_name:{partner_id}"
        cached_name = await cache.get(cache_key)
        if cached_name:
            return cached_name
        
        # Ищем в БД
        async with RequestContext() as ctx:
            query = """
            DECLARE $user_id AS Utf8;
            SELECT first_name_encrypted, username
            FROM `users`
            WHERE id = $user_id;
            """
            result = await ctx.session.transaction().execute(
                await ctx.session.prepare(query),
                {'$user_id': partner_id},
                commit_tx=True
            )
            
            if result and result[0].rows:
                row = result[0].rows[0]
                first_name_enc = row.get('first_name_encrypted')
                if first_name_enc:
                    try:
                        import base64
                        name = base64.b64decode(first_name_enc).decode('utf-8')
                        await cache.set(cache_key, name, ttl=3600)
                        return name
                    except:
                        pass
                
                username = row.get('username')
                if username:
                    await cache.set(cache_key, username, ttl=3600)
                    return username
            
            return "Unknown"
    async def get_user_chats(self, user_id: str, limit: int = 50, cursor: Optional[str] = None,
                            include_hidden: bool = False, session=None) -> Tuple[List[Chat], Optional[str]]:
        """Получить чаты пользователя"""

        logger.info(f"📋 Getting chats for user {user_id} (limit={limit}, cursor={cursor})")
        start_time = time.time()

        chats = []
        next_cursor = None

        try:
            async def _fetch_chats(uow):
                participants, _next_cursor = await uow.participants.list_by_user(
                    user_id=user_id,
                    limit=limit,
                    cursor=cursor,
                    include_hidden=include_hidden
                )
                logger.info(f"📊 Found {len(participants)} chats for user (has_next: {_next_cursor is not None})")

                chat_ids = [p.chat_id for p in participants]
                if not chat_ids:
                    return [], _next_cursor

                # Batch-запрос вместо N+1
                chats_map = await uow.chats.get_many(chat_ids)

                result = []
                for p in participants:
                    chat = chats_map.get(p.chat_id)
                    if not chat or chat.is_deleted:
                        continue
                    chat._participant_info = p
                    if chat.type == 'private':
                        partner_name = await self._get_partner_name(chat, user_id)
                        if partner_name:
                            chat.title = partner_name
                    logger.info(f"📊 Chat {chat.id}: members_count={chat.members_count}, type={chat.type}")
                    result.append(chat)
                return result, _next_cursor

            if session:
                async with await ChatUnitOfWork.with_session(session) as uow:
                    chats, next_cursor = await _fetch_chats(uow)
            else:
                async with ChatUnitOfWork() as uow:
                    chats, next_cursor = await _fetch_chats(uow)
            
            duration = time.time() - start_time
            logger.info(f"✅ Got {len(chats)} chats for user in {duration*1000:.1f}ms")

            return chats, next_cursor

        except Exception as e:
            logger.error(f"❌ Error getting user chats: {e}", exc_info=True)
            return [], None

    async def get_chat_members(self, chat_id: int, user_id: str, limit: int = 100, 
                               cursor: Optional[str] = None, session=None) -> Tuple[List[ChatParticipant], Optional[str]]:
        """
        Получить список участников чата с пагинацией
        """
        logger.info(f"👥 Getting members for chat {chat_id} (limit={limit})")
        start_time = time.time()
        
        try:
            has_access = await self.participant_cache.is_member(
                chat_id, 
                user_id, 
                session=session
            )
            if not has_access:
                logger.warning(f"⛔ User {user_id} has no access to chat {chat_id}")
                raise PermissionError("You don't have access to this chat")
            
            if session:
                async with await ChatUnitOfWork.with_session(session) as uow:
                    members, next_cursor = await uow.participants.list_by_chat_with_cursor(
                        chat_id=chat_id,
                        limit=limit,
                        cursor=cursor
                    )
            else:
                async with ChatUnitOfWork() as uow:
                    members, next_cursor = await uow.participants.list_by_chat_with_cursor(
                        chat_id=chat_id,
                        limit=limit,
                        cursor=cursor
                    )
            
            duration = time.time() - start_time
            logger.info(f"✅ Got {len(members)} members for chat {chat_id} in {duration*1000:.1f}ms")
            
            return members, next_cursor
                
        except PermissionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error getting chat members: {e}")
            return [], None
    
    async def get_public_chats(self, limit: int = 50, cursor: Optional[str] = None, session=None) -> Tuple[List[Chat], Optional[str]]:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                return await uow.chats.list_public(limit, cursor)
        else:
            async with ChatUnitOfWork() as uow:
                return await uow.chats.list_public(limit, cursor)
    
    async def search_chats(self, query: str, user_id: str, limit: int = 20, cursor: Optional[str] = None, session=None) -> Tuple[List[Chat], Optional[str]]:
        if len(query) < 2:
            raise ValidationError("Search query must be at least 2 characters")
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                chats, next_cursor = await uow.chats.search_by_title(query, limit, cursor)
                
                result = []
                for chat in chats:
                    if chat.is_public:
                        result.append(chat)
                    else:
                        participant = await uow.participants.get(chat.id, user_id)
                        if participant:
                            result.append(chat)
        else:
            async with ChatUnitOfWork() as uow:
                chats, next_cursor = await uow.chats.search_by_title(query, limit, cursor)
                
                result = []
                for chat in chats:
                    if chat.is_public:
                        result.append(chat)
                    else:
                        participant = await uow.participants.get(chat.id, user_id)
                        if participant:
                            result.append(chat)
        
        return result, next_cursor
    
    async def get_chat_bans(self, chat_id: int, user_id: str,
                           active_only: bool = True, limit: int = 50, offset: int = 0,
                           session=None) -> List[ChatBan]:
        """Получить список банов чата"""
        has_access = await self.participant_cache.check_access(
            chat_id, 
            user_id, 
            session=session
        )
        if not has_access:
            raise PermissionError("You don't have access to this chat")
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                return await uow.bans.list_by_chat(chat_id, active_only, limit, offset)
        else:
            async with ChatUnitOfWork() as uow:
                return await uow.bans.list_by_chat(chat_id, active_only, limit, offset)
    
    async def pin_message(self, chat_id: int, message_id: int, user_id: str, session=None) -> Dict:
        """Закрепить сообщение с WebSocket уведомлением"""
        logger.info(f"📌 Pinning message {message_id} in chat {chat_id}")
        
        has_permission = await self.participant_cache.check_permission(
            chat_id, 
            user_id, 
            'can_pin_messages',
            session=session
        )
        if not has_permission:
            raise PermissionError("You don't have permission to pin messages")
        
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                from handlers.message_handler import MessageRepository
                msg_repo = MessageRepository(uow._session)
                
                message = await msg_repo.get(chat_id, message_id)
                if not message:
                    raise NotFoundError(f"Message {message_id} not found")
                
                if message.is_deleted:
                    raise ValidationError("Cannot pin deleted message")
                
                pinned = await uow.pinned.pin(chat_id, message_id, user_id)
                if not pinned:
                    raise DatabaseError("Failed to pin message")
                
                all_pinned = await uow.pinned.get_active_list(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="message_pinned",
                    user_id=user_id,
                    user_role_at_time=None,
                    target_id=message_id,
                    target_type="message",
                    payload={'message_id': message_id, 'total_pinned': len(all_pinned)},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                from handlers.message_handler import MessageRepository
                msg_repo = MessageRepository(uow._session)
                
                message = await msg_repo.get(chat_id, message_id)
                if not message:
                    raise NotFoundError(f"Message {message_id} not found")
                
                if message.is_deleted:
                    raise ValidationError("Cannot pin deleted message")
                
                pinned = await uow.pinned.pin(chat_id, message_id, user_id)
                if not pinned:
                    raise DatabaseError("Failed to pin message")
                
                all_pinned = await uow.pinned.get_active_list(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="message_pinned",
                    user_id=user_id,
                    user_role_at_time=None,
                    target_id=message_id,
                    target_type="message",
                    payload={'message_id': message_id, 'total_pinned': len(all_pinned)},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        result = {
            'pinned': pinned.to_dict(),
            'all_pinned': [p.to_dict() for p in all_pinned],
            'total_pinned': len(all_pinned),
            'max_pinned': uow.pinned.MAX_PINNED
        }
        
        result['message'] = {
            'id': message.message_id,
            'content_preview': message.content[:100] if message.content else None,
            'sender_id': message.sender_id,
            'created_at': message.created_at.isoformat() if message.created_at else None
        }
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='message_pinned',
                data={
                    'chat_id': chat_id,
                    'message_id': message_id,
                    'pinned_by': user_id,
                    'all_pinned': result['all_pinned'],
                    'total_pinned': len(all_pinned),
                    'timestamp': datetime.utcnow().isoformat()
                },
                session=session
            )
        )
        
        return result

    async def unpin_message(self, chat_id: int, message_id: Optional[int] = None, user_id: str = None, session=None) -> Dict:
        """Открепить сообщение с WebSocket уведомлением"""
        if message_id is not None:
            return await self._unpin_specific_message(chat_id, message_id, user_id, session)
        else:
            return await self._unpin_first_message(chat_id, user_id, session)
    
    async def _unpin_specific_message(self, chat_id: int, message_id: int, user_id: str, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                has_permission = await self.participant_cache.check_permission(
                    chat_id, user_id, 'can_pin_messages', session=session
                )
                if not has_permission:
                    raise PermissionError("You don't have permission to unpin messages")
                
                current = await uow.pinned.get_active_list(chat_id)
                if not current:
                    raise ValidationError("No pinned messages in this chat")
                
                message_to_unpin = None
                for pin in current:
                    if pin.message_id == message_id:
                        message_to_unpin = pin
                        break
                
                if not message_to_unpin:
                    raise NotFoundError(f"Message {message_id} is not pinned")
                
                success = await uow.pinned.unpin_by_id(message_id, chat_id, user_id)
                if not success:
                    raise DatabaseError("Failed to unpin message")
                
                remaining = await uow.pinned.get_active_list(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="message_unpinned",
                    user_id=user_id,
                    user_role_at_time=None,
                    target_id=message_id,
                    target_type="message",
                    payload={'previous_message_id': message_id, 'remaining': len(remaining)},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                has_permission = await self.participant_cache.check_permission(
                    chat_id, user_id, 'can_pin_messages'
                )
                if not has_permission:
                    raise PermissionError("You don't have permission to unpin messages")
                
                current = await uow.pinned.get_active_list(chat_id)
                if not current:
                    raise ValidationError("No pinned messages in this chat")
                
                message_to_unpin = None
                for pin in current:
                    if pin.message_id == message_id:
                        message_to_unpin = pin
                        break
                
                if not message_to_unpin:
                    raise NotFoundError(f"Message {message_id} is not pinned")
                
                success = await uow.pinned.unpin_by_id(message_id, chat_id, user_id)
                if not success:
                    raise DatabaseError("Failed to unpin message")
                
                remaining = await uow.pinned.get_active_list(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="message_unpinned",
                    user_id=user_id,
                    user_role_at_time=None,
                    target_id=message_id,
                    target_type="message",
                    payload={'previous_message_id': message_id, 'remaining': len(remaining)},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='message_unpinned',
                data={
                    'chat_id': chat_id,
                    'message_id': message_id,
                    'unpinned_by': user_id,
                    'remaining_pinned': [p.to_dict() for p in remaining],
                    'total_remaining': len(remaining),
                    'timestamp': datetime.utcnow().isoformat()
                },
                session=session
            )
        )
        
        return {
            'unpinned': True,
            'message_id': message_id,
            'chat_id': chat_id,
            'remaining_pinned': [p.to_dict() for p in remaining],
            'total_remaining': len(remaining)
        }
    
    async def _unpin_first_message(self, chat_id: int, user_id: str, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                has_permission = await self.participant_cache.check_permission(
                    chat_id, user_id, 'can_pin_messages', session=session
                )
                if not has_permission:
                    raise PermissionError("You don't have permission to unpin messages")
                
                current = await uow.pinned.get_active_list(chat_id)
                if not current:
                    raise ValidationError("No pinned messages in this chat")
                
                first_pin = current[0]
                
                success = await uow.pinned.unpin_by_id(first_pin.message_id, chat_id, user_id)
                if not success:
                    raise DatabaseError("Failed to unpin message")
                
                remaining = await uow.pinned.get_active_list(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="message_unpinned",
                    user_id=user_id,
                    user_role_at_time=None,
                    target_id=first_pin.message_id,
                    target_type="message",
                    payload={'previous_message_id': first_pin.message_id, 'remaining': len(remaining)},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        else:
            async with ChatUnitOfWork() as uow:
                has_permission = await self.participant_cache.check_permission(
                    chat_id, user_id, 'can_pin_messages'
                )
                if not has_permission:
                    raise PermissionError("You don't have permission to unpin messages")
                
                current = await uow.pinned.get_active_list(chat_id)
                if not current:
                    raise ValidationError("No pinned messages in this chat")
                
                first_pin = current[0]
                
                success = await uow.pinned.unpin_by_id(first_pin.message_id, chat_id, user_id)
                if not success:
                    raise DatabaseError("Failed to unpin message")
                
                remaining = await uow.pinned.get_active_list(chat_id)
                
                event = ChatEvent(
                    event_id=None,
                    chat_id=chat_id,
                    event_type="message_unpinned",
                    user_id=user_id,
                    user_role_at_time=None,
                    target_id=first_pin.message_id,
                    target_type="message",
                    payload={'previous_message_id': first_pin.message_id, 'remaining': len(remaining)},
                    created_at=datetime.utcnow(),
                    created_date=int(datetime.utcnow().strftime('%Y%m%d')),
                    idempotency_key=None,
                    region=chat_config.DEFAULT_REGION
                )
                await uow.events.create(event)
        
        # WEBSOCKET УВЕДОМЛЕНИЕ
        asyncio.create_task(
            self._notify_chat_event(
                chat_id=chat_id,
                event_type='message_unpinned',
                data={
                    'chat_id': chat_id,
                    'message_id': first_pin.message_id,
                    'unpinned_by': user_id,
                    'remaining_pinned': [p.to_dict() for p in remaining],
                    'total_remaining': len(remaining),
                    'timestamp': datetime.utcnow().isoformat()
                },
                session=session
            )
        )
        
        return {
            'unpinned': True,
            'chat_id': chat_id,
            'previous_message_id': first_pin.message_id,
            'message': 'Oldest pinned message unpinned'
        }
    
    async def get_pinned_messages(self, chat_id: int, user_id: str, session=None) -> Dict:
        """
        ПОЛУЧИТЬ ВСЕ ЗАКРЕПЛЕННЫЕ СООБЩЕНИЯ
        """
        logger.info(f"📌 Getting pinned messages for chat {chat_id}")
        start_time = time.time()
        
        try:
            if session:
                async with await ChatUnitOfWork.with_session(session) as uow:
                    participant = await uow.participants.get(chat_id, user_id)
                    if not participant:
                        chat = await uow.chats.get_by_id(chat_id)
                        if not chat or not chat.is_public:
                            raise PermissionError("You don't have access to this chat")
                    
                    pinned_list = await uow.pinned.get_active_list(chat_id)
                    
                    if not pinned_list:
                        return {
                            'pinned': [],
                            'total': 0,
                            'max_pinned': uow.pinned.MAX_PINNED
                        }
                    
                    from handlers.message_handler import MessageRepository
                    msg_repo = MessageRepository(uow._session)
                    message_ids = [p.message_id for p in pinned_list]
                    messages_map = await msg_repo.get_many(chat_id, message_ids)
                    
                    result = []
                    for pin in pinned_list:
                        message = messages_map.get(pin.message_id)
                        pin_dict = pin.to_dict()
                        
                        if message and not message.is_deleted:
                            pin_dict['message'] = {
                                'id': message.message_id,
                                'content': message.content,
                                'content_preview': message.content[:200] if message.content else None,
                                'sender_id': message.sender_id,
                                'created_at': message.created_at.isoformat() if message.created_at else None,
                                'message_type': message.message_type,
                                'has_attachments': message.has_attachments
                            }
                        else:
                            pin_dict['message'] = {'deleted': True, 'id': pin.message_id}
                        
                        result.append(pin_dict)
            else:
                async with ChatUnitOfWork() as uow:
                    participant = await uow.participants.get(chat_id, user_id)
                    if not participant:
                        chat = await uow.chats.get_by_id(chat_id)
                        if not chat or not chat.is_public:
                            raise PermissionError("You don't have access to this chat")
                    
                    pinned_list = await uow.pinned.get_active_list(chat_id)
                    
                    if not pinned_list:
                        return {
                            'pinned': [],
                            'total': 0,
                            'max_pinned': uow.pinned.MAX_PINNED
                        }
                    
                    from handlers.message_handler import MessageRepository
                    msg_repo = MessageRepository(uow._session)
                    message_ids = [p.message_id for p in pinned_list]
                    messages_map = await msg_repo.get_many(chat_id, message_ids)
                    
                    result = []
                    for pin in pinned_list:
                        message = messages_map.get(pin.message_id)
                        pin_dict = pin.to_dict()
                        
                        if message and not message.is_deleted:
                            pin_dict['message'] = {
                                'id': message.message_id,
                                'content': message.content,
                                'content_preview': message.content[:200] if message.content else None,
                                'sender_id': message.sender_id,
                                'created_at': message.created_at.isoformat() if message.created_at else None,
                                'message_type': message.message_type,
                                'has_attachments': message.has_attachments
                            }
                        else:
                            pin_dict['message'] = {'deleted': True, 'id': pin.message_id}
                        
                        result.append(pin_dict)
            
            duration = time.time() - start_time
            logger.info(f"✅ Got {len(result)} pinned messages in {duration*1000:.1f}ms")
            
            return {
                'pinned': result,
                'total': len(result),
                'max_pinned': uow.pinned.MAX_PINNED
            }
                
        except Exception as e:
            logger.error(f"❌ Error getting pinned messages: {e}")
            return {'pinned': [], 'total': 0, 'max_pinned': 5}

    async def reorder_pinned_messages(self, chat_id: int, message_ids: List[int], user_id: str, session=None) -> Dict:
        if session:
            async with await ChatUnitOfWork.with_session(session) as uow:
                has_permission = await self.participant_cache.check_permission(
                    chat_id, user_id, 'can_pin_messages', session=session
                )
                if not has_permission:
                    raise PermissionError("You don't have permission to reorder pinned messages")
                
                success = await uow.pinned.reorder(chat_id, message_ids, user_id)
                if not success:
                    raise DatabaseError("Failed to reorder pinned messages")
                
                updated = await uow.pinned.get_active_list(chat_id)
        else:
            async with ChatUnitOfWork() as uow:
                has_permission = await self.participant_cache.check_permission(
                    chat_id, user_id, 'can_pin_messages'
                )
                if not has_permission:
                    raise PermissionError("You don't have permission to reorder pinned messages")
                
                success = await uow.pinned.reorder(chat_id, message_ids, user_id)
                if not success:
                    raise DatabaseError("Failed to reorder pinned messages")
                
                updated = await uow.pinned.get_active_list(chat_id)
        
        return {
            'reordered': True,
            'chat_id': chat_id,
            'pinned': [p.to_dict() for p in updated],
            'total': len(updated)
        }
class ChatHandler(BaseHandler):
    """Обработчик HTTP запросов для чатов"""
    
    def __init__(self):
        super().__init__()
        self.service = ChatService()
        logger.info("✅ ChatHandler initialized (optimized)")
    
    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========


    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_private_chat(self, event: Dict, user: Dict, target_user_id: str) -> Dict:
        """GET /users/me/chat-with/{targetUserId} - получить или создать приватный чат
        
        Если чат уже существует - возвращает его.
        Если нет - создаёт новый.
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            target_user_id = self._validate_user_id(target_user_id)
            
            if user_id == target_user_id:
                raise ValidationError("Cannot open chat with yourself")
            
            async with RequestContext() as ctx:
                chat = await self.service.get_or_create_private_chat(
                    user_id=user_id,
                    recipient_id=target_user_id,
                    session=ctx.session
                )
                
                # Получаем информацию об участнике для текущего пользователя
                participant = await self.service.participant_cache.get_participant(
                    chat.id, user_id, session=ctx.session
                )
                chat._participant_info = participant
                
                logger.info(f"✅ Private chat returned: {chat.id} with {target_user_id[:8]}")
                return self.response.success(chat.to_dict(), 200)
                
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"❌ Error getting private chat: {e}", exc_info=True)
            return await self.handle_error(e, event)
    def _get_chat_id(self, event: Dict) -> Optional[int]:
        """Извлечь chat_id из разных мест"""
        if event.get('pathParameters') and 'chatId' in event['pathParameters']:
            try:
                return int(event['pathParameters']['chatId'])
            except (ValueError, TypeError):
                pass
        
        if event.get('params') and 'chatId' in event['params']:
            try:
                return int(event['params']['chatId'])
            except (ValueError, TypeError):
                pass
        
        path = event.get('path', '')
        parts = path.split('/')
        if 'chats' in parts:
            idx = parts.index('chats') + 1
            if idx < len(parts):
                try:
                    return int(parts[idx])
                except (ValueError, TypeError):
                    pass
        
        return None

    # ========== ХЕНДЛЕРЫ ==========

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_my_permissions(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """
        GET /chats/{chatId}/my-permissions - Получить свои права в чате
        """
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))

            async with RequestContext() as ctx:
                result = await self.service.get_user_permissions(
                    chat_id=chat_id,
                    user_id=user_id,
                    session=ctx.session
                )

                return self.response.success(result, 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_member_permissions(self, event: Dict, user: Dict, chat_id: int, member_id: str) -> Dict:
        """
        GET /chats/{chatId}/members/{memberId}/permissions - Получить права участника
        """
        try:
            chat_id = self._validate_chat_id(chat_id)
            member_id = self._validate_user_id(member_id)
            user_id = self._validate_user_id(user.get('user_id'))

            async with RequestContext() as ctx:
                has_access = await self.service.participant_cache.check_access(
                    chat_id, 
                    user_id, 
                    session=ctx.session
                )
                if not has_access:
                    raise PermissionError("You don't have access to this chat")

                repo = ParticipantRepository(ctx.session)
                participant = await repo.get(chat_id, member_id)
                if not participant:
                    raise NotFoundError(f"User {member_id} is not a member of this chat")

                return self.response.success({
                    'chat_id': chat_id,
                    'user_id': member_id,
                    'role': participant.role,
                    'permissions': participant.permissions_json or 
                                   RolePermissions.get_default_permissions(participant.role).to_dict(),
                    'joined_at': participant.joined_at.isoformat() if participant.joined_at else None
                }, 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_update_member_permissions(self, event: Dict, user: Dict, chat_id: int, member_id: str) -> Dict:
        """
        PUT /chats/{chatId}/members/{memberId}/permissions - Обновить права участника
        """
        try:
            chat_id = self._validate_chat_id(chat_id)
            member_id = self._validate_user_id(member_id)
            
            body = self._parse_body(event)
            permissions = body.get('permissions', {})
            
            if not permissions:
                return self.response.error(
                    message="permissions object is required",
                    code="validation_error",
                    status_code=400,
                    event=event
                )
            
            async with RequestContext() as ctx:
                result = await self.service.update_member_permissions(
                    chat_id=chat_id,
                    admin_id=user['user_id'],
                    target_user_id=member_id,
                    permissions=permissions
                )
            
            return self.response.success(result, 200)
            
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_reset_member_permissions(self, event: Dict, user: Dict, chat_id: int, member_id: str) -> Dict:
        """
        POST /chats/{chatId}/members/{memberId}/permissions/reset - Сбросить права участника
        """
        try:
            chat_id = self._validate_chat_id(chat_id)
            member_id = self._validate_user_id(member_id)
            
            async with RequestContext() as ctx:
                result = await self.service.reset_member_permissions(
                    chat_id=chat_id,
                    admin_id=user['user_id'],
                    target_user_id=member_id
                )
            
            return self.response.success(result, 200)
            
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_list_members_with_permissions(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """
        GET /chats/{chatId}/members-with-permissions - Список участников с их правами
        """
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            limit = self._get_int_query_param(event, 'limit', 50)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            async with RequestContext() as ctx:
                has_access = await self.service.participant_cache.check_access(
                    chat_id, 
                    user_id, 
                    session=ctx.session
                )
                if not has_access:
                    logger.warning(f"⛔ User {user_id} has no access to chat {chat_id}")
                    raise PermissionError("You don't have access to this chat")
                
                repo = ParticipantRepository(ctx.session)
                members = await repo.list_by_chat(chat_id, limit, offset)
                
                result = []
                for member in members:
                    perms = member.permissions_json or \
                            RolePermissions.get_default_permissions(member.role).to_dict()
                    
                    result.append({
                        'user_id': member.user_id,
                        'role': member.role,
                        'permissions': perms,
                        'joined_at': member.joined_at.isoformat() if member.joined_at else None,
                        'is_active': member.is_active,
                        'last_active_at': member.last_active_at.isoformat() if member.last_active_at else None
                    })
                
                logger.info(f"✅ Found {len(result)} members with permissions for chat {chat_id}")
                
                return self.response.success({
                    'members': result,
                    'count': len(result),
                    'chat_id': chat_id
                }, 200)
            
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"❌ Error listing members with permissions: {e}", exc_info=True)
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_search_users(self, event: Dict, user: Dict) -> Dict:
        """
        GET /users/search - Поиск пользователей
        """
        try:
            query = self._get_query_param(event, 'q')
            if not query:
                return self.response.error(
                    message="Search query 'q' is required",
                    code="validation_error",
                    status_code=400,
                    event=event
                )
            
            limit = self._get_int_query_param(event, 'limit', 20)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            limit = min(limit, 50)
            
            async with RequestContext() as ctx:
                repo = UserSearchRepository(ctx.session)
                users, total = await repo.search_users(
                    query=query,
                    limit=limit,
                    offset=offset,
                    current_user_id=user['user_id']
                )
            
            return self.response.success({
                'users': [u.to_dict() for u in users],
                'count': len(users),
                'total': total,
                'query': query,
                'limit': limit,
                'offset': offset
            }, 200)
            
        except ValidationError as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error in search_users: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_chat_by_username(self, event: Dict, user: Dict, username: str) -> Dict:
        """GET /chats/by-username/{username} - Получить чат по username"""
        try:
            if not username or len(username) < 3:
                raise ValidationError("Invalid username")
            
            async with RequestContext() as ctx:
                chat = await self.service.get_chat_by_username(username, user['user_id'])
            
            result = chat.to_dict()
            if chat.username:
                result['link'] = f"https://t.me/{chat.username}"
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_profile_settings(self, event: Dict, user: Dict) -> Dict:
        """GET /users/me/chats/profile-settings - Настройки профиля"""
        try:
            async with RequestContext() as ctx:
                repo = ParticipantRepository(ctx.session)
                participants = await repo.list_by_user(
                    user_id=user['user_id'],
                    limit=1000,
                    include_hidden=True
                )
                
                result = []
                for p in participants:
                    chat_repo = ChatRepository(ctx.session)
                    chat = await chat_repo.get_by_id(p.chat_id)
                    if not chat or chat.is_deleted:
                        continue
                    
                    can_show_in_profile = chat.is_public and chat.username is not None
                    
                    result.append({
                        'chat_id': p.chat_id,
                        'title': chat.title,
                        'type': chat.type,
                        'username': chat.username,
                        'role': p.role,
                        'show_in_profile': p.show_in_profile,
                        'can_show_in_profile': can_show_in_profile,
                        'avatar_url': chat.avatar_url,
                        'members_count': chat.members_count
                    })
                
                return self.response.success({
                    'settings': result,
                    'count': len(result)
                }, 200)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_update_profile_visibility(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /users/me/chats/{chatId}/profile-visibility - Видимость в профиле"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            body = self._parse_body(event)
            show_in_profile = body.get('show_in_profile')
            
            if show_in_profile is None:
                raise ValidationError("show_in_profile is required")
            
            if not isinstance(show_in_profile, bool):
                raise ValidationError("show_in_profile must be boolean")
            
            async with RequestContext() as ctx:
                participant_repo = ParticipantRepository(ctx.session)
                chat_repo = ChatRepository(ctx.session)
                
                participant = await participant_repo.get(chat_id, user['user_id'])
                if not participant:
                    raise PermissionError("You are not a member of this chat")
                
                chat = await chat_repo.get_by_id(chat_id)
                if not chat:
                    raise NotFoundError("Chat not found")
                
                if show_in_profile and not (chat.is_public and chat.username):
                    raise ValidationError("Only public chats with username can be shown in profile")
                
                participant.show_in_profile = show_in_profile
                success = await participant_repo.update(participant)
                
                if not success:
                    raise DatabaseError("Failed to update profile visibility")
                
                await self.service.participant_cache.invalidate(chat_id, user['user_id'])
                
                return self.response.success({
                    'chat_id': chat_id,
                    'show_in_profile': show_in_profile,
                    'message': f"Chat {'will' if show_in_profile else 'will not'} be shown in profile"
                }, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_set_username(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/username - Установить username"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            body = self._parse_body(event)
            username = body.get('username')
            
            async with RequestContext() as ctx:
                result = await self.service.set_username(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    username=username
                )
            
            response_data = result.to_dict()
            if result.username:
                response_data['link'] = f"https://t.me/{result.username}"
            
            return self.response.success(response_data, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_chat_events(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/events - Получить события чата"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            limit = self._get_int_query_param(event, 'limit', 50)
            offset = self._get_int_query_param(event, 'offset', 0)
            event_type = self._get_query_param(event, 'type')
            user_id = self._get_query_param(event, 'user_id')
            from_date = self._get_query_param(event, 'from')
            to_date = self._get_query_param(event, 'to')
            
            limit = min(limit, 100)
            
            async with RequestContext() as ctx:
                has_access = await self.service.participant_cache.check_access(
                    chat_id, 
                    user['user_id'], 
                    session=ctx.session
                )
                if not has_access:
                    raise PermissionError("You don't have access to this chat")
                
                repo = EventRepository(ctx.session)
                events = await repo.list_by_chat(
                    chat_id=chat_id,
                    limit=limit,
                    offset=offset,
                    event_type=event_type,
                    user_id=user_id,
                    from_date=from_date,
                    to_date=to_date
                )
                
                event_dicts = [e.to_dict() for e in events]
                
                return self.response.success({
                    'events': event_dicts,
                    'count': len(event_dicts)
                })
                
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_mark_as_read(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/read - Отметить как прочитанные"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            body = self._parse_body(event)
            message_id = body.get('message_id')
            
            if not message_id:
                raise ValidationError("message_id is required")
            
            message_id = safe_int(message_id)
            if not message_id:
                raise ValidationError("Invalid message_id format")
            
            async with RequestContext() as ctx:
                result = await self.service.mark_as_read(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    message_id=message_id
                )
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_unread_counts(self, event: Dict, user: Dict) -> Dict:
        """GET /users/me/unread - Непрочитанные сообщения"""
        try:
            result = await self.service.get_all_unread_counts(user['user_id'])
            
            total = sum(item['unread_count'] for item in result)
            
            return self.response.success({
                'chats': result,
                'total_unread': total,
                'count': len(result)
            }, 200)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_mark_all_as_read(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/read-all - Отметить все как прочитанные"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            async with RequestContext() as ctx:
                result = await self.service.mark_all_as_read(
                    chat_id=chat_id,
                    user_id=user['user_id']
                )
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_delete_dialog(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """DELETE /chats/{chatId}/dialog - Удалить диалог"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            delete_for_all = self._get_bool_query_param(event, 'for_all', False)
            
            async with RequestContext() as ctx:
                result = await self.service.delete_dialog(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    delete_for_all=delete_for_all
                )
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_restore_dialog(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/dialog/restore - Восстановить диалог"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            async with RequestContext() as ctx:
                result = await self.service.restore_dialog(
                    chat_id=chat_id,
                    user_id=user['user_id']
                )
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_hidden_dialogs(self, event: Dict, user: Dict) -> Dict:
        """
        GET /users/me/dialogs/hidden - Скрытые диалоги
        """
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            
            limit = self._get_int_query_param(event, 'limit', 50)
            cursor = self._get_cursor(event)
            
            limit = min(limit, 100)
            
            async with RequestContext() as ctx:
                chats, next_cursor = await self.service.get_hidden_dialogs(
                    user_id=user_id,
                    limit=limit,
                    cursor=cursor,
                    session=ctx.session
                )
                
                return self.response.success({
                    'dialogs': [c.to_dict() for c in chats],
                    'count': len(chats),
                    'next_cursor': next_cursor
                })
                
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_create_join_request(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/join-requests - Создать заявку"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            body = self._parse_body(event)
            invite_code = body.get('invite_code')
            
            async with RequestContext() as ctx:
                result = await self.service.create_join_request(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    invite_code=invite_code
                )
            
            return self.response.success(result, 201)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_join_requests(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/join-requests - Список заявок"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            status = self._get_query_param(event, 'status')
            limit = self._get_int_query_param(event, 'limit', 50)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            limit = min(limit, 100)
            
            async with RequestContext() as ctx:
                requests = await self.service.list_join_requests(
                    chat_id=chat_id,
                    admin_id=user['user_id'],
                    status=status,
                    limit=limit,
                    offset=offset
                )
            
            return self.response.success({
                'requests': [r.to_dict() for r in requests],
                'count': len(requests)
            })
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_approve_join_request(self, event: Dict, user: Dict, chat_id: int, request_id: int) -> Dict:
        """POST /chats/{chatId}/join-requests/{requestId}/approve - Одобрить"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            request_id = self._validate_message_id(request_id)
            
            async with RequestContext() as ctx:
                result = await self.service.approve_join_request(
                    chat_id=chat_id,
                    request_id=request_id,
                    admin_id=user['user_id']
                )
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_reject_join_request(self, event: Dict, user: Dict, chat_id: int, request_id: int) -> Dict:
        """POST /chats/{chatId}/join-requests/{requestId}/reject - Отклонить"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            request_id = self._validate_message_id(request_id)
            
            body = self._parse_body(event)
            reason = body.get('reason')
            
            async with RequestContext() as ctx:
                result = await self.service.reject_join_request(
                    chat_id=chat_id,
                    request_id=request_id,
                    admin_id=user['user_id'],
                    reason=reason
                )
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_my_join_requests(self, event: Dict, user: Dict) -> Dict:
        """GET /users/me/join-requests - Свои заявки"""
        try:
            limit = self._get_int_query_param(event, 'limit', 50)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            limit = min(limit, 100)
            
            async with RequestContext() as ctx:
                requests = await self.service.get_my_join_requests(
                    user_id=user['user_id'],
                    limit=limit,
                    offset=offset
                )
            
            return self.response.success({
                'requests': [r.to_dict() for r in requests],
                'count': len(requests)
            })
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_enable_reactions(self, event: Dict, user: Dict, channel_id: int) -> Dict:
        """POST /chats/{channelId}/enable-reactions - Включить реакции"""
        try:
            channel_id = self._validate_chat_id(channel_id)
            
            body = self._parse_body(event)
            settings = body.get('settings', {})
            
            async with RequestContext() as ctx:
                result = await self.service.enable_reactions(
                    channel_id=channel_id,
                    user_id=user['user_id'],
                    settings=settings
                )
            
            return self.response.success(result.to_dict(), 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_disable_reactions(self, event: Dict, user: Dict, channel_id: int) -> Dict:
        """POST /chats/{channelId}/disable-reactions - Выключить реакции"""
        try:
            channel_id = self._validate_chat_id(channel_id)
            
            async with RequestContext() as ctx:
                result = await self.service.disable_reactions(
                    channel_id=channel_id,
                    user_id=user['user_id']
                )
            
            return self.response.success(result.to_dict(), 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_reactions_settings(self, event: Dict, user: Dict, channel_id: int) -> Dict:
        """GET /chats/{channelId}/reactions-settings - Настройки реакций"""
        try:
            channel_id = self._validate_chat_id(channel_id)
            
            async with RequestContext() as ctx:
                result = await self.service.get_reactions_settings(
                    channel_id=channel_id,
                    user_id=user['user_id']
                )
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_join_by_link(self, event: Dict, user: Dict, invite_code: str) -> Dict:
        """POST /join/{inviteCode} - Присоединиться по ссылке"""
        try:
            idempotency_key = self._get_idempotency_key(event)
            
            async with RequestContext() as ctx:
                result = await self.service.join_by_link(
                    invite_code=invite_code,
                    user_id=user['user_id'],
                    idempotency_key=idempotency_key
                )
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_enable_comments(self, event: Dict, user: Dict, channel_id: int) -> Dict:
        """POST /chats/{channelId}/enable-comments - Включить комментарии"""
        try:
            channel_id = self._validate_chat_id(channel_id)

            body = self._parse_body(event)
            settings = body.get('settings', {})

            async with RequestContext() as ctx:
                result = await self.service.enable_comments(
                    channel_id=channel_id,
                    user_id=user['user_id'],
                    settings=settings
                )

            return self.response.success(result.to_dict(), 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    async def handle_disable_comments(self, event: Dict, user: Dict, channel_id: int) -> Dict:
        """POST /chats/{channelId}/disable-comments - Отключить комментарии"""
        try:
            channel_id = self._validate_chat_id(channel_id)

            async with RequestContext() as ctx:
                result = await self.service.disable_comments(
                    channel_id=channel_id,
                    user_id=user['user_id']
                )

            return self.response.success(result.to_dict(), 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_create_chat(self, event: Dict, user: Dict) -> Dict:
        """POST /chats - создать чат/канал"""
        try:
            user_id = self._validate_user_id(user.get('user_id'))
            body = self._parse_body(event)
            
            chat_type = body.get('type')
            if chat_type not in ['private', 'group', 'channel']:
                raise ValidationError("Invalid chat type")
            
            # Для приватных чатов - проверяем существование
            if chat_type == 'private':
                recipient_id = body.get('recipient_id')
                if not recipient_id:
                    raise ValidationError("recipient_id is required for private chat")
                
                recipient_id = self._validate_user_id(recipient_id)
                
                if user_id == recipient_id:
                    raise ValidationError("Cannot create chat with yourself")
                
                # Получаем или создаём приватный чат через сервис
                async with RequestContext() as ctx:
                    chat = await self.service.get_or_create_private_chat(
                        user_id=user_id,
                        recipient_id=recipient_id,
                        session=ctx.session
                    )
                    
                    # Получаем информацию об участнике
                    participant = await self.service.participant_cache.get_participant(
                        chat.id, user_id, session=ctx.session
                    )
                    chat._participant_info = participant
                    
                    return self.response.success(chat.to_dict(), 201)
            
            # Для групповых чатов и каналов - существующая логика
            async with RequestContext() as ctx:
                from handlers.chat_handler import Chat, ChatRepository, ParticipantRepository, ChatParticipant
                
                chat_repo = ChatRepository(ctx.session)
                participant_repo = ParticipantRepository(ctx.session)
                
                chat_id = uuid.uuid4().int & (2**64 - 1)
                now = datetime.utcnow()
                
                chat = Chat(
                    id=chat_id,
                    type=chat_type,
                    title=body.get('title'),
                    description=body.get('description'),
                    username=body.get('username'),
                    is_public=not body.get('is_private', False),
                    join_moderation=body.get('join_moderation', False),
                    max_members=body.get('max_members', 100),
                    slow_mode_interval=body.get('slow_mode_interval', 0),
                    created_by=user_id,
                    owner_id=user_id,
                    members_count=1,
                    created_at=now
                )
                
                created_chat = await chat_repo.create(chat)
                if not created_chat:
                    raise DatabaseError("Failed to create chat")
                
                participant = ChatParticipant(
                    chat_id=chat_id,
                    user_id=user_id,
                    role='owner',
                    role_order=1,
                    joined_at=now,
                    is_active=True,
                    unread_count=0,
                    region='ru-central1',
                    is_hidden=False,
                    show_in_profile=True
                )
                
                await participant_repo.create(participant)
                created_chat._participant_info = participant
                
                logger.info(f"✅ Chat created: {chat_id}")
                return self.response.success(created_chat.to_dict(), 201)
                
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            logger.error(f"Error creating chat: {e}", exc_info=True)
            return await self.handle_error(e, event)
    @measure_time
    async def handle_get_chat(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] GET chat {chat_id}")
        
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            async with RequestContext() as ctx:
                chat = await self.service.get_chat(chat_id, user['user_id'], session=ctx.session)
                online_estimate = await self.service._get_online_estimate(chat_id)
            
            result = chat.to_dict()
            result['online_estimate'] = online_estimate
            
            logger.info(f"✅ [REQ {request_id}] Chat found: {chat_id}")
            return self.response.success(result)
            
        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Get chat error: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    
    @measure_time
    async def handle_update_chat(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] UPDATE chat {chat_id}")

        try:
            chat_id = self._validate_chat_id(chat_id)
            body = self._parse_body(event)

            # Если avatar_url — это base64, загружаем в Object Storage и заменяем на URL
            avatar_raw = body.get('avatar_url', '')
            if avatar_raw and avatar_raw.startswith('data:image'):
                if ',' in avatar_raw:
                    header, b64 = avatar_raw.split(',', 1)
                    content_type = header.split(';')[0].replace('data:', '')
                else:
                    b64, content_type = avatar_raw, 'image/jpeg'
                try:
                    image_bytes = base64.b64decode(b64)
                except Exception:
                    raise ValidationError("avatar_url: invalid base64")
                ext = content_type.split('/')[-1] if '/' in content_type else 'jpeg'
                filename = f"chat_{chat_id}_avatar_{uuid.uuid4().hex[:8]}.{ext}"
                loop = asyncio.get_event_loop()
                _bytes, _ct, _fn = image_bytes, content_type, filename
                upload_result = await loop.run_in_executor(
                    None,
                    lambda: storage.upload_file(
                        file_data=_bytes,
                        content_type=_ct,
                        filename=_fn,
                        chat_id=chat_id,
                        user_id=user['user_id'],
                    )
                )
                body['avatar_url'] = upload_result['url']
                logger.info(f"✅ [REQ {request_id}] Avatar uploaded → {body['avatar_url']}")

            async with RequestContext() as ctx:
                result = await self.service.update_chat(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    updates=body,
                    session=ctx.session
                )

                logger.info(f"✅ [REQ {request_id}] Chat updated: {chat_id}")
                return self.response.success(result.to_dict())

        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Update chat error: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_delete_chat(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """DELETE /chats/{chatId} - Удалить чат"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            permanent = self._get_bool_query_param(event, 'permanent', False)
            
            async with RequestContext() as ctx:
                result = await self.service.delete_chat(chat_id, user['user_id'], permanent)
            
            return self.response.success(result)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_archive_chat(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/archive - Архивировать чат"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            async with RequestContext() as ctx:
                result = await self.service.archive_chat(chat_id, user['user_id'])
            
            return self.response.success(result.to_dict())
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_unarchive_chat(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/unarchive - Разархивировать чат"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            async with RequestContext() as ctx:
                result = await self.service.unarchive_chat(chat_id, user['user_id'])
            
            return self.response.success(result.to_dict())
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_join_chat(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/join - Присоединиться к чату"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            body = self._parse_body(event)
            invite_code = body.get('invite_code')
            idempotency_key = self._get_idempotency_key(event)
            
            async with RequestContext() as ctx:
                result = await self.service.join_chat(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    invite_code=invite_code,
                    idempotency_key=idempotency_key
                )
            
            return self.response.success(result.to_dict(), 201, event=event)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_admin_add_member(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/members/add - Добавить участника (владелец/админ)"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            body = self._parse_body(event)
            target_user_id = body.get('user_id')
            if not target_user_id:
                raise ValidationError("user_id is required")
            target_user_id = self._validate_user_id(target_user_id)

            async with RequestContext() as ctx:
                result = await self.service.admin_add_member(
                    chat_id=chat_id,
                    admin_id=user['user_id'],
                    target_user_id=target_user_id,
                    session=ctx.session
                )

            return self.response.success({'user_id': target_user_id, 'chat_id': chat_id}, 201)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    async def handle_leave_chat(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/leave - Покинуть чат"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            async with RequestContext() as ctx:
                result = await self.service.leave_chat(chat_id, user['user_id'])
            
            return self.response.success(result)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_change_role(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """PUT /chats/{chatId}/members/{userId}/role - Изменить роль"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            path_params = event.get('pathParams', {})
            target_user_id = path_params.get('userId')
            
            if not target_user_id:
                parts = event.get('path', '').split('/')
                if len(parts) >= 5:
                    target_user_id = parts[3]
            
            if not target_user_id:
                raise ValidationError("User ID required")
            
            target_user_id = self._validate_user_id(target_user_id)
            
            body = self._parse_body(event)
            new_role = body.get('role')
            
            if not new_role:
                raise ValidationError("Role is required")
            
            async with RequestContext() as ctx:
                result = await self.service.change_role(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    target_user_id=target_user_id,
                    new_role=new_role
                )
            
            return self.response.success(result)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_transfer_ownership(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/transfer-ownership - Передать права"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            body = self._parse_body(event)
            new_owner_id = body.get('new_owner_id')
            
            if not new_owner_id:
                raise ValidationError("new_owner_id is required")
            
            new_owner_id = self._validate_user_id(new_owner_id)
            
            async with RequestContext() as ctx:
                result = await self.service.transfer_ownership(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    new_owner_id=new_owner_id
                )
            
            return self.response.success(result)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_ban_user(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/bans - Забанить пользователя"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            body = self._parse_body(event)
            target_user_id = body.get('user_id')
            
            if not target_user_id:
                raise ValidationError("user_id is required")
            
            target_user_id = self._validate_user_id(target_user_id)
            
            ban_type = body.get('type', 'ban')
            reason = body.get('reason')
            duration_minutes = body.get('duration_minutes')
            permanent = body.get('permanent', False)
            
            async with RequestContext() as ctx:
                result = await self.service.ban_user(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    target_user_id=target_user_id,
                    ban_type=ban_type,
                    reason=reason,
                    duration_minutes=duration_minutes,
                    permanent=permanent
                )
            
            return self.response.success(result.to_dict(), 201, event=event)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_link_discussion_chat(self, event: Dict, user: Dict, channel_id: int) -> Dict:
        """POST /chats/{channelId}/link-chat - Привязать обсуждение"""
        try:
            channel_id = self._validate_chat_id(channel_id)
            
            body = self._parse_body(event)
            discussion_chat_id = body.get('discussion_chat_id')
            settings = body.get('settings', {})
            
            if not discussion_chat_id:
                raise ValidationError("discussion_chat_id is required")
            
            discussion_chat_id = self._validate_chat_id(discussion_chat_id)
            
            async with RequestContext() as ctx:
                result = await self.service.link_discussion_chat(
                    channel_id=channel_id,
                    discussion_chat_id=discussion_chat_id,
                    user_id=user['user_id'],
                    settings=settings
                )
            
            return self.response.success(result.to_dict(), 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_unlink_discussion_chat(self, event: Dict, user: Dict, channel_id: int) -> Dict:
        """DELETE /chats/{channelId}/link-chat - Отвязать обсуждение"""
        try:
            channel_id = self._validate_chat_id(channel_id)
            
            async with RequestContext() as ctx:
                result = await self.service.unlink_discussion_chat(
                    channel_id=channel_id,
                    user_id=user['user_id']
                )
            
            return self.response.success(result.to_dict(), 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_discussion_chat(self, event: Dict, user: Dict, channel_id: int) -> Dict:
        """GET /chats/{channelId}/discussion - Получить чат обсуждения"""
        try:
            channel_id = self._validate_chat_id(channel_id)
            
            async with RequestContext() as ctx:
                discussion_chat = await self.service.get_discussion_chat(
                    channel_id=channel_id,
                    user_id=user['user_id']
                )
            
            if not discussion_chat:
                return self.response.success(None, 200)
            
            return self.response.success(discussion_chat.to_dict(), 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_update_discussion_settings(self, event: Dict, user: Dict, channel_id: int) -> Dict:
        """PUT /chats/{channelId}/discussion-settings - Обновить настройки"""
        try:
            channel_id = self._validate_chat_id(channel_id)
            
            body = self._parse_body(event)
            settings = body.get('settings', {})
            
            if not settings:
                raise ValidationError("settings is required")
            
            async with RequestContext() as ctx:
                result = await self.service.update_discussion_settings(
                    channel_id=channel_id,
                    user_id=user['user_id'],
                    settings=settings
                )
            
            return self.response.success(result.to_dict(), 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_unban_user(self, event: Dict, user: Dict, chat_id: int, ban_id: int) -> Dict:
        """DELETE /chats/{chatId}/bans/{banId} - Разбанить"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            ban_id = self._validate_message_id(ban_id)
            
            async with RequestContext() as ctx:
                result = await self.service.unban_user(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    ban_id=ban_id
                )
            
            return self.response.success(result)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_bans(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/bans - Список банов"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            active_only = self._get_bool_query_param(event, 'active_only', True)
            limit = self._get_int_query_param(event, 'limit', 50)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            limit = min(limit, 100)
            
            async with RequestContext() as ctx:
                bans = await self.service.get_chat_bans(
                    chat_id=chat_id,
                    user_id=user_id,
                    active_only=active_only,
                    limit=limit,
                    offset=offset,
                    session=ctx.session
                )
                
                return self.response.success({
                    'bans': [b.to_dict() for b in bans],
                    'count': len(bans)
                })
                
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_create_invite(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/invites - Создать приглашение"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            body = self._parse_body(event)
            
            expires_in_hours = body.get('expires_in_hours', 24)
            max_uses = body.get('max_uses', 0)
            requires_approval = body.get('requires_approval', False)
            default_role = body.get('default_role', 'member')
            
            async with RequestContext() as ctx:
                result = await self.service.create_invite(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    expires_in_hours=expires_in_hours,
                    max_uses=max_uses,
                    requires_approval=requires_approval,
                    default_role=default_role
                )
            
            return self.response.success(result, 201)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_invite(self, event: Dict, user: Dict, invite_code: str) -> Dict:
        """GET /invites/{inviteCode} - Информация о приглашении"""
        try:
            async with RequestContext() as ctx:
                result = await self.service.get_invite(invite_code)
                
                chat = await self.service.get_chat(result.chat_id, user['user_id'])
            
            response_data = result.to_dict()
            response_data['chat'] = {
                'id': str(chat.id),
                'title': chat.title,
                'type': chat.type,
                'is_public': chat.is_public,
                'avatar_url': chat.avatar_url,
                'members_count': chat.members_count
            }
            
            return self.response.success(response_data)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_revoke_invite(self, event: Dict, user: Dict, chat_id: int, invite_id: int) -> Dict:
        """DELETE /chats/{chatId}/invites/{inviteId} - Отозвать приглашение"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            invite_id = self._validate_message_id(invite_id)
            
            async with RequestContext() as ctx:
                result = await self.service.revoke_invite(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    invite_id=invite_id
                )
            
            return self.response.success(result)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_list_invites(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/invites - Список приглашений"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            active_only = self._get_bool_query_param(event, 'active_only', True)
            limit = self._get_int_query_param(event, 'limit', 50)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            limit = min(limit, 100)
            
            async with RequestContext() as ctx:
                invites = await self.service.list_invites(
                    chat_id=chat_id,
                    user_id=user['user_id'],
                    active_only=active_only,
                    limit=limit,
                    offset=offset
                )
            
            return self.response.success({
                'invites': [i.to_dict() for i in invites],
                'count': len(invites)
            })
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @measure_time
    async def handle_get_user_chats(self, event: Dict, user: Dict) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] GET user chats for {user.get('user_id')}")
        
        try:
            limit = self._get_int_query_param(event, 'limit', 50)
            cursor = self._get_cursor(event)
            include_hidden = self._get_bool_query_param(event, 'include_hidden', False)
            
            limit = min(limit, 100)
            
            chats, next_cursor = await self.service.get_user_chats(
                user_id=user['user_id'],
                limit=limit,
                cursor=cursor,
                include_hidden=include_hidden
            )
            
            chat_dicts = []
            for chat in chats:
                chat_dict = chat.to_dict()
                if hasattr(chat, '_participant_info') and chat._participant_info:
                    chat_dict['participant'] = chat._participant_info.to_dict()
                chat_dicts.append(chat_dict)
            
            logger.info(f"✅ [REQ {request_id}] Returned {len(chats)} chats")
            return self.response.success({
                'chats': chat_dicts,
                'count': len(chat_dicts),
                'next_cursor': next_cursor
            })
            
        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Get user chats error: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_get_user_public_chats(self, event: Dict, user: Dict, target_user_id: str) -> Dict:
        """GET /users/{userId}/public-chats - Публичные чаты пользователя"""
        try:
            if not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', target_user_id.lower()):
                raise ValidationError("Invalid user ID format")
            
            async with RequestContext() as ctx:
                result = await self.service.get_user_public_chats(
                    target_user_id=target_user_id,
                    requesting_user_id=user['user_id']
                )
            
            return self.response.success(result, 200)
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @measure_time
    async def handle_get_chat_members(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] GET members for chat {chat_id}")
        
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            limit = self._get_int_query_param(event, 'limit', 100)
            cursor = self._get_cursor(event)
            
            limit = min(limit, 200)
            
            async with RequestContext() as ctx:
                members, next_cursor = await self.service.get_chat_members(
                    chat_id=chat_id,
                    user_id=user_id,
                    limit=limit,
                    cursor=cursor,
                    session=ctx.session
                )
                
                logger.info(f"✅ [REQ {request_id}] Returned {len(members)} members")
                return self.response.success({
                    'members': [m.to_dict() for m in members],
                    'count': len(members),
                    'next_cursor': next_cursor
                })
                
        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Get members error: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    
    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_public_chats(self, event: Dict, user: Dict) -> Dict:
        """GET /chats/public - Публичные чаты"""
        try:
            limit = self._get_int_query_param(event, 'limit', 50)
            cursor = self._get_cursor(event)
            
            limit = min(limit, 100)
            
            async with RequestContext() as ctx:
                repo = ChatRepository(ctx.session)
                chats, next_cursor = await repo.list_public(limit=limit, cursor=cursor)
            
            chat_dicts = [c.to_dict() for c in chats]
            
            return self.response.success({
                'chats': chat_dicts,
                'count': len(chat_dicts),
                'next_cursor': next_cursor
            })
        except Exception as e:
            return await self.handle_error(e, event)
    
    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_search_chats(self, event: Dict, user: Dict) -> Dict:
        """GET /chats/search - Поиск чатов"""
        try:
            query = self._get_query_param(event, 'q')
            if not query:
                raise ValidationError("Search query 'q' is required")
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = self._get_cursor(event)
            
            limit = min(limit, 50)
            
            async with RequestContext() as ctx:
                chats, next_cursor = await self.service.search_chats(
                    query=query,
                    user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return self.response.success({
                'chats': [c.to_dict() for c in chats],
                'count': len(chats),
                'next_cursor': next_cursor,
                'query': query
            })
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
    
    @measure_time
    async def handle_get_chat_stats(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"🚀 [REQ {request_id}] GET stats for chat {chat_id}")
        
        try:
            chat_id = self._validate_chat_id(chat_id)
            user_id = self._validate_user_id(user.get('user_id'))
            
            async with RequestContext() as ctx:
                stats = await self.service.get_chat_stats(
                    chat_id=chat_id,
                    user_id=user_id,
                    session=ctx.session
                )
                
                logger.info(f"✅ [REQ {request_id}] Stats retrieved for chat {chat_id}")
                return self.response.success(stats, 200)
                
        except Exception as e:
            logger.error(f"❌ [REQ {request_id}] Get stats error: {e}", exc_info=True)
            return await self.handle_error(e, event)
    

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_pin_message(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """POST /chats/{chatId}/messages/{messageId}/pin - Закрепить сообщение"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            user_id = self._validate_user_id(user.get('user_id'))

            async with RequestContext() as ctx:
                result = await self.service.pin_message(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    session=ctx.session
                )

                return self.response.success(result, 200)

        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_unpin_message(self, event: Dict, user: Dict, chat_id: int, message_id: int) -> Dict:
        """DELETE /chats/{chatId}/messages/{messageId}/pin - Открепить конкретное сообщение"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            message_id = self._validate_message_id(message_id)
            
            async with RequestContext() as ctx:
                result = await self.service.unpin_message(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user['user_id']
                )
            
            return self.response.success(result, 200)
            
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_pinned_messages(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/pins - Получить все закрепленные сообщения"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            async with RequestContext() as ctx:
                result = await self.service.get_pinned_messages(
                    chat_id=chat_id,
                    user_id=user['user_id']
                )
            
            return self.response.success(result, 200)
            
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_reorder_pins(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """POST /chats/{chatId}/pins/reorder - Изменить порядок закрепленных сообщений"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            body = self._parse_body(event)
            message_ids = body.get('message_ids', [])
            
            if not message_ids:
                return self.response.error(
                    message="message_ids array is required",
                    code="validation_error",
                    status_code=400,
                    event=event
                )
            
            valid_ids = []
            for msg_id in message_ids:
                try:
                    valid_ids.append(self._validate_message_id(msg_id))
                except ValidationError:
                    continue
            
            if len(valid_ids) != len(message_ids):
                return self.response.error(
                    message="Invalid message_id format in array",
                    code="validation_error",
                    status_code=400,
                    event=event
                )
            
            async with RequestContext() as ctx:
                result = await self.service.reorder_pinned_messages(
                    chat_id=chat_id,
                    message_ids=valid_ids,
                    user_id=user['user_id']
                )
            
            return self.response.success(result, 200)
            
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=50, window=60)
    @measure_time
    async def handle_unpin_all(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """DELETE /chats/{chatId}/pins - Открепить все сообщения"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            async with RequestContext() as ctx:
                repo = PinnedMessageRepository(ctx.session)
                
                has_permission = await self.service.participant_cache.check_permission(
                    chat_id, user['user_id'], 'can_pin_messages'
                )
                if not has_permission:
                    raise PermissionError("You don't have permission to unpin messages")
                
                success = await repo.unpin_all(chat_id, user['user_id'])
                if not success:
                    raise DatabaseError("Failed to unpin all messages")
                
                return self.response.success({
                    'unpinned_all': True,
                    'chat_id': chat_id,
                    'message': 'All pinned messages removed'
                }, 200)
            
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)

    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_get_pinned_message(self, event: Dict, user: Dict, chat_id: int) -> Dict:
        """GET /chats/{chatId}/pin - Получить первое закрепленное сообщение (старая версия)"""
        try:
            chat_id = self._validate_chat_id(chat_id)
            
            async with RequestContext() as ctx:
                result = await self.service.get_pinned_messages(chat_id, user['user_id'])
            
            if result['pinned'] and len(result['pinned']) > 0:
                return self.response.success(result['pinned'][0], 200)
            else:
                return self.response.success(None, 200)
            
        except (ValidationError, PermissionError, NotFoundError) as e:
            return await self.handle_error(e, event)
        except Exception as e:
            return await self.handle_error(e, event)
chat_handler = ChatHandler()
