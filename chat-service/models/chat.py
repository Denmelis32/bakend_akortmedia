"""
Модели данных для чатов
Только структуры данных, без логики конвертации
Конвертация происходит в репозиториях с помощью utils/converters.py
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from datetime import datetime
from enum import Enum


# ============================================
# ENUMS - для типизации
# ============================================

class ChatType(Enum):
    """Типы чатов"""
    PRIVATE = "private"
    GROUP = "group"
    CHANNEL = "channel"


class ChatStatus(Enum):
    """Статусы чатов"""
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"
    BANNED = "banned"


class ParticipantRole(Enum):
    """Роли участников чата"""
    OWNER = "owner"
    ADMIN = "admin"
    MODERATOR = "moderator"
    MEMBER = "member"


class BanType(Enum):
    """Типы банов"""
    BAN = "ban"
    MUTE = "mute"
    KICK = "kick"
    WARNING = "warning"


class MessageType(Enum):
    """Типы сообщений"""
    TEXT = "text"
    IMAGE = "image"
    VIDEO = "video"
    FILE = "file"
    VOICE = "voice"
    STICKER = "sticker"
    GIF = "gif"
    SYSTEM = "system"


class EventType(Enum):
    """Типы событий"""
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
    MESSAGE_SENT = "message_sent"
    MESSAGE_EDITED = "message_edited"
    MESSAGE_DELETED = "message_deleted"
    REACTION_ADDED = "reaction_added"
    REACTION_REMOVED = "reaction_removed"
    SYSTEM = "system"
    ERROR = "error"
    WARNING = "warning"


class AttachmentType(Enum):
    """Типы вложений"""
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    FILE = "file"
    LINK = "link"


# ============================================
# OSNOVNYE MODELI - только данные
# ============================================

@dataclass
class Chat:
    """Модель чата - только данные, никакой логики"""
    
    # Все поля в том же порядке, как в таблице
    id: Optional[int]
    type: str
    subtype: Optional[str]
    title: str
    description: Optional[str]
    avatar_url: Optional[str]
    owner_id: str
    created_by: str
    created_at: datetime
    updated_at: Optional[datetime]
    is_public: bool
    join_moderation: bool
    max_members: int
    slow_mode_interval: int
    is_active: bool
    is_archived: bool
    is_deleted: bool
    deleted_at: Optional[datetime]
    settings: Dict[str, Any]
    members_count: int
    messages_count: int
    online_estimate: int
    last_message_id: Optional[int]
    last_message_at: Optional[datetime]
    last_message_preview: Optional[str]
    last_message_sender_id: Optional[str]
    views_count: int
    version: int
    primary_region: str
    status: str


@dataclass
class ChatParticipant:
    """Модель участника чата - соответствует таблице chat_participants"""
    chat_id: int
    user_id: str  # в БД Uint64, в модели str
    role: str
    permissions: Optional[Dict[str, Any]]
    joined_at: datetime
    joined_method: str  # 'invite', 'join', 'added'
    join_event_id: Optional[int]
    is_active: bool
    left_at: Optional[datetime]
    left_event_id: Optional[int]
    mute_until: Optional[datetime]
    is_blocked: bool
    last_read_at: Optional[datetime]
    last_read_message_id: Optional[int]
    last_read_message_valid: bool
    last_active_at: Optional[datetime]
    unread_count: int
    version: int
    region: str


@dataclass
class ChatBan:
    """Модель бана - соответствует таблице chat_bans"""
    ban_id: Optional[int]
    chat_id: int
    user_id: str  # в БД Uint64, в модели str
    banned_by: str  # в БД Uint64, в модели str
    ban_type: str
    reason: Optional[str]
    reason_code: Optional[str]
    restrictions: Optional[Dict[str, Any]]
    banned_at: datetime
    expires_at: Optional[datetime]
    is_permanent: bool
    event_id: Optional[int]
    is_active: bool
    region: str


@dataclass
class ChatInvite:
    """Модель приглашения - соответствует таблице chat_invites"""
    invite_id: Optional[int]
    chat_id: int
    invite_code: str
    created_by: str  # в БД Uint64, в модели str
    created_at: datetime
    expires_at: Optional[datetime]
    max_uses: int
    used_count: int
    remaining_uses: int
    can_join: bool
    requires_approval: bool
    default_role: str
    is_active: bool
    deactivated_at: Optional[datetime]
    region: str


@dataclass
class ChatEvent:
    """Модель события - соответствует таблице chat_events"""
    event_id: Optional[int]
    chat_id: int
    event_type: str
    user_id: str  # в БД Uint64, в модели str
    user_role_at_time: Optional[str]
    target_id: Optional[int]
    target_type: Optional[str]
    payload: Dict[str, Any]
    created_at: datetime
    created_date: int  # YYYYMMDD для партиционирования
    idempotency_key: Optional[str]
    region: str


@dataclass
class ChatCache:
    """Модель кэша чата - соответствует таблице chat_cache"""
    chat_id: int
    popular_messages: List[Dict[str, Any]]
    online_count: int
    updated_at: datetime
    region: str


@dataclass
class IdempotencyKey:
    """Модель ключа идемпотентности - соответствует таблице idempotency_keys"""
    idempotency_key: str
    entity_type: str  # 'chat', 'message', 'join'
    entity_id: int
    chat_id: Optional[int]
    user_id: str  # в БД Uint64, в модели str
    created_at: datetime
    expires_at: Optional[datetime]


@dataclass
class MonitoringStats:
    """Модель статистики мониторинга - соответствует таблице monitoring_stats"""
    region: str
    total_chats: int
    total_messages: int
    avg_members: float
    updated_at: datetime


# ============================================
# MESSAGE MODELS - только данные
# ============================================

@dataclass
class Message:
    """Модель сообщения - соответствует таблице messages"""
    id: Optional[int]
    chat_id: int
    sender_id: str  # в БД Uint64, в модели str
    type: str
    content: Optional[str]
    created_at: datetime
    is_deleted: bool
    is_edited: bool
    reply_to: Optional[int]
    attachments: List[Dict[str, Any]]
    mentions: List[str]
    reactions: Dict[str, int]
    views: int
    forwards: int
    edit_history: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    is_pinned: bool
    region: str


@dataclass
class Attachment:
    """Модель вложения - соответствует таблице attachments"""
    attachment_id: Optional[int]
    message_id: int
    type: str
    url: str
    file_name: str
    file_size: int
    mime_type: str
    uploaded_by: str  # в БД Uint64, в модели str
    uploaded_at: datetime
    preview_url: Optional[str]
    width: Optional[int]
    height: Optional[int]
    duration: Optional[int]
    metadata: Dict[str, Any]
    region: str


@dataclass
class Draft:
    """Модель черновика - соответствует таблице drafts"""
    chat_id: int
    user_id: str  # в БД Uint64, в модели str
    content: Optional[str]
    attachments: List[Dict[str, Any]]
    reply_to: Optional[int]
    updated_at: datetime
    has_auto_save: bool
    region: str


@dataclass
class SavedMessage:
    """Модель сохраненного сообщения - соответствует таблице saved_messages"""
    user_id: str  # в БД Uint64, в модели str
    message_id: int
    chat_id: int
    saved_at: datetime
    notes: Optional[str]
    collections: List[str]
    importance: int
    region: str


# ============================================
# USER MODELS - только данные
# ============================================

@dataclass
class Contact:
    """Модель контакта - соответствует таблице contacts"""
    user_id: str  # в БД Uint64, в модели str
    contact_id: str  # в БД Uint64, в модели str
    first_name: Optional[str]
    last_name: Optional[str]
    phone: Optional[str]
    added_at: datetime
    is_favorite: bool
    is_blocked: bool
    last_interaction_at: Optional[datetime]
    last_message_preview: Optional[str]
    nickname: Optional[str]
    region: str


@dataclass
class Block:
    """Модель блокировки - соответствует таблице blocks"""
    user_id: str  # в БД Uint64, в модели str
    blocked_id: str  # в БД Uint64, в модели str
    blocked_at: datetime
    reason: Optional[str]
    expires_at: Optional[datetime]
    region: str


# ============================================
# VIEW MODELS - для API ответов (опционально)
# ============================================

@dataclass
class ChatStats:
    """Статистика чата - для API ответов"""
    chat_id: int
    total_messages: int
    total_members: int
    active_members_today: int
    active_members_week: int
    active_members_month: int
    messages_per_day: float
    messages_per_user: float
    top_senders: List[Dict[str, Any]]
    top_hashtags: List[Dict[str, Any]]
    activity_by_hour: Dict[int, int]
    activity_by_day: Dict[str, int]
    created_at: Optional[datetime]
    last_message_at: Optional[datetime]


@dataclass
class ChatWithRole(Chat):
    """Чат с ролью пользователя - для API ответов"""
    user_role: Optional[str]
    unread_count: int = 0
    last_read_at: Optional[datetime] = None


@dataclass
class InviteWithChat(ChatInvite):
    """Приглашение с информацией о чате - для API ответов"""
    chat: Dict[str, Any] = field(default_factory=dict)  # id, title, type, is_public, avatar_url, members_count
