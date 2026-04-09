"""
FEED HANDLER v6.2.0 - КУРСОРНАЯ ПАГИНАЦИЯ
- Все эндпоинты используют cursor вместо offset
- Единый формат ответа {items: [], next_cursor: str, has_more: bool}
- Быстрая и стабильная пагинация
"""
import os
import json
import uuid
import re
import asyncio
import base64
import time
from app.handlers.common import send_ws
import io
import hashlib
import concurrent.futures
from PIL import Image
from app.utils.storage import storage
from typing import Dict, Any, Optional, List, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
from pydantic import BaseModel, Field, validator

from app.handlers.common import (
    RequestContext,
    UnitOfWork,
    BaseRepository,
    
    TransactionAwareRepository,
    UserCache,
    FeedCache,
    PostCache,
    ReactionCache,
    CommentCache, 
    RepostCache,
    cache,
    background_worker,
    background_task,
    SessionMetrics,
    Metrics,
    IdempotencyKey,
    IdempotencyRepository, 
    retry,
    rate_limit,
    measure_time,
    AppError,
    ValidationError,
    PermissionError,
    NotFoundError,
    RateLimitError,
    DatabaseError,
    to_timestamp,
    from_timestamp,
    safe_int,
    safe_str,
    safe_b64decode,
    validate_idempotency_key,
    validate_uuid,
    time_ago,
    chunk_list,
    logger,
    BaseHandler,
    common_config,
    WebSocketManager,  # 👈 Добавить
    send_ws            # 👈 Добавить
)

from app.middleware.auth import auth

# ============================================
# КОНФИГУРАЦИЯ ЛЕНТЫ
# ============================================

@dataclass
class FeedConfig:
    """Конфигурация для ленты новостей"""
    MAX_CONTENT_LENGTH: int = 2000
    MIN_CONTENT_LENGTH: int = 4
    MAX_TITLE_LENGTH: int = 200
    MAX_COMMENT_LENGTH: int = 1000
    MIN_COMMENT_LENGTH: int = 1
    MAX_IMAGES_PER_POST: int = 10
    MAX_IMAGE_SIZE_MB: int = 10
    ALLOWED_IMAGE_TYPES: List[str] = field(default_factory=lambda: ['image/jpeg', 'image/png', 'image/gif', 'image/webp'])
    CACHE_TTL_FEED: int = 60
    CACHE_TTL_POST: int = 300
    CACHE_TTL_PROFILE: int = 300
    TRENDING_DAYS: int = 7
    DEFAULT_FEED_LIMIT: int = 20
    MAX_FEED_LIMIT: int = 100
    ALLOWED_VISIBILITY: List[str] = field(default_factory=lambda: ['public', 'private', 'followers'])
    SUPPORTED_LANGUAGES: List[str] = field(default_factory=lambda: ['ru', 'en', 'kk'])
    ALLOWED_NOTIFICATION_TYPES: List[str] = field(default_factory=lambda: ['like', 'comment', 'reply', 'follow', 'repost', 'mention'])
    ALLOWED_REPORT_REASONS: List[str] = field(default_factory=lambda: ['spam', 'abuse', 'hate_speech', 'violence', 'copyright', 'other'])
    
    # Rate limits
    LIKE_RATE_LIMIT: int = 10
    COMMENT_RATE_LIMIT: int = 5
    POST_RATE_LIMIT: int = 2
    REPOST_RATE_LIMIT: int = 5
    RATE_LIMIT_PERIOD: int = 60
    
    # Batch sizes
    BATCH_SIZE_LIKES: int = 100
    BATCH_SIZE_BOOKMARKS: int = 100
    BATCH_SIZE_HASHTAGS: int = 100
    BATCH_SIZE_USERS: int = 50
    BATCH_SIZE_MENTIONS: int = 100
    
    # Thread pool for image processing
    IMAGE_PROCESSING_WORKERS: int = 4
    MAX_CONCURRENT_IMAGE_UPLOADS: int = 5

feed_config = FeedConfig()

# Thread pool for image processing
image_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=feed_config.IMAGE_PROCESSING_WORKERS,
    thread_name_prefix="image_processor"
)


# ============================================
# ТИПЫ РЕАКЦИЙ
# ============================================

class ReactionType(Enum):
    """Типы реакций на посты и комментарии"""
    LIKE = "like"
    LOVE = "love"
    LAUGH = "laugh"
    SURPRISE = "surprise"
    SAD = "sad"
    ANGRY = "angry"
    
    @classmethod
    def get_all(cls):
        return [item.value for item in cls]
    
    @classmethod
    def get_emoji(cls, reaction_type):
        emoji_map = {
            "like": "👍",
            "love": "❤️",
            "laugh": "😂",
            "surprise": "😮",
            "sad": "😢",
            "angry": "😡"
        }
        return emoji_map.get(reaction_type, "👍")
    
    @classmethod
    def get_display_name(cls, reaction_type):
        names = {
            "like": "Нравится",
            "love": "Восхищение",
            "laugh": "Смешно",
            "surprise": "Удивительно",
            "sad": "Грустно",
            "angry": "Злюсь"
        }
        return names.get(reaction_type, "Нравится")


# ============================================
# ТИПЫ ЛЕНТЫ
# ============================================

class FeedType(Enum):
    """Типы ленты"""
    FOR_YOU = "for_you"
    POPULAR = "popular"
    FRESH = "fresh"
    FOLLOWING = "following"
    TRENDING = "trending"


# ============================================
# МОДЕЛИ ДЛЯ API (Pydantic)
# ============================================

class Author(BaseModel):
    """Модель автора поста"""
    id: str
    username: str
    display_name: str = ""
    avatar_url: Optional[str] = None  # Добавлено поле для аватарки
    is_verified: bool = False
    is_following: bool = False
    
    @classmethod
    def from_token(cls, user_data: Dict) -> 'Author':
        """Создать автора из данных JWT токена"""
        user_id = user_data.get('user_id') or user_data.get('sub', '')
        username = user_data.get('username', '') or f"user_{user_id[:8]}"
        display_name = user_data.get('display_name', '') or username
        
        return cls(
            id=user_id,
            username=username,
            display_name=display_name,
            avatar_url=user_data.get('avatar_url'),  # Берем из токена, если есть
            is_verified=user_data.get('is_verified', False) or user_data.get('verified', False),
            is_following=False  # По умолчанию не подписан
        )
    
    @classmethod
    def from_db(cls, user_data: Optional[Dict]) -> 'Author':
        """Создать автора из данных, полученных из БД"""
        if not user_data:
            return cls(
                id="unknown", 
                username="unknown", 
                display_name="Unknown User",
                avatar_url=None,
                is_verified=False,
                is_following=False
            )
        
        # Получаем display_name (приоритет: display_name > first_name+last_name > username)
        display_name = user_data.get('display_name', '') or user_data.get('username', '')
        
        # Если есть first_name и last_name, но нет display_name, собираем из них
        if not display_name and user_data.get('first_name') and user_data.get('last_name'):
            display_name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
        
        return cls(
            id=user_data.get('id') or user_data.get('user_id', 'unknown'),
            username=user_data.get('username', ''),
            display_name=display_name,
            avatar_url=user_data.get('avatar_url'),  # ← ВАЖНО: берем avatar_url из БД!
            is_verified=user_data.get('is_verified', False),
            is_following=False  # Будет установлено отдельно при проверке подписок
        )
    
    class Config:
        """Конфигурация Pydantic модели"""
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat() + "Z" if v else None
        }


class Interactions(BaseModel):
    """Модель взаимодействий пользователя с постом"""
    is_liked: bool = False
    is_reposted: bool = False 
    is_bookmarked: bool = False
    is_owner: bool = False


class ReactionCount(BaseModel):
    """Количество реакций по типам"""
    type: str
    count: int
    emoji: str
    display_name: str
    user_reacted: bool = False


class Reaction(BaseModel):
    """Модель реакции"""
    id: str
    user_id: str
    entity_type: str
    entity_id: str
    reaction_type: str
    created_at: str
    time_ago: str
    user: Optional[Author] = None


class ReactionToggle(BaseModel):
    """Модель для переключения реакции"""
    entity_type: str
    entity_id: str
    reaction_type: str = "like"


class Post(BaseModel):
    """Модель поста для ответа API"""
    id: str
    user_id: str
    title: Optional[str] = ""
    content: str
    content_preview: str
    media_urls: List[str] = []
    hashtags: List[Dict[str, Any]] = []
    comments_count: int = 0
    reposts_count: int = 0
    views_count: int = 0
    bookmarks_count: int = 0
    reactions_count: int = 0  # 👈 ДОБАВЛЕНО: общий счетчик всех реакций
    reactions_preview: List[Dict[str, Any]] = [] 
    reactions: List[ReactionCount] = []
    is_repost: bool = False
    original_post_id: Optional[str] = None
    repost_comment: Optional[str] = None
    is_pinned: bool = False
    is_edited: bool = False
    visibility: str = "public"
    language: str = "ru"
    sentiment_score: Optional[float] = None
    reading_time_minutes: Optional[int] = None
    created_at: str
    updated_at: Optional[str] = None
    published_at: Optional[str] = None
    scheduled_for: Optional[str] = None
    is_deleted: bool = False
    deleted_at: Optional[str] = None
    original_author: Optional[str] = None
    original_post: Optional[str] = None
    source_channel_id: Optional[str] = None
    author: Author
    interactions: Interactions

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat() + "Z" if v else None
        }

class PostCreate(BaseModel):
    """Модель для создания нового поста"""
    content: str = Field(..., min_length=4, max_length=2000)
    title: Optional[str] = Field(None, max_length=200)
    visibility: str = "public"
    images: List[str] = Field(default=[])
    
    @validator('visibility')
    def validate_visibility(cls, v):
        if v not in feed_config.ALLOWED_VISIBILITY:
            return 'public'
        return v
    
    @validator('content')
    def validate_content(cls, v):
        if not v or not v.strip():
            raise ValueError('Content cannot be empty')
        return v.strip()
    
    @validator('images')
    def validate_images(cls, v):
        if len(v) > feed_config.MAX_IMAGES_PER_POST:
            raise ValueError(f'Maximum {feed_config.MAX_IMAGES_PER_POST} images allowed')
        return v


class PostUpdate(BaseModel):
    """Модель для обновления поста"""
    title: Optional[str] = Field(None, max_length=200)
    content: Optional[str] = Field(None, min_length=4, max_length=2000)
    visibility: Optional[str] = None
    
    @validator('visibility')
    def validate_visibility(cls, v):
        if v is not None and v not in feed_config.ALLOWED_VISIBILITY:
            raise ValueError(f'visibility must be one of: {feed_config.ALLOWED_VISIBILITY}')
        return v


class Comment(BaseModel):
    """Модель комментария"""
    id: str
    post_id: str
    user_id: str
    parent_comment_id: Optional[str] = None
    content: str
    replies_count: int = 0  # оставляем только счетчик ответов
    created_at: str
    reactions_count: int = 0  # 👈 ДОБАВЛЕНО: общий счетчик всех реакций
    reactions: List[ReactionCount] = []
    reactions_preview: List[Dict] = [] 
    time_ago: str
    author: Author
    interactions: Interactions
    replies: List['Comment'] = []
    
    def dict(self, **kwargs):
        return {
            'id': self.id,
            'post_id': self.post_id,
            'user_id': self.user_id,
            'parent_comment_id': self.parent_comment_id,
            'content': self.content,
            'replies_count': self.replies_count,
            'created_at': self.created_at,
            'reactions_preview': self.reactions_preview,
            'reactions_count': self.reactions_count,  # 👈 ДОБАВЛЕНО
            'reactions': [r.dict() for r in self.reactions] if self.reactions else [],
            'time_ago': self.time_ago,
            'author': self.author.dict() if self.author else None,
            'interactions': self.interactions.dict() if self.interactions else None,
            'replies': [r.dict() for r in self.replies] if self.replies else []
        }


class CommentCreate(BaseModel):
    """Модель для создания комментария"""
    post_id: str
    content: str = Field(..., min_length=1, max_length=1000)
    parent_comment_id: Optional[str] = None


class LikeToggle(BaseModel):
    """Модель для переключения лайка"""
    post_id: str


class LikeResponse(BaseModel):
    """Ответ на переключение лайка"""
    post_id: str
    liked: bool
    likes_count: int


class Bookmark(BaseModel):
    """Модель закладки"""
    bookmark_id: str
    user_id: str
    post_id: str
    folder: str = "general"
    tags: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[str] = None


class BookmarkResponse(BaseModel):
    """Ответ с закладкой для API"""
    id: str
    post_id: str
    folder: str = "general"
    notes: str = ""
    created_at: str
    post: Post


class BookmarkToggle(BaseModel):
    """Модель для переключения закладки"""
    post_id: str
    folder: str = 'general'
    notes: str = ''


class FollowToggle(BaseModel):
    """Модель для подписки/отписки"""
    user_id: str
    action: str = 'toggle'


class FollowResponse(BaseModel):
    """Ответ на подписку/отписку"""
    following: bool
    followers_count: int
    following_count: int


class RepostCreate(BaseModel):
    """Модель для создания репоста"""
    original_post_id: str
    comment: Optional[str] = ""


class Notification(BaseModel):
    """Модель уведомления"""
    id: str
    from_user_id: Optional[str] = None
    type: str
    entity_type: str
    entity_id: str
    is_read: bool
    created_at: str
    time_ago: str
    from_user: Optional[Author] = None
    extra_data: Optional[Dict] = None


class NotificationRead(BaseModel):
    """Модель для отметки прочитанных уведомлений"""
    notification_id: Optional[str] = None
    mark_all: bool = False


class ReportCreate(BaseModel):
    """Модель для создания жалобы"""
    entity_type: str
    entity_id: str
    reason: str
    description: str = ""


class ProfileUpdate(BaseModel):
    """Модель для обновления профиля"""
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class SearchQuery(BaseModel):
    """Модель для поискового запроса"""
    q: str = Field(..., min_length=2)


class PaginatedResponse(BaseModel):
    """Модель пагинированного ответа"""
    items: List[Any]
    next_cursor: Optional[str] = None
    has_more: bool


class Repost(BaseModel):
    """Модель репоста для ответа API"""
    repost_id: str
    original_post_id: str
    user: Author
    comment: Optional[str] = ""
    show_original: bool = True
    created_at: str
    time_ago: str


class RepostWithPost(BaseModel):
    """Модель репоста с данными оригинального поста"""
    repost: Repost
    original_post: Post


class RepostListResponse(BaseModel):
    """Ответ со списком репостов"""
    reposts: List[Repost]
    next_cursor: Optional[str] = None
    has_more: bool
    

class Mention(BaseModel):
    """Модель упоминания для API"""
    id: str
    mentioned_user_id: str
    mentioned_by_user_id: str
    username: str
    post_id: Optional[str] = None
    comment_id: Optional[str] = None
    content_preview: Optional[str] = None
    position_start: int = 0
    position_end: int = 0
    is_read: bool = False
    created_at: str
    time_ago: str
    mentioned_by: Optional[Author] = None
    context: Optional[Dict] = None


class MentionNotification(BaseModel):
    """Уведомление об упоминании"""
    type: str = "mention"
    mention: Mention


class MentionExtractionResult(BaseModel):
    """Результат извлечения упоминаний из текста"""
    usernames: List[str]
    positions: List[Tuple[int, int, str]]
    cleaned_text: str


@dataclass
class ChannelPost:
    """Связь между постом в ленте и сообщением в канале"""
    __slots__ = [
        'post_id', 'channel_id', 'channel_message_id', 'created_by',
        'created_at', 'published_at', 'is_published', 'metadata'
    ]
    
    def __init__(self, **kwargs):
        for key in self.__slots__:
            setattr(self, key, kwargs.get(key))
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.is_published is None:
            self.is_published = True
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict:
        return {
            'post_id': self.post_id,
            'channel_id': self.channel_id,
            'channel_message_id': self.channel_message_id,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() + 'Z' if self.created_at else None,
            'published_at': self.published_at.isoformat() + 'Z' if self.published_at else None,
            'is_published': self.is_published,
            'metadata': self.metadata
        }
    
    def to_db_row(self) -> Dict:
        return {
            'post_id': self.post_id,
            'channel_id': self.channel_id,
            'channel_message_id': self.channel_message_id,
            'created_by': str(self.created_by) if self.created_by else None,
            'created_at': to_timestamp(self.created_at),
            'published_at': to_timestamp(self.published_at) if self.published_at else None,
            'is_published': self.is_published,
            'metadata': json.dumps(self.metadata, ensure_ascii=False) if self.metadata else None
        }
    
    @classmethod
    def from_db_row(cls, row: Dict) -> 'ChannelPost':
        return cls(
            post_id=row.get('post_id'),
            channel_id=row.get('channel_id'),
            channel_message_id=row.get('channel_message_id'),
            created_by=str(row.get('created_by')) if row.get('created_by') else None,
            created_at=from_timestamp(row.get('created_at')),
            published_at=from_timestamp(row.get('published_at')) if row.get('published_at') else None,
            is_published=row.get('is_published', True),
            metadata=json.loads(row.get('metadata')) if row.get('metadata') else {}
        )


# ============================================
# ПАРСЕР УПОМИНАНИЙ
# ============================================

class MentionParser:
    """Парсер для извлечения @упоминаний из текста"""
    
    _pattern = re.compile(r'@([a-zA-Z0-9][a-zA-Z0-9_\-]{2,30})')
    
    @classmethod
    def extract_mentions(cls, text: str) -> MentionExtractionResult:
        if not text:
            return MentionExtractionResult(usernames=[], positions=[], cleaned_text=text)
        
        usernames = []
        positions = []
        
        for match in cls._pattern.finditer(text):
            username = match.group(1)
            start = match.start()
            end = match.end()
            
            usernames.append(username)
            positions.append((start, end, username))
        
        cleaned_text = cls._pattern.sub(r'\1', text)
        
        return MentionExtractionResult(
            usernames=list(set(usernames)),
            positions=positions,
            cleaned_text=cleaned_text
        )
    
    @staticmethod
    def get_context_around_mention(text: str, position: Tuple[int, int, str], context_chars: int = 50) -> str:
        start, end, username = position
        context_start = max(0, start - context_chars)
        context_end = min(len(text), end + context_chars)
        context = text[context_start:context_end]
        if context_start > 0:
            context = "..." + context
        if context_end < len(text):
            context = context + "..."
        return context


# ============================================
# РЕПОЗИТОРИИ - ВСЕ ТРЕБУЮТ session!
# ============================================

class PostRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_posts"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_posts"
    def _get_entity_type(self) -> str:
        return 'post'
    
    async def increment_reactions(self, uow: UnitOfWork, entity_id: str, reaction_type: str, delta: int = 1) -> int:
        entity_type = self._get_entity_type()
        now_ts = to_timestamp(datetime.utcnow())

        logger.info(f"📊 increment_reactions START: {entity_type} {entity_id}, {reaction_type}, delta={delta}, thread={threading.current_thread().name}")

        select_query = f"""
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        DECLARE $reaction_type AS Utf8;
        
        SELECT count FROM feed_reaction_counts
        WHERE entity_type = $entity_type
          AND entity_id = $entity_id
          AND reaction_type = $reaction_type;
        """
        params = {
            '$entity_type': entity_type,
            '$entity_id': entity_id,
            '$reaction_type': reaction_type,
        }

        result = await self.execute(select_query, params)
        current = result[0].get('count', 0) if result else 0
        logger.info(f"📊 Current count = {current}")

        new_count = max(0, current + delta)
        logger.info(f"📊 New count = {new_count}")

        if new_count == 0:
            delete_query = f"""
            DECLARE $entity_type AS Utf8;
            DECLARE $entity_id AS Utf8;
            DECLARE $reaction_type AS Utf8;
            
            DELETE FROM feed_reaction_counts
            WHERE entity_type = $entity_type
              AND entity_id = $entity_id
              AND reaction_type = $reaction_type;
            """
            await self.execute(delete_query, params)
            logger.info(f"🗑️ Deleted reaction count for {entity_type} {entity_id}, {reaction_type}")
            return 0

        upsert_query = f"""
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        DECLARE $reaction_type AS Utf8;
        DECLARE $count AS Uint64;
        DECLARE $now AS Timestamp;
        
        UPSERT INTO feed_reaction_counts (entity_type, entity_id, reaction_type, count, updated_at)
        VALUES ($entity_type, $entity_id, $reaction_type, $count, $now);
        """
        upsert_params = {
            '$entity_type': entity_type,
            '$entity_id': entity_id,
            '$reaction_type': reaction_type,
            '$count': new_count,
            '$now': now_ts,
        }
        await self.execute(upsert_query, upsert_params)
        logger.info(f"✅ Updated reaction count for {entity_type} {entity_id}, {reaction_type}: {new_count}")
        return new_count
    
    async def get_reactions_detail(self, entity_id: str) -> Dict[str, int]:
        """Получить детализацию реакций по типам"""
        entity_type = self._get_entity_type()
        
        query = f"""
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        
        SELECT reaction_type, count
        FROM feed_reaction_counts
        WHERE entity_type = $entity_type AND entity_id = $entity_id;
        """
        
        try:
            result = await self.execute(query, {
                '$entity_type': entity_type,
                '$entity_id': entity_id
            })
            return {row['reaction_type']: row['count'] for row in result} if result else {}
        except Exception as e:
            logger.error(f"❌ Error getting reactions detail: {e}")
            return {}
    async def create(self, post_id: str, user_id: str, title: Optional[str],
                     content: str, content_preview: Optional[str], media_urls: List[str],
                     visibility: str, language: str, created_at: datetime,
                     original_author: Optional[str] = None) -> bool:
        """Создать новый пост"""

        data = {
            'post_id': post_id,
            'user_id': user_id,
            'content': content,
            'created_at': to_timestamp(created_at),
            'comments_count': 0,
            'reposts_count': 0,
            'views_count': 0,
            'bookmarks_count': 0,
            'reactions_count': 0,
            'reactions': '{}',
            'is_repost': False,
            'is_pinned': False,
            'is_edited': False,
            'is_deleted': False,
            'visibility': visibility,
        }
        
        # Добавляем title если есть
        if title is not None:
            data['title'] = title
        
        # Добавляем content_preview если есть, иначе создаем из content
        if content_preview is not None:
            data['content_preview'] = content_preview
        else:
            preview = content[:200] + ("..." if len(content) > 200 else "")
            data['content_preview'] = preview
        
        # Сериализуем media_urls в JSON
        data['media_urls'] = json.dumps(media_urls, ensure_ascii=False)
        data['updated_at'] = to_timestamp(created_at)

        if original_author is not None:
            data['original_author'] = original_author
        
        logger.info(f"📦 Creating post: {post_id} for user {user_id}")
        logger.info(f"📊 Post data: {data}")
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ Post created successfully: {post_id}")
            
            # ✅ УДАЛЯЕМ этот блок - кэш поста будет заполнен при первом получении
            # Сохраняем в кэш
            # post_data = data.copy()
            # post_data['post_id'] = post_id
            # await PostCache.set(post_id, post_data)
            
            return True
        except Exception as e:
            logger.error(f"❌ Error creating post: {e}", exc_info=True)
            return False

    async def create_repost_post(self, post_id: str, user_id: str, original_post_id: str,
                                  repost_comment: str) -> bool:
        """Создать запись в feed_posts для репоста (is_repost=True)"""
        now = datetime.utcnow()
        comment = repost_comment or ''

        data = {
            'post_id': post_id,
            'user_id': str(user_id),
            'content': comment,
            'content_preview': comment[:200] + ('...' if len(comment) > 200 else ''),
            'media_urls': '[]',
            'reactions': '{}',
            'created_at': to_timestamp(now),
            'updated_at': to_timestamp(now),
            'comments_count': 0,
            'reposts_count': 0,
            'views_count': 0,
            'bookmarks_count': 0,
            'reactions_count': 0,
            'is_repost': True,
            'is_pinned': False,
            'is_edited': False,
            'is_deleted': False,
            'visibility': 'public',
            'original_post_id': original_post_id,
            'repost_comment': comment,
        }

        columns = ', '.join(data.keys())
        placeholders = ', '.join([f'${k}' for k in data.keys()])
        declare_block = self._generate_declare({f'${k}': v for k, v in data.items()})

        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """

        params = {f'${k}': v for k, v in data.items()}

        try:
            await self.execute(query, params)
            logger.info(f"✅ Repost feed_posts entry created: {post_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Error creating repost post entry: {e}", exc_info=True)
            return False

    # ============================================
    # READ
    # ============================================

    async def get_by_id(self, post_id: str) -> Optional[Dict]:
        """Получить пост по ID"""
        if not validate_uuid(post_id):
            return None
        
        query = f"""
        DECLARE $post_id AS Utf8;
        SELECT * FROM {self.table_name} WHERE post_id = $post_id AND is_deleted = false;
        """
        params = {'$post_id': post_id}
        
        try:
            result = await self.execute(query, params)
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting post: {e}")
            return None
    
    async def get_many_by_ids(self, post_ids: List[str]) -> Dict[str, Dict]:
        """Получить несколько постов по IDs (batch)"""
        if not post_ids:
            return {}
        
        unique_ids = list(set(post_ids))
        
        # Создаем плейсхолдеры для IN запроса
        placeholders = []
        params = {}
        for i, pid in enumerate(unique_ids):
            placeholder = f"$pid_{i}"
            placeholders.append(placeholder)
            params[placeholder] = pid
        
        placeholders_str = ', '.join(placeholders)
        
        declare_parts = []
        for i in range(len(unique_ids)):
            declare_parts.append(f"DECLARE $pid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        query = f"""
        {declare_block}
        SELECT * FROM {self.table_name}
        WHERE post_id IN ({placeholders_str}) AND is_deleted = false;
        """
        
        try:
            result = await self.execute(query, params)
            return {row['post_id']: row for row in result}
        except Exception as e:
            logger.error(f"Error getting many posts: {e}")
            return {}
    
    async def get_feed_with_cursor(self, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        Получить ленту публичных постов с курсорной пагинацией
        Возвращает (posts, next_cursor)
        """
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
                logger.info(f"📌 Decoded cursor: created_at={last_created_at}, last_id={last_id}")
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE is_deleted = false AND visibility = 'public'
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND post_id < $last_id))
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE is_deleted = false AND visibility = 'public'
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {'$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            # Логируем количество полученных строк
            logger.error(f"🔍 DEBUG: rows count = {len(rows)}, limit = {limit}")
            
            has_more = len(rows) > limit
            logger.error(f"🔍 DEBUG: has_more = {has_more}")
            
            if has_more:
                posts = rows[:-1]
                last_post = rows[-2]
                
                # Логируем информацию о последнем посте
                logger.error(f"🔍 DEBUG: last_post keys = {list(last_post.keys()) if isinstance(last_post, dict) else 'not a dict'}")
                created_at_val = last_post.get('created_at') if isinstance(last_post, dict) else getattr(last_post, 'created_at', None)
                post_id_val = last_post.get('post_id') if isinstance(last_post, dict) else getattr(last_post, 'post_id', None)
                logger.error(f"🔍 DEBUG: created_at = {created_at_val}, type = {type(created_at_val)}")
                logger.error(f"🔍 DEBUG: post_id = {post_id_val}")
                
                # Конвертируем created_at в timestamp если это объект datetime
                if hasattr(created_at_val, 'timestamp'):
                    created_at_ts = int(created_at_val.timestamp() * 1e6)
                    logger.error(f"🔍 DEBUG: converted datetime to timestamp: {created_at_ts}")
                elif isinstance(created_at_val, (int, float)):
                    created_at_ts = int(created_at_val)
                    logger.error(f"🔍 DEBUG: using integer timestamp: {created_at_ts}")
                else:
                    # Пробуем распарсить строку
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(str(created_at_val).replace('Z', '+00:00'))
                        created_at_ts = int(dt.timestamp() * 1e6)
                        logger.error(f"🔍 DEBUG: parsed string to timestamp: {created_at_ts}")
                    except Exception as e:
                        logger.error(f"🔍 DEBUG: failed to parse created_at: {e}")
                        created_at_ts = int(time.time() * 1e6)
                
                next_cursor = base64.b64encode(
                    f"{created_at_ts}:{post_id_val}".encode()
                ).decode()
                logger.error(f"🔍 DEBUG: next_cursor created: {next_cursor[:50]}...")
            else:
                posts = rows
                next_cursor = None
                logger.error(f"🔍 DEBUG: no more rows, next_cursor = None")
            
            return posts, next_cursor
            
        except Exception as e:
            logger.error(f"❌ Error getting feed with cursor: {e}", exc_info=True)
            return [], None
    
    async def get_by_user_with_cursor(self, user_id: str, limit: int, cursor: Optional[str] = None,
                                      include_reposts: bool = True) -> Tuple[List[Dict], Optional[str]]:
        """Получить посты пользователя с курсорной пагинацией"""
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        conditions = ["user_id = $user_id", "is_deleted = false"]
        if not include_reposts:
            conditions.append("is_repost = false")
        
        where_clause = " AND ".join(conditions)
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE {where_clause}
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND post_id < $last_id))
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$user_id': user_id,
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE {where_clause}
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {'$user_id': user_id, '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                posts = rows[:-1]
                last_post = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_post['created_at']}:{last_post['post_id']}".encode()
                ).decode()
            else:
                posts = rows
                next_cursor = None
            
            return posts, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting user posts with cursor: {e}")
            return [], None
    
    async def get_reposts_with_cursor(self, original_post_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить репосты поста с курсорной пагинацией"""
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $original_post_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE original_post_id = $original_post_id 
              AND is_repost = true 
              AND is_deleted = false
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND post_id < $last_id))
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$original_post_id': original_post_id,
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $original_post_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE original_post_id = $original_post_id 
              AND is_repost = true 
              AND is_deleted = false
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {'$original_post_id': original_post_id, '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                posts = rows[:-1]
                last_post = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_post['created_at']}:{last_post['post_id']}".encode()
                ).decode()
            else:
                posts = rows
                next_cursor = None
            
            return posts, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting reposts with cursor: {e}")
            return [], None
    
    async def search_with_cursor(self, search_text: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Поиск постов с курсорной пагинацией"""
        escaped = self._escape_like(search_text)
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $search AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE is_deleted = false 
                AND visibility = 'public'
                AND (content LIKE '%' || $search || '%' OR title LIKE '%' || $search || '%')
                AND (created_at < $last_created_at OR 
                     (created_at = $last_created_at AND post_id < $last_id))
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$search': escaped,
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $search AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE is_deleted = false 
                AND visibility = 'public'
                AND (content LIKE '%' || $search || '%' OR title LIKE '%' || $search || '%')
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {'$search': escaped, '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                posts = rows[:-1]
                last_post = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_post['created_at']}:{last_post['post_id']}".encode()
                ).decode()
            else:
                posts = rows
                next_cursor = None
            
            return posts, next_cursor
            
        except Exception as e:
            logger.error(f"Error searching posts with cursor: {e}")
            return [], None
    
    async def get_trending_with_cursor(self, week_ago: int, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        Получить популярные посты с курсорной пагинацией.
        Использует кэширование и предвычисленный popularity_score.
        """
        # 1. Проверяем кэш для первой страницы
        cache_key = f"trending:week:{week_ago}:limit:{limit}:cursor:{cursor or 'first'}"
        
        if not cursor:
            cached = await cache.get(cache_key)
            if cached:
                logger.info(f"📦 Trending cache hit for {cache_key}")
                return cached['posts'], cached.get('next_cursor')
        
        last_score = None
        last_id = None

        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                score_str, last_id = decoded.split(':')
                last_score = float(score_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")

        if last_score is not None and last_id:
            query = f"""
            DECLARE $week_ago AS Timestamp;
            DECLARE $limit AS Uint64;
            DECLARE $last_score AS Double;
            DECLARE $last_id AS Utf8;
            
            SELECT 
                post_id, user_id, title, content, content_preview, media_urls,
                comments_count, reposts_count, views_count, bookmarks_count,
                reactions_count,
                is_repost, original_post_id, repost_comment, is_pinned, is_edited,
                visibility, language, sentiment_score, reading_time_minutes,
                created_at, updated_at, published_at, scheduled_for, is_deleted, deleted_at,
                original_author, original_post, popularity_score
            FROM {self.table_name}
            WHERE created_at >= $week_ago 
                AND is_deleted = false 
                AND visibility = 'public'
                AND (popularity_score < $last_score OR 
                     (popularity_score = $last_score AND post_id < $last_id))
            ORDER BY popularity_score DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$week_ago': week_ago,
                '$limit': limit,
                '$last_score': last_score,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $week_ago AS Timestamp;
            DECLARE $limit AS Uint64;
            
            SELECT 
                post_id, user_id, title, content, content_preview, media_urls,
                comments_count, reposts_count, views_count, bookmarks_count,
                reactions_count,
                is_repost, original_post_id, repost_comment, is_pinned, is_edited,
                visibility, language, sentiment_score, reading_time_minutes,
                created_at, updated_at, published_at, scheduled_for, is_deleted, deleted_at,
                original_author, original_post, popularity_score
            FROM {self.table_name}
            WHERE created_at >= $week_ago 
                AND is_deleted = false 
                AND visibility = 'public'
            ORDER BY popularity_score DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {'$week_ago': week_ago, '$limit': limit}

        try:
            rows = await self.execute(query, params)

            has_more = len(rows) > limit
            if has_more:
                posts = rows[:-1]
                last_post = rows[-2]
                score = last_post.get('popularity_score')
                if score is None:
                    score = last_post.get('reactions_count', 0)
                next_cursor = base64.b64encode(
                    f"{score}:{last_post['post_id']}".encode()
                ).decode()
            else:
                posts = rows
                next_cursor = None

            # Сохраняем в кэш только первую страницу
            if not cursor and posts:
                cache_data = {
                    'posts': posts,
                    'next_cursor': next_cursor
                }
                await cache.set(cache_key, cache_data, ttl=30)  # 30 секунд кэш

            return posts, next_cursor

        except Exception as e:
            logger.error(f"Error getting trending posts with cursor: {e}")
            return [], None


    async def update_popularity_on_interaction(self, post_id: str, interaction_type: str, delta: int = 1):
        """
        Обновить popularity_score при лайке/комментарии/репосте
        Вызывать из методов increment_*
        """
        try:
            # Веса для разных типов взаимодействий
            weights = {
                'reaction': 2,
                'comment': 3,
                'repost': 2,
                'bookmark': 1
            }
            
            weight = weights.get(interaction_type, 1)
            
            query = f"""
            DECLARE $post_id AS Utf8;
            DECLARE $weight AS Int64;
            
            UPDATE {self.table_name}
            SET popularity_score = popularity_score + CAST($weight AS Double)
            WHERE post_id = $post_id;
            """
            params = {
                '$post_id': post_id,
                '$weight': weight
            }
            await self.execute(query, params)
            
            # Инвалидируем кэш трендов
            await cache.delete_pattern("trending:*")
            
        except Exception as e:
            logger.error(f"Error updating popularity on interaction: {e}")
    
    # ============================================
    # UPDATE
    # ============================================
    
    async def update_post(self, post_id: str, updates: Dict) -> bool:
        """Обновить пост"""
        updates['updated_at'] = to_timestamp(datetime.utcnow())
        
        set_parts = []
        params = {'$post_id': post_id}
        
        for key, value in updates.items():
            param_name = f'${key}'
            set_parts.append(f"{key} = {param_name}")
            params[param_name] = value
        
        set_clause = ", ".join(set_parts)
        declare_block = self._generate_declare(params)
        
        query = f"""
        {declare_block}
        UPDATE {self.table_name}
        SET {set_clause}
        WHERE post_id = $post_id;
        """
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error updating post: {e}")
            return False
    
    async def increment_likes(self, post_id: str, delta: int = 1) -> int:
        """Атомарно изменить счетчик лайков и обновить популярность"""
        query = f"""
        DECLARE $post_id AS Utf8;
        DECLARE $delta AS Int64;
        
        UPDATE {self.table_name}
        SET likes_count = likes_count + CAST($delta AS Uint32),
            popularity_score = (likes_count + CAST($delta AS Uint32)) + comments_count * 2 + reposts_count * 3 + views_count * 0.1
        WHERE post_id = $post_id
        RETURNING likes_count;
        """
        params = {'$post_id': post_id, '$delta': delta}
        
        try:
            result = await self.execute(query, params)
            return result[0]['likes_count'] if result else 0
        except Exception as e:
            logger.error(f"Error incrementing likes: {e}")
            return 0
    
    async def increment_comments(self, post_id: str, delta: int = 1) -> int:
        """Атомарно изменить счетчик комментариев и обновить популярность поста"""
        logger.info(f"🔍 [increment_comments] START: post_id={post_id}, delta={delta}")
        
        # Используем Uint32, так как это родной тип поля в таблице feed_posts
        # COALESCE вернет 0, если значение NULL, затем прибавляем дельту
        query = f"""
        DECLARE $post_id AS Utf8;
        DECLARE $delta AS Int64;

        UPDATE {self.table_name}
        SET comments_count = CAST(CAST(COALESCE(comments_count, 0) AS Int64) + $delta AS Uint32),
            popularity_score = CAST(
                (CAST(COALESCE(comments_count, 0) AS Int64) + $delta) * 2 
                + CAST(COALESCE(reposts_count, 0) AS Int64) * 3 
                + CAST(COALESCE(views_count, 0) AS Int64) * 0.1
            AS Double)
        WHERE post_id = $post_id
        RETURNING comments_count;
        """
        
        params = {
            '$post_id': post_id, 
            '$delta': delta
        }
        
        try:
            result = await self.execute(query, params)
            
            if result and len(result) > 0:
                row = result[0]
                # Безопасное извлечение значения
                comments_count = row.get('comments_count') if isinstance(row, dict) else getattr(row, 'comments_count', 0)
                
                logger.info(f"✅ [increment_comments] New comments_count: {comments_count}")
                return int(comments_count or 0)
            else:
                logger.warning(f"⚠️ [increment_comments] No result returned for post_id={post_id}")
                return 0
                
        except Exception as e:
            logger.error(f"❌ [increment_comments] Error: {e}", exc_info=True)
            return 0
    
    async def increment_reposts(self, post_id: str, delta: int = 1) -> int:
        """Атомарно изменить счетчик репостов и обновить популярность"""
        query = f"""
        DECLARE $post_id AS Utf8;
        DECLARE $delta AS Int64;
        
        UPDATE {self.table_name}
        SET reposts_count = reposts_count + CAST($delta AS Uint32),
            popularity_score = likes_count + comments_count * 2 + (reposts_count + CAST($delta AS Uint32)) * 3 + views_count * 0.1
        WHERE post_id = $post_id
        RETURNING reposts_count;
        """
        params = {'$post_id': post_id, '$delta': delta}
        
        try:
            result = await self.execute(query, params)
            return result[0]['reposts_count'] if result else 0
        except Exception as e:
            logger.error(f"Error incrementing reposts: {e}")
            return 0
    
    async def increment_views(self, post_id: str) -> int:
        """Увеличить счетчик просмотров - БЕЗ popularity_score"""
        query = f"""
        DECLARE $post_id AS Utf8;
        
        UPDATE {self.table_name}
        SET views_count = views_count + CAST(1 AS Uint32)
        WHERE post_id = $post_id
        RETURNING views_count;
        """
        params = {'$post_id': post_id}
        
        try:
            result = await self.execute(query, params)
            return result[0]['views_count'] if result else 0
        except Exception as e:
            logger.error(f"Error incrementing views: {e}")
            return 0
    
    async def increment_bookmarks(self, post_id: str, delta: int = 1) -> int:
        """Атомарно изменить счетчик закладок - ФИНАЛЬНАЯ ВЕРСИЯ с подзапросом"""
        logger.info(f"📊 ===== INCREMENT_BOOKMARKS START =====")
        logger.info(f"📊 post_id: {post_id}")
        logger.info(f"📊 delta: {delta}")
        
        query = f"""
        DECLARE $post_id AS Utf8;
        DECLARE $delta AS Int64;
        
        -- Получаем текущее значение через подзапрос
        $current = (SELECT bookmarks_count FROM {self.table_name} WHERE post_id = $post_id);
        
        -- Вычисляем новое значение (NULL превращаем в 0)
        $new_value = COALESCE($current, 0) + $delta;
        
        -- Конвертируем в Uint32
        $safe_value = CAST($new_value AS Uint32);
        
        -- Обновляем
        UPDATE {self.table_name}
        SET bookmarks_count = $safe_value
        WHERE post_id = $post_id
        RETURNING bookmarks_count;
        """
        params = {'$post_id': post_id, '$delta': delta}
        
        try:
            logger.info(f"📝 Executing query with params: {params}")
            result = await self.execute(query, params)
            logger.info(f"📊 Raw result: {result}")
            
            if result and len(result) > 0:
                row = result[0]
                if isinstance(row, dict):
                    count = row.get('bookmarks_count', 0)
                    logger.info(f"📊 Count from dict: {count}")
                elif hasattr(row, 'bookmarks_count'):
                    count = row.bookmarks_count
                    logger.info(f"📊 Count from attribute: {count}")
                else:
                    count = 0
                
                try:
                    final_count = int(count) if count is not None else 0
                    logger.info(f"✅ Final count: {final_count}")
                    return final_count
                except (TypeError, ValueError):
                    return 0
            
            logger.warning("⚠️ No result from UPDATE, returning 0")
            return 0
            
        except Exception as e:
            logger.error(f"❌ Error in increment_bookmarks: {e}", exc_info=True)
            return 0
        finally:
            logger.info(f"📊 ===== INCREMENT_BOOKMARKS END =====")
    
    async def update_popularity_score(self, post_id: str) -> bool:
        """Обновить популярность поста (вызывается при лайках/комментариях)"""
        query = f"""
        DECLARE $post_id AS Utf8;
        
        UPDATE {self.table_name}
        SET popularity_score = likes_count + comments_count * 2 + reposts_count * 3 + views_count * 0.1
        WHERE post_id = $post_id;
        """
        params = {'$post_id': post_id}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error updating popularity score: {e}")
            return False
    
    # ============================================
    # UTILS
    # ============================================
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        """Сгенерировать плейсхолдеры для IN запроса"""
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


class ChannelPostRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_channel_posts"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_channel_posts"
    
    async def create(self, channel_post: ChannelPost) -> bool:
        """Создать связь поста с каналом"""
        data = channel_post.to_db_row()
        
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
            logger.error(f"Error creating channel post: {e}")
            return False
    
    async def get_by_post_id(self, post_id: str) -> Optional[ChannelPost]:
        """Получить связь по ID поста"""
        query = f"""
        DECLARE $post_id AS Utf8;
        SELECT * FROM {self.table_name} WHERE post_id = $post_id;
        """
        params = {'$post_id': post_id}
        
        try:
            rows = await self.execute(query, params)
            if rows:
                return ChannelPost.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting channel post: {e}")
            return None
    
    async def get_by_channel_message(self, channel_id: int, message_id: int) -> Optional[ChannelPost]:
        """Получить связь по сообщению в канале"""
        query = f"""
        DECLARE $channel_id AS Uint64;
        DECLARE $channel_message_id AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE channel_id = $channel_id AND channel_message_id = $channel_message_id;
        """
        params = {
            '$channel_id': channel_id,
            '$channel_message_id': message_id
        }
        
        try:
            rows = await self.execute(query, params)
            if rows:
                return ChannelPost.from_db_row(rows[0])
            return None
        except Exception as e:
            logger.error(f"Error getting channel post by message: {e}")
            return None
    
    async def list_by_channel(self, channel_id: int, limit: int = 50, offset: int = 0) -> List[ChannelPost]:
        """Получить все посты из канала"""
        query = f"""
        DECLARE $channel_id AS Uint64;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE channel_id = $channel_id
        ORDER BY created_at DESC
        LIMIT $limit OFFSET $offset;
        """
        params = {
            '$channel_id': channel_id,
            '$limit': limit,
            '$offset': offset
        }
        
        try:
            rows = await self.execute(query, params)
            return [ChannelPost.from_db_row(row) for row in rows] if rows else []
        except Exception as e:
            logger.error(f"Error listing channel posts: {e}")
            return []
    
    async def update_publish_status(self, post_id: str, is_published: bool) -> bool:
        """Обновить статус публикации"""
        query = f"""
        DECLARE $post_id AS Utf8;
        DECLARE $is_published AS Bool;
        
        UPDATE {self.table_name}
        SET is_published = $is_published
        WHERE post_id = $post_id;
        """
        params = {
            '$post_id': post_id,
            '$is_published': is_published
        }
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error updating publish status: {e}")
            return False
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


class LikeRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_likes"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_likes"
    
    async def check(self, post_id: str, user_id: str) -> bool:
        """Проверить, есть ли лайк"""
        logger.info(f"🔍 LikeRepository.check: post_id={post_id}, user_id={user_id}")
        
        query = f"""
        DECLARE $post_id AS Utf8;
        DECLARE $user_id AS Utf8;
        
        SELECT COUNT(*) as count FROM {self.table_name}
        WHERE post_id = $post_id AND user_id = $user_id;
        """
        params = {'$post_id': post_id, '$user_id': str(user_id)}
        
        try:
            result = await self.execute(query, params)
            logger.info(f"🔍 Query result: {result}")
            
            if result and len(result) > 0:
                row = result[0]
                # YDB может возвращать результат в разных форматах
                if isinstance(row, dict):
                    count = row.get('count', 0)
                elif hasattr(row, 'count'):
                    count = row.count
                else:
                    # Пробуем получить первый элемент
                    count = row[0] if row else 0
                
                logger.info(f"✅ Like check result: {count > 0} (count={count})")
                return count > 0
            return False
        except Exception as e:
            logger.error(f"❌ Error checking like: {e}", exc_info=True)
            return False
    
    async def check_many(self, post_ids: List[str], user_id: str) -> Dict[str, bool]:
        """Проверить лайки для нескольких постов одним запросом"""
        if not post_ids:
            return {}
        
        unique_ids = list(set(post_ids))
        placeholders, params = self._generate_placeholders(unique_ids, "pid")
        params['$user_id'] = str(user_id)
        
        declare_parts = ["DECLARE $user_id AS Utf8;"]
        for i in range(len(unique_ids)):
            declare_parts.append(f"DECLARE $pid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        query = f"""
        {declare_block}
        SELECT post_id FROM {self.table_name}
        WHERE user_id = $user_id AND post_id IN ({placeholders});
        """
        
        try:
            result = await self.execute(query, params)
            liked = {row['post_id']: True for row in result}
            return {pid: pid in liked for pid in post_ids}
        except Exception as e:
            logger.error(f"Error checking many likes: {e}")
            return {}
    
    async def add(self, post_id: str, user_id: str, created_at: datetime) -> Optional[str]:
        """Добавить лайк"""
        logger.info(f"➕ LikeRepository.add: post_id={post_id}, user_id={user_id}")
        
        like_id = str(uuid.uuid4())
        
        data = {
            'like_id': like_id,
            'post_id': post_id,
            'user_id': str(user_id),
            'created_at': to_timestamp(created_at)
        }
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ Like added successfully: {like_id}")
            return like_id
        except Exception as e:
            logger.error(f"❌ Error adding like: {e}", exc_info=True)
            return None
    
    async def remove(self, post_id: str, user_id: str) -> bool:
        """Удалить лайк"""
        query = f"""
        DECLARE $post_id AS Utf8;
        DECLARE $user_id AS Utf8;
        
        DELETE FROM {self.table_name}
        WHERE post_id = $post_id AND user_id = $user_id;
        """
        params = {'$post_id': post_id, '$user_id': str(user_id)}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error removing like: {e}")
            return False
    
    async def toggle_atomic(self, uow: UnitOfWork, post_id: str, user_id: str, now: datetime) -> Dict:
        """Атомарное переключение лайка в рамках транзакции"""
        logger.info(f"🔄 LikeRepository.toggle_atomic: post_id={post_id}, user_id={user_id}")
        
        try:
            # Проверяем существование лайка
            exists = await self.check(post_id, user_id)
            logger.info(f"🔍 Like exists: {exists}")
            
            if exists:
                # Удаляем лайк
                logger.info(f"🗑️ Removing like for post {post_id}")
                remove_success = await self.remove(post_id, user_id)
                if not remove_success:
                    logger.error(f"❌ Failed to remove like")
                    return {'liked': False, 'likes_count': 0}
                
                # Обновляем счетчик
                post_repo = PostRepository(self._session)
                new_count = await post_repo.increment_likes(post_id, -1)
                logger.info(f"✅ Like removed, new count: {new_count}")
                return {'liked': False, 'likes_count': new_count}
            else:
                # Добавляем лайк
                logger.info(f"➕ Adding like for post {post_id}")
                like_id = await self.add(post_id, user_id, now)
                if not like_id:
                    logger.error(f"❌ Failed to add like")
                    return {'liked': False, 'likes_count': 0}
                
                # Обновляем счетчик
                post_repo = PostRepository(self._session)
                new_count = await post_repo.increment_likes(post_id, 1)
                logger.info(f"✅ Like added, new count: {new_count}")
                return {'liked': True, 'likes_count': new_count}
                
        except Exception as e:
            logger.error(f"❌ Error in toggle_atomic: {e}", exc_info=True)
            # ВАЖНО: возвращаем словарь даже при ошибке!
            return {'liked': False, 'likes_count': 0, 'error': str(e)}
    
    async def count_by_post(self, post_id: str) -> int:
        """Получить количество лайков поста"""
        query = f"""
        DECLARE $post_id AS Utf8;
        
        SELECT COUNT(*) as cnt FROM {self.table_name} WHERE post_id = $post_id;
        """
        params = {'$post_id': post_id}
        
        try:
            result = await self.execute(query, params)
            return result[0]['cnt'] if result else 0
        except Exception as e:
            logger.error(f"Error counting likes: {e}")
            return 0
    
    async def get_by_user_with_cursor(self, user_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить лайки пользователя с курсорной пагинацией"""
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT 
                l.like_id,
                l.created_at as like_created_at,
                p.*
            FROM {self.table_name} l
            JOIN feed_posts p ON l.post_id = p.post_id
            WHERE l.user_id = $user_id AND p.is_deleted = false
              AND (l.created_at < $last_created_at OR 
                   (l.created_at = $last_created_at AND l.like_id < $last_id))
            ORDER BY l.created_at DESC, l.like_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$user_id': str(user_id),
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT 
                l.like_id,
                l.created_at as like_created_at,
                p.*
            FROM {self.table_name} l
            JOIN feed_posts p ON l.post_id = p.post_id
            WHERE l.user_id = $user_id AND p.is_deleted = false
            ORDER BY l.created_at DESC, l.like_id DESC
            LIMIT $limit + 1;
            """
            params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['like_created_at']}:{last_item['like_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting user likes with cursor: {e}")
            return [], None
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


_S3_PUBLIC_URL = os.environ.get('OBJECT_STORAGE_PUBLIC_URL', 'https://storage.yandexcloud.net/social-media-images')

def _resolve_avatar_url(raw: Optional[str]) -> Optional[str]:
    """Convert a relative S3 path (avatars/...) to a full public URL."""
    if not raw:
        return None
    if raw.startswith('http://') or raw.startswith('https://'):
        return raw
    return f"{_S3_PUBLIC_URL}/{raw}"


class UserRepository(TransactionAwareRepository):
    """Репозиторий для получения данных о пользователях"""

    def __init__(self, session):
        super().__init__(session)
        self._user_cache = {}
        self._stats_cache = {}
        logger.info("✅ UserRepository initialized")
    
    def get_from_token(self, token_data: Dict) -> Dict:
        """Получить данные пользователя из JWT токена"""
        user_id = token_data.get('user_id') or token_data.get('sub', '')
        username = token_data.get('username', '') or f"user_{user_id[:8]}"
        display_name = token_data.get('display_name', '') or username
        
        return {
            'id': user_id,
            'user_id': user_id,
            'username': username,
            'display_name': display_name,
            'first_name': token_data.get('first_name', ''),
            'last_name': token_data.get('last_name', ''),
            'role': token_data.get('role', 'user'),
            'is_verified': token_data.get('is_verified', False) or token_data.get('verified', False)
        }
    
    async def get(self, user_id: str, token_data: Optional[Dict] = None) -> Optional[Dict]:
        """Получить пользователя по ID"""
        if user_id in self._user_cache:
            logger.debug(f"📦 User {user_id} found in memory cache")
            return self._user_cache[user_id]
        
        if token_data:
            token_user_id = token_data.get('user_id') or token_data.get('sub')
            if token_user_id == user_id:
                user_data = self.get_from_token(token_data)
                self._user_cache[user_id] = user_data
                return user_data
        
        try:
            query = """
            DECLARE $id AS Utf8;
            SELECT
                id, username,
                first_name_encrypted,
                last_name_encrypted,
                display_name, role, is_verified, avatar_url
            FROM users
            WHERE id = $id;
            """
            params = {'$id': user_id}

            result = await self.execute(query, params)

            if result:
                row = result[0]
                first_name = ''
                if row.get('first_name_encrypted'):
                    try:
                        first_name = base64.b64decode(row['first_name_encrypted']).decode('utf-8')
                    except:
                        first_name = ''

                last_name = ''
                if row.get('last_name_encrypted'):
                    try:
                        last_name = base64.b64decode(row['last_name_encrypted']).decode('utf-8')
                    except:
                        last_name = ''

                user_data = {
                    'id': row['id'],
                    'user_id': row['id'],
                    'username': row.get('username', ''),
                    'first_name': first_name,
                    'last_name': last_name,
                    'display_name': row.get('display_name', '') or f"{first_name} {last_name}".strip() or row.get('username', ''),
                    'role': row.get('role', 'user'),
                    'is_verified': row.get('is_verified', False),
                    'avatar_url': _resolve_avatar_url(row.get('avatar_url')),
                }

                self._user_cache[user_id] = user_data
                return user_data
                
        except Exception as e:
            logger.error(f"Error loading user from DB: {e}")
        
        fallback = {
            'id': user_id,
            'user_id': user_id,
            'username': f"user_{user_id[:8]}",
            'display_name': f"User {user_id[:8]}",
            'first_name': '',
            'last_name': '',
            'role': 'user',
            'is_verified': False
        }
        self._user_cache[user_id] = fallback
        return fallback
    
    async def get_many(self, user_ids: List[str], token_data: Optional[Dict] = None) -> Dict[str, Dict]:
        """
        Получить нескольких пользователей одним запросом
        """
        if not user_ids:
            return {}
        
        unique_ids = list(set(user_ids))
        result = {}
        uncached = []
        
        for uid in unique_ids:
            if uid in self._user_cache:
                result[uid] = self._user_cache[uid]
            else:
                uncached.append(uid)
        
        if not uncached:
            return result
        
        try:
            placeholders = []
            params = {}
            for i, uid in enumerate(uncached):
                placeholder = f"$uid_{i}"
                placeholders.append(placeholder)
                params[placeholder] = uid
            
            placeholders_str = ', '.join(placeholders)
            declare_lines = [f"DECLARE $uid_{i} AS Utf8;" for i in range(len(uncached))]
            declare_block = "\n".join(declare_lines)
            
            query = f"""
            {declare_block}
            SELECT
                id, username,
                first_name_encrypted,
                last_name_encrypted,
                display_name, role, is_verified, avatar_url
            FROM users
            WHERE id IN ({placeholders_str});
            """

            rows = await self.execute(query, params)

            for row in rows:
                uid = row['id']

                first_name = ''
                if row.get('first_name_encrypted'):
                    try:
                        first_name = base64.b64decode(row['first_name_encrypted']).decode('utf-8')
                    except:
                        first_name = ''

                last_name = ''
                if row.get('last_name_encrypted'):
                    try:
                        last_name = base64.b64decode(row['last_name_encrypted']).decode('utf-8')
                    except:
                        last_name = ''

                user_data = {
                    'id': uid,
                    'user_id': uid,
                    'username': row.get('username', ''),
                    'first_name': first_name,
                    'last_name': last_name,
                    'display_name': row.get('display_name', '') or f"{first_name} {last_name}".strip() or row.get('username', ''),
                    'role': row.get('role', 'user'),
                    'is_verified': row.get('is_verified', False),
                    'avatar_url': _resolve_avatar_url(row.get('avatar_url')),
                }
                
                result[uid] = user_data
                self._user_cache[uid] = user_data
            
            for uid in uncached:
                if uid not in result:
                    fallback = {
                        'id': uid,
                        'user_id': uid,
                        'username': f"user_{uid[:8]}",
                        'display_name': f"User {uid[:8]}",
                        'first_name': '',
                        'last_name': '',
                        'role': 'user',
                        'is_verified': False
                    }
                    result[uid] = fallback
                    self._user_cache[uid] = fallback
            
        except Exception as e:
            logger.error(f"Error loading multiple users: {e}")
            for uid in uncached:
                fallback = {
                    'id': uid,
                    'user_id': uid,
                    'username': f"user_{uid[:8]}",
                    'display_name': f"User {uid[:8]}",
                    'first_name': '',
                    'last_name': '',
                    'role': 'user',
                    'is_verified': False
                }
                result[uid] = fallback
                self._user_cache[uid] = fallback
        
        return result
    
    async def get_profile_stats(self, user_id: str) -> Dict:
        """
        Получить статистику профиля одним запросом
        """
        logger.info(f"📊 Getting profile stats for user {user_id}")
        
        cache_key = f"stats:{user_id}"
        if cache_key in self._stats_cache:
            return self._stats_cache[cache_key]
        
        query = """
        DECLARE $user_id AS Utf8;
        
        $stats = (
            SELECT 'total_posts' as metric, COUNT(*) as value 
            FROM feed_posts WHERE user_id = $user_id AND is_deleted = false
            UNION ALL
            SELECT 'original_posts', COUNT(*) 
            FROM feed_posts WHERE user_id = $user_id AND is_deleted = false AND is_repost = false
            UNION ALL
            SELECT 'reposts', COUNT(*) 
            FROM feed_posts WHERE user_id = $user_id AND is_deleted = false AND is_repost = true
            UNION ALL
            SELECT 'likes_given', COUNT(*) 
            FROM feed_likes WHERE user_id = $user_id
            UNION ALL
            SELECT 'likes_received', COUNT(*) 
            FROM feed_likes l JOIN feed_posts p ON l.post_id = p.post_id WHERE p.user_id = $user_id
            UNION ALL
            SELECT 'comments_given', COUNT(*) 
            FROM feed_comments WHERE user_id = $user_id AND is_deleted = false
            UNION ALL
            SELECT 'comments_received', COUNT(*) 
            FROM feed_comments c JOIN feed_posts p ON c.post_id = p.post_id WHERE p.user_id = $user_id
            UNION ALL
            SELECT 'followers', COUNT(*) 
            FROM feed_follows WHERE following_id = $user_id
            UNION ALL
            SELECT 'following', COUNT(*) 
            FROM feed_follows WHERE follower_id = $user_id
        );
        
        SELECT 
            SUM(CASE WHEN metric = 'total_posts' THEN value ELSE 0 END) as total_posts,
            SUM(CASE WHEN metric = 'original_posts' THEN value ELSE 0 END) as original_posts,
            SUM(CASE WHEN metric = 'reposts' THEN value ELSE 0 END) as reposts,
            SUM(CASE WHEN metric = 'likes_given' THEN value ELSE 0 END) as likes_given,
            SUM(CASE WHEN metric = 'likes_received' THEN value ELSE 0 END) as likes_received,
            SUM(CASE WHEN metric = 'comments_given' THEN value ELSE 0 END) as comments_given,
            SUM(CASE WHEN metric = 'comments_received' THEN value ELSE 0 END) as comments_received,
            SUM(CASE WHEN metric = 'followers' THEN value ELSE 0 END) as followers,
            SUM(CASE WHEN metric = 'following' THEN value ELSE 0 END) as following
        FROM $stats;
        """
        
        params = {'$user_id': user_id}
        
        try:
            result = await self.execute(query, params)
            
            if result and result[0]:
                row = result[0]
                stats = {
                    'total_posts': row.get('total_posts', 0),
                    'original_posts': row.get('original_posts', 0),
                    'reposts': row.get('reposts', 0),
                    'likes_given': row.get('likes_given', 0),
                    'likes_received': row.get('likes_received', 0),
                    'comments_given': row.get('comments_given', 0),
                    'comments_received': row.get('comments_received', 0),
                    'followers': row.get('followers', 0),
                    'following': row.get('following', 0)
                }
                
                self._stats_cache[cache_key] = stats
                return stats
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting profile stats: {e}")
            return {}
    
    def clear_cache(self):
        """Очистить кэш в конце запроса"""
        self._user_cache.clear()
        self._stats_cache.clear()


class RepostRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_reposts"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_reposts"
    
    async def create(self, original_post_id: str, user_id: str, 
                     comment: str = "", show_original: bool = True) -> Optional[str]:
        """Создать репост"""
        repost_id = str(uuid.uuid4()).replace('-', '')
        now = datetime.utcnow()
        
        data = {
            'repost_id': repost_id,
            'original_post_id': original_post_id,
            'user_id': str(user_id),
            'comment': comment,
            'show_original': show_original,
            'created_at': to_timestamp(now)
        }
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            return repost_id
        except Exception as e:
            logger.error(f"Error creating repost: {e}")
            return None
    
    async def get_by_id(self, repost_id: str) -> Optional[Dict]:
        """Получить репост по ID"""
        search_id = repost_id.replace('-', '')
        
        query = f"""
        DECLARE $repost_id AS Utf8;
        
        SELECT * FROM {self.table_name}
        WHERE repost_id = $repost_id;
        """
        params = {'$repost_id': search_id}
        
        try:
            result = await self.execute(query, params)
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting repost: {e}")
            return None
    
    async def delete(self, repost_id: str, user_id: str) -> bool:
        """Удалить репост"""
        query = f"""
        DECLARE $repost_id AS Utf8;
        DECLARE $user_id AS Utf8;
        
        DELETE FROM {self.table_name}
        WHERE repost_id = $repost_id AND user_id = $user_id;
        """
        params = {'$repost_id': repost_id, '$user_id': str(user_id)}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error deleting repost: {e}")
            return False
    
    async def check_reposted(self, user_id: str, original_post_id: str) -> bool:
        """Проверить, репостнул ли пользователь пост"""
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $original_post_id AS Utf8;
        
        SELECT COUNT(*) as cnt FROM {self.table_name}
        WHERE user_id = $user_id AND original_post_id = $original_post_id;
        """
        params = {'$user_id': str(user_id), '$original_post_id': original_post_id}
        
        try:
            result = await self.execute(query, params)
            return result[0]['cnt'] > 0 if result else False
        except Exception as e:
            logger.error(f"Error checking repost: {e}")
            return False
    
    async def check_many_reposted(self, user_id: str, post_ids: List[str]) -> Dict[str, bool]:
        """Проверить репосты для нескольких постов"""
        if not post_ids:
            return {}
        
        unique_ids = list(set(post_ids))
        placeholders, params = self._generate_placeholders(unique_ids, "pid")
        params['$user_id'] = str(user_id)
        
        declare_parts = ["DECLARE $user_id AS Utf8;"]
        for i in range(len(unique_ids)):
            declare_parts.append(f"DECLARE $pid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        query = f"""
        {declare_block}
        SELECT original_post_id
        FROM {self.table_name}
        WHERE user_id = $user_id 
          AND original_post_id IN ({placeholders});
        """
        
        try:
            result = await self.execute(query, params)
            reposted = {row['original_post_id']: True for row in result}
            return {pid: pid in reposted for pid in post_ids}
        except Exception as e:
            logger.error(f"Error checking many reposts: {e}")
            return {}
    
    async def get_repost_id(self, user_id: str, original_post_id: str) -> Optional[str]:
        """Получить ID репоста пользователя"""
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $original_post_id AS Utf8;
        
        SELECT repost_id FROM {self.table_name}
        WHERE user_id = $user_id AND original_post_id = $original_post_id;
        """
        params = {'$user_id': str(user_id), '$original_post_id': original_post_id}
        
        try:
            result = await self.execute(query, params)
            return result[0]['repost_id'] if result else None
        except Exception as e:
            logger.error(f"Error getting repost ID: {e}")
            return None
    
    async def get_by_original_with_cursor(self, original_post_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить репосты оригинального поста с курсорной пагинацией"""
        logger.info(f"📋 Getting reposts for post {original_post_id} with cursor, limit={limit}")
        
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
                logger.info(f"📌 Decoded cursor: created_at={last_created_at}, last_id={last_id}")
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        # SQL запрос с JOIN для получения данных пользователя
        if last_created_at and last_id:
            logger.info(f"🔍 Using cursor pagination with last_created_at={last_created_at}, last_id={last_id}")
            query = f"""
            DECLARE $original_post_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT 
                r.repost_id,
                r.original_post_id,
                r.user_id,
                r.comment,
                r.show_original,
                r.created_at,
                u.username,
                u.first_name_encrypted,
                u.last_name_encrypted,
                u.display_name,
                u.is_verified
            FROM {self.table_name} r
            JOIN users u ON r.user_id = u.id
            WHERE r.original_post_id = $original_post_id
              AND r.created_at IS NOT NULL
              AND (r.created_at < $last_created_at OR 
                   (r.created_at = $last_created_at AND r.repost_id < $last_id))
            ORDER BY r.created_at DESC, r.repost_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$original_post_id': original_post_id,
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            logger.info(f"🔍 Using first page pagination")
            query = f"""
            DECLARE $original_post_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT 
                r.repost_id,
                r.original_post_id,
                r.user_id,
                r.comment,
                r.show_original,
                r.created_at,
                u.username,
                u.first_name_encrypted,
                u.last_name_encrypted,
                u.display_name,
                u.is_verified
            FROM {self.table_name} r
            JOIN users u ON r.user_id = u.id
            WHERE r.original_post_id = $original_post_id
              AND r.created_at IS NOT NULL
            ORDER BY r.created_at DESC, r.repost_id DESC
            LIMIT $limit + 1;
            """
            params = {'$original_post_id': original_post_id, '$limit': limit}
        
        try:
            logger.info(f"⚡ Executing query for reposts")
            rows = await self.execute(query, params)
            logger.info(f"✅ Query returned {len(rows)} rows")
            
            if rows:
                # Логируем первую строку для отладки
                sample = rows[0]
                logger.info(f"📊 Sample row keys: {list(sample.keys())}")
                logger.info(f"📊 Sample created_at type: {type(sample.get('created_at'))}, value: {sample.get('created_at')}")
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                logger.info(f"📌 Has more items, last_item: {last_item.get('repost_id')}")
                
                # Получаем created_at как timestamp для курсора
                created_at_value = last_item.get('created_at')
                if created_at_value:
                    logger.info(f"📌 created_at value: {created_at_value}, type: {type(created_at_value)}")
                    
                    # Конвертируем в timestamp если это объект datetime
                    if hasattr(created_at_value, 'timestamp'):
                        created_at_ts = int(created_at_value.timestamp() * 1e6)
                        logger.info(f"📌 Converted datetime to timestamp: {created_at_ts}")
                    elif isinstance(created_at_value, (int, float)):
                        created_at_ts = int(created_at_value)
                        logger.info(f"📌 Using integer timestamp: {created_at_ts}")
                    else:
                        # Пробуем распарсить строку
                        try:
                            dt = datetime.fromisoformat(str(created_at_value).replace('Z', '+00:00'))
                            created_at_ts = int(dt.timestamp() * 1e6)
                            logger.info(f"📌 Parsed string to timestamp: {created_at_ts}")
                        except Exception as e:
                            logger.error(f"❌ Failed to parse created_at: {e}")
                            created_at_ts = int(time.time() * 1e6)
                    
                    next_cursor = base64.b64encode(
                        f"{created_at_ts}:{last_item['repost_id']}".encode()
                    ).decode()
                    logger.info(f"📌 Next cursor created: {next_cursor[:30]}...")
                else:
                    logger.warning("⚠️ No created_at value in last_item")
                    next_cursor = None
            else:
                items = rows
                next_cursor = None
                logger.info("📌 No more items")
            
            # Форматируем результат
            logger.info(f"🔄 Formatting {len(items)} repost items")
            formatted_items = []
            for idx, row in enumerate(items):
                try:
                    # Используем правильные имена ключей с учетом префиксов
                    user_id = row.get('r.user_id') or row.get('user_id')
                    if not user_id:
                        logger.error(f"❌ No user_id in row, keys: {list(row.keys())}")
                        continue
                    
                    # Декодируем имя пользователя
                    first_name = safe_b64decode(row.get('u.first_name_encrypted', ''))
                    last_name = safe_b64decode(row.get('u.last_name_encrypted', ''))
                    
                    # Формируем display_name
                    display_name = row.get('u.display_name', '')
                    if not display_name:
                        if first_name and last_name:
                            display_name = f"{first_name} {last_name}".strip()
                        elif first_name:
                            display_name = first_name
                        else:
                            display_name = row.get('u.username', f"user_{user_id[:8]}")
                    
                    # Создаем объект пользователя
                    author = {
                        'id': user_id,
                        'username': row.get('u.username', ''),
                        'display_name': display_name,
                        'is_verified': row.get('u.is_verified', False)
                    }
                    
                    # Форматируем created_at
                    created_at = row.get('r.created_at') or row.get('created_at')
                    if created_at:
                        if hasattr(created_at, 'isoformat'):
                            created_at_str = created_at.isoformat() + 'Z'
                            dt = created_at
                        elif isinstance(created_at, (int, float)):
                            dt = datetime.fromtimestamp(created_at / 1e6)
                            created_at_str = dt.isoformat() + 'Z'
                        else:
                            try:
                                dt = datetime.fromisoformat(str(created_at).replace('Z', '+00:00'))
                                created_at_str = dt.isoformat() + 'Z'
                            except:
                                dt = datetime.utcnow()
                                created_at_str = dt.isoformat() + 'Z'
                        
                        time_ago_str = time_ago(dt)
                    else:
                        created_at_str = datetime.utcnow().isoformat() + 'Z'
                        time_ago_str = 'только что'
                    
                    formatted_item = {
                        'repost_id': row.get('r.repost_id') or row.get('repost_id'),
                        'original_post_id': row.get('r.original_post_id') or row.get('original_post_id'),
                        'user': author,
                        'comment': row.get('r.comment') or row.get('comment', ''),
                        'show_original': row.get('r.show_original') or row.get('show_original', True),
                        'created_at': created_at_str,
                        'time_ago': time_ago_str
                    }
                    formatted_items.append(formatted_item)
                    logger.debug(f"✅ Formatted repost {idx+1}: {formatted_item['repost_id']}")
                    
                except Exception as e:
                    logger.error(f"❌ Error formatting repost {idx}: {e}", exc_info=True)
                    continue
            
            logger.info(f"✅ Returning {len(formatted_items)} formatted reposts, has_more={has_more}")
            return formatted_items, next_cursor
            
        except Exception as e:
            logger.error(f"❌ Error getting reposts with cursor: {e}", exc_info=True)
            return [], None
    
    async def get_with_users_with_cursor(self, original_post_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить репосты с данными пользователей и курсором"""
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $original_post_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT 
                r.repost_id,
                r.original_post_id,
                r.user_id,
                r.comment,
                r.show_original,
                r.created_at,
                u.username,
                u.first_name_encrypted,
                u.last_name_encrypted,
                u.display_name,
                u.is_verified
            FROM {self.table_name} r
            JOIN users u ON r.user_id = u.id
            WHERE r.original_post_id = $original_post_id
              AND (r.created_at < $last_created_at OR 
                   (r.created_at = $last_created_at AND r.repost_id < $last_id))
            ORDER BY r.created_at DESC, r.repost_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$original_post_id': original_post_id,
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $original_post_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT 
                r.repost_id,
                r.original_post_id,
                r.user_id,
                r.comment,
                r.show_original,
                r.created_at,
                u.username,
                u.first_name_encrypted,
                u.last_name_encrypted,
                u.display_name,
                u.is_verified
            FROM {self.table_name} r
            JOIN users u ON r.user_id = u.id
            WHERE r.original_post_id = $original_post_id
            ORDER BY r.created_at DESC, r.repost_id DESC
            LIMIT $limit + 1;
            """
            params = {'$original_post_id': original_post_id, '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['created_at']}:{last_item['repost_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            # Форматируем результат с учетом префиксов
            formatted_items = []
            for row in items:
                formatted_item = {
                    'repost_id': row.get('r.repost_id') or row.get('repost_id'),
                    'original_post_id': row.get('r.original_post_id') or row.get('original_post_id'),
                    'user_id': row.get('r.user_id') or row.get('user_id'),
                    'comment': row.get('r.comment') or row.get('comment', ''),
                    'show_original': row.get('r.show_original') or row.get('show_original', True),
                    'created_at': row.get('r.created_at') or row.get('created_at'),
                    'username': row.get('u.username') or row.get('username'),
                    'first_name_encrypted': row.get('u.first_name_encrypted') or row.get('first_name_encrypted'),
                    'last_name_encrypted': row.get('u.last_name_encrypted') or row.get('last_name_encrypted'),
                    'display_name': row.get('u.display_name') or row.get('display_name'),
                    'is_verified': row.get('u.is_verified') or row.get('is_verified', False)
                }
                formatted_items.append(formatted_item)
            
            return formatted_items, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting reposts with users and cursor: {e}")
            return [], None
    
    async def get_by_user_with_cursor(self, user_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить репосты пользователя с курсорной пагинацией"""
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE user_id = $user_id
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND repost_id < $last_id))
            ORDER BY created_at DESC, repost_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$user_id': str(user_id),
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE user_id = $user_id
            ORDER BY created_at DESC, repost_id DESC
            LIMIT $limit + 1;
            """
            params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['created_at']}:{last_item['repost_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting user reposts with cursor: {e}")
            return [], None
    
    async def get_many_by_post_ids(self, post_ids: List[str], limit_per_post: int = 3) -> Dict[str, List[Dict]]:
        """
        Получить последние репосты для нескольких постов одним запросом
        Используется для превью репостов в ленте
        """
        if not post_ids:
            return {}
        
        unique_ids = list(set(post_ids))
        
        # Создаем плейсхолдеры
        placeholders, params = self._generate_placeholders(unique_ids, "pid")
        
        declare_parts = []
        for i in range(len(unique_ids)):
            declare_parts.append(f"DECLARE $pid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        query = f"""
        {declare_block}
        SELECT 
            r.repost_id,
            r.original_post_id,
            r.user_id,
            r.comment,
            r.created_at,
            u.username,
            u.first_name_encrypted,
            u.last_name_encrypted,
            u.display_name,
            u.is_verified,
            ROW_NUMBER() OVER (PARTITION BY r.original_post_id ORDER BY r.created_at DESC) as rn
        FROM {self.table_name} r
        JOIN users u ON r.user_id = u.id
        WHERE r.original_post_id IN ({placeholders})
        QUALIFY rn <= {limit_per_post}
        ORDER BY r.original_post_id, r.created_at DESC;
        """
        
        try:
            rows = await self.execute(query, params)
            
            result = {}
            for row in rows:
                post_id = row['original_post_id']
                if post_id not in result:
                    result[post_id] = []
                
                # Декодируем имя
                first_name = safe_b64decode(row.get('first_name_encrypted', ''))
                last_name = safe_b64decode(row.get('last_name_encrypted', ''))
                
                display_name = row.get('display_name', '')
                if not display_name:
                    if first_name and last_name:
                        display_name = f"{first_name} {last_name}".strip()
                    elif first_name:
                        display_name = first_name
                    else:
                        display_name = row.get('username', f"user_{row['user_id'][:8]}")
                
                # Форматируем created_at
                created_at = row.get('created_at')
                if created_at:
                    if hasattr(created_at, 'isoformat'):
                        created_at_str = created_at.isoformat() + 'Z'
                        dt = created_at
                    elif isinstance(created_at, (int, float)):
                        dt = datetime.fromtimestamp(created_at / 1e6)
                        created_at_str = dt.isoformat() + 'Z'
                    else:
                        try:
                            dt = datetime.fromisoformat(str(created_at).replace('Z', '+00:00'))
                            created_at_str = dt.isoformat() + 'Z'
                        except:
                            dt = datetime.utcnow()
                            created_at_str = dt.isoformat() + 'Z'
                    
                    time_ago_str = time_ago(dt)
                else:
                    created_at_str = datetime.utcnow().isoformat() + 'Z'
                    time_ago_str = 'только что'
                
                result[post_id].append({
                    'repost_id': row['repost_id'],
                    'user_id': row['user_id'],
                    'username': row.get('username', ''),
                    'display_name': display_name,
                    'is_verified': row.get('is_verified', False),
                    'comment': row.get('comment', ''),
                    'created_at': created_at_str,
                    'time_ago': time_ago_str
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting many reposts: {e}")
            return {}
    
    async def count_by_original(self, original_post_id: str) -> int:
        """Получить количество репостов поста"""
        query = f"""
        DECLARE $original_post_id AS Utf8;
        
        SELECT COUNT(*) as cnt FROM {self.table_name}
        WHERE original_post_id = $original_post_id;
        """
        params = {'$original_post_id': original_post_id}
        
        try:
            result = await self.execute(query, params)
            return result[0]['cnt'] if result else 0
        except Exception as e:
            logger.error(f"Error counting reposts: {e}")
            return 0
    
    async def get_stats(self, original_post_id: str) -> Dict:
        """Получить статистику по репостам"""
        query = f"""
        DECLARE $original_post_id AS Utf8;
        
        SELECT 
            COUNT(*) as total_reposts,
            COUNT(DISTINCT user_id) as unique_users,
            MAX(created_at) as last_repost_at,
            MIN(created_at) as first_repost_at
        FROM {self.table_name}
        WHERE original_post_id = $original_post_id;
        """
        params = {'$original_post_id': original_post_id}
        
        try:
            result = await self.execute(query, params)
            if result:
                row = result[0]
                return {
                    'total_reposts': row['total_reposts'],
                    'unique_users': row['unique_users'],
                    'last_repost_at': from_timestamp(row['last_repost_at']).isoformat() + 'Z' if row['last_repost_at'] else None,
                    'first_repost_at': from_timestamp(row['first_repost_at']).isoformat() + 'Z' if row['first_repost_at'] else None
                }
            return {
                'total_reposts': 0,
                'unique_users': 0,
                'last_repost_at': None,
                'first_repost_at': None
            }
        except Exception as e:
            logger.error(f"Error getting repost stats: {e}")
            return {}
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


class ReactionRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_reactions - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_reactions"
        self._batch_size = 100
        self._query_cache = {}
    async def update_reaction(self, reaction_id: str, new_type: str, old_type: str, 
                               user_id: str, entity_type: str, entity_id: str,
                               uow: UnitOfWork) -> Optional[str]:
        """
        Обновить тип существующей реакции - без обновления счётчиков
        """
        logger.info(f"✏️ [update_reaction] START: reaction_id={reaction_id}, {old_type} -> {new_type}")
        
        now = datetime.utcnow()
        
        update_query = f"""
        DECLARE $reaction_id AS Utf8;
        DECLARE $reaction_type AS Utf8;
        DECLARE $updated_at AS Timestamp;
        
        UPDATE {self.table_name}
        SET 
            reaction_type = $reaction_type,
            updated_at = $updated_at
        WHERE reaction_id = $reaction_id
        RETURNING reaction_id, reaction_type;
        """
        
        params = {
            '$reaction_id': reaction_id,
            '$reaction_type': new_type,
            '$updated_at': to_timestamp(now)
        }
        
        try:
            result = await self.execute(update_query, params)
            if not result or len(result) == 0:
                logger.error(f"❌ [update_reaction] Reaction {reaction_id} not found")
                return None
            
            logger.info(f"✅ [update_reaction] Successfully updated {reaction_id} to {new_type}")
            await Metrics.inc_counter('reaction_updated')
            return reaction_id
            
        except Exception as e:
            logger.error(f"❌ [update_reaction] Error: {e}", exc_info=True)
            return None
    async def add(self, user_id: str, entity_type: str, entity_id: str,
                  reaction_type: str, created_at: datetime) -> Optional[str]:
        """Добавить реакцию с обработкой дубликатов"""
        reaction_id = str(uuid.uuid4())
        now_ts = to_timestamp(created_at)
        
        data = {
            'reaction_id': reaction_id,
            'user_id': user_id,
            'entity_type': entity_type,
            'entity_id': entity_id,
            'reaction_type': reaction_type,
            'created_at': now_ts,
            'updated_at': now_ts
        }
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ Added reaction {reaction_id}")
            await Metrics.inc_counter('reaction_added')
            return reaction_id
        except Exception as e:
            if "PRECONDITION_FAILED" in str(e) or "already exists" in str(e).lower():
                logger.warning(f"⚠️ Duplicate reaction for user {user_id} on {entity_type} {entity_id}")
                existing = await self.get_user_reaction(user_id, entity_type, entity_id)
                if existing:
                    if existing['reaction_type'] != reaction_type:
                        logger.info(f"✏️ Updating reaction from {existing['reaction_type']} to {reaction_type}")
                        await self.update_reaction(
                            reaction_id=existing['reaction_id'],
                            new_type=reaction_type,
                            old_type=existing['reaction_type'],
                            user_id=user_id,
                            entity_type=entity_type,
                            entity_id=entity_id,
                            uow=None
                        )
                    return existing['reaction_id']
            raise

    
    async def remove(self, user_id: str, entity_type: str, entity_id: str) -> bool:
        """Удалить реакцию - ИСПРАВЛЕНО"""
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        
        DELETE FROM {self.table_name}
        WHERE user_id = $user_id 
          AND entity_type = $entity_type 
          AND entity_id = $entity_id;
        """
        params = {
            '$user_id': str(user_id),
            '$entity_type': entity_type,
            '$entity_id': entity_id
        }
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ Removed reaction for user {user_id} on {entity_type} {entity_id}")
            await Metrics.inc_counter('reaction_removed')
            return True
        except Exception as e:
            logger.error(f"❌ Error removing reaction: {e}")
            return False
    
    async def get_user_reaction(self, user_id: str, entity_type: str, 
                                 entity_id: str) -> Optional[Dict]:
        """
        Получить реакцию пользователя - ИСПРАВЛЕНО (правильный парсинг YDB)
        """
        logger.info(f"🔍 [get_user_reaction] START: user_id={user_id}, entity_type={entity_type}, entity_id={entity_id}")
        
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        
        SELECT reaction_id, reaction_type, created_at
        FROM {self.table_name}
        WHERE user_id = $user_id 
          AND entity_type = $entity_type 
          AND entity_id = $entity_id
        ORDER BY created_at DESC
        LIMIT 1;
        """
        params = {
            '$user_id': user_id,
            '$entity_type': entity_type,
            '$entity_id': entity_id
        }
        
        try:
            # Получаем сессию
            session = await self._get_session()
            
            # prepare - синхронный
            prepared = session.prepare(query)
            
            # execute - синхронный (без await!)
            if self._transaction:
                result = self._transaction.execute(prepared, params)
            else:
                result = session.transaction().execute(prepared, params, commit_tx=True)
            
            logger.info(f"🔍 [get_user_reaction] Raw result type: {type(result)}")
            
            if result and len(result) > 0:
                result_set = result[0]
                logger.info(f"🔍 [get_user_reaction] ResultSet type: {type(result_set)}")
                
                if hasattr(result_set, 'rows') and result_set.rows:
                    rows = result_set.rows
                    logger.info(f"🔍 [get_user_reaction] Rows count: {len(rows)}")
                    
                    if len(rows) > 0:
                        row = rows[0]
                        logger.info(f"🔍 [get_user_reaction] Row type: {type(row)}")
                        
                        # Получаем значения по индексам из columns
                        if hasattr(result_set, 'columns'):
                            columns = result_set.columns
                            result_dict = {}
                            
                            for i, col in enumerate(columns):
                                col_name = col.name
                                if hasattr(row, '__getitem__'):
                                    try:
                                        value = row[i]
                                        result_dict[col_name] = value
                                        logger.info(f"🔍 [get_user_reaction] {col_name} = {value}")
                                    except:
                                        result_dict[col_name] = None
                            
                            if result_dict.get('reaction_id'):
                                logger.info(f"✅ Found reaction: {result_dict}")
                                return result_dict
                        else:
                            # Fallback: пробуем через атрибуты
                            reaction_id = getattr(row, 'reaction_id', None)
                            reaction_type = getattr(row, 'reaction_type', None)
                            created_at = getattr(row, 'created_at', None)
                            
                            if reaction_id:
                                logger.info(f"✅ Found reaction via attributes: {reaction_id}")
                                return {
                                    'reaction_id': reaction_id,
                                    'reaction_type': reaction_type,
                                    'created_at': created_at
                                }
            
            logger.info(f"ℹ️ No reaction found")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error in get_user_reaction: {e}", exc_info=True)
            return None
    
   
    
    async def get_reaction_counts(self, entity_type: str, entity_id: str) -> List[Dict]:
        """
        Получить количество реакций для сущности, считая на лету из feed_reactions
        """
        query = f"""
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        
        SELECT reaction_type, COUNT(*) as count
        FROM feed_reactions
        WHERE entity_type = $entity_type AND entity_id = $entity_id
        GROUP BY reaction_type;
        """
        params = {'$entity_type': entity_type, '$entity_id': entity_id}
        try:
            result = await self.execute(query, params)
            return result if result else []
        except Exception as e:
            logger.error(f"❌ Error getting reaction counts: {e}")
            return []
    
    async def get_user_reactions_for_entities(self, user_id: str, 
                                               entity_type: str, 
                                               entity_ids: List[str]) -> Dict[str, str]:
        """Получить реакции пользователя для списка сущностей (BATCH)"""
        if not entity_ids:
            return {}
        
        unique_ids = list(set(entity_ids))
        result = {}
        
        # Разбиваем на чанки для оптимальной производительности
        for i in range(0, len(unique_ids), feed_config.BATCH_SIZE_LIKES):
            chunk = unique_ids[i:i+feed_config.BATCH_SIZE_LIKES]
            chunk_result = await self._get_user_reactions_chunk(user_id, entity_type, chunk)
            result.update(chunk_result)
        
        await Metrics.inc_counter('reaction_queries')
        return result
    
    async def _get_user_reactions_chunk(self, user_id: str, entity_type: str, 
                                         entity_ids: List[str]) -> Dict[str, str]:
        """Получить реакции для чанка сущностей"""
        placeholders, params = self._generate_placeholders(entity_ids, "eid")
        params['$user_id'] = str(user_id)
        params['$entity_type'] = entity_type
        
        declare_parts = [
            "DECLARE $user_id AS Utf8;",
            "DECLARE $entity_type AS Utf8;"
        ]
        for i in range(len(entity_ids)):
            declare_parts.append(f"DECLARE $eid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        query = f"""
        {declare_block}
        SELECT entity_id, reaction_type
        FROM {self.table_name}
        WHERE user_id = $user_id 
          AND entity_type = $entity_type
          AND entity_id IN ({placeholders});
        """
        
        try:
            result = await self.execute(query, params)
            return {row['entity_id']: row['reaction_type'] for row in result}
        except Exception as e:
            logger.error(f"❌ Error getting user reactions: {e}")
            return {}
    
    async def get_reactions_with_users(self, entity_type: str, entity_id: str, 
                                        limit: int = 20, offset: int = 0) -> List[Dict]:
        """Получить список реакций с данными пользователей"""
        query = f"""
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT 
            r.reaction_id,
            r.user_id,
            r.reaction_type,
            r.created_at,
            u.username,
            u.first_name_encrypted,
            u.last_name_encrypted,
            u.display_name,
            u.is_verified
        FROM {self.table_name} r
        JOIN users u ON r.user_id = u.id
        WHERE r.entity_type = $entity_type AND r.entity_id = $entity_id
        ORDER BY r.created_at DESC
        LIMIT $limit OFFSET $offset;
        """
        params = {
            '$entity_type': entity_type,
            '$entity_id': entity_id,
            '$limit': limit,
            '$offset': offset
        }
        
        try:
            result = await self.execute(query, params)
            await Metrics.inc_counter('reaction_queries')
            return result if result else []
        except Exception as e:
            logger.error(f"❌ Error getting reactions with users: {e}")
            return []
    
    async def get_reactions_counts_for_entities(self, entity_type: str, entity_ids: List[str]) -> Dict[str, List[Dict]]:
        """
        Получить счетчики реакций для нескольких сущностей одним запросом
        """
        if not entity_ids:
            return {}
        
        unique_ids = list(set(entity_ids))
        result = {}
        for i in range(0, len(unique_ids), feed_config.BATCH_SIZE_LIKES):
            chunk = unique_ids[i:i+feed_config.BATCH_SIZE_LIKES]
            chunk_result = await self._get_reactions_counts_chunk(entity_type, chunk)
            result.update(chunk_result)
        return result

    async def _get_reactions_counts_chunk(self, entity_type: str, entity_ids: List[str]) -> Dict[str, List[Dict]]:
        placeholders, params = self._generate_placeholders(entity_ids, "eid")
        params['$entity_type'] = entity_type
        
        declare_parts = ["DECLARE $entity_type AS Utf8;"]
        for i in range(len(entity_ids)):
            declare_parts.append(f"DECLARE $eid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        query = f"""
        {declare_block}
        SELECT entity_id, reaction_type, COUNT(*) as count
        FROM feed_reactions
        WHERE entity_type = $entity_type AND entity_id IN ({placeholders})
        GROUP BY entity_id, reaction_type;
        """
        
        try:
            rows = await self.execute(query, params)
            reactions_map = {}
            for row in rows:
                eid = row['entity_id']
                if eid not in reactions_map:
                    reactions_map[eid] = []
                reactions_map[eid].append({
                    'reaction_type': row['reaction_type'],
                    'count': row['count']
                })
            return reactions_map
        except Exception as e:
            logger.error(f"❌ Error getting reactions counts chunk: {e}")
            return {}


    
    async def toggle_reaction(self, uow: UnitOfWork, user_id: str, entity_type: str, 
                               entity_id: str, reaction_type: str) -> Dict:
        """
        Переключение реакции - без обновления счётчиков (теперь считаем на лету)
        """
        logger.info(f"🔄 [toggle_reaction] START: user_id={user_id}, entity_type={entity_type}, entity_id={entity_id}, reaction_type={reaction_type}")
        now = datetime.utcnow()
        
        try:
            # 1. Проверяем, есть ли реакция
            existing = await self.get_user_reaction(user_id, entity_type, entity_id)
            logger.info(f"🔍 [toggle_reaction] Existing: {existing}")
            
            # 2. Если реакция есть
            if existing:
                existing_type = existing['reaction_type']
                
                # Если тот же тип - удаляем
                if existing_type == reaction_type:
                    logger.info(f"🗑️ [toggle_reaction] Removing same type {existing_type}")
                    await self.remove(user_id, entity_type, entity_id)
                    return {
                        'action': 'removed',
                        'reaction_type': None,
                        'reaction_id': existing['reaction_id']
                    }
                
                # Если другой тип - обновляем
                else:
                    logger.info(f"✏️ [toggle_reaction] Updating from {existing_type} to {reaction_type}")
                    updated = await self.update_reaction(
                        reaction_id=existing['reaction_id'],
                        new_type=reaction_type,
                        old_type=existing_type,
                        user_id=user_id,
                        entity_type=entity_type,
                        entity_id=entity_id,
                        uow=uow
                    )
                    if not updated:
                        return {
                            'action': 'error',
                            'error': 'Failed to update reaction',
                            'reaction_id': existing['reaction_id']
                        }
                    return {
                        'action': 'updated',
                        'reaction_type': reaction_type,
                        'reaction_id': existing['reaction_id'],
                        'old_type': existing_type
                    }
            
            # 3. Если реакции нет - добавляем новую
            logger.info(f"➕ [toggle_reaction] Adding new {reaction_type}")
            reaction_id = await self.add(user_id, entity_type, entity_id, reaction_type, now)
            if not reaction_id:
                return {
                    'action': 'error',
                    'error': 'Failed to add reaction'
                }
            
            return {
                'action': 'added',
                'reaction_type': reaction_type,
                'reaction_id': reaction_id
            }
            
        except Exception as e:
            logger.error(f"❌ [toggle_reaction] Error: {e}", exc_info=True)
            return {
                'action': 'error',
                'error': str(e)
            }
    async def get_reaction_stats(self, entity_type: str, entity_id: str) -> Dict:
        """Получить статистику по реакциям (кто, когда, сколько)"""
        query = f"""
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        
        SELECT 
            COUNT(DISTINCT user_id) as unique_users,
            MAX(created_at) as last_reaction_at,
            MIN(created_at) as first_reaction_at,
            COUNT(*) as total_reactions
        FROM {self.table_name}
        WHERE entity_type = $entity_type AND entity_id = $entity_id;
        """
        params = {
            '$entity_type': entity_type,
            '$entity_id': entity_id
        }
        
        try:
            result = await self.execute(query, params)
            if result and len(result) > 0:
                row = result[0]
                return {
                    'unique_users': row['unique_users'] or 0,
                    'total_reactions': row['total_reactions'] or 0,
                    'last_reaction_at': from_timestamp(row['last_reaction_at']).isoformat() + 'Z' if row['last_reaction_at'] else None,
                    'first_reaction_at': from_timestamp(row['first_reaction_at']).isoformat() + 'Z' if row['first_reaction_at'] else None
                }
            return {
                'unique_users': 0,
                'total_reactions': 0,
                'last_reaction_at': None,
                'first_reaction_at': None
            }
        except Exception as e:
            logger.error(f"❌ Error getting reaction stats: {e}")
            return {}
    
    async def get_user_reactions_history(self, user_id: str, entity_type: Optional[str] = None,
                                          limit: int = 50, offset: int = 0) -> List[Dict]:
        """Получить историю реакций пользователя"""
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        """
        
        params = {
            '$user_id': str(user_id),
            '$limit': limit,
            '$offset': offset
        }
        
        where_clause = "user_id = $user_id"
        if entity_type:
            where_clause += " AND entity_type = $entity_type"
            params['$entity_type'] = entity_type
            query += "DECLARE $entity_type AS Utf8;\n"
        
        query += f"""
        SELECT 
            reaction_id,
            entity_type,
            entity_id,
            reaction_type,
            created_at,
            updated_at
        FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT $limit OFFSET $offset;
        """
        
        try:
            result = await self.execute(query, params)
            await Metrics.inc_counter('reaction_queries')
            return result if result else []
        except Exception as e:
            logger.error(f"❌ Error getting user reactions history: {e}")
            return []
    
    async def cleanup_old_reactions(self, days: int = 30) -> int:
        """Очистить старые реакции (для админки)"""
        cutoff = to_timestamp(datetime.utcnow() - timedelta(days=days))
        
        query = f"""
        DECLARE $cutoff AS Timestamp;
        
        DELETE FROM {self.table_name}
        WHERE created_at < $cutoff;
        """
        params = {'$cutoff': cutoff}
        
        try:
            await self.execute(query, params)
            logger.info(f"🧹 Cleaned up reactions older than {days} days")
            return 0
        except Exception as e:
            logger.error(f"❌ Error cleaning up reactions: {e}")
            return 0
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        """Сгенерировать плейсхолдеры для IN запроса"""
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


class BookmarkRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_bookmarks"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_bookmarks"
    async def count(self, user_id: str, post_id: str) -> int:
        """Получить количество закладок (0 или 1) - ФИНАЛЬНАЯ ВЕРСИЯ"""
        logger.info(f"🔢 BookmarkRepository.count: user_id={user_id}, post_id={post_id}")
        
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $post_id AS Utf8;
        
        SELECT COUNT(*) as cnt
        FROM {self.table_name}
        WHERE user_id = $user_id AND post_id = $post_id;
        """
        params = {'$user_id': str(user_id), '$post_id': post_id}
        
        try:
            # Получаем сессию
            session = await self._get_session()
            
            # prepare - синхронный
            prepared = session.prepare(query)
            
            # execute - синхронный (без await!)
            if self._transaction:
                result = self._transaction.execute(prepared, params)
            else:
                result = session.transaction().execute(prepared, params, commit_tx=True)
            
            logger.info(f"📦 Result type: {type(result)}")
            
            if result and len(result) > 0:
                result_set = result[0]
                logger.info(f"📦 ResultSet type: {type(result_set)}")
                
                if hasattr(result_set, 'rows') and result_set.rows:
                    rows = result_set.rows
                    logger.info(f"📦 Rows count: {len(rows)}")
                    
                    if len(rows) > 0:
                        row = rows[0]
                        logger.info(f"📦 Row type: {type(row)}")
                        
                        # В YDB _Row хранит значения в списке, а имена колонок в _columns
                        if hasattr(row, '_columns') and hasattr(result_set, 'columns'):
                            # Ищем индекс колонки 'cnt'
                            columns = result_set.columns
                            for i, col in enumerate(columns):
                                if col.name == 'cnt':
                                    if hasattr(row, '__getitem__'):
                                        # Пробуем получить по индексу
                                        try:
                                            count_value = row[i]
                                            logger.info(f"✅ Count from index {i}: {count_value}")
                                            return int(count_value) if count_value is not None else 0
                                        except:
                                            pass
                        
                        # Альтернативный способ - через _values если есть
                        if hasattr(row, '_values') and row._values:
                            count_value = row._values[0]
                            logger.info(f"✅ Count from _values: {count_value}")
                            return int(count_value) if count_value is not None else 0
                        
                        # Пробуем через getattr
                        if hasattr(row, 'cnt'):
                            count_value = row.cnt
                            logger.info(f"✅ Count from attribute: {count_value}")
                            return int(count_value) if count_value is not None else 0
            
            logger.warning("⚠️ Could not extract count value, returning 0")
            return 0
            
        except Exception as e:
            logger.error(f"❌ Error counting bookmarks: {e}", exc_info=True)
            return 0
    async def check(self, user_id: str, post_id: str) -> bool:
        """Проверить, есть ли закладка - ИСПРАВЛЕНО (правильный парсинг YDB ответа)"""
        logger.info(f"🔍 BookmarkRepository.check: user_id={user_id}, post_id={post_id}")
        
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $post_id AS Utf8;
        
        SELECT COUNT(*) as cnt
        FROM {self.table_name}
        WHERE user_id = $user_id AND post_id = $post_id;
        """
        params = {'$user_id': str(user_id), '$post_id': post_id}
        
        try:
            logger.info(f"📝 Executing query with params: {params}")
            result = await self.execute(query, params)
            
            logger.info(f"📦 Raw result: {result}")
            
            if not result or len(result) == 0:
                logger.info("📭 Result is empty or None")
                return False
            
            # В YDB результат может быть в разных форматах
            # Нужно получить значение из первой строки первого столбца
            
            # Способ 1: через result_set.rows[0][0] если execute вернул список списков
            try:
                if hasattr(result, 'rows') and result.rows:
                    # Если это ResultSet
                    row = result.rows[0]
                    if hasattr(row, 'cnt'):
                        cnt_value = row.cnt
                        logger.info(f"📦 Got cnt from row.cnt: {cnt_value}")
                        return cnt_value > 0
                elif isinstance(result, list) and len(result) > 0:
                    row = result[0]
                    if isinstance(row, dict) and 'cnt' in row:
                        # Обычный словарь
                        cnt_value = row['cnt']
                        logger.info(f"📦 Got cnt from dict: {cnt_value}")
                        return cnt_value > 0
                    elif hasattr(row, 'cnt'):
                        # Объект с атрибутом
                        cnt_value = row.cnt
                        logger.info(f"📦 Got cnt from attribute: {cnt_value}")
                        return cnt_value > 0
                    elif hasattr(row, '_values') and row._values:
                        # YDB Row с _values
                        cnt_value = row._values[0]
                        logger.info(f"📦 Got cnt from _values: {cnt_value}")
                        return cnt_value > 0
            except Exception as e:
                logger.error(f"❌ Error parsing result: {e}")
            
            # Способ 2: если execute вернул [{'cnt': значение}]
            # Но из логов видим, что это не так
            
            logger.warning("⚠️ Could not extract count value, assuming 0")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking bookmark: {e}", exc_info=True)
            return False
    
    async def check_many(self, user_id: str, post_ids: List[str]) -> Dict[str, bool]:
        """Проверить закладки для нескольких постов - ИСПРАВЛЕНО"""
        if not post_ids:
            return {}
        
        unique_ids = list(set(post_ids))
        result = {}
        for i in range(0, len(unique_ids), feed_config.BATCH_SIZE_BOOKMARKS):
            chunk = unique_ids[i:i+feed_config.BATCH_SIZE_BOOKMARKS]
            chunk_result = await self._check_many_chunk(user_id, chunk)
            result.update(chunk_result)
        
        return result
    
    async def _check_many_chunk(self, user_id: str, post_ids: List[str]) -> Dict[str, bool]:
        """Проверить закладки для чанка постов - ИСПРАВЛЕНО"""
        placeholders, params = self._generate_placeholders(post_ids, "pid")
        params['$user_id'] = str(user_id)
        
        declare_parts = ["DECLARE $user_id AS Utf8;"]
        for i in range(len(post_ids)):
            declare_parts.append(f"DECLARE $pid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        query = f"""
        {declare_block}
        SELECT post_id FROM {self.table_name}
        WHERE user_id = $user_id AND post_id IN ({placeholders});
        """
        
        try:
            result = await self.execute(query, params)
            # ✅ Здесь все правильно - просто проверяем наличие post_id в результате
            bookmarked = {row['post_id']: True for row in result}
            return {pid: pid in bookmarked for pid in post_ids}
        except Exception as e:
            logger.error(f"Error checking many bookmarks: {e}")
            return {}
    
   
    async def add(self, user_id: str, post_id: str, folder: str, notes: str,
                  created_at: datetime) -> Optional[str]:
        """Добавить закладку - ИСПРАВЛЕНО (использует существующую сессию)"""
        bookmark_id = str(uuid.uuid4())
        
        data = {
            'bookmark_id': bookmark_id,
            'user_id': str(user_id),
            'post_id': post_id,
            'folder': folder,
            'notes': notes,
            'created_at': to_timestamp(created_at)
        }
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        
        # ✅ ВАЖНО: НЕ вызываем _generate_declare, потому что execute сам сгенерирует
        # И НЕ создаем новую сессию
        
        query = f"""
        DECLARE $bookmark_id AS Utf8;
        DECLARE $user_id AS Utf8;
        DECLARE $post_id AS Utf8;
        DECLARE $folder AS Utf8;
        DECLARE $notes AS Utf8;
        DECLARE $created_at AS Timestamp;
        
        UPSERT INTO {self.table_name} ({columns}) VALUES ($bookmark_id, $user_id, $post_id, $folder, $notes, $created_at);
        """
        
        params = {
            '$bookmark_id': bookmark_id,
            '$user_id': str(user_id),
            '$post_id': post_id,
            '$folder': folder,
            '$notes': notes,
            '$created_at': to_timestamp(created_at)
        }
        
        try:
            # ✅ Используем execute с существующей сессией
            await self.execute(query, params)
            logger.info(f"✅ Bookmark added: {bookmark_id} for user {user_id} on post {post_id}")
            return bookmark_id
        except Exception as e:
            error_str = str(e).lower()
            if "unique" in error_str or "duplicate" in error_str or "conflict" in error_str:
                logger.warning(f"⚠️ Duplicate bookmark attempt for user {user_id} on post {post_id}")
                return None
            logger.error(f"❌ Error adding bookmark: {e}")
            return None
    
    async def remove(self, user_id: str, post_id: str) -> bool:
        """Удалить закладку"""
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $post_id AS Utf8;
        
        DELETE FROM {self.table_name}
        WHERE user_id = $user_id AND post_id = $post_id;
        """
        params = {'$user_id': str(user_id), '$post_id': post_id}
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error removing bookmark: {e}")
            return False
    
    async def list_by_user_with_cursor(self, user_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить закладки пользователя с курсорной пагинацией - УПРОЩЕНО"""
        # Временно убираем курсор для теста
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        
        SELECT 
            b.bookmark_id,
            b.user_id,
            b.post_id,
            b.folder,
            b.notes,
            b.created_at
        FROM {self.table_name} b
        WHERE b.user_id = $user_id
        ORDER BY b.created_at DESC
        LIMIT $limit;
        """
        params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            return rows, None
        except Exception as e:
            logger.error(f"Error listing bookmarks: {e}")
            return [], None
    
    async def toggle_atomic(self, uow: UnitOfWork, user_id: str, post_id: str, folder: str, notes: str, now: datetime) -> Dict:
        """
        Атомарное переключение закладки - ИСПРАВЛЕНО (использует count вместо check)
        """
        try:
            # Используем новый метод count
            count_value = await self.count(user_id, post_id)
            exists = count_value > 0
            
            if exists:
                # Если есть - удаляем
                remove_success = await self.remove(user_id, post_id)
                if not remove_success:
                    logger.error(f"❌ Failed to remove bookmark")
                    return {'bookmarked': False, 'bookmarks_count': 0}
                
                post_repo = PostRepository(self._session)
                new_count = await post_repo.increment_bookmarks(post_id, -1)
                
                return {'bookmarked': False, 'bookmarks_count': new_count}
            else:
                # Если нет - добавляем
                bookmark_id = await self.add(user_id, post_id, folder, notes, now)
                if not bookmark_id:
                    logger.error(f"❌ Failed to add bookmark")
                    return {'bookmarked': False, 'bookmarks_count': 0}
                
                post_repo = PostRepository(self._session)
                new_count = await post_repo.increment_bookmarks(post_id, 1)
                
                return {'bookmarked': True, 'bookmarks_count': new_count}
                
        except Exception as e:
            logger.error(f"❌ Error in toggle_atomic: {e}")
            return {'bookmarked': False, 'bookmarks_count': 0}
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params

class CommentRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_comments - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_comments"
        self._batch_size = 100
    async def get_root_comments_with_cursor(self, post_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        Получить ТОЛЬКО корневые комментарии поста (без ответов)
        """
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $post_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE post_id = $post_id 
              AND (parent_comment_id IS NULL OR parent_comment_id = '')
              AND is_deleted = false
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND comment_id < $last_id))
            ORDER BY created_at DESC, comment_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$post_id': post_id,
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $post_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE post_id = $post_id 
              AND (parent_comment_id IS NULL OR parent_comment_id = '')
              AND is_deleted = false
            ORDER BY created_at DESC, comment_id DESC
            LIMIT $limit + 1;
            """
            params = {'$post_id': post_id, '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['created_at']}:{last_item['comment_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"❌ Error getting root comments: {e}")
            return [], None
    
    async def get_replies_with_cursor(self, parent_comment_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        Получить прямые ответы на комментарий (только один уровень)
        """
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $parent_comment_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE parent_comment_id = $parent_comment_id
              AND is_deleted = false
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND comment_id < $last_id))
            ORDER BY created_at ASC, comment_id ASC
            LIMIT $limit + 1;
            """
            params = {
                '$parent_comment_id': parent_comment_id,
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $parent_comment_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE parent_comment_id = $parent_comment_id
              AND is_deleted = false
            ORDER BY created_at ASC, comment_id ASC
            LIMIT $limit + 1;
            """
            params = {'$parent_comment_id': parent_comment_id, '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['created_at']}:{last_item['comment_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"❌ Error getting replies: {e}")
            return [], None
    async def get_by_post_with_cursor(self, post_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить корневые комментарии поста с курсором"""
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $post_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE post_id = $post_id 
              AND (parent_comment_id IS NULL OR parent_comment_id = '')
              AND is_deleted = false
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND comment_id < $last_id))
            ORDER BY created_at DESC, comment_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$post_id': post_id,
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $post_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE post_id = $post_id 
              AND (parent_comment_id IS NULL OR parent_comment_id = '')
              AND is_deleted = false
            ORDER BY created_at DESC, comment_id DESC
            LIMIT $limit + 1;
            """
            params = {'$post_id': post_id, '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['created_at']}:{last_item['comment_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"❌ Error getting comments with cursor: {e}")
            return [], None
    async def increment_replies_chain(self, parent_id: str) -> int:
        logger.info(f"📊 increment_replies_chain START: parent_id={parent_id}")
        updated_count = 0
        current_id = parent_id
        
        while current_id:
            logger.info(f"📊 Processing comment: {current_id}")
            
            select_query = f"""
            DECLARE $comment_id AS Utf8;
            SELECT comment_id, parent_comment_id, replies_count 
            FROM {self.table_name} 
            WHERE comment_id = $comment_id;
            """
            
            rows = await self.execute(select_query, {'$comment_id': current_id})
            logger.info(f"📊 rows = {rows}")
            
            if not rows:
                logger.warning(f"⚠️ Comment {current_id} not found, stopping chain")
                break
            
            row = rows[0]
            logger.info(f"📊 row = {row}")
            current_parent_id = row.get('parent_comment_id')
            current_count = row.get('replies_count', 0)
            logger.info(f"📊 parent_comment_id = {current_parent_id}, replies_count = {current_count}")
            
            new_count = current_count + 1
            logger.info(f"📊 Updating comment {current_id}: {current_count} -> {new_count}")
            
            update_query = f"""
            DECLARE $comment_id AS Utf8;
            DECLARE $new_count AS Uint32;
            UPDATE {self.table_name} 
            SET replies_count = $new_count 
            WHERE comment_id = $comment_id;
            """
            
            try:
                await self.execute(update_query, {
                    '$comment_id': current_id,
                    '$new_count': new_count
                })
                updated_count += 1
                logger.info(f"✅ Updated comment {current_id}")
            except Exception as e:
                logger.error(f"❌ Failed to update comment {current_id}: {e}")
                break
            
            if current_parent_id:
                current_id = current_parent_id
                logger.info(f"📊 Moving to parent: {current_id}")
            else:
                logger.info(f"📊 No parent found for {current_id}, stopping chain")
                break
        
        logger.info(f"✅ increment_replies_chain finished, updated {updated_count} comments")
        return updated_count
    async def decrement_replies_chain(self, parent_id: str, value: int) -> int:
        """
        Уменьшить replies_count у ВСЕХ родителей в цепочке
        """
        logger.info(f"📊 decrement_replies_chain START: parent_id={parent_id}, value={value}")
        updated_count = 0
        current_id = parent_id
        
        while current_id:
            logger.info(f"📊 Processing comment: {current_id}")
            
            select_query = f"""
            DECLARE $comment_id AS Utf8;
            SELECT parent_comment_id, replies_count 
            FROM {self.table_name} 
            WHERE comment_id = $comment_id;
            """
            result = await self.execute(select_query, {'$comment_id': current_id})
            
            if not result:
                logger.warning(f"⚠️ Comment {current_id} not found, stopping chain")
                break
            
            row = result[0]
            current_parent_id = row.get('parent_comment_id')
            current_count = row.get('replies_count', 0)
            
            new_count = max(0, current_count - value)
            logger.info(f"📊 Updating comment {current_id}: {current_count} -> {new_count}")
            
            update_query = f"""
            DECLARE $comment_id AS Utf8;
            DECLARE $new_count AS Uint32;
            
            UPDATE {self.table_name} 
            SET replies_count = $new_count 
            WHERE comment_id = $comment_id;
            """
            
            try:
                await self.execute(update_query, {
                    '$comment_id': current_id,
                    '$new_count': new_count
                })
                updated_count += 1
                logger.info(f"✅ Updated comment {current_id}")
            except Exception as e:
                logger.error(f"❌ Failed to update comment {current_id}: {e}")
                break
            
            # 🔥 Переходим к родителю
            current_id = current_parent_id if current_parent_id and current_parent_id != '' else None
            logger.info(f"📊 Next parent: {current_id}")
        
        logger.info(f"✅ decrement_replies_chain finished, updated {updated_count} comments")
        return updated_count
    async def count_all_replies(self, comment_id: str) -> int:
        """
        Подсчитать ВСЕ живые (не удалённые) вложенные комментарии (рекурсивно)
        """
        total = 0
        queue = [comment_id]

        while queue:
            current_id = queue.pop(0)

            select_query = f"""
            DECLARE $parent_id AS Utf8;
            SELECT comment_id FROM {self.table_name}
            WHERE parent_comment_id = $parent_id AND is_deleted = false;
            """
            result = await self.execute(select_query, {'$parent_id': current_id})

            for row in result:
                child_id = row['comment_id']
                total += 1
                queue.append(child_id)

        return total
    def _get_entity_type(self) -> str:
        return 'comment'
    
    async def increment_reactions(self, uow: UnitOfWork, entity_id: str, reaction_type: str, delta: int = 1) -> int:
        """
        Атомарно изменить счётчик реакций в таблице feed_reaction_counts.
        Возвращает новое значение счётчика для указанного типа реакции.
        """
        entity_type = self._get_entity_type()
        now_ts = to_timestamp(datetime.utcnow())
        
        # Получаем текущее значение
        select_query = f"""
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        DECLARE $reaction_type AS Utf8;
        
        SELECT count FROM feed_reaction_counts
        WHERE entity_type = $entity_type
          AND entity_id = $entity_id
          AND reaction_type = $reaction_type;
        """
        
        params = {
            '$entity_type': entity_type,
            '$entity_id': entity_id,
            '$reaction_type': reaction_type
        }
        
        try:
            result = await self.execute(select_query, params)
            current_count = 0
            if result and result[0]:
                current_count = result[0].get('count', 0)
        except Exception as e:
            logger.error(f"❌ Error reading reaction count: {e}")
            current_count = 0
        
        # Вычисляем новое значение
        new_count = max(0, current_count + delta)
        
        logger.info(f"📊 increment_reactions: {entity_type} {entity_id}, {reaction_type}, current={current_count}, delta={delta}, new={new_count}")
        
        # Если счётчик стал 0 - удаляем запись
        if new_count == 0:
            delete_query = f"""
            DECLARE $entity_type AS Utf8;
            DECLARE $entity_id AS Utf8;
            DECLARE $reaction_type AS Utf8;
            
            DELETE FROM feed_reaction_counts
            WHERE entity_type = $entity_type
              AND entity_id = $entity_id
              AND reaction_type = $reaction_type;
            """
            
            try:
                await self.execute(delete_query, params)
                logger.info(f"🗑️ Deleted reaction count for {entity_type} {entity_id}, {reaction_type} (count=0)")
                return 0
            except Exception as e:
                logger.error(f"❌ Error deleting reaction count: {e}")
                return 0
        
        # Обновляем или вставляем запись
        upsert_query = f"""
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        DECLARE $reaction_type AS Utf8;
        DECLARE $count AS Uint64;
        DECLARE $now AS Timestamp;
        
        UPSERT INTO feed_reaction_counts (entity_type, entity_id, reaction_type, count, updated_at)
        VALUES ($entity_type, $entity_id, $reaction_type, $count, $now);
        """
        
        upsert_params = {
            '$entity_type': entity_type,
            '$entity_id': entity_id,
            '$reaction_type': reaction_type,
            '$count': new_count,
            '$now': now_ts
        }
        
        try:
            await self.execute(upsert_query, upsert_params)
            logger.info(f"✅ Updated reaction count for {entity_type} {entity_id}, {reaction_type}: {new_count}")
            return new_count
        except Exception as e:
            logger.error(f"❌ Error updating reaction count: {e}")
            return current_count
    
    async def get_reactions_detail(self, entity_id: str) -> Dict[str, int]:
        """Получить детализацию реакций по типам"""
        entity_type = self._get_entity_type()
        
        query = f"""
        DECLARE $entity_type AS Utf8;
        DECLARE $entity_id AS Utf8;
        
        SELECT reaction_type, count
        FROM feed_reaction_counts
        WHERE entity_type = $entity_type AND entity_id = $entity_id;
        """
        
        try:
            result = await self.execute(query, {
                '$entity_type': entity_type,
                '$entity_id': entity_id
            })
            return {row['reaction_type']: row['count'] for row in result} if result else {}
        except Exception as e:
            logger.error(f"❌ Error getting reactions detail: {e}")
            return {}
    async def create(self, comment_id: str, post_id: str, user_id: str,
                     content: str, content_preview: str, parent_comment_id: Optional[str],
                     created_at: datetime) -> bool:
        """Создать комментарий"""
        parent_id = parent_comment_id if parent_comment_id is not None else ''
        
        data = {
            'comment_id': comment_id,
            'post_id': post_id,
            'user_id': str(user_id),
            'parent_comment_id': parent_id,
            'content': content,
            'content_preview': content_preview,
            'created_at': to_timestamp(created_at),
            'replies_count': 0,
            'reactions_count': 0,  # 👈 ДОБАВЛЕНО
            'reactions_json': '{}',  # 👈 ДОБАВЛЕНО
            'is_edited': False,
            'is_deleted': False
        }
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ Created comment {comment_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Error creating comment: {e}")
            return False
    
    async def get_by_id(self, comment_id: str) -> Optional[Dict]:
        """Получить комментарий по ID"""
        query = f"""
        DECLARE $comment_id AS Utf8;
        SELECT comment_id, post_id, user_id, parent_comment_id, content, 
               content_preview, created_at, replies_count, reactions_count,
               is_edited, is_deleted, deleted_at
        FROM {self.table_name}
        WHERE comment_id = $comment_id AND is_deleted = false;
        """
        params = {'$comment_id': comment_id}
        
        try:
            result = await self.execute(query, params)
            return result[0] if result else None
        except Exception as e:
            logger.error(f"❌ Error getting comment: {e}")
            return None
    
   
    
    async def get_all_replies_fallback(self, parent_comment_ids: List[str], max_depth: int = 10) -> List[Dict]:
        """
        Получить все ответы для списка родительских комментариев (итеративный подход)
        Работает в YDB и любых других БД
        """
        if not parent_comment_ids:
            return []
        
        all_replies = []
        current_ids = parent_comment_ids.copy()
        depth = 1
        
        while current_ids and depth <= max_depth:
            # Создаем плейсхолдеры для IN запроса
            placeholders = []
            params = {}
            for i, pid in enumerate(current_ids):
                placeholder = f"$pid_{i}"
                placeholders.append(placeholder)
                params[placeholder] = pid
            
            placeholders_str = ', '.join(placeholders)
            
            # Создаем DECLARE для каждого параметра
            declare_parts = []
            for i in range(len(current_ids)):
                declare_parts.append(f"DECLARE $pid_{i} AS Utf8;")
            declare_block = "\n".join(declare_parts)
            
            query = f"""
            {declare_block}
            SELECT *
            FROM {self.table_name}
            WHERE parent_comment_id IN ({placeholders_str}) 
              AND is_deleted = false
            ORDER BY created_at ASC;
            """
            
            try:
                replies = await self.execute(query, params)
                
                if not replies:
                    break
                
                # Добавляем найденные ответы в общий список
                all_replies.extend(replies)
                
                # Подготавливаем ID для следующего уровня (ищем ответы на эти ответы)
                current_ids = [r['comment_id'] for r in replies]
                depth += 1
                
                logger.debug(f"✅ Found {len(replies)} replies at depth {depth-1}")
                
            except Exception as e:
                logger.error(f"❌ Error in fallback reply fetch at depth {depth}: {e}")
                break
        
        logger.info(f"✅ Fallback: Found {len(all_replies)} total replies")
        return all_replies
    
    async def get_replies(self, parent_comment_id: str, limit: int = 10) -> List[Dict]:
        """Получить прямые ответы на комментарий (один уровень)"""
        query = f"""
        DECLARE $parent_comment_id AS Utf8;
        DECLARE $limit AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE parent_comment_id = $parent_comment_id AND is_deleted = false
        ORDER BY created_at ASC
        LIMIT $limit;
        """
        params = {'$parent_comment_id': parent_comment_id, '$limit': limit}
        
        try:
            result = await self.execute(query, params)
            return result if result else []
        except Exception as e:
            logger.error(f"❌ Error getting replies: {e}")
            return []
    
    async def get_replies_batch(self, parent_comment_ids: List[str], limit_per_parent: int = 5) -> Dict[str, List[Dict]]:
        """
        Получить последние ответы для нескольких родительских комментариев одним запросом
        Используется для превью
        """
        if not parent_comment_ids:
            return {}
        
        unique_ids = list(set(parent_comment_ids))
        
        # Создаем плейсхолдеры
        placeholders = []
        params = {}
        for i, pid in enumerate(unique_ids):
            placeholder = f"$pid_{i}"
            placeholders.append(placeholder)
            params[placeholder] = pid
        
        placeholders_str = ', '.join(placeholders)
        
        declare_parts = []
        for i in range(len(unique_ids)):
            declare_parts.append(f"DECLARE $pid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        # YDB поддерживает ROW_NUMBER и QUALIFY
        query = f"""
        {declare_block}
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY parent_comment_id ORDER BY created_at DESC) as rn
        FROM {self.table_name}
        WHERE parent_comment_id IN ({placeholders_str}) AND is_deleted = false
        QUALIFY rn <= {limit_per_parent}
        ORDER BY parent_comment_id, created_at DESC;
        """
        
        try:
            rows = await self.execute(query, params)
            
            result = {}
            for row in rows:
                parent_id = row['parent_comment_id']
                if parent_id not in result:
                    result[parent_id] = []
                result[parent_id].append(row)
            
            return result
        except Exception as e:
            logger.error(f"❌ Error getting replies batch: {e}")
            return {}
    
    async def increment_likes(self, comment_id: str, delta: int = 1) -> int:
        """Изменить счетчик лайков комментария"""
        query = f"""
        DECLARE $comment_id AS Utf8;
        DECLARE $delta AS Int64;
        
        UPDATE {self.table_name}
        SET likes_count = likes_count + CAST($delta AS Uint32)
        WHERE comment_id = $comment_id
        RETURNING likes_count;
        """
        params = {'$comment_id': comment_id, '$delta': delta}
        
        try:
            result = await self.execute(query, params)
            return result[0]['likes_count'] if result else 0
        except Exception as e:
            logger.error(f"❌ Error incrementing comment likes: {e}")
            return 0
    
    async def increment_replies(self, comment_id: str) -> int:
        """Увеличить счетчик ответов"""
        query = f"""
        DECLARE $comment_id AS Utf8;
        
        UPDATE {self.table_name}
        SET replies_count = replies_count + CAST(1 AS Uint32)
        WHERE comment_id = $comment_id
        RETURNING replies_count;
        """
        params = {'$comment_id': comment_id}
        
        try:
            result = await self.execute(query, params)
            return result[0]['replies_count'] if result else 0
        except Exception as e:
            logger.error(f"❌ Error incrementing replies: {e}")
            return 0
    
    async def get_comments_by_user(self, user_id: str, limit: int = 20, offset: int = 0) -> List[Dict]:
        """Получить комментарии пользователя с пагинацией"""
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE user_id = $user_id AND is_deleted = false
        ORDER BY created_at DESC
        LIMIT $limit OFFSET $offset;
        """
        params = {
            '$user_id': str(user_id),
            '$limit': limit,
            '$offset': offset
        }
        
        try:
            return await self.execute(query, params)
        except Exception as e:
            logger.error(f"❌ Error getting comments by user: {e}")
            return []
    
    async def get_comment_stats(self, post_id: str) -> Dict:
        """Получить статистику комментариев поста"""
        query = f"""
        DECLARE $post_id AS Utf8;
        
        SELECT 
            COUNT(*) as total_comments,
            COUNT(DISTINCT user_id) as unique_commenters,
            MAX(created_at) as last_comment_at,
            MIN(created_at) as first_comment_at
        FROM {self.table_name}
        WHERE post_id = $post_id AND is_deleted = false;
        """
        params = {'$post_id': post_id}
        
        try:
            result = await self.execute(query, params)
            if result:
                row = result[0]
                return {
                    'total_comments': row['total_comments'] or 0,
                    'unique_commenters': row['unique_commenters'] or 0,
                    'last_comment_at': from_timestamp(row['last_comment_at']).isoformat() + 'Z' if row['last_comment_at'] else None,
                    'first_comment_at': from_timestamp(row['first_comment_at']).isoformat() + 'Z' if row['first_comment_at'] else None
                }
            return {
                'total_comments': 0,
                'unique_commenters': 0,
                'last_comment_at': None,
                'first_comment_at': None
            }
        except Exception as e:
            logger.error(f"❌ Error getting comment stats: {e}")
            return {}
    
    async def search_comments(self, query_text: str, limit: int = 20, offset: int = 0) -> List[Dict]:
        """Поиск комментариев по тексту"""
        escaped = self._escape_like(query_text)
        
        sql_query = f"""
        DECLARE $query AS Utf8;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE content LIKE '%' || $query || '%' AND is_deleted = false
        ORDER BY created_at DESC
        LIMIT $limit OFFSET $offset;
        """
        params = {
            '$query': escaped,
            '$limit': limit,
            '$offset': offset
        }
        
        try:
            return await self.execute(sql_query, params)
        except Exception as e:
            logger.error(f"❌ Error searching comments: {e}")
            return []
    
    async def soft_delete(self, comment_id: str, user_id: str) -> bool:
        """Мягкое удаление комментария (только автор или админ)"""
        query = f"""
        DECLARE $comment_id AS Utf8;
        DECLARE $deleted_at AS Timestamp;
        
        UPDATE {self.table_name}
        SET is_deleted = true, deleted_at = $deleted_at
        WHERE comment_id = $comment_id
        RETURNING comment_id;
        """
        params = {
            '$comment_id': comment_id,
            '$deleted_at': to_timestamp(datetime.utcnow())
        }
        
        try:
            result = await self.execute(query, params)
            return len(result) > 0
        except Exception as e:
            logger.error(f"❌ Error soft deleting comment: {e}")
            return False
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        """Сгенерировать плейсхолдеры для IN запроса"""
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params

class FollowRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_follows"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_follows"
    
    # ============================================
    # CHECK
    # ============================================
    
    async def check(self, follower_id: str, following_id: str) -> bool:
        """Проверить, подписан ли пользователь"""
        query = f"""
        DECLARE $follower_id AS Utf8;
        DECLARE $following_id AS Utf8;
        
        SELECT COUNT(*) as cnt FROM {self.table_name}
        WHERE follower_id = $follower_id AND following_id = $following_id;
        """
        params = {'$follower_id': str(follower_id), '$following_id': str(following_id)}
        
        try:
            result = await self.execute(query, params)
            return result[0]['cnt'] > 0 if result else False
        except Exception as e:
            logger.error(f"Error checking follow: {e}")
            return False
    
    async def check_many_following(self, user_id: str, following_ids: List[str]) -> Dict[str, bool]:
        """
        ОПТИМИЗИРОВАНО: Проверить подписки на нескольких пользователей одним запросом
        """
        if not following_ids:
            return {}
        
        unique_ids = list(set(following_ids))
        
        # Создаем плейсхолдеры для IN запроса
        placeholders = []
        params = {'$user_id': str(user_id)}
        for i, fid in enumerate(unique_ids):
            placeholder = f"$fid_{i}"
            placeholders.append(placeholder)
            params[placeholder] = fid
        
        placeholders_str = ', '.join(placeholders)
        
        # Создаем DECLARE для каждого параметра
        declare_parts = ["DECLARE $user_id AS Utf8;"]
        for i in range(len(unique_ids)):
            declare_parts.append(f"DECLARE $fid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        query = f"""
        {declare_block}
        SELECT following_id FROM {self.table_name}
        WHERE follower_id = $user_id AND following_id IN ({placeholders_str});
        """
        
        try:
            result = await self.execute(query, params)
            following = {row['following_id']: True for row in result}
            return {fid: fid in following for fid in following_ids}
        except Exception as e:
            logger.error(f"Error checking many following: {e}")
            return {}
    
    # ============================================
    # FOLLOW/UNFOLLOW
    # ============================================
    
    async def follow(self, follower_id: str, following_id: str, created_at: datetime) -> Optional[str]:
        """Подписаться - ИСПРАВЛЕНО (принудительное преобразование в строку)"""
        follow_id = str(uuid.uuid4())
        
        # ПРИНУДИТЕЛЬНО преобразуем в строки и убираем возможные кавычки
        follower_id_str = str(follower_id).strip().strip("'").strip('"')
        following_id_str = str(following_id).strip().strip("'").strip('"')
        
        # Логируем для отладки
        logger.info(f"📝 Follow attempt: {follower_id_str} -> {following_id_str}")
        
        data = {
            'follow_id': follow_id,
            'follower_id': follower_id_str,
            'following_id': following_id_str,
            'created_at': to_timestamp(created_at)
        }
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ User {follower_id} followed {following_id}")
            return follow_id
        except Exception as e:
            logger.error(f"❌ Error following: {e}")
            return None
    
    async def unfollow(self, follower_id: str, following_id: str) -> bool:
        """Отписаться"""
        query = f"""
        DECLARE $follower_id AS Utf8;
        DECLARE $following_id AS Utf8;
        
        DELETE FROM {self.table_name}
        WHERE follower_id = $follower_id AND following_id = $following_id;
        """
        params = {'$follower_id': str(follower_id), '$following_id': str(following_id)}
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ User {follower_id} unfollowed {following_id}")
            return True
        except Exception as e:
            logger.error(f"Error unfollowing: {e}")
            return False
    
    # ============================================
    # GET FOLLOWERS/FOLLOWING WITH CURSOR
    # ============================================
    
    async def get_followers_with_cursor(self, user_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        ОПТИМИЗИРОВАНО: Получить подписчиков с курсорной пагинацией
        Возвращает (followers, next_cursor)
        """
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
                logger.info(f"📌 Followers cursor: created_at={last_created_at}, id={last_id}")
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT 
                f.follow_id,
                f.created_at as follow_created_at,
                u.id,
                u.username,
                u.first_name_encrypted,
                u.last_name_encrypted,
                u.display_name,
                u.is_verified
            FROM {self.table_name} f
            JOIN users u ON f.follower_id = u.id
            WHERE f.following_id = $user_id
              AND (f.created_at < $last_created_at OR 
                   (f.created_at = $last_created_at AND f.follow_id < $last_id))
            ORDER BY f.created_at DESC, f.follow_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$user_id': str(user_id),
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT 
                f.follow_id,
                f.created_at as follow_created_at,
                u.id,
                u.username,
                u.first_name_encrypted,
                u.last_name_encrypted,
                u.display_name,
                u.is_verified
            FROM {self.table_name} f
            JOIN users u ON f.follower_id = u.id
            WHERE f.following_id = $user_id
            ORDER BY f.created_at DESC, f.follow_id DESC
            LIMIT $limit + 1;
            """
            params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['follow_created_at']}:{last_item['follow_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            logger.info(f"📊 Got {len(items)} followers, has_more={has_more}")
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting followers with cursor: {e}")
            return [], None
    
    async def get_following_with_cursor(self, user_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        ОПТИМИЗИРОВАНО: Получить подписки с курсорной пагинацией
        Возвращает (following, next_cursor)
        """
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
                logger.info(f"📌 Following cursor: created_at={last_created_at}, id={last_id}")
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT 
                f.follow_id,
                f.created_at as follow_created_at,
                u.id,
                u.username,
                u.first_name_encrypted,
                u.last_name_encrypted,
                u.display_name,
                u.is_verified
            FROM {self.table_name} f
            JOIN users u ON f.following_id = u.id
            WHERE f.follower_id = $user_id
              AND (f.created_at < $last_created_at OR 
                   (f.created_at = $last_created_at AND f.follow_id < $last_id))
            ORDER BY f.created_at DESC, f.follow_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$user_id': str(user_id),
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT 
                f.follow_id,
                f.created_at as follow_created_at,
                u.id,
                u.username,
                u.first_name_encrypted,
                u.last_name_encrypted,
                u.display_name,
                u.is_verified
            FROM {self.table_name} f
            JOIN users u ON f.following_id = u.id
            WHERE f.follower_id = $user_id
            ORDER BY f.created_at DESC, f.follow_id DESC
            LIMIT $limit + 1;
            """
            params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['follow_created_at']}:{last_item['follow_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            logger.info(f"📊 Got {len(items)} following, has_more={has_more}")
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting following with cursor: {e}")
            return [], None
    
    # ============================================
    # GET IDS (WITH LIMIT)
    # ============================================
    
    async def get_follower_ids(self, user_id: str, limit: int = 1000) -> List[str]:
        """
        ОПТИМИЗИРОВАНО: Получить ID подписчиков с лимитом
        """
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        
        SELECT follower_id FROM {self.table_name}
        WHERE following_id = $user_id
        ORDER BY created_at DESC
        LIMIT $limit;
        """
        params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            result = await self.execute(query, params)
            return [row['follower_id'] for row in result]
        except Exception as e:
            logger.error(f"Error getting follower IDs: {e}")
            return []
    
    async def get_following_ids(self, user_id: str, limit: int = 1000) -> List[str]:
        """
        ОПТИМИЗИРОВАНО: Получить ID подписок с лимитом
        """
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        
        SELECT following_id FROM {self.table_name}
        WHERE follower_id = $user_id
        ORDER BY created_at DESC
        LIMIT $limit;
        """
        params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            result = await self.execute(query, params)
            return [row['following_id'] for row in result]
        except Exception as e:
            logger.error(f"Error getting following IDs: {e}")
            return []
    
    async def get_following_ids_with_cursor(self, user_id: str, limit: int = 100, cursor: Optional[str] = None) -> Tuple[List[str], Optional[str]]:
        """
        ОПТИМИЗИРОВАНО: Получить ID подписок с курсорной пагинацией
        Возвращает (ids, next_cursor)
        """
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT following_id, created_at, follow_id
            FROM {self.table_name}
            WHERE follower_id = $user_id
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND follow_id < $last_id))
            ORDER BY created_at DESC, follow_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$user_id': str(user_id),
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT following_id, created_at, follow_id
            FROM {self.table_name}
            WHERE follower_id = $user_id
            ORDER BY created_at DESC, follow_id DESC
            LIMIT $limit + 1;
            """
            params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['created_at']}:{last_item['follow_id']}".encode()
                ).decode()
                ids = [row['following_id'] for row in items]
            else:
                ids = [row['following_id'] for row in rows]
                next_cursor = None
            
            return ids, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting following IDs with cursor: {e}")
            return [], None
    
    # ============================================
    # STATS
    # ============================================
    
    async def get_counts(self, user_id: str) -> Dict[str, int]:
        """
        Получить количество подписчиков и подписок
        """
        logger.info(f"🔍 [get_counts] START: user_id={user_id}")
        
        user_id_str = str(user_id)
        
        # Проверяем, жива ли транзакция
        if self._transaction:
            try:
                # Пытаемся выполнить простой запрос
                await self._transaction.execute("SELECT 1;")
                logger.debug("✅ [get_counts] Transaction is alive")
            except Exception:
                # Если транзакция мертва, создаем новую
                logger.warning("⚠️ [get_counts] Transaction is dead, creating new one")
                self._transaction = None
        
        # Разделяем на два отдельных запроса
        followers_query = f"""
        DECLARE $user_id AS Utf8;
        SELECT COUNT(*) as count FROM feed_follows WHERE following_id = $user_id;
        """
        
        following_query = f"""
        DECLARE $user_id AS Utf8;
        SELECT COUNT(*) as count FROM feed_follows WHERE follower_id = $user_id;
        """
        
        params = {'$user_id': user_id_str}
        
        try:
            # Выполняем запросы
            followers_result = await self.execute(followers_query, params)
            following_result = await self.execute(following_query, params)
            
            # Обрабатываем результат (с учетом YDB формата)
            followers_count = 0
            if followers_result and len(followers_result) > 0:
                row = followers_result[0]
                if isinstance(row, dict):
                    followers_count = row.get('count', 0)
                elif hasattr(row, '_columns'):
                    # YDB формат
                    for col in row._columns:
                        if col.name == 'count':
                            # Здесь нужно получить значение
                            followers_count = 0  # временно
            
            following_count = 0
            if following_result and len(following_result) > 0:
                row = following_result[0]
                if isinstance(row, dict):
                    following_count = row.get('count', 0)
            
            result = {
                'followers_count': followers_count,
                'following_count': following_count
            }
            
            logger.info(f"✅ [get_counts] Result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"❌ [get_counts] Error: {e}", exc_info=True)
            return {'followers_count': 0, 'following_count': 0}
    
    # ============================================
    # BATCH FOLLOW
    # ============================================
    
    async def follow_many(self, follower_id: str, following_ids: List[str], created_at: datetime) -> int:
        """
        ОПТИМИЗИРОВАНО: Подписаться на нескольких пользователей одним запросом
        Возвращает количество успешных подписок
        """
        if not following_ids:
            return 0
        
        unique_ids = list(set(following_ids))
        now_ts = to_timestamp(created_at)
        
        # Создаем множественный UPSERT
        value_placeholders = []
        params = {}
        
        for i, following_id in enumerate(unique_ids):
            follow_id = str(uuid.uuid4())
            placeholder_suffix = f"_{i}"
            
            value_placeholders.append(f"($follow_id{placeholder_suffix}, $follower_id, $following_id{placeholder_suffix}, $created_at)")
            
            params[f'$follow_id{placeholder_suffix}'] = follow_id
            params[f'$following_id{placeholder_suffix}'] = str(following_id)
        
        params['$follower_id'] = str(follower_id)
        params['$created_at'] = now_ts
        
        # Создаем DECLARE для всех параметров
        declare_parts = [
            "DECLARE $follower_id AS Utf8;",
            "DECLARE $created_at AS Timestamp;"
        ]
        for i in range(len(unique_ids)):
            declare_parts.append(f"DECLARE $follow_id_{i} AS Utf8;")
            declare_parts.append(f"DECLARE $following_id_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        values_str = ",\n".join(value_placeholders)
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} (follow_id, follower_id, following_id, created_at)
        VALUES {values_str};
        """
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ User {follower_id} followed {len(unique_ids)} users in batch")
            return len(unique_ids)
        except Exception as e:
            logger.error(f"Error batch following: {e}")
            return 0
    
    async def unfollow_many(self, follower_id: str, following_ids: List[str]) -> int:
        """
        ОПТИМИЗИРОВАНО: Отписаться от нескольких пользователей одним запросом
        Возвращает количество успешных отписок
        """
        if not following_ids:
            return 0
        
        unique_ids = list(set(following_ids))
        
        # Создаем плейсхолдеры для IN запроса
        placeholders = []
        params = {'$follower_id': str(follower_id)}
        for i, fid in enumerate(unique_ids):
            placeholder = f"$fid_{i}"
            placeholders.append(placeholder)
            params[placeholder] = fid
        
        placeholders_str = ', '.join(placeholders)
        
        # Создаем DECLARE для всех параметров
        declare_parts = ["DECLARE $follower_id AS Utf8;"]
        for i in range(len(unique_ids)):
            declare_parts.append(f"DECLARE $fid_{i} AS Utf8;")
        declare_block = "\n".join(declare_parts)
        
        query = f"""
        {declare_block}
        DELETE FROM {self.table_name}
        WHERE follower_id = $follower_id AND following_id IN ({placeholders_str});
        """
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ User {follower_id} unfollowed {len(unique_ids)} users in batch")
            return len(unique_ids)
        except Exception as e:
            logger.error(f"Error batch unfollowing: {e}")
            return 0
    
    # ============================================
    # UTILS
    # ============================================
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        """Сгенерировать плейсхолдеры для IN запроса"""
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


class HashtagRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_hashtags и feed_post_hashtags"""
    
    def __init__(self, session):
        super().__init__(session)
        self.hashtags_table = "feed_hashtags"
        self.post_hashtags_table = "feed_post_hashtags"
    
    async def get_or_create(self, name: str, now: datetime) -> Optional[str]:
        """Получить или создать хэштег"""
        normalized = name.lower().strip()
        logger.info(f"🔍 Getting or creating hashtag: #{normalized}")
        
        try:
            # Сначала проверяем существование
            query = f"""
            DECLARE $normalized AS Utf8;
            
            SELECT hashtag_id, posts_count
            FROM {self.hashtags_table} 
            WHERE normalized_name = $normalized;
            """
            params = {'$normalized': normalized}
            
            result = await self.execute(query, params)
            
            if result:
                hashtag_id = result[0]['hashtag_id']
                current_count = result[0].get('posts_count', 0)
                logger.info(f"✅ Found existing hashtag #{name} with id {hashtag_id}, count={current_count}")
                
                # Обновляем счетчик и last_used_at
                update_query = f"""
                DECLARE $hashtag_id AS Utf8;
                DECLARE $last_used_at AS Timestamp;

                UPDATE {self.hashtags_table}
                SET posts_count = posts_count + CAST(1 AS Uint32),
                    last_used_at = $last_used_at
                WHERE hashtag_id = $hashtag_id;
                """
                update_params = {
                    '$hashtag_id': hashtag_id,
                    '$last_used_at': to_timestamp(now)
                }
                
                await self.execute(update_query, update_params)
                logger.info(f"✅ Updated posts_count for hashtag #{name}")
                return hashtag_id
            
            # Создаем новый хештег
            hashtag_id = str(uuid.uuid4()).replace('-', '')
            logger.info(f"🆕 Creating new hashtag #{name} with id {hashtag_id}")
            
            insert_query = f"""
            DECLARE $hashtag_id AS Utf8;
            DECLARE $name AS Utf8;
            DECLARE $normalized_name AS Utf8;
            DECLARE $created_at AS Timestamp;
            DECLARE $last_used_at AS Timestamp;
            DECLARE $posts_count AS Uint32;

            INSERT INTO {self.hashtags_table}
            (hashtag_id, name, normalized_name, posts_count, created_at, last_used_at)
            VALUES ($hashtag_id, $name, $normalized_name, $posts_count, $created_at, $last_used_at);
            """

            insert_params = {
                '$hashtag_id': hashtag_id,
                '$name': name,
                '$normalized_name': normalized,
                '$posts_count': 1,
                '$created_at': to_timestamp(now),
                '$last_used_at': to_timestamp(now)
            }
            
            await self.execute(insert_query, insert_params)
            logger.info(f"✅ Created new hashtag #{name}")
            return hashtag_id
            
        except Exception as e:
            logger.error(f"❌ Error in get_or_create hashtag #{name}: {e}")
            return None
    
    async def link_to_post(self, post_id: str, hashtag_id: str, created_at: datetime) -> bool:
        """Связать пост с хэштегом"""
        logger.info(f"🔗 Linking hashtag {hashtag_id} to post {post_id}")
        
        query = f"""
        DECLARE $post_id AS Utf8;
        DECLARE $hashtag_id AS Utf8;
        DECLARE $created_at AS Timestamp;
        
        INSERT INTO {self.post_hashtags_table} (post_id, hashtag_id, created_at)
        VALUES ($post_id, $hashtag_id, $created_at);
        """
        
        params = {
            '$post_id': post_id,
            '$hashtag_id': hashtag_id,
            '$created_at': to_timestamp(created_at)
        }
        
        try:
            await self.execute(query, params)
            logger.info(f"✅ Linked hashtag {hashtag_id} to post {post_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Error linking hashtag to post: {e}")
            return False
    
    async def get_by_post(self, post_id: str) -> List[Dict]:
        """Получить хэштеги поста"""
        query = f"""
        DECLARE $post_id AS Utf8;
        
        SELECT 
            h.hashtag_id as id,
            h.name,
            h.posts_count
        FROM {self.post_hashtags_table} ph
        JOIN {self.hashtags_table} h ON ph.hashtag_id = h.hashtag_id
        WHERE ph.post_id = $post_id
        ORDER BY ph.created_at DESC;
        """
        params = {'$post_id': post_id}
        
        try:
            result = await self.execute(query, params)
            return result if result else []
        except Exception as e:
            logger.error(f"Error getting hashtags by post: {e}")
            return []
    
    async def get_by_posts(self, post_ids: List[str]) -> Dict[str, List[Dict]]:
        """
        Получить хэштеги для нескольких постов одним запросом - ИСПРАВЛЕНО
        """
        if not post_ids:
            return {}
        
        unique_ids = list(set(post_ids))
        
        placeholders = []
        params = {}
        for i, pid in enumerate(unique_ids):
            placeholder = f"$pid_{i}"
            placeholders.append(placeholder)
            params[placeholder] = pid
        
        placeholders_str = ', '.join(placeholders)
        
        declare_lines = [f"DECLARE $pid_{i} AS Utf8;" for i in range(len(unique_ids))]
        declare_block = "\n".join(declare_lines)
        
        query = f"""
        {declare_block}
        SELECT 
            ph.post_id,
            h.hashtag_id as id,
            h.name,
            h.posts_count
        FROM {self.post_hashtags_table} ph
        JOIN {self.hashtags_table} h ON ph.hashtag_id = h.hashtag_id
        WHERE ph.post_id IN ({placeholders_str})
        ORDER BY ph.created_at DESC;
        """
        
        try:
            rows = await self.execute(query, params)
            
            hashtags_by_post = {}
            for row in rows:
                # Проверяем наличие post_id в разных форматах
                post_id = row.get('post_id') or row.get('ph.post_id')
                if not post_id:
                    logger.error(f"❌ No post_id in row: {row}")
                    continue
                    
                if post_id not in hashtags_by_post:
                    hashtags_by_post[post_id] = []
                
                hashtags_by_post[post_id].append({
                    'id': row.get('id') or row.get('h.hashtag_id'),
                    'name': row.get('name') or row.get('h.name'),
                    'posts_count': row.get('posts_count') or row.get('h.posts_count', 0)
                })
            
            # Возвращаем результат для всех запрошенных ID
            result = {}
            for pid in unique_ids:
                result[pid] = hashtags_by_post.get(pid, [])
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error getting hashtags by posts: {e}")
            return {pid: [] for pid in unique_ids}
    
    async def get_trending(self, limit: int) -> List[Dict]:
        """Получить популярные хэштеги"""
        query = f"""
        SELECT 
            hashtag_id as id, 
            name, 
            posts_count, 
            last_used_at
        FROM {self.hashtags_table}
        WHERE posts_count > 0
        ORDER BY posts_count DESC, last_used_at DESC
        LIMIT {limit};
        """
        
        try:
            return await self.execute(query)
        except Exception as e:
            logger.error(f"Error getting trending hashtags: {e}")
            return []
    
    async def get_id_by_name(self, name: str) -> Optional[str]:
        """
        Получить ID хештега по имени - ИСПРАВЛЕНО (без SQL-инъекции)
        """
        normalized = name.lower().strip('#')
        
        # ✅ ИСПРАВЛЕНИЕ: используем параметризованный запрос
        query = f"""
        DECLARE $normalized AS Utf8;
        
        SELECT hashtag_id 
        FROM {self.hashtags_table}
        WHERE normalized_name = $normalized;
        """
        
        params = {'$normalized': normalized}
        
        try:
            result = await self.execute(query, params)
            return result[0]['hashtag_id'] if result else None
        except Exception as e:
            logger.error(f"Error getting hashtag ID: {e}")
            return None
    
    async def get_posts_by_hashtag_id_with_cursor(self, hashtag_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        Получить посты с хештегом по ID с курсорной пагинацией - ИСПРАВЛЕНО (без SQL-инъекции)
        """
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        # Сначала получим ID постов по хештегу
        post_ids_query = f"""
        DECLARE $hashtag_id AS Utf8;
        
        SELECT post_id
        FROM {self.post_hashtags_table}
        WHERE hashtag_id = $hashtag_id
        ORDER BY created_at DESC;
        """
        
        try:
            # Получаем ID постов
            post_ids_rows = await self.execute(post_ids_query, {'$hashtag_id': hashtag_id})
            post_ids = [row['post_id'] for row in post_ids_rows]
            
            if not post_ids:
                return [], None
            
            # ✅ ИСПРАВЛЕНИЕ: используем DECLARE и параметры вместо прямой подстановки
            # Создаем плейсхолдеры для каждого post_id
            placeholders = []
            params = {}
            for i, pid in enumerate(post_ids[:50]):  # Берем первые 50, остальные отсекаем
                placeholder = f"$pid_{i}"
                placeholders.append(placeholder)
                params[placeholder] = pid
            
            placeholders_str = ', '.join(placeholders)
            
            # Создаем DECLARE для каждого параметра
            declare_parts = []
            for i in range(len(post_ids[:50])):
                declare_parts.append(f"DECLARE $pid_{i} AS Utf8;")
            declare_block = "\n".join(declare_parts)
            
            # Затем получаем сами посты с параметризованным запросом
            if last_created_at and last_id:
                query = f"""
                {declare_block}
                DECLARE $limit AS Uint64;
                DECLARE $last_created_at AS Timestamp;
                DECLARE $last_id AS Utf8;
                
                SELECT *
                FROM feed_posts
                WHERE post_id IN ({placeholders_str})
                  AND is_deleted = false 
                  AND visibility = 'public'
                  AND (created_at < $last_created_at OR 
                       (created_at = $last_created_at AND post_id < $last_id))
                ORDER BY created_at DESC, post_id DESC
                LIMIT $limit;
                """
                params.update({
                    '$limit': limit,
                    '$last_created_at': last_created_at,
                    '$last_id': last_id
                })
            else:
                query = f"""
                {declare_block}
                DECLARE $limit AS Uint64;
                
                SELECT *
                FROM feed_posts
                WHERE post_id IN ({placeholders_str})
                  AND is_deleted = false 
                  AND visibility = 'public'
                ORDER BY created_at DESC, post_id DESC
                LIMIT $limit;
                """
                params['$limit'] = limit
            
            rows = await self.execute(query, params)
            
            # Проверяем, есть ли еще посты
            has_more = len(rows) == limit
            if has_more and rows:
                last_post = rows[-1]
                next_cursor = base64.b64encode(
                    f"{last_post['created_at']}:{last_post['post_id']}".encode()
                ).decode()
            else:
                next_cursor = None
            
            return rows, next_cursor
            
        except Exception as e:
            logger.error(f"❌ Error getting posts by hashtag ID with cursor: {e}")
            return [], None
    
    async def search_hashtags_by_prefix(self, prefix: str, limit: int = 10) -> List[Dict]:
        """Поиск хештегов по префиксу"""
        if len(prefix) < 2:
            return []
        
        query = f"""
        DECLARE $prefix AS Utf8;
        DECLARE $limit AS Uint64;
        
        SELECT 
            hashtag_id as id, 
            name, 
            posts_count, 
            last_used_at
        FROM {self.hashtags_table}
        WHERE normalized_name LIKE $prefix || '%'
        ORDER BY posts_count DESC, last_used_at DESC
        LIMIT $limit;
        """
        params = {'$prefix': prefix.lower(), '$limit': limit}
        
        try:
            return await self.execute(query, params)
        except Exception as e:
            logger.error(f"Error searching hashtags: {e}")
            return []
    
    async def get_related_hashtags(self, hashtag: str, limit: int = 10) -> List[Dict]:
        """Получить связанные хештеги"""
        hashtag_id = await self.get_id_by_name(hashtag)
        if not hashtag_id:
            return []
        
        query = f"""
        SELECT 
            h.hashtag_id as id, 
            h.name as name,
            h.posts_count as posts_count,
            COUNT(*) as co_occurrence
        FROM {self.post_hashtags_table} ph1
        JOIN {self.post_hashtags_table} ph2 ON ph1.post_id = ph2.post_id
        JOIN {self.hashtags_table} h ON ph2.hashtag_id = h.hashtag_id
        WHERE ph1.hashtag_id = '{hashtag_id}' 
          AND ph2.hashtag_id != '{hashtag_id}'
        GROUP BY h.hashtag_id, h.name, h.posts_count
        ORDER BY co_occurrence DESC
        LIMIT {limit};
        """
        
        try:
            return await self.execute(query)
        except Exception as e:
            logger.error(f"Error getting related hashtags: {e}")
            return []
    
    def _generate_placeholders(self, values: List[str], prefix: str) -> Tuple[str, Dict]:
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


class MentionRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_mentions"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_mentions"
    
    async def create_many(self, mentions_data: List[Dict]) -> List[str]:
        """Создать несколько упоминаний одним батч-запросом"""
        if not mentions_data:
            return []
        
        all_mention_ids = []
        for i in range(0, len(mentions_data), feed_config.BATCH_SIZE_MENTIONS):
            chunk = mentions_data[i:i+feed_config.BATCH_SIZE_MENTIONS]
            try:
                chunk_ids = await self._create_many_chunk(chunk)
                if chunk_ids:
                    all_mention_ids.extend(chunk_ids)
            except Exception as e:
                logger.error(f"Error creating mentions chunk: {e}")
        
        return all_mention_ids
    
    async def _create_many_chunk(self, mentions_data: List[Dict]) -> List[str]:
        """Создать чанк упоминаний"""
        now = datetime.utcnow()
        now_ts = to_timestamp(now)
        mention_ids = []
        all_params = {}
        value_placeholders = []
        
        for i, mention_data in enumerate(mentions_data):
            mention_id = str(uuid.uuid4())
            mention_ids.append(mention_id)
            
            position_start = mention_data.get('position_start', 0)
            position_end = mention_data.get('position_end', 0)
            post_id = mention_data.get('post_id', '')
            comment_id = mention_data.get('comment_id', '')
            
            data = {
                'mention_id': mention_id,
                'post_id': post_id,
                'comment_id': comment_id,
                'mentioned_user_id': str(mention_data['mentioned_user_id']),
                'mentioned_by_user_id': str(mention_data['mentioned_by_user_id']),
                'username': mention_data['username'],
                'position_start': position_start,
                'position_end': position_end,
                'created_at': now_ts,
                'is_read': False,
                'read_at': 0,
                'entity_type': mention_data.get('entity_type', ''),
                'entity_id': mention_data.get('entity_id', '')
            }
            
            row_placeholders = []
            for key, value in data.items():
                param_name = f"${key}_{i}"
                row_placeholders.append(param_name)
                all_params[param_name] = value
            
            value_placeholders.append(f"({', '.join(row_placeholders)})")
        
        if not mention_ids:
            return []
        
        declare_block = self._generate_declare(all_params)
        
        columns = ", ".join(['mention_id', 'post_id', 'comment_id', 'mentioned_user_id', 
                            'mentioned_by_user_id', 'username', 'position_start', 
                            'position_end', 'created_at', 'is_read', 'read_at',
                            'entity_type', 'entity_id'])
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns})
        VALUES {', '.join(value_placeholders)};
        """
        
        try:
            await self.execute(query, all_params)
            return mention_ids
        except Exception as e:
            logger.error(f"Error creating mentions in batch: {e}")
            return []
    
    async def get_by_id(self, mention_id: str) -> Optional[Dict]:
        """Получить упоминание по ID"""
        query = f"""
        DECLARE $mention_id AS Utf8;
        SELECT * FROM {self.table_name} WHERE mention_id = $mention_id;
        """
        params = {'$mention_id': mention_id}
        
        try:
            result = await self.execute(query, params)
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting mention: {e}")
            return None
    
    async def get_unread_by_user_with_cursor(self, user_id: str, limit: int = 50, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить непрочитанные упоминания пользователя с курсором"""
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE mentioned_user_id = $user_id AND is_read = false
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND mention_id < $last_id))
            ORDER BY created_at DESC, mention_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$user_id': str(user_id),
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE mentioned_user_id = $user_id AND is_read = false
            ORDER BY created_at DESC, mention_id DESC
            LIMIT $limit + 1;
            """
            params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['created_at']}:{last_item['mention_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting unread mentions with cursor: {e}")
            return [], None
    
    async def get_by_user_with_cursor(self, user_id: str, limit: int = 50, offset: int = 0, 
                          include_read: bool = True, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить все упоминания пользователя с курсором"""
        condition = "mentioned_user_id = $user_id"
        if not include_read:
            condition += " AND is_read = false"
        
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT * FROM {self.table_name}
            WHERE {condition}
              AND (created_at < $last_created_at OR 
                   (created_at = $last_created_at AND mention_id < $last_id))
            ORDER BY created_at DESC, mention_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$user_id': str(user_id),
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $user_id AS Utf8;
            DECLARE $limit AS Uint64;
            
            SELECT * FROM {self.table_name}
            WHERE {condition}
            ORDER BY created_at DESC, mention_id DESC
            LIMIT $limit + 1;
            """
            params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['created_at']}:{last_item['mention_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting user mentions with cursor: {e}")
            return [], None
    
    async def mark_as_read(self, mention_id: str, user_id: str) -> bool:
        """Отметить упоминание как прочитанное"""
        now = datetime.utcnow()
        
        query = f"""
        DECLARE $mention_id AS Utf8;
        DECLARE $user_id AS Utf8;
        DECLARE $read_at AS Timestamp;
        
        UPDATE {self.table_name}
        SET is_read = true, read_at = $read_at
        WHERE mention_id = $mention_id AND mentioned_user_id = $user_id;
        """
        params = {
            '$mention_id': mention_id,
            '$user_id': str(user_id),
            '$read_at': to_timestamp(now)
        }
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error marking mention as read: {e}")
            return False
    
    async def mark_all_as_read(self, user_id: str) -> int:
        """Отметить все упоминания как прочитанные"""
        now = datetime.utcnow()
        
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $read_at AS Timestamp;
        
        UPDATE {self.table_name}
        SET is_read = true, read_at = $read_at
        WHERE mentioned_user_id = $user_id AND is_read = false;
        """
        params = {'$user_id': str(user_id), '$read_at': to_timestamp(now)}
        
        try:
            await self.execute(query, params)
            return 0
        except Exception as e:
            logger.error(f"Error marking all mentions as read: {e}")
            return 0
    
    async def count_unread(self, user_id: str) -> int:
        """Подсчитать количество непрочитанных упоминаний"""
        query = f"""
        SELECT COUNT(*) as cnt FROM {self.table_name}
        WHERE mentioned_user_id = '{user_id}' AND is_read = false;
        """
        
        try:
            result = await self.execute(query)
            return result[0]['cnt'] if result else 0
        except Exception as e:
            logger.error(f"Error counting unread mentions: {e}")
            return 0
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


class NotificationRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_notifications - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_notifications"
    
    async def get_by_user_with_cursor(self, user_id: str, limit: int, cursor: Optional[str] = None,
                                       unread_only: bool = False) -> Tuple[List[Dict], Optional[str]]:
        """
        Получить уведомления пользователя с курсорной пагинацией
        Использует индекс idx_notifications_user_created
        """
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                parts = decoded.split(':')
                last_created_at = int(parts[0])
                last_id = parts[1]
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        # Выбираем индекс в зависимости от условий
        if unread_only:
            # Используем индекс idx_notifications_user_unread
            if last_created_at and last_id:
                query = f"""
                DECLARE $user_id AS Utf8;
                DECLARE $limit AS Uint64;
                DECLARE $last_created_at AS Timestamp;
                DECLARE $last_id AS Utf8;
                
                SELECT *
                FROM {self.table_name}
                WHERE user_id = $user_id 
                  AND is_read = false
                  AND (created_at < $last_created_at OR 
                       (created_at = $last_created_at AND notification_id < $last_id))
                ORDER BY created_at DESC, notification_id DESC
                LIMIT $limit + 1;
                """
                params = {
                    '$user_id': str(user_id),
                    '$limit': limit,
                    '$last_created_at': last_created_at,
                    '$last_id': last_id
                }
            else:
                query = f"""
                DECLARE $user_id AS Utf8;
                DECLARE $limit AS Uint64;
                
                SELECT *
                FROM {self.table_name}
                WHERE user_id = $user_id AND is_read = false
                ORDER BY created_at DESC, notification_id DESC
                LIMIT $limit + 1;
                """
                params = {'$user_id': str(user_id), '$limit': limit}
        else:
            # Используем индекс idx_notifications_user_created
            if last_created_at and last_id:
                query = f"""
                DECLARE $user_id AS Utf8;
                DECLARE $limit AS Uint64;
                DECLARE $last_created_at AS Timestamp;
                DECLARE $last_id AS Utf8;
                
                SELECT *
                FROM {self.table_name}
                WHERE user_id = $user_id
                  AND (created_at < $last_created_at OR 
                       (created_at = $last_created_at AND notification_id < $last_id))
                ORDER BY created_at DESC, notification_id DESC
                LIMIT $limit + 1;
                """
                params = {
                    '$user_id': str(user_id),
                    '$limit': limit,
                    '$last_created_at': last_created_at,
                    '$last_id': last_id
                }
            else:
                query = f"""
                DECLARE $user_id AS Utf8;
                DECLARE $limit AS Uint64;
                
                SELECT *
                FROM {self.table_name}
                WHERE user_id = $user_id
                ORDER BY created_at DESC, notification_id DESC
                LIMIT $limit + 1;
                """
                params = {'$user_id': str(user_id), '$limit': limit}
        
        try:
            rows = await self.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                items = rows[:-1]
                last_item = rows[-2]
                next_cursor = base64.b64encode(
                    f"{last_item['created_at']}:{last_item['notification_id']}".encode()
                ).decode()
            else:
                items = rows
                next_cursor = None
            
            return items, next_cursor
            
        except Exception as e:
            logger.error(f"Error getting notifications: {e}")
            return [], None
    
    async def count_unread(self, user_id: str) -> int:
        """Получить количество непрочитанных уведомлений (с кэшированием)"""
        cache_key = f"notifications:unread_count:{user_id}"
        
        cached = await cache.get(cache_key)
        if cached is not None:
            return cached
        
        query = f"""
        DECLARE $user_id AS Utf8;
        SELECT COUNT(*) as cnt FROM {self.table_name}
        WHERE user_id = $user_id AND is_read = false;
        """
        params = {'$user_id': str(user_id)}
        
        try:
            result = await self.execute(query, params)
            count = result[0]['cnt'] if result else 0
            await cache.set(cache_key, count, ttl=10)
            return count
        except Exception as e:
            logger.error(f"Error counting unread: {e}")
            return 0
    
    async def mark_as_read(self, notification_id: str, user_id: str) -> bool:
        """Отметить одно уведомление как прочитанное"""
        query = f"""
        DECLARE $notification_id AS Utf8;
        DECLARE $user_id AS Utf8;
        
        UPDATE {self.table_name}
        SET is_read = true
        WHERE notification_id = $notification_id AND user_id = $user_id;
        """
        params = {
            '$notification_id': notification_id,
            '$user_id': str(user_id)
        }
        
        try:
            await self.execute(query, params)
            # Инвалидируем кэш
            await cache.delete(f"notifications:unread_count:{user_id}")
            await cache.delete_pattern(f"notifications:{user_id}:*")
            return True
        except Exception as e:
            logger.error(f"Error marking as read: {e}")
            return False
    
    async def mark_all_read(self, user_id: str) -> bool:
        """Отметить все уведомления как прочитанные"""
        query = f"""
        DECLARE $user_id AS Utf8;
        
        UPDATE {self.table_name}
        SET is_read = true
        WHERE user_id = $user_id AND is_read = false;
        """
        params = {'$user_id': str(user_id)}
        
        try:
            await self.execute(query, params)
            # Инвалидируем кэш
            await cache.delete(f"notifications:unread_count:{user_id}")
            await cache.delete_pattern(f"notifications:{user_id}:*")
            return True
        except Exception as e:
            logger.error(f"Error marking all read: {e}")
            return False
class ReportRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_reports"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_reports"
    
    async def create(self, reporter_id: str, entity_type: str, entity_id: str,
                     reason: str, description: Optional[str] = None) -> Optional[str]:
        """Создать жалобу"""
        report_id = str(uuid.uuid4())
        now = datetime.utcnow()
        
        data = {
            'report_id': report_id,
            'reporter_id': str(reporter_id),
            'entity_type': entity_type,
            'entity_id': entity_id,
            'reason': reason,
            'description': description,
            'status': 'pending',
            'created_at': to_timestamp(now)
        }
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            return report_id
        except Exception as e:
            logger.error(f"Error creating report: {e}")
            return None
    
    async def get_by_id(self, report_id: str) -> Optional[Dict]:
        """Получить жалобу по ID"""
        query = f"""
        DECLARE $report_id AS Utf8;
        SELECT * FROM {self.table_name} WHERE report_id = $report_id;
        """
        params = {'$report_id': report_id}
        
        try:
            result = await self.execute(query, params)
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting report: {e}")
            return None
    
    async def get_pending(self, limit: int, offset: int) -> List[Dict]:
        """Получить ожидающие жалобы"""
        query = f"""
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT * FROM {self.table_name}
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT $limit OFFSET $offset;
        """
        params = {'$limit': limit, '$offset': offset}
        
        try:
            return await self.execute(query, params)
        except Exception as e:
            logger.error(f"Error getting pending reports: {e}")
            return []
    
    async def update_status(self, report_id: str, status: str, reviewed_by: str) -> bool:
        """Обновить статус жалобы"""
        now = datetime.utcnow()
        
        query = f"""
        DECLARE $report_id AS Utf8;
        DECLARE $status AS Utf8;
        DECLARE $reviewed_by AS Utf8;
        DECLARE $reviewed_at AS Timestamp;
        
        UPDATE {self.table_name}
        SET status = $status, reviewed_by = $reviewed_by, reviewed_at = $reviewed_at
        WHERE report_id = $report_id;
        """
        params = {
            '$report_id': report_id,
            '$status': status,
            '$reviewed_by': str(reviewed_by),
            '$reviewed_at': to_timestamp(now)
        }
        
        try:
            await self.execute(query, params)
            return True
        except Exception as e:
            logger.error(f"Error updating report status: {e}")
            return False
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


class ViewRepository(TransactionAwareRepository):
    """Репозиторий для таблицы feed_post_views"""
    
    def __init__(self, session):
        super().__init__(session)
        self.table_name = "feed_post_views"
    
    async def track(self, post_id: str, user_id: Optional[str],
                    ip_address: str, user_agent: str, view_duration_ms: int = 0) -> Optional[str]:
        """Записать просмотр поста"""
        view_id = str(uuid.uuid4())
        now = datetime.utcnow()
        
        data = {
            'view_id': view_id,
            'post_id': post_id,
            'user_id': str(user_id) if user_id else None,
            'ip_address': ip_address,
            'user_agent': user_agent,
            'view_duration_ms': view_duration_ms,
            'created_at': to_timestamp(now)
        }
        
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f"${k}" for k in data.keys()])
        declare_block = self._generate_declare({f"${k}": v for k, v in data.items()})
        
        query = f"""
        {declare_block}
        UPSERT INTO {self.table_name} ({columns}) VALUES ({placeholders});
        """
        
        params = {f"${k}": v for k, v in data.items()}
        
        try:
            await self.execute(query, params)
            return view_id
        except Exception as e:
            logger.error(f"Error tracking view: {e}")
            return None
    
    def _generate_placeholders(self, values: List[Any], prefix: str = "p") -> Tuple[str, Dict]:
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


# ============================================
# ИСПРАВЛЕННЫЙ КЛАСС RecommendationRepository
# ============================================

class RecommendationRepository(BaseRepository):
    """Репозиторий для сложных запросов рекомендаций - ТОЛЬКО ЧТЕНИЕ"""
    
    def __init__(self, session):
        super().__init__(session)
        self.posts_table = "feed_posts"
        self.likes_table = "feed_likes"
        self.follows_table = "feed_follows"
        self.hashtags_table = "feed_hashtags"
        self.post_hashtags_table = "feed_post_hashtags"
    
    async def get_following_ids(self, user_id: str, limit: int = 100) -> List[str]:
        """Получить ID пользователей, на которых подписан user_id"""
        query = f"""
        SELECT following_id
        FROM {self.follows_table}
        WHERE follower_id = '{user_id}'
        ORDER BY created_at DESC
        LIMIT {limit};
        """
        
        try:
            result = await self.execute(query)
            if not result:
                return []
            return [row['following_id'] for row in result]
        except Exception as e:
            logger.error(f"Error getting following IDs: {e}")
            return []
    
    async def get_posts_from_following(self, user_id: str, limit: int) -> List[Dict]:
        """Получить посты от пользователей, на которых подписан user_id"""
        following_ids = await self.get_following_ids(user_id, 50)
        if not following_ids:
            return []
        
        all_posts = []
        for i in range(0, len(following_ids), 20):
            chunk = following_ids[i:i+20]
            chunk_posts = await self._get_posts_from_following_chunk(chunk, limit)
            if chunk_posts:
                all_posts.extend(chunk_posts)
        
        all_posts.sort(key=lambda x: x.get('created_at', 0), reverse=True)
        return all_posts[:limit]
    
    async def _get_posts_from_following_chunk(self, following_ids: List[str], limit: int) -> List[Dict]:
        """Получить посты для чанка подписок"""
        if not following_ids:
            return []
            
        placeholders = ', '.join([f"'{uid}'" for uid in following_ids])
        
        query = f"""
        SELECT p.*
        FROM {self.posts_table} p
        WHERE p.user_id IN ({placeholders})
          AND p.is_deleted = false
          AND p.visibility = 'public'
        ORDER BY p.created_at DESC
        LIMIT {limit};
        """
        
        try:
            result = await self.execute(query)
            return result if result else []
        except Exception as e:
            logger.error(f"Error getting posts from following: {e}")
            return []
    
    async def get_user_liked_hashtags(self, user_id: str, limit: int = 10) -> List[str]:
        """Получить ID хештегов, которые пользователь чаще всего лайкает"""
        query = f"""
        SELECT ph.hashtag_id, COUNT(*) as like_count
        FROM feed_likes l
        JOIN feed_posts p ON l.post_id = p.post_id
        JOIN feed_post_hashtags ph ON p.post_id = ph.post_id
        WHERE l.user_id = '{user_id}'
        GROUP BY ph.hashtag_id
        ORDER BY like_count DESC
        LIMIT {limit};
        """
        
        try:
            result = await self.execute(query)
            if not result:
                return []
            return [str(row['hashtag_id']) for row in result if row.get('hashtag_id')]
        except Exception as e:
            logger.error(f"Error getting user liked hashtags: {e}")
            return []
    
    async def get_posts_by_hashtags(self, hashtag_ids: List[str], user_id: str, limit: int) -> List[Dict]:
        """Получить посты с указанными хештегами"""
        if not hashtag_ids:
            return []
        
        all_posts = []
        for i in range(0, len(hashtag_ids), 10):
            chunk = hashtag_ids[i:i+10]
            chunk_posts = await self._get_posts_by_hashtags_chunk(chunk, user_id, limit)
            if chunk_posts:
                all_posts.extend(chunk_posts)
        
        seen = set()
        unique_posts = []
        for post in all_posts:
            if post['post_id'] not in seen:
                seen.add(post['post_id'])
                unique_posts.append(post)
        
        return unique_posts[:limit]
    
    async def _get_posts_by_hashtags_chunk(self, hashtag_ids: List[str], user_id: str, limit: int) -> List[Dict]:
        """Получить посты для чанка хештегов"""
        if not hashtag_ids:
            return []
            
        placeholders = ', '.join([f"'{hid}'" for hid in hashtag_ids])
        
        query = f"""
        SELECT DISTINCT p.*
        FROM {self.posts_table} p
        JOIN {self.post_hashtags_table} ph ON p.post_id = ph.post_id
        WHERE ph.hashtag_id IN ({placeholders})
          AND p.user_id != '{user_id}'
          AND p.is_deleted = false
          AND p.visibility = 'public'
        ORDER BY p.created_at DESC
        LIMIT {limit};
        """
        
        try:
            result = await self.execute(query)
            return result if result else []
        except Exception as e:
            logger.error(f"Error getting posts by hashtags: {e}")
            return []
    
    async def get_trending_posts(self, days: int = 7, limit: int = 20, exclude_user_id: str = None) -> List[Dict]:
        """Получить популярные посты за последние N дней"""
        week_ago = to_timestamp(datetime.utcnow() - timedelta(days=days))
        
        exclude_condition = ""
        if exclude_user_id:
            exclude_condition = f"AND p.user_id != '{exclude_user_id}'"
        
        query = f"""
        SELECT p.*
        FROM {self.posts_table} p
        WHERE p.created_at >= {week_ago} 
          AND p.is_deleted = false 
          AND p.visibility = 'public'
          {exclude_condition}
        ORDER BY (p.likes_count + p.comments_count * 2 + p.reposts_count * 3 + p.views_count * 0.1) DESC
        LIMIT {limit};
        """
        
        try:
            result = await self.execute(query)
            return result if result else []
        except Exception as e:
            logger.error(f"Error getting trending posts: {e}")
            return []


# ============================================
# СЕРВИСЫ - ВСЕ ПОЛУЧАЮТ session ПАРАМЕТРОМ
# ============================================

class ImageService:
    """Сервис для обработки и оптимизации изображений с ThreadPool"""
    
    ALLOWED_EXTENSIONS = {'jpg', 'jpeg', 'png', 'gif', 'webp'}
    ALLOWED_MIME_TYPES = {'image/jpeg', 'image/png', 'image/gif', 'image/webp'}
    
    SIZES = {
        'thumbnail': (150, 150),
        'small': (400, 400),
        'medium': (800, 800),
        'large': (1200, 1200),
    }
    
    QUALITY = {
        'thumbnail': 70,
        'small': 80,
        'medium': 85,
        'large': 90,
    }
    
    CACHE_TTL = {
        'thumbnail': 3600 * 24 * 7,
        'small': 3600 * 24 * 7,
        'medium': 3600 * 24 * 3,
        'large': 3600 * 24 * 1,
    }
    
    _upload_semaphore = asyncio.Semaphore(feed_config.MAX_CONCURRENT_IMAGE_UPLOADS)
    
    @classmethod
    async def process_base64_image(cls, base64_string: str) -> Tuple[Optional[bytes], Optional[str]]:
        """Обработать base64 изображение с проверкой размера до декодирования"""
        try:
            if len(base64_string) > feed_config.MAX_IMAGE_SIZE_MB * 1024 * 1024 * 1.37:
                return None, f"Image too large. Max {feed_config.MAX_IMAGE_SIZE_MB}MB"
            
            if ',' in base64_string:
                header, base64_data = base64_string.split(',', 1)
                if 'image/' not in header:
                    return None, "Invalid image header"
            else:
                base64_data = base64_string
            
            image_data = base64.b64decode(base64_data)
            is_valid, error = cls.validate_image(image_data)
            if not is_valid:
                return None, error
            
            return image_data, None
            
        except Exception as e:
            return None, str(e)
    
    @classmethod
    def validate_image(cls, image_data: bytes) -> Tuple[bool, Optional[str]]:
        """Проверить валидность изображения (синхронно)"""
        try:
            if len(image_data) > feed_config.MAX_IMAGE_SIZE_MB * 1024 * 1024:
                return False, f"Image too large. Max {feed_config.MAX_IMAGE_SIZE_MB}MB"
            
            img = Image.open(io.BytesIO(image_data))
            img.verify()
            
            return True, None
            
        except Exception as e:
            return False, str(e)
    
    @classmethod
    async def optimize_and_upload(cls, image_data: bytes, post_id: str, 
                                   user_id: str, index: int) -> Optional[str]:
        """Оптимизировать изображение в ThreadPool и загрузить в Storage"""
        
        async with cls._upload_semaphore:
            try:
                loop = asyncio.get_event_loop()
                
                urls = {}
                for size_name, dimensions in cls.SIZES.items():
                    optimized_data, content_type = await loop.run_in_executor(
                        image_executor,
                        cls._optimize_image_sync,
                        image_data,
                        dimensions,
                        cls.QUALITY[size_name]
                    )
                    
                    filename = f"post_{post_id}_{index}_{size_name}.webp"
                    
                    upload_result = storage.upload_file(
                        file_data=optimized_data,
                        filename=filename,
                        folder=f'posts/{size_name}',
                        user_id=user_id,
                        content_type='image/webp',
                        metadata={
                            'post_id': post_id,
                            'index': str(index),
                            'size': size_name
                        },
                        make_public=True
                    )
                    
                    urls[size_name] = upload_result['url']
                    
                    cache_key = f"image:optimized:{hashlib.md5(image_data).hexdigest()}:{size_name}"
                    await cache.set(cache_key, {
                        'url': upload_result['url'],
                        'size': size_name
                    }, ttl=cls.CACHE_TTL[size_name])
                
                return urls.get('medium', urls.get('small'))
                
            except Exception as e:
                logger.error(f"Error optimizing/uploading image: {e}")
                return None
    
    @classmethod
    def _optimize_image_sync(cls, image_data: bytes, dimensions: Tuple[int, int], 
                              quality: int) -> Tuple[bytes, str]:
        """Синхронная оптимизация изображения (для ThreadPool)"""
        img = Image.open(io.BytesIO(image_data))
        
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')
        
        if dimensions:
            img.thumbnail(dimensions, Image.Resampling.LANCZOS)
        
        output = io.BytesIO()
        img.save(output, format='WEBP', quality=quality, optimize=True)
        
        return output.getvalue(), 'image/webp'


class PostService:
    """Сервис для работы с постами - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.user_cache = UserCache()
    async def _notify_followers_about_new_post(self, user_id: str, post_id: str):
        """Фоновая задача для уведомления подписчиков о новом посте"""
        logger.info(f"📨 Starting to notify followers of user {user_id} about new post {post_id}")
        
        try:
            async with RequestContext() as ctx:
                follows_repo = FollowRepository(ctx.session)
                notifications_repo = NotificationRepository(ctx.session)
                
                follower_ids = await follows_repo.get_follower_ids(user_id, limit=1000)
                
                if not follower_ids:
                    logger.info(f"📨 No followers to notify for user {user_id}")
                    return
                
                logger.info(f"📨 Found {len(follower_ids)} followers to notify")
                
                now = datetime.utcnow()
                for follower_id in follower_ids:
                    if str(follower_id) == str(user_id):
                        continue
                    
                    # Отправляем WebSocket уведомление каждому подписчику
                    await send_ws(str(follower_id), {
                        'type': 'notification',
                        'data': {
                            'notification_type': 'post',
                            'from_user_id': user_id,
                            'post_id': post_id,
                            'created_at': now.isoformat(),
                            'new_post': True
                        }
                    })
                    
                    try:
                        await notifications_repo.create(
                            user_id=follower_id,
                            from_user_id=user_id,
                            notification_type='post',
                            entity_type='post',
                            entity_id=post_id,
                            created_at=now,
                            extra_data={'new_post': True}
                        )
                    except Exception as e:
                        logger.error(f"❌ Failed to create notification: {e}")
                        
        except Exception as e:
            logger.error(f"❌ Failed to notify followers: {e}", exc_info=True)
    async def create_from_channel(
        self,
        session,
        user_id: str,
        user_data: Dict,
        channel_id: int,
        channel_message_id: int,
        content: str,
        title: Optional[str] = None,
        images: Optional[List[str]] = None,
        metadata: Optional[Dict] = None,
        author_override: Optional['Author'] = None
    ) -> Tuple[Post, ChannelPost]:
        """
        Создать пост в ленте из сообщения в канале
        Использует переданную сессию
        """
        logger.info(f"📝 Creating post from channel {channel_id}, message {channel_message_id}")

        if not await self._check_rate_limit('create_post', user_id,
                                            feed_config.POST_RATE_LIMIT,
                                            feed_config.RATE_LIMIT_PERIOD):
            raise RateLimitError("Rate limit exceeded. Too many posts.")

        PostValidator.validate_create(content, title, images or [])

        post_id = str(uuid.uuid4())
        now = datetime.utcnow()
        content_preview = content[:200] + ("..." if len(content) > 200 else "")

        hashtags = self._extract_hashtags(content)
        logger.info(f"🔍 Extracted hashtags: {hashtags}")

        author = author_override if author_override is not None else Author.from_token(user_data)

        uploaded_images = []
        if images:
            logger.info(f"🖼️ Processing {len(images)} images")
            
            upload_tasks = []
            for idx, img_data in enumerate(images):
                task = self._process_and_upload_image(img_data, post_id, user_id, idx)
                upload_tasks.append(task)
            
            results = await asyncio.gather(*upload_tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"❌ Image upload failed: {result}")
                elif result:
                    uploaded_images.append(result)
        
        posts_repo = PostRepository(session)
        channel_posts_repo = ChannelPostRepository(session)
        hashtags_repo = HashtagRepository(session)
        
        success = await posts_repo.create(
            post_id, user_id, title, content, content_preview,
            uploaded_images, 'public', 'ru', now,
            original_author=f"channel:{channel_id}",
        )

        if not success:
            raise DatabaseError("Failed to create post")

        channel_username = (metadata or {}).get('channel_username')

        for tag in hashtags:
            try:
                hashtag_id = await hashtags_repo.get_or_create(tag, now)
                if hashtag_id:
                    await hashtags_repo.link_to_post(post_id, hashtag_id, now)
                    logger.info(f"✅ Created hashtag #{tag} for post {post_id}")
            except Exception as e:
                logger.error(f"⚠️ Error creating hashtag #{tag}: {e}")
        
        try:
            mention_service = MentionService()
            await mention_service.extract_and_create_mentions(
                session, content, 'post', post_id, user_id
            )
        except Exception as e:
            logger.error(f"⚠️ Error creating mentions: {e}")
        
        channel_post = ChannelPost(
            post_id=post_id,
            channel_id=channel_id,
            channel_message_id=channel_message_id,
            created_by=user_id,
            created_at=now,
            published_at=now,
            is_published=True,
            metadata=metadata or {}
        )
        
        await channel_posts_repo.create(channel_post)
        logger.info(f"✅ Created channel link for post {post_id}")
        
        await cache.delete_pattern(f"feed:*")
        
        post = Post(
            id=post_id,
            user_id=user_id,
            title=title,
            content=content,
            content_preview=content_preview,
            media_urls=uploaded_images,
            hashtags=[{"name": tag} for tag in hashtags],
            likes_count=0,
            comments_count=0,
            reposts_count=0,
            views_count=0,
            bookmarks_count=0,
            is_repost=False,
            is_pinned=False,
            is_edited=False,
            visibility='public',
            language='ru',
            created_at=now.isoformat() + "Z",
            updated_at=now.isoformat() + "Z",
            original_author=f"channel:{channel_id}",
            source_channel_id=str(channel_id),
            author=author,
            interactions=Interactions(
                is_liked=False,
                is_bookmarked=False,
                is_reposted=False,
                is_owner=True
            )
        )

        logger.info(f"✅ Post {post_id} created from channel {channel_id} message {channel_message_id}")
        
        return post, channel_post
    # ===== 🔥 НОВЫЙ МЕТОД 1: Получение сообщения из канала напрямую из БД =====
    async def get_channel_message_from_db(self, session, channel_id: int, message_id: int) -> Optional[Dict]:
        """
        Прочитать сообщение из канала напрямую из БД
        Не использует message_handler, только SQL
        """
        logger.info(f"📖 Reading channel message {message_id} from channel {channel_id} from DB")
        
        query = """
        DECLARE $channel_id AS Uint64;
        DECLARE $message_id AS Uint64;
        
        SELECT 
            m.message_id,
            m.chat_id,
            m.sender_id,
            m.content,
            m.message_type,
            m.attachments_json,
            m.has_attachments,
            m.created_at,
            m.sender_role_at_time
        FROM `messages` m
        WHERE m.chat_id = $channel_id 
          AND m.message_id = $message_id
          AND m.is_deleted = false;
        """
        
        params = {
            '$channel_id': channel_id,
            '$message_id': message_id
        }
        
        try:
            # Выполняем запрос напрямую через сессию
            result = session.transaction().execute(
                session.prepare(query),
                params,
                commit_tx=True
            )

            if result and result[0].rows:
                row = result[0].rows[0]
                logger.info(f"✅ Found message {message_id} in channel {channel_id}")
                
                # Парсим JSON поля
                attachments = []
                if row.get('attachments_json'):
                    try:
                        attachments = json.loads(row['attachments_json'])
                    except:
                        attachments = []
                
                return {
                    'message_id': row['message_id'],
                    'chat_id': row['chat_id'],
                    'sender_id': row['sender_id'],
                    'content': row.get('content', ''),
                    'message_type': row.get('message_type', 'text'),
                    'attachments': attachments,
                    'has_attachments': row.get('has_attachments', False),
                    'created_at': row.get('created_at'),
                    'sender_role': row.get('sender_role_at_time')
                }
            else:
                logger.warning(f"❌ Message {message_id} not found in channel {channel_id}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error reading channel message: {e}", exc_info=True)
            return None
    
    # ===== 🔥 НОВЫЙ МЕТОД 2: Проверка прав администратора канала =====
    async def check_channel_admin(self, session, channel_id: int, user_id: str) -> bool:
        """
        Проверить, является ли пользователь администратором канала
        """
        logger.info(f"🔐 Checking if user {user_id} is admin of channel {channel_id}")
        
        query = """
        DECLARE $channel_id AS Uint64;
        DECLARE $user_id AS Utf8;
        
        SELECT role, is_active
        FROM `chat_participants`
        WHERE chat_id = $channel_id 
          AND user_id = $user_id
          AND is_active = true;
        """
        
        params = {
            '$channel_id': channel_id,
            '$user_id': user_id
        }
        
        try:
            result = session.transaction().execute(
                session.prepare(query),
                params,
                commit_tx=True
            )

            if result and result[0].rows:
                role = result[0].rows[0].get('role')
                is_admin = role in ['owner', 'admin']
                logger.info(f"✅ User {user_id} is admin: {is_admin} (role: {role})")
                return is_admin
            else:
                logger.warning(f"❌ User {user_id} not found in channel {channel_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error checking channel admin: {e}", exc_info=True)
            return False
    async def create_repost_from_channel(
        self,
        session,
        user_id: str,
        user_data: Dict,
        channel_id: int,
        channel_message_id: int,
        channel_username: Optional[str] = None,
        comment: Optional[str] = None
    ) -> Tuple[Post, ChannelPost]:
        """
        Создать репост сообщения из канала в ленту
        Использует переданную сессию
        """
        logger.info(f"🔄 Creating repost from channel {channel_id}, message {channel_message_id}")
        
        if not await self._check_rate_limit('create_repost', user_id, 
                                            feed_config.REPOST_RATE_LIMIT, 
                                            feed_config.RATE_LIMIT_PERIOD):
            raise RateLimitError("Rate limit exceeded. Too many reposts.")
        
        post_id = str(uuid.uuid4())
        now = datetime.utcnow()
        
        if comment:
            content = f"{comment}\n\n📢 Из канала @{channel_username or channel_id}"
        else:
            content = f"📢 Из канала @{channel_username or channel_id}"
        
        content_preview = content[:200] + ("..." if len(content) > 200 else "")
        
        author = Author.from_token(user_data)
        
        channel_posts_repo = ChannelPostRepository(session)
        posts_repo = PostRepository(session)
        
        existing = await channel_posts_repo.get_by_channel_message(channel_id, channel_message_id)
        if existing:
            existing_post = await posts_repo.get_by_id(existing.post_id)
            if existing_post:
                logger.info(f"✅ Found existing repost {existing.post_id} for channel message {channel_message_id}")
                post = await self.get(session, existing.post_id, user_id, user_data)
                return post, existing
        
        success = await posts_repo.create(
            post_id, user_id, None, content, content_preview,
            [], 'public', 'ru', now,
            original_author=f"channel:{channel_id}",
        )

        if not success:
            raise DatabaseError("Failed to create repost")

        try:
            await posts_repo.update_post(post_id, {
                'is_repost': True,
                'original_post': str(channel_message_id)
            })
        except:
            pass
        
        channel_post = ChannelPost(
            post_id=post_id,
            channel_id=channel_id,
            channel_message_id=channel_message_id,
            created_by=user_id,
            created_at=now,
            published_at=now,
            is_published=True,
            metadata={
                'channel_username': channel_username,
                'comment': comment
            }
        )
        
        await channel_posts_repo.create(channel_post)
        logger.info(f"✅ Created channel link for repost {post_id}")
        
        await cache.delete_pattern(f"feed:*")
        
        post = Post(
            id=post_id,
            user_id=user_id,
            title=None,
            content=content,
            content_preview=content_preview,
            media_urls=[],
            hashtags=[],
            likes_count=0,
            comments_count=0,
            reposts_count=0,
            views_count=0,
            bookmarks_count=0,
            is_repost=True,
            original_post_id=None,
            repost_comment=comment,
            is_pinned=False,
            is_edited=False,
            visibility='public',
            language='ru',
            created_at=now.isoformat() + "Z",
            updated_at=now.isoformat() + "Z",
            author=author,
            interactions=Interactions(
                is_liked=False,
                is_bookmarked=False,
                is_reposted=False,
                is_owner=True
            )
        )
        
        logger.info(f"✅ Repost {post_id} created from channel {channel_id} message {channel_message_id}")
        
        return post, channel_post
    
    async def _process_and_upload_image(self, img_data: str, post_id: str, 
                                          user_id: str, idx: int) -> Optional[str]:
        """Обработать и загрузить одно изображение"""
        try:
            image_bytes, error = await ImageService.process_base64_image(img_data)
            if error:
                logger.error(f"❌ Image {idx} validation failed: {error}")
                return None
            
            url = await ImageService.optimize_and_upload(
                image_bytes, post_id, user_id, idx
            )
            
            if url:
                logger.info(f"✅ Image {idx} uploaded: {url}")
                return url
            else:
                logger.error(f"❌ Image {idx} upload failed")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to process image {idx}: {e}")
            return None
    
    async def _fetch_post_interactions_batch(self, session, post_ids: List[str], user_id: str) -> Tuple[Dict, Dict, Dict, Dict]:
        """
        Получить все взаимодействия пользователя с постами - ИСПРАВЛЕНО
        Возвращает (bookmarks_map, hashtags_map, reactions_counts_map, user_reactions_map)
        """
        if not post_ids:
            return {}, {}, {}, {}
        
        logger.info(f"⚡ Fetching interactions for {len(post_ids)} posts")
        
        bookmarks_map = {pid: False for pid in post_ids}
        user_reactions_map = {pid: None for pid in post_ids}
        reactions_counts_map = {pid: [] for pid in post_ids}
        
        # Получаем все счетчики реакций для всех постов
        try:
            reactions_repo = ReactionRepository(session)
            all_reactions = await reactions_repo.get_reactions_counts_for_entities('post', post_ids)
            
            for post_id, reactions in all_reactions.items():
                if reactions:
                    reactions_counts_map[post_id] = reactions
                    logger.info(f"📊 Post {post_id} has reactions: {reactions}")
                else:
                    reactions_counts_map[post_id] = []
                    
        except Exception as e:
            logger.error(f"❌ Reactions query failed: {e}")
        
        # Получаем реакции текущего пользователя
        try:
            user_reactions = await reactions_repo.get_user_reactions_for_entities(user_id, 'post', post_ids)
            for post_id, reaction in user_reactions.items():
                user_reactions_map[post_id] = reaction
                logger.info(f"👤 User {user_id} reaction on {post_id}: {reaction}")
        except Exception as e:
            logger.error(f"❌ User reactions query failed: {e}")
        
        # Получаем закладки
        try:
            bookmarks_repo = BookmarkRepository(session)
            bookmarks_result = await bookmarks_repo.check_many(user_id, post_ids)
            bookmarks_map.update(bookmarks_result)
        except Exception as e:
            logger.error(f"❌ Bookmarks query failed: {e}")
        
        # Получаем хэштеги
        hashtags_map = {}
        try:
            hashtags_repo = HashtagRepository(session)
            hashtags_map = await hashtags_repo.get_by_posts(post_ids)
        except Exception as e:
            logger.error(f"❌ Hashtags query failed: {e}")
            hashtags_map = {pid: [] for pid in post_ids}
        
        logger.info(f"✅ Final reactions_counts_map: {reactions_counts_map}")
        logger.info(f"✅ Final user_reactions_map: {user_reactions_map}")
        
        return bookmarks_map, hashtags_map, reactions_counts_map, user_reactions_map
    
    async def _notify_followers_background(self, user_id: str, post_id: str):
        """Фоновая задача для уведомления подписчиков"""
        try:
            async with RequestContext() as ctx:
                follows_repo = FollowRepository(ctx.session)
                notifications_repo = NotificationRepository(ctx.session)
                
                follower_ids = await follows_repo.get_follower_ids(user_id, limit=1000)
                
                if not follower_ids:
                    return
                
                notifications = []
                for follower_id in follower_ids:
                    notifications.append({
                        'user_id': follower_id,
                        'from_user_id': user_id,
                        'type': 'post',
                        'entity_type': 'post',
                        'entity_id': post_id
                    })
                    
                    if len(notifications) >= 100:
                        await notifications_repo.create_many(notifications)
                        notifications = []
                
                if notifications:
                    await notifications_repo.create_many(notifications)
                    
                logger.info(f"✅ Notified {len(follower_ids)} followers about post {post_id}")
        except Exception as e:
            logger.error(f"❌ Failed to notify followers: {e}")
    
    async def _check_rate_limit(self, action: str, user_id: str, max_requests: int, period: int) -> bool:
        """Проверить rate limit (заглушка, реальный rate limiter в хендлере)"""
        return True
    async def delete(self, session, comment_id: str, user_id: str) -> bool:
        """Удалить комментарий с обновлением цепочки replies_count"""
        logger.info(f"🗑️ Deleting comment {comment_id} by user {user_id}")
        
        comments_repo = CommentRepository(session)
        posts_repo = PostRepository(session)
        
        # Получаем комментарий
        comment = await comments_repo.get_by_id(comment_id)
        if not comment:
            raise NotFoundError(f"Comment {comment_id} not found")
        
        # Проверяем права
        if str(comment['user_id']) != str(user_id):
            raise PermissionError("You can only delete your own comments")
        
        post_id = comment['post_id']
        parent_comment_id = comment.get('parent_comment_id')
        
        # 🔥 НОВОЕ: считаем все вложенные комментарии
        total_replies = await comments_repo.count_all_replies(comment_id)
        total = total_replies + 1  # включая сам комментарий
        logger.info(f"📊 Comment {comment_id} has {total_replies} replies, total to remove: {total}")
        
        async with await UnitOfWork.from_session(session) as uow:
            comments_repo.set_transaction(uow._transaction)
            posts_repo.set_transaction(uow._transaction)
            
            # Удаляем комментарий (soft delete)
            success = await comments_repo.soft_delete(comment_id, user_id)
            if not success:
                raise DatabaseError("Failed to delete comment")
            
            # 🔥 НОВОЕ: уменьшаем replies_count у ВСЕХ родителей
            if parent_comment_id:
                await comments_repo.decrement_replies_chain(parent_comment_id, total)
            
            # Уменьшаем счётчик комментариев у поста
            await posts_repo.increment_comments(post_id, -total)
        
        # Инвалидируем кэш
        await CommentCache.invalidate(post_id)
        await PostCache.invalidate(post_id)
        
        logger.info(f"✅ Comment {comment_id} deleted, removed {total} comments from counts")
        return True
    async def create(self, session, user_id: str, user_data: Dict, content: str, 
                     title: Optional[str] = None, visibility: str = 'public', 
                     images: List[str] = None) -> Post:
        """Создать новый пост - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"📝 Creating post for user {user_id}")
        
        if not await self._check_rate_limit('create_post', user_id, 
                                            feed_config.POST_RATE_LIMIT, 
                                            feed_config.RATE_LIMIT_PERIOD):
            raise RateLimitError("Rate limit exceeded. Too many posts.")
        
        PostValidator.validate_create(content, title, images or [])
        
        post_id = str(uuid.uuid4())
        now = datetime.utcnow()
        content_preview = content[:200] + ("..." if len(content) > 200 else "")
        
        hashtags = self._extract_hashtags(content)
        logger.info(f"🔍 Extracted hashtags: {hashtags}")
        
        author = Author.from_token(user_data)
        
        uploaded_images = []
        if images:
            logger.info(f"🖼️ Processing {len(images)} images")
            
            upload_tasks = []
            for idx, img_data in enumerate(images):
                task = self._process_and_upload_image(img_data, post_id, user_id, idx)
                upload_tasks.append(task)
            
            results = await asyncio.gather(*upload_tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"❌ Image upload failed: {result}")
                elif result:
                    uploaded_images.append(result)
        
        posts_repo = PostRepository(session)
        hashtags_repo = HashtagRepository(session)
        
        success = await posts_repo.create(
            post_id, user_id, title, content, content_preview,
            uploaded_images, visibility, 'ru', now
        )
        
        if not success:
            raise DatabaseError("Failed to create post")
        
        for tag in hashtags:
            try:
                hashtag_id = await hashtags_repo.get_or_create(tag, now)
                if hashtag_id:
                    await hashtags_repo.link_to_post(post_id, hashtag_id, now)
                    logger.info(f"✅ Created hashtag #{tag} for post {post_id}")
            except Exception as e:
                logger.error(f"⚠️ Error creating hashtag #{tag}: {e}")
        
        try:
            mention_service = MentionService()
            await mention_service.extract_and_create_mentions(
                session, content, 'post', post_id, user_id
            )
        except Exception as e:
            logger.error(f"⚠️ Error creating mentions: {e}")
        
        await cache.delete_pattern(f"feed:*")
        
        # 👇 Отправляем WebSocket уведомление подписчикам о новом посте
        await send_ws('broadcast', {
            'type': 'new_post',
            'data': {
                'post_id': post_id,
                'user_id': user_id,
                'author': {
                    'id': author.id,
                    'username': author.username,
                    'display_name': author.display_name
                },
                'title': title,
                'content_preview': content_preview,
                'hashtags': hashtags,
                'media_count': len(uploaded_images),
                'created_at': now.isoformat()
            }
        })
        
        # 👇 Фоновая задача для уведомления подписчиков
        await background_worker.add_low(
            self._notify_followers_about_new_post,
            user_id=user_id,
            post_id=post_id
        )
        
        logger.info(f"✅ Post created: {post_id} with {len(uploaded_images)} images")
        
        return Post(
            id=post_id,
            user_id=user_id,
            title=title,
            content=content,
            content_preview=content_preview,
            media_urls=uploaded_images,
            hashtags=[{"name": tag} for tag in hashtags],
            likes_count=0,
            comments_count=0,
            reposts_count=0,
            views_count=0,
            bookmarks_count=0,
            is_repost=False,
            is_pinned=False,
            is_edited=False,
            visibility=visibility,
            language='ru',
            created_at=now.isoformat() + "Z",
            updated_at=now.isoformat() + "Z",
            author=author,
            interactions=Interactions(
                is_liked=False,
                is_bookmarked=False,
                is_reposted=False,
                is_owner=True
            )
        )
    

    async def get(self, session, post_id: str, user_id: str, user_data: Dict) -> Post:
        """Получить пост по ID - ОПТИМИЗИРОВАНО с логированием и правильным reactions_count"""
        logger.info(f"🔍 Getting post {post_id} for user {user_id}")
        
        # Проверяем кэш
        cached = await PostCache.get(post_id, user_id)
        if cached:
            logger.info(f"📦 Returning cached post {post_id} for user {user_id}")
            return Post(**cached)
        
        posts_repo = PostRepository(session)
        follows_repo = FollowRepository(session)
        users_repo = UserRepository(session)
        
        # ЗАПРОС 1: Получаем пост
        post_data = await posts_repo.get_by_id(post_id)
        if not post_data:
            raise NotFoundError(f"Post {post_id} not found")
        
        logger.info(f"📄 Post data retrieved: {post_data.get('post_id')}, reactions_count={post_data.get('reactions_count')}")
        
        # Проверяем доступ
        if post_data['visibility'] != 'public' and str(post_data['user_id']) != str(user_id):
            if post_data['visibility'] == 'followers':
                is_following = await follows_repo.check(user_id, str(post_data['user_id']))
                if not is_following:
                    raise PermissionError("You don't have access to this post")
            elif post_data['visibility'] == 'private':
                raise PermissionError("This post is private")
        
        # Увеличиваем просмотры в фоне (не ждем)
        asyncio.create_task(self._increment_views_background(post_id))
        
        # 👇 ПОДГОТАВЛИВАЕМ ВСЕ ЗАПРОСЫ ДЛЯ ПАРАЛЛЕЛЬНОГО ВЫПОЛНЕНИЯ
        tasks = []
        
        # Запрос автора
        tasks.append(users_repo.get(str(post_data['user_id']), 
                                    user_data if str(post_data['user_id']) == user_id else None))
        
        # Запрос хэштегов
        tasks.append(self._get_hashtags_with_cache(session, post_id))
        
        # ЗАПРОС 2: Проверка закладки
        tasks.append(self._check_bookmark(session, post_id, user_id))
        
        # ЗАПРОС 3: Проверка репоста
        repost_service = RepostService()
        tasks.append(repost_service.check_reposted(session, user_id, post_id))
        
        # ЗАПРОС 4: Получаем реакции пользователя
        reaction_service = ReactionService()
        tasks.append(reaction_service.get_reaction_counts(session, 'post', post_id, user_id))
        
        # ЗАПРОС 5: Получаем превью реакций
        tasks.append(reaction_service.get_reactions_preview(session, 'post', post_id, 3, user_id))
        
        logger.info(f"🚀 Executing {len(tasks)} parallel tasks for post {post_id}")
        
        # 👇 ВЫПОЛНЯЕМ ВСЕ ЗАПРОСЫ ПАРАЛЛЕЛЬНО
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        author_data = results[0] if not isinstance(results[0], Exception) else None
        hashtags = results[1] if not isinstance(results[1], Exception) else []
        is_bookmarked = results[2] if not isinstance(results[2], Exception) else False
        is_reposted = results[3] if not isinstance(results[3], Exception) else False
        reactions_raw = results[4] if not isinstance(results[4], Exception) else []
        reactions_preview_raw = results[5] if not isinstance(results[5], Exception) else []
        
        logger.info(f"📊 Results for post {post_id}:")
        logger.info(f"   - author_data: {'OK' if author_data else 'None'}")
        logger.info(f"   - hashtags: {len(hashtags)} items")
        logger.info(f"   - is_bookmarked: {is_bookmarked}")
        logger.info(f"   - is_reposted: {is_reposted}")
        logger.info(f"   - reactions_raw: {reactions_raw}")
        logger.info(f"   - reactions_preview_raw: {reactions_preview_raw}")
        
        # Создаем автора — для постов из канала используем данные канала
        _orig_author_raw = post_data.get('original_author') or ''
        if isinstance(_orig_author_raw, bytes):
            _orig_author_raw = _orig_author_raw.decode('utf-8')
        _orig_author_raw = str(_orig_author_raw)
        _source_channel_id = _orig_author_raw.split(':')[1] if _orig_author_raw.startswith('channel:') else None

        if _source_channel_id:
            _ch_title = None
            _ch_username = None
            try:
                _ch_query = f"""
                SELECT CAST(id AS Utf8) AS id, title, username
                FROM `chats`
                WHERE id = CAST({_source_channel_id} AS Uint64) AND is_deleted = false;
                """
                _ch_result = session.transaction().execute(
                    session.prepare(_ch_query),
                    {},
                    commit_tx=True
                )
                if _ch_result and _ch_result[0].rows:
                    _ch_row = _ch_result[0].rows[0]
                    _ch_title = _ch_row.get('title') or None
                    if isinstance(_ch_title, bytes):
                        _ch_title = _ch_title.decode('utf-8')
                    _ch_username = _ch_row.get('username') or None
                    if isinstance(_ch_username, bytes):
                        _ch_username = _ch_username.decode('utf-8')
            except Exception as _e:
                logger.warning(f"⚠️ Could not load channel info for post {post_id}: {_e}")
            author = Author(
                id=_source_channel_id,
                username=_ch_username or f"channel_{_source_channel_id[-8:]}",
                display_name=_ch_title or _ch_username or f"Channel {_source_channel_id[-8:]}",
                avatar_url=None,
                is_verified=False,
            )
        else:
            author = Author.from_db(author_data) if author_data else Author.from_token(user_data)
        
        # Обрабатываем media_urls
        media_urls = []
        if post_data.get('media_urls'):
            try:
                if isinstance(post_data['media_urls'], str):
                    media_urls = json.loads(post_data['media_urls'])
                elif isinstance(post_data['media_urls'], list):
                    media_urls = post_data['media_urls']
            except:
                media_urls = []
        
        # ========== ФОРМИРУЕМ РЕАКЦИИ В ВИДЕ ОБЪЕКТОВ REACTIONCOUNT ==========
        reaction_list = []
        total_reactions_count = 0
        
        if reactions_raw:
            for row in reactions_raw:
                if isinstance(row, ReactionCount):
                    reaction_list.append(row)
                    total_reactions_count += row.count
                elif isinstance(row, dict):
                    # Преобразуем словарь в ReactionCount
                    rt = row.get('type') or row.get('reaction_type')
                    if rt:
                        count = row.get('count', 0)
                        total_reactions_count += count
                        reaction_list.append(ReactionCount(
                            type=rt,
                            count=count,
                            emoji=row.get('emoji', ReactionType.get_emoji(rt)),
                            display_name=row.get('display_name', ReactionType.get_display_name(rt)),
                            user_reacted=row.get('user_reacted', False)
                        ))
                    else:
                        logger.warning(f"⚠️ No reaction type in row: {row}")
                else:
                    logger.warning(f"⚠️ Unknown reaction type in reactions_raw: {type(row)}")
        
        logger.info(f"✅ Formatted {len(reaction_list)} reactions for post {post_id}, total_count={total_reactions_count}")
        
        # ========== ФОРМИРУЕМ ПРЕВЬЮ В ВИДЕ СЛОВАРЕЙ ==========
        preview_list = []
        if reactions_preview_raw:
            if isinstance(reactions_preview_raw, list):
                for item in reactions_preview_raw:
                    if isinstance(item, dict):
                        preview_list.append(item)
                    elif hasattr(item, 'dict'):
                        preview_list.append(item.dict())
                    else:
                        logger.warning(f"⚠️ Unknown preview item type: {type(item)}")
            else:
                logger.warning(f"⚠️ reactions_preview_raw is not a list: {type(reactions_preview_raw)}")
        
        logger.info(f"✅ Formatted {len(preview_list)} preview items for post {post_id}")
        
        # Обрабатываем даты
        created_at = from_timestamp(post_data['created_at'])
        updated_at = from_timestamp(post_data.get('updated_at')) if post_data.get('updated_at') else None
        
        # Создаем пост
        post = Post(
            id=post_data['post_id'],
            user_id=post_data['user_id'],
            title=post_data.get('title', '') or '',
            content=post_data['content'],
            content_preview=post_data.get('content_preview', '') or 
                          (post_data['content'][:200] + ("..." if len(post_data['content']) > 200 else "")),
            media_urls=media_urls,
            hashtags=[{"id": h.get('id'), "name": h.get('name')} for h in hashtags],
            comments_count=post_data.get('comments_count') or 0,
            reposts_count=post_data.get('reposts_count') or 0,
            views_count=post_data.get('views_count') or 0,
            bookmarks_count=post_data.get('bookmarks_count') or 0,
            reactions_count=total_reactions_count,  # ✅ Используем вычисленную сумму
            reactions=reaction_list,
            reactions_preview=preview_list,
            is_repost=post_data.get('is_repost', False),
            original_post_id=post_data.get('original_post_id'),
            repost_comment=post_data.get('repost_comment'),
            is_pinned=post_data.get('is_pinned', False),
            is_edited=post_data.get('is_edited', False),
            visibility=post_data.get('visibility', 'public'),
            language=post_data.get('language', 'ru'),
            sentiment_score=post_data.get('sentiment_score'),
            reading_time_minutes=post_data.get('reading_time_minutes'),
            created_at=created_at.isoformat() + "Z",
            updated_at=updated_at.isoformat() + "Z" if updated_at else None,
            published_at=from_timestamp(post_data.get('published_at')).isoformat() + "Z" if post_data.get('published_at') else None,
            scheduled_for=from_timestamp(post_data.get('scheduled_for')).isoformat() + "Z" if post_data.get('scheduled_for') else None,
            is_deleted=post_data.get('is_deleted', False),
            deleted_at=from_timestamp(post_data.get('deleted_at')).isoformat() + "Z" if post_data.get('deleted_at') else None,
            original_author=post_data.get('original_author'),
            original_post=post_data.get('original_post'),
            source_channel_id=_source_channel_id,
            author=author,
            interactions=Interactions(
                is_liked=False,
                is_bookmarked=is_bookmarked,
                is_reposted=is_reposted,
                is_owner=str(post_data['user_id']) == str(user_id)
            )
        )
        
        logger.info(f"✅ Post {post_id} created: reactions={len(post.reactions)}, preview={len(post.reactions_preview)}, total_count={post.reactions_count}")
        
        # Сохраняем в кэш на 5 секунд
        await PostCache.set(post_id, user_id, post.dict())
        
        # Обновляем локально views_count (уже сделано в фоне)
        post_data['views_count'] = post_data.get('views_count', 0) + 1
        
        return post
    
    
    async def _increment_views_background(self, post_id: str):
        """Фоновая задача для увеличения просмотров"""
        try:
            async with RequestContext() as ctx:
                posts_repo = PostRepository(ctx.session)
                await posts_repo.increment_views(post_id)
                logger.info(f"👁️ Background views incremented for post {post_id}")
        except Exception as e:
            logger.error(f"❌ Failed to increment views in background: {e}")
    
    async def _get_hashtags_with_cache(self, session, post_id: str) -> List[Dict]:
        """Получить хэштеги с кэшированием"""
        cache_key = f"hashtags:post:{post_id}"
        cached = await cache.get(cache_key)
        if cached:
            return cached
        
        hashtags_repo = HashtagRepository(session)
        hashtags = await hashtags_repo.get_by_post(post_id)
        await cache.set(cache_key, hashtags, ttl=300)  # 5 минут
        return hashtags
    
    async def _get_user_interactions_batch(self, session, post_id: str, user_id: str) -> Dict:
        """Получить все взаимодействия пользователя - 4 ПРОСТЫХ ЗАПРОСА"""
        
        # Запускаем 4 простых запроса параллельно
        like_task = self._check_like(session, post_id, user_id)
        bookmark_task = self._check_bookmark(session, post_id, user_id)
        reaction_task = self._get_user_reaction(session, post_id, user_id)
        counts_task = self._get_reaction_counts(session, post_id)
        
        # Ждем все результаты
        like_result, bookmark_result, reaction_result, counts_result = await asyncio.gather(
            like_task, bookmark_task, reaction_task, counts_task,
            return_exceptions=True
        )
        
        return {
            'is_liked': like_result if isinstance(like_result, bool) else False,
            'is_bookmarked': bookmark_result if isinstance(bookmark_result, bool) else False,
            'user_reaction': reaction_result if isinstance(reaction_result, dict) else None,
            'reactions_counts': counts_result if isinstance(counts_result, list) else []
        }
    
    async def _check_like(self, session, post_id: str, user_id: str) -> bool:
        """Проверить лайк"""
        query = """
        DECLARE $post_id AS Utf8;
        DECLARE $user_id AS Utf8;
        SELECT COUNT(*) as cnt FROM feed_likes 
        WHERE post_id = $post_id AND user_id = $user_id;
        """
        try:
            repo = BaseRepository(session)
            result = await repo.execute(query, {'$post_id': post_id, '$user_id': user_id})
            return result and result[0] and result[0].cnt > 0
        except:
            return False
    
    async def _check_bookmark(self, session, post_id: str, user_id: str) -> bool:
        """Проверить закладку"""
        query = """
        DECLARE $post_id AS Utf8;
        DECLARE $user_id AS Utf8;
        SELECT COUNT(*) as cnt FROM feed_bookmarks 
        WHERE post_id = $post_id AND user_id = $user_id;
        """
        try:
            repo = BaseRepository(session)
            result = await repo.execute(query, {'$post_id': post_id, '$user_id': user_id})
            return result and result[0] and result[0]['cnt'] > 0
        except:
            return False
    
    async def _get_user_reaction(self, session, post_id: str, user_id: str) -> Optional[Dict]:
        """Получить реакцию пользователя"""
        logger.info(f"👤 _get_user_reaction START: post_id={post_id}, user_id={user_id}")
        
        query = """
        DECLARE $post_id AS Utf8;
        DECLARE $user_id AS Utf8;
        SELECT reaction_type FROM feed_reactions 
        WHERE entity_type = 'post' AND entity_id = $post_id AND user_id = $user_id;
        """
        try:
            repo = BaseRepository(session)
            result = await repo.execute(query, {'$post_id': post_id, '$user_id': user_id})
            logger.info(f"👤 _get_user_reaction result: {result}")
            
            if result and result[0]:
                reaction_type = result[0]['reaction_type']
                logger.info(f"✅ _get_user_reaction: found reaction_type={reaction_type}")
                return {'reaction_type': reaction_type}
            logger.info(f"ℹ️ _get_user_reaction: no reaction found")
            return None
        except Exception as e:
            logger.error(f"❌ Error in _get_user_reaction: {e}", exc_info=True)
            return None
    
    async def _get_reaction_counts(self, session, post_id: str) -> List[Dict]:
        """Получить счетчики реакций"""
        logger.info(f"📊 _get_reaction_counts START: post_id={post_id}")
        
        query = """
        DECLARE $post_id AS Utf8;
        SELECT reaction_type, COUNT(*) as count
        FROM feed_reactions 
        WHERE entity_type = 'post' AND entity_id = $post_id 
        GROUP BY reaction_type;
        """
        try:
            repo = BaseRepository(session)
            result = await repo.execute(query, {'$post_id': post_id})
            logger.info(f"📊 _get_reaction_counts result: {result}")
            
            if result:
                formatted = [{'reaction_type': row['reaction_type'], 'count': row['count']} for row in result]
                logger.info(f"✅ _get_reaction_counts formatted: {formatted}")
                return formatted
            logger.info(f"ℹ️ _get_reaction_counts: no results")
            return []
        except Exception as e:
            logger.error(f"❌ Error in _get_reaction_counts: {e}", exc_info=True)
            return []
    async def update(self, session, post_id: str, user_id: str, user_data: Dict, updates: Dict) -> Post:
        """Обновить пост - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"✏️ Updating post {post_id} by user {user_id}")
        
        PostValidator.validate_update(updates)
        
        posts_repo = PostRepository(session)
        
        post_data = await posts_repo.get_by_id(post_id)
        if not post_data:
            raise NotFoundError(f"Post {post_id} not found")
        
        if str(post_data['user_id']) != str(user_id):
            raise PermissionError("You can only update your own posts")
        
        success = await posts_repo.update_post(post_id, updates)
        if not success:
            raise DatabaseError("Failed to update post")
        
        await PostCache.invalidate(post_id)
        await cache.delete_pattern(f"feed:*")
        
        return await self.get(session, post_id, user_id, user_data)
    
    async def delete(self, session, post_id: str, user_id: str, permanent: bool = False) -> Dict:
        """Удалить пост - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"🗑️ Deleting post {post_id} by user {user_id}, permanent={permanent}")
        
        posts_repo = PostRepository(session)
        
        post_data = await posts_repo.get_by_id(post_id)
        if not post_data:
            raise NotFoundError(f"Post {post_id} not found")
        
        if str(post_data['user_id']) != str(user_id):
            raise PermissionError("You can only delete your own posts")
        
        await posts_repo.update_post(post_id, {
            'is_deleted': True, 
            'deleted_at': to_timestamp(datetime.utcnow())
        })
        
        await PostCache.invalidate(post_id)
        await cache.delete_pattern(f"feed:*")
        
        return {'deleted': True, 'post_id': post_id, 'permanent': permanent}
    
    async def get_user_posts_with_cursor(self, session, user_id: str, target_user_id: str, current_user_data: Dict,
                             post_type: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Post], Optional[str]]:
        """Получить посты пользователя с курсорной пагинацией"""
        logger.info(f"📋 Getting posts for user {target_user_id} with cursor")
        
        follows_repo = FollowRepository(session)
        posts_repo = PostRepository(session)
        
        if target_user_id != user_id:
            is_following = await follows_repo.check(user_id, target_user_id)
            if not is_following:
                # Только публичные посты
                posts_data, next_cursor = await posts_repo.get_by_user_with_cursor(
                    target_user_id, limit, cursor, False
                )
                posts_data = [p for p in posts_data if p.get('visibility') == 'public']
            else:
                # Все посты
                posts_data, next_cursor = await posts_repo.get_by_user_with_cursor(
                    target_user_id, limit, cursor, False
                )
        else:
            # Свои посты
            posts_data, next_cursor = await posts_repo.get_by_user_with_cursor(
                target_user_id, limit, cursor, False
            )
        
        posts = await self._format_posts(session, posts_data, user_id, current_user_data)
        
        return posts, next_cursor
    
    async def toggle_like(self, session, user_id: str, post_id: str) -> Dict:
        """Поставить или убрать лайк - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"❤️ Toggling like on post {post_id} by user {user_id}")
        logger.info(f"🔍 ===== НАЧАЛО toggle_like =====")
        logger.info(f"🔍 Параметры: user_id={user_id}, post_id={post_id}")
        
        # Rate limit check
        if not await self._check_rate_limit('toggle_like', user_id, 
                                            feed_config.LIKE_RATE_LIMIT, 
                                            feed_config.RATE_LIMIT_PERIOD):
            logger.warning(f"⚠️ Rate limit exceeded for user {user_id}")
            raise RateLimitError("Rate limit exceeded. Too many likes.")
        
        # Получаем репозитории
        posts_repo = PostRepository(session)
        likes_repo = LikeRepository(session)
        
        logger.info(f"🔍 Getting post {post_id} from database")
        post = await posts_repo.get_by_id(post_id)
        if not post:
            logger.error(f"❌ Post {post_id} not found")
            raise NotFoundError(f"Post {post_id} not found")
        
        logger.info(f"🔍 Post found: author={post.get('user_id')}, title={post.get('title', '')[:50]}...")
        
        now = datetime.utcnow()
        logger.info(f"🔍 Current time: {now.isoformat()}")
        
        # Транзакция
        logger.info(f"🔍 Starting transaction for like toggle")
        async with await UnitOfWork.from_session(session) as uow:
            likes_repo.set_transaction(uow._transaction)
            logger.info(f"🔍 Calling likes_repo.toggle_atomic")
            result = await likes_repo.toggle_atomic(uow, post_id, user_id, now)
            logger.info(f"🔍 toggle_atomic result: {result}")
        
        # Проверяем условие для отправки уведомления
        post_author_id = str(post['user_id'])
        current_user_id = str(user_id)
        
        logger.info(f"🔍 Like result analysis:")
        logger.info(f"  - liked: {result.get('liked')}")
        logger.info(f"  - post_author_id: {post_author_id}")
        logger.info(f"  - current_user_id: {current_user_id}")
        logger.info(f"  - authors different: {post_author_id != current_user_id}")
        logger.info(f"  - condition met: {result.get('liked') and post_author_id != current_user_id}")
        
        if result.get('liked') and post_author_id != current_user_id:
            logger.info(f"📨 Condition met! Adding like notification task")
            
            # Отправляем WebSocket уведомление
            await send_ws(post_author_id, {
                'type': 'notification',
                'data': {
                    'notification_type': 'like',
                    'from_user_id': user_id,
                    'post_id': post_id,
                    'created_at': now.isoformat()
                }
            })
            
            logger.info(f"📦 background_worker.add_high with params: user_id={user_id}, post_author_id={post_author_id}, post_id={post_id}")
            
            try:
                await background_worker.add_high(
                    self._send_like_notification,
                    user_id=user_id,
                    post_author_id=post_author_id,
                    post_id=post_id
                )
                logger.info(f"✅ Like notification task added successfully to background worker")
            except Exception as e:
                logger.error(f"❌ Failed to add like notification task: {e}", exc_info=True)
        else:
            logger.info(f"📨 Condition not met, skipping notification")
            if not result.get('liked'):
                logger.info(f"  - Reason: liked={result.get('liked')} (user unliked)")
            if post_author_id == current_user_id:
                logger.info(f"  - Reason: user liked their own post")
        
        # Инвалидируем кэш
        logger.info(f"🔍 Invalidating cache for post {post_id}")
        await PostCache.invalidate(post_id)
        
        logger.info(f"🔍 ===== КОНЕЦ toggle_like =====, returning result: {result}")
        return result
    
    async def _send_like_notification(self, user_id: str, post_author_id: str, post_id: str):
        """Фоновая задача для отправки уведомления о лайке"""
        logger.info(f"📨 ===== _send_like_notification =====")
        
        try:
            # 🔥 WebSocket уведомление (real-time)
            try:
                await send_ws(post_author_id, {
                    'type': 'like',
                    'data': {
                        'liked_by': user_id,
                        'post_id': post_id,
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                })
                logger.info(f"📨 WebSocket like notification sent to {post_author_id}")
            except Exception as e:
                logger.error(f"❌ WebSocket like notification failed: {e}")
            
            # 👇 БД уведомление (для истории)
            async with RequestContext() as ctx:
                notifications_repo = NotificationRepository(ctx.session)
                
                await notifications_repo.create(
                    user_id=post_author_id,
                    from_user_id=user_id,
                    notification_type='like',
                    entity_type='post',
                    entity_id=post_id,
                    created_at=datetime.utcnow(),
                    extra_data={'liked_by': user_id}
                )
                
        except Exception as e:
            logger.error(f"❌ Error in _send_like_notification: {e}", exc_info=True)
    
    async def get_feed_with_cursor(self, session, user_id: str, user_data: Dict, limit: int, cursor: Optional[str] = None) -> Tuple[List[Post], Optional[str]]:
        """Получить ленту новостей с курсорной пагинацией"""
        logger.info(f"📋 Getting feed for user {user_id} with cursor, limit={limit}")
        
        posts_repo = PostRepository(session)
        
        posts_data, next_cursor = await posts_repo.get_feed_with_cursor(limit, cursor)
        posts = await self._format_posts(session, posts_data, user_id, user_data)
        
        return posts, next_cursor
    
    async def search_with_cursor(self, session, query: str, user_id: str, user_data: Dict, limit: int, cursor: Optional[str] = None) -> Tuple[List[Post], Optional[str]]:
        """Поиск постов с курсорной пагинацией"""
        logger.info(f"🔎 Searching posts for '{query}' with cursor")
        
        if len(query) < 2:
            raise ValidationError("Search query must be at least 2 characters")
        
        posts_repo = PostRepository(session)
        
        posts_data, next_cursor = await posts_repo.search_with_cursor(query, limit, cursor)
        posts = await self._format_posts(session, posts_data, user_id, user_data)
        
        return posts, next_cursor
    
    async def search_by_hashtag_with_cursor(self, session, hashtag: str, user_id: str, user_data: Dict,
                                  limit: int, cursor: Optional[str] = None) -> Tuple[List[Post], Optional[str]]:
        """Найти все посты с определенным хештегом с курсором"""
        logger.info(f"🔍 Searching posts by hashtag: #{hashtag} with cursor")
        
        clean_hashtag = hashtag.lower().strip('#')
        
        if len(clean_hashtag) < 2:
            raise ValidationError("Hashtag must be at least 2 characters")
        
        hashtags_repo = HashtagRepository(session)
        
        hashtag_id = await hashtags_repo.get_id_by_name(clean_hashtag)
        if not hashtag_id:
            return [], None
        
        posts_data, next_cursor = await hashtags_repo.get_posts_by_hashtag_id_with_cursor(
            hashtag_id=hashtag_id,
            limit=limit,
            cursor=cursor
        )
        
        posts = await self._format_posts(session, posts_data, user_id, user_data)
        
        return posts, next_cursor
    
    async def suggest_hashtags(self, session, prefix: str, limit: int = 10) -> List[Dict]:
        """Подсказка хештегов при вводе"""
        logger.info(f"💡 Suggesting hashtags for prefix: '{prefix}'")
        
        if len(prefix) < 2:
            return []
        
        hashtags_repo = HashtagRepository(session)
        
        hashtags = await hashtags_repo.search_hashtags_by_prefix(prefix, limit)
        
        result = []
        for row in hashtags:
            last_used = from_timestamp(row.get('last_used_at'))
            result.append({
                'id': row.get('id'),
                'name': row.get('name'),
                'posts_count': row.get('posts_count', 0),
                'last_used': last_used.isoformat() + 'Z' if last_used else None,
                'time_ago': time_ago(last_used) if last_used else None
            })
        
        return result
    
    async def get_related_hashtags(self, session, hashtag: str, limit: int = 10) -> List[Dict]:
        """Получить связанные хештеги"""
        logger.info(f"🔗 Getting related hashtags for: #{hashtag}")
        
        clean_hashtag = hashtag.lower().strip('#')
        
        hashtags_repo = HashtagRepository(session)
        
        related = await hashtags_repo.get_related_hashtags(clean_hashtag, limit)
        
        result = []
        for row in related:
            result.append({
                'id': row.get('id'),
                'name': row.get('name'),
                'posts_count': row.get('posts_count', 0),
                'co_occurrence': row.get('co_occurrence', 0)
            })
        
        return result
    
    async def get_trending_with_cursor(self, session, user_id: str, user_data: Dict, limit: int, cursor: Optional[str] = None) -> Tuple[List[Post], Optional[str], List[Dict]]:
        """Получить популярный контент с курсором"""
        logger.info(f"📈 Getting trending content for user {user_id} with cursor")
        
        hashtags_repo = HashtagRepository(session)
        posts_repo = PostRepository(session)
        
        hashtags_data = await hashtags_repo.get_trending(limit)
        hashtags = []
        
        for row in hashtags_data:
            last_used = from_timestamp(row.get('last_used_at'))
            hashtags.append({
                "id": row.get('id'),
                "name": row.get('name'),
                "posts_count": row.get('posts_count', 0),
                "last_used": last_used.isoformat() + "Z" if last_used else None,
                "time_ago": time_ago(last_used) if last_used else None
            })
        
        week_ago = datetime.utcnow() - timedelta(days=feed_config.TRENDING_DAYS)
        week_ago_ts = to_timestamp(week_ago)
        posts_data, next_cursor = await posts_repo.get_trending_with_cursor(week_ago_ts, limit, cursor)
        posts = await self._format_posts(session, posts_data, user_id, user_data)
        
        return posts, next_cursor, hashtags
    
    def _extract_hashtags(self, text: str) -> List[str]:
        """Извлечь хэштеги из текста"""
        if not text:
            return []
        hashtags = re.findall(r'#(\w+)', text)
        return list(set(hashtags))
    
    async def _format_posts(self, session, posts_data: List[Dict], current_user_id: str,
                            current_user_data: Dict) -> List[Post]:
        """Форматировать список постов с полными данными авторов и взаимодействиями (ОПТИМИЗИРОВАННАЯ ВЕРСИЯ)"""
        if not posts_data:
            return []
        
        # 1. Собираем все ID авторов и ID постов
        author_ids = []
        post_ids = []
        
        for p in posts_data:
            post_id = p.get('post_id')
            if post_id:
                post_ids.append(post_id)
            
            user_id = p.get('user_id')
            if user_id:
                author_ids.append(str(user_id))
            
            # Для репостов добавляем автора оригинального поста
            if p.get('is_repost') and p.get('original_post_id'):
                original_post_id = p.get('original_post_id')
                # Получим оригинальный пост позже отдельным запросом
        
        author_ids = list(set(author_ids))
        logger.info(f"📊 Formatting {len(posts_data)} posts with {len(author_ids)} unique authors")
        
        # 2. Получаем все данные ОДНИМ запросом на каждый тип
        users_repo = UserRepository(session)
        authors_data = await users_repo.get_many(author_ids, current_user_data if current_user_id in author_ids else None)
        
        # 3. Получаем все закладки для этих постов
        bookmarks_repo = BookmarkRepository(session)
        bookmarks_map = await bookmarks_repo.check_many(current_user_id, post_ids)
        
        # 4. Получаем все репосты для этих постов
        repost_service = RepostService()
        reposted_map = await repost_service.check_many_reposted(session, current_user_id, post_ids)
        
        # 5. Получаем все реакции и хэштеги ОДНИМ запросом
        reactions_repo = ReactionRepository(session)
        hashtags_repo = HashtagRepository(session)
        
        # Получаем счетчики реакций для всех постов
        reactions_counts_map = await reactions_repo.get_reactions_counts_for_entities('post', post_ids)
        
        # Получаем реакции текущего пользователя
        user_reactions_map = await reactions_repo.get_user_reactions_for_entities(
            current_user_id, 'post', post_ids
        )
        
        # Получаем хэштеги для всех постов
        hashtags_map = await hashtags_repo.get_by_posts(post_ids)
        
        # 6. Загружаем оригинальные посты для репостов
        posts_repo = PostRepository(session)
        original_post_ids = list({
            str(p['original_post_id'])
            for p in posts_data
            if p.get('is_repost') and p.get('original_post_id')
        })
        originals_map: Dict[str, Dict] = {}
        if original_post_ids:
            logger.info(f"📋 Fetching {len(original_post_ids)} original posts for reposts")
            originals_map = await posts_repo.get_many_by_ids(original_post_ids)
            # Добавляем авторов оригинальных постов
            for orig in originals_map.values():
                if orig.get('user_id'):
                    author_ids.append(str(orig['user_id']))
            # Обновляем authors_data с новыми авторами
            if original_post_ids:
                new_authors = await users_repo.get_many(list(set(author_ids) - set(authors_data.keys())))
                authors_data.update(new_authors)
        
        # 6b. Загружаем данные каналов для постов с original_author = "channel:{id}"
        channel_ids_needed = list({
            str(p.get('original_author', '') or '').split(':')[1]
            for p in posts_data
            if str(p.get('original_author', '') or '').startswith('channel:')
        })
        logger.info(f"🏷️ channel_ids_needed for feed: {channel_ids_needed}")
        channels_data: Dict[str, Dict] = {}
        if channel_ids_needed:
            try:
                ids_list = ', '.join(f"CAST({cid} AS Uint64)" for cid in channel_ids_needed)
                ch_query = f"""
                SELECT CAST(id AS Utf8) AS id, title, username
                FROM `chats`
                WHERE id IN ({ids_list}) AND is_deleted = false;
                """
                ch_result = session.transaction().execute(
                    session.prepare(ch_query),
                    {},
                    commit_tx=True
                )
                if ch_result and ch_result[0].rows:
                    for row in ch_result[0].rows:
                        _cid = row.get('id', '') or ''
                        if isinstance(_cid, bytes):
                            _cid = _cid.decode('utf-8')
                        _title = row.get('title') or ''
                        if isinstance(_title, bytes):
                            _title = _title.decode('utf-8')
                        _uname = row.get('username') or ''
                        if isinstance(_uname, bytes):
                            _uname = _uname.decode('utf-8')
                        channels_data[str(_cid)] = {
                            'title': _title or None,
                            'username': _uname or None,
                        }
                logger.info(f"🏷️ channels_data loaded: {channels_data}")
            except Exception as e:
                logger.warning(f"⚠️ Could not load channel info: {e}", exc_info=True)

        # 7. Формируем список постов
        posts = []
        for row in posts_data:
            try:
                post_id = row.get('post_id')
                if not post_id:
                    continue
                
                user_id = row.get('user_id')
                if not user_id:
                    continue
                
                # Получаем данные автора (для постов из канала — используем данные канала)
                _oa = row.get('original_author', '') or ''
                if isinstance(_oa, bytes):
                    _oa = _oa.decode('utf-8')
                orig_author_raw = str(_oa)
                if orig_author_raw.startswith('channel:'):
                    ch_id = orig_author_raw.split(':')[1]
                    ch_info = channels_data.get(ch_id, {})
                    author = Author(
                        id=ch_id,
                        username=ch_info.get('username') or f"channel_{ch_id[-8:]}",
                        display_name=ch_info.get('title') or ch_info.get('username') or f"Channel {ch_id[-8:]}",
                        avatar_url=None,
                        is_verified=False,
                    )
                else:
                    author_data = authors_data.get(str(user_id))
                    if author_data:
                        author = Author.from_db(author_data)
                    else:
                        author = Author(
                            id=str(user_id),
                            username=f"user_{str(user_id)[:8]}",
                            display_name=f"User {str(user_id)[:8]}",
                            avatar_url=None,
                            is_verified=False
                        )
                
                # Обрабатываем media_urls
                media_urls = []
                media_urls_val = row.get('media_urls')
                if media_urls_val:
                    try:
                        if isinstance(media_urls_val, str):
                            media_urls = json.loads(media_urls_val)
                        elif isinstance(media_urls_val, list):
                            media_urls = media_urls_val
                    except:
                        media_urls = []
                
                # Получаем хэштеги
                hashtags = hashtags_map.get(post_id, [])
                hashtag_list = []
                for h in hashtags:
                    if isinstance(h, dict):
                        hashtag_list.append({
                            'id': str(h.get('id', '')),
                            'name': str(h.get('name', ''))
                        })
                
                # Обрабатываем даты
                created_at_val = row.get('created_at')
                created_at = from_timestamp(created_at_val) if created_at_val else datetime.utcnow()
                
                updated_at_val = row.get('updated_at')
                updated_at = from_timestamp(updated_at_val) if updated_at_val else None
                
                # Формируем реакции
                reactions_counts = reactions_counts_map.get(post_id, [])
                user_reaction_type = user_reactions_map.get(post_id)
                
                reactions = []
                total_reactions = 0
                
                for rc in reactions_counts:
                    rt = rc.get('reaction_type')
                    count = int(rc.get('count', 0))
                    if not rt or count == 0:
                        continue
                    total_reactions += count
                    user_reacted = (user_reaction_type == rt)
                    
                    reactions.append(ReactionCount(
                        type=rt,
                        count=count,
                        emoji=ReactionType.get_emoji(rt),
                        display_name=ReactionType.get_display_name(rt),
                        user_reacted=user_reacted
                    ))
                
                # Добавляем реакцию пользователя если её нет в counts
                if user_reaction_type and user_reaction_type not in [r.type for r in reactions]:
                    reactions.append(ReactionCount(
                        type=user_reaction_type,
                        count=1,
                        emoji=ReactionType.get_emoji(user_reaction_type),
                        display_name=ReactionType.get_display_name(user_reaction_type),
                        user_reacted=True
                    ))
                    total_reactions += 1
                
                # Формируем превью (топ-3)
                sorted_reactions = sorted(
                    reactions,
                    key=lambda x: (-x.count, x.type)
                )[:3]
                
                reactions_preview = []
                for r in sorted_reactions:
                    reactions_preview.append({
                        'type': r.type,
                        'count': r.count,
                        'emoji': r.emoji,
                        'display_name': r.display_name
                    })
                
                # Экранируем строковые поля
                content = str(row.get('content', ''))
                title = str(row.get('title', '')) if row.get('title') else ''
                content_preview = str(row.get('content_preview', '')) or content[:200] + ("..." if len(content) > 200 else "")
                
                # Для репостов
                original_post_json = ''
                is_repost = bool(row.get('is_repost', False))
                orig_post_id = str(row.get('original_post_id')) if row.get('original_post_id') else ''
                
                if is_repost and orig_post_id and orig_post_id in originals_map:
                    orig = originals_map[orig_post_id]
                    orig_user_id = str(orig.get('user_id', ''))
                    orig_author_data = authors_data.get(orig_user_id)
                    if orig_author_data:
                        orig_author = Author.from_db(orig_author_data)
                    else:
                        orig_author = Author(
                            id=orig_user_id,
                            username=f"user_{orig_user_id[:8]}",
                            display_name=f"User {orig_user_id[:8]}",
                            avatar_url=None,
                            is_verified=False,
                        )
                    orig_media_urls = []
                    try:
                        mu = orig.get('media_urls')
                        if mu:
                            orig_media_urls = json.loads(mu) if isinstance(mu, str) else mu
                    except Exception:
                        pass
                    orig_created_at = from_timestamp(orig.get('created_at'))
                    original_post_json = json.dumps({
                        'id': orig_post_id,
                        'content': str(orig.get('content', '')),
                        'media_urls': orig_media_urls,
                        'created_at': orig_created_at.isoformat() + 'Z' if orig_created_at else None,
                        'author': {
                            'id': orig_author.id,
                            'username': orig_author.username,
                            'display_name': orig_author.display_name,
                            'avatar_url': orig_author.avatar_url,
                            'is_verified': orig_author.is_verified,
                        },
                    }, ensure_ascii=False)
                
                # Создаём пост
                post = Post(
                    id=post_id,
                    user_id=str(user_id),
                    title=title,
                    content=content,
                    content_preview=content_preview,
                    media_urls=media_urls,
                    hashtags=hashtag_list,
                    comments_count=int(row.get('comments_count') or 0),
                    reposts_count=int(row.get('reposts_count') or 0),
                    views_count=int(row.get('views_count') or 0),
                    bookmarks_count=int(row.get('bookmarks_count') or 0),
                    reactions_count=total_reactions,
                    reactions=reactions,
                    reactions_preview=reactions_preview,
                    is_repost=is_repost,
                    original_post_id=orig_post_id if orig_post_id else None,
                    repost_comment=str(row.get('repost_comment', '')) if row.get('repost_comment') else '',
                    is_pinned=bool(row.get('is_pinned', False)),
                    is_edited=bool(row.get('is_edited', False)),
                    visibility=str(row.get('visibility', 'public')),
                    language=str(row.get('language', 'ru')),
                    sentiment_score=row.get('sentiment_score'),
                    reading_time_minutes=int(row.get('reading_time_minutes')) if row.get('reading_time_minutes') else None,
                    created_at=created_at.isoformat() + "Z",
                    updated_at=updated_at.isoformat() + "Z" if updated_at else None,
                    published_at=from_timestamp(row.get('published_at')).isoformat() + "Z" if row.get('published_at') else None,
                    scheduled_for=from_timestamp(row.get('scheduled_for')).isoformat() + "Z" if row.get('scheduled_for') else None,
                    is_deleted=bool(row.get('is_deleted', False)),
                    deleted_at=from_timestamp(row.get('deleted_at')).isoformat() + "Z" if row.get('deleted_at') else None,
                    original_author=str(row.get('original_author', '')) if row.get('original_author') else None,
                    original_post=original_post_json if original_post_json else None,
                    source_channel_id=str(row.get('original_author', '')).split(':')[1] if str(row.get('original_author', '')).startswith('channel:') else None,
                    author=author,
                    interactions=Interactions(
                        is_liked=False,
                        is_bookmarked=bookmarks_map.get(post_id, False),
                        is_reposted=reposted_map.get(post_id, False),
                        is_owner=str(user_id) == str(current_user_id)
                    )
                )
                posts.append(post)
                
            except Exception as e:
                logger.error(f"❌ Error formatting post {post_id}: {e}", exc_info=True)
                continue
        
        logger.info(f"✅ _format_posts: returning {len(posts)} posts")
        return posts

class RepostService:
    """Сервис для работы с репостами - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.post_service = PostService()
        logger.info("✅ RepostService инициализирован")
    
    async def create(self, session, user_id: str, original_post_id: str, comment: str = '') -> Dict:
        """Создать репост - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"🔄 ===== RepostService.create START =====")
        logger.info(f"📌 Параметры: user_id={user_id}, original_post_id={original_post_id}, comment='{comment}'")
        
        if not await self._check_rate_limit('create_repost', user_id, 
                                            feed_config.REPOST_RATE_LIMIT, 
                                            feed_config.RATE_LIMIT_PERIOD):
            logger.warning(f"⚠️ Rate limit exceeded for user {user_id}")
            raise RateLimitError("Rate limit exceeded. Too many reposts.")
        
        posts_repo = PostRepository(session)
        reposts_repo = RepostRepository(session)
        notifications_repo = NotificationRepository(session)
        
        logger.info(f"🔍 Checking original post {original_post_id}")
        original = await posts_repo.get_by_id(original_post_id)
        if not original:
            logger.error(f"❌ Original post {original_post_id} not found")
            raise NotFoundError(f"Original post {original_post_id} not found")
        
        logger.info(f"✅ Original post found: author={original.get('user_id')}")
        
        logger.info(f"🔍 Checking if user already reposted")
        existing = await reposts_repo.check_reposted(user_id, original_post_id)
        if existing:
            logger.warning(f"⚠️ User {user_id} already reposted post {original_post_id}")
            raise ValidationError("You have already reposted this post")
        
        logger.info(f"➕ Creating repost")
        repost_id = await reposts_repo.create(original_post_id, user_id, comment)

        if not repost_id:
            logger.error(f"❌ Failed to create repost")
            raise DatabaseError("Failed to create repost")

        logger.info(f"✅ Repost created with ID: {repost_id}")

        # Создаем запись в feed_posts чтобы репост появился в ленте
        feed_post_id = str(uuid.uuid4())
        logger.info(f"➕ Creating feed_posts entry for repost: {feed_post_id}")
        await posts_repo.create_repost_post(
            post_id=feed_post_id,
            user_id=user_id,
            original_post_id=original_post_id,
            repost_comment=comment,
        )

        # Обновляем счетчик репостов в фоне
        logger.info(f"📦 Adding background task to increment repost count")
        await background_worker.add_low(
            self._increment_repost_count_background,
            original_post_id=original_post_id
        )
        
        # Отправляем уведомление в фоне
        post_author_id = str(original['user_id'])
        current_user_id = str(user_id)
        
        logger.info(f"📨 Checking notification condition:")
        logger.info(f"   - post_author_id: {post_author_id}")
        logger.info(f"   - current_user_id: {current_user_id}")
        logger.info(f"   - authors different: {post_author_id != current_user_id}")
        
        if post_author_id != current_user_id:
            logger.info(f"📨 Condition met! Adding repost notification")
            
            # Отправляем WebSocket уведомление
            await send_ws(post_author_id, {
                'type': 'notification',
                'data': {
                    'notification_type': 'repost',
                    'from_user_id': user_id,
                    'original_post_id': original_post_id,
                    'repost_id': repost_id,
                    'comment': comment,
                    'created_at': datetime.utcnow().isoformat()
                }
            })
            
            logger.info(f"📦 Adding background task for repost notification")
            try:
                await background_worker.add_high(
                    self._send_repost_notification,
                    user_id=user_id,
                    post_author_id=post_author_id,
                    original_post_id=original_post_id,
                    repost_id=repost_id,
                    comment=comment
                )
                logger.info(f"✅ Repost notification task added successfully")
            except Exception as e:
                logger.error(f"❌ Failed to add repost notification task: {e}", exc_info=True)
        else:
            logger.info(f"📨 Condition not met - user reposted their own post")
        
        # Инвалидируем кэш
        logger.info(f"🧹 Invalidating cache")
        await RepostCache.invalidate(original_post_id)
        await PostCache.invalidate(original_post_id)
        await cache.delete_pattern(f"feed:*")
        
        logger.info(f"✅ ===== RepostService.create END =====")
        
        return {
            'success': True,
            'repost_id': repost_id,
            'message': 'Repost created successfully'
        }
    
    async def get_by_original_with_cursor(self, session, original_post_id: str, current_user_id: str,
                              limit: int = 20, cursor: Optional[str] = None) -> Dict:
        """Получить список репостов поста с курсорной пагинацией"""
        logger.info(f"📋 Getting reposts for post {original_post_id} with cursor")
        
        # Проверяем кэш
        cached = await RepostCache.get(original_post_id, cursor)
        if cached:
            logger.info(f"📦 Returning cached reposts for post {original_post_id}")
            return cached
        
        reposts_repo = RepostRepository(session)
        users_repo = UserRepository(session)
        
        try:
            reposts_data, next_cursor = await reposts_repo.get_by_original_with_cursor(
                original_post_id, limit, cursor
            )
            
            has_more = next_cursor is not None
            
            # Форматируем результат
            reposts = []
            for row in reposts_data:
                try:
                    # Создаем автора из данных
                    author = Author(
                        id=row['user']['id'],
                        username=row['user']['username'],
                        display_name=row['user']['display_name'],
                        is_verified=row['user']['is_verified'],
                        is_following=False  # Можно добавить проверку подписки
                    )
                    
                    repost_item = {
                        'repost_id': row['repost_id'],
                        'original_post_id': row['original_post_id'],
                        'user': author.dict(),
                        'comment': row.get('comment', ''),
                        'show_original': row.get('show_original', True),
                        'created_at': row['created_at'],
                        'time_ago': row['time_ago']
                    }
                    reposts.append(repost_item)
                    
                except Exception as e:
                    logger.error(f"❌ Error formatting repost: {e}")
                    continue
            
            result = {
                'reposts': reposts,
                'next_cursor': next_cursor,
                'has_more': has_more
            }
            
            # Сохраняем в кэш (только если есть данные)
            if reposts:
                await RepostCache.set(original_post_id, result, cursor)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in get_by_original_with_cursor: {e}", exc_info=True)
            # Возвращаем пустой результат вместо ошибки
            return {
                'reposts': [],
                'next_cursor': None,
                'has_more': False,
                'error': str(e)
            }
    
    async def get_by_user_with_cursor(self, session, user_id: str, current_user_id: str,
                          limit: int = 20, cursor: Optional[str] = None) -> Dict:
        """Получить репосты пользователя с курсорной пагинацией"""
        logger.info(f"📋 Getting reposts by user {user_id} with cursor")
        
        # Кэшируем по user_id (но не по всем пользователям)
        cache_key = f"user_reposts:{user_id}:{cursor or 'first'}"
        cached = await cache.get(cache_key)
        if cached:
            logger.info(f"📦 Returning cached user reposts for {user_id}")
            return cached
        
        reposts_repo = RepostRepository(session)
        posts_repo = PostRepository(session)
        users_repo = UserRepository(session)
        
        reposts_data, next_cursor = await reposts_repo.get_by_user_with_cursor(user_id, limit, cursor)
        has_more = next_cursor is not None
        
        # Batch загрузка оригинальных постов
        original_post_ids = [r['original_post_id'] for r in reposts_data]
        
        posts_map = {}
        if original_post_ids:
            # Загружаем все посты одним запросом
            posts_dict = await posts_repo.get_many_by_ids(original_post_ids)
            
            # Загружаем авторов для постов
            author_ids = []
            for pid, post in posts_dict.items():
                if post and post.get('user_id'):
                    author_ids.append(str(post['user_id']))
            
            authors_data = {}
            if author_ids:
                authors_data = await users_repo.get_many(list(set(author_ids)))
            
            for pid, post in posts_dict.items():
                if post:
                    author_data = authors_data.get(str(post['user_id']))
                    author = Author.from_db(author_data) if author_data else Author(
                        id=str(post['user_id']),
                        username=f"user_{str(post['user_id'])[:8]}",
                        display_name=f"User {str(post['user_id'])[:8]}",
                        is_verified=False
                    )
                    
                    posts_map[pid] = {
                        'id': post['post_id'],
                        'user_id': post['user_id'],
                        'title': post.get('title', ''),
                        'content': post['content'],
                        'content_preview': post.get('content_preview', ''),
                        'created_at': from_timestamp(post['created_at']).isoformat() + 'Z',
                        'author': author.dict()
                    }
        
        reposts = []
        for row in reposts_data:
            created_at = from_timestamp(row['created_at'])
            
            reposts.append({
                'repost_id': row['repost_id'],
                'original_post_id': row['original_post_id'],
                'comment': row.get('comment', ''),
                'show_original': row.get('show_original', True),
                'created_at': created_at.isoformat() + 'Z',
                'time_ago': time_ago(created_at),
                'original_post': posts_map.get(row['original_post_id'])
            })
        
        result = {
            'reposts': reposts,
            'next_cursor': next_cursor,
            'has_more': has_more
        }
        
        # Сохраняем в кэш на 10 секунд
        await cache.set(cache_key, result, ttl=10)
        
        return result
    
    async def delete(self, session, repost_id: str, user_id: str) -> Dict:
        """Удалить репост - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"🗑️ Deleting repost {repost_id} by user {user_id}")
        
        reposts_repo = RepostRepository(session)
        posts_repo = PostRepository(session)
        
        repost = await reposts_repo.get_by_id(repost_id)
        if not repost:
            raise NotFoundError(f"Repost {repost_id} not found")
        
        if str(repost['user_id']) != str(user_id):
            raise PermissionError("You can only delete your own reposts")
        
        original_post_id = repost['original_post_id']
        
        success = await reposts_repo.delete(repost['repost_id'], user_id)
        if not success:
            raise DatabaseError("Failed to delete repost")
        
        # Обновляем счетчик репостов в фоне
        await background_worker.add_low(
            self._decrement_repost_count_background,
            original_post_id=original_post_id
        )
        
        # Инвалидируем кэш
        await RepostCache.invalidate(original_post_id)
        await PostCache.invalidate(original_post_id)
        await cache.delete_pattern(f"user_reposts:{user_id}:*")
        await cache.delete_pattern(f"feed:*")
        
        return {
            'success': True,
            'deleted': True,
            'repost_id': repost_id,
            'original_post_id': original_post_id
        }
    
    async def check_reposted(self, session, user_id: str, post_id: str) -> bool:
        """Проверить, репостнул ли пользователь пост"""
        reposts_repo = RepostRepository(session)
        return await reposts_repo.check_reposted(user_id, post_id)
    
    async def check_many_reposted(self, session, user_id: str, post_ids: List[str]) -> Dict[str, bool]:
        """Проверить репосты для нескольких постов"""
        if not post_ids:
            return {}
        
        reposts_repo = RepostRepository(session)
        return await reposts_repo.check_many_reposted(user_id, post_ids)
    
    async def get_stats(self, session, original_post_id: str) -> Dict:
        """Получить статистику по репостам поста"""
        cache_key = f"repost_stats:{original_post_id}"
        cached = await cache.get(cache_key)
        if cached:
            return cached
        
        reposts_repo = RepostRepository(session)
        stats = await reposts_repo.get_stats(original_post_id)
        await cache.set(cache_key, stats, ttl=300)  # 5 минут
        return stats
    
    async def _increment_repost_count_background(self, original_post_id: str):
        """Фоновая задача для увеличения счетчика репостов"""
        logger.info(f"📊 _increment_repost_count_background START: post={original_post_id}")
        try:
            async with RequestContext() as ctx:
                posts_repo = PostRepository(ctx.session)
                await posts_repo.increment_reposts(original_post_id, 1)
                logger.info(f"✅ Incremented repost count for {original_post_id}")
        except Exception as e:
            logger.error(f"❌ Failed to increment repost count: {e}", exc_info=True)
    
    async def _decrement_repost_count_background(self, original_post_id: str):
        """Фоновая задача для уменьшения счетчика репостов"""
        logger.info(f"📊 _decrement_repost_count_background START: post={original_post_id}")
        try:
            async with RequestContext() as ctx:
                posts_repo = PostRepository(ctx.session)
                await posts_repo.increment_reposts(original_post_id, -1)
                logger.info(f"✅ Decremented repost count for {original_post_id}")
        except Exception as e:
            logger.error(f"❌ Failed to decrement repost count: {e}", exc_info=True)
    
    async def _send_repost_notification(self, user_id: str, post_author_id: str, 
                                         original_post_id: str, repost_id: str, comment: str):
        """Фоновая задача для отправки уведомления о репосте"""
        logger.info(f"📨 ===== _send_repost_notification =====")
        
        try:
            # 🔥 WebSocket уведомление
            try:
                await send_ws(post_author_id, {
                    'type': 'repost',
                    'data': {
                        'reposted_by': user_id,
                        'original_post_id': original_post_id,
                        'repost_id': repost_id,
                        'comment': comment,
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                })
                logger.info(f"📨 WebSocket repost notification sent to {post_author_id}")
            except Exception as e:
                logger.error(f"❌ WebSocket repost notification failed: {e}")
            
            # 👇 БД уведомление
            async with RequestContext() as ctx:
                notifications_repo = NotificationRepository(ctx.session)
                
                await notifications_repo.create(
                    user_id=post_author_id,
                    from_user_id=user_id,
                    notification_type='repost',
                    entity_type='post',
                    entity_id=original_post_id,
                    created_at=datetime.utcnow(),
                    extra_data={
                        'repost_id': repost_id,
                        'comment': comment,
                        'reposted_by': user_id
                    }
                )
                
        except Exception as e:
            logger.error(f"❌ Error in _send_repost_notification: {e}", exc_info=True)
    
    async def _check_rate_limit(self, action: str, user_id: str, max_requests: int, period: int) -> bool:
        """Проверить rate limit (заглушка)"""
        logger.debug(f"⏱️ Rate limit check: action={action}, user={user_id}")
        return True


class ReactionService:
    """Сервис для работы с реакциями - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ с кэшированием"""
    
    def __init__(self):
        self.post_service = PostService()
        self._stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'db_queries': 0
        }
    
    async def get_reaction_counts(self, session, entity_type: str, entity_id: str,
                                    current_user_id: str) -> List[ReactionCount]:
        """Получить только счетчики реакций с кэшированием"""
        # Проверяем кэш
        cached = await ReactionCache.get_counts(entity_type, entity_id)
        user_reaction = await ReactionCache.get_user_reaction(current_user_id, entity_type, entity_id)
        
        if cached is not None and user_reaction is not None:
            self._stats['cache_hits'] += 1
            # Преобразуем cached в ReactionCount с учетом user_reaction
            counts = []
            user_reaction_type = user_reaction.get('reaction_type') if user_reaction else None
            
            for item in cached:
                # Фильтруем только с count > 0
                if item.get('count', 0) <= 0:
                    continue
                    
                user_reacted = (user_reaction_type == item['reaction_type'])
                counts.append(ReactionCount(
                    type=item['reaction_type'],
                    count=item['count'],
                    emoji=ReactionType.get_emoji(item['reaction_type']),
                    display_name=ReactionType.get_display_name(item['reaction_type']),
                    user_reacted=user_reacted
                ))
            await Metrics.inc_counter('cache_hits')
            return counts
        
        self._stats['cache_misses'] += 1
        self._stats['db_queries'] += 1
        await Metrics.inc_counter('cache_misses')
        await Metrics.inc_counter('reaction_queries')
        
        # Если нет в кэше - идем в БД
        reactions_repo = ReactionRepository(session)
        
        # Получаем данные одним запросом
        counts_data = await reactions_repo.get_reaction_counts(entity_type, entity_id)
        user_reaction_data = await reactions_repo.get_user_reaction(
            current_user_id, entity_type, entity_id
        )
        
        # Фильтруем нулевые счётчики перед сохранением в кэш
        filtered_counts = [c for c in counts_data if c.get('count', 0) > 0]
        
        # Сохраняем в кэш
        await ReactionCache.set_counts(entity_type, entity_id, filtered_counts)
        await ReactionCache.set_user_reaction(
            current_user_id, entity_type, entity_id, 
            user_reaction_data
        )
        
        # Форматируем результат
        counts = []
        user_reaction_type = user_reaction_data.get('reaction_type') if user_reaction_data else None
        
        for row in filtered_counts:
            rt = row['reaction_type']
            user_reacted = (user_reaction_type == rt)
                
            counts.append(ReactionCount(
                type=rt,
                count=row['count'],
                emoji=ReactionType.get_emoji(rt),
                display_name=ReactionType.get_display_name(rt),
                user_reacted=user_reacted
            ))
        
        return counts
    
    async def get_reactions_batch(self, session, entity_type: str, 
                                    entity_ids: List[str], 
                                    current_user_id: str) -> Dict[str, List[ReactionCount]]:
        """Получить реакции для нескольких сущностей одним запросом (BATCH)"""
        if not entity_ids:
            return {}
        
        # Дедупликация
        unique_ids = list(set(entity_ids))
        result = {}
        
        # Проверяем кэш
        cached_results = {}
        uncached_ids = []
        
        for entity_id in unique_ids:
            cached = await ReactionCache.get_counts(entity_type, entity_id)
            user_reaction = await ReactionCache.get_user_reaction(current_user_id, entity_type, entity_id)
            
            if cached is not None and user_reaction is not None:
                self._stats['cache_hits'] += 1
                await Metrics.inc_counter('cache_hits')
                cached_results[entity_id] = (cached, user_reaction)
            else:
                uncached_ids.append(entity_id)
        
        # Обрабатываем закэшированные
        for entity_id, (cached, user_reaction) in cached_results.items():
            user_reaction_type = user_reaction.get('reaction_type') if user_reaction else None
            counts = []
            for item in cached:
                user_reacted = (user_reaction_type == item['reaction_type'])
                counts.append(ReactionCount(
                    type=item['reaction_type'],
                    count=item['count'],
                    emoji=ReactionType.get_emoji(item['reaction_type']),
                    display_name=ReactionType.get_display_name(item['reaction_type']),
                    user_reacted=user_reacted
                ))
            result[entity_id] = counts
        
        # Загружаем недостающие из БД
        if uncached_ids:
            self._stats['db_queries'] += 1
            await Metrics.inc_counter('reaction_queries')
            
            reactions_repo = ReactionRepository(session)
            
            # Получаем счетчики для всех сущностей одним запросом
            counts_map = await reactions_repo.get_reactions_counts_for_entities(entity_type, uncached_ids)
            
            # Получаем реакции пользователя для всех сущностей
            user_reactions_map = await reactions_repo.get_user_reactions_for_entities(
                current_user_id, entity_type, uncached_ids
            )
            
            # Сохраняем в кэш и форматируем
            for entity_id in uncached_ids:
                counts_data = counts_map.get(entity_id, [])
                user_reaction_type = user_reactions_map.get(entity_id)
                
                # Сохраняем в кэш
                await ReactionCache.set_counts(entity_type, entity_id, counts_data)
                user_reaction_data = {'reaction_type': user_reaction_type} if user_reaction_type else None
                await ReactionCache.set_user_reaction(current_user_id, entity_type, entity_id, user_reaction_data)
                
                # Форматируем
                counts = []
                for item in counts_data:
                    rt = item['reaction_type']
                    user_reacted = (user_reaction_type == rt)
                    counts.append(ReactionCount(
                        type=rt,
                        count=item['count'],
                        emoji=ReactionType.get_emoji(rt),
                        display_name=ReactionType.get_display_name(rt),
                        user_reacted=user_reacted
                    ))
                result[entity_id] = counts
        
        return result
    
    async def get_reactions_preview_batch(self, session, entity_type: str, 
                                           entity_ids: List[str], 
                                           limit: int = 3) -> Dict[str, List[Dict]]:
        """Получить топ-N реакций для нескольких сущностей (для превью)"""
        if not entity_ids:
            return {}
        
        # Проверяем кэш
        cache_key = f"reactions_preview_batch:{entity_type}:{hash(','.join(sorted(entity_ids)))}:{limit}"
        cached = await cache.get(cache_key)
        if cached:
            await Metrics.inc_counter('cache_hits')
            return cached
        
        await Metrics.inc_counter('cache_misses')
        await Metrics.inc_counter('reaction_queries')
        
        # Получаем все счетчики
        reactions_repo = ReactionRepository(session)
        all_counts = await reactions_repo.get_reactions_counts_for_entities(entity_type, entity_ids)
        
        # Формируем превью
        result = {}
        for eid, counts in all_counts.items():
            sorted_counts = sorted(counts, key=lambda x: x['count'], reverse=True)[:limit]
            preview = []
            for row in sorted_counts:
                rt = row['reaction_type']
                preview.append({
                    'type': rt,
                    'count': row['count'],
                    'emoji': ReactionType.get_emoji(rt),
                    'display_name': ReactionType.get_display_name(rt)
                })
            result[eid] = preview
        
        # Сохраняем в кэш
        await cache.set(cache_key, result, ttl=60)
        return result
    
    async def get_reactions_preview(self, session, entity_type, entity_id, limit=3, user_id=None):
        """Получить топ-N реакций для сущности"""
        cache_key = f"reactions_preview:{entity_type}:{entity_id}:{limit}"
        cached = await cache.get(cache_key)
        if cached:
            await Metrics.inc_counter('cache_hits')
            logger.info(f"✅ Preview cache hit: {cached}")
            return cached
        
        await Metrics.inc_counter('cache_misses')
        await Metrics.inc_counter('reaction_queries')
        
        reactions_repo = ReactionRepository(session)
        counts_data = await reactions_repo.get_reaction_counts(entity_type, entity_id)
        logger.info(f"Preview counts from DB: {counts_data}")
        
        # Фильтруем только с count > 0 и сортируем
        sorted_counts = sorted(
            [c for c in counts_data if c.get('count', 0) > 0],
            key=lambda x: (-x.get('count', 0), x.get('reaction_type', ''))
        )[:limit]
        
        preview = []
        for row in sorted_counts:
            rt = row['reaction_type']
            preview.append({
                'type': rt,
                'count': row['count'],
                'emoji': ReactionType.get_emoji(rt),
                'display_name': ReactionType.get_display_name(rt)
            })
        
        logger.info(f"Returning preview: {preview}")
        await cache.set(cache_key, preview, ttl=60)
        return preview
    
    async def toggle(self, session, user_id: str, entity_type: str, 
                      entity_id: str, reaction_type: str) -> Dict:
        """Переключить реакцию - ИСПРАВЛЕНО (оптимизированная инвалидация)"""
        logger.info(f"🔄 Toggling {reaction_type} reaction on {entity_type} {entity_id}")
        
        # Валидация
        if entity_type not in ['post', 'comment']:
            raise ValidationError("entity_type must be 'post' or 'comment'")
        if reaction_type not in ReactionType.get_all():
            raise ValidationError(f"Invalid reaction type. Allowed: {ReactionType.get_all()}")
        
        await Metrics.inc_counter('reaction_toggles')
        
        # Проверяем существование сущности
        posts_repo = PostRepository(session)
        comments_repo = CommentRepository(session)
        reactions_repo = ReactionRepository(session)
        
        if entity_type == 'post':
            entity = await posts_repo.get_by_id(entity_id)
            if not entity:
                raise NotFoundError(f"Post {entity_id} not found")
        else:
            entity = await comments_repo.get_by_id(entity_id)
            if not entity:
                raise NotFoundError(f"Comment {entity_id} not found")
        
        # Выполняем транзакцию
        async with await UnitOfWork.from_session(session) as uow:
            reactions_repo.set_transaction(uow._transaction)
            result = await reactions_repo.toggle_reaction(
                uow, user_id, entity_type, entity_id, reaction_type
            )
        
        # ✅ ИСПРАВЛЕНИЕ: единая инвалидация кэша
        # Инвалидируем ВСЕ кэши через ReactionCache
        await ReactionCache.invalidate(entity_type, entity_id)
        
        # Инвалидируем кэш самой сущности (поста или комментария)
        if entity_type == 'post':
            await PostCache.invalidate(entity_id)
        else:
            await CommentCache.invalidate(entity_id)
        
        # Инвалидируем превью (для разных лимитов)
        for limit in [3, 5, 10]:
            preview_key = f"reactions_preview:{entity_type}:{entity_id}:{limit}"
            await cache.delete(preview_key)
        
        # Инвалидируем списки реакций (все страницы)
        list_pattern = f"reactions_list:{entity_type}:{entity_id}:*"
        await cache.delete_pattern(list_pattern)
        
        # Если произошла ошибка, возвращаем её
        if result['action'] == 'error':
            return {
                'entity_type': entity_type,
                'entity_id': entity_id,
                'action': 'error',
                'error': result.get('error', 'Unknown error'),
                'user_reaction': None,
                'reactions': [],
                'total_count': 0
            }
        
        # Получаем обновленные счетчики
        counts = await self.get_reaction_counts(session, entity_type, entity_id, user_id)
        
        # Формируем ответ
        user_reaction_data = None
        if result['action'] in ['added', 'updated']:
            user_reaction_data = {
                'type': reaction_type,
                'emoji': ReactionType.get_emoji(reaction_type),
                'display_name': ReactionType.get_display_name(reaction_type)
            }
        
        return {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'action': result['action'],
            'user_reaction': user_reaction_data,
            'reactions': [c.dict() for c in counts],
            'total_count': sum(c.count for c in counts)
        }
    
    async def get_reactions(self, session, entity_type: str, entity_id: str, 
                             current_user_id: str, limit: int = 20, 
                             offset: int = 0) -> Dict:
        """
        Получить список реакций на сущность с пагинацией - ИСПРАВЛЕНО (graceful handling)
        """
        logger.info(f"📋 Getting reactions for {entity_type} {entity_id}")
        
        # Проверяем кэш (только для первых страниц)
        cache_key = f"reactions_list:{entity_type}:{entity_id}:{limit}:{offset}"
        if offset == 0:
            cached = await cache.get(cache_key)
            if cached:
                await Metrics.inc_counter('cache_hits')
                return cached
        
        await Metrics.inc_counter('cache_misses')
        await Metrics.inc_counter('reaction_queries')
        
        # Проверяем существование сущности с graceful handling
        posts_repo = PostRepository(session)
        comments_repo = CommentRepository(session)
        
        entity_exists = False
        if entity_type == 'post':
            entity = await posts_repo.get_by_id(entity_id)
            entity_exists = entity is not None
        else:
            entity = await comments_repo.get_by_id(entity_id)
            entity_exists = entity is not None
        
        if not entity_exists:
            logger.warning(f"⚠️ {entity_type} {entity_id} not found, returning empty result")
            empty_result = {
                'entity_type': entity_type,
                'entity_id': entity_id,
                'user_reaction': None,
                'reactions': [],
                'reaction_counts': [],
                'total_count': 0,
                'pagination': {
                    'limit': limit,
                    'offset': offset,
                    'has_more': False
                }
            }
            # Кэшируем пустой результат ненадолго
            if offset == 0:
                await cache.set(cache_key, empty_result, ttl=30)
            return empty_result
        
        reactions_repo = ReactionRepository(session)
        users_repo = UserRepository(session)
        
        # Получаем данные
        reactions_data = await reactions_repo.get_reactions_with_users(
            entity_type, entity_id, limit, offset
        )
        counts_data = await reactions_repo.get_reaction_counts(entity_type, entity_id)
        user_reaction_data = await reactions_repo.get_user_reaction(
            current_user_id, entity_type, entity_id
        )
        
        # Форматируем счетчики
        counts = []
        for row in counts_data:
            rt = row['reaction_type']
            user_reacted = False
            if user_reaction_data:
                user_reacted = (user_reaction_data.get('reaction_type') == rt)
            
            counts.append(ReactionCount(
                type=rt,
                count=row['count'],
                emoji=ReactionType.get_emoji(rt),
                display_name=ReactionType.get_display_name(rt),
                user_reacted=user_reacted
            ))
        
        # Форматируем список реакций
        reactions = []
        
        # Собираем все user_id
        user_ids = []
        for row in reactions_data:
            # Пробуем получить user_id из разных возможных ключей
            user_id = row.get('r.user_id') or row.get('user_id')
            if user_id:
                user_ids.append(user_id)
        
        # Загружаем данные пользователей
        users_data = {}
        if user_ids:
            users_data = await users_repo.get_many(list(set(user_ids)))
        
        for row in reactions_data:
            try:
                # Получаем reaction_id
                reaction_id = row.get('r.reaction_id') or row.get('reaction_id')
                if not reaction_id:
                    logger.error(f"❌ No reaction_id in row: {row}")
                    continue
                
                # Получаем user_id
                user_id = row.get('r.user_id') or row.get('user_id')
                if not user_id:
                    logger.error(f"❌ No user_id in row: {row}")
                    continue
                
                # Получаем reaction_type
                reaction_type = row.get('r.reaction_type') or row.get('reaction_type')
                if not reaction_type:
                    logger.error(f"❌ No reaction_type in row: {row}")
                    continue
                
                # Получаем created_at
                created_at_value = row.get('r.created_at') or row.get('created_at')
                if created_at_value:
                    created_at = from_timestamp(created_at_value)
                else:
                    logger.warning(f"⚠️ Missing created_at in reaction row: {row}")
                    created_at = datetime.utcnow()
                
                # Получаем данные пользователя
                user_data = users_data.get(user_id, {})
                
                # Декодируем имя
                first_name = safe_b64decode(user_data.get('first_name_encrypted', ''))
                last_name = safe_b64decode(user_data.get('last_name_encrypted', ''))
                
                # Получаем username (с проверкой префиксов)
                username = row.get('u.username') or row.get('username') or user_data.get('username', f"user_{user_id[:8]}")
                
                # Получаем display_name
                display_name = row.get('u.display_name') or row.get('display_name') or user_data.get('display_name', '')
                if not display_name:
                    if first_name and last_name:
                        display_name = f"{first_name} {last_name}".strip()
                    elif first_name:
                        display_name = first_name
                    else:
                        display_name = username
                
                # Получаем is_verified
                is_verified = row.get('u.is_verified') or row.get('is_verified') or user_data.get('is_verified', False)
                
                author = Author(
                    id=user_id,
                    username=username,
                    display_name=display_name,
                    is_verified=bool(is_verified)
                )
                
                reactions.append(Reaction(
                    id=reaction_id,
                    user_id=user_id,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    reaction_type=reaction_type,
                    created_at=created_at.isoformat() + "Z",
                    time_ago=time_ago(created_at),
                    user=author
                ))
                
            except Exception as e:
                logger.error(f"❌ Error formatting reaction: {e}", exc_info=True)
                continue
        
        # Форматируем реакцию пользователя
        user_reaction = None
        if user_reaction_data:
            rt = user_reaction_data.get('reaction_type')
            user_reaction = {
                'type': rt,
                'emoji': ReactionType.get_emoji(rt),
                'display_name': ReactionType.get_display_name(rt)
            }
        
        result = {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'user_reaction': user_reaction,
            'reactions': [r.dict() for r in reactions],
            'reaction_counts': [c.dict() for c in counts],
            'total_count': sum(c.count for c in counts),
            'pagination': {
                'limit': limit,
                'offset': offset,
                'has_more': len(reactions) == limit
            }
        }
        
        # Кэшируем первую страницу
        if offset == 0:
            await cache.set(cache_key, result, ttl=10)
        
        return result
    
    async def get_reaction_counts_only(self, session, entity_type: str, entity_id: str,
                                        current_user_id: str) -> Dict:
        """Получить только счетчики реакций (упрощенная версия)"""
        counts = await self.get_reaction_counts(session, entity_type, entity_id, current_user_id)
        
        return {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'reaction_counts': [c.dict() for c in counts],
            'total_count': sum(c.count for c in counts)
        }
    
    def get_stats(self) -> Dict:
        """Получить статистику сервиса"""
        return {
            **self._stats,
            'hit_rate': self._stats['cache_hits'] / max(self._stats['cache_hits'] + self._stats['cache_misses'], 1)
        }
    
    async def clear_cache(self, entity_type: Optional[str] = None, 
                           entity_id: Optional[str] = None,
                           user_id: Optional[str] = None):
        """Очистить кэш реакций"""
        if entity_type and entity_id:
            await ReactionCache.invalidate(entity_type, entity_id, user_id)
        else:
            await ReactionCache.invalidate_all()
        logger.info(f"🧹 Reaction cache cleared")


class LikeService:
    """Сервис для работы с лайками - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.post_service = PostService()
    
    async def get_user_likes_with_cursor(self, session, user_id: str, target_user_id: str,
                             limit: int, cursor: Optional[str] = None) -> Tuple[List[Post], Optional[str]]:
        """Получить посты, которые лайкнул пользователь, с курсором"""
        logger.info(f"❤️ Getting likes for user {target_user_id} with cursor")
        
        users_repo = UserRepository(session)
        likes_repo = LikeRepository(session)
        
        user = await users_repo.get(target_user_id)
        if not user:
            raise NotFoundError(f"User {target_user_id} not found")
        
        likes_data, next_cursor = await likes_repo.get_by_user_with_cursor(target_user_id, limit, cursor)
        
        if not likes_data:
            return [], None
        
        formatted_posts = []
        for row in likes_data:
            post_dict = {}
            for key, value in row.items():
                if not key.startswith('like_') and key != 'like_id':
                    post_dict[key] = value
            
            formatted_posts.append(post_dict)
        
        posts = await self.post_service._format_posts(session, formatted_posts, user_id, {})
        
        return posts, next_cursor


class BookmarkService:
    """Сервис для работы с закладками - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.post_service = PostService()
    
    async def toggle(self, session, user_id: str, post_id: str,
                     folder: str = 'general', notes: str = '') -> Dict:
        """Добавить или удалить закладку - С ЛОГИРОВАНИЕМ"""
        logger.info(f"🔖 ===== BOOKMARK_TOGGLE START =====")
        logger.info(f"🔖 user_id: {user_id}")
        logger.info(f"🔖 post_id: {post_id}")
        
        posts_repo = PostRepository(session)
        bookmarks_repo = BookmarkRepository(session)
        
        # Проверим пост до операции
        post_before = await posts_repo.get_by_id(post_id)
        logger.info(f"🔖 Post before: {post_before}")
        
        post = post_before
        if not post:
            logger.error(f"❌ Post {post_id} not found")
            raise NotFoundError(f"Post {post_id} not found")
        
        now = datetime.utcnow()
        
        async with await UnitOfWork.from_session(session) as uow:
            bookmarks_repo.set_transaction(uow._transaction)
            result = await bookmarks_repo.toggle_atomic(
                uow, user_id, post_id, folder, notes, now
            )
        
        logger.info(f"🔖 toggle_atomic result: {result}")
        
        await PostCache.invalidate(post_id)
        await cache.delete_pattern(f"bookmarks:{user_id}:*")
        
        # Проверим пост после операции
        post_after = await posts_repo.get_by_id(post_id)
        logger.info(f"🔖 Post after: {post_after}")
        
        actual_count = post_after.get('bookmarks_count', 0) if post_after else 0
        
        try:
            bookmarks_count = int(actual_count)
        except (TypeError, ValueError):
            bookmarks_count = 0
        
        final_result = {
            'bookmarked': result.get('bookmarked', False),
            'bookmarks_count': bookmarks_count
        }
        
        logger.info(f"🔖 Final result: {final_result}")
        logger.info(f"🔖 ===== BOOKMARK_TOGGLE END =====")
        
        return final_result
    
    async def list_with_cursor(self, session, user_id: str, limit: int, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Получить список закладок с курсорной пагинацией"""
        logger.info(f"📋 Listing bookmarks for user {user_id} with cursor")
        
        cache_key = f"bookmarks:{user_id}:{limit}:{cursor}"
        cached = await cache.get(cache_key)
        if cached:
            return cached.get('items', []), cached.get('next_cursor')
        
        bookmarks_repo = BookmarkRepository(session)
        posts_repo = PostRepository(session)
        users_repo = UserRepository(session)
        
        bookmarks_data, next_cursor = await bookmarks_repo.list_by_user_with_cursor(user_id, limit, cursor)
        
        if not bookmarks_data:
            return [], next_cursor
        
        post_ids = []
        for row in bookmarks_data:
            post_id = row.get('post_id')
            if post_id:
                post_ids.append(post_id)
        
        posts_map = {}
        if post_ids:
            posts_dict = await posts_repo.get_many_by_ids(post_ids)
            
            for pid, post in posts_dict.items():
                if post:
                    author_data = await users_repo.get(str(post['user_id']))
                    author = Author.from_db(author_data) if author_data else Author(
                        id=str(post['user_id']),
                        username=f"user_{str(post['user_id'])[:8]}",
                        display_name=f"User {str(post['user_id'])[:8]}",
                        is_verified=False
                    )
                    
                    posts_map[pid] = {
                        'id': post['post_id'],
                        'user_id': post['user_id'],
                        'title': post.get('title', ''),
                        'content': post['content'],
                        'content_preview': post.get('content_preview', ''),
                        'created_at': from_timestamp(post['created_at']).isoformat() + 'Z',
                        'author': author.dict()
                    }
        
        bookmarks = []
        for row in bookmarks_data:
            try:
                post_id = row.get('post_id')
                if not post_id:
                    continue
                
                created_at_val = row.get('bookmark_created_at')
                if created_at_val:
                    created_at = from_timestamp(created_at_val)
                    created_at_str = created_at.isoformat() + 'Z'
                else:
                    created_at_str = datetime.utcnow().isoformat() + 'Z'
                
                bookmark = {
                    "id": row.get('bookmark_id'),
                    "post_id": post_id,
                    "folder": row.get('folder', 'general'),
                    "notes": row.get('notes', ''),
                    "created_at": created_at_str,
                    "post": posts_map.get(post_id, {})
                }
                bookmarks.append(bookmark)
                
            except Exception as e:
                logger.error(f"❌ Error processing bookmark: {e}")
                continue
        
        result = {
            'items': bookmarks,
            'next_cursor': next_cursor
        }
        
        await cache.set(cache_key, result, ttl=feed_config.CACHE_TTL_FEED)
        
        return bookmarks, next_cursor


class SearchService:
    """Сервис для поиска - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.post_service = PostService()
    
    async def search_posts_with_cursor(self, session, query: str, user_id: str,
                           limit: int, cursor: Optional[str] = None) -> Tuple[List[Post], Optional[str]]:
        """Поиск постов с курсорной пагинацией"""
        return await self.post_service.search_with_cursor(session, query, user_id, {}, limit, cursor)


class TrendingService:
    """Сервис для трендов - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.post_service = PostService()
    
    async def get_with_cursor(self, session, user_id: str, limit: int, cursor: Optional[str] = None) -> Dict:
        """Получить популярный контент с курсором"""
        posts, next_cursor, hashtags = await self.post_service.get_trending_with_cursor(session, user_id, {}, limit, cursor)
        
        return {
            "hashtags": hashtags,
            "posts": [p.dict() for p in posts],
            "next_cursor": next_cursor,
            "has_more": next_cursor is not None
        }


class CommentService:
    """Сервис для работы с комментариями - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ с кэшированием"""
    
    def __init__(self):
        self.post_service = PostService()
        self.reaction_service = ReactionService()
        self._stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'db_queries': 0
        }
    async def _format_root_comment(self, session, row: Dict, current_user_id: str,
                                     authors: Dict, reactions_counts_map: Dict,
                                     user_reactions_map: Dict) -> Comment:
        """Форматировать корневой комментарий (без загрузки ответов)"""
        
        created_at = from_timestamp(row['created_at'])
        
        author = authors.get(str(row['user_id']), Author(
            id=str(row['user_id']),
            username=f"user_{str(row['user_id'])[:8]}",
            display_name=f"User {str(row['user_id'])[:8]}",
            is_verified=False
        ))
        
        # Получаем реакции
        reactions_counts = reactions_counts_map.get(row['comment_id'], [])
        user_reaction = user_reactions_map.get(row['comment_id'])
        
        reactions = []
        for rc in reactions_counts:
            rt = rc['reaction_type']
            reactions.append(ReactionCount(
                type=rt,
                count=rc['count'],
                emoji=ReactionType.get_emoji(rt),
                display_name=ReactionType.get_display_name(rt),
                user_reacted=(user_reaction == rt)
            ))
        
        # Превью реакций (топ-3)
        preview = await self.reaction_service.get_reactions_preview(
            session=session,
            entity_type='comment',
            entity_id=row['comment_id'],
            limit=3,
            user_id=current_user_id
        )
        
        return Comment(
            id=row['comment_id'],
            post_id=row['post_id'],
            user_id=row['user_id'],
            parent_comment_id=row.get('parent_comment_id'),
            content=row['content'],
            replies_count=row.get('replies_count') or 0,
            reactions_count=sum(r.count for r in reactions),
            created_at=created_at.isoformat() + "Z",
            time_ago=time_ago(created_at),
            author=author,
            interactions=Interactions(
                is_liked=False,
                is_owner=str(row['user_id']) == str(current_user_id)
            ),
            replies=[],  # 🔥 НЕ ЗАГРУЖАЕМ ОТВЕТЫ!
            reactions=reactions,
            reactions_preview=preview
        )
    
    async def _format_reply_comment(self, session, row: Dict, current_user_id: str,
                                      authors: Dict, reactions_counts_map: Dict,
                                      user_reactions_map: Dict) -> Comment:
        """Форматировать ответ на комментарий"""
        
        created_at = from_timestamp(row['created_at'])
        
        author = authors.get(str(row['user_id']), Author(
            id=str(row['user_id']),
            username=f"user_{str(row['user_id'])[:8]}",
            display_name=f"User {str(row['user_id'])[:8]}",
            avatar_url=None,
            is_verified=False
        ))
        
        # Получаем реакции
        reactions_counts = reactions_counts_map.get(row['comment_id'], [])
        user_reaction = user_reactions_map.get(row['comment_id'])
        
        reactions = []
        for rc in reactions_counts:
            rt = rc['reaction_type']
            reactions.append(ReactionCount(
                type=rt,
                count=rc['count'],
                emoji=ReactionType.get_emoji(rt),
                display_name=ReactionType.get_display_name(rt),
                user_reacted=(user_reaction == rt)
            ))
        
        if user_reaction and user_reaction not in [r.type for r in reactions]:
            reactions.append(ReactionCount(
                type=user_reaction,
                count=1,
                emoji=ReactionType.get_emoji(user_reaction),
                display_name=ReactionType.get_display_name(user_reaction),
                user_reacted=True
            ))
        
        preview = []
        sorted_reactions = sorted(reactions, key=lambda x: -x.count)[:3]
        for r in sorted_reactions:
            preview.append({
                'type': r.type,
                'count': r.count,
                'emoji': r.emoji,
                'display_name': r.display_name
            })
        
        return Comment(
            id=row['comment_id'],
            post_id=row['post_id'],
            user_id=row['user_id'],
            parent_comment_id=row.get('parent_comment_id'),
            content=row['content'],
            replies_count=0,  # Ответы на ответы не показываем
            reactions_count=sum(r.count for r in reactions),
            created_at=created_at.isoformat() + "Z",
            time_ago=time_ago(created_at),
            author=author,
            interactions=Interactions(
                is_liked=False,
                is_owner=str(row['user_id']) == str(current_user_id)
            ),
            replies=[],
            reactions=reactions,
            reactions_preview=preview
        )
    async def _fetch_all_comments_reactions(self, session, comment_ids: List[str], user_id: str) -> Tuple[Dict, Dict]:
        """
        Получить реакции для всех комментариев ОДНИМ ЗАПРОСОМ (с чанками)
        Возвращает (reactions_counts_map, user_reactions_map)
        """
        if not comment_ids:
            return {}, {}
        
        # Убираем дубликаты
        unique_ids = list(set(comment_ids))
        chunk_size = 100  # YDB лимит для IN запроса
        
        all_reactions_counts = {}
        all_user_reactions = {}
        
        for i in range(0, len(unique_ids), chunk_size):
            chunk = unique_ids[i:i+chunk_size]
            
            # Создаем плейсхолдеры для IN запроса
            placeholders = []
            params = {'$user_id': user_id}
            for j, cid in enumerate(chunk):
                placeholder = f"$cid_{j}"
                placeholders.append(placeholder)
                params[placeholder] = cid
            
            placeholders_str = ', '.join(placeholders)
            
            # Создаем DECLARE для каждого параметра
            declare_parts = ["DECLARE $user_id AS Utf8;"]
            for j in range(len(chunk)):
                declare_parts.append(f"DECLARE $cid_{j} AS Utf8;")
            declare_block = "\n".join(declare_parts)
            
            # 1. Получаем счетчики реакций для всех комментариев в чанке
            counts_query = f"""
            {declare_block}
            SELECT entity_id, reaction_type, COUNT(*) as count
            FROM feed_reactions
            WHERE entity_type = 'comment' 
              AND entity_id IN ({placeholders_str})
            GROUP BY entity_id, reaction_type
            ORDER BY entity_id, count DESC;
            """
            
            try:
                repo = BaseRepository(session)
                counts_rows = await repo.execute(counts_query, params)
                
                for row in counts_rows:
                    comment_id = row['entity_id']
                    if comment_id not in all_reactions_counts:
                        all_reactions_counts[comment_id] = []
                    all_reactions_counts[comment_id].append({
                        'reaction_type': row['reaction_type'],
                        'count': row['count']
                    })
            except Exception as e:
                logger.error(f"❌ Error getting reaction counts chunk: {e}")
            
            # 2. Получаем реакции текущего пользователя для всех комментариев в чанке
            user_query = f"""
            {declare_block}
            SELECT entity_id, reaction_type
            FROM feed_reactions
            WHERE user_id = $user_id 
              AND entity_type = 'comment'
              AND entity_id IN ({placeholders_str});
            """
            
            try:
                user_rows = await repo.execute(user_query, params)
                
                for row in user_rows:
                    comment_id = row['entity_id']
                    all_user_reactions[comment_id] = row['reaction_type']
            except Exception as e:
                logger.error(f"❌ Error getting user reactions chunk: {e}")
        
        return all_reactions_counts, all_user_reactions
    
    async def _send_comment_notification(self, user_id: str, post_author_id: str, 
                                          post_id: str, comment_id: str, parent_author_id: Optional[str] = None):
        """Фоновая задача для отправки уведомления о комментарии"""
        try:
            # Определяем получателя
            target_user = parent_author_id or post_author_id
            
            # 🔥 WebSocket уведомление
            try:
                await send_ws(target_user, {
                    'type': 'comment' if not parent_author_id else 'reply',
                    'data': {
                        'commented_by': user_id,
                        'post_id': post_id,
                        'comment_id': comment_id,
                        'is_reply': bool(parent_author_id),
                        'parent_author_id': parent_author_id,
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                })
                logger.info(f"📨 WebSocket comment notification sent to {target_user}")
            except Exception as e:
                logger.error(f"❌ WebSocket comment notification failed: {e}")
            
            # 👇 БД уведомление
            async with RequestContext() as ctx:
                notifications_repo = NotificationRepository(ctx.session)
                
                now = datetime.utcnow()
                
                # Отправляем уведомление автору родительского комментария (если это ответ)
                if parent_author_id and parent_author_id != user_id:
                    await notifications_repo.create(
                        user_id=parent_author_id,
                        from_user_id=user_id,
                        notification_type='reply',
                        entity_type='comment',
                        entity_id=comment_id,
                        created_at=now,
                        extra_data={'post_id': post_id}
                    )
                # Иначе отправляем автору поста
                elif post_author_id != user_id:
                    await notifications_repo.create(
                        user_id=post_author_id,
                        from_user_id=user_id,
                        notification_type='comment',
                        entity_type='comment',
                        entity_id=comment_id,
                        created_at=now,
                        extra_data={'post_id': post_id}
                    )
                    
        except Exception as e:
            logger.error(f"❌ Failed to send comment notification: {e}")
    
    async def create(self, session, user_id: str, user_data: Dict, 
                     post_id: str, content: str, parent_comment_id: Optional[str] = None) -> Comment:
        """Создать комментарий с обновлением цепочки replies_count"""
        logger.info(f"💬 Creating comment on post {post_id}")
        
        try:
            # Rate limit check
            if not await self._check_rate_limit('create_comment', user_id, 
                                                feed_config.COMMENT_RATE_LIMIT, 
                                                feed_config.RATE_LIMIT_PERIOD):
                raise RateLimitError("Rate limit exceeded. Too many comments.")
            
            # Валидация
            CommentValidator.validate_create(content)
            
            # Репозитории
            posts_repo = PostRepository(session)
            follows_repo = FollowRepository(session)
            comments_repo = CommentRepository(session)
            users_repo = UserRepository(session)
            
            # Проверяем существование поста
            post = await posts_repo.get_by_id(post_id)
            if not post:
                raise NotFoundError(f"Post {post_id} not found")
            
            # Проверяем права доступа к посту
            if post['visibility'] != 'public' and str(post['user_id']) != str(user_id):
                if post['visibility'] == 'followers':
                    is_following = await follows_repo.check(user_id, str(post['user_id']))
                    if not is_following:
                        raise PermissionError("You cannot comment on this post")
                elif post['visibility'] == 'private':
                    raise PermissionError("This post is private")
            
            # Если это ответ на комментарий, проверяем существование родителя
            parent_author_id = None
            if parent_comment_id:
                parent = await comments_repo.get_by_id(parent_comment_id)
                if not parent:
                    raise NotFoundError(f"Parent comment {parent_comment_id} not found")
                parent_author_id = str(parent['user_id'])
            
            # Создаём комментарий
            comment_id = str(uuid.uuid4())
            now = datetime.utcnow()
            content_preview = content[:100] + ("..." if len(content) > 100 else "")
            
            async with await UnitOfWork.from_session(session) as uow:
                comments_repo.set_transaction(uow._transaction)
                posts_repo.set_transaction(uow._transaction)
                
                success = await comments_repo.create(
                    comment_id, post_id, user_id, content, content_preview,
                    parent_comment_id, now
                )
                
                if not success:
                    raise DatabaseError("Failed to create comment")
                
                # 🔥 ИСПРАВЛЕНИЕ: обновляем ВСЕХ родителей в цепочке
                if parent_comment_id:
                    await comments_repo.increment_replies_chain(parent_comment_id)
                
                # Увеличиваем счётчик комментариев у поста
                await posts_repo.increment_comments(post_id, 1)
                
                # Извлекаем упоминания
                mention_service = MentionService()
                await mention_service.extract_and_create_mentions(
                    session, content, 'comment', comment_id, user_id
                )
            
            # Инвалидируем кэш
            await CommentCache.invalidate(post_id)
            await PostCache.invalidate(post_id)
            
            # Получаем данные автора
            author_data = await users_repo.get(user_id)
            author = Author.from_db(author_data) if author_data else Author(
                id=user_id,
                username=f"user_{user_id[:8]}",
                display_name=f"User {user_id[:8]}",
                is_verified=False
            )
            
            # Создаём объект комментария
            comment = Comment(
                id=comment_id,
                post_id=post_id,
                user_id=user_id,
                parent_comment_id=parent_comment_id,
                content=content,
                replies_count=0,
                reactions_count=0,
                created_at=now.isoformat() + "Z",
                reactions=[],
                time_ago="только что",
                author=author,
                interactions=Interactions(
                    is_liked=False,
                    is_reposted=False,
                    is_bookmarked=False,
                    is_owner=True
                ),
                replies=[]
            )
            
            # Отправляем уведомления
            if parent_author_id and parent_author_id != user_id:
                await send_ws(parent_author_id, {
                    'type': 'notification',
                    'data': {
                        'notification_type': 'reply',
                        'from_user_id': user_id,
                        'post_id': post_id,
                        'comment_id': comment_id,
                        'parent_comment_id': parent_comment_id,
                        'content_preview': content_preview,
                        'created_at': now.isoformat()
                    }
                })
            elif str(post['user_id']) != str(user_id):
                await send_ws(str(post['user_id']), {
                    'type': 'notification',
                    'data': {
                        'notification_type': 'comment',
                        'from_user_id': user_id,
                        'post_id': post_id,
                        'comment_id': comment_id,
                        'content_preview': content_preview,
                        'created_at': now.isoformat()
                    }
                })
            
            return comment
            
        except Exception as e:
            logger.error(f"❌ Error creating comment: {e}")
            raise
    async def get_by_post_with_cursor(self, session, post_id: str, user_id: str,
                                      limit: int, cursor: Optional[str] = None) -> Tuple[List[Comment], Optional[str]]:
        """
        Получить КОРНЕВЫЕ комментарии к посту с курсорной пагинацией
        Ответы загружаются отдельно по требованию
        """
        logger.info(f"📋 Getting ROOT comments for post {post_id} with cursor, limit={limit}")
        
        # Проверяем кэш для первой страницы
        if not cursor:
            cached = await CommentCache.get(post_id)
            if cached:
                logger.info(f"📦 Returning cached root comments for post {post_id}")
                self._stats['cache_hits'] += 1
                await Metrics.inc_counter('cache_hits')
                comments = [Comment(**c) for c in cached.get('comments', [])]
                return comments, cached.get('next_cursor')
        
        self._stats['cache_misses'] += 1
        await Metrics.inc_counter('cache_misses')
        
        # Репозитории
        posts_repo = PostRepository(session)
        follows_repo = FollowRepository(session)
        comments_repo = CommentRepository(session)
        users_repo = UserRepository(session)
        
        # Проверяем существование поста
        post = await posts_repo.get_by_id(post_id)
        if not post:
            raise NotFoundError(f"Post {post_id} not found")
        
        # Проверяем права доступа
        if post['visibility'] != 'public' and str(post['user_id']) != str(user_id):
            if post['visibility'] == 'followers':
                is_following = await follows_repo.check(user_id, str(post['user_id']))
                if not is_following:
                    return [], None
            elif post['visibility'] == 'private':
                return [], None
        
        # Получаем ТОЛЬКО корневые комментарии (без ответов!)
        comments_data, next_cursor = await comments_repo.get_root_comments_with_cursor(post_id, limit, cursor)
        self._stats['db_queries'] += 1
        
        if not comments_data:
            return [], next_cursor
        
        # Собираем ID авторов
        author_ids = set()
        for c in comments_data:
            author_ids.add(str(c['user_id']))
        
        # Получаем всех авторов одним запросом
        authors_data = await users_repo.get_many(list(author_ids))
        self._stats['db_queries'] += 1
        authors = {}
        for uid, data in authors_data.items():
            if data:
                authors[uid] = Author.from_db(data)
        
        # 🔥 ИСПРАВЛЕНО: используем правильное имя метода
        comment_ids = [c['comment_id'] for c in comments_data]
        reactions_counts_map, user_reactions_map = await self._fetch_all_comments_reactions(
            session, comment_ids, user_id
        )
        
        # Форматируем комментарии (БЕЗ загрузки ответов!)
        comments = []
        for row in comments_data:
            comment = await self._format_root_comment(
                session, row, user_id, authors,
                reactions_counts_map, user_reactions_map
            )
            comments.append(comment)
        
        # Сохраняем в кэш (только первую страницу)
        if not cursor:
            cache_data = {
                'comments': [c.dict() for c in comments],
                'next_cursor': next_cursor
            }
            await CommentCache.set(post_id, cache_data)
        
        return comments, next_cursor
    async def get_replies_with_cursor(self, session, comment_id: str, user_id: str,
                                       limit: int = 10, cursor: Optional[str] = None) -> Tuple[List[Comment], Optional[str]]:
        """
        Получить ответы на конкретный комментарий (только первый уровень)
        """
        logger.info(f"💬 Getting replies for comment {comment_id} with cursor, limit={limit}")
        
        cache_key = f"replies:{comment_id}:{limit}:{cursor or 'first'}"
        if not cursor:
            cached = await cache.get(cache_key)
            if cached:
                logger.info(f"📦 Cached replies for comment {comment_id}")
                self._stats['cache_hits'] += 1
                replies = [Comment(**c) for c in cached.get('replies', [])]
                return replies, cached.get('next_cursor')
        
        self._stats['cache_misses'] += 1
        
        comments_repo = CommentRepository(session)
        users_repo = UserRepository(session)
        
        # Проверяем существование родительского комментария
        parent_comment = await comments_repo.get_by_id(comment_id)
        if not parent_comment:
            raise NotFoundError(f"Comment {comment_id} not found")
        
        # Получаем прямые ответы (только один уровень!)
        replies_data, next_cursor = await comments_repo.get_replies_with_cursor(comment_id, limit, cursor)
        self._stats['db_queries'] += 1
        
        if not replies_data:
            return [], next_cursor
        
        # Собираем ID авторов
        author_ids = set()
        for r in replies_data:
            author_ids.add(str(r['user_id']))
        
        # Получаем всех авторов одним запросом
        authors_data = await users_repo.get_many(list(author_ids))
        authors = {}
        for uid, data in authors_data.items():
            if data:
                authors[uid] = Author.from_db(data)
        
        # Получаем реакции для ответов
        reply_ids = [r['comment_id'] for r in replies_data]
        reactions_counts_map, user_reactions_map = await self._fetch_all_comments_reactions(
            session, reply_ids, user_id
        )
        
        # Форматируем ответы
        replies = []
        for row in replies_data:
            reply = await self._format_reply_comment(
                session, row, user_id, authors,
                reactions_counts_map, user_reactions_map
            )
            replies.append(reply)
        
        # Сохраняем в кэш
        if not cursor:
            cache_data = {
                'replies': [r.dict() for r in replies],
                'next_cursor': next_cursor
            }
            await cache.set(cache_key, cache_data, ttl=60)
        
        return replies, next_cursor
    async def _build_comment_tree_flat(self, session, comment_row: Dict, user_id: str,
                                       authors: Dict, replies_map: Dict,
                                       reactions_counts_map: Dict,
                                       user_reactions_map: Dict) -> Comment:
        """Построить дерево комментариев из плоской структуры"""
        
        # Получаем автора
        author = authors.get(str(comment_row['user_id']), Author(
            id=str(comment_row['user_id']),
            username=f"user_{str(comment_row['user_id'])[:8]}",
            display_name=f"User {str(comment_row['user_id'])[:8]}",
            is_verified=False
        ))
        
        # Получаем реакции
        reactions_counts = reactions_counts_map.get(comment_row['comment_id'], [])
        user_reaction = user_reactions_map.get(comment_row['comment_id'])
        
        reactions = []
        for rc in reactions_counts:
            rt = rc['reaction_type']
            reactions.append(ReactionCount(
                type=rt,
                count=rc['count'],
                emoji=ReactionType.get_emoji(rt),
                display_name=ReactionType.get_display_name(rt),
                user_reacted=(user_reaction == rt)
            ))
        
        # Получаем ответы для этого комментария
        replies = []
        for reply_row in replies_map.get(comment_row['comment_id'], []):
            reply = await self._build_comment_tree_flat(
                session,
                reply_row, user_id, authors, replies_map,
                reactions_counts_map, user_reactions_map
            )
            replies.append(reply)
        
        return await self._format_comment(
            session,
            row=comment_row,
            author=author,
            current_user_id=user_id,
            replies=replies,
            reactions=reactions
        )
    
    async def toggle_like(self, session, user_id: str, comment_id: str) -> Dict:
        """Поставить или убрать лайк на комментарии - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"❤️ Toggling like on comment {comment_id} by user {user_id}")
        
        # Репозитории
        comments_repo = CommentRepository(session)
        comment_likes_repo = CommentLikeRepository(session)
        
        # Проверяем существование комментария
        comment = await comments_repo.get_by_id(comment_id)
        if not comment:
            raise NotFoundError(f"Comment {comment_id} not found")
        
        now = datetime.utcnow()
        
        # Транзакция
        async with await UnitOfWork.from_session(session) as uow:
            comment_likes_repo.set_transaction(uow._transaction)
            comments_repo.set_transaction(uow._transaction)
            result = await comment_likes_repo.toggle_atomic(
                uow, comment_id, user_id, now
            )
        
        # Инвалидируем кэш комментариев
        post_id = comment['post_id']
        await CommentCache.invalidate(post_id)
        
        # Отправляем уведомление в фоне
        if result['liked'] and str(comment['user_id']) != str(user_id):
            await background_worker.add_high(
                self._send_comment_like_notification,
                user_id=user_id,
                comment_author_id=str(comment['user_id']),
                comment_id=comment_id,
                post_id=post_id
            )
        
        return result
    
    async def _send_comment_like_notification(self, user_id: str, comment_author_id: str, 
                                               comment_id: str, post_id: str):
        """Фоновая задача для уведомления о лайке комментария"""
        try:
            async with RequestContext() as ctx:
                notifications_repo = NotificationRepository(ctx.session)
                
                await notifications_repo.create(
                    user_id=comment_author_id,
                    from_user_id=user_id,
                    notification_type='like',
                    entity_type='comment',
                    entity_id=comment_id,
                    created_at=datetime.utcnow(),
                    extra_data={'post_id': post_id}
                )
        except Exception as e:
            logger.error(f"❌ Failed to send comment like notification: {e}")
    
    async def _format_comment(self, session, row: Dict, author: Author, current_user_id: str,
                              replies: List[Comment] = None,
                              reactions: List[ReactionCount] = None) -> Comment:
        """Форматировать комментарий"""
        created_at = from_timestamp(row['created_at'])
        
        # 👇 формируем превью реакций
        preview = await self.reaction_service.get_reactions_preview(
            session=session,
            entity_type='comment',
            entity_id=row['comment_id'],
            limit=3,
            user_id=current_user_id
        )
        
        return Comment(
            id=row['comment_id'],
            post_id=row['post_id'],
            user_id=row['user_id'],
            parent_comment_id=row.get('parent_comment_id'),
            content=row['content'],
            replies_count=row.get('replies_count') or 0,
            reactions_count=sum(r.count for r in (reactions or [])),
            created_at=created_at.isoformat() + "Z",
            time_ago=time_ago(created_at),
            author=author,
            interactions=Interactions(
                is_liked=False,
                is_owner=str(row['user_id']) == str(current_user_id)
            ),
            replies=replies or [],
            reactions=reactions or [],
            reactions_preview=preview
        )
    
    async def _check_rate_limit(self, action: str, user_id: str, max_requests: int, period: int) -> bool:
        """Проверить rate limit (заглушка, реальный rate limiter в хендлере)"""
        return True
    
    def get_stats(self) -> Dict:
        """Получить статистику сервиса"""
        total = self._stats['cache_hits'] + self._stats['cache_misses']
        return {
            **self._stats,
            'hit_rate': self._stats['cache_hits'] / max(total, 1)
        }
class FollowService:
    """Сервис для работы с подписками - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.post_service = PostService()
    
    async def _send_follow_notification(self, follower_id: str, following_id: str):
        """Фоновая задача для уведомления о подписке"""
        try:
            async with RequestContext() as ctx:
                notifications_repo = NotificationRepository(ctx.session)
                
                await notifications_repo.create(
                    user_id=following_id,
                    from_user_id=follower_id,
                    notification_type='follow',
                    entity_type='user',
                    entity_id=follower_id,
                    created_at=datetime.utcnow()
                )
        except Exception as e:
            logger.error(f"❌ Failed to send follow notification: {e}")
    
    async def toggle(self, session, follower_id: str, following_id: str, action: str = 'toggle') -> Dict:
        """Подписаться или отписаться - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"🔄 Toggling follow: {follower_id} -> {following_id}, action={action}")
        
        if follower_id == following_id:
            raise ValidationError("Cannot follow yourself")
        
        users_repo = UserRepository(session)
        follows_repo = FollowRepository(session)
        
        user = await users_repo.get(following_id)
        if not user:
            raise NotFoundError(f"User {following_id} not found")
        
        is_following = await follows_repo.check(follower_id, following_id)
        
        # Определяем действие
        if action == 'follow':
            should_follow = True
        elif action == 'unfollow':
            should_follow = False
        else:  # action == 'toggle'
            should_follow = not is_following
        
        now = datetime.utcnow()
        
        async with await UnitOfWork.from_session(session) as uow:
            follows_repo.set_transaction(uow._transaction)
            
            if should_follow and not is_following:
                await follows_repo.follow(follower_id, following_id, now)
                await cache.delete(f"interests:{follower_id}")
                
                await send_ws(following_id, {
                    'type': 'notification',
                    'data': {
                        'notification_type': 'follow',
                        'from_user_id': follower_id,
                        'created_at': now.isoformat()
                    }
                })
                
                await background_worker.add_high(
                    self._send_follow_notification,
                    follower_id=follower_id,
                    following_id=following_id
                )
            elif not should_follow and is_following:
                await follows_repo.unfollow(follower_id, following_id)
                await cache.delete(f"interests:{follower_id}")
        
        counts = await follows_repo.get_counts(following_id)
        
        return {
            'following': should_follow,
            'followers_count': counts.get('followers_count', 0),
            'following_count': counts.get('following_count', 0)
        }
    
    async def get_followers_with_cursor(self, session, user_id: str, current_user_id: str,
                            limit: int, cursor: Optional[str] = None) -> Tuple[List[Author], Optional[str]]:
        """Получить подписчиков с курсорной пагинацией - ИСПРАВЛЕНО"""
        logger.info(f"👥 Getting followers for user {user_id} with cursor")
        
        users_repo = UserRepository(session)
        follows_repo = FollowRepository(session)
        
        # Проверяем существование пользователя
        user = await users_repo.get(user_id)
        if not user:
            raise NotFoundError(f"User {user_id} not found")
        
        # Получаем подписчиков
        followers_data, next_cursor = await follows_repo.get_followers_with_cursor(user_id, limit, cursor)
        
        if not followers_data:
            return [], next_cursor
        
        # Собираем ID подписчиков
        follower_ids = []
        for row in followers_data:
            # Пробуем получить ID из разных возможных ключей
            follower_id = row.get('id') or row.get('u.id') or row.get('follower_id')
            if follower_id:
                follower_ids.append(str(follower_id))
        
        # Проверяем, подписан ли текущий пользователь на этих людей
        following_map = {}
        if follower_ids:
            following_map = await follows_repo.check_many_following(current_user_id, follower_ids)
        
        # Форматируем результат
        followers = []
        for row in followers_data:
            try:
                # Получаем ID
                follower_id = row.get('id') or row.get('u.id') or row.get('follower_id')
                if not follower_id:
                    logger.error(f"❌ No follower_id in row: {row}")
                    continue
                
                # Декодируем имя
                first_name = ''
                enc_first = row.get('first_name_encrypted') or row.get('u.first_name_encrypted')
                if enc_first:
                    first_name = safe_b64decode(enc_first)
                
                # Декодируем фамилию
                last_name = ''
                enc_last = row.get('last_name_encrypted') or row.get('u.last_name_encrypted')
                if enc_last:
                    last_name = safe_b64decode(enc_last)
                
                username = row.get('username') or row.get('u.username', '')
                display_name = row.get('display_name') or row.get('u.display_name') or f"{first_name} {last_name}".strip() or username
                is_verified = row.get('is_verified') or row.get('u.is_verified', False)
                
                author = Author(
                    id=str(follower_id),
                    username=username,
                    display_name=display_name,
                    is_verified=is_verified,
                    is_following=following_map.get(str(follower_id), False)
                )
                followers.append(author)
                
            except Exception as e:
                logger.error(f"❌ Error formatting follower: {e}")
                continue
        
        return followers, next_cursor
    
    async def get_following_with_cursor(self, session, user_id: str, current_user_id: str,
                            limit: int, cursor: Optional[str] = None) -> Tuple[List[Author], Optional[str]]:
        """Получить подписки с курсорной пагинацией"""
        logger.info(f"👥 Getting following for user {user_id} with cursor")
        
        users_repo = UserRepository(session)
        follows_repo = FollowRepository(session)
        
        user = await users_repo.get(user_id)
        if not user:
            raise NotFoundError(f"User {user_id} not found")
        
        following_data, next_cursor = await follows_repo.get_following_with_cursor(user_id, limit, cursor)
        
        following_ids = [str(row['id']) for row in following_data]
        
        following_map = await follows_repo.check_many_following(current_user_id, following_ids)
        
        following = []
        for row in following_data:
            first_name = ''
            if row.get('first_name_encrypted'):
                try:
                    first_name = base64.b64decode(row['first_name_encrypted']).decode('utf-8')
                except:
                    first_name = ''
            
            last_name = ''
            if row.get('last_name_encrypted'):
                try:
                    last_name = base64.b64decode(row['last_name_encrypted']).decode('utf-8')
                except:
                    last_name = ''
            
            display_name = row.get('display_name') or f"{first_name} {last_name}".strip() or row.get('username', '')
            
            author = Author(
                id=row['id'],
                username=row.get('username', ''),
                display_name=display_name,
                is_verified=row.get('is_verified', False),
                is_following=following_map.get(str(row['id']), False)
            )
            following.append(author)
        
        return following, next_cursor


class ProfileService:
    """Сервис для работы с профилями - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.post_service = PostService()
    
    async def get(self, session, user_id: str, current_user_id: str) -> Dict:
        """Получить профиль пользователя"""
        logger.info(f"👤 Getting profile for user {user_id}")
        
        cache_key = f"profile:{user_id}:{current_user_id}"
        cached = await UserCache.get(user_id)
        if cached:
            return cached
        
        users_repo = UserRepository(session)
        follows_repo = FollowRepository(session)
        
        user_data = await users_repo.get(user_id)
        if not user_data:
            raise NotFoundError(f"User {user_id} not found")
        
        is_following = await follows_repo.check(current_user_id, user_id)
        stats = await users_repo.get_profile_stats(user_id)
        
        first_name = user_data.get('first_name', '')
        last_name = user_data.get('last_name', '')
        
        username = user_data.get('username', 'user')
        full_name = f"{first_name} {last_name}".strip() or username
        
        profile = {
            "id": user_data['id'],
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "full_name": full_name,
            "is_verified": user_data.get('is_verified', False),
            "is_following": is_following
        }
        
        result = {
            "profile": profile,
            "statistics": {
                "posts": {
                    "total": stats.get('total_posts', 0),
                    "original": stats.get('original_posts', 0),
                    "reposts": stats.get('reposts', 0)
                },
                "interactions": {
                    "likes_given": stats.get('likes_given', 0),
                    "likes_received": stats.get('likes_received', 0),
                    "comments_given": stats.get('comments_given', 0),
                    "comments_received": stats.get('comments_received', 0)
                },
                "followers": stats.get('followers', 0),
                "following": stats.get('following', 0)
            }
        }
        
        await UserCache.set(user_id, result)
        return result
    
    async def update(self, session, user_id: str, data: Dict) -> Dict:
        """Обновить профиль - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"✏️ Updating profile for user {user_id}")
        
        users_repo = UserRepository(session)
        
        update_data = {}
        if 'first_name' in data and data['first_name'] is not None:
            update_data['first_name_encrypted'] = base64.b64encode(data['first_name'].encode()).decode()
        if 'last_name' in data and data['last_name'] is not None:
            update_data['last_name_encrypted'] = base64.b64encode(data['last_name'].encode()).decode()
        
        success = await users_repo.update(user_id, update_data)
        if not success:
            raise DatabaseError("Failed to update profile")
        
        await UserCache.invalidate(user_id)
        
        return await self.get(session, user_id, user_id)


class NotificationService:
    """Сервис для работы с уведомлениями - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
    
    def __init__(self):
        self._stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'db_queries': 0
        }
        logger.info("🔧 NotificationService инициализирован")
    
    async def get_with_cursor(self, session, user_id: str, limit: int,
                              unread_only: bool = False, cursor: Optional[str] = None) -> Dict:
        """
        Получить уведомления с курсорной пагинацией - ОПТИМИЗИРОВАНО
        """
        logger.info(f"🔔 Getting notifications for user {user_id}, unread_only={unread_only}")
        
        # 1. Проверяем кэш для первой страницы
        cache_key = f"notifications:{user_id}:{limit}:{cursor or 'first'}:{unread_only}"
        if not cursor:
            cached = await cache.get(cache_key)
            if cached:
                self._stats['cache_hits'] += 1
                logger.info(f"📦 Notifications cache hit for user {user_id}")
                return cached
        
        self._stats['cache_misses'] += 1
        self._stats['db_queries'] += 1
        
        notifications_repo = NotificationRepository(session)
        users_repo = UserRepository(session)
        
        # 2. Получаем уведомления с пагинацией
        notifications_data, next_cursor = await notifications_repo.get_by_user_with_cursor(
            user_id, limit, cursor, unread_only
        )
        
        has_more = next_cursor is not None
        unread_count = await notifications_repo.count_unread(user_id)
        
        if not notifications_data:
            result = {
                'notifications': [],
                'unread_count': unread_count,
                'next_cursor': None,
                'has_more': False
            }
            if not cursor:
                await cache.set(cache_key, result, ttl=30)
            return result
        
        # 3. Собираем все ID отправителей ОДНИМ запросом
        from_user_ids = list(set([
            str(n.get('from_user_id')) for n in notifications_data 
            if n.get('from_user_id')
        ]))
        
        # 4. Загружаем всех отправителей БАТЧЕМ
        authors = {}
        if from_user_ids:
            users_data = await users_repo.get_many(from_user_ids)
            for uid, data in users_data.items():
                if data:
                    authors[uid] = Author.from_db(data)
        
        # 5. Форматируем уведомления
        notifications = []
        for row in notifications_data:
            created_at_val = row.get('created_at')
            created_at = from_timestamp(created_at_val) if created_at_val else datetime.utcnow()
            
            from_user_id = row.get('from_user_id')
            from_user_dict = None
            if from_user_id and authors.get(str(from_user_id)):
                from_user_dict = authors[str(from_user_id)].dict()
            
            notification = {
                "id": row['notification_id'],
                "from_user_id": from_user_id,
                "type": row['type'],
                "entity_type": row.get('entity_type'),
                "entity_id": row.get('entity_id'),
                "is_read": row.get('is_read', False),
                "created_at": created_at.isoformat() + "Z",
                "time_ago": time_ago(created_at),
                "from_user": from_user_dict,
            }
            
            # Добавляем extra_data если есть
            if row.get('extra_data'):
                try:
                    extra = json.loads(row['extra_data'])
                    notification.update(extra)
                except Exception as e:
                    logger.warning(f"Error parsing extra_data: {e}")
            
            notifications.append(notification)
        
        result = {
            'notifications': notifications,
            'unread_count': unread_count,
            'next_cursor': next_cursor,
            'has_more': has_more
        }
        
        # 6. Сохраняем в кэш
        if not cursor:
            await cache.set(cache_key, result, ttl=30)
        
        return result
    
    async def mark_read(self, session, user_id: str, notification_id: Optional[str] = None,
                        mark_all: bool = False) -> bool:
        """Отметить уведомления как прочитанные"""
        logger.info(f"✅ Marking notifications as read: user={user_id}, id={notification_id}, all={mark_all}")
        
        notifications_repo = NotificationRepository(session)
        
        if mark_all:
            success = await notifications_repo.mark_all_read(user_id)
        elif notification_id:
            success = await notifications_repo.mark_as_read(notification_id, user_id)
        else:
            return False
        
        if success:
            # Инвалидируем кэш
            await cache.delete_pattern(f"notifications:{user_id}:*")
            await cache.delete(f"notifications:unread_count:{user_id}")
        
        return success
class ReportService:
    """Сервис для работы с жалобами - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    async def create(self, session, reporter_id: str, entity_type: str, entity_id: str,
                     reason: str, description: str = "") -> Dict:
        """Создать жалобу - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"🚨 Creating report on {entity_type} {entity_id} by user {reporter_id}")
        
        ReportValidator.validate_create(entity_type, reason)
        
        posts_repo = PostRepository(session)
        comments_repo = CommentRepository(session)
        reports_repo = ReportRepository(session)
        
        if entity_type == 'post':
            entity = await posts_repo.get_by_id(entity_id)
        elif entity_type == 'comment':
            entity = await comments_repo.get_by_id(entity_id)
        else:
            raise ValidationError(f"Invalid entity type: {entity_type}")
        
        if not entity:
            raise NotFoundError(f"{entity_type.capitalize()} {entity_id} not found")
        
        report_id = await reports_repo.create(
            reporter_id, entity_type, entity_id, reason, description
        )
        
        if not report_id:
            raise DatabaseError("Failed to create report")
        
        return {
            "id": report_id,
            "reporter_id": reporter_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "reason": reason,
            "description": description,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
    
    async def list_pending(self, session, limit: int, offset: int) -> List[Dict]:
        """Получить ожидающие жалобы"""
        logger.info(f"📋 Listing pending reports")
        
        reports_repo = ReportRepository(session)
        
        reports_data = await reports_repo.get_pending(limit, offset)
        
        reports = []
        for row in reports_data:
            created_at = from_timestamp(row['created_at'])
            
            report = {
                "id": row['report_id'],
                "reporter_id": row['reporter_id'],
                "entity_type": row['entity_type'],
                "entity_id": row['entity_id'],
                "reason": row['reason'],
                "description": row.get('description'),
                "status": row['status'],
                "created_at": created_at.isoformat() + "Z"
            }
            
            if row.get('reviewed_by'):
                report["reviewed_by"] = row['reviewed_by']
                reviewed_at = from_timestamp(row['reviewed_at'])
                report["reviewed_at"] = reviewed_at.isoformat() + "Z"
            
            reports.append(report)
        
        return reports
    
    async def resolve(self, session, report_id: str, moderator_id: str,
                      action: str, delete_content: bool = False) -> Dict:
        """Разрешить жалобу - ЕДИНАЯ СЕССИЯ"""
        logger.info(f"✅ Resolving report {report_id} by moderator {moderator_id}")
        
        reports_repo = ReportRepository(session)
        posts_repo = PostRepository(session)
        
        report = await reports_repo.get_by_id(report_id)
        if not report:
            raise NotFoundError(f"Report {report_id} not found")
        
        success = await reports_repo.update_status(report_id, action, moderator_id)
        if not success:
            raise DatabaseError("Failed to update report status")
        
        if delete_content:
            if report['entity_type'] == 'post':
                await posts_repo.update_post(report['entity_id'], {'is_deleted': True})
            elif report['entity_type'] == 'comment':
                pass
        
        return {
            "id": report_id,
            "status": action,
            "reviewed_by": moderator_id,
            "reviewed_at": datetime.utcnow().isoformat() + "Z"
        }


class MentionService:
    """Сервис для работы с упоминаниями - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.parser = MentionParser()
        self.post_service = PostService()
        self.comment_service = CommentService()
    
    async def extract_and_create_mentions(self, session, text: str, entity_type: str, 
                                           entity_id: str, mentioned_by_user_id: str) -> List[str]:
        """Извлечь @упоминания из текста и создать записи в БД - ЕДИНАЯ СЕССИЯ"""
        result = self.parser.extract_mentions(text)
        
        if not result.usernames:
            return []
        
        logger.info(f"🔍 Found {len(result.usernames)} potential mentions: {result.usernames}")
        
        user_ids = await self._get_user_ids_by_usernames(session, result.usernames)
        
        if not user_ids:
            return []
        
        mentions_data = []
        for username, user_id in user_ids.items():
            positions = [(s, e) for s, e, u in result.positions if u == username]
            
            for start, end in positions:
                mention_data = {
                    'mentioned_user_id': user_id,
                    'mentioned_by_user_id': mentioned_by_user_id,
                    'username': username,
                    'position_start': start,
                    'position_end': end,
                    'entity_type': entity_type,
                    'entity_id': entity_id
                }
                
                if entity_type == 'post':
                    mention_data['post_id'] = entity_id
                elif entity_type == 'comment':
                    mention_data['comment_id'] = entity_id
                
                mentions_data.append(mention_data)
        
        if mentions_data:
            mentions_repo = MentionRepository(session)
            
            mention_ids = await mentions_repo.create_many(mentions_data)
            logger.info(f"✅ Created {len(mention_ids)} mentions")
            
            for mention_data in mentions_data:
                await background_worker.add_high(
                    self._send_mention_notification,
                    mentioned_user_id=mention_data['mentioned_user_id'],
                    mentioned_by_user_id=mentioned_by_user_id,
                    entity_type=entity_type,
                    entity_id=entity_id
                )
            
            return mention_ids
        
        return []
    
    async def _get_user_ids_by_usernames(self, session, usernames: List[str]) -> Dict[str, str]:
        """Получить ID пользователей по username (batch)"""
        if not usernames:
            return {}
        
        unique_names = list(set(usernames))
        user_map = {}
        
        try:
            placeholders, params = self._generate_placeholders(unique_names, "uname")
            
            declare_parts = []
            for i in range(len(unique_names)):
                declare_parts.append(f"DECLARE $uname_{i} AS Utf8;")
            declare_block = "\n".join(declare_parts)
            
            query = f"""
            {declare_block}
            SELECT username, entity_id 
            FROM usernames 
            WHERE username IN ({placeholders}) AND entity_type = 'user';
            """
            
            repo = BaseRepository(session)
            rows = await repo.execute(query, params)
            
            for row in rows:
                user_map[row['username']] = str(row['entity_id'])
                
            logger.info(f"✅ Found {len(user_map)} matching users for mentions")
            
        except Exception as e:
            logger.error(f"Error getting user IDs by usernames: {e}")
        
        return user_map
    
    async def _send_mention_notification(self, mentioned_user_id: str, 
                                          mentioned_by_user_id: str,
                                          entity_type: str, entity_id: str):
        """Фоновая задача для отправки уведомления об упоминании"""
        try:
            async with RequestContext() as ctx:
                users_repo = UserRepository(ctx.session)
                notifications_repo = NotificationRepository(ctx.session)
                
                mentioned_by = await users_repo.get(mentioned_by_user_id)
                if not mentioned_by:
                    return
                
                username = mentioned_by.get('username', 'unknown')
                now = datetime.utcnow()
                
                # Отправляем WebSocket уведомление
                await send_ws(mentioned_user_id, {
                    'type': 'notification',
                    'data': {
                        'notification_type': 'mention',
                        'from_user_id': mentioned_by_user_id,
                        'from_username': username,
                        'entity_type': entity_type,
                        'entity_id': entity_id,
                        'created_at': now.isoformat(),
                        'mention_text': f'@{username} упомянул вас'
                    }
                })
                
                await notifications_repo.create(
                    user_id=mentioned_user_id,
                    from_user_id=mentioned_by_user_id,
                    notification_type='mention',
                    entity_type=entity_type,
                    entity_id=entity_id,
                    created_at=now,
                    extra_data={
                        'mentioned_by_username': username,
                        'mention_text': f'@{username} упомянул вас'
                    }
                )
                
                logger.info(f"📨 Mention notification sent to {mentioned_user_id}")
                
        except Exception as e:
            logger.error(f"❌ Failed to send mention notification: {e}")
    
    async def get_user_mentions_with_cursor(self, session, user_id: str, current_user_id: str,
                                 limit: int = 50, cursor: Optional[str] = None,
                                 unread_only: bool = False) -> Dict:
        """Получить упоминания пользователя с курсором"""
        logger.info(f"📋 Getting mentions for user {user_id} with cursor")
        
        mentions_repo = MentionRepository(session)
        users_repo = UserRepository(session)
        
        if unread_only:
            mentions_data, next_cursor = await mentions_repo.get_unread_by_user_with_cursor(user_id, limit, cursor)
        else:
            mentions_data, next_cursor = await mentions_repo.get_by_user_with_cursor(user_id, limit, 0, True, cursor)
        
        has_more = next_cursor is not None
        unread_count = await mentions_repo.count_unread(user_id)
        
        mentioned_by_ids = []
        for row in mentions_data:
            mentioned_by_id = row.get('mentioned_by_user_id')
            if mentioned_by_id:
                mentioned_by_ids.append(str(mentioned_by_id))
        
        authors = {}
        if mentioned_by_ids:
            users_data = await users_repo.get_many(mentioned_by_ids)
            for uid, data in users_data.items():
                if data:
                    authors[uid] = Author.from_db(data)
        
        mentions = []
        for row in mentions_data:
            try:
                created_at_val = row.get('created_at')
                if created_at_val:
                    created_at = from_timestamp(created_at_val)
                else:
                    created_at = datetime.utcnow()
                
                mentioned_by_id = row.get('mentioned_by_user_id')
                
                mention = Mention(
                    id=row['mention_id'],
                    mentioned_user_id=row['mentioned_user_id'],
                    mentioned_by_user_id=mentioned_by_id,
                    username=row.get('username', ''),
                    post_id=row.get('post_id'),
                    comment_id=row.get('comment_id'),
                    content_preview=None,
                    position_start=row.get('position_start', 0),
                    position_end=row.get('position_end', 0),
                    is_read=row.get('is_read', False),
                    created_at=created_at.isoformat() + "Z",
                    time_ago=time_ago(created_at),
                    mentioned_by=authors.get(str(mentioned_by_id)) if mentioned_by_id else None,
                    context={}
                )
                mentions.append(mention.dict())
                
            except Exception as e:
                logger.error(f"❌ Error processing mention: {e}")
                continue
        
        result = {
            'mentions': mentions,
            'unread_count': unread_count,
            'next_cursor': next_cursor,
            'has_more': has_more
        }
        
        return result
    
    async def mark_as_read(self, session, mention_id: str, user_id: str) -> bool:
        """Отметить упоминание как прочитанное - ЕДИНАЯ СЕССИЯ"""
        mentions_repo = MentionRepository(session)
        
        success = await mentions_repo.mark_as_read(mention_id, user_id)
        
        if success:
            await cache.delete_pattern(f"mentions:{user_id}:*")
        
        return success
    
    async def mark_all_as_read(self, session, user_id: str) -> bool:
        """Отметить все упоминания как прочитанные - ЕДИНАЯ СЕССИЯ"""
        mentions_repo = MentionRepository(session)
        
        await mentions_repo.mark_all_as_read(user_id)
        await cache.delete_pattern(f"mentions:{user_id}:*")
        return True
    
    async def get_unread_count(self, session, user_id: str) -> int:
        """Получить количество непрочитанных упоминаний"""
        cache_key = f"mentions:unread:{user_id}"
        cached = await cache.get(cache_key)
        if cached is not None:
            return cached
        
        mentions_repo = MentionRepository(session)
        
        count = await mentions_repo.count_unread(user_id)
        await cache.set(cache_key, count, ttl=10)
        return count
    
    def _generate_placeholders(self, values: List[str], prefix: str) -> Tuple[str, Dict]:
        placeholders = []
        params = {}
        for i, value in enumerate(values):
            placeholder = f"${prefix}_{i}"
            placeholders.append(placeholder)
            params[placeholder] = value
        return ", ".join(placeholders), params


# ============================================
# ИСПРАВЛЕННЫЙ КЛАСС RecommendationService
# ============================================

class RecommendationService:
    """Сервис для рекомендации постов - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.post_service = PostService()
    
    # ============================================
    # FOR YOU FEED
    # ============================================
    
    async def _prefetch_next_page(self, session, user_id: str, user_data: Dict,
                                    feed_type: str, limit: int, next_cursor: str) -> None:
        """
        Фоновая предзагрузка следующей страницы ленты
        """
        cache_key = f"prefetch:{feed_type}:{user_id}:{next_cursor}"
        
        # Проверяем, не предзагружена ли уже
        cached = await cache.get(cache_key)
        if cached:
            logger.info(f"📦 Prefetch already exists for {feed_type} user {user_id}")
            return
        
        logger.info(f"🚀 Prefetching next page for {feed_type} feed, user={user_id}, cursor={next_cursor[:30]}...")
        
        try:
            # 🔥 ВАЖНО: передаём session во все методы!
            if feed_type == 'popular':
                result = await self.get_popular_feed_with_cursor(
                    session=session,  # ← передаём session
                    user_id=user_id,
                    user_data=user_data,
                    days=7,
                    limit=limit,
                    cursor=next_cursor
                )
            elif feed_type == 'fresh':
                result = await self.get_fresh_feed_simple_with_cursor(
                    session=session,  # ← передаём session
                    user_id=user_id,
                    user_data=user_data,
                    limit=limit,
                    cursor=next_cursor
                )
            elif feed_type == 'following':
                result = await self.get_following_feed_with_cursor(
                    session=session,  # ← передаём session
                    user_id=user_id,
                    limit=limit,
                    cursor=next_cursor
                )
            elif feed_type == 'for_you':
                result = await self.get_for_you_feed_with_cursor(
                    session=session,  # ← передаём session
                    user_id=user_id,
                    user_data=user_data,
                    limit=limit,
                    cursor=next_cursor
                )
            else:
                logger.warning(f"⚠️ Unknown feed type for prefetch: {feed_type}")
                return
            
            # Сохраняем предзагруженную страницу в кэш на 30 секунд
            await cache.set(cache_key, result, ttl=30)
            logger.info(f"✅ Prefetch completed for {feed_type} feed, user={user_id}")
            
        except Exception as e:
            logger.error(f"❌ Prefetch failed for {feed_type}: {e}", exc_info=True)
    async def get_for_you_feed_with_cursor(self, session, user_id: str, user_data: Dict, 
                                            limit: int = 20, cursor: Optional[str] = None) -> Dict:
        """
        ПЕРСОНАЛИЗИРОВАННАЯ ЛЕНТА "ДЛЯ ВАС" - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ
        """
        logger.info(f"🎯 [OPTIMIZED] Getting for-you feed for user {user_id}, limit={limit}")
        
        # 1. Проверяем кэш для первой страницы
        cache_key = f"for_you:{user_id}:{limit}:{cursor or 'first'}"
        if not cursor:
            cached = await cache.get(cache_key)
            if cached:
                logger.info(f"📦 For-you cache hit for user {user_id}")
                return cached
        
        # 2. Декодируем курсор
        last_score = None
        last_id = None
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                parts = decoded.split(':')
                last_score = float(parts[0])
                last_id = parts[1]
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        # 3. Получаем интересы пользователя (с кэшированием)
        interests = await self._get_user_interests_cached(session, user_id)
        
        # 4. Если нет интересов - возвращаем популярные посты
        if not interests['following_ids'] and not interests['liked_hashtags'] and not interests['liked_authors']:
            logger.info(f"ℹ️ No interests for user {user_id}, falling back to trending")
            popular_result = await self.get_popular_feed_with_cursor(session, user_id, user_data, 7, limit, cursor)
            # 🔥 ИСПРАВЛЕНО: popular_result возвращает 'items', а не 'posts'
            return {
                'items': popular_result.get('items', []),
                'next_cursor': popular_result.get('next_cursor'),
                'has_more': popular_result.get('has_more', False),
                'feed_type': 'for_you'
            }
        
        # 5. Получаем посты на основе интересов
        posts_data, next_cursor = await self._fetch_unified_posts(
            session, user_id, interests, limit, last_score, last_id
        )
        
        # 6. Форматируем посты
        posts = await self.post_service._format_posts(session, posts_data, user_id, user_data)
        
        # 7. Формируем ответ
        result = {
            'items': [p.dict() for p in posts],
            'next_cursor': next_cursor,
            'has_more': next_cursor is not None,
            'feed_type': 'for_you'
        }
        
        # 8. Сохраняем в кэш (только первую страницу)
        if not cursor:
            await cache.set(cache_key, result, ttl=30)
        
        return result
    
    async def _get_user_interests_cached(self, session, user_id: str) -> Dict:
        """Получить интересы пользователя с кэшированием (упрощённая версия)"""
        cache_key = f"user_interests:{user_id}"
        
        cached = await cache.get(cache_key)
        if cached:
            return cached
        
        # Подписки
        following_query = """
        DECLARE $user_id AS Utf8;
        DECLARE $limit AS Uint64;
        SELECT following_id FROM feed_follows WHERE follower_id = $user_id LIMIT $limit;
        """
        repo = BaseRepository(session)
        following_rows = await repo.execute(following_query, {'$user_id': user_id, '$limit': 50})
        following_ids = [row['following_id'] for row in following_rows]
        
        # Временно отключаем хэштеги (проблема с полем hashtag_id)
        liked_hashtags = []
        
        # Авторы из подписок
        liked_authors = following_ids[:5] if following_ids else []
        
        interests = {
            'following_ids': following_ids,
            'liked_hashtags': liked_hashtags,
            'liked_authors': liked_authors
        }
        
        await cache.set(cache_key, interests, ttl=600)
        return interests
    
    async def _fetch_unified_posts(self, session, user_id: str, interests: Dict,
                                     limit: int, last_score: Optional[float] = None,
                                     last_id: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """Упрощённый запрос для получения постов"""
        
        following_ids = interests.get('following_ids', [])
        hashtag_ids = interests.get('liked_hashtags', [])
        author_ids = interests.get('liked_authors', [])
        
        # Если нет подписок - используем только тренды
        if not following_ids and not hashtag_ids and not author_ids:
            week_ago = to_timestamp(datetime.utcnow() - timedelta(days=7))
            posts_repo = PostRepository(session)
            return await posts_repo.get_trending_with_cursor(week_ago, limit, None)
        
        # Формируем условия для WHERE
        conditions = []
        params = {'$limit': limit + 1}
        
        if following_ids:
            placeholders = ', '.join([f"'{{uid}}'".format(uid=uid) for uid in following_ids[:50]])
            conditions.append(f"p.user_id IN ({placeholders})")
        
        if hashtag_ids:
            hashtag_placeholders = ', '.join([f"'{{hid}}'".format(hid=hid) for hid in hashtag_ids[:10]])
            conditions.append(f"EXISTS (SELECT 1 FROM feed_post_hashtags ph WHERE ph.post_id = p.post_id AND ph.hashtag_id IN ({hashtag_placeholders}))")
        
        if author_ids:
            author_placeholders = ', '.join([f"'{{aid}}'".format(aid=aid) for aid in author_ids[:5]])
            conditions.append(f"p.user_id IN ({author_placeholders})")
        
        where_clause = " OR ".join(conditions)
        
        # Пагинация
        pagination = ""
        if last_score is not None and last_id:
            pagination = f"AND (score < {last_score} OR (score = {last_score} AND p.post_id < '{last_id}'))"
        
        week_ago_ts = to_timestamp(datetime.utcnow() - timedelta(days=7))
        
        query = f"""
        DECLARE $limit AS Uint64;
        $week_ago = CAST({week_ago_ts} AS Timestamp);

        SELECT
            p.post_id, p.user_id, p.title, p.content, p.content_preview, p.media_urls,
            p.comments_count, p.reposts_count, p.views_count, p.bookmarks_count,
            p.reactions_count,
            p.is_repost, p.original_post_id, p.repost_comment, p.is_pinned, p.is_edited,
            p.visibility, p.language, p.sentiment_score, p.reading_time_minutes,
            p.created_at, p.updated_at, p.published_at, p.scheduled_for,
            p.is_deleted, p.deleted_at, p.original_author, p.original_post,
            (CAST(p.reactions_count AS Double) * 2 +
             CAST(p.comments_count AS Double) * 3 +
             CAST(p.reposts_count AS Double) * 2) as score
        FROM feed_posts p
        WHERE ({where_clause})
          AND p.is_deleted = false
          AND p.visibility = 'public'
          AND p.created_at >= $week_ago
          {pagination}
        ORDER BY score DESC, p.post_id DESC
        LIMIT $limit;
        """
        
        try:
            repo = BaseRepository(session)
            rows = await repo.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                posts = rows[:-1]
                last_post = rows[-2]
                score = last_post.get('score', 0)
                next_cursor = base64.b64encode(f"{score}:{last_post['post_id']}".encode()).decode()
            else:
                posts = rows
                next_cursor = None
            
            return posts, next_cursor
            
        except Exception as e:
            logger.error(f"Error in _fetch_unified_posts: {e}")
            return [], None
    
    

    async def _get_following_posts_with_cursor(self, session, user_id: str, following_ids: List[str],
                                                limit: int, last_created_at: Optional[int] = None,
                                                last_id: Optional[str] = None) -> Tuple[List[Dict], bool]:
        """Получить посты от подписок с курсором"""
        if not following_ids:
            return [], False

        placeholders = ', '.join([f"'{uid}'" for uid in following_ids[:20]])

        if last_created_at and last_id:
            query = f"""
            SELECT p.*
            FROM feed_posts p
            WHERE p.user_id IN ({placeholders})
              AND p.is_deleted = false
              AND p.visibility = 'public'
              AND (p.created_at < {last_created_at} OR 
                   (p.created_at = {last_created_at} AND p.post_id < '{last_id}'))
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit + 1};
            """
        else:
            query = f"""
            SELECT p.*
            FROM feed_posts p
            WHERE p.user_id IN ({placeholders})
              AND p.is_deleted = false
              AND p.visibility = 'public'
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit + 1};
            """

        try:
            repo = BaseRepository(session)
            rows = await repo.execute(query)
            has_more = len(rows) > limit
            posts = rows[:limit] if has_more else rows
            return posts, has_more
        except Exception as e:
            logger.error(f"Error getting following posts: {e}")
            return [], False

    async def _get_hashtag_posts_with_cursor(self, session, user_id: str, hashtag_ids: List[str],
                                              limit: int, last_created_at: Optional[int] = None,
                                              last_id: Optional[str] = None) -> Tuple[List[Dict], bool]:
        """Получить посты с любимыми хэштегами с курсором"""
        if not hashtag_ids:
            return [], False

        placeholders = ', '.join([f"'{hid}'" for hid in hashtag_ids[:10]])

        if last_created_at and last_id:
            query = f"""
            SELECT DISTINCT p.*
            FROM feed_posts p
            JOIN feed_post_hashtags ph ON p.post_id = ph.post_id
            WHERE ph.hashtag_id IN ({placeholders})
              AND p.user_id != '{user_id}'
              AND p.is_deleted = false
              AND p.visibility = 'public'
              AND (p.created_at < {last_created_at} OR 
                   (p.created_at = {last_created_at} AND p.post_id < '{last_id}'))
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit + 1};
            """
        else:
            query = f"""
            SELECT DISTINCT p.*
            FROM feed_posts p
            JOIN feed_post_hashtags ph ON p.post_id = ph.post_id
            WHERE ph.hashtag_id IN ({placeholders})
              AND p.user_id != '{user_id}'
              AND p.is_deleted = false
              AND p.visibility = 'public'
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit + 1};
            """

        try:
            repo = BaseRepository(session)
            rows = await repo.execute(query)
            has_more = len(rows) > limit
            posts = rows[:limit] if has_more else rows
            return posts, has_more
        except Exception as e:
            logger.error(f"Error getting hashtag posts: {e}")
            return [], False

    async def _get_author_posts_with_cursor(self, session, user_id: str, author_ids: List[str],
                                             limit: int, last_created_at: Optional[int] = None,
                                             last_id: Optional[str] = None) -> Tuple[List[Dict], bool]:
        """Получить посты от любимых авторов с курсором"""
        if not author_ids:
            return [], False

        placeholders = ', '.join([f"'{aid}'" for aid in author_ids[:5]])

        if last_created_at and last_id:
            query = f"""
            SELECT p.*
            FROM feed_posts p
            WHERE p.user_id IN ({placeholders})
              AND p.is_deleted = false
              AND p.visibility = 'public'
              AND (p.created_at < {last_created_at} OR 
                   (p.created_at = {last_created_at} AND p.post_id < '{last_id}'))
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit + 1};
            """
        else:
            query = f"""
            SELECT p.*
            FROM feed_posts p
            WHERE p.user_id IN ({placeholders})
              AND p.is_deleted = false
              AND p.visibility = 'public'
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit + 1};
            """

        try:
            repo = BaseRepository(session)
            rows = await repo.execute(query)
            has_more = len(rows) > limit
            posts = rows[:limit] if has_more else rows
            return posts, has_more
        except Exception as e:
            logger.error(f"Error getting author posts: {e}")
            return [], False

    async def _get_trending_posts_with_cursor(self, session, user_id: str, limit: int,
                                               last_created_at: Optional[int] = None,
                                               last_id: Optional[str] = None) -> Tuple[List[Dict], bool]:
        """Получить популярные посты с курсором"""
        week_ago = datetime.utcnow() - timedelta(days=7)
        week_ago_ts = int(week_ago.timestamp() * 1e6)

        if last_created_at and last_id:
            query = f"""
            SELECT *
            FROM feed_posts
            WHERE created_at >= {week_ago_ts}
              AND is_deleted = false
              AND visibility = 'public'
              AND user_id != '{user_id}'
              AND (created_at < {last_created_at} OR 
                   (created_at = {last_created_at} AND post_id < '{last_id}'))
            ORDER BY created_at DESC, post_id DESC
            LIMIT {limit + 1};
            """
        else:
            query = f"""
            SELECT *
            FROM feed_posts
            WHERE created_at >= {week_ago_ts}
              AND is_deleted = false
              AND visibility = 'public'
              AND user_id != '{user_id}'
            ORDER BY created_at DESC, post_id DESC
            LIMIT {limit + 1};
            """

        try:
            repo = BaseRepository(session)
            rows = await repo.execute(query)
            has_more = len(rows) > limit
            posts = rows[:limit] if has_more else rows
            return posts, has_more
        except Exception as e:
            logger.error(f"Error getting trending posts: {e}")
            return [], False
    # ============================================
    # FOLLOWING FEED
    # ============================================
    
    async def get_following_feed_with_cursor(self, session, user_id: str, 
                                              limit: int = 20, cursor: Optional[str] = None) -> Dict:
        """Лента подписок с курсорной пагинацией"""
        logger.info(f"👥 Getting following feed for user {user_id}, limit={limit}, cursor={cursor}")
        
        # Проверяем кэш (только для первой страницы)
        if not cursor:
            cached = await FeedCache.get(user_id, 'following')
            if cached and isinstance(cached, dict):
                return cached
        
        follows_repo = FollowRepository(session)
        posts_repo = PostRepository(session)
        
        # Получаем ID подписок с пагинацией
        following_ids, next_following_cursor = await follows_repo.get_following_ids_with_cursor(
            user_id, limit=100, cursor=cursor
        )
        
        if not following_ids:
            return {
                'items': [],
                'next_cursor': None,
                'has_more': False
            }
        
        # Получаем посты от подписок с курсором
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        # Создаем плейсхолдеры для IN
        placeholders = ', '.join([f"'{uid}'" for uid in following_ids[:100]])
        
        if last_created_at and last_id:
            query = f"""
            SELECT p.* 
            FROM feed_posts p
            WHERE p.user_id IN ({placeholders})
              AND p.is_deleted = false
              AND p.visibility = 'public'
              AND (p.created_at < {last_created_at} OR 
                   (p.created_at = {last_created_at} AND p.post_id < '{last_id}'))
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit + 1};
            """
        else:
            query = f"""
            SELECT p.* 
            FROM feed_posts p
            WHERE p.user_id IN ({placeholders})
              AND p.is_deleted = false
              AND p.visibility = 'public'
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit + 1};
            """
        
        results = await posts_repo.execute(query)
        
        has_more = len(results) > limit
        if has_more:
            current_posts = results[:-1]
            last_post = results[-2]
            next_cursor = base64.b64encode(
                f"{last_post['created_at']}:{last_post['post_id']}".encode()
            ).decode()
        else:
            current_posts = results
            next_cursor = None
        
        posts = await self.post_service._format_posts(session, current_posts, user_id, {})
        
        response = {
            'items': [p.dict() for p in posts],
            'next_cursor': next_cursor,
            'has_more': has_more
        }
        if not cursor:
            await FeedCache.set(user_id, 'following', response, ttl=60)
        return response

    # ============================================
    # POPULAR FEED
    # ============================================
    
    async def get_popular_feed_with_cursor(self, session, user_id: str, user_data: Dict,
                                            days: int = 7, limit: int = 20, cursor: Optional[str] = None) -> Dict:
        """
        ЛЕНТА ПОПУЛЯРНЫХ ПОСТОВ - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ
        """
        logger.info(f"📈 Getting popular feed for user {user_id}, days={days}, limit={limit}")
        
        # Проверяем кэш для первой страницы
        cache_key = f"popular_feed:{days}:{limit}:{cursor or 'first'}"
        if not cursor:
            cached = await cache.get(cache_key)
            if cached:
                logger.info(f"📦 Popular feed cache hit")
                return cached
        
        # Декодируем курсор
        last_score = None
        last_id = None
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                parts = decoded.split(':')
                last_score = float(parts[0])
                last_id = parts[1]
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        # Получаем популярные посты
        week_ago_ts = to_timestamp(datetime.utcnow() - timedelta(days=days))
        
        # 🔥 ВАЖНО: передаём session в репозиторий!
        posts_repo = PostRepository(session)  # ← передаём session
        
        if last_score is not None and last_id:
            query = f"""
            DECLARE $week_ago AS Timestamp;
            DECLARE $limit AS Uint64;
            DECLARE $last_score AS Double;
            DECLARE $last_id AS Utf8;
            
            SELECT
                post_id, user_id, title, content, content_preview, media_urls,
                comments_count, reposts_count, views_count, bookmarks_count,
                reactions_count,
                is_repost, original_post_id, repost_comment, is_pinned, is_edited,
                visibility, language, sentiment_score, reading_time_minutes,
                created_at, updated_at, published_at, scheduled_for, is_deleted, deleted_at,
                original_author, original_post,
                (CAST(reactions_count AS Double) * 2 +
                 CAST(comments_count AS Double) * 3 +
                 CAST(reposts_count AS Double) * 2) as score
            FROM feed_posts
            WHERE created_at >= $week_ago
                AND is_deleted = false
                AND visibility = 'public'
                AND ((CAST(reactions_count AS Double) * 2 +
                      CAST(comments_count AS Double) * 3 +
                      CAST(reposts_count AS Double) * 2) < $last_score OR
                     ((CAST(reactions_count AS Double) * 2 +
                       CAST(comments_count AS Double) * 3 +
                       CAST(reposts_count AS Double) * 2) = $last_score AND post_id < $last_id))
            ORDER BY score DESC, post_id DESC
            LIMIT {limit + 1};
            """
            params = {
                '$week_ago': week_ago_ts,
                '$limit': limit,
                '$last_score': last_score,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $week_ago AS Timestamp;
            DECLARE $limit AS Uint64;
            
            SELECT
                post_id, user_id, title, content, content_preview, media_urls,
                comments_count, reposts_count, views_count, bookmarks_count,
                reactions_count,
                is_repost, original_post_id, repost_comment, is_pinned, is_edited,
                visibility, language, sentiment_score, reading_time_minutes,
                created_at, updated_at, published_at, scheduled_for, is_deleted, deleted_at,
                original_author, original_post,
                (CAST(reactions_count AS Double) * 2 +
                 CAST(comments_count AS Double) * 3 +
                 CAST(reposts_count AS Double) * 2) as score
            FROM feed_posts
            WHERE created_at >= $week_ago
                AND is_deleted = false
                AND visibility = 'public'
            ORDER BY score DESC, post_id DESC
            LIMIT {limit + 1};
            """
            params = {'$week_ago': week_ago_ts, '$limit': limit}
        
        try:
            rows = await posts_repo.execute(query, params)  # ← используем репозиторий с session
            
            has_more = len(rows) > limit
            if has_more:
                posts_data = rows[:-1]
                last_post = rows[-2]
                score = last_post.get('score', 0)
                next_cursor = base64.b64encode(f"{score}:{last_post['post_id']}".encode()).decode()
            else:
                posts_data = rows
                next_cursor = None
            
            # 🔥 ВАЖНО: передаём session в _format_posts
            posts = await self.post_service._format_posts(session, posts_data, user_id, user_data)
            
            result = {
                'items': [p.dict() for p in posts],
                'next_cursor': next_cursor,
                'has_more': has_more,
                'feed_type': 'popular'
            }
            
            # Сохраняем в кэш
            if not cursor:
                await cache.set(cache_key, result, ttl=60)
            
            return result
            
        except Exception as e:
            logger.error(f"Error in popular feed: {e}")
            return {
                'items': [],
                'next_cursor': None,
                'has_more': False,
                'feed_type': 'popular'
            }
    
    # ============================================
    # FRESH FEED
    # ============================================
    async def get_fresh_feed_simple_with_cursor(self, session, user_id: str, user_data: Dict,
                                                  limit: int = 20, cursor: Optional[str] = None) -> Dict:
        """
        ЛЕНТА 'СВЕЖЕЕ' - ПРОСТАЯ ВЕРСИЯ (без рекомендаций)
        Максимальная производительность
        """
        logger.info(f"🆕 Getting fresh feed (simple) for user {user_id}, limit={limit}")
        
        cache_key = f"fresh_feed_simple:{user_id}:{limit}:{cursor or 'first'}"
        if not cursor:
            cached = await cache.get(cache_key)
            if cached:
                return cached
        
        posts_repo = PostRepository(session)
        posts_data, next_cursor = await posts_repo.get_feed_with_cursor(limit, cursor)
        
        posts = await self.post_service._format_posts(session, posts_data, user_id, user_data)
        
        result = {
            'items': [p.dict() for p in posts],
            'next_cursor': next_cursor,
            'has_more': next_cursor is not None,
            'feed_type': 'fresh'
        }
        
        if not cursor:
            await cache.set(cache_key, result, ttl=30)
        
        return result
    async def get_fresh_feed_with_cursor(self, session, user_id: str, user_data: Dict, 
                                          limit: int = 20, cursor: Optional[str] = None) -> Dict:
        """
        ЛЕНТА 'СВЕЖЕЕ' - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ
        - Использует индекс idx_feed_posts_created_at
        - Кэширование результатов
        - Быстрая курсорная пагинация
        """
        logger.info(f"🆕 Getting fresh feed for user {user_id}, limit={limit}, cursor={cursor}")
        
        # 1. Проверяем кэш для первой страницы
        cache_key = f"fresh_feed:{user_id}:{limit}:{cursor or 'first'}"
        if not cursor:
            cached = await cache.get(cache_key)
            if cached:
                logger.info(f"📦 Fresh feed cache hit for user {user_id}")
                return cached
        
        # 2. Декодируем курсор (created_at:post_id)
        last_created_at = None
        last_id = None
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                parts = decoded.split(':')
                last_created_at = int(parts[0])
                last_id = parts[1]
                logger.info(f"📌 Decoded cursor: created_at={last_created_at}, post_id={last_id}")
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        # 3. Получаем свежие посты с пагинацией
        posts_repo = PostRepository(session)
        
        if last_created_at and last_id:
            query = f"""
            DECLARE $limit AS Uint64;
            DECLARE $last_created_at AS Timestamp;
            DECLARE $last_id AS Utf8;
            
            SELECT
                post_id, user_id, title, content, content_preview, media_urls,
                comments_count, reposts_count, views_count, bookmarks_count,
                reactions_count,
                is_repost, original_post_id, repost_comment, is_pinned, is_edited,
                visibility, language, sentiment_score, reading_time_minutes,
                created_at, updated_at, published_at, scheduled_for, is_deleted, deleted_at,
                original_author, original_post
            FROM feed_posts
            WHERE is_deleted = false
                AND visibility = 'public'
                AND (created_at < $last_created_at OR
                     (created_at = $last_created_at AND post_id < $last_id))
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {
                '$limit': limit,
                '$last_created_at': last_created_at,
                '$last_id': last_id
            }
        else:
            query = f"""
            DECLARE $limit AS Uint64;
            
            SELECT
                post_id, user_id, title, content, content_preview, media_urls,
                comments_count, reposts_count, views_count, bookmarks_count,
                reactions_count,
                is_repost, original_post_id, repost_comment, is_pinned, is_edited,
                visibility, language, sentiment_score, reading_time_minutes,
                created_at, updated_at, published_at, scheduled_for, is_deleted, deleted_at,
                original_author, original_post
            FROM feed_posts
            WHERE is_deleted = false
                AND visibility = 'public'
            ORDER BY created_at DESC, post_id DESC
            LIMIT $limit + 1;
            """
            params = {'$limit': limit}
        
        try:
            rows = await posts_repo.execute(query, params)
            
            has_more = len(rows) > limit
            if has_more:
                posts_data = rows[:-1]
                last_post = rows[-2]
                
                # Получаем created_at для курсора
                created_at_val = last_post.get('created_at')
                if hasattr(created_at_val, 'timestamp'):
                    created_at_ts = int(created_at_val.timestamp() * 1_000_000)
                elif isinstance(created_at_val, (int, float)):
                    created_at_ts = int(created_at_val)
                else:
                    try:
                        dt = datetime.fromisoformat(str(created_at_val).replace('Z', '+00:00'))
                        created_at_ts = int(dt.timestamp() * 1_000_000)
                    except:
                        created_at_ts = int(time.time() * 1_000_000)
                
                next_cursor = base64.b64encode(
                    f"{created_at_ts}:{last_post['post_id']}".encode()
                ).decode()
            else:
                posts_data = rows
                next_cursor = None
            
            # 4. Форматируем посты
            posts = await self.post_service._format_posts(session, posts_data, user_id, user_data)
            
            # 5. Получаем рекомендации пользователей (только для первой страницы)
            suggestions = []
            if not cursor and posts:
                try:
                    user_rec_service = UserRecommendationService()
                    suggestions_result = await user_rec_service.get_suggestions(
                        session=session,
                        current_user_id=user_id,
                        limit=5,
                        offset=0
                    )
                    suggestions = suggestions_result.get('users', [])
                except Exception as e:
                    logger.error(f"Error getting suggestions: {e}")
            
            # 6. Формируем ленту с вставками рекомендаций (каждые 10 постов)
            feed_items = []
            for i, post in enumerate(posts):
                feed_items.append(post.dict())
                
                # Вставляем рекомендации после каждого 10-го поста
                if (i + 1) % 10 == 0 and suggestions:
                    feed_items.append({
                        'type': 'suggestions',
                        'data': suggestions[:3]  # не более 3 рекомендаций
                    })
            
            # 7. Формируем ответ
            result = {
                'items': feed_items,
                'next_cursor': next_cursor,
                'has_more': has_more,
                'feed_type': 'fresh'
            }
            
            # 8. Сохраняем в кэш (только первую страницу)
            if not cursor:
                await cache.set(cache_key, result, ttl=30)  # 30 секунд кэш
            
            return result
            
        except Exception as e:
            logger.error(f"Error in fresh feed: {e}", exc_info=True)
            return {
                'items': [],
                'next_cursor': None,
                'has_more': False,
                'feed_type': 'fresh'
            }
    
    # ============================================
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # ============================================
    
    def _encode_cursor(self, post: Dict) -> Optional[str]:
        """Закодировать курсор из поста"""
        if not post:
            logger.warning(f"🎯 [FOR-YOU-DEBUG] _encode_cursor: post is None")
            return None
        
        created_at = post.get('created_at')
        post_id = post.get('post_id')
        
        logger.info(f"🎯 [FOR-YOU-DEBUG] _encode_cursor: post_id={post_id}, created_at={created_at} (type={type(created_at)})")
        
        if not created_at or not post_id:
            logger.warning(f"🎯 [FOR-YOU-DEBUG] _encode_cursor: missing created_at or post_id")
            return None
        
        try:
            # Конвертируем в timestamp если это объект datetime
            if hasattr(created_at, 'timestamp'):
                ts = int(created_at.timestamp() * 1e6)
                logger.info(f"🎯 [FOR-YOU-DEBUG] _encode_cursor: converted datetime to timestamp: {ts}")
            elif isinstance(created_at, (int, float)):
                ts = int(created_at)
                logger.info(f"🎯 [FOR-YOU-DEBUG] _encode_cursor: using integer timestamp: {ts}")
            else:
                # Пробуем распарсить строку
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(str(created_at).replace('Z', '+00:00'))
                    ts = int(dt.timestamp() * 1e6)
                    logger.info(f"🎯 [FOR-YOU-DEBUG] _encode_cursor: parsed string to timestamp: {ts}")
                except Exception as e:
                    logger.error(f"🎯 [FOR-YOU-DEBUG] _encode_cursor: failed to parse created_at: {e}")
                    ts = int(time.time() * 1e6)
            
            cursor_str = f"{ts}:{post_id}"
            encoded = base64.b64encode(cursor_str.encode()).decode()
            logger.info(f"🎯 [FOR-YOU-DEBUG] _encode_cursor: cursor_str={cursor_str}, encoded={encoded[:30]}...")
            return encoded
            
        except Exception as e:
            logger.error(f"🎯 [FOR-YOU-DEBUG] _encode_cursor: error: {e}", exc_info=True)
            return None
    
    async def _get_user_interests(self, session, user_id: str) -> Dict:
        """Интересы пользователя — кэшируются 30 минут, запросы параллельны."""
        _INTERESTS_TTL = 1800  # 30 минут
        cache_key = f"interests:{user_id}"

        cached = await cache.get(cache_key)
        if cached:
            logger.info(f"📦 Interests cache hit for {user_id}")
            return cached

        following_ids: List[str] = []
        liked_hashtags: List[str] = []
        liked_authors: List[str] = []

        async def _fetch_following() -> List[str]:
            try:
                repo = BaseRepository(session)
                rows = await repo.execute(
                    f"SELECT following_id FROM feed_follows WHERE follower_id = '{user_id}' LIMIT 100",
                    {}
                )
                return [r['following_id'] for r in rows] if rows else []
            except Exception as e:
                logger.error(f"Interests following error: {e}")
                return []

        async def _fetch_hashtags() -> List[str]:
            try:
                repo = BaseRepository(session)
                rows = await repo.execute(f"""
                    SELECT DISTINCT ph.hashtag_id AS hashtag_id
                    FROM feed_reactions r
                    JOIN feed_posts p ON r.entity_id = p.post_id
                    JOIN feed_post_hashtags ph ON p.post_id = ph.post_id
                    WHERE r.user_id = '{user_id}'
                    LIMIT 10
                """, {})
                return [r.get('hashtag_id') for r in rows if r.get('hashtag_id')] if rows else []
            except Exception as e:
                logger.error(f"Interests hashtags error: {e}")
                return []

        async def _fetch_authors() -> List[str]:
            try:
                repo = BaseRepository(session)
                rows = await repo.execute(f"""
                    SELECT DISTINCT p.user_id as author_id
                    FROM feed_reactions r
                    JOIN feed_posts p ON r.entity_id = p.post_id
                    WHERE r.user_id = '{user_id}'
                    LIMIT 5
                """, {})
                return [r['author_id'] for r in rows] if rows else []
            except Exception as e:
                logger.error(f"Interests authors error: {e}")
                return []

        following_ids, liked_hashtags, liked_authors = await asyncio.gather(
            _fetch_following(),
            _fetch_hashtags(),
            _fetch_authors(),
        )

        logger.info(f"📊 Interests: following={len(following_ids)}, hashtags={len(liked_hashtags)}, authors={len(liked_authors)}")

        result = {
            'following_ids': following_ids,
            'liked_hashtags': liked_hashtags,
            'liked_authors': liked_authors,
            'recent_interactions': [],
        }
        await cache.set(cache_key, result, _INTERESTS_TTL)
        return result
    
    
    
    async def _get_interest_based_posts(self, session, user_id: str, interests: Dict, limit: int) -> List[Dict]:
        """Получить посты на основе интересов"""
        if not interests['liked_hashtags'] and not interests['liked_authors']:
            return []
        
        all_posts = []
        seen = set()
        
        # Посты с любимыми хэштегами
        if interests['liked_hashtags']:
            hashtag_ids = interests['liked_hashtags'][:5]
            placeholders = ', '.join([f"'{hid}'" for hid in hashtag_ids])
            
            query = f"""
            SELECT DISTINCT p.*
            FROM feed_posts p
            JOIN feed_post_hashtags ph ON p.post_id = ph.post_id
            WHERE ph.hashtag_id IN ({placeholders})
              AND p.user_id != '{user_id}'
              AND p.is_deleted = false
              AND p.visibility = 'public'
            ORDER BY p.created_at DESC
            LIMIT {limit * 2};
            """
            
            try:
                repo = BaseRepository(session)
                posts = await repo.execute(query)
                for post in posts:
                    if post['post_id'] not in seen:
                        seen.add(post['post_id'])
                        post['relevance_score'] = 10
                        all_posts.append(post)
            except Exception as e:
                logger.error(f"Error getting hashtag posts: {e}")
        
        # Посты от любимых авторов
        if interests['liked_authors'] and len(all_posts) < limit * 2:
            author_ids = interests['liked_authors'][:3]
            placeholders = ', '.join([f"'{aid}'" for aid in author_ids])
            
            query = f"""
            SELECT p.*
            FROM feed_posts p
            WHERE p.user_id IN ({placeholders})
              AND p.is_deleted = false
              AND p.visibility = 'public'
            ORDER BY p.created_at DESC
            LIMIT {limit};
            """
            
            try:
                repo = BaseRepository(session)
                posts = await repo.execute(query)
                for post in posts:
                    if post['post_id'] not in seen:
                        seen.add(post['post_id'])
                        post['relevance_score'] = 15
                        all_posts.append(post)
            except Exception as e:
                logger.error(f"Error getting author posts: {e}")
        
        # Сортируем по релевантности
        all_posts.sort(key=lambda x: (-x.get('relevance_score', 0), -x.get('created_at', 0)))
        
        return all_posts[:limit]
    
    async def _get_fresh_posts_with_cursor(self, session, user_id: str, limit: int, cursor: Optional[str] = None) -> List[Dict]:
        """Получить свежие посты"""
        last_created_at = None
        last_id = None
        
        if cursor:
            try:
                decoded = base64.b64decode(cursor).decode('utf-8')
                ts_str, last_id = decoded.split(':')
                last_created_at = int(ts_str)
            except Exception as e:
                logger.warning(f"⚠️ Invalid cursor: {e}")
        
        if last_created_at and last_id:
            query = f"""
            SELECT p.*
            FROM feed_posts p
            WHERE p.is_deleted = false 
              AND p.visibility = 'public'
              AND p.user_id != '{user_id}'
              AND (p.created_at < {last_created_at} OR 
                   (p.created_at = {last_created_at} AND p.post_id < '{last_id}'))
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit};
            """
        else:
            query = f"""
            SELECT p.*
            FROM feed_posts p
            WHERE p.is_deleted = false 
              AND p.visibility = 'public'
              AND p.user_id != '{user_id}'
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT {limit};
            """
        
        try:
            repo = BaseRepository(session)
            return await repo.execute(query)
        except Exception as e:
            logger.error(f"Error getting fresh posts: {e}")
            return []


class UserRecommendationService:
    """Сервис для рекомендаций пользователей - ВСЕ МЕТОДЫ ПОЛУЧАЮТ session"""
    
    def __init__(self):
        self.user_cache = UserCache()
    
    async def get_popular_users(self, session, current_user_id: str, limit: int = 20, offset: int = 0) -> Dict:
        """
        Получить популярных пользователей (по количеству подписчиков)
        """
        logger.info(f"⭐ Getting popular users for user {current_user_id}")
        
        cache_key = f"users:popular:{current_user_id}:{limit}:{offset}"
        cached = await cache.get(cache_key)
        if cached:
            return cached
        
        query = """
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        SELECT 
            u.id,
            u.username,
            u.first_name_encrypted,
            u.last_name_encrypted,
            u.is_verified,
            COALESCE(f.followers_count, 0) as followers_count,
            COALESCE(p.posts_count, 0) as posts_count
        FROM users u
        LEFT JOIN (
            SELECT following_id, COUNT(*) as followers_count
            FROM feed_follows
            GROUP BY following_id
        ) f ON u.id = f.following_id
        LEFT JOIN (
            SELECT user_id, COUNT(*) as posts_count
            FROM feed_posts
            WHERE is_deleted = false
            GROUP BY user_id
        ) p ON u.id = p.user_id
        ORDER BY followers_count DESC, posts_count DESC
        LIMIT $limit OFFSET $offset;
        """
        
        params = {
            '$limit': limit,
            '$offset': offset
        }
        
        try:
            users_repo = UserRepository(session)
            rows = await users_repo.execute(query, params)
            
            users = []
            for row in rows:
                # Получаем ID (с учетом возможных префиксов)
                user_id = row.get('id') or row.get('u.id')
                if not user_id:
                    logger.warning(f"⚠️ Row missing id, keys: {list(row.keys())}")
                    continue
                
                # Декодируем имя
                first_name = ''
                enc_first = row.get('first_name_encrypted') or row.get('u.first_name_encrypted')
                if enc_first:
                    first_name = safe_b64decode(enc_first)
                
                # Декодируем фамилию
                last_name = ''
                enc_last = row.get('last_name_encrypted') or row.get('u.last_name_encrypted')
                if enc_last:
                    last_name = safe_b64decode(enc_last)
                
                username = row.get('username') or row.get('u.username', '')
                is_verified = row.get('is_verified') or row.get('u.is_verified', False)
                
                # Проверяем, подписан ли текущий пользователь
                is_following = False
                if current_user_id != user_id:
                    follows_repo = FollowRepository(session)
                    is_following = await follows_repo.check(current_user_id, user_id)
                
                user = {
                    'id': user_id,
                    'username': username,
                    'first_name': first_name,
                    'last_name': last_name,
                    'is_verified': is_verified,
                    'followers_count': row.get('followers_count', 0),
                    'posts_count': row.get('posts_count', 0),
                    'is_following': is_following
                }
                users.append(user)
            
            has_more = len(users) == limit
            
            result = {
                'users': users,
                'has_more': has_more,
                'pagination': {
                    'limit': limit,
                    'offset': offset,
                    'has_more': has_more
                }
            }
            
            await cache.set(cache_key, result, ttl=300)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error getting popular users: {e}")
            return {
                'users': [], 
                'has_more': False, 
                'pagination': {
                    'limit': limit, 
                    'offset': offset, 
                    'has_more': False
                }
            }
    
    async def get_suggestions(self, session, current_user_id: str, limit: int = 20, offset: int = 0) -> Dict:
        """
        Получить рекомендованных пользователей для подписки
        (пользователи с большим количеством подписчиков, на которых еще не подписан)
        """
        logger.info(f"💡 Getting user suggestions for {current_user_id}")
        
        cache_key = f"users:suggestions:{current_user_id}:{limit}:{offset}"
        cached = await cache.get(cache_key)
        if cached:
            return cached
        
        query = f"""
        DECLARE $current_user_id AS Utf8;
        DECLARE $limit AS Uint64;
        DECLARE $offset AS Uint64;
        
        $followed_users = (
            SELECT following_id 
            FROM feed_follows 
            WHERE follower_id = $current_user_id
        );
        
        SELECT 
            u.id,
            u.username,
            u.first_name_encrypted,
            u.last_name_encrypted,
            u.is_verified,
            COALESCE(f.followers_count, 0) as followers_count,
            COALESCE(p.posts_count, 0) as posts_count
        FROM users u
        LEFT JOIN (
            SELECT following_id, COUNT(*) as followers_count
            FROM feed_follows
            GROUP BY following_id
        ) f ON u.id = f.following_id
        LEFT JOIN (
            SELECT user_id, COUNT(*) as posts_count
            FROM feed_posts
            WHERE is_deleted = false
            GROUP BY user_id
        ) p ON u.id = p.user_id
        WHERE u.id != $current_user_id
          AND u.id NOT IN (SELECT following_id FROM $followed_users)
        ORDER BY followers_count DESC, posts_count DESC
        LIMIT $limit OFFSET $offset;
        """
        
        params = {
            '$current_user_id': current_user_id,
            '$limit': limit,
            '$offset': offset
        }
        
        try:
            users_repo = UserRepository(session)
            rows = await users_repo.execute(query, params)
            
            users = []
            for row in rows:
                # Получаем ID
                user_id = row.get('id') or row.get('u.id')
                if not user_id:
                    logger.error(f"❌ No id in row: {row}")
                    continue
                
                # Декодируем имя
                first_name = ''
                enc_first = row.get('first_name_encrypted') or row.get('u.first_name_encrypted')
                if enc_first:
                    first_name = safe_b64decode(enc_first)
                
                # Декодируем фамилию
                last_name = ''
                enc_last = row.get('last_name_encrypted') or row.get('u.last_name_encrypted')
                if enc_last:
                    last_name = safe_b64decode(enc_last)
                
                username = row.get('username') or row.get('u.username', '')
                is_verified = row.get('is_verified') or row.get('u.is_verified', False)
                followers_count = row.get('followers_count', 0)
                posts_count = row.get('posts_count', 0)
                
                user = {
                    'id': user_id,
                    'username': username,
                    'first_name': first_name,
                    'last_name': last_name,
                    'is_verified': is_verified,
                    'followers_count': followers_count,
                    'posts_count': posts_count,
                    'is_following': False
                }
                users.append(user)
            
            has_more = len(users) == limit
            
            result = {
                'users': users,
                'has_more': has_more,
                'pagination': {
                    'limit': limit,
                    'offset': offset,
                    'has_more': has_more
                }
            }
            
            await cache.set(cache_key, result, ttl=300)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error getting user suggestions: {e}")
            return {
                'users': [], 
                'has_more': False, 
                'pagination': {
                    'limit': limit, 
                    'offset': offset, 
                    'has_more': False
                }
            }
    
    async def get_similar_users(self, session, user_id: str, current_user_id: str, limit: int = 20) -> Dict:
        """
        Получить похожих пользователей (по общим подписчикам)
        """
        logger.info(f"👥 Getting similar users for {user_id}")
        
        cache_key = f"users:similar:{user_id}:{current_user_id}:{limit}"
        cached = await cache.get(cache_key)
        if cached:
            return cached
        
        users_repo = UserRepository(session)
        
        # Проверяем существование пользователя
        user = await users_repo.get(user_id)
        if not user:
            raise NotFoundError(f"User {user_id} not found")
        
        query = f"""
        DECLARE $user_id AS Utf8;
        DECLARE $current_user_id AS Utf8;
        DECLARE $limit AS Uint64;
        
        $followers_of_user = (
            SELECT follower_id 
            FROM feed_follows 
            WHERE following_id = $user_id
        );
        
        SELECT 
            u.id,
            u.username,
            u.first_name_encrypted,
            u.last_name_encrypted,
            u.is_verified,
            COUNT(DISTINCT f.follower_id) as common_followers,
            COALESCE(followers.followers_count, 0) as followers_count
        FROM users u
        JOIN feed_follows f ON f.following_id = u.id
        LEFT JOIN (
            SELECT following_id, COUNT(*) as followers_count
            FROM feed_follows
            GROUP BY following_id
        ) followers ON u.id = followers.following_id
        WHERE f.follower_id IN (SELECT follower_id FROM $followers_of_user)
          AND u.id != $user_id
          AND u.id != $current_user_id
        GROUP BY u.id, u.username, u.first_name_encrypted, u.last_name_encrypted, u.is_verified, followers.followers_count
        ORDER BY common_followers DESC, followers_count DESC
        LIMIT $limit;
        """
        
        params = {
            '$user_id': user_id,
            '$current_user_id': current_user_id,
            '$limit': limit
        }
        
        try:
            rows = await users_repo.execute(query, params)
            
            users = []
            for row in rows:
                similar_user_id = row.get('id') or row.get('u.id')
                if not similar_user_id:
                    continue
                
                # Декодируем имя
                first_name = ''
                enc_first = row.get('first_name_encrypted') or row.get('u.first_name_encrypted')
                if enc_first:
                    first_name = safe_b64decode(enc_first)
                
                # Декодируем фамилию
                last_name = ''
                enc_last = row.get('last_name_encrypted') or row.get('u.last_name_encrypted')
                if enc_last:
                    last_name = safe_b64decode(enc_last)
                
                username = row.get('username') or row.get('u.username', '')
                is_verified = row.get('is_verified') or row.get('u.is_verified', False)
                
                # Проверяем, подписан ли текущий пользователь
                is_following = False
                follows_repo = FollowRepository(session)
                if current_user_id != similar_user_id:
                    is_following = await follows_repo.check(current_user_id, similar_user_id)
                
                user_data = {
                    'id': similar_user_id,
                    'username': username,
                    'first_name': first_name,
                    'last_name': last_name,
                    'is_verified': is_verified,
                    'followers_count': row.get('followers_count', 0),
                    'common_followers': row.get('common_followers', 0),
                    'is_following': is_following
                }
                users.append(user_data)
            
            has_more = len(users) == limit
            
            result = {
                'user_id': user_id,
                'users': users,
                'has_more': has_more,
                'pagination': {
                    'limit': limit,
                    'offset': 0,
                    'has_more': has_more
                }
            }
            
            await cache.set(cache_key, result, ttl=600)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error getting similar users: {e}")
            # Возвращаем популярных пользователей как fallback
            popular = await self.get_popular_users(session, current_user_id, limit, 0)
            return {
                'user_id': user_id,
                'users': popular.get('users', []),
                'has_more': popular.get('has_more', False),
                'pagination': {
                    'limit': limit,
                    'offset': 0,
                    'has_more': popular.get('has_more', False)
                }
            }

# ============================================
# ВАЛИДАТОРЫ
# ============================================

class PostValidator:
    """Валидатор для постов"""
    
    @staticmethod
    def validate_create(content: str, title: Optional[str], images: List[str]):
        if not content or len(content.strip()) < feed_config.MIN_CONTENT_LENGTH:
            raise ValueError(f"Content must be at least {feed_config.MIN_CONTENT_LENGTH} characters")
        if len(content) > feed_config.MAX_CONTENT_LENGTH:
            raise ValueError(f"Content must be less than {feed_config.MAX_CONTENT_LENGTH} characters")
        if title and len(title) > feed_config.MAX_TITLE_LENGTH:
            raise ValueError(f"Title must be less than {feed_config.MAX_TITLE_LENGTH} characters")
        if len(images) > feed_config.MAX_IMAGES_PER_POST:
            raise ValueError(f"Maximum {feed_config.MAX_IMAGES_PER_POST} images allowed")
    
    @staticmethod
    def validate_update(updates: Dict):
        allowed_fields = ['title', 'content', 'visibility']
        for field in updates:
            if field not in allowed_fields:
                raise ValueError(f"Cannot update field: {field}")


class CommentValidator:
    """Валидатор для комментариев"""
    
    @staticmethod
    def validate_create(content: str):
        if not content or len(content.strip()) < feed_config.MIN_COMMENT_LENGTH:
            raise ValueError(f"Comment must be at least {feed_config.MIN_COMMENT_LENGTH} character")
        if len(content) > feed_config.MAX_COMMENT_LENGTH:
            raise ValueError(f"Comment must be less than {feed_config.MAX_COMMENT_LENGTH} characters")


class ReportValidator:
    """Валидатор для жалоб"""
    
    @staticmethod
    def validate_create(entity_type: str, reason: str):
        if entity_type not in ['post', 'comment']:
            raise ValueError("entity_type must be 'post' or 'comment'")
        if not reason:
            raise ValueError("reason is required")
        if reason not in feed_config.ALLOWED_REPORT_REASONS:
            raise ValueError(f"reason must be one of: {', '.join(feed_config.ALLOWED_REPORT_REASONS)}")


# ============================================
# ХЕНДЛЕР ЛЕНТЫ - С ЯВНЫМ RequestContext!
# ============================================

class FeedHandler(BaseHandler):
    """Обработчик HTTP запросов для ленты новостей с курсорной пагинацией"""
    
    def __init__(self):
        super().__init__()
        self.post_service = PostService()
        self.repost_service = RepostService()
        self.like_service = LikeService()
        self.bookmark_service = BookmarkService()
        self.search_service = SearchService()
        self.trending_service = TrendingService()
        self.comment_service = CommentService()
        self.follow_service = FollowService()
        self.profile_service = ProfileService()
        self.notification_service = NotificationService()
        self.report_service = ReportService()
        self.mention_service = MentionService()
        self.recommendation_service = RecommendationService() 
        self.reaction_service = ReactionService() 
        self.user_recommendation_service = UserRecommendationService()
        logger.info("✅ FeedHandler v6.2.0 initialized - с курсорной пагинацией!")
    
    # ============================================
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # ============================================
    
    def _encode_cursor(self, created_at: int, post_id: str) -> str:
        """Закодировать курсор в base64"""
        cursor_str = f"{created_at}:{post_id}"
        return base64.b64encode(cursor_str.encode()).decode()
    
    def _decode_cursor(self, cursor: str) -> Tuple[Optional[int], Optional[str]]:
        """Декодировать курсор из base64"""
        try:
            decoded = base64.b64decode(cursor).decode('utf-8')
            ts_str, post_id = decoded.split(':')
            return int(ts_str), post_id
        except Exception:
            return None, None
    
    def _cursor_response(self, items: List[Any], next_cursor: Optional[str] = None) -> Dict:
        """Сформировать ответ с курсором"""
        return {
            'items': items,
            'next_cursor': next_cursor,
            'has_more': next_cursor is not None
        }
    
    # ============================================
    # POST /feed - создать пост
    # ============================================
        # ===== 🔥 НОВЫЙ МЕТОД: Кросс-пост из канала в ленту =====
    @rate_limit(requests=10, window=60)
    @measure_time
    async def handle_publish_channel_post(self, event: Dict, user: Dict, channel_id: int, message_id: int) -> Dict:
        """
        POST /feed/channels/{channelId}/messages/{messageId}/publish
        
        Опубликовать сообщение из канала в ленту новостей
        """
        try:
            logger.info(f"📰 Publishing channel message {message_id} from channel {channel_id} to feed")
            
            # Валидация параметров
            if not channel_id or channel_id <= 0:
                return await self.response.error(
                    "Invalid channel ID",
                    400,
                    "validation_error",
                    event=event
                )

            if not message_id or message_id <= 0:
                return await self.response.error(
                    "Invalid message ID",
                    400,
                    "validation_error",
                    event=event
                )

            user_id = user.get('user_id')
            if not user_id:
                return await self.response.error(
                    "User ID not found in token",
                    401,
                    "auth_error",
                    event=event
                )
            
            # Получаем опциональные параметры из тела запроса
            body = self._parse_body(event)
            title = body.get('title')
            comment = body.get('comment')
            
            async with RequestContext() as ctx:
                # 1. Проверяем, что это действительно канал (читаем из БД напрямую)
                channel_query = """
                DECLARE $channel_id AS Uint64;
                
                SELECT id, type, title, username
                FROM `chats`
                WHERE id = $channel_id AND is_deleted = false;
                """
                
                channel_result = ctx.session.transaction().execute(
                    ctx.session.prepare(channel_query),
                    {'$channel_id': channel_id},
                    commit_tx=True
                )
                
                if not channel_result or not channel_result[0].rows:
                    return await self.response.error(
                        f"Chat {channel_id} not found",
                        404,
                        "not_found",
                        event=event
                    )

                channel = channel_result[0].rows[0]

                if channel.get('type') != 'channel':
                    return await self.response.error(
                        f"Chat {channel_id} is not a channel (type: {channel.get('type')})",
                        400,
                        "validation_error",
                        event=event
                    )

                # 2. Проверяем права (только owner/admin) - используем метод из PostService
                is_admin = await self.post_service.check_channel_admin(
                    session=ctx.session,
                    channel_id=channel_id,
                    user_id=user_id
                )

                if not is_admin:
                    return await self.response.error(
                        "Only channel owner and admins can publish to feed",
                        403,
                        "permission_denied",
                        event=event
                    )

                # 3. Получаем сообщение напрямую из БД
                message_data = await self.post_service.get_channel_message_from_db(
                    session=ctx.session,
                    channel_id=channel_id,
                    message_id=message_id
                )

                if not message_data:
                    return await self.response.error(
                        f"Message {message_id} not found in channel {channel_id}",
                        404,
                        "not_found",
                        event=event
                    )
                
                # 4. Формируем контент для поста
                content = message_data['content'] or ""
                if comment:
                    content = f"{comment}\n\n---\n{content}"
                
                # 5. Создаем пост в ленте от имени канала
                channel_author = Author(
                    id=str(channel_id),
                    username=channel.get('username') or f"channel_{channel_id}",
                    display_name=channel.get('title') or channel.get('username') or f"Channel {channel_id}",
                    avatar_url=None,
                    is_verified=False,
                    is_following=False,
                )
                post, channel_post = await self.post_service.create_from_channel(
                    session=ctx.session,
                    user_id=user_id,
                    user_data=user,
                    channel_id=channel_id,
                    channel_message_id=message_id,
                    content=content,
                    title=None,
                    images=[],
                    metadata={
                        'original_message_type': message_data['message_type'],
                        'has_attachments': message_data['has_attachments'],
                        'published_by': user_id,
                        'original_sender_id': message_data['sender_id'],
                        'original_sender_role': message_data['sender_role'],
                        'channel_username': channel.get('username')
                    },
                    author_override=channel_author,
                )
                
                logger.info(f"✅ Post {post.id} created from channel {channel_id} message {message_id}")
                
                return await self.response.success({
                    "post": post.dict(),
                    "channel_link": channel_post.to_dict(),
                    "message": "Post published to feed successfully"
                }, 201, event=event)
                
        except NotFoundError as e:
            return await self.response.error(str(e), 404, event=event)
        except PermissionError as e:
            return await self.response.error(str(e), 403, event=event)
        except Exception as e:
            logger.error(f"Error publishing to feed: {e}", exc_info=True)
            return await self.handle_error(e, event)
    @rate_limit(requests=feed_config.POST_RATE_LIMIT, window=feed_config.RATE_LIMIT_PERIOD)
    @measure_time
    async def handle_create_post(self, event: Dict, user: Dict) -> Dict:
        """POST /feed - создать пост - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"📝 Creating post for user {user.get('user_id')}")
            
            body = self._parse_body(event)
            data = PostCreate(**body)
            
            idempotency_key = self._get_idempotency_key(event)
            
            async with RequestContext() as ctx:
                if idempotency_key:
                    idem_repo = IdempotencyRepository(ctx.session)
                    existing = await idem_repo.get(idempotency_key)
                    if existing:
                        post = await self.post_service.get(
                            session=ctx.session,
                            post_id=str(existing.entity_id),
                            user_id=user['user_id'],
                            user_data=user
                        )
                        return await self.response.success(post.dict(), 201, event)
                
                # 👇 Передаем все данные пользователя из токена
                post = await self.post_service.create(
                    session=ctx.session,
                    user_id=user['user_id'],
                    user_data=user,  # ← весь объект user содержит username, first_name и т.д.
                    content=data.content,
                    title=data.title,
                    visibility=data.visibility,
                    images=data.images
                )
                
                if idempotency_key:
                    entity_id = int(str(uuid.uuid4()).replace('-', '')[:16], 16) % (2**64)
                    key = IdempotencyKey(
                        idempotency_key=idempotency_key,
                        entity_type="post",
                        entity_id=entity_id,
                        user_id=user['user_id'],
                        created_at=datetime.utcnow(),
                        expires_at=datetime.utcnow() + timedelta(hours=24)
                    )
                    await idem_repo.create(key)
            
            return await self.response.success(post.dict(), 201, event)
            
        except ValueError as e:
            return await self.response.error(str(e), 400, event=event)
        except PermissionError as e:
            return await self.response.error(str(e), 403, event=event)
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    # ============================================
    # GET /feed/for-you - персонализированная лента
    # ============================================
  
    @measure_time
    async def handle_get_prefetched_page(self, event: Dict, user: Dict) -> Dict:
        """
        GET /feed/prefetch/{feed_type}?cursor=xxx
        Получить предзагруженную страницу
        """
        try:
            feed_type = event.get('pathParameters', {}).get('feed_type')
            query = event.get('queryStringParameters', {}) or {}
            cursor = query.get('cursor')
            
            if not feed_type:
                return await self.response.error("feed_type is required", 400, event=event)
            
            if not cursor or cursor == 'null' or str(cursor).strip() == '':
                return await self.response.error("cursor is required", 400, event=event)
            
            cache_key = f"prefetch:{feed_type}:{user['user_id']}:{cursor}"
            cached = await cache.get(cache_key)
            
            if cached:
                logger.info(f"📦 Prefetched page hit for {feed_type}")
                # Удаляем после использования
                await cache.delete(cache_key)
                return await self.response.success(cached, event=event)
            
            logger.info(f"⚠️ Prefetched page not found for {feed_type}")
            return await self.response.success(None, event=event)
            
        except Exception as e:
            logger.error(f"Error getting prefetched page: {e}")
            return await self.handle_error(e, event)
        

    @measure_time
    async def handle_get_comment_replies(self, event: Dict, user: Dict, comment_id: str) -> Dict:
        """
        GET /feed/comments/{commentId}/replies - получить ответы на комментарий
        """
        try:
            logger.info(f"💬 Getting replies for comment {comment_id}")
            
            if not validate_uuid(comment_id):
                return await self.response.error("Invalid comment ID format", 400, event=event)
            
            query = event.get('queryStringParameters', {}) or {}
            limit = self._get_int_query_param(event, 'limit', 10)
            cursor = query.get('cursor')
            
            limit = min(limit, 50)
            
            async with RequestContext() as ctx:
                replies, next_cursor = await self.comment_service.get_replies_with_cursor(
                    session=ctx.session,
                    comment_id=comment_id,
                    user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "comment_id": comment_id,
                "replies": [r.dict() for r in replies],
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None
            }, event=event)
            
        except Exception as e:
            logger.error(f"❌ Error getting replies: {e}", exc_info=True)
            return await self.handle_error(e, event)
    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_for_you_feed(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/for-you - персонализированная лента с курсором"""
        try:
            query = event.get('queryStringParameters', {}) or {}
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            logger.info(f"🎯 Getting for-you feed for user {user['user_id']} with cursor")
            
            async with RequestContext() as ctx:
                result = await self.recommendation_service.get_for_you_feed_with_cursor(
                    session=ctx.session,
                    user_id=user['user_id'],
                    user_data=user,
                    limit=limit,
                    cursor=cursor
                )
            
            # 🔥 ПРЕДЗАГРУЗКА: если есть следующая страница, загружаем её в фоне
            next_cursor = result.get('next_cursor')
            if next_cursor and not cursor:  # только для первой страницы
                asyncio.create_task(
                    self.recommendation_service._prefetch_next_page(
                        ctx.session, user['user_id'], user, 'for_you', limit, next_cursor
                    )
                )
            
            return await self.response.success({
                "feed_type": "for_you",
                "posts": result['items'],
                "next_cursor": result.get('next_cursor'),
                "has_more": result.get('has_more', False)
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error in for-you feed: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_following_feed(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/following - лента подписок с курсором"""
        try:
            query = event.get('queryStringParameters', {}) or {}
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            logger.info(f"👥 Getting following feed for user {user['user_id']} with cursor")
            
            async with RequestContext() as ctx:
                result = await self.recommendation_service.get_following_feed_with_cursor(
                    session=ctx.session,
                    user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "feed_type": "following",
                "posts": result['items'],
                "next_cursor": result.get('next_cursor'),
                "has_more": result.get('has_more', False)
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error in following feed: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    
    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_popular_feed(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/popular - популярные посты с курсором"""
        try:
            query = event.get('queryStringParameters', {}) or {}
            
            limit = self._get_int_query_param(event, 'limit', 20)
            days = self._get_int_query_param(event, 'days', 7)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            logger.info(f"📈 Getting popular feed for user {user['user_id']} with cursor")
            
            async with RequestContext() as ctx:
                result = await self.recommendation_service.get_popular_feed_with_cursor(
                    session=ctx.session,
                    user_id=user['user_id'],
                    user_data=user,
                    days=days,
                    limit=limit,
                    cursor=cursor
                )
            
            # 🔥 ПРЕДЗАГРУЗКА: если есть следующая страница и это первая страница
            next_cursor = result.get('next_cursor')
            if next_cursor and next_cursor != 'null' and str(next_cursor).strip() and not cursor:
                asyncio.create_task(
                    self.recommendation_service._prefetch_next_page(
                        ctx.session, user['user_id'], user, 'popular', limit, next_cursor
                    )
                )
            
            # Получаем данные (поддержка обоих форматов)
            items = result.get('items', [])
            next_cursor = result.get('next_cursor')
            has_more = result.get('has_more', False)
            feed_type = result.get('feed_type', 'popular')
            
            return await self.response.success({
                "feed_type": feed_type,
                "posts": items,
                "next_cursor": next_cursor,
                "has_more": has_more
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error in popular feed: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_fresh_feed(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/fresh - свежие посты с курсором"""
        try:
            query = event.get('queryStringParameters', {}) or {}
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            simple = self._get_bool_query_param(event, 'simple', False)
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            logger.info(f"🆕 Getting fresh feed for user {user['user_id']} with cursor, simple={simple}")
            
            async with RequestContext() as ctx:
                if simple:
                    result = await self.recommendation_service.get_fresh_feed_simple_with_cursor(
                        session=ctx.session,
                        user_id=user['user_id'],
                        user_data=user,
                        limit=limit,
                        cursor=cursor
                    )
                else:
                    result = await self.recommendation_service.get_fresh_feed_with_cursor(
                        session=ctx.session,
                        user_id=user['user_id'],
                        user_data=user,
                        limit=limit,
                        cursor=cursor
                    )
            
            # 🔥 ПРЕДЗАГРУЗКА: если есть следующая страница, загружаем её в фоне
            next_cursor = result.get('next_cursor')
            if next_cursor and not cursor:  # только для первой страницы
                asyncio.create_task(
                    self.recommendation_service._prefetch_next_page(
                        ctx.session, user['user_id'], user, 'fresh', limit, next_cursor
                    )
                )
            
            return await self.response.success({
                "feed_type": "fresh",
                "posts": result['items'],
                "next_cursor": result.get('next_cursor'),
                "has_more": result.get('has_more', False)
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error in fresh feed: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    
    @rate_limit(requests=100, window=60)
    @measure_time
    async def handle_feed_with_type(self, event: Dict, user: Dict) -> Dict:
        """GET /feed?type=for_you&limit=20&cursor=..."""
        try:
            query = event.get('queryStringParameters', {}) or {}
            
            feed_type = query.get('type', 'for_you')
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            days = self._get_int_query_param(event, 'days', 7)
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            logger.info(f"📋 Getting {feed_type} feed for user {user['user_id']} with cursor")
            
            async with RequestContext() as ctx:
                if feed_type == FeedType.FOLLOWING.value:
                    result = await self.recommendation_service.get_following_feed_with_cursor(
                        session=ctx.session,
                        user_id=user['user_id'],
                        limit=limit,
                        cursor=cursor
                    )
                    
                elif feed_type == FeedType.POPULAR.value:
                    result = await self.recommendation_service.get_popular_feed_with_cursor(
                        session=ctx.session,
                        user_id=user['user_id'],
                        user_data=user,
                        days=days,
                        limit=limit,
                        cursor=cursor
                    )
                    
                elif feed_type == FeedType.FRESH.value:
                    result = await self.recommendation_service.get_fresh_feed_with_cursor(
                        session=ctx.session,
                        user_id=user['user_id'],
                        user_data=user,
                        limit=limit,
                        cursor=cursor
                    )
                    
                elif feed_type == FeedType.FOR_YOU.value:
                    result = await self.recommendation_service.get_for_you_feed_with_cursor(
                        session=ctx.session,
                        user_id=user['user_id'],
                        user_data=user,
                        limit=limit,
                        cursor=cursor
                    )
                    
                elif feed_type == FeedType.TRENDING.value:
                    trending = await self.trending_service.get_with_cursor(
                        session=ctx.session,
                        user_id=user['user_id'],
                        limit=limit,
                        cursor=cursor
                    )
                    return await self.response.success(trending, event=event)
                
                else:
                    return await self.response.error(f"Invalid feed type: {feed_type}", 400, event=event)
            
            return await self.response.success({
                "feed_type": feed_type,
                "posts": result['items'],
                "next_cursor": result.get('next_cursor'),
                "has_more": result.get('has_more', False)
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error in feed: {e}", exc_info=True)
            return await self.handle_error(e, event)
    
    # ============================================
    # GET /feed/posts/{postId} - получить пост
    # ============================================
    
    @measure_time
    async def handle_get_post(self, event: Dict, user: Dict, post_id: str) -> Dict:
        """GET /feed/posts/{postId} - получить пост по ID"""
        try:
            logger.info(f"🔍 Getting post {post_id}")
            
            if not validate_uuid(post_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                post = await self.post_service.get(
                    session=ctx.session,
                    post_id=post_id,
                    user_id=user['user_id'],
                    user_data=user
                )
            
            # Преобразуем в словарь
            response_data = post.dict()
            
            # Логируем содержимое перед отправкой
            logger.info(f"📦 response_data keys: {response_data.keys()}")
            logger.info(f"📦 response_data reactions: {response_data.get('reactions')}")
            logger.info(f"📦 response_data reactions_preview: {response_data.get('reactions_preview')}")
            
            # Формируем ответ
            result = await self.response.success({"post": response_data}, event=event)
            
            # Логируем тело ответа (первые 500 символов)
            body_preview = result.get('body', '')
            if isinstance(body_preview, bytes):
                body_preview = body_preview[:500].decode('utf-8', errors='ignore')
            else:
                body_preview = str(body_preview)[:500]
            logger.info(f"📦 Response body preview: {body_preview}")
            
            return result
            
        except PermissionError as e:
            return await self.response.error(str(e), 403, event=event)
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_update_post(self, event: Dict, user: Dict, post_id: str) -> Dict:
        """PUT /feed/posts/{postId} - обновить пост - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"✏️ Updating post {post_id}")
            
            if not validate_uuid(post_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            body = self._parse_body(event)
            data = PostUpdate(**body)
            
            async with RequestContext() as ctx:
                post = await self.post_service.update(
                    session=ctx.session,
                    post_id=post_id,
                    user_id=user['user_id'],
                    user_data=user,
                    updates=data.dict(exclude_unset=True)
                )
            
            return await self.response.success(post.dict(), event=event)
            
        except ValueError as e:
            return await self.response.error(str(e), 400, event=event)
        except PermissionError as e:
            return await self.response.error(str(e), 403, event=event)
        except Exception as e:
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_delete_post(self, event: Dict, user: Dict, post_id: str) -> Dict:
        """DELETE /feed/posts/{postId} - удалить пост - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"🗑️ Deleting post {post_id}")
            
            if not validate_uuid(post_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            permanent = self._get_bool_query_param(event, 'permanent', False)
            
            async with RequestContext() as ctx:
                result = await self.post_service.delete(
                    session=ctx.session,
                    post_id=post_id,
                    user_id=user['user_id'],
                    permanent=permanent
                )
            
            return await self.response.success(result, event=event)
            
        except PermissionError as e:
            return await self.response.error(str(e), 403, event=event)
        except Exception as e:
            return await self.response.error(str(e), 400, event=event)
    
    # ============================================
    # POST /feed/like - лайк
    # ============================================
    
    @rate_limit(requests=feed_config.LIKE_RATE_LIMIT, window=feed_config.RATE_LIMIT_PERIOD)
    @measure_time
    async def handle_toggle_like(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/like - переключить лайк - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"❤️ Toggling like")
            
            body = self._parse_body(event)
            data = LikeToggle(**body)
            
            if not validate_uuid(data.post_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                result = await self.post_service.toggle_like(
                    session=ctx.session,
                    user_id=user['user_id'],
                    post_id=data.post_id
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    # ============================================
    # POST /feed/bookmark - закладка
    # ============================================
    
    @rate_limit(requests=feed_config.LIKE_RATE_LIMIT, window=feed_config.RATE_LIMIT_PERIOD)
    @measure_time
    async def handle_toggle_bookmark(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/bookmark - переключить закладку - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"🔖 Toggling bookmark")
            
            body = self._parse_body(event)
            data = BookmarkToggle(**body)
            
            if not validate_uuid(data.post_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                result = await self.bookmark_service.toggle(
                    session=ctx.session,
                    user_id=user['user_id'],
                    post_id=data.post_id,
                    folder=data.folder,
                    notes=data.notes
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    # ============================================
    # POST /feed/repost - репост
    # ============================================
    
    @rate_limit(requests=feed_config.REPOST_RATE_LIMIT, window=feed_config.RATE_LIMIT_PERIOD)
    @measure_time
    async def handle_create_repost(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/repost - создать репост - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"🔄 Creating repost")
            
            body = self._parse_body(event)
            data = RepostCreate(**body)
            
            if not validate_uuid(data.original_post_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                result = await self.repost_service.create(
                    session=ctx.session,
                    user_id=user['user_id'],
                    original_post_id=data.original_post_id,
                    comment=data.comment or ''
                )
            
            return await self.response.success(result, 201, event=event)
            
        except PermissionError as e:
            return await self.response.error(str(e), 403, event=event)
        except Exception as e:
            logger.error(f"Error creating repost: {e}", exc_info=True)
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_delete_repost(self, event: Dict, user: Dict, repost_id: str) -> Dict:
        """DELETE /feed/repost/{repostId} - удалить репост - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"🗑️ Deleting repost {repost_id}")
            
            if validate_uuid(repost_id):
                pass
            elif re.match(r'^[a-f0-9]{32}$', repost_id, re.IGNORECASE):
                repost_id = f"{repost_id[0:8]}-{repost_id[8:12]}-{repost_id[12:16]}-{repost_id[16:20]}-{repost_id[20:32]}"
            else:
                return await self.response.error("Invalid repost ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                result = await self.repost_service.delete(
                    session=ctx.session,
                    repost_id=repost_id,
                    user_id=user['user_id']
                )
            
            return await self.response.success(result, event=event)
            
        except PermissionError as e:
            return await self.response.error(str(e), 403, event=event)
        except Exception as e:
            logger.error(f"Error deleting repost: {e}", exc_info=True)
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_get_reposts(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/reposts - получить репосты поста с курсором"""
        try:
            logger.info(f"🔄 Getting reposts with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            original_post_id = query.get('original_post_id')
            
            if not original_post_id:
                return await self.response.error("original_post_id is required", 400, event=event)
            
            if not validate_uuid(original_post_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                # 👇 ВАЖНО: используем правильный метод
                result = await self.repost_service.get_by_original_with_cursor(
                    session=ctx.session,
                    original_post_id=original_post_id,
                    current_user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            # Проверяем наличие ошибки
            if isinstance(result, dict) and 'error' in result:
                logger.warning(f"⚠️ Repost service returned error: {result['error']}")
                return await self.response.success({
                    "reposts": [],
                    "next_cursor": None,
                    "has_more": False
                }, event=event)
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            logger.error(f"Error getting reposts: {e}", exc_info=True)
            return await self.response.success({
                "reposts": [],
                "next_cursor": None,
                "has_more": False
            }, event=event)

    @measure_time
    async def handle_get_user_reposts(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/user/reposts - получить репосты пользователя с курсором"""
        try:
            logger.info(f"📋 Getting user reposts with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            target_user_id = query.get('user_id', user['user_id'])
            
            if not validate_uuid(target_user_id):
                return await self.response.error("Invalid user ID format", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                result = await self.repost_service.get_by_user_with_cursor(
                    session=ctx.session,
                    user_id=target_user_id,
                    current_user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            logger.error(f"Error getting user reposts: {e}", exc_info=True)
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_repost_stats(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/reposts/stats?post_id=123 - статистика репостов"""
        try:
            logger.info(f"📊 Getting repost stats")
            
            query = event.get('queryStringParameters', {}) or {}
            post_id = query.get('post_id')
            
            if not post_id:
                return await self.response.error("post_id is required", 400, event=event)
            
            if not validate_uuid(post_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                stats = await self.repost_service.get_stats(
                    session=ctx.session,
                    original_post_id=post_id
                )
            
            return await self.response.success({
                'post_id': post_id,
                'stats': stats
            }, event=event)
            
        except Exception as e:
            logger.error(f"Error getting repost stats: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    # ============================================
    # POST /feed/reactions - реакции
    # ============================================
    
    @measure_time
    async def handle_toggle_reaction(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/reactions - переключить реакцию - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"🔄 Toggling reaction")
            
            body = self._parse_body(event)
            data = ReactionToggle(**body)
            
            async with RequestContext() as ctx:
                result = await self.reaction_service.toggle(
                    session=ctx.session,
                    user_id=user['user_id'],
                    entity_type=data.entity_type,
                    entity_id=data.entity_id,
                    reaction_type=data.reaction_type
                )
            await cache.delete(f"interests:{user['user_id']}")

            return await self.response.success(result, event=event)
            
        except ValueError as e:
            return await self.response.error(str(e), 400, event=event)
        except Exception as e:
            logger.error(f"❌ Error toggling reaction: {e}", exc_info=True)
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_get_reactions(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/reactions - получить список реакций на сущность"""
        try:
            logger.info(f"📋 Getting reactions")
            
            query = event.get('queryStringParameters', {}) or {}
            
            entity_type = query.get('entity_type')
            entity_id = query.get('entity_id')
            
            if not entity_type or not entity_id:
                return await self.response.error("entity_type and entity_id are required", 400, event=event)
            
            if entity_type not in ['post', 'comment']:
                return await self.response.error("entity_type must be 'post' or 'comment'", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                result = await self.reaction_service.get_reactions(
                    session=ctx.session,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    current_user_id=user['user_id'],
                    limit=limit,
                    offset=offset
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            logger.error(f"❌ Error getting reactions: {e}", exc_info=True)
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_get_reaction_counts(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/reactions/counts - получить счетчики реакций"""
        try:
            logger.info(f"🔢 Getting reaction counts")
            
            query = event.get('queryStringParameters', {}) or {}
            
            entity_type = query.get('entity_type')
            entity_id = query.get('entity_id')
            
            if not entity_type or not entity_id:
                return await self.response.error("entity_type and entity_id are required", 400, event=event)
            
            if entity_type not in ['post', 'comment']:
                return await self.response.error("entity_type must be 'post' or 'comment'", 400, event=event)
            
            async with RequestContext() as ctx:
                counts = await self.reaction_service.get_reaction_counts(
                    session=ctx.session,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    current_user_id=user['user_id']
                )
            
            return await self.response.success({
                'entity_type': entity_type,
                'entity_id': entity_id,
                'reaction_counts': [c.dict() for c in counts],
                'total_count': sum(c.count for c in counts)
            }, event=event)
            
        except Exception as e:
            logger.error(f"❌ Error getting reaction counts: {e}", exc_info=True)
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    # ============================================
    # POST /feed/comments - комментарии
    # ============================================
    
    @rate_limit(requests=feed_config.COMMENT_RATE_LIMIT, window=feed_config.RATE_LIMIT_PERIOD)
    @measure_time
    async def handle_create_comment(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/comments - создать комментарий - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"💬 Creating comment")
            
            body = self._parse_body(event)
            data = CommentCreate(**body)
            
            if not validate_uuid(data.post_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                comment = await self.comment_service.create(
                    session=ctx.session,
                    user_id=user['user_id'],
                    user_data=user,
                    post_id=data.post_id,
                    content=data.content,
                    parent_comment_id=data.parent_comment_id
                )
            
            return await self.response.success({"comment": comment.dict()}, 201, event=event)
            
        except PermissionError as e:
            return await self.response.error(str(e), 403, event=event)
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_get_post_comments(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/comments - получить комментарии к посту с курсором"""
        try:
            logger.info(f"💬 Getting post comments with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            post_id = query.get('post_id')
            
            # 🔥 ДОБАВИТЬ ПРОВЕРКУ
            if not post_id:
                logger.error("❌ post_id is missing in request")
                return await self.response.error("post_id is required", 400, event=event)
            
            if not validate_uuid(post_id):
                logger.error(f"❌ Invalid post_id format: {post_id}")
                return await self.response.error("Invalid post ID format", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            logger.info(f"📋 Getting comments for post {post_id} with cursor, limit={limit}")
            
            async with RequestContext() as ctx:
                comments, next_cursor = await self.comment_service.get_by_post_with_cursor(
                    session=ctx.session,
                    post_id=post_id,
                    user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "comments": [c.dict() for c in comments],
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None
            }, event=event)
            
        except PermissionError as e:
            return await self.response.error(str(e), 403, event=event)
        except Exception as e:
            logger.error(f"❌ Error getting comments: {e}", exc_info=True)
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_toggle_comment_like(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/comments/like - переключить лайк на комментарии - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"❤️ Toggling comment like")
            
            body = self._parse_body(event)
            comment_id = body.get('comment_id')
            
            if not comment_id:
                return await self.response.error("comment_id is required", 400, event=event)
            
            if not validate_uuid(comment_id):
                return await self.response.error("Invalid comment ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                result = await self.comment_service.toggle_like(
                    session=ctx.session,
                    user_id=user['user_id'],
                    comment_id=comment_id
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    # ============================================
    # POST /feed/follow - подписки
    # ============================================
    
    @measure_time
    async def handle_toggle_follow(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/follow - подписаться/отписаться - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"🔄 Toggling follow")
            
            body = self._parse_body(event)
            data = FollowToggle(**body)
            
            if data.user_id == user['user_id']:
                return await self.response.error("Cannot follow yourself", 400, event=event)
            
            if not validate_uuid(data.user_id):
                return await self.response.error("Invalid user ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                result = await self.follow_service.toggle(
                    session=ctx.session,
                    follower_id=user['user_id'],
                    following_id=data.user_id,
                    action=data.action
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_get_followers(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/followers - получить подписчиков с курсором"""
        try:
            logger.info(f"👥 Getting followers with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            target_user_id = query.get('user_id', user['user_id'])
            
            if not validate_uuid(target_user_id):
                return await self.response.error("Invalid user ID format", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                followers, next_cursor = await self.follow_service.get_followers_with_cursor(
                    session=ctx.session,
                    user_id=target_user_id,
                    current_user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "followers": [f.dict() for f in followers],
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None
            }, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_get_following(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/following - получить подписки с курсором"""
        try:
            logger.info(f"👥 Getting following with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            target_user_id = query.get('user_id', user['user_id'])
            
            if not validate_uuid(target_user_id):
                return await self.response.error("Invalid user ID format", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                following, next_cursor = await self.follow_service.get_following_with_cursor(
                    session=ctx.session,
                    user_id=target_user_id,
                    current_user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "following": [f.dict() for f in following],
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None
            }, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    # ============================================
    # GET /feed/user/profile - профиль
    # ============================================
    
    @measure_time
    async def handle_get_profile(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/user/profile - получить профиль"""
        try:
            logger.info(f"👤 Getting profile")
            
            query = event.get('queryStringParameters', {}) or {}
            target_user_id = query.get('user_id', user['user_id'])
            
            if not validate_uuid(target_user_id):
                return await self.response.error("Invalid user ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                profile = await self.profile_service.get(
                    session=ctx.session,
                    user_id=target_user_id,
                    current_user_id=user['user_id']
                )
            
            return await self.response.success(profile, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_update_profile(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/user/profile/update - обновить профиль - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"✏️ Updating profile")
            
            body = self._parse_body(event)
            data = ProfileUpdate(**body)
            
            async with RequestContext() as ctx:
                profile = await self.profile_service.update(
                    session=ctx.session,
                    user_id=user['user_id'],
                    data=data.dict(exclude_unset=True)
                )
            
            return await self.response.success(profile, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_get_user_posts(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/user/posts - получить посты пользователя с курсором"""
        try:
            logger.info(f"📋 Getting user posts with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            target_user_id = query.get('user_id', user['user_id'])
            post_type = query.get('type', 'all')
            
            if not validate_uuid(target_user_id):
                return await self.response.error("Invalid user ID format", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                posts, next_cursor = await self.post_service.get_user_posts_with_cursor(
                    session=ctx.session,
                    user_id=user['user_id'],
                    target_user_id=target_user_id,
                    current_user_data=user,
                    post_type=post_type,
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "posts": [p.dict() for p in posts],
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None
            }, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_get_user_likes(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/user/likes - получить посты, которые лайкнул пользователь, с курсором"""
        try:
            logger.info(f"❤️ Getting user likes with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            target_user_id = query.get('user_id', user['user_id'])
            
            if not validate_uuid(target_user_id):
                return await self.response.error("Invalid user ID format", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                posts, next_cursor = await self.like_service.get_user_likes_with_cursor(
                    session=ctx.session,
                    user_id=user['user_id'],
                    target_user_id=target_user_id,
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "liked_posts": [p.dict() for p in posts],
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None
            }, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    # ============================================
    # GET /feed/notifications - уведомления
    # ============================================
    
    @measure_time
    async def handle_get_notifications(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/notifications - получить уведомления с курсором"""
        try:
            logger.info(f"🔔 Getting notifications with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            unread_only = self._get_bool_query_param(event, 'unread_only', False)
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                result = await self.notification_service.get_with_cursor(
                    session=ctx.session,
                    user_id=user['user_id'],
                    limit=limit,
                    unread_only=unread_only,
                    cursor=cursor
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_mark_notifications_read(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/notifications/read - отметить уведомления как прочитанные - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"✅ Marking notifications as read")
            
            body = self._parse_body(event)
            data = NotificationRead(**body)
            
            async with RequestContext() as ctx:
                success = await self.notification_service.mark_read(
                    session=ctx.session,
                    user_id=user['user_id'],
                    notification_id=data.notification_id,
                    mark_all=data.mark_all
                )
            
            if not success:
                return await self.response.error("Either notification_id or mark_all=true is required", 400, event=event)
            
            return await self.response.success({"success": True}, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 400, event=event)
    
    # ============================================
    # GET /feed/search - поиск
    # ============================================
    
    @measure_time
    async def handle_search(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/search - поиск постов с курсором"""
        try:
            logger.info(f"🔎 Searching posts with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            search_query = query.get('q', '').strip()
            
            if not search_query:
                return await self.response.error("Search query is required", 400, event=event)
            
            if len(search_query) < 2:
                return await self.response.error("Search query must be at least 2 characters", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, 50)
            
            async with RequestContext() as ctx:
                posts, next_cursor = await self.search_service.search_posts_with_cursor(
                    session=ctx.session,
                    query=search_query,
                    user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "posts": [p.dict() for p in posts],
                "query": search_query,
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None
            }, event=event)
            
        except ValueError as e:
            return await self.response.error(str(e), 400, event=event)
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    # ============================================
    # GET /feed/trending - тренды
    # ============================================
    
    @measure_time
    async def handle_trending(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/trending - получить популярный контент с курсором"""
        try:
            logger.info(f"📈 Getting trending with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            limit = self._get_int_query_param(event, 'limit', 10)
            cursor = query.get('cursor')
            limit = min(limit, 50)
            
            async with RequestContext() as ctx:
                trending = await self.trending_service.get_with_cursor(
                    session=ctx.session,
                    user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success(trending, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    # ============================================
    # GET /feed/bookmarks - закладки
    # ============================================
    
    @measure_time
    async def handle_list_bookmarks(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/bookmarks - получить закладки пользователя с курсором"""
        try:
            logger.info(f"🔖 Listing bookmarks with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                bookmarks, next_cursor = await self.bookmark_service.list_with_cursor(
                    session=ctx.session,
                    user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "bookmarks": bookmarks,
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    # ============================================
    # GET /feed/mentions - упоминания
    # ============================================
    
    @measure_time
    async def handle_get_mentions(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/mentions - получить упоминания пользователя с курсором"""
        try:
            logger.info(f"📋 Getting mentions for user {user.get('user_id')} with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            
            limit = self._get_int_query_param(event, 'limit', 50)
            cursor = query.get('cursor')
            unread_only = self._get_bool_query_param(event, 'unread_only', False)
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                result = await self.mention_service.get_user_mentions_with_cursor(
                    session=ctx.session,
                    user_id=user['user_id'],
                    current_user_id=user['user_id'],
                    limit=limit,
                    cursor=cursor,
                    unread_only=unread_only
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_get_mentions_count(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/mentions/count - получить количество непрочитанных упоминаний"""
        try:
            logger.info(f"🔢 Getting mentions count")
            
            async with RequestContext() as ctx:
                count = await self.mention_service.get_unread_count(
                    session=ctx.session,
                    user_id=user['user_id']
                )
            
            return await self.response.success({
                'unread_count': count
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_mark_mention_read(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/mentions/read - отметить упоминание как прочитанное - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"✅ Marking mention as read")
            
            body = self._parse_body(event)
            mention_id = body.get('mention_id')
            
            if not mention_id:
                return await self.response.error("mention_id is required", 400, event=event)
            
            if not validate_uuid(mention_id):
                return await self.response.error("Invalid mention ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                success = await self.mention_service.mark_as_read(
                    session=ctx.session,
                    mention_id=mention_id,
                    user_id=user['user_id']
                )
            
            if not success:
                return await self.response.error("Mention not found or already read", 404, event=event)
            
            return await self.response.success({
                'success': True,
                'mention_id': mention_id
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_mark_all_mentions_read(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/mentions/read-all - отметить все упоминания как прочитанные - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"✅ Marking all mentions as read")
            
            async with RequestContext() as ctx:
                success = await self.mention_service.mark_all_as_read(
                    session=ctx.session,
                    user_id=user['user_id']
                )
            
            return await self.response.success({
                'success': success,
                'message': 'All mentions marked as read'
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    # ============================================
    # GET /feed/hashtags - хештеги
    # ============================================
    
    @measure_time
    async def handle_search_hashtag(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/hashtags/search?q=tech - поиск по хештегу с курсором"""
        try:
            logger.info(f"🔍 Searching hashtag with cursor")
            
            query = event.get('queryStringParameters', {}) or {}
            hashtag = query.get('q', '').strip()
            
            if not hashtag:
                return await self.response.error("Hashtag is required (q parameter)", 400, event=event)
            
            if len(hashtag) < 2:
                return await self.response.error("Hashtag must be at least 2 characters", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            cursor = query.get('cursor')
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                posts, next_cursor = await self.post_service.search_by_hashtag_with_cursor(
                    session=ctx.session,
                    hashtag=hashtag,
                    user_id=user['user_id'],
                    user_data=user,
                    limit=limit,
                    cursor=cursor
                )
            
            return await self.response.success({
                "hashtag": hashtag,
                "posts": [p.dict() for p in posts],
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None
            }, event=event)
            
        except ValueError as e:
            return await self.response.error(str(e), 400, event=event)
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_suggest_hashtags(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/hashtags/suggest?prefix=te - автодополнение хештегов"""
        try:
            logger.info(f"💡 Suggesting hashtags")
            
            query = event.get('queryStringParameters', {}) or {}
            prefix = query.get('prefix', '').strip()
            
            if not prefix or len(prefix) < 2:
                return await self.response.success({"hashtags": []}, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 10)
            limit = min(limit, 50)
            
            async with RequestContext() as ctx:
                hashtags = await self.post_service.suggest_hashtags(
                    session=ctx.session,
                    prefix=prefix,
                    limit=limit
                )
            
            return await self.response.success({
                "prefix": prefix,
                "hashtags": hashtags
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_related_hashtags(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/hashtags/related?q=tech - связанные хештеги"""
        try:
            logger.info(f"🔗 Getting related hashtags")
            
            query = event.get('queryStringParameters', {}) or {}
            hashtag = query.get('q', '').strip()
            
            if not hashtag:
                return await self.response.error("Hashtag is required (q parameter)", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 10)
            limit = min(limit, 30)
            
            async with RequestContext() as ctx:
                related = await self.post_service.get_related_hashtags(
                    session=ctx.session,
                    hashtag=hashtag,
                    limit=limit
                )
            
            return await self.response.success({
                "hashtag": hashtag,
                "related": related
            }, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    # ============================================
    # GET /feed/users - рекомендации пользователей
    # ============================================
    
    @measure_time
    async def handle_popular_users(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/users/popular - популярные пользователи"""
        try:
            logger.info(f"⭐ Getting popular users")
            
            query = event.get('queryStringParameters', {}) or {}
            limit = self._get_int_query_param(event, 'limit', 20)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                result = await self.user_recommendation_service.get_popular_users(
                    session=ctx.session,
                    current_user_id=user['user_id'],
                    limit=limit,
                    offset=offset
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error in popular users: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_user_suggestions(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/users/suggestions - рекомендации пользователей для подписки"""
        try:
            logger.info(f"💡 Getting user suggestions")
            
            query = event.get('queryStringParameters', {}) or {}
            limit = self._get_int_query_param(event, 'limit', 20)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                result = await self.user_recommendation_service.get_suggestions(
                    session=ctx.session,
                    current_user_id=user['user_id'],
                    limit=limit,
                    offset=offset
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error in user suggestions: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_similar_users(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/users/similar - похожие пользователи"""
        try:
            logger.info(f"👥 Getting similar users")
            
            query = event.get('queryStringParameters', {}) or {}
            target_user_id = query.get('user_id', user['user_id'])
            
            if not validate_uuid(target_user_id):
                return await self.response.error("Invalid user ID format", 400, event=event)
            
            limit = self._get_int_query_param(event, 'limit', 20)
            limit = min(limit, 50)
            
            async with RequestContext() as ctx:
                result = await self.user_recommendation_service.get_similar_users(
                    session=ctx.session,
                    user_id=target_user_id,
                    current_user_id=user['user_id'],
                    limit=limit
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            logger.error(f"Unexpected error in similar users: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    # ============================================
    # POST /feed/report - жалобы
    # ============================================
    
    @measure_time
    async def handle_create_report(self, event: Dict, user: Dict) -> Dict:
        """POST /feed/report - создать жалобу - ЕДИНАЯ СЕССИЯ"""
        try:
            logger.info(f"🚨 Creating report")
            
            body = self._parse_body(event)
            data = ReportCreate(**body)
            
            if data.entity_type == 'post' and not validate_uuid(data.entity_id):
                return await self.response.error("Invalid post ID format", 400, event=event)
            if data.entity_type == 'comment' and not validate_uuid(data.entity_id):
                return await self.response.error("Invalid comment ID format", 400, event=event)
            
            async with RequestContext() as ctx:
                report = await self.report_service.create(
                    session=ctx.session,
                    reporter_id=user['user_id'],
                    entity_type=data.entity_type,
                    entity_id=data.entity_id,
                    reason=data.reason,
                    description=data.description
                )
            
            return await self.response.success(report, 201, event=event)
            
        except ValueError as e:
            return await self.response.error(str(e), 400, event=event)
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    @measure_time
    async def handle_list_pending_reports(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/reports - получить ожидающие жалобы (админ)"""
        try:
            user_role = self._get_user_role(event)
            
            if user_role not in ['admin', 'moderator']:
                return await self.response.error("Insufficient permissions", 403, event=event)
            
            logger.info(f"📋 Listing pending reports")
            
            query = event.get('queryStringParameters', {}) or {}
            limit = self._get_int_query_param(event, 'limit', 20)
            offset = self._get_int_query_param(event, 'offset', 0)
            
            limit = min(limit, feed_config.MAX_FEED_LIMIT)
            
            async with RequestContext() as ctx:
                reports = await self.report_service.list_pending(
                    session=ctx.session,
                    limit=limit,
                    offset=offset
                )
            
            return await self.response.success({
                "reports": reports,
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "has_more": len(reports) == limit
                }
            }, event=event)
            
        except Exception as e:
            logger.error(f"Error listing reports: {e}", exc_info=True)
            return await self.response.error(str(e), 400, event=event)
    
    @measure_time
    async def handle_resolve_report(self, event: Dict, user: Dict, report_id: str) -> Dict:
        """POST /feed/reports/{reportId}/resolve - разрешить жалобу (админ) - ЕДИНАЯ СЕССИЯ"""
        try:
            user_role = self._get_user_role(event)
            
            if user_role not in ['admin', 'moderator']:
                return await self.response.error("Insufficient permissions", 403, event=event)
            
            logger.info(f"✅ Resolving report {report_id}")
            
            if not validate_uuid(report_id):
                return await self.response.error("Invalid report ID format", 400, event=event)
            
            body = self._parse_body(event)
            action = body.get('action', 'resolved')
            delete_content = body.get('delete_content', False)
            
            async with RequestContext() as ctx:
                result = await self.report_service.resolve(
                    session=ctx.session,
                    report_id=report_id,
                    moderator_id=user['user_id'],
                    action=action,
                    delete_content=delete_content
                )
            
            return await self.response.success(result, event=event)
            
        except Exception as e:
            return await self.response.error(str(e), 404 if "not found" in str(e).lower() else 400, event=event)
    
    # ============================================
    # GET /feed/info - информация
    # ============================================
    
    async def handle_info(self, event: Dict, user: Dict) -> Dict:
        """GET /feed/info - информация о сервисе"""
        return await self.response.success({
            "service": "Feed Service",
            "version": "6.2.0",
            "features": [
                "Курсорная пагинация",
                "Batch запросы",
                "Фоновые задачи",
                "Кэширование",
                "Graceful shutdown"
            ],
            "endpoints": [
                "GET /feed/for-you",
                "GET /feed/following", 
                "GET /feed/popular",
                "GET /feed/fresh",
                "POST /feed",
                "GET /feed/posts/{id}",
                "POST /feed/like",
                "POST /feed/bookmark",
                "GET /feed/bookmarks",
                "POST /feed/repost",
                "GET /feed/reposts",
                "POST /feed/comments",
                "GET /feed/comments",
                "POST /feed/follow",
                "GET /feed/followers",
                "GET /feed/following",
                "GET /feed/user/profile",
                "GET /feed/user/posts",
                "GET /feed/user/likes",
                "GET /feed/notifications",
                "GET /feed/search",
                "GET /feed/trending",
                "GET /feed/mentions",
                "POST /feed/reactions",
                "GET /feed/hashtags/search"
            ]
        }, event=event)
    
    async def handle_health(self, event: Dict) -> Dict:
        """GET /health - проверка здоровья"""
        
        health_status = {
            "status": "ok",
            "version": "6.2.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "services": {}
        }
        
        try:
            async with RequestContext() as ctx:
                await ctx.session.transaction().execute("SELECT 1;")
            health_status["services"]["database"] = "ok"
        except Exception as e:
            health_status["services"]["database"] = f"error: {e}"
            health_status["status"] = "degraded"
        
        try:
            await cache.set("health:test", "ok", ttl=1)
            test_value = await cache.get("health:test")
            if test_value == "ok":
                health_status["services"]["cache"] = "ok"
            else:
                health_status["services"]["cache"] = "unhealthy"
                health_status["status"] = "degraded"
        except Exception as e:
            health_status["services"]["cache"] = f"error: {e}"
            health_status["status"] = "degraded"
        
        health_status["services"]["worker"] = background_worker.get_stats()
        
        if storage.client:
            health_status["services"]["storage"] = "ok"
        else:
            health_status["services"]["storage"] = "not configured"
        
        health_status["metrics"] = await Metrics.get_metrics()
        health_status["session_metrics"] = await SessionMetrics.get_stats()
        
        return await self.response.success(health_status, event=event)

# ============================================
# ЭКСПОРТ
# ============================================

feed_handler = FeedHandler()

__all__ = ['feed_handler']


