"""
Валидаторы для проверки данных
Проверяют содержание данных, не меняют их формат
"""
import re
from typing import Optional, List, Any
from datetime import datetime, timedelta

from utils.errors import ValidationError
from config import config


class BaseValidator:
    """Базовый валидатор с общими методами"""
    
    @staticmethod
    def not_empty(value: Any, field_name: str):
        """Проверить что значение не пустое"""
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValidationError(f"{field_name} cannot be empty")
    
    @staticmethod
    def length(value: str, min_len: int, max_len: int, field_name: str):
        """Проверить длину строки"""
        if value and (len(value) < min_len or len(value) > max_len):
            raise ValidationError(
                f"{field_name} must be between {min_len} and {max_len} characters"
            )
    
    @staticmethod
    def range(value: int, min_val: int, max_val: int, field_name: str):
        """Проверить диапазон числа"""
        if value is not None and (value < min_val or value > max_val):
            raise ValidationError(
                f"{field_name} must be between {min_val} and {max_val}"
            )
    
    @staticmethod
    def regex(value: str, pattern: str, field_name: str):
        """Проверить по регулярному выражению"""
        if value and not re.match(pattern, value):
            raise ValidationError(f"{field_name} has invalid format")


class ChatValidator(BaseValidator):
    """Валидатор для чатов"""
    
    def validate_create(self, title: str, chat_type: str, max_members: int):
        """Валидация создания чата"""
        self.not_empty(title, "Title")
        self.length(title, 3, 255, "Title")
        
        valid_types = ['private', 'group', 'channel']
        if chat_type not in valid_types:
            raise ValidationError(f"Chat type must be one of: {valid_types}")
        
        self.range(max_members, 2, 10000, "Max members")
    
    def validate_update(self, updates: dict):
        """Валидация обновления чата"""
        allowed_fields = ['title', 'description', 'avatar_url', 'is_public', 
                         'join_moderation', 'max_members', 'slow_mode_interval']
        
        for field in updates:
            if field not in allowed_fields:
                raise ValidationError(f"Cannot update field: {field}")
        
        if 'title' in updates:
            self.length(updates['title'], 3, 255, "Title")
        
        if 'max_members' in updates:
            self.range(updates['max_members'], 2, 10000, "Max members")
        
        if 'slow_mode_interval' in updates:
            self.range(updates['slow_mode_interval'], 0, 3600, "Slow mode interval")


class MessageValidator(BaseValidator):
    """Валидатор для сообщений"""
    
    def validate_send(self, content: Optional[str], 
                      message_type: str,
                      attachments: Optional[List],
                      reply_to: Optional[int]):
        """Валидация отправки сообщения"""
        
        # Проверка типа сообщения
        valid_types = ['text', 'image', 'video', 'file', 'voice', 'sticker', 'gif']
        if message_type not in valid_types:
            raise ValidationError(f"Message type must be one of: {valid_types}")
        
        # Для текстовых сообщений нужен контент
        if message_type == 'text':
            if not content:
                raise ValidationError("Text message must have content")
            self.length(content, 1, 10000, "Content")
        
        # Для сообщений с вложениями нужны attachments
        if message_type in ['image', 'video', 'file']:
            if not attachments or len(attachments) == 0:
                raise ValidationError(f"{message_type} message must have attachments")
            if len(attachments) > 10:
                raise ValidationError("Cannot attach more than 10 files")
        
        # Проверка reply_to (если есть)
        if reply_to is not None and reply_to <= 0:
            raise ValidationError("Invalid reply_to message ID")
    
    def validate_edit(self, new_content: str):
        """Валидация редактирования"""
        if not new_content:
            raise ValidationError("Edited content cannot be empty")
        self.length(new_content, 1, 10000, "Content")
    
    def validate_reaction(self, reaction: str):
        """Валидация реакции"""
        if not reaction:
            raise ValidationError("Reaction cannot be empty")
        if len(reaction) > 10:
            raise ValidationError("Reaction too long")
        
        # Опционально: проверка что это валидный emoji
        # emoji_pattern = re.compile("[\U00010000-\U0010ffff]")
        # if not emoji_pattern.match(reaction):
        #     raise ValidationError("Reaction must be an emoji")


class ParticipantValidator(BaseValidator):
    """Валидатор для участников"""
    
    def validate_role_change(self, current_role: str, 
                             target_current_role: str,
                             new_role: str):
        """Валидация смены роли"""
        
        valid_roles = ['owner', 'admin', 'moderator', 'member']
        if new_role not in valid_roles:
            raise ValidationError(f"Invalid role: {new_role}")
        
        # Иерархия ролей
        role_level = {
            'owner': 4,
            'admin': 3,
            'moderator': 2,
            'member': 1
        }
        
        current_level = role_level.get(current_role, 0)
        target_level = role_level.get(target_current_role, 0)
        new_level = role_level.get(new_role, 0)
        
        # Проверка прав
        if current_role == 'owner':
            # Owner может все
            pass
        elif current_role == 'admin':
            if target_level >= current_level:
                raise ValidationError("Cannot change role of users with equal or higher role")
            if new_level >= current_level:
                raise ValidationError("Cannot assign role equal to or higher than your own")
        else:
            raise ValidationError("You don't have permission to change roles")


class InviteValidator(BaseValidator):
    """Валидатор для приглашений"""
    
    def validate_create(self, expires_in_hours: int, max_uses: int, default_role: str):
        """Валидация создания приглашения"""
        
        self.range(expires_in_hours, 1, 720, "Expiration time")  # до 30 дней
        
        self.range(max_uses, 0, 1000, "Max uses")
        
        valid_roles = ['member', 'moderator', 'admin']
        if default_role not in valid_roles:
            raise ValidationError(f"Default role must be one of: {valid_roles}")
        
        if default_role == 'owner':
            raise ValidationError("Cannot create invites with owner role")


class BanValidator(BaseValidator):
    """Валидатор для банов"""
    
    def validate_ban(self, ban_type: str, duration_minutes: Optional[int], permanent: bool):
        """Валидация бана"""
        
        valid_types = ['ban', 'mute', 'kick', 'warning']
        if ban_type not in valid_types:
            raise ValidationError(f"Ban type must be one of: {valid_types}")
        
        if not permanent:
            if duration_minutes is None:
                raise ValidationError("Duration required for non-permanent ban")
            self.range(duration_minutes, 1, 43200, "Duration")  # до 30 дней


class AttachmentValidator(BaseValidator):
    """Валидатор для вложений"""
    
    # Допустимые MIME типы
    ALLOWED_IMAGE_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'image/webp']
    ALLOWED_VIDEO_TYPES = ['video/mp4', 'video/webm', 'video/quicktime']
    ALLOWED_FILE_TYPES = ['application/pdf', 'text/plain', 'application/zip']
    
    # Максимальные размеры (в байтах)
    MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10 MB
    MAX_VIDEO_SIZE = 100 * 1024 * 1024  # 100 MB
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
    
    def validate_upload(self, file_size: int, mime_type: str, file_type: str):
        """Валидация загрузки файла"""
        
        if file_type == 'image':
            if mime_type not in self.ALLOWED_IMAGE_TYPES:
                raise ValidationError(f"Unsupported image type: {mime_type}")
            if file_size > self.MAX_IMAGE_SIZE:
                raise ValidationError(f"Image too large. Max {self.MAX_IMAGE_SIZE//1024//1024}MB")
        
        elif file_type == 'video':
            if mime_type not in self.ALLOWED_VIDEO_TYPES:
                raise ValidationError(f"Unsupported video type: {mime_type}")
            if file_size > self.MAX_VIDEO_SIZE:
                raise ValidationError(f"Video too large. Max {self.MAX_VIDEO_SIZE//1024//1024}MB")
        
        elif file_type == 'file':
            if mime_type not in self.ALLOWED_FILE_TYPES:
                raise ValidationError(f"Unsupported file type: {mime_type}")
            if file_size > self.MAX_FILE_SIZE:
                raise ValidationError(f"File too large. Max {self.MAX_FILE_SIZE//1024//1024}MB")
        
        else:
            raise ValidationError(f"Unsupported file type: {file_type}")


class ContactValidator(BaseValidator):
    """Валидатор для контактов"""
    
    def validate_contact_id(self, contact_id: str):
        """Валидация ID контакта"""
        self.not_empty(contact_id, "Contact ID")
        # ID пользователя должен быть UUID или число
        if not re.match(r'^[0-9a-f-]{36}$|^\d+$', contact_id):
            raise ValidationError("Invalid contact ID format")
    
    def validate_phone(self, phone: Optional[str]):
        """Валидация телефона"""
        if phone:
            # Простая проверка формата телефона
            phone_pattern = r'^\+?[0-9]{10,15}$'
            if not re.match(phone_pattern, phone):
                raise ValidationError("Invalid phone number format")


class IdempotencyValidator(BaseValidator):
    """Валидатор для ключей идемпотентности"""
    
    def validate_key(self, key: Optional[str]):
        """Валидация ключа идемпотентности"""
        if key:
            if len(key) > 255:
                raise ValidationError("Idempotency key too long")
            # Ключ должен быть безопасным для использования в URL
            if not re.match(r'^[A-Za-z0-9\-_]+$', key):
                raise ValidationError("Invalid idempotency key format")


# Глобальные экземпляры для удобства
chat_validator = ChatValidator()
message_validator = MessageValidator()
participant_validator = ParticipantValidator()
invite_validator = InviteValidator()
ban_validator = BanValidator()
attachment_validator = AttachmentValidator()
contact_validator = ContactValidator()
idempotency_validator = IdempotencyValidator()
