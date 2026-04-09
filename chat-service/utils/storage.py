"""
Работа с Yandex Object Storage (S3-совместимое хранилище)
Для загрузки и управления файлами (аватары, вложения)
"""
import boto3
import uuid
from botocore.config import Config
from botocore.exceptions import ClientError
from typing import Optional, Dict, Any, List
from datetime import datetime
from config.config import Config as AppConfig
from utils.errors import AppError
from middleware.logging import logger

# Создаем экземпляр конфигурации приложения
app_config = AppConfig()

class StorageService:
    """
    Сервис для работы с Object Storage
    Поддерживает загрузку, удаление и получение файлов
    """
    
    def __init__(self):
        """Инициализация клиента S3"""
        try:
            self.session = boto3.session.Session()
            self.client = self.session.client(
                's3',
                endpoint_url=app_config.OBJECT_STORAGE_ENDPOINT,
                aws_access_key_id=app_config.OBJECT_STORAGE_ACCESS_KEY,
                aws_secret_access_key=app_config.OBJECT_STORAGE_SECRET_KEY,
                region_name=app_config.OBJECT_STORAGE_REGION,
                config=Config(
                    s3={'addressing_style': 'virtual'},
                    retries={'max_attempts': 3, 'mode': 'standard'}
                )
            )
            self.bucket = app_config.OBJECT_STORAGE_BUCKET
            self.public_url = app_config.OBJECT_STORAGE_PUBLIC_URL
            
            # Проверяем доступ к бакету
            self._check_bucket()
            
            logger.info("✅ Object Storage initialized")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Object Storage: {e}", exc_info=True)
            # Не падаем, но логируем ошибку
    
    def _check_bucket(self):
        """Проверить доступ к бакету"""
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404':
                logger.warning(f"Bucket {self.bucket} does not exist")
            else:
                logger.warning(f"Cannot access bucket {self.bucket}: {e}")
    
    def _generate_key(self, chat_id: Optional[int], user_id: str, filename: str) -> str:
        """
        Сгенерировать ключ для файла
        Формат: chats/{chat_id}/{YYYY/MM/DD}/{uuid}/{filename}
        """
        now = datetime.utcnow()
        date_path = now.strftime('%Y/%m/%d')
        file_uuid = str(uuid.uuid4())
        
        if chat_id:
            return f"chats/{chat_id}/{date_path}/{file_uuid}/{filename}"
        else:
            return f"temp/{user_id}/{date_path}/{file_uuid}/{filename}"
    
    def upload_file(
        self,
        file_data: bytes,
        content_type: str,
        filename: str,
        chat_id: Optional[int] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Загрузить файл в storage
        Возвращает информацию о загруженном файле
        """
        try:
            # Генерируем ключ
            key = self._generate_key(chat_id, user_id or 'system', filename)
            
            # Подготавливаем метаданные
            file_metadata = {
                'uploaded_by': str(user_id) if user_id else 'system',
                'uploaded_at': datetime.utcnow().isoformat(),
                'original_filename': filename,
                'content_type': content_type
            }
            if metadata:
                file_metadata.update(metadata)
            
            # Загружаем файл
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=file_data,
                ContentType=content_type,
                Metadata=file_metadata,
                ACL='public-read'  # Файлы публично доступны
            )
            
            # Формируем URL
            url = f"{self.public_url}/{key}"
            
            # Для изображений генерируем preview URL (можно добавить обработку изображений)
            preview_url = None
            if content_type.startswith('image/'):
                # В реальном проекте здесь может быть генерация превью через ImageMagick
                preview_url = url  # пока используем тот же URL
            
            logger.info(f"✅ File uploaded: {key}, size: {len(file_data)} bytes")
            
            return {
                'url': url,
                'preview_url': preview_url,
                'key': key,
                'file_name': filename,
                'file_size': len(file_data),
                'mime_type': content_type,
                'uploaded_at': datetime.utcnow().isoformat()
            }
            
        except ClientError as e:
            logger.error(f"❌ S3 upload error: {e}", exc_info=True)
            raise AppError(f"Failed to upload file: {e.response.get('Error', {}).get('Message', str(e))}")
        except Exception as e:
            logger.error(f"❌ Unexpected upload error: {e}", exc_info=True)
            raise AppError("Failed to upload file")
    
    def delete_file(self, url: str) -> bool:
        """
        Удалить файл по URL
        Возвращает True если успешно, False если файл не найден
        """
        try:
            # Извлекаем ключ из URL
            if not url.startswith(self.public_url):
                logger.warning(f"Invalid URL format: {url}")
                return False
            
            key = url.replace(f"{self.public_url}/", "")
            
            # Удаляем файл
            self.client.delete_object(
                Bucket=self.bucket,
                Key=key
            )
            
            logger.info(f"✅ File deleted: {key}")
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'NoSuchKey':
                logger.warning(f"File not found: {url}")
                return False
            else:
                logger.error(f"❌ S3 delete error: {e}", exc_info=True)
                return False
        except Exception as e:
            logger.error(f"❌ Unexpected delete error: {e}", exc_info=True)
            return False
    
    def get_file_url(self, key: str) -> str:
        """Получить публичный URL файла по ключу"""
        return f"{self.public_url}/{key}"
    
    def generate_presigned_url(self, key: str, expires_in: int = 3600) -> Optional[str]:
        """
        Сгенерировать временную подпись для доступа к приватному файлу
        (если файл не публичный)
        """
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': key},
                ExpiresIn=expires_in
            )
            return url
        except Exception as e:
            logger.error(f"❌ Failed to generate presigned URL: {e}")
            return None
    
    def file_exists(self, key: str) -> bool:
        """Проверить существование файла"""
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
    
    def get_file_info(self, key: str) -> Optional[Dict]:
        """Получить информацию о файле"""
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=key)
            return {
                'key': key,
                'url': self.get_file_url(key),
                'size': response.get('ContentLength'),
                'content_type': response.get('ContentType'),
                'metadata': response.get('Metadata', {}),
                'last_modified': response.get('LastModified').isoformat() if response.get('LastModified') else None,
                'etag': response.get('ETag', '').strip('"')
            }
        except ClientError:
            return None
        except Exception as e:
            logger.error(f"❌ Failed to get file info: {e}")
            return None
    
    def copy_file(self, source_key: str, destination_key: str) -> bool:
        """Копировать файл внутри бакета"""
        try:
            copy_source = {'Bucket': self.bucket, 'Key': source_key}
            self.client.copy_object(
                Bucket=self.bucket,
                Key=destination_key,
                CopySource=copy_source,
                ACL='public-read'
            )
            logger.info(f"✅ File copied: {source_key} -> {destination_key}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to copy file: {e}")
            return False
    
    def list_files(self, prefix: str, max_keys: int = 100) -> List[Dict]:
        """Список файлов с определенным префиксом"""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            for obj in response.get('Contents', []):
                files.append({
                    'key': obj['Key'],
                    'url': self.get_file_url(obj['Key']),
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'etag': obj['ETag'].strip('"')
                })
            
            return files
        except Exception as e:
            logger.error(f"❌ Failed to list files: {e}")
            return []


# Глобальный экземпляр
storage = StorageService()
