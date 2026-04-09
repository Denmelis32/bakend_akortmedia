"""
Работа с Yandex Object Storage (S3-совместимое хранилище)
Для загрузки и управления файлами (аватары, вложения)
С поддержкой fallback режима при недоступности
"""
import os
import boto3
import uuid
import mimetypes
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectTimeoutError
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class StorageService:
    """
    Сервис для работы с Object Storage
    Поддерживает загрузку, удаление и получение файлов
    С fallback режимом - если storage недоступен, возвращает заглушки
    """
    
    def __init__(self):
        """Инициализация клиента S3"""
        self.client = None
        self.available = False
        self._init_attempted = False
        
        # Получаем конфигурацию из переменных окружения
        self.endpoint = os.environ.get('OBJECT_STORAGE_ENDPOINT')
        self.access_key = os.environ.get('OBJECT_STORAGE_ACCESS_KEY')
        self.secret_key = os.environ.get('OBJECT_STORAGE_SECRET_KEY')
        self.region = os.environ.get('OBJECT_STORAGE_REGION')
        self.bucket = os.environ.get('OBJECT_STORAGE_BUCKET')
        self.public_url = os.environ.get('OBJECT_STORAGE_PUBLIC_URL')
        
        # Пытаемся инициализировать
        self._init_client()
    
    def _init_client(self):
        """Инициализация клиента с обработкой ошибок"""
        if self._init_attempted:
            return
        
        self._init_attempted = True
        
        try:
            # Проверяем обязательные переменные
            required_vars = {
                'OBJECT_STORAGE_ENDPOINT': self.endpoint,
                'OBJECT_STORAGE_ACCESS_KEY': self.access_key,
                'OBJECT_STORAGE_SECRET_KEY': self.secret_key,
                'OBJECT_STORAGE_REGION': self.region,
                'OBJECT_STORAGE_BUCKET': self.bucket,
                'OBJECT_STORAGE_PUBLIC_URL': self.public_url
            }
            
            missing = [name for name, value in required_vars.items() if not value]
            if missing:
                logger.warning(f"⚠️ Missing Object Storage env vars: {', '.join(missing)}. Running in fallback mode.")
                self.available = False
                return
            
            # Создаем сессию и клиент
            self.session = boto3.session.Session()
            self.client = self.session.client(
                's3',
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
                config=Config(
                    s3={'addressing_style': 'virtual'},
                    retries={'max_attempts': 3, 'mode': 'standard'},
                    connect_timeout=5,  # Уменьшил таймаут
                    read_timeout=10
                )
            )
            
            # Проверяем доступ к бакету
            self._check_bucket()
            
            logger.info("✅ Object Storage initialized successfully")
            logger.info(f"📦 Bucket: {self.bucket}")
            logger.info(f"📦 Endpoint: {self.endpoint}")
            
        except ConnectTimeoutError:
            logger.error(f"❌ Object Storage connection timeout. Running in fallback mode.")
            self.client = None
            self.available = False
        except Exception as e:
            logger.error(f"❌ Failed to initialize Object Storage: {e}. Running in fallback mode.")
            self.client = None
            self.available = False
    
    def _check_bucket(self):
        """Проверить доступ к бакету"""
        if not self.client:
            return
            
        try:
            self.client.head_bucket(Bucket=self.bucket)
            logger.info(f"✅ Bucket '{self.bucket}' is accessible")
            self.available = True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404':
                logger.error(f"❌ Bucket '{self.bucket}' does not exist")
            elif error_code == '403':
                logger.error(f"❌ No permission to access bucket '{self.bucket}'")
            else:
                logger.error(f"❌ Cannot access bucket '{self.bucket}': {e}")
            self.available = False
        except Exception as e:
            logger.error(f"❌ Error checking bucket: {e}")
            self.available = False
    
    def _generate_key(self, folder: str, user_id: str, filename: str) -> str:
        """
        Сгенерировать ключ для файла
        Формат: {folder}/{user_id}/{YYYY/MM/DD}/{uuid}/{filename}
        """
        now = datetime.utcnow()
        date_path = now.strftime('%Y/%m/%d')
        file_uuid = str(uuid.uuid4())
        
        # Очищаем filename от опасных символов
        safe_filename = "".join(c for c in filename if c.isalnum() or c in '._- ').rstrip()
        if not safe_filename:
            safe_filename = f"file_{file_uuid[:8]}"
        
        return f"{folder}/{user_id}/{date_path}/{file_uuid}/{safe_filename}"
    
    def upload_file(
        self,
        file_data: bytes,
        filename: str,
        folder: str = 'uploads',
        user_id: Optional[str] = None,
        content_type: Optional[str] = None,
        metadata: Optional[Dict] = None,
        make_public: bool = True
    ) -> Dict[str, Any]:
        """
        Загрузить файл в storage
        
        Если storage недоступен, возвращает заглушку с локальным URL
        """
        # Fallback режим - возвращаем заглушку
        if not self.available or not self.client:
            logger.warning(f"⚠️ Storage unavailable, using fallback for {filename}")
            
            # Генерируем заглушку URL
            file_id = str(uuid.uuid4())
            fake_url = f"/placeholder/{folder}/{file_id}/{filename}"
            
            return {
                'url': fake_url,
                'preview_url': fake_url,
                'key': f"placeholder/{file_id}",
                'filename': filename,
                'size': len(file_data),
                'content_type': content_type or 'application/octet-stream',
                'folder': folder,
                'uploaded_at': datetime.utcnow().isoformat(),
                'is_public': True,
                'is_placeholder': True  # Флаг, что это заглушка
            }
        
        try:
            if not file_data:
                raise ValueError("File data is empty")
            
            # Определяем content type если не указан
            if not content_type:
                content_type, _ = mimetypes.guess_type(filename)
                if not content_type:
                    content_type = 'application/octet-stream'
            
            # Генерируем ключ
            user_id = user_id or 'system'
            key = self._generate_key(folder, user_id, filename)
            
            # Подготавливаем метаданные
            file_metadata = {
                'uploaded_by': user_id,
                'uploaded_at': datetime.utcnow().isoformat(),
                'original_filename': filename,
                'content_type': content_type,
                'folder': folder
            }
            if metadata:
                file_metadata.update(metadata)
            
            # Параметры загрузки
            upload_params = {
                'Bucket': self.bucket,
                'Key': key,
                'Body': file_data,
                'ContentType': content_type,
                'Metadata': file_metadata
            }
            
            # Добавляем ACL если нужно публичный доступ
            if make_public:
                upload_params['ACL'] = 'public-read'
            
            # Загружаем файл
            self.client.put_object(**upload_params)
            
            # Формируем URL
            if make_public:
                url = f"{self.public_url}/{key}"
                preview_url = url
            else:
                # Для приватных файлов будем генерировать временные ссылки
                url = self.generate_presigned_url(key)
                preview_url = None
            
            logger.info(f"✅ File uploaded successfully: {key} ({len(file_data)} bytes)")
            
            return {
                'url': url,
                'preview_url': preview_url,
                'key': key,
                'filename': filename,
                'size': len(file_data),
                'content_type': content_type,
                'folder': folder,
                'uploaded_at': file_metadata['uploaded_at'],
                'is_public': make_public,
                'is_placeholder': False
            }
            
        except ClientError as e:
            logger.error(f"❌ S3 upload error: {e}")
            file_id = str(uuid.uuid4())
            fake_url = f"/placeholder/{folder}/{file_id}/{filename}"
            return {
                'url': fake_url,
                'preview_url': fake_url,
                'key': f"placeholder/{file_id}",
                'filename': filename,
                'size': len(file_data),
                'content_type': content_type or 'application/octet-stream',
                'folder': folder,
                'uploaded_at': datetime.utcnow().isoformat(),
                'is_public': True,
                'is_placeholder': True
            }
        except Exception as e:
            logger.error(f"❌ Unexpected upload error: {e}")
            file_id = str(uuid.uuid4())
            fake_url = f"/placeholder/{folder}/{file_id}/{filename}"
            return {
                'url': fake_url,
                'preview_url': fake_url,
                'key': f"placeholder/{file_id}",
                'filename': filename,
                'size': len(file_data),
                'content_type': content_type or 'application/octet-stream',
                'folder': folder,
                'uploaded_at': datetime.utcnow().isoformat(),
                'is_public': True,
                'is_placeholder': True
            }
    
    def delete_file(self, url: str) -> bool:
        """
        Удалить файл по URL
        В fallback режиме всегда возвращает True
        """
        if not self.available or not self.client:
            logger.warning(f"⚠️ Storage unavailable, ignoring delete for {url}")
            return True
        
        try:
            # Извлекаем ключ из URL
            if not url.startswith(self.public_url):
                logger.warning(f"⚠️ Invalid URL format: {url}")
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
                logger.warning(f"⚠️ File not found: {url}")
                return False
            else:
                logger.error(f"❌ S3 delete error: {e}")
                return False
        except Exception as e:
            logger.error(f"❌ Unexpected delete error: {e}")
            return False
    
    def delete_file_by_key(self, key: str) -> bool:
        """Удалить файл по ключу"""
        if not self.available or not self.client:
            return True
        
        try:
            self.client.delete_object(
                Bucket=self.bucket,
                Key=key
            )
            logger.info(f"✅ File deleted: {key}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to delete file {key}: {e}")
            return False
    
    def get_file_url(self, key: str) -> str:
        """Получить публичный URL файла по ключу"""
        if not self.available:
            return f"/placeholder/{key}"
        return f"{self.public_url}/{key}"
    
    def generate_presigned_url(self, key: str, expires_in: int = 3600) -> Optional[str]:
        """
        Сгенерировать временную подпись для доступа к приватному файлу
        """
        if not self.available or not self.client:
            return None
        
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket,
                    'Key': key
                },
                ExpiresIn=expires_in
            )
            return url
        except Exception as e:
            logger.error(f"❌ Failed to generate presigned URL: {e}")
            return None
    
    def file_exists(self, key: str) -> bool:
        """Проверить существование файла по ключу"""
        if not self.available or not self.client:
            return False
        
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
    
    def get_file_info(self, key: str) -> Optional[Dict[str, Any]]:
        """Получить информацию о файле по ключу"""
        if not self.available or not self.client:
            return {
                'key': key,
                'url': self.get_file_url(key),
                'is_placeholder': True
            }
        
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=key)
            
            # Парсим метаданные
            metadata = response.get('Metadata', {})
            
            return {
                'key': key,
                'size': response.get('ContentLength', 0),
                'content_type': response.get('ContentType', 'application/octet-stream'),
                'etag': response.get('ETag', '').strip('"'),
                'last_modified': response.get('LastModified').isoformat() if response.get('LastModified') else None,
                'metadata': metadata,
                'url': self.get_file_url(key),
                'is_placeholder': False
            }
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') != 'NoSuchKey':
                logger.error(f"❌ Failed to get file info for {key}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error getting file info for {key}: {e}")
            return None
    
    def list_files(self, prefix: str, max_keys: int = 100) -> List[Dict[str, Any]]:
        """
        Получить список файлов в папке
        """
        if not self.available or not self.client:
            return []
        
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
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'etag': obj['ETag'].strip('"'),
                    'url': self.get_file_url(obj['Key'])
                })
            
            return files
            
        except Exception as e:
            logger.error(f"❌ Failed to list files with prefix '{prefix}': {e}")
            return []
    
    def copy_file(self, source_key: str, destination_key: str) -> Optional[str]:
        """
        Скопировать файл внутри бакета
        """
        if not self.available or not self.client:
            return self.get_file_url(destination_key)
        
        try:
            copy_source = {'Bucket': self.bucket, 'Key': source_key}
            
            self.client.copy_object(
                Bucket=self.bucket,
                Key=destination_key,
                CopySource=copy_source,
                ACL='public-read'
            )
            
            logger.info(f"✅ File copied: {source_key} -> {destination_key}")
            return self.get_file_url(destination_key)
            
        except Exception as e:
            logger.error(f"❌ Failed to copy file {source_key}: {e}")
            return None


# Глобальный экземпляр для использования во всем приложении
storage = StorageService()
