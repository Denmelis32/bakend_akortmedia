"""
Структурированное логирование в JSON формате
Для Yandex Cloud - все логи автоматически собираются в Yandex Logging
"""
import logging
import json
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional

class StructuredLogger:
    """
    Логгер со структурированным выводом в JSON
    Каждая запись - отдельный JSON объект
    """
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Отключаем propagate чтобы не дублировать в root-логгер
        self.logger.propagate = False

        # Добавляем handler если его нет
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
    
    def _log(self, level: str, message: str, **kwargs):
        """
        Внутренний метод логирования
        Формирует JSON и отправляет в лог
        """
        record = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'level': level.upper(),
            'message': message,
            'service': 'chat-service',
            **kwargs
        }
        
        # Добавляем traceback если есть ошибка
        if 'exc_info' in kwargs and kwargs['exc_info']:
            record['traceback'] = traceback.format_exc()
            # Убираем exc_info из kwargs чтобы не дублировать
            kwargs.pop('exc_info')
        
        # Убираем None значения для чистоты
        record = {k: v for k, v in record.items() if v is not None}
        
        # Отправляем в лог
        getattr(self.logger, level.lower())(json.dumps(record))
    
    def info(self, message: str, **kwargs):
        """Информационное сообщение"""
        self._log('INFO', message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Сообщение об ошибке"""
        self._log('ERROR', message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Предупреждение"""
        self._log('WARNING', message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Отладочное сообщение (обычно не используется в проде)"""
        self._log('DEBUG', message, **kwargs)


# Функция для настройки логирования при старте
def setup_logging():
    """Настройка корневого логгера"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Убираем дублирующие handlers
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)
    
    # Добавляем handler для stdout
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    root_logger.addHandler(handler)
    
    return StructuredLogger('chat')


# Глобальный экземпляр логгера
logger = setup_logging()

