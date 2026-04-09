"""
Логирование для микросервисов
Стандартизированное логирование с поддержкой структурированного формата
"""
import logging
import json
import sys
from typing import Optional, Dict, Any
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """Форматтер для вывода логов в JSON формате"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Добавляем exception если есть
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Добавляем дополнительные атрибуты
        for key, value in record.__dict__.items():
            if key not in ('name', 'msg', 'args', 'created', 'filename', 'funcName',
                          'levelname', 'levelno', 'lineno', 'module', 'msecs',
                          'pathname', 'process', 'processName', 'relativeCreated',
                          'stack_info', 'exc_info', 'thread', 'threadName'):
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str)


def setup_logging(
    service_name: str,
    level: int = logging.INFO,
    json_format: bool = True,
    include_console: bool = True
) -> logging.Logger:
    """
    Настроить логирование для сервиса
    
    Args:
        service_name: Имя сервиса для идентификации в логах
        level: Уровень логирования
        json_format: Использовать JSON формат (True) или текстовый (False)
        include_console: Включить вывод в консоль
    
    Returns:
        Настроенный logger
    """
    logger = logging.getLogger(service_name)
    logger.setLevel(level)
    
    # Очищаем существующие handlers
    logger.handlers.clear()
    
    # Создаём formatter
    if json_format:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            f'%(asctime)s - {service_name} - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # Добавляем console handler
    if include_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized for {service_name}")
    return logger


# Глобальная функция для быстрого получения logger
def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Получить logger с указанным именем"""
    return logging.getLogger(name)
