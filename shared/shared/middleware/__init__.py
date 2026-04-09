"""Shared middleware"""

from shared.shared.middleware.auth import AuthMiddleware, auth
from shared.shared.middleware.logging import setup_logging, get_logger, JSONFormatter

__all__ = [
    'AuthMiddleware', 'auth',
    'setup_logging', 'get_logger', 'JSONFormatter',
]
