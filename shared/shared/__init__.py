"""
Shared library for microservices
Common utilities, middleware, and base classes
"""

__version__ = "1.0.0"

from shared.shared.utils import (
    ResponseBuilder, response,
    AppError, ValidationError, AuthError, ForbiddenError,
    PermissionError, NotFoundError, RateLimitError, ConflictError,
    ServiceUnavailableError, error_handler,
    LRUCache, cached, default_cache,
    Validator, validator,
)
from shared.shared.middleware import (
    AuthMiddleware, auth,
    setup_logging, get_logger, JSONFormatter,
)
from shared.shared.handlers import BaseHandler

__all__ = [
    # Utils
    'ResponseBuilder', 'response',
    'AppError', 'ValidationError', 'AuthError', 'ForbiddenError',
    'PermissionError', 'NotFoundError', 'RateLimitError', 'ConflictError',
    'ServiceUnavailableError', 'error_handler',
    'LRUCache', 'cached', 'default_cache',
    'Validator', 'validator',
    # Middleware
    'AuthMiddleware', 'auth',
    'setup_logging', 'get_logger', 'JSONFormatter',
    # Handlers
    'BaseHandler',
]
