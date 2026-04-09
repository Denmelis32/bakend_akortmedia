# Shared Library

Общая библиотека для микросервисов, содержащая переиспользуемые компоненты.

## Структура

```
shared/
├── utils/          # Утилиты
│   ├── response.py    # Построитель HTTP ответов
│   ├── errors.py      # Кастомные ошибки
│   ├── cache.py       # LRU кэш с TTL
│   └── validators.py  # Валидаторы данных
├── middleware/     # Middleware компоненты
│   ├── auth.py        # JWT аутентификация
│   └── logging.py     # Настройка логирования
├── handlers/       # Базовые классы хендлеров
│   └── base.py        # BaseHandler
└── __init__.py     # Экспорт API
```

## Установка

Добавьте путь к shared в PYTHONPATH или установите как package:

```bash
export PYTHONPATH="/workspace/shared:$PYTHONPATH"
```

Или используйте pip install -e:

```bash
cd /workspace/shared && pip install -e .
```

## Использование

### Импорт компонентов

```python
from shared.shared import (
    # Utils
    response, ResponseBuilder,
    AppError, ValidationError, AuthError, NotFoundError,
    LRUCache, cached,
    validator, Validator,
    
    # Middleware
    auth, AuthMiddleware,
    setup_logging,
    
    # Handlers
    BaseHandler,
)
```

### Пример использования ResponseBuilder

```python
from shared.shared import response

# Успешный ответ
return response.success({'key': 'value'})

# Ответ с ошибкой
return response.error('Not found', 'not_found', 404)

# Пагинированный ответ
return response.paginated(items, total, page=0, page_size=50)
```

### Пример использования ошибок

```python
from shared.shared import AppError, ValidationError, NotFoundError

try:
    if not user:
        raise NotFoundError("User not found")
    if invalid_data:
        raise ValidationError("Invalid input")
except AppError as e:
    return response.error(e.message, e.code, e.status_code)
```

### Пример использования кэша

```python
from shared.shared import LRUCache, cached

# Создание кэша с TTL 5 минут и размером 1000 элементов
cache = LRUCache(max_size=1000, ttl=300)

# Использование декоратора
@cached(cache, key_prefix='user')
def get_user(user_id):
    # expensive operation
    return db.query(...)

# Ручное управление
cache.set('key', value)
value = cache.get('key')
cache.delete('key')
```

### Пример использования валидаторов

```python
from shared.shared import validator, ValidationError

# Валидация email
if not validator.validate_email(email):
    raise ValidationError("Invalid email")

# Валидация username
is_valid, error = validator.validate_username(username)
if not is_valid:
    raise ValidationError(error)

# Санитизация строки
clean_text = validator.sanitize_string(user_input, max_length=500)

# Пагинация
page, page_size = validator.validate_pagination(page, page_size)
```

### Пример использования BaseHandler

```python
from shared.shared import BaseHandler, response

class MyHandler(BaseHandler):
    
    @BaseHandler.safe_handler
    def handle_request(self, event):
        # Аутентификация
        user = self.authenticate(event)
        
        # Получение параметров
        item_id = self.get_path_param(event, 'id')
        data = self.get_body(event)
        
        # Бизнес логика
        result = self.process_item(item_id, data, user)
        
        return response.success(result)
    
    def process_item(self, item_id, data, user):
        # Ваша логика
        return {'id': item_id, 'processed': True}

# Использование
handler = MyHandler()
result = handler.handle_request(event)
```

### Пример настройки логирования

```python
from shared.shared import setup_logging

# JSON формат для production
logger = setup_logging('my-service', json_format=True)

# Текстовый формат для development
logger = setup_logging('my-service', json_format=False)
```

## API Reference

### Utils

#### ResponseBuilder
- `success(data, status_code=200, headers=None)` - успешный ответ
- `error(message, code='error', status_code=400, headers=None)` - ответ с ошибкой
- `paginated(items, total, page=0, page_size=50, **kwargs)` - пагинированный ответ
- `options()` - CORS preflight ответ
- `redirect(url, status_code=302)` - редирект

#### Errors
- `AppError` - базовый класс ошибок
- `ValidationError` - ошибка валидации (400)
- `AuthError` - ошибка аутентификации (401)
- `ForbiddenError` - ошибка доступа (403)
- `NotFoundError` - ресурс не найден (404)
- `RateLimitError` - превышен лимит (429)
- `ConflictError` - конфликт данных (409)
- `ServiceUnavailableError` - сервис недоступен (503)

#### LRUCache
- `get(key)` - получить значение
- `set(key, value)` - установить значение
- `delete(key)` - удалить значение
- `clear()` - очистить кэш
- `stats()` - статистика кэша
- `cleanup_expired()` - очистка просроченных элементов

#### Validator
- `validate_email(email)` - валидация email
- `validate_username(username)` - валидация username
- `validate_password(password)` - валидация пароля
- `validate_uuid(uuid_str)` - валидация UUID
- `sanitize_string(value, max_length)` - санитизация строки
- `validate_pagination(page, page_size)` - валидация пагинации

### Middleware

#### AuthMiddleware
- `verify_token(token)` - проверка JWT токена
- `get_user_from_request(event)` - извлечение пользователя из запроса
- `require_role(user, required_role)` - проверка роли
- `is_admin(user)` - проверка на админа

#### Logging
- `setup_logging(service_name, level, json_format, include_console)` - настройка логирования
- `get_logger(name, level)` - получение logger

### Handlers

#### BaseHandler
- `handle_request(event)` - обработка запроса (переопределяется)
- `authenticate(event)` - аутентификация пользователя
- `require_auth(func)` - декоратор требования аутентификации
- `require_role(role)` - декоратор проверки роли
- `safe_handler(func)` - декоратор безопасной обработки
- `get_query_param(event, name, default)` - получение query параметра
- `get_body(event)` - получение тела запроса
- `get_path_param(event, name, default)` - получение path параметра
- `get_header(event, name, default)` - получение заголовка
