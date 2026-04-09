"""
FEED SERVICE v6.2.0 - FastAPI версия для виртуальной машины с WebSocket
"""
import os
import uuid
import json
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

from fastapi import FastAPI, Request, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from app.config import config
from app.middleware.logging import logger
from app.middleware.auth import auth
from app.middleware.metrics import metrics
from app.utils.response import response
from app.utils.errors import AppError, AuthError, error_handler

from app.handlers.feed_handler import FeedHandler
from app.handlers.common import (
    initialize_common,
    shutdown_common,
    background_worker,
    WebSocketManager,
    RequestContext,  # 👈 ВАЖНО: добавлен этот импорт!
    cache,
    Metrics,
    SessionMetrics,
    send_ws
)

# Создаем экземпляр хендлера
feed_handler = FeedHandler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения
    """
    # Startup
    logger.info("🚀 Starting Feed Service...")
    try:
        await initialize_common()
        logger.info("✅ Feed Service started successfully")
    except Exception as e:
        logger.error(f"❌ Failed to start Feed Service: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down Feed Service...")
    try:
        await shutdown_common()
        logger.info("✅ Feed Service stopped successfully")
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")


# Создаем FastAPI приложение
app = FastAPI(
    title="Feed Service",
    description="Social Media Feed Service with WebSocket support",
    version="6.2.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# WEBSOCKET
# ============================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket соединение для real-time уведомлений"""
    await websocket.accept()
    connection_id = str(uuid.uuid4())
    user_id = None
    
    try:
        # Ждем первое сообщение с аутентификацией
        data = await websocket.receive_text()
        try:
            auth_data = json.loads(data)
            token = auth_data.get('token')
            
            if token:
                # Валидируем токен
                payload = auth.verify_token(token)
                user_id = payload.get('sub')
                
                if not user_id:
                    await websocket.send_json({
                        'type': 'error',
                        'data': {'message': 'Invalid token'}
                    })
                    await websocket.close()
                    return
                
                # Регистрируем соединение
                success = await WebSocketManager.register(connection_id, user_id, websocket)
                if not success:
                    await websocket.send_json({
                        'type': 'error',
                        'data': {'message': 'Too many connections'}
                    })
                    await websocket.close()
                    return
                
                await websocket.send_json({
                    'type': 'connected',
                    'data': {
                        'connection_id': connection_id,
                        'user_id': user_id
                    }
                })
                
                # Отправляем количество непрочитанных уведомлений
                from app.handlers.feed_handler import NotificationRepository
                async with RequestContext() as ctx:  # 👈 Теперь работает!
                    notifications_repo = NotificationRepository(ctx.session)
                    unread_count = await notifications_repo.count_unread(user_id)
                    
                    await websocket.send_json({
                        'type': 'unread_count',
                        'data': {'count': unread_count}
                    })
                
                logger.info(f"✅ WebSocket connected: {connection_id} for user {user_id}")
                
                # Основной цикл получения сообщений
                while True:
                    message = await websocket.receive_text()
                    # Обрабатываем входящие сообщения если нужно
                    try:
                        data = json.loads(message)
                        if data.get('type') == 'ping':
                            await websocket.send_json({'type': 'pong'})
                    except:
                        pass
                        
            else:
                await websocket.send_json({
                    'type': 'error',
                    'data': {'message': 'Token required'}
                })
                await websocket.close()
                
        except json.JSONDecodeError:
            await websocket.send_json({
                'type': 'error',
                'data': {'message': 'Invalid JSON'}
            })
            await websocket.close()
            
    except WebSocketDisconnect:
        logger.info(f"📴 WebSocket disconnected: {connection_id}")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
    finally:
        if user_id:
            await WebSocketManager.unregister(connection_id)


# ============================================
# HEALTH CHECK
# ============================================

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    uptime = (datetime.utcnow() - startup_time).total_seconds() if 'startup_time' in globals() else 0
    
    health_status = {
        "status": "ok",
        "service": "feed-service",
        "version": "6.2.0",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "uptime_seconds": uptime,
        "services": {}
    }
    
    # Проверка БД
    try:
        from app.db.pool import ydb_pool
        import asyncio
        def _check_db():
            with ydb_pool.acquire() as session:
                session.transaction().execute("SELECT 1;", commit_tx=True)
        await asyncio.get_event_loop().run_in_executor(None, _check_db)
        health_status["services"]["database"] = "ok"
    except Exception as e:
        health_status["services"]["database"] = f"error: {e}"
        health_status["status"] = "degraded"
    
    # Проверка кэша
    try:
        await cache.set("health:test", "ok", ttl=1)
        test_value = await cache.get("health:test")
        health_status["services"]["cache"] = "ok" if test_value == "ok" else "unhealthy"
    except Exception as e:
        health_status["services"]["cache"] = f"error: {e}"
        health_status["status"] = "degraded"
    
    # WebSocket статистика
    try:
        ws_stats = await WebSocketManager.get_stats()
        health_status["services"]["websocket"] = ws_stats
    except Exception as e:
        health_status["services"]["websocket"] = f"error: {e}"
    
    # Статистика воркера
    health_status["services"]["worker"] = background_worker.get_stats()
    
    # Метрики
    health_status["metrics"] = await Metrics.get_metrics()
    health_status["session_metrics"] = await SessionMetrics.get_stats()
    
    return health_status


# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================

def get_user_from_request(request: Request) -> Dict:
    """
    Извлечь пользователя из запроса (для FastAPI)
    """
    # Получаем заголовки
    headers = dict(request.headers)
    
    # Создаем event-like объект для совместимости с auth.get_user_from_request
    event = {
        'headers': headers,
        'queryStringParameters': dict(request.query_params)
    }
    
    try:
        user = auth.get_user_from_request(event)
        return user
    except AuthError as e:
        raise HTTPException(status_code=401, detail=str(e))


# ============================================
# МАРШРУТЫ
# ============================================

@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {
        "service": "Feed Service",
        "version": "6.2.0",
        "docs": "/api/docs",
        "websocket": "/ws"
    }


@app.get("/feed/info")
async def get_info(user: Dict = Depends(get_user_from_request)):
    """Информация о сервисе"""
    result = await feed_handler.handle_info({}, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/for-you")
async def for_you_feed(
    limit: int = 20,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Персонализированная лента"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_for_you_feed(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/following")
async def following_feed(
    limit: int = 20,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Лента подписок"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_following_feed(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/popular")
async def popular_feed(
    limit: int = 20,
    days: int = 7,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Популярные посты"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'days': str(days),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_popular_feed(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/fresh")
async def fresh_feed(
    limit: int = 20,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Свежие посты"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_fresh_feed(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed")
async def feed_with_type(
    type: str = 'for_you',
    limit: int = 20,
    days: int = 7,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Лента с выбором типа"""
    event = {
        'queryStringParameters': {
            'type': type,
            'limit': str(limit),
            'days': str(days),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_feed_with_type(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed")
async def create_post(request: Request, user: Dict = Depends(get_user_from_request)):
    """Создать пост"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_create_post(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 201),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/posts/{post_id}")
async def get_post(post_id: str, user: Dict = Depends(get_user_from_request)):
    """Получить пост по ID"""
    event = {}
    result = await feed_handler.handle_get_post(event, user, post_id)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.put("/feed/posts/{post_id}")
async def update_post(
    post_id: str,
    request: Request,
    user: Dict = Depends(get_user_from_request)
):
    """Обновить пост"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_update_post(event, user, post_id)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.delete("/feed/posts/{post_id}")
async def delete_post(
    post_id: str,
    permanent: bool = False,
    user: Dict = Depends(get_user_from_request)
):
    """Удалить пост"""
    event = {
        'queryStringParameters': {
            'permanent': str(permanent).lower()
        }
    }
    result = await feed_handler.handle_delete_post(event, user, post_id)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/like")
async def toggle_like(request: Request, user: Dict = Depends(get_user_from_request)):
    """Поставить или убрать лайк"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_toggle_like(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/bookmark")
async def toggle_bookmark(request: Request, user: Dict = Depends(get_user_from_request)):
    """Добавить или удалить закладку"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_toggle_bookmark(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/bookmarks")
async def list_bookmarks(
    limit: int = 20,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Список закладок"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_list_bookmarks(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/repost")
async def create_repost(request: Request, user: Dict = Depends(get_user_from_request)):
    """Создать репост"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_create_repost(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 201),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.delete("/feed/repost/{repost_id}")
async def delete_repost(repost_id: str, user: Dict = Depends(get_user_from_request)):
    """Удалить репост"""
    event = {}
    result = await feed_handler.handle_delete_repost(event, user, repost_id)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/reposts")
async def get_reposts(
    original_post_id: str,
    limit: int = 20,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Получить репосты поста"""
    event = {
        'queryStringParameters': {
            'original_post_id': original_post_id,
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_get_reposts(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/reactions")
async def toggle_reaction(request: Request, user: Dict = Depends(get_user_from_request)):
    """Переключить реакцию"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_toggle_reaction(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/reactions")
async def get_reactions(
    entity_type: str,
    entity_id: str,
    limit: int = 20,
    offset: int = 0,
    user: Dict = Depends(get_user_from_request)
):
    """Получить список реакций"""
    event = {
        'queryStringParameters': {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'limit': str(limit),
            'offset': str(offset)
        }
    }
    result = await feed_handler.handle_get_reactions(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/reactions/counts")
async def get_reaction_counts(
    entity_type: str,
    entity_id: str,
    user: Dict = Depends(get_user_from_request)
):
    """Получить счетчики реакций"""
    event = {
        'queryStringParameters': {
            'entity_type': entity_type,
            'entity_id': entity_id
        }
    }
    result = await feed_handler.handle_get_reaction_counts(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/comments")
async def create_comment(request: Request, user: Dict = Depends(get_user_from_request)):
    """Создать комментарий"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_create_comment(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 201),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/comments")
async def get_post_comments(
    post_id: str,
    limit: int = 20,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Получить комментарии к посту"""
    event = {
        'queryStringParameters': {
            'post_id': post_id,
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_get_post_comments(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/follow")
async def toggle_follow(request: Request, user: Dict = Depends(get_user_from_request)):
    """Подписаться или отписаться"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_toggle_follow(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/followers")
async def get_followers(
    user_id: Optional[str] = None,
    limit: int = 20,
    cursor: Optional[str] = None,
    current_user: Dict = Depends(get_user_from_request)
):
    """Получить подписчиков"""
    target_id = user_id or current_user['user_id']
    event = {
        'queryStringParameters': {
            'user_id': target_id,
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_get_followers(event, current_user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )
@app.get("/feed/comments/{comment_id}/replies")
async def get_comment_replies(
    comment_id: str,
    limit: int = 10,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Получить ответы на комментарий (ленивая загрузка)"""
    event = {
        'pathParameters': {'comment_id': comment_id},  # ← передаём в pathParameters
        'queryStringParameters': {
            'limit': str(limit),
            'cursor': cursor
        }
    }
    # 👇 ВАЖНО: передаём comment_id как отдельный аргумент
    result = await feed_handler.handle_get_comment_replies(event, user, comment_id)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )
@app.get("/feed/prefetch/{feed_type}")
async def get_prefetched_page(
    feed_type: str,
    cursor: str,
    user: Dict = Depends(get_user_from_request)
):
    """Получить предзагруженную страницу ленты"""
    event = {
        'pathParameters': {'feed_type': feed_type},
        'queryStringParameters': {'cursor': cursor}
    }
    result = await feed_handler.handle_get_prefetched_page(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )
@app.get("/feed/following")
async def get_following(
    user_id: Optional[str] = None,
    limit: int = 20,
    cursor: Optional[str] = None,
    current_user: Dict = Depends(get_user_from_request)
):
    """Получить подписки"""
    target_id = user_id or current_user['user_id']
    event = {
        'queryStringParameters': {
            'user_id': target_id,
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_get_following(event, current_user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/user/profile")
async def get_profile(
    user_id: Optional[str] = None,
    current_user: Dict = Depends(get_user_from_request)
):
    """Получить профиль пользователя"""
    target_id = user_id or current_user['user_id']
    event = {
        'queryStringParameters': {
            'user_id': target_id
        }
    }
    result = await feed_handler.handle_get_profile(event, current_user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/user/posts")
async def get_user_posts(
    user_id: Optional[str] = None,
    type: str = 'all',
    limit: int = 20,
    cursor: Optional[str] = None,
    current_user: Dict = Depends(get_user_from_request)
):
    """Получить посты пользователя"""
    target_id = user_id or current_user['user_id']
    event = {
        'queryStringParameters': {
            'user_id': target_id,
            'type': type,
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_get_user_posts(event, current_user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/user/likes")
async def get_user_likes(
    user_id: Optional[str] = None,
    limit: int = 20,
    cursor: Optional[str] = None,
    current_user: Dict = Depends(get_user_from_request)
):
    """Получить посты, которые лайкнул пользователь"""
    target_id = user_id or current_user['user_id']
    event = {
        'queryStringParameters': {
            'user_id': target_id,
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_get_user_likes(event, current_user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/notifications")
async def get_notifications(
    limit: int = 20,
    cursor: Optional[str] = None,
    unread_only: bool = False,
    user: Dict = Depends(get_user_from_request)
):
    """Получить уведомления"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'cursor': cursor,
            'unread_only': str(unread_only).lower()
        }
    }
    result = await feed_handler.handle_get_notifications(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/notifications/read")
async def mark_notifications_read(request: Request, user: Dict = Depends(get_user_from_request)):
    """Отметить уведомления как прочитанные"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_mark_notifications_read(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/search")
async def search_posts(
    q: str,
    limit: int = 20,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Поиск постов"""
    event = {
        'queryStringParameters': {
            'q': q,
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_search(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/trending")
async def get_trending(
    limit: int = 10,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Получить популярный контент"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_trending(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/mentions")
async def get_mentions(
    limit: int = 50,
    cursor: Optional[str] = None,
    unread_only: bool = False,
    user: Dict = Depends(get_user_from_request)
):
    """Получить упоминания"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'cursor': cursor,
            'unread_only': str(unread_only).lower()
        }
    }
    result = await feed_handler.handle_get_mentions(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/mentions/count")
async def get_mentions_count(user: Dict = Depends(get_user_from_request)):
    """Количество непрочитанных упоминаний"""
    event = {}
    result = await feed_handler.handle_get_mentions_count(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/mentions/read")
async def mark_mention_read(request: Request, user: Dict = Depends(get_user_from_request)):
    """Отметить упоминание как прочитанное"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_mark_mention_read(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/mentions/read-all")
async def mark_all_mentions_read(user: Dict = Depends(get_user_from_request)):
    """Отметить все упоминания как прочитанные"""
    event = {}
    result = await feed_handler.handle_mark_all_mentions_read(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/hashtags/search")
async def search_hashtag(
    q: str,
    limit: int = 20,
    cursor: Optional[str] = None,
    user: Dict = Depends(get_user_from_request)
):
    """Поиск по хештегу"""
    event = {
        'queryStringParameters': {
            'q': q,
            'limit': str(limit),
            'cursor': cursor
        }
    }
    result = await feed_handler.handle_search_hashtag(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/hashtags/suggest")
async def suggest_hashtags(
    prefix: str,
    limit: int = 10,
    user: Dict = Depends(get_user_from_request)
):
    """Автодополнение хештегов"""
    event = {
        'queryStringParameters': {
            'prefix': prefix,
            'limit': str(limit)
        }
    }
    result = await feed_handler.handle_suggest_hashtags(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/hashtags/related")
async def related_hashtags(
    q: str,
    limit: int = 10,
    user: Dict = Depends(get_user_from_request)
):
    """Связанные хештеги"""
    event = {
        'queryStringParameters': {
            'q': q,
            'limit': str(limit)
        }
    }
    result = await feed_handler.handle_related_hashtags(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/users/popular")
async def get_popular_users(
    limit: int = 20,
    offset: int = 0,
    user: Dict = Depends(get_user_from_request)
):
    """Популярные пользователи"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'offset': str(offset)
        }
    }
    result = await feed_handler.handle_popular_users(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/users/suggestions")
async def get_user_suggestions(
    limit: int = 20,
    offset: int = 0,
    user: Dict = Depends(get_user_from_request)
):
    """Рекомендации пользователей"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'offset': str(offset)
        }
    }
    result = await feed_handler.handle_user_suggestions(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/users/similar")
async def get_similar_users(
    user_id: str,
    limit: int = 20,
    current_user: Dict = Depends(get_user_from_request)
):
    """Похожие пользователи"""
    event = {
        'queryStringParameters': {
            'user_id': user_id,
            'limit': str(limit)
        }
    }
    result = await feed_handler.handle_similar_users(event, current_user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/report")
async def create_report(request: Request, user: Dict = Depends(get_user_from_request)):
    """Создать жалобу"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_create_report(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 201),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.get("/feed/reports")
async def list_pending_reports(
    limit: int = 20,
    offset: int = 0,
    user: Dict = Depends(get_user_from_request)
):
    """Список ожидающих жалоб (админ)"""
    event = {
        'queryStringParameters': {
            'limit': str(limit),
            'offset': str(offset)
        }
    }
    result = await feed_handler.handle_list_pending_reports(event, user)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/reports/{report_id}/resolve")
async def resolve_report(
    report_id: str,
    request: Request,
    user: Dict = Depends(get_user_from_request)
):
    """Разрешить жалобу (админ)"""
    body = await request.json()
    event = {'body': body}
    result = await feed_handler.handle_resolve_report(event, user, report_id)
    return JSONResponse(
        status_code=result.get('statusCode', 200),
        content=json.loads(result['body']) if result.get('body') else {}
    )


@app.post("/feed/channels/{channel_id}/messages/{message_id}/publish")
async def publish_channel_post(
    channel_id: str,
    message_id: str,
    request: Request,
    user: Dict = Depends(get_user_from_request)
):
    """Опубликовать сообщение из канала в ленту"""
    body = await request.json()
    event = {'body': body, 'headers': dict(request.headers)}
    result = await feed_handler.handle_publish_channel_post(event, user, int(channel_id), int(message_id))
    raw_body = result.get('body')
    if isinstance(raw_body, bytes):
        import gzip as _gzip
        raw_body = _gzip.decompress(raw_body).decode('utf-8')
    content = json.loads(raw_body) if raw_body else {}
    return JSONResponse(
        status_code=int(result.get('statusCode', 201)),
        content=content
    )


@app.post("/feed/posts/{post_id}/recalc-comments")
async def recalc_post_comments(
    post_id: str,
    user: Dict = Depends(get_user_from_request)
):
    """
    Пересчитать comments_count и replies_count для поста и всех его комментариев.
    Использовать для починки старых данных.
    """
    from app.handlers.feed_handler import CommentRepository, PostRepository, RequestContext
    import uuid as _uuid

    async with RequestContext() as ctx:
        session = ctx.session
        comments_repo = CommentRepository(session)
        posts_repo = PostRepository(session)

        # Считаем ВСЕ живые комментарии поста (любой глубины)
        query = f"""
        DECLARE $post_id AS Utf8;
        SELECT comment_id, parent_comment_id
        FROM feed_comments
        WHERE post_id = $post_id AND is_deleted = false;
        """
        rows = await comments_repo.execute(query, {'$post_id': post_id})

        # Строим карту parent→children
        children_map: dict = {}
        all_ids = set()
        for row in rows:
            cid = row['comment_id']
            pid = row.get('parent_comment_id') or ''
            all_ids.add(cid)
            children_map.setdefault(pid, []).append(cid)

        total_count = len(all_ids)

        # Пересчитываем replies_count для каждого комментария
        def count_subtree(comment_id: str) -> int:
            children = children_map.get(comment_id, [])
            return sum(1 + count_subtree(c) for c in children)

        # Обновляем replies_count для каждого комментария
        updated_replies = 0
        for cid in all_ids:
            new_replies = count_subtree(cid)
            upd_query = f"""
            DECLARE $cid AS Utf8;
            DECLARE $cnt AS Uint32;
            UPDATE feed_comments SET replies_count = $cnt WHERE comment_id = $cid;
            """
            await comments_repo.execute(upd_query, {'$cid': cid, '$cnt': new_replies})
            updated_replies += 1

        # Обновляем comments_count у поста
        upd_post_query = f"""
        DECLARE $post_id AS Utf8;
        DECLARE $cnt AS Uint32;
        UPDATE feed_posts SET comments_count = $cnt WHERE post_id = $post_id;
        """
        await posts_repo.execute(upd_post_query, {'$post_id': post_id, '$cnt': total_count})

        # Инвалидируем кэши
        from app.handlers.feed_handler import CommentCache, PostCache
        await CommentCache.invalidate(post_id)
        await PostCache.invalidate(post_id)

    return JSONResponse(content={
        "success": True,
        "post_id": post_id,
        "comments_count": total_count,
        "comments_updated": updated_replies,
    })


# Сохраняем время запуска для health check
startup_time = datetime.utcnow()


# ============================================
# ЗАПУСК (для прямого запуска)
# ============================================

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=config.HOST,
        port=config.PORT,
        reload=False,
        workers=4,
        log_level="info"
    )
