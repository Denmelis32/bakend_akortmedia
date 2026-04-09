"""
PRODUCTION CHAT SERVICE v2.1 - FULL WEB SERVER WITH WEBSOCKET
Главный entry point для запуска на виртуальной машине
Все эндпоинты + подробное логирование WebSocket с поддержкой подписок
"""

import asyncio
import json
import uuid
import os
from typing import Dict, Any, Optional, List
from datetime import datetime
from contextlib import asynccontextmanager

# FastAPI
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import uvicorn

# Наши модули
from config import config
from db.pool import init_db_pool_async, close_db_pool
from middleware.auth import auth
from middleware.logging import logger, setup_logging
from utils.response import response

# Импорты обработчиков
from handlers.chat_handler import chat_handler
from handlers.message_handler import message_handler
from handlers.common import (
    start_workers, stop_workers,
    SessionMetrics, cache, notification_worker,
    RequestContext, TransactionMonitor, WebSocketManager
)

# ============================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================
setup_logging()

# ============================================
# СОЗДАНИЕ FASTAPI ПРИЛОЖЕНИЯ
# ============================================
app = FastAPI(
    title="Chat Service API",
    description="Real-time chat service with WebSocket support",
    version="2.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# ============================================
# CORS
# ============================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# ============================================
start_time = datetime.utcnow()
request_count = 0

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================

def get_client_ip(request: Request) -> str:
    """Получить IP клиента"""
    if request.client:
        return request.client.host
    headers = {k.lower(): v for k, v in request.headers.items()}
    return headers.get(
        'x-real-ip',
        headers.get('x-forwarded-for', 'unknown')
    ).split(',')[0].strip()

def normalize_event(request: Request, body: Any = None) -> Dict:
    """Конвертирует FastAPI request в формат event для хендлеров"""
    path_params = dict(request.path_params)
    query_params = dict(request.query_params)
    headers = dict(request.headers)
    
    event = {
        "httpMethod": request.method,
        "path": request.url.path,
        "pathParameters": path_params,
        "queryStringParameters": query_params,
        "headers": headers,
        "requestContext": {
            "identity": {
                "sourceIp": get_client_ip(request)
            }
        }
    }
    
    if body:
        event["body"] = json.dumps(body)
    
    return event

def safe_int(value: Any) -> Optional[int]:
    """Безопасное преобразование в int"""
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

async def get_current_user(request: Request) -> Dict:
    """Получить пользователя из токена"""
    global request_count
    request_count += 1
    
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        return {"user_id": "anonymous", "role": "anonymous", "is_authenticated": False}
    
    token = auth_header.split(" ")[1]
    
    try:
        user_data = auth.verify_token(token)
        if user_data:
            return {
                "user_id": user_data.get('sub'),
                "username": user_data.get('username'),
                "first_name": user_data.get('first_name', ''),
                "role": user_data.get('role', 'user'),
                "is_verified": user_data.get('verified', False),
                "is_authenticated": True
            }
    except Exception as e:
        logger.error(f"Auth error: {e}")
    
    return {"user_id": "anonymous", "role": "anonymous", "is_authenticated": False}

async def get_authenticated_user(request: Request) -> Dict:
    """Только для аутентифицированных"""
    user = await get_current_user(request)
    if not user.get("is_authenticated"):
        raise HTTPException(status_code=401, detail="Authentication required")
    return user

# ============================================
# LIFESPAN
# ============================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом"""
    global start_time
    start_time = datetime.utcnow()
    
    logger.info("=" * 60)
    logger.info("🔧 INITIALIZING CHAT SERVICE v2.1")
    logger.info("=" * 60)
    
    # Инициализация YDB
    try:
        logger.info("📦 Initializing YDB connection pool...")
        await init_db_pool_async()
        logger.info("✅ YDB initialized successfully")

        # Прогрев пула — захватываем и возвращаем 5 сессий заранее
        try:
            from db.pool import get_db_pool
            pool = get_db_pool()
            warmup_sessions = []
            for _ in range(5):
                s = await pool.get_session()
                warmup_sessions.append(s)
            for s in warmup_sessions:
                await pool.release_session(s)
            logger.info("✅ Session pool warmed up (5 sessions)")
        except Exception as e:
            logger.warning(f"⚠️ Session pool warmup failed: {e}")
    except Exception as e:
        logger.error(f"❌ Failed to initialize YDB: {e}", exc_info=True)
    
    # Запуск воркеров
    try:
        logger.info("🚀 Starting background workers...")
        await start_workers()
        await WebSocketManager.start_heartbeat()
        logger.info("✅ Background workers started")
    except Exception as e:
        logger.error(f"❌ Failed to start workers: {e}", exc_info=True)
    
    # Инициализация кэша
    try:
        if hasattr(cache, 'ensure_started'):
            await cache.ensure_started()
            logger.info("✅ Cache manager started")
    except Exception as e:
        logger.error(f"❌ Failed to start cache: {e}")
    
    # Запуск мониторинга транзакций
    try:
        TransactionMonitor.start_periodic_logging(interval=30)
        logger.info("✅ Transaction monitor started (logs every 30 seconds)")
    except Exception as e:
        logger.error(f"❌ Failed to start transaction monitor: {e}")
    
    logger.info("=" * 60)
    logger.info("✅ SERVICE READY")
    logger.info("=" * 60)
    
    yield
    
    # Завершение
    logger.info("=" * 60)
    logger.info("🛑 SHUTTING DOWN")
    logger.info("=" * 60)
    
    try:
        TransactionMonitor.stop_periodic_logging()
        logger.info("✅ Transaction monitor stopped")
    except Exception as e:
        logger.error(f"❌ Failed to stop transaction monitor: {e}")
    
    await stop_workers()
    await WebSocketManager.stop_heartbeat()
    await close_db_pool()
    
    logger.info("👋 Service stopped")
    logger.info("=" * 60)

app.router.lifespan_context = lifespan

# ============================================
# WEBSOCKET (С ПОДДЕРЖКОЙ ПОДПИСОК)
# ============================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket для real-time уведомлений с поддержкой подписок на чаты"""
    connection_id = f"conn_{uuid.uuid4().hex[:8]}"
    user_id = "anonymous"
    registered = False
    client_host = websocket.client.host if websocket.client else "unknown"
    
    logger.info(f"🔌 [WS:{connection_id}] New WebSocket connection attempt from {client_host}")
    await websocket.accept()
    logger.info(f"✅ [WS:{connection_id}] WebSocket connection accepted")
    
    try:
        # Аутентификация - ждем токен 5 секунд
        logger.info(f"🔐 [WS:{connection_id}] Waiting for authentication token (timeout=5s)")
        
        try:
            auth_message_text = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
            auth_message = json.loads(auth_message_text)
            token = auth_message.get("token")
            session_id = auth_message.get("session_id", connection_id)
            
            logger.info(f"📨 [WS:{connection_id}] Authentication message received, token length: {len(token) if token else 0}")
            
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ [WS:{connection_id}] Authentication timeout")
            await websocket.close(code=1008)
            return
        except json.JSONDecodeError as e:
            logger.warning(f"❌ [WS:{connection_id}] Invalid JSON: {e}")
            await websocket.send_json({"type": "error", "error": "Invalid JSON"})
            await websocket.close(code=1008)
            return
        
        if not token:
            logger.warning(f"❌ [WS:{connection_id}] No token provided")
            await websocket.send_json({"type": "error", "error": "Authentication required"})
            await websocket.close(code=1008)
            return
        
        # Валидируем токен
        logger.info(f"🔑 [WS:{connection_id}] Verifying token...")
        user_data = auth.verify_token(token)
        
        if not user_data:
            logger.warning(f"❌ [WS:{connection_id}] Invalid token")
            await websocket.send_json({"type": "error", "error": "Invalid token"})
            await websocket.close(code=1008)
            return
        
        user_id = user_data.get("sub")
        logger.info(f"✅ [WS:{connection_id}] Token verified for user: {user_id[:8]} (role: {user_data.get('role', 'unknown')})")
        
        # Создаем RequestContext на всё соединение
        logger.info(f"📦 [WS:{connection_id}] Creating RequestContext")
        async with RequestContext() as ctx:
            # Регистрация соединения
            logger.info(f"📝 [WS:{connection_id}] Registering connection with WebSocketManager")
            success = await WebSocketManager.register(connection_id, user_id, websocket)
            
            if not success:
                logger.warning(f"❌ [WS:{connection_id}] Too many connections for user {user_id[:8]}")
                await websocket.send_json({"type": "error", "error": "Too many connections"})
                await websocket.close(code=1008)
                return

            registered = True
            logger.info(f"✅ [WS:{connection_id}] Successfully registered")
            
            await websocket.send_json({
                "type": "connected",
                "connection_id": connection_id,
                "user_id": user_id
            })
            logger.info(f"📤 [WS:{connection_id}] Sent connection confirmation")
            
            # Уведомляем о подключении
            logger.info(f"📡 [WS:{connection_id}] Notifying service about user connection")
            await message_handler.service.user_connected(user_id, session_id)
            
            # Основной цикл
            message_count = 0
            logger.info(f"🔄 [WS:{connection_id}] Entering main message loop")
            
            while True:
                try:
                    message_text = await websocket.receive_text()
                    message_count += 1
                    
                    try:
                        message = json.loads(message_text)
                        msg_type = message.get("type", "unknown")
                        
                        logger.info(f"📨 [WS:{connection_id}] Received message #{message_count}: type={msg_type}")
                        
                        if msg_type == "typing":
                            chat_id = message.get("chat_id")
                            is_typing = message.get("is_typing", True)
                            logger.info(f"⌨️ [WS:{connection_id}] Typing: chat={chat_id}, is_typing={is_typing}")
                            
                            if chat_id:
                                if is_typing:
                                    await message_handler.service.set_typing(int(chat_id), user_id, session=ctx.session)
                                    logger.info(f"✅ [WS:{connection_id}] Typing started for chat {chat_id}")
                                else:
                                    await message_handler.service.stop_typing(int(chat_id), user_id)
                                    logger.info(f"✅ [WS:{connection_id}] Typing stopped for chat {chat_id}")
                            
                        elif msg_type == "ping":
                            await websocket.send_json({"type": "pong"})
                            await message_handler.service.update_user_activity(user_id)
                            
                        elif msg_type == "subscribe":
                            chat_id = message.get("chat_id")
                            if chat_id:
                                await WebSocketManager.subscribe_to_chat(connection_id, user_id, int(chat_id))
                                await websocket.send_json({
                                    "type": "subscribed",
                                    "chat_id": chat_id,
                                    "message": f"Subscribed to chat {chat_id}"
                                })
                                logger.info(f"📡 [WS:{connection_id}] Subscribed to chat {chat_id}")
                            else:
                                await websocket.send_json({
                                    "type": "error",
                                    "error": "chat_id is required for subscription"
                                })
                                logger.warning(f"⚠️ [WS:{connection_id}] Subscribe without chat_id")
                            
                        elif msg_type == "unsubscribe":
                            chat_id = message.get("chat_id")
                            if chat_id:
                                await WebSocketManager.unsubscribe_from_chat(connection_id, user_id, int(chat_id))
                                await websocket.send_json({
                                    "type": "unsubscribed",
                                    "chat_id": chat_id,
                                    "message": f"Unsubscribed from chat {chat_id}"
                                })
                                logger.info(f"📡 [WS:{connection_id}] Unsubscribed from chat {chat_id}")
                            else:
                                await websocket.send_json({
                                    "type": "error",
                                    "error": "chat_id is required for unsubscription"
                                })
                            
                        elif msg_type == "message":
                            chat_id = message.get("chat_id")
                            text = message.get("text")
                            logger.info(f"💬 [WS:{connection_id}] Message: chat={chat_id}, text='{text[:50] if text else ''}...'")
                            
                        elif msg_type == "read":
                            chat_id = message.get("chat_id")
                            msg_id = message.get("message_id")
                            logger.info(f"👁️ [WS:{connection_id}] Read receipt: chat={chat_id}, message={msg_id}")
                            
                        elif msg_type == "reaction":
                            chat_id = message.get("chat_id")
                            msg_id = message.get("message_id")
                            reaction = message.get("reaction")
                            logger.info(f"❤️ [WS:{connection_id}] Reaction: chat={chat_id}, message={msg_id}, reaction={reaction}")
                            
                        else:
                            logger.info(f"🤔 [WS:{connection_id}] Unknown message type: {msg_type}")
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"⚠️ [WS:{connection_id}] Invalid JSON: {e}")
                        await websocket.send_json({"type": "error", "error": "Invalid JSON"})
                        
                except WebSocketDisconnect:
                    logger.info(f"📴 [WS:{connection_id}] WebSocket disconnected")
                    break
                    
                except Exception as e:
                    logger.error(f"❌ [WS:{connection_id}] Error in message loop: {e}", exc_info=True)
                    try:
                        await websocket.send_json({"type": "error", "error": "Internal server error"})
                    except:
                        pass
                    break
                
    except WebSocketDisconnect:
        logger.info(f"📴 [WS:{connection_id}] WebSocket disconnected (outer)")
        
    except Exception as e:
        logger.error(f"❌ [WS:{connection_id}] WebSocket error: {e}", exc_info=True)
        
    finally:
        logger.info(f"🧹 [WS:{connection_id}] Cleaning up connection for user {user_id[:8]}")
        
        # Очищаем подписки пользователя
        await WebSocketManager.clear_user_subscriptions(user_id)
        
        await WebSocketManager.unregister(connection_id)

        if user_id != "anonymous" and registered:
            session_id = f"ws_{connection_id}"
            await message_handler.service.user_disconnected(user_id, session_id)
            logger.info(f"✅ [WS:{connection_id}] User {user_id[:8]} disconnected")
        
        logger.info(f"👋 [WS:{connection_id}] Connection closed")

# ============================================
# HEALTH CHECK
# ============================================

@app.get("/health", tags=["System"])
async def health_check():
    """Проверка работоспособности"""
    ws_stats = await WebSocketManager.get_stats()
    
    return {
        "success": True,
        "data": {
            "status": "ok",
            "service": "chat-service",
            "version": "2.1.0",
            "timestamp": datetime.utcnow().isoformat(),
            "uptime": (datetime.utcnow() - start_time).total_seconds(),
            "requests": request_count,
            "websocket": ws_stats
        }
    }

@app.get("/health/detailed", tags=["System"])
async def health_detailed():
    """Детальная проверка"""
    ws_stats = await WebSocketManager.get_stats()
    session_stats = SessionMetrics.get_stats()
    
    return {
        "success": True,
        "data": {
            "status": "ok",
            "service": "chat-service",
            "version": "2.1.0",
            "timestamp": datetime.utcnow().isoformat(),
            "uptime": (datetime.utcnow() - start_time).total_seconds(),
            "metrics": {
                "requests": request_count,
                "websocket": ws_stats,
                "sessions": session_stats
            }
        }
    }

# ============================================
# ТОКЕН ВАЛИДАЦИЯ
# ============================================

@app.post("/validate", tags=["Auth"])
async def validate_token(request: Request):
    """Проверка токена для WebSocket"""
    try:
        body = await request.json()
        token = body.get("token")
        
        if not token:
            return JSONResponse(
                status_code=200,
                content={"success": True, "data": {"valid": False}}
            )
        
        payload = auth.verify_token(token)
        
        if payload:
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "data": {
                        "valid": True,
                        "user": {
                            "user_id": payload.get("sub"),
                            "role": payload.get("role", "user")
                        }
                    }
                }
            )
        
        return JSONResponse(
            status_code=200,
            content={"success": True, "data": {"valid": False}}
        )
        
    except Exception as e:
        logger.error(f"Token validation error: {e}")
        return JSONResponse(
            status_code=200,
            content={"success": True, "data": {"valid": False}}
        )

# ============================================
# ПУБЛИЧНЫЕ ЭНДПОИНТЫ
# ============================================

@app.get("/", tags=["Public"])
async def root():
    """Корневой эндпоинт"""
    return {
        "success": True,
        "data": {
            "service": "Chat Service",
            "version": "2.1.0",
            "documentation": "/api/docs",
            "websocket": "/ws"
        }
    }

@app.get("/invites/{invite_code}", tags=["Public"])
async def get_invite(invite_code: str, request: Request, user: Dict = Depends(get_current_user)):
    """Получить информацию о приглашении"""
    event = normalize_event(request)
    event["pathParameters"]["inviteCode"] = invite_code
    
    result = await chat_handler.handle_get_invite(event, user, invite_code)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/join/{invite_code}", tags=["Public"])
async def join_by_link(invite_code: str, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Присоединиться по ссылке"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    event["pathParameters"]["inviteCode"] = invite_code
    
    result = await chat_handler.handle_join_by_link(event, user, invite_code)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

# ============================================
# ПОЛЬЗОВАТЕЛЬСКИЕ ЭНДПОИНТЫ
# ============================================

@app.get("/users/me/chats", tags=["Users"])
async def get_my_chats(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Получить чаты пользователя"""
    event = normalize_event(request)
    result = await chat_handler.handle_get_user_chats(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/me/chat-with/{target_user_id}", tags=["Users"])
async def get_or_create_private_chat(target_user_id: str, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Получить или создать приватный чат с пользователем"""
    try:
        user_id = user.get("user_id")
        target_user_id = target_user_id.strip()
        
        if not target_user_id:
            raise HTTPException(status_code=400, detail="target_user_id is required")
        
        if user_id == target_user_id:
            raise HTTPException(status_code=400, detail="Cannot create chat with yourself")
        
        logger.info(f"📦 Getting or creating private chat between {user_id[:8]} and {target_user_id[:8]}")
        
        async with RequestContext() as ctx:
            chat = await chat_handler.service.get_or_create_private_chat(
                user_id=user_id,
                recipient_id=target_user_id,
                session=ctx.session
            )
            
            participant = await chat_handler.service.participant_cache.get_participant(
                chat.id, user_id, session=ctx.session
            )
            chat._participant_info = participant
            
            logger.info(f"✅ Private chat returned: {chat.id}")
            
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "data": chat.to_dict()
                }
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting private chat: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/me/unread", tags=["Users"])
async def get_unread_counts(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Количество непрочитанных"""
    result = await chat_handler.handle_get_unread_counts({}, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/me/saved", tags=["Users"])
async def get_saved_messages(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Сохраненные сообщения"""
    event = normalize_event(request)
    result = await message_handler.handle_get_saved_messages(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/me/contacts", tags=["Users"])
async def get_contacts(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Получить контакты"""
    event = normalize_event(request)
    result = await message_handler.handle_get_contacts(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/users/me/contacts", tags=["Users"])
async def add_contact(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Добавить контакт"""
    body = await request.json()
    event = normalize_event(request, body)
    result = await message_handler.handle_add_contact(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.put("/users/me/contacts/{contact_id}", tags=["Users"])
async def update_contact(contact_id: str, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Обновить контакт"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["contactId"] = contact_id
    result = await message_handler.handle_update_contact(event, user, contact_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/users/me/contacts/{contact_id}", tags=["Users"])
async def delete_contact(contact_id: str, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Удалить контакт"""
    event = normalize_event(request)
    event["pathParameters"]["contactId"] = contact_id
    result = await message_handler.handle_delete_contact(event, user, contact_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/me/contacts/search", tags=["Users"])
async def search_contacts(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Поиск по контактам"""
    event = normalize_event(request)
    result = await message_handler.handle_search_contacts(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/me/blocks", tags=["Users"])
async def get_blocked_users(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Список заблокированных"""
    result = await message_handler.handle_get_blocked_users({}, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/users/me/blocks", tags=["Users"])
async def block_user(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Заблокировать пользователя"""
    body = await request.json()
    event = normalize_event(request, body)
    result = await message_handler.handle_block_user(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/users/me/blocks/{blocked_id}", tags=["Users"])
async def unblock_user(blocked_id: str, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Разблокировать"""
    event = normalize_event(request)
    event["pathParameters"]["blockedId"] = blocked_id
    result = await message_handler.handle_unblock_user(event, user, blocked_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/me/drafts", tags=["Users"])
async def get_drafts(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Черновики"""
    event = normalize_event(request)
    result = await message_handler.handle_get_all_drafts(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/me/join-requests", tags=["Users"])
async def get_join_requests(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Заявки на вступление"""
    event = normalize_event(request)
    result = await chat_handler.handle_get_my_join_requests(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/me/dialogs/hidden", tags=["Users"])
async def get_hidden_dialogs(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Скрытые диалоги"""
    event = normalize_event(request)
    result = await chat_handler.handle_get_hidden_dialogs(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/users/me/connected", tags=["Users"])
async def user_connected(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Пользователь подключился"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    result = await message_handler.handle_user_connected(event, user)
    return Response(status_code=204)

@app.post("/users/me/disconnected", tags=["Users"])
async def user_disconnected(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Пользователь отключился"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    result = await message_handler.handle_user_disconnected(event, user)
    return Response(status_code=204)

@app.get("/users/me/stats", tags=["Users"])
async def get_user_stats(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Статистика пользователя"""
    result = await message_handler.handle_get_user_stats({}, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/me/notification-settings", tags=["Users"])
async def get_notification_settings(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Настройки уведомлений"""
    result = await message_handler.handle_get_notification_settings({}, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.put("/users/me/notification-settings", tags=["Users"])
async def update_notification_settings(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Обновить настройки уведомлений"""
    body = await request.json()
    event = normalize_event(request, body)
    result = await message_handler.handle_update_notification_settings(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/{user_id}/online", tags=["Users"])
async def check_user_online(user_id: str, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Проверить онлайн статус"""
    result = await message_handler.handle_check_online({}, user, user_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/{user_id}/public-chats", tags=["Users"])
async def get_user_public_chats(user_id: str, request: Request, user: Dict = Depends(get_current_user)):
    """Публичные чаты пользователя"""
    result = await chat_handler.handle_get_user_public_chats({}, user, user_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

# ============================================
# ЧАТЫ
# ============================================

@app.get("/chats/search", tags=["Chats"])
async def search_chats(request: Request, user: Dict = Depends(get_current_user)):
    """Поиск чатов"""
    event = normalize_event(request)
    result = await chat_handler.handle_search_chats(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/public", tags=["Chats"])
async def get_public_chats(request: Request, user: Dict = Depends(get_current_user)):
    """Публичные чаты"""
    event = normalize_event(request)
    result = await chat_handler.handle_get_public_chats(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/by-username/{username}", tags=["Chats"])
async def get_chat_by_username(username: str, request: Request, user: Dict = Depends(get_current_user)):
    """Найти чат по username"""
    result = await chat_handler.handle_get_chat_by_username({}, user, username)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats", tags=["Chats"])
async def create_chat(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Создать чат"""
    body = await request.json()
    event = normalize_event(request, body)
    result = await chat_handler.handle_create_chat(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}", tags=["Chats"])
async def get_chat(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Получить информацию о чате"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.put("/chats/{chat_id}", tags=["Chats"])
async def update_chat(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Обновить чат"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_update_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}", tags=["Chats"])
async def delete_chat(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Удалить чат"""
    permanent = request.query_params.get("permanent", "false").lower() == "true"
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["queryStringParameters"] = {"permanent": str(permanent).lower()}
    result = await chat_handler.handle_delete_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/join", tags=["Chats"])
async def join_chat(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Присоединиться к чату"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_join_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/members/add", tags=["Chats"])
async def admin_add_member(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Добавить участника в чат (только владелец/админ)"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_admin_add_member(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/leave", tags=["Chats"])
async def leave_chat(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Покинуть чат"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_leave_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/archive", tags=["Chats"])
async def archive_chat(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Архивировать чат"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_archive_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/unarchive", tags=["Chats"])
async def unarchive_chat(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Разархивировать чат"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_unarchive_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/stats", tags=["Chats"])
async def get_chat_stats(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Статистика чата"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_chat_stats(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/members", tags=["Chats"])
async def get_chat_members(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Участники чата"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_chat_members(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/transfer-ownership", tags=["Chats"])
async def transfer_ownership(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Передать права"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_transfer_ownership(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/bans", tags=["Chats"])
async def get_bans(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Список банов"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_bans(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/bans", tags=["Chats"])
async def ban_user(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Забанить пользователя"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_ban_user(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/bans/{ban_id}", tags=["Chats"])
async def unban_user(chat_id: int, ban_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Разбанить"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["banId"] = str(ban_id)
    result = await chat_handler.handle_unban_user(event, user, chat_id, ban_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/invites", tags=["Chats"])
async def get_invites(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Список приглашений"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_list_invites(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/invites", tags=["Chats"])
async def create_invite(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Создать приглашение"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_create_invite(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/invites/{invite_id}", tags=["Chats"])
async def revoke_invite(chat_id: int, invite_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Отозвать приглашение"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["inviteId"] = str(invite_id)
    result = await chat_handler.handle_revoke_invite(event, user, chat_id, invite_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/username", tags=["Chats"])
async def set_username(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Установить username"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_set_username(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/events", tags=["Chats"])
async def get_events(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """События чата"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_chat_events(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/photos", tags=["Chats"])
async def upload_photos(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Загрузить фото"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_upload_photos(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 202),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/photos", tags=["Chats"])
async def get_chat_photos(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Получить фото чата"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_list_chat_photos(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/attachments", tags=["Chats"])
async def upload_attachment(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Загрузить вложение"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_upload_attachment(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/attachments/{attachment_id}", tags=["Chats"])
async def get_attachment(chat_id: int, attachment_id: str, request: Request, user: Dict = Depends(get_current_user)):
    """Получить вложение"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["attachmentId"] = attachment_id
    result = await message_handler.handle_get_attachment(event, user, chat_id, attachment_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/drafts", tags=["Chats"])
async def get_draft(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Получить черновик"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_get_draft(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/drafts", tags=["Chats"])
async def save_draft(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Сохранить черновик"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_save_draft(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/drafts", tags=["Chats"])
async def delete_draft(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Удалить черновик"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_delete_draft(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/read", tags=["Chats"])
async def mark_as_read(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Отметить как прочитанное"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_mark_as_read(event, user, chat_id)

    if result.get("statusCode") == 200:
        message_id = body.get("message_id")
        if message_id:
            asyncio.create_task(WebSocketManager.send_to_chat(
                chat_id,
                {
                    "type": "read_receipt",
                    "data": {
                        "chat_id": chat_id,
                        "user_id": user["user_id"],
                        "message_id": str(message_id),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                }
            ))

    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/read-all", tags=["Chats"])
async def mark_all_as_read(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Отметить все как прочитанное"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_mark_all_as_read(event, user, chat_id)

    if result.get("statusCode") == 200:
        try:
            result_data = json.loads(result.get("body", "{}")).get("data", {})
            last_message_id = result_data.get("last_message_id") or result_data.get("message_id")
            asyncio.create_task(WebSocketManager.send_to_chat(
                chat_id,
                {
                    "type": "read_receipt",
                    "data": {
                        "chat_id": chat_id,
                        "user_id": user["user_id"],
                        "message_id": str(last_message_id) if last_message_id else None,
                        "all_read": True,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                }
            ))
        except Exception as e:
            logger.warning(f"Failed to broadcast read-all receipt: {e}")

    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/dialog", tags=["Chats"])
async def delete_dialog(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Удалить диалог"""
    for_all = request.query_params.get("for_all", "false").lower() == "true"
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["queryStringParameters"] = {"for_all": str(for_all).lower()}
    result = await chat_handler.handle_delete_dialog(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/dialog/restore", tags=["Chats"])
async def restore_dialog(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Восстановить диалог"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_restore_dialog(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/pins", tags=["Chats"])
async def get_pinned_messages(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Закрепленные сообщения"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_pinned_messages(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/pins/reorder", tags=["Chats"])
async def reorder_pins(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Изменить порядок закрепленных"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_reorder_pins(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/pins", tags=["Chats"])
async def unpin_all(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Открепить все"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_unpin_all(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/my-permissions", tags=["Chats"])
async def get_my_permissions(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Мои права"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_my_permissions(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/members-with-permissions", tags=["Chats"])
async def get_members_with_permissions(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Участники с правами"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_list_members_with_permissions(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/members/{member_id}/permissions", tags=["Chats"])
async def get_member_permissions(chat_id: int, member_id: str, request: Request, user: Dict = Depends(get_current_user)):
    """Права участника"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["memberId"] = member_id
    result = await chat_handler.handle_get_member_permissions(event, user, chat_id, member_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.put("/chats/{chat_id}/members/{member_id}/permissions", tags=["Chats"])
async def update_member_permissions(chat_id: int, member_id: str, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Обновить права"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["memberId"] = member_id
    result = await chat_handler.handle_update_member_permissions(event, user, chat_id, member_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/members/{member_id}/permissions/reset", tags=["Chats"])
async def reset_member_permissions(chat_id: int, member_id: str, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Сбросить права"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["memberId"] = member_id
    result = await chat_handler.handle_reset_member_permissions(event, user, chat_id, member_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/online", tags=["Chats"])
async def get_online_users(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Кто онлайн"""
    result = await message_handler.handle_get_online_users({}, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/typing/start", tags=["Chats"])
async def typing_start(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Начать печатать"""
    result = await message_handler.handle_typing_start({}, user, chat_id)
    return Response(status_code=204)

@app.post("/chats/{chat_id}/typing/stop", tags=["Chats"])
async def typing_stop(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Перестать печатать"""
    result = await message_handler.handle_typing_stop({}, user, chat_id)
    return Response(status_code=204)

@app.get("/chats/{chat_id}/typing", tags=["Chats"])
async def get_typing_users(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Кто печатает"""
    result = await message_handler.handle_get_typing_users({}, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/join-requests", tags=["Chats"])
async def create_join_request(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Создать заявку"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_create_join_request(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/join-requests", tags=["Chats"])
async def get_join_requests(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Список заявок"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_join_requests(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/join-requests/{request_id}/approve", tags=["Chats"])
async def approve_join_request(chat_id: int, request_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Одобрить заявку"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["requestId"] = str(request_id)
    result = await chat_handler.handle_approve_join_request(event, user, chat_id, request_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/join-requests/{request_id}/reject", tags=["Chats"])
async def reject_join_request(chat_id: int, request_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Отклонить заявку"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["requestId"] = str(request_id)
    result = await chat_handler.handle_reject_join_request(event, user, chat_id, request_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/link-chat", tags=["Chats"])
async def link_discussion_chat(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Привязать обсуждение"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_link_discussion_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/link-chat", tags=["Chats"])
async def unlink_discussion_chat(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Отвязать обсуждение"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_unlink_discussion_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/discussion", tags=["Chats"])
async def get_discussion_chat(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Чат обсуждения"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_discussion_chat(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.put("/chats/{chat_id}/discussion-settings", tags=["Chats"])
async def update_discussion_settings(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Обновить настройки обсуждения"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_update_discussion_settings(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/enable-comments", tags=["Chats"])
async def enable_comments(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Включить комментарии"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_enable_comments(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/disable-comments", tags=["Chats"])
async def disable_comments(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Выключить комментарии"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_disable_comments(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/enable-reactions", tags=["Chats"])
async def enable_reactions(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Включить реакции"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_enable_reactions(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/disable-reactions", tags=["Chats"])
async def disable_reactions(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Выключить реакции"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_disable_reactions(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/reactions-settings", tags=["Chats"])
async def get_reactions_settings(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Настройки реакций"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await chat_handler.handle_get_reactions_settings(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

# ============================================
# СООБЩЕНИЯ
# ============================================

@app.get("/chats/{chat_id}/messages", tags=["Messages"])
async def get_chat_messages(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Сообщения чата"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_get_chat_messages(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/messages", tags=["Messages"])
async def send_message(chat_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Отправить сообщение"""
    body = await request.json()
    idempotency_key = request.headers.get("x-idempotency-key")
    
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    if idempotency_key:
        event["headers"]["x-idempotency-key"] = idempotency_key
    
    result = await message_handler.handle_send_message(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/search", tags=["Messages"])
async def search_chat_messages(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Поиск по сообщениям"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_search_messages(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/forwarded", tags=["Messages"])
async def get_forwarded_messages(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Пересланные сообщения"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_get_forwarded_messages(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/top", tags=["Messages"])
async def get_top_messages(chat_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Топ сообщений"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    result = await message_handler.handle_get_top_messages(event, user, chat_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/{message_id}", tags=["Messages"])
async def get_message(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Получить сообщение"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_get_message(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.put("/chats/{chat_id}/messages/{message_id}", tags=["Messages"])
async def edit_message(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Редактировать сообщение"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_edit_message(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/messages/{message_id}", tags=["Messages"])
async def delete_message(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Удалить сообщение"""
    permanent = request.query_params.get("permanent", "false").lower() == "true"
    reason = request.query_params.get("reason")
    
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    event["queryStringParameters"] = {
        "permanent": str(permanent).lower(),
        "reason": reason or ""
    }
    
    result = await message_handler.handle_delete_message(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/messages/{message_id}/forward", tags=["Messages"])
async def forward_message(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Переслать сообщение"""
    body = await request.json()
    idempotency_key = request.headers.get("x-idempotency-key")
    
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    if idempotency_key:
        event["headers"]["x-idempotency-key"] = idempotency_key
    
    result = await message_handler.handle_forward_message(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/{message_id}/original", tags=["Messages"])
async def get_original_message(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Оригинал пересланного"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_get_original_message(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/{message_id}/forward-info", tags=["Messages"])
async def get_forwarding_info(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Информация о пересылке"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_get_forwarding_info(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/messages/{message_id}/pin", tags=["Messages"])
async def pin_message(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Закрепить сообщение"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await chat_handler.handle_pin_message(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/messages/{message_id}/pin", tags=["Messages"])
async def unpin_message(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Открепить сообщение"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await chat_handler.handle_unpin_message(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/messages/{message_id}/save", tags=["Messages"])
async def save_message(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Сохранить сообщение"""
    body = await request.json() if await request.body() else {}
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_save_message(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/messages/{message_id}/save", tags=["Messages"])
async def unsave_message(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Удалить из сохраненных"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_unsave_message(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/{message_id}/photos", tags=["Messages"])
async def get_message_photos(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Фото сообщения"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_get_message_photos(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/{message_id}/discussion", tags=["Messages"])
async def get_message_discussion(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Обсуждение сообщения"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_get_message_discussion(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/messages/{message_id}/publish-to-feed", tags=["Messages"])
async def publish_to_feed(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Опубликовать в ленту"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_publish_to_feed(event, user, chat_id, message_id)
    raw = result.get("body", "{}")
    if result.get("isBase64Encoded") and isinstance(raw, str) and raw:
        import gzip as _gzip, base64 as _b64
        raw = _gzip.decompress(_b64.b64decode(raw)).decode("utf-8")
    elif isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    content = json.loads(raw) if raw else {}
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=content
    )

@app.get("/chats/{chat_id}/threads/{thread_root_id}", tags=["Messages"])
async def get_thread_messages(chat_id: int, thread_root_id: int, request: Request, user: Dict = Depends(get_current_user)):
    """Сообщения треда"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["threadRootId"] = str(thread_root_id)
    result = await message_handler.handle_get_thread_messages(event, user, chat_id, thread_root_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/chats/{chat_id}/messages/{message_id}/reactions", tags=["Messages"])
async def add_reaction(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Добавить реакцию"""
    body = await request.json()
    event = normalize_event(request, body)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_add_reaction(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/chats/{chat_id}/messages/{message_id}/reactions/{reaction}", tags=["Messages"])
async def remove_reaction(chat_id: int, message_id: int, reaction: str, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Удалить реакцию"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    event["pathParameters"]["reaction"] = reaction
    result = await message_handler.handle_remove_reaction(event, user, chat_id, message_id, reaction)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/{message_id}/reactions/{reaction}/users", tags=["Messages"])
async def get_reaction_users(chat_id: int, message_id: int, reaction: str, request: Request, user: Dict = Depends(get_current_user)):
    """Пользователи с реакцией"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    event["pathParameters"]["reaction"] = reaction
    result = await message_handler.handle_get_reaction_users(event, user, chat_id, message_id, reaction)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/chats/{chat_id}/messages/{message_id}/reactions/me", tags=["Messages"])
async def get_my_reactions(chat_id: int, message_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Мои реакции"""
    event = normalize_event(request)
    event["pathParameters"]["chatId"] = str(chat_id)
    event["pathParameters"]["messageId"] = str(message_id)
    result = await message_handler.handle_get_my_reactions(event, user, chat_id, message_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

# ============================================
# УВЕДОМЛЕНИЯ
# ============================================

@app.get("/notifications", tags=["Notifications"])
async def get_notifications(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Получить уведомления"""
    event = normalize_event(request)
    result = await message_handler.handle_get_notifications(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/notifications/unread-count", tags=["Notifications"])
async def get_unread_count(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Количество непрочитанных"""
    result = await message_handler.handle_get_unread_count({}, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/notifications/read-all", tags=["Notifications"])
async def mark_all_read(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Отметить все как прочитанные"""
    result = await message_handler.handle_mark_all_read({}, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.post("/notifications/{notification_id}/read", tags=["Notifications"])
async def mark_notification_read(notification_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Отметить уведомление как прочитанное"""
    result = await message_handler.handle_mark_notification_read({}, user, notification_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.delete("/notifications/{notification_id}", tags=["Notifications"])
async def delete_notification(notification_id: int, request: Request, user: Dict = Depends(get_authenticated_user)):
    """Удалить уведомление"""
    result = await message_handler.handle_delete_notification({}, user, notification_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

# ============================================
# ФОТО (ГЛОБАЛЬНЫЕ)
# ============================================

@app.get("/photos/batch", tags=["Photos"])
async def get_photos_batch(request: Request, user: Dict = Depends(get_current_user)):
    """Получить статусы нескольких фото"""
    event = normalize_event(request)
    result = await message_handler.handle_get_photos_batch(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/photos/{photo_id}/status", tags=["Photos"])
async def get_photo_status(photo_id: str, request: Request, user: Dict = Depends(get_current_user)):
    """Статус загрузки фото"""
    result = await message_handler.handle_get_photo_status({}, user, photo_id)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

# ============================================
# ГЛОБАЛЬНЫЙ ПОИСК
# ============================================

@app.get("/messages/search", tags=["Search"])
async def global_search_messages(request: Request, user: Dict = Depends(get_current_user)):
    """Глобальный поиск сообщений"""
    event = normalize_event(request)
    result = await message_handler.handle_search_messages(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

@app.get("/users/search", tags=["Search"])
async def search_users(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Поиск пользователей"""
    event = normalize_event(request)
    result = await chat_handler.handle_search_users(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 200),
        content=json.loads(result.get("body", "{}"))
    )

# ============================================
# ЛИЧНЫЕ СООБЩЕНИЯ
# ============================================

@app.post("/messages/private", tags=["Messages"])
async def send_private_message(request: Request, user: Dict = Depends(get_authenticated_user)):
    """Отправить личное сообщение"""
    body = await request.json()
    idempotency_key = request.headers.get("x-idempotency-key")
    
    event = normalize_event(request, body)
    if idempotency_key:
        event["headers"]["x-idempotency-key"] = idempotency_key
    
    result = await message_handler.handle_send_private_message(event, user)
    return JSONResponse(
        status_code=result.get("statusCode", 201),
        content=json.loads(result.get("body", "{}"))
    )

# ============================================
# ЗАПУСК
# ============================================

def run():
    """Запуск сервера"""
    host = getattr(config, "HOST", "0.0.0.0")
    port = int(getattr(config, "PORT", 8000))
    
    logger.info("=" * 60)
    logger.info(f"🚀 STARTING CHAT SERVICE ON {host}:{port}")
    logger.info("=" * 60)
    
    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        reload=False,
        log_level="info"
    )

if __name__ == "__main__":
    run()