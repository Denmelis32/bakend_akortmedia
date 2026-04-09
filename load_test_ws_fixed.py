import asyncio
import websockets
import json
import aiohttp
import time
from datetime import datetime

# Конфигурация
AUTH_URL = "http://localhost:8080"
CHAT_WS_URL = "ws://localhost:8000/ws"

# Существующие пользователи (которые точно есть в базе)
# Если пользователя нет, он будет зарегистрирован автоматически
USERS = [
    {"username": "tes12t_user_123", "password": "Test123!@#", "first_name": "Test", "last_name": "User"},
    {"username": "testus123111er", "password": "TestPassword123!", "first_name": "Test", "last_name": "User"},
    {"username": "user2_test", "password": "Test123!@#", "first_name": "Test", "last_name": "User"},
    {"username": "chatuser2026", "password": "Test123!@#", "first_name": "Test", "last_name": "User"}
]

# Логи
transactions_log = []
sessions_log = []

def log_transaction(event_type, user, status, details=None):
    """Запись транзакции"""
    transaction = {
        "timestamp": datetime.now().isoformat(),
        "event_type": event_type,
        "user": user,
        "status": status,
        "details": details or {}
    }
    transactions_log.append(transaction)
    print(f"[TRANSACTION] {event_type} | {user} | {status}")

def log_session(user, session_data):
    """Запись сессии"""
    session = {
        "timestamp": datetime.now().isoformat(),
        "user": user,
        "session_data": session_data
    }
    sessions_log.append(session)
    print(f"[SESSION] {user} | Session logged")

async def login_or_register_user(username, password, first_name="Test", last_name="User"):
    """Логин пользователя, если не существует - регистрация"""
    async with aiohttp.ClientSession() as session:
        # Сначала пробуем логин
        try:
            async with session.post(
                f"{AUTH_URL}/login",
                json={"username": username, "password": password},
                headers={"Content-Type": "application/json"}
            ) as response:
                text = await response.text()
                
                if response.status == 200:
                    try:
                        data = json.loads(text)
                    except:
                        data = {}
                    
                    log_transaction("LOGIN", username, "SUCCESS", {
                        "status_code": response.status,
                        "token_present": "data" in data and "access_token" in data.get("data", {})
                    })
                    return data.get("data", {}).get("access_token")
                else:
                    # Пользователь не найден, регистрируем
                    log_transaction("LOGIN", username, "NOT_FOUND", {
                        "status_code": response.status,
                        "action": "registering"
                    })
        except Exception as e:
            log_transaction("LOGIN", username, "ERROR", {"error": str(e), "action": "registering"})
        
        # Регистрация нового пользователя
        try:
            async with session.post(
                f"{AUTH_URL}/register",
                json={
                    "username": username,
                    "password": password,
                    "confirm_password": password,
                    "first_name": first_name,
                    "last_name": last_name
                },
                headers={"Content-Type": "application/json"}
            ) as response:
                text = await response.text()
                
                if response.status in [200, 201]:
                    try:
                        data = json.loads(text)
                    except:
                        data = {}
                    
                    log_transaction("REGISTER", username, "SUCCESS", {
                        "status_code": response.status,
                        "user_id": data.get("data", {}).get("id", "unknown")
                    })
                    
                    # Теперь логинимся после регистрации
                    async with session.post(
                        f"{AUTH_URL}/login",
                        json={"username": username, "password": password},
                        headers={"Content-Type": "application/json"}
                    ) as login_response:
                        login_text = await login_response.text()
                        
                        if login_response.status == 200:
                            try:
                                login_data = json.loads(login_text)
                            except:
                                login_data = {}
                            
                            log_transaction("LOGIN_AFTER_REGISTER", username, "SUCCESS", {
                                "status_code": login_response.status
                            })
                            return login_data.get("data", {}).get("access_token")
                        else:
                            log_transaction("LOGIN_AFTER_REGISTER", username, "FAILED", {
                                "status_code": login_response.status,
                                "error": login_text
                            })
                            return None
                else:
                    try:
                        data = json.loads(text)
                    except:
                        data = {"raw": text}
                    
                    log_transaction("REGISTER", username, "FAILED", {
                        "status_code": response.status,
                        "error": data.get("error", "Unknown error")
                    })
                    return None
        except Exception as e:
            log_transaction("REGISTER", username, "ERROR", {"error": str(e)})
            return None

async def websocket_client(username, password, client_id, first_name="Test", last_name="User"):
    """WebSocket клиент для одного пользователя"""
    print(f"\n[CLIENT {client_id}] Starting for user: {username}")
    
    # Шаг 1: Логин или регистрация
    token = await login_or_register_user(username, password, first_name, last_name)
    if not token:
        print(f"[CLIENT {client_id}] ❌ Login/Registration failed for {username}")
        return
    
    # Шаг 2: Подключение к WebSocket
    try:
        async with websockets.connect(CHAT_WS_URL) as websocket:
            # Отправляем токен для аутентификации (требуется формат chat-service)
            auth_message = {"token": token}
            await websocket.send(json.dumps(auth_message))
            print(f"[CLIENT {client_id}] 🔐 Sent authentication token")
            
            # Ждем приветственное сообщение
            try:
                welcome_msg = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                welcome_data = json.loads(welcome_msg)
                print(f"[CLIENT {client_id}] 📩 Received: {welcome_data}")
                
                session_start = time.time()
                
                # Логирование сессии
                log_session(username, {
                    "connection_id": welcome_data.get("connection_id", "unknown"),
                    "type": welcome_data.get("type", "unknown"),
                    "duration": "active"
                })
                
                # Активная сессия - отправляем несколько сообщений
                messages_sent = 0
                messages_received = 0
                
                # Отправляем тестовые сообщения
                for i in range(3):
                    test_msg = {
                        "type": "ping",
                        "message": f"Ping from {username} - {i}",
                        "timestamp": time.time()
                    }
                    await websocket.send(json.dumps(test_msg))
                    messages_sent += 1
                    print(f"[CLIENT {client_id}] 📤 Sent ping {i+1}/3")
                    
                    # Ждем ответ
                    try:
                        response = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                        response_data = json.loads(response)
                        messages_received += 1
                        print(f"[CLIENT {client_id}] 📥 Received: {response_data}")
                        
                        log_transaction("MESSAGE_EXCHANGE", username, "SUCCESS", {
                            "sent": test_msg,
                            "received": response_data,
                            "latency_ms": round((time.time() - session_start) * 1000 / (i + 1), 2)
                        })
                    except asyncio.TimeoutError:
                        print(f"[CLIENT {client_id}] ⏱️ Timeout waiting for response")
                        log_transaction("MESSAGE_EXCHANGE", username, "TIMEOUT", {
                            "message_index": i
                        })
                    
                    await asyncio.sleep(0.5)
                
                session_duration = time.time() - session_start
                
                # Обновляем лог сессии
                log_session(username, {
                    "connection_id": welcome_data.get("connection_id", "unknown"),
                    "type": welcome_data.get("type", "unknown"),
                    "duration_seconds": round(session_duration, 2),
                    "messages_sent": messages_sent,
                    "messages_received": messages_received,
                    "status": "completed"
                })
                
                print(f"[CLIENT {client_id}] 🏁 Session completed: {session_duration:.2f}s")
                
            except asyncio.TimeoutError:
                print(f"[CLIENT {client_id}] ⏱️ Timeout waiting for welcome message")
                log_transaction("WEBSOCKET_CONNECT", username, "TIMEOUT", {
                    "error": "No welcome message received"
                })
                
    except websockets.exceptions.InvalidStatusCode as e:
        print(f"[CLIENT {client_id}] ❌ WebSocket connection failed: {e}")
        log_transaction("WEBSOCKET_CONNECT", username, "FAILED", {
            "error": str(e),
            "status_code": e.status_code if hasattr(e, 'status_code') else None
        })
    except Exception as e:
        print(f"[CLIENT {client_id}] ❌ Error: {e}")
        log_transaction("WEBSOCKET_CONNECT", username, "ERROR", {
            "error": str(e)
        })

async def load_test():
    """Запуск нагрузочного теста"""
    print("=" * 60)
    print("🚀 НАГРУЗОЧНЫЙ ТЕСТ WEBSOCKET - ЧАТ СЕРВИС")
    print("=" * 60)
    print(f"Auth URL: {AUTH_URL}")
    print(f"Chat WS URL: {CHAT_WS_URL}")
    print(f"Пользователей: {len(USERS)}")
    print("=" * 60)
    
    start_time = time.time()
    
    # Запускаем всех клиентов параллельно
    tasks = [
        websocket_client(
            user["username"], 
            user["password"], 
            i+1,
            user.get("first_name", "Test"),
            user.get("last_name", "User")
        )
        for i, user in enumerate(USERS)
    ]
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    total_time = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("📊 ИТОГИ ТЕСТА")
    print("=" * 60)
    print(f"Общее время: {total_time:.2f} сек")
    print(f"Всего пользователей: {len(USERS)}")
    print(f"Транзакций записано: {len(transactions_log)}")
    print(f"Сессий записано: {len(sessions_log)}")
    
    # Статистика по транзакциям
    success_count = sum(1 for t in transactions_log if t["status"] == "SUCCESS")
    failed_count = sum(1 for t in transactions_log if t["status"] in ["FAILED", "ERROR", "TIMEOUT"])
    
    print(f"✅ Успешных транзакций: {success_count}")
    print(f"❌ Ошибок: {failed_count}")
    
    # Сохраняем логи
    with open("/workspace/transactions_log.json", "w", encoding="utf-8") as f:
        json.dump(transactions_log, f, indent=2, ensure_ascii=False)
    print(f"\n💾 Логи транзакций сохранены: /workspace/transactions_log.json")
    
    with open("/workspace/sessions_log.json", "w", encoding="utf-8") as f:
        json.dump(sessions_log, f, indent=2, ensure_ascii=False)
    print(f"💾 Логи сессий сохранены: /workspace/sessions_log.json")
    
    # Вывод деталей по ошибкам
    if failed_count > 0:
        print("\n⚠️ ДЕТАЛИ ОШИБОК:")
        for t in transactions_log:
            if t["status"] in ["FAILED", "ERROR", "TIMEOUT"]:
                print(f"  - {t['event_type']} | {t['user']}: {t['details']}")

if __name__ == "__main__":
    asyncio.run(load_test())
