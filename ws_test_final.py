#!/usr/bin/env python3
"""WebSocket нагрузочный тест с регистрацией новых пользователей"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
import uuid

# Генерируем уникальные имена для теста
TEST_USERS = [
    {"username": f"test_ws_user_{i}", "password": "Test123!@#", "first_name": f"Test{i}"}
    for i in range(1, 5)
]

AUTH_URL = "http://localhost:8080"
CHAT_WS_URL = "ws://localhost:8000/ws"

transactions_log = []
sessions_log = []

async def register(session, user):
    """Регистрация пользователя"""
    try:
        payload = {
            "username": user["username"],
            "password": user["password"],
            "first_name": user["first_name"],
            "confirm_password": user["password"]
        }
        async with session.post(f"{AUTH_URL}/register", json=payload) as resp:
            data = await resp.json()
            if resp.status == 201 or (resp.status == 200 and 'data' in data):
                transactions_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "type": "register",
                    "username": user["username"],
                    "status": "success",
                    "user_id": data.get("data", {}).get("id")
                })
                return True
            else:
                # Пользователь уже существует - это ОК
                transactions_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "type": "register",
                    "username": user["username"],
                    "status": "exists",
                    "response": data
                })
                return True
    except Exception as e:
        transactions_log.append({
            "timestamp": datetime.now().isoformat(),
            "type": "register",
            "username": user["username"],
            "status": "error",
            "error": str(e)
        })
        return False

async def login(session, user):
    """Логин пользователя"""
    try:
        async with session.post(f"{AUTH_URL}/login", json=user) as resp:
            data = await resp.json()
            if resp.status == 200 and 'data' in data and 'access_token' in data['data']:
                transactions_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "type": "login",
                    "username": user["username"],
                    "status": "success"
                })
                return data['data']['access_token']
            else:
                transactions_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "type": "login",
                    "username": user["username"],
                    "status": "failed",
                    "error": data.get("error", "Unknown error"),
                    "response": data
                })
                return None
    except Exception as e:
        transactions_log.append({
            "timestamp": datetime.now().isoformat(),
            "type": "login",
            "username": user["username"],
            "status": "error",
            "error": str(e)
        })
        return None

async def websocket_test(user, token, session_id):
    """Тест WebSocket подключения"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(CHAT_WS_URL) as ws:
                # Отправляем токен
                await ws.send_json({"token": token})
                
                start_time = time.time()
                msg_count = 0
                ping_count = 0
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        msg_count += 1
                        
                        if data.get("type") == "connected":
                            sessions_log.append({
                                "session_id": session_id,
                                "username": user["username"],
                                "status": "authenticated",
                                "timestamp": datetime.now().isoformat(),
                                "response": data
                            })
                        elif data.get("type") == "pong":
                            ping_count += 1
                        
                        # Завершаем после 5 сообщений или через 10 сек
                        if msg_count >= 5 or (time.time() - start_time) > 10:
                            break
                            
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        sessions_log.append({
                            "session_id": session_id,
                            "username": user["username"],
                            "status": "error",
                            "error": "WebSocket error",
                            "timestamp": datetime.now().isoformat()
                        })
                        break
                
                duration = time.time() - start_time
                
                transactions_log.append({
                    "session_id": session_id,
                    "username": user["username"],
                    "type": "websocket_session",
                    "status": "success",
                    "messages_received": msg_count,
                    "pings": ping_count,
                    "duration_sec": round(duration, 2),
                    "timestamp": datetime.now().isoformat()
                })
                
                return {"success": True, "messages": msg_count, "duration": duration}
                
    except Exception as e:
        sessions_log.append({
            "session_id": session_id,
            "username": user["username"],
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        })
        transactions_log.append({
            "session_id": session_id,
            "username": user["username"],
            "type": "websocket_session",
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        })
        return {"success": False, "error": str(e)}

async def main():
    print("=" * 60)
    print("WEBSOCKET НАГРУЗОЧНЫЙ ТЕСТ (с регистрацией)")
    print("=" * 60)
    
    start_total = time.time()
    results = []
    
    async with aiohttp.ClientSession() as session:
        # Регистрируем всех пользователей
        print("\n[1] Регистрация пользователей...")
        for i, user in enumerate(TEST_USERS):
            success = await register(session, user)
            if success:
                print(f"  ✅ {user['username']} - зарегистрирован/существует")
            else:
                print(f"  ❌ {user['username']} - ошибка регистрации")
        
        # Логиним всех пользователей
        print("\n[2] Логин пользователей...")
        tokens = []
        for i, user in enumerate(TEST_USERS):
            token = await login(session, user)
            if token:
                tokens.append((user, token))
                print(f"  ✅ {user['username']} - залогинен")
            else:
                print(f"  ❌ {user['username']} - ошибка логина")
        
        if not tokens:
            print("\n❌ Нет успешных логинов. Завершение теста.")
            return
        
        # WebSocket тесты
        print(f"\n[3] WebSocket тесты ({len(tokens)} пользователей)...")
        tasks = []
        for i, (user, token) in enumerate(tokens):
            task = websocket_test(user, token, f"session_{i+1}")
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
    
    # Статистика
    total_time = time.time() - start_total
    success_count = sum(1 for r in results if r.get("success"))
    total_messages = sum(r.get("messages", 0) for r in results if r.get("success"))
    
    print("\n" + "=" * 60)
    print("ИТОГИ ТЕСТА")
    print("=" * 60)
    print(f"Всего пользователей: {len(TEST_USERS)}")
    print(f"Успешных подключений: {success_count}/{len(tokens)}")
    print(f"Всего сообщений: {total_messages}")
    print(f"Общее время: {round(total_time, 2)} сек")
    
    # Сохраняем логи
    with open("/workspace/transactions_log.json", "w") as f:
        json.dump(transactions_log, f, indent=2, ensure_ascii=False)
    
    with open("/workspace/sessions_log.json", "w") as f:
        json.dump(sessions_log, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Логи сохранены:")
    print(f"   - /workspace/transactions_log.json ({len(transactions_log)} записей)")
    print(f"   - /workspace/sessions_log.json ({len(sessions_log)} записей)")
    
    # Вывод ошибок если есть
    errors = [t for t in transactions_log if t.get("status") in ["failed", "error"]]
    if errors:
        print(f"\n⚠️ Найдено ошибок: {len(errors)}")
        for err in errors[:5]:
            print(f"   - {err.get('username', 'unknown')}: {err.get('error', 'Unknown')}")
    
    # Детали сессий
    print(f"\n📊 ДЕТАЛИ СЕССИЙ:")
    for session in sessions_log:
        if session.get("status") == "authenticated":
            print(f"   ✅ {session['username']}: {session.get('response', {})}")

if __name__ == "__main__":
    asyncio.run(main())
