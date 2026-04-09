#!/usr/bin/env python3
"""
Тест интеграции Flutter клиента с Chat Service через WebSocket
Эмулирует поведение Flutter app из chat_websocket_service.dart
"""

import asyncio
import websockets
import json
import httpx
from datetime import datetime

# Конфигурация (как во Flutter)
AUTH_URL = "http://localhost:8080"
CHAT_WS_URL = "ws://localhost:8000/ws"

# Тестовые креды
TEST_USER = "flutter_test_user"
TEST_PASSWORD = "Test123!@#"

class IntegrationTester:
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.user_id = None
        self.sessions = []
        self.transactions = []
        
    def log_transaction(self, tx_type, status, details=None):
        tx = {
            "timestamp": datetime.now().isoformat(),
            "type": tx_type,
            "status": status,
            "details": details or {}
        }
        self.transactions.append(tx)
        print(f"💼 [TX] {tx_type}: {status}")
        if details:
            print(f"   Details: {json.dumps(details, ensure_ascii=False)[:200]}")
    
    def log_session(self, session_type, status, duration_sec=None, details=None):
        session = {
            "timestamp": datetime.now().isoformat(),
            "session_type": session_type,
            "status": status,
            "duration_sec": duration_sec,
            "details": details or {}
        }
        self.sessions.append(session)
        print(f"📋 [SESSION] {session_type}: {status}, duration={duration_sec}s")
    
    async def login(self):
        """Логин как во Flutter (auth_remote_data_source.dart)"""
        print("\n🔐 [TEST] Logging in...")
        start = datetime.now()
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{AUTH_URL}/login",
                    json={
                        "username": TEST_USER,
                        "password": TEST_PASSWORD
                    },
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.access_token = data['data']['access_token']
                    self.refresh_token = data['data']['refresh_token']
                    self.user_id = data['data']['user_id']
                    
                    elapsed = (datetime.now() - start).total_seconds()
                    self.log_transaction("LOGIN", "SUCCESS", {
                        "user_id": self.user_id,
                        "token_length": len(self.access_token),
                        "elapsed_sec": elapsed
                    })
                    
                    print(f"✅ [TEST] Login successful!")
                    print(f"   User ID: {self.user_id}")
                    print(f"   Token: {self.access_token[:50]}...")
                    return True
                else:
                    self.log_transaction("LOGIN", "FAILED", {
                        "status_code": response.status_code,
                        "response": response.text[:200]
                    })
                    print(f"❌ [TEST] Login failed: {response.status_code}")
                    print(f"   Response: {response.text}")
                    return False
                    
        except Exception as e:
            self.log_transaction("LOGIN", "ERROR", {"error": str(e)})
            print(f"❌ [TEST] Login error: {e}")
            return False
    
    async def test_websocket_connection(self):
        """Подключение к WebSocket как во Flutter (chat_remote_data_source.dart)"""
        print("\n🔌 [TEST] Testing WebSocket connection...")
        start = datetime.now()
        
        if not self.access_token:
            print("❌ [TEST] No access token! Login first.")
            return False
        
        try:
            session_id = f"ws_{int(datetime.now().timestamp() * 1000)}"
            print(f"📡 [TEST] Connecting to {CHAT_WS_URL}")
            print(f"   Session ID: {session_id}")
            
            async with websockets.connect(CHAT_WS_URL) as websocket:
                # Шаг 1: Отправляем токен (как Flutter через 100мс после подключения)
                print("🔑 [TEST] Sending auth message...")
                auth_message = {
                    "token": self.access_token,
                    "session_id": session_id
                }
                await websocket.send(json.dumps(auth_message))
                print(f"   Sent: {json.dumps(auth_message)[:100]}...")
                
                # Шаг 2: Ждем ответ "connected"
                print("⏳ [TEST] Waiting for connection confirmation...")
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    data = json.loads(response)
                    
                    print(f"📩 [TEST] Received: {json.dumps(data)}")
                    
                    if data.get("type") == "connected":
                        elapsed = (datetime.now() - start).total_seconds()
                        
                        self.log_transaction("WS_CONNECT", "SUCCESS", {
                            "session_id": session_id,
                            "connection_id": data.get("connection_id"),
                            "user_id": data.get("user_id"),
                            "elapsed_sec": elapsed
                        })
                        
                        self.log_session("WEBSOCKET", "CONNECTED", elapsed, {
                            "session_id": session_id,
                            "server_response": data
                        })
                        
                        print(f"✅ [TEST] WebSocket connected!")
                        print(f"   Connection ID: {data.get('connection_id')}")
                        print(f"   Server User ID: {data.get('user_id')}")
                        
                        # Шаг 3: Тест отправки сообщения
                        await self.test_send_message(websocket, session_id)
                        
                        # Держим соединение для теста
                        await asyncio.sleep(2)
                        
                        # Закрываем
                        await websocket.close()
                        
                        total_duration = (datetime.now() - start).total_seconds()
                        self.log_session("WEBSOCKET", "DISCONNECTED", total_duration, {
                            "reason": "test_completed"
                        })
                        
                        return True
                    else:
                        self.log_transaction("WS_CONNECT", "FAILED", {
                            "response": data,
                            "expected": "type=connected"
                        })
                        print(f"❌ [TEST] Unexpected response type: {data.get('type')}")
                        return False
                        
                except asyncio.TimeoutError:
                    self.log_transaction("WS_CONNECT", "TIMEOUT", {"timeout_sec": 5})
                    print("❌ [TEST] Timeout waiting for response")
                    return False
                    
        except Exception as e:
            self.log_transaction("WS_CONNECT", "ERROR", {"error": str(e)})
            print(f"❌ [TEST] WebSocket error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def test_send_message(self, websocket, session_id):
        """Тест отправки сообщения"""
        print("\n💬 [TEST] Testing message sending...")
        
        # Подписка на чат (как Flutter)
        test_chat_id = "test_chat_123"
        subscribe_msg = {
            "type": "subscribe",
            "chat_id": test_chat_id
        }
        
        print(f"📡 [TEST] Subscribing to chat {test_chat_id}...")
        await websocket.send(json.dumps(subscribe_msg))
        
        # Ждем подтверждения
        try:
            response = await asyncio.wait_for(websocket.recv(), timeout=3.0)
            data = json.loads(response)
            print(f"📩 [TEST] Subscribe response: {json.dumps(data)}")
            
            self.log_transaction("WS_SUBSCRIBE", "SUCCESS", {
                "chat_id": test_chat_id,
                "response": data
            })
        except asyncio.TimeoutError:
            print("⚠️ [TEST] No subscribe confirmation received")
        
        # Ping/Pong тест
        print("🏓 [TEST] Sending ping...")
        await websocket.send(json.dumps({"type": "ping"}))
        
        try:
            response = await asyncio.wait_for(websocket.recv(), timeout=3.0)
            data = json.loads(response)
            print(f"📩 [TEST] Ping response: {json.dumps(data)}")
            
            self.log_transaction("WS_PING", "SUCCESS", {"response": data})
        except asyncio.TimeoutError:
            print("⚠️ [TEST] No pong received")
    
    async def run_full_test(self):
        """Полный тест интеграции"""
        print("="*60)
        print("🧪 FLUTTER INTEGRATION TEST")
        print("="*60)
        
        # 1. Логин
        if not await self.login():
            print("\n❌ [TEST] Login failed, stopping test")
            return False
        
        # 2. WebSocket подключение
        ws_success = await self.test_websocket_connection()
        
        # 3. Итоги
        print("\n" + "="*60)
        print("📊 TEST RESULTS")
        print("="*60)
        print(f"Total transactions: {len(self.transactions)}")
        print(f"Total sessions: {len(self.sessions)}")
        
        success_count = sum(1 for t in self.transactions if t["status"] == "SUCCESS")
        print(f"Successful transactions: {success_count}/{len(self.transactions)}")
        
        # Сохраняем логи
        with open("/workspace/flutter_integration_transactions.json", "w") as f:
            json.dump(self.transactions, f, indent=2, ensure_ascii=False)
        
        with open("/workspace/flutter_integration_sessions.json", "w") as f:
            json.dump(self.sessions, f, indent=2, ensure_ascii=False)
        
        print(f"\n📁 Logs saved:")
        print(f"   - /workspace/flutter_integration_transactions.json")
        print(f"   - /workspace/flutter_integration_sessions.json")
        
        return ws_success

async def main():
    tester = IntegrationTester()
    success = await tester.run_full_test()
    
    if success:
        print("\n✅ INTEGRATION TEST PASSED!")
    else:
        print("\n❌ INTEGRATION TEST FAILED!")
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(asyncio.run(main()))
