import asyncio
import websockets
import json

async def listen():
    uri = "ws://158.160.16.246/ws"
    print(f"🔌 Connecting to {uri}...")
    
    # Новый токен
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhNDhlYWM2OS0xYTE5LTRjNGEtOWExMC0wZTE0NjliM2Q4ODgiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZmlyc3RfbmFtZSI6IkNoYXQiLCJ1c2VybmFtZSI6ImNoYXR1c2VyMjAyNiIsImV4cCI6MTc3NDUwOTAzMSwidHlwZSI6ImFjY2VzcyIsImp0aSI6ImVhODg1MjBiLWQyZjMtNGEyNy04NjM2LTMyMjMxN2E5NjUzZSIsImlhdCI6MTc3NDUwODEzMX0.erzC7XjHDh3mz_gR4LaX-jENHLSJViK-Mg324eDEpwo"
    
    try:
        async with websockets.connect(uri) as websocket:
            print("✅ WebSocket connected!")
            
            # Отправляем токен
            auth_msg = {
                "token": token,
                "session_id": "listener_test"
            }
            await websocket.send(json.dumps(auth_msg))
            print("📤 Auth message sent")
            
            # Ждём подтверждение
            response = await websocket.recv()
            print(f"✅ Connected: {response}")
            
            # Слушаем сообщения
            print("👂 Listening for messages...")
            print("=" * 50)
            
            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    
                    if data.get('type') == 'new_message':
                        msg_data = data.get('data', {})
                        msg_content = msg_data.get('message', {}).get('content', 'No content')
                        print(f"📨 NEW MESSAGE: {msg_content}")
                        print(f"   Chat ID: {msg_data.get('chat_id')}")
                        print(f"   Sender: {msg_data.get('sender_id')}")
                        print("-" * 50)
                    elif data.get('type') == 'pong':
                        print("🏓 Pong received")
                    elif data.get('type') == 'connected':
                        pass  # уже обработали
                    else:
                        print(f"📩 {data.get('type')}: {data}")
                        
                except Exception as e:
                    print(f"❌ Error: {e}")
                    break
                    
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(listen())
