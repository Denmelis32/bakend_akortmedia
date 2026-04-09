import asyncio
import websockets
import json

async def heartbeat(websocket, stop_event):
    """Отправляет пинги каждые 25 секунд"""
    ping_count = 0
    while not stop_event.is_set():
        await asyncio.sleep(25)
        try:
            await websocket.send(json.dumps({"type": "ping"}))
            ping_count += 1
            print(f"💓 Heartbeat #{ping_count} sent")
        except Exception as e:
            print(f"❌ Heartbeat error: {e}")
            break

async def listen():
    uri = "ws://158.160.16.246/ws"
    print(f"🔌 Connecting to {uri}...")
    
    # Новый токен (полученный из curl)
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhNDhlYWM2OS0xYTE5LTRjNGEtOWExMC0wZTE0NjliM2Q4ODgiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZmlyc3RfbmFtZSI6IkNoYXQiLCJ1c2VybmFtZSI6ImNoYXR1c2VyMjAyNiIsImV4cCI6MTc3NDUwOTczNCwidHlwZSI6ImFjY2VzcyIsImp0aSI6IjQxZjRkYWJjLTM3M2MtNDY0NC04MDVmLTk3NjJjMTRhOGRlNiIsImlhdCI6MTc3NDUwODgzNH0.27XTNNu_YQM28WF7NG6aBGfkqr9Bx9vh-w_U6uFN5rI"
    
    try:
        async with websockets.connect(uri, ping_interval=None) as websocket:
            print("✅ WebSocket connected!")
            
            # Отправляем токен аутентификации
            auth_msg = {"token": token}
            await websocket.send(json.dumps(auth_msg))
            print("📤 Auth message sent")
            
            # Получаем ответ
            response = await websocket.recv()
            print(f"📥 Auth response: {response}")
            
            # Проверяем, что подключение успешно
            resp_data = json.loads(response)
            if resp_data.get("type") != "connected":
                print("❌ Authentication failed!")
                return
            
            connection_id = resp_data.get("connection_id")
            print(f"✅ Connected with ID: {connection_id}")
            
            # Запускаем heartbeat
            stop_heartbeat = asyncio.Event()
            heartbeat_task = asyncio.create_task(heartbeat(websocket, stop_heartbeat))
            
            print("\n👂 Listening for messages... (press Ctrl+C to stop)")
            print("=" * 60)
            
            message_count = 0
            try:
                while True:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=60)
                        message_count += 1
                        data = json.loads(message)
                        msg_type = data.get("type", "unknown")
                        
                        if msg_type == "new_message":
                            msg_data = data.get("data", {})
                            msg_content = msg_data.get("message", {}).get("content", "No content")
                            sender = msg_data.get("sender_id", "unknown")
                            print(f"\n📨 [NEW MESSAGE #{message_count}]")
                            print(f"   From: {sender[:8]}...")
                            print(f"   Content: {msg_content}")
                        elif msg_type == "pong":
                            print(f"🏓 Pong received")
                        else:
                            print(f"📥 [{msg_type}] {data}")
                            
                    except asyncio.TimeoutError:
                        print(".", end="", flush=True)
                        
            except asyncio.CancelledError:
                pass
            finally:
                stop_heartbeat.set()
                heartbeat_task.cancel()
                
    except websockets.exceptions.ConnectionClosed as e:
        print(f"❌ Connection closed: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(listen())
    except KeyboardInterrupt:
        print("\n👋 Stopped by user")
