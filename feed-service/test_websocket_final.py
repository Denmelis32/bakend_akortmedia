import asyncio
import websockets
import json

async def test_websocket():
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJmZmFiMTc2NC0yNzM1LTQwODYtYThkZC0yYWY0NmEwYjY0OGEiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZmlyc3RfbmFtZSI6IlRlc3QxMTEiLCJ1c2VybmFtZSI6InRlc3R1czEyMzExMWVyIiwiZXhwIjoxNzczMzE0MDgzLCJ0eXBlIjoiYWNjZXNzIiwianRpIjoiYWMxYzk5NmMtZjQ5ZC00ZTMzLWI1Y2UtNGFmYWQ4ODNmZmMyIiwiaWF0IjoxNzczMzEzMTgzfQ.x8-FqmhwmZ_9nelQU9kC2cGfatVmWOYUftGWlTDVuXs"
    
    async with websockets.connect('ws://localhost:8000/ws') as ws:
        print("✅ Подключено к WebSocket")
        print(f"👤 Токен: {token[:30]}...")
        
        # Отправляем токен
        await ws.send(json.dumps({"token": token}))
        print("📤 Токен отправлен")
        
        # Получаем ответ
        response = await ws.recv()
        print(f"📥 Получено: {response}")
        
        print("⏳ Ожидание уведомлений...")
        print("-" * 50)
        
        while True:
            try:
                message = await ws.recv()
                print(f"🔔 ПОЛУЧЕНО СООБЩЕНИЕ: {message}")
                print("-" * 50)
            except Exception as e:
                print(f"❌ Ошибка: {e}")
                break

asyncio.run(test_websocket())
