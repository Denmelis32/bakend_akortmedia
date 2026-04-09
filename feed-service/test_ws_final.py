import asyncio
import websockets
import json

async def test_websocket():
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJmZmFiMTc2NC0yNzM1LTQwODYtYThkZC0yYWY0NmEwYjY0OGEiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZmlyc3RfbmFtZSI6IlRlc3QxMTEiLCJ1c2VybmFtZSI6InRlc3R1czEyMzExMWVyIiwiZXhwIjoxNzczMzE1MTQ2LCJ0eXBlIjoiYWNjZXNzIiwianRpIjoiMzY0N2JmYzYtMzk1NS00MDJiLWIwZDktMjcyNzdmM2JhMmY4IiwiaWF0IjoxNzczMzE0MjQ2fQ.yrpjmGzlfIPsigYa0jOqe7jWRUlE7rA1itjwMzYIhyY"
    
    async with websockets.connect('ws://localhost:8000/ws') as ws:
        print("✅ WebSocket подключен")
        print("📤 Отправка токена...")
        await ws.send(json.dumps({"token": token}))
        
        response = await ws.recv()
        print(f"📥 Ответ сервера: {response}")
        
        print("\n🔔 Ожидание уведомлений... (нажмите Ctrl+C для выхода)")
        print("=" * 50)
        
        message_count = 0
        while True:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                message_count += 1
                print(f"\n📨 ПОЛУЧЕНО СООБЩЕНИЕ #{message_count}:")
                print(message)
                print("=" * 50)
            except asyncio.TimeoutError:
                # Просто продолжаем ждать
                print(".", end="", flush=True)
            except Exception as e:
                print(f"\n❌ Ошибка: {e}")
                break

if __name__ == "__main__":
    try:
        asyncio.run(test_websocket())
    except KeyboardInterrupt:
        print("\n\n🛑 Скрипт остановлен")
