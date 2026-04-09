import asyncio
import websockets
import json
import time

async def test_websocket():
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJmZmFiMTc2NC0yNzM1LTQwODYtYThkZC0yYWY0NmEwYjY0OGEiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZmlyc3RfbmFtZSI6IlRlc3QxMTEiLCJ1c2VybmFtZSI6InRlc3R1czEyMzExMWVyIiwiZXhwIjoxNzczMzE0MDgzLCJ0eXBlIjoiYWNjZXNzIiwianRpIjoiYWMxYzk5NmMtZjQ5ZC00ZTMzLWI1Y2UtNGFmYWQ4ODNmZmMyIiwiaWF0IjoxNzczMzEzMTgzfQ.x8-FqmhwmZ_9nelQU9kC2cGfatVmWOYUftGWlTDVuXs"
    
    print(f"⏱️ {time.strftime('%H:%M:%S')} - Попытка подключения к WebSocket...")
    
    try:
        async with websockets.connect('ws://localhost:8000/ws') as ws:
            print(f"✅ {time.strftime('%H:%M:%S')} - Подключено к WebSocket")
            
            # Отправляем токен
            await ws.send(json.dumps({"token": token}))
            print(f"📤 {time.strftime('%H:%M:%S')} - Токен отправлен")
            
            # Получаем ответ
            response = await ws.recv()
            print(f"📥 {time.strftime('%H:%M:%S')} - Получено: {response}")
            
            print(f"⏳ {time.strftime('%H:%M:%S')} - Ожидание уведомлений...")
            print("=" * 60)
            
            message_count = 0
            while True:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    message_count += 1
                    print(f"\n🔔 [{time.strftime('%H:%M:%S')}] СООБЩЕНИЕ #{message_count}:")
                    print(f"{message}")
                    print("=" * 60)
                except asyncio.TimeoutError:
                    # Просто продолжаем ждать
                    pass
                except websockets.exceptions.ConnectionClosed:
                    print(f"\n❌ {time.strftime('%H:%M:%S')} - Соединение закрыто")
                    break
                except Exception as e:
                    print(f"\n❌ {time.strftime('%H:%M:%S')} - Ошибка: {e}")
                    break
                    
    except Exception as e:
        print(f"❌ {time.strftime('%H:%M:%S')} - Ошибка подключения: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(test_websocket())
    except KeyboardInterrupt:
        print(f"\n🛑 {time.strftime('%H:%M:%S')} - Скрипт остановлен")
