import asyncio
import websockets
import json

async def test():
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJmZmFiMTc2NC0yNzM1LTQwODYtYThkZC0yYWY0NmEwYjY0OGEiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZmlyc3RfbmFtZSI6IlRlc3QxMTEiLCJ1c2VybmFtZSI6InRlc3R1czEyMzExMWVyIiwiZXhwIjoxNzczMzE2MTI0LCJ0eXBlIjoiYWNjZXNzIiwianRpIjoiMTA3YTA3NmEtYzg3NC00NDljLWExOTQtOGQyMzk3NjJiNzdiIiwiaWF0IjoxNzczMzE1MjI0fQ.7CmmBH_BzQpafSLVgwUx6j1HzLqPsD7AqKWFiw7IZqk"
    
    async with websockets.connect('ws://localhost:8000/ws') as ws:
        print("✅ Подключено к WebSocket")
        
        # Отправляем токен
        await ws.send(json.dumps({"token": token}))
        print("📤 Токен отправлен")
        
        # Получаем ответ
        response = await ws.recv()
        print(f"📥 Получено: {response}")
        
        print("\n⏳ Ожидание уведомлений... (нажмите Ctrl+C для выхода)")
        print("=" * 50)
        
        # Просто читаем все входящие сообщения
        async for message in ws:
            print(f"\n🔔 ПОЛУЧЕНО СООБЩЕНИЕ:")
            print(json.dumps(json.loads(message), indent=2, ensure_ascii=False))
            print("=" * 50)

asyncio.run(test())
