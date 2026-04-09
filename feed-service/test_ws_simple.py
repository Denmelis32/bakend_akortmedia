import asyncio
import websockets
import json

async def test():
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJmZmFiMTc2NC0yNzM1LTQwODYtYThkZC0yYWY0NmEwYjY0OGEiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZmlyc3RfbmFtZSI6IlRlc3QxMTEiLCJ1c2VybmFtZSI6InRlc3R1czEyMzExMWVyIiwiZXhwIjoxNzczMzE0MDgzLCJ0eXBlIjoiYWNjZXNzIiwianRpIjoiYWMxYzk5NmMtZjQ5ZC00ZTMzLWI1Y2UtNGFmYWQ4ODNmZmMyIiwiaWF0IjoxNzczMzEzMTgzfQ.x8-FqmhwmZ_9nelQU9kC2cGfatVmWOYUftGWlTDVuXs"
    
    async with websockets.connect('ws://localhost:8000/ws') as ws:
        print("✅ Connected")
        await ws.send(json.dumps({"token": token}))
        response = await ws.recv()
        print(f"📥 Connected: {response}")
        
        print("⏳ Waiting for messages...")
        async for message in ws:
            print(f"\n🔔 RECEIVED: {message}\n")

asyncio.run(test())
