import asyncio
import websockets
import json

TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhNDhlYWM2OS0xYTE5LTRjNGEtOWExMC0wZTE0NjliM2Q4ODgiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZmlyc3RfbmFtZSI6IkNoYXQiLCJ1c2VybmFtZSI6ImNoYXR1c2VyMjAyNiIsImV4cCI6MTc3NDUxMDU5NSwidHlwZSI6ImFjY2VzcyIsImp0aSI6IjEwNjljZTIyLWFhZDctNDRkNy04MTQwLWMzNWE1ODBiYTBjMyIsImlhdCI6MTc3NDUwOTY5NX0.Whnp7zCt6PXiECVIzPvgBWPF4v1_nvcdrk7dvlA0iv4"

async def test():
    uri = "ws://158.160.16.246/ws"
    print(f"Connecting to {uri}...")
    
    try:
        # Убираем timeout из connect()
        async with websockets.connect(uri) as ws:
            print("✅ Connected!")
            
            # Отправляем токен
            await ws.send(json.dumps({"token": TOKEN}))
            print("📤 Token sent, waiting for response...")
            
            # Ждём ответ с таймаутом через asyncio.wait_for
            try:
                resp = await asyncio.wait_for(ws.recv(), timeout=5)
                print(f"📥 Response: {resp}")
                
                data = json.loads(resp)
                if data.get("type") == "connected":
                    print(f"✅ Success! Connection ID: {data.get('connection_id')}")
                    print("Waiting for messages... (Ctrl+C to stop)")
                    
                    # Ждём сообщения
                    while True:
                        msg = await ws.recv()
                        print(f"📨 Received: {msg}")
                else:
                    print(f"❌ Unexpected response: {data}")
                    
            except asyncio.TimeoutError:
                print("❌ Timeout waiting for response from server")
                
    except asyncio.TimeoutError:
        print("❌ Connection timeout")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test())
