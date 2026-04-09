import asyncio
import websockets
import json

# НОВЫЙ ТОКЕН для пользователя chatuser2026 (получен при логине)
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhNDhlYWM2OS0xYTE5LTRjNGEtOWExMC0wZTE0NjliM2Q4ODgiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZmlyc3RfbmFtZSI6IkNoYXQiLCJ1c2VybmFtZSI6ImNoYXR1c2VyMjAyNiIsImV4cCI6MTc3NDUxNDk5MiwidHlwZSI6ImFjY2VzcyIsImp0aSI6ImViZmU4ZTY2LTNjMTYtNGI3Ny1hNDBjLTVlNGUyYjU4NWVlNiIsImlhdCI6MTc3NDUxNDA5Mn0.cihffM0o-45QyhKhoDRs0tkmTWyjNSLeiaz_0E_KFy0"

async def test():
    uri = "ws://158.160.16.246/ws"
    print(f"Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as ws:
            print("✅ Connected!")
            
            await ws.send(json.dumps({"token": TOKEN}))
            print("📤 Token sent, waiting for response...")
            
            try:
                resp = await asyncio.wait_for(ws.recv(), timeout=5)
                print(f"📥 Response: {resp}")
                
                data = json.loads(resp)
                if data.get("type") == "connected":
                    print(f"✅ Authenticated! Connection ID: {data.get('connection_id')}")
                    print("Waiting for messages... (Ctrl+C to stop)")
                    print("=" * 60)
                    
                    while True:
                        msg = await ws.recv()
                        print(f"📨 Received: {msg}")
                else:
                    print(f"❌ Unexpected response: {data}")
                    
            except asyncio.TimeoutError:
                print("❌ Timeout waiting for response from server")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(test())
    except KeyboardInterrupt:
        print("\n👋 Stopped by user")
