"""
WebSocket Chat — полный тест
Покрывает:
  1. Health check
  2. Валидация токенов
  3. WS auth (оба пользователя)
  4. Ping / Pong
  5. Создание чата
  6. Подписка на чат
  7. HTTP send → WS receive (основной сценарий)
  8. exclude_user_id: отправитель НЕ получает своё по WS
  9. Read receipt (mark as read → WS событие)
 10. Typing indicator
"""

import asyncio, json, time, uuid, httpx, websockets

BASE_HTTP = "http://localhost:8000"
BASE_WS   = "ws://localhost:8000/ws"

# chatuser2026 — единственный реальный пользователь в БД
TOKEN_U1 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhNDhlYWM2OS0xYTE5LTRjNGEtOWExMC0wZTE0NjliM2Q4ODgiLCJ1c2VybmFtZSI6ImNoYXR1c2VyMjAyNiIsImZpcnN0X25hbWUiOiJDaGF0Iiwicm9sZSI6InVzZXIiLCJ2ZXJpZmllZCI6ZmFsc2UsImV4cCI6MTc3NTkyMTk5MSwidHlwZSI6ImFjY2VzcyIsImp0aSI6IjNiZTFhMTQwLThmY2YtNGM1My04NzI5LWQ3OTU0Y2RlMjMwNCIsImlhdCI6MTc3NTMxNzE5MX0.oQLt6g0pHiJGw_TnFzUAqhLcEbL6mz5tEshM7v0phpA"
# testus123111er — фиктивный пользователь, JWT валиден, но нет в YDB
# Может подключаться по WS (auth JWT-only), но HTTP-запросы с проверкой членства → 403
TOKEN_U2 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJiNTlmYmQ3MC0yYjIwLTVkNWItYWIyMS0xZjI1NzBjNGU5OTkiLCJ1c2VybmFtZSI6InRlc3R1czEyMzExMWVyIiwiZmlyc3RfbmFtZSI6IlRlc3QiLCJyb2xlIjoidXNlciIsInZlcmlmaWVkIjpmYWxzZSwiZXhwIjoxNzc1OTIxOTkxLCJ0eXBlIjoiYWNjZXNzIiwianRpIjoiNzMzNDhlNTctYTNlNy00YTliLWI5NzUtYzljNjgzMTI5MzI4IiwiaWF0IjoxNzc1MzE3MTkxfQ.tByQ57CVoQ7Zcoz4w_pERQmAo3UEejBsAs36PpqV9zc"
USER1_ID = "a48eac69-1a19-4c4a-9a10-0e1469b3d888"
USER2_ID = "b59fbd70-2b20-5d5b-ab21-1f2570c4e999"

G = "\033[92m✓\033[0m"
R = "\033[91m✗\033[0m"
I = "\033[94m→\033[0m"
results: list[tuple[str,str]] = []

def ok(msg):   results.append(("PASS", msg)); print(f"  {G} {msg}")
def fail(msg): results.append(("FAIL", msg)); print(f"  {R} {msg}")
def info(msg): print(f"  {I} {msg}")

# ─── helpers ──────────────────────────────────────────────────────────────────

async def ws_connect(token: str, name: str):
    """Подключиться, авторизоваться, вернуть websocket."""
    ws = await websockets.connect(BASE_WS, open_timeout=5)
    await ws.send(json.dumps({"token": token, "session_id": f"{name}_{int(time.time())}"}))
    resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
    assert resp["type"] == "connected", f"Expected connected, got {resp}"
    return ws

async def ws_subscribe(ws, chat_id: str):
    await ws.send(json.dumps({"type": "subscribe", "chat_id": chat_id}))
    resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
    assert resp["type"] == "subscribed"

async def collect(ws, timeout=6.0) -> list[dict]:
    """Собрать все сообщения за timeout секунд."""
    msgs = []
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
            msgs.append(json.loads(raw))
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            break
    return msgs

def http(method: str, path: str, token: str, **kwargs):
    return httpx.request(
        method, f"{BASE_HTTP}{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10, **kwargs
    )

# ─── тесты ────────────────────────────────────────────────────────────────────

def test_health():
    print("\n\033[1m[1] Health\033[0m")
    r = httpx.get(f"{BASE_HTTP}/health", timeout=5)
    if r.status_code == 200:
        ok(f"chat-service up (uptime={r.json()['data']['uptime']:.0f}s)")
    else:
        fail(f"health {r.status_code}")


def test_tokens():
    print("\n\033[1m[2] Token validation\033[0m")
    for name, tok in [("chatuser2026", TOKEN_U1), ("testus123111er", TOKEN_U2)]:
        r = httpx.post(f"{BASE_HTTP}/validate", json={"token": tok}, timeout=5)
        d = r.json().get("data", {})
        if d.get("valid"):
            ok(f"{name} token valid")
        else:
            fail(f"{name} token invalid: {r.text[:80]}")


async def test_ws_auth():
    print("\n\033[1m[3] WS authentication\033[0m")
    for name, tok in [("chatuser2026", TOKEN_U1), ("testus123111er", TOKEN_U2)]:
        try:
            ws = await ws_connect(tok, name)
            ok(f"{name} authenticated via WS")
            await ws.close()
        except Exception as e:
            fail(f"{name} WS auth failed: {e}")


async def test_ping():
    print("\n\033[1m[4] Ping / Pong\033[0m")
    try:
        ws = await ws_connect(TOKEN_U1, "ping_test")
        await ws.send(json.dumps({"type": "ping"}))
        resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        if resp.get("type") == "pong":
            ok("ping → pong")
        else:
            fail(f"expected pong, got {resp}")
        await ws.close()
    except Exception as e:
        fail(f"ping test: {e}")


def create_chat() -> str | None:
    print("\n\033[1m[5] Создание чата\033[0m")
    r = http("POST", "/chats", TOKEN_U1, json={
        "type": "group", "title": f"WS Test {uuid.uuid4().hex[:4]}",
        "member_ids": [USER2_ID]
    })
    if r.status_code in (200, 201):
        chat_id = r.json()["data"]["id"]
        ok(f"Чат создан: {chat_id}")
        return chat_id
    fail(f"POST /chats {r.status_code}: {r.text[:120]}")
    return None


async def test_subscribe(chat_id: str):
    print(f"\n\033[1m[6] Subscribe to chat\033[0m")
    try:
        ws = await ws_connect(TOKEN_U1, "sub_test")
        await ws_subscribe(ws, chat_id)
        ok(f"Подписка на {chat_id[:8]}… OK")
        await ws.close()
    except Exception as e:
        fail(f"subscribe: {e}")


async def test_message_flow(chat_id: str):
    """[7] User1 шлёт → User2 (WS-слушатель) получает по WS. Страница не перезагружается."""
    print(f"\n\033[1m[7] HTTP send → WS receive (no page reload)\033[0m")

    u2_received = asyncio.Event()
    u2_message  = {}

    async def user2_listener():
        # U2 — фиктивный, но WS-авторизация проходит (JWT валиден)
        ws = await ws_connect(TOKEN_U2, "u2_listener")
        await ws_subscribe(ws, chat_id)
        u2_received.set()          # слушаем, готовы
        msgs = await collect(ws, timeout=8)
        for m in msgs:
            if m.get("type") == "new_message":
                u2_message.update(m)
                break
        await ws.close()

    async def user1_sender():
        await asyncio.wait_for(u2_received.wait(), timeout=5)
        text = f"привет {uuid.uuid4().hex[:6]}"
        info(f"User1 шлёт: '{text}'")
        r = http("POST", f"/chats/{chat_id}/messages", TOKEN_U1,
                 json={"content": text, "message_type": "text",
                       "idempotency_key": str(uuid.uuid4())})
        info(f"HTTP POST → {r.status_code}")
        if r.status_code in (200, 201):
            ok("Сообщение сохранено в БД (HTTP 201)")
            return text
        fail(f"HTTP send failed: {r.text[:120]}")
        return None

    text_sent, _ = await asyncio.gather(user1_sender(), user2_listener())

    if u2_message.get("type") == "new_message":
        recv = u2_message["data"]["message"]["content"]
        if recv == text_sent:
            ok(f"User2 получил по WS: '{recv}' ✓ (контент совпадает)")
        else:
            fail(f"контент не совпал: sent='{text_sent}' recv='{recv}'")
    else:
        fail("User2 НЕ получил new_message по WS")

    return text_sent


async def test_exclude_sender(chat_id: str):
    """[8] Отправитель НЕ должен получать своё сообщение по WS (exclude_user_id).

    Схема: U1 слушает WS, U1 шлёт HTTP. U2 (WS-only) тоже слушает.
    Ожидания:
      - U2 WS получает сообщение U1 ✓
      - U1 WS НЕ получает своё сообщение ✓ (exclude_user_id работает)
    """
    print(f"\n\033[1m[8] exclude_user_id: отправитель не получает своё по WS\033[0m")

    u1_msgs, u2_msgs = [], []
    ready = asyncio.Event()

    async def listen_u1():
        ws = await ws_connect(TOKEN_U1, "u1_listen")
        await ws_subscribe(ws, chat_id)
        ready.set()
        msgs = await collect(ws, timeout=7)
        u1_msgs.extend([m for m in msgs if m.get("type") == "new_message"])
        await ws.close()

    async def listen_u2():
        ws = await ws_connect(TOKEN_U2, "u2_listen")
        await ws_subscribe(ws, chat_id)
        msgs = await collect(ws, timeout=7)
        u2_msgs.extend([m for m in msgs if m.get("type") == "new_message"])
        await ws.close()

    async def sender():
        await asyncio.wait_for(ready.wait(), timeout=5)
        await asyncio.sleep(0.3)   # дать u2 подписаться тоже
        t = f"msg_{uuid.uuid4().hex[:4]}"
        r = http("POST", f"/chats/{chat_id}/messages", TOKEN_U1,
                 json={"content": t, "message_type": "text",
                       "idempotency_key": str(uuid.uuid4())})
        info(f"U1 послал '{t}' → HTTP {r.status_code}")
        return t

    t, _, _ = await asyncio.gather(sender(), listen_u1(), listen_u2())

    # U2 должен получить сообщение U1
    u2_contents = [m["data"]["message"]["content"] for m in u2_msgs]
    if t in u2_contents:
        ok(f"U2 (WS-listener) получил сообщение U1: '{t}' ✓")
    else:
        fail(f"U2 не получил '{t}', получил: {u2_contents}")

    # U1 НЕ должен получить своё же сообщение по WS
    u1_contents = [m["data"]["message"]["content"] for m in u1_msgs]
    if t not in u1_contents:
        ok(f"U1 НЕ получил своё сообщение по WS (exclude_user_id работает) ✓")
    else:
        fail(f"U1 получил своё же сообщение по WS — exclude_user_id НЕ работает")


async def test_read_receipt(chat_id: str, last_msg_id: str):
    """[9] U1 вызывает markAsRead → все WS-слушатели чата получают read_receipt.

    Используем две WS-сессии U1 (session A слушает, U1 делает HTTP /read).
    read_receipt рассылается всем подписчикам (без exclude).
    """
    print(f"\n\033[1m[9] Read receipt\033[0m")

    receipt_event = asyncio.Event()
    receipt_data  = {}

    async def ws_listener():
        # Отдельная сессия U1 слушает WS
        ws = await ws_connect(TOKEN_U1, "u1_receipt_listener")
        await ws_subscribe(ws, chat_id)
        receipt_event.set()
        msgs = await collect(ws, timeout=8)
        for m in msgs:
            if m.get("type") == "read_receipt":
                receipt_data.update(m)
                break
        await ws.close()

    async def reader():
        await asyncio.wait_for(receipt_event.wait(), timeout=5)
        info(f"U1 вызывает markAsRead до message_id={last_msg_id}")
        r = http("POST", f"/chats/{chat_id}/read", TOKEN_U1,
                 json={"message_id": last_msg_id})
        info(f"POST /read → {r.status_code}")
        if r.status_code == 200:
            ok("markAsRead HTTP 200")
        else:
            fail(f"markAsRead {r.status_code}: {r.text[:120]}")

    await asyncio.gather(ws_listener(), reader())

    if receipt_data.get("type") == "read_receipt":
        d = receipt_data["data"]
        ok(f"WS-listener получил read_receipt: user={d.get('user_id','?')[:8]}… msg={d.get('message_id')}")
    else:
        fail("WS-listener НЕ получил read_receipt по WS")


async def test_typing(chat_id: str):
    """[10] User1 отправляет typing по WS → User2 (WS-слушатель) получает typing_status."""
    print(f"\n\033[1m[10] Typing indicator\033[0m")

    typing_events = []
    ready = asyncio.Event()

    async def u2_listener():
        ws = await ws_connect(TOKEN_U2, "u2_typing")
        await ws_subscribe(ws, chat_id)
        ready.set()
        msgs = await collect(ws, timeout=8)
        typing_events.extend([m for m in msgs if m.get("type") == "typing_status"])
        await ws.close()

    async def u1_typer():
        ws = await ws_connect(TOKEN_U1, "u1_typer")
        await ws_subscribe(ws, chat_id)
        await asyncio.wait_for(ready.wait(), timeout=5)
        await asyncio.sleep(0.2)  # дать u2 готовность
        await ws.send(json.dumps({"type": "typing", "chat_id": chat_id, "is_typing": True}))
        info("U1 → typing=True")
        await asyncio.sleep(0.5)
        await ws.send(json.dumps({"type": "typing", "chat_id": chat_id, "is_typing": False}))
        info("U1 → typing=False")
        await asyncio.sleep(0.3)
        await ws.close()

    await asyncio.gather(u2_listener(), u1_typer())

    if typing_events:
        d = typing_events[0]["data"]
        ok(f"U2 получил typing_status: user={d.get('user_id','?')[:8]}… is_typing={d.get('is_typing')}")
    else:
        fail("U2 не получил typing_status")


# ─── main ─────────────────────────────────────────────────────────────────────

async def main():
    print("\n" + "="*60)
    print("  Chat WebSocket — Full Test Suite")
    print("="*60)

    test_health()
    test_tokens()
    await test_ws_auth()
    await test_ping()

    chat_id = create_chat()
    if not chat_id:
        print("\n❌ Нет chat_id, пропускаем WS тесты\n")
        return

    await test_subscribe(chat_id)
    await test_message_flow(chat_id)
    await test_exclude_sender(chat_id)

    # Для read receipt нужен реальный message_id — берём из HTTP
    r = http("GET", f"/chats/{chat_id}/messages", TOKEN_U1)
    last_msg_id = None
    if r.status_code == 200:
        msgs = r.json().get("data", {}).get("messages", [])
        if msgs:
            last_msg_id = str(msgs[-1].get("id") or msgs[-1].get("message_id"))

    if last_msg_id:
        await test_read_receipt(chat_id, last_msg_id)
    else:
        fail("Не удалось получить message_id для read receipt теста")

    await test_typing(chat_id)

    # ── итог ──
    print("\n" + "="*60)
    print("  РЕЗУЛЬТАТЫ")
    print("="*60)
    passed = sum(1 for s,_ in results if s == "PASS")
    failed = sum(1 for s,_ in results if s == "FAIL")
    for status, msg in results:
        sym = G if status == "PASS" else R
        print(f"  {sym} {msg}")
    print(f"\n  Итого: {passed} passed, {failed} failed")
    print("="*60 + "\n")

if __name__ == "__main__":
    asyncio.run(main())
