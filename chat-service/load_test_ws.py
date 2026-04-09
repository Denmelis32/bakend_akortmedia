"""
WebSocket Load Test — Нагрузочное тестирование
Покрывает:
  - Множество одновременных подключений
  - Логирование сессий и транзакций
  - Статистика по задержкам и пропускной способности
"""

import asyncio, json, time, uuid, httpx, websockets, logging
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List

# ─── Конфигурация ─────────────────────────────────────────────────────────────
BASE_HTTP = "http://localhost:8000"
BASE_WS   = "ws://localhost:8000/ws"

# Тестовые пользователи - будем регистрировать динамически
BASE_USERS = [
    ("chatuser2026", "Test123!@#"),
    ("loadusera", "Test123!@#"),
    ("loaduserb", "Test123!@#"),
    ("loaduserc", "Test123!@#"),
    ("loaduserd", "Test123!@#"),
]

AUTH_URL = "http://localhost:8080"
TOKENS = []  # Заполняется при запуске

# Параметры нагрузки
NUM_CLIENTS = 10          # Количество одновременных клиентов
MESSAGES_PER_CLIENT = 5   # Сообщений на клиента
CHAT_ID = None            # Будет создан динамически

# ─── Логирование ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("load_test")

# Файловые логи
session_log = open("/workspace/chat-service/logs/sessions.log", "w", encoding="utf-8")
transaction_log = open("/workspace/chat-service/logs/transactions.log", "w", encoding="utf-8")

def log_session(event: str, session_id: str, user: str, extra: dict = None):
    """Логирование сессий"""
    ts = datetime.now().isoformat()
    entry = {
        "timestamp": ts,
        "event": event,
        "session_id": session_id,
        "username": user,
        **(extra or {})
    }
    session_log.write(json.dumps(entry, ensure_ascii=False) + "\n")
    session_log.flush()
    logger.info(f"SESSION [{event}] {user}@{session_id[:8]}...")

def log_transaction(event: str, tx_id: str, session_id: str, data: dict = None):
    """Логирование транзакций"""
    ts = datetime.now().isoformat()
    entry = {
        "timestamp": ts,
        "event": event,
        "tx_id": tx_id,
        "session_id": session_id,
        "data": data or {}
    }
    transaction_log.write(json.dumps(entry, ensure_ascii=False) + "\n")
    transaction_log.flush()
    logger.info(f"TX [{event}] {tx_id[:8]}... session={session_id[:8]}...")

# ─── Статистика ───────────────────────────────────────────────────────────────
@dataclass
class Stats:
    total_connections: int = 0
    successful_auths: int = 0
    failed_auths: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    total_latency_ms: float = 0.0
    latencies: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    def record_latency(self, ms: float):
        self.latencies.append(ms)
        self.total_latency_ms += ms
    
    @property
    def avg_latency(self) -> float:
        return self.total_latency_ms / len(self.latencies) if self.latencies else 0
    
    @property
    def p95_latency(self) -> float:
        if not self.latencies:
            return 0
        sorted_lat = sorted(self.latencies)
        idx = int(len(sorted_lat) * 0.95)
        return sorted_lat[min(idx, len(sorted_lat)-1)]

stats = Stats()

# ─── HTTP Helper ──────────────────────────────────────────────────────────────
def http(method: str, path: str, token: str, **kwargs):
    return httpx.request(
        method, f"{BASE_HTTP}{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10, **kwargs
    )

def auth_login(username: str, password: str) -> str | None:
    """Логин и получение access токена"""
    try:
        r = httpx.post(f"{AUTH_URL}/login", json={
            "username": username,
            "password": password
        }, timeout=10)
        if r.status_code == 200:
            return r.json()["data"]["access_token"]
        logger.warning(f"Login failed for {username}: {r.status_code}")
        return None
    except Exception as e:
        logger.error(f"Login error for {username}: {e}")
        return None

def auth_register(username: str, password: str, first_name: str) -> bool:
    """Регистрация пользователя"""
    try:
        r = httpx.post(f"{AUTH_URL}/register", json={
            "username": username,
            "password": password,
            "confirm_password": password,
            "first_name": first_name
        }, timeout=10)
        if r.status_code in (200, 201):
            logger.info(f"Registered user: {username}")
            return True
        # Если пользователь уже существует - это OK
        if "taken" in r.text.lower() or "exists" in r.text.lower():
            logger.info(f"User {username} already exists")
            return True
        logger.warning(f"Registration failed for {username}: {r.status_code} - {r.text[:100]}")
        return False
    except Exception as e:
        logger.error(f"Registration error for {username}: {e}")
        return False

async def prepare_tokens():
    """Подготовка токенов: регистрация и логин пользователей"""
    global TOKENS
    logger.info("Подготовка токенов пользователей...")
    
    for username, password in BASE_USERS:
        # Пробуем зарегистрировать (если еще нет)
        first_name = username.replace("load", "Load ").replace("chatuser", "Chat").title()
        auth_register(username, password, first_name)
        # Логинимся и получаем токен
        token = auth_login(username, password)
        if token:
            TOKENS.append((username, token))
            logger.info(f"✓ Токен получен для {username}")
        else:
            logger.warning(f"⏳ Ждем 5 сек для снятия rate limit для {username}...")
            await asyncio.sleep(5)
            token = auth_login(username, password)
            if token:
                TOKENS.append((username, token))
                logger.info(f"✓ Токен получен для {username} (после ожидания)")
            else:
                logger.error(f"✗ Не удалось получить токен для {username}")
        
        # Пауза между запросами чтобы избежать rate limiting
        await asyncio.sleep(1)
    
    if not TOKENS:
        logger.error("Не удалось получить ни одного токена!")
        raise RuntimeError("No valid tokens available")
    
    logger.info(f"Готово токенов: {len(TOKENS)}")

# ─── WebSocket Client ─────────────────────────────────────────────────────────
async def ws_client(client_id: int, username: str, token: str, chat_id: str, barrier: asyncio.Barrier):
    """Один WebSocket клиент"""
    session_id = f"{username}_session_{client_id}_{uuid.uuid4().hex[:6]}"
    start_time = time.time()
    
    try:
        # Подключение
        log_session("CONNECTING", session_id, username, {"client_id": client_id})
        ws = await websockets.connect(BASE_WS, open_timeout=10)
        stats.total_connections += 1
        
        # Auth
        auth_start = time.time()
        tx_id = uuid.uuid4().hex
        log_transaction("AUTH_START", tx_id, session_id, {"username": username})
        
        await ws.send(json.dumps({
            "token": token,
            "session_id": session_id
        }))
        
        resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        auth_latency = (time.time() - auth_start) * 1000
        stats.record_latency(auth_latency)
        
        if resp.get("type") == "connected":
            stats.successful_auths += 1
            log_transaction("AUTH_SUCCESS", tx_id, session_id, {"latency_ms": auth_latency})
            log_session("CONNECTED", session_id, username, {"auth_latency_ms": auth_latency})
        else:
            stats.failed_auths += 1
            log_transaction("AUTH_FAILED", tx_id, session_id, {"response": resp})
            log_session("AUTH_FAILED", session_id, username, {"response": resp})
            await ws.close()
            return
        
        # Подписка на чат
        await barrier.wait()  # Синхронизация старта
        sub_tx_id = uuid.uuid4().hex
        log_transaction("SUBSCRIBE_START", sub_tx_id, session_id, {"chat_id": chat_id})
        
        await ws.send(json.dumps({"type": "subscribe", "chat_id": chat_id}))
        sub_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        
        if sub_resp.get("type") == "subscribed":
            log_transaction("SUBSCRIBE_SUCCESS", sub_tx_id, session_id)
            log_session("SUBSCRIBED", session_id, username, {"chat_id": chat_id[:8]})
        else:
            log_transaction("SUBSCRIBE_FAILED", sub_tx_id, session_id, {"response": sub_resp})
        
        # Отправка сообщений
        for msg_idx in range(MESSAGES_PER_CLIENT):
            msg_tx_id = uuid.uuid4().hex
            content = f"Load test message #{msg_idx} from {username} (client {client_id})"
            
            log_transaction("SEND_START", msg_tx_id, session_id, {"msg_idx": msg_idx, "content": content[:50]})
            send_start = time.time()
            
            # HTTP отправка сообщения
            r = http("POST", f"/chats/{chat_id}/messages", token,
                     json={"content": content, "message_type": "text",
                           "idempotency_key": uuid.uuid4().hex})
            
            send_latency = (time.time() - send_start) * 1000
            stats.record_latency(send_latency)
            
            if r.status_code in (200, 201):
                stats.messages_sent += 1
                log_transaction("SEND_SUCCESS", msg_tx_id, session_id, 
                               {"status_code": r.status_code, "latency_ms": send_latency})
            else:
                log_transaction("SEND_FAILED", msg_tx_id, session_id, 
                               {"status_code": r.status_code, "response": r.text[:100]})
                stats.errors.append(f"Send failed: {r.status_code}")
            
            await asyncio.sleep(0.1)  # Небольшая пауза между сообщениями
        
        # Сбор входящих сообщений
        received_msgs = []
        collect_start = time.time()
        while time.time() - collect_start < 3:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                if msg.get("type") == "new_message":
                    stats.messages_received += 1
                    received_msgs.append(msg)
                    log_transaction("MESSAGE_RECEIVED", uuid.uuid4().hex, session_id, 
                                   {"from_user": msg.get("data", {}).get("user_id", "?")[:8]})
            except asyncio.TimeoutError:
                break
        
        # Ping/Pong тест
        ping_tx_id = uuid.uuid4().hex
        log_transaction("PING_START", ping_tx_id, session_id)
        ping_start = time.time()
        await ws.send(json.dumps({"type": "ping"}))
        pong_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        ping_latency = (time.time() - ping_start) * 1000
        stats.record_latency(ping_latency)
        
        if pong_resp.get("type") == "pong":
            log_transaction("PONG_RECEIVED", ping_tx_id, session_id, {"latency_ms": ping_latency})
        
        # Закрытие
        close_tx_id = uuid.uuid4().hex
        log_transaction("DISCONNECT_START", close_tx_id, session_id)
        await ws.close()
        log_session("DISCONNECTED", session_id, username, 
                   {"duration_sec": time.time() - start_time, "msgs_sent": MESSAGES_PER_CLIENT})
        log_transaction("DISCONNECT_COMPLETE", close_tx_id, session_id)
        
    except Exception as e:
        stats.errors.append(str(e))
        log_session("ERROR", session_id, username, {"error": str(e)})
        logger.error(f"Client {client_id} ({username}) error: {e}")

# ─── Создание чата ────────────────────────────────────────────────────────────
def create_test_chat() -> str | None:
    """Создать тестовый чат для нагрузочного теста"""
    logger.info("Создание тестового чата...")
    token = TOKENS[-1][1]  # Используем последнего пользователя (chatuser2026)
    
    user_ids = [t[0] for t in TOKENS]
    r = http("POST", "/chats", token, json={
        "type": "group",
        "title": f"Load Test Chat {uuid.uuid4().hex[:6]}",
        "member_ids": user_ids  # Добавляем всех пользователей
    })
    
    if r.status_code in (200, 201):
        chat_id = r.json()["data"]["id"]
        logger.info(f"✓ Чат создан: {chat_id}")
        return chat_id
    else:
        logger.error(f"✗ Ошибка создания чата: {r.status_code} - {r.text[:200]}")
        return None

# ─── Основной тест ────────────────────────────────────────────────────────────
async def run_load_test():
    global CHAT_ID
    
    print("\n" + "="*70)
    print("  WebSocket Load Test — Нагрузочное тестирование")
    print("="*70)
    print(f"  Клиентов: {NUM_CLIENTS}")
    print(f"  Сообщений на клиента: {MESSAGES_PER_CLIENT}")
    print(f"  Ожидаемо всего сообщений: {NUM_CLIENTS * MESSAGES_PER_CLIENT}")
    print("="*70 + "\n")
    
    # Подготовка токенов
    await prepare_tokens()
    
    # Создание чата
    CHAT_ID = create_test_chat()
    if not CHAT_ID:
        logger.error("Не удалось создать чат, завершение теста")
        return
    
    # Барьер для синхронизации клиентов
    barrier = asyncio.Barrier(NUM_CLIENTS)
    
    # Запуск клиентов
    start_time = time.time()
    logger.info(f"Запуск {NUM_CLIENTS} клиентов...")
    
    tasks = []
    for i in range(NUM_CLIENTS):
        username, token = TOKENS[i % len(TOKENS)]
        task = asyncio.create_task(ws_client(i, username, token, CHAT_ID, barrier))
        tasks.append(task)
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    total_time = time.time() - start_time
    
    # Вывод статистики
    print("\n" + "="*70)
    print("  РЕЗУЛЬТАТЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ")
    print("="*70)
    print(f"  ⏱  Общее время: {total_time:.2f} сек")
    print(f"  🔌 Всего подключений: {stats.total_connections}")
    print(f"  ✅ Успешных авторизаций: {stats.successful_auths}")
    print(f"  ❌ Неудачных авторизаций: {stats.failed_auths}")
    print(f"  📤 Отправлено сообщений: {stats.messages_sent}")
    print(f"  📥 Получено сообщений (WS): {stats.messages_received}")
    print(f"  📊 Средняя задержка (auth/send/ping): {stats.avg_latency:.2f} мс")
    print(f"  📈 P95 задержка: {stats.p95_latency:.2f} мс")
    print(f"  ⚠️  Ошибок: {len(stats.errors)}")
    
    if stats.errors:
        print("\n  Последние ошибки:")
        for err in stats.errors[:5]:
            print(f"    - {err}")
    
    print("\n  Пропускная способность:")
    msgs_per_sec = stats.messages_sent / total_time if total_time > 0 else 0
    print(f"    - Сообщений/сек: {msgs_per_sec:.2f}")
    print(f"    - Подключений/сек: {stats.total_connections / total_time:.2f}")
    
    print("\n  Логи сохранены:")
    print(f"    - Сессии: /workspace/chat-service/logs/sessions.log")
    print(f"    - Транзакции: /workspace/chat-service/logs/transactions.log")
    print("="*70 + "\n")
    
    # Закрытие логов
    session_log.close()
    transaction_log.close()

# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    os.makedirs("/workspace/chat-service/logs", exist_ok=True)
    asyncio.run(run_load_test())
