"""
WebSocket Load Test — Упрощенная версия (без создания чатов)
Тестирует:
  - Множество одновременных WS подключений
  - Auth через JWT
  - Ping/Pong
  - Логирование сессий и транзакций
"""

import asyncio, json, time, uuid, httpx, websockets, logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import List

# ─── Конфигурация ─────────────────────────────────────────────────────────────
BASE_HTTP = "http://localhost:8000"
BASE_WS   = "ws://localhost:8000/ws"
AUTH_URL = "http://localhost:8080"

NUM_CLIENTS = 10          # Количество одновременных клиентов
PING_INTERVAL = 2         # Интервал ping в секундах
TEST_DURATION = 15        # Длительность теста в секундах

# ─── Логирование ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("load_test")

session_log = open("/workspace/chat-service/logs/sessions.log", "w", encoding="utf-8")
transaction_log = open("/workspace/chat-service/logs/transactions.log", "w", encoding="utf-8")

def log_session(event: str, session_id: str, user: str, extra: dict = None):
    ts = datetime.now().isoformat()
    entry = {"timestamp": ts, "event": event, "session_id": session_id, "username": user, **(extra or {})}
    session_log.write(json.dumps(entry, ensure_ascii=False) + "\n")
    session_log.flush()
    logger.info(f"SESSION [{event}] {user}@{session_id[:8]}...")

def log_transaction(event: str, tx_id: str, session_id: str, data: dict = None):
    ts = datetime.now().isoformat()
    entry = {"timestamp": ts, "event": event, "tx_id": tx_id, "session_id": session_id, "data": data or {}}
    transaction_log.write(json.dumps(entry, ensure_ascii=False) + "\n")
    transaction_log.flush()
    logger.info(f"TX [{event}] {tx_id[:8]}...")

# ─── Статистика ───────────────────────────────────────────────────────────────
@dataclass
class Stats:
    total_connections: int = 0
    successful_auths: int = 0
    failed_auths: int = 0
    pings_sent: int = 0
    pongs_received: int = 0
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
        if not self.latencies: return 0
        sorted_lat = sorted(self.latencies)
        return sorted_lat[int(len(sorted_lat) * 0.95)]

stats = Stats()

# ─── Auth Helpers ─────────────────────────────────────────────────────────────
def auth_login(username: str, password: str) -> str | None:
    try:
        r = httpx.post(f"{AUTH_URL}/login", json={"username": username, "password": password}, timeout=10)
        if r.status_code == 200:
            return r.json()["data"]["access_token"]
        return None
    except Exception as e:
        logger.error(f"Login error for {username}: {e}")
        return None

async def prepare_tokens():
    """Подготовка токенов"""
    global TOKENS
    users = [("chatuser2026", "Test123!@#")]
    
    # Регистрируем дополнительных пользователей если нужно
    for i in range(1, NUM_CLIENTS + 1):
        users.append((f"loadtest{i:02d}", "Test123!@#"))
    
    logger.info("Подготовка токенов...")
    for username, password in users:
        # Пробуем зарегистрировать
        try:
            r = httpx.post(f"{AUTH_URL}/register", json={
                "username": username, "password": password,
                "confirm_password": password, "first_name": f"Load{i}"
            }, timeout=10)
        except: pass
        
        await asyncio.sleep(0.3)
        token = auth_login(username, password)
        if token:
            TOKENS.append((username, token))
            logger.info(f"✓ {username}")
        await asyncio.sleep(0.5)
    
    logger.info(f"Готово токенов: {len(TOKENS)}")

TOKENS = []

# ─── WebSocket Client ─────────────────────────────────────────────────────────
async def ws_client(client_id: int, username: str, token: str, duration: int):
    session_id = f"{username}_session_{client_id}_{uuid.uuid4().hex[:6]}"
    start_time = time.time()
    
    try:
        log_session("CONNECTING", session_id, username, {"client_id": client_id})
        
        ws = await websockets.connect(BASE_WS, open_timeout=10)
        stats.total_connections += 1
        
        # Auth
        tx_id = uuid.uuid4().hex
        log_transaction("AUTH_START", tx_id, session_id, {"username": username})
        auth_start = time.time()
        
        await ws.send(json.dumps({"token": token, "session_id": session_id}))
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
            await ws.close()
            return
        
        # Ping/Pong цикл
        end_time = time.time() + duration
        ping_count = 0
        
        while time.time() < end_time:
            ping_tx_id = uuid.uuid4().hex
            log_transaction("PING_SEND", ping_tx_id, session_id)
            ping_start = time.time()
            
            await ws.send(json.dumps({"type": "ping"}))
            stats.pings_sent += 1
            ping_count += 1
            
            try:
                pong_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                ping_latency = (time.time() - ping_start) * 1000
                stats.record_latency(ping_latency)
                
                if pong_resp.get("type") == "pong":
                    stats.pongs_received += 1
                    log_transaction("PONG_RECV", ping_tx_id, session_id, {"latency_ms": ping_latency})
            except asyncio.TimeoutError:
                stats.errors.append(f"Ping timeout for {username}")
                break
            
            await asyncio.sleep(PING_INTERVAL)
        
        # Закрытие
        await ws.close()
        log_session("DISCONNECTED", session_id, username, 
                   {"duration_sec": time.time() - start_time, "pings": ping_count})
        
    except Exception as e:
        stats.errors.append(str(e))
        log_session("ERROR", session_id, username, {"error": str(e)})
        logger.error(f"Client {client_id} ({username}) error: {e}")

# ─── Основной тест ────────────────────────────────────────────────────────────
async def run_load_test():
    print("\n" + "="*70)
    print("  WebSocket Load Test — Нагрузочное тестирование")
    print("="*70)
    print(f"  Клиентов: {NUM_CLIENTS}")
    print(f"  Длительность: {TEST_DURATION} сек")
    print(f"  Ping interval: {PING_INTERVAL} сек")
    print("="*70 + "\n")
    
    # Health check
    try:
        r = httpx.get(f"{BASE_HTTP}/health", timeout=5)
        if r.status_code == 200:
            logger.info(f"✓ Chat service health OK (uptime={r.json()['data']['uptime']:.0f}s)")
        else:
            logger.error(f"✗ Chat service health failed: {r.status_code}")
            return
    except Exception as e:
        logger.error(f"✗ Chat service unreachable: {e}")
        return
    
    # Подготовка токенов
    await prepare_tokens()
    
    if not TOKENS:
        logger.error("Нет токенов для теста!")
        return
    
    # Запуск клиентов
    start_time = time.time()
    logger.info(f"Запуск {len(TOKENS)} WebSocket клиентов...")
    
    tasks = []
    for i, (username, token) in enumerate(TOKENS[:NUM_CLIENTS]):
        task = asyncio.create_task(ws_client(i, username, token, TEST_DURATION))
        tasks.append(task)
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    total_time = time.time() - start_time
    
    # Результаты
    print("\n" + "="*70)
    print("  РЕЗУЛЬТАТЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ")
    print("="*70)
    print(f"  ⏱  Общее время: {total_time:.2f} сек")
    print(f"  🔌 Всего подключений: {stats.total_connections}")
    print(f"  ✅ Успешных авторизаций: {stats.successful_auths}")
    print(f"  ❌ Неудачных авторизаций: {stats.failed_auths}")
    print(f"  🏓 Отправлено ping: {stats.pings_sent}")
    print(f"  🏓 Получено pong: {stats.pongs_received}")
    print(f"  📊 Средняя задержка: {stats.avg_latency:.2f} мс")
    print(f"  📈 P95 задержка: {stats.p95_latency:.2f} мс")
    print(f"  ⚠️  Ошибок: {len(stats.errors)}")
    
    if stats.errors:
        print("\n  Последние ошибки:")
        for err in stats.errors[:5]:
            print(f"    - {err}")
    
    print("\n  Пропускная способность:")
    pings_per_sec = stats.pings_sent / total_time if total_time > 0 else 0
    print(f"    - Ping/сек: {pings_per_sec:.2f}")
    print(f"    - Подключений/сек: {stats.total_connections / total_time:.2f}")
    
    print("\n  Логи сохранены:")
    print(f"    - Сессии: /workspace/chat-service/logs/sessions.log")
    print(f"    - Транзакции: /workspace/chat-service/logs/transactions.log")
    print("="*70 + "\n")
    
    session_log.close()
    transaction_log.close()

if __name__ == "__main__":
    import os
    os.makedirs("/workspace/chat-service/logs", exist_ok=True)
    asyncio.run(run_load_test())
