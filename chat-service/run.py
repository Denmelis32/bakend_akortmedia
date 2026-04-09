#!/usr/bin/env python3
"""
RUN SCRIPT FOR CHAT SERVICE v2.0
Упрощенный запуск для виртуальной машины с мониторингом транзакций
"""

import os
import sys
import asyncio
import uvicorn
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

async def start_monitoring():
    """Запустить фоновый мониторинг транзакций"""
    try:
        # Импортируем монитор после загрузки приложения
        from handlers.common import TransactionMonitor
        
        # Даем приложению немного времени на старт
        await asyncio.sleep(5)
        
        # Запускаем периодическое логирование
        TransactionMonitor.start_periodic_logging(interval=30)
        
        print("✅ Transaction monitor started (logs every 30 seconds)")
        
        # Логируем начальное состояние
        await asyncio.sleep(2)
        TransactionMonitor.log_status()
        
    except Exception as e:
        print(f"⚠️ Failed to start monitor: {e}")

def run_with_monitoring():
    """Запустить сервер с мониторингом"""
    # Параметры запуска
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    reload = os.getenv("RELOAD", "true").lower() == "true"
    
    print("=" * 60)
    print("🚀 CHAT SERVICE v2.0 - STARTING WITH MONITORING")
    print("=" * 60)
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Reload: {reload}")
    print("=" * 60)
    
    # Для режима reload нужно запустить мониторинг особым образом
    if reload:
        print("⚠️ Reload mode: Transaction monitor will start after app loads")
        # В reload режиме мониторинг запустится при старте приложения
        # Добавляем startup event в app
        
        # Запускаем сервер
        uvicorn.run(
            "app:app",
            host=host,
            port=port,
            reload=reload,
            log_level="info"
        )
    else:
        # В обычном режиме запускаем мониторинг в том же процессе
        import threading
        
        def run_server():
            uvicorn.run(
                "app:app",
                host=host,
                port=port,
                reload=False,
                log_level="info"
            )
        
        # Запускаем сервер в отдельном потоке
        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        
        # Запускаем мониторинг в основном потоке
        try:
            asyncio.run(start_monitoring())
            # Держим поток живым
            server_thread.join()
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            sys.exit(0)

if __name__ == "__main__":
    # Запускаем с мониторингом
    run_with_monitoring()