#!/usr/bin/env python3
"""
Точка входа для запуска feed-service на ВМ
"""
import os
import sys
import uvicorn
from pathlib import Path
from dotenv import load_dotenv

# Добавляем путь к проекту
sys.path.insert(0, str(Path(__file__).parent))

# Загружаем переменные из .env файла
load_dotenv()

if __name__ == "__main__":
    # Порт из окружения или по умолчанию 8000
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    
    print("=" * 60)
    print(f"🚀 FEED SERVICE STARTING on {host}:{port}")
    print("=" * 60)
    
    # Запускаем FastAPI приложение
    reload = os.environ.get("RELOAD", "true").lower() == "true"
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload,
        workers=1 if reload else 4,
        log_level="info",
        access_log=True,
        proxy_headers=True,
        forwarded_allow_ips="*"
    )
