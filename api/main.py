"""
MOEX Analytics API
Главный файл приложения
"""

import asyncio
import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from api.routers import (
    instruments_router,
    candles_router,
    open_interest_router,
    chart_router,
    heatmap_router,
    funds_router,
    breadth_router,
    buffett_router,
)
from api.routers import stats
from api.routers import auth  # ← НОВОЕ: Аутентификация
from api.routers import oauth  # ← OAuth (Google, VK, Telegram)
from api.routers import events  # ← SSE real-time events

# Логирование
from api.logger import setup_logging, get_logger

# Middleware
from api.middleware import (
    RequestLoggingMiddleware,
    SlowRequestMiddleware,
    SecurityHeadersMiddleware,
    RateLimitMiddleware,
    setup_exception_handlers,
)

# ═══════════════════════════════════════════════════════════════
# Определение среды
# ═══════════════════════════════════════════════════════════════

IS_PRODUCTION = os.getenv("ENV", "development") == "production"

# ═══════════════════════════════════════════════════════════════
# Настройка логирования (до создания app!)
# ═══════════════════════════════════════════════════════════════

setup_logging(
    level=os.getenv("LOG_LEVEL", "INFO"),
    json_logs=IS_PRODUCTION or os.getenv("JSON_LOGS", "false").lower() == "true",
    log_to_file=not IS_PRODUCTION,  # На Render файловая система ephemeral
)
logger = get_logger()

# ═══════════════════════════════════════════════════════════════
# Создание приложения
# ═══════════════════════════════════════════════════════════════

app = FastAPI(
    title="Фрейм API",
    description="API аналитики Московской биржи: инструменты, свечи, открытый интерес",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# ═══════════════════════════════════════════════════════════════
# Обработчики ошибок (без утечки stack trace)
# ═══════════════════════════════════════════════════════════════

setup_exception_handlers(app)

# ═══════════════════════════════════════════════════════════════
# MIDDLEWARE (порядок важен — выполняются снизу вверх!)
# ═══════════════════════════════════════════════════════════════

# 6. GZip сжатие
app.add_middleware(GZipMiddleware, minimum_size=500)

# 5. CORS
# ВАЖНО: В production заменить "*" на конкретные домены!
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

if ALLOWED_ORIGINS == ["*"]:
    # Development mode
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    # Production mode
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["Authorization", "Content-Type"],
        expose_headers=["X-Request-ID", "X-RateLimit-Remaining"],
        max_age=600,
    )

# 4. Security Headers
app.add_middleware(SecurityHeadersMiddleware)

# 3. Rate Limiting
app.add_middleware(
    RateLimitMiddleware,
    requests_per_minute=100,
    auth_requests_per_minute=10,
    heavy_requests_per_minute=30,
)

# 2. Логирование медленных запросов (> 1 сек)
app.add_middleware(SlowRequestMiddleware, threshold_ms=1000)

# 1. Логирование всех запросов
app.add_middleware(
    RequestLoggingMiddleware,
    exclude_paths=["/health", "/assets", "/favicon.ico", "/vite.svg"]
)

# ═══════════════════════════════════════════════════════════════
# События жизненного цикла
# ═══════════════════════════════════════════════════════════════

_notify_task = None

@app.on_event("startup")
async def startup_event():
    global _notify_task
    # Запускаем NOTIFY listener для SSE
    from api.notify_listener import start_notify_listener
    _notify_task = asyncio.create_task(start_notify_listener())
    logger.info("🚀 MOEX Analytics API запущен (SSE enabled)", extra={
        "extra_data": {"type": "startup", "version": "1.0.0"}
    })

@app.on_event("shutdown")
async def shutdown_event():
    global _notify_task
    if _notify_task:
        _notify_task.cancel()
        try:
            await _notify_task
        except asyncio.CancelledError:
            pass
    logger.info("👋 MOEX Analytics API остановлен", extra={
        "extra_data": {"type": "shutdown"}
    })

# ═══════════════════════════════════════════════════════════════
# Роутеры API
# ═══════════════════════════════════════════════════════════════

app.include_router(instruments_router)
app.include_router(candles_router)
app.include_router(open_interest_router)
app.include_router(chart_router)
app.include_router(stats.router)
app.include_router(heatmap_router)
app.include_router(funds_router)
app.include_router(breadth_router)
app.include_router(buffett_router)
app.include_router(auth.router, prefix="/api")  # ← НОВОЕ: /api/auth/*
app.include_router(oauth.router, prefix="/api")  # ← OAuth: /api/auth/oauth/*
app.include_router(events.router)  # ← SSE: /api/events/*

# ═══════════════════════════════════════════════════════════════
# Служебные эндпоинты
# ═══════════════════════════════════════════════════════════════

def _health_payload():
    result = {"status": "ok"}

    # Проверяем подключение к БД
    try:
        from api.database import get_engine
        from sqlalchemy import text
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        result["database"] = "ok"
    except Exception:
        result["database"] = "error"
        result["status"] = "degraded"

    return result


@app.get("/health")
def health():
    """Проверка работоспособности (для load balancer)"""
    return _health_payload()


@app.get("/api/health")
def api_health():
    """Health endpoint для фронтенда (через /api/*)"""
    return _health_payload()


@app.get("/api/info")
def api_info():
    """Информация об API"""
    return {
        "name": "MOEX Analytics API",
        "version": "1.0.0",
        "status": "running",
    }

# ═══════════════════════════════════════════════════════════════
# Раздача фронтенда (SPA)
# ═══════════════════════════════════════════════════════════════

FRONTEND_DIR = Path(__file__).parent.parent / "frontend" / "dist"

if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")

    @app.get("/{path:path}")
    async def serve_spa(path: str):
        file_path = FRONTEND_DIR / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(FRONTEND_DIR / "index.html")