"""
MOEX Analytics API
Главный файл приложения
"""

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
# Настройка логирования (до создания app!)
# ═══════════════════════════════════════════════════════════════

setup_logging(
    level=os.getenv("LOG_LEVEL", "INFO"),
    json_logs=os.getenv("JSON_LOGS", "false").lower() == "true",
    log_to_file=True,
)
logger = get_logger()

# ═══════════════════════════════════════════════════════════════
# Создание приложения
# ═══════════════════════════════════════════════════════════════

app = FastAPI(
    title="MOEX Aggregator API",
    description="API для данных Московской биржи: инструменты, свечи, открытый интерес",
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

@app.on_event("startup")
async def startup_event():
    logger.info("🚀 MOEX Analytics API запущен", extra={
        "extra_data": {"type": "startup", "version": "1.0.0"}
    })

@app.on_event("shutdown")
async def shutdown_event():
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

# ═══════════════════════════════════════════════════════════════
# Служебные эндпоинты
# ═══════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    """Проверка работоспособности (для load balancer)"""
    return {"status": "ok"}


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