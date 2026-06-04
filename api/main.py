"""
MOEX Analytics API
Главный файл приложения
"""

import asyncio
import mimetypes
import os
from pathlib import Path
from fastapi import FastAPI

# HLS mime types — Python mimetypes по умолчанию не знает .m3u8/.ts (в обоих
# случаях не video). Регистрируем явно, иначе Safari не понимает HLS playlist.
mimetypes.add_type("application/vnd.apple.mpegurl", ".m3u8")
mimetypes.add_type("video/mp2t", ".ts")
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
    seasonality_router,
    billing_router,
    cbr_flows_router,
    csp_report_router,
    exports_router,
    api_keys_router,
    public_api_router,
    fund_trades_router,
)
from api.routers import stats
from api.routers import auth  # ← НОВОЕ: Аутентификация
from api.routers import oauth  # ← OAuth (Google, VK, Telegram)
from api.routers import events  # ← SSE real-time events
from api.routers import analytics  # ← Custom analytics: POST /event + GET /stats

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
    # Development mode — credentials=False с wildcard (CORS spec требует)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
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
    # Прогреваем кэш в фоне
    asyncio.create_task(_warmup_cache())
    logger.info("🚀 MOEX Analytics API запущен (SSE enabled)", extra={
        "extra_data": {"type": "startup", "version": "1.0.0"}
    })


async def _warmup_cache():
    """Прогрев кэша при старте — дёргает тяжёлые эндпоинты чтобы пользователи не ждали."""
    import httpx
    await asyncio.sleep(2)  # дать uvicorn полностью подняться

    # Карта «Все акции» (/api/heatmap/stocks) — tier-gated (Basic+), поэтому
    # прогреть её через HTTP guest-клиентом нельзя (403 до записи в кеш).
    # Наполняем кеш напрямую через build_stocks_heatmap (минует tier-проверку).
    # Ключи должны совпадать с фронтом: getHeatmapData('market_cap','change_1d',groupBy).
    try:
        from api.routers.heatmap import build_stocks_heatmap
        for gb in ("sector", "none"):
            await asyncio.to_thread(build_stocks_heatmap, "market_cap", "change_1d", gb)
    except Exception as e:
        logger.warning(f"Heatmap stocks warmup failed: {e}")

    urls = [
        "/api/heatmap/imoex?color_by=change_1d&group_by=sector",
        "/api/funds/chart?category=money_market&period=6m",
        "/api/funds/catalog",
        "/api/breadth/current?ema_period=200&universe=imoex",
        "/api/breadth/history?ema_period=200&days=365&universe=imoex",
        "/api/chart/SR?sectype=SR&inst_type=futures&interval=24&clgroup=FIZ&show_oi=true&period=6m",
        "/api/buffett/cap-gdp?period=10y&smooth=false",
    ]
    try:
        async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=30) as client:
            for url in urls:
                try:
                    await client.get(url)
                except Exception:
                    pass
        logger.info(f"Cache warmup done ({len(urls)} endpoints)")
    except Exception as e:
        logger.warning(f"Cache warmup failed: {e}")

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
app.include_router(seasonality_router)
app.include_router(cbr_flows_router)  # ← /api/cbr-flows/* (ОРФР ЦБ — потоки участников)
app.include_router(csp_report_router)  # ← /api/csp-report (browser violation reports)
# KILL-SWITCH: публичный API + CSV-экспорт скрыты до официального запуска.
# По умолчанию (PUBLIC_API_CSV_ENABLED не задан) роутеры НЕ монтируются →
# /api/export/*, /api/keys/*, /api/v1/public/* отдают 404 ДЛЯ ВСЕХ (включая
# прямые вызовы мимо UI). Вернуть: env PUBLIC_API_CSV_ENABLED=1 + recreate api.
from api.billing.features import PUBLIC_API_CSV_ENABLED as _API_CSV_ON
if _API_CSV_ON:
    app.include_router(exports_router)  # ← /api/export/*.csv (Pro-only data download)
    app.include_router(api_keys_router)  # ← /api/keys/* (manage personal API keys)
    app.include_router(public_api_router)  # ← /api/v1/public/* (programmatic JSON access)
app.include_router(fund_trades_router)  # ← /api/fund-trades/* (диффы holdings БПИФов)
app.include_router(billing_router)  # ← /api/billing/* (подписки через ЮKassa)
app.include_router(auth.router, prefix="/api")  # ← НОВОЕ: /api/auth/*
app.include_router(oauth.router, prefix="/api")  # ← OAuth: /api/auth/oauth/*
app.include_router(events.router)  # ← SSE: /api/events/*
app.include_router(analytics.router)  # ← Analytics: /api/analytics/*

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
            # sw.js и manifest — никогда не кешировать
            if path in ("sw.js", "manifest.json"):
                return FileResponse(
                    file_path,
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
                )
            return FileResponse(file_path)
        # Pre-rendered HTML per route — кладётся scripts/prerender-meta.ts в postbuild.
        # dist/<route>/index.html содержит правильные <title>/<meta description>/
        # canonical/og/twitter/JSON-LD для конкретного URL. Без этого Yandex/Google
        # видят одинаковый title на всех страницах SPA (duplicate content penalty).
        if path:
            prerendered = FRONTEND_DIR / path / "index.html"
            if prerendered.exists() and prerendered.is_file():
                return FileResponse(
                    prerendered,
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
                )
        # SPA fallback — dist/index.html (главная + неизвестные routes)
        return FileResponse(
            FRONTEND_DIR / "index.html",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )