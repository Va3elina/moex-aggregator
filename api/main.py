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
)
from api.routers import stats

# Логирование
from api.logger import setup_logging, get_logger
from api.middleware import RequestLoggingMiddleware, SlowRequestMiddleware

# Настраиваем логирование (до создания app!)
# Для продакшена: json_logs=True
setup_logging(
    level=os.getenv("LOG_LEVEL", "INFO"),
    json_logs=os.getenv("JSON_LOGS", "false").lower() == "true",
    log_to_file=True,
)
logger = get_logger()

# Создаём приложение
app = FastAPI(
    title="MOEX Aggregator API",
    description="API для данных Московской биржи: инструменты, свечи, открытый интерес",
    version="1.0.0",
)

# === MIDDLEWARE (порядок важен — снизу вверх!) ===

# 4. GZip сжатие (сжимает ответы больше 500 байт)
app.add_middleware(GZipMiddleware, minimum_size=500)

# 3. Настройка CORS (для фронтенда)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: ограничить для продакшена
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Логирование медленных запросов (> 1 сек)
app.add_middleware(SlowRequestMiddleware, threshold_ms=1000)

# 1. Логирование всех запросов (первый в цепочке)
app.add_middleware(
    RequestLoggingMiddleware,
    exclude_paths=["/health", "/assets", "/favicon.ico", "/vite.svg"]
)


# === СОБЫТИЯ ЖИЗНЕННОГО ЦИКЛА ===

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


# === РОУТЕРЫ ===

app.include_router(instruments_router)
app.include_router(candles_router)
app.include_router(open_interest_router)
app.include_router(chart_router)
app.include_router(stats.router)
app.include_router(heatmap_router)


# === СЛУЖЕБНЫЕ ЭНДПОИНТЫ ===

@app.get("/health")
def health():
    """Проверка работоспособности"""
    return {"status": "ok"}


@app.get("/api/info")
def api_info():
    """Информация об API"""
    return {
        "name": "MOEX Analytics API",
        "version": "1.0.0",
        "status": "running",
    }


# === РАЗДАЧА ФРОНТЕНДА ===

FRONTEND_DIR = Path(__file__).parent.parent / "frontend" / "dist"

if FRONTEND_DIR.exists():
    # Статика (JS, CSS, картинки)
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")


    # Все остальные пути -> index.html (SPA)
    @app.get("/{path:path}")
    async def serve_spa(path: str):
        file_path = FRONTEND_DIR / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(FRONTEND_DIR / "index.html")