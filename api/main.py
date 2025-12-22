

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
)
from api.routers import stats

# Создаём приложение
app = FastAPI(
    title="MOEX Aggregator API",
    description="API для данных Московской биржи: инструменты, свечи, открытый интерес",
    version="1.0.0",
)

# GZip сжатие (сжимает ответы больше 500 байт)
app.add_middleware(GZipMiddleware, minimum_size=500)

# Настройка CORS (для фронтенда)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем роутеры
app.include_router(instruments_router)
app.include_router(candles_router)
app.include_router(open_interest_router)
app.include_router(chart_router)
app.include_router(stats.router)

# Путь к билду фронтенда
FRONTEND_DIR = Path(__file__).parent.parent / "frontend" / "dist"


@app.get("/health")
def health():
    """Проверка работоспособности"""
    return {"status": "ok"}


# Раздача фронтенда (после всех API роутов!)
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
