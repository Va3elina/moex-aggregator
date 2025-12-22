"""
FastAPI приложение — точка входа
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

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


@app.get("/")
def root():
    """Главная страница API"""
    return {
        "message": "MOEX Aggregator API",
        "docs": "/docs",
        "endpoints": {
            "instruments": "/api/instruments",
            "candles": "/api/candles/{sec_id}",
            "open_interest": "/api/openinterest/{sectype}",
            "chart": "/api/chart/{sec_id}",
            "stats": "/api/stats",
        }
    }


@app.get("/health")
def health():
    """Проверка работоспособности"""
    return {"status": "ok"}