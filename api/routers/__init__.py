"""
API роутеры
"""
from api.routers.instruments import router as instruments_router
from api.routers.candles import router as candles_router
from api.routers.open_interest import router as open_interest_router
from api.routers.chart import router as chart_router
from api.routers.heatmap import router as heatmap_router

__all__ = [
    "instruments_router",
    "candles_router",
    "open_interest_router",
    "chart_router",
    "heatmap_router",
]