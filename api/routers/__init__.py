"""
API роутеры
"""
from api.routers.instruments import router as instruments_router
from api.routers.candles import router as candles_router
from api.routers.open_interest import router as open_interest_router
from api.routers.open_interest import oi_router as oi_router
from api.routers.chart import router as chart_router
from api.routers.heatmap import router as heatmap_router
from api.routers.funds import router as funds_router
from api.routers.breadth import router as breadth_router
from api.routers.buffett import router as buffett_router
from api.routers.seasonality import router as seasonality_router
from api.routers.billing import router as billing_router
from api.routers.cbr_flows import router as cbr_flows_router
from api.routers.csp_report import router as csp_report_router
from api.routers.exports import router as exports_router
from api.routers.api_keys import router as api_keys_router
from api.routers.public_api import router as public_api_router
from api.routers.fund_trades import router as fund_trades_router

__all__ = [
    "instruments_router",
    "candles_router",
    "open_interest_router",
    "oi_router",
    "chart_router",
    "heatmap_router",
    "funds_router",
    "breadth_router",
    "buffett_router",
    "seasonality_router",
    "billing_router",
    "cbr_flows_router",
    "csp_report_router",
    "exports_router",
    "api_keys_router",
    "public_api_router",
    "fund_trades_router",
]