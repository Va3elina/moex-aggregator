"""
SSE endpoint для real-time обновлений данных.
Клиенты подписываются на /api/events/stream и получают уведомления
когда оркестратор обновляет данные.
"""

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from api.sse import sse_manager

router = APIRouter(prefix="/api/events", tags=["events"])


@router.get("/stream")
async def event_stream():
    """
    SSE endpoint — поток событий обновления данных.

    События приходят когда оркестратор обновляет данные в БД.
    Формат: JSON с полями source, tables, ts.

    Пример события:
        data: {"source": "5min", "tables": ["candles", "open_interest"], "ts": "2024-01-15T10:05:00"}
    """
    return StreamingResponse(
        sse_manager.subscribe(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Отключить буферизацию nginx
        },
    )


@router.get("/status")
async def event_status():
    """Статус SSE — количество подключённых клиентов."""
    return {
        "connected_clients": sse_manager.client_count,
    }
