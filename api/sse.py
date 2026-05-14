"""
SSE (Server-Sent Events) менеджер.
Управляет подключёнными клиентами и рассылает события обновления данных.
"""

import asyncio
import json
import logging
from typing import AsyncGenerator

logger = logging.getLogger(__name__)


class SSEManager:
    """Менеджер SSE подключений."""

    def __init__(self):
        self._clients: set[asyncio.Queue] = set()

    async def subscribe(self) -> AsyncGenerator[str, None]:
        """Подписка клиента на события. Yields SSE-форматированные строки.

        Каждые 25 секунд шлёт heartbeat-comment `: keepalive\\n\\n` если
        нет real events — это не доходит до клиента как event, но
        держит TCP/HTTP connection «активным» в глазах nginx/CDN/браузер.
        Без heartbeat idle connection через ~60-90с считается мёртвым
        (особенно за HTTP/2 proxy) и рвётся → клиент видит ERR_HTTP2_*
        и 502 на reconnect → log spam в console.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=50)
        self._clients.add(queue)
        logger.info(f"SSE client connected (total: {len(self._clients)})")
        try:
            yield f"data: {json.dumps({'type': 'connected', 'clients': len(self._clients)})}\n\n"
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {data}\n\n"
                except asyncio.TimeoutError:
                    # Heartbeat — comment строка (browser ignores, no event fires)
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            self._clients.discard(queue)
            logger.info(f"SSE client disconnected (total: {len(self._clients)})")

    async def broadcast(self, event_data: str):
        """Отправить событие всем подключённым клиентам."""
        if not self._clients:
            return

        disconnected = set()
        for queue in self._clients:
            try:
                queue.put_nowait(event_data)
            except asyncio.QueueFull:
                disconnected.add(queue)
                logger.warning("SSE client queue full, disconnecting")

        self._clients -= disconnected

        if self._clients:
            logger.debug(f"SSE broadcast to {len(self._clients)} clients: {event_data[:100]}")

    @property
    def client_count(self) -> int:
        return len(self._clients)


# Глобальный синглтон
sse_manager = SSEManager()
