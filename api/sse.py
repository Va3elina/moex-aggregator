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
        """Подписка клиента на события. Yields SSE-форматированные строки."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=50)
        self._clients.add(queue)
        logger.info(f"SSE client connected (total: {len(self._clients)})")
        try:
            # Отправляем приветственное событие
            yield f"data: {json.dumps({'type': 'connected', 'clients': len(self._clients)})}\n\n"
            while True:
                data = await queue.get()
                yield f"data: {data}\n\n"
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
