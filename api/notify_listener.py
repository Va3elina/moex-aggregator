"""
PostgreSQL NOTIFY listener.
Слушает канал 'data_updated' и рассылает SSE-события клиентам.

Работает через asyncpg — единственное место в проекте где используется asyncpg.
Остальной API работает через pg8000 + SQLAlchemy.
"""

import asyncio
import json
import os
import logging

logger = logging.getLogger(__name__)

# Маппинг: источник события → префиксы кеша для инвалидации
SOURCE_CACHE_MAP = {
    "5min": ["chart:", "candles:", "oi:"],
    "hourly": ["chart:", "oi:"],
    "daily": None,  # None = очистить весь кеш
    "mv_refresh": ["heatmap:", "stats:"],
    "funds": ["funds_chart:", "fear_history:", "fear_index:"],
    "breadth": ["breadth:"],
    "buffett": ["buffett:"],
    "market_cap": ["heatmap:"],
}


def _get_asyncpg_dsn() -> str:
    """Получить DSN для asyncpg из переменных окружения."""
    # Пробуем async URL
    dsn = os.getenv("DB_URL_ASYNC", "")
    if dsn:
        return dsn

    # Фоллбэк: конвертируем sync URL
    dsn = os.getenv("DB_URL", "")
    dsn = dsn.replace("postgresql+pg8000://", "postgresql://")
    dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")
    return dsn


async def start_notify_listener():
    """
    Запускает LISTEN на PostgreSQL канале 'data_updated'.
    При получении NOTIFY — инвалидирует кеш и рассылает SSE.
    Автоматически переподключается при ошибках.
    """
    import asyncpg

    dsn = _get_asyncpg_dsn()
    if not dsn:
        logger.error("NOTIFY listener: DB_URL not configured, skipping")
        return

    while True:
        conn = None
        try:
            conn = await asyncpg.connect(dsn)
            logger.info("NOTIFY listener: connected to PostgreSQL")

            await conn.add_listener("data_updated", _on_notification)

            # Keepalive loop
            while True:
                await asyncio.sleep(30)
                try:
                    await conn.execute("SELECT 1")
                except Exception:
                    logger.warning("NOTIFY listener: keepalive failed, reconnecting...")
                    break

        except asyncio.CancelledError:
            logger.info("NOTIFY listener: shutting down")
            if conn:
                await conn.close()
            return

        except Exception as e:
            logger.error(f"NOTIFY listener error: {e}, reconnecting in 5s...")

        finally:
            if conn and not conn.is_closed():
                try:
                    await conn.remove_listener("data_updated", _on_notification)
                    await conn.close()
                except Exception:
                    pass

        await asyncio.sleep(5)


def _on_notification(conn, pid, channel, payload):
    """Callback для PostgreSQL NOTIFY."""
    asyncio.ensure_future(_handle_notification(payload))


async def _handle_notification(payload: str):
    """Обработка NOTIFY: инвалидация кеша + SSE broadcast."""
    from api.cache import invalidate
    from api.sse import sse_manager

    try:
        data = json.loads(payload)
        source = data.get("source", "unknown")
        logger.info(f"NOTIFY received: source={source}")

        # Инвалидация кеша
        prefixes = SOURCE_CACHE_MAP.get(source)
        if prefixes is None:
            invalidate()  # Очистить весь кеш
        else:
            for prefix in prefixes:
                invalidate(prefix)

        # SSE broadcast
        await sse_manager.broadcast(payload)

    except json.JSONDecodeError:
        logger.error(f"NOTIFY: invalid JSON payload: {payload[:200]}")
    except Exception as e:
        logger.error(f"NOTIFY handler error: {e}")
