"""
In-memory TTL-кеш для API эндпоинтов.
Снижает нагрузку на БД для часто запрашиваемых данных.

Использование:
    from api.cache import get_or_set

    @router.get("/heavy-endpoint")
    async def heavy_endpoint(param: str):
        cache_key = f"heavy:{param}"

        cached = get_or_set(cache_key)
        if cached is not None:
            return cached

        result = expensive_computation()
        get_or_set(cache_key, result, ttl=300)
        return result
"""
import time
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Глобальное хранилище кеша: {key: (data, expire_at)}
_cache: dict[str, tuple[Any, float]] = {}


def get_or_set(key: str, value: Any = None, ttl: int = 300) -> Any | None:
    """
    Получить из кеша или сохранить значение.

    - get_or_set("key") — получить (вернёт None если нет или истёк)
    - get_or_set("key", data, ttl=300) — сохранить на 5 минут

    Args:
        key: Ключ кеша
        value: Значение для сохранения (None = только чтение)
        ttl: Время жизни в секундах
    """
    now = time.time()

    if value is None:
        # GET
        if key in _cache:
            data, expire_at = _cache[key]
            if now < expire_at:
                logger.debug(f"Cache HIT: {key}")
                return data
            else:
                del _cache[key]
        return None
    else:
        # SET
        _cache[key] = (value, now + ttl)
        logger.debug(f"Cache SET: {key} (ttl={ttl}s)")
        return value


def invalidate(prefix: str | None = None):
    """Инвалидирует кеш по префиксу или весь кеш."""
    global _cache
    if prefix is None:
        count = len(_cache)
        _cache.clear()
        logger.info(f"Cache CLEAR: {count} entries removed")
    else:
        keys_to_delete = [k for k in _cache if k.startswith(prefix)]
        for k in keys_to_delete:
            del _cache[k]
        if keys_to_delete:
            logger.info(f"Cache INVALIDATE '{prefix}': {len(keys_to_delete)} entries removed")


def cache_stats() -> dict:
    """Статистика кеша для health endpoint."""
    now = time.time()
    total = len(_cache)
    expired = sum(1 for _, (_, exp) in _cache.items() if exp <= now)
    return {"total_entries": total, "expired": expired, "active": total - expired}
