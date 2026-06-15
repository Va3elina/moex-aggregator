"""
Redis TTL-кеш для API эндпоинтов.
Общий для всех uvicorn-воркеров.

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
import json
import logging
import os
import time
from typing import Any

import redis

logger = logging.getLogger(__name__)

DEFAULT_TTL = 1800  # 30 минут

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

_redis: redis.Redis | None = None


def _get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        # socket_timeout=2: ограничивает не только connect, но и чтение/команду —
        # «подвисший» (не refused) Redis не блокирует воркер дольше 2с до fail-open.
        _redis = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2, socket_timeout=2)
    return _redis


def get_or_set(key: str, value: Any = None, ttl: int = DEFAULT_TTL) -> Any | None:
    """
    Получить из кеша или сохранить значение.

    - get_or_set("key") — получить (вернёт None если нет или истёк)
    - get_or_set("key", data, ttl=300) — сохранить на 5 минут
    """
    try:
        r = _get_redis()
        if value is None:
            raw = r.get(key)
            if raw is not None:
                return json.loads(raw)
            return None
        else:
            r.setex(key, ttl, json.dumps(value, default=str))
            return value
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error: {e}")
        return None if value is None else value


def invalidate(prefix: str | None = None):
    """Инвалидирует кеш по префиксу или весь кеш."""
    try:
        r = _get_redis()
        if prefix is None:
            r.flushdb()
            logger.info("Cache CLEAR: flushed")
        else:
            cursor = 0
            count = 0
            while True:
                cursor, keys = r.scan(cursor, match=f"{prefix}*", count=200)
                if keys:
                    r.delete(*keys)
                    count += len(keys)
                if cursor == 0:
                    break
            if count:
                logger.info(f"Cache INVALIDATE '{prefix}': {count} entries removed")
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on invalidate: {e}")


def set_cache(key: str, value: Any, ttl: int = DEFAULT_TTL):
    """Явная запись в кеш. Используется cache_updater для инкрементальных обновлений."""
    try:
        r = _get_redis()
        r.setex(key, ttl, json.dumps(value, default=str))
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on set_cache: {e}")


def get_all_by_prefix(prefix: str) -> dict[str, Any]:
    """Вернуть все записи с данным префиксом. {key: data}"""
    result = {}
    try:
        r = _get_redis()
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"{prefix}*", count=200)
            if keys:
                values = r.mget(keys)
                for k, v in zip(keys, values):
                    if v is not None:
                        result[k] = json.loads(v)
            if cursor == 0:
                break
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on get_all_by_prefix: {e}")
    return result


def cache_stats() -> dict:
    """Статистика кеша для health endpoint."""
    try:
        r = _get_redis()
        info = r.info("keyspace")
        db_info = info.get("db0", {})
        return {
            "total_entries": db_info.get("keys", 0),
            "backend": "redis",
        }
    except (redis.RedisError, ConnectionError):
        return {"total_entries": 0, "backend": "redis", "status": "disconnected"}
