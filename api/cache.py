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


def get_or_compute(key, compute_fn, ttl: int = DEFAULT_TTL, *,
                   lock_ttl: int = 30, wait_timeout: float = 10.0,
                   poll_interval: float = 0.1):
    """Single-flight кэш: защита от cache-stampede.

    При промахе ТОЛЬКО первый воркер берёт Redis-лок и считает compute_fn;
    остальные ждут и читают его результат — вместо того чтобы все N воркеров
    одновременно кинулись пересчитывать тяжёлый запрос при истечении горячего
    ключа (ровно то, что давало 502-штормы).

    fail-open (кэш — оптимизация, не критический путь):
    - Redis недоступен / лок не взять → считаем сами (без дедупликации).
    - Вычислитель завис/упал и за wait_timeout результат не появился → считаем сами.
    - Исключения из compute_fn ПРОБРАСЫВАЮТСЯ и НЕ кэшируются (ошибки не залипают).

    compute_fn должна возвращать non-None (None = «нет данных» для get_or_set).
    """
    cached = get_or_set(key)
    if cached is not None:
        return cached

    lock_key = f"sflight:{key}"
    have_lock = False
    try:
        have_lock = bool(_get_redis().set(lock_key, "1", nx=True, ex=lock_ttl))
    except (redis.RedisError, ConnectionError):
        have_lock = True  # Redis лёг → считаем сами (fail-open, без дедупа)

    if have_lock:
        try:
            value = compute_fn()
            if value is not None:
                get_or_set(key, value, ttl)
            return value
        finally:
            try:
                _get_redis().delete(lock_key)
            except (redis.RedisError, ConnectionError):
                pass

    # Кто-то уже считает — ждём его результат, периодически опрашивая кэш.
    deadline = time.monotonic() + wait_timeout
    while time.monotonic() < deadline:
        time.sleep(poll_interval)
        cached = get_or_set(key)
        if cached is not None:
            return cached

    # Не дождались (медленный/упавший вычислитель) → считаем сами, fail-open.
    value = compute_fn()
    if value is not None:
        get_or_set(key, value, ttl)
    return value


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


def touch(key: str, ttl: int = DEFAULT_TTL) -> None:
    """Продлить TTL записи, не переписывая её содержимое.

    Нужен cache_updater'у для «замороженных» ключей (потолок свежести задан:
    Free-задержка 24ч). Их контент за день не меняется, но без продления TTL они
    протухают каждые 30 минут → холодный пересчёт 3–10 с на КАЖДОГО гостя (замер
    2026-08-10: 5м/6м = 6.2 МБ, cold 6.1 с). Раньше прогрев случайно делал сам
    set_cache внутри инкрементального апдейта; когда ветку закрыли ради tier-гейта
    (#1068), вместе с ней ушёл и прогрев. EXPIRE вместо SETEX — не гоняем 7 МБ
    JSON в Redis ради одного TTL.
    """
    try:
        _get_redis().expire(key, ttl)
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on touch: {e}")


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
