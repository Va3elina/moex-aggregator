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
import gzip
import json
import logging
import os
import time
from typing import Any

import redis

logger = logging.getLogger(__name__)

DEFAULT_TTL = 1800  # 30 минут

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Уровень сжатия для gzip-кеша. Замер на проде 2026-08-10 (график 5м/6м, 6.55 МБ
# JSON): level=1 → 1150 КБ за 37 мс, level=6 → 867 КБ за 119 мс, распаковка в
# обоих случаях ~15-20 мс. Берём 6: сжимаем РЕДКО (при записи в кеш), а отдаём
# часто — экономия 283 КБ на каждой отдаче важнее 80 мс раз в пять минут.
# Совпадает с тем, что раньше выдавал nginx (797 КБ), — трафик не вырос.
GZIP_LEVEL = 6

_redis: redis.Redis | None = None
_redis_bytes: redis.Redis | None = None


def _get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        # socket_timeout=2: ограничивает не только connect, но и чтение/команду —
        # «подвисший» (не refused) Redis не блокирует воркер дольше 2с до fail-open.
        _redis = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2, socket_timeout=2)
    return _redis


def _get_redis_bytes() -> redis.Redis:
    """Отдельный клиент БЕЗ decode_responses — для gzip-значений.

    Основной клиент декодирует ответ как UTF-8 и на сжатых байтах падал бы.
    """
    global _redis_bytes
    if _redis_bytes is None:
        _redis_bytes = redis.from_url(REDIS_URL, decode_responses=False,
                                      socket_connect_timeout=2, socket_timeout=2)
    return _redis_bytes


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


# ─────────────────────────────────────────────────────────────────────────────
# GZIP-кеш: значение лежит УЖЕ СЖАТЫМ и уходит клиенту как есть.
#
# Зачем (замер на проде 2026-08-10, график 5м/6м):
#   было:  Redis GET 6.5 МБ (15 мс) → json.loads (71) → json.dumps (80) →
#          nginx жмёт 6.2 МБ в 797 КБ (~200 мс CPU)  ≈ 370 мс на КАЖДЫЙ хит
#   стало: Redis GET 867 КБ (~3 мс) → отдали как есть  ≈ 3 мс
# Сжимаем один раз при записи (119 мс), а не на каждом запросе. Бонусом память
# Redis падает в 7.5 раз (6.5 МБ → 867 КБ на ключ) — при maxmemory 256 МБ это
# разница между «LRU выбивает горячие ключи» и «в кеш влезает прогрев».
# ─────────────────────────────────────────────────────────────────────────────

def get_gzip(key: str) -> bytes | None:
    """Сжатое значение из кеша — готово к отдаче с Content-Encoding: gzip.

    Значение без gzip-сигнатуры (0x1f 0x8b) считаем промахом: под тем же
    префиксом могли остаться записи в прежнем формате (сырой JSON) — отдать их
    как gzip значило бы прислать клиенту нечитаемый ответ. Такой ключ просто
    пересчитается и ляжет уже сжатым.
    """
    try:
        val = _get_redis_bytes().get(key)
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on get_gzip: {e}")
        return None
    if val is None or not val.startswith(b"\x1f\x8b"):
        return None
    return val


def set_gzip(key: str, payload: str, ttl: int = DEFAULT_TTL) -> bytes:
    """Сжать JSON-строку, положить в кеш и вернуть сжатые байты (для отдачи)."""
    gz = gzip.compress(payload.encode("utf-8"), GZIP_LEVEL)
    try:
        _get_redis_bytes().setex(key, ttl, gz)
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on set_gzip: {e}")
    return gz


def get_gzip_json(key: str) -> Any | None:
    """Значение gzip-кеша как объект — для cache_updater, которому надо править."""
    gz = get_gzip(key)
    if gz is None:
        return None
    try:
        return json.loads(gzip.decompress(gz))
    except (OSError, ValueError) as e:
        logger.warning(f"Broken gzip cache entry {key[:60]}: {e}")
        return None


def get_all_gzip_by_prefix(prefix: str) -> dict[str, Any]:
    """Все записи gzip-кеша с префиксом, распакованные. {key: data}

    Читаем ПООЧЕРЁДНО, а не mget-пачкой: chart-ключи весят до 6.5 МБ каждый в
    распакованном виде, и одновременный разбор всех разом раздувал бы RSS
    воркера на сотни мегабайт при 8 ГБ на всей машине.
    """
    result = {}
    try:
        r = _get_redis_bytes()
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"{prefix}*".encode(), count=200)
            for k in keys:
                # try ВНУТРИ цикла: один нечитаемый ключ не должен рвать обход.
                # ⚠️ Так и случилось 2026-08-10: служебный счётчик спроса лежал
                # под chart:demand, GET на ZSET дал WRONGTYPE (это RedisError,
                # не ValueError), и внешний except обрывал скан — cache_updater
                # видел 54 ключа из 257, остальные не обновлялись и тихо
                # истекали по TTL. Счётчики с тех пор живут под chartmeta:,
                # но защита остаётся: маска префикса — не гарантия типа.
                try:
                    gz = r.get(k)
                    if gz is None:
                        continue
                    result[k.decode()] = json.loads(gzip.decompress(gz))
                except (redis.RedisError, OSError, ValueError, UnicodeDecodeError):
                    continue      # чужой тип/битый формат — пропускаем молча
            if cursor == 0:
                break
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on get_all_gzip_by_prefix: {e}")
    return result


def get_or_compute_gzip(key: str, compute_fn, ttl: int = DEFAULT_TTL, *,
                        lock_ttl: int = 30, wait_timeout: float = 10.0,
                        poll_interval: float = 0.1) -> bytes:
    """Single-flight поверх gzip-кеша. Возвращает СЖАТЫЕ байты ответа.

    Та же защита от cache-stampede, что в get_or_compute: при истечении ключа
    считает один воркер, остальные ждут его результат. compute_fn возвращает
    объект; сериализация и сжатие — здесь.
    """
    gz = get_gzip(key)
    if gz is not None:
        return gz

    lock_key = f"sflight:{key}"
    have_lock = False
    try:
        have_lock = bool(_get_redis().set(lock_key, "1", nx=True, ex=lock_ttl))
    except (redis.RedisError, ConnectionError):
        have_lock = True  # Redis лёг → считаем сами (fail-open, без дедупа)

    if have_lock:
        try:
            return set_gzip(key, json.dumps(compute_fn(), default=str), ttl)
        finally:
            try:
                _get_redis().delete(lock_key)
            except (redis.RedisError, ConnectionError):
                pass

    deadline = time.monotonic() + wait_timeout
    while time.monotonic() < deadline:
        time.sleep(poll_interval)
        gz = get_gzip(key)
        if gz is not None:
            return gz

    # Не дождались (медленный/упавший вычислитель) → считаем сами, fail-open.
    return set_gzip(key, json.dumps(compute_fn(), default=str), ttl)


DEMAND_KEY = "chartmeta:demand"
# Счётчик активов, которым фоновый прогрев должен пересчитать график ПРИНУДИТЕЛЬНО
# (даже если ключ тёплый). Ставится на daily-NOTIFY вместо стирания графиков.
CHART_REFRESH_KEY = "chartmeta:warm:force_left"


CHART_REFRESH_PENDING = "chartmeta:warm:refresh_pending"
_CHART_REFRESH_TTL = 7200


def mark_chart_refresh(ttl: int = _CHART_REFRESH_TTL) -> None:
    """Пометить графики на постепенный принудительный пересчёт.

    Вместо стирания ~100 МБ прогретого кеша в 19:10 (посреди вечерней сессии)
    прогрев обойдёт активы по кругу и перезапишет ключи. Пользователь до момента
    замены получает прежний ответ, а не ждёт холодный пересчёт.

    Ставим только флаг: сколько активов в круге, знает прогрев, а не отправитель
    NOTIFY. Иначе пришлось бы зашивать число здесь — и при 91 активе счётчик на
    «200 с запасом» гонял бы лишние два круга по 12 минут CPU.
    """
    try:
        _get_redis().setex(CHART_REFRESH_PENDING, ttl, 1)
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on mark_chart_refresh: {e}")


def start_chart_refresh(total: int) -> bool:
    """Забрать флаг и открыть ровно `total` слотов пересчёта. True — режим начат.

    Зовёт прогрев, когда знает размер круга. DELETE возвращает 1 только тому, кто
    реально снял флаг, поэтому режим стартует один раз, даже если сюда зайдут
    несколько воркеров.
    """
    try:
        r = _get_redis()
        if not r.delete(CHART_REFRESH_PENDING):
            return False
        r.setex(CHART_REFRESH_KEY, _CHART_REFRESH_TTL, total)
        return True
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on start_chart_refresh: {e}")
        return False


def take_chart_refresh_slot() -> bool:
    """Занять один слот принудительного пересчёта. True — этот актив пересчитываем.

    Уменьшает счётчик на единицу; когда он исчерпан — режим выключается сам.
    """
    try:
        r = _get_redis()
        # EXISTS перед DECR: на несуществующем ключе DECR создал бы его со
        # значением -1 и БЕЗ TTL — режим завис бы навсегда. Гонки тут нет:
        # прогрев ходит под локом warm:chart, работает один воркер.
        if not r.exists(CHART_REFRESH_KEY):
            return False
        left = r.decr(CHART_REFRESH_KEY)
        if left is None or left <= 0:
            r.delete(CHART_REFRESH_KEY)
        return True
    except (redis.RedisError, ConnectionError):
        return False


def record_demand(member: str) -> None:
    """Отметить обращение к активу — счётчик спроса для приоритета прогрева.

    Прогреть все 91 видимый актив разом нельзя (≈12 минут CPU), поэтому греем
    по кругу, начиная с тех, что реально смотрят. Список «популярного» ведёт
    сам трафик, а не догадка в константе: замер 2026-08-10 показал 9 активов
    за день при 91 доступном.

    Fire-and-forget: ошибка Redis не должна ронять запрос за графиком.
    """
    try:
        _get_redis().zincrby(DEMAND_KEY, 1, member)
    except (redis.RedisError, ConnectionError):
        pass


def top_demand(limit: int = 200) -> list[str]:
    """Активы по убыванию спроса."""
    try:
        return [m for m in _get_redis().zrevrange(DEMAND_KEY, 0, limit - 1)]
    except (redis.RedisError, ConnectionError):
        return []


def try_lock(name: str, ttl: int) -> bool:
    """Взять распределённый лок на ttl секунд. True — лок наш.

    Нужен фоновым задачам: startup-хук выполняется в КАЖДОМ gunicorn-воркере
    (их три), и без лока прогрев кеша шёл бы втройне — три одинаковых тяжёлых
    пересчёта одновременно на 4 ядрах. Лок сам истекает по ttl, поэтому упавший
    воркер не блокирует прогрев навсегда.
    """
    try:
        return bool(_get_redis().set(f"lock:{name}", "1", nx=True, ex=ttl))
    except (redis.RedisError, ConnectionError):
        return False      # Redis недоступен → прогрев пропускаем, это не критично


def get_raw(key: str) -> str | None:
    """Сырая JSON-строка из кеша, без разбора в объекты.

    Для больших ответов (график 5м/6м — 6.5 МБ) обычный путь тратит ~150 мс CPU
    впустую: get_or_set делает json.loads, а FastAPI тут же сериализует dict
    обратно в те же байты (замер на проде 2026-08-10: loads 71 мс + dumps 80 мс
    при Redis GET 25 мс). Роутер отдаёт эту строку как Response(media_type=
    'application/json') — контент байт-в-байт тот же.
    """
    try:
        return _get_redis().get(key)
    except (redis.RedisError, ConnectionError) as e:
        logger.warning(f"Redis error on get_raw: {e}")
        return None


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
