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
    # oi_screener: — лента скринера. Префикс "oi:" её НЕ покрывает (там
    # двоеточие сразу после oi), и без явной инвалидации лента жила по своему
    # TTL=300с: SSE-рефетч фронта приходил на свежий NOTIFY, а бэкенд отдавал
    # прежний срез. oi_extremes: (TTL 30 мин, тяжёлый пересчёт) намеренно не
    # трогаем — исторические экстремумы за 5 минут не меняются.
    "5min": ["chart:", "candles:", "oi:", "oi_screener:"],
    "hourly": ["chart:", "oi:", "oi_screener:"],
    # daily прилетает в 19:10 МСК торгового дня — ПОСРЕДИ вечерней сессии, а не
    # ночью. Раньше здесь стоял None = flushdb, и раз в день сносился весь Redis
    # целиком. Это стирало не только данные:
    #   • api_usage:{user}:{дата} (TTL 32 дня) — счётчики квоты платного API:
    #     пользователь получал полную квоту заново, а история потребления в
    #     личном кабинете обнулялась;
    #   • oauth:state:… — CSRF-состояние входа: кто в этот момент был на странице
    #     провайдера, возвращался с «невалидным state»;
    #   • ratelimit:/auth_iprl:/apikey:rate: — окна защиты от перебора.
    # Плюс с 2026-08-10 в кеше лежит ~100 МБ прогретых графиков, и их полное
    # стирание в 19:10 роняло всех в холодный пересчёт (5м/6м — до 14 с).
    #
    # Поэтому перечисляем префиксы ДАННЫХ явно. Пропустить что-то не страшно:
    # у аналитических ключей TTL 30 минут, максимум полчаса на самовосстановление.
    # Стереть лишнее — страшно, поэтому счётчиков, состояний и локов здесь нет.
    #
    # chart: сознательно НЕ в списке: вместо стирания 100 МБ помечаем их на
    # постепенный пересчёт (_handle_notification ниже) — данные всё это время
    # остаются доступными, а свежесть точек и так поддерживает cache_updater.
    "daily": [
        "breadth:", "buffett:", "candles:", "cbr_flows:", "ex_dates_db:",
        "funds:", "funds_catalog:", "funds_chart:",
        "heatmap:", "heatmap_imoex:", "imoex_tickers_set:", "imoex_weights:",
        "index:", "instruments:", "macro:",
        "oi:", "oi_extremes:", "oi_low_activity:", "oi_screener:",
        "seasonality:", "seasonality_price:", "seasonality_yearly:", "seasonality_years:",
        "stats:", "ticker:", "usd_rates:",
    ],
    "mv_refresh": ["heatmap:", "stats:"],
    "funds": ["funds_chart:"],
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
            await conn.add_listener("anomaly", _on_anomaly)   # лента аномалий → SSE-нудж
            await conn.add_listener("alert_fire", _on_alert_fire)  # сработал алерт юзера → SSE-нудж

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
                    await conn.remove_listener("anomaly", _on_anomaly)
                    await conn.remove_listener("alert_fire", _on_alert_fire)
                    await conn.close()
                except Exception:
                    pass

        await asyncio.sleep(5)


def _on_notification(conn, pid, channel, payload):
    """Callback для PostgreSQL NOTIFY."""
    asyncio.ensure_future(_handle_notification(payload))


def _on_anomaly(conn, pid, channel, payload):
    """Callback канала 'anomaly' (продюсер шлёт NOTIFY на каждую новую аномалию)."""
    asyncio.ensure_future(_handle_anomaly(payload))


async def _handle_anomaly(payload: str):
    """Новая аномалия → SSE-нудж клиентам. Сам объект не несём — фронт по нудж'у
    тянет /api/anomalies/feed?since=<last_id> (REST, как везде). Кеш не трогаем."""
    from api.sse import sse_manager
    try:
        data = json.loads(payload) if payload else {}
    except json.JSONDecodeError:
        data = {}
    await sse_manager.broadcast(json.dumps({"source": "anomaly", "id": data.get("id")}))


def _on_alert_fire(conn, pid, channel, payload):
    """Callback канала 'alert_fire' (alerts_run шлёт при site-доставке алерта)."""
    asyncio.ensure_future(_handle_alert_fire(payload))


async def _handle_alert_fire(payload: str):
    """Сработал алерт юзера → SSE-нудж. Broadcast несёт ТОЛЬКО user_id (без контента,
    чтобы не течь в общий SSE); адресат сверяет свой id и тянет
    /api/alerts/recent-fires (user-scoped). Кеш не трогаем."""
    from api.sse import sse_manager
    try:
        data = json.loads(payload) if payload else {}
    except json.JSONDecodeError:
        data = {}
    await sse_manager.broadcast(json.dumps({"source": "alert_fire", "user_id": data.get("user_id")}))


async def _handle_notification(payload: str):
    """Обработка NOTIFY: инвалидация кеша + SSE broadcast."""
    from api.cache import invalidate
    from api.sse import sse_manager

    try:
        data = json.loads(payload)
        source = data.get("source", "unknown")
        logger.info(f"NOTIFY received: source={source}")

        # ⚠️ ТЕЛЕМЕТРИЯ ПРОЦЕССОВ КЭША НЕ КАСАЕТСЯ. Событие «процесс начался /
        # закончился» несёт только факт запуска, никаких новых данных за ним нет.
        # Пропустить его дальше нельзя: ниже неизвестный источник трактуется как
        # «перестраховка» и сбрасывает ВЕСЬ Redis, а процессы стартуют десятки раз
        # в час — кэш не выжил бы ни часа. Ранний выход: только broadcast.
        if source == "pipeline":
            await sse_manager.broadcast(payload)
            return

        # Обновление кеша
        if source in ("5min", "hourly"):
            # Инкрементально дописываем новые данные в существующий кеш
            # (chart: обновляем, остальные префиксы инвалидируем)
            for prefix in (SOURCE_CACHE_MAP.get(source) or []):
                if prefix == "chart:":
                    # Дописываем в кеш вместо удаления
                    from api.cache_updater import async_update_chart_caches
                    asyncio.ensure_future(async_update_chart_caches(source))
                else:
                    invalidate(prefix)
        else:
            prefixes = SOURCE_CACHE_MAP.get(source)
            if prefixes is None:
                invalidate()      # неизвестный источник → перестраховка
            else:
                for prefix in prefixes:
                    invalidate(prefix)

            if source == "daily":
                # Графики не стираем, а помечаем на постепенный пересчёт: их ~100 МБ
                # и до 14 с холодного счёта на ключ, а нужны они ровно в 19:10, когда
                # идёт вечерняя сессия. Фоновый прогрев (api/main._warm_chart_cache)
                # перезапишет их по кругу за ~25 минут, отдавая пользователю прежний
                # ответ до момента замены. Свежесть самих точек всё это время держит
                # cache_updater (он дописывает бары по 5min/hourly), пересчёт нужен
                # ради метаданных: ролловер контракта, available_intervals, склейка.
                from api.cache import mark_chart_refresh
                mark_chart_refresh()

        # SSE broadcast
        await sse_manager.broadcast(payload)

    except json.JSONDecodeError:
        logger.error(f"NOTIFY: invalid JSON payload: {payload[:200]}")
    except Exception as e:
        logger.error(f"NOTIFY handler error: {e}")
