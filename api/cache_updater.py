"""
Инкрементальное обновление кеша графиков при NOTIFY.

Вместо полной инвалидации (→ cold query 7 сек) — дописываем
последние свечи/OI в закешированные ответы.

Вызывается из notify_listener при source="5min".
"""
import asyncio
import logging
import time

from sqlalchemy import text

from api.cache import get_all_by_prefix, set_cache, touch, DEFAULT_TTL
from api.database import SessionLocal
from api.services.chart_live import append_live_points, strip_live_points
from datetime import date, datetime, time as dt_time, timedelta

log = logging.getLogger(__name__)


def _parse_cache_key(key: str) -> dict | None:
    """Парсит ключ chart:SR:SR:futures:24:FIZ:True:all:None:None → dict."""
    parts = key.split(":")
    if len(parts) < 10 or parts[0] != "chart":
        return None
    return {
        "sec_id": parts[1],
        "sectype": parts[2],
        "inst_type": parts[3],
        "interval": int(parts[4]),
        "clgroup": parts[5],
        "show_oi": parts[6] == "True",
        "period": parts[7],
        "date_from": parts[8],
        "date_to": parts[9],
    }


def _update_single_entry(db, key: str, cached_response) -> bool:
    """Обновляет одну запись кеша. Возвращает True если обновлено."""
    params = _parse_cache_key(key)
    if not params:
        return False

    sectype = params["sectype"]
    interval = params["interval"]
    clgroup = params["clgroup"]
    show_oi = params["show_oi"]

    # ── Потолок свежести записи ──────────────────────────────────────────────
    # date_from задан → ручной диапазон (шаринг-ссылка): контент зафиксирован с
    # обоих концов, дописывать нечего. Пусть уходит по TTL.
    if params.get("date_from") != "None":
        return False

    # date_to задан → потолок свежести (Free-задержка 24ч: роутер подменяет
    # date_to на today−1). Такие записи МОЖНО дописывать закрытыми барами, но
    # только до cap, и НИКОГДА — live-точкой: иначе реалтайм утечёт мимо
    # tier-гейта (баг 2026-08-10, PR #1068 — тогда гейт стоял только у
    # live-точки, а закрытые бары доливались всем подряд).
    cap: date | None = None
    date_to_s = params.get("date_to")
    if date_to_s and date_to_s != "None":
        try:
            cap = date.fromisoformat(date_to_s)
        except ValueError:
            return False
        # Потолок старше вчерашнего — запись уже никем не запрашивается
        # (сегодняшний Free-ключ несёт cap = вчера). Не греем, пусть уходит.
        if cap < date.today() - timedelta(days=1):
            return False

    try:
        # Срезаем прошлую live-точку: last_candle_time нужно считать по последней
        # ЗАКРЫТОЙ свече, а свежую live-точку пересоберём в конце (append_live_points).
        had_live = strip_live_points(cached_response)

        if not cached_response["candles"]:
            return False

        # Запрашиваем все sec_id для этого sectype
        sec_ids = [r[0] for r in db.execute(text(
            "SELECT sec_id FROM instruments WHERE sectype = :sectype"
        ), {"sectype": sectype}).fetchall()]

        appended_candles = 0
        appended_oi = 0

        if sec_ids:
            last_candle_time = datetime.fromisoformat(cached_response["candles"][-1]["time"])

            # Берём новые свечи после последней закрытой в кеше
            new_candles_raw = db.execute(text("""
                SELECT begin_time, open, high, low, close, volume, sec_id
                FROM candles
                WHERE sec_id = ANY(:sec_ids) AND interval = :interval
                  AND begin_time > :last_time
                ORDER BY begin_time
            """), {
                "sec_ids": sec_ids,
                "interval": interval,
                "last_time": last_candle_time,
            }).fetchall()

            # Потолок свежести (Free-задержка): бары СТРОГО после cap не наши.
            if cap is not None:
                new_candles_raw = [c for c in new_candles_raw if c[0].date() <= cap]

            if new_candles_raw:
                # Фильтруем volume=0 для интрадей (как в chart.py)
                if interval != 24:
                    new_candles_raw = [c for c in new_candles_raw if float(c[5] or 0) > 0]

                # Определяем лучший контракт за сегодня (как в chart.py)
                from collections import defaultdict
                daily_volume = defaultdict(lambda: defaultdict(float))
                for c in new_candles_raw:
                    day = c[0].date()
                    sid = c[6]
                    daily_volume[day][sid] += float(c[5] or 0)

                best_by_day = {}
                for day, contracts in daily_volume.items():
                    if contracts:
                        best_by_day[day] = max(contracts, key=contracts.get)

                # Фильтруем по лучшему контракту
                filtered = []
                for c in new_candles_raw:
                    day = c[0].date()
                    sid = c[6]
                    best = best_by_day.get(day, sid)
                    if sid == best:
                        filtered.append(c)

                # Дописываем свечи (dict, как в chart.py)
                for c in filtered:
                    cached_response["candles"].append({
                        "time": c[0].isoformat(),
                        "open": float(c[1] or 0),
                        "high": float(c[2] or 0),
                        "low": float(c[3] or 0),
                        "close": float(c[4] or 0),
                        "volume": float(c[5] or 0),
                    })
                    appended_candles += 1

                # Дописываем OI
                if show_oi and cached_response["open_interest"]:
                    last_oi_time_str = cached_response["open_interest"][-1]["time"]
                    last_oi_dt = datetime.fromisoformat(last_oi_time_str)

                    new_oi_raw = db.execute(text("""
                        SELECT tradedate, tradetime, pos, pos_long, pos_short,
                               pos_long_num, pos_short_num
                        FROM open_interest
                        WHERE sectype = :sectype AND clgroup = :clgroup AND interval = :interval
                          AND (tradedate > :last_date
                               OR (tradedate = :last_date AND tradetime > :last_time_part))
                        ORDER BY tradedate, tradetime
                    """), {
                        "sectype": sectype,
                        "clgroup": clgroup,
                        "interval": interval,
                        "last_date": last_oi_dt.date(),
                        "last_time_part": last_oi_dt.time(),
                    }).fetchall()

                    for oi in new_oi_raw:
                        trade_date = oi[0]
                        if cap is not None and trade_date > cap:
                            continue      # тот же потолок, что у свечей
                        trade_time = oi[1] if oi[1] else dt_time(23, 50)

                        if isinstance(trade_time, str):
                            parts = trade_time.split(":")
                            trade_time = dt_time(int(parts[0]), int(parts[1]),
                                                 int(parts[2]) if len(parts) > 2 else 0)

                        pos_long = int(oi[3] or 0)
                        pos_short = int(oi[4] or 0)

                        cached_response["open_interest"].append({
                            "time": datetime.combine(trade_date, trade_time).isoformat(),
                            "pos": int(oi[2] or 0),
                            "pos_long": pos_long,
                            "pos_short": pos_short,
                            "pos_long_num": int(oi[5] or 0),
                            "pos_short_num": int(oi[6] or 0),
                            "net_position": pos_long + pos_short,
                        })
                        appended_oi += 1

        # Обновляем метаданные по закрытым свечам
        cached_response["candles_count"] = len(cached_response["candles"])
        cached_response["oi_count"] = len(cached_response["open_interest"])
        if cached_response["candles"]:
            last_time = cached_response["candles"][-1]["time"][:10]
            cached_response["candles_end_date"] = last_time
            cached_response["data_end"] = last_time

        # Пересобираем свежую live-точку (текущее значение). Делаем это КАЖДЫЙ цикл,
        # даже когда новых закрытых свечей не было — live-точку нужно обновлять
        # каждые 5 минут. ⚠️ Только для записей БЕЗ потолка: у Free-записи (cap)
        # live-точка = сегодняшний реалтайм мимо tier-гейта.
        live_added = append_live_points(db, cached_response) if cap is None else False

        # Перезаписываем кеш только если что-то изменилось (новые свечи/OI, добавлена
        # или убрана live-точка) — иначе лишние записи в Redis на каждый NOTIFY.
        changed = bool(appended_candles or appended_oi or live_added or had_live)
        if changed:
            set_cache(key, cached_response, ttl=DEFAULT_TTL)
        else:
            # Контент не изменился — но TTL продлить НАДО. Иначе запись протухает
            # каждые 30 минут и следующий гость платит за холодный пересчёт (замер
            # 2026-08-10: 5м/6м — 6.1 с, 1ч/1г — 10.8 с). У Free-записей дописывать
            # обычно нечего (потолок = вчера), поэтому без touch они не грелись бы
            # вовсе — ровно та регрессия, что вылезла после #1068.
            touch(key, ttl=DEFAULT_TTL)

        if appended_candles > 0 or appended_oi > 0:
            log.info(f"Cache UPDATE: {key[:50]}... +{appended_candles} candles +{appended_oi} OI")

        return changed

    except Exception as e:
        log.error(f"Cache update failed for {key[:50]}...: {e}")
        return False


def update_chart_caches(source: str = "5min"):
    """
    Инкрементально обновляет все закешированные chart-ответы.
    Sync-функция — вызывается из async обёртки через run_in_executor.
    """
    t0 = time.time()
    entries = get_all_by_prefix("chart:")

    if not entries:
        return

    log.info(f"Cache updater: {len(entries)} chart entries to update (source={source})")

    db = SessionLocal()
    updated = 0
    try:
        for key, cached_response in entries.items():
            if _update_single_entry(db, key, cached_response):
                updated += 1
    finally:
        db.close()

    elapsed = (time.time() - t0) * 1000
    log.info(f"Cache updater done: {updated}/{len(entries)} updated in {elapsed:.0f}ms")


async def async_update_chart_caches(source: str = "5min"):
    """Async обёртка для вызова из notify_listener."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, update_chart_caches, source)
