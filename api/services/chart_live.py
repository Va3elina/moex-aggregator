"""
«Живая» (текущая) точка для графика OI.

Проблема: на дневном таймфрейме последняя точка графика — это последняя
ЗАКРЫТАЯ свеча (вчера и старше), потому что дневные данные пишутся раз в сутки
(свечи в 00:01 МСК, дневной OI в 00:10 МСК за прошлый день). На часовом ТФ
сегодняшние бары уже есть, на дневном — нет. Из-за этого днём дневной график
показывает «застывшее» значение, а часовой — почти текущее.

Решение: к концу рядов candles/open_interest дописываем синтетическую точку с
ТЕКУЩИМ значением, взятым из самых свежих 5-минутных данных. Точка помечается
флагом ``live=True``:
- ``chart.py`` строит ряды закрытых свечей и вызывает append_live_points перед
  кешированием;
- ``cache_updater.py`` при каждом NOTIFY срезает старую live-точку, дописывает
  новые закрытые свечи и снова вызывает append_live_points — так live-точка
  всегда свежая и не дублируется, когда официальная дневная свеча наконец
  приходит.

Применяется только к дневному (24) и часовому (60) ТФ. На 5-минутном последняя
закрытая свеча и так самая свежая из имеющихся данных — там live-точка не нужна.
"""
from datetime import datetime, time as dt_time

from sqlalchemy import text

from api.logger import get_logger

log = get_logger()

# ТФ, для которых имеет смысл дописывать live-точку (на 5м нет более свежего
# источника, чем сама 5-минутка).
_LIVE_INTERVALS = (24, 60)


def strip_live_points(response: dict) -> bool:
    """Удаляет хвостовые live-точки из candles/open_interest in-place.

    Возвращает True, если что-то было удалено (для логирования)."""
    removed = False
    for field in ("candles", "open_interest"):
        arr = response.get(field)
        if not arr:
            continue
        while arr and isinstance(arr[-1], dict) and arr[-1].get("live"):
            arr.pop()
            removed = True
    return removed


def _is_newer(candidate: datetime, last: datetime, daily: bool) -> bool:
    """Свежее ли candidate, чем last. На дневке сравниваем по дате, на интрадей —
    по полному timestamp."""
    if daily:
        return candidate.date() > last.date()
    return candidate > last


def append_live_points(db, response: dict) -> bool:
    """Дописывает live-точку (текущее значение) в конец candles/open_interest.

    Идемпотентно: сначала срезает любые хвостовые live-точки, затем добавляет
    свежие. Источник — самая свежая 5-минутная свеча активного контракта и
    самая свежая 5-минутная запись OI.

    Возвращает True, если хоть одна live-точка была добавлена.

    Best-effort: любые ошибки гасятся, основной ответ не должен страдать.
    """
    try:
        interval = response.get("interval")
        if interval not in _LIVE_INTERVALS:
            return False

        # Идемпотентность: убираем прошлые live-точки перед пересчётом.
        strip_live_points(response)

        sec_ids = response.get("contracts") or []
        candles = response.get("candles")
        if not sec_ids or not candles:
            return False

        daily = interval == 24
        added = False

        # 1. Самая свежая 5-минутная свеча активного контракта.
        #    volume > 0 отсекает zero-fill артефакты агрегации; при равном
        #    времени берём контракт с большим объёмом (активный при ролловере).
        #    LATERAL по каждому контракту вместо sec_id = ANY(...): глобальный
        #    ORDER BY + LIMIT 1 с ANY заставляет планировщик вычитать ВСЕ
        #    5-минутки актива с диска и сортировать (2-18 сек на холодных
        #    страницах). Спуск по индексу с конца на каждый контракт — мс.
        #    volume DESC внутри LATERAL: на один begin_time может быть две
        #    датированные серии одного перпетуала (TBH5/TBH6) — берём активную.
        row = db.execute(text("""
            SELECT c.begin_time, c.close, c.volume
            FROM unnest(CAST(:sec_ids AS text[])) AS s(sid)
            CROSS JOIN LATERAL (
                SELECT begin_time, close, volume
                FROM candles
                WHERE sec_id = s.sid AND interval = 5 AND close > 0 AND volume > 0
                ORDER BY begin_time DESC, volume DESC
                LIMIT 1
            ) c
            ORDER BY c.begin_time DESC, c.volume DESC
            LIMIT 1
        """), {"sec_ids": sec_ids}).fetchone()

        if not row or not row[0]:
            return False

        live_dt = row[0]
        live_close = float(row[1] or 0)
        live_vol = float(row[2] or 0)
        if live_close <= 0:
            return False

        # Штамп времени live-точки: на дневке — сегодняшняя дата 00:00 (как у
        # дневных свечей, выравнивание OI по свечам идёт по дате); на интрадей —
        # точное время свежей 5-минутки (выравнивание идёт по полному timestamp,
        # поэтому live-свеча и live-OI должны нести ОДИН И ТОТ ЖЕ штамп).
        live_time = datetime.combine(live_dt.date(), dt_time.min) if daily else live_dt
        live_time_iso = live_time.isoformat()

        last_candle_dt = datetime.fromisoformat(candles[-1]["time"])
        if _is_newer(live_time, last_candle_dt, daily):
            candles.append({
                "time": live_time_iso,
                "open": live_close,
                "high": live_close,
                "low": live_close,
                "close": live_close,
                "volume": live_vol,
                "live": True,
            })
            response["candles_count"] = len(candles)
            response["candles_end_date"] = live_time.date().isoformat()
            response["data_end"] = live_time.date().isoformat()
            added = True

        # 2. Самая свежая 5-минутная запись OI (только если OI вообще показываем).
        oi = response.get("open_interest")
        if response.get("mode") != "price_only" and oi:
            sectype = response.get("sectype")
            clgroup = response.get("clgroup")
            orow = db.execute(text("""
                SELECT tradedate, tradetime, pos, pos_long, pos_short,
                       pos_long_num, pos_short_num
                FROM open_interest
                WHERE sectype = :sectype AND clgroup = :clgroup AND interval = 5
                ORDER BY tradedate DESC, tradetime DESC
                LIMIT 1
            """), {"sectype": sectype, "clgroup": clgroup}).fetchone()

            if orow and orow[0] is not None:
                oi_dt = datetime.combine(orow[0], orow[1] or dt_time.min)
                last_oi_dt = datetime.fromisoformat(oi[-1]["time"])
                if _is_newer(oi_dt, last_oi_dt, daily):
                    pos_long = int(orow[3] or 0)
                    pos_short = int(orow[4] or 0)
                    oi.append({
                        # Тот же штамп, что и у live-свечи — для выравнивания на интрадей.
                        "time": live_time_iso,
                        "pos": int(orow[2] or 0),
                        "pos_long": pos_long,
                        "pos_short": pos_short,
                        "pos_long_num": int(orow[5] or 0),
                        "pos_short_num": int(orow[6] or 0),
                        "net_position": pos_long + pos_short,
                        "live": True,
                    })
                    response["oi_count"] = len(oi)
                    response["oi_end_date"] = oi_dt.date().isoformat()
                    added = True

        return added

    except Exception as e:
        log.warning(f"append_live_points failed: {e}")
        return False
