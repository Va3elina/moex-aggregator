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
from api.services.contract_calendar import front_sec_id
from api.services.market_delay import cutoff_for_interval

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


def strip_stretched_points(response: dict) -> bool:
    """Снимает хвост «растянутой» цены из candles in-place.

    Растянутые точки — не данные, а заполнение отрезка, где ОИ уже есть, а
    свеча по лицензии ещё не раздаётся. Их обязательно срезать ПЕРЕД тем, как
    дописывать настоящие свечи: иначе на одном и том же времени осталась бы
    сначала растянутая точка, а следом фактическая.
    """
    removed = False
    arr = response.get("candles")
    if not arr:
        return False
    while arr and isinstance(arr[-1], dict) and arr[-1].get("stretched"):
        arr.pop()
        removed = True
    if removed:
        response["candles_count"] = len(arr)
    return removed


def append_stretched_price(response: dict) -> bool:
    """Продлевает последнюю известную цену до конца ряда ОИ.

    Зачем. По лицензии MOEX цена раздаётся с задержкой 15 минут, а ОИ приходит
    актуальным (решение владельца: ОИ не придерживаем, чтобы его не терять).
    В результате правее последней свечи висят точки ОИ, под которыми нет цены —
    выглядит как обрыв графика. Достраиваем цену горизонтально: значение то же,
    что у последней реальной свечи, флаг stretched=True.

    Точки помечены, поэтому:
      • при следующем обновлении они срезаются (strip_stretched_points) и на их
        месте оказывается настоящая свеча, когда та доедет;
      • фронт может отрисовать участок иначе, не выдавая заполнение за факт.

    Возвращает True, если что-то дописали.
    """
    try:
        candles = response.get("candles")
        oi = response.get("open_interest")
        if not candles or not oi:
            return False

        last_candle = candles[-1]
        if last_candle.get("live") or last_candle.get("stretched"):
            return False        # растягиваем только от РЕАЛЬНОЙ свечи

        last_ct = datetime.fromisoformat(last_candle["time"])
        close = float(last_candle.get("close") or 0)
        if close <= 0:
            return False

        # Времена ОИ правее последней свечи — ровно тот отрезок, где цены нет.
        gap = [p["time"] for p in oi
               if not p.get("live") and datetime.fromisoformat(p["time"]) > last_ct]
        if not gap:
            return False

        for t in gap:
            candles.append({
                "time": t,
                "open": close, "high": close, "low": close, "close": close,
                "volume": 0,
                "stretched": True,
            })

        response["candles_count"] = len(candles)
        return True

    except Exception as e:
        log.warning(f"append_stretched_price failed: {e}")
        return False


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
        sectype = response.get("sectype")

        # Календарный фронт сегодня: live-точка должна сидеть на том же контракте,
        # что и непрерывная серия (chart.py / get_candles_continuous) — без
        # преждевременного ролла. Если фронт известен и есть в contracts — берём
        # 5м только из него; иначе (или если у фронта нет свежей 5м) — из всех
        # контрактов (объёмный fallback, как раньше).
        front = front_sec_id(db, sectype) if sectype else None
        live_sec_ids = [front] if (front and front in sec_ids) else sec_ids

        # 1. Самая свежая 5-минутная свеча активного контракта.
        #    volume > 0 отсекает zero-fill артефакты агрегации; при равном
        #    времени берём контракт с большим объёмом (активный при ролловере).
        #    LATERAL по каждому контракту вместо sec_id = ANY(...): глобальный
        #    ORDER BY + LIMIT 1 с ANY заставляет планировщик вычитать ВСЕ
        #    5-минутки актива с диска и сортировать (2-18 сек на холодных
        #    страницах). Спуск по индексу с конца на каждый контракт — мс.
        #    volume DESC внутри LATERAL: на один begin_time может быть две
        #    датированные серии одного перпетуала (TBH5/TBH6) — берём активную.
        #    ⚠️ begin_time <= :cutoff — лицензия MOEX (задержка 5-минуток).
        #    Live-точка ЧЕРПАЕТ ИЗ 5-МИНУТОК, даже когда показывается на
        #    дневном графике, — то есть попадает под задержку по источнику
        #    данных, а не по таймфрейму отображения. Без потолка она тянула бы
        #    самую свежую 5-минутку мимо основного запроса свечей.
        _LIVE_5M_SQL = text("""
            SELECT c.begin_time, c.close, c.volume
            FROM unnest(CAST(:sec_ids AS text[])) AS s(sid)
            CROSS JOIN LATERAL (
                SELECT begin_time, close, volume
                FROM candles
                WHERE sec_id = s.sid AND interval = 5 AND close > 0 AND volume > 0
                  AND begin_time <= :cutoff
                ORDER BY begin_time DESC, volume DESC
                LIMIT 1
            ) c
            ORDER BY c.begin_time DESC, c.volume DESC
            LIMIT 1
        """)
        row = db.execute(_LIVE_5M_SQL, {"sec_ids": live_sec_ids, "cutoff": cutoff_for_interval(5)}).fetchone()
        if (not row or not row[0]) and live_sec_ids is not sec_ids:
            row = db.execute(_LIVE_5M_SQL, {"sec_ids": sec_ids, "cutoff": cutoff_for_interval(5)}).fetchone()

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
