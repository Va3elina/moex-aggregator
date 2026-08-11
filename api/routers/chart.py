"""
API endpoint для получения свечей и данных OI
С валидацией входных данных и защитой от SQL injection

ИСПРАВЛЕНО: добавлено поле net_position = pos_long + pos_short
(pos_short в БД уже отрицательный, поэтому используем ПЛЮС)
"""

from fastapi import APIRouter, Depends, Query, HTTPException, Request, Response
from sqlalchemy.orm import Session
from sqlalchemy import text
import gzip
from datetime import datetime, date, time as dt_time, timedelta
from api.cache import get_or_set, get_or_compute, get_or_compute_gzip, record_demand, DEFAULT_TTL
from typing import Optional
from pydantic import BaseModel
import time

from api.logger import get_logger

log = get_logger()

from api.database import get_db
from api.models import Instrument
# CandleResponse/OpenInterestResponse не используются — строим dict напрямую для скорости
from api.schemas.validators import (
    ChartParams,
    IntervalsParams,
    ClgroupType,
    PeriodType,
    InstTypeType,
    validate_safe_id
)
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_tier_limits, get_effective_end_date
from api.services.chart_live import append_live_points, append_stretched_price
from api.services.market_delay import price_cutoff, cutoff_for_interval
from api.services.contract_calendar import front_windows, resolve_day, front_sec_ids, stable_runs

router = APIRouter(prefix='/api/chart', tags=['chart'])


# ChartResponse больше не Pydantic — endpoint возвращает dict напрямую для скорости
# (Pydantic сериализация 13000 объектов занимала ~1 сек)


class AvailableIntervalsResponse(BaseModel):
    sectype: str
    intervals: list[dict]


PERIODS = {
    "1d": 1, "1w": 7, "1m": 30, "3m": 90,
    "6m": 180, "1y": 365, "2y": 730, "5y": 1825, "all": 10000
}


# ─────────────────────────────────────────────────────────────────────────────
# Реестр кросс-контрактных корпоративных событий (сплит / консолидация).
#
# Within-contract back-adjustment (шаг 7.5) сглаживает сплит только ВНУТРИ одного
# контракта (скачок >2× между соседними свечами одного secid). Но если MOEX
# пере-специфицирует фьючерс на СТЫКЕ контрактов + разрыв листинга (старый
# контракт истёк в старом масштабе, новый залистился в новом) — детектор её НЕ
# видит, и непрерывный график получает вертикальный обрыв.
#
# Это курируемый список таких событий. Auto-детект кросс-контрактных скачков
# намеренно НЕ включаем: на неликвидных роллах он даёт ложные срабатывания (см.
# комментарий к шагу 7.5). ratio = first_new_close / last_old_close на стыке —
# домножает свечи СТАРЕЕ boundary_date, делая линию непрерывной. OI (в
# контрактах) НЕ трогаем: рост числа контрактов после сплита — реальный.
# Найдено сканом фронта на 2.5×-обрывы; span within-contract у этих серий
# 1.1–1.9 (<2.0) → с детектором 7.5 не пересекается (задвоения нет).
# Ключ — sectype (событие общее для всей серии контрактов), не sec_id.
KNOWN_FUT_CORP_ACTIONS: dict[str, list[tuple[date, float]]] = {
    # sectype: [(boundary_date, ratio), ...]
    # ⚠️ События подтверждены НОВОСТЯМИ/раскрытием эмитентов, НЕ нашим рядом акций:
    # он ретро-адъюстнут под пост-сплит (GMKN показан ~150, VTBR ~100) и сам обрыв
    # ПРЯЧЕТ — сверка по акции в БД ложно «не подтверждает». Ratio ниже —
    # ЭМПИРИЧЕСКИЙ (first_new/last_old на стыке), для непрерывности графика; он НЕ
    # равен коэффициенту сплита акции, т.к. MOEX на сплите меняет и множитель/лот
    # контракта → «net»-скачок фьючерса иной.
    # ⚠️ boundary_date — день ПЕРВОЙ свечи в новом масштабе в цепочке ФРОНТА (не
    # дата листинга дальнего контракта): всё СТАРЕЕ этой даты домножается. Взять
    # более позднюю дату = задавить множителем уже пост-сплитные свечи (так было
    # с TN: боундари стоял на 17.06.2024 → фронт TNM4 с 21.02 по 14.06.2024
    # рисовался ~18 ₽ вместо ~1700 ₽, и на экспирации июньского контракта график
    # прыгал обратно в 100×).
    'TN': [(date(2024, 2, 21), 0.010833)],  # Транснефть: сплит акций 1:100, торги с 21.02.2024. Фронт TNH4 157849 (02.02) → TNM4 1710 (21.02), фьюч ~92×. Interfax 930673
    'GK': [(date(2024, 4, 9),  0.1219)],  # Норникель: сплит акций 1:100, конвертация 04.04.2024. GKH4 ~15k → GKZ4 ~1.8k (фьюч ~8×, лот тоже менялся). Interfax 951692
    'VB': [(date(2024, 7, 16), 4.5130)],  # ВТБ: обратный сплит (консолидация) 5000:1, торги с 15.07.2024. VBH4 ~2.2k → VBZ4 ~10.1k (фьюч ~4.5×). Interfax 965973
}


@router.get("/intervals/{sectype}", response_model=AvailableIntervalsResponse)
def get_available_intervals(
    sectype: str,
    clgroup: ClgroupType = Query("FIZ"),
    db: Session = Depends(get_db)
):
    """Получить доступные интервалы OI для инструмента"""

    try:
        sectype = validate_safe_id(sectype, "sectype")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    query = text("""
        SELECT 
            interval,
            COUNT(*) as cnt,
            MIN(tradedate) as start_date,
            MAX(tradedate) as end_date
        FROM open_interest 
        WHERE sectype = :sectype 
          AND clgroup = :clgroup
        GROUP BY interval
        ORDER BY interval
    """)

    result = db.execute(query, {"sectype": sectype, "clgroup": clgroup}).fetchall()

    intervals = []
    for row in result:
        intervals.append({
            "interval": row.interval,
            "count": row.cnt,
            "start": str(row.start_date) if row.start_date else None,
            "end": str(row.end_date) if row.end_date else None,
        })

    return AvailableIntervalsResponse(
        sectype=sectype,
        intervals=intervals
    )


@router.get("/{sec_id}")
def get_chart_data(
        sec_id: str,
        request: Request,
        sectype: str = Query(...),
        inst_type: InstTypeType = Query("futures"),
        interval: int = Query(24),
        clgroup: ClgroupType = Query("FIZ"),
        show_oi: bool = Query(True),
        period: PeriodType = Query("6m"),
        date_from: Optional[date] = Query(None),
        date_to: Optional[date] = Query(None),
        db: Session = Depends(get_db),
        user = Depends(get_current_user_optional)
):
    """Получить данные графика с валидацией"""

    if interval not in {5, 60, 24}:
        raise HTTPException(status_code=400, detail="interval должен быть 5, 60 или 24")

    try:
        sec_id = validate_safe_id(sec_id, "sec_id")
        sectype = validate_safe_id(sectype, "sectype")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Tier-ограничения (chart endpoint используется на OI странице → правила OI)
    enforce_tier_limits(
        user, "open_interest",
        asset=sectype, interval=interval, period=period, clgroup=clgroup,
    )

    # 24h delay для Free
    effective_end = get_effective_end_date(user, "open_interest")
    if effective_end is not None:
        if date_to is None or date_to > effective_end:
            date_to = effective_end

    if date_from and date_to and date_to < date_from:
        raise HTTPException(status_code=400, detail="date_to не может быть раньше date_from")

    # Спрос на актив — приоритет фонового прогрева (api/main.py). Держать
    # тёплыми все 91 видимый актив разом нельзя (≈12 минут CPU), поэтому
    # порядок задаёт трафик, а не константа в коде.
    record_demand(sectype)

    # Кеширование (TTL 30мин — инкрементально обновляется при NOTIFY)
    cache_key = f"chart:{sec_id}:{sectype}:{inst_type}:{interval}:{clgroup}:{show_oi}:{period}:{date_from}:{date_to}"
    # Ответ лежит в кеше УЖЕ СЖАТЫМ и уходит клиенту как есть — ни json.loads →
    # dumps, ни gzip на nginx. На 5м/6м это ~370 мс CPU на каждом хите против
    # ~3 мс (замер 2026-08-10, см. api/cache.py). При 4 ядрах именно эти
    # миллисекунды складывались в очередь: одиночный запрос 1.6 с, шесть
    # подряд — 8-50 с.
    #
    # single-flight внутри: при истечении ключа считает один воркер, остальные
    # ждут его результат. Tier-логика выше — на роуте, date_to уже учитывает
    # effective_end → у Free/Paid разные cache_key, не течёт.
    gz = get_or_compute_gzip(
        cache_key,
        lambda: _compute_chart_data(
            db, sec_id, sectype, inst_type, interval,
            clgroup, show_oi, period, date_from, date_to,
        ),
        ttl=DEFAULT_TTL,
    )

    # Клиент без gzip (curl без заголовка, старые интеграции) — распаковываем.
    # Браузеры и наш фронт всегда шлют Accept-Encoding: gzip, так что это
    # редкий путь, а не общий.
    if "gzip" not in request.headers.get("accept-encoding", "").lower():
        return Response(content=gzip.decompress(gz), media_type="application/json")

    return Response(content=gz, media_type="application/json",
                    headers={"Content-Encoding": "gzip", "Vary": "Accept-Encoding"})


# Дельта старше этого возраста не имеет смысла: клиент долго спал (ноутбук в
# сне), выгоднее и проще сказать ему перезагрузить ряд целиком, чем гнать
# многодневный хвост через append-логику.
_DELTA_MAX_AGE_DAYS = 3


@router.get("/{sec_id}/delta")
def get_chart_delta(
        sec_id: str,
        sectype: str = Query(...),
        inst_type: InstTypeType = Query("futures"),
        interval: int = Query(24),
        clgroup: ClgroupType = Query("FIZ"),
        show_oi: bool = Query(True),
        since_candle: datetime = Query(..., description="time последней ЗАКРЫТОЙ свечи у клиента (без live)"),
        since_oi: Optional[datetime] = Query(None, description="time последней закрытой точки OI (без live)"),
        db: Session = Depends(get_db),
        user = Depends(get_current_user_optional)
):
    """Инкрементальный догруз графика по SSE-событию.

    Полный ответ /api/chart на 5м/6м — 6.2 МБ (26k свечей + 24k точек OI), и до
    этой ручки фронт перекачивал его ЦЕЛИКОМ на каждый NOTIFY раз в ~5 минут —
    отсюда конкуренция за CPU при пачке панелей и лаг перерисовки. Дельта отдаёт
    только новые ЗАКРЫТЫЕ бары/OI после since_* + свежую live-точку — килобайты.

    Контракт с клиентом: он присылает time последней закрытой точки (свою live
    предварительно срезает), назад получает строго более новые закрытые точки и,
    для актуальных тиров, live-точку (помечена live=true). Клиент срезает свою
    старую live и дописывает ответ в хвост.

    Tier-гейт зеркалит /api/chart: у Free потолок свежести effective_end
    (закрытые бары режутся по нему, live-точка не отдаётся вовсе) — дельта у
    Free почти всегда пустая, что корректно: его срез и не должен меняться
    внутри дня. Кеша нет намеренно: запросы точечные (индексный спуск за
    миллисекунды), а ключ включал бы since_* клиента — hit rate был бы нулевой.
    """
    if interval not in {5, 60, 24}:
        raise HTTPException(status_code=400, detail="interval должен быть 5, 60 или 24")

    try:
        sec_id = validate_safe_id(sec_id, "sec_id")
        sectype = validate_safe_id(sectype, "sectype")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Те же tier-правила, что у полного графика (актив/ТФ/срез).
    enforce_tier_limits(
        user, "open_interest",
        asset=sectype, interval=interval, clgroup=clgroup,
    )
    effective_end = get_effective_end_date(user, "open_interest")  # None = без задержки

    # Клиент слишком отстал → дельта выродится в пол-истории. Пусть грузит полный
    # ряд (тот как раз лежит в тёплом chart:-кеше).
    if since_candle.date() < date.today() - timedelta(days=_DELTA_MAX_AGE_DAYS):
        return {"full_reload": True, "candles": [], "open_interest": []}

    # sec_ids — как в _compute_chart_data: instruments + календарные контракты.
    sec_ids = [r[0] for r in db.execute(text("""
        SELECT DISTINCT sec_id FROM instruments
        WHERE sectype = :sectype AND type = :inst_type
    """), {"sectype": sectype, "inst_type": inst_type}).fetchall()] or [sec_id]
    if inst_type == 'futures':
        cal_ids = front_sec_ids(db, sectype)
        if cal_ids:
            sec_ids = list(dict.fromkeys(sec_ids + cal_ids))

    # ── Новые закрытые свечи (зеркало cache_updater._update_single_entry) ────
    new_candles_raw = db.execute(text("""
        SELECT begin_time, open, high, low, close, volume, sec_id
        FROM candles
        WHERE sec_id = ANY(:sec_ids) AND interval = :interval
          AND begin_time > :since
          AND (:cutoff IS NULL OR begin_time <= :cutoff)
        ORDER BY begin_time
    """), {"sec_ids": sec_ids, "interval": interval, "since": since_candle,
            # Лицензия MOEX: дельта — ещё один путь наружу (после роутера и
            # cache_updater). NULL для нерегулируемых ТФ — предикат вырождается.
            "cutoff": cutoff_for_interval(interval)}).fetchall()

    if effective_end is not None:
        new_candles_raw = [c for c in new_candles_raw if c[0].date() <= effective_end]
    if interval != 24:
        new_candles_raw = [c for c in new_candles_raw if float(c[5] or 0) > 0]

    # Лучший контракт дня по объёму — как в cache_updater / chart.py.
    from collections import defaultdict
    daily_volume = defaultdict(lambda: defaultdict(float))
    for c in new_candles_raw:
        daily_volume[c[0].date()][c[6]] += float(c[5] or 0)
    best_by_day = {day: max(contracts, key=contracts.get)
                   for day, contracts in daily_volume.items() if contracts}

    candles = [{
        "time": c[0].isoformat(),
        "open": float(c[1] or 0), "high": float(c[2] or 0),
        "low": float(c[3] or 0), "close": float(c[4] or 0),
        "volume": float(c[5] or 0),
    } for c in new_candles_raw if c[6] == best_by_day.get(c[0].date(), c[6])]

    # ── Новые закрытые точки OI ──────────────────────────────────────────────
    open_interest = []
    if show_oi and since_oi is not None:
        new_oi_raw = db.execute(text("""
            SELECT tradedate, tradetime, pos, pos_long, pos_short,
                   pos_long_num, pos_short_num
            FROM open_interest
            WHERE sectype = :sectype AND clgroup = :clgroup AND interval = :interval
              AND (tradedate > :last_date
                   OR (tradedate = :last_date AND tradetime > :last_time_part))
            ORDER BY tradedate, tradetime
        """), {
            "sectype": sectype, "clgroup": clgroup, "interval": interval,
            "last_date": since_oi.date(), "last_time_part": since_oi.time(),
        }).fetchall()

        for oi in new_oi_raw:
            trade_date = oi[0]
            if effective_end is not None and trade_date > effective_end:
                continue
            trade_time = oi[1] if oi[1] else dt_time(23, 50)
            if isinstance(trade_time, str):
                parts = trade_time.split(":")
                trade_time = dt_time(int(parts[0]), int(parts[1]),
                                     int(parts[2]) if len(parts) > 2 else 0)
            pos_long = int(oi[3] or 0)
            pos_short = int(oi[4] or 0)
            open_interest.append({
                "time": datetime.combine(trade_date, trade_time).isoformat(),
                "pos": int(oi[2] or 0),
                "pos_long": pos_long, "pos_short": pos_short,
                "pos_long_num": int(oi[5] or 0), "pos_short_num": int(oi[6] or 0),
                "net_position": pos_long + pos_short,
            })

    # ── Live-точка (только тирам без задержки — как в cache_updater) ─────────
    # Трюк с якорем: append_live_points сравнивает live-время с candles[-1],
    # поэтому подсовываем ему мини-ответ, где последняя закрытая точка — либо
    # свежедописанная, либо синтетический якорь с client-времени since_*.
    # Якоря после вызова отбрасываем — клиенту уходят только новые точки.
    if effective_end is None and interval in (24, 60):
        anchor_c = {"time": since_candle.isoformat()}
        anchor_o = {"time": (since_oi or since_candle).isoformat()}
        mini = {
            "interval": interval, "sectype": sectype, "clgroup": clgroup,
            "mode": "price_and_oi" if (show_oi and since_oi is not None) else "price_only",
            "contracts": sec_ids,
            "candles": [anchor_c] + candles,
            "open_interest": ([anchor_o] + open_interest) if (show_oi and since_oi is not None) else [],
        }
        append_live_points(db, mini)
        candles = mini["candles"][1:]
        if mini["open_interest"]:
            open_interest = mini["open_interest"][1:]

    # ── Растяжка цены в дельте ───────────────────────────────────────────────
    # Без неё дельта ломала #1113: клиент срезает свой stretched-хвост перед
    # склейкой (иначе заполнение стало бы since и настоящие свечи потерялись
    # бы), а сервер прежде ничего взамен не присылал. Итог: через 5 минут после
    # загрузки правый край цены отступал к потолку лицензии, и точки ОИ снова
    # висели без цены — ровно тот разрыв, который #1113 закрывал.
    #
    # Якорь тот же приём, что у live-точки: append_stretched_price растягивает
    # от ПОСЛЕДНЕЙ РЕАЛЬНОЙ свечи, а она может быть у клиента и не попасть в
    # дельту. Берём её из БД (одна строка по индексу) и подставляем якорем,
    # после чего отбрасываем — клиенту уходят только новые точки.
    if effective_end is None and show_oi and open_interest:
        anchor = None
        if candles and not candles[-1].get("live"):
            anchor = candles[-1]
        else:
            row = db.execute(text("""
                SELECT begin_time, close FROM candles
                WHERE sec_id = ANY(:sec_ids) AND interval = :interval
                  AND (:cutoff IS NULL OR begin_time <= :cutoff) AND close > 0
                ORDER BY begin_time DESC LIMIT 1
            """), {"sec_ids": sec_ids, "interval": interval,
                   "cutoff": cutoff_for_interval(interval)}).fetchone()
            if row and row[0]:
                anchor = {"time": row[0].isoformat(), "close": float(row[1] or 0)}

        if anchor:
            live_tail = [c for c in candles if c.get("live")]
            closed = [c for c in candles if not c.get("live")]
            mini = {
                "candles": ([anchor] if anchor not in closed else []) + closed,
                "open_interest": open_interest,
            }
            if append_stretched_price(mini):
                stretched = [c for c in mini["candles"] if c.get("stretched")]
                candles = closed + stretched + live_tail

    return {"full_reload": False, "candles": candles, "open_interest": open_interest}


def _compute_chart_data(db, sec_id, sectype, inst_type, interval,
                        clgroup, show_oi, period, date_from, date_to):
    """Тяжёлый расчёт данных графика (вызывается через single-flight выше)."""
    log.info(f"REQUEST: {sec_id}, sectype={sectype}, interval={interval}, period={period}")
    total_start = time.time()

    # 1. Получаем sec_ids
    t0 = time.time()
    sec_ids_result = db.execute(text("""
        SELECT DISTINCT sec_id FROM instruments
        WHERE sectype = :sectype AND type = :inst_type
    """), {"sectype": sectype, "inst_type": inst_type}).fetchall()
    sec_ids = [r[0] for r in sec_ids_result] or [sec_id]
    # Расширяем календарными контрактами (futures_contracts): для фьючерсов
    # подтягиваем sec_id'ы, которых ещё нет в instruments (напр. летние
    # BRQ/BRU/BRV до их авто-регистрации) — иначе их свечи не попадут в выборку
    # и календарный фронт за эти дни откатится на объём. Спот не трогаем.
    if inst_type == 'futures':
        cal_ids = front_sec_ids(db, sectype)
        if cal_ids:
            sec_ids = list(dict.fromkeys(sec_ids + cal_ids))
    log.info(f"[1] sec_ids: {(time.time()-t0)*1000:.0f} мс | {sec_ids}")

    # 2-4. Рабочий период
    # Для интрадей (5мин/1час) с фиксированным period — пропускаем тяжёлые
    # MIN/MAX bounds запросы (экономим ~3 сек) и считаем даты от сегодня.
    # Для дневных или period=all — нужны точные границы из БД.
    t0 = time.time()
    use_fast_path = interval != 24 and period != "all" and not (date_from and date_to)

    if use_fast_path:
        # Fast path: считаем от сегодня, без bounds запросов.
        # date_to — ПОТОЛОК свежести (у Free это today−1 из get_effective_end_date).
        # Раньше fast path его игнорировал, и задержка 24ч действовала только на
        # дневном ТФ: на 5м/1ч Free получал бары вплоть до текущего момента
        # (баг 2026-08-10, вылез когда интрадей открыли всем тирам).
        work_end = min(date.today(), date_to) if date_to else date.today()
        days = PERIODS.get(period, 180)
        work_start = work_end - timedelta(days=days)
        has_oi_data = show_oi
        mode = "price_and_oi" if show_oi else "price_only"
        data_start = work_start
        data_end = work_end
        c_start = work_start
        c_end = work_end
        oi_start = work_start
        oi_end = work_end
        log.info(f"[2-4] fast path: {(time.time()-t0)*1000:.0f} мс | {work_start} - {work_end}")
    else:
        # Full path: запрашиваем точные границы из БД.
        # НЕ заменять на MIN/MAX + sec_id = ANY(...): планировщик не умеет
        # пер-элементный спуск по (sec_id, interval, begin_time) и уходит в скан
        # индекса (interval, begin_time) с начала истории — для недавно
        # листингованных активов (TB, IB, BT) это 40-60 сек. LATERAL по каждому
        # контракту = один спуск по индексу, миллисекунды.
        bounds_row = db.execute(text("""
            SELECT MIN(b.lo), MAX(b.hi)
            FROM unnest(CAST(:sec_ids AS text[])) AS s(sid)
            CROSS JOIN LATERAL (
                SELECT (SELECT begin_time FROM candles
                        WHERE sec_id = s.sid AND interval = :interval
                        ORDER BY begin_time ASC LIMIT 1) AS lo,
                       (SELECT begin_time FROM candles
                        WHERE sec_id = s.sid AND interval = :interval
                        ORDER BY begin_time DESC LIMIT 1) AS hi
            ) b
        """), {"sec_ids": sec_ids, "interval": interval}).fetchone()

        c_start = bounds_row[0] if bounds_row else None
        c_end = bounds_row[1] if bounds_row else None

        if c_start:
            c_start = c_start.date() if hasattr(c_start, 'date') else c_start
        if c_end:
            c_end = c_end.date() if hasattr(c_end, 'date') else c_end

        log.info(f"[2] candles bounds: {(time.time()-t0)*1000:.0f} мс | {c_start} - {c_end}")

        t0 = time.time()
        oi_bounds = db.execute(text("""
            SELECT MIN(tradedate), MAX(tradedate) FROM open_interest
            WHERE sectype = :sectype AND clgroup = :clgroup AND interval = :interval
        """), {"sectype": sectype, "clgroup": clgroup, "interval": interval}).fetchone()
        oi_start, oi_end = oi_bounds if oi_bounds else (None, None)
        log.info(f"[3] OI bounds: {(time.time()-t0)*1000:.0f} мс | {oi_start} - {oi_end}")

        if not c_end:
            log.warning("[!] Нет данных свечей!")
            return {
                "sec_id": sec_id, "sectype": sectype, "interval": interval, "clgroup": clgroup,
                "candles_count": 0, "oi_count": 0, "candles": [], "open_interest": [],
                "has_oi_data": False, "contracts": sec_ids, "mode": "price_only", "period": period,
                "available_intervals": [], "contract_switches": [],
            }

        has_oi_data = oi_end is not None

        if show_oi and has_oi_data:
            data_start = max(c_start, oi_start)
            data_end = min(c_end, oi_end)
            mode = "price_and_oi"
        else:
            data_start = c_start
            data_end = c_end
            mode = "price_only"

        # date_to применяется НЕЗАВИСИМО от date_from: это потолок свежести
        # (у Free — today−1 из get_effective_end_date), а не только вторая
        # граница ручного диапазона. Раньше без date_from он игнорировался, и
        # тирная задержка на этом пути не работала вовсе.
        work_end = min(data_end, date_to) if date_to else data_end
        if date_from:
            work_start = max(data_start, date_from)
        else:
            days = PERIODS.get(period, 180)
            work_start = max(data_start, work_end - timedelta(days=days))

        log.info(f"[4] work period: {(time.time()-t0)*1000:.0f} мс | {work_start} - {work_end}")

    # 5. Запрос свечей (включаем sec_id для умной дедупликации)
    t0 = time.time()
    candles_raw = db.execute(text("""
        SELECT begin_time, open, high, low, close, volume, sec_id, secid
        FROM candles
        WHERE sec_id = ANY(:sec_ids)
          AND interval = :interval
          AND begin_time >= :start_time
          AND begin_time <= :end_time
          AND close > 0
        ORDER BY begin_time
    """), {
        "sec_ids": sec_ids,
        "interval": interval,
        "start_time": datetime.combine(work_start, dt_time.min),
        # ⚠️ ЛИЦЕНЗИЯ MOEX: 5-минутная цена раздаётся с задержкой
        # PRICE_DELAY_MINUTES. Режем ЗДЕСЬ, в самом запросе, а не после сборки
        # ответа: так свежая свеча физически не попадает ни в ответ, ни в кеш,
        # ни в производные (непрерывная серия, back-adjust).
        # Часовой и дневной ТФ под задержку не подпадают (решение владельца
        # 2026-08-11), поэтому cutoff_for_interval вернёт им None и граница
        # останется прежней. ОИ не придерживаем ни на одном ТФ — отсюда хвост
        # без цены на 5м, который закрывает append_stretched_price.
        "end_time": min(datetime.combine(work_end, dt_time.max), _cut)
                    if (_cut := cutoff_for_interval(interval))
                    else datetime.combine(work_end, dt_time.max),
    }).fetchall()
    log.info(f"[5] candles query: {(time.time()-t0)*1000:.0f} мс | rows: {len(candles_raw)}")

    # Fast path fallback: если данных нет — инструмент не торгуется или снят
    if use_fast_path and len(candles_raw) == 0:
        log.warning(f"[!] Fast path: нет данных для {sec_id} interval={interval} period={period}, "
                     f"диапазон {work_start}..{work_end}. Инструмент может быть снят с торгов.")
        return {
            "sec_id": sec_id, "sectype": sectype, "interval": interval, "clgroup": clgroup,
            "candles_count": 0, "oi_count": 0, "candles": [], "open_interest": [],
            "has_oi_data": False, "contracts": sec_ids, "mode": "price_only", "period": period,
            "available_intervals": [], "contract_switches": [],
        }

    # 6. Склейка контрактов — КАЛЕНДАРНЫЙ ролловер по датам экспирации.
    # Для каждого дня берём фронт-контракт из календаря (futures_contracts),
    # с откатом на объём, если за день нет свечей календарного фронта.
    t0 = time.time()

    # 6a. Дедупликация: для каждого (begin_time, sec_id) оставляем свечу с макс volume
    # (ISS иногда отдаёт дубли с разных бордов — vol=269k и vol=20)
    dedup = {}  # {(begin_time, sec_id): candle_row}
    for c in candles_raw:
        bt = c[0]
        vol = float(c[5] or 0)
        sec_id_c = c[6] if len(c) > 6 else 'unknown'
        # Для интрадей: пропускаем фейковые свечи (volume=0, zero-fill артефакты)
        if interval != 24 and vol == 0:
            continue
        key = (bt, sec_id_c)
        if key not in dedup or vol > float(dedup[key][5] or 0):
            dedup[key] = c

    # 6b. Группируем по дням, считаем объём каждого контракта
    daily_volume = {}  # {date: {sec_id: total_volume}}
    for (bt, sid), c in dedup.items():
        day = bt.date()
        vol = float(c[5] or 0)
        if day not in daily_volume:
            daily_volume[day] = {}
        daily_volume[day][sid] = daily_volume[day].get(sid, 0) + vol

    # 6c. КАЛЕНДАРНЫЙ ролловер: фронт-контракт каждого дня берём из календаря
    # экспираций (futures_contracts.lsttrade), а не по объёму. Детерминированно:
    # без преждевременных роллов (контракт — фронт ВКЛЮЧАЯ день экспирации) и без
    # пропусков (контракты по очереди по lsttrade, в т.ч. неликвидные летние BR).
    # Если за исторический день нет свечей календарного фронта (старый объёмный
    # фетчер сохранил другой контракт) — resolve_day откатывается на макс-объём
    # дня. Календаря нет совсем → чистый объём (прежнее поведение). Единый
    # источник истины — api/services/contract_calendar.
    windows = front_windows(db, sectype)
    sorted_days = sorted(daily_volume.keys())
    best_contract_by_day = {}
    prev_contract = None
    for day in sorted_days:
        chosen = resolve_day(windows, day, daily_volume[day], prev=prev_contract)
        best_contract_by_day[day] = chosen
        if chosen is not None:
            prev_contract = chosen

    # Метки смены контракта — по УСТОЙЧИВЫМ ранам, а не по-дневным выборам:
    # в зонах без календаря fallback может на день-два переметнуться между
    # контрактами, и каждый флип давал бы ложную метку экспирации на графике.
    contract_switches = []
    prev_run = None
    for run_day, run_sec_id in stable_runs(sorted_days, best_contract_by_day):
        contract_switches.append({
            "date": run_day.isoformat(),
            "from": prev_run,
            "to": run_sec_id,
        })
        prev_run = run_sec_id

    # 6d. Фильтруем свечи: оставляем только лучший контракт каждого дня
    best_by_time = {}  # {begin_time: candle_row}
    for (bt, sid), c in dedup.items():
        day = bt.date()
        best_sid = best_contract_by_day.get(day)
        if sid == best_sid:
            # Если на тот же timestamp уже есть свеча — берём с большим volume
            if bt not in best_by_time or float(c[5] or 0) > float(best_by_time[bt][5] or 0):
                best_by_time[bt] = c

    sorted_candles = sorted(best_by_time.values(), key=lambda x: x[0])
    log.info(f"[6] chain: {(time.time()-t0)*1000:.0f} мс | candles: {len(sorted_candles)}")

    # 7. Запрос OI
    oi_raw = []
    if show_oi and has_oi_data and sorted_candles:
        t0 = time.time()
        actual_start = sorted_candles[0][0].date()
        actual_end = sorted_candles[-1][0].date()

        oi_raw = db.execute(text("""
            SELECT 
                tradedate, 
                tradetime, 
                pos,
                pos_long, 
                pos_short, 
                pos_long_num, 
                pos_short_num
            FROM open_interest
            WHERE sectype = :sectype
              AND clgroup = :clgroup
              AND interval = :interval
              AND tradedate >= :start_date
              AND tradedate <= :end_date
            ORDER BY tradedate, tradetime
        """), {
            "sectype": sectype,
            "clgroup": clgroup,
            "interval": interval,
            "start_date": actual_start,
            "end_date": actual_end
        }).fetchall()
        log.info(f"[7] OI query: {(time.time()-t0)*1000:.0f} мс | rows: {len(oi_raw)}")

    # 7.5 Back-adjustment непрерывного фьючерса по корпоративным событиям.
    #
    # При сплите акции MOEX пере-специфицирует фьючерс — цена контракта
    # скачкообразно меняется в разы (ГМК Норникель, апрель 2024: ~15000 → ~1800).
    # Без коррекции многолетний график получает вертикальный «обрыв». Скачок
    # детектируем по отношению close соседних свечей: рыночное движение и
    # ролловер-гэп ликвидного фьючерса не дают >2x за бар — сплит и
    # пере-спецификация дают. Идём с конца (свежие цены — якорь, не трогаем),
    # накапливаем множитель и применяем к более старым свечам.
    # Детекция ТОЛЬКО внутри одного контракта (sec_id совпадает у соседних
    # свечей): пере-спецификация при сплите меняет цену внутри контракта, а
    # >2x скачок НА СТЫКЕ ролловера у неликвидных серий — это разница уровней
    # контрактов, не сплит. Без этого ограничения одна ложная детекция на
    # стыке домножала (×2+) всю старую историю графика.
    SPLIT_RATIO_THRESHOLD = 2.0
    n_c = len(sorted_candles)
    price_mult = [1.0] * n_c
    adj_factor = 1.0
    for i in range(n_c - 1, 0, -1):
        prev_close = float(sorted_candles[i - 1][4] or 0)
        cur_close = float(sorted_candles[i][4] or 0)
        # Сравниваем по ПОЛНОМУ secid (index 7, 'BRN6'), а не sec_id (index 6,
        # 'BRN'): sec_id не уникален по годам (BRN5/BRN6 → 'BRN'), что при годовом
        # разрыве в цепочке давало бы ложный same_contract на стыке ролла.
        same_contract = sorted_candles[i - 1][7] == sorted_candles[i][7]
        if same_contract and prev_close > 0 and cur_close > 0:
            ratio = cur_close / prev_close
            if ratio >= SPLIT_RATIO_THRESHOLD or ratio <= 1.0 / SPLIT_RATIO_THRESHOLD:
                adj_factor *= ratio
        price_mult[i - 1] = adj_factor
    if adj_factor != 1.0:
        log.info(f"[7.5] back-adjust {sec_id}: factor={adj_factor:.4g}")

    # 7.6 Реестр кросс-контрактных корпоративных событий (сплит/консолидация на
    # стыке контрактов + разрыв листинга — within-contract детектор 7.5 их не
    # ловит). Домножаем свечи СТАРЕЕ boundary_date на ratio (first_new/last_old),
    # чтобы непрерывный график стал непрерывным. Ключ — sectype (общее для серии).
    corp_actions = KNOWN_FUT_CORP_ACTIONS.get(sectype)
    if corp_actions:
        applied = 0
        for idx, c in enumerate(sorted_candles):
            cdate = c[0].date()
            m = 1.0
            for boundary_date, ratio in corp_actions:
                if cdate < boundary_date:
                    m *= ratio
            if m != 1.0:
                price_mult[idx] *= m
                applied += 1
        if applied:
            log.info(f"[7.6] corp-action adjust {sectype}: {len(corp_actions)} event(s), {applied} candles")

    # 8. Формируем ответ (прямые dict вместо Pydantic — в 5-10x быстрее)
    t0 = time.time()
    candles_list = [
        {
            "time": c[0].isoformat(),
            "open": float(c[1] or 0) * price_mult[idx],
            "high": float(c[2] or 0) * price_mult[idx],
            "low": float(c[3] or 0) * price_mult[idx],
            "close": float(c[4] or 0) * price_mult[idx],
            "volume": float(c[5] or 0),
        } for idx, c in enumerate(sorted_candles)
    ]

    # net_position = pos_long + pos_short
    # pos_short в БД уже ОТРИЦАТЕЛЬНЫЙ, поэтому ПЛЮС
    oi_list = []
    for r in oi_raw:
        pos_long = int(r[3] or 0)
        pos_short = int(r[4] or 0)
        oi_list.append({
            "time": datetime.combine(r[0], r[1]).isoformat(),
            "pos": int(r[2] or 0),
            "pos_long": pos_long,
            "pos_short": pos_short,
            "pos_long_num": int(r[5] or 0),
            "pos_short_num": int(r[6] or 0),
            "net_position": pos_long + pos_short,
        })

    log.info(f"[8] build response: {(time.time()-t0)*1000:.0f} мс")

    total_ms = (time.time() - total_start) * 1000
    log.info(f"TOTAL: {sec_id} {total_ms:.0f} мс")

    # 9. Получаем доступные интервалы OI
    available_intervals = [
        row[0] for row in db.execute(text("""
            SELECT DISTINCT interval
            FROM open_interest
            WHERE sectype = :sectype AND clgroup = :clgroup
            ORDER BY interval
        """), {"sectype": sectype, "clgroup": clgroup}).fetchall()
    ]

    response = {
        "sec_id": sec_id,
        "sectype": sectype,
        "interval": interval,
        "clgroup": clgroup,
        "candles_count": len(candles_list),
        "oi_count": len(oi_list),
        "candles": candles_list,
        "open_interest": oi_list,
        "oi_start_date": str(oi_raw[0][0]) if oi_raw else None,
        "oi_end_date": str(oi_raw[-1][0]) if oi_raw else None,
        "candles_start_date": str(sorted_candles[0][0].date()) if sorted_candles else None,
        "candles_end_date": str(sorted_candles[-1][0].date()) if sorted_candles else None,
        "has_oi_data": has_oi_data,
        "contracts": sec_ids,
        "mode": mode,
        "period": period,
        "data_start": str(data_start) if data_start else None,
        "data_end": str(data_end) if data_end else None,
        "available_intervals": available_intervals,
        "contract_switches": contract_switches,
    }

    # Дописываем «живую» точку с текущим значением (см. api/services/chart_live.py):
    # на дневном/часовом ТФ последняя закрытая свеча отстаёт от текущего момента,
    # поэтому добавляем свежую 5-минутную котировку и net-позицию как live-точку.
    # Только для актуального запроса без ограничения по дате: Free-тариф получает
    # OI с задержкой 24ч (date_to подменён на effective_end выше) — для него
    # live-точку НЕ добавляем, иначе реалтайм утечёт мимо tier-гейта. Исторические
    # запросы (date_from/date_to заданы явно) тоже без live-точки.
    if date_from is None and date_to is None:
        append_live_points(db, response)

    # Цена придержана на 15 минут, ОИ актуален → правее последней свечи висят
    # точки ОИ без цены под ними. Продлеваем последнюю известную цену на этот
    # отрезок (флаг stretched), иначе график выглядит оборванным. Когда свеча
    # доедет, растянутая точка на том же времени заменится фактической.
    append_stretched_price(response)

    return response