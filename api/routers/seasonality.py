"""
Seasonality API — Сезонность
Рассчитывает среднее изменение цены по временным интервалам.

4 режима:
- intraday: по часам (10-18), interval=60
- weekday: по дням недели (Пн-Пт), interval=24
- monthday: по дням месяца (1-31), interval=24
- monthly: по месяцам (Янв-Дек), interval=24

Период задаётся через iterations (последние N группировочных единиц).

Дивидендные гэпы:
- Загружаются registryclosedate + value с ISS MOEX
- Экс-дата определяется эмпирически: ищем в наших свечах день с гэпом
  (prev_close - open) ≈ dividend_amount в окне [registryclosedate - 5, registryclosedate]
- При exclude_dividends=True: на экс-дате return корректируется добавлением
  dividend/prev_close * 100 к дневному изменению
"""
from fastapi import APIRouter, Query, HTTPException, Depends
from sqlalchemy import text
from datetime import date, timedelta
import time

from api.database import get_engine
from api.cache import get_or_set
from api.logger import get_logger
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_guest_limits

log = get_logger()

router = APIRouter(prefix="/api/seasonality", tags=["seasonality"])

HOUR_LABELS = {h: f"{h}:00" for h in range(10, 19)}  # 10:00 - 18:00
WEEKDAY_LABELS = {1: "Пн", 2: "Вт", 3: "Ср", 4: "Чт", 5: "Пт"}
MONTH_LABELS = {
    1: "Янв", 2: "Фев", 3: "Мар", 4: "Апр", 5: "Май", 6: "Июн",
    7: "Июл", 8: "Авг", 9: "Сен", 10: "Окт", 11: "Ноя", 12: "Дек",
}


def _get_ex_dates_from_db(engine, secid: str) -> dict[str, float]:
    """
    Читает экс-дивидендные даты из предрассчитанной таблицы dividends.
    Возвращает {ex_date_iso: dividend_value, ...}
    """
    cache_key = f"ex_dates_db:{secid}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    ex_dates = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ex_date, value
                FROM dividends
                WHERE secid = :secid AND ex_date IS NOT NULL
            """), {"secid": secid}).fetchall()

        for row in rows:
            ex_dates[row[0].isoformat()] = float(row[1])
    except Exception as e:
        log.warning(f"Failed to read dividends for {secid}: {e}")

    get_or_set(cache_key, ex_dates, ttl=3600)
    return ex_dates


def _compute_seasonality_daily(engine, secid: str, mode: str, iterations: int,
                                ex_dates: dict[str, float]) -> list[dict]:
    """
    Вычисляет сезонность для дневных режимов (weekday, monthday, monthly).
    Использует close-to-close returns с опциональной дивидендной корректировкой.
    """
    # Определяем группировку и CTE для итераций
    if mode == "weekday":
        iteration_cte = """
            SELECT DISTINCT date_trunc('week', begin_time::date) AS iter_key
            FROM candles
            WHERE secid = :secid AND interval = 24 AND type = 'stock' AND open > 0
            ORDER BY iter_key DESC
            LIMIT :iterations
        """
        group_expr = "EXTRACT(ISODOW FROM c.begin_time)::int"
        extra_filter = "AND EXTRACT(ISODOW FROM c.begin_time) BETWEEN 1 AND 5"
        join_cond = "date_trunc('week', c.begin_time::date) = ri.iter_key"
        labels = WEEKDAY_LABELS

    elif mode == "monthday":
        iteration_cte = """
            SELECT DISTINCT date_trunc('month', begin_time::date) AS iter_key
            FROM candles
            WHERE secid = :secid AND interval = 24 AND type = 'stock' AND open > 0
              AND EXTRACT(ISODOW FROM begin_time) BETWEEN 1 AND 5
            ORDER BY iter_key DESC
            LIMIT :iterations
        """
        group_expr = "EXTRACT(DAY FROM c.begin_time)::int"
        extra_filter = "AND EXTRACT(ISODOW FROM c.begin_time) BETWEEN 1 AND 5"
        join_cond = "date_trunc('month', c.begin_time::date) = ri.iter_key"
        labels = None

    else:  # monthly
        iteration_cte = """
            SELECT DISTINCT EXTRACT(YEAR FROM begin_time)::int AS iter_key
            FROM candles
            WHERE secid = :secid AND interval = 24 AND type = 'stock' AND open > 0
              AND EXTRACT(ISODOW FROM begin_time) BETWEEN 1 AND 5
            ORDER BY iter_key DESC
            LIMIT :iterations
        """
        group_expr = "EXTRACT(MONTH FROM c.begin_time)::int"
        extra_filter = "AND EXTRACT(ISODOW FROM c.begin_time) BETWEEN 1 AND 5"
        join_cond = "EXTRACT(YEAR FROM c.begin_time)::int = ri.iter_key"
        labels = MONTH_LABELS

    # Получаем все свечи за нужный период с предыдущими close через LAG
    with engine.connect() as conn:
        # Шаг 1: все свечи для этого тикера с LAG(close) по дневным
        all_candles = conn.execute(text(f"""
            WITH recent_iters AS (
                {iteration_cte}
            ),
            filtered AS (
                SELECT
                    c.begin_time,
                    c.begin_time::date as trade_date,
                    c.open,
                    c.close,
                    ({group_expr}) as grp_key
                FROM candles c
                INNER JOIN recent_iters ri ON {join_cond}
                WHERE c.secid = :secid
                  AND c.interval = 24
                  AND c.type = 'stock'
                  AND c.open > 0
                  {extra_filter}
            ),
            with_prev AS (
                SELECT
                    trade_date,
                    open,
                    close,
                    grp_key,
                    LAG(close) OVER (ORDER BY begin_time) as prev_close
                FROM filtered
            )
            SELECT trade_date, open, close, grp_key, prev_close
            FROM with_prev
            WHERE prev_close IS NOT NULL AND prev_close > 0
            ORDER BY trade_date
        """), {"secid": secid, "iterations": iterations}).fetchall()

    if not all_candles:
        return []

    # Шаг 2: вычисляем returns с дивидендной корректировкой
    from collections import defaultdict
    groups = defaultdict(list)

    for row in all_candles:
        trade_date = row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0])
        open_p = float(row[1])
        close_p = float(row[2])
        grp_key = int(row[3])
        prev_close = float(row[4])

        # Close-to-close return
        raw_return = (close_p - prev_close) / prev_close * 100

        # Дивидендная корректировка
        if trade_date in ex_dates:
            div_val = ex_dates[trade_date]
            # На экс-дате: добавляем дивиденд обратно к close
            # adjusted_return = (close + div - prev_close) / prev_close * 100
            adjusted_return = (close_p + div_val - prev_close) / prev_close * 100
            groups[grp_key].append(adjusted_return)
        else:
            groups[grp_key].append(raw_return)

    # Шаг 3: агрегация
    bars = []
    for key in sorted(groups.keys()):
        values = groups[key]
        avg_change = sum(values) / len(values) if values else 0
        if labels:
            label = labels.get(key, str(key))
        else:
            label = str(key)
        bars.append({
            "label": label,
            "key": key,
            "avg_change": round(avg_change, 4),
            "count": len(values),
        })

    return bars


@router.get("")
async def get_seasonality(
    secid: str = Query(..., description="Тикер акции"),
    mode: str = Query("weekday", description="Режим: intraday, weekday, monthday, monthly"),
    iterations: int = Query(90, ge=1, le=9999, description="Кол-во последних итераций"),
    exclude_dividends: bool = Query(False, description="Убрать дивидендные гэпы"),
    user=Depends(get_current_user_optional),
):
    if mode not in ("intraday", "weekday", "monthday", "monthly"):
        raise HTTPException(400, "mode must be one of: intraday, weekday, monthday, monthly")

    # Гостевые ограничения
    if mode == "intraday":
        enforce_guest_limits(user, interval=60)

    cache_key = f"seasonality:{secid}:{mode}:iter{iterations}:nodiv{exclude_dividends}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    start_time = time.time()
    log.info(f"REQUEST: /seasonality secid={secid} mode={mode} iter={iterations} excl_div={exclude_dividends}")

    engine = get_engine()

    if mode == "intraday":
        # Intraday: open-to-close per hour, дивиденды не влияют
        query = text("""
            WITH recent_days AS (
                SELECT DISTINCT begin_time::date AS trade_date
                FROM candles
                WHERE secid = :secid AND interval = 60 AND type = 'stock' AND open > 0
                ORDER BY trade_date DESC
                LIMIT :iterations
            )
            SELECT
                EXTRACT(HOUR FROM c.begin_time)::int as key,
                AVG((c.close - c.open) / NULLIF(c.open, 0) * 100) as avg_change,
                COUNT(*) as cnt
            FROM candles c
            INNER JOIN recent_days rd ON c.begin_time::date = rd.trade_date
            WHERE c.secid = :secid
              AND c.interval = 60
              AND c.type = 'stock'
              AND EXTRACT(HOUR FROM c.begin_time) BETWEEN 10 AND 18
              AND c.open > 0
            GROUP BY EXTRACT(HOUR FROM c.begin_time)
            ORDER BY key
        """)
        with engine.connect() as conn:
            rows = conn.execute(query, {"secid": secid, "iterations": iterations}).fetchall()

        bars = []
        for row in rows:
            bars.append({
                "label": HOUR_LABELS.get(int(row[0]), str(int(row[0]))),
                "key": int(row[0]),
                "avg_change": round(float(row[1]), 4) if row[1] else 0,
                "count": int(row[2]),
            })
    else:
        # Дневные режимы: close-to-close returns с дивидендной корректировкой
        ex_dates: dict[str, float] = {}
        if exclude_dividends:
            ex_dates = _get_ex_dates_from_db(engine, secid)
            if ex_dates:
                log.info(f"  Found {len(ex_dates)} ex-dividend dates for {secid} from DB")

        bars = _compute_seasonality_daily(engine, secid, mode, iterations, ex_dates)

    if not bars:
        raise HTTPException(404, f"Нет данных для {secid} в режиме {mode}")

    duration = time.time() - start_time
    log.info(f"DONE: /seasonality {secid}/{mode} {len(bars)} bars, {duration:.2f}s")

    result = {
        "secid": secid,
        "mode": mode,
        "iterations": iterations,
        "exclude_dividends": exclude_dividends,
        "ex_dates_count": len(ex_dates) if mode != "intraday" else 0,
        "bars": bars,
    }
    get_or_set(cache_key, result, ttl=300)
    return result


@router.get("/price")
async def get_price_chart(
    secid: str = Query(..., description="Тикер акции"),
    days: int = Query(365, ge=30, le=10000, description="Кол-во календарных дней"),
    user=Depends(get_current_user_optional),
):
    """
    График цены акции с и без дивидендных гэпов.
    Возвращает два ряда: raw (как есть) и adjusted (дивиденды добавлены обратно).

    Алгоритм корректировки:
    - Идём от конца к началу
    - На каждой экс-дате: все предыдущие close умножаем на (1 + div/close_before_ex)
    - Это стандартный метод adjusted close (как у Yahoo Finance)
    """
    cache_key = f"seasonality_price:{secid}:{days}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    engine = get_engine()
    start_time = time.time()

    from datetime import date as date_type
    date_from = date_type.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT begin_time::date as trade_date, close
            FROM candles
            WHERE secid = :secid
              AND interval = 24
              AND type = 'stock'
              AND close > 0
              AND begin_time::date >= :date_from
            ORDER BY begin_time
        """), {"secid": secid, "date_from": date_from.isoformat()}).fetchall()

    if not rows:
        raise HTTPException(404, f"Нет данных для {secid}")

    # Получаем экс-даты из БД
    ex_dates = _get_ex_dates_from_db(engine, secid)

    # Строим ценовой ряд
    dates = [r[0] for r in rows]
    closes = [float(r[1]) for r in rows]

    # Adjusted close: идём с конца, на каждой экс-дате корректируем предыдущие цены
    adjusted = closes.copy()
    if ex_dates:
        # Собираем экс-даты в диапазоне и сортируем по убыванию
        relevant_divs = []
        for ex_str, div_val in ex_dates.items():
            ex_d = date_type.fromisoformat(ex_str)
            if date_from <= ex_d <= date_type.today():
                relevant_divs.append((ex_d, div_val))
        relevant_divs.sort(reverse=True)  # от новых к старым

        for ex_d, div_val in relevant_divs:
            # Находим индекс экс-даты в нашем ряде
            ex_idx = None
            for i, d in enumerate(dates):
                if d >= ex_d:
                    ex_idx = i
                    break
            if ex_idx is not None and ex_idx > 0:
                # Коэффициент корректировки: используем close ДО экс-даты (pre-gap)
                close_before_ex = adjusted[ex_idx - 1]
                if close_before_ex > 0:
                    factor = 1 + div_val / close_before_ex
                    # Умножаем все предыдущие цены на factor
                    for j in range(ex_idx):
                        adjusted[j] *= factor

    # Формируем ответ
    data = []
    for i in range(len(dates)):
        data.append({
            "date": dates[i].isoformat(),
            "close": round(closes[i], 2),
            "adjusted": round(adjusted[i], 2),
        })

    duration = time.time() - start_time
    log.info(f"GET /seasonality/price {secid} {len(data)} points, {len(ex_dates)} divs, {duration:.2f}s")

    result = {
        "secid": secid,
        "days": days,
        "ex_dates_count": len(ex_dates),
        "data": data,
    }
    get_or_set(cache_key, result, ttl=300)
    return result
