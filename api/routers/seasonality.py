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

# Инструменты из index_data (не candles)
INDEX_DATA_INSTRUMENTS = {
    "IMOEX", "RTSI", "GLDRUB_TOM", "RGBITR", "RGBI", "RVI",
    "USD000UTSTOM", "EUR_RUB__TOM", "CNYRUB_TOM",
    "MCFTR", "RUSFAR3M",
}

# Вечные фьючерсы (candles, type='futures')
PERPETUAL_FUTURES = {"USDRUBF", "EURRUBF", "CNYRUBF", "IMOEXF"}


def _resolve_source(secid: str) -> tuple[str, str]:
    """Определяет источник данных: ('index_data'|'candles', type_filter)"""
    if secid in INDEX_DATA_INSTRUMENTS:
        return "index_data", ""
    if secid in PERPETUAL_FUTURES:
        return "candles", "futures"
    return "candles", "stock"

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


def _compute_monthly_returns_index_data(
    engine,
    secid: str,
    iterations: int,
    since_year: int | None = None,
    exclude_years: list[int] | None = None,
) -> list[dict]:
    """
    Месячная сезонность через close-to-close (правильная методология).
    Возвращает массив баров со статистикой: avg_change, std_change, count.
    """
    from collections import defaultdict

    with engine.connect() as conn:
        rows = conn.execute(text("""
            WITH month_ends AS (
                SELECT DISTINCT ON (
                    EXTRACT(YEAR FROM trade_date)::int,
                    EXTRACT(MONTH FROM trade_date)::int
                )
                    EXTRACT(YEAR FROM trade_date)::int as y,
                    EXTRACT(MONTH FROM trade_date)::int as m,
                    close
                FROM index_data
                WHERE secid = :secid AND close > 0
                ORDER BY 1, 2, trade_date DESC
            ),
            recent_years AS (
                SELECT DISTINCT y FROM month_ends
                ORDER BY y DESC LIMIT :iterations
            ),
            with_prev AS (
                SELECT y, m, close, LAG(close) OVER (ORDER BY y, m) as prev_close
                FROM month_ends
                WHERE y IN (SELECT y FROM recent_years)
                   OR y = (SELECT MIN(y) - 1 FROM recent_years)
            )
            SELECT y, m, close, prev_close FROM with_prev
            WHERE prev_close IS NOT NULL AND prev_close > 0 AND m IS NOT NULL
        """), {"secid": secid, "iterations": iterations}).fetchall()

    exclude_set = set(exclude_years or [])
    groups: dict[int, list[float]] = defaultdict(list)
    for y, m, close, prev_close in rows:
        if since_year is not None and y < since_year:
            continue
        if y in exclude_set:
            continue
        ret = (float(close) / float(prev_close) - 1) * 100
        groups[int(m)].append(ret)

    bars = []
    for m in sorted(groups.keys()):
        vals = groups[m]
        mean = sum(vals) / len(vals)
        variance = sum((v - mean) ** 2 for v in vals) / len(vals) if len(vals) > 1 else 0
        std = variance ** 0.5
        bars.append({
            "label": MONTH_LABELS.get(m, str(m)),
            "key": m,
            "avg_change": round(mean, 4),
            "std_change": round(std, 4),
            "count": len(vals),
        })
    return bars


def _compute_monthly_returns_candles(
    engine,
    secid: str,
    inst_type: str,
    iterations: int,
    ex_dates: dict[str, float],
    since_year: int | None = None,
    exclude_years: list[int] | None = None,
) -> list[dict]:
    """
    Месячная сезонность через close-to-close для акций/фьючерсов из candles.
    При exclude_dividends=True: корректируем все исторические цены мультипликативно
    до вычисления месячных возвратов.
    """
    from collections import defaultdict
    from datetime import date as _date

    type_filter = f"type = '{inst_type}'" if inst_type else "TRUE"

    with engine.connect() as conn:
        # Все дневные свечи нужны для adjusted close + month-end выборки
        rows = conn.execute(text(f"""
            SELECT begin_time::date as d, close
            FROM candles
            WHERE secid = :secid AND interval = 24 AND {type_filter} AND close > 0
            ORDER BY begin_time
        """), {"secid": secid}).fetchall()

    if not rows:
        return []

    dates = [r[0] for r in rows]
    closes = [float(r[1]) for r in rows]

    # Дивидендная корректировка (мультипликативная, Yahoo/CRSP style)
    if ex_dates:
        adjusted = closes.copy()
        for ex_str, div_val in sorted(ex_dates.items()):
            ex_d = _date.fromisoformat(ex_str)
            ex_idx = next((i for i, d in enumerate(dates) if d >= ex_d), None)
            if ex_idx is not None and ex_idx > 0:
                raw_prev = closes[ex_idx - 1]
                if raw_prev > 0 and div_val < raw_prev:
                    factor = (raw_prev - div_val) / raw_prev
                    for j in range(ex_idx):
                        adjusted[j] *= factor
        prices = adjusted
    else:
        prices = closes

    # Выбираем последний close каждого (year, month)
    last_per_ym: dict[tuple[int, int], float] = {}
    for d, p in zip(dates, prices):
        last_per_ym[(d.year, d.month)] = p  # перезаписывается на более поздний день
    sorted_keys = sorted(last_per_ym.keys())

    # Ограничиваем последними N годами
    recent_years = sorted(set(y for y, _ in sorted_keys), reverse=True)[:iterations]
    earliest_year = min(recent_years) if recent_years else None
    # Допустим один доп. год до earliest для LAG в январе
    filtered = [(y, m) for (y, m) in sorted_keys
                if earliest_year is None or y >= earliest_year - 1]

    exclude_set = set(exclude_years or [])
    groups: dict[int, list[float]] = defaultdict(list)
    for i in range(1, len(filtered)):
        (y, m) = filtered[i]
        if y < (earliest_year or 0):
            continue
        if since_year is not None and y < since_year:
            continue
        if y in exclude_set:
            continue
        prev_close = last_per_ym[filtered[i - 1]]
        close = last_per_ym[filtered[i]]
        if prev_close > 0:
            ret = (close / prev_close - 1) * 100
            groups[m].append(ret)

    bars = []
    for m in sorted(groups.keys()):
        vals = groups[m]
        mean = sum(vals) / len(vals)
        variance = sum((v - mean) ** 2 for v in vals) / len(vals) if len(vals) > 1 else 0
        std = variance ** 0.5
        bars.append({
            "label": MONTH_LABELS.get(m, str(m)),
            "key": m,
            "avg_change": round(mean, 4),
            "std_change": round(std, 4),
            "count": len(vals),
        })
    return bars


def _compute_seasonality_index_data(
    engine,
    secid: str,
    mode: str,
    iterations: int,
    since_year: int | None = None,
    exclude_years: list[int] | None = None,
) -> list[dict]:
    """Сезонность для инструментов из index_data (IMOEX, RTSI, GLDRUB_TOM и т.д.)."""
    from collections import defaultdict

    # Monthly режим → отдельная функция с close-to-close методологией
    if mode == "monthly":
        return _compute_monthly_returns_index_data(
            engine, secid, iterations,
            since_year=since_year, exclude_years=exclude_years,
        )

    # Оставшиеся режимы: weekday и monthday считают дневной средний
    # возврат (корректно для «средний понедельник» и «средний 15-й день месяца»).
    if mode == "weekday":
        group_expr = "EXTRACT(ISODOW FROM trade_date)::int"
        iter_expr = "date_trunc('week', trade_date)"
        extra = "AND EXTRACT(ISODOW FROM trade_date) BETWEEN 1 AND 5"
        labels = WEEKDAY_LABELS
    else:  # monthday
        group_expr = "EXTRACT(DAY FROM trade_date)::int"
        iter_expr = "date_trunc('month', trade_date)"
        extra = "AND EXTRACT(ISODOW FROM trade_date) BETWEEN 1 AND 5"
        labels = None

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            WITH recent_iters AS (
                SELECT DISTINCT {iter_expr} AS iter_key
                FROM index_data
                WHERE secid = :secid AND close > 0
                ORDER BY iter_key DESC
                LIMIT :iterations
            ),
            with_prev AS (
                SELECT
                    d.trade_date,
                    d.close,
                    ({group_expr}) as grp_key,
                    LAG(d.close) OVER (ORDER BY d.trade_date) as prev_close
                FROM index_data d
                INNER JOIN recent_iters ri ON {iter_expr} = ri.iter_key
                WHERE d.secid = :secid AND d.close > 0 {extra}
            )
            SELECT grp_key, trade_date, close, prev_close
            FROM with_prev
            WHERE prev_close IS NOT NULL AND prev_close > 0
            ORDER BY trade_date
        """), {"secid": secid, "iterations": iterations}).fetchall()

    if not rows:
        return []

    # Применяем фильтры since_year / exclude_years по дате каждого возврата
    exclude_set = set(exclude_years or [])
    groups = defaultdict(list)
    for grp_key, trade_date, close, prev_close in rows:
        year = trade_date.year
        if since_year is not None and year < since_year:
            continue
        if year in exclude_set:
            continue
        ret = (float(close) - float(prev_close)) / float(prev_close) * 100
        groups[int(grp_key)].append(ret)

    bars = []
    for key in sorted(groups.keys()):
        vals = groups[key]
        label = (labels or {}).get(key, str(key))
        bars.append({
            "label": label,
            "key": key,
            "avg_change": round(sum(vals) / len(vals), 4),
            "count": len(vals),
        })
    return bars


def _compute_seasonality_daily(engine, secid: str, mode: str, iterations: int,
                                ex_dates: dict[str, float],
                                since_year: int | None = None,
                                exclude_years: list[int] | None = None) -> list[dict]:
    """
    Вычисляет сезонность для дневных режимов (weekday, monthday, monthly).

    При exclude_dividends=True: строит мультипликативную adjusted close серию
    (стандарт Yahoo/CRSP), затем считает returns по adjusted ценам.
    При exclude_dividends=False: raw close-to-close returns.

    Monthly режим направляется в отдельную функцию — правильная методология
    «месячный возврат» (close конца месяца к close конца пред. месяца).
    """
    source, inst_type = _resolve_source(secid)

    # Monthly → close-to-close по месяцам (для индексов и акций/фьючерсов)
    if mode == "monthly":
        if source == "index_data":
            return _compute_monthly_returns_index_data(
                engine, secid, iterations,
                since_year=since_year, exclude_years=exclude_years,
            )
        return _compute_monthly_returns_candles(
            engine, secid, inst_type, iterations, ex_dates,
            since_year=since_year, exclude_years=exclude_years,
        )

    # Для index_data (не monthly) — отдельная логика
    if source == "index_data":
        return _compute_seasonality_index_data(
            engine, secid, mode, iterations,
            since_year=since_year, exclude_years=exclude_years,
        )

    type_filter = f"type = '{inst_type}'" if inst_type else "TRUE"

    # Определяем группировку и CTE для итераций
    if mode == "weekday":
        iteration_cte = f"""
            SELECT DISTINCT date_trunc('week', begin_time::date) AS iter_key
            FROM candles
            WHERE secid = :secid AND interval = 24 AND {type_filter} AND open > 0
            ORDER BY iter_key DESC
            LIMIT :iterations
        """
        group_expr = "EXTRACT(ISODOW FROM c.begin_time)::int"
        extra_filter = "AND EXTRACT(ISODOW FROM c.begin_time) BETWEEN 1 AND 5"
        join_cond = "date_trunc('week', c.begin_time::date) = ri.iter_key"
        labels = WEEKDAY_LABELS

    else:  # monthday — единственный оставшийся режим (monthly обработан выше)
        iteration_cte = f"""
            SELECT DISTINCT date_trunc('month', begin_time::date) AS iter_key
            FROM candles
            WHERE secid = :secid AND interval = 24 AND {type_filter} AND open > 0
              AND EXTRACT(ISODOW FROM begin_time) BETWEEN 1 AND 5
            ORDER BY iter_key DESC
            LIMIT :iterations
        """
        group_expr = "EXTRACT(DAY FROM c.begin_time)::int"
        extra_filter = "AND EXTRACT(ISODOW FROM c.begin_time) BETWEEN 1 AND 5"
        join_cond = "date_trunc('month', c.begin_time::date) = ri.iter_key"
        labels = None

    # Шаг 1: получаем все свечи за нужный период
    with engine.connect() as conn:
        all_candles = conn.execute(text(f"""
            WITH recent_iters AS (
                {iteration_cte}
            ),
            filtered AS (
                SELECT
                    c.begin_time,
                    c.begin_time::date as trade_date,
                    c.close,
                    ({group_expr}) as grp_key
                FROM candles c
                INNER JOIN recent_iters ri ON {join_cond}
                WHERE c.secid = :secid
                  AND c.interval = 24
                  AND {type_filter}
                  AND c.open > 0
                  {extra_filter}
                ORDER BY c.begin_time
            )
            SELECT trade_date, close, grp_key
            FROM filtered
        """), {"secid": secid, "iterations": iterations}).fetchall()

    if not all_candles:
        return []

    # Шаг 2: строим массивы дат, close, grp_key
    from collections import defaultdict
    trade_dates = []
    closes = []
    grp_keys = []
    for row in all_candles:
        td = row[0]
        trade_dates.append(td)
        closes.append(float(row[1]))
        grp_keys.append(int(row[2]))

    # Шаг 3: мультипликативная коррекция (если есть ex_dates)
    if ex_dates:
        adjusted = closes.copy()
        relevant_divs = []
        for ex_str, div_val in ex_dates.items():
            from datetime import date as dt
            ex_d = dt.fromisoformat(ex_str)
            relevant_divs.append((ex_d, div_val))
        relevant_divs.sort()  # от старых к новым

        for ex_d, div_val in relevant_divs:
            ex_idx = None
            for i, d in enumerate(trade_dates):
                d_cmp = d if isinstance(d, date) else d
                if d_cmp >= ex_d:
                    ex_idx = i
                    break
            if ex_idx is not None and ex_idx > 0:
                raw_prev_close = closes[ex_idx - 1]
                if raw_prev_close > 0 and div_val < raw_prev_close:
                    factor = (raw_prev_close - div_val) / raw_prev_close
                    for j in range(ex_idx):
                        adjusted[j] *= factor
        prices = adjusted
    else:
        prices = closes

    # Шаг 4: вычисляем returns и группируем (с фильтрами по годам)
    exclude_set = set(exclude_years or [])
    groups = defaultdict(list)
    for i in range(1, len(prices)):
        prev_price = prices[i - 1]
        if prev_price <= 0:
            continue
        year = trade_dates[i].year
        if since_year is not None and year < since_year:
            continue
        if year in exclude_set:
            continue
        ret = (prices[i] - prev_price) / prev_price * 100
        groups[grp_keys[i]].append(ret)

    # Шаг 5: агрегация
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


def _compute_yearly_seasonality(
    engine,
    secid: str,
    ex_dates: dict[str, float],
    since_year: int | None = None,
    exclude_years: list[int] | None = None,
) -> dict:
    """
    Годовая сезонность: кумулятивное изменение цены с начала года.

    Ключевой принцип: выравнивание по ТОРГОВОМУ ДНЮ (1-й, 2-й, 3-й...),
    а не по календарному дню года. Это устраняет шум от разных торговых
    календарей (выходные, праздники, субботние сессии).

    Для X-оси: маппим номер торгового дня → приблизительный месяц
    через текущий год (так Янв-Дек расположены пропорционально).
    """
    from collections import defaultdict
    from datetime import date as dt

    source, inst_type = _resolve_source(secid)

    with engine.connect() as conn:
        if source == "index_data":
            rows = conn.execute(text("""
                SELECT trade_date, close
                FROM index_data
                WHERE secid = :secid AND close > 0
                ORDER BY trade_date
            """), {"secid": secid}).fetchall()
        else:
            type_filter = f"type = '{inst_type}'" if inst_type else "TRUE"
            rows = conn.execute(text(f"""
                SELECT begin_time::date as trade_date, close
                FROM candles
                WHERE secid = :secid AND interval = 24 AND {type_filter} AND close > 0
                ORDER BY begin_time
            """), {"secid": secid}).fetchall()

    if not rows:
        return {}

    trade_dates = [r[0] for r in rows]
    closes = [float(r[1]) for r in rows]

    # Дивидендная коррекция (мультипликативная)
    if ex_dates:
        adjusted = closes.copy()
        relevant_divs = sorted(
            [(dt.fromisoformat(ex_str), dv) for ex_str, dv in ex_dates.items()]
        )
        for ex_d, div_val in relevant_divs:
            ex_idx = None
            for i, d in enumerate(trade_dates):
                if d >= ex_d:
                    ex_idx = i
                    break
            if ex_idx is not None and ex_idx > 0:
                raw_prev = closes[ex_idx - 1]
                if raw_prev > 0 and div_val < raw_prev:
                    factor = (raw_prev - div_val) / raw_prev
                    for j in range(ex_idx):
                        adjusted[j] *= factor
        prices = adjusted
    else:
        prices = closes

    # Группируем по годам — список (date, price) в хронологическом порядке
    current_year = dt.today().year
    years_data = defaultdict(list)
    for i, td in enumerate(trade_dates):
        years_data[td.year].append((td, prices[i]))

    # Calendar-aligned averaging: каждый год ресемплим к N_BUCKETS точек
    # (одна точка = доля года). Средняя траектория получается плавной и
    # end-of-year значение сходится к реальному среднему годовому возврату
    # (нет «клиффа», т.к. все годы дают по точке в каждый bucket).
    N_BUCKETS = 252  # типичное число торговых дней в году

    def _resample(values: list[float], n: int) -> list[float]:
        if len(values) == 0:
            return []
        if len(values) == 1:
            return [values[0]] * n
        out = []
        for i in range(n):
            f = i / (n - 1)
            src = f * (len(values) - 1)
            lo = int(src)
            hi = min(lo + 1, len(values) - 1)
            frac = src - lo
            out.append(values[lo] * (1 - frac) + values[hi] * frac)
        return out

    historical_buckets = [[] for _ in range(N_BUCKETS)]
    current_series = []
    min_year = None
    exclude_set = set(exclude_years or [])

    for yr in sorted(years_data.keys()):
        # Фильтры: since_year и exclude_years (не применяем к текущему году —
        # его траектория всегда показывается отдельно)
        if yr != current_year:
            if since_year is not None and yr < since_year:
                continue
            if yr in exclude_set:
                continue

        points = years_data[yr]
        if len(points) < 10:
            continue  # Слишком мало данных за год
        first_close = points[0][1]
        if first_close <= 0:
            continue
        pcts = [(p - first_close) / first_close * 100 for _, p in points]

        if yr == current_year:
            for td_idx, (td, _) in enumerate(points):
                current_series.append({
                    "td": td_idx,
                    "month": td.month,
                    "pct": round(pcts[td_idx], 2),
                    "date": td.isoformat(),
                })
        else:
            if min_year is None:
                min_year = yr
            resampled = _resample(pcts, N_BUCKETS)
            for i, v in enumerate(resampled):
                historical_buckets[i].append(v)

    # Среднее + стандартное отклонение по каждому bucket'у.
    # MIN_YEARS=1: при фильтрах since_year может остаться 1 год — это не
    # "среднее" статистически, но полезно для сравнения "как вёл себя 2025".
    # С calendar-aligned resampling все годы дают точки во все bucket'ы.
    MIN_YEARS = 1
    raw_avg = []  # (bucket, avg, std, count)
    for i in range(N_BUCKETS):
        vals = historical_buckets[i]
        if len(vals) >= MIN_YEARS:
            mean = sum(vals) / len(vals)
            variance = sum((v - mean) ** 2 for v in vals) / len(vals)
            std = variance ** 0.5
            raw_avg.append((i, mean, std, len(vals)))

    # Лёгкое сглаживание (окно 5 bucket'ов) для avg и std
    SMOOTH = 5
    def _smooth(series):
        out = []
        for i in range(len(series)):
            lo = max(0, i - SMOOTH // 2)
            hi = min(len(series), i + SMOOTH // 2 + 1)
            out.append(sum(series[lo:hi]) / (hi - lo))
        return out
    smoothed = _smooth([v[1] for v in raw_avg])
    smoothed_std = _smooth([v[2] for v in raw_avg])

    # max_trading_days теперь = число bucket'ов (для X-axis)
    max_trading_days = N_BUCKETS

    # Bucket → месяц: bucket i представляет долю i/(N-1) года.
    # Распределяем 12 месяцев пропорционально по N_BUCKETS.
    def _bucket_to_month(bucket_idx: int) -> int:
        return max(1, min(12, int(bucket_idx * 12 / N_BUCKETS) + 1))

    average_series = []
    for i, (td_idx, _, _, count) in enumerate(raw_avg):
        month = _bucket_to_month(td_idx)
        average_series.append({
            "td": td_idx,
            "month": month,
            "avg_pct": round(smoothed[i], 2),
            "std_pct": round(smoothed_std[i], 2),
            "years_count": count,
        })

    max_hist_year = current_year - 1
    years_range = f"{min_year}-{max_hist_year}" if min_year else ""

    # Обрезаем max_trading_days до того места, где реально есть данные avg
    # (после MIN_YEARS-фильтра хвост отсекся). Так грей-линия заполняет всю
    # ширину графика вплоть до декабря, а не обрывается на 80%.
    effective_max_td = average_series[-1]["td"] + 1 if average_series else max_trading_days

    return {
        "average": average_series,
        "current": current_series,
        "years_range": years_range,
        "current_year": current_year,
        "max_trading_days": effective_max_td,
    }


@router.get("")
async def get_seasonality(
    secid: str = Query(..., description="Тикер акции"),
    mode: str = Query("weekday", description="Режим: intraday, weekday, monthday, monthly"),
    iterations: int = Query(90, ge=1, le=9999, description="Кол-во последних итераций"),
    exclude_dividends: bool = Query(False, description="Убрать дивидендные гэпы"),
    since_year: int | None = Query(None, description="Учитывать годы ≥ since_year (для monthly)"),
    exclude_years: str = Query("", description="Исключить года через запятую (для monthly)"),
    user=Depends(get_current_user_optional),
):
    if mode not in ("intraday", "weekday", "monthday", "monthly"):
        raise HTTPException(400, "mode must be one of: intraday, weekday, monthday, monthly")

    # Парсинг исключённых годов
    excl_list: list[int] = []
    if exclude_years:
        try:
            excl_list = [int(y.strip()) for y in exclude_years.split(",") if y.strip()]
        except ValueError:
            raise HTTPException(400, "exclude_years must be comma-separated integers")

    # Гостевые ограничения
    if mode == "intraday":
        enforce_guest_limits(user, interval=60)

    cache_key = f"seasonality:{secid}:{mode}:iter{iterations}:nodiv{exclude_dividends}:sy{since_year}:ex{','.join(map(str,sorted(excl_list)))}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    start_time = time.time()
    log.info(f"REQUEST: /seasonality secid={secid} mode={mode} iter={iterations} excl_div={exclude_dividends}")

    engine = get_engine()

    source, inst_type = _resolve_source(secid)

    if mode == "intraday":
        # Intraday: index_data не поддерживает
        if source == "index_data":
            raise HTTPException(status_code=404, detail=f"Нет интрадей данных для {secid}")

        intra_type_filter = f"type = '{inst_type}'" if inst_type else "TRUE"
        # Intraday: open-to-close per hour, дивиденды не влияют
        query = text(f"""
            WITH recent_days AS (
                SELECT DISTINCT begin_time::date AS trade_date
                FROM candles
                WHERE secid = :secid AND interval = 60 AND {intra_type_filter} AND open > 0
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
              AND {intra_type_filter}
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
        # Дивиденды только для акций
        _, src_type = _resolve_source(secid)
        ex_dates: dict[str, float] = {}
        if exclude_dividends and src_type == "stock":
            ex_dates = _get_ex_dates_from_db(engine, secid)
            if ex_dates:
                log.info(f"  Found {len(ex_dates)} ex-dividend dates for {secid} from DB")

        bars = _compute_seasonality_daily(
            engine, secid, mode, iterations, ex_dates,
            since_year=since_year, exclude_years=excl_list,
        )

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

    source, inst_type = _resolve_source(secid)

    with engine.connect() as conn:
        if source == "index_data":
            rows = conn.execute(text("""
                SELECT trade_date, close
                FROM index_data
                WHERE secid = :secid AND close > 0 AND trade_date >= :date_from
                ORDER BY trade_date
            """), {"secid": secid, "date_from": date_from.isoformat()}).fetchall()
        else:
            price_type_filter = f"type = '{inst_type}'" if inst_type else "TRUE"
            rows = conn.execute(text(f"""
                SELECT begin_time::date as trade_date, close
                FROM candles
                WHERE secid = :secid
                  AND interval = 24
                  AND {price_type_filter}
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

    # Adjusted close: мультипликативная коррекция (стандарт Yahoo/CRSP)
    # Используем RAW prev_close для расчёта factor, не adjusted
    adjusted = closes.copy()
    if ex_dates:
        relevant_divs = []
        for ex_str, div_val in ex_dates.items():
            ex_d = date_type.fromisoformat(ex_str)
            if date_from <= ex_d <= date_type.today():
                relevant_divs.append((ex_d, div_val))
        relevant_divs.sort()  # от старых к новым

        for ex_d, div_val in relevant_divs:
            ex_idx = None
            for i, d in enumerate(dates):
                if d >= ex_d:
                    ex_idx = i
                    break
            if ex_idx is not None and ex_idx > 0:
                # factor на основе RAW close дня перед экс-датой
                raw_prev_close = closes[ex_idx - 1]
                if raw_prev_close > 0 and div_val < raw_prev_close:
                    factor = (raw_prev_close - div_val) / raw_prev_close
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

    # Дивиденды для отображения на графике
    dividends_list = []
    for ex_str, div_val in ex_dates.items():
        ex_d = date_type.fromisoformat(ex_str)
        if date_from <= ex_d <= date_type.today():
            dividends_list.append({"date": ex_str, "value": round(div_val, 2)})
    dividends_list.sort(key=lambda x: x["date"])

    result = {
        "secid": secid,
        "days": days,
        "ex_dates_count": len(ex_dates),
        "dividends": dividends_list,
        "data": data,
    }
    get_or_set(cache_key, result, ttl=300)
    return result


@router.get("/available-years")
async def get_available_years(
    secid: str = Query(..., description="Тикер"),
):
    """
    Возвращает диапазон и список доступных лет для тикера.
    Используется фронтом для заполнения dropdown «Сравнить с годом».
    """
    cache_key = f"seasonality_years:{secid}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    engine = get_engine()
    source, inst_type = _resolve_source(secid)

    with engine.connect() as conn:
        if source == "index_data":
            rows = conn.execute(text("""
                SELECT DISTINCT EXTRACT(YEAR FROM trade_date)::int as y
                FROM index_data WHERE secid = :secid AND close > 0
                ORDER BY y
            """), {"secid": secid}).fetchall()
        else:
            type_filter = f"type = '{inst_type}'" if inst_type else "TRUE"
            rows = conn.execute(text(f"""
                SELECT DISTINCT EXTRACT(YEAR FROM begin_time)::int as y
                FROM candles
                WHERE secid = :secid AND interval = 24 AND {type_filter} AND close > 0
                ORDER BY y
            """), {"secid": secid}).fetchall()

    years = [int(r[0]) for r in rows]
    if not years:
        raise HTTPException(404, f"Нет данных для {secid}")

    result = {
        "secid": secid,
        "min_year": years[0],
        "max_year": years[-1],
        "years": years,
    }
    get_or_set(cache_key, result, ttl=3600)  # годы меняются медленно — 1 час
    return result


@router.get("/yearly")
async def get_yearly_seasonality(
    secid: str = Query(..., description="Тикер"),
    exclude_dividends: bool = Query(False, description="Убрать дивидендные гэпы"),
    since_year: int | None = Query(None, description="Учитывать годы ≥ since_year"),
    exclude_years: str = Query("", description="Исключить года (через запятую), напр. '2008,2014,2020,2022'"),
    user=Depends(get_current_user_optional),
):
    """
    Годовая сезонность: среднее кумулятивное изменение цены с начала года.
    Две серии: average (calendar-aligned resample) и current (текущий год).

    Параметры фильтрации:
    - since_year: брать только годы ≥ этого значения (для «с 2015 г.»)
    - exclude_years: список годов для исключения (для «без выбросов»)
    """
    # Парсинг списка исключённых годов
    excl_list: list[int] = []
    if exclude_years:
        try:
            excl_list = [int(y.strip()) for y in exclude_years.split(",") if y.strip()]
        except ValueError:
            raise HTTPException(400, "exclude_years must be comma-separated integers")

    cache_key = f"seasonality_yearly:{secid}:nodiv{exclude_dividends}:sy{since_year}:ex{','.join(map(str,sorted(excl_list)))}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    engine = get_engine()
    start_time = time.time()

    source, inst_type = _resolve_source(secid)
    ex_dates: dict[str, float] = {}
    if exclude_dividends and inst_type == "stock":
        ex_dates = _get_ex_dates_from_db(engine, secid)

    data = _compute_yearly_seasonality(engine, secid, ex_dates,
                                       since_year=since_year, exclude_years=excl_list)
    if not data:
        raise HTTPException(404, f"Нет данных для {secid}")

    duration = time.time() - start_time
    avg_len = len(data.get("average", []))
    cur_len = len(data.get("current", []))
    log.info(f"GET /seasonality/yearly {secid} sy={since_year} ex={excl_list} avg={avg_len} cur={cur_len} {duration:.2f}s")

    result = {
        "secid": secid,
        "exclude_dividends": exclude_dividends,
        "since_year": since_year,
        "excluded_years": excl_list,
        **data,
    }
    get_or_set(cache_key, result, ttl=300)
    return result
