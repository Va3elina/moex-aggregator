"""
«Перекраска» — экспериментальный admin-only индикатор (идея Вадима, 2026-08-30).

Сколько % от free float бумаги сменило руки за последний месяц: дельта-объём
(CDV) считается аппроксимацией по 4Ч-свечам (как CDV в TradingView — из OHLCV,
биржевого разреза buy/sell в БД нет), его изменение за месяц соотносится с
количеством акций в свободном обращении.

Вторая метрика — отклонение текущего CDV от его среднего за месяц, тоже в %
от free float: насколько напокупали/напродавали относительно накопленной базы
(оценка спекулятивного спроса).

Аппроксимация дельты одной свечи (стандартная формула CDV из OHLCV):
    tw = high - max(open, close)      # верхняя тень
    bw = min(open, close) - low       # нижняя тень
    body = |close - open|
    delta = sign(close - open) * body / (tw + bw + body) * volume
Формула инвариантна к масштабу цены, поэтому сплиты влияют только на volume.

Free float в акциях = ffcap (freefloat_cap, помесячно) / дневной close на
дату среза as_of. Объём свечей MOEX — в штуках (проверено против ISS value/close
2026-08-30), так что деление даёт сопоставимые единицы.

Всё под require_admin: индикатор экспериментальный, наружу не торчит.
"""
import math
from bisect import bisect_right
from collections import defaultdict
from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.logger import get_logger
from api.models import User
from api.routers.auth import require_admin
from api.routers.breadth import KNOWN_SPLITS

log = get_logger()

router = APIRouter(prefix="/api/admin/repaint", tags=["repaint"])

WINDOW_DAYS = 30   # «месяц» обеих метрик
WARMUP_DAYS = 40   # запас истории слева, чтобы окно было полным с первой точки
MAX_DAYS = 1830

# SQL-выражение дельты свечи — то же, что _candle_delta, для агрегатов.
_DELTA_SQL = """
    CASE WHEN volume > 0
              AND (high - GREATEST(open, close))
                + (LEAST(open, close) - low)
                + ABS(close - open) > 0
         THEN SIGN(close - open) * ABS(close - open)
              / ((high - GREATEST(open, close))
                 + (LEAST(open, close) - low)
                 + ABS(close - open))
              * volume
         ELSE 0 END
"""


def _candle_delta(o: float, h: float, l: float, c: float, v: float) -> float:
    tw = h - max(o, c)
    bw = min(o, c) - l
    body = abs(c - o)
    total = tw + bw + body
    if total <= 0 or v <= 0:
        return 0.0
    return math.copysign(body / total * v, c - o)


def _split_volume_ratio(sec_id: str, day: date) -> float:
    """Множитель объёма для дат ДО сплита: 1 старая акция = ratio новых."""
    split = KNOWN_SPLITS.get(sec_id)
    if split and day < split[0]:
        return split[1]
    return 1.0


def _ff_shares_by_month(db: Session, sec_ids: list[str]) -> dict[str, list[tuple[date, float]]]:
    """{sec_id: [(month, ff_акций), ...]} по возрастанию месяца."""
    rows = db.execute(text("""
        SELECT f.sec_id, f.month, f.ffcap, c.close
        FROM freefloat_cap f
        JOIN candles c ON c.sec_id = f.sec_id AND c.type = 'stock'
                      AND c.interval = 24 AND c.begin_time::date = f.as_of
        WHERE f.sec_id = ANY(:ids) AND f.ffcap > 0 AND c.close > 0
        ORDER BY f.sec_id, f.month
    """), {"ids": sec_ids}).fetchall()
    out: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for sec_id, month, ffcap, close in rows:
        out[sec_id].append((month, float(ffcap) / float(close)))
    return out


def _ff_at(ff_list: list[tuple[date, float]], day: date) -> tuple[date, float] | None:
    """Последний месячный срез free float, не позже дня точки."""
    if not ff_list:
        return None
    months = [m for m, _ in ff_list]
    i = bisect_right(months, day) - 1
    if i < 0:
        i = 0  # история точки старше первого среза — берём первый, лучше чем ничего
    return ff_list[i]


def _rolling_metrics(times: list[date], cdv: list[float],
                     ff_list: list[tuple[date, float]]):
    """repaint_pct / dev_pct для каждой точки. None пока окно неполное."""
    n = len(cdv)
    repaint = [None] * n
    dev = [None] * n
    prefix = [0.0]
    for v in cdv:
        prefix.append(prefix[-1] + v)
    j = 0  # первый индекс внутри окна
    for i in range(n):
        lo = times[i] - timedelta(days=WINDOW_DAYS)
        while times[j] <= lo:
            j += 1
        # Окно неполное, если история не дотягивается до его левого края.
        if times[0] > lo:
            continue
        ff = _ff_at(ff_list, times[i])
        if not ff or ff[1] <= 0:
            continue
        base = cdv[j - 1] if j > 0 else 0.0
        mean = (prefix[i + 1] - prefix[j]) / (i - j + 1)
        repaint[i] = (cdv[i] - base) / ff[1] * 100.0
        dev[i] = (cdv[i] - mean) / ff[1] * 100.0
    return repaint, dev


@router.get("/screener")
def repaint_screener(
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Текущие метрики перекраски по всем акциям с часовыми свечами и free float."""
    since = date.today() - timedelta(days=WINDOW_DAYS + WARMUP_DAYS)
    # Дневная дельта из часовых свечей — на порядок точнее дневной свечи и
    # на два порядка меньше строк, чем сырые часовики.
    rows = db.execute(text(f"""
        SELECT sec_id, begin_time::date AS d,
               SUM({_DELTA_SQL}) AS delta,
               (ARRAY_AGG(close ORDER BY begin_time DESC))[1] AS px
        FROM candles
        WHERE type = 'stock' AND interval = 60 AND begin_time >= :since
        GROUP BY sec_id, begin_time::date
        ORDER BY sec_id, d
    """), {"since": since}).fetchall()

    by_sec: dict[str, list[tuple[date, float, float]]] = defaultdict(list)
    for sec_id, d, delta, px in rows:
        by_sec[sec_id].append((d, float(delta or 0), float(px or 0)))

    names = dict(db.execute(text(
        "SELECT sec_id, name FROM instruments WHERE type = 'stock'"
    )).fetchall())
    ff_map = _ff_shares_by_month(db, list(by_sec.keys()))

    out = []
    for sec_id, days_rows in by_sec.items():
        ff_list = ff_map.get(sec_id)
        if not ff_list or len(days_rows) < 5:
            continue
        times = [d for d, _, _ in days_rows]
        cdv, acc = [], 0.0
        for d, delta, _ in days_rows:
            acc += delta * _split_volume_ratio(sec_id, d)
            cdv.append(acc)
        repaint, dev = _rolling_metrics(times, cdv, ff_list)
        if repaint[-1] is None:
            continue
        out.append({
            "sec_id": sec_id,
            "name": names.get(sec_id, sec_id),
            "repaint_pct": round(repaint[-1], 2),
            "dev_pct": round(dev[-1], 2),
            "close": days_rows[-1][2],
            "ff_shares": ff_list[-1][1],
        })
    out.sort(key=lambda r: abs(r["repaint_pct"]), reverse=True)
    return {"window_days": WINDOW_DAYS, "rows": out}


@router.get("/series/{sec_id}")
def repaint_series(
    sec_id: str,
    days: int = Query(365, ge=60, le=MAX_DAYS),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """4Ч-ряд: цена + CDV + обе метрики перекраски по одной акции."""
    sec_id = sec_id.upper()
    since = date.today() - timedelta(days=days + WARMUP_DAYS)
    rows = db.execute(text("""
        SELECT begin_time, open, high, low, close, volume
        FROM candles
        WHERE sec_id = :s AND type = 'stock' AND interval = 60
          AND begin_time >= :since
        ORDER BY begin_time
    """), {"s": sec_id, "since": since}).fetchall()
    if not rows:
        raise HTTPException(404, f"Нет часовых свечей по {sec_id}")

    ff_list = _ff_shares_by_month(db, [sec_id]).get(sec_id)
    if not ff_list:
        raise HTTPException(404, f"Нет данных free float по {sec_id}")

    # Часовые дельты → 4Ч-бакеты (ключ: день + номер четырёхчасовки).
    buckets: dict[tuple[date, int], dict] = {}
    for bt, o, h, l, c, v in rows:
        o, h, l, c, v = (float(o or 0), float(h or 0), float(l or 0),
                         float(c or 0), float(v or 0))
        if h <= 0 or v < 0:
            continue
        ratio = _split_volume_ratio(sec_id, bt.date())
        # Дельта инвариантна к масштабу цены — сплит корректирует только объём.
        delta = _candle_delta(o, h, l, c, v) * ratio
        o, h, l, c = o / ratio, h / ratio, l / ratio, c / ratio
        key = (bt.date(), bt.hour // 4)
        b = buckets.get(key)
        if b is None:
            buckets[key] = {"time": bt, "open": o, "high": h, "low": l,
                            "close": c, "volume": v * ratio, "delta": delta}
        else:
            b["high"] = max(b["high"], h)
            b["low"] = min(b["low"], l)
            b["close"] = c
            b["volume"] += v * ratio
            b["delta"] += delta

    pts = [buckets[k] for k in sorted(buckets)]
    acc = 0.0
    for p in pts:
        acc += p["delta"]
        p["cdv"] = acc

    times = [p["time"].date() for p in pts]
    cdv = [p["cdv"] for p in pts]
    repaint, dev = _rolling_metrics(times, cdv, ff_list)

    cut = date.today() - timedelta(days=days)
    points = [{
        "time": p["time"].isoformat(),
        "open": round(p["open"], 6), "high": round(p["high"], 6),
        "low": round(p["low"], 6), "close": round(p["close"], 6),
        "volume": p["volume"],
        "delta": round(p["delta"], 2),
        "cdv": round(p["cdv"], 2),
        "repaint_pct": None if r is None else round(r, 3),
        "dev_pct": None if dv is None else round(dv, 3),
    } for p, r, dv in zip(pts, repaint, dev) if p["time"].date() >= cut]
    if not points:
        raise HTTPException(404, f"Нет данных за период по {sec_id}")

    name = db.execute(text(
        "SELECT name FROM instruments WHERE sec_id = :s AND type = 'stock' LIMIT 1"
    ), {"s": sec_id}).scalar()
    ff_month, ff_shares = _ff_at(ff_list, times[-1])
    last = points[-1]
    return {
        "sec_id": sec_id,
        "name": name or sec_id,
        "window_days": WINDOW_DAYS,
        "ff_shares": ff_shares,
        "ff_month": ff_month.isoformat(),
        "summary": {
            "repaint_pct": last["repaint_pct"],
            "dev_pct": last["dev_pct"],
            "cdv": last["cdv"],
            "close": last["close"],
        },
        "points": points,
    }
