"""
«Перекраска» — экспериментальный admin-only индикатор (идея Вадима, 2026-08-30).

Сколько % от free float бумаги сменило руки за последний месяц: дельта-объём
(CDV) считается аппроксимацией по свечам выбранного ТФ 1ч/4ч/1д/1н (как CDV
в TradingView — из OHLCV, биржевого разреза buy/sell в БД нет), его изменение
за месяц соотносится с количеством акций в свободном обращении.

Вторая метрика — отклонение текущего CDV от его среднего за месяц, тоже в %
от free float: насколько напокупали/напродавали относительно накопленной базы
(оценка спекулятивного спроса).

Аппроксимация дельты одной свечи — формула скрипта CDV из TradingView
(LonesomeTheBlue, сверено по исходнику 2026-09-24):
    tw = high - max(open, close)      # верхняя тень
    bw = min(open, close) - low       # нижняя тень
    body = |close - open|
    rate = (body + 0.5 * (tw + bw)) / (tw + bw + body)   # 0.5, если всё ноль
    delta = +volume * rate, если close >= open, иначе -volume * rate
То есть тело свечи целиком идёт в сторону закрытия, тени делятся пополам:
по модулю дельта всегда не меньше половины объёма (дожи = +0.5 объёма).
Прежняя формула body / (tw + bw + body) давала вдвое-втрое меньшие
размахи CDV и не сходилась с TradingView (SVCB: −1.4 млрд против −2.4).
Формула инвариантна к масштабу цены, поэтому сплиты влияют только на volume.

Как в TradingView, дельта считается по самой свече ТФ, а не суммой дельт
мелких свечей: CDV меняется вместе с ТФ, на крупном ТФ оценка грубее.
Свечи ТФ: 1ч/4ч собираются из часовиков, 1д/1н — из дневных свечей.
Часовики и 5-минутки акций в candles с весны 2026 недописаны (объём часовиков
за сентябрь 2026 — ~5% дневного, замер 2026-09-12), дневные свечи полные:
на 1ч/4ч свежий CDV занижен, пока не починен фетчер.

Free float в акциях = ffcap (freefloat_cap, помесячно) / дневной close на
дату среза as_of. Объём свечей MOEX — в штуках (проверено против ISS value/close
2026-08-30), так что деление даёт сопоставимые единицы.

Свечи читаем по колонке secid, не sec_id: у части акций дневки до марта
2025 лежат без sec_id (OZON терял 4 года истории). История переехавших бумаг
(YDEX ← YNDX, X5 ← FIVE, RAGR ← AGRO…) склеивается по securities_ref
(canonical_isin), как в /fund-trades/price-weekly: цены старой серии делятся
на коэффициент обмена, объём умножается (REDOMICILE_RATIO там же).

Доступ: админы и email из REPAINT_EARLY_ACCESS (api/services/early_access.py) —
индикатор экспериментальный, наружу не торчит.
"""
from bisect import bisect_right
from collections import defaultdict
from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.logger import get_logger
from api.models import User
from api.services.early_access import require_feature
from api.routers.fund_trades import REDOMICILE_RATIO
from api.services.splits import price_divisor, volume_multiplier

log = get_logger()

router = APIRouter(prefix="/api/admin/repaint", tags=["repaint"])

WINDOW_DAYS = 30   # «месяц» обеих метрик
WARMUP_DAYS = 40   # запас истории слева, чтобы окно было полным с первой точки
MAX_DAYS = 9000    # «Всё»: дневные свечи акций лежат с 2007 года
# Скринеру нужна только последняя точка: окно + запас на недельную свечу и
# выходные. Прогрев как у ряда тянул бы вдвое больше сырых часовиков.
SCREENER_LOOKBACK_DAYS = WINDOW_DAYS + 14

_TF_PATTERN = "^(1h|4h|1d|1w)$"

# Источник свечей ТФ (interval в candles): внутридневные ТФ — из часовиков,
# дневной и недельный — из дневных свечей, как у биржи и в TradingView.
_TF_SOURCE = {"1h": 60, "4h": 60, "1d": 24, "1w": 24}

# Ключ свечи ТФ по begin_time свечи источника. Внутридневные ТФ отдают время
# свечи, дневной и недельный — только дату: время в тултипе дневной точки
# читалось бы как «на этот час».
_TF_BUCKET = {
    "1h": lambda bt: (bt.date(), bt.hour),
    "4h": lambda bt: (bt.date(), bt.hour // 4),
    "1d": lambda bt: (bt.date(),),
    "1w": lambda bt: tuple(bt.date().isocalendar())[:2],
}
_TF_INTRADAY = ("1h", "4h")


def _candle_delta(o: float, h: float, l: float, c: float, v: float) -> float:
    if v <= 0:
        return 0.0
    tw = h - max(o, c)
    bw = min(o, c) - l
    body = abs(c - o)
    total = tw + bw + body
    rate = 0.5 if total <= 0 else (body + 0.5 * (tw + bw)) / total
    return v * rate if c >= o else -v * rate


# Сплиты — общий реестр stock_splits (api/services/splits.py): цена делится
# только там, где свечи сырые; объём умножается всегда (биржа его не
# пересчитывает, у T до 2026-04-17 объём в старых акциях, в 10 раз меньше).


def _tf_bars(sec_id: str, rows, tf: str) -> list[dict]:
    """Свечи источника (по возрастанию времени) → свечи ТФ с дельтой и CDV.

    Цены до даты сплита делятся на ratio, объём умножается — свеча ТФ,
    накрывающая сплит, остаётся в одном масштабе."""
    bucket_of = _TF_BUCKET[tf]
    buckets: dict[tuple, dict] = {}
    for bt, o, h, l, c, v in rows:
        o, h, l, c, v = (float(o or 0), float(h or 0), float(l or 0),
                         float(c or 0), float(v or 0))
        if h <= 0 or v < 0:
            continue
        pr = price_divisor(sec_id, bt.date())
        o, h, l, c = o / pr, h / pr, l / pr, c / pr
        v *= volume_multiplier(sec_id, bt.date())
        key = bucket_of(bt)
        b = buckets.get(key)
        if b is None:
            buckets[key] = {"time": bt, "open": o, "high": h, "low": l,
                            "close": c, "volume": v}
        else:
            b["high"] = max(b["high"], h)
            b["low"] = min(b["low"], l)
            b["close"] = c
            b["volume"] += v

    bars = [buckets[k] for k in sorted(buckets)]
    acc = 0.0
    for b in bars:
        b["delta"] = _candle_delta(b["open"], b["high"], b["low"], b["close"], b["volume"])
        acc += b["delta"]
        b["cdv"] = acc
    return bars


def _ff_shares_by_month(db: Session, sec_ids: list[str]) -> dict[str, list[tuple[date, float]]]:
    """{sec_id: [(month, ff_акций), ...]} по возрастанию месяца."""
    rows = db.execute(text("""
        SELECT f.sec_id, f.month, f.ffcap, c.close
        FROM freefloat_cap f
        JOIN candles c ON c.secid = f.sec_id AND c.type = 'stock' AND c.interval = 24
                      -- диапазон, а не begin_time::date = as_of: так идёт по индексу
                      AND c.begin_time >= f.as_of AND c.begin_time < f.as_of + 1
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
    """repaint_pct / dev_pct (% от FF) и dev_shares (штуки) для каждой точки.
    None пока окно неполное."""
    n = len(cdv)
    repaint = [None] * n
    dev = [None] * n
    dev_sh = [None] * n
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
        dev_sh[i] = cdv[i] - mean
        dev[i] = dev_sh[i] / ff[1] * 100.0
    return repaint, dev, dev_sh


@router.get("/screener")
def repaint_screener(
    tf: str = Query("4h", pattern=_TF_PATTERN),
    db: Session = Depends(get_db),
    _user: User = Depends(require_feature("repaint")),
):
    """Текущие метрики перекраски по всем акциям со свечами и free float — на свечах ТФ."""
    since = date.today() - timedelta(days=SCREENER_LOOKBACK_DAYS)
    rows = db.execute(text("""
        SELECT secid, begin_time, open, high, low, close, volume
        FROM candles
        WHERE type = 'stock' AND interval = :iv AND begin_time >= :since
          AND secid IN (SELECT DISTINCT sec_id FROM freefloat_cap)
        ORDER BY secid, begin_time
    """), {"iv": _TF_SOURCE[tf], "since": since}).fetchall()

    by_sec: dict[str, list] = defaultdict(list)
    for sec_id, *candle in rows:
        by_sec[sec_id].append(candle)

    names = dict(db.execute(text(
        "SELECT sec_id, name FROM instruments WHERE type = 'stock'"
    )).fetchall())
    ff_map = _ff_shares_by_month(db, list(by_sec.keys()))

    out = []
    for sec_id, candles in by_sec.items():
        ff_list = ff_map.get(sec_id)
        if not ff_list:
            continue
        bars = _tf_bars(sec_id, candles, tf)
        if len(bars) < 5:
            continue
        times = [b["time"].date() for b in bars]
        repaint, dev, _ = _rolling_metrics(times, [b["cdv"] for b in bars], ff_list)
        if repaint[-1] is None:
            continue
        out.append({
            "sec_id": sec_id,
            "name": names.get(sec_id, sec_id),
            "repaint_pct": round(repaint[-1], 2),
            "dev_pct": round(dev[-1], 2),
            "close": bars[-1]["close"],
            "ff_shares": ff_list[-1][1],
        })
    # Таблица и график показывают отклонение от среднего — сортируем по нему.
    out.sort(key=lambda r: abs(r["dev_pct"]), reverse=True)
    return {"window_days": WINDOW_DAYS, "tf": tf, "rows": out}


def _legacy_secids(db: Session, secid: str) -> list[str]:
    """Старые secid той же бумаги (расписка до редомициляции) по securities_ref."""
    return [r[0] for r in db.execute(text("""
        SELECT DISTINCT sr.secid
        FROM securities_ref sr
        WHERE sr.canonical_isin = (
                SELECT canonical_isin FROM securities_ref
                WHERE secid = :t AND canonical_isin IS NOT NULL LIMIT 1
              )
          AND sr.secid IS NOT NULL AND sr.secid <> :t
    """), {"t": secid}).all()]


def _load_candles(db: Session, secid: str, interval: int, since: date) -> list:
    """Свечи бумаги + история её предшественников до редомициляции.

    Старая серия (ГДР) приводится к масштабу новой акции: цена / k, объём × k,
    k = REDOMICILE_RATIO (1.0, если обмен 1:1). На стыке выигрывает новая
    бумага: строки старой с даты первой свечи новой отбрасываются."""
    q = text("""
        SELECT begin_time, open, high, low, close, volume
        FROM candles
        WHERE secid = :s AND type = 'stock' AND interval = :iv
          AND begin_time >= :since
        ORDER BY begin_time
    """)
    rows = [tuple(r) for r in db.execute(q, {"s": secid, "iv": interval, "since": since})]
    first_new = rows[0][0] if rows else None
    legacy: list = []
    for old in _legacy_secids(db, secid):
        k = REDOMICILE_RATIO.get(old, 1.0)
        for bt, o, h, l, c, v in db.execute(q, {"s": old, "iv": interval, "since": since}):
            if first_new is not None and bt >= first_new:
                continue
            legacy.append((bt, float(o or 0) / k, float(h or 0) / k, float(l or 0) / k,
                           float(c or 0) / k, float(v or 0) * k))
    if not legacy:
        return rows
    return sorted(legacy + rows, key=lambda r: r[0])


@router.get("/series/{sec_id}")
def repaint_series(
    sec_id: str,
    days: int = Query(365, ge=7, le=MAX_DAYS),
    tf: str = Query("4h", pattern=_TF_PATTERN),
    db: Session = Depends(get_db),
    _user: User = Depends(require_feature("repaint")),
):
    """Ряд выбранного ТФ: цена + CDV + обе метрики перекраски по одной акции."""
    sec_id = sec_id.upper()
    since = date.today() - timedelta(days=days + WARMUP_DAYS)
    rows = _load_candles(db, sec_id, _TF_SOURCE[tf], since)
    if not rows:
        raise HTTPException(404, f"Нет свечей по {sec_id}")

    ff_list = _ff_shares_by_month(db, [sec_id]).get(sec_id)
    if not ff_list:
        raise HTTPException(404, f"Нет данных free float по {sec_id}")

    pts = _tf_bars(sec_id, rows, tf)
    times = [p["time"].date() for p in pts]
    cdv = [p["cdv"] for p in pts]
    repaint, dev, dev_sh = _rolling_metrics(times, cdv, ff_list)

    cut = date.today() - timedelta(days=days)
    first = next((i for i, d in enumerate(times) if d >= cut), None)
    if first is None:
        raise HTTPException(404, f"Нет данных за период по {sec_id}")
    # CDV отсчитываем от начала выбранного периода: база всё равно условна, а так
    # уровень читается как «накоплено с начала периода». Метрики считаются по
    # разностям CDV — сдвиг на константу их не меняет.
    base = cdv[first - 1] if first > 0 else 0.0
    # CDV в % от free float: дельта каждой свечи делится на FF на её дату,
    # а не итог на текущий FF — помесячные ступеньки FF не ломают историю.
    cdv_ff = 0.0
    points = []
    for p, r, dv, ds in zip(pts[first:], repaint[first:], dev[first:], dev_sh[first:]):
        cdv_ff += p["delta"] / _ff_at(ff_list, p["time"].date())[1] * 100.0
        points.append({
            "time": (p["time"].isoformat() if tf in _TF_INTRADAY
                     else p["time"].date().isoformat()),
            "open": round(p["open"], 6), "high": round(p["high"], 6),
            "low": round(p["low"], 6), "close": round(p["close"], 6),
            "volume": p["volume"],
            "delta": round(p["delta"], 2),
            "cdv": round(p["cdv"] - base, 2),
            "cdv_ff_pct": round(cdv_ff, 4),
            "repaint_pct": None if r is None else round(r, 3),
            "dev_pct": None if dv is None else round(dv, 3),
            "dev_shares": None if ds is None else round(ds, 2),
        })

    name = db.execute(text(
        "SELECT name FROM instruments WHERE sec_id = :s AND type = 'stock' LIMIT 1"
    ), {"s": sec_id}).scalar()
    ff_month, ff_shares = _ff_at(ff_list, times[-1])
    last = points[-1]
    return {
        "sec_id": sec_id,
        "name": name or sec_id,
        "tf": tf,
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
