"""
Ядро форвард-теста сессионных троек.

Одна и та же функция строит сетку и на исторических данных (замороженный
baseline), и на форвардных. Это принципиально: если бы forward-код отличался
от baseline-кода, сравнение было бы бессмысленным.

Данных не собирает ничего нового — 5-минутки уже лежат в проде в `candles`.
Форвард здесь означает не «новый сбор», а «жёстко зафиксированная граница
даты, после которой результат в отбор часов не входил».
"""
from __future__ import annotations
import datetime
import json
import pathlib
import subprocess

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
CACHE = ROOT / "data"
SPEC = json.loads((ROOT / "spec.json").read_text(encoding="utf-8"))

SSH = [
    "ssh", "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none",
    "-o", "ConnectTimeout=30", "-i", str(pathlib.Path.home() / ".ssh/id_ed25519"),
    "root@103.88.243.232",
]


# ─────────────────────────────────────────────────────────────────────────
# 1. ДАННЫЕ
# ─────────────────────────────────────────────────────────────────────────
def fetch(sectype: str, since: str, until: str) -> pd.DataFrame:
    """Непрерывный front-month фьючерс, 5-мин.

    ROW_NUMBER по lsttrade берёт на каждый момент ближайший НЕ истёкший
    контракт. Условие `lsttrade >= begin_time::date` обязательно — без него
    просачиваются уже экспирировавшие контракты.
    """
    sql = f"""
    WITH ranked AS (
      SELECT c.begin_time, c.open, c.high, c.low, c.close, c.volume,
             ROW_NUMBER() OVER (PARTITION BY c.begin_time ORDER BY f.lsttrade ASC) AS rn
      FROM candles c JOIN futures_contracts f ON c.secid = f.secid
      WHERE f.sectype = '{sectype}' AND c.interval = 5 AND c.type = 'futures'
        AND f.lsttrade >= c.begin_time::date
        AND c.begin_time >= '{since}' AND c.begin_time < '{until}'
    )
    SELECT begin_time, open, high, low, close, volume
    FROM ranked WHERE rn = 1 ORDER BY begin_time;
    """
    cmd = SSH + [
        "docker exec frame-db-1 psql -U postgres -d moex_db -t -A -F',' -c "
        + json.dumps(" ".join(sql.split()))
    ]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=900, stdin=subprocess.DEVNULL)
    if out.returncode != 0:
        raise RuntimeError(f"{sectype}: {out.stderr[:400]}")
    rows = [r for r in out.stdout.strip().split("\n") if r.strip()]
    df = pd.DataFrame([r.split(",") for r in rows],
                      columns=["ts", "o", "hi", "lo", "c", "vol"])
    df["ts"] = pd.to_datetime(df["ts"])
    for col in ["o", "hi", "lo", "c", "vol"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["c"])


def cached(sectype: str, since: str, until: str) -> pd.DataFrame:
    CACHE.mkdir(exist_ok=True)
    f = CACHE / f"{sectype}_{since}_{until}.csv"
    if f.exists():
        df = pd.read_csv(f)
    else:
        df = fetch(sectype, since, until)
        df.to_csv(f, index=False)
    # у пустого файла pandas даёт object-колонку — .dt тогда падает
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    return df.dropna(subset=["ts"])


# ─────────────────────────────────────────────────────────────────────────
# 2. ПОДГОТОВКА: 5-минутки -> цены на КОНЦАХ получасовых слотов
# ─────────────────────────────────────────────────────────────────────────
def slot_prices(df: pd.DataFrame, min_coverage: float = 0.6,
                slots: list[str] | None = None) -> pd.DataFrame:
    """Строки = торговые дни, столбцы = получасовые слоты.

    Значение — close ПОСЛЕДНЕЙ 5-минутки внутри слота, то есть цена в КОНЦЕ
    слота. Ловушка, стоившая 3-56% эджа: Pine мерил на НАЧАЛЕ слота, Python
    на конце; расхождение 25 минут. Метка слота 19:30 = сделка в 20:00.

    ⚠️ Набор живых слотов ЗАВИСИТ ОТ ПЕРИОДА (биржа меняет расписание, в
    коротком окне покрытие другое). Если считать его заново на форварде,
    сетка не совпадёт с baseline. Поэтому на форварде slots передаётся
    явно — тот же список, что был заморожен.
    """
    if df.empty:
        return pd.DataFrame()
    g = df.copy()
    g["date"] = pd.to_datetime(g["ts"].dt.date)
    g["slot"] = g["ts"].dt.floor("30min").dt.time
    px = g.groupby(["date", "slot"])["c"].last().unstack().sort_index()
    # ⚠️ MOEX торгует и по ВЫХОДНЫМ (с 27.04.2024). Оборот там 15% от буднего,
    #    TradingView эти сессии не показывает, и «следующий торговый день»
    #    после пятницы превращался в субботу. Выбрасываем — см. ловушку 9.
    if SPEC.get("weekdays_only", True):
        px = px[px.index.dayofweek < 5]
    if slots is not None:
        want = [datetime.time.fromisoformat(s) for s in sorted(slots)]
        return px.reindex(columns=want)
    live = [s for s, v in px.notna().mean().items() if v >= min_coverage]
    return px[sorted(live)]


# ─────────────────────────────────────────────────────────────────────────
# 3. СЕТКА: все тройки (A, B, C) x направление
# ─────────────────────────────────────────────────────────────────────────
def build_grid(px: pd.DataFrame, threshold: float, max_gap_days: int,
               min_trades: int = 0):
    """Возвращает (labels, R) где R — матрица (дни x конфигурации) ВАЛОВЫХ
    доходностей; 0 в дни без сигнала.

    A и B — в один день, A раньше B. C — на следующий торговый день.

    ⚠️ GAP-SAFE: следующая строка таблицы — не всегда следующий календарный
    день. Праздники и пропуски дают «ночную» доходность, которой не было.
    Разрыв больше max_gap_days -> сделка выбрасывается.
    """
    slots = list(px.columns)
    n = len(slots)
    days = px.index
    if len(days) < 2 or n < 2:          # окно ещё пустое или в один день
        return [], np.zeros((len(days), 0))
    P = px.values.astype(float)

    gap = np.append((days[1:] - days[:-1]).days.values, 10 ** 9)
    P_next = np.roll(P, -1, axis=0).astype(float)
    P_next[gap > max_gap_days] = np.nan
    P_next[-1] = np.nan

    cols, labels = [], []
    for a in range(n):
        for b in range(a + 1, n):
            move = P[:, b] / P[:, a] - 1
            raw = np.nan_to_num(np.where(move >= threshold, 1.0,
                                         np.where(move <= -threshold, -1.0, 0.0)))
            gross = P_next / P[:, b][:, None] - 1
            for sgn, dname in ((1, "cont"), (-1, "rev")):
                sig = sgn * raw
                r = sig[:, None] * gross
                r = np.where((sig[:, None] != 0) & ~np.isnan(r), r, 0.0)
                for c in range(n):
                    v = r[:, c]
                    if (v != 0).sum() < min_trades:
                        continue
                    cols.append(v)
                    labels.append((str(slots[a]), str(slots[b]), str(slots[c]), dname))
    if not cols:
        return [], np.zeros((len(days), 0))
    return labels, np.column_stack(cols)


def per_config_stats(R: np.ndarray, cost: float) -> pd.DataFrame:
    """n сделок, средняя ЧИСТАЯ доходность, Шарп на сделку, суммарно."""
    traded = R != 0
    n = traded.sum(axis=0)
    net = np.where(traded, R - cost, 0.0)
    with np.errstate(invalid="ignore", divide="ignore"):
        s = net.sum(axis=0)
        mean = np.where(n > 0, s / np.maximum(n, 1), np.nan)
        sq = np.where(traded, (net - mean) ** 2, 0.0).sum(axis=0)
        sd = np.sqrt(np.where(n > 1, sq / np.maximum(n - 1, 1), np.nan))
        sr = np.where(sd > 0, mean / sd, np.nan)
    return pd.DataFrame({"n": n, "mean": mean, "sr": sr, "total": s})


def grid_frame(sectype: str, since: str, until: str, min_trades: int = 0,
               slots: list[str] | None = None) -> pd.DataFrame:
    """Полный путь данные -> статистика по каждой конфигурации сетки."""
    px = slot_prices(cached(sectype, since, until), SPEC["min_slot_coverage"], slots)
    labels, R = build_grid(px, SPEC["signal_threshold"], SPEC["max_gap_days"], min_trades)
    if not labels:
        return pd.DataFrame()
    st = per_config_stats(R, SPEC["primary_cost"])
    lab = pd.DataFrame(labels, columns=["A", "B", "C", "dir"])
    out = pd.concat([lab, st], axis=1)
    out.insert(0, "sectype", sectype)
    out["key"] = out.sectype + "|" + out.A + "|" + out.B + "|" + out.C + "|" + out["dir"]
    return out
