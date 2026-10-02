"""«Спокойный Kamaz» на монетах, отобранных правилом каждый день (десятка по обороту за 30 дней, торгуются ≥ 90 дней,
включая снятые позже с торгов — LUNA, FTT, MATIC, FTM и др.). Вход по монете — только в дни, когда она в десятке;
открытая кампания доводится до выхода. Счёт — 10 ячеек по $10 000 (капитал $100 000), кусок от ячейки фиксирован.
Уровни: как у DOGE; для монет спокойнее DOGE — уменьшаем пропорционально дневному размаху (не меньше ×0.4),
размах пересчитывается каждый день по 90 дням до него (без заглядывания вперёд) — так же, как будет считать бот.
Варианты: без фильтра / фильтр по биткоину; пропуск монеты после −25% за сутки; стоп 30% от первой покупки + пауза 24 ч
+ лесенка не больше текущего счёта; плечо x1 и x3.
Процессы — через spawn: при fork после чтения parquet дочерние процессы зависают (пул потоков pyarrow).
"""
from __future__ import annotations

import glob
import multiprocessing as mp
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))
from factory import market  # noqa: E402
from factory.funding import funding  # noqa: E402
import grid_engine as ge  # noqa: E402

END = pd.Timestamp("2026-09-27 11:00")
SKIP = dict(crash_skip=25.0)
PROT = dict(crash_skip=25.0, hard_stop=30.0, stop_cooldown_h=24, cap_to_equity=True)
VARIANTS = {"без фильтра x1": dict(btc_filter=False, L=1.0), "фильтр x1": dict(btc_filter=True, L=1.0),
            "фильтр + пропуск x1": dict(btc_filter=True, L=1.0, **SKIP), "фильтр + пропуск + стоп 30% x1": dict(btc_filter=True, L=1.0, **PROT),
            "без фильтра x3": dict(btc_filter=False, L=3.0), "фильтр x3": dict(btc_filter=True, L=3.0),
            "фильтр + пропуск x3": dict(btc_filter=True, L=3.0, **SKIP), "фильтр + пропуск + стоп 30% x3": dict(btc_filter=True, L=3.0, **PROT)}
if __import__("os").environ.get("UNIV_SET") == "extra":
    VARIANTS = {"без фильтра + пропуск x1": dict(btc_filter=False, L=1.0, **SKIP),
                "без фильтра + пропуск + не больше счёта x1": dict(btc_filter=False, L=1.0, cap_to_equity=True, **SKIP),
                "фильтр + пропуск x2": dict(btc_filter=True, L=2.0, **SKIP),
                "без фильтра + пропуск x2": dict(btc_filter=False, L=2.0, **SKIP)}
if __import__("os").environ.get("UNIV_SET") == "reality":
    BASE = dict(btc_filter=False, L=1.0, cap_to_equity=True, **SKIP)
    VARIANTS = {"база": BASE,
                "лимитки только при проходе на 0.1% + комиссии ×2": dict(BASE, through=0.1, maker=0.0004, taker=0.0011),
                "лимитки только при проходе на 0.2%": dict(BASE, through=0.2),
                "проскальзывание 0.1% на рыночных": dict(BASE, slip=0.1),
                **{f"вход наугад #{sd}": dict(BASE, random_entry=True, seed=sd) for sd in (1, 2, 3)},
                **{f"вход наугад, столько же сделок #{sd}": dict(BASE, random_entry=True, random_onsets=True, seed=sd) for sd in (1, 2, 3, 4, 5)}}
if __import__("os").environ.get("UNIV_SET") == "capital":
    VARIANTS = {"без фильтра + пропуск + не больше счёта x1": dict(btc_filter=False, L=1.0, cap_to_equity=True, **SKIP),
                "фильтр + пропуск + не больше счёта x1": dict(btc_filter=True, L=1.0, cap_to_equity=True, **SKIP)}
if __import__("os").environ.get("UNIV_SET") == "small":
    BASE = dict(btc_filter=False, L=1.0, cap_to_equity=True, **SKIP)
    VARIANTS = {"полная лесенка, 7 покупок": BASE,
                "5 покупок (до −6%)": dict(BASE, levels=ge.DOGE_LV[:4].copy()),
                "3 покупки (до −2.8%)": dict(BASE, levels=ge.DOGE_LV[:2].copy()),
                "7 равных покупок": dict(BASE, sizes=np.ones(7)),
                "полная лесенка x1.6 (одна монета на $100)": dict(BASE, L=1.63)}
if __import__("os").environ.get("UNIV_SET") == "spot":
    BASE = dict(btc_filter=False, L=1.0, cap_to_equity=True, **SKIP)
    VARIANTS = {"фьючерсы (комиссии 0.02/0.055%, фандинг)": BASE,
                "спот (комиссии 0.1/0.1%, без фандинга)": dict(BASE, maker=0.001, taker=0.001, funding_on=False),
                "спот, 7 равных покупок": dict(BASE, maker=0.001, taker=0.001, funding_on=False, sizes=np.ones(7))}
if __import__("os").environ.get("UNIV_SET") == "one":          # один рабочий вариант (для сравнения разных отборов монет, UNIV_FILE)
    VARIANTS = {"база": dict(btc_filter=False, L=1.0, cap_to_equity=True, **SKIP)}
if __import__("os").environ.get("UNIV_SET") == "placebo":
    BASE = dict(btc_filter=False, L=1.0, cap_to_equity=True, **SKIP)
    VARIANTS = {"база": BASE, **{f"вход наугад, столько же сделок #{sd}": dict(BASE, random_entry=True, random_onsets=True, seed=sd) for sd in range(1, 11)}}
_G: dict = {}


def glob_data():
    if not _G:
        _G["U"] = pd.read_parquet(__import__("os").environ.get("UNIV_FILE") or ROOT / "data/universe_daily_top10.parquet")  # UNIV_FILE — другой отбор
        btc = market.klines("BTCUSDT", "1", "2021-01-01", END).astype(float)
        _G["BTC_LO12"] = (btc.c <= btc.l.shift(30).rolling(720).min())
        dd = market.klines("DOGEUSDT", "D", "2021-01-01", END).astype(float)
        _G["DOGE_RNG"] = ((dd.h - dd.l) / dd.c).rolling(90, min_periods=20).median().shift(1)
    return _G


def load_1m(sym: str, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
    parts = []
    f = ROOT / f"data/klines/{sym}_1.parquet"
    if f.exists():
        parts.append(pd.read_parquet(f))
    for g in glob.glob(str(ROOT / f"data/klines_seg/{sym}_*.parquet")):
        parts.append(pd.read_parquet(g))
    if not parts:
        return pd.DataFrame()
    k = pd.concat(parts); k = k[~k.index.duplicated(keep="last")].sort_index().astype(float)
    return k[(k.index >= a) & (k.index <= b)]


class SegCoin(ge.Coin):
    """Та же логика сигналов, что в grid_engine.Coin, но из готового кадра минуток и с маской дней в десятке."""

    def __init__(self, sym, k_full: pd.DataFrame, a: pd.Timestamp, b: pd.Timestamp, allowed: np.ndarray):
        G = glob_data()
        self.sym = sym
        k = k_full
        self.r3 = ge.rsi3(k.c)
        self.hi60 = k.h.rolling(60).max().values; self.lo60 = k.l.rolling(60).min().values
        self.ch120 = ((k.c / k.c.shift(120) - 1) * 100).values
        self.ch1440 = ((k.c / k.c.shift(1440) - 1) * 100).values
        self.btc_lo12 = G["BTC_LO12"].reindex(k.index).fillna(False).values.astype(bool)
        self.btc_hi12 = np.zeros(len(k), bool)
        self.btc_bear = np.zeros(len(k), bool)
        sel = (k.index >= a) & (k.index <= b)
        self.k = k[sel]; self.sel = sel
        try:
            fr = funding(sym, a, b)
        except Exception:
            fr = pd.Series(dtype=float)
        fm = np.zeros(len(self.k))
        if len(fr):
            pos = np.searchsorted(self.k.index.values, fr.index.values); ok = pos < len(self.k); fm[pos[ok]] = fr.values[ok]
        self.fund = fm; self.fund_last = np.zeros(len(self.k))
        self.allowed = allowed
        rng = market.klines(sym, "D", a - pd.Timedelta(days=95), a - pd.Timedelta(days=1)).astype(float)
        self.daily_range = float(((rng.h - rng.l) / rng.c).tail(90).median()) if len(rng) >= 20 else np.nan

    def signal(self, p):
        return super().signal(p) & self.allowed


def segments():
    U = glob_data()["U"]
    out = []
    for s in U.columns:
        d = U.index[U[s] == 1]
        if not len(d):
            continue
        starts = [d[0]]; ends = []
        for x, y in zip(d[:-1], d[1:]):
            if (y - x).days > 7:
                ends.append(x); starts.append(y)
        ends.append(d[-1])
        out += [(s, a, b) for a, b in zip(starts, ends)]
    return out


def task(args):
    sym, a, b = args
    G = glob_data()
    A = a; B = min(b + pd.Timedelta(days=3), END)     # 3 дня на доведение открытой сделки
    k = load_1m(sym, A - pd.Timedelta(days=2), B)
    if k.empty or len(k) < 3000:
        return dict(sym=sym, a=A, b=B, skipped=True, n=len(k))
    day_ok = G["U"][sym].reindex(k.index.floor("D")).fillna(0).values.astype(bool)
    sel = (k.index >= A) & (k.index <= B)
    c = SegCoin(sym, k, A, B, day_ok[sel])
    scale = ge.daily_scale(sym, c.k.index, G["DOGE_RNG"])      # пересчёт каждый день по 90 дням до него
    res = {}
    for vn, kw in VARIANTS.items():
        ex = {} if __import__("os").environ.get("UNIV_SET") == "capital" else None
        E, C, liq, tr = ge.run(c, ge.P(side=1, scale=scale, **kw), expo_out=ex)
        C["монета"] = sym.replace("USDT", "")
        d = E.resample("D").last().dropna()
        res[vn] = dict(days=d.diff().fillna(d.iloc[0] - 10_000.0), camps=C, liq=liq, expo=ex)
    return dict(sym=sym, a=A, b=B, skipped=False, scale=float(np.median(scale)), res=res, minutes=int(sel.sum()), first=k.index.min())


if __name__ == "__main__":
    pd.set_option("display.width", 250)
    segs = segments()
    t0 = time.time()
    print(f"отрезков {len(segs)}", flush=True)
    out = []
    with mp.get_context("spawn").Pool(6) as pool:
        for j, r in enumerate(pool.imap_unordered(task, segs, chunksize=1), 1):
            out.append(r)
            if j % 20 == 0:
                print(f"  готово {j}/{len(segs)} за {time.time() - t0:.0f} с", flush=True)
    skipped = [r for r in out if r["skipped"]]
    ok = [r for r in out if not r["skipped"]]
    print(f"посчитано отрезков {len(ok)}, пропущено без минуток {len(skipped)}: " + ", ".join(f"{r['sym']} {r['a']:%d.%m.%y}" for r in skipped), flush=True)
    late = [r for r in ok if r["first"] > r["a"] - pd.Timedelta(days=1)]
    if late:
        print("  минутки начинаются позже начала отрезка:", ", ".join(f"{r['sym']} {r['a']:%d.%m.%y}→{r['first']:%d.%m.%y}" for r in late))

    for vn in VARIANTS:
        P = pd.concat([r["res"][vn]["days"] for r in ok], axis=1).sum(axis=1).sort_index()
        eq = 100_000 + P.cumsum()
        C = pd.concat([r["res"][vn]["camps"] for r in ok if len(r["res"][vn]["camps"])])
        C["t0"] = pd.to_datetime(C.t0); C["t1"] = pd.to_datetime(C.t1); C["год"] = C.t1.dt.year
        liqs = [r for r in ok if r["res"][vn]["liq"] is not None]
        print(f"\n=== {vn}: ликвидаций {len(liqs)}" + (" (" + ", ".join(f"{r['sym'].replace('USDT', '')} {r['res'][vn]['liq']:%d.%m.%y}" for r in liqs) + ")" if liqs else "")
              + f" | вся просадка {(eq / eq.cummax() - 1).min() * 100:.1f}% | итог {eq.iloc[-1] - 100_000:+,.0f}$")
        for y, g in C.groupby("год"):
            e = eq[eq.index.year == y]
            start = eq[eq.index < e.index[0]].iloc[-1] if (eq.index < e.index[0]).any() else 100_000
            w = g[g.pnl > 0]; lo = g[g.pnl <= 0]
            print(f"  {y}: сделок {len(g):5d}, прибыльных {len(w) / len(g) * 100:.0f}%, средний плюс {w.pnl.mean():.0f}$, средний минус {lo.pnl.mean():.0f}$, "
                  f"стопов {(g.exit == 'стоп').sum()}, итог {e.iloc[-1] - start:+,.0f}$ ({(e.iloc[-1] - start) / 1000:+.1f}%), "
                  f"просадка в году {(e / e.cummax() - 1).min() * 100:.1f}%, монет {g.монета.nunique()}")
        worst = C.sort_values("pnl").head(8)
        print("  худшие сделки:", "; ".join(f"{r.монета} {r.t0:%d.%m.%y} {r.pnl:,.0f}$ ({r.exit})" for r in worst.itertuples()))
        by = C.groupby("монета").pnl.sum().sort_values()
        print("  хуже всего по монетам:", ", ".join(f"{m} {v:+,.0f}$" for m, v in by.head(6).items()))
        print("  лучше всего по монетам:", ", ".join(f"{m} {v:+,.0f}$" for m, v in by.tail(6)[::-1].items()))
        wd = P.sort_values().head(3)
        print("  худшие дни:", ", ".join(f"{d:%d.%m.%y} {v:+,.0f}$" for d, v in wd.items()))
        tag = vn.replace(" ", "_").replace("+", "p").replace("/", "-").replace("%", "")
        C.to_parquet(ROOT / f"inbox/private/work/itek/universe_camps_{tag}.parquet")
        (100_000 + P.cumsum()).to_frame("equity").to_parquet(ROOT / f"inbox/private/work/itek/universe_eq_{tag}.parquet")
    if __import__("os").environ.get("UNIV_SET") == "capital":
        for vn in VARIANTS:
            NT = pd.concat([r["res"][vn]["expo"]["notional"] for r in ok]).groupby(level=0).sum()
            UR = pd.concat([r["res"][vn]["expo"]["unreal"] for r in ok]).groupby(level=0).sum()
            full = pd.date_range(pd.Timestamp("2021-07-01"), END, freq="min")
            share = len(NT) / len(full)
            nt_all = NT.reindex(full, fill_value=0.0) / 1000.0            # в % от $100 000
            need = (-UR + 0.01 * NT)                                     # нужен капитал, чтобы не ликвидировали: убыток + поддержка ~1%
            print(f"\n=== капитал в сделках, {vn}")
            print(f"  хоть одна позиция открыта {share * 100:.0f}% времени")
            print(f"  в сделках (сумма позиций, % от $100 000): в среднем {nt_all.mean():.1f}%, медиана {nt_all.median():.1f}%, "
                  f"90% времени ≤ {nt_all.quantile(0.9):.1f}%, 99% ≤ {nt_all.quantile(0.99):.1f}%, 99.9% ≤ {nt_all.quantile(0.999):.1f}%, "
                  f"максимум {nt_all.max():.1f}% ({nt_all.idxmax():%d.%m.%y %H:%M})")
            yr = nt_all.groupby(nt_all.index.year)
            print("  по годам (среднее / максимум, %):", ", ".join(f"{y}: {m_:.1f}/{x_:.0f}" for (y, m_), x_ in zip(yr.mean().items(), yr.max().values)))
            print(f"  открытый убыток всех позиций (по худшей цене минуты): худший {UR.min():+,.0f}$ ({UR.idxmin():%d.%m.%y %H:%M}), "
                  f"1% худших минут ≤ {UR.quantile(0.01):+,.0f}$")
            print(f"  нужно на торговом счёте, чтобы пережить худшую минуту: {need.max():,.0f}$ ({need.idxmax():%d.%m.%y %H:%M})")
            top = need.resample("D").max().sort_values(ascending=False).head(6)
            print("  самые тяжёлые дни по требуемому капиталу:", ", ".join(f"{d:%d.%m.%y} {v:,.0f}$" for d, v in top.items()))
    print(f"\nвсего {time.time() - t0:.0f} с", flush=True)
