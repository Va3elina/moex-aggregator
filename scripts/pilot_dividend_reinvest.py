#!/usr/bin/env python3
"""ПИЛОТ #1 — Дивидендный реинвест: event-study на нашей БД.

Гипотеза: розница реинвестирует полученные дивиденды → у «народных» бумаг в окне
ex_date+8…+20 торговых дней положительная аномальная доходность (vs IMOEX),
сильнее после 2022 (доминация физиков). Подтверждение через OI: рост числа
ФИЗ-лонгов (pos_long_num) по соответствующему фьючерсу в окне реинвеста.

Чистый бэктест на имеющихся данных, без внешнего добора. Метрика — market-adjusted
оконная доходность (beta=1): AR = (S[t2]/S[t1]-1) - (IMOEX[t2]/IMOEX[t1]-1).
Плацебо-окно [-20,-8] до ex_date — чтобы убедиться, что пост-окно особенное.
"""
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DB_URL"])

# Окна в ТОРГОВЫХ днях от ex_date
W_POST = (8, 20)    # окно реинвеста (гипотеза)
W_PRE = (-20, -8)   # плацебо до события
OI_MID = 14         # день в окне для замера ФИЗ-лонгов

# Маппинг дивидендных акций → sectype фьючерса (для OI-наложения)
STOCK2FUT = {
    "SBER": "SR", "SBERP": "SR", "GAZP": "GZ", "LKOH": "LK", "GMKN": "GK",
    "ROSN": "RN", "TATN": "TT", "TATNP": "TT", "SNGS": "SN", "SNGSP": "SN",
    "MTSS": "MT", "MGNT": "MN", "NVTK": "NK", "PLZL": "PZ", "ALRS": "AL",
    "CHMF": "CH", "NLMK": "NM", "MAGN": "MG", "AFLT": "AF", "MOEX": "ME",
    "PHOR": "PH", "RUAL": "RL", "FEES": "FS", "HYDR": "HY", "IRAO": "IR",
    "SIBN": "SO", "VTBR": "VB", "BANE": "BN", "BANEP": "BN", "BSPB": "BS",
    "AFKS": "AK", "FLOT": "FL", "PIKK": "PI", "RTKM": "RT", "TRNFP": "TN",
}
# «Народные»/ликвидные имена для подвыборки (дивидендные блючипы розницы)
LIQUID = set(STOCK2FUT.keys()) | {"SBERP", "SNGSP", "TATNP", "GAZP", "LKOH"}


def load():
    div = pd.read_sql(
        "SELECT secid, ex_date FROM dividends WHERE ex_date IS NOT NULL",
        eng, parse_dates=["ex_date"])
    imoex = pd.read_sql(
        "SELECT trade_date, close FROM index_data WHERE secid='IMOEX' AND close>0 ORDER BY trade_date",
        eng, parse_dates=["trade_date"]).set_index("trade_date")["close"]
    secids = sorted(div["secid"].unique())
    px = pd.read_sql(
        text("SELECT secid, begin_time::date AS d, close FROM candles "
             "WHERE type='stock' AND interval=24 AND close>0 AND secid = ANY(:s)"),
        eng, params={"s": secids}, parse_dates=["d"])
    return div, imoex, px


def build_series(px):
    out = {}
    for sid, g in px.groupby("secid"):
        s = g.sort_values("d").set_index("d")["close"]
        s = s[~s.index.duplicated(keep="last")]
        out[sid] = s
    return out


def win_ar(stock, imoex, cal, ex, lo, hi):
    """Market-adjusted доходность в окне [lo,hi] торговых дней от ex (в календаре cal)."""
    pos = cal.searchsorted(np.datetime64(ex))
    i1, i2 = pos + lo, pos + hi
    if i1 < 0 or i2 >= len(cal):
        return None
    d1, d2 = cal[i1], cal[i2]
    try:
        s1 = stock.asof(d1); s2 = stock.asof(d2)
        m1 = imoex.asof(d1); m2 = imoex.asof(d2)
    except Exception:
        return None
    if not (np.isfinite(s1) and np.isfinite(s2) and np.isfinite(m1) and np.isfinite(m2)) or s1 <= 0 or m1 <= 0:
        return None
    # asof мог отдать значение сильно раньше окна (бумага не торговалась) — грубый guard
    return (s2 / s1 - 1.0) - (m2 / m1 - 1.0)


def stats(arr):
    a = np.asarray(arr, float)
    a = a[np.isfinite(a)]
    if len(a) < 5:
        return None
    mean = a.mean(); sd = a.std(ddof=1)
    t = mean / (sd / np.sqrt(len(a))) if sd > 0 else 0.0
    return dict(n=len(a), mean=mean, t=t, win=(a > 0).mean(), med=np.median(a))


def fmt(s):
    if s is None:
        return "n<5"
    return (f"n={s['n']:4d}  AR_сред={s['mean']*100:+5.2f}%  "
            f"медиана={s['med']*100:+5.2f}%  t={s['t']:+4.1f}  win={s['win']*100:4.1f}%")


def main():
    div, imoex, px = load()
    series = build_series(px)
    cal = imoex.index.values.astype("datetime64[D]")
    cal.sort()

    rows = []
    for _, r in div.iterrows():
        sid, ex = r["secid"], r["ex_date"]
        st = series.get(sid)
        if st is None:
            continue
        ar_post = win_ar(st, imoex, cal, ex, *W_POST)
        ar_pre = win_ar(st, imoex, cal, ex, *W_PRE)
        rows.append(dict(secid=sid, ex=ex, year=ex.year, liquid=sid in LIQUID,
                         post=ar_post, pre=ar_pre))
    ev = pd.DataFrame(rows)

    def report(df, title):
        print(f"\n── {title} ──")
        print(f"  ПОСТ  [{W_POST[0]:+},{W_POST[1]:+}] (реинвест): {fmt(stats(df['post']))}")
        print(f"  ПЛАЦЕБО[{W_PRE[0]:+},{W_PRE[1]:+}] (до):       {fmt(stats(df['pre']))}")

    print("=" * 78)
    print("ПИЛОТ #1 — Дивидендный реинвест (event-study, market-adjusted vs IMOEX)")
    print(f"Всего событий с ценой: {len(ev)}")
    print("=" * 78)
    report(ev, "ВСЕ бумаги, весь период")
    report(ev[ev.year < 2022], "ВСЕ, до 2022 (низкая доля физиков)")
    report(ev[ev.year >= 2022], "ВСЕ, 2022+ (доминация физиков)")
    report(ev[ev.liquid], "Только ликвидные/народные, весь период")
    report(ev[ev.liquid & (ev.year >= 2022)], "Ликвидные, 2022+ (главная гипотеза)")

    # ── OI-наложение: растут ли ФИЗ-лонги по фьючерсу в окне реинвеста? ──
    print("\n" + "=" * 78)
    print("OI-НАЛОЖЕНИЕ: Δ числа ФИЗ-лонгов (pos_long_num, FIZ) ex_date → ex_date+%d тд" % OI_MID)
    print("=" * 78)
    futs = sorted({STOCK2FUT[s] for s in ev["secid"].unique() if s in STOCK2FUT})
    oi = pd.read_sql(
        text("SELECT sectype, tradedate AS d, pos_long_num FROM open_interest "
             "WHERE clgroup='FIZ' AND interval=24 AND sectype = ANY(:s) AND pos_long_num IS NOT NULL"),
        eng, params={"s": futs}, parse_dates=["d"])
    oi_ser = {st: g.sort_values("d").set_index("d")["pos_long_num"]
              for st, g in oi.groupby("sectype")}

    deltas = []
    for _, r in ev.iterrows():
        ft = STOCK2FUT.get(r["secid"])
        if not ft or ft not in oi_ser:
            continue
        s = oi_ser[ft]
        pos = cal.searchsorted(np.datetime64(r["ex"]))
        if pos < 0 or pos + OI_MID >= len(cal):
            continue
        d0, d1 = cal[pos], cal[pos + OI_MID]
        v0 = s.asof(d0); v1 = s.asof(d1)
        if np.isfinite(v0) and np.isfinite(v1) and v0 > 0:
            deltas.append(dict(year=r["ex"].year, d=(v1 / v0 - 1.0),
                               post=r["post"]))
    od = pd.DataFrame(deltas)
    if len(od):
        def oirep(df, t):
            a = df["d"].dropna().values
            if len(a) < 5:
                print(f"  {t}: n<5"); return
            print(f"  {t}: n={len(a):4d}  ΔФИЗ-лонг_сред={a.mean()*100:+5.2f}%  "
                  f"медиана={np.median(a)*100:+5.2f}%  доля_роста={(a>0).mean()*100:4.1f}%")
        oirep(od, "Все события с фьючерсом    ")
        oirep(od[od.year >= 2022], "2022+                      ")
        # связь: совпадает ли знак ФИЗ-притока с положительной AR?
        both = od.dropna(subset=["d", "post"])
        if len(both) > 10:
            c = np.corrcoef(both["d"], both["post"])[0, 1]
            print(f"  corr(ΔФИЗ-лонг, AR_пост) = {c:+.3f}  (n={len(both)})")
    else:
        print("  нет пересечений событий с OI-фьючерсами")

    print("\n[интерпретация: гипотеза в плюс, если ПОСТ-окно AR_сред>0 с t>2, заметно "
          "выше ПЛАЦЕБО, усиливается в 2022+, и ΔФИЗ-лонг в окне положителен]")


if __name__ == "__main__":
    main()
