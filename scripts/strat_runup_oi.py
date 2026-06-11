#!/usr/bin/env python3
"""ИССЛЕДОВАНИЕ — кто стоит за дивидендным разгоном: ФИЗ vs ЮР + нетто после спредов.

(А) В окне разгона [T-20,T-8] смотрим Δ нетто-позиции ФИЗ и ЮР по single-stock
фьючерсу бумаги. Гипотеза «охоты на толпу»: розница (ФИЗ) НАБИРАЕТ лонг (гонит
капчур-разгон), профи (ЮР) РАЗДАЮТ/шортят (фейдят к отсечке).
(Б) Нетто-доходность стратегии после грубого вычета round-trip издержек по эшелонам.
"""
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DB_URL"])
ENTRY, EXIT = -20, -8
LIQ_WIN = (-90, -21)

STOCK2FUT = {
    "SBER": "SR", "SBERP": "SR", "GAZP": "GZ", "LKOH": "LK", "GMKN": "GK",
    "ROSN": "RN", "TATN": "TT", "TATNP": "TT", "SNGS": "SN", "SNGSP": "SN",
    "MTSS": "MT", "MGNT": "MN", "NVTK": "NK", "PLZL": "PZ", "ALRS": "AL",
    "CHMF": "CH", "NLMK": "NM", "MAGN": "MG", "AFLT": "AF", "MOEX": "ME",
    "PHOR": "PH", "RUAL": "RL", "FEES": "FS", "HYDR": "HY", "IRAO": "IR",
    "SIBN": "SO", "VTBR": "VB", "AFKS": "AK", "PIKK": "PI", "RTKM": "RT",
}
# грубый round-trip (спред+комиссия), % — по эшелонам
COST = {"1-й (голубые)": 0.15, "2-й эшелон": 0.45, "3-й эшелон": 1.30}


def main():
    div = pd.read_sql("SELECT secid, ex_date, value FROM dividends WHERE ex_date IS NOT NULL",
                      eng, parse_dates=["ex_date"])
    imoex = pd.read_sql("SELECT trade_date, close FROM index_data WHERE secid='IMOEX' AND close>0 ORDER BY trade_date",
                        eng, parse_dates=["trade_date"]).set_index("trade_date")["close"]
    secids = sorted(div["secid"].unique())
    px = pd.read_sql(text("SELECT secid, begin_time::date d, close, value FROM candles "
                          "WHERE type='stock' AND interval=24 AND close>0 AND secid = ANY(:s)"),
                     eng, params={"s": secids}, parse_dates=["d"])
    closes, values = {}, {}
    for sid, g in px.groupby("secid"):
        g = g.sort_values("d")
        closes[sid] = g.set_index("d")["close"]; values[sid] = g.set_index("d")["value"]
    futs = sorted(set(STOCK2FUT.values()))
    oi = pd.read_sql(text("SELECT sectype, clgroup, tradedate d, pos FROM open_interest "
                          "WHERE interval=24 AND pos IS NOT NULL AND sectype = ANY(:s)"),
                     eng, params={"s": futs}, parse_dates=["d"])
    oinet = {(st, cg): g.sort_values("d").set_index("d")["pos"]
             for (st, cg), g in oi.groupby(["sectype", "clgroup"])}

    cal = imoex.index.values.astype("datetime64[D]"); cal.sort()

    def at(s, i):
        if s is None or i < 0 or i >= len(cal):
            return None
        v = s.asof(cal[i]); return v if np.isfinite(v) else None

    rows = []
    for _, r in div.iterrows():
        sid, ex = r["secid"], r["ex_date"]
        cl = closes.get(sid)
        if cl is None:
            continue
        pos = cal.searchsorted(np.datetime64(ex))
        s_in, s_out = at(cl, pos + ENTRY), at(cl, pos + EXIT)
        m_in, m_out = at(imoex, pos + ENTRY), at(imoex, pos + EXIT)
        if not all(x and x > 0 for x in (s_in, s_out, m_in, m_out)):
            continue
        ar = (s_out / s_in - 1.0) - (m_out / m_in - 1.0)
        vv = values[sid]
        tw = [at(vv, pos + k) for k in range(LIQ_WIN[0], LIQ_WIN[1] + 1)]
        tw = [x for x in tw if x and x > 0]
        rec = dict(secid=sid, year=ex.year, ar=ar,
                   turnover=float(np.median(tw)) if tw else np.nan, dfiz=np.nan, dyur=np.nan)
        ft = STOCK2FUT.get(sid)
        if ft:
            fz, yr = oinet.get((ft, "FIZ")), oinet.get((ft, "YUR"))
            f0, f1 = at(fz, pos + ENTRY), at(fz, pos + EXIT)
            y0, y1 = at(yr, pos + ENTRY), at(yr, pos + EXIT)
            # нормируем Δ на |позицию входа| → доля, сопоставимо между бумагами
            if f0 is not None and f1 is not None and abs(f0) > 100:
                rec["dfiz"] = (f1 - f0) / abs(f0)
            if y0 is not None and y1 is not None and abs(y0) > 100:
                rec["dyur"] = (y1 - y0) / abs(y0)
        rows.append(rec)
    ev = pd.DataFrame(rows)

    # эшелон по обороту-терцилю внутри года
    ev["ech"] = pd.Series([None] * len(ev), index=ev.index, dtype=object)
    for y, g in ev.groupby("year"):
        t = g["turnover"].dropna()
        if len(t) < 6:
            continue
        q1, q2 = t.quantile([1/3, 2/3])
        ev.loc[g.index, "ech"] = np.where(g["turnover"] >= q2, "1-й (голубые)",
                                  np.where(g["turnover"] >= q1, "2-й эшелон", "3-й эшелон"))

    def m(a):
        a = np.asarray(a, float); a = a[np.isfinite(a)]
        return (len(a), a.mean() if len(a) else np.nan, np.median(a) if len(a) else np.nan)

    print("=" * 80)
    print("(А) КТО В РАЗГОНЕ: Δ нетто-позиции ФИЗ vs ЮР в окне [T-20,T-8]")
    print("     (норм. на |вход|; >0 = набирают лонг, <0 = шортят/раздают)")
    print("=" * 80)
    sub = ev.dropna(subset=["dfiz", "dyur"])
    nf, mf, _ = m(sub["dfiz"]); ny, my, _ = m(sub["dyur"])
    print(f"  событий с фьючерсом: {len(sub)}")
    print(f"  ФИЗ: сред Δ={mf*100:+5.1f}%  доля_набора={ (sub['dfiz']>0).mean()*100:4.0f}%")
    print(f"  ЮР:  сред Δ={my*100:+5.1f}%  доля_набора={ (sub['dyur']>0).mean()*100:4.0f}%")
    opp = ((sub["dfiz"] > 0) & (sub["dyur"] < 0)).mean()
    print(f"  ФИЗ↑ и ЮР↓ одновременно (толпа покупает, профи раздают): {opp*100:.0f}% событий")
    if len(sub) > 20:
        print(f"  corr(Δ ФИЗ, Δ ЮР) = {np.corrcoef(sub['dfiz'], sub['dyur'])[0,1]:+.2f}  "
              f"(сильно отрицательная = противоположные стороны)")
        print(f"  corr(Δ ФИЗ, разгон AR) = {np.corrcoef(sub['dfiz'], sub['ar'])[0,1]:+.2f}")
        print(f"  corr(Δ ЮР,  разгон AR) = {np.corrcoef(sub['dyur'], sub['ar'])[0,1]:+.2f}")

    print("\n" + "=" * 80)
    print("(Б) НЕТТО-ДОХОДНОСТЬ ПОСЛЕ ИЗДЕРЖЕК (round-trip спред+комиссия по эшелону)")
    print("=" * 80)
    for e, c in COST.items():
        g = ev.loc[ev.ech == e, "ar"]
        n, mn, md = m(g)
        if n >= 8:
            print(f"  {e:16s}: брутто {mn*100:+5.2f}%  − издержки {c:.2f}%  = "
                  f"НЕТТО {mn*100 - c:+5.2f}%  (медиана нетто {md*100 - c:+5.2f}%, n={n})")
    # high-yield нетто (берём дивдох>6% — там разгон сильнейший)
    print("\n[вывод: смотрим, противоположны ли ФИЗ и ЮР, и остаётся ли НЕТТО-край после спредов]")


if __name__ == "__main__":
    main()
