#!/usr/bin/env python3
"""СТРАТЕГИЯ — Пред-отсечечный дивидендный разгон, с разрезами по сегментам.

Сигнал (из пилота #1): рынок РФ загоняет «народную» бумагу ВВЕРХ перед отсечкой
(дивидендный кэптюр). Стратегия: купить за T-20 торговых дней до ex_date, продать
за T-8, доходность market-adjusted к IMOEX (= захеджировано рынком). Каждое
дивидендное событие = одна сделка.

Разрезы (ключевое — поток бьёт по сегментам ПО-РАЗНОМУ):
- ЭШЕЛОН (ликвидность) — терциль медианного дневного оборота (₽) ВНУТРИ года:
  1-й эшелон (голубые фишки) / 2-й / 3-й. cap историч. нет (3 мес) → оборот = прокси.
- СЕКТОР — instruments.sector.
- ДИВ. ДОХОДНОСТЬ — value / цена(T-20), бакеты.

Метрики на сегмент: N сделок, средняя доходность/сделку, медиана, t-стат, win-rate.
"""
import os
from datetime import date
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DB_URL"])

ENTRY, EXIT = -20, -8        # окно «разгона» (торговые дни от ex_date)
LIQ_WIN = (-90, -21)         # окно замера оборота (до сигнала, без перекрытия)


def load():
    div = pd.read_sql("SELECT secid, ex_date, value FROM dividends WHERE ex_date IS NOT NULL",
                      eng, parse_dates=["ex_date"])
    imoex = pd.read_sql("SELECT trade_date, close FROM index_data WHERE secid='IMOEX' AND close>0 ORDER BY trade_date",
                        eng, parse_dates=["trade_date"]).set_index("trade_date")["close"]
    secids = sorted(div["secid"].unique())
    px = pd.read_sql(text("SELECT secid, begin_time::date d, close, value FROM candles "
                          "WHERE type='stock' AND interval=24 AND close>0 AND secid = ANY(:s)"),
                     eng, params={"s": secids}, parse_dates=["d"])
    sect = pd.read_sql("SELECT sec_id, sector FROM instruments WHERE type='stock'", eng)
    return div, imoex, px, dict(zip(sect.sec_id, sect.sector))


def main():
    div, imoex, px, sect_map = load()
    cal = imoex.index.values.astype("datetime64[D]"); cal.sort()
    closes, values = {}, {}
    for sid, g in px.groupby("secid"):
        g = g.sort_values("d")
        closes[sid] = g.set_index("d")["close"]
        values[sid] = g.set_index("d")["value"]

    def at(s, i):
        if i < 0 or i >= len(cal):
            return None
        v = s.asof(cal[i])
        return v if np.isfinite(v) else None

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
        ar = (s_out / s_in - 1.0) - (m_out / m_in - 1.0)   # market-adjusted P&L сделки
        # ликвидность: медианный дневной оборот ₽ в окне до сигнала
        vv = values[sid]
        tw = [at(vv, pos + k) for k in range(LIQ_WIN[0], LIQ_WIN[1] + 1)]
        tw = [x for x in tw if x and x > 0]
        turnover = float(np.median(tw)) if tw else np.nan
        dy = (float(r["value"]) / s_in) if s_in else np.nan   # дивдоходность к цене входа
        rows.append(dict(secid=sid, year=ex.year, ar=ar, turnover=turnover,
                         dy=dy, sector=sect_map.get(sid, "—")))
    ev = pd.DataFrame(rows)

    # эшелон: терциль оборота ВНУТРИ года (эшелон — понятие относительное)
    ev["ech"] = pd.Series([None] * len(ev), index=ev.index, dtype=object)
    for y, g in ev.groupby("year"):
        t = g["turnover"].dropna()
        if len(t) < 6:
            continue
        q1, q2 = t.quantile([1/3, 2/3])
        ev.loc[g.index, "ech"] = np.where(g["turnover"] >= q2, "1-й (голубые)",
                                  np.where(g["turnover"] >= q1, "2-й эшелон", "3-й эшелон"))

    def stat(a):
        a = np.asarray(a, float); a = a[np.isfinite(a)]
        if len(a) < 8:
            return f"n={len(a):4d}  (мало)"
        m, sd = a.mean(), a.std(ddof=1)
        t = m/(sd/np.sqrt(len(a))) if sd > 0 else 0
        return f"n={len(a):4d}  сред={m*100:+5.2f}%  медиана={np.median(a)*100:+5.2f}%  t={t:+4.1f}  win={ (a>0).mean()*100:4.1f}%"

    print("=" * 88)
    print(f"СТРАТЕГИЯ: дивидендный разгон, вход T{ENTRY}, выход T{EXIT}, market-adjusted к IMOEX")
    print(f"Сделок (событий): {ev['ar'].notna().sum()}")
    print("=" * 88)
    print("\n▸ ВСЕГО:                    ", stat(ev["ar"]))
    print("\n▸ ПО ЭШЕЛОНАМ (ликвидность, оборот-терциль внутри года):")
    for e in ["1-й (голубые)", "2-й эшелон", "3-й эшелон"]:
        print(f"   {e:16s}", stat(ev.loc[ev.ech == e, "ar"]))
    print("\n▸ ПО ЭПОХЕ:")
    print("   до 2022        ", stat(ev.loc[ev.year < 2022, "ar"]))
    print("   2022+ (физики) ", stat(ev.loc[ev.year >= 2022, "ar"]))
    print("\n▸ ПО СЕКТОРАМ (только где n>=8):")
    for s, g in sorted(ev.groupby("sector"), key=lambda x: -x[1]["ar"].mean()):
        st = stat(g["ar"])
        if "мало" not in st:
            print(f"   {s:16s}", st)
    print("\n▸ ПО ДИВ-ДОХОДНОСТИ (к цене входа):")
    ev["dyb"] = pd.cut(ev["dy"], [0, 0.03, 0.06, 0.10, 1.0],
                       labels=["<3%", "3-6%", "6-10%", ">10%"])
    for b in ["<3%", "3-6%", "6-10%", ">10%"]:
        print(f"   дивдох {b:6s}", stat(ev.loc[ev.dyb == b, "ar"]))
    # связка эшелон × эпоха (главная гипотеза: разгон сильнее у неликвида + 2022+)
    print("\n▸ ЭШЕЛОН × 2022+ (где поток розницы максимален):")
    for e in ["1-й (голубые)", "2-й эшелон", "3-й эшелон"]:
        print(f"   {e:16s} 2022+", stat(ev.loc[(ev.ech == e) & (ev.year >= 2022), "ar"]))

    print("\n[стратегия торгуема, если сред>0 с t>2 в сегменте; смотрим, в каком "
          "эшелоне/секторе разгон сильнее и устойчивее]")


if __name__ == "__main__":
    main()
