#!/usr/bin/env python3
"""СТРАТЕГИЯ — дивидендный пред-отсечечный разгон. ПОЛНАЯ ВЕРСИЯ (perf+groups+WF).

Событие: ex-дата дивиденда. Купить close[T-ENTRY] до ex, продать close[T-EXIT] до ex
(до ex, не держим через гэп). Доходность market-adjusted к IMOEX.
NET = брутто − round-trip издержки эшелона (центральная метрика).
Группы: див-бакет / эшелон(оборот-терциль) / капа(market_cap-терциль) / сектор / режим.
Окна: основное (T-20→T-1) и робастное (T-15→T-3). Тестируем ОБА.
Кластер-SE по (год×месяц) ex-даты. Walk-forward по сезонам. Блок последних 12 мес. OI ФИЗ.

═══ ВАЛИДАЦИЯ 11.06.2026 (workflow: 2 независимых построения + критик-арбитр) ═══
ТОРГУЕМО на исторической базе. Плацебо ПРОЙДЕН (реал +2.22% vs плацебо-распред сред +0.67%,
bootstrap p=0.0000). АЛЬФА, не бета (держится во всех beta-терцилях). НЕ концентрация
(62 имени, leave-one-out устойчив). Net-of-cost ВЫЖИВАЕТ в: ВЫСОКАЯ дивдох >6% (NET +3.6%,
кл-t +6.4), 1-й эшелон/голубые (+2.1%, breakeven-спред +2.2% >> 0.15%), крупная капа (+1.7%),
секторы Нефть-газ/Финансы. УМИРАЕТ после издержек: дивдох <3% (NET≈0), 3-й эшелон, Химия/IT.
WF OOS: 115 сделок, NET/сделку +2.37%, кл-t +3.2, выбор стабилизировался на «2-й эш×ВЫСОКАЯ>6%».
⚠️ ДЕГРАДАЦИЯ: последние 12 мес край просел до ~+0.5-1% и СТАТ.НЕЗНАЧИМ (кл-t +0.78); 3-й эшелон
сейчас УБЫТОЧЕН (спред 1.30% съедает). Вероятно, дивкэптюр стал общеизвестным.

КАК ТОРГОВАТЬ (если торговать): юниверс = 1-й эшелон × прогнозная дивдох >6%; окно T-20→T-1
(или робастнее T-15→T-3); хедж IMOEX (край определён market-adjusted); лимит ~5-7%/имя; спред <0.5%;
сезонная (не круглогодичная) экспозиция. Полный разбор — .claude/BACKTEST_RESULTS_2026-06-11.md.
"""
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DB_URL"])

WINDOWS = {"main_T20_T1": (-20, -1), "robust_T15_T3": (-15, -3)}
LIQ_WIN = (-90, -21)          # окно замера оборота (до сигнала, без перекрытия)
# round-trip издержки по эшелону, % (как в strat_runup_oi.py)
COST = {"1-й (голубые)": 0.15, "2-й эшелон": 0.45, "3-й эшелон": 1.30}

STOCK2FUT = {
    "SBER": "SR", "SBERP": "SR", "GAZP": "GZ", "LKOH": "LK", "GMKN": "GK",
    "ROSN": "RN", "TATN": "TT", "TATNP": "TT", "SNGS": "SN", "SNGSP": "SN",
    "MTSS": "MT", "MGNT": "MN", "NVTK": "NK", "PLZL": "PZ", "ALRS": "AL",
    "CHMF": "CH", "NLMK": "NM", "MAGN": "MG", "AFLT": "AF", "MOEX": "ME",
    "PHOR": "PH", "RUAL": "RL", "FEES": "FS", "HYDR": "HY", "IRAO": "IR",
    "SIBN": "SO", "VTBR": "VB", "AFKS": "AK", "PIKK": "PI", "RTKM": "RT",
}


def load():
    div = pd.read_sql("SELECT secid, ex_date, value FROM dividends WHERE ex_date IS NOT NULL AND value>0",
                      eng, parse_dates=["ex_date"])
    imoex = pd.read_sql("SELECT trade_date, close FROM index_data WHERE secid='IMOEX' AND close>0 ORDER BY trade_date",
                        eng, parse_dates=["trade_date"]).set_index("trade_date")["close"]
    secids = sorted(div["secid"].unique())
    px = pd.read_sql(text("SELECT secid, begin_time::date d, close, value FROM candles "
                          "WHERE type='stock' AND interval=24 AND close>0 AND secid = ANY(:s)"),
                     eng, params={"s": secids}, parse_dates=["d"])
    sect = pd.read_sql("SELECT sec_id, sector FROM instruments WHERE type='stock'", eng)
    mcap = pd.read_sql("SELECT sec_id, period_date, market_cap FROM stock_market_cap WHERE market_cap>0",
                       eng, parse_dates=["period_date"])
    futs = sorted(set(STOCK2FUT.values()))
    oi = pd.read_sql(text("SELECT sectype, clgroup, tradedate d, pos FROM open_interest "
                          "WHERE interval=24 AND pos IS NOT NULL AND clgroup='FIZ' AND sectype = ANY(:s)"),
                     eng, params={"s": futs}, parse_dates=["d"])
    return div, imoex, px, dict(zip(sect.sec_id, sect.sector)), mcap, oi


def cluster_t(df, col, cl_col="clust"):
    """Среднее, кластер-SE по cl_col, кластер-t. df имеет столбцы col и cl_col."""
    a = df[[col, cl_col]].dropna()
    a = a[np.isfinite(a[col])]
    n = len(a)
    if n < 8:
        return n, np.nan, np.nan, np.nan
    m = a[col].mean()
    # OLS на константу: y = m + e. Кластер-робастный SE среднего.
    # Var(mean) = (1/n^2) * sum_g (sum_{i in g} e_i)^2  ; e_i = y_i - m
    e = a[col].values - m
    var = 0.0
    for _, g in a.groupby(cl_col):
        idx = g.index
        eg = (a.loc[idx, col].values - m)
        var += eg.sum() ** 2
    se = np.sqrt(var) / n
    t = m / se if se > 0 else np.nan
    win = (a[col].values > 0).mean()
    return n, m, t, win


def main():
    div, imoex, px, sect_map, mcap, oi = load()
    cal = imoex.index.values.astype("datetime64[D]"); cal.sort()

    closes, values = {}, {}
    for sid, g in px.groupby("secid"):
        g = g.sort_values("d")
        closes[sid] = g.set_index("d")["close"]
        values[sid] = g.set_index("d")["value"]

    # market_cap по secid → серия по дате (asof)
    mcap_ser = {}
    for sid, g in mcap.groupby("sec_id"):
        mcap_ser[sid] = g.sort_values("period_date").set_index("period_date")["market_cap"]

    # OI FIZ серия по фьючерсу
    oi_fiz = {st: g.sort_values("d").set_index("d")["pos"] for st, g in oi.groupby("sectype")}

    def at(s, i):
        if s is None or i < 0 or i >= len(cal):
            return None
        v = s.asof(cal[i])
        return v if (v is not None and np.isfinite(v)) else None

    def at_ser(s, dt):
        if s is None:
            return None
        v = s.asof(np.datetime64(dt))
        return v if (v is not None and np.isfinite(v)) else None

    # один проход: собираем базовые поля (общие для окон считаем ar по каждому окну)
    rows = []
    for _, r in div.iterrows():
        sid, ex = r["secid"], r["ex_date"]
        cl = closes.get(sid)
        if cl is None:
            continue
        pos = cal.searchsorted(np.datetime64(ex))
        m_anchor_in = at(imoex, pos - 20)   # для div-yield используем ~T-20 цену как «до разгона»
        if pos - 20 < 0:
            continue
        rec = dict(secid=sid, ex=ex, year=ex.year, month=ex.month,
                   clust=f"{ex.year}-{ex.month:02d}",
                   season_year=ex.year,  # дивсезон = год ex
                   sector=sect_map.get(sid, "—"))
        # дивдоходность к цене ~T-20 (до разгона)
        c_t20 = at(cl, pos - 20)
        rec["dy"] = (float(r["value"]) / c_t20) if (c_t20 and c_t20 > 0) else np.nan
        # оборот в окне до сигнала
        vv = values[sid]
        tw = [at(vv, pos + k) for k in range(LIQ_WIN[0], LIQ_WIN[1] + 1)]
        tw = [x for x in tw if x and x > 0]
        rec["turnover"] = float(np.median(tw)) if tw else np.nan
        # market_cap на момент ex (помесячный asof)
        rec["mcap"] = at_ser(mcap_ser.get(sid), ex)
        # AR по каждому окну
        ok_any = False
        for wn, (en, ex_off) in WINDOWS.items():
            s_in, s_out = at(cl, pos + en), at(cl, pos + ex_off)
            m_in, m_out = at(imoex, pos + en), at(imoex, pos + ex_off)
            if all(x and x > 0 for x in (s_in, s_out, m_in, m_out)):
                rec[f"ar_{wn}"] = (s_out / s_in - 1.0) - (m_out / m_in - 1.0)
                ok_any = True
            else:
                rec[f"ar_{wn}"] = np.nan
        # OI FIZ: Δ нетто-позиции в окне [T-20,T-1] (нормир. на |вход|)
        ft = STOCK2FUT.get(sid)
        rec["dfiz"] = np.nan
        if ft:
            fz = oi_fiz.get(ft)
            f0, f1 = at(fz, pos - 20), at(fz, pos - 1)
            if f0 is not None and f1 is not None and abs(f0) > 100:
                rec["dfiz"] = (f1 - f0) / abs(f0)
        if ok_any:
            rows.append(rec)

    ev = pd.DataFrame(rows)

    # ---- ГРУППЫ ----
    # эшелон: терциль оборота ВНУТРИ года
    ev["ech"] = pd.Series([None] * len(ev), index=ev.index, dtype=object)
    for y, g in ev.groupby("year"):
        t = g["turnover"].dropna()
        if len(t) < 6:
            continue
        q1, q2 = t.quantile([1/3, 2/3])
        ev.loc[g.index, "ech"] = np.where(g["turnover"] >= q2, "1-й (голубые)",
                                  np.where(g["turnover"] >= q1, "2-й эшелон", "3-й эшелон"))
    # капа: терциль market_cap ВНУТРИ года
    ev["capb"] = pd.Series([None] * len(ev), index=ev.index, dtype=object)
    for y, g in ev.groupby("year"):
        t = g["mcap"].dropna()
        if len(t) < 6:
            continue
        q1, q2 = t.quantile([1/3, 2/3])
        ev.loc[g.index, "capb"] = np.where(g["mcap"] >= q2, "крупная",
                                   np.where(g["mcap"] >= q1, "средняя", "малая"))
    # див-бакет
    def dyb(x):
        if not np.isfinite(x):
            return None
        if x > 0.06:
            return "ВЫСОКАЯ >6%"
        if x >= 0.03:
            return "СРЕДНЯЯ 3-6%"
        return "НИЗКАЯ <3%"
    ev["dyb"] = ev["dy"].map(dyb)
    # издержки эшелона (по строке)
    ev["cost"] = ev["ech"].map(COST)

    def report_window(wn):
        col = f"ar_{wn}"
        sub = ev[ev[col].notna()].copy()
        print("=" * 100)
        print(f"ОКНО {wn}  ({WINDOWS[wn][0]}→{WINDOWS[wn][1]} торг.дней до ex)  | событий {len(sub)}")
        print("=" * 100)
        n, m, t, w = cluster_t(sub, col)
        # для ВСЕГО издержки — средневзвешенные по эшелону строк (используем медианный COST 2-го)
        print(f"  ВСЕГО: n={n}  брутто={m*100:+.2f}%  кластер-t={t:+.1f}  доля>0={w*100:.0f}%")

        def grp(title, gcol, order=None):
            print(f"\n  ▸ {title}")
            print(f"    {'группа':18s} {'n':>4s} {'брутто%':>8s} {'издерж%':>8s} {'NET%':>8s} {'кл-t':>6s} {'>0%':>5s}")
            keys = order if order else sorted([x for x in sub[gcol].dropna().unique()])
            for k in keys:
                gg = sub[sub[gcol] == k]
                n, m, tt, w = cluster_t(gg, col)
                if n < 8:
                    print(f"    {str(k):18s} {n:>4d}  (мало)")
                    continue
                # издержки: для эшелон-группы — её COST; иначе — средние издержки строк группы
                if gcol == "ech":
                    c = COST[k]
                else:
                    c = gg["cost"].dropna().mean()
                    c = c if np.isfinite(c) else 0.45
                net = m * 100 - c
                # кластер-t для NET: сдвиг среднего на −c (издержки детерминированы) → t от (ar−c/100)
                gg2 = gg.copy()
                gg2["net"] = gg2[col] - (gg2["cost"].fillna(c) / 100.0 if gcol != "ech" else c / 100.0)
                nn, mn, tn, wn2 = cluster_t(gg2, "net")
                print(f"    {str(k):18s} {n:>4d} {m*100:>+7.2f} {c:>8.2f} {net:>+7.2f} {tn:>+6.1f} {w*100:>4.0f}")

        grp("ДИВ-ДОХОДНОСТЬ (бакеты)", "dyb", ["ВЫСОКАЯ >6%", "СРЕДНЯЯ 3-6%", "НИЗКАЯ <3%"])
        grp("ЭШЕЛОН (оборот-терциль)", "ech", ["1-й (голубые)", "2-й эшелон", "3-й эшелон"])
        grp("КАПИТАЛИЗАЦИЯ (market_cap-терциль)", "capb", ["крупная", "средняя", "малая"])
        grp("СЕКТОР", "sector")
        # режим до/после 2022
        print(f"\n  ▸ РЕЖИМ до/после 2022")
        print(f"    {'режим':18s} {'n':>4s} {'брутто%':>8s} {'издерж%':>8s} {'NET%':>8s} {'кл-t':>6s} {'>0%':>5s}")
        for label, mask in [("до 2022", sub["year"] < 2022), ("2022+ (физики)", sub["year"] >= 2022)]:
            gg = sub[mask]
            n, m, tt, w = cluster_t(gg, col)
            if n < 8:
                continue
            c = gg["cost"].dropna().mean(); c = c if np.isfinite(c) else 0.45
            gg2 = gg.copy(); gg2["net"] = gg2[col] - gg2["cost"].fillna(c) / 100.0
            nn, mn, tn, wn2 = cluster_t(gg2, "net")
            print(f"    {label:18s} {n:>4d} {m*100:>+7.2f} {c:>8.2f} {m*100-c:>+7.2f} {tn:>+6.1f} {w*100:>4.0f}")

    for wn in WINDOWS:
        report_window(wn)

    # ---- WALK-FORWARD по сезонам (выбираем лучший эшелон+бакет на ПРОШЛЫХ сезонах) ----
    print("\n" + "=" * 100)
    print("WALK-FORWARD OOS: на сезонах <Y выбираем лучшую (эшелон×бакет) комбу по NET, торгуем сезон Y")
    print("Окно: main_T20_T1. Сезон = год ex. NET по эшелону строки.")
    print("=" * 100)
    col = "ar_main_T20_T1"
    wf = ev[ev[col].notna() & ev["ech"].notna() & ev["dyb"].notna()].copy()
    wf["net"] = wf[col] - wf["ech"].map(COST) / 100.0
    seasons = sorted(wf["year"].unique())
    oos_rows = []
    print(f"  {'сезон':>6s} {'выбор(эшелон/бакет)':28s} {'n_oos':>6s} {'NET_oos%':>9s} {'cum_net%':>9s}")
    cum = 0.0
    for y in seasons:
        if y < 2014:  # нужна история для выбора
            continue
        past = wf[wf["year"] < y]
        cur = wf[wf["year"] == y]
        if len(past) < 30 or len(cur) < 3:
            continue
        # средний NET по (эшелон,бакет) на прошлом, n>=15
        grp = past.groupby(["ech", "dyb"])["net"]
        agg = grp.agg(["mean", "count"])
        agg = agg[agg["count"] >= 15]
        if agg.empty:
            continue
        best = agg["mean"].idxmax()
        be, bb = best
        pick = cur[(cur["ech"] == be) & (cur["dyb"] == bb)]
        if len(pick) == 0:
            # если в этом сезоне нет такой комбы — пропускаем (нет сделок)
            oos_rows.append(dict(year=y, pick=f"{be}/{bb}", n=0, net=np.nan))
            print(f"  {y:>6d} {be[:12]+'/'+bb:28s} {0:>6d} {'—':>9s} {cum*100:>+8.2f}%")
            continue
        net_oos = pick["net"].mean()
        cum += net_oos * len(pick)  # суммарный накопленный (сумма сделочных NET)
        oos_rows.append(dict(year=y, pick=f"{be}/{bb}", n=len(pick), net=net_oos))
        print(f"  {y:>6d} {be[:12]+'/'+bb:28s} {len(pick):>6d} {net_oos*100:>+8.2f}% {cum*100:>+8.2f}%")

    oos = pd.DataFrame(oos_rows)
    oos_valid = oos[oos["n"] > 0]
    if len(oos_valid):
        # средний OOS NET на сделку + кластер по сезону
        # реконструируем сделки OOS для t-стата
        oos_trades = []
        for _, rr in oos.iterrows():
            if rr["n"] == 0 or not isinstance(rr["pick"], str):
                continue
        # проще: пересоберём сделки
        oos_trade_rows = []
        cum = 0.0
        for y in seasons:
            if y < 2014:
                continue
            past = wf[wf["year"] < y]; cur = wf[wf["year"] == y]
            if len(past) < 30 or len(cur) < 3:
                continue
            agg = past.groupby(["ech", "dyb"])["net"].agg(["mean", "count"])
            agg = agg[agg["count"] >= 15]
            if agg.empty:
                continue
            be, bb = agg["mean"].idxmax()
            pick = cur[(cur["ech"] == be) & (cur["dyb"] == bb)]
            for _, p in pick.iterrows():
                oos_trade_rows.append(dict(year=y, clust=f"{y}", net=p["net"]))
        ot = pd.DataFrame(oos_trade_rows)
        if len(ot) >= 8:
            n, m, t, w = cluster_t(ot, "net", "clust")
            print(f"\n  OOS ИТОГ: сделок={n}  средний NET/сделку={m*100:+.2f}%  "
                  f"кластер-t(по сезону)={t:+.1f}  доля>0={w*100:.0f}%")
            # in-sample для сравнения: лучшая комба на ВСЕХ данных
            agg_all = wf.groupby(["ech", "dyb"])["net"].agg(["mean", "count"])
            agg_all = agg_all[agg_all["count"] >= 15]
            be, bb = agg_all["mean"].idxmax()
            ins = wf[(wf["ech"] == be) & (wf["dyb"] == bb)]
            ni, mi, ti, wi = cluster_t(ins, "net")
            print(f"  IN-SAMPLE (та же логика на всех данных, лучшая={be}/{bb}): "
                  f"n={ni} NET={mi*100:+.2f}% t={ti:+.1f}")

    # ---- ПОСЛЕДНИЕ 12 МЕС: дивсезоны 2025-06..2026-06 ----
    print("\n" + "=" * 100)
    print("ПОСЛЕДНИЕ 12 МЕС (ex_date 2025-06-01 .. 2026-06-30): жив ли край сейчас, NET")
    print("=" * 100)
    last = ev[(ev["ex"] >= pd.Timestamp("2025-06-01")) & (ev["ex"] <= pd.Timestamp("2026-06-30"))].copy()
    for wn in WINDOWS:
        col = f"ar_{wn}"
        sub = last[last[col].notna()].copy()
        if len(sub) == 0:
            continue
        n, m, t, w = cluster_t(sub, col)
        sub["net"] = sub[col] - sub["ech"].map(COST).fillna(0.45) / 100.0
        nn, mn, tn, wn2 = cluster_t(sub, "net")
        print(f"  {wn}: n={n} брутто={m*100:+.2f}% NET={mn*100:+.2f}% кл-t(net)={tn:+.1f} >0={w*100:.0f}%")
        # high-yield подвыборка (>6%)
        hy = sub[sub["dyb"] == "ВЫСОКАЯ >6%"]
        if len(hy) >= 5:
            nh, mh, th, whh = cluster_t(hy, "net")
            print(f"     └ ВЫСОКАЯ дивдох >6%: n={nh} NET={mh*100:+.2f}% >0={whh*100:.0f}%")
    # по сезонам в окне последних 12 мес: 2025 и 2026
    print("  по сезонам (main окно, NET):")
    for y in [2025, 2026]:
        sub = last[(last["year"] == y) & last["ar_main_T20_T1"].notna()].copy()
        if len(sub) == 0:
            continue
        sub["net"] = sub["ar_main_T20_T1"] - sub["ech"].map(COST).fillna(0.45) / 100.0
        n, m, t, w = cluster_t(sub, "net")
        print(f"     {y}: n={n} NET={m*100:+.2f}% >0={w*100:.0f}%")

    # ---- OI ПОДТВЕРЖДЕНИЕ ----
    print("\n" + "=" * 100)
    print("OI-ПОДТВЕРЖДЕНИЕ: набирают ли ФИЗ лонг в окне разгона [T-20,T-1]? (норм. на |вход|)")
    print("=" * 100)
    sub = ev.dropna(subset=["dfiz"])
    if len(sub) > 10:
        print(f"  событий с фьючерсом: {len(sub)}  сред Δ ФИЗ={sub['dfiz'].mean()*100:+.1f}%  "
              f"доля набора(>0)={(sub['dfiz']>0).mean()*100:.0f}%")
        # связь набора ФИЗ с разгоном
        s2 = sub.dropna(subset=["ar_main_T20_T1"])
        if len(s2) > 20:
            cc = np.corrcoef(s2["dfiz"], s2["ar_main_T20_T1"])[0, 1]
            print(f"  corr(Δ ФИЗ лонг, разгон AR) = {cc:+.2f}  (>0 = набор ФИЗ совпадает с разгоном)")


if __name__ == "__main__":
    main()
