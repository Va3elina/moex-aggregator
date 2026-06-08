"""Где контрарность ФИЗЛИЦ подтверждается чаще всего. НЕ прод, in-sample.

Гипотеза (что просил Вадим): физики в лонге (COT-индекс физ ≥80, толпа max long)
→ цена ЧАЩЕ падает; физики в шорте (≤20) → цена ЧАЩЕ растёт.

Метрика на актив:
  • hit-rate — доля экстремум-событий, где направление ПОДТВЕРДИЛОСЬ
    (hi-экстремум → fwd<0, lo-экстремум → fwd>0). 50% = монетка.
  • edge (п.п.) = ср.fwd(lo) − ср.fwd(hi). >0 = контрарность работает.
  • hi→fwd — средняя forward-доходность после «толпа в лонге» (контр = отрицательная).
Горизонты 10 и 20 дней объединены. Группировка по сектору + поиск принципа.

ВАЖНО: окна forward перекрываются (автокорреляция завышает значимость) — это
ОТНОСИТЕЛЬНОЕ ранжирование «где надёжнее», а не торгуемый грааль.
Запуск: DB_URL=... /opt/frame/signals/.venv/bin/python -m signals.research.oi_contrarian_fiz
"""
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text

from signals import config
from signals.research.cot_index import load_oi, load_price, cot_index

W = 110          # окно COT-индекса (~22 недели)
FWD = [10, 20]   # горизонты forward
HI, LO = 80, 20  # пороги экстремума COT
MIN_EVENTS = 40  # минимум суммарных экстремум-событий (по обоим горизонтам)


def group_map(conn):
    rows = conn.execute(text(
        "SELECT DISTINCT sectype, \"group\" FROM instruments "
        "WHERE type='futures' AND \"group\" IS NOT NULL"
    )).fetchall()
    return {r[0]: r[1] for r in rows}


def events(s_oi, price, H):
    dates = [x[0] for x in s_oi]
    tot = [x[3] for x in s_oi]
    willf = [s_oi[i][1] / tot[i] if tot[i] else 0 for i in range(len(s_oi))]
    idx = cot_index(willf, W)
    hi, lo = [], []
    for i in range(len(s_oi) - H):
        p0 = price.get(dates[i])
        pf = price.get(dates[i + H])
        if not p0 or not pf or idx[i] is None:
            continue
        ret = (pf - p0) / p0 * 100
        if idx[i] >= HI:
            hi.append(ret)
        elif idx[i] <= LO:
            lo.append(ret)
    return hi, lo


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    rows = []
    with engine.connect() as conn:
        gmap = group_map(conn)
        for sectype in config.OI_ASSETS:
            try:
                s_oi = load_oi(conn, sectype)
                price = load_price(conn, sectype)
            except Exception:
                continue
            if not price or len(s_oi) < W + max(FWD) + 50:
                continue
            hi_all, lo_all = [], []
            for H in FWD:
                h, l = events(s_oi, price, H)
                hi_all += h
                lo_all += l
            n = len(hi_all) + len(lo_all)
            if n < MIN_EVENTS:
                continue
            hits = sum(1 for r in hi_all if r < 0) + sum(1 for r in lo_all if r > 0)
            hit_rate = 100 * hits / n
            hi_mean = st.fmean(hi_all) if hi_all else None
            lo_mean = st.fmean(lo_all) if lo_all else None
            edge = (lo_mean - hi_mean) if (hi_mean is not None and lo_mean is not None) else 0.0
            rows.append({
                "sec": sectype, "grp": gmap.get(sectype, "?"), "n": n,
                "hit": hit_rate, "edge": edge,
                "hi_mean": hi_mean if hi_mean is not None else 0.0,
            })

    rows.sort(key=lambda d: (d["hit"], d["edge"]), reverse=True)
    print(f"\n=== Контрарность физлиц: где ЧАЩЕ подтверждается "
          f"(W={W}, экстремум ≥{HI}/≤{LO}, гориз. 10+20дн, {len(rows)} активов) ===")
    print(f"{'актив':>8}{'сектор':>9}{'событий':>8}{'hit-rate':>10}{'edge п.п.':>11}{'hi→fwd %':>10}")
    for d in rows[:25]:
        print(f"{d['sec']:>8}{d['grp']:>9}{d['n']:>8}{d['hit']:>9.0f}%"
              f"{d['edge']:>10.2f}{d['hi_mean']:>10.2f}")

    g = defaultdict(list)
    for d in rows:
        g[d["grp"]].append(d)
    print(f"\n=== По секторам (где контрарность физлиц надёжнее) ===")
    print(f"{'сектор':>9}{'активов':>8}{'ср.hit':>9}{'ср.edge':>9}{'%(hit>55)':>11}")
    for grp, ds in sorted(g.items(), key=lambda kv: -st.fmean([d["hit"] for d in kv[1]])):
        mh = st.fmean([d["hit"] for d in ds])
        me = st.fmean([d["edge"] for d in ds])
        pgt = 100 * sum(1 for d in ds if d["hit"] > 55) / len(ds)
        print(f"{grp:>9}{len(ds):>8}{mh:>8.0f}%{me:>9.2f}{pgt:>10.0f}%")

    all_hit = [d["hit"] for d in rows]
    strong = [d["sec"] for d in rows if d["hit"] > 57 and d["edge"] > 0]
    print(f"\nМедиана hit-rate: {st.median(all_hit):.0f}% (50% = монетка). "
          f"Активов hit>55%: {sum(1 for h in all_hit if h > 55)}/{len(all_hit)}.")
    print(f"Надёжные (hit>57% и edge>0): {', '.join(strong) if strong else 'нет'}")


if __name__ == "__main__":
    run()
