"""Где позиционирование (OI) лучше всего предсказывает цену — ранжирование по активам/секторам.

Сигналы (физлица = «толпа», юрлица — зеркало):
  COT  — уровень COT-индекса физ (WillCo = net_физ/OI, мин-макс окно W → 0..100).
         Контрарность: высокий COT (толпа в лонге) → цена вниз  ⇒  IC < 0.
  FLOW — изменение WillCo физ за FLOW_LAG дней (поток позиционирования).
Метрика — Spearman IC (ранговая корреляция) сигнал ↔ forward-доходность через H дней.
Группы — instruments.group (Акции / Сырьё / Валюта / Индексы).

ВАЖНО (читать вместе с цифрами):
  • IN-SAMPLE, окна forward ПЕРЕКРЫВАЮТСЯ (дневные наблюдения, H-дн. доходность)
    → автокорреляция завышает кажущуюся значимость. Это ОТНОСИТЕЛЬНОЕ ранжирование
    «где связь сильнее», а НЕ доказательство торгуемого эджа.
  • |IC| ~ 0.05 — шум; ~0.10 — заметно; ~0.15+ — сильно для дневных данных.
Запуск: DB_URL=... /opt/frame/signals/.venv/bin/python -m signals.research.oi_predict_by_asset
"""
import os
from collections import defaultdict

from sqlalchemy import create_engine, text

from signals import config
from signals.research.cot_index import load_oi, load_price, cot_index, asset_edge

W = 110            # окно COT-индекса (~22 недели, ATAS-стандарт)
FWD = [10, 20]     # горизонты forward-доходности (торг. дней)
FLOW_LAG = 5       # окно потока
MIN_OBS = 150      # минимум наблюдений для IC


def _rank(v):
    order = sorted(range(len(v)), key=lambda i: v[i])
    r = [0.0] * len(v)
    i = 0
    while i < len(v):
        j = i
        while j + 1 < len(v) and v[order[j + 1]] == v[order[i]]:
            j += 1
        avg = (i + j) / 2.0
        for k in range(i, j + 1):
            r[order[k]] = avg
        i = j + 1
    return r


def _pearson(a, b):
    n = len(a)
    if n < 2:
        return None
    ma = sum(a) / n
    mb = sum(b) / n
    num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    da = sum((a[i] - ma) ** 2 for i in range(n)) ** 0.5
    db = sum((b[i] - mb) ** 2 for i in range(n)) ** 0.5
    return num / (da * db) if da > 0 and db > 0 else None


def spearman(xs, ys):
    if len(xs) < MIN_OBS:
        return None
    return _pearson(_rank(xs), _rank(ys))


def group_map(conn):
    rows = conn.execute(text(
        "SELECT DISTINCT sectype, \"group\" FROM instruments "
        "WHERE type='futures' AND \"group\" IS NOT NULL"
    )).fetchall()
    return {r[0]: r[1] for r in rows}


def analyse(s_oi, price):
    dates = [x[0] for x in s_oi]
    tot = [x[3] for x in s_oi]
    willf = [s_oi[i][1] / tot[i] if tot[i] else 0 for i in range(len(s_oi))]
    idx = cot_index(willf, W)
    out = {}
    n_eff = 0
    for H in FWD:
        cs, rs = [], []
        for i in range(len(s_oi) - H):
            p0 = price.get(dates[i])
            pf = price.get(dates[i + H])
            if not p0 or not pf or idx[i] is None:
                continue
            cs.append(idx[i])
            rs.append((pf - p0) / p0 * 100)
        out[("cot", H)] = spearman(cs, rs) if cs else None
        if H == FWD[0]:
            n_eff = len(cs)
        fs, rs2 = [], []
        for i in range(FLOW_LAG, len(s_oi) - H):
            p0 = price.get(dates[i])
            pf = price.get(dates[i + H])
            if not p0 or not pf:
                continue
            fs.append(willf[i] - willf[i - FLOW_LAG])
            rs2.append((pf - p0) / p0 * 100)
        out[("flow", H)] = spearman(fs, rs2) if fs else None
    out["n"] = n_eff
    return out


def fmt(x):
    return f"{x:+.3f}" if x is not None else "  —  "


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
            if not price or len(s_oi) < W + max(FWD) + MIN_OBS:
                continue
            r = analyse(s_oi, price)
            if r["n"] < MIN_OBS:
                continue
            try:
                edge, _, _, _, _ = asset_edge(s_oi, price, W, H=20)
            except Exception:
                edge = None
            rows.append({
                "sec": sectype, "grp": gmap.get(sectype, "?"), "n": r["n"],
                "cot10": r[("cot", 10)], "cot20": r[("cot", 20)],
                "flow10": r[("flow", 10)], "flow20": r[("flow", 20)],
                "edge": edge,
            })

    def strength(d):
        a, b = d["cot10"], d["cot20"]
        if a is None or b is None:
            return -1
        # сила = средний |IC| при СОГЛАСОВАННОМ знаке (иначе штраф)
        consistent = (a < 0) == (b < 0)
        s = (abs(a) + abs(b)) / 2
        return s if consistent else s * 0.3

    rows.sort(key=strength, reverse=True)

    print(f"\n=== Предсказательная сила позиционирования по активам ===")
    print(f"(W={W}, IC=Spearman физ-COT↔fwd; знак<0 = КОНТРАРНОСТЬ физ работает; {len(rows)} активов)")
    print(f"{'актив':>7} {'сектор':>8} {'n':>5} {'IC cot10':>9} {'IC cot20':>9} {'IC flow10':>10} {'согл':>5}")
    for d in rows[:25]:
        cons = "да" if (d["cot10"] is not None and d["cot20"] is not None and (d["cot10"] < 0) == (d["cot20"] < 0)) else "—"
        print(f"{d['sec']:>7} {d['grp']:>8} {d['n']:>5} {fmt(d['cot10']):>9} {fmt(d['cot20']):>9} {fmt(d['flow10']):>10} {cons:>5}")

    # ── агрегат по секторам ──
    g = defaultdict(list)
    for d in rows:
        if d["cot20"] is not None:
            g[d["grp"]].append(d)
    print(f"\n=== По секторам (контрарность физ, IC cot20) ===")
    print(f"{'сектор':>8} {'активов':>8} {'ср.IC':>8} {'ср.|IC|':>9} {'%контр':>8}")
    gsum = []
    for grp, ds in g.items():
        ics = [d["cot20"] for d in ds]
        mean_ic = sum(ics) / len(ics)
        mean_abs = sum(abs(x) for x in ics) / len(ics)
        pct_contra = 100 * sum(1 for x in ics if x < 0) / len(ics)
        gsum.append((grp, len(ds), mean_ic, mean_abs, pct_contra))
    for grp, n, mi, ma, pc in sorted(gsum, key=lambda t: t[2]):  # по ср.IC (контрарность = отрицательнее)
        print(f"{grp:>8} {n:>8} {mi:>+8.3f} {ma:>9.3f} {pc:>7.0f}%")

    # ── сводка распределения (честность) ──
    all_ic = [d["cot20"] for d in rows if d["cot20"] is not None]
    import statistics as st
    print(f"\n=== Распределение IC cot20 по всем {len(all_ic)} активам ===")
    print(f"медиана={st.median(all_ic):+.3f}  среднее={st.fmean(all_ic):+.3f}  "
          f"|IC|>0.10: {sum(1 for x in all_ic if abs(x) > 0.10)} активов  "
          f"|IC|>0.15: {sum(1 for x in all_ic if abs(x) > 0.15)}")


if __name__ == "__main__":
    run()
