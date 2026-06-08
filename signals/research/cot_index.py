"""COT Index — research / backtest на данных MOEX (физлица/юрлица OI). НЕ прод.

Считает индекс Вильямса (COT Index) по net-позиции каждой категории:
    idx = (net − min(net,W)) / (max(net,W) − min(net,W)) × 100   →  0..100%
  100% = самый бычий net за окно W, 0% = самый медвежий. Экстремум: ≥80 / ≤20.

Два варианта:
  STD    — min-max по сырой net.
  WILLCO — сначала net/total_OI (доля рынка), потом min-max. Устойчив к росту OI.
           total_OI = сумма лонгов обеих категорий (физ_long + юр_long = весь рынок).

Проверяет КОНТРАРНОСТЬ на наших данных (главный вопрос исследования — не переносить
вывод CFTC вслепую): на днях экстремума считает forward-доходность цены через H дней.
  Контрарность физлиц: idx_FIZ≥80 (толпа max лонг) → ждём forward-return < 0.
  Подтверждение юрлиц:  idx_YUR≥80 → ждём forward-return > 0.

Запуск (хост):
  cd /opt/frame && export DB_URL=$(grep ^DB_URL= .env | cut -d= -f2- | sed 's/@db:/@127.0.0.1:/')
  /opt/frame/signals/.venv/bin/python -m signals.research.cot_index
"""
from __future__ import annotations
import os
from collections import defaultdict

from sqlalchemy import create_engine, text

WINDOWS = {"1г": 250, "2г": 500, "3г": 750}
EXTREME_HI, EXTREME_LO = 90, 10   # пороги Вильямса/ATAS (строгие): >90 / <10
FWD = (10, 20)                 # горизонты forward-доходности (торг. дней)
PRIMARY_W = 500               # окно для проверки контрарности (2 года)
# ATAS/MOEX-стандарт: короткие окна в ДНЯХ (≈1 и 3 торговых месяца), не годы.
W_SHORT, W_MED = 22, 66
FWD_SHORT = 10                # forward под короткое окно
# Ликвидные активы для проверки цены (price-запрос тяжёлый — ограничиваем набор)
LIQUID = ["SR", "GZ", "Si", "MX", "RI", "VB", "LK", "GD", "BR", "NG", "SV", "MM"]


def cot_index(vals, W):
    """Список COT-индексов (или None) для ряда vals с окном W."""
    out = []
    for i in range(len(vals)):
        if i < W - 1:
            out.append(None)
            continue
        win = vals[i - W + 1:i + 1]
        lo, hi = min(win), max(win)
        out.append(None if hi == lo else (vals[i] - lo) / (hi - lo) * 100.0)
    return out


def load_oi(conn, sectype):
    """{date: (net_fiz, net_yur, total_oi)} по interval=24, отсортировано по дате."""
    rows = conn.execute(text("""
        SELECT tradedate, clgroup,
               COALESCE(pos_long,0) AS pl, COALESCE(pos_short,0) AS ps
        FROM open_interest
        WHERE sectype = :s AND interval = 24 AND clgroup IN ('FIZ','YUR')
        ORDER BY tradedate
    """), {"s": sectype}).fetchall()
    by_date = {}
    for d, g, pl, ps in rows:
        by_date.setdefault(d, {})[g] = (int(pl), int(ps))
    series = []
    for d in sorted(by_date):
        v = by_date[d]
        if "FIZ" not in v or "YUR" not in v:
            continue
        fl, fs = v["FIZ"]; yl, ys = v["YUR"]
        net_f, net_y = fl + fs, yl + ys
        total = fl + yl          # сумма лонгов = весь открытый интерес рынка
        series.append((d, net_f, net_y, total))
    return series


def load_price(conn, sectype):
    """{date: close} — фронт-контракт (макс volume за день), interval=24. Индексный запрос."""
    rows = conn.execute(text("""
        SELECT begin_time::date AS d, close, COALESCE(volume,0) AS v
        FROM candles
        WHERE sec_id IN (SELECT sec_id FROM instruments WHERE sectype = :s AND type='futures')
          AND interval = 24 AND close > 0
    """), {"s": sectype}).fetchall()
    best = {}
    for d, c, v in rows:
        if d not in best or v > best[d][1]:
            best[d] = (float(c), float(v))
    return {d: c for d, (c, _) in best.items()}


import statistics as st
from signals.db import get_asset_name


def asset_edge(s_oi, price, W, H=20):
    """Per-asset контрарность физлиц (WillCo, окно W, forward H дней).
    edge = ср.fwd(лоу≤20) − ср.fwd(хай≥80). >0 = контрарность работает
    (толпа в шорте → цена растёт сильнее, в лонге → слабее)."""
    dates = [x[0] for x in s_oi]
    tot = [x[3] for x in s_oi]
    willf = [s_oi[i][1] / tot[i] if tot[i] else 0 for i in range(len(s_oi))]
    idx = cot_index(willf, W)
    hi_r, lo_r = [], []
    for i in range(len(s_oi) - H):
        p0 = price.get(dates[i]); pf = price.get(dates[i + H])
        if not p0 or not pf:
            continue
        v = idx[i]
        if v is None:
            continue
        ret = (pf - p0) / p0 * 100
        if v >= EXTREME_HI:
            hi_r.append(ret)
        elif v <= EXTREME_LO:
            lo_r.append(ret)
    fh = st.fmean(hi_r) if hi_r else None
    fl = st.fmean(lo_r) if lo_r else None
    edge = (fl - fh) if (fh is not None and fl is not None) else None
    return edge, fh, fl, len(hi_r), len(lo_r)


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    from signals import config
    rows = []
    with engine.connect() as conn:
        for sectype in config.OI_ASSETS:
            try:
                s = load_oi(conn, sectype)
            except Exception as e:
                print(f"[skip] {sectype}: {e}")
                continue
            if len(s) <= 760 + 20:    # нужно ≥3 года истории + горизонт
                continue
            try:
                price = load_price(conn, sectype)
            except Exception:
                price = {}
            if not price:
                continue
            eS = asset_edge(s, price, W_SHORT, H=FWD_SHORT)   # ATAS 22д
            eM = asset_edge(s, price, W_MED, H=FWD_SHORT)      # 66д (3 мес)
            if eS[0] is None or (eS[3] + eS[4]) < 60:   # мало экстремум-дней → пропуск
                continue
            rows.append((sectype, get_asset_name(sectype) or "", eS, eM))

    # ранжируем по edge короткого окна (ATAS 22д)
    rows.sort(key=lambda r: (r[2][0] if r[2][0] is not None else -999), reverse=True)
    print(f"\n=== Контрарность физлиц per-asset — MOEX/ATAS-стиль (WillCo, пороги 90/10, fwd 10д) ===")
    print(f"   edge = ср.fwd(лоу≤10) − ср.fwd(хай≥90).  >0 → контрарность работает (толпа неправа)\n")
    print(f"{'тикер':8}{'имя':22}{'edge22д':>9}{'edge66д':>9}{'fwd хай':>8}{'fwd лоу':>8}{'n хай/лоу':>12}")
    wS = wM = 0
    for sec, nm, eS, eM in rows:
        edS = f"{eS[0]:+.2f}" if eS[0] is not None else "—"
        edM = f"{eM[0]:+.2f}" if eM[0] is not None else "—"
        fh = f"{eS[2]:+.2f}" if eS[2] is not None else "—"
        fl = f"{eS[1]:+.2f}" if eS[1] is not None else "—"
        if eS[0] is not None and eS[0] > 0:
            wS += 1
        if eM[0] is not None and eM[0] > 0:
            wM += 1
        print(f"{sec:8}{nm[:21]:22}{edS:>9}{edM:>9}{fh:>8}{fl:>8}{str(eS[3])+'/'+str(eS[4]):>12}")
    n = len(rows)
    print(f"\n  Контрарность РАБОТАЕТ (edge>0): окно 22д — {wS}/{n}, окно 66д — {wM}/{n} бумаг.")
    edgesS = [r[2][0] for r in rows if r[2][0] is not None]
    if edgesS:
        print(f"  Медианный edge (22д): {st.median(edgesS):+.2f} п.п.;  среднее: {st.fmean(edgesS):+.2f} п.п.")
    print("  (сравни с длинным окном: было 20/33 при 750д/80-20)")


if __name__ == "__main__":
    run()
