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
EXTREME_HI, EXTREME_LO = 80, 20
FWD = (10, 20)                 # горизонты forward-доходности (торг. дней)
PRIMARY_W = 500               # окно для проверки контрарности (2 года)
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


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    freq = defaultdict(lambda: defaultdict(int))   # freq[window][grp_var] = extremes
    total_pts = defaultdict(int)
    # контрарность: суммируем forward-returns по бакетам
    fwd = defaultdict(lambda: defaultdict(list))   # fwd[grp][bucket] -> list of returns @ H=20

    from signals import config
    with engine.connect() as conn:
        for sectype in config.OI_ASSETS:
            try:
                s = load_oi(conn, sectype)
            except Exception as e:
                print(f"[skip oi] {sectype}: {e}")
                continue
            if len(s) < 260:
                continue
            net_f = [x[1] for x in s]
            net_y = [x[2] for x in s]
            tot = [x[3] for x in s]
            willf = [net_f[i] / tot[i] if tot[i] else 0 for i in range(len(s))]
            wily = [net_y[i] / tot[i] if tot[i] else 0 for i in range(len(s))]

            # частота экстремумов по окнам/вариантам
            for wn, W in WINDOWS.items():
                if len(s) <= W:
                    continue
                for name, vals in (("FIZ-std", net_f), ("YUR-std", net_y),
                                   ("FIZ-will", willf), ("YUR-will", wily)):
                    idx = cot_index(vals, W)
                    for v in idx:
                        if v is None:
                            continue
                        total_pts[(wn, name)] += 1
                        if v >= EXTREME_HI or v <= EXTREME_LO:
                            freq[wn][name] += 1

    # контрарность — отдельно по ликвидным (нужна цена)
    with engine.connect() as conn:
        for sectype in LIQUID:
            try:
                s = load_oi(conn, sectype)
                price = load_price(conn, sectype)
            except Exception as e:
                print(f"[skip val] {sectype}: {e}")
                continue
            if len(s) <= PRIMARY_W + max(FWD):
                continue
            dates = [x[0] for x in s]
            idx_f = cot_index([x[1] for x in s], PRIMARY_W)
            idx_y = cot_index([x[2] for x in s], PRIMARY_W)
            for i in range(len(s) - max(FWD)):
                p0 = price.get(dates[i])
                if not p0:
                    continue
                pf = price.get(dates[i + 20])
                if not pf:
                    continue
                ret = (pf - p0) / p0 * 100
                for grp, idx in (("FIZ", idx_f), ("YUR", idx_y)):
                    v = idx[i]
                    if v is None:
                        continue
                    if v >= EXTREME_HI:
                        fwd[grp]["хай(≥80)"].append(ret)
                    elif v <= EXTREME_LO:
                        fwd[grp]["лоу(≤20)"].append(ret)
                    else:
                        fwd[grp]["норма"].append(ret)

    # отчёт
    print("\n=== Частота экстремумов COT-индекса (≥80 или ≤20), % дней ===")
    print(f"{'окно':6} {'FIZ-std':>9} {'YUR-std':>9} {'FIZ-will':>9} {'YUR-will':>9}")
    for wn in WINDOWS:
        cells = []
        for name in ("FIZ-std", "YUR-std", "FIZ-will", "YUR-will"):
            tp = total_pts[(wn, name)]
            cells.append(f"{100*freq[wn][name]/tp:.1f}%" if tp else "—")
        print(f"{wn:6} {cells[0]:>9} {cells[1]:>9} {cells[2]:>9} {cells[3]:>9}")

    print(f"\n=== Контрарность: forward-доходность цены через 20 дн (окно {PRIMARY_W}д, {len(LIQUID)} ликв.) ===")
    print(f"{'группа/бакет':18} {'ср.fwd%':>9} {'медиана%':>9} {'дней':>7}  трактовка")
    import statistics as st
    for grp in ("FIZ", "YUR"):
        for bucket in ("хай(≥80)", "норма", "лоу(≤20)"):
            xs = fwd[grp][bucket]
            if not xs:
                continue
            m = st.fmean(xs); md = st.median(xs)
            print(f"  {grp}/{bucket:12} {m:>+8.2f} {md:>+8.2f} {len(xs):>7}")
    print("\n  Контрарность физлиц = на хай(≥80) ждём ср.fwd < 0, на лоу(≤20) > 0.")
    print("  Подтверждение юрлиц = на хай(≥80) ждём ср.fwd > 0, на лоу(≤20) < 0.")


if __name__ == "__main__":
    run()
