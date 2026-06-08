"""Калибровка intraday-детектора «резкого движения позиции» (5м/1ч). НЕ прод.

Дневной детектор: ratio = |Δ net за день| / ATR(14 дней), guard'ы npart≥50,
|Δ|/net≥2%, ATR≥0.1%·net. Для intraday масштабы ДРУГИЕ (Δ за 5мин ≈ 0.02-0.1% net),
поэтому окно (в барах), материальность и floor надо подобрать заново.

Считает на реальных данных (interval=5 и 60, 64 ликвидных, последние ~20 торг.дней):
  - распределение |Δ|/net за бар (масштаб материальности),
  - типичный ATR (в контрактах) для floor,
  - частоту сигналов ratio≥T для сетки окон×порогов (сигналов/день/бумага),
  - влияние overnight-skip (Δ через ночь не считаем).

Запуск: DB_URL=... /opt/frame/signals/.venv/bin/python -m signals.research.oi_intraday_calib
"""
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text

INTERVALS = [5, 60]
WINDOWS = {5: [14, 30, 60], 60: [7, 14, 30]}   # окна ATR в БАРАХ
TIERS = [1.5, 2.0, 3.0, 5.0, 7.0, 10.0]
TEST_DAYS = 20
MIN_PART = 50          # ликвидность (как дневной)
SAMPLE_LIMIT = 64


def intraday_assets(conn):
    rows = conn.execute(text(
        "SELECT DISTINCT sectype FROM open_interest "
        "WHERE interval=5 AND tradedate >= CURRENT_DATE - 20 ORDER BY sectype"
    )).fetchall()
    return [r[0] for r in rows][:SAMPLE_LIMIT]


def load(conn, sectype, interval):
    rows = conn.execute(text("""
        SELECT tradedate, tradetime,
               (COALESCE(pos_long,0)+COALESCE(pos_short,0)) AS net,
               (COALESCE(pos_long_num,0)+COALESCE(pos_short_num,0)) AS npart
        FROM open_interest
        WHERE sectype=:s AND clgroup='FIZ' AND interval=:i
          AND tradedate >= CURRENT_DATE - CAST(:d AS INTEGER)
        ORDER BY tradedate, tradetime
    """), {"s": sectype, "i": interval, "d": TEST_DAYS + 5}).fetchall()
    return [(r[0], r[1], int(r[2]), int(r[3])) for r in rows]


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    with engine.connect() as conn:
        assets = intraday_assets(conn)
        print(f"\n=== Intraday-калибровка: {len(assets)} активов, ~{TEST_DAYS} дней ===")

        for interval in INTERVALS:
            rel_samples = []          # |Δ|/net за бар (intraday-материальность)
            atr_rel_samples = []      # ATR/net (для floor)
            ndays = set()
            # частота: (W,T) -> срабатываний;  W -> валидных баров
            flags = defaultdict(int)
            valid = defaultdict(int)
            Wmax = max(WINDOWS[interval])

            for sectype in assets:
                s = load(conn, sectype, interval)
                if len(s) < Wmax + 50:
                    continue
                if st.median([abs(x[2]) for x in s]) < 500:
                    continue
                # Δ ТОЛЬКО внутри одного дня (overnight-skip: первый бар дня без Δ)
                diffs = []            # список (idx_in_series) -> |Δ| или None для overnight
                adiff = [None] * len(s)
                for i in range(1, len(s)):
                    if s[i][0] == s[i - 1][0]:        # та же дата → внутридневной Δ
                        adiff[i] = abs(s[i][2] - s[i - 1][2])
                # скользящий ATR по последним W внутридневным |Δ| (только не-None)
                for i in range(len(s)):
                    if adiff[i] is None:
                        continue
                    net = s[i][2]
                    npart = s[i][3]
                    if npart < MIN_PART:
                        continue
                    # окно: последние W валидных adiff ДО i
                    win = []
                    j = i - 1
                    while j >= 0 and len(win) < Wmax:
                        if adiff[j] is not None:
                            win.append(adiff[j])
                        j -= 1
                    if len(win) < Wmax:
                        continue
                    rel_samples.append(adiff[i] / max(abs(net), 1))
                    ndays.add(s[i][0])
                    for W in WINDOWS[interval]:
                        atr = st.fmean(win[:W])
                        if atr <= 0:
                            continue
                        valid[W] += 1
                        atr_rel_samples.append(atr / max(abs(net), 1))
                        ratio = adiff[i] / atr
                        for T in TIERS:
                            if ratio >= T:
                                flags[(W, T)] += 1

            nday = len(ndays) or 1
            print(f"\n  ──────── interval = {interval} мин ────────")
            if rel_samples:
                rs = sorted(rel_samples)
                p = lambda q: rs[int(q * (len(rs) - 1))]
                print(f"  |Δ|/net за бар: медиана={st.median(rs)*100:.4f}%  "
                      f"p90={p(0.9)*100:.4f}%  p99={p(0.99)*100:.4f}%  (дневной порог был 2%)")
            if atr_rel_samples:
                ar = sorted(atr_rel_samples)
                print(f"  ATR/net: медиана={st.median(ar)*100:.4f}%  "
                      f"p10={ar[int(0.1*len(ar))]*100:.4f}%  (дневной floor был 0.1%)")
            print(f"  Торг.дат в выборке: {nday}")
            print(f"\n  Частота (сигналов/день/бумага) по окну×порогу:")
            print(f"   {'порог':>7}" + "".join(f"{('W='+str(W)):>14}" for W in WINDOWS[interval]))
            n_assets_eff = len([1 for a in assets]) or 1
            for T in TIERS:
                line = f"   {('≥%.1f×' % T):>7}"
                for W in WINDOWS[interval]:
                    v = valid[W] or 1
                    per_bar = flags[(W, T)] / v
                    per_day_per_asset = flags[(W, T)] / nday / n_assets_eff
                    line += f"{('%.2f/д (%.1f%%)' % (per_day_per_asset, per_bar*100)):>14}"
                print(line)
            print(f"\n  «сигналов/день/бумага» — сколько раз в день по ОДНОЙ бумаге сработает порог.")
            print(f"  (для канала ×{n_assets_eff} бумаг). 175 баров/день у 5м — отсюда высокая частота.")


if __name__ == "__main__":
    run()
