"""OI ATR-аномалии — research / backtest. НЕ прод.

ATR-подход к аномалиям позиций (знакомый трейдеру индикатор, без «сигм»):
  TR_позиции = |дневное Δ чистой позиции|
  ATR(N)     = среднее |Δ| за N дней = «обычный дневной шаг» позиции этого актива
  ratio      = |Δ_сегодня| / ATR(N)_вчерашний   → «во сколько раз больше обычного»
  аномалия   = ratio ≥ k  (например 2× или 3×)

ATR считается на ПРЕДЫДУЩЕМ окне (без сегодня) — нет look-ahead.
Liquidity floor: ≥MIN_PART участников (иначе шум на мёртвом активе).

Запуск (хост):
  cd /opt/frame && export DB_URL=$(grep ^DB_URL= .env | cut -d= -f2- | sed 's/@db:/@127.0.0.1:/')
  /opt/frame/signals/.venv/bin/python -m signals.research.oi_atr
"""
from __future__ import annotations
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text
from signals import config
from signals.db import get_asset_name

ATR_N = 14          # окно ATR (классический дефолт)
MIN_PART = 50
K2, K3 = 2.0, 3.0   # пороги «во сколько раз больше обычного»


def load(conn, sectype, clgroup):
    rows = conn.execute(text("""
        SELECT tradedate,
               (COALESCE(pos_long,0)+COALESCE(pos_short,0)) AS net,
               (COALESCE(pos_long_num,0)+COALESCE(pos_short_num,0)) AS npart
        FROM open_interest
        WHERE sectype=:s AND clgroup=:g AND interval=24
        ORDER BY tradedate
    """), {"s": sectype, "g": clgroup}).fetchall()
    return [(r[0], int(r[1]), int(r[2])) for r in rows]


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    agg = defaultdict(int)
    per_asset = defaultdict(lambda: [0, 0, 0])   # sectype -> [days, n2, n3]
    examples = []
    with engine.connect() as conn:
        for sectype in config.OI_ASSETS:
            for clg in ("FIZ", "YUR"):
                try:
                    s = load(conn, sectype, clg)
                except Exception:
                    continue
                if len(s) < ATR_N + 60:
                    continue
                nets = [x[1] for x in s]
                npart = [x[2] for x in s]
                adiff = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
                # ratio для дня i (в индексах adiff): ATR = mean(adiff[i-N..i-1])
                for i in range(ATR_N, len(adiff)):
                    atr = st.fmean(adiff[i - ATR_N:i])
                    if atr <= 0:
                        continue
                    day_idx = i + 1   # adiff[i] = nets[i+1]-nets[i]
                    if npart[day_idx] < MIN_PART:
                        continue
                    ratio = adiff[i] / atr
                    agg["days"] += 1
                    per_asset[sectype][0] += 1
                    if ratio >= K2:
                        agg["n2"] += 1; per_asset[sectype][1] += 1
                    if ratio >= K3:
                        agg["n3"] += 1; per_asset[sectype][2] += 1
                    examples.append((ratio, sectype, clg, s[day_idx][0],
                                     nets[day_idx] - nets[day_idx - 1], nets[day_idx]))

    d = agg["days"] or 1
    print(f"\n=== ATR-аномалии позиций (ATR={ATR_N}д, liquidity≥{MIN_PART}уч), {agg['days']} точек ===")
    print(f"  ratio ≥ {K2:.0f}× обычного: {agg['n2']:5} ({100*agg['n2']/d:.1f}% дней)")
    print(f"  ratio ≥ {K3:.0f}× обычного: {agg['n3']:5} ({100*agg['n3']/d:.1f}% дней)")
    print(f"\n=== Топ-15 самых резких движений (ratio) ===")
    examples.sort(reverse=True)
    for r, s, g, dt, dn, net in examples[:15]:
        print(f"  {s:6}/{g} {dt}  {r:5.1f}× обычного   Δ={dn:+,}  net={net:+,}  ({get_asset_name(s) or ''})")
    # стабильность частоты по ликвидным
    print(f"\n=== Частота ≥{K3:.0f}× по бумагам (топ-12 по активности) ===")
    liq = sorted(per_asset.items(), key=lambda kv: kv[1][0], reverse=True)[:12]
    for sec, (days, n2, n3) in liq:
        print(f"  {sec:6} {get_asset_name(sec) or '':20} ≥2×: {100*n2/days:4.1f}%   ≥3×: {100*n3/days:4.1f}%")


if __name__ == "__main__":
    run()
