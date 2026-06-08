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

ATR_WINDOWS = [5, 14, 30]   # «обычный шаг» за: неделю / 3 недели / месяц
TIERS = [2.0, 3.0, 5.0]     # уровни: заметно / сильно / экстремально
MIN_PART = 50
MIN_REL = 0.02              # |Δ|/|net| ≥ 2%
GAP_MAX = 5                 # пропуск дня после разрыва >5 дней (закрытие рынка → артефакт)
ATR_FLOOR_REL = 0.001       # ATR ≥ 0.1% позиции, иначе «заморожено» → ratio бессмыслен


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
    Wmax = max(ATR_WINDOWS)
    days_per_w = defaultdict(int)              # W -> сколько валидных (день,актив) точек
    hits = defaultdict(int)                    # (W, tier) -> срабатываний
    examples = []                              # для окна 14
    with engine.connect() as conn:
        for sectype in config.OI_ASSETS:
            for clg in ("FIZ", "YUR"):
                try:
                    s = load(conn, sectype, clg)
                except Exception:
                    continue
                if len(s) < Wmax + 60:
                    continue
                dates = [x[0] for x in s]
                nets = [x[1] for x in s]
                npart = [x[2] for x in s]
                adiff = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
                for i in range(Wmax, len(adiff)):
                    day_idx = i + 1                       # adiff[i] = nets[i+1]-nets[i]
                    if npart[day_idx] < MIN_PART:
                        continue
                    if (dates[day_idx] - dates[day_idx - 1]).days > GAP_MAX:
                        continue                          # разрыв (закрытие рынка) → артефакт
                    net = nets[day_idx]
                    if adiff[i] / max(abs(net), 1) < MIN_REL:
                        continue                          # immaterial
                    for W in ATR_WINDOWS:
                        atr = st.fmean(adiff[i - W:i])
                        if atr < ATR_FLOOR_REL * max(abs(net), 1) or atr <= 0:
                            continue                      # «заморожено» — ratio бессмыслен
                        ratio = adiff[i] / atr
                        days_per_w[W] += 1
                        for t in TIERS:
                            if ratio >= t:
                                hits[(W, t)] += 1
                        if W == 14:
                            examples.append((ratio, sectype, clg, dates[day_idx],
                                             nets[day_idx] - nets[day_idx - 1], net))

    print("\n=== Сетка: частота срабатываний ATR-аномалии (% дней) ===")
    hdr = "  ".join(f"≥{int(t)}×" for t in TIERS)
    print(f"{'ATR-окно':12}{'точек':>9}   {hdr}")
    for W in ATR_WINDOWS:
        dp = days_per_w[W] or 1
        cells = "  ".join(f"{100*hits[(W,t)]/dp:5.1f}%" for t in TIERS)
        print(f"{str(W)+'д':12}{days_per_w[W]:>9}   {cells}")
    print("\n  Идея уровней: ≥2× «заметно» · ≥3× «сильно» · ≥5× «экстремально».")
    print(f"\n=== Топ-15 резких движений (окно 14д, с guard'ами — закрытия отсеяны) ===")
    examples.sort(reverse=True)
    for r, s, g, dt, dn, net in examples[:15]:
        print(f"  {s:6}/{g} {dt}  {r:5.1f}× обычного   Δ={dn:+,}  net={net:+,}  ({get_asset_name(s) or ''})")


if __name__ == "__main__":
    run()
