"""Аудит: ATR-аномалии позиций физлиц (дневные OI) vs движение ЦЕНЫ. НЕ прод.

Вопрос: когда позиция физлиц двигается резко (|Δ| ≥ k·ATR), цена тоже двигается?
И в какую сторону (физлица купили → цена вверх/вниз сегодня и через 5 дней)?

1 день (interval=24 — есть у всех OI-активов). Фильтр ликвидности: ≥MIN_PART
участников И медиана |net| ≥ MIN_MED_NET (отсев тонких бумаг типа пшеницы).
"""
from __future__ import annotations
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text
from signals import config
from signals.db import get_asset_name
from signals.research.cot_index import load_price   # фронт-контракт дневная цена

ATR_N = 14
MIN_PART = 50
MIN_REL = 0.02
GAP_MAX = 5
ATR_FLOOR_REL = 0.001
MIN_MED_NET = 500       # абсолютный фильтр ликвидности
TIERS = [2.0, 3.0, 5.0]
FWD = 5                 # горизонт forward-цены (торг. дней)


def load_fiz(conn, sectype):
    rows = conn.execute(text("""
        SELECT tradedate,
               (COALESCE(pos_long,0)+COALESCE(pos_short,0)) AS net,
               (COALESCE(pos_long_num,0)+COALESCE(pos_short_num,0)) AS npart
        FROM open_interest WHERE sectype=:s AND clgroup='FIZ' AND interval=24
        ORDER BY tradedate
    """), {"s": sectype}).fetchall()
    return [(r[0], int(r[1]), int(r[2])) for r in rows]


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    # |ход цены| в тот день по уровням аномалии + базовая (обычные дни)
    absday = defaultdict(list)          # tier -> [|ret_day|]
    base_absday = []
    # направление: физлица купили(+)/продали(−) при аномалии ≥3× → цена сегодня/+5д
    dir_day = defaultdict(list)         # "купили"/"продали" -> [ret_day]
    dir_fwd = defaultdict(list)         # "купили"/"продали" -> [ret_fwd]
    # РАСХОЖДЕНИЕ: на аномалии ≥3× цена стоит / слабо / двинулась + forward 10д
    div_dist = defaultdict(int)         # "стоит(<0.5%)"/"слабо"/"двинулась(≥1.5%)" -> n
    div_fwd = defaultdict(list)         # (price_bucket, side) -> [fwd10]
    examples = []
    n_assets = 0
    with engine.connect() as conn:
        for sectype in config.OI_ASSETS:
            try:
                s = load_fiz(conn, sectype)
            except Exception:
                continue
            if len(s) < ATR_N + 80:
                continue
            if st.median([abs(x[1]) for x in s]) < MIN_MED_NET:
                continue                                # тонкая бумага — пропуск
            try:
                price = load_price(conn, sectype)
            except Exception:
                price = {}
            if not price:
                continue
            n_assets += 1
            dates = [x[0] for x in s]; nets = [x[1] for x in s]; npart = [x[2] for x in s]
            adiff = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
            for i in range(ATR_N, len(adiff)):
                di = i + 1
                if npart[di] < MIN_PART:
                    continue
                if (dates[di] - dates[di - 1]).days > GAP_MAX:
                    continue
                net = nets[di]
                if adiff[i] / max(abs(net), 1) < MIN_REL:
                    continue
                atr = st.fmean(adiff[i - ATR_N:i])
                if atr <= 0 or atr < ATR_FLOOR_REL * max(abs(net), 1):
                    continue
                ratio = adiff[i] / atr
                p0 = price.get(dates[di]); pprev = price.get(dates[di - 1])
                if not p0 or not pprev:
                    continue
                ret_day = (p0 - pprev) / pprev * 100
                ret_fwd = None
                if di + FWD < len(s):
                    pf = price.get(dates[di + FWD])
                    if pf:
                        ret_fwd = (pf - p0) / p0 * 100
                ret_fwd10 = None
                if di + 10 < len(s):
                    pf10 = price.get(dates[di + 10])
                    if pf10:
                        ret_fwd10 = (pf10 - p0) / p0 * 100
                dn = nets[di] - nets[di - 1]            # знак: физлица купили(+)/продали(−)

                if ratio < 2:
                    base_absday.append(abs(ret_day))
                for t in TIERS:
                    if ratio >= t:
                        absday[t].append(abs(ret_day))
                if ratio >= 3:
                    side = "купили(+)" if dn > 0 else "продали(−)"
                    dir_day[side].append(ret_day)
                    if ret_fwd is not None:
                        dir_fwd[side].append(ret_fwd)
                    # расхождение позиция/цена
                    a = abs(ret_day)
                    pb = "стоит(<0.5%)" if a < 0.5 else ("двинулась(≥1.5%)" if a >= 1.5 else "слабо")
                    div_dist[pb] += 1
                    if ret_fwd10 is not None:
                        div_fwd[(pb, side)].append(ret_fwd10)
                if ratio >= 5:
                    examples.append((ratio, sectype, dates[di], dn, ret_day, ret_fwd))

    print(f"\n=== Аудит ATR-аномалий позиций vs цена ({n_assets} ликвидных бумаг, daily) ===\n")
    print("1) СОВПАДАЮТ ли резкие движения позиций с ходом цены В ТОТ ЖЕ ДЕНЬ?")
    print(f"   средний |ход цены| за день:")
    b = st.fmean(base_absday) if base_absday else 0
    print(f"     обычный день (<2×):     {b:.2f}%   (n={len(base_absday)})")
    for t in TIERS:
        xs = absday[t]
        if xs:
            print(f"     аномалия ≥{int(t)}×:          {st.fmean(xs):.2f}%   (n={len(xs)})")
    print("   → если число растёт с уровнем, резкие движения позиций идут ВМЕСТЕ с движением цены.\n")

    print("2) НАПРАВЛЕНИЕ (аномалия ≥3×): когда физлица резко КУПИЛИ/ПРОДАЛИ — куда цена?")
    print(f"   {'действие физлиц':16}{'цена в тот день':>17}{'цена через 5д':>16}{'n':>8}")
    for side in ("купили(+)", "продали(−)"):
        dd = dir_day[side]; df = dir_fwd[side]
        if dd:
            md = st.fmean(dd); mf = st.fmean(df) if df else 0
            print(f"   {side:16}{md:>+15.2f}% {mf:>+14.2f}% {len(dd):>8}")
    print("   (если физлица купили → цена сегодня + : позиции и цена идут вместе.")
    print("    если через 5д знак МЕНЯЕТСЯ на противоположный → контрарность.)\n")

    print("3) А МОЖЕТ ли цена НЕ двинуться при резком движении позиций? (аномалии ≥3×)")
    tot = sum(div_dist.values()) or 1
    for pb in ("стоит(<0.5%)", "слабо", "двинулась(≥1.5%)"):
        print(f"     цена {pb:18} {div_dist[pb]:6}  ({100*div_dist[pb]/tot:.1f}%)")
    print("   → «стоит» = тихая перекладка позиций без следа на цене (accumulation).\n")

    print("4) Предсказывает ли ТИХАЯ перекладка (цена стоит) что-то через 10 дней?")
    print(f"   {'сценарий':40}{'цена +10д':>12}{'n':>8}")
    for pb in ("стоит(<0.5%)", "двинулась(≥1.5%)"):
        for side in ("купили(+)", "продали(−)"):
            xs = div_fwd[(pb, side)]
            if xs:
                print(f"   цена {pb:14} + физлица {side:12}{st.fmean(xs):>+10.2f}% {len(xs):>8}")
    print("   (тихо физлица купили → если +10д < 0 = контрарно; продали → если +10д > 0 = контрарно)\n")

    print(f"=== Примеры экстремальных (≥5×) движений и что было с ценой ===")
    examples.sort(reverse=True)
    for r, s, dt, dn, rd, rf in examples[:15]:
        act = "купили" if dn > 0 else "продали"
        rf_s = f"{rf:+.1f}%" if rf is not None else "—"
        print(f"  {s:6} {dt}  {r:5.1f}× ({act} {dn:+,})  цена день {rd:+.1f}%  +5д {rf_s}  ({get_asset_name(s) or ''})")


if __name__ == "__main__":
    run()
