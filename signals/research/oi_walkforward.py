"""Walk-forward бэктест контрарности физлиц по ВСЕМ фьючерсам, вся история. НЕ прод.

Сигнал: COT-индекс физ (WillCo, трейлинг-окно W) на РЕДКОМ экстремуме.
  hi-экстремум (COT≥HI, толпа в лонге) → контрарно ШОРТ, профит = −fwd_ret.
  lo-экстремум (COT≤LO, толпа в шорте) → контрарно ЛОНГ,  профит = +fwd_ret.
COT считается по данным ДО T (causal), исход — T→T+H (out-of-sample). Чисто walk-forward.

Два слоя:
  (1) СОБЫТИЙНЫЙ — все события по сетке порогов × ГОДАМ (фикс-порог = валидация
      каждого года out-of-sample; смотрим, держится ли в 2026).
  (2) WALK-FORWARD GATE — актив «торгуем» в момент T только если его ПРОШЛЫЕ
      РАЗРЕШИВШИЕСЯ сигналы (resolve_date ≤ T) дали hit-rate ≥ GATE на ≥MIN_PAST
      событиях. Никакого look-ahead в отборе активов. Это «редкие но надёжные».

Запуск: DB_URL=... /opt/frame/signals/.venv/bin/python -m signals.research.oi_walkforward
"""
import os
import statistics as st
from collections import defaultdict

from sqlalchemy import create_engine, text

from signals.research.cot_index import load_oi, load_price, cot_index

W = 110               # окно COT
H = 10                # горизонт forward (торг. дней)
THRESHOLDS = [(80, 20), (90, 10), (95, 5)]
GATE = 0.58           # порог надёжности прошлых сигналов
MIN_PAST = 8          # минимум прошлых разрешившихся событий, чтобы «доверять» активу
GATE_THRESH = (90, 10)  # порог редкости для walk-forward gate


def universe(conn):
    rows = conn.execute(text("""
        SELECT sectype FROM open_interest
        WHERE interval=24 AND clgroup='FIZ'
        GROUP BY sectype
        HAVING count(*) > 300 AND max(tradedate) >= DATE '2025-01-01'
    """)).fetchall()
    return [r[0] for r in rows]


def build_events(s_oi, price, hi, lo):
    """Список событий: (date, year, contr_ret, is_hit, resolve_date). causal COT, fwd H."""
    dates = [x[0] for x in s_oi]
    tot = [x[3] for x in s_oi]
    willf = [s_oi[i][1] / tot[i] if tot[i] else 0 for i in range(len(s_oi))]
    idx = cot_index(willf, W)
    out = []
    for i in range(len(s_oi) - H):
        v = idx[i]
        if v is None:
            continue
        p0 = price.get(dates[i])
        pf = price.get(dates[i + H])
        if not p0 or not pf:
            continue
        ret = (pf - p0) / p0 * 100
        if v >= hi:
            contr = -ret
        elif v <= lo:
            contr = ret
        else:
            continue
        out.append((dates[i], dates[i].year, contr, contr > 0, dates[i + H]))
    return out


def run():
    engine = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    with engine.connect() as conn:
        assets = universe(conn)
        print(f"\nВселенная: {len(assets)} фьючерсов (OI≥300дн, торгуются в 2025+)")

        # кэш загрузок
        cache = {}
        for sectype in assets:
            try:
                s_oi = load_oi(conn, sectype)
                price = load_price(conn, sectype)
            except Exception:
                continue
            if not price or len(s_oi) < W + H + 60:
                continue
            cache[sectype] = (s_oi, price)
        print(f"С достаточной историей: {len(cache)}")

    # ── (1) СОБЫТИЙНЫЙ по порогам × годам ──
    print(f"\n=== (1) Все события: контрарность физлиц по годам (H={H}д, окно COT={W}) ===")
    for hi, lo in THRESHOLDS:
        by_year = defaultdict(list)   # year -> [contr_ret]
        for sectype, (s_oi, price) in cache.items():
            for d, yr, contr, hit, rdate in build_events(s_oi, price, hi, lo):
                by_year[yr].append(contr)
        allc = [c for cs in by_year.values() for c in cs]
        if not allc:
            continue
        hitall = 100 * sum(1 for c in allc if c > 0) / len(allc)
        print(f"\n  ── Порог ≥{hi}/≤{lo} (реже = надёжнее?) — всего {len(allc)} событий, "
              f"hit {hitall:.0f}%, ср.профит {st.fmean(allc):+.2f} п.п. ──")
        print(f"   {'год':>6}{'событий':>9}{'hit-rate':>10}{'ср.профит п.п.':>16}")
        for yr in sorted(by_year):
            cs = by_year[yr]
            if len(cs) < 20:
                continue
            h = 100 * sum(1 for c in cs if c > 0) / len(cs)
            mark = "  ← 2026" if yr == 2026 else ""
            print(f"   {yr:>6}{len(cs):>9}{h:>9.0f}%{st.fmean(cs):>15.2f}{mark}")

    # ── (2) WALK-FORWARD GATE ──
    hi, lo = GATE_THRESH
    print(f"\n=== (2) WALK-FORWARD: торгуем сигнал актива только если ПРОШЛЫЕ "
          f"(≥{MIN_PAST}, hit≥{int(GATE*100)}%) надёжны. Порог ≥{hi}/≤{lo} ===")
    traded_by_year = defaultdict(list)        # year -> [contr_ret]
    qualified_assets = defaultdict(set)       # year -> {sectype}
    for sectype, (s_oi, price) in cache.items():
        ev = build_events(s_oi, price, hi, lo)
        ev.sort(key=lambda e: e[0])
        for i in range(len(ev)):
            d_i = ev[i][0]
            # прошлые РАЗРЕШИВШИЕСЯ события (resolve_date ≤ d_i) — без look-ahead
            past = [ev[j][3] for j in range(i) if ev[j][4] <= d_i]
            if len(past) >= MIN_PAST and (sum(past) / len(past)) >= GATE:
                traded_by_year[ev[i][1]].append(ev[i][2])
                qualified_assets[ev[i][1]].add(sectype)

    allt = [c for cs in traded_by_year.values() for c in cs]
    if allt:
        print(f"   Всего сделок (gated): {len(allt)}, "
              f"hit {100*sum(1 for c in allt if c>0)/len(allt):.0f}%, "
              f"ср.профит {st.fmean(allt):+.2f} п.п.")
        print(f"   {'год':>6}{'сделок':>8}{'hit-rate':>10}{'ср.профит':>11}{'активов':>9}")
        for yr in sorted(traded_by_year):
            cs = traded_by_year[yr]
            h = 100 * sum(1 for c in cs if c > 0) / len(cs)
            mark = "  ← 2026" if yr == 2026 else ""
            print(f"   {yr:>6}{len(cs):>8}{h:>9.0f}%{st.fmean(cs):>10.2f}"
                  f"{len(qualified_assets[yr]):>9}{mark}")
        if 2026 in qualified_assets:
            print(f"\n   Активы, прошедшие gate в 2026: "
                  f"{', '.join(sorted(qualified_assets[2026]))}")
    else:
        print("   Ни один сигнал не прошёл gate (порог надёжности слишком строгий).")


if __name__ == "__main__":
    run()
