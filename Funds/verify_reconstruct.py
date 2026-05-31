"""
Сверка reconstruct (cbonds_calc) ↔ SCHA (interfax_manual/vim_sdr) по ISIN.

Назначение: подтвердить, что cbonds-reconstruct (NAV×weight/price) сходится с
точным SCHA там, где есть оба источника — чтобы доверять reconstruct на фондах
БЕЗ SCHA. Матч строго по ISIN (имена нестабильны). Для каждого фонда берём
свежий cbonds_calc-снапшот и ближайший SCHA (±7 дней).

Запуск на проде:
    docker exec frame-api-1 python3 /app/Funds/verify_reconstruct.py
"""
import sys
import statistics
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from api.database import get_engine

SCHA_SOURCES = ("interfax_manual", "vim_sdr")


def main():
    engine = get_engine()
    with engine.connect() as conn:
        funds = conn.execute(text("""
            SELECT f.fund_id, f.ticker, f.category
            FROM funds f
            WHERE EXISTS (SELECT 1 FROM fund_holdings_history h WHERE h.fund_id=f.fund_id AND h.source='cbonds_calc')
              AND EXISTS (SELECT 1 FROM fund_holdings_history h WHERE h.fund_id=f.fund_id AND h.source = ANY(:sc))
            ORDER BY f.category, f.ticker
        """), {"sc": list(SCHA_SOURCES)}).mappings().all()

        print(f"{'ticker':10} {'cat':7} {'calc_date':11} {'scha_date':11} "
              f"{'match':>6} {'cOnly':>6} {'sOnly':>6} {'med%':>6} {'max%':>7}  verdict")
        print("─" * 92)
        for fd in funds:
            fid = fd["fund_id"]
            cdate = conn.execute(text(
                "SELECT MAX(snapshot_date) FROM fund_holdings_history WHERE fund_id=:f AND source='cbonds_calc'"
            ), {"f": fid}).scalar()
            sdate = conn.execute(text("""
                SELECT snapshot_date FROM fund_holdings_history
                WHERE fund_id=:f AND source = ANY(:sc) AND snapshot_date BETWEEN :c - 7 AND :c + 7
                ORDER BY ABS(snapshot_date - :c) LIMIT 1
            """), {"f": fid, "sc": list(SCHA_SOURCES), "c": cdate}).scalar()
            if not sdate:
                print(f"{fd['ticker']:10} {fd['category']:7} {str(cdate):11} {'—':11}  нет SCHA в ±7д")
                continue

            rows = conn.execute(text("""
                WITH c AS (SELECT isin, positions FROM fund_holdings_history
                           WHERE fund_id=:f AND source='cbonds_calc' AND snapshot_date=:cd AND isin IS NOT NULL AND positions>0),
                     s AS (SELECT isin, positions FROM fund_holdings_history
                           WHERE fund_id=:f AND source = ANY(:sc) AND snapshot_date=:sd AND isin IS NOT NULL AND positions>0)
                SELECT c.isin AS ci, s.isin AS si, c.positions AS cp, s.positions AS sp
                FROM c FULL OUTER JOIN s USING (isin)
            """), {"f": fid, "cd": cdate, "sd": sdate, "sc": list(SCHA_SOURCES)}).mappings().all()

            diffs, match, c_only, s_only = [], 0, 0, 0
            for r in rows:
                if r["cp"] is not None and r["sp"] is not None:
                    match += 1
                    if r["sp"]:
                        diffs.append(abs(r["cp"] - r["sp"]) * 100.0 / r["sp"])
                elif r["cp"] is not None:
                    c_only += 1
                else:
                    s_only += 1
            med = statistics.median(diffs) if diffs else None
            mx = max(diffs) if diffs else None
            verdict = "—"
            if med is not None:
                cover = match / (match + s_only) if (match + s_only) else 0
                verdict = "✅ 1:1" if med < 3 and cover > 0.9 else (
                    "⚠️ расхождения" if med < 10 else "❌ плохо")
            print(f"{fd['ticker']:10} {fd['category']:7} {str(cdate):11} {str(sdate):11} "
                  f"{match:>6} {c_only:>6} {s_only:>6} "
                  f"{(f'{med:.1f}' if med is not None else '-'):>6} "
                  f"{(f'{mx:.0f}' if mx is not None else '-'):>7}  {verdict}")


if __name__ == "__main__":
    main()
