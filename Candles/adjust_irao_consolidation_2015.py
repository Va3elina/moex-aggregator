#!/usr/bin/env python3
"""
One-off data fix: back-adjust IRAO daily/hourly/weekly candles for the
1:100 share consolidation (консолидация акций) that took effect 2015-01-20.

Background
----------
ОАО «Интер РАО» consolidated 100 old shares into 1 new share (номинал ×100).
Trading halted 25 Dec 2014, resumed 20 Jan 2015. Our `candles` table stored the
raw, UNADJUSTED series — close jumped 0.00712 (2014-12-18) -> 0.76410 (2015-01-20),
a +10632% one-day "return" that poisoned the Seasonality indicator
(Январь ≈ +592%, annualised vol ~2600%) and all long-term IRAO charts.

Why not re-import via backfill_daily_history.py?
------------------------------------------------
ISS `/candles.json` returns this OLD consolidation UNADJUSTED (verified 2026-06-09:
curl returns the exact same 0.00712 -> 0.76410 jump). ISS only retroactively adjusts
RECENT splits (e.g. GMKN 2024), not pre-migration corporate actions. So re-import is
a no-op for this bug. The correct fix is a direct multiplicative back-adjustment.

What this does
--------------
For all IRAO stock bars BEFORE the resumption week (begin_time < 2015-01-19):
    open/high/low/close ×100   (old kopeck denom -> new share denom)
    volume ÷100                (100 old shares == 1 new share)
    value (ruble turnover)      unchanged
Within-period returns are preserved; only the corrupt cross-boundary return is fixed
(becomes a sane +7.3% — the real price drift across the ~1-month halt).

Boundary date gotcha: weekly bars (interval=7) are stamped with their Monday
begin_time, and the resumption week starts Mon 2015-01-19. Cutoff is therefore
'2015-01-19' (NOT -20) so the post-consolidation weekly bar is NOT double-scaled.
Daily/hourly are unaffected by the choice (nothing trades during the halt window).

Safety
------
Runs inside a single transaction that verifies the result (boundary return sane,
no remaining >100% artifact jump, expected row count) BEFORE commit; rolls back on
any failure. Guarded against double-application (aborts if data already adjusted).

Inverse (if ever needed): same WHERE clause, open/high/low/close ÷100, volume ×100.

Scope: scanning all 98 stocks (|daily ret| > 200%) on 2026-06-09 found IRAO as the
ONLY affected name. Applied to production 2026-06-09.

Usage:
    python Candles/adjust_irao_consolidation_2015.py
"""
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).parent.parent
load_dotenv(PROJECT_DIR / ".env")
DB_URL = os.getenv("DB_URL", "postgresql+pg8000://postgres:1803@localhost:5432/moex_db")

FACTOR = 100
BOUNDARY = "2015-01-19"            # UPDATE cutoff (Monday of the resumption week)
FIRST_POST_DAILY = "2015-01-20"    # first post-consolidation DAILY bar (verification)
INTERVALS = (7, 24, 60)
ARTIFACT_THRESHOLD = 1.0           # >100% single-bar move == unadjusted artifact


def max_abs_ret(conn, interval):
    rows = conn.execute(text(
        "SELECT begin_time, close FROM candles WHERE secid='IRAO' AND type='stock' "
        "AND interval=:iv AND close>0 ORDER BY begin_time"
    ), {"iv": interval}).fetchall()
    s = pd.Series({pd.Timestamp(r[0]): float(r[1]) for r in rows}).sort_index()
    return float(s.pct_change().abs().max())


def main():
    engine = create_engine(DB_URL, pool_pre_ping=True)

    with engine.connect() as conn:
        pre = conn.execute(text(
            "SELECT close FROM candles WHERE secid='IRAO' AND type='stock' "
            "AND interval=24 AND begin_time=:d"
        ), {"d": "2014-12-18"}).fetchone()
        if pre is None:
            raise SystemExit("ABORT: reference bar 2014-12-18 (interval=24) not found")
        pre_close = float(pre[0])
        print("BEFORE: 2014-12-18 daily close = %.6f" % pre_close)
        if pre_close >= 0.1:
            raise SystemExit("ABORT: already adjusted (close >= 0.1) — refusing to double-apply")
        for iv in INTERVALS:
            print("  BEFORE max |ret| interval=%2d : %.1f%%" % (iv, max_abs_ret(conn, iv) * 100))

    with engine.begin() as conn:
        res = conn.execute(text(
            "UPDATE candles SET open=open*:f, high=high*:f, low=low*:f, close=close*:f, "
            "volume=volume/:f "
            "WHERE secid='IRAO' AND type='stock' AND interval IN (7,24,60) "
            "AND begin_time < :b"
        ), {"f": FACTOR, "b": BOUNDARY})
        affected = res.rowcount
        print("\nUPDATED rows (begin_time < %s): %d" % (BOUNDARY, affected))

        new_pre = float(conn.execute(text(
            "SELECT close FROM candles WHERE secid='IRAO' AND type='stock' "
            "AND interval=24 AND begin_time=:d"
        ), {"d": "2014-12-18"}).fetchone()[0])
        bclose = float(conn.execute(text(
            "SELECT close FROM candles WHERE secid='IRAO' AND type='stock' "
            "AND interval=24 AND begin_time=:d"
        ), {"d": FIRST_POST_DAILY}).fetchone()[0])
        boundary_ret = bclose / new_pre - 1
        print("AFTER:  2014-12-18 close=%.4f  2015-01-20 close=%.4f  boundary=%.2f%%"
              % (new_pre, bclose, boundary_ret * 100))

        ok = True
        for iv in INTERVALS:
            m = max_abs_ret(conn, iv)
            print("  AFTER max |ret| interval=%2d : %.1f%%" % (iv, m * 100))
            if m > ARTIFACT_THRESHOLD:
                ok = False

        checks = {
            "boundary return in (0%, 20%)": 0.0 < boundary_ret < 0.20,
            "new pre-close ~0.712": 0.6 < new_pre < 0.85,
            "no remaining >100% artifact jump": ok,
            "affected rows > 1000": affected > 1000,
        }
        print("\nVERIFICATION:")
        for k, v in checks.items():
            print("  [%s] %s" % ("OK" if v else "FAIL", k))
        if not all(checks.values()):
            raise RuntimeError("Verification FAILED — transaction ROLLED BACK")
        print("\nAll checks passed -> COMMIT")

    print("DONE — committed. Remember to clear Redis chart:IRAO:* / seasonality:IRAO:* if cached.")


if __name__ == "__main__":
    main()
