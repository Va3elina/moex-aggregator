"""ОИ 5 мин: общий срез одним запросом вместо запросов по тикерам (21.10.2026).
Проверяем ветки update_all без сети и БД: срез без дыр, дыра у тикера, тикер вне общего ответа,
общий запрос не удался."""
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
os.environ.setdefault("DB_URL", "postgresql+pg8000://offline@127.0.0.1:1/none")
os.environ.setdefault("ALGOPACK_API_KEY", "test")
from OI import fetch_oi_5min_realtime as M  # noqa: E402

SLOT = ("2026-10-21", "22:20:00")


def _rows(t):
    return [{"ticker": t, "tradedate": SLOT[0], "tradetime": SLOT[1], "clgroup": g, "pos": 1, "pos_long": 1,
             "pos_short": 0, "pos_long_num": 1, "pos_short_num": 0, "systime": None} for g in ("FIZ", "YUR")]


def _updater(bulk, last):
    up = object.__new__(M.OI5minUpdater)
    up.tickers = ["Si", "BR", "CH"]
    up.session_stats = {"records_saved": 0}
    calls = {"bulk": 0, "per_ticker": [], "saved": []}

    class F:
        stats = {"requests": 0}

        async def fetch_latest_all(self, session):
            calls["bulk"] += 1
            return bulk

    up.fetcher = F()
    up.get_last_datetime = lambda t: last.get(t)
    up.save_records = lambda recs: (calls["saved"].append(recs[0]["ticker"]), (len(recs), len(recs)))[1]

    async def update_ticker(session, ticker, f, t):
        calls["per_ticker"].append((ticker, f, t))
        return 0, 0
    up.update_ticker = update_ticker
    return up, calls


def test_bulk_no_gaps_and_absent_ticker_today_only():
    prev = datetime(2026, 10, 21, 22, 15)
    up, c = _updater(_rows("Si") + _rows("BR"), {"Si": prev, "BR": prev, "CH": datetime(2026, 6, 19, 9, 35)})
    asyncio.run(up.update_all(session=None))
    assert c["bulk"] == 1 and sorted(c["saved"]) == ["BR", "Si"]
    # CH нет в общем ответе — только сегодняшний день, а не от последней записи (июнь)
    assert [t for t, *_ in c["per_ticker"]] == ["CH"] and c["per_ticker"][0][1] == c["per_ticker"][0][2]


def test_gap_falls_back_to_per_ticker_from_last_record():
    up, c = _updater(_rows("Si") + _rows("BR"),
                     {"Si": datetime(2026, 10, 21, 22, 15), "BR": datetime(2026, 10, 21, 21, 0), "CH": None})
    asyncio.run(up.update_all(session=None))
    assert c["saved"] == ["Si"]
    br = [x for x in c["per_ticker"] if x[0] == "BR"][0]
    assert br[1] == "2026-10-20"                     # от последней записи минус день, как раньше


def test_bulk_failed_means_old_behaviour_for_all():
    up, c = _updater(None, {"Si": datetime(2026, 10, 21, 22, 15), "BR": datetime(2026, 10, 21, 22, 15), "CH": None})
    asyncio.run(up.update_all(session=None))
    assert c["saved"] == [] and sorted(t for t, *_ in c["per_ticker"]) == ["BR", "CH", "Si"]
