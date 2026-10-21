"""moex_calendar: праздники из календаря ISS вместо ручного списка (21.10.2026).

Ручной список считал праздниками дни, когда биржа торговала (ОИ за 7 дней 2026 потерян).
Выходные не трогаем — у них своя логика (is_weekend_session)."""
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import moex_calendar as C  # noqa: E402


def test_exchange_traded_on_these_public_holidays():
    for d in ("2024-01-03", "2025-02-24", "2025-06-13", "2026-01-05", "2026-02-23", "2026-03-09",
              "2026-05-01", "2026-05-11", "2026-06-12", "2026-11-04"):
        assert C.is_trading_day(date.fromisoformat(d))[0], d


def test_real_holidays_stay_closed():
    for d in ("2024-03-08", "2025-01-07", "2025-05-09", "2026-01-01", "2026-01-07", "2026-12-31"):
        assert not C.is_trading_day(date.fromisoformat(d))[0], d


def test_weekends_unchanged_and_markets_differ():
    assert not C.is_trading_day(date(2026, 10, 24))[0]                 # суббота — как раньше
    assert C.is_weekend_session(datetime(2026, 10, 24, 12))[0]          # сессия выходного дня — как раньше
    assert all(d.weekday() < 5 for d in C.MOEX_HOLIDAYS)
    # 4.11.2027: фьючерсы закрыты, акции торгуют → общий праздник нет, по рынкам — да/нет
    assert C.is_holiday(date(2027, 11, 4), "futures") and not C.is_holiday(date(2027, 11, 4), "stock")
    assert not C.is_holiday(date(2027, 11, 4))
    assert C.get_previous_trading_day(date(2026, 2, 24)) == date(2026, 2, 23)
