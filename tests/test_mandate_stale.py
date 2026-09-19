"""Мандаты: старое событие не шлётся в бот (19.09: делистинг ПИК от 07.09 пришёл 19.09)."""
import inspect
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from api.routers import mandate_scan as ms  # noqa: E402


def test_week_old_event_is_stale():
    today = date(2026, 9, 19)
    assert ms._is_stale(date(2026, 9, 7), today)          # ПИК: решение СД 07.09
    assert not ms._is_stale(date(2026, 9, 18), today)     # free-float вступил 18.09
    assert not ms._is_stale(None, today)                   # старый скаут без даты — как раньше


def test_stale_checked_before_notify():
    src = inspect.getsource(ms.submit_candidate)
    assert src.index("_is_stale(") < src.index("_notify_admin(")
