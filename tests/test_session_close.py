#!/usr/bin/env python3
"""Юнит-тесты правила «цена только на закрытие 19:00» (api/services/session_close).

Чистая логика без БД: граница публикации, цена-ступенька интрадей-графика,
выбор контракта дня, отсечение неопубликованного дня, роль и вид админа.

Главное требование: цена дня D не выходит наружу раньше 19:10 дня D, а сделки
вечерней сессии не попадают ни в один публичный ряд.

Запуск:
    python tests/test_session_close.py      # печатает PASS/FAIL
    pytest tests/test_session_close.py       # тоже работает
"""
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from api.services.session_close import (  # noqa: E402
    day_closes, is_live_viewer, last_published_date, publish_daily_closes,
    published_end, published_next_day, step_values, view_tag,
)

FAILURES: list[str] = []


def check(cond: bool, what: str) -> None:
    if cond:
        print(f"  PASS  {what}")
    else:
        print(f"  FAIL  {what}")
        FAILURES.append(what)


def test_publication_boundary() -> None:
    print("\nГраница публикации:")
    check(last_published_date(datetime(2026, 9, 10, 19, 9)) == date(2026, 9, 9),
          "до 19:10 опубликован вчерашний день")
    check(last_published_date(datetime(2026, 9, 10, 19, 10)) == date(2026, 9, 10),
          "с 19:10 опубликован сегодняшний")
    check(last_published_date(datetime(2026, 9, 11, 0, 5)) == date(2026, 9, 10),
          "после полуночи опубликован вчерашний")
    check(published_end(datetime(2026, 9, 10, 15, 0)) == datetime(2026, 9, 9, 19, 0),
          "потолок баров днём — 19:00 вчерашнего дня")
    check(published_next_day(datetime(2026, 9, 10, 20, 0)) == datetime(2026, 9, 11, 0, 0),
          "потолок дневных свечей вечером — полночь после сегодняшнего дня")


def test_step_values() -> None:
    print("\nЦена-ступенька:")
    closes = {date(2026, 9, 9): 100.0, date(2026, 9, 10): 110.0}
    times = [
        datetime(2026, 9, 10, 10, 0), datetime(2026, 9, 10, 18, 55),
        datetime(2026, 9, 10, 19, 0), datetime(2026, 9, 10, 20, 0),
        datetime(2026, 9, 11, 10, 0),
    ]
    vals = step_values(times, closes, date(2026, 9, 10))
    check(vals == [100.0, 100.0, 110.0, 110.0, 110.0],
          "до 19:00 — вчерашнее закрытие, с 19:00 — сегодняшнее")

    vals = step_values(times[:4], closes, date(2026, 9, 9))
    check(vals == [100.0, 100.0, None, None], "вечер неопубликованной сессии выпадает")
    check(110.0 not in vals, "закрытие неопубликованного дня наружу не попадает")

    weekend = {date(2026, 9, 4): 50.0, date(2026, 9, 5): 51.0}
    vals = step_values([datetime(2026, 9, 5, 12, 0), datetime(2026, 9, 6, 12, 0)],
                       weekend, date(2026, 9, 5))
    check(vals == [50.0, 51.0], "выходная сессия — обычный день по тем же правилам")

    vals = step_values([datetime(2026, 9, 9, 12, 0)], {date(2026, 9, 9): 100.0}, date(2026, 9, 9))
    check(vals == [None], "до первой известной цены закрытия бар выпадает")
    vals = step_values([datetime(2026, 9, 9, 12, 0)], {date(2026, 9, 9): 100.0},
                       date(2026, 9, 9), prior=95.0)
    check(vals == [95.0], "prior закрывает начало ряда")

    gap = {date(2026, 9, 8): 90.0}
    vals = step_values([datetime(2026, 9, 10, 12, 0)], gap, date(2026, 9, 9))
    check(vals == [90.0], "день без баров в ряду не рвёт ступеньку")


def test_day_closes_contract() -> None:
    print("\nКонтракт дня:")
    bars = {
        ("SRU6", date(2026, 9, 10)): ("SRU", 1.0, 1.0, 1.0, 100.0, 5000.0),
        ("SRZ6", date(2026, 9, 10)): ("SRZ", 1.0, 1.0, 1.0, 102.0, 9000.0),
    }
    check(day_closes(bars, {date(2026, 9, 10): "SRU"})[date(2026, 9, 10)] == 100.0,
          "берётся контракт, выбранный графиком")
    check(day_closes(bars)[date(2026, 9, 10)] == 102.0,
          "без выбора — самый торгуемый за сессию")


def test_publish_daily_closes() -> None:
    print("\nДневной ряд:")
    rows = [(date(2026, 9, 9), 100.0), (date(2026, 9, 10), 105.0)]
    out = publish_daily_closes(rows, {date(2026, 9, 9): 99.0}, now=datetime(2026, 9, 10, 15, 0))
    check(out == [(date(2026, 9, 9), 99.0)],
          "неопубликованный день выпал, закрытие 19:00 заменило дневную свечу")
    out = publish_daily_closes(rows, {}, now=datetime(2026, 9, 10, 19, 30))
    check(len(out) == 2, "после 19:10 сегодняшний день в ряду")


def test_live_viewer() -> None:
    print("\nВерсия цены зрителя:")

    class U:
        def __init__(self, role):
            self.role = role

    class R:
        def __init__(self, headers):
            self.headers = headers

    check(not is_live_viewer(None), "гость — только закрытие 19:00")
    check(not is_live_viewer(U("pro")), "платный тариф — только закрытие 19:00")
    check(is_live_viewer(U("admin")), "админ — незамедленная версия")
    check(is_live_viewer(U("admin"), R({})), "админ без заголовка — незамедленная версия")
    check(not is_live_viewer(U("admin"), R({"x-frame-view": "user"})),
          "админ в виде пользователя — только закрытие 19:00")
    check(view_tag(True) != view_tag(False), "версии живут под разными ключами кеша")


if __name__ == "__main__":
    test_publication_boundary()
    test_step_values()
    test_day_closes_contract()
    test_publish_daily_closes()
    test_live_viewer()

    print()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)}")
        for f in FAILURES:
            print(f"  • {f}")
        sys.exit(1)
    print("Все проверки пройдены.")
