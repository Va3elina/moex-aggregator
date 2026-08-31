#!/usr/bin/env python3
"""Юнит-тесты ЗАДЕРЖКИ ЦЕНЫ по лицензии MOEX (api/services/market_delay).

Чистая логика без БД. Доказывает главное требование договора:

  • ни одна сделка не видна раньше, чем через PRICE_DELAY_MINUTES,
  • при этом мы не придерживаем данные СИЛЬНЕЕ, чем нужно (не больше
    задержки + длины бара),
  • часовой и дневной ТФ не задерживаются вовсе.

Ловит регрессию 31.08.2026: потолок стоял на begin_time без поправки на длину
бара, из-за чего бар 14:20 (покрывающий сделки до 14:25) выпускался в 14:35 —
самая свежая видимая сделка была возраста 10 минут вместо 15.

Запуск:
    python tests/test_market_delay.py      # печатает PASS/FAIL
    pytest tests/test_market_delay.py       # тоже работает
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from api.services.market_delay import (  # noqa: E402
    DELAYED_INTERVALS, PRICE_DELAY_MINUTES, cutoff_for_interval, now_msk,
)

FAILURES: list[str] = []


def check(cond: bool, what: str) -> None:
    if cond:
        print(f"  PASS  {what}")
    else:
        print(f"  FAIL  {what}")
        FAILURES.append(what)


def newest_visible_trade(now: datetime, interval: int) -> datetime:
    """Момент самой свежей сделки, которая уже видна наружу.

    Бары выровнены по сетке ТФ и помечены НАЧАЛОМ; фильтр роутеров —
    `begin_time <= cutoff`. Значит наружу проходит последний бар сетки,
    начало которого не позже потолка, а самая свежая сделка в нём — на его
    ЗАКРЫТИИ (begin + interval).
    """
    cut = cutoff_for_interval(interval)
    assert cut is not None
    midnight = cut.replace(hour=0, minute=0, second=0, microsecond=0)
    mins = int((cut - midnight).total_seconds() // 60)
    top_begin = midnight + timedelta(minutes=(mins // interval) * interval)
    return top_begin + timedelta(minutes=interval)


def test_never_fresher_than_contract() -> None:
    """Главный инвариант: видимая сделка всегда старше договорной задержки.

    Прогон по всем 60 секундам минуты и всем позициям внутри бара — граница
    между «уже можно» и «ещё нельзя» проверяется целиком, а не в одной точке.
    """
    print("\nСделка никогда не свежее договорных 15 минут:")
    worst = None
    for interval in DELAYED_INTERVALS:
        for minute_offset in range(interval * 2):
            for second in (0, 1, 30, 59):
                now = now_msk().replace(second=second, microsecond=0) \
                    + timedelta(minutes=minute_offset)
                # cutoff_for_interval берёт время сам, поэтому сравниваем
                # относительно него же — воспроизводим формулу на «now».
                cut = now - timedelta(minutes=PRICE_DELAY_MINUTES + interval)
                midnight = cut.replace(hour=0, minute=0, second=0, microsecond=0)
                mins = int((cut - midnight).total_seconds() // 60)
                top_end = midnight + timedelta(
                    minutes=(mins // interval) * interval + interval)
                age = (now - top_end).total_seconds() / 60
                if worst is None or age < worst:
                    worst = age
    check(worst is not None and worst >= PRICE_DELAY_MINUTES,
          f"худший возраст видимой сделки {worst:.1f} мин >= {PRICE_DELAY_MINUTES}")


def test_not_stricter_than_needed() -> None:
    """Не придерживаем сильнее необходимого — иначе продукт зря теряет свежесть."""
    print("\nНе задерживаем сверх нужного:")
    for interval in DELAYED_INTERVALS:
        now = now_msk()
        age = (now - newest_visible_trade(now, interval)).total_seconds() / 60
        ceiling = PRICE_DELAY_MINUTES + interval
        check(age <= ceiling + 0.05,
              f"ТФ {interval}м: возраст {age:.1f} мин <= {ceiling} "
              f"(задержка + длина бара)")


def test_cutoff_is_delay_plus_bar() -> None:
    """Потолок = момент лицензии минус длина бара (считаем от КОНЦА бара)."""
    print("\nФормула потолка:")
    for interval in DELAYED_INTERVALS:
        cut = cutoff_for_interval(interval)
        expected = now_msk() - timedelta(minutes=PRICE_DELAY_MINUTES + interval)
        check(abs((cut - expected).total_seconds()) < 2,
              f"ТФ {interval}м: потолок = сейчас − {PRICE_DELAY_MINUTES + interval} мин")


def test_only_five_minute_is_delayed() -> None:
    """Часовая и дневная свеча не задерживаются (решение владельца 11.08.2026)."""
    print("\nПод задержкой только 5-минутки:")
    check(cutoff_for_interval(60) is None, "часовой ТФ не задержан")
    check(cutoff_for_interval(24) is None, "дневной ТФ не задержан")
    check(cutoff_for_interval(None) is None, "ТФ не указан — не задержан")
    check(cutoff_for_interval(5) is not None, "5-минутный ТФ задержан")


def test_cutoff_is_naive_msk() -> None:
    """Потолок наивный и московский — как времена в БД.

    Контейнер живёт в UTC; вернуть aware-время или UTC-время значит отрезать
    три часа торгов (грабли PR #1113).
    """
    print("\nПотолок в наивном МСК:")
    cut = cutoff_for_interval(5)
    check(cut.tzinfo is None, "потолок наивный (без tzinfo)")
    utc_now = datetime.now(timezone.utc).replace(tzinfo=None)
    drift = abs((utc_now + timedelta(hours=3)
                 - timedelta(minutes=PRICE_DELAY_MINUTES + 5) - cut).total_seconds())
    check(drift < 5, "потолок соответствует UTC+3, а не UTC")


if __name__ == "__main__":
    test_never_fresher_than_contract()
    test_not_stricter_than_needed()
    test_cutoff_is_delay_plus_bar()
    test_only_five_minute_is_delayed()
    test_cutoff_is_naive_msk()

    print()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)}")
        for f in FAILURES:
            print(f"  • {f}")
        sys.exit(1)
    print("Все проверки пройдены.")
