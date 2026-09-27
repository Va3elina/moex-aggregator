"""Сканеры находок и связок ждут дневные позиции за последний торговый день и прогоняются один раз на день данных.

27.09: позиции за пятницу МосБиржа выкладывает в субботу утром, а докачка оркестратора в выходной шла раз в сутки,
в полночь, — пятница доезжала в полночь на воскресенье или при перезапуске на деплое. Сканеры шли в субботу в 10:30
МСК на четверге, а во вторник пятница уже не последний день (R24): завод не видел ни одной пятницы. Русал 25.09 —
лонг физлиц +42 тыс. контрактов за день, крупнейший прирост в нашей истории, — прошёл мимо.

Теперь крон зовёт сканер каждый час (`30 7-17 * * *`), а сканер работает, только когда в базе последний торговый
день и на этом дне он ещё не прогонялся. Отметка — файл в STATE_DIR (вне git, деплой её не стирает).
"""
import os
from datetime import date

from sqlalchemy import text

from api.database import SessionLocal
from moex_calendar import get_moscow_time, get_previous_trading_day

STATE_DIR = os.environ.get("SCAN_STATE_DIR", "/opt/frame/data/scan_state")

_LAST_DAY = text("SELECT max(tradedate) FROM open_interest WHERE interval = 24 AND clgroup = 'FIZ'")


def last_oi_day() -> date | None:
    db = SessionLocal()
    try:
        return db.execute(_LAST_DAY).scalar()
    finally:
        db.close()


def _state(name: str) -> str:
    return os.path.join(STATE_DIR, f"{name}.done")


def done_day(name: str) -> str | None:
    try:
        with open(_state(name), encoding="utf-8") as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None


def mark_done(name: str, day) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(_state(name), "w", encoding="utf-8") as f:
        f.write(str(day)[:10])


def wait_reason(name: str) -> str | None:
    """Почему сканер сейчас не запускать; None — пора."""
    need = get_previous_trading_day(get_moscow_time().date())
    have = last_oi_day()
    if have is None:
        return "в базе нет дневных позиций"
    if have < need:
        return f"ждём дневные позиции за {need:%d.%m}, в базе по {have:%d.%m}"
    if done_day(name) == str(have):
        return f"на данных по {have:%d.%m} уже прогнан"
    return None
