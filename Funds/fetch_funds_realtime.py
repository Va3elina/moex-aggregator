#!/usr/bin/env python3
"""
Обновление данных фондов через Cbonds API.

Источник: Cbonds REST API (https://rest2.cbonds.info)
Данные:
- ПАЙ (nav_per_share) — расчётная стоимость пая от УК
- СЧА (nav) — общая стоимость чистых активов фонда
- Состав фондов (holdings) — активы и доли

Конфигурация фондов: таблица `funds` (поле cbonds_id).

Расписание (МСК):
- Раз в день: 09:00 (данные за вчера появляются к утру)
- Выходные пропускаются

Использование:
  python fetch_funds_realtime.py --once --force    # Разово
  python fetch_funds_realtime.py                   # Демон
"""

import asyncio
import aiohttp
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List
import logging
import sys
import json
from pathlib import Path
import argparse

# === Импорт из корневой папки ===
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

try:
    from moex_calendar import get_moscow_time, is_trading_day
except ImportError:
    def get_moscow_time():
        return datetime.utcnow() + timedelta(hours=3)
    def is_trading_day(check_date=None):
        check = check_date or get_moscow_time().date()
        if check.weekday() in [5, 6]:
            return False, "Выходной"
        return True, "Торговый день"


# ═══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════════════════════

# Время обновления (МСК)
UPDATE_HOUR = 9
UPDATE_MINUTE = 0

# Директория логов — /app/logs (named volume, writable на read-only rootfs прод-контейнера).
# Раньше было Funds/logs → read-only → файловый лог демона молча терялся (а detached
# `docker exec -d` ещё и глотает stdout). Теперь лог демона персистится в volume.
LOG_DIR = Path(__file__).parent.parent / "logs"
try:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    pass

# Cbonds API
CBONDS_URL = "https://rest2.cbonds.info"
CBONDS_UA = "Cbonds.K/3.0.8 (ru.cbonds.cbonds; build:636; Android 9) OkHttp/4.12.0"
CBONDS_LOGIN = os.getenv("CBONDS_LOGIN")
CBONDS_PASSWORD = os.getenv("CBONDS_PASSWORD")


# ═══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging():
    detailed_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_fmt)
    root.addHandler(ch)

    # File handler — best-effort. На read-only rootfs (production API
    # контейнер) пропускаем file logging и работаем только через stdout.
    # Docker логи всё равно зафиксируют вывод.
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(
            LOG_DIR / f"funds_{datetime.now():%Y%m%d}.log",
            encoding='utf-8'
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(detailed_fmt)
        root.addHandler(fh)
    except OSError:
        # Read-only filesystem (production) — silent skip.
        # stdout handler уже подключён выше, лог не потеряется.
        pass

    try:
        fh_err = logging.FileHandler(
            LOG_DIR / f"funds_errors_{datetime.now():%Y%m%d}.log",
            encoding='utf-8'
        )
        fh_err.setLevel(logging.WARNING)
        fh_err.setFormatter(detailed_fmt)
        root.addHandler(fh_err)
    except OSError:
        # Read-only filesystem — silent skip (production API контейнер).
        pass

    return logging.getLogger(__name__)


log = setup_logging()


# ═══════════════════════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

def get_engine():
    if not DB_URL:
        raise ValueError("DB_URL не установлен в .env")
    return create_engine(DB_URL, connect_args={"ssl_context": False})


def get_last_date(engine, fund_id: int) -> Optional[date]:
    """Получить последнюю дату для фонда в БД"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MAX(trade_date) FROM fund_data WHERE fund_id = :fund_id
        """), {"fund_id": fund_id}).fetchone()
        return result[0] if result and result[0] else None


def save_fund_data(engine, fund_id: int, merged: Dict[str, dict]) -> int:
    """Сохранить данные в fund_data."""
    if not merged:
        return 0
    with engine.connect() as conn:
        n = 0
        for td_str, vals in merged.items():
            pay = vals.get("pay")
            nav = vals.get("nav")
            if pay is None and nav is None:
                continue
            conn.execute(text("""
                INSERT INTO fund_data (fund_id, trade_date, pay, nav)
                VALUES (:fid, :td, :pay, :nav)
                ON CONFLICT (fund_id, trade_date) DO UPDATE SET
                    pay = COALESCE(EXCLUDED.pay, fund_data.pay),
                    nav = COALESCE(EXCLUDED.nav, fund_data.nav),
                    created_at = CURRENT_TIMESTAMP
            """), {"fid": fund_id, "td": td_str, "pay": pay, "nav": nav})
            n += 1
        conn.commit()
        return n


def load_funds_from_db(engine) -> List[dict]:
    """Загрузить список фондов с cbonds_share_id из БД."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT fund_id, ticker, name, cbonds_share_id FROM funds WHERE cbonds_share_id IS NOT NULL ORDER BY fund_id"
        )).fetchall()
        return [{"fund_id": r[0], "ticker": r[1], "name": r[2], "cbonds_id": r[3]} for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# CBONDS API
# ═══════════════════════════════════════════════════════════════════════════════

async def cbonds_auth(session) -> bool:
    """Авторизация в Cbonds API. Устанавливает PHPSESSID в сессии."""
    url = f"{CBONDS_URL}/m/auth/tariffs/global/json/logout=1?lang=rus"
    headers = {"Content-Type": "application/json; charset=UTF-8", "User-Agent": CBONDS_UA}
    body = {"login": CBONDS_LOGIN, "password": CBONDS_PASSWORD}
    try:
        async with session.post(url, json=body, headers=headers, timeout=15) as resp:
            data = await resp.json()
            err = data.get("error", {}).get("err_no", -1)
            if err == 0:
                user = data.get("auth", {}).get("title", "?")
                log.info(f"  Cbonds авторизация: ✅ {user}")
                return True
            else:
                log.error(f"  Cbonds авторизация: ❌ {data.get('error', {}).get('err_str', '?')}")
                return False
    except Exception as e:
        log.error(f"  Cbonds авторизация: ❌ {e}")
        return False


def _next_business_day(d: date) -> date:
    """Следующий рабочий день (пропускает Сб/Вс)."""
    d += timedelta(days=1)
    while d.weekday() >= 5:  # 5=Сб, 6=Вс
        d += timedelta(days=1)
    return d


async def fetch_cbonds_nav(session, cbonds_id: int, date_from: date, date_to: date) -> Dict[str, dict]:
    """Получить историю NAV фонда из Cbonds API → {date_str: {pay, nav}}

    Cbonds отдаёт дату расчёта УК (T), а MOEX использует дату торгов (T+1).
    Сдвигаем дату на следующий рабочий день для совместимости.
    """
    url = (f"{CBONDS_URL}/m/exchange_traded_funds/nav/global/json/"
           f"{cbonds_id}/{date_from.isoformat()}/{date_to.isoformat()}/?lang=rus")
    headers = {"Content-Type": "application/json; charset=UTF-8", "User-Agent": CBONDS_UA}
    try:
        async with session.post(url, headers=headers, timeout=30) as resp:
            data = await resp.json()
            err = data.get("error", {}).get("err_no", -1)
            if err != 0:
                log.warning(f"  Cbonds NAV {cbonds_id}: err={err}")
                return {}
            items = data.get("response", {}).get("items", [])
            merged = {}
            for item in items:
                ts = item.get("date")
                if not ts:
                    continue
                # date — unix timestamp, конвертируем
                dt = datetime.utcfromtimestamp(ts).date()
                # Cbonds дата = дата расчёта УК (T), сдвигаем на дату торгов (T+1 рабочий день)
                trade_date = _next_business_day(dt)
                nav_per_share = item.get("nav_per_share")  # СЧА на пай (расчётный пай от УК)
                nav_total = item.get("nav")  # Общая СЧА фонда
                if nav_per_share is not None:
                    merged[trade_date.isoformat()] = {
                        "pay": nav_per_share,  # пай = nav_per_share
                        "nav": nav_total,       # общая СЧА
                    }
            return merged
    except Exception as e:
        log.error(f"  Cbonds NAV {cbonds_id}: ❌ {e}")
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# СОСТАВ ФОНДОВ (Cbonds API)
# ═══════════════════════════════════════════════════════════════════════════════

async def fetch_cbonds_holdings(session, parent_fund_id: int) -> tuple[List[dict], str | None]:
    """
    Получить состав фонда из Cbonds API.

    Cbonds возвращает `weight` в долях (0.0-1.0). Мы домножаем на 100
    чтобы привести к процентам (0-100) — одинаковая шкала с ВИМ-парсером.
    Sanity-check: sum(weight) после конверсии должно быть близко к 100;
    если сильно меньше — логируем warning (вероятно non-full snapshot).

    Returns:
        (holdings, snapshot_date_iso). snapshot_date — дата от УК которую
        возвращает Cbonds (поле 'date' в item). Может быть None если ответ
        не содержит дату.
    """
    url = f"{CBONDS_URL}/m/exchange_traded_funds/structure/global/json/{parent_fund_id}/?lang=rus"
    headers = {"Content-Type": "application/json; charset=UTF-8", "User-Agent": CBONDS_UA}
    body = {"quantity": {"limit": 50, "offset": 0}}
    try:
        async with session.post(url, json=body, headers=headers, timeout=30) as resp:
            data = await resp.json()
            items = data.get("response", {}).get("items", [])
            # Дедуп по имени (Cbonds может вернуть дубли с пустым name)
            seen = set()
            result = []
            snapshot_date = None
            for i in items:
                name = str(i.get("asset_name") or i.get("name") or "Прочее").strip()
                # Cbonds weight = доля (0.0-1.0), приводим к проценту (0-100).
                weight = float(i.get("weight", 0)) * 100
                # Cbonds возвращает дату как unix timestamp (int) или ISO string.
                # На практике видели integer (1777496400 = 2026-04-29). Парсим
                # оба варианта graceful'но.
                if snapshot_date is None and i.get("date"):
                    raw_date = i["date"]
                    try:
                        if isinstance(raw_date, (int, float)):
                            from datetime import datetime as _dt, timezone as _tz
                            snapshot_date = _dt.fromtimestamp(
                                raw_date, tz=_tz.utc
                            ).date().isoformat()
                        elif isinstance(raw_date, str):
                            # ISO "2026-04-29T00:00:00Z" → "2026-04-29"
                            snapshot_date = raw_date[:10]
                    except (ValueError, OSError):
                        pass
                if weight > 0 and name not in seen:
                    seen.add(name)
                    result.append({"name": name, "weight": weight})

            # Sanity-check: суммарный вес должен быть ~100%. Если сильно ниже
            # — partial snapshot или баг шкалы (например уже в %).
            total = sum(r["weight"] for r in result)
            if result and (total < 80 or total > 120):
                log.warning(
                    f"  Cbonds {parent_fund_id}: подозрительный sum(weight)={total:.2f}%"
                    f" (ожидаем ~100). {len(result)} позиций."
                )
            return result, snapshot_date
    except Exception as e:
        log.warning(f"  Cbonds holdings {parent_fund_id}: {e}")
        return [], None


def save_fund_holdings(
    engine,
    fund_id: int,
    holdings: List[dict],
    snapshot_date: str | None = None,
    source: str = "cbonds",
) -> int:
    """
    Сохранить состав фонда в `fund_holdings` (current snapshot) И
    в `fund_holdings_history` (append с датой).

    Двойная шкала weight (важно):
      - `fund_holdings` (legacy table, читается /funds-catalog с UI × 100)
        → хранит ДОЛИ (0.0-1.0). Конвертация: weight / 100 при save.
      - `fund_holdings_history` (новая table для /fund-trades) → хранит
        ПРОЦЕНТЫ (0-100). Сохраняется as-is.

    Все парсеры возвращают `holdings[i]["weight"]` в ПРОЦЕНТАХ (как видно
    юзеру: 15.73). Эта функция конвертирует под нужную таблицу.

    Args:
        snapshot_date: ISO YYYY-MM-DD дата к которой относится snapshot.
            Если None — используется CURRENT_DATE (сегодня).
        source: 'cbonds' / 'vim' / 'pervaya' etc. — для трекинга
            источника данных в analytics.

    History — АТОМАРНАЯ ЗАМЕНА снапшота (DELETE+INSERT в одной транзакции)
    по ключу (fund_id, snapshot_date, source). Именно замена, а не append
    с ON CONFLICT DO NOTHING: `isin` входит в uq_fund_holdings_history, но
    проставляется задним числом бэкфиллом (backfill_holdings_isin.py) —
    после обогащения ключ строки менялся с ('', name) на (isin, name), и
    повторный прогон за тот же день уже не ловил конфликт и дописывал
    строку-двойник без ISIN. Замена идемпотентна независимо от того, кто и
    когда трогал isin, и вдобавок повторный прогон теперь ИСПРАВЛЯЕТ
    weight/positions, а не оставляет протухшие (DO NOTHING их не обновлял).

    Уже известные ISIN'ы переносятся в новые строки по asset_name (только
    однозначные — если у имени в истории фонда больше одного ISIN, не
    гадаем), чтобы замена не откатывала обогащение.
    """
    if not holdings:
        return 0

    date_clause = ":snap_date" if snapshot_date else "CURRENT_DATE"
    params_base: dict = {"fid": fund_id, "source": source}
    if snapshot_date:
        params_base["snap_date"] = snapshot_date

    with engine.connect() as conn:
        # 0. Замена снапшота, в отличие от прежнего append'а, теряет одно
        # полезное свойство: сломавшийся парсер (ВИМ отдал половину таблицы)
        # раньше просто ничего не дописывал, а теперь затёр бы уже собранные
        # строки — а у дневных данных ВИМ другого источника нет. Поэтому если
        # входящих строк вдвое меньше, чем уже лежит за эту дату, снапшот не
        # трогаем вовсе. Для первого прогона за новую дату (existing=0) защита
        # неактивна, но там и терять нечего — ровно как было до замены.
        existing = conn.execute(text(f"""
            SELECT count(*) FROM fund_holdings_history
            WHERE fund_id = :fid AND source = :source
              AND snapshot_date = {date_clause}
        """), params_base).scalar() or 0
        if existing and len(holdings) * 2 < existing:
            log.warning(
                "  fund_id=%s source=%s snap=%s: подозрительно короткий состав "
                "(%d строк против %d уже сохранённых) — снапшот НЕ заменён",
                fund_id, source, snapshot_date or "today", len(holdings), existing,
            )
            return 0

        # 1. Current snapshot — DELETE + INSERT. Сохраняем weight / 100
        # для backwards-compat (legacy /funds-catalog UI домножает на 100).
        conn.execute(text("DELETE FROM fund_holdings WHERE fund_id = :fid"), {"fid": fund_id})
        n = 0
        for h in holdings:
            conn.execute(text("""
                INSERT INTO fund_holdings (fund_id, asset_name, weight, updated_at)
                VALUES (:fid, :name, :weight, NOW())
            """), {
                "fid": fund_id,
                "name": h["name"],
                "weight": h["weight"] / 100.0,  # % → доли для legacy table
            })
            n += 1

        # 2. History — weight в процентах (0-100), как пришёл.
        # 2a. Карта asset_name → isin из истории ЭТОГО фонда и источника.
        # Только однозначные имена: у interfax_manual одно имя эмитента может
        # нести несколько выпусков (Сбербанк ао/ап, разные ОФЗ Минфина) —
        # такие не переносим, пусть их проставляет бэкфилл по рег-номеру.
        known_isin = {
            row[0]: row[1]
            for row in conn.execute(text("""
                SELECT asset_name, min(isin)
                FROM fund_holdings_history
                WHERE fund_id = :fid AND source = :source AND isin IS NOT NULL
                GROUP BY asset_name
                HAVING count(DISTINCT isin) = 1
            """), {"fid": fund_id, "source": source}).fetchall()
        }

        # 2b. Снапшот заменяем целиком — см. docstring.
        conn.execute(text(f"""
            DELETE FROM fund_holdings_history
            WHERE fund_id = :fid AND source = :source
              AND snapshot_date = {date_clause}
        """), params_base)

        # ON CONFLICT оставлен на случай, если сам парсер вернёт две строки с
        # одинаковым именем: после DELETE это был бы уже не тихий пропуск, а
        # падение всей транзакции и потеря снапшота целиком.
        for h in holdings:
            conn.execute(text(f"""
                INSERT INTO fund_holdings_history
                    (fund_id, asset_name, weight, positions, amount_rub,
                     snapshot_date, source, isin, created_at)
                VALUES
                    (:fid, :name, :weight, :positions, :amount_rub,
                     {date_clause}, :source, :isin, NOW())
                ON CONFLICT (fund_id, (COALESCE(isin,'')), asset_name, snapshot_date, source) DO NOTHING
            """), {
                **params_base,
                "name": h["name"],
                "weight": h["weight"],  # проценты as-is
                "positions": h.get("positions"),
                "amount_rub": h.get("amount_rub"),
                "isin": h.get("isin") or known_isin.get(h["name"]),
            })

        conn.commit()
        return n


async def update_all_holdings(engine, session) -> int:
    """Обновить состав всех фондов через Cbonds API.

    Использует УЖЕ авторизованную `session` (общий логин с update_all_funds) —
    раньше функция логинилась повторно, и каждый logout=1-вход приближал
    rate-limit Cbonds «limited rights». Один прогон = один вход."""
    log.info("")
    log.info("=" * 60)
    log.info("📊 ОБНОВЛЕНИЕ СОСТАВА ФОНДОВ (Cbonds API)")
    log.info("=" * 60)

    # Получаем все фонды с cbonds_parent_id из БД
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT fund_id, ticker, cbonds_parent_id FROM funds WHERE cbonds_parent_id IS NOT NULL"
        )).fetchall()

    if not rows:
        log.info("  Нет фондов с cbonds_parent_id")
        return 0

    total_saved = 0
    for fund_id, ticker, parent_id in rows:
        holdings, snapshot_date = await fetch_cbonds_holdings(session, parent_id)
        if holdings:
            saved = save_fund_holdings(
                engine, fund_id, holdings,
                snapshot_date=snapshot_date,
                source="cbonds",
            )
            snap_info = f" snap={snapshot_date}" if snapshot_date else ""
            log.info(f"  {ticker:12s} fund_id={fund_id:>6d}: {saved} позиций{snap_info}")
            total_saved += saved
        else:
            log.debug(f"  {ticker:12s} fund_id={fund_id:>6d}: нет данных")
        await asyncio.sleep(0.3)

    log.info(f"\n✅ Состав ГОТОВО: {total_saved} позиций для {len(rows)} фондов")
    return total_saved


# ═══════════════════════════════════════════════════════════════════════════════
# СОСТАВ ФОНДОВ — ВИМ (прямой парсинг сайта, daily T+1)
# ═══════════════════════════════════════════════════════════════════════════════

def update_vim_holdings(engine) -> int:
    """
    Обновить состав ВИМ-фондов (LQDT, EQMX, GOLD, OBLG, ESGE, CNYM)
    через прямой парсинг wealthim.ru.

    Преимущество vs Cbonds:
      - daily (T+1) vs monthly (T-30)
      - positions (штуки) vs только weight (%)

    Стратегия записи:
      - source='vim' в history
      - Для тех же fund_id+snapshot_date ВИМ записи затрут cbonds через
        ON CONFLICT DO UPDATE (мы upgrade'им: weight остаётся, добавляем
        positions, source='vim'). Точнее: записи ВИМ и Cbonds могут иметь
        разные asset_name (форматы отличаются) — поэтому это не conflict,
        а **дополнение** snapshot'а. UI сможет filter'нуть по source если
        нужно.

    Возвращает суммарное число загруженных строк.
    """
    from Funds.parsers.vim_parser import fetch_vim_holdings, VIM_FUND_SLUGS

    log.info("")
    log.info("=" * 60)
    log.info("📊 ОБНОВЛЕНИЕ ВИМ (прямой парсинг wealthim.ru)")
    log.info("=" * 60)

    # Берём fund_id'ы для всех ВИМ-тикеров.
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT fund_id, ticker FROM funds
            WHERE ticker = ANY(:tickers)
        """), {"tickers": list(VIM_FUND_SLUGS.keys())}).fetchall()

    if not rows:
        log.info("  Нет ВИМ-фондов в БД (проверь tickers в `funds` table)")
        return 0

    total = 0
    for fund_id, ticker in rows:
        try:
            snap_date, holdings = fetch_vim_holdings(ticker)
        except Exception as e:
            log.warning(f"  {ticker:8s}: parser error: {e}")
            continue

        if not holdings:
            log.debug(f"  {ticker:8s}: пустой ответ")
            continue

        saved = save_fund_holdings(
            engine,
            fund_id,
            holdings,
            snapshot_date=snap_date.isoformat() if snap_date else None,
            source="vim",
        )
        log.info(f"  {ticker:8s} fund_id={fund_id:>6d}: {saved} позиций (snap={snap_date})")
        total += saved

    log.info(f"\n✅ ВИМ ГОТОВО: {total} позиций для {len(rows)} фондов")
    return total


# ═══════════════════════════════════════════════════════════════════════════════
# ОСНОВНАЯ ЛОГИКА
# ═══════════════════════════════════════════════════════════════════════════════

async def update_all_funds(force: bool = False) -> Dict[int, int]:
    """Обновить все фонды через Cbonds API.

    Загружает список фондов из таблицы funds (WHERE cbonds_id IS NOT NULL),
    для каждого получает NAV и nav_per_share из Cbonds.
    """
    engine = get_engine()
    results = {}
    today = date.today()

    # Загружаем список фондов из БД
    funds = load_funds_from_db(engine)
    if not funds:
        log.warning("Нет фондов с cbonds_id в таблице funds")
        return results

    log.info("=" * 60)
    log.info(f"📊 ОБНОВЛЕНИЕ ФОНДОВ (Cbonds API) — {len(funds)} фондов")
    log.info("=" * 60)

    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar()) as session:
        if not await cbonds_auth(session):
            log.error("  Пропуск: не удалось авторизоваться в Cbonds")
            return None  # сигнал «auth не прошла» → демон не закрывает день, повтор по cooldown

        for fund in funds:
            fund_id = fund["fund_id"]
            cbonds_id = fund["cbonds_id"]
            ticker = fund["ticker"] or str(fund_id)
            name = fund["name"] or ""

            log.info(f"\n── {ticker:8s} │ cbonds={cbonds_id} │ fund_id={fund_id} │ {name}")

            # Определяем дату начала
            last_date = get_last_date(engine, fund_id)
            if last_date and not force:
                start = last_date - timedelta(days=3)
                log.info(f"   Докачка с {start} (последняя: {last_date})")
            else:
                start = today - timedelta(days=365 * 3)  # 3 года истории
                log.info(f"   Полная загрузка с {start}")

            merged = await fetch_cbonds_nav(session, cbonds_id, start, today)
            log.info(f"   Cbonds: {len(merged)} дат")

            saved = save_fund_data(engine, fund_id, merged)
            results[fund_id] = saved

            if saved > 0:
                log.info(f"   ✅ Сохранено: {saved}")
            else:
                log.debug(f"   — нет новых данных")

            await asyncio.sleep(0.5)  # Не спамим Cbonds

        # 1) Состав фондов Cbonds — В ТОЙ ЖЕ session (без повторного логина).
        await update_all_holdings(engine, session)

    # 2) ВИМ — 6 фондов, daily T+1, с positions (штуки).
    #    Запускается ПОСЛЕ Cbonds. Из-за разных asset_name форматов это
    #    дополнение, не overwrite — в UI можно отфильтровать по source.
    try:
        update_vim_holdings(engine)
    except Exception as e:
        # ВИМ-парсер не должен ломать основной цикл fetch'а NAV.
        log.warning(f"  ВИМ parser failed: {e}", exc_info=True)

    engine.dispose()

    total = sum(results.values())
    log.info("=" * 60)
    log.info(f"✅ ГОТОВО: {total} записей для {len(funds)} фондов")
    log.info("=" * 60)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# ДЕМОН
# ═══════════════════════════════════════════════════════════════════════════════

# Файл состояния в /app/logs (named volume — переживает рестарт контейнера).
# Хранит дату последнего УСПЕШНОГО обновления и время последней ПОПЫТКИ. Благодаря
# этому перезапуск демона watchdog'ом НЕ триггерит повторный логин в Cbonds, если
# день уже закрыт, а при throttle попытки идут не чаще RETRY_COOLDOWN — это и
# устраняет churn логинов, который ловил rate-limit «limited rights».
STATE_FILE = Path(__file__).parent.parent / "logs" / "funds_state.json"
RETRY_COOLDOWN = timedelta(minutes=30)


def _read_state():
    """→ (last_success_date | None, last_attempt_datetime | None)."""
    try:
        d = json.loads(STATE_FILE.read_text())
        ls = date.fromisoformat(d["last_success"]) if d.get("last_success") else None
        la = datetime.fromisoformat(d["last_attempt"]) if d.get("last_attempt") else None
        return ls, la
    except Exception:
        return None, None


def _write_state(last_success, last_attempt):
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps({
            "last_success": last_success.isoformat() if last_success else None,
            "last_attempt": last_attempt.isoformat() if last_attempt else None,
        }))
    except Exception as e:
        log.warning(f"  Не удалось записать state {STATE_FILE}: {e}")


async def run_daemon():
    """Запуск в режиме демона"""
    log.info("🚀 Запуск демона обновления фондов")
    log.info(f"  Расписание: {UPDATE_HOUR:02d}:{UPDATE_MINUTE:02d} МСК")

    last_update_date, last_attempt = _read_state()
    log.info(f"  Состояние: last_success={last_update_date} last_attempt={last_attempt}")

    while True:
        now = get_moscow_time()
        today = now.date()
        is_trade_day, reason = is_trading_day(today)

        if is_trade_day and now.hour >= UPDATE_HOUR and last_update_date != today:
            # Не чаще RETRY_COOLDOWN — иначе при throttle демон (и его рестарты)
            # долбят логинами и сами поддерживают «limited rights».
            if last_attempt is None or (now - last_attempt) >= RETRY_COOLDOWN:
                log.info(f"⏰ [{now:%H:%M:%S} МСК] Запуск обновления...")
                last_attempt = now
                _write_state(last_update_date, last_attempt)  # фиксируем попытку ДО запуска (переживёт рестарт)
                try:
                    res = await update_all_funds(force=False)
                    if res is not None:  # авторизация прошла → день закрыт
                        last_update_date = today
                        _write_state(last_update_date, last_attempt)
                        log.info(f"  ✅ День закрыт: {today}")
                    else:
                        log.warning("  Cbonds auth не прошла — повтор после cooldown")
                except Exception as e:
                    log.error(f"Ошибка обновления: {e}")

        await asyncio.sleep(60)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="Обновление данных фондов (Cbonds API)")
    parser.add_argument("--once", action="store_true", help="Однократный запуск")
    parser.add_argument("--force", action="store_true", help="Принудительное обновление")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ЗАПУСК СКРИПТА ФОНДОВ — Cbonds API")
    log.info(f"Время: {datetime.now()}")
    log.info(f"МСК: {get_moscow_time()}")
    log.info(f"Режим: {'однократный' if args.once else 'daemon'}")
    log.info("=" * 60)

    # Проверка торгового дня в режиме --once
    if args.once and not args.force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            log.info(f"⏭️ Пропуск: {reason}")
            log.info("  (используйте --force для принудительного запуска)")
            return

    if args.once:
        await update_all_funds(force=args.force)
    else:
        await run_daemon()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено")
    except Exception as e:
        log.critical(f"Ошибка: {e}")
        sys.exit(1)
