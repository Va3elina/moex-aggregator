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

# Директория логов
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Cbonds API
CBONDS_URL = "https://rest2.cbonds.info"
CBONDS_UA = "Cbonds.K/3.0.8 (ru.cbonds.cbonds; build:636; Android 9) OkHttp/4.12.0"
CBONDS_LOGIN = os.getenv("CBONDS_LOGIN", "ermolaeffvadick@yandex.ru")
CBONDS_PASSWORD = os.getenv("CBONDS_PASSWORD", "Qwghty56")


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

    fh = logging.FileHandler(
        LOG_DIR / f"funds_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(detailed_fmt)
    root.addHandler(fh)

    fh_err = logging.FileHandler(
        LOG_DIR / f"funds_errors_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

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
                weight = float(i.get("weight", 0))
                # Дата snapshot'а от УК — берём из первого item с непустой датой.
                # У всех items должна быть одна дата (один snapshot).
                if snapshot_date is None and i.get("date"):
                    snapshot_date = i["date"][:10]  # YYYY-MM-DD из ISO
                if weight > 0 and name not in seen:
                    seen.add(name)
                    result.append({"name": name, "weight": weight})
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

    Args:
        snapshot_date: ISO YYYY-MM-DD дата к которой относится snapshot.
            Если None — используется CURRENT_DATE (сегодня).
        source: 'cbonds' / 'vim' / 'pervaya' etc. — для трекинга
            источника данных в analytics.

    History append идёт через INSERT ... ON CONFLICT DO NOTHING чтобы
    повторные запуски с тем же snapshot'ом не плодили дубли.
    """
    if not holdings:
        return 0
    with engine.connect() as conn:
        # 1. Current snapshot — DELETE + INSERT, как раньше.
        conn.execute(text("DELETE FROM fund_holdings WHERE fund_id = :fid"), {"fid": fund_id})
        n = 0
        for h in holdings:
            conn.execute(text("""
                INSERT INTO fund_holdings (fund_id, asset_name, weight, updated_at)
                VALUES (:fid, :name, :weight, NOW())
            """), {"fid": fund_id, "name": h["name"], "weight": h["weight"]})
            n += 1

        # 2. History append — идемпотентно через UNIQUE constraint.
        # Если snapshot_date не передана — fallback на сегодня (для парсеров
        # которые не отдают дату).
        date_clause = ":snap_date" if snapshot_date else "CURRENT_DATE"
        params_base: dict = {"fid": fund_id, "source": source}
        if snapshot_date:
            params_base["snap_date"] = snapshot_date

        for h in holdings:
            conn.execute(text(f"""
                INSERT INTO fund_holdings_history
                    (fund_id, asset_name, weight, positions, amount_rub,
                     snapshot_date, source, created_at)
                VALUES
                    (:fid, :name, :weight, :positions, :amount_rub,
                     {date_clause}, :source, NOW())
                ON CONFLICT (fund_id, asset_name, snapshot_date) DO NOTHING
            """), {
                **params_base,
                "name": h["name"],
                "weight": h["weight"],
                "positions": h.get("positions"),
                "amount_rub": h.get("amount_rub"),
            })

        conn.commit()
        return n


async def update_all_holdings(engine) -> int:
    """Обновить состав всех фондов через Cbonds API."""
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
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar()) as session:
        if not await cbonds_auth(session):
            log.error("  Пропуск: не удалось авторизоваться")
            return 0

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
            return results

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

    # Состав фондов (раз в день)
    # 1) Cbonds — все фонды, monthly snapshot, weight only.
    await update_all_holdings(engine)
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

async def run_daemon():
    """Запуск в режиме демона"""
    log.info("🚀 Запуск демона обновления фондов")
    log.info(f"  Расписание: {UPDATE_HOUR:02d}:{UPDATE_MINUTE:02d} МСК")

    last_update_date = None

    while True:
        now = get_moscow_time()
        today = now.date()

        # Проверяем расписание
        is_trade_day, reason = is_trading_day(today)

        if is_trade_day:
            if now.hour >= UPDATE_HOUR and now.minute >= UPDATE_MINUTE:
                if last_update_date != today:
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Запуск обновления...")
                    try:
                        await update_all_funds(force=False)
                        last_update_date = today
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
