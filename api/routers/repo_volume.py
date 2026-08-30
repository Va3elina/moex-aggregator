"""
/api/repo — Репо в акциях (экспериментальная вкладка, тест гипотезы).

Гипотеза: объём сделок РЕПО с ЦК по конкретной акции — прокси шортов
(участники берут бумагу в репо, чтобы продать её в короткую). Вкладка
накладывает дневной объём репо на спот-котировку той же бумаги.

Источник репо — ISS history рынка ccp (РЕПО с ЦК): по каждой бумаге дневные
строки per-board. Суммируем ТОЛЬКО рублёвые борды EQRP (стакан) + PSRP
(адресные): у валютных EQRD/PSRD поле VALUE пустое, а юаневый PSRY отдаёт
VALUE в другой валюте — смешивать нельзя (проверено на SBER 2026-08).
Все строки идут с TRADINGSESSION=3 (итог дня) — дублей по сессиям нет.

В БД репо-объёмы не ингестятся (фича тестовая, 5 бумаг) — история тянется
с ISS on-demand и держится в Redis. Первая загрузка тикера ~40-60 страниц
ISS (пагинация по 100 строк), дальше отдаётся из кэша.

Спот — дневные свечи из candles (как в breadth), со split-adjustment из
общего реестра KNOWN_SPLITS (SFIN 1.93 обязателен — иначе разрыв цены).
"""
import asyncio
from datetime import date

import httpx
from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from api.cache import get_or_set
from api.database import get_engine
from api.logger import get_logger
from api.routers.breadth import _adjust_for_split

log = get_logger()

router = APIRouter(prefix="/api/repo", tags=["repo"])

# Тестовая пятёрка: SFIN — исходный кейс гипотезы (сквиз-история), SBER/GAZP —
# ликвидные бенчмарки, MGNT/MVID — бумаги с известными шорт-историями.
# Имена — по единому стандарту (instruments/ОИ).
REPO_ASSETS: dict[str, str] = {
    "SFIN": "ЭсЭфАй",
    "SBER": "Сбербанк",
    "GAZP": "Газпром",
    "MGNT": "Магнит",
    "MVID": "М.Видео",
}

# Рублёвые борды РЕПО с ЦК (см. docstring — почему только эти два).
RUB_BOARDS = frozenset({"EQRP", "PSRP"})

# Ковид-обвал 2020 включён в окно намеренно — интересная точка для гипотезы.
HISTORY_FROM = date(2020, 1, 1)

ISS_URL = "https://iss.moex.com/iss/history/engines/stock/markets/ccp/securities/{ticker}.json"


async def _fetch_iss_page(client: httpx.AsyncClient, ticker: str, start: int) -> dict:
    resp = await client.get(
        ISS_URL.format(ticker=ticker),
        params={
            "iss.meta": "off",
            "from": HISTORY_FROM.isoformat(),
            "till": date.today().isoformat(),
            "start": start,
        },
    )
    resp.raise_for_status()
    return resp.json()


def _sum_rub_boards(pages: list[dict], acc: dict[str, float]) -> None:
    """Складывает VALUE рублёвых бордов в acc[date]. Пустые VALUE пропускает."""
    for page in pages:
        cols = page["history"]["columns"]
        i_board = cols.index("BOARDID")
        i_date = cols.index("TRADEDATE")
        i_value = cols.index("VALUE")
        for row in page["history"]["data"]:
            if row[i_board] not in RUB_BOARDS or not row[i_value]:
                continue
            acc[row[i_date]] = acc.get(row[i_date], 0.0) + float(row[i_value])


async def _fetch_repo_series(ticker: str) -> dict[str, float]:
    """{ISO-дата: дневной объём репо, ₽} с ISS. Кэш Redis 6ч на тикер."""
    cache_key = f"repo_shorts:iss:{ticker}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    acc: dict[str, float] = {}
    async with httpx.AsyncClient(timeout=20) as client:
        first = await _fetch_iss_page(client, ticker, 0)
        cursor_cols = first["history.cursor"]["columns"]
        cursor_row = first["history.cursor"]["data"][0]
        total = int(cursor_row[cursor_cols.index("TOTAL")])
        page_size = int(cursor_row[cursor_cols.index("PAGESIZE")]) or 100
        _sum_rub_boards([first], acc)

        # Остальные страницы параллельно, но не больше 4 запросов к ISS разом.
        sem = asyncio.Semaphore(4)

        async def fetch_limited(start: int) -> dict:
            async with sem:
                return await _fetch_iss_page(client, ticker, start)

        starts = range(page_size, total, page_size)
        pages = await asyncio.gather(*(fetch_limited(s) for s in starts))
        _sum_rub_boards(list(pages), acc)

    get_or_set(cache_key, acc, ttl=6 * 3600)
    return acc


def _load_spot_closes(ticker: str) -> list[tuple]:
    """[(date, close), ...] дневного спота из candles, split-adjusted."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT begin_time::date AS d, close
            FROM candles
            WHERE secid = :ticker AND interval = 24 AND type = 'stock'
              AND begin_time::date >= :date_from AND close > 0
            ORDER BY begin_time
        """), {"ticker": ticker, "date_from": HISTORY_FROM}).fetchall()
    dated = [(r[0], float(r[1])) for r in rows]
    return _adjust_for_split(ticker, dated)


@router.get("/assets")
async def get_repo_assets():
    """Тестовый шорт-лист бумаг вкладки."""
    return {"assets": [{"ticker": t, "name": n} for t, n in REPO_ASSETS.items()]}


@router.get("/history")
async def get_repo_history(ticker: str = "SFIN"):
    """
    Объём репо + спот-цена по одной бумаге на общих датах.
    repo — ₽ за день (EQRP+PSRP), close — дневное закрытие спота.
    """
    ticker = ticker.upper()
    if ticker not in REPO_ASSETS:
        raise HTTPException(status_code=404, detail="Бумага не входит в тестовый список")

    # Кэш готового ответа короче кэша ISS-серии: спот-свечи обновляются intraday.
    cache_key = f"repo_shorts:hist:{ticker}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    try:
        repo_by_date = await _fetch_repo_series(ticker)
    except httpx.HTTPError as e:
        log.error(f"repo/history: ISS fetch failed for {ticker}: {e}")
        raise HTTPException(status_code=502, detail="ISS MOEX недоступен, попробуйте позже")

    spot = _load_spot_closes(ticker)

    # Общие даты: без спота точка не нужна (выходные репо не торгуется, и
    # наоборот — выходные споты без репо дают провалы в ноль). Дни спота без
    # репо-сделок (неликвид) отдаём с repo=0 — это честный «нет объёма».
    data = [
        {"date": d.isoformat(), "close": close, "repo": repo_by_date.get(d.isoformat(), 0.0)}
        for d, close in spot
    ]

    result = {
        "ticker": ticker,
        "name": REPO_ASSETS[ticker],
        "data": data,
        "updated_at": data[-1]["date"] if data else None,
    }
    get_or_set(cache_key, result, ttl=1800)
    return result
