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

Бумаги — любая акция из instruments, по которой у нас есть свежий дневной
спот (~130 штук). В БД репо-объёмы не ингестятся: история тянется с ISS
on-demand и держится в Redis. Холодная загрузка ликвидной бумаги — до ~100
страниц ISS (SBER с 2013 ≈ 9.8к строк, 8-10 секунд), дальше из кэша. Если
вкладку будут открывать публично, тут нужен нормальный ингест в таблицу.

Спот — дневные свечи из candles (как в breadth), со split-adjustment из
общего реестра KNOWN_SPLITS (SFIN 1.93 обязателен — иначе разрыв цены).

Помимо объёма отдаём «градусник шорта» — ставки:
  rate   — ставка стакана EQRP (WAPRICE, fallback CLOSE), % годовых. Только
           стакан: в адресном PSRP ставки договорные (внутригрупповые сделки,
           фиксированные 18%/-0.5% день за днём) и дефицит бумаги не отражают.
  rusfar — бенчмарк «цены денег» RUSFAR (индекс MOEX по РЕПО с ЦК КСУ,
           борд MMIX). rate << rusfar ⇒ бумага «special»: её берут ради неё
           самой (шорты), а не ради денег. rate ≈ rusfar ⇒ фондирование.

Вкладка admin-only (обкатка гипотезы перед публичным релизом) — оба
эндпоинта под require_admin.
"""
import asyncio
from datetime import date

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from api.cache import get_or_set
from api.database import get_engine
from api.logger import get_logger
from api.routers.auth import require_admin
from api.routers.breadth import _adjust_for_split

log = get_logger()

router = APIRouter(prefix="/api/repo", tags=["repo"])

# Рублёвые борды РЕПО с ЦК (см. docstring — почему только эти два).
RUB_BOARDS = frozenset({"EQRP", "PSRP"})

# Рынок РЕПО с ЦК запущен MOEX в 2013 (первые строки ISS — июль 2013), раньше
# данных нет вообще. Спред считается только с 2018 — RUSFAR начинается там.
HISTORY_FROM = date(2013, 1, 1)

ISS_URL = "https://iss.moex.com/iss/history/engines/stock/markets/ccp/securities/{ticker}.json"


def _collect_rub_boards(pages: list[dict], vol: dict[str, float], rate: dict[str, float]) -> None:
    """Из строк рублёвых бордов: vol[date] += VALUE (EQRP+PSRP), rate[date] =
    ставка стакана EQRP (WAPRICE, fallback CLOSE). Пустые значения пропускает."""
    for page in pages:
        cols = page["history"]["columns"]
        i_board = cols.index("BOARDID")
        i_date = cols.index("TRADEDATE")
        i_value = cols.index("VALUE")
        i_wap = cols.index("WAPRICE")
        i_close = cols.index("CLOSE")
        for row in page["history"]["data"]:
            if row[i_board] not in RUB_BOARDS or not row[i_value]:
                continue
            vol[row[i_date]] = vol.get(row[i_date], 0.0) + float(row[i_value])
            if row[i_board] == "EQRP":
                # WAPRICE может быть 0/None — тогда CLOSE. 0 как ставка возможна,
                # но ISS отдаёт 0 и в «нет данных»; для градусника потеря
                # нулевой точки некритична, ложная — вредна.
                r = row[i_wap] if row[i_wap] not in (None, 0, "0") else row[i_close]
                if r is not None:
                    rate[row[i_date]] = float(r)


async def _fetch_iss_history(
    client: httpx.AsyncClient, url: str, params: dict
) -> list[dict]:
    """Все страницы ISS-history эндпоинта (cursor TOTAL/PAGESIZE, конкуренция 6).

    История с 2013 — до ~100 страниц на ликвидную бумагу (SBER ≈ 9.8к строк),
    порядка 8-10 секунд на холодную загрузку. Конкуренцию выше 6 не поднимаем:
    ISS начинает резать запросы."""
    resp0 = await client.get(url, params={**params, "start": 0})
    resp0.raise_for_status()
    first = resp0.json()
    cursor_cols = first["history.cursor"]["columns"]
    cursor_row = first["history.cursor"]["data"][0]
    total = int(cursor_row[cursor_cols.index("TOTAL")])
    page_size = int(cursor_row[cursor_cols.index("PAGESIZE")]) or 100

    sem = asyncio.Semaphore(6)

    async def fetch_limited(start: int) -> dict:
        async with sem:
            resp = await client.get(url, params={**params, "start": start})
            resp.raise_for_status()
            return resp.json()

    pages = await asyncio.gather(*(fetch_limited(s) for s in range(page_size, total, page_size)))
    return [first, *pages]


async def _fetch_repo_series(ticker: str) -> dict[str, dict[str, float]]:
    """{"vol": {дата: объём ₽}, "rate": {дата: ставка EQRP %}}. Кэш 6ч."""
    cache_key = f"repo_shorts:iss:v3:{ticker}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    params = {
        "iss.meta": "off",
        "from": HISTORY_FROM.isoformat(),
        "till": date.today().isoformat(),
    }
    async with httpx.AsyncClient(timeout=30) as client:
        pages = await _fetch_iss_history(client, ISS_URL.format(ticker=ticker), params)
    vol: dict[str, float] = {}
    rate: dict[str, float] = {}
    _collect_rub_boards(pages, vol, rate)

    result = {"vol": vol, "rate": rate}
    get_or_set(cache_key, result, ttl=6 * 3600)
    return result


RUSFAR_URL = "https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RUSFAR.json"


async def _fetch_rusfar_series() -> dict[str, float]:
    """{ISO-дата: RUSFAR % годовых} с ISS (борд MMIX, CLOSE). Кэш 6ч, общий."""
    cache_key = "repo_shorts:rusfar:v3"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    params = {
        "iss.meta": "off",
        "from": HISTORY_FROM.isoformat(),
        "till": date.today().isoformat(),
    }
    async with httpx.AsyncClient(timeout=30) as client:
        pages = await _fetch_iss_history(client, RUSFAR_URL, params)
    acc: dict[str, float] = {}
    for page in pages:
        cols = page["history"]["columns"]
        i_date = cols.index("TRADEDATE")
        i_close = cols.index("CLOSE")
        for row in page["history"]["data"]:
            if row[i_close] is not None:
                acc[row[i_date]] = float(row[i_close])

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


def _resolve_stock(ticker: str) -> str | None:
    """Имя акции из instruments, если такая бумага у нас есть. Иначе None.

    Заодно это валидация: тикер уходит в URL запроса к ISS, поэтому в фетчер
    пускаем только то, что реально лежит в нашей таблице инструментов."""
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT COALESCE(NULLIF(name, ''), sec_id)
            FROM instruments
            WHERE sec_id = :ticker AND type = 'stock'
            LIMIT 1
        """), {"ticker": ticker}).fetchone()
    return row[0] if row else None


@router.get("/assets")
async def get_repo_assets(user=Depends(require_admin)):
    """Акции, по которым вкладка может построить график — те, у кого есть
    дневной спот за последние 2 месяца (репо тянется с ISS уже по факту)."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT i.sec_id, COALESCE(NULLIF(i.name, ''), i.sec_id) AS name
            FROM instruments i
            JOIN candles c ON c.secid = i.sec_id AND c.interval = 24 AND c.type = 'stock'
            WHERE i.type = 'stock' AND c.begin_time > CURRENT_DATE - 60
            ORDER BY name
        """)).fetchall()
    return {"assets": [{"ticker": r[0], "name": r[1]} for r in rows]}


@router.get("/history")
async def get_repo_history(ticker: str = "SFIN", user=Depends(require_admin)):
    """
    Объём репо + спот-цена + ставки по одной бумаге.
    repo — ₽ за день (EQRP+PSRP), close — дневное закрытие спота,
    rate — ставка стакана EQRP (% годовых, null если сделок не было),
    rusfar — бенчмарк цены денег (% годовых, null для дат без фиксинга).
    """
    ticker = ticker.upper()
    name = _resolve_stock(ticker)
    if not name:
        raise HTTPException(status_code=404, detail="Бумага не найдена среди акций")

    # Кэш готового ответа короче кэша ISS-серии: спот-свечи обновляются intraday.
    cache_key = f"repo_shorts:hist:v3:{ticker}"
    cached = get_or_set(cache_key)
    if cached is not None:
        return cached

    try:
        series = await _fetch_repo_series(ticker)
        rusfar = await _fetch_rusfar_series()
    except httpx.HTTPError as e:
        log.error(f"repo/history: ISS fetch failed for {ticker}: {e}")
        raise HTTPException(status_code=502, detail="ISS MOEX недоступен, попробуйте позже")

    spot = _load_spot_closes(ticker)
    vol_by_date, rate_by_date = series["vol"], series["rate"]

    # База — даты спота: без цены точка не нужна (репо в выходные не торгуется).
    # Дни спота без репо-сделок отдаём с repo=0 — честный «нет объёма»; ставки
    # без сделок — null (ноль тут врал бы «бумага стоит как деньги»).
    data = [
        {
            "date": (ds := d.isoformat()),
            "close": close,
            "repo": vol_by_date.get(ds, 0.0),
            "rate": rate_by_date.get(ds),
            "rusfar": rusfar.get(ds),
        }
        for d, close in spot
    ]

    result = {
        "ticker": ticker,
        "name": name,
        "data": data,
        "updated_at": data[-1]["date"] if data else None,
        # Ноль репо-сделок за всю историю — у многих неликвидов; фронт по
        # этому флагу говорит «репо не торгуется», а не рисует пустую линию.
        "has_repo": any(p["repo"] > 0 for p in data),
    }
    get_or_set(cache_key, result, ttl=1800)
    return result
