"""Движок находок: ряды из боевой базы вместо CSV-выгрузок исследования.

Исследование (research/content_pipeline_v2/insights/) читало data/*.csv — выгрузки ровно этих
запросов. Здесь те же таблицы через SQLAlchemy + pg8000, как у content_ai на хосте; имена и
колонки DataFrame совпадают с CSV, поэтому detect.py и cards.py перенесены почти без правок:
`pd.read_csv(".../x.csv", parse_dates=[...])` → `data.read("x", parse_dates=[...])`.

⚠️ pg8000: литерал «%» в тексте запроса ломает подстановку параметров — поэтому в запросах нет
LIKE 'x%' (вместо него left(...) = ...), а «:» стоит только внутри строковых литералов.
"""
import decimal
from functools import lru_cache

import pandas as pd
from sqlalchemy import text

from api.database import SessionLocal

# Новости и события мозга — окно, достаточное для контекста карточки (неделя до поста и
# полтора месяца событий); позиции, фонды, цены — вся история: рекорд «с 2012 года»
# считается по всему ряду.
QUERIES = {
    "oi_daily": """
        SELECT DISTINCT ON (sectype, clgroup, tradedate)
               sectype, clgroup, tradedate, pos_long, pos_short, pos_long_num, pos_short_num
        FROM open_interest
        WHERE interval = 24 AND clgroup = 'FIZ'
        ORDER BY sectype, clgroup, tradedate, tradetime DESC""",
    "instruments": 'SELECT name, sectype, sec_id, type, "group", iss_code, sector, hidden FROM instruments',
    "index_data": """SELECT secid, trade_date, close FROM index_data
                     WHERE secid IN ('IMOEX', 'RTSI', 'RGBI', 'RGBITR', 'MCFTR')""",
    "candles_stocks": """SELECT secid, CAST(begin_time AS date) AS d, close FROM candles
                         WHERE interval = 24 AND type = 'stock' AND begin_time >= '2021-01-01'""",
    "candles_perp": """SELECT secid, CAST(begin_time AS date) AS d, close FROM candles
                       WHERE interval = 24 AND secid IN ('USDRUBF', 'CNYRUBF', 'EURRUBF', 'GLDRUBF', 'IMOEXF')""",
    "funds": "SELECT fund_id, ticker, name, category, subcategory FROM funds",
    "fund_data": "SELECT fund_id, trade_date, nav, pay FROM fund_data WHERE trade_date >= '2021-01-01'",
    "breadth": "SELECT trade_date, ema_period, universe, percent_above FROM breadth_history",
    "macro": """SELECT indicator, period_date, value FROM macro_data
                WHERE indicator IN ('MARKET_CAP_TOTAL', 'GDP_QUARTERLY')""",
    "brain_events": "SELECT id, kind, ts, title, payload FROM brain_nodes WHERE kind IN ('fund_event', 'index_event')",
    "news_ctx": """
        SELECT channel, posted_at, coalesce(views, 0) AS views,
               left(regexp_replace(text, '\\s+', ' ', 'g'), 320) AS text
        FROM news_archive
        WHERE posted_at >= now() - interval '21 days'
          AND text ~* '(ЦБ|ставк|инфляц|Минфин|бюджет|санкц|переговор|нефт|рубл|доллар|юан|валют|курс|облигац|ОФЗ|индекс|IMOEX|Мосбирж|фонд|БПИФ|девальвац|доходност)'""",
    "key_rate": "SELECT valid_from, statement FROM world_facts WHERE kind = 'ключевая ставка' ORDER BY valid_from",
    "brain_ctx": """
        SELECT n.id, n.kind, n.ts, left(coalesce(n.title, ''), 220) AS title,
               string_agg(DISTINCT CASE WHEN e.src = n.id THEN e.dst ELSE e.src END, ' ') AS comps
        FROM brain_nodes n
        LEFT JOIN brain_edges e
          ON (e.src = n.id AND left(e.dst, 8) = 'company:') OR (e.dst = n.id AND left(e.src, 8) = 'company:')
        WHERE n.kind IN ('fund_event', 'index_event', 'disclosure', 'report', 'exchange')
          AND n.ts >= now() - interval '60 days'
        GROUP BY n.id, n.kind, n.ts, n.title""",
    # свежие посты канала — фильтр повторов и строка «что канал уже писал»
    "channel_posts": """SELECT post_id, posted_at, text FROM channel_posts
                        WHERE channel = 'FrameTool' AND posted_at >= now() - interval '45 days'
                        ORDER BY posted_at DESC""",
}


@lru_cache(None)
def _frame(name: str) -> pd.DataFrame:
    db = SessionLocal()
    try:
        res = db.execute(text(QUERIES[name]))
        df = pd.DataFrame(res.fetchall(), columns=list(res.keys()))
    finally:
        db.close()
    # NUMERIC приходит из pg8000 как Decimal — в float, как было в CSV
    for c in df.columns:
        s = df[c].dropna()
        if len(s) and isinstance(s.iloc[0], decimal.Decimal):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def read(name: str, parse_dates=None, **_kw) -> pd.DataFrame:
    """Замена pd.read_csv для рядов исследования: та же таблица, те же колонки."""
    df = _frame(name).copy()
    for c in parse_dates or []:
        df[c] = pd.to_datetime(df[c])
    return df
