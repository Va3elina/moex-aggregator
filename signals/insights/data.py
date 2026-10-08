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

def _news_recent() -> tuple[str, dict]:
    """Связки (signals/insights/combos.py): новости за 25 дней — ноги-новости и «обычный день темы» для всплеска
    (медиана за 20 дней). Темы — единым словарём второго мозга (Brain/news_types.py), шум — его же фильтром
    (vocab.sql_шум): своего классификатора тем у связок нет (Вадим 27.09: «надо закрыть тему второго мозга»)."""
    from api.brain_core import _словарь  # noqa: PLC0415 — Brain/ ищется от корня репо, в образе и на хосте
    vocab, nt = _словарь()
    p: dict = {}
    чисто = vocab.sql_чисто("na.text", p)
    return f"""
        SELECT na.channel, na.posted_at, coalesce(na.views, 0) AS views,
               coalesce(array_to_string(na.hashtags, ','), '') AS hashtags, left(na.text, 2000) AS text,
               CASE WHEN NOT {vocab.sql_шум("na.text", "x.ч", p, канал="na.channel", теги="na.hashtags")}
                    THEN {nt.sql_topics("na", p)} ELSE CAST(ARRAY[] AS text[]) END AS темы
        FROM news_archive na
        CROSS JOIN LATERAL (SELECT {чисто} AS ч) x
        WHERE na.posted_at >= now() - interval '25 days'""", p


# Новости и события мозга — окно, достаточное для контекста карточки (неделя до поста и
# полтора месяца событий); позиции, фонды, цены — вся история: рекорд «с 2012 года»
# считается по всему ряду. Запрос с параметрами — функция, возвращающая (SQL, параметры).
QUERIES = {
    # последняя 5-минутная запись физлиц за самый свежий день — сверка находки с утром (Вадим 18.09:
    # Мечел — шорт 70,7 тыс. на закрытии 17.09, к 09:10 МСК 38,8 тыс.: позиции «полетели вниз»)
    "oi_intraday_last": """
        SELECT DISTINCT ON (sectype) sectype, tradedate, tradetime, pos_long, pos_short, pos_long_num, pos_short_num
        FROM open_interest
        WHERE interval = 5 AND clgroup = 'FIZ'
          AND tradedate = (SELECT max(tradedate) FROM open_interest WHERE interval = 5 AND clgroup = 'FIZ')
        ORDER BY sectype, tradetime DESC""",
    "oi_daily": """
        SELECT DISTINCT ON (sectype, clgroup, tradedate)
               sectype, clgroup, tradedate, pos_long, pos_short, pos_long_num, pos_short_num
        FROM open_interest
        WHERE interval = 24 AND clgroup = 'FIZ'
        ORDER BY sectype, clgroup, tradedate, tradetime DESC""",
    "instruments": 'SELECT name, sectype, sec_id, type, "group", iss_code, sector, hidden FROM instruments',
    "index_data": """SELECT secid, trade_date, close FROM index_data
                     WHERE secid IN ('IMOEX', 'RTSI', 'RGBI', 'RGBITR', 'MCFTR', 'RUSFAR3M', 'RUSFARCNY')""",
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
    "news_recent": _news_recent,
    "cbr_flows": """SELECT instrument_type, period_kind, period_end_date, category, value, updated_at
                    FROM cbr_flows""",
}


@lru_cache(None)
def _frame(name: str) -> pd.DataFrame:
    db = SessionLocal()
    try:
        q = QUERIES[name]
        sql, p = q() if callable(q) else (q, {})
        res = db.execute(text(sql), p)
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


# ── цена фьючерса без базового актива ────────────────────────────────────────────
# У нефти, газа, металлов (BR, NG, SV, PT…) нет акции, индекса или курса — цена на графике и в брифе пропадала
# (#3961, рывок шорта по Brent 08.10: линии цены на графике нет). Склейка ближнего фьючерса — та же, что у витрины
# /hot (api/services/hot.py: _prices + front_month); завод витрину не импортирует, правило повторено здесь.
_FRONT_DAILY = """
    SELECT CAST(c.begin_time AS date) AS d, c.close, f.lsttrade, f.is_perpetual
      FROM candles c JOIN futures_contracts f ON f.secid = c.secid
     WHERE f.sectype = :s AND c.interval = 24 AND c.type = 'futures' AND c.close > 0
     ORDER BY 1"""
_FRONT_LIVE = """
    SELECT c.begin_time AS t, c.close
      FROM candles c JOIN futures_contracts f ON f.secid = c.secid
     WHERE f.sectype = :s AND c.interval = 5 AND c.type = 'futures' AND c.close > 0
       AND c.begin_time >= :d AND c.begin_time <= :now
       AND (f.is_perpetual OR f.lsttrade >= CAST(:d AS date))
     ORDER BY coalesce(f.lsttrade, DATE '9999-01-01'), c.begin_time DESC
     LIMIT 1"""


def splice_front(rows) -> pd.Series:
    """На каждый день — ближайший к экспирации контракт, который ещё торгуется; вечный — когда срочных нет.
    rows: (день, close, lsttrade, вечный). Повтор api/services/hot.front_month."""
    far = pd.Timestamp("2262-01-01").date()
    best: dict = {}
    for d, close, lst, perp in rows:
        d = pd.Timestamp(d).date()
        exp = far if perp or lst is None or pd.isna(lst) else pd.Timestamp(lst).date()
        if exp < d:
            continue
        if d not in best or exp < best[d][0]:
            best[d] = (exp, float(close))
    days = sorted(best)
    return pd.Series([best[d][1] for d in days], index=pd.DatetimeIndex(days), dtype=float)


def _query(sql: str, params: dict) -> list:
    db = SessionLocal()
    try:
        return db.execute(text(sql), params).fetchall()
    finally:
        db.close()


@lru_cache(None)
def front_month(sectype: str) -> pd.Series:
    """Дневная цена фьючерса склейкой ближнего контракта (пустой ряд, если свечей нет)."""
    return splice_front(_query(_FRONT_DAILY, {"s": sectype}))


def front_live(sectype: str, day, now):
    """Последняя 5-минутная цена ближнего контракта за день `day`, не позже now → (цена, время МСК) или None."""
    rows = _query(_FRONT_LIVE, {"s": sectype, "d": pd.Timestamp(day).normalize(), "now": pd.Timestamp(now)})
    return (float(rows[0][1]), pd.Timestamp(rows[0][0])) if rows else None
