"""
Ныряние в базу для админской панели: /api/admin/dashboard/db/*

⚠️ НЕ БРАУЗЕР ПО 72 ТАБЛИЦАМ, А ДВА ДЕСЯТКА КУРИРУЕМЫХ СРЕЗОВ. Свободный SQL сюда
не попадёт: candles — 32 миллиона строк и 14 ГБ индексов, один COUNT(*) без
условия кладёт базу на минуту. Каждый срез описан целиком здесь — откуда берём,
какие колонки, какие фильтры допустимы, что обязательно и какое окно по
умолчанию, — а в запрос уходят только ЗНАЧЕНИЯ через bind-параметры. Имя колонки
или таблицы от пользователя не принимается никогда.

⚠️ БЕЛЫЙ СПИСОК ЗАКРЫТ НА СЕРВЕРЕ, НЕ СПРЯТАН ВО ФРОНТЕ. users, refresh_tokens,
subscriptions, user_payment_methods, api_keys, analytics_events и остальное с
персональными данными в реестре не объявлено — значит, недостижимо через эту
ручку в принципе, а не «пока кнопки нет». Проверка внизу (_ЗАПРЕЩЕНО) роняет
импорт модуля, если кто-то объявит такой срез.

⚠️ ЛОВУШКА ДВУХ ИМЁН КАНАЛА. Исторический экспорт лежит под `MarketTwits` и
`СМАРТЛАБ НОВОСТИ` (по 31.08), живой ингест — под `markettwits` и `newssmartlab`.
Фильтр «по MarketTwits» буквально отрезал бы всё свежее. Поэтому значение
выбора канала — это ОБА имени через запятую, а условие раскрывает его
string_to_array'ем (см. _КАНАЛЫ).

⚠️ ТЯЖЁЛЫЕ ТАБЛИЦЫ — ТОЛЬКО С ОБЯЗАТЕЛЬНЫМ КЛЮЧОМ И ОКНОМ ПО ДАТЕ. У candles
обязателен тикер, у open_interest — код фьючерса, у company_metrics — тикер;
окно по умолчанию узкое, потолок — год. Всё это ложится на существующие индексы
(secid, interval, begin_time) и (sectype, clgroup, tradedate).

⚠️ `:по::date` В text() — ЛОВУШКА SQLAlchemy. Регулярка bind-параметров не
принимает двоеточие сразу после имени и откатывается на более короткое имя —
`:по::date` тихо превращается в параметр `п`. Поэтому везде CAST(:x AS date).

Только чтение, только админ.
"""

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.cache import get_or_set
from api.database import get_db
from api.models import User
from api.routers.auth import require_admin

router = APIRouter(prefix="/api/admin/dashboard/db", tags=["admin-dashboard"])

_TTL_СПРАВОЧНИКОВ = 300  # варианты селектов и оценка размеров — раз в пять минут
_ПОТОЛОК_СЧЁТА = 10_000  # дальше считать не станем: «10 000+» честнее минуты ожидания
_ПРЕДЕЛ = 100
_МАКС_ПРЕДЕЛ = 500


@dataclass(frozen=True)
class Фильтр:
    ключ: str
    подпись: str
    # text | ticker | select | number | bool | date | date_from | date_to
    тип: str
    sql: str
    обязателен: bool = False
    варианты_sql: str | None = None       # SELECT значение, подпись  (маленькие таблицы)
    варианты: tuple[tuple[str, str], ...] | None = None
    по_умолчанию: str | None = None
    подсказка: str | None = None


@dataclass(frozen=True)
class Колонка:
    ключ: str
    подпись: str
    # text | long | int | num | pct | date | ts | arr | bool | json | ticker | candidate | pipeline | tg
    тип: str = "text"
    sql: str | None = None                # по умолчанию — сам ключ


@dataclass(frozen=True)
class Срез:
    код: str
    группа: str
    имя: str
    описание: str
    таблица: str                          # основная таблица — для оценки размера
    откуда: str                           # FROM … (с JOIN'ами)
    колонки: tuple[Колонка, ...]
    фильтры: tuple[Фильтр, ...]
    порядок: str
    дата_колонка: str | None = None       # к чему относятся с/по
    окно_дней: int | None = None          # окно по умолчанию, если с/по не заданы
    макс_окно_дней: int | None = None
    подготовить: Callable | None = None   # (db, значения) -> пояснение | None
    предупреждение: str | None = None


# Значение выбора = оба имени канала; условие раскрывает его в массив.
_КАНАЛЫ = (
    ("MarketTwits,markettwits", "MarketTwits"),
    ("СМАРТЛАБ НОВОСТИ,newssmartlab", "Смартлаб новости"),
)
_КАНАЛЫ_ЖИВЫЕ = (("markettwits", "MarketTwits"), ("newssmartlab", "Смартлаб новости"))
_ИНТЕРВАЛЫ = (("24", "день"), ("60", "час"), ("5", "5 минут"))


def _с(кол: str) -> Фильтр:
    return Фильтр("с", "с", "date_from", f"{кол} >= CAST(:с AS date)")


def _по(кол: str) -> Фильтр:
    # Для timestamp-колонок «по 05.09» должно включать весь день 05.09.
    return Фильтр("по", "по", "date_to", f"{кол} < CAST(:по AS date) + 1")


def _тикер(ключ: str, подпись: str, sql: str, обязателен: bool = False, подсказка: str | None = None) -> Фильтр:
    return Фильтр(ключ, подпись, "ticker", sql, обязателен=обязателен, подсказка=подсказка)


def _последняя_дата(db: Session, sql: str, параметры: dict) -> date | None:
    return db.execute(text(sql), параметры).scalar()


def _подготовить_состав(db: Session, з: dict) -> str | None:
    """Без тикера и даты — последний состав выбранного индекса, а не 200 тысяч строк."""
    if з.get("ticker") or з.get("date"):
        return None
    d = _последняя_дата(db, "SELECT MAX(trade_date) FROM index_composition WHERE index_id = :index_id",
                        {"index_id": з.get("index_id") or "IMOEX"})
    if d is None:
        return None
    з["date"] = d.isoformat()
    return f"показан последний состав — от {d.strftime('%d.%m.%Y')}"


def _подготовить_фонды(db: Session, з: dict) -> str | None:
    """Без даты — последний снимок фонда (или последний по всем фондам)."""
    if з.get("date") or з.get("q"):
        return None
    if з.get("fund"):
        d = _последняя_дата(db, "SELECT MAX(snapshot_date) FROM fund_holdings_history WHERE fund_id = CAST(:fund AS int)",
                            {"fund": з["fund"]})
    else:
        d = _последняя_дата(db, "SELECT MAX(snapshot_date) FROM fund_holdings_history", {})
    if d is None:
        return None
    з["date"] = d.isoformat()
    return f"показан последний снимок — от {d.strftime('%d.%m.%Y')}"


СРЕЗЫ: tuple[Срез, ...] = (
    # ── Новости и каналы ────────────────────────────────────────────────────
    Срез(
        код="news", группа="Новости и каналы", имя="Архив новостей",
        описание="Всё, что пришло из двух Telegram-каналов: исторический экспорт с 2020 года и живой ингест. Поиск — полнотекстовый, по русской морфологии.",
        таблица="news_archive", откуда="news_archive n",
        колонки=(
            Колонка("posted_at", "Когда", "ts"), Колонка("channel", "Канал"),
            Колонка("text", "Текст", "long"), Колонка("tickers", "Тикеры", "arr"),
            Колонка("views", "Просмотры", "int"),
            Колонка("пост", "Пост", "tg", "'https://t.me/' || CASE channel WHEN 'MarketTwits' THEN 'markettwits' WHEN 'СМАРТЛАБ НОВОСТИ' THEN 'newssmartlab' ELSE channel END || '/' || message_id"),
        ),
        фильтры=(
            Фильтр("channel", "Канал", "select", "n.channel = ANY(string_to_array(:channel, ','))", варианты=_КАНАЛЫ),
            _тикер("ticker", "Тикер", "n.tickers && ARRAY[:ticker]::text[]"),
            Фильтр("q", "Слова", "text", "to_tsvector('russian', n.text) @@ plainto_tsquery('russian', :q)", подсказка="морфология русская: «дивиденд» найдёт «дивидендов»"),
            _с("n.posted_at"), _по("n.posted_at"),
        ),
        порядок="n.posted_at DESC", дата_колонка="posted_at", окно_дней=7, макс_окно_дней=366,
    ),
    Срез(
        код="watch", группа="Новости и каналы", имя="Разгон репостов",
        описание="Каждый пост живых каналов и сколько репостов он набрал через 3, 5, 6, 7, 8, 15 и 90 минут. По этой кривой пайплайн решает, хайп это или нет.",
        таблица="tg_channel_watch", откуда="tg_channel_watch w",
        колонки=(
            Колонка("posted_at", "Когда", "ts"), Колонка("channel", "Канал"),
            Колонка("msg_text", "Текст", "long"),
            Колонка("fwd_3", "3 мин", "int"), Колонка("fwd_5", "5", "int"), Колонка("fwd_6", "6", "int"),
            Колонка("fwd_7", "7", "int"), Колонка("fwd_8", "8", "int"), Колонка("fwd_15", "15", "int"),
            Колонка("fwd_90", "90 мин", "int"), Колонка("promoted", "Хайп", "bool"),
            Колонка("пост", "Пост", "tg", "'https://t.me/' || channel || '/' || message_id"),
        ),
        фильтры=(
            Фильтр("channel", "Канал", "select", "w.channel = :channel", варианты=_КАНАЛЫ_ЖИВЫЕ),
            Фильтр("promoted", "Только хайп", "bool", "w.promoted = TRUE"),
            Фильтр("q", "Текст содержит", "text", "w.msg_text ILIKE :q"),
            _с("w.posted_at"), _по("w.posted_at"),
        ),
        порядок="w.posted_at DESC", дата_колонка="posted_at", окно_дней=7,
    ),
    Срез(
        код="candidates", группа="Новости и каналы", имя="Кандидаты в посты",
        описание="Плоский список кандидатов. Полный путь каждого — новость, судья, черновик, человек — открывается по номеру на «Заводе постов».",
        таблица="content_candidates", откуда="content_candidates c",
        колонки=(
            Колонка("id", "№", "candidate"), Колонка("created_at", "Создан", "ts"),
            Колонка("status", "Статус"), Колонка("headline", "Заголовок", "long"),
            Колонка("tickers", "Тикеры", "arr"), Колонка("judge_verdict", "Судья"),
            Колонка("importance_1_5", "Важность", "int"), Колонка("source", "Источник"),
        ),
        фильтры=(
            Фильтр("status", "Статус", "select", "c.status = :status",
                   варианты_sql="SELECT status, status FROM content_candidates GROUP BY status ORDER BY status"),
            _тикер("ticker", "Тикер", "c.tickers && ARRAY[:ticker]::text[]"),
            Фильтр("q", "Заголовок содержит", "text", "c.headline ILIKE :q"),
            _с("c.created_at"), _по("c.created_at"),
        ),
        порядок="c.created_at DESC", дата_колонка="created_at",
    ),
    Срез(
        код="signals", группа="Новости и каналы", имя="Сигналы о владении",
        описание="Новости, где детектор заподозрил смену акционера. Очередь на подтверждение — на вкладке «Связи»; здесь — вся история с исходами.",
        таблица="ownership_signals", откуда="ownership_signals s",
        колонки=(
            Колонка("posted_at", "Когда", "ts"), Колонка("tickers", "Тикеры", "arr"),
            Колонка("snippet", "Фрагмент", "long"), Колонка("strength", "Сила"),
            Колонка("has_percent", "Есть %", "bool"), Колонка("edge_state", "Ребро"),
            Колонка("status", "Статус"), Колонка("review_note", "Заметка", "long"),
            Колонка("пост", "Пост", "tg", "'https://t.me/' || channel || '/' || message_id"),
        ),
        фильтры=(
            Фильтр("status", "Статус", "select", "s.status = :status",
                   варианты_sql="SELECT status, status FROM ownership_signals GROUP BY status ORDER BY status"),
            Фильтр("strength", "Сила", "select", "s.strength = :strength",
                   варианты_sql="SELECT strength, strength FROM ownership_signals GROUP BY strength ORDER BY strength"),
            Фильтр("edge_state", "Ребро", "select", "s.edge_state = :edge_state",
                   варианты_sql="SELECT edge_state, edge_state FROM ownership_signals WHERE edge_state IS NOT NULL GROUP BY edge_state ORDER BY edge_state"),
            _тикер("ticker", "Тикер", "s.tickers && ARRAY[:ticker]::text[]"),
        ),
        порядок="s.posted_at DESC", дата_колонка="posted_at",
    ),
    # ── Аномалии и алерты ───────────────────────────────────────────────────
    Срез(
        код="anomalies", группа="Аномалии и алерты", имя="Аномалии",
        описание="Всплески по индикаторам, которые ушли в ленту и в тосты. Порог ×3 к типичному; дедуп по (тип, актив, день).",
        таблица="anomalies", откуда="anomalies a",
        колонки=(
            Колонка("signal_date", "День", "date"), Колонка("created_at", "Замечена", "ts"),
            Колонка("type", "Тип"), Колонка("asset_id", "Актив", "ticker"),
            Колонка("asset_name", "Название"), Колонка("clgroup", "Группа"),
            Колонка("direction", "Куда"), Колонка("severity_value", "Сила", "num"),
            Колонка("headline", "Заголовок", "long"), Колонка("scope", "Область"),
        ),
        фильтры=(
            Фильтр("type", "Тип", "select", "a.type = :type",
                   варианты_sql="SELECT type, type FROM anomalies GROUP BY type ORDER BY type"),
            Фильтр("direction", "Куда", "select", "a.direction = :direction",
                   варианты_sql="SELECT direction, direction FROM anomalies WHERE direction IS NOT NULL GROUP BY direction ORDER BY direction"),
            _тикер("asset", "Актив", "a.asset_id = :asset"),
            _с("a.signal_date"), _по("a.signal_date"),
        ),
        порядок="a.signal_date DESC, a.created_at DESC", дата_колонка="signal_date", окно_дней=30,
    ),
    Срез(
        код="alert_fires", группа="Аномалии и алерты", имя="Сработавшие алерты",
        описание="Пользовательские алерты, которые сработали: по какому индикатору, по какому активу, на каком значении. Без владельцев — только сами события.",
        таблица="alert_fires", откуда="alert_fires f JOIN alerts al ON al.id = f.alert_id",
        колонки=(
            Колонка("fired_at", "Когда", "ts", "f.fired_at"), Колонка("indicator", "Индикатор", "text", "al.indicator"),
            Колонка("asset", "Актив", "ticker", "al.asset"), Колонка("metric", "Метрика", "text", "al.metric"),
            Колонка("op", "Условие", "text", "al.op || ' ' || al.threshold"), Колонка("value", "Значение", "num", "f.value"),
            Колонка("message_text", "Текст", "long", "f.message_text"),
        ),
        фильтры=(
            Фильтр("indicator", "Индикатор", "select", "al.indicator = :indicator",
                   варианты_sql="SELECT indicator, indicator FROM alerts GROUP BY indicator ORDER BY indicator"),
            _тикер("asset", "Актив", "al.asset = :asset"),
            _с("f.fired_at"), _по("f.fired_at"),
        ),
        порядок="f.fired_at DESC", дата_колонка="fired_at", окно_дней=30,
    ),
    # ── Рынок ───────────────────────────────────────────────────────────────
    Срез(
        код="candles", группа="Рынок", имя="Свечи",
        описание="32 миллиона строк, поэтому тикер обязателен, а окно — не больше года. Дневные, часовые и пятиминутные свечи акций, фьючерсов и индексов.",
        таблица="candles", откуда="candles k",
        колонки=(
            Колонка("begin_time", "Начало", "ts"), Колонка("secid", "Код"), Колонка("type", "Тип"),
            Колонка("open", "Откр.", "num"), Колонка("high", "Макс.", "num"), Колонка("low", "Мин.", "num"),
            Колонка("close", "Закр.", "num"), Колонка("volume", "Объём, шт.", "int"), Колонка("value", "Оборот, ₽", "num"),
        ),
        фильтры=(
            _тикер("secid", "Код бумаги", "k.secid = :secid", обязателен=True, подсказка="SBER, SiU6, IMOEX — регистр как в ISS"),
            Фильтр("interval", "Интервал", "select", 'k."interval" = CAST(:interval AS int)', варианты=_ИНТЕРВАЛЫ, по_умолчанию="24"),
            _с("k.begin_time"), _по("k.begin_time"),
        ),
        порядок="k.begin_time DESC", дата_колонка="begin_time", окно_дней=30, макс_окно_дней=366,
    ),
    Срез(
        код="oi", группа="Рынок", имя="Открытый интерес",
        описание="Позиции физлиц и юрлиц по фьючерсам с 5-минутным шагом. ⚠️ pos — ВАЛОВАЯ позиция, шорт хранится со знаком минус; нетто = лонг + шорт.",
        таблица="open_interest", откуда="open_interest o",
        колонки=(
            Колонка("tradedate", "День", "date"), Колонка("tradetime", "Время"), Колонка("clgroup", "Кто"),
            Колонка("pos", "Валовая", "int"), Колонка("pos_long", "Лонг", "int"), Колонка("pos_short", "Шорт", "int"),
            # ⚠️ ПЛЮС, НЕ МИНУС: pos_short в базе отрицательный (−80 491), и «лонг −
            # шорт» давал валовую позицию под именем нетто. Проверено на SR 04.09.
            Колонка("нетто", "Нетто", "int", "o.pos_long + o.pos_short"),
            Колонка("pos_long_num", "Лиц в лонге", "int"), Колонка("pos_short_num", "Лиц в шорте", "int"),
        ),
        фильтры=(
            Фильтр("sectype", "Фьючерс", "select", "o.sectype = :sectype", обязателен=True,
                   варианты_sql="SELECT sectype, sectype || ' · ' || MIN(assetcode) FROM futures_contracts GROUP BY sectype ORDER BY sectype",
                   подсказка="код группы контрактов, как в ISS: SR — Сбер, LK — Лукойл"),
            Фильтр("clgroup", "Кто", "select", "o.clgroup = :clgroup", варианты=(("FIZ", "физлица"), ("YUR", "юрлица"))),
            Фильтр("interval", "Шаг", "select", 'o."interval" = CAST(:interval AS int)', варианты=_ИНТЕРВАЛЫ, по_умолчанию="24"),
            _с("o.tradedate"), _по("o.tradedate"),
        ),
        порядок="o.tradedate DESC, o.tradetime DESC, o.clgroup", дата_колонка="tradedate", окно_дней=30, макс_окно_дней=366,
    ),
    Срез(
        код="index_data", группа="Рынок", имя="Индексы",
        описание="Дневные значения индексов МосБиржи и капитализация. Часть индексов ISS публикует с задержкой в день — это не сбой.",
        таблица="index_data", откуда="index_data d",
        колонки=(
            Колонка("trade_date", "День", "date"), Колонка("secid", "Индекс"),
            Колонка("open", "Откр.", "num"), Колонка("high", "Макс.", "num"), Колонка("low", "Мин.", "num"),
            Колонка("close", "Закр.", "num"), Колонка("value", "Оборот, ₽", "num"), Колонка("capitalization", "Капитализация, ₽", "num"),
        ),
        фильтры=(
            Фильтр("secid", "Индекс", "select", "d.secid = :secid",
                   варианты_sql="SELECT secid, secid || ' — ' || name FROM indices ORDER BY secid", по_умолчанию="IMOEX"),
            _с("d.trade_date"), _по("d.trade_date"),
        ),
        порядок="d.trade_date DESC", дата_колонка="trade_date", окно_дней=90,
    ),
    Срез(
        код="index_composition", группа="Рынок", имя="Состав индексов",
        описание="Веса бумаг в базах расчёта по дням. Без тикера и даты показывается последний состав; с тикером — история его веса.",
        таблица="index_composition", откуда="index_composition ic",
        колонки=(
            Колонка("trade_date", "День", "date"), Колонка("index_id", "Индекс"),
            Колонка("ticker", "Тикер", "ticker"), Колонка("weight", "Вес", "pct"),
        ),
        фильтры=(
            Фильтр("index_id", "Индекс", "select", "ic.index_id = :index_id",
                   варианты_sql="SELECT index_id, index_id FROM index_composition GROUP BY index_id ORDER BY index_id", по_умолчанию="IMOEX"),
            _тикер("ticker", "Тикер", "ic.ticker = :ticker"),
            Фильтр("date", "На дату", "date", "ic.trade_date = CAST(:date AS date)"),
        ),
        порядок="ic.trade_date DESC, ic.weight DESC", дата_колонка="trade_date", подготовить=_подготовить_состав,
    ),
    Срез(
        код="market_cap", группа="Рынок", имя="Капитализация",
        описание="Рыночная капитализация акций по дням — та, что тянется со Смартлаба и ISS для карточек и ребаланса.",
        таблица="stock_market_cap", откуда="stock_market_cap m",
        колонки=(
            Колонка("period_date", "День", "date"), Колонка("sec_id", "Тикер", "ticker"),
            Колонка("market_cap", "Капитализация, ₽", "num"),
        ),
        фильтры=(_тикер("sec_id", "Тикер", "m.sec_id = :sec_id"), _с("m.period_date"), _по("m.period_date")),
        порядок="m.period_date DESC, m.market_cap DESC", дата_колонка="period_date", окно_дней=30,
    ),
    Срез(
        код="freefloat", группа="Рынок", имя="Free-float и веса",
        описание="Коэффициенты free-float и ограничения веса по месяцам — то, из чего считается ребаланс индексов.",
        таблица="freefloat_cap", откуда="freefloat_cap ff",
        колонки=(
            Колонка("month", "Месяц", "date"), Колонка("as_of", "На дату", "date"), Колонка("sec_id", "Тикер", "ticker"),
            Колонка("ffcap", "FF-капитализация, ₽", "num"), Колонка("ff_factor", "FF", "num"), Колонка("w_factor", "Огранич. веса", "num"),
        ),
        фильтры=(_тикер("sec_id", "Тикер", "ff.sec_id = :sec_id"), _с("ff.month"), _по("ff.month")),
        порядок="ff.month DESC, ff.ffcap DESC", дата_колонка="month", окно_дней=62,
    ),
    # ── Фонды ───────────────────────────────────────────────────────────────
    Срез(
        код="fund_holdings", группа="Фонды", имя="Состав фондов",
        описание="Снимки состава БПИФов по раскрытиям управляющих. ⚠️ Это месячные СНИМКИ, не сделки: разница двух снимков — оценка, не факт.",
        таблица="fund_holdings_history", откуда="fund_holdings_history h JOIN funds f ON f.fund_id = h.fund_id",
        колонки=(
            Колонка("snapshot_date", "Снимок", "date", "h.snapshot_date"), Колонка("fund", "Фонд", "text", "f.ticker"),
            Колонка("asset_name", "Актив", "text", "h.asset_name"), Колонка("isin", "ISIN", "text", "h.isin"),
            Колонка("weight", "Вес", "pct", "h.weight"), Колонка("positions", "Штук", "int", "h.positions"),
            Колонка("amount_rub", "Сумма, ₽", "num", "h.amount_rub"), Колонка("source", "Источник", "text", "h.source"),
        ),
        фильтры=(
            Фильтр("fund", "Фонд", "select", "h.fund_id = CAST(:fund AS int)",
                   варианты_sql="SELECT fund_id::text, ticker || ' — ' || name FROM funds ORDER BY ticker"),
            Фильтр("q", "Актив или ISIN", "text", "(h.asset_name ILIKE :q OR h.isin ILIKE :q)"),
            Фильтр("date", "Снимок от", "date", "h.snapshot_date = CAST(:date AS date)"),
        ),
        порядок="h.snapshot_date DESC, h.weight DESC NULLS LAST", дата_колонка="snapshot_date", подготовить=_подготовить_фонды,
    ),
    Срез(
        код="fund_nav", группа="Фонды", имя="СЧА и паи",
        описание="Стоимость чистых активов и цена пая по дням. Из этого считаются притоки и оттоки на /fund-trades.",
        таблица="fund_data", откуда="fund_data fd JOIN funds f ON f.fund_id = fd.fund_id",
        колонки=(
            Колонка("trade_date", "День", "date", "fd.trade_date"), Колонка("fund", "Фонд", "text", "f.ticker"),
            Колонка("name", "Название", "text", "f.name"), Колонка("nav", "СЧА, ₽", "num", "fd.nav"),
            Колонка("pay", "Пай, ₽", "num", "fd.pay"), Колонка("nav_cny", "СЧА, ¥", "num", "fd.nav_cny"),
        ),
        фильтры=(
            Фильтр("fund", "Фонд", "select", "fd.fund_id = CAST(:fund AS int)",
                   варианты_sql="SELECT fund_id::text, ticker || ' — ' || name FROM funds ORDER BY ticker"),
            _с("fd.trade_date"), _по("fd.trade_date"),
        ),
        порядок="fd.trade_date DESC, f.ticker", дата_колонка="trade_date", окно_дней=30,
    ),
    # ── Компании ────────────────────────────────────────────────────────────
    Срез(
        код="metrics", группа="Компании", имя="Показатели отчётности",
        описание="Финансовые показатели из FinanceMarker с историей версий: одно и то же значение может переписываться при пересдаче отчёта. Тикер обязателен — строк полмиллиона.",
        таблица="company_metrics", откуда="company_metrics m LEFT JOIN metrics_ref r ON r.metric_code = m.metric_code",
        колонки=(
            Колонка("period_end", "Период до", "date", "m.period_end"), Колонка("period_label", "Период", "text", "m.period_label"),
            Колонка("label", "Показатель", "text", "COALESCE(r.label_ru, m.metric_code)"), Колонка("value", "Значение", "num", "m.value"),
            Колонка("unit", "Ед.", "text", "r.unit"), Колонка("standard", "Стандарт", "text", "m.standard"),
            Колонка("period_type", "Тип", "text", "m.period_type"), Колонка("source", "Источник", "text", "m.source"),
            Колонка("first_seen", "Впервые", "date", "m.first_seen"), Колонка("last_seen", "Последний раз", "date", "m.last_seen"),
        ),
        фильтры=(
            _тикер("secid", "Тикер", "m.secid = :secid", обязателен=True),
            Фильтр("metric_code", "Показатель", "select", "m.metric_code = :metric_code",
                   варианты_sql="SELECT metric_code, COALESCE(label_ru, metric_code) FROM metrics_ref ORDER BY 2"),
            Фильтр("standard", "Стандарт", "select", "m.standard = :standard",
                   варианты_sql="SELECT standard, standard FROM company_metrics GROUP BY standard ORDER BY standard"),
            Фильтр("period_type", "Тип периода", "select", "m.period_type = :period_type",
                   варианты_sql="SELECT period_type, period_type FROM company_metrics GROUP BY period_type ORDER BY period_type"),
        ),
        порядок="m.period_end DESC, m.metric_code", дата_колонка="period_end",
    ),
    Срез(
        код="shareholders", группа="Компании", имя="Акционеры",
        описание="Структура акционеров по данным Смартлаба. Дата структуры бывает старше двух лет — это норма рынка, не сбой ингеста.",
        таблица="company_shareholders", откуда="company_shareholders s JOIN issuers i ON i.issuer_id = s.issuer_id",
        колонки=(
            Колонка("ticker", "Тикер", "ticker", "i.smartlab_ticker"), Колонка("name", "Компания", "text", "i.name_short"),
            Колонка("holder", "Акционер", "text", "s.holder"), Колонка("share_pct", "Доля", "pct", "s.share_pct"),
            Колонка("structure_as_of", "Структура на", "date", "s.structure_as_of"), Колонка("source", "Источник", "text", "s.source"),
        ),
        фильтры=(
            _тикер("ticker", "Тикер", "i.smartlab_ticker = :ticker"),
            Фильтр("q", "Акционер содержит", "text", "s.holder ILIKE :q"),
        ),
        порядок="i.smartlab_ticker, s.share_pct DESC NULLS LAST",
    ),
    Срез(
        код="facts", группа="Компании", имя="Факты о мире",
        описание="Плоская таблица фактов второго мозга: связи владения, казначейские пакеты, ставка, денежная масса, ВВП. Вектора здесь не живут — и по замыслу.",
        таблица="world_facts", откуда="world_facts w",
        колонки=(
            Колонка("valid_from", "С", "date"), Колонка("valid_until", "По", "date"), Колонка("kind", "Вид"),
            Колонка("statement", "Утверждение", "long"), Колонка("entities", "Сущности", "arr"),
            Колонка("confidence", "Уверенность", "num"), Колонка("source", "Источник"), Колонка("fact_key", "Ключ"),
        ),
        фильтры=(
            Фильтр("kind", "Вид", "select", "w.kind = :kind",
                   варианты_sql="SELECT kind, kind FROM world_facts GROUP BY kind ORDER BY kind"),
            _тикер("entity", "Сущность", "w.entities && ARRAY[:entity]::text[]"),
            Фильтр("q", "Текст содержит", "text", "w.statement ILIKE :q"),
        ),
        порядок="w.valid_from DESC NULLS LAST, w.id DESC", дата_колонка="valid_from",
    ),
    Срез(
        код="dividends", группа="Компании", имя="Дивиденды (ISS)",
        описание="Дивидендные отсечки с ISS МосБиржи: дата закрытия реестра, экс-дата, размер. Будущих отсечек здесь может не быть — ISS отдаёт их поздно.",
        таблица="dividends", откуда="dividends dv",
        колонки=(
            Колонка("ex_date", "Экс-дата", "date"), Колонка("registry_close_date", "Реестр", "date"),
            Колонка("secid", "Тикер", "ticker"), Колонка("value", "Размер, ₽", "num"),
        ),
        фильтры=(_тикер("secid", "Тикер", "dv.secid = :secid"), _с("dv.ex_date"), _по("dv.ex_date")),
        порядок="dv.ex_date DESC NULLS LAST", дата_колонка="ex_date", окно_дней=365,
    ),
    Срез(
        код="company_dividends", группа="Компании", имя="Дивиденды (Смартлаб)",
        описание="История выплат по компаниям с ценой и доходностью на дату отсечки.",
        таблица="company_dividends", откуда="company_dividends cd",
        колонки=(
            Колонка("record_date", "Отсечка", "date"), Колонка("secid", "Тикер", "ticker"), Колонка("period", "Период"),
            Колонка("dividend", "Дивиденд, ₽", "num"), Колонка("price", "Цена, ₽", "num"), Колонка("div_yield", "Доходность", "pct"),
        ),
        фильтры=(_тикер("secid", "Тикер", "cd.secid = :secid"), _с("cd.record_date"), _по("cd.record_date")),
        порядок="cd.record_date DESC NULLS LAST", дата_колонка="record_date", окно_дней=365,
    ),
    # ── Макро ───────────────────────────────────────────────────────────────
    Срез(
        код="macro", группа="Макро", имя="Макро-ряды",
        описание="Ставка, инфляция, денежная масса, курсы — ряды ЦБ и Росстата, как они лежат в базе.",
        таблица="macro_data", откуда="macro_data md JOIN macro mc ON mc.indicator = md.indicator",
        колонки=(
            Колонка("period_date", "Дата", "date", "md.period_date"), Колонка("name", "Показатель", "text", "mc.name"),
            Колонка("indicator", "Код", "text", "md.indicator"), Колонка("value", "Значение", "num", "md.value"),
            Колонка("frequency", "Частота", "text", "mc.frequency"), Колонка("source", "Источник", "text", "md.source"),
        ),
        фильтры=(
            Фильтр("indicator", "Показатель", "select", "md.indicator = :indicator",
                   варианты_sql="SELECT indicator, name FROM macro ORDER BY name"),
            _с("md.period_date"), _по("md.period_date"),
        ),
        порядок="md.period_date DESC, mc.name", дата_колонка="period_date", окно_дней=365,
    ),
    # ── Служебное ───────────────────────────────────────────────────────────
    Срез(
        код="runs", группа="Служебное", имя="Журнал прогонов",
        описание="Каждый завершённый прогон каждого процесса: сколько шёл, чем кончился, сколько строк записал. Хранится 30 дней.",
        таблица="pipeline_run_log", откуда="pipeline_run_log l",
        колонки=(
            Колонка("finished_at", "Закончил", "ts"), Колонка("pipeline", "Процесс", "pipeline"),
            Колонка("status", "Исход"), Колонка("duration_sec", "Длился, с", "num"),
            Колонка("rows_written", "Записал строк", "int"), Колонка("note", "Итог", "long"),
        ),
        фильтры=(
            Фильтр("pipeline", "Процесс", "select", "l.pipeline = :pipeline",
                   варианты_sql="SELECT pipeline, pipeline FROM pipeline_runs ORDER BY pipeline"),
            Фильтр("status", "Исход", "select", "l.status = :status",
                   варианты_sql="SELECT status, status FROM pipeline_run_log GROUP BY status ORDER BY status"),
            _с("l.finished_at"), _по("l.finished_at"),
        ),
        порядок="l.finished_at DESC", дата_колонка="finished_at", окно_дней=3, макс_окно_дней=30,
    ),
)

ПО_КОДУ = {с.код: с for с in СРЕЗЫ}

# Таблицы с персональными данными и секретами. Их имена не должны встречаться ни
# в одном срезе — ни в FROM, ни в JOIN, ни в справочнике вариантов.
_ЗАПРЕЩЕНО = (
    "users", "refresh_tokens", "subscriptions", "user_payment_methods", "api_keys",
    "extension_tokens", "trial_redemptions", "analytics_events", "user_settings",
    "telegram_link_tokens", "subscription_invites", "subscription_invite_redemptions",
    "alert_events",
)


def _слова(sql: str) -> set[str]:
    import re
    return set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", sql))


for _с_ in СРЕЗЫ:
    _исп = _слова(_с_.откуда) | {_с_.таблица}
    for _ф in _с_.фильтры:
        if _ф.варианты_sql:
            _исп |= _слова(_ф.варианты_sql)
    _плохо = _исп & set(_ЗАПРЕЩЕНО)
    assert not _плохо, f"срез {_с_.код} трогает закрытую таблицу: {_плохо}"
    assert len({к.ключ for к in _с_.колонки}) == len(_с_.колонки), f"срез {_с_.код}: ключи колонок повторяются"


def _json(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if hasattr(v, "isoformat"):     # time
        return v.isoformat()
    if isinstance(v, bytes):
        return None
    if isinstance(v, list):
        return [_json(x) for x in v]
    if isinstance(v, dict):
        return {k: _json(x) for k, x in v.items()}
    return v


def _привести(ф: Фильтр, v: str):
    v = v.strip()
    if ф.тип == "ticker":
        # «sber» → SBER, но «SiU6» остаётся как есть: у фьючерсов регистр значим,
        # и слепой upper() превращал SiU6 в несуществующий SIU6.
        return v.upper() if v == v.lower() else v
    if ф.тип == "text" and "ILIKE" in ф.sql:
        return f"%{v}%"
    if ф.тип in ("date", "date_from", "date_to"):
        try:
            return date.fromisoformat(v).isoformat()
        except ValueError:
            raise HTTPException(400, f"«{ф.подпись}»: дата в виде ГГГГ-ММ-ДД")
    if ф.тип == "number":
        try:
            return float(v)
        except ValueError:
            raise HTTPException(400, f"«{ф.подпись}»: число")
    if ф.тип == "select" and ф.варианты:
        if v not in {з for з, _ in ф.варианты}:
            raise HTTPException(400, f"«{ф.подпись}»: нет такого варианта")
    return v


def _варианты(db: Session, ф: Фильтр) -> list[dict] | None:
    if ф.варианты:
        return [{"значение": з, "подпись": п} for з, п in ф.варианты]
    if not ф.варианты_sql:
        return None
    # md5, а не hash(): встроенный hash солится на каждый процесс, и четыре воркера
    # держали бы четыре копии одного справочника.
    ключ = "dashboard:db:opts:" + hashlib.md5(ф.варианты_sql.encode()).hexdigest()[:12]
    cached = get_or_set(ключ)
    if cached is not None:
        return cached
    rows = db.execute(text(ф.варианты_sql)).all()
    out = [{"значение": str(r[0]), "подпись": str(r[1])} for r in rows if r[0] is not None]
    get_or_set(ключ, out, ttl=_TTL_СПРАВОЧНИКОВ)
    return out


def _описание_среза(с: Срез, db: Session | None = None) -> dict:
    d = {
        "код": с.код, "группа": с.группа, "имя": с.имя, "описание": с.описание,
        "таблица": с.таблица, "окно_дней": с.окно_дней, "макс_окно_дней": с.макс_окно_дней,
        "предупреждение": с.предупреждение,
        "колонки": [{"ключ": к.ключ, "подпись": к.подпись, "тип": к.тип} for к in с.колонки],
        "фильтры": [{
            "ключ": ф.ключ, "подпись": ф.подпись, "тип": ф.тип, "обязателен": ф.обязателен,
            "по_умолчанию": ф.по_умолчанию, "подсказка": ф.подсказка,
            "варианты": _варианты(db, ф) if db is not None else None,
        } for ф in с.фильтры],
    }
    return d


@router.get("/slices")
def срезы(db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    """Реестр срезов по группам с оценкой размера таблиц (по статистике, не COUNT)."""
    ключ = "dashboard:db:slices"
    cached = get_or_set(ключ)
    if cached is not None:
        return cached
    # n_live_tup — оценка планировщика, для candles она отличается от точной на доли
    # процента и стоит ноль. Точный COUNT(*) по 32 миллионам — минута.
    размеры = {r[0]: {"строк": int(r[1]), "размер": r[2]} for r in db.execute(text("""
        SELECT relname, n_live_tup, pg_size_pretty(pg_total_relation_size(relid))
          FROM pg_stat_user_tables
    """)).all()}
    группы: dict[str, list] = {}
    for с in СРЕЗЫ:
        группы.setdefault(с.группа, []).append({
            "код": с.код, "имя": с.имя, "описание": с.описание, "таблица": с.таблица,
            "строк": размеры.get(с.таблица, {}).get("строк"),
            "размер": размеры.get(с.таблица, {}).get("размер"),
            "обязательный_фильтр": next((ф.подпись for ф in с.фильтры if ф.обязателен), None),
        })
    out = {"группы": [{"группа": г, "срезы": с} for г, с in группы.items()]}
    get_or_set(ключ, out, ttl=_TTL_СПРАВОЧНИКОВ)
    return out


# ⚠️ ИМЯ ПАРАМЕТРА ПУТИ — ЛАТИНИЦЕЙ. Starlette распознаёт {param} только из
# ASCII-букв; «{код}» он считал буквальным текстом, и /slices/candles отвечал 404.
@router.get("/slices/{code}")
def срез(
    code: str,
    request: Request,
    limit: int = Query(_ПРЕДЕЛ, ge=1, le=_МАКС_ПРЕДЕЛ),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Строки среза по фильтрам из query-string. Неизвестные параметры игнорируются."""
    с = ПО_КОДУ.get(code)
    if с is None:
        raise HTTPException(404, "нет такого среза")

    значения = {ф.ключ: request.query_params.get(ф.ключ) for ф in с.фильтры}
    значения = {k: v for k, v in значения.items() if v not in (None, "")}
    for ф in с.фильтры:
        if ф.по_умолчанию and ф.ключ not in значения:
            значения[ф.ключ] = ф.по_умолчанию

    пояснения: list[str] = []
    if с.подготовить:
        п = с.подготовить(db, значения)
        if п:
            пояснения.append(п)

    # Окно по умолчанию и потолок окна — только там, где есть дата и лимит.
    if с.окно_дней and "с" not in значения and "по" not in значения:
        сегодня = date.today()
        значения["по"] = сегодня.isoformat()
        значения["с"] = (сегодня - timedelta(days=с.окно_дней)).isoformat()
        пояснения.append(f"без дат показаны последние {с.окно_дней} дн.")
    elif с.окно_дней and "с" in значения and "по" not in значения:
        значения["по"] = date.today().isoformat()
    elif с.окно_дней and "по" in значения and "с" not in значения:
        try:
            значения["с"] = (date.fromisoformat(значения["по"]) - timedelta(days=с.окно_дней)).isoformat()
        except ValueError:
            pass
    if с.макс_окно_дней and "с" in значения and "по" in значения:
        try:
            дн = (date.fromisoformat(значения["по"]) - date.fromisoformat(значения["с"])).days
        except ValueError:
            дн = 0
        if дн > с.макс_окно_дней:
            raise HTTPException(400, f"окно не больше {с.макс_окно_дней} дн. — таблица тяжёлая")

    условия, параметры = ["TRUE"], {}
    показ = dict(значения)   # что вернуть экрану: тикер уже в верхнем регистре
    for ф in с.фильтры:
        v = значения.get(ф.ключ)
        if v is None:
            if ф.обязателен:
                # Не 400: экрану нужно описание среза, чтобы нарисовать форму с
                # этим самым обязательным полем. Пустой результат с объяснением.
                return {
                    "срез": _описание_среза(с, db), "фильтры_применены": значения, "пояснения": пояснения,
                    "всего": 0, "всего_с_потолком": False, "предел": limit, "смещение": offset, "строки": [],
                    "ошибка": f"нужен фильтр «{ф.подпись}»" + (f" — например {ф.подсказка}" if ф.подсказка else ""),
                }
            continue
        if ф.тип == "bool":
            if v.lower() in ("1", "true", "да", "on"):
                условия.append(ф.sql)
            continue
        параметры[ф.ключ] = _привести(ф, v)
        if ф.тип == "ticker":
            показ[ф.ключ] = параметры[ф.ключ]
        условия.append(ф.sql)
    где = " AND ".join(условия)

    выбор = ", ".join(f"{к.sql or к.ключ} AS \"{к.ключ}\"" for к in с.колонки)
    строки = db.execute(text(f"""
        SELECT {выбор} FROM {с.откуда} WHERE {где}
        ORDER BY {с.порядок} LIMIT :limit OFFSET :offset
    """), {**параметры, "limit": limit, "offset": offset}).mappings().all()

    всего = db.execute(text(f"""
        SELECT COUNT(*) FROM (SELECT 1 FROM {с.откуда} WHERE {где} LIMIT {_ПОТОЛОК_СЧЁТА + 1}) t
    """), параметры).scalar() or 0

    return {
        "срез": _описание_среза(с, db),
        "фильтры_применены": показ,
        "пояснения": пояснения,
        "всего": min(всего, _ПОТОЛОК_СЧЁТА),
        "всего_с_потолком": всего > _ПОТОЛОК_СЧЁТА,
        "предел": limit, "смещение": offset,
        "строки": [[_json(r[к.ключ]) for к in с.колонки] for r in строки],
        "ошибка": None,
    }
