#!/usr/bin/env python3
"""Синхронизация карты нодов второго мозга (brain_nodes / brain_edges).

⚠️ ВСЁ СЧИТАЕТСЯ В БАЗЕ, ОДНИМ SQL НА ИСТОЧНИК. Python не гоняет строки: у каждого
источника — INSERT … SELECT … ON CONFLICT с водяным знаком по времени. Полный
прогон по 60 тыс. новостей за два года — секунды, инкремент — миллисекунды.

⚠️ УЗЛЫ КОМПАНИЙ — ПО ЭМИТЕНТУ, НЕ ПО БУМАГЕ. SBER и SBERP — одна компания;
префы, фьючерсы (SR, SP, SBERF) и ISIN ведут в тот же узел через brain_ticker_map.
Тикеры, которых нет в справочнике (BTC, NVDA, хэштеги вроде TODAY), узлов не
получают — карта про наш рынок.

⚠️ НОВОСТИ — ТОЛЬКО С ТИКЕРАМИ И ЗА ДВА ГОДА. Иначе узлов полмиллиона и обход
кольца перестаёт быть мгновенным. Остальной архив остаётся в news_archive и
достижим полнотекстом, а не обходом.

⚠️ ИМЯ КАНАЛА НОРМАЛИЗУЕТСЯ: исторический экспорт лежит под MarketTwits и
«СМАРТЛАБ НОВОСТИ», живой ингест — под markettwits и newssmartlab. Id узла
news:markettwits/130070 совпадает с тем, что в source_url кандидата.

Запуск: python Brain/brain_sync.py [--full]
Итог — JSON последней строкой для оркестратора.
"""
import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL")
НОВОСТИ_ДНЕЙ = 730

_КАНАЛ = "CASE channel WHEN 'MarketTwits' THEN 'markettwits' WHEN 'СМАРТЛАБ НОВОСТИ' THEN 'newssmartlab' ELSE channel END"


def _водяной(conn, source: str):
    return conn.execute(text("SELECT watermark FROM brain_sync_state WHERE source = :s"), {"s": source}).scalar()


def _отметить(conn, source: str, watermark, rows: int):
    conn.execute(text("""
        INSERT INTO brain_sync_state (source, watermark, rows_last, updated_at)
        VALUES (:s, :w, :n, NOW())
        ON CONFLICT (source) DO UPDATE SET watermark = EXCLUDED.watermark, rows_last = EXCLUDED.rows_last, updated_at = NOW()
    """), {"s": source, "w": watermark, "n": rows})


def карта_тикеров(conn) -> int:
    conn.execute(text("TRUNCATE brain_ticker_map"))
    r = conn.execute(text("""
        INSERT INTO brain_ticker_map (ticker, company_id)
        SELECT t, 'company:' || i.smartlab_ticker
          FROM issuer_securities s
          JOIN issuers i USING (issuer_id)
          CROSS JOIN LATERAL unnest(ARRAY[s.secid, s.futures_sectype_quarterly, s.futures_sectype_perpetual, s.isin, s.canonical_isin]) AS t
         WHERE t IS NOT NULL AND t <> '' AND i.smartlab_ticker IS NOT NULL
        ON CONFLICT (ticker) DO NOTHING
    """))
    return r.rowcount


def индексы_узлы(conn) -> int:
    """Все индексы из справочника — узлы; их тикеры в карте ведут на index:<secid>, а не на компанию.
    ⚠️ RGBI, IMOEX в тикерах кандидатов — четыре из 36 кандидатов замера были про индекс облигаций
    и не находили узла вовсе."""
    conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'index:' || secid, 'index', secid, COALESCE(name, secid), CAST(NULL AS text), CAST(NULL AS timestamptz),
               jsonb_build_object('engine', engine, 'market', market, 'start_date', start_date), NOW()
          FROM indices
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, payload = brain_nodes.payload || EXCLUDED.payload, updated_at = NOW()
    """))
    r = conn.execute(text("""
        INSERT INTO brain_ticker_map (ticker, company_id)
        SELECT secid, 'index:' || secid FROM indices
        ON CONFLICT (ticker) DO NOTHING
    """))
    return r.rowcount


def компании(conn) -> int:
    r = conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'company:' || i.smartlab_ticker, 'company', i.smartlab_ticker,
               COALESCE(i.name_short, i.smartlab_ticker), i.sector, i.updated_at,
               jsonb_build_object(
                   'name_full', i.name_full, 'sector', i.sector, 'active', i.is_active,
                   'secids', (SELECT jsonb_agg(s.secid ORDER BY s.secid) FROM issuer_securities s WHERE s.issuer_id = i.issuer_id),
                   'futures', (SELECT jsonb_agg(DISTINCT s.futures_sectype_quarterly) FROM issuer_securities s WHERE s.issuer_id = i.issuer_id AND s.futures_sectype_quarterly IS NOT NULL)
               ), NOW()
          FROM issuers i
         WHERE i.smartlab_ticker IS NOT NULL
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary, ts = EXCLUDED.ts, payload = EXCLUDED.payload, updated_at = NOW()
    """))
    return r.rowcount


def новости(conn, full: bool) -> tuple[int, datetime | None]:
    вод = None if full else _водяной(conn, "news")
    с = datetime.now(timezone.utc) - timedelta(days=НОВОСТИ_ДНЕЙ)
    п = {"с": с, "вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    r = conn.execute(text(f"""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        -- DISTINCT ON: на стыке 31.08 одно сообщение лежит и под MarketTwits, и под
        -- markettwits — после нормализации это один id, а ON CONFLICT в одном
        -- INSERT дважды одну строку трогать не может.
        SELECT DISTINCT ON (1) 'news:' || {_КАНАЛ} || '/' || message_id, 'news', {_КАНАЛ} || '/' || message_id,
               left(regexp_replace(text, '\\s+', ' ', 'g'), 160), CAST(NULL AS text), posted_at,
               jsonb_build_object('channel', {_КАНАЛ}, 'views', views, 'tickers', to_jsonb(tickers),
                                  'url', 'https://t.me/' || {_КАНАЛ} || '/' || message_id), NOW()
          FROM news_archive n
         WHERE posted_at > :с AND imported_at > :вод AND cardinality(tickers) > 0
           AND EXISTS (SELECT 1 FROM brain_ticker_map m WHERE m.ticker = ANY(n.tickers))
         ORDER BY 1, imported_at DESC
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text(f"""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'news:' || {_КАНАЛ} || '/' || message_id, m.company_id, 'упоминает', posted_at, CAST(NULL AS real), 'news_archive'
          FROM news_archive n
          JOIN brain_ticker_map m ON m.ticker = ANY(n.tickers)
         WHERE posted_at > :с AND imported_at > :вод
        ON CONFLICT DO NOTHING
    """), п)
    новый = conn.execute(text("SELECT MAX(imported_at) FROM news_archive")).scalar()
    _отметить(conn, "news", новый, n)
    return n, новый


def кандидаты(conn, full: bool) -> int:
    вод = None if full else _водяной(conn, "candidates")
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    r = conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'candidate:' || id, CASE WHEN status = 'published' THEN 'post' ELSE 'candidate' END, id::text,
               COALESCE(headline, '(без заголовка)'), left(annotation, 300), COALESCE(published_at, created_at),
               jsonb_build_object('status', status, 'verdict', judge_verdict, 'source', source, 'event_type', event_type,
                                  'importance', importance_1_5, 'tickers', to_jsonb(tickers), 'published_at', published_at), NOW()
          FROM content_candidates
         WHERE COALESCE(updated_at, created_at) > :вод
        ON CONFLICT (id) DO UPDATE SET kind = EXCLUDED.kind, title = EXCLUDED.title, summary = EXCLUDED.summary, ts = EXCLUDED.ts, payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'candidate:' || c.id, m.company_id, 'о', c.created_at, CAST(NULL AS real), 'content_candidates'
          FROM content_candidates c JOIN brain_ticker_map m ON m.ticker = ANY(c.tickers)
         WHERE COALESCE(c.updated_at, c.created_at) > :вод
        ON CONFLICT DO NOTHING
    """), п)
    # кандидат → новость: формального ключа нет, разбираем t.me/<канал>/<id> из source_url.
    # ⚠️ «(?\\:s/)» в text() — это bind-параметр :s; двоеточие экранировано как \:
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT 'candidate:' || c.id, 'news:' || lower(x.ch) || '/' || x.mid, 'из_новости', c.created_at, CAST(NULL AS real), 'source_url'
          FROM content_candidates c
          CROSS JOIN LATERAL (SELECT substring(c.source_url FROM 't\\.me/(?\\:s/)?([A-Za-z0-9_]+)/\\d+') AS ch,
                                     substring(c.source_url FROM 't\\.me/(?\\:s/)?[A-Za-z0-9_]+/(\\d+)') AS mid) x
         WHERE x.ch IS NOT NULL AND COALESCE(c.updated_at, c.created_at) > :вод
           AND EXISTS (SELECT 1 FROM brain_nodes b WHERE b.id = 'news:' || lower(x.ch) || '/' || x.mid)
        ON CONFLICT DO NOTHING
    """), п)
    _отметить(conn, "candidates", conn.execute(text("SELECT MAX(COALESCE(updated_at, created_at)) FROM content_candidates")).scalar(), n)
    return n


def документы(conn, full: bool) -> int:
    вод = None if full else _водяной(conn, "docs")
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    r = conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT DISTINCT ON (md5(d.url)) 'doc:' || md5(d.url), 'doc', md5(d.url),
               CASE d.doc_type WHEN 'financial_report' THEN 'Отчёт' WHEN 'presentation' THEN 'Презентация' ELSE d.doc_type END
                 || COALESCE(' · ' || d.period, ''),
               CAST(NULL AS text),
               -- ⚠️ ts = период документа, не created_at: архив пересобирается еженедельно,
               -- и по created_at все 8 тысяч документов выглядели «свежими» каждую неделю.
               CASE WHEN d.period ~ '^[0-9]{4}$' THEN make_date(CAST(d.period AS int), 12, 31) ELSE d.created_at::date END,
               jsonb_build_object('url', d.url, 'source', d.source, 'doc_type', d.doc_type, 'period', d.period, 'parsed', d.parsed), NOW()
          FROM company_documents d
         WHERE d.created_at > :вод
         ORDER BY md5(d.url), d.created_at DESC
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, ts = EXCLUDED.ts, payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'doc:' || md5(d.url), 'company:' || i.smartlab_ticker, 'отчитался',
               CASE WHEN d.period ~ '^[0-9]{4}$' THEN make_date(CAST(d.period AS int), 12, 31) ELSE d.created_at::date END,
               CAST(NULL AS real), d.source
          FROM company_documents d JOIN issuers i USING (issuer_id)
         WHERE d.created_at > :вод AND i.smartlab_ticker IS NOT NULL
        ON CONFLICT DO NOTHING
    """), п)
    _отметить(conn, "docs", conn.execute(text("SELECT MAX(created_at) FROM company_documents")).scalar(), n)
    return n


def фонды(conn) -> int:
    """Последний снимок каждого фонда; ISIN → бумага → компания. Рёбра «держит» пересобираются целиком (их ~1 600)."""
    conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'fund:' || f.ticker, 'fund', f.ticker, f.name, f.category, CAST(NULL AS timestamptz),
               jsonb_build_object('uk', f.uk, 'category', f.category, 'isin', f.isin_pif), NOW()
          FROM funds f
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary, payload = EXCLUDED.payload, updated_at = NOW()
    """))
    conn.execute(text("DELETE FROM brain_edges WHERE kind = 'держит'"))
    r = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT 'fund:' || f.ticker, m.company_id, 'держит', MAX(h.snapshot_date), MAX(h.weight), 'fund_holdings_history'
          FROM fund_holdings_history h
          JOIN funds f ON f.fund_id = h.fund_id
          JOIN brain_ticker_map m ON m.ticker = h.isin
         WHERE h.snapshot_date = (SELECT MAX(x.snapshot_date) FROM fund_holdings_history x WHERE x.fund_id = h.fund_id)
         GROUP BY f.ticker, m.company_id
        ON CONFLICT DO NOTHING
    """))
    return r.rowcount


def индексы(conn) -> int:
    conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'index:' || ic.index_id, 'index', ic.index_id, COALESCE(i.name, ic.index_id), CAST(NULL AS text), MAX(ic.trade_date),
               jsonb_build_object('бумаг', COUNT(*)), NOW()
          FROM index_composition ic
          LEFT JOIN indices i ON i.secid = ic.index_id
         WHERE ic.trade_date = (SELECT MAX(x.trade_date) FROM index_composition x WHERE x.index_id = ic.index_id)
         GROUP BY ic.index_id, i.name
        ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts, payload = EXCLUDED.payload, updated_at = NOW()
    """))
    conn.execute(text("DELETE FROM brain_edges WHERE kind = 'включает'"))
    r = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT ON (ic.index_id, m.company_id) 'index:' || ic.index_id, m.company_id, 'включает', ic.trade_date, ic.weight, 'index_composition'
          FROM index_composition ic
          JOIN brain_ticker_map m ON m.ticker = ic.ticker
         WHERE ic.trade_date = (SELECT MAX(x.trade_date) FROM index_composition x WHERE x.index_id = ic.index_id)
         ORDER BY ic.index_id, m.company_id, ic.weight DESC
        ON CONFLICT DO NOTHING
    """))
    return r.rowcount


def факты(conn) -> int:
    """Связи и казначейские пакеты из world_facts. Направление — из fact_key own:A:B; доля в базе не хранится."""
    conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'fact:' || id, 'fact', id::text, left(statement, 160), statement, valid_from,
               jsonb_build_object('kind', kind, 'fact_key', fact_key, 'entities', to_jsonb(entities), 'source', source,
                                  'source_url', source_url, 'valid_until', valid_until, 'confidence', confidence), NOW()
          FROM world_facts
         WHERE kind IN ('связь', 'казначейский пакет') AND superseded_by IS NULL
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary, ts = EXCLUDED.ts, payload = EXCLUDED.payload, updated_at = NOW()
    """))
    conn.execute(text("DELETE FROM brain_edges WHERE kind = 'факт_о' OR (kind = 'владеет' AND (method IS NULL OR method = 'world_facts'))"))
    r = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
        SELECT 'company:' || x.a, 'company:' || x.b, 'владеет', w.valid_from, CAST(NULL AS real), 'fact:' || w.id,
               CASE WHEN w.confidence >= 0.9 THEN 'A' ELSE 'B' END, 'world_facts', w.valid_from
          FROM world_facts w
          CROSS JOIN LATERAL (SELECT substring(w.fact_key FROM '^(?\\:own|link):([A-Z0-9_]+):') AS a,
                                     substring(w.fact_key FROM '^(?\\:own|link):[A-Z0-9_]+:([A-Z0-9_]+)$') AS b) x
         WHERE w.kind = 'связь' AND w.superseded_by IS NULL AND x.a IS NOT NULL
           AND EXISTS (SELECT 1 FROM brain_nodes n WHERE n.id = 'company:' || x.a)
           AND EXISTS (SELECT 1 FROM brain_nodes n WHERE n.id = 'company:' || x.b)
        ON CONFLICT DO NOTHING
    """))
    n = r.rowcount
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'fact:' || w.id, m.company_id, 'факт_о', w.valid_from, CAST(NULL AS real), 'world_facts'
          FROM world_facts w JOIN brain_ticker_map m ON m.ticker = ANY(w.entities)
         WHERE w.kind IN ('связь', 'казначейский пакет') AND w.superseded_by IS NULL
        ON CONFLICT DO NOTHING
    """))
    return n


def аномалии(conn, full: bool) -> int:
    вод = None if full else _водяной(conn, "anomalies")
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    r = conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'anomaly:' || a.id, 'anomaly', a.id::text, COALESCE(a.headline, a.type || ' ' || a.asset_id), a.context, a.created_at,
               jsonb_build_object('type', a.type, 'asset', a.asset_id, 'asset_name', a.asset_name, 'direction', a.direction,
                                  'severity', a.severity_value, 'signal_date', a.signal_date, 'clgroup', a.clgroup), NOW()
          FROM anomalies a
         WHERE a.scope = 'public' AND a.created_at > :вод
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary, payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT 'anomaly:' || a.id, m.company_id, 'аномалия_по', a.created_at, a.severity_value, 'anomalies'
          FROM anomalies a JOIN brain_ticker_map m ON m.ticker = a.asset_id
         WHERE a.scope = 'public' AND a.created_at > :вод
        ON CONFLICT DO NOTHING
    """), п)
    _отметить(conn, "anomalies", conn.execute(text("SELECT MAX(created_at) FROM anomalies")).scalar(), n)
    return n


def сигналы(conn, full: bool) -> int:
    вод = None if full else _водяной(conn, "signals")
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    r = conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'signal:' || s.id, 'signal', s.id::text, left(regexp_replace(s.snippet, '\\s+', ' ', 'g'), 160), s.review_note, s.posted_at,
               jsonb_build_object('status', s.status, 'strength', s.strength, 'has_percent', s.has_percent, 'edge_state', s.edge_state,
                                  'channel', s.channel, 'message_id', s.message_id, 'tickers', to_jsonb(s.tickers)), NOW()
          FROM ownership_signals s
         WHERE GREATEST(s.created_at, COALESCE(s.reviewed_at, s.created_at)) > :вод
        ON CONFLICT (id) DO UPDATE SET summary = EXCLUDED.summary, payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'signal:' || s.id, m.company_id, 'сигнал_о', s.posted_at, CAST(NULL AS real), 'ownership_signals'
          FROM ownership_signals s JOIN brain_ticker_map m ON m.ticker = ANY(s.tickers)
         WHERE GREATEST(s.created_at, COALESCE(s.reviewed_at, s.created_at)) > :вод
        ON CONFLICT DO NOTHING
    """), п)
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT 'signal:' || s.id, 'news:' || lower(s.channel) || '/' || s.message_id, 'из_новости', s.posted_at, CAST(NULL AS real), 'ownership_signals'
          FROM ownership_signals s
         WHERE GREATEST(s.created_at, COALESCE(s.reviewed_at, s.created_at)) > :вод
           AND EXISTS (SELECT 1 FROM brain_nodes b WHERE b.id = 'news:' || lower(s.channel) || '/' || s.message_id)
        ON CONFLICT DO NOTHING
    """), п)
    _отметить(conn, "signals", conn.execute(text("SELECT MAX(GREATEST(created_at, COALESCE(reviewed_at, created_at))) FROM ownership_signals")).scalar(), n)
    return n


def держатели_узлы(conn) -> int:
    """Держатели как узлы: «Прочие» и «free float» — не держатели, их пропускаем.
    Рёбра теперь строит держатели_резолв()."""
    conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT DISTINCT ON (md5(lower(trim(holder)))) 'holder:' || md5(lower(trim(holder))), 'holder', md5(lower(trim(holder))),
               trim(holder), CAST(NULL AS text), CAST(NULL AS timestamptz), jsonb_build_object('source', source), NOW()
          FROM company_shareholders
         WHERE holder IS NOT NULL AND lower(trim(holder)) NOT IN ('прочие', 'прочее', 'free float', 'free-float', 'фри флоат', 'миноритарии')
         ORDER BY md5(lower(trim(holder))), updated_at DESC
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, updated_at = NOW()
    """))
    return 0


def _старые_рёбра_держателей(conn) -> int:  # не вызывается, оставлено до следующей чистки
    conn.execute(text("DELETE FROM brain_edges WHERE kind = 'владеет_долей'"))
    r = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT ON (md5(lower(trim(s.holder))), i.smartlab_ticker)
               'holder:' || md5(lower(trim(s.holder))), 'company:' || i.smartlab_ticker, 'владеет_долей',
               s.structure_as_of, s.share_pct, s.source
          FROM company_shareholders s JOIN issuers i USING (issuer_id)
         WHERE i.smartlab_ticker IS NOT NULL AND s.holder IS NOT NULL
           AND lower(trim(s.holder)) NOT IN ('прочие', 'прочее', 'free float', 'free-float', 'фри флоат', 'миноритарии')
         ORDER BY md5(lower(trim(s.holder))), i.smartlab_ticker, s.structure_as_of DESC NULLS LAST
        ON CONFLICT DO NOTHING
    """))
    return r.rowcount


# ── слой доверия ──────────────────────────────────────────────────────────────
# Уровень и способ — по виду связи. Новый вид связи = одна строка здесь; без строки
# ребро останется без уровня, и экран покажет «?» — лучше, чем молча выдать D за A.
УРОВНИ = {
    "упоминает":         ("B", "хэштег"),
    "о":                 ("B", "тикер_кандидата"),
    "из_новости":        ("A", "ссылка_на_источник"),
    "отчитался":         ("B", "financemarker"),
    "держит":            ("A", "раскрытие_ук"),
    "включает":          ("A", "moex"),
    "факт_о":            ("B", "world_facts"),
    "аномалия_по":       ("C", "детектор"),
    "сигнал_о":          ("C", "детектор"),
    "владеет_долей":     ("B", "акционеры"),
    "владеет":           ("B", "акционеры"),
    "в_секторе":         ("B", "классификация_smartlab"),
    "вместе_в_новостях": ("D", "совместные_упоминания"),
}
# Обычные слова, совпадающие с именами компаний: по ним автоматически не размечаем.
# Список сеется в brain_name_rules (ambiguous=true) и дальше правится в таблице.
НЕОДНОЗНАЧНЫЕ = {"магнит", "полюс", "самолет", "самолёт", "система", "лента", "энергия", "диод",
                 "мать и дитя", "красный октябрь", "т", "вуш", "озон", "русагро", "инарктика",
                 "европлан", "яковлев", "аптека", "детский мир", "белуга", "черкизово", "пик"}
# ⚠️ Полнотекст не различает регистр: «ПИК» и «пик цен» — одно слово, поэтому ПИК в списке.
# ВТБ, МТС, ММК, МКБ, ЛСР — аббревиатуры без бытового смысла, их короткая длина не повод.


def уровни(conn) -> int:
    """Проставить level/method/snapshot_date всем рёбрам, у которых их ещё нет."""
    n = 0
    for kind, (lvl, meth) in УРОВНИ.items():
        r = conn.execute(text("""
            UPDATE brain_edges SET level = COALESCE(level, :l), method = COALESCE(method, :m),
                                   snapshot_date = COALESCE(snapshot_date, ts::date)
             WHERE kind = :k AND (level IS NULL OR method IS NULL OR (snapshot_date IS NULL AND ts IS NOT NULL))
        """), {"l": lvl, "m": meth, "k": kind})
        n += r.rowcount
    return n


def правила_имён(conn) -> int:
    """Сеем правила разметки из справочника: короткое имя, отображаемое имя, имена активов в
    фондах (обрезаем «, акция об.», «ао»). Правки руками переживают пересев (DO NOTHING)."""
    r = conn.execute(text("""
        INSERT INTO brain_name_rules (pattern, company_id, ambiguous, enabled, source)
        SELECT DISTINCT ON (lower(p.pattern), p.company_id) p.pattern, p.company_id,
               ((length(p.pattern) <= 3 AND p.pattern <> upper(p.pattern)) OR lower(p.pattern) = ANY(string_to_array(:amb, '|'))),
               NOT ((length(p.pattern) <= 3 AND p.pattern <> upper(p.pattern)) OR lower(p.pattern) = ANY(string_to_array(:amb, '|'))), p.source
          FROM (
                SELECT i.name_short AS pattern, 'company:' || i.smartlab_ticker AS company_id, 'name_short' AS source
                  FROM issuers i WHERE i.smartlab_ticker IS NOT NULL AND i.name_short IS NOT NULL
                UNION ALL
                SELECT a.alias_value, m.company_id, 'display_name'
                  FROM issuer_aliases a JOIN brain_ticker_map m ON m.ticker = a.secid
                 WHERE a.alias_type = 'display_name' AND a.instrument_kind = 'share'
                UNION ALL
                SELECT trim(regexp_replace(a.alias_value, '(,\\s*(акци[яи]\\s*об\\.?|ао|ап|обыкн\\.?|прив\\.?)|\\s+(ао|ап)|-?ао)\\s*$', '', 'i')), m.company_id, 'fund_asset_name'
                  FROM issuer_aliases a JOIN brain_ticker_map m ON m.ticker = a.secid
                 WHERE a.alias_type = 'fund_asset_name' AND a.instrument_kind = 'share' AND a.secid IS NOT NULL
               ) p
         WHERE length(trim(p.pattern)) >= 2 AND p.pattern !~ '^[A-Z0-9.\\- ]+$'
           AND p.pattern !~* '(публичное|акционерное|общество|limited|company|public)'   -- юридические простыни не ищем в новостях
         ORDER BY lower(p.pattern), p.company_id
        ON CONFLICT (pattern, company_id) DO NOTHING
    """), {"amb": "|".join(sorted(НЕОДНОЗНАЧНЫЕ))})
    # Неоднозначность, выводимая из самих правил (правки руками — manual — не трогаем):
    #  • одно и то же имя у двух компаний («Россети» → FEES и MSRS);
    #  • однословное имя из названий активов фондов («Ренессанс», «МосБиржа») — это
    #    ярлыки бумаг, а не то, как компанию называют в новостях;
    #  • имя из списка обычных слов.
    conn.execute(text("""
        UPDATE brain_name_rules r SET ambiguous = x.amb, enabled = NOT x.amb, updated_at = NOW()
          FROM (
            SELECT r2.id,
                   (lower(r2.pattern) = ANY(string_to_array(:amb, '|'))
                    OR (length(r2.pattern) <= 3 AND r2.pattern <> upper(r2.pattern))
                    OR EXISTS (SELECT 1 FROM brain_name_rules o WHERE lower(o.pattern) = lower(r2.pattern) AND o.company_id <> r2.company_id)
                    OR (r2.source = 'fund_asset_name' AND position(' ' IN trim(r2.pattern)) = 0)) AS amb
              FROM brain_name_rules r2 WHERE NOT r2.manual
          ) x
         WHERE r.id = x.id AND (r.ambiguous <> x.amb OR r.enabled = x.amb)
    """), {"amb": "|".join(sorted(НЕОДНОЗНАЧНЫЕ))})
    # Мосбиржа: «Индекс Мосбиржи» — не компания. Вычитаем и проверяем остаток.
    conn.execute(text("""
        UPDATE brain_name_rules SET exclude_regex = 'индекс\w*\s+(мосбиржи|московской\s+биржи)|imoex',
               verify_regex = 'мосбирж|московск\w*\s+бирж'
         WHERE company_id = 'company:MOEX' AND NOT manual AND exclude_regex IS NULL
    """))
    return r.rowcount


def новости_по_имени(conn, full: bool) -> int:
    """Новости без хэштега тикера: разметка по имени компании полнотекстом (русская
    морфология: «Сбербанка», «Полюсом»). Уровень C, способ «имя». Только правила
    enabled и не ambiguous — остальные ждут решения в таблице правил."""
    вод = None if full else _водяной(conn, "news_names")
    с = datetime.now(timezone.utc) - timedelta(days=НОВОСТИ_ДНЕЙ)
    правила = conn.execute(text("SELECT pattern, company_id, exclude_regex, verify_regex FROM brain_name_rules WHERE enabled AND NOT ambiguous")).all()
    все_имена = [(r[0].lower(), r[1]) for r in conn.execute(text("SELECT pattern, company_id FROM brain_name_rules")).all()]
    if full:
        # полная пересборка разметки по имени: старые ложные рёбра не должны пережить новые правила
        conn.execute(text("DELETE FROM brain_edges WHERE kind = 'упоминает' AND method = 'имя'"))
    n = 0
    for pattern, company_id, excl, verify in правила:
        # Самое длинное имя побеждает: «Газпром» не срабатывает на «Газпром нефть»,
        # «Россети» — на «Россети Центр». Продолжения берутся из правил других компаний.
        продолжения = sorted({имя[len(pattern.lower()):].strip() for имя, cid in все_имена
                              if cid != company_id and имя.startswith(pattern.lower() + " ") and len(имя) > len(pattern) + 1})
        # Продолжение — по основе (первые 4 буквы): «Газпром нефти» и «нефтью» — одно слово.
        стоп = "(?!\\s+(" + "|".join(re.escape(x.split()[0][:4]) for x in продолжения if x) + "))" if продолжения else ""
        # Полнотекст стеммит: «Эталон» находит «эталонных». Проверка — целое слово с
        # русскими окончаниями, а не префикс.
        окончания = "(а|я|у|ю|ом|ем|ём|е|ы|и|ов|ев|ам|ям|ами|ями|ах|ях|ой|ей|ью|ия|ии|ию|ией)?"
        проверка = verify or ("\\m" + re.escape(pattern) + окончания + "\\M" + стоп)
        п = {"с": с, "вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc), "q": pattern, "cid": company_id,
             "ex": excl or "(?!x)x", "vf": проверка}
        conn.execute(text(f"""
            INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
            SELECT DISTINCT ON (1) 'news:' || {_КАНАЛ} || '/' || message_id, 'news', {_КАНАЛ} || '/' || message_id,
                   left(regexp_replace(text, '\\\\s+', ' ', 'g'), 160), CAST(NULL AS text), posted_at,
                   jsonb_build_object('channel', {_КАНАЛ}, 'views', views, 'tickers', to_jsonb(tickers),
                                      'url', 'https://t.me/' || {_КАНАЛ} || '/' || message_id), NOW()
              FROM news_archive n
             WHERE posted_at > :с AND imported_at > :вод
               AND to_tsvector('russian', text) @@ phraseto_tsquery('russian', :q)
               AND regexp_replace(text, :ex, '', 'gi') ~* :vf
               AND left(text, 160) !~* '(доброе утро!|итоги дня|акции и инвестиции|календарь на сегодня|ожидаем следующие события)'   -- дайджесты
             ORDER BY 1, imported_at DESC
            ON CONFLICT (id) DO NOTHING
        """), п)
        r = conn.execute(text(f"""
            INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
            SELECT DISTINCT 'news:' || {_КАНАЛ} || '/' || message_id, :cid, 'упоминает', posted_at, CAST(NULL AS real),
                   'news_archive', 'C', 'имя', posted_at::date
              FROM news_archive n
             WHERE posted_at > :с AND imported_at > :вод
               AND to_tsvector('russian', text) @@ phraseto_tsquery('russian', :q)
               AND regexp_replace(text, :ex, '', 'gi') ~* :vf
               AND left(text, 160) !~* '(доброе утро!|итоги дня|акции и инвестиции|календарь на сегодня|ожидаем следующие события)'
            ON CONFLICT DO NOTHING
        """), п)
        n += r.rowcount
    # Новость с пятью и больше компаниями по имени — обзор, а не упоминание: снимаем разметку.
    conn.execute(text("""
        DELETE FROM brain_edges e USING (
            SELECT src FROM brain_edges WHERE kind = 'упоминает' AND method = 'имя' GROUP BY src HAVING COUNT(*) >= 5
        ) d WHERE e.src = d.src AND e.kind = 'упоминает' AND e.method = 'имя'
    """))
    # Узлы новостей, оставшиеся без единой связи, карте не нужны.
    conn.execute(text("""
        DELETE FROM brain_nodes n WHERE n.kind = 'news'
           AND NOT EXISTS (SELECT 1 FROM brain_edges e WHERE e.src = n.id OR e.dst = n.id)
    """))
    _отметить(conn, "news_names", conn.execute(text("SELECT MAX(imported_at) FROM news_archive")).scalar(), n)
    return n


def держатели_резолв(conn) -> dict:
    """Акционеры строкой → компании справочника. Точное совпадение нормализованных имён —
    «авто»; похожее (pg_trgm ≥ 0.5) — «на_проверке» с вариантами; решения человека
    («подтверждено»/«отклонено») не перезаписываются."""
    conn.execute(text("DROP TABLE IF EXISTS _имена"))
    conn.execute(text("""
        CREATE TEMP TABLE _имена AS
        SELECT DISTINCT brain_norm(p.name) AS norm, p.company_id FROM (
            SELECT i.name_short AS name, 'company:' || i.smartlab_ticker AS company_id FROM issuers i WHERE i.smartlab_ticker IS NOT NULL
            UNION ALL SELECT i.name_full, 'company:' || i.smartlab_ticker FROM issuers i WHERE i.smartlab_ticker IS NOT NULL AND i.name_full IS NOT NULL
            UNION ALL SELECT a.alias_value, m.company_id FROM issuer_aliases a JOIN brain_ticker_map m ON m.ticker = a.secid
                       WHERE a.alias_type IN ('display_name', 'fund_asset_name') AND a.instrument_kind = 'share'
            UNION ALL SELECT r.pattern, r.company_id FROM brain_name_rules r
        ) p WHERE length(brain_norm(p.name)) >= 3
    """))
    r1 = conn.execute(text("""
        INSERT INTO brain_holder_map (holder_norm, holder, company_id, method, confidence, status, candidates, updated_at)
        -- ⚠️ Однословное точное совпадение — НЕ авто: «ООО Озон» (держатель Озон Фармацевтики)
        -- совпало с Ozon. Два и больше слов («афк система», «россети ленэнерго») — авто;
        -- одно слово — в очередь с уверенностью 0.9, человек подтверждает один раз.
        SELECT DISTINCT ON (brain_norm(s.holder)) brain_norm(s.holder), trim(s.holder), n.company_id, 'точное',
               CASE WHEN array_length(string_to_array(brain_norm(s.holder), ' '), 1) >= 2 THEN 1.0 ELSE 0.9 END,
               CASE WHEN array_length(string_to_array(brain_norm(s.holder), ' '), 1) >= 2 THEN 'авто' ELSE 'на_проверке' END,
               jsonb_build_array(jsonb_build_object('company_id', n.company_id, 'sim', 1.0)), NOW()
          FROM company_shareholders s JOIN _имена n ON n.norm = brain_norm(s.holder)
         WHERE length(brain_norm(s.holder)) >= 3
         ORDER BY brain_norm(s.holder), n.company_id
        ON CONFLICT (holder_norm) DO UPDATE SET company_id = EXCLUDED.company_id, method = EXCLUDED.method,
               confidence = EXCLUDED.confidence, status = EXCLUDED.status, candidates = EXCLUDED.candidates, updated_at = NOW()
         WHERE brain_holder_map.status IN ('авто', 'на_проверке', 'нет')
    """))
    r2 = conn.execute(text("""
        INSERT INTO brain_holder_map (holder_norm, holder, company_id, method, confidence, status, candidates, updated_at)
        SELECT h.norm, h.holder, (h.c->0->>'company_id'), 'похожее', CAST(h.c->0->>'sim' AS real), 'на_проверке', h.c, NOW()
          FROM (
                SELECT x.norm, x.holder,
                       (SELECT jsonb_agg(jsonb_build_object('company_id', y.company_id, 'sim', round(CAST(y.sim AS numeric), 2)) ORDER BY y.sim DESC)
                          FROM (SELECT n.company_id, MAX(similarity(n.norm, x.norm)) AS sim FROM _имена n
                                 WHERE similarity(n.norm, x.norm) >= 0.5 GROUP BY n.company_id ORDER BY sim DESC LIMIT 3) y) AS c
                  FROM (SELECT DISTINCT ON (brain_norm(holder)) brain_norm(holder) AS norm, trim(holder) AS holder FROM company_shareholders
                         WHERE length(brain_norm(holder)) >= 3 ORDER BY brain_norm(holder)) x
               ) h
         WHERE h.c IS NOT NULL AND NOT EXISTS (SELECT 1 FROM _имена n WHERE n.norm = h.norm)
        ON CONFLICT (holder_norm) DO UPDATE SET candidates = EXCLUDED.candidates, updated_at = NOW()
         WHERE brain_holder_map.status IN ('на_проверке', 'нет')
    """))
    r3 = conn.execute(text("""
        INSERT INTO brain_holder_map (holder_norm, holder, company_id, method, confidence, status, updated_at)
        SELECT DISTINCT ON (brain_norm(holder)) brain_norm(holder), trim(holder), NULL, NULL, NULL, 'нет', NOW() FROM company_shareholders
         WHERE length(brain_norm(holder)) >= 3
           AND lower(trim(holder)) NOT IN ('прочие', 'прочее', 'free float', 'free-float', 'фри флоат', 'миноритарии')
         ORDER BY brain_norm(holder)
        ON CONFLICT (holder_norm) DO NOTHING
    """))
    conn.execute(text("DELETE FROM brain_edges WHERE kind = 'владеет' AND method = 'акционеры'"))
    conn.execute(text("DELETE FROM brain_edges WHERE kind = 'владеет_долей'"))
    r4 = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
        SELECT DISTINCT ON (h.company_id, i.smartlab_ticker) h.company_id, 'company:' || i.smartlab_ticker, 'владеет',
               s.structure_as_of, s.share_pct, s.source, 'B', 'акционеры', s.structure_as_of
          FROM company_shareholders s JOIN issuers i USING (issuer_id)
          JOIN brain_holder_map h ON h.holder_norm = brain_norm(s.holder)
         WHERE i.smartlab_ticker IS NOT NULL AND h.status IN ('авто', 'подтверждено') AND h.company_id IS NOT NULL
           AND h.company_id <> 'company:' || i.smartlab_ticker
         ORDER BY h.company_id, i.smartlab_ticker, s.structure_as_of DESC NULLS LAST
        ON CONFLICT DO NOTHING
    """))
    r5 = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
        SELECT DISTINCT ON (md5(lower(trim(s.holder))), i.smartlab_ticker)
               'holder:' || md5(lower(trim(s.holder))), 'company:' || i.smartlab_ticker, 'владеет_долей',
               s.structure_as_of, s.share_pct, s.source, 'B', 'акционеры', s.structure_as_of
          FROM company_shareholders s JOIN issuers i USING (issuer_id)
          LEFT JOIN brain_holder_map h ON h.holder_norm = brain_norm(s.holder)
         WHERE i.smartlab_ticker IS NOT NULL AND s.holder IS NOT NULL
           AND lower(trim(s.holder)) NOT IN ('прочие', 'прочее', 'free float', 'free-float', 'фри флоат', 'миноритарии')
           AND NOT COALESCE(h.status IN ('авто', 'подтверждено') AND h.company_id IS NOT NULL, FALSE)
         ORDER BY md5(lower(trim(s.holder))), i.smartlab_ticker, s.structure_as_of DESC NULLS LAST
        ON CONFLICT DO NOTHING
    """))
    return {"точных": r1.rowcount, "на_проверке": r2.rowcount, "без_пары": r3.rowcount,
            "рёбер_владения": r4.rowcount, "рёбер_держателей": r5.rowcount}


def секторы(conn) -> int:
    conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'sector:' || md5(sector), 'sector', md5(sector), sector, CAST(NULL AS text), CAST(NULL AS timestamptz),
               jsonb_build_object('компаний', COUNT(*), 'классификация', 'smart-lab'), NOW()
          FROM issuers WHERE sector IS NOT NULL AND smartlab_ticker IS NOT NULL GROUP BY sector
        ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
    """))
    conn.execute(text("DELETE FROM brain_edges WHERE kind = 'в_секторе'"))
    r = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
        SELECT 'company:' || smartlab_ticker, 'sector:' || md5(sector), 'в_секторе', updated_at, CAST(NULL AS real),
               'issuers', 'B', 'классификация_smartlab', updated_at::date
          FROM issuers WHERE sector IS NOT NULL AND smartlab_ticker IS NOT NULL
        ON CONFLICT DO NOTHING
    """))
    return r.rowcount


def вместе(conn) -> int:
    """Компании, которые встречаются в одних новостях (2–5 компаний в новости; обзоры с
    десятком тикеров исключены). Уровень D: это факт о корпусе, не об отношениях."""
    conn.execute(text("DELETE FROM brain_edges WHERE kind = 'вместе_в_новостях'"))
    r = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
        SELECT a.dst, b.dst, 'вместе_в_новостях', MAX(a.ts), CAST(COUNT(*) AS real), 'brain_edges', 'D', 'совместные_упоминания', MAX(a.ts)::date
          FROM brain_edges a JOIN brain_edges b ON a.src = b.src AND a.dst < b.dst
          JOIN (SELECT src FROM brain_edges WHERE kind = 'упоминает' GROUP BY src HAVING COUNT(DISTINCT dst) BETWEEN 2 AND 5) ok ON ok.src = a.src
         WHERE a.kind = 'упоминает' AND b.kind = 'упоминает' AND a.dst LIKE 'company:%' AND b.dst LIKE 'company:%'
         GROUP BY a.dst, b.dst HAVING COUNT(*) >= 2
        ON CONFLICT DO NOTHING
    """))
    return r.rowcount


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="пересобрать всё, игнорируя водяные знаки")
    args = ap.parse_args()
    t0 = time.time()
    eng = create_engine(DB_URL)
    итог = {}
    with eng.begin() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '600s'"))
        итог["тикеров"] = карта_тикеров(conn)
        итог["компаний"] = компании(conn)
        итог["индексов"] = индексы_узлы(conn)
        итог["новостей"], _ = новости(conn, args.full)
        итог["кандидатов"] = кандидаты(conn, args.full)
        итог["документов"] = документы(conn, args.full)
        итог["фонды_рёбер"] = фонды(conn)
        итог["индексы_рёбер"] = индексы(conn)
        итог["владение_рёбер"] = факты(conn)
        итог["аномалий"] = аномалии(conn, args.full)
        итог["сигналов"] = сигналы(conn, args.full)
        держатели_узлы(conn)
        итог["правил_имён"] = правила_имён(conn)
        итог["новостей_по_имени"] = новости_по_имени(conn, args.full)
        итог["держатели"] = держатели_резолв(conn)
        итог["секторов_рёбер"] = секторы(conn)
        итог["вместе_рёбер"] = вместе(conn)
        итог["уровней_проставлено"] = уровни(conn)
        итог["узлов"] = conn.execute(text("SELECT COUNT(*) FROM brain_nodes")).scalar()
        итог["рёбер"] = conn.execute(text("SELECT COUNT(*) FROM brain_edges")).scalar()
    итог["сек"] = round(time.time() - t0, 1)
    итог["режим"] = "полный" if args.full else "инкремент"
    print(json.dumps(итог, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
