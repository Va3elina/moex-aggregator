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
               CAST(NULL AS text), d.created_at,
               jsonb_build_object('url', d.url, 'source', d.source, 'doc_type', d.doc_type, 'period', d.period, 'parsed', d.parsed), NOW()
          FROM company_documents d
         WHERE d.created_at > :вод
         ORDER BY md5(d.url), d.created_at DESC
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, ts = EXCLUDED.ts, payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'doc:' || md5(d.url), 'company:' || i.smartlab_ticker, 'отчитался', d.created_at, CAST(NULL AS real), d.source
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
    conn.execute(text("DELETE FROM brain_edges WHERE kind IN ('владеет', 'факт_о')"))
    r = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT 'company:' || x.a, 'company:' || x.b, 'владеет', w.valid_from, CAST(NULL AS real), 'fact:' || w.id
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


def акционеры(conn) -> int:
    """Держатели как узлы: «Прочие» и «free float» — не держатели, их пропускаем."""
    conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT DISTINCT ON (md5(lower(trim(holder)))) 'holder:' || md5(lower(trim(holder))), 'holder', md5(lower(trim(holder))),
               trim(holder), CAST(NULL AS text), CAST(NULL AS timestamptz), jsonb_build_object('source', source), NOW()
          FROM company_shareholders
         WHERE holder IS NOT NULL AND lower(trim(holder)) NOT IN ('прочие', 'прочее', 'free float', 'free-float', 'фри флоат', 'миноритарии')
         ORDER BY md5(lower(trim(holder))), updated_at DESC
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, updated_at = NOW()
    """))
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
        итог["акционеры_рёбер"] = акционеры(conn)
        итог["узлов"] = conn.execute(text("SELECT COUNT(*) FROM brain_nodes")).scalar()
        итог["рёбер"] = conn.execute(text("SELECT COUNT(*) FROM brain_edges")).scalar()
    итог["сек"] = round(time.time() - t0, 1)
    итог["режим"] = "полный" if args.full else "инкремент"
    print(json.dumps(итог, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
