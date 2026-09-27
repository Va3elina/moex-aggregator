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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import vocab  # noqa: E402 — единый словарь разметки: типы, чистка текста, шум
import news_types as nt  # noqa: E402 — отбор новостей без компании и их темы (с проверкой «про наш рынок»)

DB_URL = os.getenv("DB_URL")
НОВОСТИ_ДНЕЙ = 730

_КАНАЛ = "CASE channel WHEN 'MarketTwits' THEN 'markettwits' WHEN 'СМАРТЛАБ НОВОСТИ' THEN 'newssmartlab' ELSE channel END"
_URL = f"'https://t.me/' || {_КАНАЛ} || '/' || message_id"


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


def _текст_новости_sql(п: dict) -> tuple[str, str, str, str]:
    """Заголовок, суть, хэштеги и «шум ли» новости — одинаково для всех путей в мозг (хэштег тикера, имя компании).
    Текст — без хэштегов, эмодзи и ссылок (vocab.sql_чисто); хэштеги автора — в payload и в теги-рёбра разметки.
    Ожидает LATERAL (SELECT <чистый текст> AS ч) x рядом с news_archive n."""
    заголовок = vocab.sql_обрезать("x.ч", vocab.ЗАГОЛОВОК, п)
    суть = f"CASE WHEN length(x.ч) > {vocab.ЗАГОЛОВОК} THEN {vocab.sql_обрезать('x.ч', vocab.СУТЬ, п)} END"
    return заголовок, суть, vocab.sql_теги("n.hashtags"), vocab.sql_шум("n.text", "x.ч", п)


def новости(conn, full: bool) -> tuple[int, datetime | None]:
    вод = None if full else _водяной(conn, "news")
    с = datetime.now(timezone.utc) - timedelta(days=НОВОСТИ_ДНЕЙ)
    п = {"с": с, "вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    чисто = vocab.sql_чисто("n.text", п)
    заголовок, суть, теги, шум = _текст_новости_sql(п)
    # ⚠️ Шум — дайджесты, календари, котировки без слов («#SGZH = +35%») — в мозг не идёт (Вадим 27.09: «там должна
    # быть отфильтрованная информация»). Раньше календарь отчётностей с десятком тикеров становился «новостью про»
    # каждую компанию.
    r = conn.execute(text(f"""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        -- DISTINCT ON: на стыке 31.08 одно сообщение лежит и под MarketTwits, и под
        -- markettwits — после нормализации это один id, а ON CONFLICT в одном
        -- INSERT дважды одну строку трогать не может.
        SELECT DISTINCT ON (1) 'news:' || {_КАНАЛ} || '/' || message_id, 'news', {_КАНАЛ} || '/' || message_id,
               {заголовок}, {суть}, posted_at,
               jsonb_build_object('channel', {_КАНАЛ}, 'views', views, 'tickers', to_jsonb(tickers),
                                  'hashtags', to_jsonb({теги}), 'url', {_URL}), NOW()
          FROM news_archive n
          CROSS JOIN LATERAL (SELECT {чисто} AS ч) x
         WHERE posted_at > :с AND imported_at > :вод AND cardinality(tickers) > 0
           AND EXISTS (SELECT 1 FROM brain_ticker_map m WHERE m.ticker = ANY(n.tickers))
           AND NOT {шум}
         ORDER BY 1, imported_at DESC
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary, payload = EXCLUDED.payload,
               updated_at = NOW()
    """), п)
    n = r.rowcount
    # Связи — только у новостей, ставших узлами: у шума узла нет, значит, и «упоминает» нет.
    conn.execute(text(f"""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'news:' || {_КАНАЛ} || '/' || message_id, m.company_id, 'упоминает', posted_at, CAST(NULL AS real), 'news_archive'
          FROM news_archive n
          JOIN brain_ticker_map m ON m.ticker = ANY(n.tickers)
         WHERE posted_at > :с AND imported_at > :вод
           AND EXISTS (SELECT 1 FROM brain_nodes b WHERE b.id = 'news:' || {_КАНАЛ} || '/' || n.message_id)
        ON CONFLICT DO NOTHING
    """), п)
    # Дайджест с шестью и больше компаниями — обзор, а не упоминание: «Итоги дня» по 10
    # тикерам делали новость «про» каждую из них (122 такие новости ≈ 1 000 связей на
    # 10.09.2026). Считаем по компаниям, а не по тикерам: SBER и SBERP — одна.
    # Узлы, оставшиеся без связей, чистит новости_по_имени.
    conn.execute(text("""
        DELETE FROM brain_edges e USING (
            SELECT src FROM brain_edges WHERE kind = 'упоминает' AND COALESCE(method, 'хэштег') = 'хэштег'
             GROUP BY src HAVING COUNT(*) > 5
        ) d WHERE e.src = d.src AND e.kind = 'упоминает' AND COALESCE(e.method, 'хэштег') = 'хэштег'
    """))
    новый = conn.execute(text("SELECT MAX(imported_at) FROM news_archive")).scalar()
    _отметить(conn, "news", новый, n)
    return n, новый


def _заголовок_кандидата(п: dict) -> str:
    """Заголовок кандидата — первая строка поста. У MarketTwits она бывает из одних хэштегов и эмодзи («❗️🛢🇺🇸#ормуз
    #трамп #нефть»): тогда — начало исходного текста, тоже без них."""
    h = vocab.sql_чисто("c.headline", п, "чк")
    t = vocab.sql_чисто("left(c.raw_text, 400)", п, "чт")
    return (f"COALESCE(NULLIF({vocab.sql_обрезать(h, vocab.ЗАГОЛОВОК, п, 'ок')}, ''), "
            f"NULLIF({vocab.sql_обрезать(t, vocab.ЗАГОЛОВОК, п, 'от')}, ''), '(без заголовка)')")


def _суть_кандидата(п: dict) -> str:
    return f"NULLIF({vocab.sql_обрезать(vocab.sql_чисто('c.annotation', п, 'ча'), vocab.СУТЬ, п, 'оа')}, '')"


def кандидаты(conn, full: bool) -> int:
    вод = None if full else _водяной(conn, "candidates")
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    r = conn.execute(text(f"""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'candidate:' || id, CASE WHEN status = 'published' THEN 'post' ELSE 'candidate' END, id::text,
               {_заголовок_кандидата(п)}, {_суть_кандидата(п)}, COALESCE(published_at, created_at),
               jsonb_build_object('status', status, 'verdict', judge_verdict, 'source', source, 'event_type', event_type,
                                  'importance', importance_1_5, 'tickers', to_jsonb(tickers), 'published_at', published_at), NOW()
          FROM content_candidates c
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


# ── официальные события: раскрытия, объявления биржи, отчёты ────────────────────
# ⚠️ ВСЁ, А НЕ ТОЛЬКО КАНДИДАТЫ (Вадим 10.09.2026: «это должно попадать в мозг
# независимо от того, кандидат это или нет»). Раньше раскрытие FinanceMarker или
# объявление МосБиржи попадало в карту, только если сканер делал из него кандидата в
# пост: из 78 отчётных раскрытий за 4 дня не появилось ни одного узла, из 93 объявлений
# биржи — три. Фильтр «стоит ли поста» — дело завода постов, а не памяти.
#
# Служебный шум торгов (коридоры РЕПО, аукционы, риск-параметры) о компаниях ничего не
# говорит — его в карту не несём.
_БИРЖА_ШУМ = (r"РЕПО|ценов\w+ коридор|дискретн\w+ аукцион|депозитн\w+ аукцион|риск-параметр|"
              r"ставк\w+ риска|дестабилизации цен|дополнительн\w+ услови\w+ проведения торгов|цен\w* исполнения"
              r"|технические работы|плановые релизы")


def раскрытия(conn, full: bool) -> int:
    """Раскрытия FinanceMarker (disclosure_events) — все, не только ставшие кандидатами.
    Уровень B: FinanceMarker пересказывает официальное сообщение, это посредник.

    ⚠️ ts — когда раскрытие появилось в ленте, а не event_date: у отчётов FinanceMarker
    ставит дату отчёта (31.08 для отчёта, вышедшего 09.09), и свежее выглядело бы старым."""
    вод = None if full else _водяной(conn, "disclosures")
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    r = conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'disclosure:' || d.id, 'disclosure', CAST(d.id AS text),
               CASE d.category WHEN 'REPORT' THEN 'Отчётность' WHEN 'DIVIDEND' THEN 'Дивиденды'
                    WHEN 'INSIDER_TRANSACTION' THEN 'Сделка инсайдера'
                    WHEN 'OPERATION' THEN 'Операционные результаты' ELSE 'Событие' END
                 || COALESCE(' · ' || d.name, '') || ': ' || left(d.title, 200),
               left(regexp_replace(COALESCE(d.description, ''), '\\s+', ' ', 'g'), 600),
               COALESCE(d.created_at, CAST(d.event_date AS timestamptz)),
               jsonb_build_object('category', d.category, 'type', d.type, 'event_date', d.event_date,
                                  'code', d.code, 'url', COALESCE(d.dir_link, d.link),
                                  'dividend_status', d.dividend_status, 'transaction_type', d.transaction_type), NOW()
          FROM disclosure_events d
         WHERE COALESCE(d.updated_at, d.created_at) > :вод
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary,
               payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'disclosure:' || d.id, m.company_id, 'раскрытие_о',
               COALESCE(d.created_at, CAST(d.event_date AS timestamptz)), CAST(NULL AS real), 'financemarker'
          FROM disclosure_events d JOIN brain_ticker_map m ON m.ticker = COALESCE(d.secid, d.code)
         WHERE COALESCE(d.updated_at, d.created_at) > :вод
        ON CONFLICT DO NOTHING
    """), п)
    _отметить(conn, "disclosures", conn.execute(text(
        "SELECT MAX(COALESCE(updated_at, created_at)) FROM disclosure_events")).scalar(), n)
    return n


def отчёты(conn, full: bool) -> int:
    """Отчёты, скачанные целиком (document_versions), с цифрами, которые из них извлёк
    агент-читатель (document_facts: значение, страница, цитата). Уровень A — это
    собственный отчёт компании. Цифры лежат в узле отчёта (сводка и payload), отдельными
    узлами их не плодим: ни с чем, кроме своего отчёта, они не связаны."""
    вод = None if full else _водяной(conn, "reports")
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    r = conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'report:' || v.id, 'report', CAST(v.id AS text),
               'Отчёт ' || COALESCE(v.standard || ' ', '') || COALESCE(v.secid || ' ', '')
                 || CASE v.period_code WHEN 'y' THEN 'за ' || v.year || ' год'
                                       WHEN '6m' THEN 'за 6 мес. ' || v.year
                                       WHEN '9m' THEN 'за 9 мес. ' || v.year
                                       WHEN 'q' THEN 'за ' || COALESCE(v.month / 3, 0) || ' кв. ' || v.year
                                       ELSE COALESCE(v.period_code || ' ', '') || COALESCE(CAST(v.year AS text), '') END,
               NULLIF(left(COALESCE(rd.summary, '') || COALESCE(' Цифры: ' || f.цифры, ''), 1200), ''),
               v.fetched_at,
               jsonb_build_object('doc_type', v.doc_type, 'standard', v.standard, 'period_code', v.period_code,
                                  'year', v.year, 'month', v.month, 'pages', v.pages, 'url', v.url,
                                  -- ⚠️ не '[]'::jsonb: pg8000 портит литерал, база получает пустую строку
                                  'facts', COALESCE(f.facts, jsonb_build_array())), NOW()
          FROM document_versions v
          -- ⚠️ summary у агента-читателя — jsonb с разделами (сегменты, дивиденды, риски…);
          -- для узла берём «одной_фразой», остальное доступно по ссылке на отчёт.
          LEFT JOIN LATERAL (SELECT x.summary ->> 'одной_фразой' AS summary FROM document_reads x
                              WHERE x.version_id = v.id ORDER BY x.created_at DESC LIMIT 1) rd ON TRUE
          -- Коды полей читателя (net_profit, revenue) — по-русски из справочника показателей;
          -- цифра, которая не сошлась с FinanceMarker, помечается в самой строке.
          LEFT JOIN LATERAL (
              SELECT string_agg(COALESCE(mr.label_ru, x.field) || ' '
                                || COALESCE(replace(CAST(x.value_num AS text), '.', ','), x.value_text, '')
                                || COALESCE(' ' || x.unit, '') || COALESCE(' (стр. ' || x.page || ')', '')
                                || CASE WHEN x.mismatch THEN ' — не сходится с FinanceMarker' ELSE '' END,
                                '; ' ORDER BY x.field) AS цифры,
                     jsonb_agg(jsonb_build_object('поле', COALESCE(mr.label_ru, x.field), 'код', x.field,
                                                  'число', x.value_num, 'текст', x.value_text, 'ед', x.unit,
                                                  'стр', x.page, 'цитата', left(x.quote, 200),
                                                  'расхождение_с_fm', x.mismatch)) AS facts
                FROM document_facts x LEFT JOIN metrics_ref mr ON mr.metric_code = x.field
               WHERE x.version_id = v.id) f ON TRUE
         WHERE v.superseded_at IS NULL
           AND GREATEST(v.fetched_at, (SELECT MAX(x.created_at) FROM document_reads x WHERE x.version_id = v.id)) > :вод
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary,
               payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'report:' || v.id, m.company_id, 'отчёт_о', v.fetched_at, CAST(NULL AS real), 'document_versions'
          FROM document_versions v JOIN brain_ticker_map m ON m.ticker = v.secid
         WHERE v.superseded_at IS NULL AND v.fetched_at > :вод
        ON CONFLICT DO NOTHING
    """), п)
    # Заменённая версия отчёта (перевыпуск) из карты уходит — вместе со связями.
    conn.execute(text("""
        DELETE FROM brain_edges WHERE src IN (SELECT 'report:' || id FROM document_versions WHERE superseded_at IS NOT NULL)
    """))
    conn.execute(text("""
        DELETE FROM brain_nodes WHERE id IN (SELECT 'report:' || id FROM document_versions WHERE superseded_at IS NOT NULL)
    """))
    _отметить(conn, "reports", conn.execute(text("""
        SELECT GREATEST((SELECT MAX(fetched_at) FROM document_versions), (SELECT MAX(created_at) FROM document_reads))
    """)).scalar(), n)
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
           -- «держит 0,0007 %» — остаток после ребаланса, не позиция: в карту не берём
           AND (h.weight IS NULL OR h.weight >= 0.05)
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


# ── история индексов и фондов: события «вошла / вышла», «открыл / закрыл» ─────────
# ⚠️ Зачем (Вадим 10.09). Мозг знал только ПОСЛЕДНИЙ состав индекса и последний срез
# фонда: «входит в IMOEX», «фонд держит». Когда бумага вошла, когда фонд её набрал —
# терялось, а это ровно то, что двигает цену и позиции. Правило Вадима — всё, что
# добавляем, автоматически и с фильтром: у индексов событий мало и данные чистые (вся
# история IMOEX с 2007 года — 118 входов и 93 выхода), у фондов шума много — фильтр ниже.

_СОБЫТИЯ_ИНДЕКСОВ = """
    WITH d AS (SELECT DISTINCT index_id, trade_date FROM index_composition),
         p AS (SELECT index_id, trade_date, lag(trade_date) OVER (PARTITION BY index_id ORDER BY trade_date) AS prev FROM d),
         ev AS (
            SELECT c.index_id, c.ticker, p.trade_date, p.prev, 'вход' AS тип, c.weight
              FROM p JOIN index_composition c ON c.index_id = p.index_id AND c.trade_date = p.trade_date
             WHERE p.prev IS NOT NULL AND p.trade_date > CAST(:вод AS date)
               AND NOT EXISTS (SELECT 1 FROM index_composition x
                                WHERE x.index_id = p.index_id AND x.trade_date = p.prev AND x.ticker = c.ticker)
            UNION ALL
            SELECT x.index_id, x.ticker, p.trade_date, p.prev, 'выход', x.weight
              FROM p JOIN index_composition x ON x.index_id = p.index_id AND x.trade_date = p.prev
             WHERE p.prev IS NOT NULL AND p.trade_date > CAST(:вод AS date)
               AND NOT EXISTS (SELECT 1 FROM index_composition c
                                WHERE c.index_id = p.index_id AND c.trade_date = p.trade_date AND c.ticker = x.ticker))
"""


def события_индексов(conn, full: bool) -> int:
    """Входы и выходы бумаг из индексов МосБиржи — сравнением соседних дат состава.
    Уровень A: состав публикует биржа. Событие связано и с компанией, и с индексом."""
    вод = None if full else _водяной(conn, "index_events")
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc)}
    r = conn.execute(text(f"""
        {_СОБЫТИЯ_ИНДЕКСОВ}
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'index_event:' || ev.index_id || '/' || ev.ticker || '/' || ev.trade_date, 'index_event',
               ev.index_id || '/' || ev.ticker || '/' || ev.trade_date,
               COALESCE(c.title, ev.ticker)
                 || CASE ev.тип WHEN 'вход' THEN ' вошла в индекс ' ELSE ' вышла из индекса ' END || ev.index_id,
               CAST(NULL AS text), CAST(ev.trade_date AS timestamptz),
               jsonb_build_object('индекс', ev.index_id, 'тикер', ev.ticker, 'тип', ev.тип, 'дата', ev.trade_date,
                                  'прежний_состав_на', ev.prev, 'вес', ev.weight), NOW()
          FROM ev
          LEFT JOIN brain_ticker_map m ON m.ticker = ev.ticker
          LEFT JOIN brain_nodes c ON c.id = m.company_id
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text(f"""
        {_СОБЫТИЯ_ИНДЕКСОВ}
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT 'index_event:' || ev.index_id || '/' || ev.ticker || '/' || ev.trade_date, x.dst,
               'событие_индекса', CAST(ev.trade_date AS timestamptz), ev.weight, 'index_composition'
          FROM ev JOIN brain_ticker_map m ON m.ticker = ev.ticker
          CROSS JOIN LATERAL (VALUES (m.company_id), ('index:' || ev.index_id)) x(dst)
        ON CONFLICT DO NOTHING
    """), п)
    _отметить(conn, "index_events", conn.execute(text(
        "SELECT CAST(MAX(trade_date) AS timestamptz) FROM index_composition")).scalar(), n)
    return n


# Фильтр фондов — замер на боевой БД 10.09 (2 года, наши компании): без фильтра 2 036
# «новых» и 1 979 «закрытых» почти поровну — признак того, что часть фондов раскрывает
# не весь портфель, а крупнейшие бумаги, и хвост списка то появляется, то пропадает.
# Поэтому: только полные срезы (≥ 5 бумаг; один фонд присылает по одной строке в день),
# вход и выход — только весомой бумаги, изменение — от половины позиции.
# ⚠️ У фондов СНИМКИ месячные, не сделки: «между срезами», а не «купил в день X».
_ФОНД_СРЕЗ_МИН = 5       # бумаг в срезе, чтобы считать его полным
_ФОНД_ВЕС_МИН = 1.0      # % фонда: вход и выход считаем только для весомой бумаги
_ФОНД_ИЗМ = 0.5          # изменение числа бумаг на половину и больше
_ФОНД_ДНЕЙ = 730
# Источник — тот же, что у страницы сделок фондов (api/routers/fund_trades.py, MONTHLY_SOURCES):
# только документы УК. Прогон 10.09: реконструкция cbonds/cbonds_calc лежит в те же даты рядом с
# документами, доли в срезе складывались до 160 %, и 483 события из 1 416 были мнимыми —
# облигационный фонд «сокращал» X5 в 10 раз и через месяц «наращивал» обратно.
_ФОНД_ИСТОЧНИКИ = ["vim_sdr", "interfax_manual"]
_ФОНД_СУММА_МАКС = 120   # % фонда: срез, где доли в сумме больше, склеен из двух раскрытий
# Водяной знак с версией: смена правил отбора требует пересобрать события целиком, а не
# дописать новые поверх мнимых. Новая версия ключа = один полный прогон с очисткой.
_ФОНД_ВЕРСИЯ = "fund_events:2"

_СОБЫТИЯ_ФОНДОВ = """
    -- ⚠️ Пороги — с явным типом: pg8000 выводит тип параметра из соседа, и 0,5 рядом с
    -- bigint-колонкой positions приходил как «bigint 0.5» → ошибка ввода.
    -- Одна бумага в срезе бывает несколькими строками (лоты, разные источники):
    -- складываем, иначе событие дублируется и ON CONFLICT падает на повторе.
    -- Один источник на срез — только документы УК (_ФОНД_ИСТОЧНИКИ). В одну дату бывают и
    -- vim_sdr, и interfax_manual: берём тот, чьи доли в сумме ближе к 100. Срез, где сумма
    -- выше потолка, склеен из двух раскрытий — в сравнение не идёт.
    WITH src AS (SELECT DISTINCT ON (fund_id, snapshot_date) fund_id, snapshot_date, source
                   FROM (SELECT fund_id, snapshot_date, source, SUM(weight) AS s
                           FROM fund_holdings_history
                          WHERE snapshot_date >= CAST(:окно AS date) AND source = ANY(:источники)
                          GROUP BY fund_id, snapshot_date, source) x
                  WHERE COALESCE(s, 0) <= CAST(:сумма AS numeric)
                  ORDER BY fund_id, snapshot_date, ABS(COALESCE(s, 0) - 100), source),
         h AS (SELECT h.fund_id, h.snapshot_date, h.isin, SUM(h.positions) AS positions,
                      SUM(h.weight) AS weight, MIN(m.company_id) AS company_id
                 FROM fund_holdings_history h
                 JOIN src ON src.fund_id = h.fund_id AND src.snapshot_date = h.snapshot_date AND src.source = h.source
                 JOIN brain_ticker_map m ON m.ticker = h.isin
                -- окно: события считаем за недавние срезы, прежний срез — в пределах года до них
                WHERE h.snapshot_date >= CAST(:окно AS date)
                GROUP BY h.fund_id, h.snapshot_date, h.isin),
         -- ⚠️ Размер среза — по СОПОСТАВЛЕННЫМ бумагам, а не по строкам. Прогон 10.09: EQMX до
         -- 25.08 присылал 47 строк без ISIN, с 26.08 — с ISIN, и весь портфель (Лукойл 15,7 %%)
         -- вышел «новыми позициями». Срез, где ничего не сопоставилось, в сравнение не идёт.
         full_snap AS (SELECT fund_id, snapshot_date, COUNT(*) AS n FROM h
                        GROUP BY fund_id, snapshot_date HAVING COUNT(*) >= CAST(:срез AS bigint)),
         s0 AS (SELECT fund_id, snapshot_date, n,
                       lag(snapshot_date) OVER (PARTITION BY fund_id ORDER BY snapshot_date) AS prev,
                       lag(n) OVER (PARTITION BY fund_id ORDER BY snapshot_date) AS prev_n FROM full_snap),
         -- Сравниваем только срезы похожего размера. Прогон 10.09: «Индекс МосБиржи» 25.08
         -- раскрыл урезанный список, 26.08 — полный, и Сбербанк с долей 12,8 %% вышел
         -- «новой позицией». Разный размер срезов = разная полнота, а не сделки.
         s AS (SELECT fund_id, snapshot_date, prev FROM s0
                WHERE prev IS NULL OR LEAST(n, prev_n) >= 0.7 * GREATEST(n, prev_n)),
         cur AS (SELECT s.fund_id, s.snapshot_date, s.prev, h.isin, h.company_id, h.positions, h.weight
                   FROM s JOIN h ON h.fund_id = s.fund_id AND h.snapshot_date = s.snapshot_date
                  WHERE s.prev IS NOT NULL AND s.snapshot_date > CAST(:с AS date) AND s.snapshot_date > CAST(:вод AS date)),
         pre AS (SELECT s.fund_id, s.snapshot_date, s.prev, h.isin, h.company_id, h.positions, h.weight
                   FROM s JOIN h ON h.fund_id = s.fund_id AND h.snapshot_date = s.prev
                  WHERE s.prev IS NOT NULL AND s.snapshot_date > CAST(:с AS date) AND s.snapshot_date > CAST(:вод AS date)),
         ev AS (
            SELECT c.fund_id, c.snapshot_date, c.prev, c.isin, c.company_id, 'новая позиция' AS тип, c.weight,
                   CAST(NULL AS numeric) AS было, CAST(c.positions AS numeric) AS стало
              FROM cur c
             WHERE COALESCE(c.weight, 0) >= CAST(:вес AS numeric)
               AND NOT EXISTS (SELECT 1 FROM pre p WHERE p.fund_id = c.fund_id AND p.snapshot_date = c.snapshot_date AND p.isin = c.isin)
            UNION ALL
            SELECT p.fund_id, p.snapshot_date, p.prev, p.isin, p.company_id, 'закрыта позиция', p.weight,
                   CAST(p.positions AS numeric), CAST(0 AS numeric)
              FROM pre p
             WHERE COALESCE(p.weight, 0) >= CAST(:вес AS numeric)
               AND NOT EXISTS (SELECT 1 FROM cur c WHERE c.fund_id = p.fund_id AND c.snapshot_date = p.snapshot_date AND c.isin = p.isin)
            UNION ALL
            SELECT c.fund_id, c.snapshot_date, c.prev, c.isin, c.company_id,
                   CASE WHEN c.positions > p.positions THEN 'нарастил позицию' ELSE 'сократил позицию' END, c.weight,
                   CAST(p.positions AS numeric), CAST(c.positions AS numeric)
              FROM cur c JOIN pre p ON p.fund_id = c.fund_id AND p.snapshot_date = c.snapshot_date AND p.isin = c.isin
             WHERE c.positions > 0 AND p.positions > 0
               AND ABS(c.positions - p.positions) >= CAST(:изм AS numeric) * p.positions
               AND GREATEST(COALESCE(c.weight, 0), COALESCE(p.weight, 0)) >= CAST(:вес AS numeric) / 2)
"""


def события_фондов(conn, full: bool) -> int:
    """Фонд открыл, закрыл, нарастил или сократил позицию — между соседними полными
    срезами одного фонда, только по нашим компаниям, за два года. Уровень A: раскрытие
    управляющей компании. Событие связано и с компанией, и с фондом."""
    вод = None if full else _водяной(conn, _ФОНД_ВЕРСИЯ)
    if вод is None:
        # полная пересборка — с чистого листа; вектора уходят каскадом (brain_embeddings)
        conn.execute(text("DELETE FROM brain_edges WHERE kind = 'событие_фонда'"))
        conn.execute(text("DELETE FROM brain_nodes WHERE kind = 'fund_event'"))
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc),
         "с": datetime.now(timezone.utc) - timedelta(days=_ФОНД_ДНЕЙ),
         "срез": _ФОНД_СРЕЗ_МИН, "вес": _ФОНД_ВЕС_МИН, "изм": _ФОНД_ИЗМ,
         "источники": _ФОНД_ИСТОЧНИКИ, "сумма": _ФОНД_СУММА_МАКС}
    # ⚠️ Скорость. Прогон 10.09: 44 с — расчёт шёл дважды (узлы и связи) и по всей истории
    # с 2021 года, а синк мозга каждые 15 минут обычно укладывается в 10–20 с. Считаем
    # один раз во временную таблицу и только в окне: прежний срез — не старше года.
    п["окно"] = max(п["с"], п["вод"]) - timedelta(days=400)
    ключ = "'fund_event:' || ev.fund_id || '/' || ev.isin || '/' || ev.snapshot_date"
    conn.execute(text("DROP TABLE IF EXISTS _fund_ev"))
    conn.execute(text(f"CREATE TEMP TABLE _fund_ev ON COMMIT DROP AS {_СОБЫТИЯ_ФОНДОВ} SELECT * FROM ev"), п)
    r = conn.execute(text(f"""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT {ключ}, 'fund_event', ev.fund_id || '/' || ev.isin || '/' || ev.snapshot_date,
               COALESCE(f.name, 'фонд ' || ev.fund_id) || ' — ' || ev.тип || ': ' || COALESCE(c.title, ev.isin),
               'между срезами ' || to_char(ev.prev, 'DD.MM.YYYY') || ' и ' || to_char(ev.snapshot_date, 'DD.MM.YYYY')
                 || COALESCE(', было ' || ev.было || ' шт.', '') || ', стало ' || ev.стало || ' шт.'
                 -- знак процента — chr(37): литерал «%» pg8000 читает как плейсхолдер,
                 -- а «%%» в этом месте доезжал до базы как есть
                 || COALESCE(', доля ' || replace(CAST(round(ev.weight, 2) AS text), '.', ',') || chr(37), ''),
               CAST(ev.snapshot_date AS timestamptz),
               jsonb_build_object('фонд', f.ticker, 'isin', ev.isin, 'тип', ev.тип, 'срез', ev.snapshot_date,
                                  'прежний_срез', ev.prev, 'было', ev.было, 'стало', ev.стало, 'доля', ev.weight), NOW()
          FROM _fund_ev ev
          LEFT JOIN funds f ON f.fund_id = ev.fund_id
          LEFT JOIN brain_nodes c ON c.id = ev.company_id
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary,
               payload = EXCLUDED.payload, updated_at = NOW()
    """))
    n = r.rowcount
    conn.execute(text(f"""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source)
        SELECT DISTINCT {ключ}, x.dst, 'событие_фонда', CAST(ev.snapshot_date AS timestamptz),
               CAST(ev.weight AS real), 'fund_holdings_history'
          FROM _fund_ev ev JOIN funds f ON f.fund_id = ev.fund_id
          CROSS JOIN LATERAL (VALUES (ev.company_id), ('fund:' || f.ticker)) x(dst)
        ON CONFLICT DO NOTHING
    """))
    _отметить(conn, _ФОНД_ВЕРСИЯ, conn.execute(text(
        "SELECT CAST(MAX(snapshot_date) AS timestamptz) FROM fund_holdings_history")).scalar(), n)
    return n


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
    заголовок = vocab.sql_обрезать(vocab.sql_чисто("s.snippet", п), vocab.ЗАГОЛОВОК, п)
    r = conn.execute(text(f"""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'signal:' || s.id, 'signal', s.id::text, COALESCE(NULLIF({заголовок}, ''), '(без текста)'), s.review_note, s.posted_at,
               jsonb_build_object('status', s.status, 'strength', s.strength, 'has_percent', s.has_percent, 'edge_state', s.edge_state,
                                  'channel', s.channel, 'message_id', s.message_id, 'tickers', to_jsonb(s.tickers)), NOW()
          FROM ownership_signals s
         WHERE GREATEST(s.created_at, COALESCE(s.reviewed_at, s.created_at)) > :вод
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary, payload = EXCLUDED.payload, updated_at = NOW()
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
               -- Источник режет имя на 100 символах посреди слова («…ПАО АФК «Систе»): доводим до
               -- целого слова и ставим многоточие, чтобы агент не вынес обрубок в текст.
               CASE WHEN length(trim(holder)) >= 100 THEN regexp_replace(trim(holder), '\s+\S{0,12}$', '') || '…' ELSE trim(holder) END,
               CAST(NULL AS text), CAST(NULL AS timestamptz), jsonb_build_object('source', source), NOW()
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
    "раскрытие_о":       ("B", "раскрытие_fm"),
    "объявление_о":      ("A", "moex"),      # по имени в тексте ставится явно: C, «имя»
    "отчёт_о":           ("A", "документ"),
    "событие_индекса":   ("A", "moex"),
    "событие_фонда":     ("A", "раскрытие_ук"),
    "тип":               ("C", "слова"),       # ставит разметка() со своим способом; строка — на случай без него
    "отрасль":           ("B", "компания"),
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
         WHERE length(trim(p.pattern)) >= 2
           AND p.pattern !~ '^[A-Z]{3,6}$' AND p.pattern !~ '^[A-Z]{2}[A-Z0-9]{10}$'     -- тикеры и ISIN — не имена; «X5» — имя
           AND p.pattern !~* '(публичное|акционерное|общество|limited|company|public)'   -- юридические простыни не ищем в новостях
           -- ⚠️ В названиях активов фондов у ГДР стоит депозитарий: «The Bank of New York Mellon» → X5.
           AND p.pattern !~* '(bank of new york|mellon|citibank|deutsche bank|jpmorgan|j\.p\. morgan|депозитар|custod)'
         ORDER BY lower(p.pattern), p.company_id,
                  CASE p.source WHEN 'name_short' THEN 0 WHEN 'display_name' THEN 1 ELSE 2 END
        ON CONFLICT (pattern, company_id) DO NOTHING
    """), {"amb": "|".join(sorted(НЕОДНОЗНАЧНЫЕ))})
    # ⚠️ Источник правила решает его судьбу: однословное из fund_asset_name — неоднозначно,
    # из name_short — нет. Первый сев сохранил «Роснефть» как fund_asset_name (DISTINCT ON
    # взял первую попавшуюся строку), и 60 нормальных имён — Роснефть, НОВАТЭК, Мечел —
    # оказались выключены. Поэтому источник поднимаем до лучшего из справочника.
    conn.execute(text("""
        UPDATE brain_name_rules r SET source = 'name_short', updated_at = NOW()
          FROM issuers i
         WHERE NOT r.manual AND r.source <> 'name_short'
           AND 'company:' || i.smartlab_ticker = r.company_id AND lower(i.name_short) = lower(r.pattern)
    """))
    conn.execute(text("""
        UPDATE brain_name_rules r SET source = 'display_name', updated_at = NOW()
          FROM issuer_aliases a JOIN brain_ticker_map m ON m.ticker = a.secid
         WHERE NOT r.manual AND r.source = 'fund_asset_name' AND a.alias_type = 'display_name'
           AND m.company_id = r.company_id AND lower(a.alias_value) = lower(r.pattern)
    """))
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


_ОКОНЧАНИЯ = "(а|я|у|ю|ом|ем|ём|е|ы|и|ов|ев|ам|ям|ами|ями|ах|ях|ой|ей|ью|ия|ии|ию|ией)?"


def _проверка_имени(pattern: str, company_id: str, verify, все_имена) -> str:
    """Регэксп проверки имени после полнотекста — общий для новостей и объявлений биржи.

    Самое длинное имя побеждает: «Газпром» не срабатывает на «Газпром нефть», «Россети» —
    на «Россети Центр»; продолжения берутся из правил других компаний, по основе (первые
    4 буквы): «Газпром нефти» и «нефтью» — одно слово. Полнотекст стеммит («Эталон»
    находит «эталонных»), поэтому проверка — целое слово с русскими окончаниями."""
    продолжения = sorted({имя[len(pattern.lower()):].strip() for имя, cid in все_имена
                          if cid != company_id and имя.startswith(pattern.lower() + " ") and len(имя) > len(pattern) + 1})
    стоп = "(?!\\s+(" + "|".join(re.escape(x.split()[0][:4]) for x in продолжения if x) + "))" if продолжения else ""
    return verify or ("\\m" + re.escape(pattern) + _ОКОНЧАНИЯ + "\\M" + стоп)


def объявления_биржи(conn, full: bool) -> int:
    """Лента объявлений МосБиржи (moex_sitenews) — всё, кроме служебного шума торгов.

    Компания — по тикерам, которые нашёл сканер (A: пишет сама биржа), и по правилам имён
    в заголовке и тексте (C). Сама Мосбиржа по имени не ставится: она издатель каждого
    объявления. Объявление без компании остаётся узлом — его находит поиск по смыслу
    («включение в индекс», «делистинг»)."""
    вод = None if full else _водяной(conn, "exchange")
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc), "шум": _БИРЖА_ШУМ}
    r = conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'exchange:' || s.id, 'exchange', CAST(s.id AS text), left(s.title, 200),
               NULLIF(left(regexp_replace(COALESCE(s.body, ''), '\\s+', ' ', 'g'), 600), ''),
               s.published_at,
               jsonb_build_object('rubric', s.rubric, 'tag', s.tag, 'tickers', to_jsonb(s.tickers),
                                  'url', 'https://www.moex.com/n' || s.id), NOW()
          FROM moex_sitenews s
         WHERE COALESCE(s.modified_at, s.created_at) > :вод AND s.title !~* :шум
        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary,
               payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    n = r.rowcount
    conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
        SELECT DISTINCT 'exchange:' || s.id, m.company_id, 'объявление_о', s.published_at, CAST(NULL AS real),
               'moex_sitenews', 'A', 'moex', CAST(s.published_at AS date)
          FROM moex_sitenews s JOIN brain_ticker_map m ON m.ticker = ANY(s.tickers)
         WHERE COALESCE(s.modified_at, s.created_at) > :вод AND s.title !~* :шум
        ON CONFLICT DO NOTHING
    """), п)
    правила = conn.execute(text(
        "SELECT pattern, company_id, exclude_regex, verify_regex FROM brain_name_rules "
        "WHERE enabled AND NOT ambiguous AND company_id <> 'company:MOEX'")).all()
    все_имена = [(x[0].lower(), x[1]) for x in conn.execute(text("SELECT pattern, company_id FROM brain_name_rules")).all()]
    for pattern, company_id, excl, verify in правила:
        conn.execute(text("""
            INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
            SELECT DISTINCT 'exchange:' || s.id, :cid, 'объявление_о', s.published_at, CAST(NULL AS real),
                   'moex_sitenews', 'C', 'имя', CAST(s.published_at AS date)
              FROM moex_sitenews s
             WHERE COALESCE(s.modified_at, s.created_at) > :вод AND s.title !~* :шум
               AND to_tsvector('russian', s.title || ' ' || COALESCE(s.body, '')) @@ phraseto_tsquery('russian', :q)
               AND regexp_replace(s.title || ' ' || COALESCE(s.body, ''), :ex, '', 'gi') ~* :vf
            ON CONFLICT DO NOTHING
        """), {**п, "q": pattern, "cid": company_id, "ex": excl or "(?!x)x",
               "vf": _проверка_имени(pattern, company_id, verify, все_имена)})
    _отметить(conn, "exchange", conn.execute(text(
        "SELECT MAX(COALESCE(modified_at, created_at)) FROM moex_sitenews")).scalar(), n)
    return n


def новости_без_компании(conn, full: bool) -> dict:
    """Новости без нашей компании — за год, только с темой рынка и «про наш рынок» (Brain/news_types.py: удары по
    инфраструктуре, налоги, ставка, сырьё, санкции…). Вадим 27.09: «давай, начинай с новостей без тикера за год». По
    ним завод найдёт похожие события — прошлые удары по НПЗ, налоги, решения ЦБ, — а у отрасли видно, что с ней было.
    Отбор — тот же, что проверен прогоном research/brain/eval_brain.py (часть 1).

    Узел — тот же вид news с меткой payload.без_компании: чистка «новостей без связей» его не трогает, разметка берёт
    тип по темам с той же проверкой «про наш рынок», отрасль — по теме. Тема перестала находиться или старше года —
    узел уходит.

    ⚠️ Первая заливка — окнами по vocab.БЕЗ_КОМПАНИИ_ОКНО дней от свежих к старым, одно окно за синк: год за раз
    (≈57 тыс. новостей через регэкспы тем плюс разметка) упёрся бы в лимит оркестратора (10 минут) и откатывался бы
    целиком каждый прогон. Свежие новости идут по водяному знаку импорта, как у остальных источников."""
    п: dict = {"дней": vocab.БЕЗ_КОМПАНИИ_ДНЕЙ}
    чисто = vocab.sql_чисто("n.text", п)
    заголовок, суть, теги, шум = _текст_новости_sql(п)
    тема = nt.sql_type("n", п, "нк")
    годна = nt.sql_relevant("n", п, "нк")
    сейчас = conn.execute(text("SELECT NOW()")).scalar()
    год_назад = сейчас - timedelta(days=vocab.БЕЗ_КОМПАНИИ_ДНЕЙ)
    далеко = datetime(2000, 1, 1, tzinfo=timezone.utc)
    if full:
        вод, лево, право, окно = далеко, далеко, далеко, "весь год"
    else:
        вод = _водяной(conn, "news_tickerless") or сейчас   # первый прогон: свежее придёт первым окном заливки
        граница = _водяной(conn, "news_tickerless_back") or сейчас
        if граница > год_назад:
            право, лево = граница, max(год_назад, граница - timedelta(days=vocab.БЕЗ_КОМПАНИИ_ОКНО))
            окно = f"{лево:%d.%m.%Y}–{право:%d.%m.%Y}"
        else:
            право = лево = далеко
            окно = "заливка года закончена"
    п.update({"вод": вод, "лево": лево, "право": право})
    r = conn.execute(text(f"""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT DISTINCT ON (1) 'news:' || {_КАНАЛ} || '/' || message_id, 'news', {_КАНАЛ} || '/' || message_id,
               {заголовок}, {суть}, posted_at,
               jsonb_build_object('channel', {_КАНАЛ}, 'views', views, 'tickers', to_jsonb(tickers),
                                  'hashtags', to_jsonb({теги}), 'url', {_URL}, 'без_компании', TRUE), NOW()
          FROM news_archive n
          CROSS JOIN LATERAL (SELECT {чисто} AS ч) x
         WHERE n.posted_at > NOW() - make_interval(days => :дней)
           AND (n.imported_at > :вод OR (n.posted_at >= :лево AND n.posted_at < :право))
           AND NOT EXISTS (SELECT 1 FROM brain_ticker_map m WHERE m.ticker = ANY(n.tickers))
           AND NOT {шум} AND {годна} AND ({тема}) IS NOT NULL
         ORDER BY 1, n.imported_at DESC
        ON CONFLICT (id) DO NOTHING
    """), п)
    n = r.rowcount
    _отметить(conn, "news_tickerless", conn.execute(text("SELECT MAX(imported_at) FROM news_archive")).scalar(), n)
    if full:
        _отметить(conn, "news_tickerless_back", год_назад, n)
    elif право != далеко:
        _отметить(conn, "news_tickerless_back", лево, n)
    # Старше года — уходит (окно «за год»). Сначала связи: и ярлыки, и «из_новости» кандидатов.
    старые = ("SELECT id FROM brain_nodes WHERE kind = 'news' AND coalesce(CAST(payload ->> 'без_компании' AS boolean), FALSE)"
              " AND ts < NOW() - make_interval(days => :дней)")
    conn.execute(text(f"DELETE FROM brain_edges WHERE src IN ({старые}) OR dst IN ({старые})"), п)
    устарело = conn.execute(text(f"DELETE FROM brain_nodes WHERE id IN ({старые})"), п).rowcount
    return {"добавлено": n, "окно": окно, "устарело": устарело}


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
        проверка = _проверка_имени(pattern, company_id, verify, все_имена)
        п = {"с": с, "вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc), "q": pattern, "cid": company_id,
             "ex": excl or "(?!x)x", "vf": проверка}
        чисто = vocab.sql_чисто("n.text", п)
        заголовок, суть, теги, шум = _текст_новости_sql(п)
        conn.execute(text(f"""
            INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
            SELECT DISTINCT ON (1) 'news:' || {_КАНАЛ} || '/' || message_id, 'news', {_КАНАЛ} || '/' || message_id,
                   {заголовок}, {суть}, posted_at,
                   jsonb_build_object('channel', {_КАНАЛ}, 'views', views, 'tickers', to_jsonb(tickers),
                                      'hashtags', to_jsonb({теги}), 'url', {_URL}), NOW()
              FROM news_archive n
              CROSS JOIN LATERAL (SELECT {чисто} AS ч) x
             WHERE posted_at > :с AND imported_at > :вод
               AND to_tsvector('russian', text) @@ phraseto_tsquery('russian', :q)
               AND regexp_replace(text, :ex, '', 'gi') ~* :vf
               AND NOT {шум}   -- дайджесты, календари, котировки без слов — тот же фильтр, что у пути по хэштегу
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
               AND EXISTS (SELECT 1 FROM brain_nodes b WHERE b.id = 'news:' || {_КАНАЛ} || '/' || n.message_id)
            ON CONFLICT DO NOTHING
        """), п)
        n += r.rowcount
    # Новость с пятью и больше компаниями по имени — обзор, а не упоминание: снимаем разметку.
    conn.execute(text("""
        DELETE FROM brain_edges e USING (
            SELECT src FROM brain_edges WHERE kind = 'упоминает' AND method = 'имя' GROUP BY src HAVING COUNT(*) >= 5
        ) d WHERE e.src = d.src AND e.kind = 'упоминает' AND e.method = 'имя'
    """))
    # Решения аудита (Routine frame-brain-audit, research/brain/prompt_name_audit_routine.md):
    # связь уходит и не возвращается — ни инкрементом, ни полной пересборкой, — только если
    # «неверно» сказали ДВЕ независимые проверки или человек нажал «убрать». Одного агента
    # мало: в проверочном прогоне 10.09 единственное «неверно» из 40 было его ошибкой
    # (Sitronics «входит в АФК Систему»). «Оставить» человека сильнее любых агентов.
    conn.execute(text("""
        DELETE FROM brain_edges e USING brain_edge_reviews r
         WHERE e.src = r.src AND e.dst = r.dst AND e.kind = r.kind AND e.method = 'имя'
           AND (r.human_decision = 'убрать'
                OR (r.human_decision IS NULL AND r.verdict = 'неверно' AND r.second_verdict = 'неверно'))
    """))
    # Узлы новостей, оставшиеся без единой связи, карте не нужны. ⚠️ Рёбра разметки («тип», «отрасль») — ярлыки,
    # а не связь: без этой оговорки новость, у которой аудит снял последнюю компанию, жила бы вечно с одним «типом»
    # (27.09, после единой разметки). Сначала уходят её ярлыки, потом она сама.
    # Новость без компании (payload.без_компании, новости_без_компании()) сиротой не считается: она в мозге ради темы.
    conn.execute(text("""
        DELETE FROM brain_edges WHERE kind IN ('тип', 'отрасль') AND src IN (
            SELECT n.id FROM brain_nodes n WHERE n.kind = 'news'
               AND NOT coalesce(CAST(n.payload ->> 'без_компании' AS boolean), FALSE)
               AND NOT EXISTS (SELECT 1 FROM brain_edges e WHERE (e.src = n.id OR e.dst = n.id)
                                                            AND e.kind NOT IN ('тип', 'отрасль')))
    """))
    conn.execute(text("""
        DELETE FROM brain_nodes n WHERE n.kind = 'news'
           AND NOT coalesce(CAST(n.payload ->> 'без_компании' AS boolean), FALSE)
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
    # ⚠️ Рёбра — только из САМОГО СВЕЖЕГО снимка эмитента. Источников два (smartlab,
    # financemarker), держатели у них названы по-разному, и DISTINCT ON по держателю
    # пропускал оба снимка: у Сбера рядом стояли «Американские инвесторы 33%» (2020)
    # и «Минфин 50%» (2022). 13.09.2026 так было 317 из 708 рёбер. Свежий, но грубый
    # снимок лучше подробного устаревшего: агент называет долю текущей.
    r4 = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
        SELECT DISTINCT ON (h.company_id, i.smartlab_ticker) h.company_id, 'company:' || i.smartlab_ticker, 'владеет',
               s.structure_as_of, s.share_pct, s.source, 'B', 'акционеры', s.structure_as_of
          FROM company_shareholders s JOIN issuers i USING (issuer_id)
          JOIN (SELECT issuer_id, MAX(structure_as_of) AS d FROM company_shareholders GROUP BY issuer_id) l USING (issuer_id)
          JOIN brain_holder_map h ON h.holder_norm = brain_norm(s.holder)
         WHERE s.structure_as_of IS NOT DISTINCT FROM l.d
           AND i.smartlab_ticker IS NOT NULL AND h.status IN ('авто', 'подтверждено') AND h.company_id IS NOT NULL
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
          JOIN (SELECT issuer_id, MAX(structure_as_of) AS d FROM company_shareholders GROUP BY issuer_id) l USING (issuer_id)
          LEFT JOIN brain_holder_map h ON h.holder_norm = brain_norm(s.holder)
         WHERE s.structure_as_of IS NOT DISTINCT FROM l.d
           AND i.smartlab_ticker IS NOT NULL AND s.holder IS NOT NULL
           AND lower(trim(s.holder)) NOT IN ('прочие', 'прочее', 'free float', 'free-float', 'фри флоат', 'миноритарии')
           -- Номинальные держатели (НРД, «Депозитарии», Clearstream) — не владельцы: в раскрытии они
           -- числятся с процентом, но агент, увидев «владельцы: НРД», напишет ложь. 05.09 у Роснефти
           -- в «владельцах» стоял НРД.
           AND s.holder !~* 'депозитар|номинальн|nominee|clearstream|euroclear'
           AND NOT COALESCE(h.status IN ('авто', 'подтверждено') AND h.company_id IS NOT NULL, FALSE)
         ORDER BY md5(lower(trim(s.holder))), i.smartlab_ticker, s.structure_as_of DESC NULLS LAST
        ON CONFLICT DO NOTHING
    """))
    return {"точных": r1.rowcount, "на_проверке": r2.rowcount, "без_пары": r3.rowcount,
            "рёбер_владения": r4.rowcount, "рёбер_держателей": r5.rowcount}


# Холдинги: smart-lab кладёт АФК Систему в «Потреб. сектор», ЭсЭфАй — в «Финансы». Для агента
# это ложный контекст («потребительская компания»). Переопределяем ТОЛЬКО в мозге — issuers.sector
# и карточки на сайте не трогаем. Помечено как наша правка (уровень C), а не smart-lab.
ХОЛДИНГИ = ("AFKS", "SFIN")


def секторы(conn) -> int:
    п = {"h": ",".join(ХОЛДИНГИ)}
    conn.execute(text("""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'sector:' || md5(s), 'sector', md5(s), s, CAST(NULL AS text), CAST(NULL AS timestamptz),
               jsonb_build_object('компаний', COUNT(*), 'классификация', CASE WHEN s = 'Холдинги' THEN 'наша правка' ELSE 'smart-lab' END), NOW()
          FROM (SELECT CASE WHEN smartlab_ticker = ANY(string_to_array(:h, ',')) THEN 'Холдинги' ELSE sector END AS s
                  FROM issuers WHERE sector IS NOT NULL AND smartlab_ticker IS NOT NULL) x
         GROUP BY s
        ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
    """), п)
    conn.execute(text("DELETE FROM brain_edges WHERE kind = 'в_секторе'"))
    r = conn.execute(text("""
        INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date)
        SELECT 'company:' || smartlab_ticker,
               'sector:' || md5(CASE WHEN smartlab_ticker = ANY(string_to_array(:h, ',')) THEN 'Холдинги' ELSE sector END),
               'в_секторе', updated_at, CAST(NULL AS real), 'issuers',
               CASE WHEN smartlab_ticker = ANY(string_to_array(:h, ',')) THEN 'C' ELSE 'B' END,
               CASE WHEN smartlab_ticker = ANY(string_to_array(:h, ',')) THEN 'наша_правка_холдинг' ELSE 'классификация_smartlab' END,
               updated_at::date
          FROM issuers WHERE sector IS NOT NULL AND smartlab_ticker IS NOT NULL
        ON CONFLICT DO NOTHING
    """), п)
    # Осиротевшие узлы секторов (если у сектора не осталось компаний) — убрать.
    conn.execute(text("DELETE FROM brain_nodes n WHERE n.kind = 'sector' AND NOT EXISTS (SELECT 1 FROM brain_edges e WHERE e.dst = n.id)"))
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


# ── ярлыки новостей: тип события и роль компании ──────────────────────────────────
# ⚠️ Зачем (Вадим 10.09). Новость в карте была просто «упоминает компанию»: без типа
# события и без различия «новость про неё» / «названа мимоходом». Правило Вадима — всё
# автоматически и с фильтром: сначала правила по хэштегам и ключевым словам (замер на
# 4 510 новостях за 90 дней: 47 % размечаются), остаток — ночной агент (режим labels
# аудита). Только за 90 дней — это окно, которое видит писатель; старый архив не нужен.
#
# ⚠️ Ярлыки — в своей таблице, а не в payload узла: новости() перезаписывает payload при
# повторном импорте, и разметка агента пропала бы. Порядок правил — первое совпадение.
_ЯРЛЫКИ_ДНЕЙ = 90
# Типы — из единого словаря (Brain/vocab.py): события компании, затем темы рынка (список Вадима 27.09). Тот же
# список у агента (api/brain_core.py) и у разметки всех видов узлов (разметка()).
_ТИПЫ_НОВОСТЕЙ = vocab.ТИПЫ
_ЯРЛЫКИ_ГОЛОВА = 150   # символов начала текста: компания в них — «главная», иначе «упоминание»


def ярлыки_темы_однократно(conn) -> int:
    """Один раз после появления тем рынка (27.09.2026): «прочее», которое ставил ночной агент, пока у него были только
    события компании, возвращается ему на разметку — «экспорт СПГ в Китай +43%», «ОПЕК: экспорт нефти в Индию» теперь
    имеют тему. Замер на 860 таких новостях: правилами тема нашлась бы у 290, но агент читает текст и ошибается реже
    (правила уводили «встречу Путина с Грефом» в геополитику). Отметка в brain_sync_state — повторно не сработает."""
    if _водяной(conn, "labels_topics_v1") is not None:
        return 0
    r = conn.execute(text("""
        UPDATE brain_news_labels l SET тип = NULL, тип_источник = NULL, updated_at = NOW()
          FROM brain_nodes b
         WHERE b.id = l.node_id AND b.ts > NOW() - INTERVAL '90 days'
           AND l.тип = 'прочее' AND l.тип_источник = 'агент'
    """))
    _отметить(conn, "labels_topics_v1", datetime.now(timezone.utc), r.rowcount)
    return r.rowcount


def ярлыки_новостей(conn, full: bool, сброс: bool = False) -> int:
    """Тип события и роль компании у новостей за 90 дней — правилами, по одному разу на
    новость. Тип, который правила не нашли, остаётся пустым — его ставит ночной агент.

    Тип — тем же правилом, что у разметки всех узлов (vocab.sql_тип: сначала хэштеги автора,
    потом горячие слова), чтобы ярлык и тег-ребро не расходились. сброс — сменилась версия
    словаря: типы правил пересчитываются, ответы агента и людей остаются.

    Роль: компания одна — «главная»; несколько — «главная» та, чьё имя или тикер-хэштег
    стоит в первых 150 символах, остальные — «упоминание». Роль копируется на связь
    «упоминает», чтобы обход карты мог отличить новость про компанию от перечня."""
    if сброс:
        conn.execute(text("DELETE FROM brain_news_labels WHERE тип_источник IS NULL OR тип_источник = 'правило'"))
    п = {"с": datetime.now(timezone.utc) - timedelta(days=_ЯРЛЫКИ_ДНЕЙ), "голова": _ЯРЛЫКИ_ГОЛОВА}
    # Регэкспы и теги — параметрами: без экранирования в тексте запроса и без «%».
    по_тегам, по_словам = vocab.sql_тип(vocab.sql_теги("a.hashtags"), "a.text", п)
    r = conn.execute(text(f"""
        INSERT INTO brain_news_labels (node_id, тип, тип_источник, роли, роли_источник, updated_at)
        SELECT b.id, x.тип, CASE WHEN x.тип IS NOT NULL THEN 'правило' END, x.роли, 'правило', NOW()
          FROM brain_nodes b
          JOIN news_archive a ON a.message_id = CAST(split_part(b.id, '/', 2) AS bigint)
               AND a.channel IN (split_part(substr(b.id, 6), '/', 1),
                                 CASE split_part(substr(b.id, 6), '/', 1) WHEN 'markettwits' THEN 'MarketTwits'
                                      WHEN 'newssmartlab' THEN 'СМАРТЛАБ НОВОСТИ' END)
          CROSS JOIN LATERAL (
              SELECT (SELECT {vocab.sql_правило('y.пт', 'y.пс', п)} FROM (SELECT {по_тегам} AS пт, {по_словам} AS пс) y) AS тип,
                     (SELECT jsonb_object_agg(e.dst,
                               CASE WHEN cnt.n = 1
                                      OR EXISTS (SELECT 1 FROM brain_name_rules x
                                                  WHERE x.company_id = e.dst AND x.enabled AND NOT x.ambiguous
                                                    AND strpos(lower(left(a.text, :голова)), lower(x.pattern)) > 0)
                                      OR EXISTS (SELECT 1 FROM brain_ticker_map m
                                                  WHERE m.company_id = e.dst
                                                    AND strpos(upper(left(a.text, :голова)), '#' || m.ticker) > 0)
                                    THEN 'главная' ELSE 'упоминание' END)
                        FROM brain_edges e
                        CROSS JOIN (SELECT COUNT(*) AS n FROM brain_edges e2
                                     WHERE e2.src = b.id AND e2.kind = 'упоминает') cnt
                       WHERE e.src = b.id AND e.kind = 'упоминает') AS роли) x
         WHERE b.kind = 'news' AND b.ts > :с
           AND NOT EXISTS (SELECT 1 FROM brain_news_labels l WHERE l.node_id = b.id)
        ON CONFLICT (node_id) DO NOTHING
    """), п)
    # Роль — на связь «упоминает» (источник истины — таблица ярлыков).
    conn.execute(text("""
        UPDATE brain_edges e SET role = l.роли ->> e.dst
          FROM brain_news_labels l
         WHERE e.src = l.node_id AND e.kind = 'упоминает'
           AND (l.роли ->> e.dst) IS NOT NULL AND e.role IS DISTINCT FROM (l.роли ->> e.dst)
    """))
    # Ярлыки новостей, которых в карте больше нет, не копим.
    conn.execute(text("""
        DELETE FROM brain_news_labels l WHERE NOT EXISTS (SELECT 1 FROM brain_nodes b WHERE b.id = l.node_id)
    """))
    return r.rowcount


# ── единая разметка: тип и отрасль у каждого события, из любого источника ─────────
# Вадим 27.09.2026: «то, что лежит во втором мозге, — отфильтрованная и подготовленная информация, со всех источников
# один и тот же формат… пополняется и фильтруется каждый раз». Словарь и правила — Brain/vocab.py. Шаг идёт каждым
# синком (раз в 15 минут): свежий узел размечается сразу, ответ ночного агента подхватывается следующим прогоном,
# новая версия словаря (vocab.ВЕРСИЯ) — одна полная переразметка и одна чистка уже лежащих узлов.
_НОВОСТЬ_АРХИВ = """a.message_id = CAST(NULLIF(split_part(b.id, '/', 2), '') AS bigint)
               AND a.channel IN (split_part(substr(b.id, 6), '/', 1),
                                 CASE split_part(substr(b.id, 6), '/', 1) WHEN 'markettwits' THEN 'MarketTwits'
                                      WHEN 'newssmartlab' THEN 'СМАРТЛАБ НОВОСТИ' END)"""


def чистка_однократно(conn) -> dict:
    """Сменилась версия словаря: уже лежащие узлы — в новый вид. У новостей, кандидатов и сигналов — чистый заголовок
    и суть (без хэштегов, эмодзи, ссылок; хэштеги — в payload и теги-рёбра). Шум — дайджесты, календари, котировки
    без слов — уходит из мозга вместе со связями (вектора — каскадом, ярлыки чистит ярлыки_новостей). Новые узлы
    такими рождаются сами (новости(), новости_по_имени(), кандидаты(), сигналы())."""
    п: dict = {}
    чисто = vocab.sql_чисто("a.text", п)
    заголовок = vocab.sql_обрезать("x.ч", vocab.ЗАГОЛОВОК, п)
    суть = f"CASE WHEN length(x.ч) > {vocab.ЗАГОЛОВОК} THEN {vocab.sql_обрезать('x.ч', vocab.СУТЬ, п)} END"
    шум = vocab.sql_шум("a.text", "x.ч", п)
    conn.execute(text("DROP TABLE IF EXISTS _шум"))
    п["бшум"] = _БИРЖА_ШУМ
    conn.execute(text(f"""
        CREATE TEMP TABLE _шум AS
        SELECT DISTINCT b.id FROM brain_nodes b
          JOIN news_archive a ON {_НОВОСТЬ_АРХИВ}
          CROSS JOIN LATERAL (SELECT {чисто} AS ч) x
         WHERE b.kind = 'news' AND {шум}
        UNION
        SELECT id FROM brain_nodes WHERE kind = 'exchange' AND title ~* :бшум   -- служебное биржи, пришедшее до фильтра
    """), п)
    рёбер = conn.execute(text("DELETE FROM brain_edges WHERE src IN (SELECT id FROM _шум)")).rowcount
    рёбер += conn.execute(text("DELETE FROM brain_edges WHERE dst IN (SELECT id FROM _шум)")).rowcount
    шума = conn.execute(text("DELETE FROM brain_nodes WHERE id IN (SELECT id FROM _шум)")).rowcount
    новостей = conn.execute(text(f"""
        UPDATE brain_nodes b SET title = {заголовок}, summary = {суть},
               payload = COALESCE(b.payload, jsonb_build_object()) || jsonb_build_object('hashtags', to_jsonb({vocab.sql_теги('a.hashtags')})),
               updated_at = NOW()
          FROM news_archive a CROSS JOIN LATERAL (SELECT {чисто} AS ч) x
         WHERE b.kind = 'news' AND {_НОВОСТЬ_АРХИВ}
    """), п).rowcount
    пк: dict = {}
    кандидатов = conn.execute(text(f"""
        UPDATE brain_nodes b SET title = {_заголовок_кандидата(пк)}, summary = {_суть_кандидата(пк)}, updated_at = NOW()
          FROM content_candidates c WHERE b.id = 'candidate:' || c.id
    """), пк).rowcount
    пс: dict = {}
    заголовок_с = vocab.sql_обрезать(vocab.sql_чисто("s.snippet", пс), vocab.ЗАГОЛОВОК, пс)
    сигналов = conn.execute(text(f"""
        UPDATE brain_nodes b SET title = COALESCE(NULLIF({заголовок_с}, ''), '(без текста)'), updated_at = NOW()
          FROM ownership_signals s WHERE b.id = 'signal:' || s.id
    """), пс).rowcount
    return {"шума_убрано": шума, "рёбер_шума": рёбер, "новостей": новостей, "кандидатов": кандидатов,
            "сигналов": сигналов}


def _разница(conn, kind: str, желаемое: str, где_старое: str, п: dict, уровень: str, способ: str, роль: str) -> tuple:
    """Рёбра вида kind привести к желаемому набору (temp-таблица с колонками id, dst, способ, kind): лишнее — удалить,
    недостающее — вставить, способ/уровень — обновить. У изменившихся узлов сдвигается updated_at — вектор с новыми
    тегами пересчитает brain_embed. Возвращает (убрано, поставлено)."""
    r = conn.execute(text(f"""
        WITH del AS (DELETE FROM brain_edges e WHERE e.kind = :вид AND {где_старое}
                        AND NOT EXISTS (SELECT 1 FROM {желаемое} j WHERE j.id = e.src AND j.dst = e.dst)
                     RETURNING e.src),
             ins AS (INSERT INTO brain_edges (src, dst, kind, ts, weight, source, level, method, snapshot_date, role)
                     SELECT j.id, j.dst, :вид, b.ts, CAST(NULL AS real), 'разметка', {уровень}, {способ},
                            CAST(b.ts AS date), {роль}
                       FROM {желаемое} j JOIN brain_nodes b ON b.id = j.id
                     ON CONFLICT (src, dst, kind) DO UPDATE SET level = EXCLUDED.level, method = EXCLUDED.method
                      WHERE brain_edges.level IS DISTINCT FROM EXCLUDED.level
                         OR brain_edges.method IS DISTINCT FROM EXCLUDED.method
                     RETURNING src),
             upd AS (UPDATE brain_nodes SET updated_at = NOW()
                      WHERE id IN (SELECT src FROM del UNION SELECT src FROM ins) RETURNING 1)
        SELECT (SELECT COUNT(*) FROM del), (SELECT COUNT(*) FROM ins), (SELECT COUNT(*) FROM upd)
    """), {**п, "вид": kind}).first()
    return int(r[0]), int(r[1])


def _итог_разметки(conn, в_работе: str, п: dict, с_агентом: bool = True) -> None:
    """Temp-таблицы _разм (текст и хэштеги узла) и _итог (тип и способ) для узлов, отобранных условием в_работе
    (по alias b = brain_nodes). Общая для разметки и прогона на эталоне (research/brain/eval_brain.py): эталон
    проверяет ровно то, что работает в синке. с_агентом=False — только правила, без ответов ночного агента."""
    п.setdefault("типы", [t for t, _, _ in vocab.ТИПЫ])
    # текст и хэштеги узлов в работе — по видам, одной таблицей
    шаг_а = "CASE c.event_type " + " ".join(
        f"WHEN CAST(:ша{i} AS text) THEN CAST(:шт{i} AS text)" for i in range(len(vocab.ТИП_ШАГА_А))) + " END"
    for i, (k, v) in enumerate(vocab.ТИП_ШАГА_А.items()):
        п[f"ша{i}"], п[f"шт{i}"] = k, v
    раскрытие = "CASE b.payload ->> 'category' " + " ".join(
        f"WHEN CAST(:рк{i} AS text) THEN CAST(:рт{i} AS text)" for i in range(len(vocab.ТИП_РАСКРЫТИЯ))) + " END"
    for i, (k, v) in enumerate(vocab.ТИП_РАСКРЫТИЯ.items()):
        п[f"рк{i}"], п[f"рт{i}"] = k, v
    от_вида = "CASE b.kind " + " ".join(
        f"WHEN CAST(:вк{i} AS text) THEN CAST(:вт{i} AS text)" for i in range(len(vocab.ТИП_ВИДА))) + f" ELSE {раскрытие} END"
    for i, (k, v) in enumerate(vocab.ТИП_ВИДА.items()):
        п[f"вк{i}"], п[f"вт{i}"] = k, v
    теги_к = vocab.sql_теги_из_текста("concat_ws(' ', c.headline, c.raw_text)", п, "тк")
    п["шум_к"] = vocab.ШУМ
    теги_с = vocab.sql_теги_из_текста("s.snippet", п, "тс")
    conn.execute(text("DROP TABLE IF EXISTS _разм"))
    conn.execute(text(f"""
        CREATE TEMP TABLE _разм AS
        (SELECT DISTINCT ON (b.id) b.id, b.kind, a.text AS текст, {vocab.sql_теги('a.hashtags')} AS теги,
                CAST(NULL AS text) AS от_источника,
                -- новость без компании: тип — по темам с проверкой «про наш рынок» (тот же отбор, что пустил её в мозг)
                CASE WHEN coalesce(CAST(b.payload ->> 'без_компании' AS boolean), FALSE)
                          AND NOT EXISTS (SELECT 1 FROM brain_edges e WHERE e.src = b.id AND e.kind = 'упоминает')
                     THEN {nt.sql_type('a', п, 'нт')} END AS тип_темы
           FROM brain_nodes b JOIN news_archive a ON {_НОВОСТЬ_АРХИВ}
          WHERE b.kind = 'news' AND {в_работе}
          ORDER BY b.id)
        UNION ALL
        -- Кандидат-дайджест («КАЛЕНДАРЬ НА СЕГОДНЯ») типа не получает: тот же шум, что не пускается в мозг у новостей.
        -- Находка и связка — карточка с данными, а не новость: слова в ней («ОФЗ», «доходность» в контексте) давали
        -- случайный тип (прогон 27.09). Их вид — в payload.event_type (insight_positions, combo_…).
        SELECT b.id, b.kind,
               CASE WHEN left(concat_ws(' ', c.headline, c.raw_text), 160) !~* :шум_к
                         AND c.source NOT IN ('insight', 'combo')
                    THEN concat_ws(' ', c.headline, c.annotation, left(c.raw_text, 1500)) END,
               CASE WHEN left(concat_ws(' ', c.headline, c.raw_text), 160) !~* :шум_к
                         AND c.source NOT IN ('insight', 'combo') THEN {теги_к}
                    ELSE CAST(ARRAY[] AS text[]) END, {шаг_а}, CAST(NULL AS text)
          FROM brain_nodes b JOIN content_candidates c ON b.id = 'candidate:' || c.id
         WHERE b.kind IN ('candidate', 'post') AND {в_работе}
        UNION ALL
        SELECT b.id, b.kind, concat_ws(' ', s.snippet, b.summary), {теги_с}, CAST(NULL AS text), CAST(NULL AS text)
          FROM brain_nodes b JOIN ownership_signals s ON b.id = 'signal:' || s.id
         WHERE b.kind = 'signal' AND {в_работе}
        UNION ALL
        -- У раскрытия слова — только по заголовку: там тип сообщения FinanceMarker («Проведение заседания совета
        -- директоров»), а в повестке — всё подряд («бюджет», «инсайдер»), и тип по ней выходил случайным.
        SELECT b.id, b.kind, CASE WHEN b.kind = 'disclosure' THEN b.title ELSE concat_ws(' ', b.title, b.summary) END,
               CAST(ARRAY[] AS text[]), {от_вида}, CAST(NULL AS text)
          FROM brain_nodes b
         WHERE b.kind IN ('exchange', 'disclosure', 'report', 'index_event') AND {в_работе}
    """), п)
    # тип: источник (где он главнее) → хэштеги → слова → агент → тип Шага А
    по_тегам, по_словам = vocab.sql_тип("r0.теги", "r0.текст", п)
    слова_за = vocab.sql_слова_за("r.по_тегам", "r.текст", п)
    правило = vocab.sql_правило("r1.по_тегам", "r1.по_словам", п)
    теги_темы = vocab.sql_теги_за("r.тип_темы", "r.теги", п)
    слова_темы = vocab.sql_слова_за("r.тип_темы", "r.текст", п)
    conn.execute(text("DROP TABLE IF EXISTS _итог"))
    conn.execute(text(f"""
        CREATE TEMP TABLE _итог AS
        WITH r1 AS (SELECT r0.id, r0.kind, r0.текст, r0.от_источника, r0.тип_темы, r0.теги,
                           {по_тегам} AS по_тегам, {по_словам} AS по_словам
                      FROM _разм r0),
             r AS (SELECT r1.*, {правило} AS правило FROM r1),
             аг AS (SELECT node_id, тип FROM brain_news_labels
                     WHERE CAST(:с_агентом AS boolean) AND тип = ANY(CAST(:типы AS text[]))
                       AND тип_источник IS NOT NULL AND тип_источник <> 'правило')
        SELECT r.id, r.kind, r.тип_темы,
               CASE WHEN r.тип_темы IS NOT NULL THEN r.тип_темы
                    WHEN r.kind = ANY(CAST(:главнее AS text[])) AND r.от_источника IS NOT NULL THEN r.от_источника
                    ELSE COALESCE(r.правило, аг.тип, r.от_источника) END AS тип,
               CASE WHEN r.тип_темы IS NOT NULL
                         THEN CASE WHEN {теги_темы} THEN CASE WHEN {слова_темы} THEN 'хэштег+слова' ELSE 'хэштег' END
                                   ELSE 'слова' END
                    WHEN r.kind = ANY(CAST(:главнее AS text[])) AND r.от_источника IS NOT NULL THEN 'источник'
                    WHEN r.правило IS NOT NULL AND r.правило = r.по_тегам
                         THEN CASE WHEN {слова_за} THEN 'хэштег+слова' ELSE 'хэштег' END
                    WHEN r.правило IS NOT NULL THEN 'слова'
                    WHEN аг.тип IS NOT NULL THEN 'агент'
                    WHEN r.от_источника IS NOT NULL THEN 'источник' END AS способ
          FROM r LEFT JOIN аг ON аг.node_id = r.id
    """), {**п, "главнее": list(vocab.ИСТОЧНИК_ГЛАВНЕЕ), "с_агентом": с_агентом})


def разметка(conn, полный: bool) -> dict:
    """Тип события и отрасль — у каждого узла-события, из любого источника (Brain/vocab.py).

    Тип: у новости — хэштеги автора и слова (полный текст из news_archive); у кандидата — заголовок, аннотация и
    исходный текст (хэштеги вынимаются из текста), а если правила молчат — тип Шага А; у раскрытия — категория
    FinanceMarker, иначе слова; у объявления биржи и сигнала — слова; у отчёта и события индекса — сам источник.
    Правила молчат — ответ ночного агента (brain_news_labels); нет и его — узел без типа ждёт агента (90 дней).

    Отрасль: сектор компании, про которую событие. У новости — только «главной» (роли есть за 90 дней, у старых —
    если компаний одна-две): перечень из десяти компаний отраслей не даёт.

    Инкремент: узлы, изменившиеся с прошлого прогона, и узлы с новым ответом агента; отрасль сверяется целиком
    (это join по рёбрам — доли секунды). Рёбра меняются разницей, а не пересборкой: 15-минутный синк не должен
    переписывать десятки тысяч строк."""
    вод = None if полный else _водяной(conn, vocab.ВЕРСИЯ)
    типы = [t for t, _, _ in vocab.ТИПЫ]
    п = {"вод": вод or datetime(2000, 1, 1, tzinfo=timezone.utc), "полный": полный, "типы": типы}
    # 1) узлы-теги словаря; тип, убранный из словаря, уходит вместе со своими рёбрами
    conn.execute(text(f"""
        INSERT INTO brain_nodes (id, kind, key, title, summary, ts, payload, updated_at)
        SELECT 'tag:тип/' || {vocab.sql_ключ_тега('u.t')}, 'tag', 'тип/' || {vocab.sql_ключ_тега('u.t')}, u.t,
               CAST(NULL AS text), CAST(NULL AS timestamptz),
               jsonb_build_object('грань', 'тип', 'порядок', u.o, 'версия', CAST(:версия AS text)), NOW()
          FROM unnest(CAST(:типы AS text[])) WITH ORDINALITY AS u(t, o)
        ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
         WHERE brain_nodes.payload IS DISTINCT FROM EXCLUDED.payload
    """), {**п, "версия": vocab.ВЕРСИЯ})
    conn.execute(text(f"""
        DELETE FROM brain_edges WHERE kind = 'тип'
           AND NOT (dst = ANY(ARRAY(SELECT 'tag:тип/' || {vocab.sql_ключ_тега('t')} FROM unnest(CAST(:типы AS text[])) t)))
    """), п)
    conn.execute(text(f"""
        DELETE FROM brain_nodes WHERE kind = 'tag' AND starts_with(id, 'tag:тип/')
           AND NOT (id = ANY(ARRAY(SELECT 'tag:тип/' || {vocab.sql_ключ_тега('t')} FROM unnest(CAST(:типы AS text[])) t)))
    """), п)
    в_работе = ("(CAST(:полный AS boolean) OR b.updated_at > :вод OR EXISTS (SELECT 1 FROM brain_news_labels l"
                " WHERE l.node_id = b.id AND l.updated_at > :вод))")
    _итог_разметки(conn, в_работе, п)
    # Новость без компании, у которой тема больше не находится (правила поменялись), в мозге не нужна.
    conn.execute(text("DROP TABLE IF EXISTS _без_темы"))
    conn.execute(text("""
        CREATE TEMP TABLE _без_темы AS
        SELECT t.id FROM _итог t JOIN brain_nodes b ON b.id = t.id
         WHERE t.kind = 'news' AND t.тип_темы IS NULL AND coalesce(CAST(b.payload ->> 'без_компании' AS boolean), FALSE)
           AND NOT EXISTS (SELECT 1 FROM brain_edges e WHERE e.src = t.id AND e.kind = 'упоминает')
    """))
    без_темы = conn.execute(text("SELECT COUNT(*) FROM _без_темы")).scalar() or 0
    if без_темы:
        conn.execute(text("DELETE FROM brain_edges WHERE src IN (SELECT id FROM _без_темы) OR dst IN (SELECT id FROM _без_темы)"))
        conn.execute(text("DELETE FROM brain_nodes WHERE id IN (SELECT id FROM _без_темы)"))
        conn.execute(text("DELETE FROM _итог WHERE id IN (SELECT id FROM _без_темы)"))
    conn.execute(text("DROP TABLE IF EXISTS _тип"))
    conn.execute(text(f"""
        CREATE TEMP TABLE _тип AS SELECT id, kind, 'tag:тип/' || {vocab.sql_ключ_тега('тип')} AS dst, способ
          FROM _итог WHERE тип IS NOT NULL
    """))
    уровень = ("CASE j.способ WHEN 'источник' THEN CASE j.kind "
               + " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in vocab.УРОВЕНЬ_ИСТОЧНИКА.items()) + " ELSE 'C' END "
               + " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in vocab.УРОВЕНЬ.items()) + " ELSE 'C' END")
    тип_убрано, тип_поставлено = _разница(conn, "тип", "_тип", "e.src IN (SELECT id FROM _итог)", п, уровень,
                                          "j.способ", "'главный'")
    # 4) очередь агенту: объявления биржи, раскрытия, сигналы за 90 дней без типа (у новостей её ведёт
    #    ярлыки_новостей). Тип правил тоже пишется — чтобы в очереди был ровно остаток.
    conn.execute(text("""
        INSERT INTO brain_news_labels (node_id, тип, тип_источник, роли, роли_источник, updated_at)
        SELECT t.id, t.тип, CASE WHEN t.тип IS NOT NULL THEN 'правило' END, CAST(NULL AS jsonb), CAST(NULL AS text), NOW()
          FROM _итог t JOIN brain_nodes b ON b.id = t.id
         WHERE t.kind = ANY(CAST(:агенту AS text[])) AND t.kind <> 'news' AND b.ts > NOW() - INTERVAL '90 days'
           AND t.способ IS DISTINCT FROM 'агент'
        ON CONFLICT (node_id) DO UPDATE SET тип = EXCLUDED.тип, тип_источник = EXCLUDED.тип_источник, updated_at = NOW()
         WHERE (brain_news_labels.тип_источник IS NULL OR brain_news_labels.тип_источник = 'правило')
           AND brain_news_labels.тип IS DISTINCT FROM EXCLUDED.тип
    """), {"агенту": list(vocab.ВИДЫ_АГЕНТУ)})
    # 5) отрасль: сектор компании события; у новости — главной (или одной-двух, если ролей ещё нет)
    conn.execute(text("DROP TABLE IF EXISTS _отр"))
    conn.execute(text("""
        CREATE TEMP TABLE _отр AS
        WITH cnt AS (SELECT src, COUNT(*) AS n FROM brain_edges WHERE kind = 'упоминает' GROUP BY src)
        SELECT DISTINCT e.src AS id, s.dst AS dst, 'компания' AS способ
          FROM brain_edges e
          JOIN brain_nodes b ON b.id = e.src AND b.kind = ANY(CAST(:виды AS text[]))
          JOIN brain_edges s ON s.src = e.dst AND s.kind = 'в_секторе'
          LEFT JOIN cnt ON cnt.src = e.src
         WHERE e.kind = ANY(CAST(:рёбра AS text[])) AND starts_with(e.dst, 'company:')
           AND (e.kind <> 'упоминает' OR e.role = 'главная' OR (e.role IS NULL AND cnt.n <= 2))
        UNION
        -- новость без компании — отрасль по теме, где она однозначна (vocab.ТЕМА_ОТРАСЛЬ; удобрения — «Химия»)
        SELECT x.id, x.dst, 'тема' FROM (
            SELECT t.src AS id,
                   'sector:' || md5(CASE WHEN t.dst = :тег_металлы AND concat_ws(' ', b.title, b.summary) ~* :удобрения
                                          THEN :химия ELSE m.отрасль END) AS dst
              FROM brain_edges t
              JOIN unnest(CAST(:теги_отр AS text[]), CAST(:отрасли AS text[])) AS m(tag, отрасль) ON m.tag = t.dst
              JOIN brain_nodes b ON b.id = t.src
             WHERE t.kind = 'тип' AND b.kind = 'news' AND coalesce(CAST(b.payload ->> 'без_компании' AS boolean), FALSE)
        ) x JOIN brain_nodes s ON s.id = x.dst
    """), {"виды": list(vocab.ВИДЫ_С_ОТРАСЛЬЮ), "рёбра": list(vocab.РЁБРА_К_КОМПАНИИ),
           "теги_отр": ["tag:тип/" + vocab.ключ_тега(t) for t in vocab.ТЕМА_ОТРАСЛЬ],
           "отрасли": list(vocab.ТЕМА_ОТРАСЛЬ.values()),
           "тег_металлы": "tag:тип/" + vocab.ключ_тега("металлы и удобрения"),
           "удобрения": vocab.УДОБРЕНИЯ[1], "химия": vocab.УДОБРЕНИЯ[0]})
    отр_убрано, отр_поставлено = _разница(conn, "отрасль", "_отр", "TRUE", {}, "CASE j.способ WHEN 'тема' THEN 'C' ELSE 'B' END",
                                          "j.способ", "CAST(NULL AS text)")
    всего, с_типом = conn.execute(text("SELECT COUNT(*), COUNT(тип) FROM _итог")).first()
    # Водяной знак — с запасом в 10 минут: ответ агента, записанный, пока шёл синк, не потеряется.
    _отметить(conn, vocab.ВЕРСИЯ, conn.execute(text("SELECT NOW() - INTERVAL '10 minutes'")).scalar(), int(всего))
    return {"в_работе": int(всего), "с_типом": int(с_типом), "тип_убрано": тип_убрано, "тип_поставлено": тип_поставлено,
            "отрасль_убрано": отр_убрано, "отрасль_поставлено": отр_поставлено, "без_темы_убрано": int(без_темы)}


def классифицировать(conn, ids: list, с_агентом: bool = False) -> dict:
    """Тип по правилам разметки для списка узлов — для прогона на эталоне. Узел, которого нет или который шум
    (новость-дайджест, котировка без слов, служебное биржи), — None: в мозг он не попадёт. Пишет только temp-таблицы."""
    п: dict = {"ids": list(ids)}
    _итог_разметки(conn, "b.id = ANY(CAST(:ids AS text[]))", п, с_агентом=с_агентом)
    итог = {r[0]: (r[1], r[2]) for r in conn.execute(text("SELECT id, тип, способ FROM _итог")).all()}
    пш: dict = {"ids": list(ids), "бшум": _БИРЖА_ШУМ}
    чисто = vocab.sql_чисто("a.text", пш)
    шум = {r[0] for r in conn.execute(text(f"""
        SELECT b.id FROM brain_nodes b JOIN news_archive a ON {_НОВОСТЬ_АРХИВ}
          CROSS JOIN LATERAL (SELECT {чисто} AS ч) x
         WHERE b.id = ANY(CAST(:ids AS text[])) AND b.kind = 'news' AND {vocab.sql_шум("a.text", "x.ч", пш)}
        UNION
        SELECT id FROM brain_nodes WHERE id = ANY(CAST(:ids AS text[])) AND kind = 'exchange' AND title ~* :бшум
    """), пш).all()}
    return {i: (None if i in шум else итог.get(i, (None, None))[0]) for i in ids}


def таблицы_аудита(conn) -> None:
    """Таблицы аудита разметки по имени — зеркало db/migrations/090 (идемпотентно):
    синк создаёт их сам, миграции на проде руками не применяются."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS brain_edge_reviews (
            src TEXT NOT NULL, dst TEXT NOT NULL, kind TEXT NOT NULL DEFAULT 'упоминает',
            verdict TEXT NOT NULL CHECK (verdict IN ('верно', 'неверно', 'неясно')),
            reason TEXT, reviewer TEXT NOT NULL DEFAULT 'routine', batch TEXT,
            reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (src, dst, kind))
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS brain_rule_proposals (
            id BIGSERIAL PRIMARY KEY, company_id TEXT NOT NULL, exclude_regex TEXT NOT NULL,
            examples JSONB, reason TEXT, status TEXT NOT NULL DEFAULT 'на_проверке',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), decided_at TIMESTAMPTZ)
    """))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_brain_edge_reviews_when ON brain_edge_reviews (reviewed_at DESC)"))
    # Второе мнение и решение человека — зеркало db/migrations/091.
    for колонка in ("second_verdict TEXT CHECK (second_verdict IN ('верно', 'неверно', 'неясно'))",
                    "second_reason TEXT", "second_at TIMESTAMPTZ",
                    "human_decision TEXT CHECK (human_decision IN ('убрать', 'оставить'))",
                    "human_at TIMESTAMPTZ"):
        conn.execute(text(f"ALTER TABLE brain_edge_reviews ADD COLUMN IF NOT EXISTS {колонка}"))
    # Ярлыки новостей и роль компании на связи — зеркало db/migrations/092.
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS brain_news_labels (
            node_id TEXT PRIMARY KEY, тип TEXT, тип_источник TEXT,
            роли JSONB, роли_источник TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())
    """))
    conn.execute(text("ALTER TABLE brain_edges ADD COLUMN IF NOT EXISTS role TEXT"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="пересобрать всё, игнорируя водяные знаки")
    args = ap.parse_args()
    t0 = time.time()
    eng = create_engine(DB_URL)
    итог = {}
    with eng.begin() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '600s'"))
        # Один синк за раз: ручной прогон и прогон оркестратора, пошедшие вместе, блокировали бы друг друга на тех же
        # строках до таймаута. Второй просто пропускает — через 15 минут будет следующий.
        if not conn.execute(text("SELECT pg_try_advisory_xact_lock(hashtext('brain_sync'))")).scalar():
            print(json.dumps({"пропуск": "синк мозга уже идёт"}, ensure_ascii=False))
            return 0
        таблицы_аудита(conn)
        # Новая версия словаря (или первый запуск разметки): одна чистка уже лежащих узлов и полная переразметка.
        новая_версия = _водяной(conn, vocab.ВЕРСИЯ) is None
        итог["тикеров"] = карта_тикеров(conn)
        итог["компаний"] = компании(conn)
        итог["индексов"] = индексы_узлы(conn)
        итог["новостей"], _ = новости(conn, args.full)
        итог["кандидатов"] = кандидаты(conn, args.full)
        итог["документов"] = документы(conn, args.full)
        итог["раскрытий"] = раскрытия(conn, args.full)
        итог["отчётов"] = отчёты(conn, args.full)
        итог["фонды_рёбер"] = фонды(conn)
        итог["индексы_рёбер"] = индексы(conn)
        итог["событий_индексов"] = события_индексов(conn, args.full)
        итог["событий_фондов"] = события_фондов(conn, args.full)
        итог["владение_рёбер"] = факты(conn)
        итог["аномалий"] = аномалии(conn, args.full)
        итог["сигналов"] = сигналы(conn, args.full)
        держатели_узлы(conn)
        итог["правил_имён"] = правила_имён(conn)
        итог["новостей_по_имени"] = новости_по_имени(conn, args.full)
        итог["объявлений_биржи"] = объявления_биржи(conn, args.full)
        итог["новости_без_компании"] = новости_без_компании(conn, args.full)
        if новая_версия:
            итог["чистка"] = чистка_однократно(conn)
        итог["ярлыков_новостей"] = ярлыки_новостей(conn, args.full, сброс=новая_версия)
        итог["ярлыков_агенту_заново"] = ярлыки_темы_однократно(conn)
        итог["держатели"] = держатели_резолв(conn)
        итог["секторов_рёбер"] = секторы(conn)
        итог["вместе_рёбер"] = вместе(conn)
        итог["разметка"] = разметка(conn, args.full or новая_версия)
        итог["уровней_проставлено"] = уровни(conn)
        итог["узлов"] = conn.execute(text("SELECT COUNT(*) FROM brain_nodes")).scalar()
        итог["рёбер"] = conn.execute(text("SELECT COUNT(*) FROM brain_edges")).scalar()
    итог["сек"] = round(time.time() - t0, 1)
    итог["режим"] = "полный" if args.full else "инкремент"
    print(json.dumps(итог, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
