-- Один источник на срез состава фонда: справка interfax_manual замещает vim_sdr.
--
-- Как ломалось (обнаружено 2026-09-10): у OBLG (fund_id=12000) одна и та же справка
-- о СЧА ВИМ лежит дважды на 31 дату 2023-10-31…2026-04-30 — старым скрейпером
-- (vim_sdr) и парсером parse_scha (interfax_manual). interfax залили без --replace,
-- поэтому manual_scha_backfill не вычистил vim_sdr. Читатели фильтруют
-- source = ANY(MONTHLY_SOURCES) по одной дате и видят обе копии: карточка фонда
-- /api/funds/detail/12000 отдавала 135 строк на 70 ISIN, сумма долей 184 %.
-- /fund-trades (только акционерные фонды) облигационный OBLG не показывает.
--
-- Почему остаётся interfax_manual, а не «сумма долей ближе к 100» (правило мозга, #1474):
--   vim_sdr у OBLG битый — баг ×1000 в штуках (Funds/MONTHLY_SCHA_REFRESH.md, «Остатки»):
--   Славнефть 1Р3 15 664 564 шт. на 697 млн ₽ (≈44 ₽ за облигацию) против 664 564 у
--   interfax; нет ВТБ Т2-3 (8 % фонда) и выпусков Автодора. Его 100,00 % — артефакт
--   нормировки на сумму бумаг, а interfax делит на общую стоимость активов (84,43 %
--   на 30.04.2026, остаток — деньги). Правило «ближе к 100» выбрало бы как раз vim_sdr.
--   Тот же приоритет уже зашит в manual_scha_backfill (REPLACE_SOURCES).
--
-- Масштаб на проде: 1636 строк vim_sdr, 31 срез, только OBLG. Перекрытий на уровне
-- месяца (разные дни одного месяца) нет — сравнения по точной дате достаточно.
-- vim_sdr OBLG за 2022-08…2023-09 (interfax там нет) не трогаем.
--
-- Удалённые строки сохраняются в fund_holdings_history_shadowed. Откат:
--   INSERT INTO fund_holdings_history SELECT * FROM fund_holdings_history_shadowed;
--
-- Идемпотентно. Синтаксис общий для Postgres и SQLite — тест гоняет этот файл
-- на SQLite (tests/test_fund_holdings_one_source.py). Применение:
--   cat db/migrations/093_fund_holdings_one_source.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

BEGIN;

CREATE TABLE IF NOT EXISTS fund_holdings_history_shadowed AS
    SELECT * FROM fund_holdings_history WHERE 1 = 0;

INSERT INTO fund_holdings_history_shadowed
SELECT h.* FROM fund_holdings_history h
WHERE h.source = 'vim_sdr'
  AND EXISTS (SELECT 1 FROM fund_holdings_history i
              WHERE i.fund_id = h.fund_id
                AND i.snapshot_date = h.snapshot_date
                AND i.source = 'interfax_manual');

DELETE FROM fund_holdings_history
WHERE source = 'vim_sdr'
  AND EXISTS (SELECT 1 FROM fund_holdings_history i
              WHERE i.fund_id = fund_holdings_history.fund_id
                AND i.snapshot_date = fund_holdings_history.snapshot_date
                AND i.source = 'interfax_manual');

COMMIT;

-- Проверка после применения (ожидаем 0 строк):
--   SELECT fund_id, snapshot_date FROM fund_holdings_history
--   WHERE source IN ('vim_sdr', 'interfax_manual')
--   GROUP BY 1, 2 HAVING count(DISTINCT source) > 1;
