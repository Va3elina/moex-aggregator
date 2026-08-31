-- Чистка дублей в fund_holdings_history + защита от их повторного появления.
--
-- Как ломалось (обнаружено 2026-08-31 при подсчёте вынужденных потоков под
-- ребалансировку индексов: позиции EQMX на 2026-06-21 удваивались в штуках):
--   uq_fund_holdings_history стоит на (fund_id, COALESCE(isin,''), asset_name,
--   snapshot_date, source), но `isin` проставляется ЗАДНИМ ЧИСЛОМ — бэкфиллом
--   Funds/backfill_holdings_isin.py по рег-номеру из имени. После обогащения
--   ключ строки меняется с ('', name) на (isin, name), и следующий за бэкфиллом
--   прогон дневного джоба (Funds/fetch_funds_realtime.py::save_fund_holdings,
--   который isin не писал вовсе → NULL) в конфликт уже не попадал и дописывал
--   строку-двойник без ISIN. Итог: EQMX 2026-06-21 = 47 строк с ISIN (14:38) +
--   46 без ISIN (19:24) = 93 вместо 47.
--
-- Масштаб на проде до чистки: 936 строк, 27 снапшотов, окно 2026-04-29…2026-06-21
--   (cbonds 887 / vim 49). Дальше не росло только потому, что ISIN-бэкфилл
--   с 21.06 не запускали — баг спящий, а не потухший.
--
-- Что НЕ является дублем и здесь не трогается: одно asset_name с РАЗНЫМИ ISIN
--   в одном снапшоте — у interfax_manual это норма (Сбербанк ао+ап под одним
--   именем эмитента, три выпуска ОФЗ под «Министерство финансов РФ»). Таких
--   групп 1617 у interfax_manual и 8 у cbonds — это реальные позиции.
--
-- Причина закрыта в коде: save_fund_holdings теперь заменяет снапшот целиком
--   (DELETE+INSERT по fund_id+snapshot_date+source в одной транзакции) и пишет
--   isin сразу, а не ждёт бэкфилла.
--
-- Идемпотентно. Применение:
--   cat db/migrations/056_fund_holdings_dedup_isin_orphans.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

BEGIN;

-- 1. Сносим строку-сироту БЕЗ ISIN там, где рядом в том же снапшоте живёт
--    та же бумага с ISIN. Оставляем именно ISIN-строку: проверено на проде —
--    во всех 936 парах она надмножество (в 614 случаях только у неё заполнены
--    positions/amount_rub, обратного нет ни одного). Единственное расхождение
--    значений — OBLG (fund_id=12000), «ВТБ, T2-3, 40901000B» на 2026-06-21:
--    вес 7.05 у ISIN-строки против 7.06 у сироты, разница 0.01 п.п.
DELETE FROM fund_holdings_history h
WHERE h.isin IS NULL
  AND EXISTS (
    SELECT 1 FROM fund_holdings_history h2
    WHERE h2.fund_id       = h.fund_id
      AND h2.snapshot_date = h.snapshot_date
      AND h2.source        = h.source
      AND h2.asset_name    = h.asset_name
      AND h2.isin IS NOT NULL
  );

-- 2. Защита на уровне БД для 'vim' — единственного источника, который пишется
--    ежедневно и автоматически. Имена ВИМ несут рег-номер бумаги («ЛУКОЙЛ, ао,
--    1-01-00077-A»), поэтому внутри снапшота имя уникально независимо от того,
--    заполнен isin или нет — то есть индекс НЕ зависит от обогащаемой колонки.
--    После шага 1 конфликтов по 'vim' ноль (проверено), индекс встанет.
--    На cbonds/interfax_manual такой индекс НЕ вешаем — там одно имя эмитента
--    легитимно несёт несколько выпусков.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fhh_vim_snapshot
    ON fund_holdings_history (fund_id, snapshot_date, asset_name)
    WHERE source = 'vim';

COMMIT;

-- Проверка после применения (ожидаем 0 строк):
--   SELECT fund_id, snapshot_date, source, asset_name, count(*)
--   FROM fund_holdings_history
--   GROUP BY 1,2,3,4
--   HAVING count(*) FILTER (WHERE isin IS NULL) > 0
--      AND count(*) FILTER (WHERE isin IS NOT NULL) > 0;
--
-- И что EQMX вернулся к нормальному размеру снапшота (ожидаем 47):
--   SELECT count(*) FROM fund_holdings_history
--   WHERE fund_id = 6073 AND snapshot_date = '2026-06-21';
