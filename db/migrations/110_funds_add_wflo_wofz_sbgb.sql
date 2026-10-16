-- «Деньги в фондах»: три индексных облигационных БПИФ.
--
--   WFLO  ВИМ – Облигации с переменным купоном  (RUFLBITR)  → Короткие
--   WOFZ  ВИМ – ОФЗ                             (RGBITR)    → Длинные
--   SBGB  Первая – Фонд Государственные облигации (RGBITR)  → Длинные
--
-- fund_id = внутренний id investfunds, cbonds_share_id найден через
-- cbonds.ru/api/suggest/etf/ и сверен по ISIN (playbook: Funds/DATA_SOURCES.md §8).
-- cbonds_parent_id не заполнен: состав для «Покупок фондов» не подключаем.
-- NAV за 3 года нальёт fetch_funds_realtime.py при первом прогоне.
--
-- Идемпотентно. Применение:
--   cat db/migrations/110_funds_add_wflo_wofz_sbgb.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

INSERT INTO funds (fund_id, ticker, name, uk, category, subcategory, subcategory_order,
                   uk_id, cbonds_share_id, isin_pif, currency)
VALUES
    (8063, 'WFLO', 'ВИМ - Облигации с переменным купоном',    'ВИМ Инвестиции', 'bonds', 'Короткие', 3, '7',  369417, 'RU000A10G1E7', 'RUB'),
    (8065, 'WOFZ', 'ВИМ - ОФЗ',                               'ВИМ Инвестиции', 'bonds', 'Длинные',  1, '7',  369421, 'RU000A10G1K4', 'RUB'),
    (3629, 'SBGB', 'Первая - Фонд Государственные облигации', 'Первая',         'bonds', 'Длинные',  1, '34', 208991, 'RU000A1000F9', 'RUB')
ON CONFLICT DO NOTHING;

-- Первый прогон на проде прошёл без uk — дозаполняем.
UPDATE funds SET uk = 'ВИМ Инвестиции' WHERE fund_id IN (8063, 8065) AND uk IS NULL;
UPDATE funds SET uk = 'Первая'         WHERE fund_id = 3629 AND uk IS NULL;
