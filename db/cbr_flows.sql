-- cbr_flows — данные ОРФР Банка России о покупках/продажах по категориям
-- участников биржевых торгов.
--
-- Источник: cbr.ru/analytics/finstab/orfr/ — ежемесячные XLSX с листами:
--   "рис. 32" — Акции (stocks), 7 категорий
--   "рис. 14" — ОФЗ (ofz), 7 категорий
--   "рис. 8"  — Валюты (fx), 5 категорий, только месячные данные
--
-- Каждый новый XLSX содержит весь архив (~2 года кварталами + последний месяц
-- для акций/ОФЗ; ~9 месяцев для валют), поэтому ingest = full upsert каждый раз.
--
-- Long format: одна row на (период × категория) — гибко относительно того, что
-- категории различаются между инструментами и могут меняться у ЦБ.

CREATE TABLE IF NOT EXISTS cbr_flows (
    instrument_type  VARCHAR(20)    NOT NULL,  -- 'stocks' | 'ofz' | 'fx'
    period_year      INT            NOT NULL,
    period_label     VARCHAR(40)    NOT NULL,  -- 'I квартал', 'Апрель', ...
    period_kind      VARCHAR(10)    NOT NULL,  -- 'quarter' | 'month'
    period_end_date  DATE           NOT NULL,  -- сортировка/фильтр по дате
    category         VARCHAR(60)    NOT NULL,
    value            NUMERIC(20, 6) NOT NULL,  -- млрд руб.
    source_file      VARCHAR(60),              -- 'ORFR_2026-4'
    updated_at       TIMESTAMP      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (instrument_type, period_end_date, category)
);

CREATE INDEX IF NOT EXISTS idx_cbr_flows_lookup
    ON cbr_flows (instrument_type, period_end_date);

COMMENT ON TABLE cbr_flows IS
    'ОРФР Банка России — потоки по категориям участников на бирже. Обновляется ежемесячно.';
COMMENT ON COLUMN cbr_flows.instrument_type IS 'stocks | ofz | fx';
COMMENT ON COLUMN cbr_flows.period_kind IS 'quarter | month';
COMMENT ON COLUMN cbr_flows.period_end_date IS 'Последняя дата периода — для сортировки и фильтрации';
COMMENT ON COLUMN cbr_flows.value IS 'Нетто-покупка (+) / нетто-продажа (−) в млрд руб.';
