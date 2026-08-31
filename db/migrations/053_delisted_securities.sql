-- Реестр исторических бумаг, которых больше нет на бирже.
--
-- Зачем нужна отдельная таблица: делистингованные и переименованные бумаги
-- НЕЛЬЗЯ держать в instruments — оттуда их подхватят пикеры индикаторов и карта
-- рынка, и мёртвый тикер поедет по сайту как живой актив. При этом их дневные
-- свечи в candles нужны: по ним считается «Сила рынка» за прошлые годы (бумага
-- входила в базу расчёта IMOEX своего времени) и рисуется «Карта сделок» в
-- «Потоках по компании». Строка здесь = пометка «этот secid в candles —
-- историческая бумага, не действующий инструмент».
--
-- Наполняется Candles/backfill_delisted_history.py (он же качает свечи).
--
-- source — откуда бумага попала в работу:
--   'index_composition' — была в базе расчёта индекса (index_composition);
--   'fund_holdings'     — встречалась в составах фондов (securities_ref).
-- Бумага может прийти из обоих источников, пишется последний сработавший.
--
-- last_candle — фактическая дата последней свечи с ISS, то есть дата, после
-- которой бумага на бирже не торговалась. Точной даты приказа о делистинге у
-- нас нет, и она здесь не нужна.
--
-- Идемпотентно. Применение:
--   cat db/migrations/053_delisted_securities.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS delisted_securities (
  secid         VARCHAR(16) PRIMARY KEY,
  short_name    VARCHAR(120),
  isin          VARCHAR(20),
  source        VARCHAR(32),
  first_candle  DATE,
  last_candle   DATE,
  candles_count INTEGER,
  note          TEXT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_delisted_securities_last
  ON delisted_securities(last_candle DESC);
