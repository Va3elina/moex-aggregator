-- Исторические базы расчёта индексов МосБиржи (пока только IMOEX).
--
-- Зачем: «Сила рынка» с вселенной imoex до этого считала ВСЮ историю широты по
-- СЕГОДНЯШНЕМУ составу индекса (список тикеров спрашивался у ISS в момент
-- расчёта). Это look-ahead: график 2015 года строился по бумагам, которые вошли
-- в индекс в 2024-м, а выбывшие (и обанкротившиеся) в нём не участвовали вовсе.
-- Одна строка = одна бумага в базе расчёта индекса на конкретный торговый день.
--
-- Источник: ISS /statistics/engines/stock/markets/index/analytics/{IDX}?date=D
-- (глубина с 2001-01-03). Наполняется Candles/fetch_index_composition.py.
--
-- weight — официальный вес бумаги в индексе, %. Для широты не нужен, но идёт в
-- том же ответе ISS и пригодится для взвешенных метрик и как durable-fallback
-- вместо ad-hoc таблицы imoex_weights у карты рынка.
--
-- Состав есть только на торговые дни. Выходные сессии MOEX (суббота/воскресенье)
-- своей строки не имеют — потребитель берёт последний состав на дату <= нужной.
--
-- Идемпотентно. Применение:
--   cat db/migrations/052_index_composition.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS index_composition (
  index_id   VARCHAR(16) NOT NULL,  -- 'IMOEX'
  trade_date DATE        NOT NULL,
  ticker     VARCHAR(16) NOT NULL,
  weight     REAL,                  -- вес в индексе, % (может быть NULL)
  PRIMARY KEY (index_id, trade_date, ticker)
);

-- Лукап «состав на дату» и «последняя известная дата состава».
CREATE INDEX IF NOT EXISTS idx_index_composition_date
  ON index_composition(index_id, trade_date DESC);
