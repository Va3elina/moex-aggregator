-- 104: единый реестр сплитов акций.
--
-- Раньше сплиты были захардкожены в четырёх местах (api/routers/breadth.py,
-- api/routers/heatmap.py, Candles/compute_breadth_history.py, CTE known_splits
-- в db/mv_heatmap_stocks.sql) и разъехались: heatmap знал только SFIN, репо
-- «Перекраски» — свой T. Теперь одна таблица, читают все через
-- api/services/splits.py (матвьюха — напрямую).
--
-- ratio — сколько новых акций на 1 старую (обратный сплит VTBR: 1/5000).
-- price_adjusted — биржа пересчитала цены до сплита задним числом (ISS отдаёт
--   уже в новых акциях). Тогда цену НЕ трогаем, иначе двойная коррекция
--   (T в mv_heatmap_stocks давал +811% за год). Объём биржа не пересчитывает
--   НИКОГДА: до split_date volume лежит в старых акциях и всегда умножается
--   на ratio там, где объём считается в штуках (CDV в «Перекраске»).
-- Даты и признаки сверены по дневным свечам на проде 2026-09-24.

CREATE TABLE IF NOT EXISTS stock_splits (
    secid          VARCHAR(20) PRIMARY KEY,
    split_date     DATE        NOT NULL,   -- первый день торгов новыми акциями
    ratio          NUMERIC     NOT NULL CHECK (ratio > 0),
    price_adjusted BOOLEAN     NOT NULL DEFAULT TRUE,
    note           TEXT
);

COMMENT ON TABLE stock_splits IS 'Сплиты акций: применяется к строкам candles с begin_time < split_date';
COMMENT ON COLUMN stock_splits.ratio IS 'Новых акций на 1 старую';
COMMENT ON COLUMN stock_splits.price_adjusted IS 'Цены до сплита уже пересчитаны биржей (делить нельзя); объём — никогда';

INSERT INTO stock_splits (secid, split_date, ratio, price_adjusted, note) VALUES
    ('BELU',  '2024-08-22', 8,       FALSE, 'НоваБев 1:8, свечи в БД сырые (разрыв 4680→714)'),
    ('SFIN',  '2025-12-25', 1.93,    FALSE, 'ЭсЭфАй, свечи сырые (разрыв 1828→947), ratio как в mv_heatmap_stocks'),
    ('T',     '2026-04-17', 10,      TRUE,  'Т-Технологии 1:10, цены пересчитаны ISS, объём нет (~10× с 17.04)'),
    ('GMKN',  '2024-04-08', 100,     TRUE,  'Норникель 1:100, объём 61 тыс → 22 млн'),
    ('TRNFP', '2024-02-21', 100,     TRUE,  'Транснефть 1:100, объём 20 тыс → 5.7 млн'),
    ('VTBR',  '2024-07-15', 0.0002,  TRUE,  'ВТБ обратный 5000:1, объём 87 млрд → 98 млн')
ON CONFLICT (secid) DO NOTHING;
