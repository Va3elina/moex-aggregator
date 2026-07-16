-- RGBI (индекс ОФЗ) — фьючерс sectype 'RB' (контракты RBH/RBM/RBU/RBZ, инстру-
-- менты подтверждены в instruments.sectype='RB'). Найдено 2026-07-16: пост про
-- "ИНДЕКС ОФЗ (RGBI) < 112" отброшен Шагом А, т.к. known_tickers содержит
-- только акции (ticker_futures_map — было заточено под "акция↔фьючерс"), а
-- RGBI — индекс, не акция. Это ТОЧНЫЙ 1:1 инструмент (не путать с убранным
-- категорийным матчингом 030_drop_content_category — там была fuzzy-привязка
-- новости к ЛЮБОЙ аномалии в широкой категории; здесь — ровно тот же принцип,
-- что уже работает для GAZP/PLZL/SBER, просто индекс вместо акции).
INSERT INTO ticker_futures_map (stock_ticker, futures_sectype, display_name, source)
VALUES ('RGBI', 'RB', 'Индекс ОФЗ (RGBI)', 'manual_2026-07-16')
ON CONFLICT (stock_ticker) DO NOTHING;
