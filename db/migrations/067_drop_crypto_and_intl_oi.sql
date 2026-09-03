-- Уборка 03.09.2026 (решение Вадима): крипта и международный ОИ убраны из проекта целиком.
-- Код удалён в PR #1362/#1363 (Crypto/, IntlOI/, api/routers/oi_intl.py, модели, панели фронта, навык moex-crypto).
-- ⚠️ Применено на проде РУКАМИ 03.09.2026 (файл в PR #1362 не попал). Данные сохранены:
--   /opt/frame/backups/removed/crypto_tables_2026-09-03.sql.gz   (crypto_candles, crypto_open_interest, btc_network_stats)
--   /opt/frame/backups/removed/intl_oi_tables_2026-09-03.sql.gz  (open_interest_intl, candles_intl, price_breadth_intl_history)
-- Восстановить: zcat <файл> | docker exec -i frame-db-1 psql -U postgres -d moex_db
DROP TABLE IF EXISTS crypto_candles;
DROP TABLE IF EXISTS crypto_open_interest;
DROP TABLE IF EXISTS btc_network_stats;
DROP TABLE IF EXISTS open_interest_intl;
DROP TABLE IF EXISTS candles_intl;
DROP TABLE IF EXISTS price_breadth_intl_history;
DROP TABLE IF EXISTS oi_intl_strength_history;
