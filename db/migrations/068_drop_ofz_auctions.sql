-- Уборка 03.09.2026: аукционы ОФЗ Минфина (ofz_auctions) убраны — таблицу никто не читал,
-- скрипт Candles/fetch_ofz_auctions.py запускался только вручную, данные до 03.06.2026.
-- Применено на проде руками 03.09.2026. Бэкап: /opt/frame/backups/removed/ofz_auctions_2026-09-03.sql.gz
DROP TABLE IF EXISTS ofz_auctions;
