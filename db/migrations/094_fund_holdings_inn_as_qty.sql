-- Срез 30.04.2026 облигационных фондов ВИМ (OBLG, OPIF-54): ИНН эмитента вместо
-- количества и двойная заливка OPIF-54.
--
-- 1. ИНН вместо штук. У субординированных выпусков с номиналом 10 млн ₽ (и ВТБ СУБТ1-1,
--    150 000 $) парсер справки записал в «количество» ИНН эмитента: 7702070139 (Банк ВТБ),
--    7707083893 (Сбербанк). Стоимость и доля в этих строках верные. is_implausible_row
--    их пропустил: стоимость крупная, цена за штуку вышла 0,03–0,27 ₽ при пороге 0,0001.
--    Настоящее количество = стоимость / цена за штуку, сходится до копейки:
--      ВТБ Т2-3   RU000A1014J2  10 778 717,81 ₽ за шт. (100 % + НКД 778 717,81, ISS 30.04):
--                 OBLG 797 625 117,94 ₽ = 74 шт.; OPIF-54 2 091 071 255,14 ₽ = 194 шт.
--      ВТБСУБТ2-1 RU000A102879  9 687 416,44 ₽ за шт. (OPIF-54: 80 шт. на 774 993 315,20 ₽):
--                 OBLG 116 248 997,28 ₽ = 12 шт.
--    Для ВТБСУБТ2-1, Сбер2СУБ2R и ВТБСУБТ1-1 в OPIF-54 количество — из строк первой
--    заливки того же среза (п. 2): та же бумага, та же стоимость до копейки.
--
-- 2. Двойная заливка OPIF-54. Срез залит дважды под разными написаниями имени (длинное
--    юр-имя и короткое MOEX) — ключ уникальности содержит asset_name, ON CONFLICT не
--    сматчил, сумма долей 197,83 %. Первая заливка (id 110271…110297) неполная: 27 бумаг
--    на 6,80 млрд ₽ при СЧА 44,4 млрд, доли нормированы на сумму бумаг (100 %). Вторая
--    (id 588889…588977) полная: 89 бумаг на 43,81 млрд, 97,83 % общей стоимости активов.
--    Все 27 бумаг первой есть во второй с той же стоимостью. Первую удаляем.
--
-- Идемпотентно. Откат:
--   INSERT INTO fund_holdings_history SELECT * FROM fund_holdings_history_shadowed
--    WHERE id BETWEEN 110271 AND 110297;
--   и вернуть positions = 7702070139 в id 120987, 120966, 588906, 588890, 588892,
--   positions = 7707083893 в id 588891.
-- Применение:
--   cat db/migrations/094_fund_holdings_inn_as_qty.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

BEGIN;

UPDATE fund_holdings_history SET positions = 74  WHERE id = 120987 AND positions = 7702070139;  -- OBLG ВТБ Т2-3
UPDATE fund_holdings_history SET positions = 12  WHERE id = 120966 AND positions = 7702070139;  -- OBLG ВТБСУБТ2-1
UPDATE fund_holdings_history SET positions = 194 WHERE id = 588906 AND positions = 7702070139;  -- OPIF-54 ВТБ Т2-3
UPDATE fund_holdings_history SET positions = 80  WHERE id = 588890 AND positions = 7702070139;  -- OPIF-54 ВТБСУБТ2-1
UPDATE fund_holdings_history SET positions = 25  WHERE id = 588891 AND positions = 7707083893;  -- OPIF-54 Сбер2СУБ2R
UPDATE fund_holdings_history SET positions = 10  WHERE id = 588892 AND positions = 7702070139;  -- OPIF-54 ВТБСУБТ1-1

CREATE TABLE IF NOT EXISTS fund_holdings_history_shadowed AS
    SELECT * FROM fund_holdings_history WHERE 1 = 0;

INSERT INTO fund_holdings_history_shadowed
SELECT * FROM fund_holdings_history
WHERE fund_id = 54 AND snapshot_date = '2026-04-30' AND source = 'interfax_manual'
  AND id BETWEEN 110271 AND 110297;

DELETE FROM fund_holdings_history
WHERE fund_id = 54 AND snapshot_date = '2026-04-30' AND source = 'interfax_manual'
  AND id BETWEEN 110271 AND 110297;

COMMIT;

-- Проверка после применения:
--   ИНН в количестве (ожидаем 0 строк):
--     SELECT id FROM fund_holdings_history WHERE positions IN (7702070139, 7707083893);
--   OPIF-54 30.04 (ожидаем 89 строк, сумма долей 97,83):
--     SELECT count(*), sum(weight) FROM fund_holdings_history
--     WHERE fund_id = 54 AND snapshot_date = '2026-04-30' AND source = 'interfax_manual';
