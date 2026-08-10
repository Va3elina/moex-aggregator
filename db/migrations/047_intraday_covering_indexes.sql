-- 047: covering-индексы для интрадей-графика ОИ (5м / 1ч)
--
-- ПРОБЛЕМА (замер на проде 2026-08-10). Холодный расчёт графика 5м/6мес стоил
-- от 3 до 52 секунд, у CNYRUBF доходило до 92. Причём один и тот же SQL в psql
-- при прогретом кеше отрабатывал за 148 мс — то есть план был идеальным, а
-- страдал ввод-вывод:
--
--   Bitmap Heap Scan on candles ... rows=40033
--     Heap Blocks: exact=16653        ← 133 МБ страниц ради 4 МБ данных
--
-- Индекс (sec_id, interval, begin_time) отвечает на вопрос «где искать», но
-- хранит лишь ключи и указатели. За open/high/low/close/volume Postgres шёл в
-- саму таблицу — а нужные строки размазаны по 16 тысячам страниц, потому что
-- candles (24 млн пятиминуток) пишется в порядке поступления, вперемешку по
-- активам и интервалам. На машине с 8 ГБ памяти и таблицей на 11 ГБ это
-- гарантированные случайные чтения с диска.
--
-- РЕШЕНИЕ. INCLUDE-колонки: индекс несёт сами значения, и запрос закрывается
-- Index Only Scan — читается только индекс, последовательно по диапазону.
--
--   Index Only Scan using idx_candles_intraday_cover ... rows=40033
--     Heap Fetches: 0
--     Buffers: shared hit=519         ← было 16900
--     Execution Time: 26.5 ms         ← было 148 мс тёплым, секунды холодным
--
-- РЕЗУЛЬТАТ на живом API (кеш графиков очищен, холодный заход):
--   SR 5м/6мес   14-52 с → 1.66 с      CNYRUBF  92 с → 1.55 с
--   MX 5м/6мес      23 с → 1.64 с      RI       15 с → 1.86 с
--   GZ 5м/6мес      11 с → 1.53 с      SR 1ч/1г 1.3 с → 0.58 с
-- Разброс исчез: раньше от долей секунды до полутора минут в зависимости от
-- того, лежат ли страницы актива в page cache, теперь ровно ~1.6 с у всех.
--
-- ⚠️ ОБЯЗАТЕЛЕН VACUUM после создания. Index Only Scan опирается на карту
-- видимости: пока страница не помечена как «все строки видимы», Postgres всё
-- равно идёт в таблицу. Без VACUUM замер показывал Heap Fetches: 18063 и
-- 5.7 с вместо 26 мс — то есть индекс есть, а выигрыша нет.
--
-- ⚠️ Частичные (WHERE interval IN (5,60)): дневной ТФ и так дёшев (852 тыс.
-- строк против 24 млн), включать его — платить объёмом за то, что не болит.
--
-- Цена: +2.2 ГБ (candles) и +428 МБ (open_interest) на диске, плюс поддержка
-- при вставке. На сервере было свободно 41 ГБ.
--
-- CONCURRENTLY — без блокировки таблиц, можно катить на живом проде.
-- Выполнять ПООЧЕРЁДНО и вне транзакции (CREATE INDEX CONCURRENTLY этого
-- требует), затем VACUUM каждой таблицы.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_candles_intraday_cover
    ON candles (sec_id, "interval", begin_time)
    INCLUDE (open, high, low, close, volume, secid)
    WHERE "interval" IN (5, 60);

VACUUM (ANALYZE) candles;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oi_intraday_cover
    ON open_interest (sectype, clgroup, "interval", tradedate, tradetime)
    INCLUDE (pos, pos_long, pos_short, pos_long_num, pos_short_num)
    WHERE "interval" IN (5, 60);

VACUUM (ANALYZE) open_interest;
