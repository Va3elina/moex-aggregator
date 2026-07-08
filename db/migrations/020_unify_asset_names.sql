-- 020_unify_asset_names.sql — унификация названий активов между разделами
-- (Открытые позиции → Сезонность → Покупки фондов), решение от 2026-07-08.
--
-- Стандарт: имена ОИ каноничны. Фьючерсные приписки «(вечн)» / «(мини)» /
-- «(микро)» остаются ТОЛЬКО в ОИ (различают контракты), в канон не входят.
-- Скобки-пояснения о компании («(FESCO)», «(Тинькофф)») убраны. Маркер
-- привилегированных акций — единый: «(прив)». Два исключения-дизамбигуации
-- оставлены: «Биткоин (IBIT)» и «Биткоин (Индекс МосБиржи)» — иначе два
-- разных актива ОИ становятся неразличимы.
--
-- Идемпотентно: повторный прогон — no-op.
-- Frontend-зеркало имён: frontend/src/config/fundConfig.ts (TICKER_TO_NAME).

BEGIN;

-- ── ОИ (фьючерсы): актуализация имён и стиля ─────────────────────────────
-- Переименования компаний (решение юзера 2026-07-08): НоваБев, ВК.
UPDATE instruments SET name = 'ДВМП'         WHERE type = 'futures' AND sectype = 'FE';
UPDATE instruments SET name = 'Т-Технологии' WHERE type = 'futures' AND sectype = 'TB';
UPDATE instruments SET name = 'НоваБев'      WHERE type = 'futures' AND sectype = 'NB';
UPDATE instruments SET name = 'ВК'           WHERE type = 'futures' AND sectype = 'VK';
-- Опечатка: «Паладий» → «Палладий».
UPDATE instruments SET name = 'Палладий'     WHERE type = 'futures' AND sectype = 'PD';
-- Стиль: как у спот-ряда RVI, без скобок.
UPDATE instruments SET name = 'Индекс волатильности RVI' WHERE type = 'futures' AND sectype = 'VI';
-- «Золото (в руб за грамм)» / «... ВЕЧН» → компактно и без скобок.
UPDATE instruments SET name = 'Золото руб/г' WHERE type = 'futures' AND sectype = 'GL';
UPDATE instruments SET name = 'Золото руб/г (вечн)' WHERE sec_id = 'GLDRUBF';

-- ── Сезонность (акции): имена по стандарту ОИ ────────────────────────────
UPDATE instruments SET name = 'АФК Система'           WHERE sec_id = 'AFKS';
UPDATE instruments SET name = 'АЛРОСА'                WHERE sec_id = 'ALRS';
UPDATE instruments SET name = 'Астра'                 WHERE sec_id = 'ASTR';
UPDATE instruments SET name = 'Банк Санкт-Петербург'  WHERE sec_id = 'BSPB';
UPDATE instruments SET name = 'ФСК Россети'           WHERE sec_id = 'FEES';
UPDATE instruments SET name = 'Норильский никель'     WHERE sec_id = 'GMKN';
UPDATE instruments SET name = 'HeadHunter'            WHERE sec_id = 'HEAD';
UPDATE instruments SET name = 'Интер РАО'             WHERE sec_id = 'IRAO';
UPDATE instruments SET name = 'Московская биржа'      WHERE sec_id = 'MOEX';
UPDATE instruments SET name = 'НОВАТЭК'               WHERE sec_id = 'NVTK';
UPDATE instruments SET name = 'Positive Technologies' WHERE sec_id = 'POSI';
UPDATE instruments SET name = 'Ренессанс Страхование' WHERE sec_id = 'RENI';
UPDATE instruments SET name = 'Ростелеком'            WHERE sec_id = 'RTKM';
UPDATE instruments SET name = 'Сбербанк (прив)'       WHERE sec_id = 'SBERP';
UPDATE instruments SET name = 'ЭсЭфАй'                WHERE sec_id = 'SFIN';
UPDATE instruments SET name = 'Газпром нефть'         WHERE sec_id = 'SIBN';
UPDATE instruments SET name = 'Самолет'               WHERE sec_id = 'SMLT';
UPDATE instruments SET name = 'Сургутнефтегаз'        WHERE sec_id = 'SNGS';
UPDATE instruments SET name = 'Сургутнефтегаз (прив)' WHERE sec_id = 'SNGSP';
UPDATE instruments SET name = 'Т-Технологии'          WHERE sec_id = 'T';
UPDATE instruments SET name = 'Татнефть (прив)'       WHERE sec_id = 'TATNP';
UPDATE instruments SET name = 'Транснефть (прив)'     WHERE sec_id = 'TRNFP';
UPDATE instruments SET name = 'X5 Group'              WHERE sec_id = 'X5';
-- Тикер вместо имени в данных — человекочитаемый бренд.
UPDATE instruments SET name = 'Мать и дитя'           WHERE sec_id = 'MDMG';

-- ── Сезонность (спот): валюты в формате ОИ ───────────────────────────────
UPDATE instruments SET name = 'USD/RUB'      WHERE sec_id = 'USD000UTSTOM';
UPDATE instruments SET name = 'EUR/RUB'      WHERE sec_id = 'EUR_RUB__TOM';
UPDATE instruments SET name = 'CNY/RUB'      WHERE sec_id = 'CNYRUB_TOM';
UPDATE instruments SET name = 'Золото руб/г' WHERE sec_id = 'GLDRUB_TOM';

COMMIT;
