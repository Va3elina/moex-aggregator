-- Дедуп по source_ref (свободный текст) хрупок к перефразировке одного и того же
-- акта между разными еженедельными прогонами Routine (frame-mandate-scan) — LLM не
-- гарантированно повторяет ту же строку дословно между запусками (Вадим, 2026-07-22:
-- «он поймёт что новое что старое что уже было проверено?»). Переходим на dedup_key:
-- короткий канонический номер акта (source_key, напр. "6433-У", "771", "287НК"),
-- нормализованный в api/routers/mandate_scan.py (только буквы/цифры, верхний регистр)
-- ДО сравнения — устойчиво к пробелам, "№" vs "N", регистру, дефисам, скобкам.
--
-- Заодно засеваем 13 мандатов, уже вручную обсуждённых с Вадимом в этой же сессии
-- (см. переписку 2026-07-22) — напрямую SQL, notified_at=now() СРАЗУ, БЕЗ реального
-- похода в Telegram (это не новые находки, а базовая линия, от которой Routine
-- отталкивается дальше через GET /api/internal/mandate-scan/known).
--
-- Идемпотентно. Применение:
--   cat db/migrations/040_mandate_candidates_dedup_key.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

ALTER TABLE mandate_candidates DROP CONSTRAINT IF EXISTS uq_mandate_candidates_source_ref;
DROP INDEX IF EXISTS uq_mandate_candidates_source_ref;

ALTER TABLE mandate_candidates ADD COLUMN IF NOT EXISTS source_key VARCHAR(64);
ALTER TABLE mandate_candidates ADD COLUMN IF NOT EXISTS dedup_key VARCHAR(64);

-- NOT NULL добавляется отдельным шагом (после ALTER ADD COLUMN — на пустой/старой
-- таблице это безопасно, но идемпотентно проверяем через DO-блок, чтобы повторный
-- прогон миграции на таблице с уже заполненными строками не упал).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'mandate_candidates' AND column_name = 'source_key' AND is_nullable = 'NO'
  ) THEN
    UPDATE mandate_candidates SET source_key = source_ref WHERE source_key IS NULL;
    ALTER TABLE mandate_candidates ALTER COLUMN source_key SET NOT NULL;
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mandate_candidates_dedup_key ON mandate_candidates (dedup_key);

-- ─── Seed: 13 мандатов РФ-рынка, уже найденных и разобранных вручную (2026-07-22) ───
INSERT INTO mandate_candidates (
  source_ref, source_key, dedup_key, source_url, mandate_type, participant, sector, asset,
  status_now, mechanical_trigger, trigger_description, pine_testable, hypothesis, durability_note,
  found_at, notified_at
) VALUES
('Указ Президента РФ №771 от 11.10.2023 (обязательная продажа валютной выручки экспортёрами)',
 '№771', '771', 'https://www.consultant.ru/legalnews/28582/', 'government_decree', 'exporters', NULL, 'USDRUB/CNYRUB',
 'zeroed', true, 'норматив репатриации/продажи включается и выключается решением правительства; сейчас 0% с авг.2025, инфраструктура продлена до 2029',
 'medium', 'Если норматив вновь включат, USDRUB/CNYRUB должны получить доп. давление вниз в дни, близкие к налоговому периоду (28-е число)', 'высокая волатильность мандата — менялся 5-6 раз за 3 года', now(), now()),

('ст.287 НК РФ (авансовые платежи налога на прибыль, 28-е число ежемесячно)',
 '287НК', '287НК', 'https://www.consultant.ru/document/cons_doc_LAW_28165/', 'physical_necessity', 'exporters', NULL, 'USDRUB/CNYRUB',
 'active', true, 'ежемесячный авансовый платёж не позднее 28-го числа (перенос на ближайший рабочий день)',
 'high', 'USDRUB/CNYRUB должны показывать смещение к укреплению рубля в окне T-7..T-1 перед 28-м числом каждый месяц', 'налоговый закон — самый устойчивый тип мандата из всех 13', now(), now()),

('Инструкция Банка России №178-И от 28.12.2016 (лимит открытой валютной позиции банков)',
 '178-И', '178И', 'https://www.garant.ru/products/ipo/prime/doc/71539162/', 'regulator_rule', 'banks', NULL, 'USDRUB',
 'active', false, 'непрерывный потолок (ежедневное соблюдение), не календарное событие',
 'low', 'банки структурно ограничены в размере открытой валютной позиции — без прямого календарного триггера гипотезу под Pine не построить', NULL, now(), now()),

('Нормативы Н1/Н2/Н3 Банка России (капитал/ликвидность банков, ОФЗ как высоколиквидные активы)',
 'Н1Н2Н3', 'Н1Н2Н3', NULL, 'regulator_rule', 'banks', NULL, 'ОФЗ',
 'active', false, 'непрерывный норматив, нет разовой даты',
 'low', 'ОФЗ входят в высоколиквидные активы для нормативов ликвидности — структурный спрос без точной календарной даты', NULL, now(), now()),

('Положение Банка России №580-П (структура активов НПФ)',
 '580-П', '580П', NULL, 'regulator_rule', 'pension_funds', NULL, 'акции/облигации (широкий список)',
 'active', false, 'квоты по классам активов, нет публичной даты ребаланса НПФ',
 'low', 'квоты на риск-активы ограничивают ребаланс НПФ, но без публичной даты ребаланса гипотезу не построить', NULL, now(), now()),

('Указания Банка России №3444-У/№4297-У (размещение страховых резервов)',
 '3444-У/4297-У', '3444У4297У', NULL, 'regulator_rule', 'insurers', NULL, 'облигации/акции',
 'active', false, 'квоты по классам активов, нет разовой даты',
 'low', 'структурные ограничения на размещение страховых резервов, без календарного триггера', NULL, now(), now()),

('Счета типа «С» (заморозка средств нерезидентов, узкий выход в ОФЗ/налоги)',
 'СЧЕТА-С', 'СЧЕТАС', 'https://www.cbr.ru/explan/app_decrees_president_aimed_ensuring_financial_stability/', 'government_decree', 'nonresidents', NULL, 'ОФЗ',
 'active', false, 'фоновый навес спроса, без конкретной даты',
 'low', 'замороженный капитал нерезидентов создаёт фоновый спрос на ОФЗ — концептуально похоже на уже отклонённый паттерн навеса ОФЗ (эндогенность спроса, см. research_forced_flows)', 'завязано на геополитику/санкционный режим — устойчивее, чем чисто курсовые указы', now(), now()),

('Указание Банка России №6433-У от 01.06.2023 (обязательные резервы, период усреднения)',
 '6433-У', '6433У', 'https://www.cbr.ru/oper_br/oap/', 'regulator_rule', 'banks', NULL, 'USDRUB / ставочные инструменты',
 'active', true, 'график периодов усреднения (4-5 недель) публикуется ЦБ заранее на cbr.ru/oper_br/oap/',
 'medium', 'к концу периода усреднения банкам нужна рублёвая ликвидность → возможное давление на USDRUB в дату окончания периода', 'самый точный форвардный календарь из всех 13 — приоритетный кандидат', now(), now()),

('Федеральный закон №173-ФЗ от 10.12.2003 (валютное регулирование и валютный контроль)',
 '173-ФЗ', '173ФЗ', 'https://www.consultant.ru/document/cons_doc_LAW_45458/', 'physical_necessity', 'exporters', NULL, 'USDRUB',
 'active', false, 'рамочный закон без конкретной календарной даты — конкретика в ст.287 НК/указе 771',
 'low', 'базовая репатриационная обязанность экспортёров, без собственного календарного триггера', NULL, now(), now()),

('Правила листинга Московской биржи (минимальный free-float, 2-й уровень листинга)',
 'FREEFLOAT-MOEX', 'FREEFLOATMOEX', 'https://www.moex.com/ru/listing/free-float.aspx', 'index_methodology', 'exchange', NULL, 'акции 2-го уровня листинга',
 'active', true, 'льготный порог 1% действует до 30.07.2027, далее возврат к 10%',
 'medium', 'эмитенты у порога free-float могут проводить SPO/размещения к контрольным датам окончания льготного периода', NULL, now(), now()),

('Плавающие экспортные пошлины с привязкой к курсу рубля (с 01.10.2023)',
 'ЭКСП-ПОШЛИНЫ-FX', 'ЭКСППОШЛИНЫFX', 'https://www.rbc.ru/economics/21/09/2023/650c6eec9a79477be8f1569a', 'government_decree', 'exporters', NULL, 'USDRUB',
 'active', false, 'порог курса (80 руб/долл), не календарная дата',
 'medium', 'при пересечении курсом порогового значения пошлина меняется ступенчато — потенциальный психологический уровень для USDRUB', NULL, now(), now()),

('Бюджетное правило Минфина (цена отсечения нефти, покупка/продажа валюты из ФНБ)',
 'БЮДЖ-ПРАВИЛО', 'БЮДЖПРАВИЛО', NULL, 'government_decree', 'state_entities', NULL, 'USDRUB',
 'active', false, 'решение по факту среднемесячной цены нефти, не жёсткая календарная дата внутри месяца',
 'low', 'Минфин покупает/продаёт валюту по итогам месяца в зависимости от цены Urals к цене отсечения — уже частично протестировано и отклонено как паттерн P1 (эндогенность спроса, см. research_forced_flows)', 'уже проверялось эмпирически — не запускать заново без нового угла', now(), now()),

('Программа маркет-мейкеров/первичных дилеров ОФЗ (Московская биржа/Минфин)',
 'ММ-ОФЗ', 'ММОФЗ', 'https://www.moex.com/s1949', 'regulator_rule', 'market_makers', NULL, 'ОФЗ',
 'active', false, 'обязательства поддержания котировок в течение торгового дня, не разовая дата',
 'low', 'маркет-мейкеры обязаны поддерживать котировки ОФЗ — структурная, но не календарная история', NULL, now(), now())
ON CONFLICT (dedup_key) DO NOTHING;
