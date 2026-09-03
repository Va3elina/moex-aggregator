-- Справочник эмитентов + карточки компаний. Второй мозг для агентов, пишущих посты,
-- и заодно фундамент фундаментального слоя на будущее.
--
-- ЗАЧЕМ СПРАВОЧНИК. Один и тот же Сбербанк лежит у нас под пятью разными ключами,
-- и ни один не является ключом остальных (проверено на проде 03.09.2026):
--   свечи / индекс / капитализация — тикер бумаги: SBER и SBERP ОТДЕЛЬНО;
--   открытый интерес              — sectype серии: SR (кварт., с 2012), SP (преф),
--                                   SBERF (вечный, только с окт-2024);
--   контракты фьючерсов           — assetcode: SBRF, SBPR, SBERF;
--   сделки фондов                 — ISIN + имя из справки УК, а имён ВОСЕМЬ:
--                                   «Сбербанк», «Сбербанк России, акция об.»,
--                                   «Сбербанк, ао, 10301481B», «Публичное акционерное
--                                   общество "Сбербанк России"» и т.д., причём часть
--                                   строк приходит вообще без ISIN;
--   smart-lab                     — тикер обыкновенной: SBER, и карточка одна на
--                                   обе бумаги (данные префа лежат там же, но под
--                                   отдельными кодами priv_share/dividend_pr).
--
-- ⚠️ ПОЧЕМУ НЕ ПОИСК ПО КЛЮЧЕВЫМ СЛОВАМ. В fund_holdings_history по подстроке «Сбер»
-- находятся ОБЛИГАЦИИ Сбербанка («Сбер Sb15R», «Сбербанк, 002СУБ-02R») и субординаты.
-- Агент, ищущий «Сбер», получит облигации в карточке акции и напишет ерунду уверенно.
-- Поэтому не поиск, а резолв: любой ключ на входе → эмитент и его бумаги.
--
-- ⚠️ ТРИ СЛОЯ, А НЕ ДВА. Часть ключей висит на ЭМИТЕНТЕ (имя в справке УК «Сбербанк»),
-- часть — на БУМАГЕ (SBERP → sectype SP). Плоская пара issuers+aliases это не выражает,
-- поэтому между ними стоит issuer_securities.
--
-- Применение на проде:
--   docker cp db/migrations/069_issuers_company_cards.sql frame-db-1:/tmp/
--   docker exec frame-db-1 psql -U postgres -d moex_db -f /tmp/069_issuers_company_cards.sql

-- ======================================================================
--                        СЛОЙ 1: ЭМИТЕНТ
-- ======================================================================

CREATE TABLE IF NOT EXISTS issuers (
    issuer_id       SERIAL PRIMARY KEY,
    issuer_key      VARCHAR(24) NOT NULL UNIQUE,   -- 'SBER' — тикер обыкновенной как ключ
    name_short      VARCHAR(120) NOT NULL,         -- 'Сбербанк'
    name_full       VARCHAR(300),                  -- 'ПАО Сбербанк России'
    sector          VARCHAR(60),                   -- как в instruments.sector
    smartlab_ticker VARCHAR(24),                   -- NULL у 6 бумаг без карточки (T, TRNFP, EVRZ…)
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ======================================================================
--                        СЛОЙ 2: БУМАГА
-- ======================================================================

CREATE TABLE IF NOT EXISTS issuer_securities (
    secid            VARCHAR(24) PRIMARY KEY,      -- 'SBER', 'SBERP' — как в candles.secid
    issuer_id        INTEGER NOT NULL REFERENCES issuers(issuer_id) ON DELETE CASCADE,
    share_class      VARCHAR(12) NOT NULL,         -- common | pref | dr
    isin             VARCHAR(20),
    canonical_isin   VARCHAR(20),                  -- склейка после редомициляции (см. securities_ref)
    -- ⚠️ ДВЕ ГЕНЕРАЦИИ ФЬЮЧЕРСОВ, и выбор между ними не бесплатен:
    -- у SBER вечный SBERF даёт ОИ только с окт-2024, квартальный SR — с 2012.
    -- Поэтому храним оба, а «фьючерс компании» выбирает потребитель.
    futures_sectype_quarterly VARCHAR(24),         -- 'SR', 'SP'
    futures_sectype_perpetual VARCHAR(24),         -- 'SBERF', NULL если вечного нет
    futures_assetcode         VARCHAR(24),         -- 'SBRF', 'SBPR'
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_issuer_securities_issuer ON issuer_securities (issuer_id);
CREATE INDEX IF NOT EXISTS idx_issuer_securities_isin   ON issuer_securities (isin);

-- ======================================================================
--                        СЛОЙ 3: РЕЗОЛВЕР
-- ======================================================================

CREATE TABLE IF NOT EXISTS issuer_aliases (
    alias_type   VARCHAR(24) NOT NULL,   -- secid | isin | isin_old | assetcode | sectype
                                         -- | fund_asset_name | smartlab | display_name
    alias_value  VARCHAR(300) NOT NULL,
    issuer_id    INTEGER NOT NULL REFERENCES issuers(issuer_id) ON DELETE CASCADE,
    -- NULL, когда ключ не различает класс бумаги: имя «Сбербанк» в справке УК может
    -- относиться к любой из двух бумаг, а sectype 'SP' — однозначно к SBERP.
    secid        VARCHAR(24) REFERENCES issuer_securities(secid) ON DELETE SET NULL,
    source       VARCHAR(40) NOT NULL DEFAULT 'manual',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (alias_type, alias_value)
);
CREATE INDEX IF NOT EXISTS idx_issuer_aliases_issuer ON issuer_aliases (issuer_id);

-- ======================================================================
--                     СПРАВОЧНИК ПОКАЗАТЕЛЕЙ
-- ======================================================================
-- ⚠️ НАПОЛНЯЕТСЯ НА ЛЕТУ ПАРСЕРОМ, а не сидируется руками. Причина: код показателя
-- лежит прямо в строке таблицы smart-lab (?field=net_interest_income), и набор кодов
-- ОТРАСЛЕВОЙ. У Сбера 52 кода, из них банковские (loan_portfolio, cost_of_risk_ratio,
-- share_of_non_performing_loans) вообще не встречаются на сводных страницах ?field=,
-- по которым собирался первоначальный список из 48 кодов. Фиксированный словарь
-- гарантированно отстанет от сайта.
CREATE TABLE IF NOT EXISTS metrics_ref (
    metric_code  VARCHAR(60) PRIMARY KEY,
    label_ru     VARCHAR(200) NOT NULL,    -- 'Чист. проц. доходы'
    unit         VARCHAR(60),              -- 'млрд руб', '%', 'руб/акцию'
    first_seen   DATE NOT NULL DEFAULT CURRENT_DATE,
    last_seen    DATE NOT NULL DEFAULT CURRENT_DATE
);

-- ======================================================================
--                   ДАННЫЕ КАРТОЧКИ: ДЛИННЫЙ ФОРМАТ, SCD-2
-- ======================================================================
-- ⚠️ ПОЧЕМУ НЕ ЕЖЕДНЕВНЫЙ СНИМОК. 220 бумаг × ~55 показателей × 11 периодов ≈ 145 тыс.
-- строк в день, 53 млн за год — при том что значение за 2022 год не меняется никогда.
-- Поэтому новая строка пишется ТОЛЬКО когда значение изменилось, иначе двигается
-- last_seen. История ревизий сохраняется (видно, что smart-lab переписал прибыль),
-- объём — тысячи строк в день.
CREATE TABLE IF NOT EXISTS company_metrics (
    id           BIGSERIAL PRIMARY KEY,
    secid        VARCHAR(24) NOT NULL REFERENCES issuer_securities(secid) ON DELETE CASCADE,
    metric_code  VARCHAR(60) NOT NULL,
    standard     VARCHAR(10) NOT NULL,     -- MSFO | RSBU | USGAAP
    period_type  VARCHAR(10) NOT NULL,     -- year | quarter | ltm
    period_label VARCHAR(12) NOT NULL,     -- '2025', '2026Q2', 'LTM'
    period_end   DATE,                     -- 2025-12-31 / 2026-06-30 / NULL у LTM
    value        NUMERIC,                  -- NULL = нет данных, см. note
    -- ⚠️ ЛОВУШКА НУЛЯ. smart-lab печатает пропуск нулём: у Роснефти чистый долг идёт
    -- 5 416 → 0.00 → 2 700, и Долг/EBITDA тоже 0.00. Это не «нет долга», это пустая
    -- клетка. Загрузчик пишет NULL и ставит note='zero_as_missing'; сырой текст
    -- остаётся в raw_text, чтобы решение можно было пересмотреть, не перекачивая сайт.
    note         VARCHAR(24),              -- NULL | no_data | zero_as_missing
    raw_text     VARCHAR(40),
    report_date  DATE,                     -- дата публикации отчёта за период
    source       VARCHAR(24) NOT NULL DEFAULT 'smartlab',
    first_seen   DATE NOT NULL DEFAULT CURRENT_DATE,
    last_seen    DATE NOT NULL DEFAULT CURRENT_DATE
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_company_metrics_version
    ON company_metrics (secid, metric_code, standard, period_type, period_label, first_seen);
CREATE INDEX IF NOT EXISTS idx_company_metrics_lookup
    ON company_metrics (secid, metric_code, period_type, period_end DESC);

-- ======================================================================
--                     ДИВИДЕНДЫ, АКЦИОНЕРЫ, ТЕЗИСЫ, ДОКУМЕНТЫ
-- ======================================================================

-- Отдельно от нашей таблицы dividends: та ведёт ex_date по данным биржи с 2001 года,
-- эта — то, что видит smart-lab (с 2017, но свежее: на 03.09.2026 у нас нет выплаты
-- 37,64 ₽ с отсечкой 20.07.2026, а у них она есть). Две таблицы = возможность сверки.
CREATE TABLE IF NOT EXISTS company_dividends (
    secid        VARCHAR(24) NOT NULL REFERENCES issuer_securities(secid) ON DELETE CASCADE,
    period       VARCHAR(40) NOT NULL,     -- '2025 год', '9 мес 2025'
    record_date  DATE NOT NULL,            -- дата отсечки
    dividend     NUMERIC,
    price        NUMERIC,
    div_yield    NUMERIC,
    source       VARCHAR(24) NOT NULL DEFAULT 'smartlab',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (secid, record_date, period)
);

-- ⚠️ structure_as_of ОБЯЗАТЕЛЕН И ОБЯЗАН ПОКАЗЫВАТЬСЯ АГЕНТУ. Структура Сбера на
-- smart-lab не обновлялась с 08.05.2020 и до сих пор содержит «Американские инвесторы
-- 33.00%»; у Роснефти — с 28.09.2021. Без даты агент напишет, что треть Сбера
-- принадлежит американцам, и будет звучать уверенно.
CREATE TABLE IF NOT EXISTS company_shareholders (
    issuer_id       INTEGER NOT NULL REFERENCES issuers(issuer_id) ON DELETE CASCADE,
    holder          VARCHAR(200) NOT NULL,
    share_pct       NUMERIC,
    structure_as_of DATE,
    source          VARCHAR(24) NOT NULL DEFAULT 'smartlab',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (issuer_id, holder, structure_as_of)
);

-- «Факторы роста и падения акций» — датированные тезисы. Устроены как world_facts:
-- утверждение + дата, с которой оно заявлено. Видно 5 из 10 (остальные за подпиской),
-- поэтому в visible_of_total честно пишем, сколько всего заявлено.
CREATE TABLE IF NOT EXISTS company_theses (
    issuer_id        INTEGER NOT NULL REFERENCES issuers(issuer_id) ON DELETE CASCADE,
    direction        VARCHAR(8) NOT NULL,  -- growth | fall
    statement        TEXT NOT NULL,
    stated_date      DATE,
    visible_of_total VARCHAR(12),          -- '5/10'
    source           VARCHAR(24) NOT NULL DEFAULT 'smartlab',
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (issuer_id, direction, statement)
);

-- Первый шаг по PDF — ТОЛЬКО ссылки. Текст и факты со страницами — отдельная задача:
-- она тянет за собой проверяемость (документ + страница у каждого утверждения), и это
-- не довесок к парсеру таблиц.
CREATE TABLE IF NOT EXISTS company_documents (
    issuer_id   INTEGER NOT NULL REFERENCES issuers(issuer_id) ON DELETE CASCADE,
    doc_type    VARCHAR(24) NOT NULL,      -- financial_report | annual_report | presentation
    period      VARCHAR(12) NOT NULL,      -- '2024', 'LTM'
    url         TEXT NOT NULL,
    source      VARCHAR(24) NOT NULL DEFAULT 'smartlab',
    parsed      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (issuer_id, doc_type, period, url)
);

-- ======================================================================
--            СИД: СБЕРБАНК — эталонный эмитент, все ключи проверены
-- ======================================================================

INSERT INTO issuers (issuer_key, name_short, name_full, sector, smartlab_ticker)
VALUES ('SBER', 'Сбербанк', 'Публичное акционерное общество "Сбербанк России"',
        'Финансы', 'SBER')
ON CONFLICT (issuer_key) DO UPDATE
   SET name_short = EXCLUDED.name_short, name_full = EXCLUDED.name_full,
       sector = EXCLUDED.sector, smartlab_ticker = EXCLUDED.smartlab_ticker,
       updated_at = now();

INSERT INTO issuer_securities (secid, issuer_id, share_class, isin, canonical_isin,
                               futures_sectype_quarterly, futures_sectype_perpetual,
                               futures_assetcode)
SELECT v.secid, i.issuer_id, v.share_class, v.isin, v.isin, v.q, v.p, v.ac
FROM issuers i,
     (VALUES ('SBER',  'common', 'RU0009029540', 'SR', 'SBERF', 'SBRF'),
             ('SBERP', 'pref',   'RU0009029557', 'SP', NULL,    'SBPR')
     ) AS v(secid, share_class, isin, q, p, ac)
WHERE i.issuer_key = 'SBER'
ON CONFLICT (secid) DO UPDATE
   SET issuer_id = EXCLUDED.issuer_id, share_class = EXCLUDED.share_class,
       isin = EXCLUDED.isin, canonical_isin = EXCLUDED.canonical_isin,
       futures_sectype_quarterly = EXCLUDED.futures_sectype_quarterly,
       futures_sectype_perpetual = EXCLUDED.futures_sectype_perpetual,
       futures_assetcode = EXCLUDED.futures_assetcode, updated_at = now();

-- Алиасы. Имена из справок УК взяты не из головы, а из реальных значений
-- fund_holdings_history.asset_name на проде (03.09.2026) — их у Сбера восемь штук.
INSERT INTO issuer_aliases (alias_type, alias_value, issuer_id, secid, source)
SELECT v.t, v.val, i.issuer_id, v.secid, 'seed_069'
FROM issuers i,
     (VALUES
        ('secid',           'SBER',                                              'SBER'),
        ('secid',           'SBERP',                                             'SBERP'),
        ('isin',            'RU0009029540',                                      'SBER'),
        ('isin',            'RU0009029557',                                      'SBERP'),
        ('sectype',         'SR',                                                'SBER'),
        ('sectype',         'SBERF',                                             'SBER'),
        ('sectype',         'SP',                                                'SBERP'),
        ('assetcode',       'SBRF',                                              'SBER'),
        ('assetcode',       'SBERF',                                             'SBER'),
        ('assetcode',       'SBPR',                                              'SBERP'),
        ('smartlab',        'SBER',                                              'SBER'),
        ('display_name',    'Сбербанк',                                          NULL),
        ('display_name',    'Сбербанк (прив)',                                   'SBERP'),
        ('display_name',    'Сбербанк (вечн)',                                   'SBER'),
        ('fund_asset_name', 'Сбербанк',                                          NULL),
        ('fund_asset_name', 'Публичное акционерное общество "Сбербанк России"',  NULL),
        ('fund_asset_name', 'ПАО "Сбербанк России"',                             NULL),
        ('fund_asset_name', 'Сбербанк России, акция об.',                        'SBER'),
        ('fund_asset_name', 'Сбербанк, ао, 10301481B',                           'SBER'),
        ('fund_asset_name', 'Сбербанк-п',                                        'SBERP'),
        ('fund_asset_name', 'Сбербанк России, акция прив.',                      'SBERP'),
        ('fund_asset_name', 'Сбербанк, ап, 20301481B',                           'SBERP')
     ) AS v(t, val, secid)
WHERE i.issuer_key = 'SBER'
ON CONFLICT (alias_type, alias_value) DO UPDATE
   SET issuer_id = EXCLUDED.issuer_id, secid = EXCLUDED.secid;

COMMENT ON TABLE issuer_aliases IS
  'Резолвер: любой ключ (тикер, ISIN, sectype, assetcode, имя из справки УК, подпись '
  'на сайте) → эмитент и, где ключ это различает, конкретная бумага. Заведён потому, '
  'что поиск по подстроке «Сбер» в fund_holdings_history возвращает облигации Сбербанка.';
COMMENT ON COLUMN company_metrics.note IS
  'zero_as_missing — smart-lab напечатал 0.00 там, где данных нет (чистый долг Роснефти '
  'за 2023). value в этом случае NULL, сырой текст в raw_text.';
COMMENT ON COLUMN company_shareholders.structure_as_of IS
  'Дата, на которую smart-lab обновлял структуру. У Сбера 08.05.2020 — там до сих пор '
  '«Американские инвесторы 33%». Отдавать агенту структуру БЕЗ этой даты нельзя.';
