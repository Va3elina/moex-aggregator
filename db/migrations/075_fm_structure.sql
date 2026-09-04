-- Структура под FinanceMarker: карточка компании = один источник, понятные таблицы.
--
-- Решение Вадима 04.09.2026: уходим со smart-lab полностью, качаем у FM максимум.
-- Причина не в глубине (хотя 2011–2026 против 5 лет), а в классе ошибок: парсер HTML
-- за день дал ноль-вместо-пропуска, сдвиг колонок на год, кириллическую «К» вместо Q,
-- двойное экранирование и задвоение Татнефти — каждая из разбора вёрстки.
--
-- ⚠️ ДАННЫЕ SMART-LAB СТИРАЮТСЯ. Это осознанное решение, а не уборка: пополнять их
-- мы не будем, а два словаря кодов (176 у smart-lab против 114 у FM, совпадает 13)
-- в одной таблице означают, что читающий должен знать, из какого источника строка, и
-- переводить коды в уме. Восстановимо: парсер Company/smartlab_parser.py остаётся в
-- репозитории, прогон занимает 12 минут.
--
-- ⚠️ ЧТО ТЕРЯЕМ ВМЕСТЕ С НИМИ: 163 отраслевых кода, которых у FM в reports нет —
-- кредитный портфель банка, выпуск алюминия, средний чек, биомасса в воде. У FM они
-- лежат в operations (справочник на 620 кодов), и эта миграция заводит под них
-- отдельную таблицу. Насколько они заполнены — покажет первый полный обход.
--
-- ПОЧЕМУ РАСКРЫТИЕ ОТДЕЛЬНО, А НЕ В КАРТОЧКЕ. Раскрытие — ЛЕНТА ПО ВСЕМУ РЫНКУ
-- (~15-17 событий в день у всех эмитентов сразу), а карточка — свойство одной
-- компании. Их разрезы не совпадают: лента отвечает «что случилось сегодня», карточка
-- «что известно про эту компанию». Сложить их в одну таблицу значит потерять первый
-- вопрос: по компании события найдутся, а «покажи всё за сегодня» превратится в скан.
--
-- Применение на проде:
--   docker cp db/migrations/075_fm_structure.sql frame-db-1:/tmp/
--   docker exec frame-db-1 psql -U postgres -d moex_db -f /tmp/075_fm_structure.sql

-- ── 1. Чистка smart-lab ──────────────────────────────────────────────────────
DELETE FROM company_metrics      WHERE source = 'smartlab';
DELETE FROM company_dividends    WHERE source = 'smartlab';
DELETE FROM company_shareholders WHERE source = 'smartlab';
DELETE FROM company_documents    WHERE source = 'smartlab';
-- Тезисы «факторы роста и падения» существуют только у smart-lab; от них отказались
-- сознательно (Вадим, 04.09). Таблицу оставляем: она пригодится, если появится
-- источник датированных мнений.
DELETE FROM company_theses       WHERE source = 'smartlab';

-- ── 2. Операционные показатели: то, чем компания живёт ───────────────────────
-- Отдельно от company_metrics, потому что у них СВОИ единицы измерения и своя
-- разрядность: «декалитры», «млн тонн», «₽ на пользователя». Складывать выручку в
-- рублях и вылов в тоннах в одну колонку value без единицы — способ однажды сложить
-- их в отчёте.
CREATE TABLE IF NOT EXISTS company_operations (
    secid        VARCHAR(24) NOT NULL REFERENCES issuer_securities(secid) ON DELETE CASCADE,
    metric_id    VARCHAR(24) NOT NULL,   -- код из справочника FM (620 показателей)
    period_type  VARCHAR(10) NOT NULL,   -- year | quarter | ltm
    period_label VARCHAR(20) NOT NULL,
    value        NUMERIC,                -- стандартизованное значение
    unit         VARCHAR(40),            -- стандартизованная единица
    orig_value   NUMERIC,                -- как в отчёте компании
    orig_unit    VARCHAR(40),
    source       VARCHAR(24) NOT NULL DEFAULT 'financemarker',
    first_seen   DATE NOT NULL DEFAULT CURRENT_DATE,
    last_seen    DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (secid, metric_id, period_type, period_label, source, first_seen)
);
CREATE INDEX IF NOT EXISTS idx_company_operations_lookup
    ON company_operations (secid, metric_id, period_label);

-- Справочник 620 операционных показателей: код → название, описание, единица.
CREATE TABLE IF NOT EXISTS fm_operation_metrics (
    metric_id   VARCHAR(24) PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    description TEXT,
    unit        VARCHAR(40),
    amount      BIGINT,                  -- разрядность: 1, 1000, 1000000
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── 3. Сделки инсайдеров ─────────────────────────────────────────────────────
-- ⚠️ Здесь ЕСТЬ доля до и после сделки — то, чего нет ни в одном другом нашем
-- источнике. Смена доли крупного владельца видна прямо из строки, без вывода.
CREATE TABLE IF NOT EXISTS company_insider_trades (
    secid            VARCHAR(24) NOT NULL REFERENCES issuer_securities(secid) ON DELETE CASCADE,
    transaction_date DATE        NOT NULL,
    filling_date     DATE,
    insider          VARCHAR(300) NOT NULL,
    insider_role     VARCHAR(120),
    insider_title    VARCHAR(200),
    transaction_type VARCHAR(40),
    amount           NUMERIC,            -- количество акций
    price            NUMERIC,
    value            NUMERIC,            -- объём в деньгах
    own_before       NUMERIC,
    own_after        NUMERIC,
    market_trade     BOOLEAN,
    approximate      BOOLEAN,            -- флаг примерного расчёта у самого FM
    link             TEXT,
    source           VARCHAR(24) NOT NULL DEFAULT 'financemarker',
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (secid, transaction_date, insider, transaction_type)
);
CREATE INDEX IF NOT EXISTS idx_insider_trades_date
    ON company_insider_trades (transaction_date DESC);

-- ── 4. Сводка: то, что FM посчитал за нас ────────────────────────────────────
-- Рост 3/5 лет по десяти показателям, Грэм, Линч, консенсус аналитиков, дивидендный
-- рейтинг, среднее закрытие гэпа. Одна строка на компанию — это СНИМОК, а не ряд:
-- «средний рост выручки за 5 лет» пересчитывается каждый раз заново.
CREATE TABLE IF NOT EXISTS company_summary (
    secid      VARCHAR(24) PRIMARY KEY REFERENCES issuer_securities(secid) ON DELETE CASCADE,
    payload    JSONB       NOT NULL,
    source     VARCHAR(24) NOT NULL DEFAULT 'financemarker',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── 5. История числа акций ───────────────────────────────────────────────────
-- ⚠️ Нужна отдельно, потому что делает сопоставимыми показатели «на акцию» через
-- допэмиссии и обратные выкупы. У Сбербанка 118 записей с 2011 года.
CREATE TABLE IF NOT EXISTS company_shares (
    secid      VARCHAR(24) NOT NULL REFERENCES issuer_securities(secid) ON DELETE CASCADE,
    year       SMALLINT    NOT NULL,
    month      SMALLINT    NOT NULL,
    num        BIGINT,
    source     VARCHAR(24) NOT NULL DEFAULT 'financemarker',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (secid, year, month, source)
);

-- ── 6. Лента раскрытия: события по всему рынку ───────────────────────────────
CREATE TABLE IF NOT EXISTS disclosure_events (
    id         BIGSERIAL PRIMARY KEY,
    code       VARCHAR(24) NOT NULL,     -- тикер как его называет FM
    secid      VARCHAR(24),              -- наш, после резолва через справочник
    event_date DATE        NOT NULL,
    category   VARCHAR(40) NOT NULL,     -- REPORT | EVENT | OPERATION | INSIDER… 
    type       VARCHAR(24),              -- МСФО | РСБУ | NA
    period     VARCHAR(12),
    year       SMALLINT,
    month      SMALLINT,
    title      TEXT        NOT NULL,
    link       TEXT,
    source     VARCHAR(24) NOT NULL DEFAULT 'financemarker',
    -- Признак «эта карточка обновлена по этому событию»: раскрытие говорит, что у
    -- компании вышел отчёт, и это дешёвый сигнал обновить именно её, а не всех.
    handled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, code, event_date, category, title)
);
CREATE INDEX IF NOT EXISTS idx_disclosure_new
    ON disclosure_events (handled_at NULLS FIRST, event_date DESC);
CREATE INDEX IF NOT EXISTS idx_disclosure_secid
    ON disclosure_events (secid, event_date DESC);

COMMENT ON TABLE disclosure_events IS
  'Лента раскрытия по всему рынку (~15-17 событий в день). Отдельно от карточки '
  'намеренно: карточка отвечает «что известно про компанию», лента — «что случилось '
  'сегодня». Сложенные вместе, они теряют второй вопрос.';
COMMENT ON TABLE company_operations IS
  'Операционные показатели со своими единицами измерения. Отдельно от company_metrics: '
  'выручка в рублях и вылов в тоннах не должны лежать в одной колонке без единицы.';
