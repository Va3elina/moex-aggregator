-- История торгов облигаций с MOEX ISS (21.10.2026): для «Сделок фондов» → «По бумаге» (режим «Сделки»
-- на цене) и вообще аналитики облигаций. Отдельно от candles: цена в % от номинала, номинал меняется
-- (амортизация), есть НКД и дни без сделок (оценка без свечи); candles (18 ГБ) держит акции/фьючерсы.
-- Берём С ИЗБЫТКОМ: все 50 колонок history ISS, полный description выпуска, все купоны/амортизации/оферты.
--   cat db/migrations/113_bond_history.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

-- Дневная история (history/engines/stock/markets/bonds, marketprice_board=1 → одна строка в день, основной режим)
CREATE TABLE IF NOT EXISTS bond_history (
    secid                   VARCHAR(40) NOT NULL,
    trade_date              DATE        NOT NULL,
    boardid                 VARCHAR(12) NOT NULL,
    isin                    VARCHAR(12),
    shortname               VARCHAR(64),
    numtrades               INTEGER,
    value                   NUMERIC,      -- оборот, ₽
    low                     NUMERIC,      -- цены — % от номинала
    high                    NUMERIC,
    close                   NUMERIC,
    legalcloseprice         NUMERIC,
    accint                  NUMERIC,      -- НКД на бумагу, в валюте номинала
    waprice                 NUMERIC,
    yieldclose              NUMERIC,
    open                    NUMERIC,
    volume                  NUMERIC,      -- штуки
    marketprice2            NUMERIC,
    marketprice3            NUMERIC,
    admittedquote           NUMERIC,
    mp2valtrd               NUMERIC,
    marketprice3tradesvalue NUMERIC,
    admittedvalue           NUMERIC,
    matdate                 DATE,
    duration                NUMERIC,      -- дни
    yieldatwap              NUMERIC,
    iricpiclose             NUMERIC,
    beiclose                NUMERIC,
    couponpercent           NUMERIC,
    couponvalue             NUMERIC,
    buybackdate             DATE,
    lasttradedate           DATE,
    facevalue               NUMERIC,      -- текущий номинал (после амортизаций)
    currencyid              VARCHAR(8),
    cbrclose                NUMERIC,
    yieldtooffer            NUMERIC,
    yieldlastcoupon         NUMERIC,
    offerdate               DATE,
    faceunit                VARCHAR(8),
    tradingsession          SMALLINT,
    calloptiondate          DATE,
    calloptionyield         NUMERIC,
    calloptionduration      NUMERIC,
    putoptiondate           DATE,
    dateyieldfromissuer     DATE,
    trade_session_date      DATE,
    zspread                 NUMERIC,
    zspreadatwaprice        NUMERIC,
    bondtype                TEXT,
    bondsubtype             TEXT,
    coupon_details          TEXT,
    facevalue_type          TEXT,
    loaded_at               TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (secid, trade_date, boardid)
);
CREATE INDEX IF NOT EXISTS idx_bond_history_isin_date ON bond_history (isin, trade_date);
CREATE INDEX IF NOT EXISTS idx_bond_history_date ON bond_history (trade_date);

-- Справочник выпуска (securities/<SECID>.json → description + boards)
CREATE TABLE IF NOT EXISTS bond_ref (
    isin                    VARCHAR(12) PRIMARY KEY,
    secid                   VARCHAR(40) NOT NULL,
    shortname               VARCHAR(64),
    name                    TEXT,
    regnumber               VARCHAR(64),
    emitter_id              INTEGER,
    issuedate               DATE,
    matdate                 DATE,
    initialfacevalue        NUMERIC,
    faceunit                VARCHAR(8),
    issuesize               NUMERIC,
    couponfrequency         NUMERIC,
    bond_type               TEXT,
    bond_subtype            TEXT,
    secsubtype              TEXT,
    coupon_benchmark        TEXT,
    coupon_benchmark_spread TEXT,
    listlevel               SMALLINT,
    is_qualified            BOOLEAN,
    is_subord               BOOLEAN,
    has_default             BOOLEAN,
    has_technical_default   BOOLEAN,
    history_from            DATE,
    history_till            DATE,
    description             JSONB,        -- весь description ISS как есть
    updated_at              TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bond_ref_secid ON bond_ref (secid);

-- Купоны, амортизации, оферты (statistics/.../bondization/<SECID>.json)
CREATE TABLE IF NOT EXISTS bond_events (
    secid        VARCHAR(40) NOT NULL,
    kind         VARCHAR(16) NOT NULL,     -- coupon | amortization | offer
    event_date   DATE        NOT NULL,
    isin         VARCHAR(12),
    value        NUMERIC,                  -- сумма на бумагу, в валюте номинала
    value_prc    NUMERIC,                  -- % от номинала (купон — ставка, амортизация — доля)
    value_rub    NUMERIC,
    facevalue    NUMERIC,
    raw          JSONB,                    -- вся строка ISS
    updated_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (secid, kind, event_date)
);
CREATE INDEX IF NOT EXISTS idx_bond_events_isin ON bond_events (isin, kind, event_date);
