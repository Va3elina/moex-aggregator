-- 013_futures_contracts.sql
--
-- Календарь экспираций фьючерсных контрактов MOEX (из ISS).
-- Источник:
--   список:  https://iss.moex.com/iss/engines/futures/markets/forts/securities.json
--            (SECID, ASSETCODE, LASTTRADEDATE=lsttrade, LASTDELDATE=lstdeldate)
--   деталь:  https://iss.moex.com/iss/securities/{secid}.json?iss.only=description
--            (FRSTTRADE, EXPIRATION_TIME) — опционально, в --backfill
--
-- Назначение: КАЛЕНДАРНЫЙ ролловер фьючерсов вместо объёмного. Фронт-контракт =
-- ближайший непросроченный по lsttrade; смена — на след. день после экспирации.
-- Это даёт 0 преждевременных роллов и 0 пропусков (см.
-- api/services/contract_calendar.py).
--
-- Связь кодов с остальными таблицами:
--   secid   ('BRN6')  — полный код контракта  (= candles.secid)
--   sec_id  ('BRN')   — prefix+месяц           (= candles.sec_id, = instruments.sec_id)
--   sectype ('BR')    — базовый тикер          (= instruments.sectype, = open_interest.sectype)
--
-- Идемпотентно: запускать сколько угодно раз.

CREATE TABLE IF NOT EXISTS futures_contracts (
    secid           VARCHAR(32) PRIMARY KEY,            -- "BRN6" полный код контракта
    sectype         VARCHAR(20) NOT NULL,               -- "BR" базовый тикер
    sec_id          VARCHAR(20) NOT NULL,               -- "BRN" prefix+месяц (без года)
    assetcode       VARCHAR(20),                        -- ISS ASSETCODE ("GAZR") — справочно
    frsttrade       DATE,                               -- первый день торгов (из description)
    lsttrade        DATE,                               -- последний день торгов (= экспирация) — КЛЮЧ ролла
    lstdeldate      DATE,                               -- дата поставки/исполнения
    expiration_time TIME,                               -- время экспирации (обычно 19:00 МСК)
    is_perpetual    BOOLEAN NOT NULL DEFAULT FALSE,     -- вечный фьючерс (нет экспирации)
    is_traded       BOOLEAN NOT NULL DEFAULT TRUE,      -- сейчас в активном листинге ISS
    last_seen       TIMESTAMP,                          -- когда последний раз видели в ISS-листинге
    updated_at      TIMESTAMP DEFAULT now()
);

-- Поиск фронт-контракта: WHERE sectype=? AND lsttrade>=? ORDER BY lsttrade
CREATE INDEX IF NOT EXISTS idx_futures_contracts_roll
    ON futures_contracts (sectype, lsttrade);
