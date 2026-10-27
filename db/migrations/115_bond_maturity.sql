-- Даты погашения облигаций из составов фондов (27.10.2026): «Сделки фондов» не должны считать
-- погашение продажей. Облигация, погашенная между двумя справками, исчезает из состава, и дифф
-- рисовал её как полную распродажу (РусГидро БО-П06, погашена 15.09.2026: −720.8 млн ₽ в
-- «Чистых продажах» за сентябрь у Первой смешанной).
-- Наполняет Funds/sync_bond_maturities.py (шаг оркестратора bond_maturity_daily) из ISS.
-- is_bond: TRUE облигация, FALSE не облигация (акция, пай), NULL ISS бумагу не знает.
-- Таблицу создают и API на старте, и сам скрипт (CREATE IF NOT EXISTS), эта миграция для истории.
--   cat db/migrations/115_bond_maturity.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS bond_maturity (
    isin        VARCHAR(20) PRIMARY KEY,
    secid       VARCHAR(40),
    shortname   VARCHAR(64),
    matdate     DATE,
    is_bond     BOOLEAN,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bond_maturity_matdate ON bond_maturity (matdate) WHERE matdate IS NOT NULL;
