-- maint_register_futures_prefixes.sql
--
-- Idempotent maintenance: регистрирует «забытые» prefix'ы фьючерсных
-- контрактов в таблице instruments.
--
-- Контекст:
--   У фьючерсных серий на МосБирже квартальные/ежемесячные контракты.
--   Каждый контракт имеет полный код типа `MNH6` (Магнит-март-2026):
--     - В candles.secid хранится полный код («MNH6»).
--     - В candles.sec_id хранится 3-буквенный prefix без года («MNH»).
--   В таблице instruments один row на prefix — `(sec_id='MNH', sectype='MN', ...)`.
--
--   Фетчер `fetch_candles_futures_realtime.py` автоматически находит активный
--   контракт через `_find_active_contract` + `_generate_candidates` — пробует
--   все 12 буквенных кодов месяцев (F G H J K M N Q U V X Z) для prefix'а
--   текущего и следующего года. И корректно скачивает свечи в БД.
--
--   НО: до 2026-05-14 фетчер не регистрировал новые prefix'ы в instruments
--   автоматически. Результат: за полгода MOEX выпустил новые серии (`MNM` =
--   Магнит-июнь, `MGM` = ММК-июнь, `GKM` = Норникель-июнь, и т.д.), фетчер их
--   качал, но в instruments их не было. API `/api/chart/{sec_id}` берёт
--   список prefix'ов из instruments → не видел новые контракты → график
--   обрывался на дате expiration последнего зарегистрированного prefix'а.
--
-- Этот скрипт безопасно (ON CONFLICT DO NOTHING) подписывает все prefix'ы
-- из candles, которых нет в instruments. Имя/группу/сектор/iss_code берёт
-- от существующего prefix того же sectype.
--
-- Запускать можно сколько угодно раз — idempotent.

BEGIN;

WITH new_prefixes AS (
  SELECT DISTINCT
    c.sec_id,
    LEFT(c.sec_id, 2) AS sectype_inferred
  FROM candles c
  WHERE c.type = 'futures'
    AND char_length(c.sec_id) = 3
    AND RIGHT(c.sec_id, 1) IN ('F','G','H','J','K','M','N','Q','U','V','X','Z')
    AND NOT EXISTS (
      SELECT 1 FROM instruments i WHERE i.sec_id = c.sec_id
    )
),
to_insert AS (
  SELECT
    np.sec_id,
    ref.name,
    ref.sectype,
    ref.type,
    ref."group",
    ref.iss_code,
    ref.sector
  FROM new_prefixes np
  CROSS JOIN LATERAL (
    SELECT name, sectype, type, "group", iss_code, sector
    FROM instruments
    WHERE sectype = np.sectype_inferred AND type = 'futures'
    LIMIT 1
  ) ref
)
INSERT INTO instruments (sec_id, name, sectype, type, "group", iss_code, sector)
SELECT sec_id, name, sectype, type, "group", iss_code, sector
FROM to_insert
ON CONFLICT (sec_id) DO NOTHING;

COMMIT;
