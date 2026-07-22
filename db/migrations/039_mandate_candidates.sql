-- mandate_candidates — вынужденные потоки/мандаты (законы, указы, правила регулятора),
-- найденные еженедельным Routine-сканом (frame-mandate-scan) как кандидаты для
-- гипотез Pine Script в исследовании "структурных edge" на рынке РФ (запрос Вадима,
-- 2026-07-22). Обнаружение — WebSearch-сетка участник×домен права; отбор — 6 критериев
-- (источник / тип принуждения / статус / механический триггер / Pine-тестируемость / шум).
--
-- dedup: source_ref — стабильный идентификатор акта (напр. "Указание Банка России №6433-У"),
-- уникальный индекс не даёт повторно уведомлять про уже найденный мандат при следующем
-- еженедельном скане (ON CONFLICT DO NOTHING в api/routers/mandate_scan.py).
--
-- notified_at IS NULL после INSERT означает "отправка в Telegram не подтверждена" —
-- либо ещё не пробовали, либо send упал (см. api/routers/mandate_scan.py) — запись НЕ
-- теряется молча, можно проверить вручную (см. idx_mandate_candidates_unnotified).
--
-- Идемпотентно. Применение:
--   cat db/migrations/039_mandate_candidates.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS mandate_candidates (
  id                    SERIAL PRIMARY KEY,
  source_ref            VARCHAR(200) NOT NULL,   -- "Указание Банка России №6433-У от 01.06.2023"
  source_url            TEXT,
  mandate_type          VARCHAR(24) NOT NULL
    CHECK (mandate_type IN (
      'physical_necessity', 'government_decree', 'regulator_rule',
      'index_methodology', 'corporate_policy'
    )),
  participant           VARCHAR(24) NOT NULL
    CHECK (participant IN (
      'exporters', 'banks', 'insurers', 'pension_funds', 'nonresidents',
      'market_makers', 'state_entities', 'issuers', 'retail', 'exchange'
    )),
  sector                VARCHAR(64),             -- нефтегаз/металлурги/н/д и т.д.
  asset                 VARCHAR(120) NOT NULL,   -- USDRUB, ОФЗ, конкретный тикер, IMOEX...
  status_now            VARCHAR(20) NOT NULL DEFAULT 'active'
    CHECK (status_now IN ('active', 'suspended', 'zeroed', 'pending')),
  mechanical_trigger    BOOLEAN NOT NULL DEFAULT false,
  trigger_description   TEXT,
  pine_testable         VARCHAR(10) NOT NULL DEFAULT 'low'
    CHECK (pine_testable IN ('high', 'medium', 'low')),
  hypothesis            TEXT NOT NULL,
  durability_note       TEXT,
  found_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  notified_at           TIMESTAMPTZ
);

-- Dedup — повторная находка того же акта не долетает до Telegram второй раз.
CREATE UNIQUE INDEX IF NOT EXISTS uq_mandate_candidates_source_ref ON mandate_candidates (source_ref);
-- Ручная/мониторинговая проверка "что не долетело до Telegram".
CREATE INDEX IF NOT EXISTS idx_mandate_candidates_unnotified ON mandate_candidates (found_at) WHERE notified_at IS NULL;
-- Приоритизация по тестируемости при разборе очереди гипотез.
CREATE INDEX IF NOT EXISTS idx_mandate_candidates_pine ON mandate_candidates (pine_testable, found_at DESC);
