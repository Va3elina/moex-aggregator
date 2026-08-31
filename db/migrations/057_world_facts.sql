-- Второй мозг: датированные факты о мире, которые генератор постов может спросить
-- «что было верно на такую-то дату» (research/content_pipeline_v2/MACRO_BRAIN.md).
--
-- ЗАЧЕМ. Черновик 700 писал: «что именно спровоцировало пробой — санкции, ожидания
-- по отчётности или что-то ещё — в переданных данных не указано». Модель не «не
-- смогла объяснить» — ей не показали мир: в бриф попадала ОДНА новость. При этом
-- вокруг того же 14.07.2026 в архиве лежат запрет Китая на экспорт гелия, прогресс
-- переговоров Россия-Иран по газу, газ в Европе выше $600 и атака БПЛА на промзону
-- Салавата. Эта таблица — про то, чтобы такой контекст был доступен по запросу.
--
-- ⚠️ ПОЧЕМУ НЕ ГРАФ И НЕ ВЕКТОРА. Вопрос к этой таблице — «что ДЕЙСТВОВАЛО на дату»,
-- а не «что похоже». Вектор вернёт похожее, а нам нужно актуальное на момент:
-- ровно на этом сломался черновик 845, где сигнал из будущего был подан как
-- известный на момент события. Отсюда обязательная отсечка по valid_from/valid_until
-- в каждом запросе. Граф связей оправдан позже — когда факты начнут противоречить
-- друг другу; на старте плоская таблица покрывает задачу целиком.
--
-- ⚠️ ИСТОЧНИКИ РАЗНОЙ ПРИРОДЫ, и это отражено в confidence:
--   structured — из своей БД (macro_data: ключевая ставка, M2, ВВП). Точные даты,
--                точные значения, конфликтов быть не может → confidence = 1.00;
--   news       — извлечённое из news_archive. Даты и формулировки приблизительны,
--                факты могут противоречить друг другу → confidence ниже, и
--                обязательно source_url, чтобы можно было проверить.
-- Смешивать их в одной колонке statement можно, а вот доверять одинаково — нет.

CREATE TABLE IF NOT EXISTS world_facts (
    id            BIGSERIAL PRIMARY KEY,
    -- Детерминированный ключ пересборки: 'KEY_RATE:2026-07-24'. Позволяет гонять
    -- сборщик сколько угодно раз — повторный прогон обновит, а не размножит.
    fact_key      TEXT        NOT NULL UNIQUE,
    statement     TEXT        NOT NULL,          -- факт одной фразой, человеческим языком
    kind          TEXT        NOT NULL,          -- ставка / инфляция / ввп / санкции / ...
    entities      TEXT[]      NOT NULL DEFAULT '{}',  -- тикеры, страны, институты
    valid_from    DATE        NOT NULL,
    valid_until   DATE,                          -- NULL = действует до сих пор
    source        TEXT        NOT NULL,          -- structured | news
    source_url    TEXT,
    confidence    NUMERIC(3,2) NOT NULL DEFAULT 1.00,
    superseded_by BIGINT REFERENCES world_facts(id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Главный запрос второго мозга: «что действовало на дату X».
-- WHERE valid_from <= :d AND (valid_until IS NULL OR valid_until >= :d)
CREATE INDEX IF NOT EXISTS idx_world_facts_validity
    ON world_facts (valid_from, valid_until);
CREATE INDEX IF NOT EXISTS idx_world_facts_kind ON world_facts (kind, valid_from DESC);
CREATE INDEX IF NOT EXISTS idx_world_facts_entities ON world_facts USING GIN (entities);

COMMENT ON COLUMN world_facts.valid_until IS
  'NULL = факт действует до сих пор. Запрос ОБЯЗАН фильтровать по дате новости, '
  'иначе в бриф попадёт факт из будущего — ровно ошибка черновика 845.';
COMMENT ON COLUMN world_facts.confidence IS
  '1.00 у фактов из своей БД (structured), ниже у извлечённых из новостей (news).';
