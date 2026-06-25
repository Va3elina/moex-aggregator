-- Лента всплывающих аномалий на сайте (тосты в углу + колокол в хедере).
--
-- Источник — ГИБРИД, обе ветки переиспользуют существующие detector-функции
-- движка сигналов (signals/detectors/oi.py, funds.py — те же, что шлют пуши в бот):
--   • scope='public'   — тонкий маркет-вайд скан (compute_position_atr по всем
--     OI_ASSETS × FIZ/YUR + compute_fund_flow_atr по категориям) при пороге ×3.
--     Не привязан к подпискам → ровное покрытие, ВСЕ видят одно и то же.
--   • scope='personal' — write-through из alerts_run.py при реальном срабатывании
--     личного алерта (тот же fire, что ушёл в Telegram). user_id = владелец.
--
-- ВИДИМОСТЬ не делится по силе/тарифу — все видят одну ленту (решение Вадима).
-- Тариф влияет ТОЛЬКО на клик «Открыть график» (gated-цель → апселл + дефолт,
-- считается в момент клика по features.py) и на «Поставить сигнал» (квота).
--
-- НЕ ЗАДВОИТЬ: ключ события = (type·asset_id·clgroup·direction·signal_date),
-- ИГНОРИРует scope/user. Публичная и личная строки одного события сосуществуют
-- в таблице; /feed отдаёт DISTINCT ON(ключ) с приоритетом личной → событие ровно
-- раз. Один id-простор → чистый seen-трекинг и `since=`.
--
-- Идемпотентно. Применение:
--   cat db/migrations/015_anomaly_feed.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS anomalies (
  id             BIGSERIAL PRIMARY KEY,
  scope          VARCHAR(8)  NOT NULL DEFAULT 'public',  -- 'public' | 'personal'
  user_id        INTEGER REFERENCES users(id) ON DELETE CASCADE,  -- NULL для public
  type           VARCHAR(24) NOT NULL,        -- 'oi_move'|'funds_flow'|'oi_participants'|'oi_zscore'
  asset_id       VARCHAR(24) NOT NULL,        -- secid фьючерса / ключ категории фондов
  asset_name     VARCHAR(120),                -- человекочитаемо в шапку тоста
  clgroup        VARCHAR(4),                  -- 'FIZ'|'YUR'|'ALL'|NULL
  direction      VARCHAR(4),                  -- 'up'|'down'
  headline       TEXT        NOT NULL,        -- суть: «Физлица резко нарастили лонг»
  context        TEXT,                        -- деталь: «×5.2 к обычному, максимум с марта»
  severity_value NUMERIC,                     -- ×N или z — ТОЛЬКО для отображения/сорта, НЕ гейт
  signal_date    DATE        NOT NULL DEFAULT CURRENT_DATE,  -- торговый день аномалии
  deep_link      JSONB       NOT NULL,        -- {route,secid,interval,period,clgroup,mode}
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Дедуп НА ЗАПИСИ: одна строка на (scope·владелец·тип·актив·группа·сторона·день).
-- Продюсер пишет ON CONFLICT DO NOTHING → пере-скан в тот же день не плодит копий
-- (зеркало гейта «новый торговый день» в alerts_run).
CREATE UNIQUE INDEX IF NOT EXISTS uq_anomalies_dedup ON anomalies
  (scope, COALESCE(user_id, 0), type, asset_id,
   COALESCE(clgroup, ''), COALESCE(direction, ''), signal_date);

-- Лента: свежие сначала.
CREATE INDEX IF NOT EXISTS idx_anomalies_feed ON anomalies (created_at DESC);
-- Личная ветка: быстрый выбор по владельцу.
CREATE INDEX IF NOT EXISTS idx_anomalies_user ON anomalies (user_id, created_at DESC)
  WHERE user_id IS NOT NULL;

-- Пер-юзерное состояние ленты на users:
--   last_seen_anomaly_id   — монотонный маркер «всё ≤ него просмотрено» (дедуп
--                            тостов между визитами/реконнектами SSE, основа `since=`);
--   anomaly_toasts_enabled — тумблер «Показывать всплывающие аномалии» (вкл по
--                            умолчанию). Кнопка «выключить» ставит false; обратно
--                            включает сам юзер в настройках. Гость — localStorage.
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS last_seen_anomaly_id   BIGINT,
  ADD COLUMN IF NOT EXISTS anomaly_toasts_enabled BOOLEAN NOT NULL DEFAULT true;
