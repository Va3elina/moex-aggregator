-- Гейт «этот релиз данных уже анонсирован» для скана data_release_scan.py
-- (уведомления «вышли новые данные»: Telegram-рассылка привязавшим бота +
-- строка type='data_release' в ленте аномалий сайта).
--
-- Одна строка = один анонсированный период датасета. Продюсер делает
-- INSERT ... ON CONFLICT DO NOTHING RETURNING — вставилось → релиз новый,
-- шлём уведомления; нет → уже анонсирован (пере-скан молчит).
--
-- Идемпотентно. Применение:
--   cat db/migrations/051_data_releases.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS data_releases (
  dataset      VARCHAR(32)  NOT NULL,  -- 'fund_trades' | 'cbr_flows'
  period       DATE         NOT NULL,  -- месяц релиза (1-е число) / period_end_date ОРФР
  announced_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (dataset, period)
);
