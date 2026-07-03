-- Каналы доставки алертов (TradingView-подобные оповещения, фаза 1).
--
-- До сих пор личный алерт доставлялся ТОЛЬКО в Telegram (если привязан).
-- Теперь каждый алерт несёт набор каналов, а у юзера есть дефолт для новых:
--   • telegram — пуш в @framesignalbot (как раньше);
--   • site     — тост на сайте + вкладка «Мои алерты» в колоколе (источник
--                alert_fires — уже пишется, нового хранилища не нужно);
--   • email    — письмо с noreply (доставка включается после настройки SMTP).
--
-- CSV-формат (как alerts.fund_ids) — без отдельной таблицы, поднабор
-- {telegram,site,email}, непустой. Дефолт 'telegram' у alerts сохраняет
-- поведение ВСЕХ существующих алертов байт-в-байт (миграция обратносовместима).
--
-- Идемпотентно. Применение:
--   cat db/migrations/019_alert_channels.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

ALTER TABLE alerts
  ADD COLUMN IF NOT EXISTS channels VARCHAR(32) NOT NULL DEFAULT 'telegram';

-- Дефолт-каналы для НОВЫХ алертов юзера (пресет в модалке создания). Сайт
-- включён по умолчанию — он бесплатный и не требует привязки/SMTP.
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS notify_channels_default VARCHAR(32) NOT NULL DEFAULT 'telegram,site';
