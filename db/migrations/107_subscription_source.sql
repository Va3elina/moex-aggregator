-- 107: откуда человек пришёл к оплате.
--
-- source — призыв, с которого перешли на тарифы: paywall (окно «Доступно на
-- тарифе»), delay_banner (плашка «Данные с задержкой»), header (кнопка Plus),
-- landing, profile; pricing — прямой заход на тарифы без метки.
-- source_feature — что упёрлось в замок (название функции или индикатор).
-- Пишется на первую строку из checkout / СБП / триала (api/billing/source.py);
-- продления, подарочный Pro и подписки до 2026-10-14 — NULL.

ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS source         VARCHAR(16);
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS source_feature VARCHAR(64);
