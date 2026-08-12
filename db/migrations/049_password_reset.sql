-- 049: восстановление пароля по коду из письма.
--
-- Механика намеренно повторяет подтверждение email (email_verify_*), а не
-- вводит вторую: там уже отлажены TTL, счётчик неверных попыток и кулдаун
-- ресенда, и держать рядом два разных механизма одноразовых кодов — верный
-- способ починить один и забыть про другой.
--
-- Почему колонки в users, а не отдельная таблица токенов: код одноразовый и
-- живёт 30 минут, история сбросов нам не нужна, а параллельных запросов на
-- один аккаунт быть не должно (каждый новый код затирает предыдущий — это
-- и есть желаемое поведение).
--
-- Колонки NULLable: у большинства пользователей сброс не запрашивался ни
-- разу, а attempts с DEFAULT 0 — чтобы код проверки не разбирал NULL.
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_reset_code VARCHAR(6);
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_reset_expires_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_reset_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_reset_sent_at TIMESTAMPTZ;
