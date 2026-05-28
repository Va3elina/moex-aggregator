-- Phase 2: поля подтверждения email (SMTP Yandex 360)
-- Применять: cat db/add_email_verification_fields.sql | ssh ... 'docker exec -i frame-db-1 psql -U postgres -d moex_db'
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verify_code VARCHAR(6);
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verify_expires_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verify_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verify_sent_at TIMESTAMPTZ;
