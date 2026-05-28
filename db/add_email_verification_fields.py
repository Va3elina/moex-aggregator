# db/add_email_verification_fields.py
"""
Миграция: поля подтверждения email в таблицу users (Phase 2, SMTP Yandex 360).

Добавляет колонки:
  - email_verify_code        VARCHAR(6)       — 6-значный код из письма
  - email_verify_expires_at  TIMESTAMPTZ      — срок действия кода (30 мин)
  - email_verify_attempts    INTEGER NOT NULL DEFAULT 0  — неверные попытки ввода
  - email_verify_sent_at     TIMESTAMPTZ      — время последней отправки (кулдаун resend)

Только для email+password юзеров. OAuth-юзеры приходят is_verified=True и эти
поля не используют.

ИСПОЛЬЗОВАНИЕ (локально, если БД доступна):
  python db/add_email_verification_fields.py

НА ПРОДЕ (БД в контейнере) применять SQL напрямую — см. deploy_manual.md:
  cat db/add_email_verification_fields.sql | ssh ... 'docker exec -i frame-db-1 psql -U postgres -d moex_db'
  (или те же ALTER ... IF NOT EXISTS вручную)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from api.database import get_engine
from sqlalchemy import text

MIGRATIONS = [
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verify_code VARCHAR(6);",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verify_expires_at TIMESTAMPTZ;",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verify_attempts INTEGER NOT NULL DEFAULT 0;",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verify_sent_at TIMESTAMPTZ;",
]


def migrate():
    engine = get_engine()
    with engine.connect() as conn:
        for i, sql in enumerate(MIGRATIONS, 1):
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"  ✓ Шаг {i}/{len(MIGRATIONS)} — OK")
            except Exception as e:
                print(f"  ✗ Шаг {i}/{len(MIGRATIONS)} — {e}")
                conn.rollback()
    print("\n✅ Миграция email-verification завершена!")


if __name__ == "__main__":
    print("🔄 Добавление полей подтверждения email в таблицу users...\n")
    migrate()
