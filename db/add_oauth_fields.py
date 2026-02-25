# db/add_oauth_fields.py
"""
Миграция: добавление OAuth полей в таблицу users.

Добавляет колонки:
  - oauth_provider VARCHAR(20) — провайдер (google, vk, telegram)
  - oauth_id VARCHAR(255) — ID пользователя у провайдера
  - avatar_url VARCHAR(500) — URL аватара

Также:
  - Делает hashed_password nullable (OAuth юзеры без пароля)
  - Добавляет уникальный constraint на (oauth_provider, oauth_id)

ИСПОЛЬЗОВАНИЕ:
  python db/add_oauth_fields.py
"""

import sys
from pathlib import Path

# Добавляем корень проекта в sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.database import get_engine
from sqlalchemy import text

def migrate():
    engine = get_engine()

    migrations = [
        # 1. Добавляем oauth_provider
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'oauth_provider'
            ) THEN
                ALTER TABLE users ADD COLUMN oauth_provider VARCHAR(20);
            END IF;
        END $$;
        """,

        # 2. Добавляем oauth_id
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'oauth_id'
            ) THEN
                ALTER TABLE users ADD COLUMN oauth_id VARCHAR(255);
            END IF;
        END $$;
        """,

        # 3. Добавляем avatar_url
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'avatar_url'
            ) THEN
                ALTER TABLE users ADD COLUMN avatar_url VARCHAR(500);
            END IF;
        END $$;
        """,

        # 4. Делаем hashed_password nullable (для OAuth юзеров без пароля)
        """
        ALTER TABLE users ALTER COLUMN hashed_password DROP NOT NULL;
        """,

        # 5. Уникальный constraint на (oauth_provider, oauth_id)
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'uq_oauth_provider_id'
            ) THEN
                ALTER TABLE users
                ADD CONSTRAINT uq_oauth_provider_id
                UNIQUE (oauth_provider, oauth_id);
            END IF;
        END $$;
        """,
    ]

    with engine.connect() as conn:
        for i, sql in enumerate(migrations, 1):
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"  ✓ Шаг {i}/{len(migrations)} — OK")
            except Exception as e:
                print(f"  ✗ Шаг {i}/{len(migrations)} — {e}")
                conn.rollback()

    print("\n✅ Миграция OAuth завершена!")


if __name__ == "__main__":
    print("🔄 Добавление OAuth полей в таблицу users...\n")
    migrate()
