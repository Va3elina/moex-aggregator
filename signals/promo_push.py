"""
Ежемесячный промо канала Фрейм в ленту аномалий (тост + колокол). Кладёт ОДНУ
type='promo' строку с РОТАЦИЕЙ формулировок (round-robin по числу прошлых промо —
не повторяем текст месяц к месяцу). Host-cron раз в месяц:
  0 12 1 * *  /bin/bash /opt/frame/signals/promo_push.sh >> /opt/frame/logs/promo_push.log 2>&1

Ничего не детектит — просто промо-карточка, открывается внешней ссылкой на канал.
"""
import os
import json
from datetime import datetime, timezone, date

from dotenv import load_dotenv
from sqlalchemy import text

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from api.database import SessionLocal  # noqa: E402

CHANNEL_URL = "https://t.me/FrameTool"

# Пул формулировок (round-robin). (headline, context). Не повторяемся.
VARIANTS = [
    ("Канал Фрейма в Telegram", "Разборы рыночных аномалий, которых нет на сайте — коротко и по делу"),
    ("Подпишись на @FrameTool", "Что двигает рынок прямо сейчас — нашими словами, без воды"),
    ("Рыночные аномалии — в Telegram", "Графики, потоки, открытый интерес: ежедневные заметки в канале Фрейма"),
    ("Хочешь видеть это раньше?", "Самые громкие движения рынка разбираем в канале @FrameTool"),
    ("Канал Фрейма: рынок без шума", "Только значимое — аномалии, потоки фондов, позиции крупных игроков"),
    ("Читай Фрейм в Telegram", "Ежедневный дайджест аномалий и потоков — подписывайся на @FrameTool"),
]

_INSERT = text("""
  INSERT INTO anomalies (scope, user_id, type, asset_id, asset_name, clgroup,
                         direction, headline, context, severity_value, signal_date, deep_link)
  VALUES ('public', NULL, 'promo', 'FrameTool', :name, NULL, NULL,
          :headline, :context, NULL, :signal_date, CAST(:deep_link AS JSONB))
  ON CONFLICT DO NOTHING
  RETURNING id
""")
_COUNT = text("SELECT count(*) FROM anomalies WHERE type = 'promo'")


def run_once() -> dict:
    summary = {"inserted": 0, "variant": None}
    db = SessionLocal()
    try:
        n = db.execute(_COUNT).scalar() or 0
        idx = n % len(VARIANTS)          # round-robin без отдельного стейта
        headline, context = VARIANTS[idx]
        summary["variant"] = idx
        row = db.execute(_INSERT, {
            "name": "Канал Фрейма", "headline": headline, "context": context,
            "signal_date": date.today(),
            "deep_link": json.dumps({"route": CHANNEL_URL}, ensure_ascii=False),
        }).fetchone()
        if row:
            summary["inserted"] = 1
            db.execute(text("SELECT pg_notify('anomaly', :p)"),
                       {"p": json.dumps({"source": "anomaly", "id": row[0]})})
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[promo_push] error: {e}")
    finally:
        db.close()
    return summary


def main():
    s = run_once()
    print(f"[{datetime.now(timezone.utc)}] promo_push: {s}")


if __name__ == "__main__":
    main()
