"""
Этап 6 content-пайплайна: репост-хайп @markettwits → content_candidates
(status='candidate', source='markettwits'). Слой 1б — независимый от RSS
сигнал важности (полный текст не всегда есть в RSS/ссылка не всегда рабочая,
здесь то, что реально видит толпа).

Гипотеза «репосты = сигнал важности» ПРОВЕРЕНА вживую 2026-07-13 (3000
сообщений, живая MTProto-сессия): просмотры — слабый сигнал (разброс ~9×),
репосты — сильный (разброс ~3000×, p50=80/p90=302/p99=969). Формула порога —
измеренная, не гипотетическая (лонгитюдный замер 20 постов × 9 раундов):
репосты приходят ДИСКРЕТНЫМИ всплесками, не плавно — подавляющая часть роста
укладывается в первые 15 минут, ~10% сообщений получают ОТДЕЛЬНЫЙ поздний
всплеск на 60-90-й минуте. Поэтому ДВА чекпоинта (+15мин/+90мин), не
постоянный поллинг и не разовый замер — см. db/migrations/025_markettwits_watch.

⚠️ MTProto — сырой TCP, Cloudflare-релей (signals/relay/) НЕ помогает (тот
только HTTP). Обязательно `use_ipv6=True` в TelegramClient — без него
Telethon пытается заблокированные РКН IPv4-адреса Telegram DC и стабильно
фейлится (подтверждено вживую 2026-07-14). Session-файл персистентный:
/opt/frame/signals/mtp_session.session (залогинен один раз, дальше держится
сам, переживает рестарты процесса).

Порог хайпа ОТНОСИТЕЛЬНЫЙ (та же ATR-философия, что у OI/фондов) — ×N к
медиане недавних fwd_90-наблюдений САМОГО канала, не абсолютное число
(абсолютные пороги за месяцы дрейфуют вместе с ростом канала).

Запуск раз в 15 минут (чекпоинты дискретные, cron должен успевать поймать
окно [checkpoint, checkpoint+MTP_CHECKPOINT_TOLERANCE_MIN]):
  /opt/frame/signals/marketwits_scan.sh   (cron, напр. */15 * * * *)
"""
import os
import statistics
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from sqlalchemy import text
from telethon.sync import TelegramClient

# ── .env + DB_URL override ДО импорта api.database (host-side, как rss_scan) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat              # noqa: E402
from api.database import SessionLocal  # noqa: E402
from signals import config             # noqa: E402
from signals.content_ai import (       # noqa: E402
    _fire, _step_a_payload, _known_tickers_line, _known_categories_line, TRIGGER_ID_STEP_A,
)

SESSION_PATH = os.path.join(_ROOT, "signals", "mtp_session")
MIN_BASELINE_COUNT = 10   # холодный старт: не решаем про хайп, пока мало истории

_EXISTS_WATCH = text("SELECT 1 FROM markettwits_watch WHERE message_id = :id")
_INSERT_WATCH = text("""
    INSERT INTO markettwits_watch (message_id, posted_at, msg_text)
    VALUES (:id, :posted_at, :text)
    ON CONFLICT (message_id) DO NOTHING
""")

_SELECT_PENDING_15 = text("""
    SELECT message_id, posted_at FROM markettwits_watch
    WHERE fwd_15 IS NULL
      AND posted_at <= :cutoff_min AND posted_at > :cutoff_max
""")
_SELECT_PENDING_90 = text("""
    SELECT message_id, posted_at, msg_text FROM markettwits_watch
    WHERE fwd_15 IS NOT NULL AND fwd_90 IS NULL
      AND posted_at <= :cutoff_min AND posted_at > :cutoff_max
""")
_UPDATE_FWD_15 = text("""
    UPDATE markettwits_watch SET fwd_15 = :fwd, fwd_15_at = now() WHERE message_id = :id
""")
_UPDATE_FWD_90 = text("""
    UPDATE markettwits_watch SET fwd_90 = :fwd, fwd_90_at = now() WHERE message_id = :id
""")
_MARK_PROMOTED = text("UPDATE markettwits_watch SET promoted = TRUE WHERE message_id = :id")
_RECENT_FWD_90 = text("""
    SELECT fwd_90 FROM markettwits_watch
    WHERE fwd_90 IS NOT NULL
    ORDER BY fwd_90_at DESC LIMIT :n
""")

_EXISTS_CANDIDATE = text(
    "SELECT 1 FROM content_candidates WHERE source = 'markettwits' AND headline = :headline LIMIT 1"
)
_INSERT_CANDIDATE = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, created_at, updated_at)
    VALUES
        ('candidate', 'markettwits', :headline, :raw_text, ARRAY[]::text[], now(), now())
    RETURNING id
""")
_MARK_DISPATCHED = text("UPDATE content_candidates SET last_checked_at = now() WHERE id = :id")


def _headline_from_text(msg_text: str) -> str:
    """Заголовок = первая содержательная строка (посты MarketTwits часто
    начинаются с эмодзи-тегов вроде ⚠️🇺🇸#санкции на отдельной строке)."""
    for line in (msg_text or "").split("\n"):
        stripped = line.strip()
        if len(stripped) >= 15:
            return stripped[:300]
    return (msg_text or "")[:300].strip() or "(без текста)"


def run_once() -> dict:
    summary = {"fetched": 0, "new_watched": 0, "checked_15": 0, "checked_90": 0,
               "promoted": 0, "step_a_fired": 0, "step_a_fire_errors": 0, "errors": 0}

    api_id = os.environ.get("MTP_API_ID", "")
    api_hash = os.environ.get("MTP_API_HASH", "")
    if not api_id or not api_hash:
        print("[marketwits_scan] MTP_API_ID / MTP_API_HASH не заданы — пропуск")
        summary["errors"] += 1
        return summary

    internal_token = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    token_a = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_A", "")
    can_fire = bool(internal_token and token_a)

    now = datetime.now(timezone.utc)
    db = SessionLocal()
    client = TelegramClient(SESSION_PATH, int(api_id), api_hash, use_ipv6=True)
    try:
        client.connect()
        if not client.is_user_authorized():
            print("[marketwits_scan] MTProto-сессия не авторизована — нужен повторный логин")
            summary["errors"] += 1
            return summary

        entity = client.get_entity(config.MARKETTWITS_CHANNEL)

        # ── 1. Забрать свежие сообщения (последние ~2ч — с запасом под +90мин чекпоинт) ──
        for m in client.iter_messages(entity, offset_date=now - timedelta(hours=2), reverse=True):
            summary["fetched"] += 1
            posted = m.date if m.date.tzinfo else m.date.replace(tzinfo=timezone.utc)
            res = db.execute(_INSERT_WATCH, {
                "id": m.id, "posted_at": posted, "text": (m.text or "")[:2000],
            })
            if res.rowcount:
                summary["new_watched"] += 1
        db.commit()

        # ── 2. Чекпоинт +15мин ──
        cutoff_min = now - timedelta(minutes=config.MTP_CHECKPOINT_1_MIN)
        cutoff_max = now - timedelta(minutes=config.MTP_CHECKPOINT_1_MIN + config.MTP_CHECKPOINT_TOLERANCE_MIN)
        pending_15 = db.execute(_SELECT_PENDING_15, {
            "cutoff_min": cutoff_min, "cutoff_max": cutoff_max,
        }).mappings().all()
        for row in pending_15:
            try:
                fresh = client.get_messages(entity, ids=row["message_id"])
                fwd = (fresh.forwards or 0) if fresh else 0
                db.execute(_UPDATE_FWD_15, {"id": row["message_id"], "fwd": fwd})
                db.commit()
                summary["checked_15"] += 1
            except Exception as e:
                summary["errors"] += 1
                print(f"[marketwits_scan] checkpoint-15 failed for {row['message_id']}: "
                      f"{type(e).__name__}: {e}")

        # ── 3. Чекпоинт +90мин: финальное решение о хайпе ──
        cutoff_min = now - timedelta(minutes=config.MTP_CHECKPOINT_2_MIN)
        cutoff_max = now - timedelta(minutes=config.MTP_CHECKPOINT_2_MIN + config.MTP_CHECKPOINT_TOLERANCE_MIN)
        pending_90 = db.execute(_SELECT_PENDING_90, {
            "cutoff_min": cutoff_min, "cutoff_max": cutoff_max,
        }).mappings().all()
        for row in pending_90:
            try:
                fresh = client.get_messages(entity, ids=row["message_id"])
                fwd = (fresh.forwards or 0) if fresh else 0
                db.execute(_UPDATE_FWD_90, {"id": row["message_id"], "fwd": fwd})
                db.commit()
                summary["checked_90"] += 1

                baseline_vals = [r[0] for r in db.execute(
                    _RECENT_FWD_90, {"n": config.MTP_BASELINE_WINDOW}
                ).fetchall()]
                if len(baseline_vals) < MIN_BASELINE_COUNT:
                    continue  # холодный старт — недостаточно истории для решения
                baseline = statistics.median(baseline_vals)
                if baseline <= 0 or fwd < baseline * config.MTP_HYPE_RATIO_MIN:
                    continue

                headline = _headline_from_text(row["msg_text"])
                if db.execute(_EXISTS_CANDIDATE, {"headline": headline}).fetchone():
                    db.execute(_MARK_PROMOTED, {"id": row["message_id"]})
                    db.commit()
                    continue

                new_id = db.execute(_INSERT_CANDIDATE, {
                    "headline": headline, "raw_text": row["msg_text"] or headline,
                }).scalar()
                db.execute(_MARK_PROMOTED, {"id": row["message_id"]})
                db.commit()
                summary["promoted"] += 1

                if can_fire:
                    try:
                        known_tickers = _known_tickers_line(db)
                        known_categories = _known_categories_line(db)
                        payload = _step_a_payload(
                            {"id": new_id, "source": "markettwits", "headline": headline,
                             "raw_text": row["msg_text"] or headline},
                            internal_token, known_tickers, known_categories,
                        )
                        _fire(TRIGGER_ID_STEP_A, token_a, payload)
                        db.execute(_MARK_DISPATCHED, {"id": new_id})
                        db.commit()
                        summary["step_a_fired"] += 1
                    except Exception as e:
                        summary["step_a_fire_errors"] += 1
                        print(f"[marketwits_scan] step-a fire failed for candidate "
                              f"{new_id}: {type(e).__name__}: {e}")
            except Exception as e:
                summary["errors"] += 1
                print(f"[marketwits_scan] checkpoint-90 failed for {row['message_id']}: "
                      f"{type(e).__name__}: {e}")
    except Exception as e:
        summary["errors"] += 1
        print(f"[marketwits_scan] fatal: {e}")
    finally:
        client.disconnect()
        db.close()
    return summary


def main():
    t0 = datetime.now(timezone.utc)
    s = run_once()
    dur = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[{datetime.now(timezone.utc)}] marketwits_scan: {s}")
    pipeline_heartbeat.record_pipeline_run(
        "content_marketwits_scan", s["errors"] == 0, str(s), dur
    )


if __name__ == "__main__":
    main()
