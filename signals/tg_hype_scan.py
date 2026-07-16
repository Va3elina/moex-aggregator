"""
Этап 6 content-пайплайна: репост-хайп TG-каналов → content_candidates
(status='candidate', source=<имя канала>). Слой 1б — сигнал важности "то,
что реально видит толпа" (полный текст не всегда есть/доступен по ссылке).

Каналы — config.TG_HYPE_CHANNELS (сейчас markettwits, newssmartlab). Один и
тот же механизм для всех: гипотеза «репосты = сигнал важности» ПРОВЕРЕНА
вживую 2026-07-13 на @markettwits (3000 сообщений, живая MTProto-сессия):
просмотры — слабый сигнал (разброс ~9×), репосты — сильный (разброс ~3000×,
p50=80/p90=302/p99=969).

Изначально решение принималось на чекпоинте +90мин (два чекпоинта, был расчёт
на "поздний всплеск" у части постов). Найдено 2026-07-15 (замер 78 постов с
обоими чекпоинтами): у ВСЕХ 6 постов, реально пересёкших порог хайпа, fwd_15
уже был выше ×3 к медиане fwd_15-истории — ждать до +90мин не требовалось НИ
РАЗУ. Решение переехало на +15мин (MTP_CHECKPOINT_1_MIN), baseline считается
на fwd_15. +90мин-чекпоинт убран целиком.

fwd_3 (MTP_CHECKPOINT_EARLY_MIN) — ЕЩЁ более ранний чекпоинт, ПОКА ТОЛЬКО для
измерения (ни на что не влияет) — копим данные, чтобы через несколько дней
честно ответить, можно ли сократить decision-чекпоинт ниже 15 минут (см.
db/migrations/031_tg_channel_watch_early_checkpoint).

Baseline (медиана fwd_15) считается ОТДЕЛЬНО по каждому каналу — репосты
разных каналов не сравнимы напрямую (разный размер аудитории), поэтому
tg_channel_watch хранит канал и все выборки baseline фильтруются по нему.

⚠️ MTProto — сырой TCP, Cloudflare-релей (signals/relay/) НЕ помогает (тот
только HTTP). Обязательно `use_ipv6=True` в TelegramClient — без него
Telethon пытается заблокированные РКН IPv4-адреса Telegram DC и стабильно
фейлится (подтверждено вживую 2026-07-14). Session-файл персистентный:
/opt/frame/signals/mtp_session.session (залогинен один раз, дальше держится
сам, переживает рестарты процесса) — общий на все каналы, отдельный логин
на каждый канал не нужен (это один Telegram-аккаунт, читающий N каналов).

Запуск раз в 5 минут — чаще, чем раньше (было 15), чтобы физически успевать
поймать более узкое окно измерительного чекпоинта +3мин (чекпоинты
дискретные, cron должен успевать поймать окно
[checkpoint, checkpoint+MTP_CHECKPOINT_*_TOLERANCE_MIN]):
  /opt/frame/signals/tg_hype_scan.sh   (cron, напр. */5 * * * *)
"""
import os
import statistics
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from sqlalchemy import text
from telethon.sync import TelegramClient

# ── .env + DB_URL override ДО импорта api.database (host-side) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat              # noqa: E402
from api.database import SessionLocal  # noqa: E402
from signals import config             # noqa: E402
from signals.content_ai import (       # noqa: E402
    _fire, _step_a_payload, _known_tickers_line, TRIGGER_ID_STEP_A,
    _hype_filter_payload, TRIGGER_ID_HYPE_FILTER,
)

SESSION_PATH = os.path.join(_ROOT, "signals", "mtp_session")
MIN_BASELINE_COUNT = 10   # холодный старт: не решаем про хайп, пока мало истории на канале

# Найдено 2026-07-14 (session 3) — пауза между _fire() подряд в одном прогоне,
# иначе несколько облачных контейнеров запрашиваются одновременно и
# конкурируют за мощность аккаунта.
FIRE_STAGGER_SEC = 8

_EXISTS_WATCH = text("SELECT 1 FROM tg_channel_watch WHERE channel = :channel AND message_id = :id")
_INSERT_WATCH = text("""
    INSERT INTO tg_channel_watch (channel, message_id, posted_at, msg_text)
    VALUES (:channel, :id, :posted_at, :text)
    ON CONFLICT (channel, message_id) DO NOTHING
""")

_SELECT_PENDING_EARLY = text("""
    SELECT message_id FROM tg_channel_watch
    WHERE channel = :channel AND fwd_3 IS NULL
      AND posted_at <= :cutoff_min AND posted_at > :cutoff_max
""")
_UPDATE_FWD_EARLY = text("""
    UPDATE tg_channel_watch SET fwd_3 = :fwd, fwd_3_at = now()
    WHERE channel = :channel AND message_id = :id
""")

_SELECT_PENDING_MID = text("""
    SELECT message_id FROM tg_channel_watch
    WHERE channel = :channel AND fwd_8 IS NULL
      AND posted_at <= :cutoff_min AND posted_at > :cutoff_max
""")
_UPDATE_FWD_MID = text("""
    UPDATE tg_channel_watch SET fwd_8 = :fwd, fwd_8_at = now()
    WHERE channel = :channel AND message_id = :id
""")

_SELECT_PENDING_DECISION = text("""
    SELECT message_id, posted_at, msg_text FROM tg_channel_watch
    WHERE channel = :channel AND fwd_15 IS NULL
      AND posted_at <= :cutoff_min AND posted_at > :cutoff_max
""")
_UPDATE_FWD_DECISION = text("""
    UPDATE tg_channel_watch SET fwd_15 = :fwd, fwd_15_at = now()
    WHERE channel = :channel AND message_id = :id
""")
_MARK_PROMOTED = text(
    "UPDATE tg_channel_watch SET promoted = TRUE WHERE channel = :channel AND message_id = :id"
)
_RECENT_FWD_DECISION = text("""
    SELECT fwd_15 FROM tg_channel_watch
    WHERE channel = :channel AND fwd_15 IS NOT NULL
    ORDER BY fwd_15_at DESC LIMIT :n
""")

_EXISTS_CANDIDATE = text(
    "SELECT 1 FROM content_candidates WHERE source = :channel AND headline = :headline LIMIT 1"
)
_INSERT_CANDIDATE = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, source_url, created_at, updated_at)
    VALUES
        ('candidate', :channel, :headline, :raw_text, ARRAY[]::text[], :source_url, now(), now())
    RETURNING id
""")
_MARK_DISPATCHED = text("UPDATE content_candidates SET last_checked_at = now() WHERE id = :id")


def _headline_from_text(msg_text: str) -> str:
    """Заголовок = первая содержательная строка (посты часто начинаются
    с эмодзи-тегов вроде ⚠️🇺🇸#санкции на отдельной строке)."""
    for line in (msg_text or "").split("\n"):
        stripped = line.strip()
        if len(stripped) >= 15:
            return stripped[:300]
    return (msg_text or "")[:300].strip() or "(без текста)"


def _scan_channel(client, db, channel: str, now: datetime, can_fire: bool, token_a: str,
                   can_fire_hype: bool, token_hype: str,
                   internal_token: str, summary: dict) -> None:
    entity = client.get_entity(channel)

    # ── 1. Забрать свежие сообщения (последние ~2ч — с запасом под decision-чекпоинт) ──
    for m in client.iter_messages(entity, offset_date=now - timedelta(hours=2), reverse=True):
        summary["fetched"] += 1
        posted = m.date if m.date.tzinfo else m.date.replace(tzinfo=timezone.utc)
        res = db.execute(_INSERT_WATCH, {
            "channel": channel, "id": m.id, "posted_at": posted, "text": (m.text or "")[:2000],
        })
        if res.rowcount:
            summary["new_watched"] += 1
    db.commit()

    # ── 2. Ранний чекпоинт +3мин — ТОЛЬКО измерение, ни на что не влияет ──
    cutoff_min = now - timedelta(minutes=config.MTP_CHECKPOINT_EARLY_MIN)
    cutoff_max = now - timedelta(
        minutes=config.MTP_CHECKPOINT_EARLY_MIN + config.MTP_CHECKPOINT_EARLY_TOLERANCE_MIN)
    pending_early = db.execute(_SELECT_PENDING_EARLY, {
        "channel": channel, "cutoff_min": cutoff_min, "cutoff_max": cutoff_max,
    }).mappings().all()
    for row in pending_early:
        try:
            fresh = client.get_messages(entity, ids=row["message_id"])
            fwd = (fresh.forwards or 0) if fresh else 0
            db.execute(_UPDATE_FWD_EARLY, {"channel": channel, "id": row["message_id"], "fwd": fwd})
            db.commit()
            summary["checked_early"] += 1
        except Exception as e:
            summary["errors"] += 1
            print(f"[tg_hype_scan] {channel}: checkpoint-early failed for {row['message_id']}: "
                  f"{type(e).__name__}: {e}")

    # ── 2b. Промежуточный чекпоинт +8мин — ТОЖЕ только измерение ──
    cutoff_min = now - timedelta(minutes=config.MTP_CHECKPOINT_MID_MIN)
    cutoff_max = now - timedelta(
        minutes=config.MTP_CHECKPOINT_MID_MIN + config.MTP_CHECKPOINT_MID_TOLERANCE_MIN)
    pending_mid = db.execute(_SELECT_PENDING_MID, {
        "channel": channel, "cutoff_min": cutoff_min, "cutoff_max": cutoff_max,
    }).mappings().all()
    for row in pending_mid:
        try:
            fresh = client.get_messages(entity, ids=row["message_id"])
            fwd = (fresh.forwards or 0) if fresh else 0
            db.execute(_UPDATE_FWD_MID, {"channel": channel, "id": row["message_id"], "fwd": fwd})
            db.commit()
            summary["checked_mid"] += 1
        except Exception as e:
            summary["errors"] += 1
            print(f"[tg_hype_scan] {channel}: checkpoint-mid failed for {row['message_id']}: "
                  f"{type(e).__name__}: {e}")

    # ── 3. Decision-чекпоинт +15мин: финальное решение о хайпе ──
    cutoff_min = now - timedelta(minutes=config.MTP_CHECKPOINT_1_MIN)
    cutoff_max = now - timedelta(minutes=config.MTP_CHECKPOINT_1_MIN + config.MTP_CHECKPOINT_TOLERANCE_MIN)
    pending_decision = db.execute(_SELECT_PENDING_DECISION, {
        "channel": channel, "cutoff_min": cutoff_min, "cutoff_max": cutoff_max,
    }).mappings().all()
    for row in pending_decision:
        try:
            fresh = client.get_messages(entity, ids=row["message_id"])
            fwd = (fresh.forwards or 0) if fresh else 0
            db.execute(_UPDATE_FWD_DECISION, {"channel": channel, "id": row["message_id"], "fwd": fwd})
            db.commit()
            summary["checked_decision"] += 1

            baseline_vals = [r[0] for r in db.execute(
                _RECENT_FWD_DECISION, {"channel": channel, "n": config.MTP_BASELINE_WINDOW}
            ).fetchall()]
            if len(baseline_vals) < MIN_BASELINE_COUNT:
                continue  # холодный старт — недостаточно истории на этом канале для решения
            baseline = statistics.median(baseline_vals)
            if baseline <= 0 or fwd < baseline * config.MTP_HYPE_RATIO_MIN:
                continue

            headline = _headline_from_text(row["msg_text"])
            if db.execute(_EXISTS_CANDIDATE, {"channel": channel, "headline": headline}).fetchone():
                db.execute(_MARK_PROMOTED, {"channel": channel, "id": row["message_id"]})
                db.commit()
                continue

            new_id = db.execute(_INSERT_CANDIDATE, {
                "channel": channel, "headline": headline, "raw_text": row["msg_text"] or headline,
                "source_url": f"https://t.me/{channel}/{row['message_id']}",
            }).scalar()
            db.execute(_MARK_PROMOTED, {"channel": channel, "id": row["message_id"]})
            db.commit()
            summary["promoted"] += 1

            if can_fire:
                try:
                    known_tickers = _known_tickers_line(db)
                    payload = _step_a_payload(
                        {"id": new_id, "source": channel, "headline": headline,
                         "raw_text": row["msg_text"] or headline},
                        internal_token, known_tickers,
                    )
                    _fire(TRIGGER_ID_STEP_A, token_a, payload)
                    db.execute(_MARK_DISPATCHED, {"id": new_id})
                    db.commit()
                    summary["step_a_fired"] += 1
                    time.sleep(FIRE_STAGGER_SEC)  # см. FIRE_STAGGER_SEC выше
                except Exception as e:
                    summary["step_a_fire_errors"] += 1
                    print(f"[tg_hype_scan] {channel}: step-a fire failed for candidate "
                          f"{new_id}: {type(e).__name__}: {e}")

            # Шаг Н — независимый фильтр «шутка/мусор vs реальная новость» для
            # уведомления коллеги (см. TRIGGER_ID_HYPE_FILTER). Не завязан на
            # Шаг А/тикеры, отдельный Routine — пока не создан в UI, token_hype
            # пуст и фильтр просто не стреляет (см. run_once).
            if can_fire_hype:
                try:
                    hf_payload = _hype_filter_payload(
                        new_id, channel, row["msg_text"] or headline, internal_token,
                    )
                    _fire(TRIGGER_ID_HYPE_FILTER, token_hype, hf_payload)
                    summary["hype_filter_fired"] += 1
                    time.sleep(FIRE_STAGGER_SEC)
                except Exception as e:
                    summary["hype_filter_fire_errors"] += 1
                    print(f"[tg_hype_scan] {channel}: hype-filter fire failed for candidate "
                          f"{new_id}: {type(e).__name__}: {e}")
        except Exception as e:
            summary["errors"] += 1
            print(f"[tg_hype_scan] {channel}: checkpoint-decision failed for {row['message_id']}: "
                  f"{type(e).__name__}: {e}")


def run_once() -> dict:
    summary = {"fetched": 0, "new_watched": 0, "checked_early": 0, "checked_mid": 0, "checked_decision": 0,
               "promoted": 0, "step_a_fired": 0, "step_a_fire_errors": 0,
               "hype_filter_fired": 0, "hype_filter_fire_errors": 0, "errors": 0}

    api_id = os.environ.get("MTP_API_ID", "")
    api_hash = os.environ.get("MTP_API_HASH", "")
    if not api_id or not api_hash:
        print("[tg_hype_scan] MTP_API_ID / MTP_API_HASH не заданы — пропуск")
        summary["errors"] += 1
        return summary

    internal_token = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    token_a = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_A", "")
    can_fire = bool(internal_token and token_a)

    # Шаг Н (см. TRIGGER_ID_HYPE_FILTER) — опционален: пока Routine не создана
    # в UI и/или токен не задан в .env, просто не стреляет (не ошибка).
    token_hype = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_HYPE_FILTER", "")
    can_fire_hype = bool(internal_token and token_hype and TRIGGER_ID_HYPE_FILTER)

    now = datetime.now(timezone.utc)
    db = SessionLocal()
    client = TelegramClient(SESSION_PATH, int(api_id), api_hash, use_ipv6=True)
    try:
        client.connect()
        if not client.is_user_authorized():
            print("[tg_hype_scan] MTProto-сессия не авторизована — нужен повторный логин")
            summary["errors"] += 1
            return summary

        for channel in config.TG_HYPE_CHANNELS:
            try:
                _scan_channel(client, db, channel, now, can_fire, token_a,
                               can_fire_hype, token_hype, internal_token, summary)
            except Exception as e:
                summary["errors"] += 1
                print(f"[tg_hype_scan] {channel}: fatal: {e}")
    finally:
        client.disconnect()
        db.close()
    return summary


def main():
    t0 = datetime.now(timezone.utc)
    s = run_once()
    dur = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[{datetime.now(timezone.utc)}] tg_hype_scan: {s}")
    pipeline_heartbeat.record_pipeline_run(
        "content_tg_hype_scan", s["errors"] == 0, str(s), dur
    )


if __name__ == "__main__":
    main()
