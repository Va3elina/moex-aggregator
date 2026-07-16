"""
Этап 6 content-пайплайна: репост-хайп TG-каналов → content_candidates
(status='candidate', source=<имя канала>). Слой 1б — сигнал важности "то,
что реально видит толпа" (полный текст не всегда есть/доступен по ссылке).

Каналы — config.TG_HYPE_CHANNELS (сейчас markettwits, newssmartlab). Один и
тот же механизм для всех: гипотеза «репосты = сигнал важности» ПРОВЕРЕНА
вживую 2026-07-13 на @markettwits (3000 сообщений, живая MTProto-сессия):
просмотры — слабый сигнал (разброс ~9×), репосты — сильный (разброс ~3000×,
p50=80/p90=302/p99=969).

Изначально решение принималось на чекпоинте +90мин, потом +15мин (найдено
2026-07-15: у ВСЕХ 6 реально хайповых постов fwd_15 уже был выше ×3 к медиане
— ждать до +90мин не требовалось НИ РАЗУ). Найдено 2026-07-16 (219 постов с
fwd_3+fwd_15, 38 promoted): на +3мин у promoted было видно только 46%
итогового fwd_15-сигнала (против 73% на +8мин) — формально более рискованная
точка, но Вадим осознанно выбрал скорость: decision-чекпоинт переехал сразу
на +3мин (MTP_CHECKPOINT_1_MIN), baseline теперь на fwd_3, не fwd_15.

fwd_5/fwd_6/fwd_7/fwd_8 — ТОЛЬКО измерение (ни на что не влияют), держим как
запасные точки на случай, если 3 минуты дадут слишком много ложных
срабатываний/пропусков и decision-чекпоинт придётся вернуть выше (см.
db/migrations/031, 038_tg_channel_watch_fine_checkpoints).

Baseline (медиана fwd_3) считается ОТДЕЛЬНО по каждому каналу — репосты
разных каналов не сравнимы напрямую (разный размер аудитории), поэтому
tg_channel_watch хранит канал и все выборки baseline фильтруются по нему.

⚠️ MTProto — сырой TCP, Cloudflare-релей (signals/relay/) НЕ помогает (тот
только HTTP). Обязательно `use_ipv6=True` в TelegramClient — без него
Telethon пытается заблокированные РКН IPv4-адреса Telegram DC и стабильно
фейлится (подтверждено вживую 2026-07-14). Session-файл персистентный:
/opt/frame/signals/mtp_session.session (залогинен один раз, дальше держится
сам, переживает рестарты процесса) — общий на все каналы, отдельный логин
на каждый канал не нужен (это один Telegram-аккаунт, читающий N каналов).

Запуск раз в 2 минуты — учащено (было 5, до этого 15), т.к. измерительные
точки теперь лежат вплотную (5/6/7 мин, шаг 1 минута) — крон раз в 5 мин не
успевал бы разделить их (чекпоинты дискретные, крон должен успевать поймать
окно [checkpoint, checkpoint+MTP_CHECKPOINT_*_TOLERANCE_MIN]):
  /opt/frame/signals/tg_hype_scan.sh   (cron, напр. */2 * * * *)
"""
import os
import re
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

# Фото поста (Вадим 2026-07-16: "посты без фото приходят") — качаем ТОЛЬКО для
# промотированных (реальный хайп-кандидат), не для каждого проверенного сообщения.
# api-контейнер читает тот же каталог read-only (docker-compose.yml volume).
MEDIA_DIR = os.path.join(_ROOT, "data", "content_media")
os.makedirs(MEDIA_DIR, exist_ok=True)

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

# Измерительные чекпоинты (ни на что не влияют, только копят данные) — общий
# цикл вместо копипасты под каждую точку. Каждый кортеж: (колонка, минуты,
# допуск-минуты, ключ в summary). Порядок — по возрастанию минут.
MEASURE_CHECKPOINTS = [
    ("fwd_5", config.MTP_CHECKPOINT_5_MIN, config.MTP_CHECKPOINT_5_TOLERANCE_MIN, "checked_5"),
    ("fwd_6", config.MTP_CHECKPOINT_6_MIN, config.MTP_CHECKPOINT_6_TOLERANCE_MIN, "checked_6"),
    ("fwd_7", config.MTP_CHECKPOINT_7_MIN, config.MTP_CHECKPOINT_7_TOLERANCE_MIN, "checked_7"),
    ("fwd_8", config.MTP_CHECKPOINT_8_MIN, config.MTP_CHECKPOINT_8_TOLERANCE_MIN, "checked_8"),
]


def _select_pending_sql(col: str):
    return text(f"""
        SELECT message_id FROM tg_channel_watch
        WHERE channel = :channel AND {col} IS NULL
          AND posted_at <= :cutoff_min AND posted_at > :cutoff_max
    """)


def _update_fwd_sql(col: str):
    return text(f"""
        UPDATE tg_channel_watch SET {col} = :fwd, {col}_at = now()
        WHERE channel = :channel AND message_id = :id
    """)


# Decision-чекпоинт (+3мин, MTP_CHECKPOINT_1_MIN) — единственный, что решает
# судьбу поста, пишет в fwd_3 (см. docstring выше, было fwd_15→fwd_8). Нужен
# msg_text (для headline/raw_text кандидата), поэтому отдельный SELECT, не через хелпер.
_SELECT_PENDING_DECISION = text("""
    SELECT message_id, posted_at, msg_text FROM tg_channel_watch
    WHERE channel = :channel AND fwd_3 IS NULL
      AND posted_at <= :cutoff_min AND posted_at > :cutoff_max
""")
_UPDATE_FWD_DECISION = _update_fwd_sql("fwd_3")
_MARK_PROMOTED = text(
    "UPDATE tg_channel_watch SET promoted = TRUE WHERE channel = :channel AND message_id = :id"
)
_RECENT_FWD_DECISION = text("""
    SELECT fwd_3 FROM tg_channel_watch
    WHERE channel = :channel AND fwd_3 IS NOT NULL
    ORDER BY fwd_3_at DESC LIMIT :n
""")

_EXISTS_CANDIDATE = text(
    "SELECT 1 FROM content_candidates WHERE source = :channel AND headline = :headline LIMIT 1"
)
_INSERT_CANDIDATE = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, source_url, media_filename, created_at, updated_at)
    VALUES
        ('candidate', :channel, :headline, :raw_text, ARRAY[]::text[], :source_url, :media_filename, now(), now())
    RETURNING id
""")
_MARK_DISPATCHED = text("UPDATE content_candidates SET last_checked_at = now() WHERE id = :id")
_MARK_HYPE_FILTER_DISPATCHED = text("""
    UPDATE content_candidates
    SET hype_filter_dispatch_attempts = hype_filter_dispatch_attempts + 1, hype_filter_checked_at = now()
    WHERE id = :id
""")


# Smartlab подписывает КАЖДЫЙ пост фиксированным футером-рекламой своего
# канала в MAX (2026-07-16, Вадим: "убираем [мы в max]") — не часть новости,
# режем при сохранении, чтобы не тянуть его дальше в уведомление/Kanban/черновик.
_SMARTLAB_FOOTER_RE = re.compile(
    r"\n*\[мы в max\]\(https://max\.ru/newssmartlab\)\s*$", re.IGNORECASE
)


def _strip_known_footers(msg_text: str) -> str:
    return _SMARTLAB_FOOTER_RE.sub("", msg_text or "").rstrip()


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
            "channel": channel, "id": m.id, "posted_at": posted,
            "text": _strip_known_footers(m.text or "")[:2000],
        })
        if res.rowcount:
            summary["new_watched"] += 1
    db.commit()

    # ── 2. Измерительные чекпоинты (5/6/7/8мин) — ТОЛЬКО измерение, ни на что не влияют ──
    for col, minutes, tolerance, summary_key in MEASURE_CHECKPOINTS:
        cutoff_min = now - timedelta(minutes=minutes)
        cutoff_max = now - timedelta(minutes=minutes + tolerance)
        pending = db.execute(_select_pending_sql(col), {
            "channel": channel, "cutoff_min": cutoff_min, "cutoff_max": cutoff_max,
        }).mappings().all()
        for row in pending:
            try:
                fresh = client.get_messages(entity, ids=row["message_id"])
                fwd = (fresh.forwards or 0) if fresh else 0
                db.execute(_update_fwd_sql(col), {"channel": channel, "id": row["message_id"], "fwd": fwd})
                db.commit()
                summary[summary_key] += 1
            except Exception as e:
                summary["errors"] += 1
                print(f"[tg_hype_scan] {channel}: checkpoint-{col} failed for {row['message_id']}: "
                      f"{type(e).__name__}: {e}")

    # ── 3. Decision-чекпоинт +3мин: финальное решение о хайпе ──
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

            media_filename = None
            if getattr(fresh, "photo", None):
                candidate_filename = f"{channel}_{row['message_id']}.jpg"
                try:
                    client.download_media(fresh, file=os.path.join(MEDIA_DIR, candidate_filename))
                    media_filename = candidate_filename
                except Exception as e:
                    print(f"[tg_hype_scan] {channel}: photo download failed for "
                          f"{row['message_id']}: {type(e).__name__}: {e}")

            new_id = db.execute(_INSERT_CANDIDATE, {
                "channel": channel, "headline": headline, "raw_text": row["msg_text"] or headline,
                "source_url": f"https://t.me/{channel}/{row['message_id']}",
                "media_filename": media_filename,
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
                    db.execute(_MARK_HYPE_FILTER_DISPATCHED, {"id": new_id})
                    db.commit()
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
    summary = {"fetched": 0, "new_watched": 0,
               "checked_5": 0, "checked_6": 0, "checked_7": 0, "checked_8": 0, "checked_decision": 0,
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
