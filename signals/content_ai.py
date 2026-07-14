"""
Диспетчер Шага А/В content-пайплайна: дёргает Claude Routine через ПУБЛИЧНЫЙ
`fire`-эндпоинт (НЕ через Claude Code SDK/сессию) для 'candidate'-кандидатов
(Шаг А) и 'draft_ready' без draft_text (Шаг В). Routine сама читает
candidate_id/internal_token из текста и PATCH'ит internal_router
(api/routers/content_news.py) с результатом.

Механизм найден и подтверждён вживую 2026-07-13 (см. память content-pipeline-design):
POST https://api.anthropic.com/v1/claude_code/routines/{trigger_id}/fire
с телом {"text": "..."} — этот text добавляется отдельным user-ходом ПОВЕРХ
статичных инструкций Routine. Внутренний RemoteTrigger (`action: run`, через
Claude Code сессию) НЕ передаёт per-run данные — тупиковый путь, не использовать.

⚠️ experimental Anthropic API (`anthropic-beta: experimental-cc-routine-2026-04-01`)
— может измениться без анонса. Каждая Routine требует СВОЙ bearer-токен
(токен Шага А не работает для Шага В и наоборот, проверено — 401).

⚠️ api.anthropic.com отдаёт 403 "Request not allowed" с российских IP (подтверждено
2026-07-14 — идентичный запрос с не-РФ машины даёт 200). Поэтому вызов идёт через
Cloudflare Worker релей (signals/relay/cf-worker.js, тот же паттерн что и Telegram/
Yahoo) — CLAUDE_ROUTINE_API_ROOT в .env. Без этой переменной падает на прямой
api.anthropic.com (не сработает с прод-сервера, но полезно для локальных тестов).

Fire — это "выстрелил и забыл": ответ 200 означает только то, что облачная
сессия СТАРТОВАЛА, не то, что она успешно завершилась и сделала PATCH (в
проде наблюдались сессии по 1-5 минут). Поэтому диспетчер не ждёт результата
синхронно — использует last_checked_at как cooldown-троттлинг (тот же
паттерн, что и content_match.py для дневных-only активов), чтобы не
перевыстреливать один и тот же кандидат, пока предыдущая сессия ещё не
успела приземлить PATCH.

⚠️ BATCH_LIMIT ограничивает число fire-вызовов ЗА ОДИН ПРОГОН — каждый fire это
отдельная, независимо оплачиваемая облачная AI-сессия (НЕ один чат на всех
кандидатов). Без лимита первый же прогон после накопления очереди (напр. 437
RSS-кандидатов за ночь) выстрелил бы их ВСЕ разом. Лимит + периодичность крона —
это и есть троттлинг темпа обработки, не разовая порция.

Запуск раз в 15-20 минут (Routine-сессии не мгновенные):
  /opt/frame/signals/content_ai.sh   (cron, напр. */15 * * * *)
"""
import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from sqlalchemy import text

# ── .env + DB_URL override ДО импорта api.database (host-side, как content_match) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat                  # noqa: E402
from api.database import SessionLocal      # noqa: E402

# CLAUDE_ROUTINE_API_ROOT — релей (Cloudflare Worker) для обхода гео-блока
# api.anthropic.com. Дефолт = прямой Anthropic (не работает с прод-сервера,
# но не ломает локальные/не-РФ тесты, пока env не задан).
_API_ROOT = os.environ.get("CLAUDE_ROUTINE_API_ROOT", "https://api.anthropic.com")
FIRE_URL_TMPL = _API_ROOT + "/v1/claude_code/routines/{trigger_id}/fire"

TRIGGER_ID_STEP_A = "trig_01CTyFze4rXBRGwPKVFtSooj"   # frame-content-step-a
TRIGGER_ID_STEP_C = "trig_01KPtMNbEYNfqewKvwhdo4rj"   # frame-content-step-c

DISPATCH_COOLDOWN_MIN = 15   # не перевыстреливать кандидата чаще этого окна
BATCH_LIMIT = 10             # максимум fire-вызовов НА ШАГ за один прогон (см. docstring)
INTERNAL_API_HOST = "https://xn--80aklbnczmv.xn--p1ai"  # ⚠️ punycode: кириллица ломает curl внутри Routine

_ANTHROPIC_HEADERS_BASE = {
    "anthropic-version": "2023-06-01",
    "anthropic-beta": "experimental-cc-routine-2026-04-01",
    "Content-Type": "application/json",
}

_SELECT_CANDIDATES = text("""
    SELECT id, source, headline, raw_text, last_checked_at
    FROM content_candidates
    WHERE status = 'candidate'
      AND (last_checked_at IS NULL OR last_checked_at < :cutoff)
    ORDER BY id
    LIMIT :batch_limit
""")

_SELECT_DRAFT_READY = text("""
    SELECT c.id, c.headline, c.tickers, c.event_type, c.futures_ticker, c.reasoning,
           a.asset_id, a.asset_name, a.type AS anomaly_type, a.direction,
           a.severity_value, a.signal_date, a.headline AS anomaly_headline
    FROM content_candidates c
    JOIN anomalies a ON a.id = c.matched_anomaly_id
    WHERE c.status = 'draft_ready' AND c.draft_text IS NULL
      AND (c.last_checked_at IS NULL OR c.last_checked_at < :cutoff)
    ORDER BY c.id
    LIMIT :batch_limit
""")

_MARK_DISPATCHED = text("""
    UPDATE content_candidates SET last_checked_at = now() WHERE id = :id
""")


def _fire(trigger_id: str, bearer_token: str, text_payload: str) -> None:
    resp = requests.post(
        FIRE_URL_TMPL.format(trigger_id=trigger_id),
        headers={**_ANTHROPIC_HEADERS_BASE, "Authorization": f"Bearer {bearer_token}"},
        json={"text": text_payload},
        timeout=15,
    )
    resp.raise_for_status()


_SELECT_KNOWN_TICKERS = text(
    "SELECT stock_ticker, display_name FROM ticker_futures_map ORDER BY stock_ticker"
)


def _known_tickers_line(db) -> str:
    """Реальный список отслеживаемых тикеров — ГРУНТ для Шага А, чтобы модель
    не угадывала тикер по памяти (риск спутать похожие компании, напр. Газпром/
    Газпромнефть/Газпромбанк — при совпадении с чужим реальным тикером в нашей
    таблице Шаг Б подтянет ЧУЖИЕ данные). Тикеры вне списка Шаг Б всё равно не
    отработает (нет futures_ticker), но теперь это ЯВНОЕ решение модели, а не
    случайность."""
    rows = db.execute(_SELECT_KNOWN_TICKERS).fetchall()
    if not rows:
        return "(пусто)"
    return ", ".join(f"{t}={name}" for t, name in rows)


def _step_a_payload(row, internal_token: str, known_tickers: str) -> str:
    return (
        f"candidate_id: {row['id']}\n"
        f"source: {row['source']}\n"
        f"headline: {row['headline']}\n"
        f"raw_text: {row['raw_text'] or row['headline']}\n"
        f"known_tickers (ТОЛЬКО из этого списка, больше ниоткуда): {known_tickers}\n"
        f"internal_token: {internal_token}\n"
        f"api_host: {INTERNAL_API_HOST}"
    )


def _step_c_payload(row, internal_token: str) -> str:
    return (
        f"candidate_id: {row['id']}\n"
        f"headline: {row['headline']}\n"
        f"tickers: {', '.join(row['tickers'] or [])}\n"
        f"event_type: {row['event_type'] or ''}\n"
        f"reasoning (Шаг А): {row['reasoning'] or ''}\n"
        f"matched_anomaly:\n"
        f"  asset: {row['asset_id']} ({row['asset_name'] or ''})\n"
        f"  type: {row['anomaly_type']}\n"
        f"  direction: {row['direction']}\n"
        f"  multiplier: x{row['severity_value']}\n"
        f"  signal_date: {row['signal_date']}\n"
        f"  headline: {row['anomaly_headline']}\n"
        f"internal_token: {internal_token}\n"
        f"api_host: {INTERNAL_API_HOST}"
    )


def run_once() -> dict:
    summary = {"step_a_fired": 0, "step_c_fired": 0, "errors": 0, "skipped_no_token": 0}

    internal_token = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    token_a = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_A", "")
    token_c = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_C", "")
    if not internal_token or not token_a or not token_c:
        summary["skipped_no_token"] = 1
        print("[content_ai] отсутствует CONTENT_AI_INTERNAL_TOKEN / "
              "CLAUDE_ROUTINE_FIRE_TOKEN_STEP_A / _STEP_C в .env — пропуск")
        return summary

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=DISPATCH_COOLDOWN_MIN)

    db = SessionLocal()
    try:
        candidates = db.execute(
            _SELECT_CANDIDATES, {"cutoff": cutoff, "batch_limit": BATCH_LIMIT}
        ).mappings().all()
        known_tickers = _known_tickers_line(db) if candidates else ""
        for row in candidates:
            try:
                _fire(TRIGGER_ID_STEP_A, token_a,
                      _step_a_payload(row, internal_token, known_tickers))
                db.execute(_MARK_DISPATCHED, {"id": row["id"]})
                summary["step_a_fired"] += 1
            except Exception as e:
                summary["errors"] += 1
                print(f"[content_ai] step-a fire failed for candidate {row['id']}: "
                      f"{type(e).__name__}: {e}")
        db.commit()

        draft_ready = db.execute(
            _SELECT_DRAFT_READY, {"cutoff": cutoff, "batch_limit": BATCH_LIMIT}
        ).mappings().all()
        for row in draft_ready:
            try:
                _fire(TRIGGER_ID_STEP_C, token_c, _step_c_payload(row, internal_token))
                db.execute(_MARK_DISPATCHED, {"id": row["id"]})
                summary["step_c_fired"] += 1
            except Exception as e:
                summary["errors"] += 1
                print(f"[content_ai] step-c fire failed for candidate {row['id']}: "
                      f"{type(e).__name__}: {e}")
        db.commit()
    except Exception as e:
        db.rollback()
        summary["errors"] += 1
        print(f"[content_ai] fatal: {e}")
    finally:
        db.close()
    return summary


def main():
    t0 = datetime.now(timezone.utc)
    s = run_once()
    dur = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[{datetime.now(timezone.utc)}] content_ai: {s}")
    pipeline_heartbeat.record_pipeline_run(
        "content_ai_backstop", s["errors"] == 0, str(s), dur
    )


if __name__ == "__main__":
    main()
