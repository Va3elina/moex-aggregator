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

⚠️ Найдено 2026-07-14: Routine-сессия иногда падает на этапе провижининга
облачного контейнера (Anthropic-инфраструктура, ДО старта Claude Code) — живой
случай, 3 кандидата зависли в status='candidate' навсегда, потому что этот
бэкстоп существовал, но не был на кроне. Если Routine сломана системно (не
разовый сбой), бесконечный ретрай молча жжёт деньги без результата —
MAX_DISPATCH_ATTEMPTS (028_content_candidates_dispatch_attempts) ограничивает
число ПОВТОРНЫХ выстрелов ЭТОГО бэкстопа (не оригинальный fire из
источника-скана — тот не считается попыткой ретрая). После лимита Шаг А сдаётся
в discarded с честной причиной, Шаг В — откатывается в pending (тот же путь,
что и при явном отказе модели, content_news.py:apply_step_c) — тред не
теряется, content_match.py может поймать более позднюю аномалию заново.
"""
import os
import time
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
# Шаг Н (найдено 2026-07-16, запрос Вадима) — НЕЗАВИСИМЫЙ от Шага А фильтр для
# уведомления коллеги: "это реальная новость или шутка/мусор?", БЕЗ привязки к
# тикеру/компании (в отличие от Шага А, чей `relevant` калиброван под "можно ли
# написать пост" — конфликтует с целью "коллеге интересно любое хайповое
# событие"). env, не хардкод — из .env читает ТОЛЬКО tg_hype_scan.py (host-side),
# деплоить код заново не нужно, когда Вадим создаст Routine в UI.
TRIGGER_ID_HYPE_FILTER = os.environ.get("TRIGGER_ID_HYPE_FILTER", "")

DISPATCH_COOLDOWN_MIN = 15   # не перевыстреливать кандидата чаще этого окна
BATCH_LIMIT = 10             # максимум fire-вызовов НА ШАГ за один прогон (см. docstring)
BATCH_LIMIT_HYPE_FILTER = 3  # Шаг Н — намеренно МЕНЬШЕ BATCH_LIMIT (Вадим 2026-07-16:
                              # не заваливать Routine параллельными повторами разом)
MAX_DISPATCH_ATTEMPTS = 3    # сколько раз ЭТОТ бэкстоп повторяет зависшего кандидата,
                              # прежде чем сдаться (см. docstring — защита от бесконечного
                              # ретрая системно сломанной Routine)
# Найдено 2026-07-14 (session 3): несколько _fire() подряд без
# паузы в одном прогоне (до BATCH_LIMIT штук) почти одновременно просят облако
# поднять cloud-контейнер — конкуренция за мощность аккаунта роняет часть
# попыток на этапе провижининга, ещё до старта самой Routine-сессии.
FIRE_STAGGER_SEC = 8
INTERNAL_API_HOST = "https://xn--80aklbnczmv.xn--p1ai"  # ⚠️ punycode: кириллица ломает curl внутри Routine

_ANTHROPIC_HEADERS_BASE = {
    "anthropic-version": "2023-06-01",
    "anthropic-beta": "experimental-cc-routine-2026-04-01",
    "Content-Type": "application/json",
}

_SELECT_CANDIDATES = text("""
    SELECT id, source, headline, raw_text, last_checked_at, dispatch_attempts
    FROM content_candidates
    WHERE status = 'candidate'
      AND (last_checked_at IS NULL OR last_checked_at < :cutoff)
    ORDER BY id
    LIMIT :batch_limit
""")

_SELECT_DRAFT_READY = text("""
    SELECT c.id, c.headline, c.raw_text, c.tickers, c.event_type, c.futures_ticker,
           c.reasoning, c.dispatch_attempts, c.forwards_count,
           c.thread_key, c.created_at,
           a.id AS anomaly_id, a.asset_id, a.asset_name, a.type AS anomaly_type,
           a.clgroup AS anomaly_clgroup, a.direction,
           a.severity_value, a.signal_date, a.headline AS anomaly_headline
    FROM content_candidates c
    JOIN anomalies a ON a.id = c.matched_anomaly_id
    WHERE c.status = 'draft_ready' AND c.draft_text IS NULL
      AND (c.last_checked_at IS NULL OR c.last_checked_at < :cutoff)
    ORDER BY c.id
    LIMIT :batch_limit
""")

_SELECT_RECENT_SIGNALS = text("""
    SELECT asset_name, direction, severity_value, signal_date
    FROM anomalies
    WHERE scope = 'public' AND type = 'oi_move' AND id != :exclude_id
    ORDER BY signal_date DESC, id DESC
    LIMIT 5
""")

_SELECT_PRIOR_POST = text("""
    SELECT headline, draft_text, published_at, updated_at
    FROM content_candidates
    WHERE thread_key = :thread_key AND id != :self_id AND draft_text IS NOT NULL
    ORDER BY COALESCE(published_at, updated_at) DESC
    LIMIT 1
""")

_MARK_DISPATCHED = text("""
    UPDATE content_candidates
    SET last_checked_at = now(), dispatch_attempts = dispatch_attempts + 1
    WHERE id = :id
""")

# Бэкстоп Шага Н (найдено 2026-07-16) — раньше tg_hype_scan.py стрелял РОВНО
# один раз и никогда не повторял (в отличие от Шага А/В, у которых уже был
# этот бэкстоп) — молчаливая потеря при любом сбое Routine (env-misconfig,
# кончившаяся подписка и т.п., см. живой инцидент того же дня). source_url
# IS NOT NULL — только tg_hype-кандидаты (markettwits/newssmartlab), у
# moex_calendar Шаг Н не запускается вообще, им нечего ждать.
_SELECT_HYPE_FILTER_PENDING = text("""
    SELECT id, source, headline, raw_text, hype_filter_dispatch_attempts
    FROM content_candidates
    WHERE hype_filter_result IS NULL AND source_url IS NOT NULL
      AND hype_filter_dispatch_attempts > 0 AND hype_filter_dispatch_attempts < :max_attempts
      AND (hype_filter_checked_at IS NULL OR hype_filter_checked_at < :cutoff)
    ORDER BY id
    LIMIT :batch_limit
""")
_MARK_HYPE_FILTER_DISPATCHED = text("""
    UPDATE content_candidates
    SET hype_filter_dispatch_attempts = hype_filter_dispatch_attempts + 1, hype_filter_checked_at = now()
    WHERE id = :id
""")
_GIVE_UP_HYPE_FILTER = text("""
    UPDATE content_candidates SET hype_filter_checked_at = now() WHERE id = :id
""")

# Лимит попыток исчерпан — Routine либо системно сломана, либо candidate
# неудачный (не тратим деньги дальше). Шаг А: честный отказ (discarded), как
# и любой другой content_candidate, не прошедший оценку. Шаг В: откат в
# pending с причиной — тот же путь, что при явном отказе модели
# (apply_step_c), thread не теряется, content_match.py подхватит заново.
_GIVE_UP_STEP_A = text("""
    UPDATE content_candidates
    SET status = 'discarded', reasoning = :reasoning, updated_at = now()
    WHERE id = :id
""")
_GIVE_UP_STEP_C = text("""
    UPDATE content_candidates
    SET status = 'pending', synth_declined_reason = :reason, updated_at = now()
    WHERE id = :id
""")


def _notify_pipeline_stuck(step: str, candidate_id: int, reason: str) -> None:
    """Найдено 2026-07-16 (Вадим): give-up после MAX_DISPATCH_ATTEMPTS менял
    статус кандидата МОЛЧА — узнавали о сломанной Routine (env-misconfig,
    кончившаяся подписка) только постфактум, через ручные 409 часы спустя.
    Сюда — та же связка HYPE_NOTIFY_BOT_TOKEN/CHAT_ID + TELEGRAM_API_ROOT
    релей, что и у api/routers/content_news.py:_notify_hype_colleague, но
    вызывается ХОСТ-СКРИПТОМ (content_ai.py уже на host, не в api-контейнере
    — .env читает напрямую, дублировать docker-compose проброс не нужно).
    Best-effort — сбой отправки не должен ронять сам бэкстоп."""
    token = os.environ.get("HYPE_NOTIFY_BOT_TOKEN", "")
    chat_id = os.environ.get("HYPE_NOTIFY_CHAT_ID", "")
    if not token or not chat_id:
        return
    text_msg = (
        f"⚠️ Шаг {step} сдался по кандидату #{candidate_id} после "
        f"{MAX_DISPATCH_ATTEMPTS} попыток бэкстопа — Routine не ответила.\n\n"
        f"{reason}\n\n"
        f"Проверь окружение/подписку (см. скилл moex-content-routines: "
        f"environment_id/allowed_tools, лимиты аккаунта)."
    )
    api_root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
    try:
        requests.post(
            f"{api_root}/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text_msg},
            timeout=10,
        )
    except Exception as e:
        print(f"[content_ai] pipeline-stuck notify failed: {type(e).__name__}: {e}")


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


def _hype_filter_payload(candidate_id: int, source: str, raw_text: str, internal_token: str) -> str:
    """Шаг Н — независимый от Шага А промпт: только «шутка/мусор или реальная
    новость?», без тикеров/компаний/значимости для завода постов."""
    return (
        f"candidate_id: {candidate_id}\n"
        f"source: {source}\n"
        f"raw_text: {raw_text}\n"
        f"internal_token: {internal_token}\n"
        f"api_host: {INTERNAL_API_HOST}"
    )


def _oi_context_line(asset_id: str, clgroup: str | None) -> str:
    """История позиции физлиц по инструменту — рамка «где мы относительно
    исторического макс/мин», ту же самую подсвечивает скринер сигналов. Без
    неё Шаг В брал историческую аналогию из памяти модели (галлюцинация);
    с ней есть настоящая фактура для честного «как было раньше»."""
    from signals.db import get_position_series
    clg = clgroup or "FIZ"
    series = get_position_series(asset_id, clg, days=730)
    if len(series) < 30:
        return "(недостаточно истории по этому инструменту для контекста)"
    pcts = []
    for _d, net, _npart, plong, pshort in series:
        gross = (plong or 0) - (pshort or 0)
        pcts.append((net / gross * 100) if gross else 0.0)
    today_pct = pcts[-1]
    hist_max, hist_min = max(pcts), min(pcts)
    trend_30 = pcts[-30]
    trend = "растёт" if today_pct > trend_30 else "снижается" if today_pct < trend_30 else "без изменений"
    return (f"текущий перекос физлиц (net/gross): {today_pct:.1f}%; "
            f"диапазон за 2 года: [{hist_min:.1f}%; {hist_max:.1f}%]; "
            f"тенденция за 30 дней: {trend}")


def _recent_signals_line(db, exclude_anomaly_id: int) -> str:
    """Сравнимые недавние публичные OI-сигналы по ДРУГИМ тикерам — фактура
    для «для масштаба: N был ×M», вместо выдуманного сравнения."""
    rows = db.execute(
        _SELECT_RECENT_SIGNALS, {"exclude_id": exclude_anomaly_id}
    ).fetchall()
    if not rows:
        return "(нет сравнимых недавних сигналов)"
    parts = []
    for name, direction, severity, sig_date in rows:
        word = "лонг" if direction == "up" else "шорт"
        parts.append(f"{name}: ×{severity:g} {word} ({sig_date})")
    return "; ".join(parts)


def _prior_post_line(db, thread_key, self_id: int, reused_signal: bool) -> str:
    """Более ранний опубликованный/готовый пост по этому же треду — фактура
    для честного «продолжение истории» вместо повторного изобретения того же
    сюжета с нуля. reused_signal=True добавляет явную оговорку: сам сигнал ОИ
    не новый (дневной актив, новых данных сегодня физически не появится),
    Шаг В не должен подавать ×N как случившееся заново."""
    if not thread_key:
        return "(нет — новый тред)"
    row = db.execute(
        _SELECT_PRIOR_POST, {"thread_key": thread_key, "self_id": self_id}
    ).mappings().first()
    if not row:
        return "(нет — новый тред)"
    note = ""
    if reused_signal:
        note = ("ВНИМАНИЕ: аномалия та же самая, что и в посте ниже — новых данных "
                 "по позициям сегодня нет (актив дневной, разово в сутки). НЕ подавай "
                 "×N как случившееся заново, пиши только обновление новостной истории.\n")
    when = row["published_at"] or row["updated_at"]
    return f"{note}пост от {when}:\n{row['draft_text'] or row['headline']}"


def _step_c_payload(db, row, internal_token: str) -> str:
    oi_context = _oi_context_line(row["asset_id"], row["anomaly_clgroup"])
    recent_signals = _recent_signals_line(db, row["anomaly_id"])
    created_at = row.get("created_at")
    reused_signal = bool(created_at and row["signal_date"] < created_at.date())
    prior_post = _prior_post_line(db, row.get("thread_key"), row["id"], reused_signal)
    return (
        f"candidate_id: {row['id']}\n"
        f"headline: {row['headline']}\n"
        f"raw_text: {row['raw_text'] or row['headline']}\n"
        f"tickers: {', '.join(row['tickers'] or [])}\n"
        f"event_type: {row['event_type'] or ''}\n"
        f"reasoning (Шаг А): {row['reasoning'] or ''}\n"
        f"forwards_count: {row['forwards_count'] if row['forwards_count'] is not None else '(нет данных)'}\n"
        f"matched_anomaly:\n"
        f"  asset: {row['asset_id']} ({row['asset_name'] or ''})\n"
        f"  type: {row['anomaly_type']}\n"
        f"  direction: {row['direction']}\n"
        f"  multiplier: x{row['severity_value']}\n"
        f"  signal_date: {row['signal_date']}\n"
        f"  headline: {row['anomaly_headline']}\n"
        f"oi_context: {oi_context}\n"
        f"recent_signals: {recent_signals}\n"
        f"prior_post: {prior_post}\n"
        f"internal_token: {internal_token}\n"
        f"api_host: {INTERNAL_API_HOST}"
    )


def run_once() -> dict:
    summary = {"step_a_fired": 0, "step_c_fired": 0, "errors": 0, "skipped_no_token": 0,
               "step_a_gave_up": 0, "step_c_gave_up": 0,
               "hype_filter_fired": 0, "hype_filter_gave_up": 0}

    internal_token = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    token_a = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_A", "")
    token_c = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_C", "")
    # Шаг Н — отдельный токен, отдельная проверка (не блокирует А/В, если ещё
    # не создан в UI, и наоборот — см. skill moex-content-routines).
    token_hype = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_HYPE_FILTER", "")
    can_fire_hype = bool(internal_token and token_hype and TRIGGER_ID_HYPE_FILTER)
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
            if row["dispatch_attempts"] >= MAX_DISPATCH_ATTEMPTS:
                give_up_reason = (f"Routine не ответила за {MAX_DISPATCH_ATTEMPTS} "
                                   f"попыток бэкстопа — сдаёмся (см. content_ai.py)")
                db.execute(_GIVE_UP_STEP_A, {"id": row["id"], "reasoning": give_up_reason})
                summary["step_a_gave_up"] += 1
                _notify_pipeline_stuck("А", row["id"], give_up_reason)
                continue
            try:
                _fire(TRIGGER_ID_STEP_A, token_a,
                      _step_a_payload(row, internal_token, known_tickers))
                db.execute(_MARK_DISPATCHED, {"id": row["id"]})
                summary["step_a_fired"] += 1
                time.sleep(FIRE_STAGGER_SEC)  # см. FIRE_STAGGER_SEC выше
            except Exception as e:
                summary["errors"] += 1
                print(f"[content_ai] step-a fire failed for candidate {row['id']}: "
                      f"{type(e).__name__}: {e}")
        db.commit()

        draft_ready = db.execute(
            _SELECT_DRAFT_READY, {"cutoff": cutoff, "batch_limit": BATCH_LIMIT}
        ).mappings().all()
        for row in draft_ready:
            if row["dispatch_attempts"] >= MAX_DISPATCH_ATTEMPTS:
                give_up_reason = (f"Routine не ответила за {MAX_DISPATCH_ATTEMPTS} "
                                   f"попыток бэкстопа — откат в pending (см. content_ai.py)")
                db.execute(_GIVE_UP_STEP_C, {"id": row["id"], "reason": give_up_reason})
                summary["step_c_gave_up"] += 1
                _notify_pipeline_stuck("В", row["id"], give_up_reason)
                continue
            try:
                _fire(TRIGGER_ID_STEP_C, token_c, _step_c_payload(db, row, internal_token))
                db.execute(_MARK_DISPATCHED, {"id": row["id"]})
                summary["step_c_fired"] += 1
                time.sleep(FIRE_STAGGER_SEC)  # см. FIRE_STAGGER_SEC выше
            except Exception as e:
                summary["errors"] += 1
                print(f"[content_ai] step-c fire failed for candidate {row['id']}: "
                      f"{type(e).__name__}: {e}")
        db.commit()

        # Бэкстоп Шага Н — намеренно МАЛЕНЬКИЙ BATCH_LIMIT_HYPE_FILTER (не
        # BATCH_LIMIT, как у А/В) + тот же FIRE_STAGGER_SEC между вызовами:
        # Вадим попросил не заваливать Routine параллельными запросами разом
        # (риск конкуренции за облачный контейнер, см. FIRE_STAGGER_SEC выше).
        if can_fire_hype:
            hype_pending = db.execute(_SELECT_HYPE_FILTER_PENDING, {
                "cutoff": cutoff, "max_attempts": MAX_DISPATCH_ATTEMPTS,
                "batch_limit": BATCH_LIMIT_HYPE_FILTER,
            }).mappings().all()
            for row in hype_pending:
                try:
                    _fire(TRIGGER_ID_HYPE_FILTER, token_hype,
                          _hype_filter_payload(row["id"], row["source"], row["raw_text"] or row["headline"],
                                                internal_token))
                    db.execute(_MARK_HYPE_FILTER_DISPATCHED, {"id": row["id"]})
                    db.commit()
                    summary["hype_filter_fired"] += 1
                    time.sleep(FIRE_STAGGER_SEC)  # см. FIRE_STAGGER_SEC выше
                except Exception as e:
                    summary["errors"] += 1
                    print(f"[content_ai] hype-filter fire failed for candidate {row['id']}: "
                          f"{type(e).__name__}: {e}")

            # Отдельный проход — кандидаты, у которых уже был последний (MAX-й)
            # повтор и ответа так и не было: сдаёмся молча (это side-канал коллеги,
            # не влияет на "завод" — но notify всё равно, тот же принцип, что у А/В).
            gave_up = db.execute(text("""
                SELECT id FROM content_candidates
                WHERE hype_filter_result IS NULL AND source_url IS NOT NULL
                  AND hype_filter_dispatch_attempts >= :max_attempts
                  AND hype_filter_checked_at < :cutoff
            """), {"max_attempts": MAX_DISPATCH_ATTEMPTS, "cutoff": cutoff}).scalars().all()
            for cid in gave_up:
                db.execute(_GIVE_UP_HYPE_FILTER, {"id": cid})
                summary["hype_filter_gave_up"] += 1
                _notify_pipeline_stuck("Н", cid, f"Routine не ответила за {MAX_DISPATCH_ATTEMPTS} "
                                                   f"попыток бэкстопа — сдаёмся (см. content_ai.py)")
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
