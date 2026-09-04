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
import hashlib
import html
import re
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

from api.agent_trace import (              # noqa: E402
    ВЗЯТО, НЕ_ВЗЯТО, ПУСТО, трассировать,
)
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
# Шаг Г (судья, добавлен 31.08). Вставлен между Шагом В и человеком после живого
# прогона: короткий промпт сам по себе фактуру не гарантирует — в черновике 1104
# новость «против прибыли ₽147,55 млрд ГОДОМ ранее» превратилась в «КВАРТАЛОМ
# ранее», и самопроверка внутри промпта подмену не поймала. Судья НЕ блокирует, а
# размечает: решение остаётся за человеком. Как и у Шага Н — из env, не хардкод,
# чтобы деплой кода не зависел от того, создан ли уже триггер.
TRIGGER_ID_STEP_G = os.environ.get("TRIGGER_ID_STEP_G", "")
# Версия контракта брифа (миграция 059). Поднимать при КАЖДОМ изменении набора полей
# брифа: судья обязан судить черновик по той версии, по которой он написан, иначе
# получает артефактные провалы ворот фактуры. Живой случай — 19 облачных сессий, из
# которых осмысленными оказались 2.
BRIEF_VERSION = 16

# ⚠️ 15 → 30. Кулдаун был РАВЕН периоду крона (*/15), и медленная Routine-сессия
# получала второй запуск: draft_text остаётся NULL, пока сессия не ответила, значит
# кандидат снова попадает в выборку. Итог — два черновика подряд по одному кандидату,
# и вердикт судьи, прочитавшего первый, ложился на второй. Поймано батчем 01.09:
# судья описывал абзацы («прибыль упала на 59% до $829 млн»), которых в сохранённом
# тексте нет вообще.
DISPATCH_COOLDOWN_MIN = 30   # не перевыстреливать кандидата чаще этого окна
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
INTERNAL_API_HOST = "https://framedata.ru"  # ⚠️ punycode: кириллица ломает curl внутри Routine

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
# ⚠️ Найдено 2026-07-17 (жалоба Вадима — ВТБ/Аэрофлот не дошли до коллеги):
# исходный `> 0` здесь предполагал, что ПЕРВЫЙ fire в tg_hype_scan.py всегда
# успевает инкрементировать hype_filter_dispatch_attempts перед тем, как
# может упасть. Но `_MARK_HYPE_FILTER_DISPATCHED` там вызывается ПОСЛЕ
# `_fire()`, внутри того же try — если сам fire бросает исключение (сетевой
# сбой/провижининг облачного контейнера, тот же класс проблем, что уже был
# у Шага А/В), attempts так и остаётся 0 НАВСЕГДА: этот SELECT такую строку
# не видит (`> 0`), И give-up/alert-запрос ниже тоже не видит (`>= max_attempts`
# никогда не станет true от 0) — кандидат тихо теряется без единого уведомления,
# даже без "Шаг Н сдался". Живой пример: id 772/773 — `step-a` PATCH в логах
# есть, `hype-filter` PATCH — ни разу. Убрал `> 0`, добавил `created_at < :cutoff`
# — та же граница, что и у "давно не проверялось", защищает от гонки с ЕЩЁ НЕ
# случившейся первой попыткой tg_hype_scan.py (крон раз в 2 мин, cutoff здесь
# 15 мин — с большим запасом).
# ⚠️ ТОЛЬКО status='draft_ready'. Первый живой прогон судьи выстрелил по ДЕСЯТИ
# кандидатам, из которых все десять были уже решены человеком (published/rejected),
# и все десять получили «брак» по одним и тем же двум пунктам. Судья был технически
# прав — тех чисел в брифе действительно нет, — но судил СТАРЫЕ черновики по НОВОМУ
# брифу: в нём больше нет market_rank, recent_signals, ATR-множителя и двухлетнего
# диапазона oi_context, из которых старые черновики брали цифры. Историю по
# изменившемуся контракту судить бессмысленно, а решённых кандидатов — ещё и
# бесполезно: 10 облачных сессий впустую.
#
# Судить есть что, когда черновик уже есть, а вердикта ещё нет. Отдельный
# счётчик попыток и своя терминальная метка (judge_gave_up_at) — статус кандидата
# Шаг Г не трогает, значит выводить строку из выборки должен собственный признак,
# ровно как у Шага Н (см. миграцию 048 и её разбор).
_SELECT_JUDGE_PENDING = text("""
    SELECT c.id, c.headline, c.raw_text, c.tickers, c.event_type, c.draft_text,
           c.thread_key, c.created_at, c.judge_dispatch_attempts,
           a.asset_id, a.asset_name, a.clgroup AS anomaly_clgroup,
           a.direction, a.severity_value, a.signal_date
    FROM content_candidates c
    JOIN anomalies a ON a.id = c.matched_anomaly_id
    WHERE c.draft_text IS NOT NULL
      AND c.status = 'draft_ready'
      AND c.brief_version = :brief_version
      AND c.judge_verdict IS NULL
      AND c.judge_gave_up_at IS NULL
      AND c.judge_dispatch_attempts < :max_attempts
      AND (c.judge_checked_at IS NULL OR c.judge_checked_at < :cutoff)
    ORDER BY c.id
    LIMIT :batch_limit
""")
_MARK_JUDGE_DISPATCHED = text("""
    UPDATE content_candidates
    SET judge_dispatch_attempts = judge_dispatch_attempts + 1, judge_checked_at = now()
    WHERE id = :id
""")
_GIVE_UP_JUDGE = text("""
    UPDATE content_candidates
    SET judge_checked_at = now(), judge_gave_up_at = now()
    WHERE id = :id
""")

_SELECT_HYPE_FILTER_PENDING = text("""
    SELECT id, source, headline, raw_text, hype_filter_dispatch_attempts
    FROM content_candidates
    WHERE hype_filter_result IS NULL AND source_url IS NOT NULL
      AND hype_filter_gave_up_at IS NULL
      AND hype_filter_dispatch_attempts < :max_attempts
      AND created_at < :cutoff
      AND (hype_filter_checked_at IS NULL OR hype_filter_checked_at < :cutoff)
    ORDER BY id
    LIMIT :batch_limit
""")
_MARK_HYPE_FILTER_DISPATCHED = text("""
    UPDATE content_candidates
    SET hype_filter_dispatch_attempts = hype_filter_dispatch_attempts + 1, hype_filter_checked_at = now()
    WHERE id = :id
""")
# ⚠️ Найдено 2026-08-11: раньше здесь был ТОЛЬКО `hype_filter_checked_at =
# now()`. Это не выводило строку из выборки «сдались» ниже — её условия
# (hype_filter_result IS NULL + attempts >= MAX) вечны, а `checked_at < cutoff`
# снова становилось истинным ровно через DISPATCH_COOLDOWN_MIN, то есть через
# один прогон крона. Итог: один и тот же алерт уходил Вадиму каждые полчаса
# бесконечно (живой случай — 15 кандидатов, 13 повторов за 6 часов, пока сам
# инцидент давно закончился). Терминальный признак обязателен: у Шага А/В его
# роль играет status (discarded/pending), у Шага Н статуса нет.
_GIVE_UP_HYPE_FILTER = text("""
    UPDATE content_candidates
    SET hype_filter_checked_at = now(), hype_filter_gave_up_at = now()
    WHERE id = :id
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


def _notify_pipeline_stuck(step: str, gave_up: list) -> None:
    """Найдено 2026-07-16 (Вадим): give-up после MAX_DISPATCH_ATTEMPTS менял
    статус кандидата МОЛЧА — узнавали о сломанной Routine (env-misconfig,
    кончившаяся подписка) только постфактум, через ручные 409 часы спустя.
    Сюда — та же связка HYPE_NOTIFY_BOT_TOKEN/CHAT_ID + TELEGRAM_API_ROOT
    релей, что и у api/routers/content_news.py:_notify_hype_colleague, но
    вызывается ХОСТ-СКРИПТОМ (content_ai.py уже на host, не в api-контейнере
    — .env читает напрямую, дублировать docker-compose проброс не нужно).
    Best-effort — сбой отправки не должен ронять сам бэкстоп.

    Найдено 2026-07-17 (после многочасового 401 на fire-эндпоинте, инцидент
    Anthropic): раньше слали ОДНО сообщение НА КАЖДОГО сдавшегося кандидата —
    при затяжном сбое (не разовый глюк, а Routine реально недоступна часами)
    десятки кандидатов пересекают MAX_DISPATCH_ATTEMPTS в одном и том же
    прогоне почти одновременно → флуд из N почти одинаковых сообщений подряд.
    Теперь вызывающий копит give-up'ы шага за весь прогон в список и зовёт
    сюда ОДИН раз — здесь одно сообщение на всех. gave_up — список
    (candidate_id, reason); причина у всех кандидатов одного шага в одном
    прогоне идентична (один и тот же MAX_DISPATCH_ATTEMPTS-текст), поэтому
    печатаем её один раз, а не N раз."""
    if not gave_up:
        return
    token = os.environ.get("HYPE_NOTIFY_BOT_TOKEN", "")
    chat_id = os.environ.get("HYPE_NOTIFY_CHAT_ID", "")
    if not token or not chat_id:
        return
    n = len(gave_up)
    ids_line = ", ".join(f"#{cid}" for cid, _ in gave_up)
    reason = gave_up[0][1]
    text_msg = (
        f"⚠️ Шаг {step} сдался по {n} {'кандидату' if n == 1 else 'кандидатам'} "
        f"после {MAX_DISPATCH_ATTEMPTS} попыток бэкстопа — Routine не ответила.\n\n"
        f"{ids_line}\n\n{reason}\n\n"
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


def _ru(x) -> str:
    """Точка → запятая в отформатированном числе: питоновский float/Decimal даёт
    точку по умолчанию (4.69, -34.4), русская типографика — запятую (4,69, -34,4).
    Найдено 2026-07-17 — модель Шага В честно копирует цифры брифа как есть,
    включая точку, поэтому чинить нужно на входе, а не надеяться на промпт."""
    return str(x).replace(".", ",")


def _raz(n: int) -> str:
    """«в 2 раза» / «в 5 раз» — русское согласование числительного."""
    return "раза" if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14) else "раз"


def _times(r: float) -> str:
    """Кратность КРУГЛЫМ человеческим числом: 3,03 → «в 3 раза», 1,46 → «примерно
    в полтора раза», 12,7 → «более чем в 13 раз».

    ⚠️ Почему округляем в брифе, а не просим в промпте. Модель Шага В дословно
    копирует числа брифа (тот же механизм, что с точкой вместо запятой в _ru).
    Бриф v3 отдал «в 3,03 раза» и «на 46,2%» — модель напечатала ровно это.
    Вадим 31.08: «стараться искать крупные круглые числа — это в 3 раза больше».
    Точность до сотых в тексте про толпу не несёт смысла и выдаёт машину.
    """
    if r < 1.4:
        return ""  # мелкий рост — кратность не говорят, отдаём проценты
    if r < 1.75:
        return "примерно в полтора раза"
    # Прижимаем к ближайшей половинке (до 10× — «в 2,5 раза» ещё круглое и точнее,
    # чем «почти в 3 раза»), выше — к целому: «в 12,5 раза» уже не читается.
    step = 1.0 if r >= 10 else 0.5
    h = round(r / step) * step
    if h == 2 and abs(r - 2) > 0.15:
        return "почти вдвое" if r < 2 else "более чем вдвое"
    label = (f"{int(h)} {_raz(int(h))}" if h == int(h) else f"{_ru(f'{h:.1f}')} раза")
    if abs(r - h) <= 0.15:
        return f"в {label}"
    return f"почти в {label}" if r < h else f"более чем в {label}"


def _pct(p: float) -> str:
    """Процент округлённо. Крупное движение — до 5%, среднее — до целого, мелкое
    оставляем как есть: «на 3,2%» честно, а «примерно на 5%» из 3,2% — враньё."""
    a = abs(p)
    if a >= 20:
        return f"примерно на {round(a / 5) * 5}%"
    if a >= 5:
        return f"на {round(a)}%"
    return f"на {_ru(f'{a:.1f}')}%"


def _window_ru(days: int) -> str:
    """Окно наблюдения словами. 400 дн. в тексте — протёкшая техническая деталь:
    бриф v3 дал «за последние 400 дней», и модель это напечатала."""
    if days >= 330:
        years = days / 365
        return "год" if years < 1.25 else f"{years:.0f} года"
    if days >= 150:
        return "полгода"
    if days >= 75:
        return "квартал"
    return f"{max(1, round(days / 30))} мес."


def _money_ru(v: float) -> str:
    """Цена без копеечной точности: ₽91,81 → «около ₽92». Копейки значимы только
    для дешёвых бумаг, где они и есть основная часть цены."""
    if v >= 1000:
        return f"около {round(v / 10) * 10:,.0f}".replace(",", " ")
    if v >= 100:
        return f"около {v:,.0f}".replace(",", " ")
    if v >= 10:
        return f"около {_ru(f'{v:.0f}')}"
    return _ru(f"{v:.2f}")


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


_SELECT_STOCK_FOR_FUTURES = text(
    "SELECT stock_ticker FROM ticker_futures_map WHERE futures_sectype = :f LIMIT 1")
_SELECT_PRICE_SERIES = text("""
    SELECT begin_time::date AS d, close
    FROM candles
    WHERE secid = :secid AND interval = 24 AND type = 'stock'
      AND begin_time::date BETWEEN :since AND :as_of
    ORDER BY begin_time
""")


def _price_context(db, asset_id: str, tickers, as_of) -> dict:
    """Цена за длинные горизонты — чтобы позицию можно было привязать к простому
    сравнению («цена упала вдвое, а лонг вырос втрое»). Именно так пишет канал:
    «за 10 дней сложились в 2 раза», «с начала года потеряли около 60%».

    Без этого блока модель оперирует только позициями, и текст получается про
    контракты вместо истории."""
    secid = (tickers or [None])[0]
    if not secid:
        secid = db.execute(_SELECT_STOCK_FOR_FUTURES, {"f": asset_id}).scalar()
    if not secid:
        return {}
    rows = db.execute(_SELECT_PRICE_SERIES, {
        "secid": secid, "since": as_of - timedelta(days=400), "as_of": as_of,
    }).fetchall()
    if len(rows) < 20:
        return {}
    dates = [r[0] for r in rows]
    px = {r[0]: float(r[1]) for r in rows if r[1] is not None}
    if not px:
        return {}
    last_d = dates[-1]
    # ⚠️ Ключ говорит, как пользоваться. Отдельным предложением «Акция сейчас стоит
    # 7,30 рубля» цена повисает в конце поста ни к чему — Вадим 01.09 назвал это
    # шумом. В понравившемся ему варианте цена стояла В ОДНОЙ фразе с изменением:
    # «Акция Системы стоит 7,30 — за год упала более чем вдвое».
    out = {"цена_сейчас_только_вместе_с_изменением": _money_ru(px[last_d])}
    for days, label in ((30, "за_месяц"), (180, "за_полгода"), (365, "за_год")):
        target = last_d - timedelta(days=days)
        base_d = min((d for d in dates if d >= target and d in px), default=None)
        if not base_d or base_d == last_d or not px[base_d]:
            continue
        chg = (px[last_d] - px[base_d]) / px[base_d] * 100
        # Круглое сравнение, если движение крупное: «упала вдвое» читается лучше,
        # чем «упала на 51,3%». Вадим 31.08: «стараться привязать всё к простому».
        word = "упала" if chg < 0 else "выросла"
        r = px[base_d] / px[last_d] if chg < 0 else px[last_d] / px[base_d]
        t = _times(r) if abs(chg) >= 40 else ""
        out[f"цена_{label}"] = f"{word} {t}" if t else f"{word} {_pct(chg)}"
    return out


def _position_phrases(asset_id: str, clgroup: str | None, as_of=None) -> dict:
    """Изменение позиции ЧЕЛОВЕЧЕСКОЙ фразой + выбор главного числа.

    ⚠️ Почему не ATR-множитель. Детектор считает ratio = |Δ за день| / ATR(14):
    ×4,69 означает «дневное изменение в 4,69 раза крупнее обычного дневного», а НЕ
    рост позиции. Канал говорит «лонг вырос в 5 раз» — это отношение «было → стало»
    за период. Подмена дала фактические ошибки в двух ОПУБЛИКОВАННЫХ постах (793:
    «выросла в 3,53 раза за один день», хотя за день было +37%). Множитель остаётся
    служебным признаком аномальности и в текст поста не идёт.
    См. research/content_pipeline_v2/METRIC_MISMATCH.md.

    ⚠️ Почему ГЛАВНОЕ_ЧИСЛО выбирает код, а не модель. Замер: когда бриф выкладывал
    шесть равноправных чисел, модель добросовестно брала все — плотность оставалась
    5 показателей на абзац против 1-2 у канала. «Дай всё и надейся, что выберет» не
    работает; это тот же провал, что и с раздутым промптом."""
    from signals.db import get_position_series
    clg = clgroup or "FIZ"
    # ⚠️ as_of ОБЯЗАТЕЛЕН. Без него ряд заканчивается СЕГОДНЯШНИМ днём, а не датой
    # сигнала: поймано тестом на историческом кандидате 793 — бриф сообщал
    # «дата_сигнала 2026-07-16» и при этом позицию на 2026-08-31 (58 489 контрактов
    # вместо 38 943). В живой работе Шаг В стреляет сразу после сигнала и даты почти
    # совпадают, но «почти» недопустимо там, где весь смысл в корректности дат.
    series = get_position_series(asset_id, clg, days=400, as_of_date=as_of)
    if len(series) < 3:
        return {"ошибка": "недостаточно истории по инструменту"}

    dates = [r[0] for r in series]
    net = {r[0]: r[1] for r in series}
    last_d, last = dates[-1], net[dates[-1]]
    prev = net[dates[-2]]
    word = "лонг" if last > 0 else "шорт"

    def phrase(new_v, old_v) -> str:
        # ⚠️ Процент через смену знака бессмыслен: VK (лонг → шорт) давал «−369,9%»,
        # и модель написала «позиция изменилась на −369,9%» — человек прочтёт
        # «упало на 370%». При развороте отдаём описание разворота.
        if not old_v:
            return "не с чем сравнить — позиции не было"
        if (old_v > 0) != (new_v > 0):
            # ⚠️ Без голых контрактов. Бриф v3 отдавал «было лонг 4142, стало шорт
            # 2333» — и черновик 1357 напечатал именно это. Вадим 31.08: «просто
            # количество контрактов никому не интересно, нужна интерпретация».
            was, now = ("лонг", "шорт") if old_v > 0 else ("шорт", "лонг")
            # ⚠️ Без размерной оговорки. В v5 я добавлял «причём новый шорт крупнее
            # прежнего лонга вдвое», чтобы не терять масштаб. Вадим по 1638:
            # «ну и извращенское заявление, предыдущего хватает более чем». Сам факт
            # разворота — уже сильное утверждение, и он самодостаточен; сравнение
            # размеров двух позиций разного знака читателю ничего не добавляет.
            return f"толпа перевернулась из чистого {was}а в чистый {now}"
        grew = abs(new_v) > abs(old_v)
        r = abs(new_v) / abs(old_v)
        verb = "вырос" if grew else "сократился"
        chg = abs(abs(new_v) - abs(old_v)) / abs(old_v) * 100
        if grew:
            t = _times(r)
            if t:
                return f"чистый {word} {verb} {t}"
        else:
            t = _times(1 / r) if r else ""
            if t and chg >= 50:
                return f"чистый {word} {verb} {t}"
        return f"чистый {word} {verb} {_pct(chg)}"

    def strength(new_v, old_v) -> float:
        if not old_v:
            return 0.0
        if (old_v > 0) != (new_v > 0):
            return (abs(new_v) + abs(old_v)) / abs(old_v)
        return abs(abs(new_v) - abs(old_v)) / abs(old_v)

    periods = {"за_сутки": (phrase(last, prev), strength(last, prev))}
    for days, label in ((7, "за_неделю"), (30, "за_месяц"),
                        (180, "за_полгода"), (365, "за_год")):
        target = last_d - timedelta(days=days)
        base_d = min((d for d in dates if d >= target), default=None)
        if base_d and base_d != last_d:
            periods[label] = (phrase(last, net[base_d]), strength(last, net[base_d]))

    lead = max(periods, key=lambda k: periods[k][1])
    pcts = []
    for _d, n, _npart, pl, ps in series:
        gross = (pl or 0) - (ps or 0)
        pcts.append((n / gross * 100) if gross else 0.0)
    span = (dates[-1] - dates[0]).days
    # ⚠️ Терминология — тоже интерфейс. Вадим 31.08: «перекос net/gross» в пост
    # писать нельзя, это жаргон. Модель писала его дословно, потому что дословно
    # так называлось поле брифа. Переименовываем и переводим на человеческий:
    # net/gross — это доля чистой позиции в ОТКРЫТОМ ИНТЕРЕСЕ физлиц.
    share = abs(pcts[-1])
    return {
        # ⚠️ Ключи с «_код_» удаляет _build_brief ДО отправки модели: они нужны
        # только коду, чтобы собрать парную связку «позиция ↔ цена» по ОДНОМУ
        # окну. В брифе их быть не должно — любое видимое поле модель считает
        # обязанной израсходовать, и оно вернётся числом в текст.
        "_код_период_главного_числа": lead,
        "_код_фраза_главного_числа": periods[lead][0],
        "направление_позиции": f"чистый {word.upper()}",
        "ГЛАВНОЕ_ЧИСЛО": f"{lead}: {periods[lead][0]}",
        "фон_упоминать_не_обязательно": {k: v[0] for k, v in periods.items() if k != lead},

        # ⚠️ Окно в НАЗВАНИИ поля: прежнее «перекос_диапазон_за_ряд» модель прочла
        # как «за всё время наблюдений» и написала «максимум за всё время».
        # ⚠️ Имя поля переименовано в «не_для_текста»: под прежним названием модель
        # выносила диапазон в пост («а за год доля доходила до 50%»), и Вадим 01.09:
        # «сложная формулировка, не понятна сразу, нужно проще». Поле нужно только
        # чтобы НЕ соврать про рекорд — это проверка, а не содержание.
        f"не_для_текста_проверка_рекорда_за_{_window_ru(span).replace(' ', '_').replace('.', '')}": (
            f"диапазон от {_ru(f'{min(pcts):.0f}')}% до {_ru(f'{max(pcts):.0f}')}% — "
            f"это окно за {_window_ru(span)}, НЕ исторический экстремум. "
            f"В ТЕКСТ ЭТОТ ДИАПАЗОН НЕ ВЫНОСИТЬ — он тут только для проверки: если "
            f"собрался написать «рекорд», сверься и не пиши, либо пиши с оговоркой "
            f"про окно словами («за год»), а не в днях."),
        "служебное_не_для_текста": {
            "чистая_позиция_контрактов": abs(last),
            "пояснение": ("Голое число контрактов читателю ничего не говорит "
                           "(Вадим 31.08). В текст выносить НЕ надо — нужна "
                           "интерпретация: во сколько раз или на сколько процентов."),
        },
    }


def _story_frame(signal_date, news_date) -> str:
    """Рамка сюжета из порядка дат. Её отсутствие дало черновик 845: сигнал был
    датирован на 3 дня ПОЗЖЕ новости, а пост утверждал «толпа шла в шорт ещё до
    ралли». Порядок дат известен точно — значит и говорить о нём должен код, а не
    модель по догадке."""
    d = (signal_date - news_date).days
    # ⚠️ Порог 2 дня и ослабленная формулировка — правка Вадима по кандидату 1638
    # (АКРА/AFKS): «фьючерс поменялся за день и спрогнозировало — спорное заявление
    # и кто знает». Прежняя рамка при отрыве в ОДИН день выдавала «только в этой
    # рамке можно говорить, что толпа встала заранее» — то есть бриф САМ разрешал
    # утверждение о предвидении, и модель им пользовалась в выводе поста. Один день
    # от шума не отличим.
    #
    # Заодно ослаблено и само УПРЕЖДЕНИЕ: порядок дат можно КОНСТАТИРОВАТЬ, но он
    # не доказывает предвидения ни при каком отрыве. «И кто знает» — это про то,
    # что причинности в данных нет, сколько бы дней ни было.
    if d <= -2:
        return (f"УПРЕЖДЕНИЕ: позиции менялись за {abs(d)} дн. ДО новости. Можно "
                f"КОНСТАТИРОВАТЬ этот порядок дат. Утверждать, что толпа знала "
                f"заранее, предвидела или спрогнозировала событие, НЕЛЬЗЯ: "
                f"совпадение по времени не доказывает предвидение. Сам этот запрет в "
                f"посте не проговаривай — просто не заявляй предвидение.")
    if d == -1:
        # ⚠️ Указание НЕ ДЛЯ ТЕКСТА. Модель вынесла оговорку в пост дословно:
        # «Разворот случился за день до новости — от шума такой срок почти не
        # отличить». Вадим 01.09: «вот эта часть уже не нужна, это просто шум».
        # Запрет, попавший в бриф, модель проговаривает вслух — как и «пробелы в
        # данных», которые пришлось запрещать отдельным правилом.
        return ("СОВПАДЕНИЕ: позиции менялись за 1 дн. до новости — от шума такой "
                "срок не отличим. НЕ заявляй предвидение. И НЕ пиши в посте про сам "
                "суточный разрыв и про шум: это указание тебе, а не факт для "
                "читателя — просто промолчи об этом.")
    if d == 0:
        return ("СОВПАДЕНИЕ: позиции и новость в один день. Утверждать, что толпа "
                "встала ЗАРАНЕЕ, нельзя — данных на это нет. Сам этот запрет в посте "
                "не проговаривай — просто не заявляй предвидение.")
    return (f"РЕАКЦИЯ: позиции менялись на {d} дн. ПОЗЖЕ новости. Фразы «знали "
            f"заранее», «встали до» запрещены как факт. И НЕ пиши в посте, что это "
            f"«отклик, а не опережение»: это указание тебе, а не факт для читателя. "
            f"Батч 01.09 показал утечку — черновик 1104 напечатал «Это отклик на "
            f"новость, не опережение» дословно.")


_HORIZON_ORDER = ["за_месяц", "за_полгода", "за_год"]


def _pair_price_with_position(price: dict, pos: dict) -> dict:
    """Одна готовая связка «позиция ↔ цена» по ОДНОМУ окну вместо четырёх
    равноправных горизонтов.

    ⚠️ Зачем. Судья на кандидате 1104 поставил дефект numbers_density: во втором
    абзаце оказалось четыре числа («в 3 раза», «84%», «на 25%», «около 92») против
    одного-двух у канала. Правило «не больше двух чисел» в промпте уже было — и не
    сработало, потому что дело не в правиле: блок цена_акции выкладывал цену
    сейчас и три горизонта равным весом, а любое поле брифа модель считает
    обязанной израсходовать. Это ровно та болезнь, от которой для позиций спасло
    ГЛАВНОЕ_ЧИСЛО; здесь тот же приём.

    ⚠️ Окна называем оба и явно. Если у позиции ведущее окно «за_сутки», парного
    окна у цены нет — тогда берём самый длинный горизонт цены и проговариваем ОБА
    срока, а не выдаём разные окна за одно («за сутки лонг втрое, акция вдвое» —
    ложь, которую читатель не поймает).
    """
    lead = pos.get("_код_период_главного_числа")
    phrase = pos.get("_код_фраза_главного_числа")
    if not price or not lead or not phrase:
        return price
    horizons = {k[len("цена_"):]: v for k, v in price.items() if k.startswith("цена_за_")}
    if not horizons:
        return price
    matched = lead in horizons
    key = lead if matched else max(horizons, key=lambda k: _HORIZON_ORDER.index(k))
    window = key.replace("за_", "за ")
    if matched:
        pair = f"{window}: акция {horizons[key]}, а {phrase}"
    else:
        pair = (f"{phrase} ({lead.replace('за_', 'за ')}), "
                f"а акция {horizons[key]} {window}")
    out = {"цена_сейчас": price.get("цена_сейчас", ""),
           "ГЛАВНОЕ_СРАВНЕНИЕ": pair + " — это и есть связка для поста"}
    rest = {k: v for k, v in horizons.items() if k != key}
    if rest:
        out["остальные_горизонты_упоминать_не_обязательно"] = rest
    return out


# ⚠️ ВОЗРАСТ ФАКТА ЕДЕТ ВМЕСТЕ С ФАКТОМ. Раньше выбирался только текст, и связь,
# снятая пять лет назад, приходила в бриф неотличимой от вчерашней. Дата в тексте
# есть не у всех рёбер: курируемые (link:*) писались людьми и её могут не содержать.
_STALE_YEARS = 2


def _лет(n: int) -> str:
    """«5 ГОДА(ЛЕТ)» читается как машинный вывод и подрывает доверие к тексту,
    в который вшито. Пост пишется по-русски — предупреждение тоже."""
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return "лет"
    return {1: "год", 2: "года", 3: "года", 4: "года"}.get(n % 10, "лет")

_SELECT_ENTITY_LINKS = text("""
    SELECT entities, statement,
           EXTRACT(YEAR FROM age(CURRENT_DATE, valid_from))::int AS лет,
           confidence
    FROM world_facts
    WHERE kind = 'связь'
      AND entities && CAST(:tickers AS text[])
      AND valid_from <= :as_of
      AND (valid_until IS NULL OR valid_until >= :as_of)
      AND superseded_by IS NULL
    ORDER BY confidence DESC, valid_from DESC
    LIMIT 4
""")

_SELECT_TICKER_NAME = text("SELECT name FROM instruments WHERE sec_id = :tk LIMIT 1")

# ⚠️ Отбор по РАЗМЕТКЕ tickers, а не по ILIKE по тексту. Поиск словом «озон» тащит
# Озон Фармацевтику (OZPH) — другую компанию; разметка их разделяет.
#
# ⚠️ И ИМЕННО `tickers @> ARRAY[:tk]`, А НЕ `:tk = ANY(tickers)`. Форма с ANY GIN-индекс
# использовать НЕ МОЖЕТ: планировщик шёл по индексу дат и отбрасывал фильтром 24 тысячи
# строк. Замер на проде 03.09.2026, окно 4 месяца по GAZP:
#     = ANY  → cost 18 545, 16 349 буферов, 3 936 мс на холодном кэше
#     @>     → cost  1 032,    451 буфер,      36 мс
# Индекс idx_news_archive_tickers существовал всё это время и просто не применялся.
_SELECT_TICKER_NEWS = text("""
    SELECT posted_at::date AS d, text, coalesce(array_length(tickers, 1), 0) AS nt
    FROM news_archive
    WHERE tickers @> ARRAY[:tk]::text[] AND posted_at >= :since AND posted_at <= :until
    ORDER BY posted_at DESC
    LIMIT 40
""")


def _pick_snippet(body: str, nt: int, name: str) -> str:
    """Из поста — короткий фрагмент про НУЖНУЮ компанию.

    ⚠️ Почему абзац, а не пост. Самые содержательные посты архива — дайджесты
    «Итоги дня»: медиана 1 тикер на пост, но максимум 61. Именно в дайджесте лежала
    вся фактура по Озону (залог, атаки БПЛА). Отдать такой пост в бриф целиком —
    залить его десятком чужих тикеров, а поле, попавшее в бриф, модель считает
    обязанной израсходовать.
    """
    lines = [ln.strip() for ln in (body or "").splitlines() if ln.strip()]
    if not lines:
        return ""
    if nt <= 3:
        return " ".join(lines[:2])[:220]
    # Дайджест: берём строку, где компания названа по имени. Совпадение по слову
    # целиком, иначе «Озон» поймает «ОзонФарма».
    pat = re.compile(rf"(?<![А-Яа-яЁёA-Za-z]){re.escape(name)}(?![А-Яа-яЁёA-Za-z])", re.I)
    for ln in lines:
        if pat.search(ln):
            return ln[:220]
    return ""


def _related_context(db, tickers, as_of, trace=None) -> dict:
    """Связанные компании: что у них с ценой и что о них писал архив.

    trace — счётчик следа (api/agent_trace). Здесь агент реально ходит по графу
    владения, и без записи этого обхода потом невозможно объяснить, почему в текст
    попал Озон, а МТС нет. Необязателен: без него функция работает как раньше.

    ⚠️ Это КОНТЕКСТ, НЕ ПРИЧИНА, и подписано так в самом брифе. Разбор кандидата
    1638 (АКРА понизило рейтинг АФК Системы) показал соблазн: цепочка «залог Озона →
    Озон упал на 40% → рейтинг понизили» выглядит убедительно, но обоснования АКРА
    в наших данных НЕТ. Утверждать причину нельзя — можно показать
    последовательность. Ворота claim_falsifiable и event_matters у судьи ловят
    именно это.
    """
    tks = [t for t in (tickers or []) if t]
    if not tks:
        return {}
    links = db.execute(_SELECT_ENTITY_LINKS,
                       {"tickers": tks, "as_of": as_of}).fetchall()
    if trace:
        старых = sum(1 for r in links
                     if (r[2] or 0) >= _STALE_YEARS
                     and (r[3] is None or float(r[3]) >= 0.80))
        trace.record("world_facts", "кто связан с %s" % ", ".join(tks),
                     outcome=(ВЗЯТО if links else ПУСТО),
                     result_count=len(links),
                     result_note=(("%d рёбер, из них %d со старым снимком"
                                   % (len(links), старых)) if links else "связей нет"),
                     params={"tickers": tks, "as_of": as_of, "старых": старых})
    if not links:
        return {}
    out = {}
    for entities, statement, лет, conf in links:
        # ⚠️ У факта, чей источник даты НЕ УКАЗАЛ, valid_from — консервативная
        # заглушка, а не снимок. Объявить по ней «снимку 6 лет» значило бы выдумать
        # возраст: мы не знаем его ни в одну сторону. Такие факты помечены низкой
        # уверенностью, и предупреждение у них уже вшито в текст при записи.
        известна_дата = conf is None or float(conf) >= 0.80
        if известна_дата and лет is not None and лет >= _STALE_YEARS:
            # Не выбрасываем: старая связь чаще всего верна (контрольный пакет
            # держат десятилетиями), а выбросив её, мы потеряем настоящий сюжет.
            # Но и молчать нельзя — приписка идёт В САМ ТЕКСТ факта, потому что
            # модель читает факты, а не наши поля рядом с ними.
            statement = ("%s [СНИМКУ %d %s: долю называть только со ссылкой на дату "
                         "снимка, «сейчас» про неё писать нельзя]"
                         % (statement, лет, _лет(лет)))
        for tk in (entities or []):
            if tk in tks or tk in out:
                continue
            if len(out) >= 2:
                # Лимит в две компании — не «ничего не нашлось», а осознанный отказ.
                # Без этой строки в дашборде связь выглядела бы просто отсутствующей.
                if trace:
                    trace.record("world_facts", "связанная компания %s" % tk,
                                 outcome=НЕ_ВЗЯТО, result_count=1,
                                 result_note=statement[:120],
                                 reason="в бриф идут максимум две связанные компании")
                continue
            name = db.execute(_SELECT_TICKER_NAME, {"tk": tk}).scalar() or tk
            item = {"связь": statement}
            price = _price_context(db, tk, [tk], as_of)
            for k in ("цена_за_месяц", "цена_за_полгода"):
                if price.get(k):
                    item[k.replace("цена_", "цена_")] = price[k]
            rows = db.execute(_SELECT_TICKER_NEWS, {
                "tk": tk, "since": as_of - timedelta(days=120), "until": as_of,
            }).fetchall()
            seen, snippets = set(), []
            for d, body, nt in rows:
                sn = _pick_snippet(body, nt, name)
                key = sn[:60]
                if sn and key not in seen:
                    seen.add(key)
                    snippets.append(f"{d.strftime('%d.%m.%Y')}: {sn}")
                if len(snippets) >= 2:
                    break
            if snippets:
                item["из_архива"] = snippets
            out[tk] = item
            if trace:
                trace.record("news_archive", "что писали про %s" % name,
                             outcome=(ВЗЯТО if snippets else НЕ_ВЗЯТО),
                             result_count=len(snippets),
                             result_note=("; ".join(snippets))[:200] if snippets else None,
                             reason=None if snippets else "связь есть, свежих событий нет",
                             params={"ticker": tk, "окно_дней": 120})
    if not out:
        return {}
    out["ПОЯСНЕНИЕ"] = (
        "КОНТЕКСТ, НЕ ПРИЧИНА. Это связанные компании и то, что о них писали. "
        "Официального обоснования события у нас НЕТ. Показывать можно только "
        "последовательность («там произошло это, здесь — то»); утверждать, что одно "
        "вызвало другое, НЕЛЬЗЯ. Долю владения в процентах не указывать — её в "
        "данных нет. "
        # ⚠️ Место в тексте — часть смысла, а не оформление. Судья по черновику 1638:
        # «соседство с абзацем о позициях толпы может подтолкнуть читателя додумать
        # связь, которая прямо не утверждается». Причинность возникала не из слов, а
        # из порядка абзацев: сначала позиции, сразу за ними — беды у дочек, и
        # читатель сам склеивал. Указание живёт ЗДЕСЬ, рядом с данными, а не
        # очередным правилом в промпте.
        "МЕСТО В ТЕКСТЕ: этот блок идёт РАНЬШЕ разговора о позициях толпы — он фон "
        "самого события. Если поставить его сразу после абзаца о позициях, читатель "
        "склеит причинность, которую мы не утверждаем. "
        # ⚠️ Вадим 01.09: новый черновик «стал длиньше и более подробным» и понравился
        # меньше. Замер подтвердил: 1105 знаков против медианы жанра 661. Причина
        # предсказуемая — добавил в бриф два блока и не поставил границу, а поле в
        # брифе модель считает обязанной израсходовать целиком.
        # ⚠️ Было «максимум ОДИН факт отсюда» — и модель выбросила Озон целиком
        # вместе с залогом под кредит ВТБ, то есть самое интересное. Граница нужна по
        # ОБЪЁМУ (фраза), а не по числу компаний.
        # ⚠️ Было «по одной фразе на компанию, обе в одном абзаце» — и модель
        # втиснула три факта в одно предложение со вставкой в скобках. Замер: у
        # канала медиана 11 слов в предложении и скобочные вставки в 3% абзацев, у
        # того черновика — 17 слов, 75-й процентиль 32 и вставки в трети абзацев.
        # ⚠️ Один факт на компанию оказалось СЛИШКОМ туго: в абзаце, который Вадим
        # назвал лучшим, про Озон было ДВА факта — сорванная сделка со Сбером и залог
        # пакета. Разрешаем одно-два коротких предложения на компанию.
        "ОБЪЁМ: это ФОН, а не содержание поста. Из каждой компании — самое крупное, "
        "ОДНИМ-ДВУМЯ КОРОТКИМИ ПРЕДЛОЖЕНИЯМИ. Не втискивать две компании в одно "
        "предложение и не добавлять уточнений в скобках. "
        # ⚠️ Порядок переписан второй раз. Первая версия говорила «сначала кто кому
        # кем является, потом что у кого произошло» — и получилось буквально: два
        # родства свалены в одно предложение, а дальше факты в обратном хронологическом
        # порядке. Вадим 01.09: «как-то не складно, будто просто факты накидал, но нет
        # начала связи». Правило было верным по идее и слишком буквальным по форме.
        "ПОРЯДОК АБЗАЦА, три шага:\n"
        "  1) ПЕРВОЕ предложение — зачем эти компании вообще в посте. Одной короткой "
        "фразой и СТРУКТУРНЫМ фактом, без причинности: «Система — холдинг, и оба эти "
        "актива её». Не выкладывай все родства разом: «Сегежа входит в группу Системы, "
        "а сама Система — крупный акционер Озона» — это уже два родства в одном "
        "предложении, читателю их не удержать.\n"
        "  2) Дальше по ОДНОЙ компании на предложение, и в порядке ВРЕМЕНИ — от "
        "раннего к позднему, а не наоборот.\n"
        "  3) Родство каждой компании упоминай там же, где её факт, коротким "
        "оборотом: «Сегеже, которая тоже входит в группу, рейтинг срезали 28 августа». "
        "⚠️ Если в факте стоит пометка СПОРНО — либо не пиши это вовсе, либо пиши с "
        "указанием, кто именно что заявил. Утвердительно спорное подавать нельзя."
    )
    return out


# Рейтинговые агентства и шкалы. ⚠️ Шкала у каждого агентства СВОЯ: AA-(RU) у АКРА и
# ruAA- у Эксперт РА — разные системы, и сравнивать их между собой нельзя.
_RATING_AGENCIES = ("АКРА", "Эксперт РА", "ЭкспертРА", "НКР", "НРА", "АК&М",
                    "Fitch", "Moody", "S&P", "Standard")
_RE_AGENCY = re.compile("|".join(_RATING_AGENCIES), re.I)
# Уровень: латинская лесенка, опционально с префиксом ru или суффиксом (RU).
_RE_SCALE = re.compile(
    r"(?<![A-Za-zА-Яа-я])(?:ru)?(?:AAA|AA[+-]?|A[+-]?|BBB[+-]?|BB[+-]?|B[+-]?"
    r"|CCC[+-]?|CC|C|D)(?:\(RU\))?(?![A-Za-zА-Яа-я])")

_SELECT_RATING_LINES = text("""
    SELECT posted_at::date AS d, unnest(string_to_array(text, chr(10))) AS ln
    FROM news_archive
    WHERE tickers @> ARRAY[:tk]::text[] AND posted_at >= :since AND posted_at < :until
    ORDER BY posted_at DESC
""")


_MONTHS_RU = {1: "январе", 2: "феврале", 3: "марте", 4: "апреле", 5: "мае",
              6: "июне", 7: "июле", 8: "августе", 9: "сентябре", 10: "октябре",
              11: "ноябре", 12: "декабре"}


def _plausible_level(tok: str) -> bool:
    """Отсев одиночных латинских букв, случайно похожих на уровень.

    ⚠️ «B» из «B2B» проходит границы слова и стало бы «прежним уровнем» — цифра
    выглядела бы правдоподобно, и ошибку никто бы не заметил. Требуем либо явную
    национальную метку (ru… / …(RU)), либо минимум две буквы в ступени. Теряем
    легитимные одиночные «B»/«C» у международных агентств — приемлемо: они и так за
    пределами 24-месячного окна.
    """
    t = tok.strip()
    has_ru = t.lower().startswith("ru") or t.upper().endswith("(RU)")
    core = re.sub(r"^ru", "", t, flags=re.I)
    core = re.sub(r"\(RU\)$", "", core, flags=re.I)
    return has_ru or len(core) >= 2


# ⚠️ _size_vs_peak УДАЛЕНА (Вадим 01.09). Она отдавала «чистый лонг сейчас в 10 раз
# меньше, чем на пике за год», и вердикт был: «такие факты нам не нужны — если есть
# с чем сравнить глобально, это для глобального поста про макродвижения, а тут
# достаточно найти круглое число за период: полгода, год, лето, зима, отчётный
# период». Эталон, который он привёл: «Акции Системы за год упали более чем в два
# раза. За это же время физлица перешли от чистого шорта в чистый лонг».
#
# Это ровно то, что уже собирает ГЛАВНОЕ_СРАВНЕНИЕ в блоке цена_акции — пара
# «цена ↔ позиция» по одному названному окну. Отдельное поле про размер позиции
# добавляло второй, более слабый факт и тянуло пост в макро-разговор, поэтому
# вычеркнуто, а не переформулировано.


def _issuer_named_before_level(line: str, stems) -> bool:
    """Назван ли ИМЕННО наш эмитент, а не его дочка.

    ⚠️ Простой проверки «имя есть в строке» недостаточно, и это поймал тест на
    реальной строке архива: «Эксперт РА присвоил кредитный рейтинг агрохолдингу
    "Степь" на уровне ruBBB+ — АФК "Система"». Пост помечен тикером AFKS, имя
    материнской компании в строке ЕСТЬ — но действие по дочке, и ruBBB+ ушёл бы в
    бриф как прежний уровень Системы.

    Признак — порядок в предложении: рейтинговое действие пишется как
    «агентство <действие> рейтинг <КОМУ> … на уровне <УРОВЕНЬ>», то есть эмитент
    стоит ДО уровня. В строке про дочку «Система» оказывается уже после уровня, в
    подписи об источнике.
    """
    if not stems:
        return True
    m = _RE_SCALE.search(line)
    if not m:
        return False
    head = line[:m.start()].lower()
    return any(st.lower() in head for st in stems)


def _name_stems(name: str) -> list:
    """Корни названия эмитента для отсева чужих строк. «АФК Система» → ['Систе'].

    Пятибуквенная обрезка — тот же приём, что в подборе примеров корпуса: снимает
    падежи и кавычки («Системы», «Система», «"Система"») без словаря.
    """
    return [w[:5] for w in re.findall(r"[А-Яа-яЁёA-Za-z]{5,}", name or "")]


def _rating_history(db, headline: str, raw_text: str, tickers, as_of) -> dict:
    """Прошлые рейтинговые действия по эмитенту — ИЗ АРХИВА, без внешних источников.

    ⚠️ Зачем. Судья по черновику 1638: «не указан прежний уровень рейтинга, поэтому
    масштаб понижения читатель домысливает сам», и отдельно — «"срезали" звучит
    резче одного notch-понижения». Замечание верное: сама новость прежний уровень не
    называет (ни smartlab, ни markettwits). Зато он есть в архиве — в ПРЕДЫДУЩЕМ
    действии того же агентства.

    ⚠️ Агентство обязано совпадать. У АКРА уровень AA-(RU), у Эксперт РА — ruAA-;
    это разные шкалы. Подставить прежний уровень другого агентства значило бы выдать
    фактическую ошибку, которую никто не заметит: цифра выглядит правдоподобно.
    Поэтому прежний уровень отдаётся ТОЛЬКО когда агентство то же, что в новости.

    ⚠️ Блок появляется только у рейтинговых новостей. Добавлять историю рейтингов в
    бриф про отчётность — насыпать поле, которое модель обязана израсходовать.

    ⚠️ Окно 24 месяца. Оно же снимает шум: в старых постах под тем же тикером
    попадаются действия по ДОЧКАМ («Эксперт РА присвоил рейтинг агрохолдингу
    "Степь" … — АФК "Система"», 2022), а прежний уровень пятилетней давности всё
    равно бесполезен.
    """
    news = f"{headline or ''} {raw_text or ''}"
    if "рейтинг" not in news.lower():
        return {}
    tks = [t for t in (tickers or []) if t]
    if not tks:
        return {}
    m = _RE_AGENCY.search(news)
    agency_now = m.group(0) if m else None

    actions, same_agency = [], []
    for tk in tks[:2]:
        name = db.execute(_SELECT_TICKER_NAME, {"tk": tk}).scalar() or tk
        stems = _name_stems(name)
        rows = db.execute(_SELECT_RATING_LINES, {
            "tk": tk, "since": as_of - timedelta(days=730), "until": as_of,
        }).fetchall()
        for d, ln in rows:
            ln = (ln or "").strip()
            if len(ln) < 25 or len(ln) > 400:
                continue
            if not (_RE_AGENCY.search(ln) and _RE_SCALE.search(ln)):
                continue
            if not _issuer_named_before_level(ln, stems):
                continue
            item = f"{d.strftime('%d.%m.%Y')}: {ln[:230]}"
            if item in actions:
                continue
            actions.append(item)
            if agency_now and agency_now.lower() in ln.lower():
                same_agency.append((d, ln))
            if len(actions) >= 3:
                break
        if actions:
            break
    if not actions:
        return {}

    out = {}
    if same_agency:
        d, ln = same_agency[0]
        # ⚠️ Берём ПОСЛЕДНИЙ уровень в строке. У подтверждения («на уровне AA-(RU)»)
        # он один; у понижения («с ruAA- до ruA+») последний — это уровень ПОСЛЕ того
        # действия, то есть ровно тот, что действовал до нынешней новости.
        levels = [x for x in _RE_SCALE.findall(ln) if _plausible_level(x)]
        prev = levels[-1] if levels else None
        if prev:
            # ⚠️ Месяц и год СЛОВАМИ. Дата в формате 30.12.2025 модель пересказала как
            # «в конце декабря» — без года, и судья справедливо заметил, что в
            # сентябре 2026 это читается двусмысленно. Готовая фраза копируется как
            # есть, дата в цифрах — пересказывается.
            out["ПРЕЖНИЙ_УРОВЕНЬ"] = (
                f"до этого, в {_MONTHS_RU[d.month]} {d.year} года, "
                f"у {agency_now} было {prev}")
    out["прошлые_действия"] = actions
    out["ПОЯСНЕНИЕ"] = (
        "Прошлые рейтинговые действия по этому эмитенту из архива новостей. "
        "⚠️ ШКАЛА У КАЖДОГО АГЕНТСТВА СВОЯ: AA-(RU) у АКРА и ruAA- у Эксперт РА — "
        "разные системы. Уровень одного агентства НЕЛЬЗЯ приписывать другому и "
        "нельзя сравнивать их между собой. Если поля ПРЕЖНИЙ_УРОВЕНЬ нет — значит "
        "прошлого действия ТОГО ЖЕ агентства в архиве не нашлось, и прежний уровень "
        "называть нельзя. "
        "ОБЪЁМ: это ФОН. Прежний уровень — ОДНИМ КОРОТКИМ ПРЕДЛОЖЕНИЕМ в том же "
        "абзаце, где само решение агентства. Отдельного абзаца под историю рейтингов "
        "не делать, в скобки ничего не убирать."
    )
    return out



# ══════════════════════════════════════════════════════════════════════
#                  ФУНДАМЕНТ КОМПАНИИ В БРИФ
# ══════════════════════════════════════════════════════════════════════
# В карточке 55 показателей за 5 лет и 5 кварталов. В бриф идут ЕДИНИЦЫ, и это
# главное решение всего блока.
#
# ⚠️ ПОЛЕ, ПОПАВШЕЕ В БРИФ, МОДЕЛЬ СЧИТАЕТ ОБЯЗАННОЙ ИЗРАСХОДОВАТЬ. Проверено
# дорого: 01.09 в бриф добавили два блока без границы, и черновик разбух до 1 105
# знаков против медианы жанра 661 — Вадиму он понравился меньше предыдущего. Вывалить
# сюда всю карточку значит гарантированно испортить пост.
#
# ⚠️ ДИНАМИКА, А НЕ УРОВЕНЬ. «Долг/EBITDA 3,11» читателю не говорит ничего, «1,07 →
# 3,11 за год» говорит всё. Поэтому каждый показатель отдаётся парой год-назад → сейчас,
# и только если он реально изменился.
#
# ⚠️ НАБОР ЗАВИСИТ ОТ ПОВОДА. Пост про дивиденды и пост про санкции держатся на разных
# числах. Показывать один и тот же список — тот же вывал, только меньшего размера.
#
# Разбор поста про Полюс (кандидат 793), на котором блок и проектировался: повод —
# «акции сложились вдвое, дивиденды приостановлены до 2030». Агент пересказал повод,
# потому что больше ничего не знал. В карточке лежало объяснение: Долг/EBITDA 1,07 →
# 3,11, чистый долг 554,6 → 912,9 млрд, и датированный тезис «капзатраты на Сухой Лог
# могут составить $6 млрд, запуск в 2028-2029». Приостановка дивидендов ИМЕННО до 2030
# из этого следует.

# Какие показатели относятся к делу при каком поводе. Ключи — event_type кандидата.
_FUND_BY_EVENT = {
    "dividend":         ["dividend", "div_yield", "div_payout_ratio", "debt_ebitda", "net_debt"],
    "register_closing": ["dividend", "div_yield", "div_payout_ratio"],
    "earnings":         ["revenue", "net_income", "ebitda", "net_margin", "debt_ebitda"],
    "sanctions":        ["revenue", "net_debt", "debt_ebitda", "capex"],
    "regulatory":       ["revenue", "net_income", "debt_ebitda"],
    "corporate_action": ["market_cap", "p_e", "net_debt", "free_float"],
}
_FUND_DEFAULT = ["revenue", "net_income", "debt_ebitda", "p_e"]

# Сколько показателей максимум доезжает до модели. Четыре — не круглое число, а
# граница, за которой абзац перестаёт быть абзацем: у канала медиана 11 слов в
# предложении, а каждое число требует своей опоры в тексте.
_FUND_LIMIT = 4

# ⚠️ КАРТОЧКА КЛЮЧУЕТСЯ ПО ТИКЕРУ АКЦИИ, А asset_id КАНДИДАТА — ЭТО ФЬЮЧЕРС.
# У кандидата asset_id приходит из аномалии открытого интереса: SR, SBERF, GZ. Взять
# его как secid значит не найти карточку никогда — поймано на первом же тесте.
#
# Резолвим через issuer_aliases: он знает и sectype, и assetcode, и тикер бумаги, и
# ISIN, и имя из справки УК. Старая ticker_futures_map остаётся запасным вариантом,
# но она беднее — один sectype на тикер, из-за чего у Газпрома там вечный GAZPF, а
# квартальный GZ не резолвится вовсе. Это первый потребитель справочника в конвейере,
# и он же показывает, зачем справочник заводился.
_RESOLVE_SECID = text("""
    SELECT s.secid
    FROM issuer_aliases a
    JOIN issuer_securities s ON s.issuer_id = a.issuer_id AND s.share_class = 'common'
    WHERE a.alias_value = :v AND a.instrument_kind <> 'bond'
    LIMIT 1
""")


def _secid_for_card(db, asset_id: str, tickers) -> str | None:
    """Тикер обыкновенной акции, под которым лежит карточка компании."""
    for ключ in [asset_id] + [t for t in (tickers or []) if t]:
        if not ключ:
            continue
        secid = db.execute(_RESOLVE_SECID, {"v": ключ}).scalar()
        if secid:
            return secid
    # Запасной путь: старая карта фьючерс → акция.
    return db.execute(_SELECT_STOCK_FOR_FUTURES, {"f": asset_id}).scalar()


_SELECT_FUND = text("""
    SELECT m.metric_code, r.label_ru, r.unit, m.period_label, m.value, m.report_date
    FROM company_metrics m
    JOIN metrics_ref r USING (metric_code)
    WHERE m.secid = :secid AND m.standard = 'MSFO'
      AND m.metric_code = ANY(:codes)
      AND m.period_type IN ('year', 'ltm')
      AND m.value IS NOT NULL
      AND m.last_seen >= CURRENT_DATE - INTERVAL '30 days'
""")

# ⚠️ ПО ОДНОМУ ТЕЗИСУ С КАЖДОЙ СТОРОНЫ, А НЕ ДВА САМЫХ СВЕЖИХ. Проверка на живом
# кандидате: у Полюса три свежих тезиса «в плюс» и один «в минус» — «капзатраты на
# Сухой Лог могут составить $6 млрд», ТОЙ ЖЕ ДАТЫ. Сортировка по свежести взяла два
# «в плюс», и в пост про обвал акций и срезанные дивиденды поехало «добыча вырастет
# вдвое». Ровно обратное тому, что объясняет повод.
#
# Односторонняя картина хуже отсутствия картины: она выглядит как вывод, а не как
# выборка. Поэтому берём свежайший «за» и свежайший «против» — пусть модель видит обе
# стороны и выбирает ту, что относится к делу.
_SELECT_THESES = text("""
    SELECT direction, statement, stated_date FROM (
        SELECT direction, statement, stated_date,
               ROW_NUMBER() OVER (PARTITION BY direction ORDER BY stated_date DESC) AS n
        FROM company_theses
        WHERE issuer_id = (SELECT issuer_id FROM issuer_securities WHERE secid = :secid)
          AND stated_date IS NOT NULL AND stated_date >= :since
    ) t WHERE n = 1
    ORDER BY direction DESC
""")


def _только_текущее(val: str) -> str:
    """Из «1,07 → 3,11 (было за 2025…)» оставить «3,11»: в подписи нужна величина,
    а не история. Историю модель объясняет словами в самом посте."""
    if "→" in val:
        val = val.split("→", 1)[1]
    return val.split("(")[0].strip()


def _fund_number(v) -> str:
    """Числа в бриф идут уже человеческими: модель не должна их форматировать."""
    v = float(v)
    if abs(v) >= 1000:
        return _ru(round(v))
    if abs(v) >= 10:
        return _ru(round(v, 1))
    return _ru(round(v, 2))


def _company_fundamentals(db, asset_id: str, tickers, event_type: str, as_of,
                          trace=None) -> dict:
    """Несколько чисел из карточки, относящихся к поводу, и датированные тезисы."""
    # ⚠️ ПОСТ НЕ ПРО ОДНУ КОМПАНИЮ — ФУНДАМЕНТ НЕ ПРИ ЧЁМ. Кандидат 748 («исторические
    # минимумы обновляют более 30 акций») размечен шестью тикерами, и блок цеплял
    # отчётность АЛРОСЫ — первой попавшейся. В обзорном посте это не контекст, а
    # приглашение свернуть на разговор об одной компании, которая тут ни при чём.
    if len([t for t in (tickers or []) if t]) > 2:
        if trace:
            trace.record("company_metrics", "фундамент под обзорный пост",
                         outcome=НЕ_ВЗЯТО, result_count=0,
                         reason="в кандидате %d тикеров — пост не про одну компанию"
                                % len(tickers))
        return {}

    secid = _secid_for_card(db, asset_id, tickers)
    if not secid:
        if trace:
            trace.record("issuer_aliases", "какая компания за %s" % asset_id,
                         outcome=ПУСТО, reason="ключ не резолвится в эмитента")
        return {}
    codes = _FUND_BY_EVENT.get(event_type or "", _FUND_DEFAULT)
    rows = db.execute(_SELECT_FUND, {"secid": secid, "codes": codes}).fetchall()
    if not rows:
        if trace:
            trace.record("company_metrics", "фундамент %s под повод «%s»" % (secid, event_type),
                         outcome=ПУСТО, result_count=0,
                         reason="карточки нет или показатели пустые")
        return {}

    # Собираем «прошлый год → сейчас». LTM — это «сейчас»; за «прошлый год» берём
    # последний ПОЛНЫЙ год, он же самый свежий year-период.
    по_коду, дата_отчёта = {}, None
    for code, label, unit, period, value, report_date in rows:
        if period == "LTM" and report_date and (not дата_отчёта or report_date > дата_отчёта):
            дата_отчёта = report_date
        d = по_коду.setdefault(code, {"label": label, "unit": unit, "years": {}})
        if period == "LTM":
            d["ltm"] = float(value)
        else:
            d["years"][period] = float(value)

    out, взято = {}, 0
    for code in codes:                      # порядок = приоритет для повода
        if взято >= _FUND_LIMIT:
            break
        d = по_коду.get(code)
        if not d:
            continue
        годы = sorted(d["years"])
        сейчас = d.get("ltm", d["years"].get(годы[-1]) if годы else None)
        было = d["years"].get(годы[-1]) if годы else None
        if сейчас is None:
            continue
        ед = (" " + d["unit"]) if d["unit"] and "%" not in d["unit"] else ("%" if d["unit"] else "")
        # ⚠️ Показатель без изменения отдаём одним числом, а не парой: «выручка
        # 712,8 → 712,8» выглядит как значимая динамика, которой нет.
        if было is not None and abs(сейчас - было) > abs(было) * 0.02:
            out[d["label"]] = "%s → %s%s (было за %s, стало за последние 12 мес)" % (
                _fund_number(было), _fund_number(сейчас), ед, годы[-1])
        else:
            out[d["label"]] = "%s%s" % (_fund_number(сейчас), ед)
        взято += 1

    # Тезисы smart-lab — датированные утверждения о компании. Берём только свежие:
    # «Сухой Лог запустят в 2028-2029» от 2025 года содержателен, тот же тезис от
    # 2019 — уже история, а не контекст.
    тезисы = []
    for direction, statement, stated in db.execute(
            _SELECT_THESES, {"secid": secid, "since": as_of - timedelta(days=730)}).fetchall():
        # ⚠️ Двойное экранирование у источника: в тезисах приезжает «&quot;подарила&quot;».
        # Парсер снимает один слой, а их два. Чистим и здесь тоже: уже записанные строки
        # сами не исправятся, а показывать модели разметку нельзя — она её процитирует.
        statement = html.unescape(html.unescape(statement or "")).strip()
        # Тезис длиной с абзац занимает четверть блока и провоцирует цитирование
        # целиком. Нужна суть, а не текст: 160 знаков хватает на утверждение.
        if len(statement) > 160:
            statement = statement[:157].rsplit(" ", 1)[0] + "…"
        тезисы.append("%s (%s, %s)" % (statement, stated.strftime("%d.%m.%Y"),
                                       "в плюс" if direction == "growth" else "в минус"))
    if тезисы:
        out["чем_объясняют"] = тезисы

    if trace:
        trace.record("company_metrics", "фундамент %s под повод «%s»" % (secid, event_type),
                     outcome=ВЗЯТО, result_count=взято,
                     result_note="; ".join("%s: %s" % (k, v) for k, v in list(out.items())[:3])[:200],
                     params={"secid": secid, "event_type": event_type, "codes": codes})

    # ── АННОТАЦИЯ. Собирается ЗДЕСЬ и целиком кодом; модель её не пишет и не правит.
    # Числа, придуманные моделью, неотличимы от настоящих — а мы за один день поймали
    # и выдуманный возраст факта, и ноль вместо пропуска. Приклеивается на слое
    # публикации, как фирменная подпись, и по той же причине (см. миграцию 073).
    числа = [(k, v) for k, v in out.items() if k not in ("чем_объясняют", "ГРАНИЦА")]
    if числа:
        # В аннотации — «стало», без стрелок: это подпись, а не рассуждение о динамике.
        части = []
        for label, val in числа:
            # ⚠️ Регистр не трогаем: .lower() превращал «P/E» в «p/e», а «EBITDA» в
            # «ebitda». Аббревиатуры в подписи под постом выглядят как опечатка.
            части.append("%s %s" % (label, _только_текущее(val)))
        # ⚠️ ДАТА — ЭТО ДАТА ОТЧЁТНОСТИ, А НЕ ДАТА НОВОСТИ. Первый рендер подписывал
        # «на 01.09.2026», хотя это день сигнала: читатель понял бы, что цифры
        # свежие на эту дату, а они за последние 12 месяцев по отчёту, вышедшему
        # раньше. Берём дату публикации последнего отчёта; если её нет — говорим
        # честно, что это дата, на которую мы данные сняли.
        if дата_отчёта:
            когда = "по отчётности за последние 12 мес (отчёт от %s)" % дата_отчёта.strftime("%d.%m.%Y")
        else:
            когда = "по данным на %s" % as_of.strftime("%d.%m.%Y")
        out["аннотация"] = ("Данные smart-lab, %s: %s." % (когда, ", ".join(части)))
        out["_аннотация_служебное"] = (
            "Эта строка приклеивается к посту АВТОМАТИЧЕСКИ на публикации. "
            "НЕ переписывать её, НЕ вставлять в текст и не дублировать."
        )

    if out:
        # Граница сама была на 400 знаков — четверть блока уходила на инструкцию.
        # Сокращено без потери смысла: три запрета вместо пяти предложений.
        # ⚠️ ЧИСЛА В ТЕКСТ НЕ ПЕЧАТАТЬ. Раньше здесь стояло «максимум одно число» —
        # инструкция, которую модель обходит, потому что поле в брифе она считает
        # обязанной израсходовать. Теперь числа нужны ей только для ПОНИМАНИЯ причины,
        # а в пост они попадают отдельной строкой, собранной кодом.
        out["ГРАНИЦА"] = (
            "Числа отсюда в текст поста НЕ ПЕРЕНОСИТЬ: они уйдут отдельной строкой "
            "автоматически. Объяснять словами («долговая нагрузка выросла втрое»), "
            "без цифр. Тезис — чужое мнение: называть с датой."
        )
    return out

def _build_brief(db, row) -> dict:
    """ЕДИНСТВЕННЫЙ сборщик брифа — для Шага В (писатель) и Шага Г (судья).

    ⚠️ Почему один. Бриф собирался дважды, руками, в _step_c_payload и
    _step_g_payload, и версии разъехались: судья не получал блок цена_акции,
    добавленный в v3. Итог на кандидате 1104 — вердикт «брак» с провалом
    numbers_traceable и no_invented_facts по абзацу «акция подешевела примерно на
    25%, стоит около 92 рублей». Числа были ВЕРНЫЕ и взяты из брифа, но судья
    этого брифа не видел, поэтому честно назвал их выдуманными. Вердикт был
    корректен относительно своего входа и неверен относительно мира.

    Асимметрия контекста — тихий отказ LLM-as-judge: судья не ошибается, он
    отвечает на другой вопрос. Лечится закрытием разрыва входов, а НЕ смягчением
    рубрики: ослабь numbers_traceable — и потеряешь единственную проверку,
    которая ловит настоящие выдумки.

    Из v1 УБРАНЫ market_rank и recent_signals: оба порождали дефекты —
    recent_signals дал в черновике 773 список 📌 из пяти чужих тикеров, который тот
    же пост следующей строкой сам и дисклеймил, а market_rank дал «второе по
    резкости среди 72 активов» там, где это ничего не добавляло. Поле, попавшее в
    бриф, модель считает обязанной израсходовать — поэтому лишние поля убираются,
    а не запрещаются очередным правилом.
    """
    created_at = row.get("created_at")
    news_date = created_at.date() if created_at else row["signal_date"]
    reused_signal = bool(created_at and row["signal_date"] < news_date)
    pos = _position_phrases(row["asset_id"], row["anomaly_clgroup"],
                            as_of=row["signal_date"])
    _trace = трассировать(db, row["id"], "бриф")
    _фундамент = _company_fundamentals(
        db, row["asset_id"], row["tickers"], row["event_type"], row["signal_date"],
        trace=_trace)
    # ⚠️ Аннотацию храним на кандидате, а не пересобираем при публикации. Между
    # написанием поста и его выходом может пройти день: карточка успеет обновиться,
    # и подпись «на основании таких-то данных» стала бы описывать не те данные, на
    # которых пост написан. Подпись должна соответствовать моменту написания.
    if _фундамент.get("аннотация"):
        db.execute(text("UPDATE content_candidates SET annotation = :a WHERE id = :i"),
                   {"a": _фундамент["аннотация"], "i": row["id"]})
    brief = {
        "candidate_id": row["id"],
        "headline": row["headline"],
        "raw_text": row["raw_text"] or row["headline"],
        "tickers": row["tickers"] or [],
        "event_type": row["event_type"] or "",
        "инструмент": f"{row['asset_id']} ({row['asset_name'] or ''})",
        "дата_новости": str(news_date),
        "дата_сигнала": str(row["signal_date"]),
        "рамка_сюжета": _story_frame(row["signal_date"], news_date),
        "позиции_физлиц": pos,
        "история_рейтинга": _rating_history(db, row["headline"], row["raw_text"],
                                             row["tickers"], row["signal_date"]),
        "связанные_компании": _related_context(db, row["tickers"], row["signal_date"],
                                               trace=_trace),
        "фундамент_компании": _фундамент,
        "цена_акции": _pair_price_with_position(
            _price_context(db, row["asset_id"], row["tickers"], row["signal_date"]),
            pos),
        "служебное": {
            "atr_множитель": float(row["severity_value"]),
            "пояснение": ("ВНУТРЕННЕЕ. Отношение дневного изменения к обычному "
                           "дневному, а НЕ рост позиции. В текст поста не выносить."),
        },
    }
    prior = _prior_post_line(db, row.get("thread_key"), row["id"], reused_signal)
    if prior and not prior.startswith("(нет"):
        brief["предыдущий_пост_этого_треда"] = prior
    for k in [k for k in pos if k.startswith("_код_")]:
        pos.pop(k)
    # Пустые блоки убираем: поле, попавшее в бриф, модель считает обязанной
    # израсходовать — пустое «связанные_компании: {}» провоцирует придумать связь.
    for empty in ("связанные_компании", "история_рейтинга"):
        if not brief.get(empty):
            brief.pop(empty, None)
    return brief


def _payload(obj, internal_token: str) -> str:
    """JSON + служебный хвост с токеном и хостом — общий формат обоих шагов."""
    import json as _json
    return (_json.dumps(obj, ensure_ascii=False, indent=2)
            + f"\ninternal_token: {internal_token}"
            + f"\napi_host: {INTERNAL_API_HOST}")


def _step_c_payload(db, row, internal_token: str) -> str:
    return _payload(_build_brief(db, row), internal_token)


def _step_g_payload(db, row, internal_token: str) -> str:
    """Судье — РОВНО тот же бриф, что был у писателя, плюс сам черновик. Бриф
    берётся тем же кодом (_build_brief), а не пересказывается: см. историю 1104
    в докстринге _build_brief."""
    # ⚠️ draft_hash — отпечаток ИМЕННО того текста, который судья сейчас увидит.
    # Судья возвращает его в PATCH, а бэкенд сверяет с текущим черновиком и отклоняет
    # вердикт, если текст успел измениться. Без этого поздний вердикт молча ложится
    # на другой текст, и по карточке это неотличимо от настоящей претензии.
    draft = row["draft_text"] or ""
    return _payload({
        "candidate_id": row["id"],
        "бриф": _build_brief(db, row),
        "черновик_на_проверку": draft,
        "draft_hash": hashlib.md5(draft.encode("utf-8")).hexdigest(),
    }, internal_token)


def run_once() -> dict:
    summary = {"step_a_fired": 0, "step_c_fired": 0, "errors": 0, "skipped_no_token": 0,
               "step_a_gave_up": 0, "step_c_gave_up": 0,
               "hype_filter_fired": 0, "hype_filter_gave_up": 0,
               "judge_fired": 0, "judge_gave_up": 0}

    internal_token = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    token_a = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_A", "")
    token_c = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_C", "")
    # Шаг Н — отдельный токен, отдельная проверка (не блокирует А/В, если ещё
    # не создан в UI, и наоборот — см. skill moex-content-routines).
    token_hype = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_HYPE_FILTER", "")
    can_fire_hype = bool(internal_token and token_hype and TRIGGER_ID_HYPE_FILTER)
    # Шаг Г — тоже свой токен и своя проверка: пока триггер не создан, судья просто
    # не запускается, а Шаги А/В/Н работают как раньше.
    token_g = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_G", "")
    can_fire_judge = bool(internal_token and token_g and TRIGGER_ID_STEP_G)
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
        step_a_gave_up = []
        for row in candidates:
            if row["dispatch_attempts"] >= MAX_DISPATCH_ATTEMPTS:
                give_up_reason = (f"Routine не ответила за {MAX_DISPATCH_ATTEMPTS} "
                                   f"попыток бэкстопа — сдаёмся (см. content_ai.py)")
                db.execute(_GIVE_UP_STEP_A, {"id": row["id"], "reasoning": give_up_reason})
                summary["step_a_gave_up"] += 1
                step_a_gave_up.append((row["id"], give_up_reason))
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
        _notify_pipeline_stuck("А", step_a_gave_up)

        draft_ready = db.execute(
            _SELECT_DRAFT_READY, {"cutoff": cutoff, "batch_limit": BATCH_LIMIT}
        ).mappings().all()
        step_c_gave_up = []
        for row in draft_ready:
            if row["dispatch_attempts"] >= MAX_DISPATCH_ATTEMPTS:
                give_up_reason = (f"Routine не ответила за {MAX_DISPATCH_ATTEMPTS} "
                                   f"попыток бэкстопа — откат в pending (см. content_ai.py)")
                db.execute(_GIVE_UP_STEP_C, {"id": row["id"], "reason": give_up_reason})
                summary["step_c_gave_up"] += 1
                step_c_gave_up.append((row["id"], give_up_reason))
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
        _notify_pipeline_stuck("В", step_c_gave_up)

        # Бэкстоп Шага Н — намеренно МАЛЕНЬКИЙ BATCH_LIMIT_HYPE_FILTER (не
        # BATCH_LIMIT, как у А/В) + тот же FIRE_STAGGER_SEC между вызовами:
        # Вадим попросил не заваливать Routine параллельными запросами разом
        # (риск конкуренции за облачный контейнер, см. FIRE_STAGGER_SEC выше).
        # ── Шаг Г: судья по свежим черновикам ─────────────────────────
        if can_fire_judge:
            judge_rows = db.execute(
                _SELECT_JUDGE_PENDING,
                {"cutoff": cutoff, "batch_limit": BATCH_LIMIT,
                 "max_attempts": MAX_DISPATCH_ATTEMPTS,
                 "brief_version": BRIEF_VERSION},
            ).mappings().all()
            for row in judge_rows:
                try:
                    _fire(TRIGGER_ID_STEP_G, token_g, _step_g_payload(db, row, internal_token))
                    db.execute(_MARK_JUDGE_DISPATCHED, {"id": row["id"]})
                    db.commit()
                    summary["judge_fired"] += 1
                    time.sleep(FIRE_STAGGER_SEC)  # см. FIRE_STAGGER_SEC выше
                except Exception as e:
                    summary["errors"] += 1
                    print(f"[content_ai] step-g fire failed for candidate {row['id']}: "
                          f"{type(e).__name__}: {e}")

            # Тот же бэкстоп, что у остальных шагов: исчерпанные попытки → сдаёмся
            # с терминальной меткой, иначе алерт уходил бы каждый прогон крона
            # (живой случай Шага Н, миграция 048).
            judge_gave_up = db.execute(text("""
                SELECT id FROM content_candidates
                WHERE draft_text IS NOT NULL AND judge_verdict IS NULL
                  AND judge_gave_up_at IS NULL
                  AND judge_dispatch_attempts >= :max_attempts
                  AND judge_checked_at < :cutoff
            """), {"max_attempts": MAX_DISPATCH_ATTEMPTS, "cutoff": cutoff}).scalars().all()
            g_reason = (f"Судья (Шаг Г) не ответил за {MAX_DISPATCH_ATTEMPTS} попыток "
                        f"бэкстопа — черновик уходит на ревью БЕЗ проверки")
            g_list = []
            for cid in judge_gave_up:
                db.execute(_GIVE_UP_JUDGE, {"id": cid})
                summary["judge_gave_up"] += 1
                g_list.append((cid, g_reason))
            if g_list:
                db.commit()
                _notify_pipeline_stuck("Г", g_list)

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
                  AND hype_filter_gave_up_at IS NULL
                  AND hype_filter_dispatch_attempts >= :max_attempts
                  AND hype_filter_checked_at < :cutoff
            """), {"max_attempts": MAX_DISPATCH_ATTEMPTS, "cutoff": cutoff}).scalars().all()
            hype_give_up_reason = (f"Routine не ответила за {MAX_DISPATCH_ATTEMPTS} "
                                    f"попыток бэкстопа — сдаёмся (см. content_ai.py)")
            hype_gave_up = []
            for cid in gave_up:
                db.execute(_GIVE_UP_HYPE_FILTER, {"id": cid})
                summary["hype_filter_gave_up"] += 1
                hype_gave_up.append((cid, hype_give_up_reason))
            db.commit()
            _notify_pipeline_stuck("Н", hype_gave_up)
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
    ok = s["errors"] == 0
    # degraded: были ошибки, но что-то всё же прошло (напр. хайп-фильтр
    # отстрелялся, пока Шаг А/В цеплял 401) — не топим статус в общий "fail"
    # неотличимо от полного отказа, см. pipeline_heartbeat.record_pipeline_run.
    fired_any = (s["step_a_fired"] + s["step_c_fired"]
                 + s["hype_filter_fired"] + s["judge_fired"]) > 0
    pipeline_heartbeat.record_pipeline_run(
        "content_ai_backstop", ok, str(s), dur, degraded=(not ok and fired_any)
    )


if __name__ == "__main__":
    main()
