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
BRIEF_VERSION = 6

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
    out = {"цена_сейчас": _money_ru(px[last_d])}
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
        "доля_чистой_позиции_в_ои": (
            f"чистый {word} — {_ru(f'{share:.0f}')}% открытого интереса физлиц"),
        # ⚠️ Окно в НАЗВАНИИ поля: прежнее «перекос_диапазон_за_ряд» модель прочла
        # как «за всё время наблюдений» и написала «максимум за всё время».
        f"эта_доля_за_{_window_ru(span).replace(' ', '_').replace('.', '')}": (
            f"диапазон от {_ru(f'{min(pcts):.0f}')}% до {_ru(f'{max(pcts):.0f}')}% — "
            f"это окно за {_window_ru(span)}, НЕ исторический экстремум. "
            f"Если пишешь про рекорд — обязательно с оговоркой про окно, "
            f"и окно называй словами («за год»), а не в днях."),
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
                f"совпадение по времени не доказывает предвидение.")
    if d == -1:
        return ("СОВПАДЕНИЕ: позиции менялись за 1 дн. до новости — это практически "
                "один день, и от шума он не отличим. Утверждать, что толпа встала "
                "ЗАРАНЕЕ или что-то спрогнозировала, НЕЛЬЗЯ.")
    if d == 0:
        return ("СОВПАДЕНИЕ: позиции и новость в один день. Утверждать, что толпа "
                "встала ЗАРАНЕЕ, нельзя — данных на это нет.")
    return (f"РЕАКЦИЯ: позиции менялись на {d} дн. ПОЗЖЕ новости. Это отклик на "
            f"событие, а не предвидение. Фразы «знали заранее», «встали до» "
            f"запрещены как факт.")


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


_SELECT_ENTITY_LINKS = text("""
    SELECT entities, statement
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
# Озон Фармацевтику (OZPH) — другую компанию; разметка их разделяет. Индекс GIN по
# tickers уже есть, окно 180 дней укладывается в ~20 мс на 487 тыс. строк.
_SELECT_TICKER_NEWS = text("""
    SELECT posted_at::date AS d, text, coalesce(array_length(tickers, 1), 0) AS nt
    FROM news_archive
    WHERE :tk = ANY(tickers) AND posted_at >= :since AND posted_at <= :until
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


def _related_context(db, tickers, as_of) -> dict:
    """Связанные компании: что у них с ценой и что о них писал архив.

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
    if not links:
        return {}
    out = {}
    for entities, statement in links:
        for tk in (entities or []):
            if tk in tks or tk in out or len(out) >= 2:
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
        "склеит причинность, которую мы не утверждаем."
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
        "связанные_компании": _related_context(db, row["tickers"], row["signal_date"]),
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
    if not brief.get("связанные_компании"):
        brief.pop("связанные_компании", None)
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
    return _payload({
        "candidate_id": row["id"],
        "бриф": _build_brief(db, row),
        "черновик_на_проверку": row["draft_text"],
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
