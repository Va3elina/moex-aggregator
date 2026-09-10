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
from datetime import date, datetime, timedelta, timezone

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
# Второй мозг — те же функции, что у ручек /api/internal/brain/*, но в процессе:
# бриф собирается здесь, и ходить за ним по HTTP к самому себе незачем. Прогрев
# эмбеддинг-модели в фоне выключаем: скрипт живёт минуту, модель нужна только
# подсказке Шага А и грузится лениво (int8-копия, ~3,5 с).
os.environ.setdefault("BRAIN_WARMUP", "0")
from api.brain_core import контекст as _brain_context, поиск as _brain_search  # noqa: E402

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
BRIEF_VERSION = 22   # v22: события вокруг новости — что ещё было и как цена шла 2 часа после
# Окно новостей второго мозга. Было 14 дней: у АФК Системы это 13 новостей, а
# рейтинговая история и соседи по новостям живут кварталами. Вадим 06.09: «я бы
# увеличил радиус до пары месяцев или квартала». Окно касается только колец
# «новости» и «вместе в новостях»; владельцы, фонды, индексы и без того без срока,
# кандидаты и аномалии — 60 дней. В бриф уходят счётчик и последние три заголовка,
# поэтому размер брифа от окна не растёт.
BRAIN_CONTEXT_DAYS = 90

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


_SELECT_NAME_HITS = text("""
    SELECT DISTINCT r.company_id, n.title, n.payload->>'sector' AS sector
      FROM brain_name_rules r JOIN brain_nodes n ON n.id = r.company_id
     WHERE r.enabled AND NOT r.ambiguous
       AND to_tsvector('russian', :t) @@ phraseto_tsquery('russian', r.pattern)
     LIMIT 6
""")


def _brain_hint_for_step_a(db, row) -> str:
    """Подсказка Шагу А от второго мозга, двумя слоями:
    1) компании, чьё имя стоит В САМОМ ТЕКСТЕ новости (правила разметки, полнотекст с
       морфологией) — уровень C, это почти всегда и есть ответ;
    2) на что новость похожа по смыслу (вектор) — уровень D, только если имени нет
       или как соседи; замер 05.09: по смыслу «Роснефть ракетит» уходила к Россетям.
    Тикер по-прежнему выбирается Шагом А только из known_tickers и по тексту.
    Вызовы оставляют след в agent_trace (шаг «мозг»)."""
    текст = ((row["headline"] or "") + " " + (row["raw_text"] or ""))[:1500]
    части = []
    # Слой 0 — хэштеги автора (#PLZL): уровень B, надёжнее нашей разметки. Кандидат 1795
    # «#золото #PLZL» по имени находил только ВТБ из текста — тикер в хэштеге не имя.
    хэштеги = sorted({h.upper() for h in re.findall(r"#([A-Za-z0-9]{2,6})\b", текст)})
    по_хэштегу, по_смыслу = [], []
    if хэштеги:
        for тикер, company_id in db.execute(text("""
            SELECT m.ticker, m.company_id FROM brain_ticker_map m
             WHERE m.ticker = ANY(string_to_array(:h, ',')) AND m.company_id LIKE 'company:%'
        """), {"h": ",".join(хэштеги)}).all():
            части.append(f"хэштег автора: {тикер} [B]")
            по_хэштегу.append(тикер)
    try:
        по_имени = db.execute(_SELECT_NAME_HITS, {"t": текст}).all()
    except Exception as e:  # noqa: BLE001
        по_имени = []
        части.append(f"(разметка по имени недоступна: {type(e).__name__})")
    for company_id, title, sector in по_имени:
        тикер = company_id.split(":", 1)[1]
        try:
            c = _brain_context(ticker=тикер, days=BRAIN_CONTEXT_DAYS, candidate_id=row["id"], db=db, _who="agent")
            хвост = (f"кандидатов за 60 дн: {c['кандидаты'].get('всего', 0)}, аномалий за 60 дн: {c['аномалии'].get('всего', 0)}, "
                     f"новостей за {BRAIN_CONTEXT_DAYS} дн: {c['новости'].get('всего', 0)}")
        except Exception:  # noqa: BLE001
            хвост = ""
        части.append(f"названа в тексте: {тикер} ({title}, {sector or 'сектор неизвестен'}) [C]" + (f" — {хвост}" if хвост else ""))
    if not по_имени and not части:
        try:
            найдено = _brain_search(q=текст[:300], kind="company", mode="meaning", limit=3,
                                    candidate_id=row["id"], db=db, _who="agent")["найдено"]
            # Ниже 0.30 — шум: геополитика «похожа» на Россети с 0.20. Молчим, а не подсказываем.
            найдено = [x for x in найдено if float((x.get("почему") or "0").split()[-1].replace(",", ".") or 0) >= 0.30]
            похоже = ", ".join(f"{x['id'].split(':', 1)[1]} ({x['заголовок']}, {x['почему']})" for x in найдено)
            по_смыслу = [{"тикер": x["id"].split(":", 1)[1], "имя": x["заголовок"], "сходство": x["почему"]} for x in найдено]
            if похоже:
                части.append(f"по смыслу похоже на: {похоже} [D — подсказка, имени в тексте нет]")
        except Exception as e:  # noqa: BLE001 — подсказка не имеет права ронять Шаг А
            части.append(f"(смысловой поиск недоступен: {type(e).__name__})")
    подсказка = "; ".join(части) if части else "(ни имени наших компаний в тексте, ни похожих по смыслу)"
    # Итоговая строка следа: в разборе поста видно, что именно мозг сказал Шагу А.
    try:
        трассировать(db, row["id"], "мозг").record(
            "brain", "подсказка Шагу А", outcome=ВЗЯТО if части else ПУСТО, result_count=len(части),
            result_note=подсказка, params={"хэштеги": хэштеги, "по_хэштегу": по_хэштегу,
                                             "по_имени": [{"тикер": c.split(":", 1)[1], "имя": t} for c, t, _ in по_имени],
                                             "по_смыслу": по_смыслу})
        db.commit()
    except Exception:  # noqa: BLE001 — след не имеет права ронять Шаг А
        pass
    return подсказка


def _step_a_payload(row, internal_token: str, known_tickers: str, brain_hint: str = "") -> str:
    return (
        f"candidate_id: {row['id']}\n"
        f"source: {row['source']}\n"
        f"headline: {row['headline']}\n"
        f"raw_text: {row['raw_text'] or row['headline']}\n"
        f"known_tickers (ТОЛЬКО из этого списка, больше ниоткуда): {known_tickers}\n"
        f"подсказка_второго_мозга (ПОДСКАЗКА, не основание для tickers: [C] — имя компании найдено в тексте нашей "
        f"разметкой, [D] — лишь похоже по смыслу; тикер — только если компания названа в тексте И есть в known_tickers): {brain_hint}\n"
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


def _move_phrase(new_v, old_v) -> str:
    """Изменение чистой позиции ЧЕЛОВЕЧЕСКОЙ фразой: «чистый лонг вырос в 3 раза»."""
    # ⚠️ Процент через смену знака бессмыслен: VK (лонг → шорт) давал «−369,9%»,
    # и модель написала «позиция изменилась на −369,9%» — человек прочтёт
    # «упало на 370%». При развороте отдаём описание разворота.
    if not old_v:
        return "не с чем сравнить — позиции не было"
    word = "лонг" if new_v > 0 else "шорт"
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


def _position_phrases(asset_id: str, clgroup: str | None, as_of=None) -> dict:
    """Направление позиции толпы, проверка «рекорда» и разворот за год.

    ⚠️ Почему не ATR-множитель. Детектор считает ratio = |Δ за день| / ATR(14):
    ×4,69 означает «дневное изменение в 4,69 раза крупнее обычного дневного», а НЕ
    рост позиции. Канал говорит «лонг вырос в 5 раз» — это отношение «было → стало»
    за период. Подмена дала фактические ошибки в двух ОПУБЛИКОВАННЫХ постах (793:
    «выросла в 3,53 раза за один день», хотя за день было +37%). Множитель остаётся
    служебным признаком аномальности и в текст поста не идёт.
    См. research/content_pipeline_v2/METRIC_MISMATCH.md.

    ⚠️ ГЛАВНОГО_ЧИСЛА здесь больше нет (v20): его выбор из горизонтов до года уводил
    пост от новости (кандидат 1933). Костяк поста собирает _news_reaction."""
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
    word = "лонг" if last > 0 else "шорт"

    # ⚠️ ДЛИННЫЕ ГОРИЗОНТЫ — НЕ КОСТЯК (Вадим 10.09, кандидат 1933). Раньше отсюда
    # ехали сутки/неделя/месяц/полгода/год, и ГЛАВНЫМ становился самый сильный — а
    # за год позиция успевает вырасти в разы, за пару дней — на проценты, так что
    # год выигрывал почти всегда и пост открывался не новостью. Остаётся только
    # разворот за год: смена лонга на шорт меняет смысл самой реакции (толпа
    # реагирует уже из другой позиции), а рост «в 11 раз» — нет.
    base_d = min((d for d in dates if d >= last_d - timedelta(days=365)), default=None)
    year_flip = bool(base_d and base_d != last_d and net[base_d]
                     and (net[base_d] > 0) != (last > 0))
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
    out = {
        "направление_позиции": f"чистый {word.upper()}",

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
    if year_flip:
        out["фон_за_год_одной_фразой_после_реакции"] = f"за год {_move_phrase(last, net[base_d])}"
    return out


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
        # ⚠️ Движение ДО новости в пост не выносим (Вадим 10.09 по 1933: «не факт, что
        # за 5 дней до новости кто-то что-то узнал — это сомнительно, хотя имеет место
        # быть»). Даже голая констатация порядка дат читается как намёк на инсайд.
        return (f"УПРЕЖДЕНИЕ: сигнал по позициям был за {abs(d)} дн. ДО новости. В пост "
                f"это НЕ выносим: совпадение по времени не доказывает предвидение, а "
                f"абзац про «до новости» читается как намёк, что кто-то знал заранее. "
                f"Пиши только о том, что было после новости (реакция_на_новость). Сам "
                f"этот запрет в посте не проговаривай.")
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


_MONTHS_GEN = {1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая",
               6: "июня", 7: "июля", 8: "августа", 9: "сентября", 10: "октября",
               11: "ноября", 12: "декабря"}


def _day_ru(d) -> str:
    return f"{d.day} {_MONTHS_GEN[d.month]}"


def _span_ru(a, b) -> str:
    """«4 сентября», «с 4 по 8 сентября», «с 29 августа по 2 сентября»."""
    if a == b:
        return _day_ru(a)
    if a.month == b.month:
        return f"с {a.day} по {_day_ru(b)}"
    return f"с {_day_ru(a)} по {_day_ru(b)}"


def _net_move(new_v, old_v) -> str:
    """Как _move_phrase, но мелочь называет мелочью. За пару дней позиция часто
    сдвигается на доли процента: «вырос на 0,3%» — шум, выданный за событие, а
    «почти не изменился» — честная реакция (толпа новость пропустила)."""
    if old_v and (old_v > 0) == (new_v > 0) and abs(abs(new_v) - abs(old_v)) < 0.01 * abs(old_v):
        return f"чистый {'лонг' if new_v > 0 else 'шорт'} почти не изменился"
    return _move_phrase(new_v, old_v)


def _price_move(new_p, old_p) -> str:
    chg = (new_p - old_p) / old_p * 100
    if abs(chg) < 0.5:
        return "акция почти не изменилась в цене"
    return f"акция {'подешевела' if chg < 0 else 'подорожала'} {_pct(chg)}"


# Окно реакции — до трёх торговых дней после новости, от закрытия дня перед ней.
# Если сигнал пришёл позже новости, окно тянется до него, но не больше чем ещё на
# неделю. Писатель ждёт первый срез после новости не дольше _REACTION_WAIT_DAYS.
_REACTION_DAYS = 3
_REACTION_REACH = 5
_REACTION_WAIT_DAYS = 3


def _reaction_from_series(series, prices, news_date, signal_date) -> dict:
    """Что сделали толпа и цена ПОСЛЕ новости — одной строкой, с одной датой.

    ⚠️ Только после (Вадим 10.09, вторая правка по 1933). Версия с «до новости» дала
    абзац «с 4 по 8 сентября лонг сократился на 9%… резче всего 4 сентября, за пять
    дней до новости» — вердикт: «много дат», «просто после новости и как сказалось»,
    «не факт, что за 5 дней до новости кто-то что-то узнал». Движение до новости
    читается как намёк на инсайд, а связь доказать нечем.

    ⚠️ Зачем (Вадим 10.09, кандидат 1933). Черновик про переговоры Новатэка с
    Petrovietnam открывался годом: «акции за год подешевели на 17%, а чистый лонг
    вырос в 11 раз», и лишь следующим абзацем — что было за пять дней до новости.
    Вердикт: «вот идёт новость, мы освещаем новость, говорим, как толпа
    отреагировала за пару дней до и после — а к чему тут данные про год? они тут ни
    к чему». Причина была в брифе, не в модели: ГЛАВНОЕ_ЧИСЛО выбиралось по силе
    движения из горизонтов до года, и год выигрывал почти всегда.

    ⚠️ Позиция и цена — по ОДНОМУ окну в одной строке (урок 1104: разные окна рядом
    читатель сложит в одно). И числа отбирает код, а не модель: дай ей шесть
    равноправных чисел — возьмёт все шесть.

    Позиции — дневные закрытия по будням. Цена — последняя свеча не позже той же
    даты: акции торгуются и в выходные, а в ряду позиций выходных нет."""
    dates = [r[0] for r in series]
    net = {r[0]: r[1] for r in series}
    before = [d for d in dates if d < news_date]
    after_all = [d for d in dates if d >= news_date]
    if not before or not after_all:
        return {}
    n_after = _REACTION_DAYS
    if signal_date in after_all:
        n_after = min(max(n_after, after_all.index(signal_date) + 1),
                      _REACTION_DAYS + _REACTION_REACH)
    after = after_all[:n_after]
    anchor = before[-1]

    px = sorted((d, float(c)) for d, c in prices if c is not None)

    def price_on(d):
        vals = [c for dd, c in px if dd <= d]
        return vals[-1] if vals else None

    parts = [_net_move(net[after[-1]], net[anchor])]
    pa, pb = price_on(anchor), price_on(after[-1])
    if pa and pb:
        parts.append(_price_move(pb, pa))
    return {"после_новости": f"{_span_ru(after[0], after[-1])}: " + ", ".join(parts)}


def _news_reaction(db, asset_id: str, clgroup, tickers, news_date, signal_date) -> dict:
    """Ряды для _reaction_from_series: от месяца до новости до двух недель после
    (или до сегодня, если они ещё не прошли)."""
    from signals.db import get_position_series
    end = min(datetime.now(timezone.utc).date(),
              max(news_date, signal_date) + timedelta(days=14))
    series = get_position_series(asset_id, clgroup or "FIZ", days=45, as_of_date=end)
    secid = (tickers or [None])[0] or db.execute(
        _SELECT_STOCK_FOR_FUTURES, {"f": asset_id}).scalar()
    prices = db.execute(_SELECT_PRICE_SERIES, {
        "secid": secid, "since": end - timedelta(days=45), "as_of": end,
    }).fetchall() if secid else []
    return _reaction_from_series(series, prices, news_date, signal_date)


def _waiting_for_reaction(db, row) -> bool:
    """Шаг В ждёт первый дневной срез позиций после новости: пост — о том, как она
    сказалась, и без среза писать не о чем. MOEX публикует один срез за день, так что
    в день новости его ещё нет. Не дольше _REACTION_WAIT_DAYS: если данные так и не
    пришли (сбой источника), пост пишется без блока реакции."""
    created_at = row.get("created_at")
    if not created_at:
        return False
    news_date = created_at.date()
    if (datetime.now(timezone.utc).date() - news_date).days > _REACTION_WAIT_DAYS:
        return False
    return "после_новости" not in _news_reaction(
        db, row["asset_id"], row["anomaly_clgroup"], row["tickers"], news_date,
        row["signal_date"])


# ── События вокруг новости ────────────────────────────────────────────────────
# ⚠️ Зачем (Вадим 10.09, кандидат 1933). Черновик писал «новость сразу отразилась на
# бумаге: 9 сентября акция подешевела на 2,6%». По часовым свечам: после новости о
# Вьетнаме (15:30 МСК) цена полтора часа стояла, а упала в 18:00 — вслед за атакой
# дронов на Новый Уренгой (17:17), Газпром в тот день тоже −2,4%. Писатель этого
# видеть не мог: у него были только наша новость и дневные числа. Теперь он видит,
# что ещё писали про компанию и отрасль в эти дни и как цена шла два часа после
# каждого события, — и сам решает, к чему относится движение.
#
# Отраслевые хэштеги — для новостей без тикеров: пост MarketTwits про Уренгой шёл
# только с «#газ». Ключи — как в issuers.sector.
_SECTOR_TAGS = {
    "Нефть и газ": ["#нефть", "#газ", "#спг", "#бензин", "#опек", "#ормуз"],
    "Металлы": ["#металлы", "#золото", "#сталь", "#никель", "#алюминий"],
    "Финансы": ["#банки", "#дкп"],
    "Энергетика": ["#электроэнергия"],
    "Застройщики": ["#ипотека", "#недвижимость"],
}
_AROUND_BEFORE_DAYS = 2
_AROUND_AFTER_DAYS = 5
# Чужая новость попадает в бриф, только если после неё наша бумага за два часа
# сдвинулась хотя бы на столько. Иначе это 250 новостей в день, а поле, попавшее в
# бриф, модель считает обязанной израсходовать.
_AROUND_MOVE_PCT = 1.0
_AROUND_MAX = 12
# Москва без перехода на летнее время с 2014 года; свечи в БД — наивное время МСК.
_MSK = timezone(timedelta(hours=3))

_SELECT_SECTOR_PEERS = text("""
    SELECT s.secid FROM issuers i JOIN issuer_securities s USING (issuer_id)
    WHERE i.sector = (SELECT i2.sector FROM issuers i2 JOIN issuer_securities s2 USING (issuer_id)
                      WHERE s2.secid = :secid LIMIT 1)
""")
_SELECT_NEWS_AROUND = text("""
    SELECT posted_at, text, coalesce(tickers, '{}') AS tickers
    FROM news_archive
    WHERE posted_at BETWEEN :since AND :until
      AND (tickers && CAST(:peers AS text[]) OR hashtags && CAST(:tags AS text[]))
    ORDER BY posted_at
    LIMIT 400
""")
_SELECT_HOURLY = text("""
    SELECT begin_time, close FROM candles
    WHERE secid = :secid AND interval = 60 AND type = 'stock'
      AND begin_time BETWEEN :since AND :until
    ORDER BY begin_time
""")


def _events_from(news, bars, secid: str, name: str, our_at) -> list:
    """Строки «когда — что — как цена шла два часа после» по новостям вокруг нашей.

    Своя новость (тикер или имя компании) попадает всегда, чужая — только если после
    неё цена сдвинулась на _AROUND_MOVE_PCT за два часа. Цена «до» — закрытие часовой
    свечи, закончившейся к моменту новости, «после» — закончившейся через два часа."""
    bars = [(b, float(c)) for b, c in bars if c is not None]

    def close_by(t):
        vals = [c for b, c in bars if b + timedelta(hours=1) <= t]
        return vals[-1] if vals else None

    items, seen = [], set()
    for posted_at, body, tks in news:
        tks = list(tks or [])
        t = posted_at.astimezone(_MSK).replace(tzinfo=None)
        snippet = _pick_snippet(body, len(tks), name or secid)
        own = secid in tks or bool(name and name.lower() in (body or "").lower())
        traded = any(t < b + timedelta(hours=1) <= t + timedelta(hours=2) for b, _ in bars)
        before, after = close_by(t), close_by(t + timedelta(hours=2))
        move = (after - before) / before * 100 if traded and before and after else None
        if not snippet or (not own and (move is None or abs(move) < _AROUND_MOVE_PCT)):
            continue
        snippet = re.sub(r"\s*Читать далее.*$", "", " ".join(snippet.split()))[:180]
        key = re.sub(r"[^а-яёa-z]", "", snippet.lower())[:50]
        if key in seen:
            continue
        seen.add(key)
        ours = bool(own and our_at and abs((posted_at - our_at).total_seconds()) <= 20 * 60)
        price = _price_move(after, before) if move is not None else "торгов не было"
        line = f"{_day_ru(t.date())}, {t:%H:%M} МСК — {snippet} → за 2 часа {price}"
        items.append((t, ours, own, abs(move or 0), ("[наша новость] " if ours else "") + line))
    # Лишнее режем с конца приоритета: наша новость, свои, самые резкие чужие.
    # ⚠️ Не «по резкости впереди своих»: одно падение цены даёт одинаковое «−1,4% за
    # 2 часа» ВСЕМ новостям того часа, и на 1933 список забили дизель в США и Пакистан
    # — модель могла бы приписать движение не тому. Главное событие (Уренгой) и так
    # было среди своих: smart-lab пометил его #NVTK.
    items.sort(key=lambda x: (not x[1], not x[2], -x[3]))
    return [x[4] for x in sorted(items[:_AROUND_MAX], key=lambda x: x[0])]


def _news_around(db, row, news_date) -> list:
    """Новости компании и её отрасли от двух дней до новости до пяти дней после."""
    secid = (row["tickers"] or [None])[0] or db.execute(
        _SELECT_STOCK_FOR_FUTURES, {"f": row["asset_id"]}).scalar()
    if not secid:
        return []
    since = datetime.combine(news_date - timedelta(days=_AROUND_BEFORE_DAYS), datetime.min.time())
    until = min(datetime.now(_MSK).replace(tzinfo=None),
                datetime.combine(news_date + timedelta(days=_AROUND_AFTER_DAYS), datetime.max.time()))
    peers = [r[0] for r in db.execute(_SELECT_SECTOR_PEERS, {"secid": secid}).fetchall()]
    if secid not in peers:
        peers.append(secid)
    sector = db.execute(_SELECT_SECTOR, {"secid": secid}).scalar() or ""
    # Пустой массив pg8000 не типизирует — подставляем тег, которого не бывает.
    tags = _SECTOR_TAGS.get(sector) or ["#-"]
    news = db.execute(_SELECT_NEWS_AROUND, {
        "since": since.replace(tzinfo=_MSK), "until": until.replace(tzinfo=_MSK),
        "peers": peers, "tags": tags,
    }).fetchall()
    bars = db.execute(_SELECT_HOURLY, {
        "secid": secid, "since": since, "until": until + timedelta(hours=3),
    }).fetchall()
    name = re.sub(r"\s*\(.*?\)", "", row.get("asset_name") or "").strip()
    return _events_from(news, bars, secid, name, row.get("created_at"))


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


# Живая связь — это компании рядом В СЮЖЕТЕ, а не только в графе владения.
# ⚠️ Форма `tickers @> ARRAY[...]` та же, что и у запроса выше, и по той же причине:
# с двумя элементами GIN-индекс тоже работает, а `= ANY` его не использует.
_SELECT_PAIR_NEWS = text("""
    SELECT posted_at::date AS d, coalesce(array_length(tickers, 1), 0) AS nt
    FROM news_archive
    WHERE tickers @> ARRAY[:a, :b]::text[]
      AND posted_at >= :since AND posted_at <= :until
      AND coalesce(array_length(tickers, 1), 0) <= 3
    ORDER BY posted_at DESC
    LIMIT 1
""")

# Окно, в котором совместное упоминание ещё считается общим сюжетом.
# ⚠️ И только НЕ дайджест (условие nt <= 3 в запросе): в «Итогах дня» рядом стоят
# шесть десятков тикеров, и совместное упоминание там не значит ничего.
_LINK_LIVE_DAYS = 30


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


def _sector_of(db, tk, мозг) -> str:
    """Сектор компании по карте. Для тикеров кандидата берём уже сделанный обход,
    для связанной компании — свой: это ещё один вопрос агента ко второму мозгу, и
    он стоит ~30 мс (замер ручек 05.09)."""
    c = (мозг or {}).get(tk)
    if c is None:
        try:
            c = _brain_context(ticker=tk, days=1, db=db, _who="agent")
        except Exception:  # noqa: BLE001 — карта не имеет права ронять бриф
            return ""
    эл = ((c.get("сектор") or {}).get("элементы") or [])
    return (эл[0].get("заголовок") or "") if эл else ""


def _link_evidence(db, row, tks, tk, name, as_of, мозг) -> tuple:
    """Признаки того, что связь при чём-то в ЭТОМ событии, и сила: «прямая», «слабая», «».

    ⚠️ Граф владения отвечает на вопрос «связаны ли компании вообще», и ответ у него
    почти всегда «да»: контрольные пакеты держат десятилетиями. Кандидат 1933
    (НОВАТЭК ведёт переговоры с Petrovietnam) показал цену этого «да» — в черновик
    уехал абзац про долю Газпрома по снимку 25.03.2021, к переговорам отношения не
    имеющий. Модель его не выдумала: поле было в брифе, а поле в брифе модель
    считает обязанной израсходовать.

    ⚠️ И совместного упоминания в новостях для «прямой» связи НЕ ХВАТАЕТ. Замер по
    тому же 1933: NVTK и GAZP попадали в одну новость десять раз за месяц — «Россия
    увеличила экспорт СПГ», «Новак о поставках в Китай». Это отраслевые обзоры, обе
    компании там подлежащие, а к переговорам во Вьетнаме они отношения не имеют.
    Признак, который срабатывает у любых двух компаний одного сектора, не отделяет
    сюжет от фона — поэтому у однокашников по сектору совместное упоминание считается
    СЛАБЫМ, и решение по нему остаётся за писателем.

    Три уровня, и они соответствуют слоям доверия карты:
      прямая — компания названа в самой новости [A] или общий сюжет вне сектора [C];
      слабая — только отраслевое соседство [C/D]: вопрос писателю, а не фактура;
      пусто  — связь есть лишь в графе владения: до писателя не доезжает вовсе.
    """
    признаки, сила = [], ""
    текст = " ".join(x for x in (row.get("headline"), row.get("raw_text")) if x)
    if текст and name:
        pat = re.compile(rf"(?<![А-Яа-яЁёA-Za-z]){re.escape(name)}(?![А-Яа-яЁёA-Za-z])", re.I)
        if pat.search(текст):
            признаки.append(f"«{name}» названа в самой новости [A — текст новости]")
            сила = "прямая"
    сектор_свой = _sector_of(db, tks[0], мозг) if tks else ""
    сектор_чужой = _sector_of(db, tk, мозг)
    один_сектор = bool(сектор_свой) and сектор_свой == сектор_чужой
    for основной in tks:
        строка = db.execute(_SELECT_PAIR_NEWS, {
            "a": основной, "b": tk,
            "since": as_of - timedelta(days=_LINK_LIVE_DAYS), "until": as_of,
        }).first()
        if not строка:
            continue
        d, n = строка[0], строка[1]
        если_отраслевое = " — но обе из сектора «%s», это скорее отраслевой обзор, а не общий сюжет" % сектор_свой
        признаки.append("обе компании были в одной новости %s (тикеров в ней %d) [C — наша разметка]%s"
                        % (d.strftime("%d.%m.%Y"), n, если_отраслевое if один_сектор else ""))
        сила = сила or ("слабая" if один_сектор else "прямая")
        break
    рядом = []
    for c in (мозг or {}).values():
        рядом += [(э.get("заголовок") or "")
                  for э in ((c.get("вместе_в_новостях") or {}).get("элементы") or [])]
    if name and any(name.lower() in р.lower() for р in рядом):
        признаки.append("карта видит их рядом в новостях "
                        "[D — соседство, не отношение: утверждать по нему нельзя]")
        сила = сила or "слабая"
    return признаки, сила


def _related_context(db, row, as_of, trace=None, мозг=None) -> dict:
    """Связанные компании: что у них с ценой и что о них писал архив.

    Возвращает ДВА блока: фактуру по прямым связям и вопрос по слабым.

    ⚠️ Блок собирается ПОСЛЕ обхода второго мозга, и что с какой связью делать,
    решает сила признаков (_link_evidence):
      • прямая — как раньше: фактура, цена, архив, правила абзаца;
      • слабая — «связи_под_вопросом»: одна строка и ВОПРОС писателю, БЕЗ цены, без
        архива и без указаний, как писать абзац. Указания «как писать» сами по себе
        толкают написать: пока они лежали рядом со всякой связью, отказаться от
        сомнительной было не от чего — бриф уже объяснял, как её подать;
      • пусто — связь есть только в графе владения: до писателя не доезжает вовсе.
        Оговорка рядом с полем модель не удерживает, отсутствие поля — удерживает.

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
    tks = [t for t in (row["tickers"] or []) if t]
    if not tks:
        return {}, {}
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
        return {}, {}
    out, спорные = {}, {}
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
            if tk in tks or tk in out or tk in спорные:
                continue
            if len(out) + len(спорные) >= 2:
                # Лимит в две компании — не «ничего не нашлось», а осознанный отказ.
                # Без этой строки в дашборде связь выглядела бы просто отсутствующей.
                if trace:
                    trace.record("world_facts", "связанная компания %s" % tk,
                                 outcome=НЕ_ВЗЯТО, result_count=1,
                                 result_note=statement[:120],
                                 reason="в бриф идут максимум две связанные компании")
                continue
            name = db.execute(_SELECT_TICKER_NAME, {"tk": tk}).scalar() or tk
            признаки, сила = _link_evidence(db, row, tks, tk, name, as_of, мозг)
            if сила == "слабая":
                # Не фактура, а вопрос: одна строка про связь и признаки, по которым
                # писатель решает сам. Ни цены, ни архива — их незачем расходовать.
                спорные[tk] = {"связь": statement, "почему_она_здесь": признаки}
                if trace:
                    trace.record("brain", "связь %s — при чём она здесь" % name,
                                 outcome=ВЗЯТО, result_count=len(признаки),
                                 result_note=("; ".join(признаки))[:200] or None,
                                 reason="признаки слабые — уходит писателю вопросом, а не фактурой",
                                 params={"ticker": tk, "сила": сила})
                continue
            if not сила:
                # ⚠️ Отказ ЗАПИСЫВАЕМ. Без следа «связь была, но её отвергли» в
                # разборе поста это неотличимо от «связей не нашлось», и понять,
                # почему Газпрома нет в посте про НОВАТЭК, нельзя ничем.
                if trace:
                    trace.record("brain", "связь %s — при чём она здесь" % name,
                                 outcome=НЕ_ВЗЯТО, result_count=len(признаки),
                                 result_note=("; ".join(признаки))[:200] or None,
                                 reason=("в этой новости компания не названа и общих "
                                         "сюжетов за %d дней нет — связь есть только "
                                         "в графе владения" % _LINK_LIVE_DAYS),
                                 params={"ticker": tk, "окно_дней": _LINK_LIVE_DAYS,
                                         "связь": statement[:200]})
                continue
            item = {"связь": statement, "почему_она_здесь": признаки}
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
    if спорные:
        спорные["ВОПРОС"] = (
            # ⚠️ Формулировка — не украшение. Раньше слабая связь приходила той же
            # фактурой, что и прямая, вместе с инструкцией «как писать абзац», и
            # писателю нечем было отказаться. Здесь у поля нет фактуры вовсе: только
            # связь, признаки и вопрос с уже названным ответом по умолчанию.
            "ЭТО ВОПРОС, А НЕ ФАКТУРА. Признаки слабые: компания в новости не названа, "
            "а совместные упоминания могут быть просто отраслевыми. Ответь себе: что "
            "именно эта связь объясняет в ЭТОМ событии? Если ответ звучит как «они обе "
            "из одной отрасли» или «они давно связаны» — это не объяснение, а фон, и "
            "тогда НЕ УПОМИНАЙ связь вовсе. ОТВЕТ ПО УМОЛЧАНИЮ — НЕТ. Пост без абзаца "
            "о связях — нормальный пост; натянутая связь — брак.")
    if not out:
        return {}, спорные
    out["ПОЯСНЕНИЕ"] = (
        # ⚠️ Карта отсеивает связи, которых в сюжете нет вовсе; уместность
        # оставшейся — вопрос смысла, и его решает писатель. Раньше решения не
        # существовало ни у кого: связь была в брифе, значит, ехала в текст.
        "УПОМИНАТЬ ЛИ СВЯЗЬ — РЕШАЕШЬ ТЫ. У каждой компании есть поле "
        "почему_она_здесь: это признаки, по которым карта сочла связь живой, а НЕ "
        "доказательство, что связь нужна посту. Прочитай их и спроси себя: объясняет "
        "ли эта связь хоть что-то в ЭТОМ событии? Если нет — не упоминай её вовсе. "
        "Пост без абзаца о связях — нормальный, годный пост; натянутая связь — брак. "
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
    return out, спорные


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
# Отдельное поле про размер позиции добавляло второй, более слабый факт и тянуло
# пост в макро-разговор, поэтому вычеркнуто, а не переформулировано. С v20 и сам
# годовой горизонт больше не костяк — см. _reaction_from_series.


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
        "называть нельзя. Это ФОН: брать его в текст или нет — решай сам."
    )
    # ⚠️ Раньше здесь стояло «прежний уровень — одним предложением в том же абзаце».
    # Указание, КАК вставить, модель читает как указание ВСТАВИТЬ: на кандидате 1638
    # в первый абзац уехали и A+(RU), и AA-(RU) — коды шкалы, которых канал не писал
    # ни разу (0 из 1459 постов корпуса). Вадим 06.09: «никто не поймёт». Данные
    # остаются, инструкция по вёрстке — нет.
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
# Поводы, при которых числа — ТЕМА поста, а не фон: только для них блок фундамента
# доезжает до писателя. Для остальных (regulatory, sanctions, прочее) числа уходят
# лишь подписью под постом. См. _build_brief.
_FUND_NUMERIC_EVENTS = {"dividend", "register_closing", "earnings", "corporate_action"}

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


# ⚠️ КОДЫ И СТАНДАРТ — FINANCEMARKER, А НЕ SMART-LAB (с 04.09.2026, миграция 075).
# Три дня после переезда блок молчал у всех постов и никто не заметил: запрос искал
# standard = 'MSFO' латиницей, а FM пишет «МСФО»; просил net_income / p_e /
# debt_ebitda, а у FM это earnings / pe, а долг/EBITDA не отдаётся вовсе — считаем
# сами из чистого долга и EBITDA за те же 12 месяцев. Значения FM — в МИЛЛИОНАХ
# рублей, подписи в metrics_ref — в миллиардах: делим на тысячу.
#
# ⚠️ ЕДИНИЦЫ У FM — КАК В ОТЧЁТЕ ЭМИТЕНТА, поля «unit» нет. Роснефть отчитывается в
# миллиардах (выручка 8262), Магнит — в тысячах (3 509 225 556), большинство — в
# миллионах. Масштаб восстанавливаем из выручки на акцию × число акций / выручка:
# порядок 3, 6 или 9. Если восстановить нельзя — денежные показатели не отдаём,
# только коэффициенты (P/E, долг/EBITDA), у них единиц нет.
_FM_КОД = {"net_income": "earnings", "p_e": "pe", "debt_ebitda": "netdebt_ebitda", "market_cap": None,
           "free_float": None, "dividend": None, "div_yield": None, "div_payout_ratio": None}
_FM_ПОДПИСЬ = {"earnings": ("Чистая прибыль", "млрд руб"), "pe": ("P/E", None),
               "netdebt_ebitda": ("Чистый долг/EBITDA", None)}
# У банков EBITDA — бессмыслица (у Сбера она отрицательная): не отдаём.
_НЕ_ДЛЯ_ФИНАНСОВ = {"ebitda", "netdebt_ebitda", "net_debt"}
_SELECT_SCALE = text("""
    WITH r AS (SELECT value AS rev FROM company_metrics WHERE secid = :secid AND metric_code = 'revenue'
                 AND standard = 'МСФО' AND period_type = 'ltm' ORDER BY period_label DESC LIMIT 1),
         p AS (SELECT value AS ps FROM company_metrics WHERE secid = :secid AND metric_code = 'revenue_ps'
                 AND standard = 'МСФО' AND period_type = 'ltm' ORDER BY period_label DESC LIMIT 1),
         s AS (SELECT num FROM company_shares WHERE secid = :secid ORDER BY year DESC, month DESC LIMIT 1)
    SELECT ROUND(LOG(10, NULLIF(p.ps * s.num / NULLIF(r.rev, 0), 0))) FROM r, p, s
""")
_SELECT_SECTOR = text("""
    SELECT i.sector FROM issuers i JOIN issuer_securities s USING (issuer_id) WHERE s.secid = :secid LIMIT 1
""")
_SELECT_FUND = text("""
    SELECT m.metric_code, r.label_ru, r.unit, m.period_type, m.period_label, m.value
    FROM company_metrics m
    LEFT JOIN metrics_ref r USING (metric_code)
    WHERE m.secid = :secid AND m.standard = 'МСФО' AND m.source = 'financemarker'
      AND m.metric_code = ANY(:codes)
      AND m.period_type IN ('year', 'ltm')
      AND m.value IS NOT NULL
      AND m.last_seen >= CURRENT_DATE - INTERVAL '45 days'
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
    сектор = db.execute(_SELECT_SECTOR, {"secid": secid}).scalar() or ""
    codes = [_FM_КОД.get(c, c) for c in _FUND_BY_EVENT.get(event_type or "", _FUND_DEFAULT)]
    codes = [c for c in codes if c and not (сектор == "Финансы" and c in _НЕ_ДЛЯ_ФИНАНСОВ)]
    порядок = db.execute(_SELECT_SCALE, {"secid": secid}).scalar()
    масштаб = 10 ** int(порядок) / 1e9 if порядок is not None and int(порядок) in (3, 6, 9) else None
    rows = db.execute(_SELECT_FUND, {"secid": secid, "codes": codes}).fetchall()
    if not rows:
        if trace:
            trace.record("company_metrics", "фундамент %s под повод «%s»" % (secid, event_type),
                         outcome=ПУСТО, result_count=0,
                         reason="карточки нет или показатели пустые")
        return {}

    # Собираем «прошлый год → сейчас». LTM — это «сейчас» (метка LTM-2026-06 = 12 мес
    # до июня 2026); за «прошлый год» берём последний ПОЛНЫЙ год (метка «2025»).
    по_коду, дата_отчёта, ltm_метка = {}, None, None
    for code, label, unit, period_type, period_label, value in rows:
        label, unit = (label, unit) if label else _FM_ПОДПИСЬ.get(code, (code, None))
        if unit and "руб" in unit:
            if масштаб is None:
                continue                    # единицы эмитента неизвестны — число врало бы
            v = float(value) * масштаб
        else:
            v = float(value)
        d = по_коду.setdefault(code, {"label": label, "unit": unit, "years": {}})
        if period_type == "ltm":
            d["ltm"] = v
            if not ltm_метка or period_label > ltm_метка:
                ltm_метка = period_label
        elif period_label and period_label[:4].isdigit():
            d["years"][period_label[:4]] = v
    if ltm_метка and ltm_метка.startswith("LTM-"):
        try:
            дата_отчёта = date(int(ltm_метка[4:8]), int(ltm_метка[9:11]), 1)
        except ValueError:
            дата_отчёта = None

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
                     params={"secid": secid, "event_type": event_type, "codes": codes, "масштаб": порядок})

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
        # ⚠️ Дата отчёта у FM не приходит — есть только метка периода. Подписываем
        # период честно: «за 12 мес до июня 2026», а не выдуманную дату публикации.
        if дата_отчёта:
            когда = "МСФО за 12 мес до %s" % дата_отчёта.strftime("%m.%Y")
        else:
            когда = "по данным на %s" % as_of.strftime("%d.%m.%Y")
        out["аннотация"] = ("Данные FinanceMarker, %s: %s." % (когда, ", ".join(части)))

    if out:
        # ⚠️ БЕЗ ПРИМЕРОВ-ПРЕДЛОЖЕНИЙ. Прежняя граница разрешала «одно число, если оно
        # спорит с поводом» и давала пример: «рейтинг понизили, а долговая нагрузка за
        # год снизилась». Писатель воспроизвёл пример дословно — на кандидате 1638 в
        # пост про рейтинг уехал Долг/EBITDA 4,68 → 3,46. Вадим 06.09: «ни к селу ни к
        # городу, ты бы ещё про выручку и мультипликаторы написал». Пример для модели
        # равен инструкции, поэтому примеров в оговорках брифа больше нет.
        #
        # Сам блок теперь доезжает до писателя только под повод про цифры (см.
        # _FUND_NUMERIC_EVENTS в _build_brief) — здесь остаётся короткая граница на
        # случай, когда он доехал.
        # ⚠️ С 10.09.2026 строки с этими числами под постом НЕТ (Вадим: «не пиши в
        # самом посту»). Прежняя граница обещала писателю, что числа «уже уйдут
        # отдельной строкой», — теперь это неправда, и обещание убрано.
        out["ГРАНИЦА"] = (
            "Под постом эти числа НЕ публикуются. В текст бери только то, о чём сама "
            "новость; число, которое просто описывает компанию, не брать. "
            "Остальное словами. Тезис — чужое мнение: называть с датой."
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
    # ⚠️ Порядок значим: карта обходится ПЕРВОЙ, и уже по ней решается, поедет ли
    # связь к писателю (см. _link_evidence, кандидат 1933).
    _карта = _brain_contexts(db, row)
    _мозг = _brain_block(db, row, _карта)
    _связи, _спорные_связи = _related_context(db, row, row["signal_date"],
                                              trace=_trace, мозг=_карта)
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
    # ⚠️ Готовая строка «Данные FinanceMarker, …» писателю НЕ едет: под постом её с
    # 10.09.2026 не публикуем, а увидев её в брифе, модель вставила бы её в текст.
    # На кандидате она остаётся — как запись о том, какие числа видел бриф.
    _фундамент.pop("аннотация", None)
    # ⚠️ ЦИФРЫ — ПОД ПОВОД ПРО ЦИФРЫ (Вадим 06.09: «если говорим про цифры, то про
    # цифры»). Отчёт, дивиденды, корпоративное действие — блок нужен, писатель пишет
    # про эти числа. Рейтинг, санкции, прочее — блока в брифе нет вовсе: поле, которое
    # доехало до модели, она считает обязанной израсходовать (см. 1638). Подпись под
    # постом при этом остаётся — она уже сохранена строкой выше и клеится на
    # публикации, читатель числа увидит, но не в тексте.
    if (row["event_type"] or "") not in _FUND_NUMERIC_EVENTS and _фундамент:
        _trace.record("company_metrics", "фундамент в бриф под повод «%s»" % (row["event_type"] or "—"),
                      outcome=НЕ_ВЗЯТО, result_count=0,
                      reason="повод не про цифры — числа уйдут только подписью под постом")
        _фундамент = {}
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
        "реакция_на_новость": _news_reaction(db, row["asset_id"], row["anomaly_clgroup"],
                                             row["tickers"], news_date, row["signal_date"]),
        "события_вокруг_новости": _news_around(db, row, news_date),
        "позиции_физлиц": pos,
        "история_рейтинга": _rating_history(db, row["headline"], row["raw_text"],
                                             row["tickers"], row["signal_date"]),
        "связанные_компании": _связи,
        "связи_под_вопросом": _спорные_связи,
        "второй_мозг": _мозг,
        "фундамент_компании": _фундамент,
        "служебное": {
            "atr_множитель": float(row["severity_value"]),
            "пояснение": ("ВНУТРЕННЕЕ. Отношение дневного изменения к обычному "
                           "дневному, а НЕ рост позиции. В текст поста не выносить."),
        },
    }
    prior = _prior_post_line(db, row.get("thread_key"), row["id"], reused_signal)
    if prior and not prior.startswith("(нет"):
        brief["предыдущий_пост_этого_треда"] = prior
    # Пустые блоки убираем: поле, попавшее в бриф, модель считает обязанной
    # израсходовать — пустое «связанные_компании: {}» провоцирует придумать связь.
    for empty in ("связанные_компании", "связи_под_вопросом", "история_рейтинга",
                  "второй_мозг", "фундамент_компании", "реакция_на_новость",
                  "события_вокруг_новости"):
        if not brief.get(empty):
            brief.pop(empty, None)
    return brief


def _brain_contexts(db, row) -> dict:
    """Обход карты по первым двум тикерам кандидата — ОДИН на весь бриф.

    ⚠️ Карта нужна двум блокам сразу: «второй_мозг» показывает её как есть, а
    «связанные_компании» по ней решает, тянуть ли связь в пост. Второй вызов дал бы
    вторую запись в agent_trace и второй счёт времени — след перестал бы отвечать на
    вопрос «сколько раз агент ходил в карту».
    """
    ctxs = {}
    for tk in [t for t in (row["tickers"] or []) if t][:2]:
        try:
            ctxs[tk] = _brain_context(ticker=tk, days=BRAIN_CONTEXT_DAYS,
                                      candidate_id=row["id"], db=db, _who="agent")
        except Exception as e:  # noqa: BLE001 — карта не имеет права ронять бриф
            ctxs[tk] = {"недоступно": f"{type(e).__name__}"}
    return ctxs


def _brain_block(db, row, ctxs=None) -> dict:
    """Блок «второй_мозг» брифа: что карта знает о компании, с уровнем у каждой строки.

    ⚠️ УРОВЕНЬ — ЧАСТЬ ФАКТА, А НЕ ПОЛЕ РЯДОМ. Модель читает строки, а не наши
    колонки, поэтому [A]/[B]/[C]/[D] и дата снимка стоят в самой строке, а правило
    «что можно утверждать» — прямо в блоке. Владельцев и фонды отдаём с датой
    структуры: у половины компаний она старше двух лет, и «сейчас» про неё писать
    нельзя. Вызов контекста сам пишет след в agent_trace (шаг «мозг»).
    """
    ctxs = _brain_contexts(db, row) if ctxs is None else ctxs
    if not ctxs:
        return {}
    out = {}
    for tk, c in ctxs.items():
        if "недоступно" in c:
            out[tk] = dict(c)
            continue
        if "индекс" in c:
            out[tk] = {"это_индекс": c["индекс"]["заголовок"], "в_составе_бумаг": c["состав"]["всего"]}
            continue
        def стр(э, с_датой=True, с_весом=False):
            дата = f" на {э['на_дату'][:7]}" if с_датой and э.get("на_дату") else ""
            вес = f" {э['вес']:g}%" if с_весом and э.get("вес") is not None else ""
            return f"{э['заголовок']}{вес}{дата} [{э.get('уровень') or '?'}]"
        блок = {"компания": f"{c['компания']['заголовок']} [B]"}
        if c["сектор"]["элементы"]:
            с = c["сектор"]["элементы"][0]
            блок["сектор"] = (f"{с['заголовок']} [C, наша правка: холдинг]" if с.get("уровень") == "C"
                              else f"{с['заголовок']} [B, классификация smart-lab]")
        if c["владельцы"]["элементы"]:
            блок["владельцы_по_снимку"] = [стр(э, с_весом=True) for э in c["владельцы"]["элементы"][:4]]
        if c["владеет"]["элементы"]:
            блок["владеет"] = [стр(э, с_весом=True) for э in c["владеет"]["элементы"][:4]]
        if c["фонды_держатели"]["всего"]:
            топ = sorted(c["фонды_держатели"]["элементы"], key=lambda э: -(э.get("вес") or 0))[:3]
            блок["фонды_держатели"] = (f"{c['фонды_держатели']['всего']} фондов держат бумагу [A, раскрытие УК]; "
                                      "крупнейшие доли: " + ", ".join(стр(э, с_весом=True, с_датой=True) for э in топ))
        if c["индексы"]["элементы"]:
            блок["в_индексах"] = [стр(э, с_весом=True) for э in c["индексы"]["элементы"]]
        if c["новости"]["всего"]:
            блок[f"о_компании_писали_за_{BRAIN_CONTEXT_DAYS}_дней"] = (f"{c['новости']['всего']} новостей; последние: " +
                " | ".join(f"{(э.get('время') or '')[:10]} {э['заголовок'][:90]} [{э.get('уровень') or '?'}]" for э in c["новости"]["элементы"][:3]))
        if c["кандидаты"]["всего"]:
            блок["прошлые_кандидаты_60_дней"] = (f"{c['кандидаты']['всего']}; " +
                " | ".join(f"{(э.get('время') or '')[:10]} {э['заголовок'][:70]}" for э in c["кандидаты"]["элементы"][:3]))
        if c["аномалии"]["всего"]:
            блок["аномалии_позиций_60_дней"] = (f"{c['аномалии']['всего']} [C, наш детектор]; " +
                " | ".join(f"{(э.get('время') or '')[:10]} {э['заголовок'][:60]}" for э in c["аномалии"]["элементы"][:3]))
        if c["вместе_в_новостях"]["элементы"]:
            блок["часто_рядом_в_новостях"] = ", ".join(э["заголовок"] for э in c["вместе_в_новостях"]["элементы"][:4]) + " [D — не связь, а соседство]"
        out[tk] = блок
    if out:
        out["ПРАВИЛО"] = ("[A] — первоисточник с датой: можно утверждать со ссылкой на дату. "
                          "[B] — посредник (FinanceMarker, smart-lab, хэштег канала): утверждать с источником и датой снимка; "
                          "если снимок старше года — только «по данным на <дата>», не «сейчас». "
                          "[C] — наша разметка/детектор: только как «по нашей разметке». "
                          "[D] — подсказка: НЕ утверждать, в текст не выносить. "
                          "Доли владения — только с датой снимка. Это КОНТЕКСТ, НЕ ПРИЧИНА события.")
    return out


def _payload(obj, internal_token: str) -> str:
    """JSON + служебный хвост с токеном и хостом — общий формат обоих шагов."""
    import json as _json
    return (_json.dumps(obj, ensure_ascii=False, indent=2)
            + f"\ninternal_token: {internal_token}"
            + f"\napi_host: {INTERNAL_API_HOST}")


def _step_c_payload(db, row, internal_token: str) -> str:
    return _payload(_build_brief(db, row), internal_token)


# Для переделки по замечанию: тот же набор полей, что у _SELECT_DRAFT_READY (его
# читает _build_brief), но черновик уже есть и нужен сам текст.
_SELECT_FOR_REVISION = text("""
    SELECT c.id, c.headline, c.raw_text, c.tickers, c.event_type, c.futures_ticker,
           c.reasoning, c.dispatch_attempts, c.forwards_count,
           c.thread_key, c.created_at, c.draft_text,
           a.id AS anomaly_id, a.asset_id, a.asset_name, a.type AS anomaly_type,
           a.clgroup AS anomaly_clgroup, a.direction,
           a.severity_value, a.signal_date, a.headline AS anomaly_headline
    FROM content_candidates c
    JOIN anomalies a ON a.id = c.matched_anomaly_id
    WHERE c.id = :id AND c.status = 'draft_ready' AND c.draft_text IS NOT NULL
""")


def fire_revision(db, candidate_id: int, remarks: str) -> None:
    """Переделка черновика по замечанию человека — кнопка «✏️ Править» в боте.

    Писателю (Шаг В) уходит ТОТ ЖЕ бриф, что при первом черновике, плюс блок
    правка_ревьюера: текущий черновик и замечание словами человека. Что с ним
    делать, сказано в промпте Шага В (раздел про правку ревьюера). Новый черновик
    приходит обычным PATCH step-c, который обнуляет вердикт и отметку «отправлено»,
    поэтому дальше всё идёт штатно: судья, затем новая карточка в боте.

    Бросает исключение, если запускать нечего или нечем, — бот покажет его текст.
    """
    token = os.environ.get("CLAUDE_ROUTINE_FIRE_TOKEN_STEP_C", "")
    internal = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    if not token or not internal:
        raise RuntimeError("в .env нет токена Шага В или внутреннего токена")
    row = db.execute(_SELECT_FOR_REVISION, {"id": candidate_id}).mappings().first()
    if not row:
        raise LookupError("кандидат не ждёт ревью или у него нет черновика")
    brief = _build_brief(db, row)
    brief["правка_ревьюера"] = {
        "текущий_черновик": row["draft_text"],
        "что_поправить": (remarks or "").strip(),
    }
    db.commit()   # _build_brief пишет след брифа и аннотацию
    _fire(TRIGGER_ID_STEP_C, token, _payload(brief, internal))


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
                      _step_a_payload(row, internal_token, known_tickers,
                                      _brain_hint_for_step_a(db, row)))
                db.commit()   # след подсказки в agent_trace — сразу, до долгого stagger
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
            # Пост — о том, как новость сказалась: ждём срез позиций после неё.
            if _waiting_for_reaction(db, row):
                summary["step_c_waiting"] = summary.get("step_c_waiting", 0) + 1
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
