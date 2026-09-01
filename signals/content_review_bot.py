"""
Content-пайплайн, Этап 9 — review-бот. Long-poll host-side, по прямому
шаблону signals/alert_bot.py (см. тот файл за подробным объяснением паттерна
send/send_kb/edit_kb/answer_cb + view-builder'ов (text, inline_keyboard)).

Отличие от alert_bot.py: там ОДИН бот (ALERT_BOT_TOKEN) обслуживает МНОГО
пользователей по их alerts. Здесь — ОДИН админ (config.ADMIN_USER_ID, личка),
бот = config.BOT_TOKEN (тот же, что publish/telegram.py уже использует для
sendPhoto/sendMessage — у него нет своего getUpdates-поллера нигде, кроме
этого файла, конфликта с alert_bot.py, у которого СВОЙ токен, нет).

Поток: content_ai.py (Шаг В) пишет draft_text → status='draft_ready' остаётся
(матрица переходов в db/migrations/022). Этот бот подхватывает НОВЫЕ
draft_ready-с-черновиком кандидаты (reviewer_notified_at IS NULL, СВОЯ
колонка — миграция 037). ⚠️ Раньше здесь читалась last_checked_at, но её же
писал content_match.py в ТОЙ ЖЕ команде, что ставила status='draft_ready' —
условие никогда не выполнялось, бот не сработал НИ РАЗУ (найдено 2026-07-16,
0 published/rejected через него). См. content_match.py за подробностями.
Шлёт карточку в личку. Три действия:
  ✅ Одобрить  → публикует в CONTENT_CHANNEL_ID (ТЕСТОВЫЙ канал на старте!),
                 status: draft_ready → in_review → published (та же пара
                 переходов, что и ручная кнопка в админ-Kanban — держим
                 согласованно, не короткий путь в обход state machine).
  ✏️ Править   → бот просит текст следующим сообщением, обновляет draft_text,
                 статус НЕ меняется (остаётся draft_ready, снова на ревью).
  ❌ Отклонить → draft_ready → in_review → rejected, без публикации.

⭐ Обратная связь (миграция 054, research/content_pipeline_v2/ §2). До неё ревью
было разовым решением, а не размеченным примером: правка человека затирала
draft_text (оригинал ИИ терялся), причина отказа не писалась никуда. Из-за этого
единственным каналом человеческого суждения в систему был Вадим, дописывающий
правило в промпт Шага В — так промпт и дорос до журнала багов на ~13 тыс. знаков.
Теперь:
  • оригинал ИИ живёт в draft_text_ai и НЕ перезаписывается — дифф с draft_text
    показывает, что именно редактор счёл нужным исправить;
  • после ❌ и после ✏️ бот спрашивает ПРИЧИНУ кнопками из
    config.REVIEW_REASON_CODES (+ необязательный свободный комментарий).
Решение (reject) применяется СРАЗУ, до вопроса о причине: если Вадим не ответит,
теряется причина, а не само решение.

ЗАПУСК НА ХОСТЕ (не в контейнере — тот же резон, что и alert_bot.py, релей
теперь и для api.anthropic.com, см. signals/relay/):
  /opt/frame/signals/.venv/bin/python -m signals.content_review_bot
Под systemd (frame-content-review-bot.service, по образцу frame-alert-bot).
"""
import html
import json
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# ── .env + DB_URL override ДО импорта api.database (host-side, как alert_bot) ──
import os
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from sqlalchemy import text  # noqa: E402
from api.database import SessionLocal  # noqa: E402
from signals import config  # noqa: E402
from signals.publish.telegram import (  # noqa: E402
    send_text_post, apply_custom_emoji, with_frame_signature,
)

API_ROOT = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
API_BASE = f"{API_ROOT}/bot{config.BOT_TOKEN}"

_DRAFT_PREVIEW_LIMIT = 3500  # запас под Telegram-лимит 4096 с учётом обвязки карточки


def _redact(s) -> str:
    return str(s).replace(config.BOT_TOKEN, "<REDACTED>")


def send(chat_id, text_msg: str) -> None:
    """Обычные служебные сообщения бота (не превью поста) — БЕЗ HTML, текст
    plain (не эскейпленный вызывающим), чтобы не заставлять каждого caller'а
    думать про экранирование ради простых строк вроде «Жду текст...»."""
    try:
        requests.post(f"{API_BASE}/sendMessage",
                       data={"chat_id": chat_id, "text": text_msg,
                             "disable_web_page_preview": True},
                       timeout=15)
    except requests.RequestException as e:
        print(f"[content_review_bot] send error: {_redact(e)}")


def send_kb(chat_id, text_msg: str, inline_keyboard: list) -> None:
    """text_msg уже готов к HTML-режиму (собран через _card_view — сам
    экранирует свои части). parse_mode=HTML нужен для превью фирменных
    custom-emoji (см. apply_custom_emoji), ровно то, что увидят в канале."""
    try:
        requests.post(f"{API_BASE}/sendMessage",
                       json={"chat_id": chat_id, "text": text_msg, "parse_mode": "HTML",
                             "disable_web_page_preview": True,
                             "reply_markup": {"inline_keyboard": inline_keyboard}},
                       timeout=15)
    except requests.RequestException as e:
        print(f"[content_review_bot] send_kb error: {_redact(e)}")


def edit_kb(chat_id, message_id, text_msg: str, inline_keyboard: list) -> None:
    try:
        requests.post(f"{API_BASE}/editMessageText",
                       json={"chat_id": chat_id, "message_id": message_id, "text": text_msg,
                             "parse_mode": "HTML", "disable_web_page_preview": True,
                             "reply_markup": {"inline_keyboard": inline_keyboard}},
                       timeout=15)
    except requests.RequestException as e:
        print(f"[content_review_bot] edit_kb error: {_redact(e)}")


def answer_cb(callback_query_id, text_msg: str = "") -> None:
    try:
        data = {"callback_query_id": callback_query_id}
        if text_msg:
            data["text"] = text_msg
        requests.post(f"{API_BASE}/answerCallbackQuery", data=data, timeout=15)
    except requests.RequestException as e:
        print(f"[content_review_bot] answer_cb error: {_redact(e)}")


# ── DB helpers ────────────────────────────────────────────────────────────
# ⚠️ Карточка ждёт Шага Г, но НЕ бесконечно.
# Если отправлять сразу по появлению черновика, человек увидит «судья ещё не
# смотрел» и решит без вердикта — карточка это статичное сообщение, само оно не
# обновится. Если же ждать судью безусловно, то при ненастроенном триггере Шага Г
# (judge_dispatch_attempts так и остаётся 0, до MAX не доходит, give-up не
# срабатывает) карточки потерялись бы МОЛЧА — тот же класс тихой потери, что уже
# был у Шага Н (см. content_ai.py и миграцию 048).
# Поэтому: ждём вердикт или явный отказ судьи, но не дольше 30 минут.
_SELECT_NEW_DRAFTS = text("""
    SELECT id FROM content_candidates
    WHERE status = 'draft_ready' AND draft_text IS NOT NULL AND reviewer_notified_at IS NULL
      AND (judge_verdict IS NOT NULL
           OR judge_gave_up_at IS NOT NULL
           OR updated_at < now() - interval '30 minutes')
    ORDER BY id
""")

# ⚠️ Порядок колонок значим: индексы 0-4 используются позиционно (_approve
# читает row[3]=draft_text, row[4]=status). Новые поля — ТОЛЬКО в конец.
_SELECT_CANDIDATE = text("""
    SELECT id, headline, tickers, draft_text, status,
           reviewer_reason_code, reviewer_reason,
           judge_verdict, judge_failed, judge_defects, judge_paragraphs,
           judge_fixed_at, judge_fix_note
    FROM content_candidates WHERE id = :id
""")

_MARK_NOTIFIED = text("UPDATE content_candidates SET reviewer_notified_at = now() WHERE id = :id")

# ⚠️ draft_text_ai здесь НАМЕРЕННО не трогается (миграция 054): в нём лежит
# оригинал ИИ, и его сохранность — весь смысл обратной связи. Дифф
# (draft_text_ai → draft_text) и есть то, чему система должна учиться.
_UPDATE_DRAFT = text("""
    UPDATE content_candidates SET draft_text = :t, updated_at = now() WHERE id = :id
""")

_SET_REVIEW_REASON = text("""
    UPDATE content_candidates
    SET reviewer_reason_code = :code, updated_at = now()
    WHERE id = :id
""")

# Свободный комментарий ДОПОЛНЯЕТ код, не заменяет: код нужен для агрегации,
# текст — для деталей, которые в код не влезают.
_APPEND_REVIEW_REASON = text("""
    UPDATE content_candidates
    SET reviewer_reason = CASE
            WHEN reviewer_reason IS NULL OR reviewer_reason = '' THEN :t
            ELSE reviewer_reason || E'\n' || :t
        END,
        updated_at = now()
    WHERE id = :id
""")

_TO_IN_REVIEW = text("""
    UPDATE content_candidates SET status = 'in_review', updated_at = now()
    WHERE id = :id AND status = 'draft_ready'
""")

_TO_PUBLISHED = text("""
    UPDATE content_candidates
    SET status = 'published', reviewer_action = 'approved', reviewer_id = :rid,
        published_at = now(), updated_at = now()
    WHERE id = :id AND status = 'in_review'
""")

_TO_REJECTED = text("""
    UPDATE content_candidates
    SET status = 'rejected', reviewer_action = 'rejected', reviewer_id = :rid,
        updated_at = now()
    WHERE id = :id AND status = 'in_review'
""")


_INSERT_FEEDBACK = text("""
    INSERT INTO content_feedback (candidate_id, event, draft_ai, draft_human,
                                   reason_code, reason_text, brief_version,
                                   judge_verdict, judge_failed, judge_defects,
                                   judge_paragraphs, reviewer_id)
    SELECT c.id, :event,
           coalesce(c.draft_text_ai, c.draft_text),   -- у старых кандидатов ai пуст
           :draft_human, :reason_code, :reason_text, c.brief_version,
           c.judge_verdict, c.judge_failed, c.judge_defects, c.judge_paragraphs,
           :rid
    FROM content_candidates c WHERE c.id = :id
""")


def _log_feedback(db, cid: int, event: str, reason_code=None, reason_text=None,
                  draft_human=None) -> None:
    """Записать решение человека вместе со СНИМКОМ того, к чему оно относится.

    ⚠️ Только INSERT, никогда UPDATE. Обратная связь раньше жила в колонках самого
    кандидата, и повторный прогон Шага В стирал черновик вместе со смыслом причины
    отказа: датасет самоуничтожался тем быстрее, чем активнее мы правили промпт.
    «Обновлять последнюю строку» воспроизвело бы ту же ошибку в новом месте.

    ⚠️ Снимок берётся SQL-выражением из самого кандидата, а не передаётся аргументом:
    иначе вызывающий код должен помнить, что снимать ДО своего UPDATE, и рано или
    поздно забудет. Поэтому _log_feedback вызывается ПЕРЕД изменением черновика.

    ⚠️ Сбой журнала не должен ломать ревью. Человек нажал кнопку — решение обязано
    примениться; потеря строки журнала неприятна, но обратима, а неприменённое
    решение оставляет карточку в подвешенном состоянии.
    """
    try:
        db.execute(_INSERT_FEEDBACK, {
            "id": cid, "event": event, "reason_code": reason_code,
            "reason_text": reason_text, "draft_human": draft_human,
            "rid": config.CONTENT_REVIEWER_USER_ID,
        })
    except Exception as e:  # noqa: BLE001 — см. докстринг: журнал не блокирует ревью
        print(f"[content_review_bot] не записал фидбек по #{cid}: {_redact(e)}")


_JUDGE_MARK = {"годится": "✅", "спорно": "🟡", "брак": "⛔️"}


def _judge_line(verdict, failed, defects) -> str:
    """Вердикт Шага Г прямо в карточке — судья не блокирует ревью, его задача в том,
    чтобы дефект не прошёл НЕЗАМЕЧЕННЫМ. Провалы ворот и дефекты производства
    показываются РАЗДЕЛЬНО: первые определяют вердикт, вторые — нет (пункты
    чек-листа срабатывают и на реальных постах канала)."""
    if not verdict:
        return "\n\n🔍 судья ещё не смотрел"
    out = f"\n\n{_JUDGE_MARK.get(verdict, '')} судья: {html.escape(verdict)}"
    if failed:
        out += "\nпровалены ворота: " + html.escape(", ".join(failed))
    if defects:
        out += "\nдефекты производства: " + html.escape(", ".join(defects))
    return out


def _fix_line(fixed_at, note) -> str:
    """Отметка «текст правил судья» — обязательна, а не для красоты.

    ⚠️ Сегодня уже было наглядно, чего стоит вердикт, относящийся к ДРУГОМУ тексту
    (гонка Шага В, лечится draft_hash). Путать «текст писателя» и «текст судьи» —
    та же ошибка: человек читает пост, думая, что видит работу писателя, и его
    решение относится не к тому, к чему он думает.

    Вердикт при этом выдан на ИСХОДНЫЙ текст — судья себя не перепроверяет, — и в
    карточке это тоже должно быть сказано прямо.
    """
    if not fixed_at:
        return ""
    out = "\n\n✏️ текст выше ПОПРАВЛЕН СУДЬЁЙ (вердикт ниже — на исходный текст)"
    if note:
        out += "\n" + html.escape(str(note)[:300])
    return out


def _doubts_line(paragraphs) -> str:
    """Сомнения судьи по абзацам — прямо в карточке.

    ⚠️ Смысл поабзацного разбора не в отчётности, а в том, чтобы человек видел, за
    что именно зацепиться. Вердикт «годится» одной строкой ничего не даёт: на
    кандидате 1638 он был именно таким, а к каждому абзацу была своя претензия.
    Поэтому абзацы БЕЗ опоры показываются всегда, а сомнения по остальным — тоже,
    но короче: «чуйка» полезна ровно настолько, насколько её видно.
    """
    if not paragraphs:
        return ""
    try:
        items = paragraphs if isinstance(paragraphs, list) else json.loads(paragraphs)
    except (TypeError, ValueError):
        return ""
    lines = []
    for p in items:
        if not isinstance(p, dict):
            continue
        n = p.get("n", "?")
        doubt = (p.get("doubt") or "").strip()
        if p.get("supported") is False:
            claim = (p.get("claim") or "").strip()
            lines.append(f"⛔ абз.{n}: не на чём держится — {html.escape(claim[:110])}")
        elif doubt:
            lines.append(f"• абз.{n}: {html.escape(doubt[:110])}")
    if not lines:
        return ""
    return "\n\n🤔 под сомнением:\n" + "\n".join(lines)


def _reason_line(reason_code, reason_text) -> str:
    """Причина в карточке — чтобы повторно открытая карточка показывала не только
    ЧТО решили, но и ПОЧЕМУ (иначе решение снова становится неразмеченным)."""
    if not reason_code and not reason_text:
        return ""
    label = config.REVIEW_REASON_LABELS.get(reason_code or "", reason_code or "")
    out = f"\n[причина: {html.escape(label)}]" if label else ""
    if reason_text:
        out += f"\n{html.escape(reason_text)}"
    return out


def _reason_kb(cid: int) -> list:
    """Клавиатура причин — по две в ряд, чтобы влезало на телефоне.
    callback_data = 'r:<code>:<cid>' (лимит Telegram 64 байта, самый длинный
    код 17 символов — с запасом)."""
    rows, pair = [], []
    for code, label in config.REVIEW_REASON_CODES:
        pair.append({"text": label, "callback_data": f"r:{code}:{cid}"})
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    return rows


def _card_view(row):
    """Превью карточки — тело поста рендерится ТЕМИ ЖЕ premium custom-emoji, что
    и реальная публикация (apply_custom_emoji), чтобы «Одобрить» не преподносил
    сюрпризов в оформлении. Обвязка карточки (заголовок/тикеры) — свой html.escape,
    т.к. сообщение целиком уходит с parse_mode=HTML (см. send_kb)."""
    (cid, headline, tickers, draft_text, status, reason_code, reason_text,
     j_verdict, j_failed, j_defects, j_paragraphs, j_fixed_at, j_fix_note) = row
    body = apply_custom_emoji(with_frame_signature((draft_text or "")[:_DRAFT_PREVIEW_LIMIT]))
    tick = html.escape(", ".join(tickers or []) or "—")
    txt = f"📝 Кандидат #{cid} · {tick}\n{html.escape(headline or '')}\n\n{body}"
    txt += (_fix_line(j_fixed_at, j_fix_note)
            + _judge_line(j_verdict, j_failed, j_defects)
            + _doubts_line(j_paragraphs))
    if status != "draft_ready":
        # Карточка открыта повторно ПОСЛЕ решения (напр. по старой кнопке) — не даём
        # кнопки действия, только факт.
        return txt + f"\n\n[статус: {status}]{_reason_line(reason_code, reason_text)}", []
    kb = [
        [{"text": "✅ Одобрить и опубликовать", "callback_data": f"a:{cid}"}],
        [{"text": "✏️ Править", "callback_data": f"e:{cid}"},
         {"text": "❌ Отклонить", "callback_data": f"x:{cid}"}],
    ]
    return txt, kb


def _notify_new_drafts() -> None:
    """Раз за цикл поллинга — новые draft_ready кандидаты в личку админа."""
    if not config.ADMIN_USER_ID:
        return
    db = SessionLocal()
    try:
        ids = [r[0] for r in db.execute(_SELECT_NEW_DRAFTS).fetchall()]
        for cid in ids:
            row = db.execute(_SELECT_CANDIDATE, {"id": cid}).fetchone()
            if not row:
                continue
            txt, kb = _card_view(row)
            send_kb(config.ADMIN_USER_ID, txt, kb)
            db.execute(_MARK_NOTIFIED, {"id": cid})
            db.commit()
    except Exception as e:
        db.rollback()
        print(f"[content_review_bot] notify error: {e}")
    finally:
        db.close()


def _approve(db, cid: int) -> tuple:
    """Публикует в CONTENT_CHANNEL_ID, ТОЛЬКО при успехе двигает статус.
    Возврат: (ok: bool, message: str)."""
    row = db.execute(_SELECT_CANDIDATE, {"id": cid}).fetchone()
    if not row or row[4] != "draft_ready":
        return False, "Кандидат не найден или уже не в статусе draft_ready"
    if not config.CONTENT_CHANNEL_ID:
        return False, "CONTENT_CHANNEL_ID не задан в .env — некуда публиковать"

    db.execute(_TO_IN_REVIEW, {"id": cid})
    db.commit()

    ok, msg_id, err = send_text_post(text=row[3], channel_id=config.CONTENT_CHANNEL_ID)
    if not ok:
        # Оставляем в in_review — видно в Kanban как «застряло», не откатываем
        # молча в draft_ready (не даём повторный auto-triggered показ карточки).
        return False, f"Публикация не удалась: {err}. Статус: in_review (нужна ручная проверка)."

    _log_feedback(db, cid, "approved")
    res = db.execute(_TO_PUBLISHED, {"id": cid, "rid": config.CONTENT_REVIEWER_USER_ID})
    db.commit()
    if not res.rowcount:
        return False, f"Опубликовано (msg_id={msg_id}), но UPDATE статуса не применился — проверьте вручную"
    return True, f"Опубликовано ✅ (message_id={msg_id})"


def _reject(db, cid: int) -> tuple:
    row = db.execute(_SELECT_CANDIDATE, {"id": cid}).fetchone()
    if not row or row[4] != "draft_ready":
        return False, "Кандидат не найден или уже не в статусе draft_ready"
    db.execute(_TO_IN_REVIEW, {"id": cid})
    db.commit()
    _log_feedback(db, cid, "rejected")
    res = db.execute(_TO_REJECTED, {"id": cid, "rid": config.CONTENT_REVIEWER_USER_ID})
    db.commit()
    return (bool(res.rowcount), "Отклонено ❌" if res.rowcount else "Не удалось отклонить")


# ── in-memory состояние «ждём текст правки от этого чата» (v1, простое) ────
_awaiting_edit: dict = {}   # chat_id -> candidate_id
# Ждём СВОБОДНЫЙ комментарий к причине. Отдельный словарь, а не флаг внутри
# _awaiting_edit: правка и комментарий — разные вещи, и путать их значило бы
# записать текст поста в reviewer_reason (или наоборот). Ставится только по
# явному действию («другое» / кнопка «добавить комментарий»), иначе бот
# проглатывал бы любое следующее сообщение админа.
_awaiting_reason: dict = {}  # chat_id -> candidate_id

# Присланный по «Править» текст, который НЕ похож на пост — ждёт уточнения.
# ⚠️ Зачем. Кандидат 1638: Вадим нажал «✏️ Править» и написал туда РАЗБОР
# («спрогнозировало — спорное заявление и кто знает…»). Бот честно сделал то, о
# чём предупреждал, — заменил текст поста критикой. Кнопка «Править» стоит первой
# на карточке, а естественный жест после плохого черновика — сказать, что не так,
# а не переписать пост целиком. Публикация после этого отправила бы в канал
# разбор вместо поста.
_pending_edit: dict = {}  # chat_id -> (candidate_id, text)


def _looks_like_post(txt: str, current: str | None) -> bool:
    """Похож ли присланный текст на пост, а не на комментарий к нему.

    Признак — только ФОРМАТ: маркеры абзацев ◽️ либо хэштег рубрики последней
    строкой. Пост без хэштега рубрики и так не проходит format_ok у судьи, так
    что требование не лишнее.

    ⚠️ Запаса по длине здесь СОЗНАТЕЛЬНО нет, хотя он выглядел естественно
    («переписка редко втрое короче исходника»). Тест на реальном тексте 1638
    показал, как он обманывается: длинный комментарий против короткого поста
    проходит по длине и молча заменяет пост. Длина не отличает разбор от текста.

    Асимметрия цен решает всё: ошибка в одну сторону — лишний вопрос с двумя
    кнопками, в другую — критика уходит в канал как пост. Поэтому по умолчанию
    сомневаемся.
    """
    body = txt.strip()
    if not body:
        return False
    if "◽" in body:
        return True
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    return bool(lines) and lines[-1].startswith("#")


def process_callback(cb: dict) -> None:
    cb_id = cb.get("id")
    data = (cb.get("data") or "").strip()
    msg = cb.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    message_id = msg.get("message_id")
    if chat_id is None or message_id is None:
        if cb_id:
            answer_cb(cb_id)
        return

    # 'r:<code>:<cid>' несёт ДВА поля, остальные операции — только id.
    # Без этой развилки int("stretched_link:845") падал бы в ValueError и
    # кнопка причины молча ничего не делала.
    op, _, rest = data.partition(":")
    reason_code = ""
    if op == "r":
        reason_code, _, rest = rest.partition(":")
    try:
        cid = int(rest)
    except ValueError:
        answer_cb(cb_id)
        return

    db = SessionLocal()
    try:
        if op == "a":
            ok, note = _approve(db, cid)
            row = db.execute(_SELECT_CANDIDATE, {"id": cid}).fetchone()
            txt, kb = _card_view(row) if row else (note, [])
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id, note[:190])
        elif op == "x":
            ok, note = _reject(db, cid)
            row = db.execute(_SELECT_CANDIDATE, {"id": cid}).fetchone()
            txt, kb = _card_view(row) if row else (note, [])
            edit_kb(chat_id, message_id, txt, kb)
            answer_cb(cb_id, note[:190])
            # Причину спрашиваем ПОСЛЕ применённого решения: если Вадим не
            # ответит, теряется причина, а не сам отказ.
            if ok:
                send_kb(chat_id, f"Почему #{cid} не годится?", _reason_kb(cid))
        elif op == "r":
            _log_feedback(db, cid, "comment", reason_code=reason_code)
            db.execute(_SET_REVIEW_REASON, {"code": reason_code, "id": cid})
            db.commit()
            label = config.REVIEW_REASON_LABELS.get(reason_code, reason_code)
            answer_cb(cb_id, f"Причина: {label}"[:190])
            if reason_code == "other":
                _awaiting_reason[chat_id] = cid
                edit_kb(chat_id, message_id,
                        f"#{cid} — жду причину текстом следующим сообщением ✍️", [])
            else:
                # Комментарий не навязываем, но даём в один клик: свободный текст
                # часто содержит то, ради чего вся эта разметка и затевалась.
                edit_kb(chat_id, message_id, f"#{cid} — причина: {label}",
                        [[{"text": "✍️ добавить комментарий",
                           "callback_data": f"c:{cid}"}]])
        elif op == "c":
            _awaiting_reason[chat_id] = cid
            answer_cb(cb_id, "Жду комментарий ✍️")
            send(chat_id, f"Пришлите комментарий к #{cid} следующим сообщением.")
        elif op in ("ep", "ec"):
            pending = _pending_edit.pop(chat_id, None)
            if not pending or pending[0] != cid:
                answer_cb(cb_id, "Текст потерялся, пришлите заново")
                edit_kb(chat_id, message_id, f"#{cid} — текст потерялся, пришлите заново", [])
            elif op == "ep":
                _log_feedback(db, cid, "edited", draft_human=pending[1])
                db.execute(_UPDATE_DRAFT, {"t": pending[1], "id": cid})
                db.commit()
                answer_cb(cb_id, "Черновик обновлён ✏️")
                edit_kb(chat_id, message_id, f"#{cid} — черновик заменён вашим текстом ✏️", [])
                send_kb(chat_id, f"Что было не так в черновике #{cid}?", _reason_kb(cid))
            else:
                _log_feedback(db, cid, "comment", reason_text=pending[1])
                db.execute(_APPEND_REVIEW_REASON, {"t": pending[1], "id": cid})
                db.commit()
                answer_cb(cb_id, "Записал как комментарий ✍️")
                edit_kb(chat_id, message_id,
                        f"#{cid} — записал как комментарий, текст поста не тронут 👍", [])
        elif op == "e":
            _awaiting_edit[chat_id] = cid
            answer_cb(cb_id, "Жду текст следующим сообщением ✏️")
            send(chat_id, f"Пришлите новый текст поста для #{cid} следующим сообщением "
                           f"(целиком, он заменит текущий черновик).")
        else:
            answer_cb(cb_id)
    except Exception as e:
        db.rollback()
        print(f"[content_review_bot] callback error: {_redact(e)}")
        if cb_id:
            answer_cb(cb_id, "Ошибка, попробуйте ещё раз")
    finally:
        db.close()


def process_update(update: dict) -> None:
    if update.get("callback_query"):
        process_callback(update["callback_query"])
        return
    msg = update.get("message")
    if not msg:
        return
    chat_id = msg["chat"]["id"]
    txt = (msg.get("text") or "").strip()
    if not txt:
        return

    # Комментарий проверяем ПЕРВЫМ: если оба состояния как-то окажутся
    # выставлены разом, текст скорее относится к последнему вопросу бота.
    if chat_id in _awaiting_reason:
        cid = _awaiting_reason.pop(chat_id)
        db = SessionLocal()
        try:
            _log_feedback(db, cid, "comment", reason_text=txt)
            db.execute(_APPEND_REVIEW_REASON, {"t": txt, "id": cid})
            db.commit()
            send(chat_id, f"Записал причину к #{cid} 👍")
        except Exception as e:
            db.rollback()
            send(chat_id, f"Не удалось записать причину: {_redact(e)}")
        finally:
            db.close()
        return

    if chat_id in _awaiting_edit:
        cid = _awaiting_edit.pop(chat_id)
        db = SessionLocal()
        try:
            row = db.execute(_SELECT_CANDIDATE, {"id": cid}).fetchone()
            current = row[3] if row else None
            if not _looks_like_post(txt, current):
                # Не подменяем пост молча: спрашиваем, что это было.
                _pending_edit[chat_id] = (cid, txt)
                send_kb(chat_id,
                        f"Текст не похож на пост: нет ни ◽️, ни хэштега рубрики. "
                        f"Черновик #{cid} НЕ изменён.\n\n"
                        f"Это новый текст поста или комментарий к нему?",
                        [[{"text": "📝 это текст поста", "callback_data": f"ep:{cid}"},
                          {"text": "✍️ это комментарий", "callback_data": f"ec:{cid}"}]])
                return
            _log_feedback(db, cid, "edited", draft_human=txt)
            db.execute(_UPDATE_DRAFT, {"t": txt, "id": cid})
            db.commit()
            row = db.execute(_SELECT_CANDIDATE, {"id": cid}).fetchone()
            if row:
                card_txt, kb = _card_view(row)
                send_kb(chat_id, "Черновик обновлён ✏️\n\n" + card_txt, kb)
            else:
                send(chat_id, "Черновик обновлён, но кандидат не найден при повторном чтении")
            # Правка — тоже суждение человека: оригинал ИИ остался в
            # draft_text_ai, но БЕЗ причины дифф не объясняет, что было не так.
            send_kb(chat_id, f"Что было не так в черновике #{cid}?", _reason_kb(cid))
        except Exception as e:
            db.rollback()
            send(chat_id, f"Не удалось обновить черновик: {_redact(e)}")
        finally:
            db.close()
        return

    if txt.split()[0].lower() == "/start":
        send(chat_id, "👋 Review-бот content-пайплайна Frame. Карточки новых черновиков "
                       "приходят сюда автоматически.")


def main() -> None:
    print(f"[{datetime.now(timezone.utc)}] content_review_bot started")
    offset = None
    while True:
        try:
            _notify_new_drafts()
            # long-poll 20с (не 30): ходим через CF-worker relay, на 30с Telegram
            # отдаёт ответ впритык к нашему requests-таймауту и edge-лимитам
            # relay — ловили ~900 polling-ошибок/день (Read timed out + не-JSON).
            params = {"timeout": 20,
                      "allowed_updates": json.dumps(["message", "callback_query"])}
            if offset:
                params["offset"] = offset
            resp = requests.get(f"{API_BASE}/getUpdates", params=params, timeout=30)
            data = resp.json()
            if not data.get("ok"):
                time.sleep(5)
                continue
            for update in data["result"]:
                offset = update["update_id"] + 1
                try:
                    process_update(update)
                except Exception as e:
                    print(f"[content_review_bot] update error: {_redact(e)}")
        except requests.RequestException as e:
            print(f"[content_review_bot] polling error: {_redact(e)}")
            time.sleep(5)
        except Exception as e:
            print(f"[content_review_bot] loop error: {_redact(e)}")
            time.sleep(5)


if __name__ == "__main__":
    main()
