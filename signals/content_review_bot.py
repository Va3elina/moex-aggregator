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
draft_ready-с-черновиком кандидаты (last_checked_at IS NULL — сюда их не
трогают ни content_match.py, ни content_ai.py, семантика поля свободна),
шлёт карточку в личку. Три действия:
  ✅ Одобрить  → публикует в CONTENT_CHANNEL_ID (ТЕСТОВЫЙ канал на старте!),
                 status: draft_ready → in_review → published (та же пара
                 переходов, что и ручная кнопка в админ-Kanban — держим
                 согласованно, не короткий путь в обход state machine).
  ✏️ Править   → бот просит текст следующим сообщением, обновляет draft_text,
                 статус НЕ меняется (остаётся draft_ready, снова на ревью).
  ❌ Отклонить → draft_ready → in_review → rejected, без публикации.

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
_SELECT_NEW_DRAFTS = text("""
    SELECT id FROM content_candidates
    WHERE status = 'draft_ready' AND draft_text IS NOT NULL AND last_checked_at IS NULL
    ORDER BY id
""")

_SELECT_CANDIDATE = text("""
    SELECT id, headline, tickers, draft_text, status, category, match_type
    FROM content_candidates WHERE id = :id
""")

_MARK_NOTIFIED = text("UPDATE content_candidates SET last_checked_at = now() WHERE id = :id")

_UPDATE_DRAFT = text("""
    UPDATE content_candidates SET draft_text = :t, updated_at = now() WHERE id = :id
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


def _card_view(row):
    """Превью карточки — тело поста рендерится ТЕМИ ЖЕ premium custom-emoji, что
    и реальная публикация (apply_custom_emoji), чтобы «Одобрить» не преподносил
    сюрпризов в оформлении. Обвязка карточки (заголовок/тикеры) — свой html.escape,
    т.к. сообщение целиком уходит с parse_mode=HTML (см. send_kb)."""
    cid, headline, tickers, draft_text, status, category, match_type = row
    body = apply_custom_emoji(with_frame_signature((draft_text or "")[:_DRAFT_PREVIEW_LIMIT]))
    tick = html.escape(", ".join(tickers or []) or (category or "—"))
    warn = (
        "\n🔶 <b>категория, не тикер</b> — эмитент новостью не назван, проверьте связь внимательнее\n"
        if match_type == "category" else ""
    )
    txt = f"📝 Кандидат #{cid} · {tick}\n{html.escape(headline or '')}{warn}\n\n{body}"
    if status != "draft_ready":
        # Карточка открыта повторно ПОСЛЕ решения (напр. по старой кнопке) — не даём
        # кнопки действия, только факт.
        return txt + f"\n\n[статус: {status}]", []
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
    res = db.execute(_TO_REJECTED, {"id": cid, "rid": config.CONTENT_REVIEWER_USER_ID})
    db.commit()
    return (bool(res.rowcount), "Отклонено ❌" if res.rowcount else "Не удалось отклонить")


# ── in-memory состояние «ждём текст правки от этого чата» (v1, простое) ────
_awaiting_edit: dict = {}   # chat_id -> candidate_id


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

    op, _, raw_id = data.partition(":")
    try:
        cid = int(raw_id)
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

    if chat_id in _awaiting_edit:
        cid = _awaiting_edit.pop(chat_id)
        db = SessionLocal()
        try:
            db.execute(_UPDATE_DRAFT, {"t": txt, "id": cid})
            db.commit()
            row = db.execute(_SELECT_CANDIDATE, {"id": cid}).fetchone()
            if row:
                card_txt, kb = _card_view(row)
                send_kb(chat_id, "Черновик обновлён ✏️\n\n" + card_txt, kb)
            else:
                send(chat_id, "Черновик обновлён, но кандидат не найден при повторном чтении")
        except Exception as e:
            db.rollback()
            send(chat_id, f"Не удалось обновить черновик: {e}")
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
            params = {"timeout": 30,
                      "allowed_updates": json.dumps(["message", "callback_query"])}
            if offset:
                params["offset"] = offset
            resp = requests.get(f"{API_BASE}/getUpdates", params=params, timeout=35)
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
