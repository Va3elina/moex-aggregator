"""Бот ревью: отправка карточки и кнопка «✏️ Править».

10.09.2026, кандидат 1933. Три вещи, которые здесь закреплены:
- карточка считается отправленной, только если Telegram её ПРИНЯЛ: раньше ответ
  не читался, и отказ Telegram терял карточку молча;
- «Править» отправляет замечание ИИ-писателю, а не заменяет текст поста
  присланным сообщением;
- строка «Данные FinanceMarker, …» под постом больше не клеится ни в превью,
  ни в публикацию, а писатель её в брифе не видит.
"""
import inspect
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from signals import content_review_bot as bot  # noqa: E402
from signals import content_ai as ca  # noqa: E402


class _Resp:
    def __init__(self, payload, status=200):
        self._p, self.status_code = payload, status

    def json(self):
        if isinstance(self._p, Exception):
            raise self._p
        return self._p


def _post_returning(payload, status=200):
    return lambda *a, **k: _Resp(payload, status)


def test_send_kb_true_only_when_telegram_accepts(monkeypatch):
    monkeypatch.setattr(bot.requests, "post", _post_returning({"ok": True, "result": {}}))
    assert bot.send_kb(1, "x", []) is True


def test_send_kb_false_when_telegram_refuses(monkeypatch):
    monkeypatch.setattr(bot.requests, "post", _post_returning(
        {"ok": False, "description": "Bad Request: can't parse entities"}, 400))
    assert bot.send_kb(1, "x", []) is False


def test_send_kb_false_on_non_json(monkeypatch):
    monkeypatch.setattr(bot.requests, "post", _post_returning(ValueError("html"), 502))
    assert bot.send_kb(1, "x", []) is False


def test_card_marked_sent_only_after_telegram_accepts():
    src = inspect.getsource(bot._notify_new_drafts)
    assert "if send_kb(" in src
    assert src.index("if send_kb(") < src.index("_MARK_NOTIFIED")


def test_edit_button_sends_remarks_to_writer_not_replaces_post():
    src = inspect.getsource(bot.process_update)
    ветка = src[src.index("if chat_id in _awaiting_edit:"):]
    ветка = ветка[:ветка.index("return")]
    assert "fire_revision" in ветка
    assert "_UPDATE_DRAFT" not in ветка
    assert "revision_requested" in ветка


def test_no_financemarker_line_in_card_or_post():
    for fn in (bot._card_view, bot._approve):
        assert "with_data_annotation" not in inspect.getsource(fn), fn.__name__


def test_writer_brief_has_no_ready_made_financemarker_line():
    src = inspect.getsource(ca._build_brief)
    assert '_фундамент.pop("аннотация", None)' in src
    граница = inspect.getsource(ca._company_fundamentals).split('out["ГРАНИЦА"]')[-1]
    assert "НЕ публикуются" in граница
    assert "УЖЕ уйдут" not in граница


def test_revision_payload_carries_draft_and_remarks():
    src = inspect.getsource(ca.fire_revision)
    assert "текущий_черновик" in src and "что_поправить" in src
    assert "_build_brief(db, row)" in src


# ── Коллега в боте (Вадим 10.09): ему — только черновики ──────────────────────

def test_colleague_list_is_read_only_by_the_review_bot():
    """Бэкапы и мониторинг шлют другие скрипты. Пока список получателей читает
    только review-бот, ничего, кроме карточек черновиков, коллеге не уйдёт."""
    root = Path(__file__).resolve().parents[2]
    users = set()
    for d in ("signals", "scripts", "api", "db"):
        for p in (root / d).rglob("*"):
            if p.suffix not in (".py", ".sh") or ".venv" in p.parts:
                continue
            if "CONTENT_DRAFT_EXTRA_CHAT_IDS" in p.read_text(encoding="utf-8", errors="ignore"):
                users.add(p.relative_to(root).as_posix())
    assert users == {"signals/config.py", "signals/content_review_bot.py"}, users


def test_colleague_gets_card_only_after_admin_card_accepted():
    src = inspect.getsource(bot._notify_new_drafts)
    assert src.index("_MARK_NOTIFIED") < src.index("CONTENT_DRAFT_EXTRA_CHAT_IDS")


def test_strangers_cannot_press_buttons(monkeypatch):
    monkeypatch.setattr(bot.config, "ADMIN_USER_ID", 1)
    monkeypatch.setattr(bot.config, "CONTENT_DRAFT_EXTRA_CHAT_IDS", [2])
    answered = []
    monkeypatch.setattr(bot, "answer_cb", lambda *a, **k: answered.append(a))

    def _no_db():
        raise AssertionError("чужой чат дошёл до БД")
    monkeypatch.setattr(bot, "SessionLocal", _no_db)
    bot.process_callback({"id": "q", "data": "x:1933",
                          "message": {"chat": {"id": 3}, "message_id": 5}})
    assert answered


def test_colleague_actions_reach_admin_but_admins_own_do_not(monkeypatch):
    monkeypatch.setattr(bot.config, "ADMIN_USER_ID", 1)
    sent = []
    monkeypatch.setattr(bot, "send", lambda c, t: sent.append((c, t)))
    bot._tell_admin(1, {"first_name": "Вадим"}, "x")
    assert sent == []
    bot._tell_admin(2, {"first_name": "Саша"}, "❌ отклонил #1933")
    assert sent == [(1, "👤 Саша: ❌ отклонил #1933")]
