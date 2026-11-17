"""Вход в кабинет бота через браузер: сессию получает только окно того Telegram, что начал попытку,
и только после билета, который видел подтвердивший на сайте. Ничего вводить не нужно."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api.services import browser_login as bl  # noqa: E402


class FakeRedis:
    def __init__(self):
        self.d = {}

    def set(self, k, v, ex=None, xx=False, keepttl=False):
        if xx and k not in self.d:
            return None
        self.d[k] = v
        return True

    def get(self, k):
        return self.d.get(k)

    def ttl(self, k):
        return 300 if k in self.d else -2

    def incr(self, k):
        self.d[k] = int(self.d.get(k, 0)) + 1
        return self.d[k]

    def expire(self, k, ex):
        pass

    def delete(self, k):
        return 1 if self.d.pop(k, None) is not None else 0


WHO = {"username": "vadim", "name": "Вадим"}


def test_full_path_gives_session_once():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    assert bl.take(r, s["secret"], 42) == ("pending", None)
    assert bl.info(r, s["code"]) == {"who": WHO}
    ticket = bl.approve(r, s["code"], 7)
    assert ticket and len(bl.TICKET_PREFIX + ticket) <= 64          # влезает в параметр /start
    assert bl.take(r, s["secret"], 42) == ("need_confirm", None)   # одного «Подтвердить» мало
    assert bl.confirm(r, ticket, 42) == 7
    assert bl.take(r, s["secret"], 42) == ("approved", 7)
    assert bl.take(r, s["secret"], 42) == ("expired", None)        # второй раз сессию не отдаём


def test_phished_victim_cannot_hand_session_to_attacker():
    """Злоумышленник (tg 666) начал вход и прислал ссылку; жертва подтвердила и вернулась в СВОЙ Telegram."""
    r = FakeRedis()
    s = bl.start(r, 666, {"username": "attacker", "name": None})
    ticket = bl.approve(r, s["code"], 7)                            # жертва нажала «Подтвердить»
    assert bl.confirm(r, ticket, 42) is None                        # билет пришёл боту от жертвы, не от 666
    assert bl.take(r, s["secret"], 666) == ("need_confirm", None)   # окно злоумышленника сессию не получило
    assert bl.confirm(r, ticket, 666) is None                       # и сам билет уже сгорел


def test_ticket_is_one_time_and_unknown_rejected():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    ticket = bl.approve(r, s["code"], 7)
    assert bl.confirm(r, ticket, 42) == 7
    assert bl.confirm(r, ticket, 42) is None
    assert bl.confirm(r, "нет-такого-билета", 42) is None


def test_other_telegram_account_cannot_take_session():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    bl.confirm(r, bl.approve(r, s["code"], 7), 42)
    assert bl.take(r, s["secret"], 99) == ("expired", None)        # секрет утёк — чужому окну не поможет
    assert bl.take(r, s["secret"], 42) == ("approved", 7)


def test_public_code_cannot_take_session():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    bl.confirm(r, bl.approve(r, s["code"], 7), 42)
    assert bl.take(r, s["code"], 42) == ("expired", None)          # код из адреса страницы — не секрет окна


def test_code_approved_only_once_and_unknown_rejected():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    assert bl.approve(r, s["code"], 7)
    assert bl.approve(r, s["code"], 8) is None                     # чужой не перехватит уже подтверждённый
    assert bl.info(r, s["code"]) is None
    assert bl.approve(r, "нет-такого-кода", 7) is None
    assert bl.take(r, "x" * 43, 42) == ("expired", None)


def test_start_is_rate_limited_per_telegram_account():
    r = FakeRedis()
    assert all(bl.start(r, 42, WHO) for _ in range(bl.MAX_STARTS))
    assert bl.start(r, 42, WHO) is None
    assert bl.start(r, 43, WHO) is not None                        # другой аккаунт не страдает


def test_secrets_not_stored_in_clear_and_burned_attempt_not_revived():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    ticket = bl.approve(r, s["code"], 7)
    assert all(x not in str(k) and x not in str(v) for k, v in r.d.items() for x in (s["secret"], ticket))
    key = next(k for k in r.d if k.startswith("tglogin:s:"))
    del r.d[key]                                                   # попытка сгорела
    assert bl.confirm(r, ticket, 42) is None                       # запоздалый билет её не вернёт
    assert key not in r.d


def test_bot_signature_matches_server():
    """Бот (signals/alert_bot.py) и сервер подписывают «билет:чат» одинаково."""
    import hashlib
    import hmac
    import inspect
    import os
    os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
    from api.routers import tg_login
    want = hmac.new(b"1:TOKEN", b"abc:42", hashlib.sha256).hexdigest()
    assert tg_login.confirm_sig("1:TOKEN", "abc", 42) == want
    bot_src = (Path(__file__).resolve().parents[1] / "signals" / "alert_bot.py").read_text(encoding="utf-8")
    assert 'f"{ticket}:{chat_id}".encode(), hashlib.sha256' in bot_src
    assert "compare_digest" in inspect.getsource(tg_login.confirm)
