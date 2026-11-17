"""Вход в кабинет бота через браузер: одноразовый код. Сессию получает только окно с секретом и только раз."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api.services import browser_login as bl  # noqa: E402


class FakeRedis:
    def __init__(self):
        self.d = {}

    def set(self, k, v, ex=None):
        self.d[k] = v

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
    assert bl.info(r, s["code"]) == {"who": WHO, "pin": s["pin"]}
    assert len(s["pin"]) == 4 and s["pin"].isdigit()
    assert bl.approve(r, s["code"], 7)
    assert bl.take(r, s["secret"], 42) == ("approved", 7)
    assert bl.take(r, s["secret"], 42) == ("expired", None)   # второй раз сессию не отдаём


def test_other_telegram_account_cannot_take_session():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    bl.approve(r, s["code"], 7)
    assert bl.take(r, s["secret"], 99) == ("expired", None)   # секрет утёк — чужому окну не поможет
    assert bl.take(r, s["secret"], 42) == ("approved", 7)


def test_public_code_cannot_take_session():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    bl.approve(r, s["code"], 7)
    assert bl.take(r, s["code"], 42) == ("expired", None)     # код из адреса страницы — не секрет окна


def test_code_approved_only_once_and_unknown_rejected():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    assert bl.approve(r, s["code"], 7)
    assert not bl.approve(r, s["code"], 8)                    # чужой не перехватит уже подтверждённый
    assert bl.info(r, s["code"]) is None
    assert not bl.approve(r, "нет-такого-кода", 7)
    assert bl.take(r, "x" * 43, 42) == ("expired", None)


def test_start_is_rate_limited_per_telegram_account():
    r = FakeRedis()
    assert all(bl.start(r, 42, WHO) for _ in range(bl.MAX_STARTS))
    assert bl.start(r, 42, WHO) is None
    assert bl.start(r, 43, WHO) is not None                   # другой аккаунт не страдает


def test_secret_not_stored_in_clear():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    assert all(s["secret"] not in str(k) and s["secret"] not in str(v) for k, v in r.d.items())
