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

    def delete(self, k):
        return 1 if self.d.pop(k, None) is not None else 0


def test_full_path_gives_session_once():
    r = FakeRedis()
    s = bl.start(r, "@vadim")
    assert bl.take(r, s["secret"]) == ("pending", None)
    assert bl.info(r, s["code"]) == {"who": "@vadim"}
    assert bl.approve(r, s["code"], 7)
    assert bl.take(r, s["secret"]) == ("approved", 7)
    assert bl.take(r, s["secret"]) == ("expired", None)      # второй раз сессию не отдаём


def test_public_code_cannot_take_session():
    r = FakeRedis()
    s = bl.start(r)
    bl.approve(r, s["code"], 7)
    assert bl.take(r, s["code"]) == ("expired", None)        # код из адреса страницы — не секрет окна


def test_code_approved_only_once_and_unknown_rejected():
    r = FakeRedis()
    s = bl.start(r)
    assert bl.approve(r, s["code"], 7)
    assert not bl.approve(r, s["code"], 8)                    # чужой не перехватит уже подтверждённый
    assert bl.info(r, s["code"]) is None
    assert not bl.approve(r, "нет-такого-кода", 7)
    assert bl.take(r, "x" * 43) == ("expired", None)


def test_secret_not_stored_in_clear():
    r = FakeRedis()
    s = bl.start(r)
    assert all(s["secret"] not in k and s["secret"] not in v for k, v in r.d.items())
