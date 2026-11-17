"""Вход в кабинет бота через браузер: одноразовый код. Сессию получает только окно с секретом и только раз."""
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
    assert "pin" not in s                                      # окно кода не знает
    assert bl.take(r, s["secret"], 42) == ("pending", None)
    assert bl.info(r, s["code"]) == {"who": WHO}               # и страница до подтверждения — тоже
    pin = bl.approve(r, s["code"], 7)
    assert pin and len(pin) == 4 and pin.isdigit()
    assert bl.take(r, s["secret"], 42) == ("need_pin", None)
    assert bl.take(r, s["secret"], 42, pin) == ("approved", 7)
    assert bl.take(r, s["secret"], 42, pin) == ("expired", None)   # второй раз сессию не отдаём


def test_wrong_pin_burns_attempt_after_three_tries():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    pin = bl.approve(r, s["code"], 7)
    bad = "0000" if pin != "0000" else "1111"
    assert bl.take(r, s["secret"], 42, bad) == ("wrong_pin", None)
    assert bl.take(r, s["secret"], 42, bad) == ("wrong_pin", None)
    assert bl.take(r, s["secret"], 42, bad) == ("expired", None)
    assert bl.take(r, s["secret"], 42, pin) == ("expired", None)   # верный код уже не поможет


def test_other_telegram_account_cannot_take_session():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    pin = bl.approve(r, s["code"], 7)
    assert bl.take(r, s["secret"], 99, pin) == ("expired", None)   # секрет и код утекли — чужому окну не помогут
    assert bl.take(r, s["secret"], 42, pin) == ("approved", 7)


def test_public_code_cannot_take_session():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    pin = bl.approve(r, s["code"], 7)
    assert bl.take(r, s["code"], 42, pin) == ("expired", None)     # код из адреса страницы — не секрет окна


def test_code_approved_only_once_and_unknown_rejected():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    assert bl.approve(r, s["code"], 7)
    assert bl.approve(r, s["code"], 8) is None                # чужой не перехватит уже подтверждённый
    assert bl.info(r, s["code"]) is None
    assert bl.approve(r, "нет-такого-кода", 7) is None
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


def test_parallel_wrong_pins_cannot_exceed_limit():
    """Пачка одновременных неверных кодов: счётчик отдельным ключом, состояние не перезаписывается."""
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    pin = bl.approve(r, s["code"], 7)
    wrong = [f"{i:04d}" for i in range(20) if f"{i:04d}" != pin]
    results = [bl.take(r, s["secret"], 42, w)[0] for w in wrong]
    assert results.count("wrong_pin") == bl.MAX_PIN_TRIES - 1
    assert set(results[bl.MAX_PIN_TRIES - 1:]) == {"expired"}
    assert bl.take(r, s["secret"], 42, pin) == ("expired", None)


def test_burned_attempt_is_not_revived_by_late_write():
    r = FakeRedis()
    s = bl.start(r, 42, WHO)
    assert bl.take(r, s["secret"], 99) == ("expired", None)        # чужое окно ничего не меняет
    key = next(k for k in r.d if k.startswith("tglogin:s:"))
    del r.d[key]                                                   # попытка сгорела
    assert bl.approve(r, s["code"], 7) is None                     # и запоздалое подтверждение её не вернёт
    assert key not in r.d
