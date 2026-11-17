"""Мини-приложение Telegram: подпись initData. Привязка чата идёт только по верной и свежей подписи."""
import hashlib
import hmac
import json
import sys
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api.services.tg_webapp import MAX_AGE_SEC, parse_init_data  # noqa: E402

TOKEN = "123456:TEST-TOKEN"
NOW = 1_800_000_000


def make(user: dict, auth_date: int = NOW, token: str = TOKEN, **extra) -> str:
    fields = {"auth_date": str(auth_date), "query_id": "AAE", "user": json.dumps(user, ensure_ascii=False), **extra}
    check = "\n".join(f"{k}={v}" for k, v in sorted(fields.items()))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    fields["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return urlencode(fields)


def test_valid_signature_returns_user():
    u = parse_init_data(make({"id": 42, "username": "vadim", "first_name": "Вадим"}), TOKEN, NOW + 60)
    assert u and u["id"] == 42 and u["username"] == "vadim"


def test_tampered_user_rejected():
    good = make({"id": 42})
    assert parse_init_data(good.replace("42", "43"), TOKEN, NOW) is None


def test_other_bot_token_rejected():
    assert parse_init_data(make({"id": 42}, token="999:OTHER"), TOKEN, NOW) is None


def test_stale_data_rejected():
    assert parse_init_data(make({"id": 42}), TOKEN, NOW + MAX_AGE_SEC + 1) is None


def test_missing_hash_user_or_token_rejected():
    assert parse_init_data("auth_date=1&user=%7B%22id%22%3A1%7D", TOKEN, NOW) is None
    assert parse_init_data(make({"first_name": "x"}), TOKEN, NOW) is None       # нет id
    assert parse_init_data(make({"id": 42, "is_bot": True}), TOKEN, NOW) is None
    assert parse_init_data(make({"id": 42}), "", NOW) is None
    assert parse_init_data("", TOKEN, NOW) is None


def test_brand_emoji_fallbacks_are_emoji():
    """Telegram отклоняет сообщение, если запасной символ своего эмодзи — не эмодзи (было «F», «E»)."""
    import re
    from api.services import tg_webapp
    for html in (tg_webapp.FRAME_MARK, tg_webapp.FRAME_LOGO):
        for fallback in re.findall(r"<tg-emoji[^>]*>(.*?)</tg-emoji>", html):
            assert not re.search(r"[A-Za-zА-Яа-я0-9]", fallback), fallback
