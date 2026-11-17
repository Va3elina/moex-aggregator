"""Данные мини-приложения Telegram (initData): проверка подписи и сообщение в чат бота.

Нужны ТОЛЬКО для привязки чата уведомлений к уже вошедшему пользователю (api/routers/alerts.py,
link-webapp). Входом это не служит и служить не должно: авторизация через Telegram запрещена 406-ФЗ
(см. api/routers/oauth.py) — сессию по этим данным не выдаём.

Подпись — по документации Telegram: secret = HMAC_SHA256("WebAppData", токен бота),
hash = HMAC_SHA256(secret, строки «ключ=значение» по алфавиту через перевод строки).
"""
import hashlib
import hmac
import json
import os
import time
from urllib.parse import parse_qsl

import requests

MAX_AGE_SEC = 24 * 3600          # окно Telegram может быть открыто долго; старше суток — открыть заново
WEBAPP_URL = "https://framedata.ru/tg"


def bot_token() -> str:
    return os.environ.get("ALERT_BOT_TOKEN", "").strip()


def parse_init_data(init_data: str, token: str, now: float | None = None) -> dict | None:
    """Пользователь Telegram из initData ({id, username, first_name, ...}) или None, если подпись
    не сошлась, данные старше MAX_AGE_SEC или пользователя в них нет."""
    if not init_data or not token:
        return None
    pairs = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=False))
    got = pairs.pop("hash", "")
    if not got:
        return None
    check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    want = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(want, got):
        return None
    try:
        if (now if now is not None else time.time()) - int(pairs.get("auth_date", "0")) > MAX_AGE_SEC:
            return None
        user = json.loads(pairs.get("user", ""))
    except (ValueError, TypeError):
        return None
    if not isinstance(user, dict) or not isinstance(user.get("id"), int) or user.get("is_bot"):
        return None
    return user


def send_linked_message(chat_id: int) -> None:
    """«Готово» в чат бота: итог привязки должен быть виден и без открытого окна. Сбой не критичен."""
    token = bot_token()
    if not token:
        return
    root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/")
    try:
        requests.post(f"{root}/bot{token}/sendMessage", timeout=10, json={
            "chat_id": chat_id,
            "text": "✅ <b>Аккаунт Фрейма подключён</b>\nСигналы будут приходить в этот чат.",
            "parse_mode": "HTML",
            "reply_markup": {"inline_keyboard": [[{"text": "Открыть кабинет", "web_app": {"url": WEBAPP_URL}}]]},
        })
    except requests.RequestException as e:
        print(f"[tg_webapp] не отправил подтверждение привязки: {str(e).replace(token, '<REDACTED>')}")
