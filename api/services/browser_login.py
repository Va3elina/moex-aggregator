"""Вход в кабинет бота через обычный браузер — одноразовый код, как подключение телевизора.

В окне Telegram нет сохранённых паролей и входа в Яндекс/VK, поэтому окно берёт код, открывает с ним
сайт в обычном браузере, там уже вошедший человек подтверждает — и окно получает сессию.
Вход даёт САЙТ (обычная сессия подтверждающего), Telegram в авторизации не участвует (406-ФЗ).

Два значения на одну попытку:
  code   — публичный, едет в адресе страницы подтверждения; по нему сессию получить нельзя;
  secret — остаётся в окне, по нему окно забирает сессию; в Redis лежит только его sha256.
Живёт TTL_SEC, сессия по подтверждённому коду выдаётся один раз.
"""
import hashlib
import json
import secrets

TTL_SEC = 600
_S = "tglogin:s:"      # sha256(secret) → {"code", "user_id", "who"}
_C = "tglogin:c:"      # code → sha256(secret)


def _h(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def start(r, who: str | None = None) -> dict:
    """Новая попытка. who — подпись «у кого откроется кабинет» (имя из Telegram), только для показа."""
    code, secret = secrets.token_urlsafe(16), secrets.token_urlsafe(32)
    r.set(_S + _h(secret), json.dumps({"code": code, "user_id": None, "who": who}), ex=TTL_SEC)
    r.set(_C + code, _h(secret), ex=TTL_SEC)
    return {"code": code, "secret": secret, "expires_in": TTL_SEC}


def _load(r, code: str) -> tuple[str, dict] | None:
    key = r.get(_C + code)
    raw = r.get(_S + key) if key else None
    return (key, json.loads(raw)) if raw else None


def info(r, code: str) -> dict | None:
    """Для страницы подтверждения: жив ли код и чьё окно ждёт. None — устарел или уже использован."""
    got = _load(r, code)
    if not got or got[1]["user_id"] is not None:
        return None
    return {"who": got[1].get("who")}


def approve(r, code: str, user_id: int) -> bool:
    """Вошедший на сайте человек разрешает окну войти под собой. Подтвердить можно один раз."""
    got = _load(r, code)
    if not got or got[1]["user_id"] is not None:
        return False
    key, state = got
    state["user_id"] = user_id
    ttl = r.ttl(_S + key)
    r.set(_S + key, json.dumps(state), ex=ttl if ttl and ttl > 0 else TTL_SEC)
    return True


def take(r, secret: str) -> tuple[str, int | None]:
    """Окно спрашивает, подтвердили ли. ('pending', None) | ('expired', None) | ('approved', user_id).
    'approved' отдаётся ровно один раз: ключ удаляем, и второй опрос уже 'expired'."""
    key = _S + _h(secret)
    raw = r.get(key)
    if not raw:
        return "expired", None
    state = json.loads(raw)
    if state["user_id"] is None:
        return "pending", None
    if r.delete(key) != 1:           # параллельный опрос уже забрал сессию
        return "expired", None
    r.delete(_C + state["code"])
    return "approved", int(state["user_id"])
