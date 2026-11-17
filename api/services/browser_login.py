"""Вход в кабинет бота через обычный браузер — одноразовый код, как подключение телевизора.

В окне Telegram нет сохранённых паролей и входа в Яндекс/VK, поэтому окно берёт код, открывает с ним
сайт в обычном браузере, там уже вошедший человек подтверждает — и окно получает сессию.
Вход даёт САЙТ (обычная сессия подтверждающего), Telegram в авторизации не участвует (406-ФЗ).

Значения одной попытки:
  code   — публичный, едет в адресе страницы подтверждения; по нему сессию получить нельзя;
  secret — остаётся в окне, по нему окно забирает сессию; в Redis лежит только его sha256;
  tg_id  — чьё это окно (из подписанных данных мини-приложения): сессию отдаём только ему;
  pin    — 4 цифры: появляются ТОЛЬКО на странице сайта после подтверждения, вводятся в окне бота.
           Так окно доказывает, что перед ним тот же человек, что подтвердил; MAX_PIN_TRIES ошибок — и всё.
Живёт TTL_SEC, сессия по подтверждённому коду выдаётся один раз.

Защита от чужой ссылки («подтвердите вход» присылает злоумышленник): попытку можно начать только из
окна бота, страница показывает, в чьём Telegram откроется кабинет; забрать сессию может только тот же
Telegram-аккаунт и только с кодом, который видел подтвердивший — злоумышленник страницы жертвы не видит.
"""
import hashlib
import hmac
import json
import secrets

TTL_SEC = 600
MAX_STARTS = 10        # попыток на один Telegram-аккаунт за TTL_SEC
MAX_PIN_TRIES = 3      # неверных кодов на одну попытку
_R = "tglogin:r:"      # tg_id → счётчик попыток
_S = "tglogin:s:"      # sha256(secret) → {"code", "user_id", "tg_id", "who", "pin"}
_C = "tglogin:c:"      # code → sha256(secret)
_T = "tglogin:t:"      # sha256(secret) → сколько раз присылали код (атомарный счётчик)


def _h(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def start(r, tg_id: int, who: dict) -> dict | None:
    """Новая попытка из окна Telegram-пользователя tg_id. who = {"username", "name"} — для показа на
    странице подтверждения. None — слишком много попыток подряд."""
    n = r.incr(_R + str(tg_id))
    if n == 1:
        r.expire(_R + str(tg_id), TTL_SEC)
    if n > MAX_STARTS:
        return None
    code, secret = secrets.token_urlsafe(16), secrets.token_urlsafe(32)
    r.set(_S + _h(secret), json.dumps({"code": code, "user_id": None, "tg_id": tg_id, "who": who, "pin": None}), ex=TTL_SEC)
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
    return {"who": got[1]["who"]}


def _save(r, key: str, state: dict) -> None:
    # XX + KEEPTTL: только обновить живую попытку — сгоревшую или забранную запись не воскрешаем
    r.set(_S + key, json.dumps(state), xx=True, keepttl=True)


def approve(r, code: str, user_id: int) -> str | None:
    """Вошедший на сайте человек разрешает окну войти под собой. Возвращает код из 4 цифр — его
    показывают ТОЛЬКО подтвердившему, а вводят в окне бота. None — код устарел или уже подтверждён."""
    got = _load(r, code)
    if not got or got[1]["user_id"] is not None:
        return None
    key, state = got
    state["user_id"] = user_id
    state["pin"] = f"{secrets.randbelow(10000):04d}"
    _save(r, key, state)
    return state["pin"]


def take(r, secret: str, tg_id: int, pin: str | None = None) -> tuple[str, int | None]:
    """Окно спрашивает, подтвердили ли, и забирает сессию.
      ('pending', None)   — ещё не подтвердили;
      ('need_pin', None)  — подтвердили, окно должно прислать код со страницы сайта;
      ('wrong_pin', None) — код не тот (после MAX_PIN_TRIES ошибок попытка сгорает);
      ('approved', id)    — ровно один раз: ключ удаляем, дальше 'expired';
      ('expired', None)   — попытки нет, она чужая или сгорела.
    Отвечаем только окну того же Telegram-аккаунта, что начал попытку."""
    hk = _h(secret)
    raw = r.get(_S + hk)
    if not raw:
        return "expired", None
    state = json.loads(raw)
    if state["tg_id"] != tg_id:
        return "expired", None
    if state["user_id"] is None:
        return "pending", None
    if not pin:
        return "need_pin", None
    # Счётчик попыток — отдельным ключом и ДО сравнения: INCR атомарен, поэтому пачка одновременных
    # запросов не переберёт больше MAX_PIN_TRIES кодов (состояние в этой ветке не перезаписываем).
    n = r.incr(_T + hk)
    if n == 1:
        r.expire(_T + hk, TTL_SEC)
    if n > MAX_PIN_TRIES:
        r.delete(_S + hk)
        r.delete(_C + state["code"])
        return "expired", None
    if not hmac.compare_digest(pin, state["pin"]):
        if n == MAX_PIN_TRIES:        # последняя попытка потрачена — сжигаем сразу
            r.delete(_S + hk)
            r.delete(_C + state["code"])
            return "expired", None
        return "wrong_pin", None
    if r.delete(_S + hk) != 1:        # параллельный опрос уже забрал сессию
        return "expired", None
    r.delete(_C + state["code"])
    return "approved", int(state["user_id"])
