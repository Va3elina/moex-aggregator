"""Вход в кабинет бота через обычный браузер — как подключение телевизора к аккаунту.

В окне Telegram нет сохранённых паролей и входа в Яндекс/VK, поэтому окно открывает сайт в обычном
браузере, там уже вошедший человек подтверждает — и окно получает сессию.
Вход даёт САЙТ (обычная сессия подтверждающего), Telegram в авторизации не участвует (406-ФЗ).

Значения одной попытки:
  code   — публичный, едет в адресе страницы подтверждения; по нему сессию получить нельзя;
  secret — остаётся в окне, по нему окно забирает сессию; в Redis лежит только его sha256;
  tg_id  — чьё это окно (из подписанных данных мини-приложения): сессию отдаём только ему;
  ticket — одноразовый билет: выдаётся ТОЛЬКО подтвердившему на сайте, зашит в его кнопку «Вернуться
           в Telegram» (t.me/бот?start=in_<ticket>) и приходит боту из его же Telegram.
Живёт TTL_SEC, сессия выдаётся один раз.

Защита от чужой ссылки («подтвердите вход» присылает злоумышленник): попытку начинает только окно бота;
билет видит лишь тот, кто подтвердил, а бот принимает его только от Telegram-аккаунта, начавшего попытку.
Жертва, нажавшая «Подтвердить» по чужой ссылке, отправит билет из СВОЕГО Telegram — он не подойдёт и сгорит.
Кодов никто не вводит (Вадим 17.11: ввод цифр неудобен).
"""
import hashlib
import json
import secrets

TTL_SEC = 600
MAX_STARTS = 10        # попыток на один Telegram-аккаунт за TTL_SEC
TICKET_PREFIX = "in_"  # параметр /start: in_<ticket>
_R = "tglogin:r:"      # tg_id → счётчик попыток
_S = "tglogin:s:"      # sha256(secret) → {"code", "user_id", "tg_id", "who", "ok"}
_C = "tglogin:c:"      # code → sha256(secret)
_K = "tglogin:k:"      # sha256(ticket) → sha256(secret)


def _h(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def start(r, tg_id: int, who: dict) -> dict | None:
    """Новая попытка из окна Telegram-пользователя tg_id. who = {"username", "name"} — для показа на
    странице подтверждения. None — слишком много попыток подряд."""
    n = r.incr(_R + str(tg_id))
    if n == 1:
        r.expire(_R + str(tg_id), TTL_SEC)
    if n > MAX_STARTS:
        return None
    code, secret = secrets.token_urlsafe(16), secrets.token_urlsafe(32)
    r.set(_S + _h(secret), json.dumps({"code": code, "user_id": None, "tg_id": tg_id, "who": who, "ok": False}), ex=TTL_SEC)
    r.set(_C + code, _h(secret), ex=TTL_SEC)
    return {"code": code, "secret": secret, "expires_in": TTL_SEC}


def _load(r, code: str) -> tuple[str, dict] | None:
    key = r.get(_C + code)
    raw = r.get(_S + key) if key else None
    return (key, json.loads(raw)) if raw else None


def _save(r, key: str, state: dict) -> None:
    # XX + KEEPTTL: только обновить живую попытку — сгоревшую или забранную запись не воскрешаем
    r.set(_S + key, json.dumps(state), xx=True, keepttl=True)


def info(r, code: str) -> dict | None:
    """Для страницы подтверждения: жив ли код и чьё окно ждёт. None — устарел или уже использован."""
    got = _load(r, code)
    if not got or got[1]["user_id"] is not None:
        return None
    return {"who": got[1]["who"]}


def approve(r, code: str, user_id: int) -> str | None:
    """Вошедший на сайте человек разрешает окну войти под собой. Возвращает одноразовый билет — его
    получает ТОЛЬКО подтвердивший (в кнопке возврата в Telegram). None — код устарел или уже подтверждён."""
    got = _load(r, code)
    if not got or got[1]["user_id"] is not None:
        return None
    key, state = got
    state["user_id"] = user_id
    _save(r, key, state)
    ticket = secrets.token_urlsafe(18)
    ttl = r.ttl(_S + key)
    r.set(_K + _h(ticket), key, ex=ttl if ttl and ttl > 0 else TTL_SEC)
    return ticket


def confirm(r, ticket: str, tg_id: int) -> int | None:
    """Бот получил билет по /start из чата tg_id. Билет одноразовый: сгорает при первом же предъявлении,
    даже неудачном. Возвращает user_id, если билет пришёл из того же Telegram, что начал попытку."""
    kkey = _K + _h(ticket)
    key = r.get(kkey)
    if not key or r.delete(kkey) != 1:
        return None
    raw = r.get(_S + key)
    if not raw:
        return None
    state = json.loads(raw)
    if state["user_id"] is None or state["tg_id"] != tg_id:
        return None
    state["ok"] = True
    _save(r, key, state)
    return int(state["user_id"])


def take(r, secret: str, tg_id: int) -> tuple[str, int | None]:
    """Окно спрашивает, можно ли войти.
      ('pending', None)      — на сайте ещё не подтвердили;
      ('need_confirm', None) — подтвердили, ждём билет через бота («Вернуться в Telegram» → «Запустить»);
      ('approved', id)       — ровно один раз: ключ удаляем, дальше 'expired';
      ('expired', None)      — попытки нет или она чужая.
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
    if not state["ok"]:
        return "need_confirm", None
    if r.delete(_S + hk) != 1:        # параллельный опрос уже забрал сессию
        return "expired", None
    r.delete(_C + state["code"])
    return "approved", int(state["user_id"])
