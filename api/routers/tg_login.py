"""
/api/tg-login/* — вход в кабинет бота @framesignalbot через обычный браузер (api/services/browser_login.py).

  POST /start    — окно бота берёт одноразовый код (без входа)
  GET  /info     — страница подтверждения: жив ли код (нужен вход на сайте)
  POST /approve  — вошедший человек разрешает окну войти под собой; получает ссылку возврата в бота
  POST /confirm  — бот передаёт билет из этой ссылки (вызывает signals/alert_bot.py, подпись токеном бота)
  POST /poll     — окно забирает сессию: после подтверждения и билета (один раз)

Префикс не /api/auth: там лимит 10 запросов в минуту, а окно опрашивает /poll раз в несколько секунд.
"""
import hashlib
import hmac
import os

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.cache import _get_redis
from api.database import get_db
from api.logger import get_logger
from api.models import User
from api.routers.alerts import link_chat
from api.routers.auth import get_current_user, persist_refresh_token
from api.security import create_token_pair
from api.services import browser_login, tg_webapp

router = APIRouter(prefix="/api/tg-login", tags=["tg-login"])
logger = get_logger(__name__)

_CODE = r"^[A-Za-z0-9_-]+$"


class StartRequest(BaseModel):
    init_data: str = Field(min_length=1, max_length=4096)


class CodeRequest(BaseModel):
    code: str = Field(min_length=16, max_length=64, pattern=_CODE)


class PollRequest(BaseModel):
    secret: str = Field(min_length=32, max_length=128, pattern=_CODE)
    init_data: str = Field(min_length=1, max_length=4096)


class ConfirmRequest(BaseModel):
    ticket: str = Field(min_length=16, max_length=64, pattern=_CODE)
    chat_id: int
    username: str | None = Field(default=None, max_length=64)
    sig: str = Field(min_length=64, max_length=64)


def confirm_sig(token: str, ticket: str, chat_id: int) -> str:
    """Подпись запроса бота: только тот, у кого токен бота, может сказать «билет пришёл из чата chat_id»."""
    return hmac.new(token.encode(), f"{ticket}:{chat_id}".encode(), hashlib.sha256).hexdigest()


def _tg_user(init_data: str) -> dict:
    """Пользователь Telegram из подписанных данных окна бота. Этот вход работает только из него."""
    token = tg_webapp.bot_token()
    tg_user = tg_webapp.parse_init_data(init_data, token) if token else None
    if tg_user is None:
        raise HTTPException(status_code=400, detail="Откройте кабинет из бота заново")
    return tg_user


def _redis():
    try:
        r = _get_redis()
        r.ping()
        return r
    except Exception as e:  # noqa: BLE001 — без Redis этот вход не работает, остаётся вход формой
        logger.warning(f"tg-login: Redis недоступен: {e}")
        raise HTTPException(status_code=503, detail="Вход через браузер временно недоступен")


@router.post("/start")
def start(data: StartRequest):
    """Попытку начинает только окно бота (подпись initData). Запоминаем, чей это Telegram: страница
    подтверждения покажет его человеку, а сессию потом получит только он. Входом это не служит —
    сессию даёт сайт, когда вошедший человек нажмёт «Подтвердить»."""
    tg_user = _tg_user(data.init_data)
    # Ник — как есть (в Telegram он уникален). Имя — свободный текст: убираем «@», чтобы оно не
    # выдавало себя за ник; страница показывает их по-разному.
    who = {
        "username": (tg_user.get("username") or None),
        "name": " ".join(x for x in (tg_user.get("first_name"), tg_user.get("last_name")) if x).replace("@", "").strip()[:60] or None,
    }
    got = browser_login.start(_redis(), tg_user["id"], who)
    if got is None:
        raise HTTPException(status_code=429, detail="Слишком много попыток. Подождите 10 минут")
    return got


@router.get("/info")
def info(code: str, user: User = Depends(get_current_user)):
    if not (16 <= len(code) <= 64):
        raise HTTPException(status_code=404, detail="Ссылка устарела")
    got = browser_login.info(_redis(), code)
    if got is None:
        raise HTTPException(status_code=404, detail="Ссылка устарела")
    return got


@router.post("/approve")
def approve(data: CodeRequest, user: User = Depends(get_current_user)):
    ticket = browser_login.approve(_redis(), data.code, user.id)
    if ticket is None:
        raise HTTPException(status_code=404, detail="Ссылка устарела")
    logger.info(f"tg-login: user {user.id} подтвердил вход в окне бота")
    # Билет видит только подтвердивший: он зашит в его кнопку возврата в Telegram
    bot = os.environ.get("ALERT_BOT_USERNAME", "framesignalbot")
    return {"ok": True, "return_url": f"https://t.me/{bot}?start={browser_login.TICKET_PREFIX}{ticket}"}


@router.post("/confirm")
def confirm(data: ConfirmRequest, db: Session = Depends(get_db)):
    """Бот получил /start in_<билет> из чата chat_id. Если билет выдан попытке этого же Telegram —
    окно сможет забрать сессию, а чат сразу привязываем к аккаунту для сигналов."""
    token = tg_webapp.bot_token()
    if not token or not hmac.compare_digest(confirm_sig(token, data.ticket, data.chat_id), data.sig):
        raise HTTPException(status_code=403, detail="Нет доступа")
    user_id = browser_login.confirm(_redis(), data.ticket, data.chat_id)
    user = db.query(User).filter(User.id == user_id).first() if user_id else None
    if not user or not user.is_active:
        return {"ok": False}
    link_chat(db, user, data.chat_id, data.username)
    return {"ok": True}


@router.post("/poll")
def poll(data: PollRequest, request: Request, db: Session = Depends(get_db)):
    state, user_id = browser_login.take(_redis(), data.secret, _tg_user(data.init_data)["id"])
    if state != "approved":
        return {"status": state}
    user = db.query(User).filter(User.id == user_id).first()
    if not user or not user.is_active:
        return {"status": "expired"}
    tokens = create_token_pair(user.id, user.role)
    persist_refresh_token(db, user.id, tokens.refresh_token, request)
    db.commit()
    return {"status": "approved", "access_token": tokens.access_token, "refresh_token": tokens.refresh_token}
