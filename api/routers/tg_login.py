"""
/api/tg-login/* — вход в кабинет бота @framesignalbot через обычный браузер (api/services/browser_login.py).

  POST /start    — окно бота берёт одноразовый код (без входа)
  GET  /info     — страница подтверждения: жив ли код (нужен вход на сайте)
  POST /approve  — вошедший человек разрешает окну войти под собой
  POST /poll     — окно забирает сессию после подтверждения (один раз)

Префикс не /api/auth: там лимит 10 запросов в минуту, а окно опрашивает /poll раз в несколько секунд.
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.cache import _get_redis
from api.database import get_db
from api.logger import get_logger
from api.models import User
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
    if not browser_login.approve(_redis(), data.code, user.id):
        raise HTTPException(status_code=404, detail="Ссылка устарела")
    logger.info(f"tg-login: user {user.id} подтвердил вход в окне бота")
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
