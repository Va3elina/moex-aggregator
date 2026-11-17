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
    init_data: str | None = Field(default=None, max_length=4096)


class CodeRequest(BaseModel):
    code: str = Field(min_length=16, max_length=64, pattern=_CODE)


class PollRequest(BaseModel):
    secret: str = Field(min_length=32, max_length=128, pattern=_CODE)


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
    """Имя из Telegram (если подпись верна) покажем на странице подтверждения: человек видит,
    В ЧЬЁМ Telegram откроется его кабинет — защита от чужой ссылки. Для входа оно не используется."""
    who = None
    token = tg_webapp.bot_token()
    tg_user = tg_webapp.parse_init_data(data.init_data or "", token) if token else None
    if tg_user:
        name = " ".join(x for x in (tg_user.get("first_name"), tg_user.get("last_name")) if x)
        who = (f"@{tg_user['username']}" if tg_user.get("username") else name)[:80] or None
    return browser_login.start(_redis(), who)


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
    state, user_id = browser_login.take(_redis(), data.secret)
    if state != "approved":
        return {"status": state}
    user = db.query(User).filter(User.id == user_id).first()
    if not user or not user.is_active:
        return {"status": "expired"}
    tokens = create_token_pair(user.id, user.role)
    persist_refresh_token(db, user.id, tokens.refresh_token, request)
    db.commit()
    return {"status": "approved", "access_token": tokens.access_token, "refresh_token": tokens.refresh_token}
