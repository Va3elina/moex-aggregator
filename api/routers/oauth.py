# api/routers/oauth.py
"""
OAuth аутентификация через внешние провайдеры.

ПРОВАЙДЕРЫ:
  - Google   — OAuth 2.0 (Authorization Code Flow)
  - VK       — VK ID OAuth 2.0
  - Telegram — Login Widget (hash-based verification)

FLOW:
  1. Frontend редиректит пользователя на провайдера
  2. Провайдер возвращает code/data на callback URL
  3. Frontend отправляет code/data на этот API
  4. API обменивает code на токен, получает профиль
  5. Находит или создаёт пользователя → возвращает JWT

НАСТРОЙКА:
  Добавить в .env:
    GOOGLE_CLIENT_ID=...
    GOOGLE_CLIENT_SECRET=...
    VK_CLIENT_ID=...
    VK_CLIENT_SECRET=...
    TELEGRAM_BOT_TOKEN=...
"""

import os
import hashlib
import hmac
import time
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.database import get_db
from api.models.user import User
from api.security import create_token_pair
from api.logger import get_logger

log = get_logger()

router = APIRouter(prefix="/auth/oauth", tags=["OAuth"])


# ============================================================================
# НАСТРОЙКИ
# ============================================================================

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "")

VK_CLIENT_ID = os.getenv("VK_CLIENT_ID", "")
VK_CLIENT_SECRET = os.getenv("VK_CLIENT_SECRET", "")
VK_REDIRECT_URI = os.getenv("VK_REDIRECT_URI", "")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")


# ============================================================================
# СХЕМЫ
# ============================================================================

class OAuthCodeRequest(BaseModel):
    """Запрос с authorization code от провайдера."""
    code: str


class TelegramAuthRequest(BaseModel):
    """Данные от Telegram Login Widget."""
    id: int
    first_name: str
    last_name: Optional[str] = None
    username: Optional[str] = None
    photo_url: Optional[str] = None
    auth_date: int
    hash: str


class OAuthURLResponse(BaseModel):
    """URL для редиректа на провайдера."""
    url: str
    provider: str


class OAuthTokenResponse(BaseModel):
    """Ответ с JWT токенами после OAuth."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    is_new_user: bool
    user: dict


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def _find_or_create_oauth_user(
    db: Session,
    provider: str,
    oauth_id: str,
    email: Optional[str],
    avatar_url: Optional[str],
) -> tuple[User, bool]:
    """
    Ищет пользователя по OAuth провайдеру+ID.
    Если не найден — создаёт нового.

    Returns:
        (user, is_new) — пользователь и флаг "новый ли"
    """
    # 1. Ищем по oauth_provider + oauth_id
    user = db.query(User).filter(
        User.oauth_provider == provider,
        User.oauth_id == oauth_id,
    ).first()

    if user:
        # Обновляем аватар если изменился
        if avatar_url and user.avatar_url != avatar_url:
            user.avatar_url = avatar_url
            db.commit()
        return user, False

    # 2. Если есть email — проверяем, может уже зареган через email+password
    if email:
        existing = db.query(User).filter(User.email == email).first()
        if existing:
            # Привязываем OAuth к существующему аккаунту
            existing.oauth_provider = provider
            existing.oauth_id = oauth_id
            if avatar_url:
                existing.avatar_url = avatar_url
            db.commit()
            return existing, False

    # 3. Создаём нового пользователя
    new_user = User(
        email=email or f"{provider}_{oauth_id}@oauth.local",
        oauth_provider=provider,
        oauth_id=oauth_id,
        avatar_url=avatar_url,
        is_verified=True,  # OAuth пользователи уже верифицированы
        is_active=True,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    log.info(f"Новый OAuth пользователь: {provider}/{oauth_id} ({email})")
    return new_user, True


def _make_token_response(user: User, is_new: bool) -> dict:
    """Формирует ответ с JWT токенами."""
    token_pair = create_token_pair(user_id=user.id, role=user.role or "user")
    return {
        "access_token": token_pair.access_token,
        "refresh_token": token_pair.refresh_token,
        "token_type": "bearer",
        "expires_in": token_pair.expires_in,
        "is_new_user": is_new,
        "user": {
            "id": user.id,
            "email": user.email,
            "avatar_url": user.avatar_url,
            "role": user.role,
        },
    }


def _check_configured(provider: str, *keys: str):
    """Проверяет, настроен ли провайдер."""
    for key in keys:
        if not key:
            raise HTTPException(
                status_code=501,
                detail=f"OAuth через {provider} пока не настроен. Добавьте ключи в .env",
            )


# ============================================================================
# ENDPOINTS — URL для редиректа
# ============================================================================

@router.get("/google/url")
async def google_auth_url():
    """Получить URL для авторизации через Google."""
    _check_configured("Google", GOOGLE_CLIENT_ID, GOOGLE_REDIRECT_URI)

    url = (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        f"client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={GOOGLE_REDIRECT_URI}"
        "&response_type=code"
        "&scope=openid%20email%20profile"
        "&access_type=offline"
        "&prompt=consent"
    )
    return OAuthURLResponse(url=url, provider="google")


@router.get("/vk/url")
async def vk_auth_url():
    """Получить URL для авторизации через VK."""
    _check_configured("VK", VK_CLIENT_ID, VK_REDIRECT_URI)

    url = (
        "https://id.vk.com/authorize?"
        f"client_id={VK_CLIENT_ID}"
        f"&redirect_uri={VK_REDIRECT_URI}"
        "&response_type=code"
        "&scope=email"
        "&display=popup"
    )
    return OAuthURLResponse(url=url, provider="vk")


# ============================================================================
# ENDPOINTS — Обмен code на токены
# ============================================================================

@router.post("/google")
async def google_oauth_callback(
    data: OAuthCodeRequest,
    db: Session = Depends(get_db),
):
    """
    Обмен Google authorization code на JWT.

    Frontend получает code после редиректа от Google
    и отправляет его сюда.
    """
    _check_configured("Google", GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET)

    try:
        # 1. Обмениваем code на access_token
        async with httpx.AsyncClient() as client:
            token_resp = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": data.code,
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "redirect_uri": GOOGLE_REDIRECT_URI,
                    "grant_type": "authorization_code",
                },
            )

        if token_resp.status_code != 200:
            log.error(f"Google token error: {token_resp.text}")
            raise HTTPException(400, "Не удалось получить токен от Google")

        token_data = token_resp.json()
        access_token = token_data.get("access_token")

        # 2. Получаем профиль пользователя
        async with httpx.AsyncClient() as client:
            profile_resp = await client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {access_token}"},
            )

        if profile_resp.status_code != 200:
            raise HTTPException(400, "Не удалось получить профиль от Google")

        profile = profile_resp.json()

        # 3. Создаём/находим пользователя
        user, is_new = _find_or_create_oauth_user(
            db=db,
            provider="google",
            oauth_id=str(profile["id"]),
            email=profile.get("email"),
            avatar_url=profile.get("picture"),
        )

        return _make_token_response(user, is_new)

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Google OAuth error: {e}", exc_info=True)
        raise HTTPException(500, "Ошибка авторизации через Google")


@router.post("/vk")
async def vk_oauth_callback(
    data: OAuthCodeRequest,
    db: Session = Depends(get_db),
):
    """
    Обмен VK authorization code на JWT.
    """
    _check_configured("VK", VK_CLIENT_ID, VK_CLIENT_SECRET)

    try:
        # 1. Обмениваем code на access_token
        async with httpx.AsyncClient() as client:
            token_resp = await client.post(
                "https://id.vk.com/oauth2/auth",
                data={
                    "code": data.code,
                    "client_id": VK_CLIENT_ID,
                    "client_secret": VK_CLIENT_SECRET,
                    "redirect_uri": VK_REDIRECT_URI,
                    "grant_type": "authorization_code",
                },
            )

        if token_resp.status_code != 200:
            log.error(f"VK token error: {token_resp.text}")
            raise HTTPException(400, "Не удалось получить токен от VK")

        token_data = token_resp.json()
        access_token = token_data.get("access_token")
        vk_user_id = token_data.get("user_id")
        email = token_data.get("email")

        # 2. Получаем профиль
        async with httpx.AsyncClient() as client:
            profile_resp = await client.get(
                "https://api.vk.com/method/users.get",
                params={
                    "access_token": access_token,
                    "fields": "photo_200,screen_name",
                    "v": "5.131",
                },
            )

        profile = profile_resp.json().get("response", [{}])[0]

        avatar = profile.get("photo_200")

        # 3. Создаём/находим пользователя
        user, is_new = _find_or_create_oauth_user(
            db=db,
            provider="vk",
            oauth_id=str(vk_user_id or profile.get("id")),
            email=email,
            avatar_url=avatar,
        )

        return _make_token_response(user, is_new)

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"VK OAuth error: {e}", exc_info=True)
        raise HTTPException(500, "Ошибка авторизации через VK")


@router.post("/telegram")
async def telegram_oauth_callback(
    data: TelegramAuthRequest,
    db: Session = Depends(get_db),
):
    """
    Верификация данных от Telegram Login Widget.

    Telegram не использует OAuth. Вместо этого:
    1. Виджет на фронте собирает данные пользователя
    2. Подписывает их HMAC-SHA256 с хэшем bot token
    3. Мы проверяем подпись
    """
    _check_configured("Telegram", TELEGRAM_BOT_TOKEN)

    try:
        # 1. Проверяем auth_date (не старше 1 дня)
        if time.time() - data.auth_date > 86400:
            raise HTTPException(400, "Данные авторизации устарели")

        # 2. Проверяем подпись
        secret_key = hashlib.sha256(TELEGRAM_BOT_TOKEN.encode()).digest()

        check_data = {
            "id": data.id,
            "first_name": data.first_name,
        }
        if data.last_name:
            check_data["last_name"] = data.last_name
        if data.username:
            check_data["username"] = data.username
        if data.photo_url:
            check_data["photo_url"] = data.photo_url
        check_data["auth_date"] = data.auth_date

        data_check_string = "\n".join(
            f"{k}={v}" for k, v in sorted(check_data.items())
        )

        expected_hash = hmac.new(
            secret_key,
            data_check_string.encode(),
            hashlib.sha256,
        ).hexdigest()

        if expected_hash != data.hash:
            log.warning(f"Telegram auth: invalid hash for user {data.id}")
            raise HTTPException(400, "Невалидная подпись Telegram")

        # 3. Создаём/находим пользователя
        user, is_new = _find_or_create_oauth_user(
            db=db,
            provider="telegram",
            oauth_id=str(data.id),
            email=None,  # Telegram не даёт email
            avatar_url=data.photo_url,
        )

        return _make_token_response(user, is_new)

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Telegram OAuth error: {e}", exc_info=True)
        raise HTTPException(500, "Ошибка авторизации через Telegram")


# ============================================================================
# ИНФОРМАЦИЯ О ПРОВАЙДЕРАХ
# ============================================================================

@router.get("/providers")
async def get_oauth_providers():
    """
    Список доступных OAuth провайдеров и их статус.
    Frontend использует это чтобы показывать/скрывать кнопки.
    """
    return {
        "providers": [
            {
                "id": "google",
                "name": "Google",
                "icon": "google",
                "configured": bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET),
            },
            {
                "id": "vk",
                "name": "ВКонтакте",
                "icon": "vk",
                "configured": bool(VK_CLIENT_ID and VK_CLIENT_SECRET),
            },
            {
                "id": "telegram",
                "name": "Telegram",
                "icon": "telegram",
                "configured": bool(TELEGRAM_BOT_TOKEN),
            },
        ]
    }
