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
from datetime import datetime, timezone
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

YANDEX_CLIENT_ID = os.getenv("YANDEX_CLIENT_ID", "")
YANDEX_CLIENT_SECRET = os.getenv("YANDEX_CLIENT_SECRET", "")
YANDEX_REDIRECT_URI = os.getenv("YANDEX_REDIRECT_URI", "")


# ============================================================================
# СХЕМЫ
# ============================================================================

class OAuthCodeRequest(BaseModel):
    """Запрос с authorization code от провайдера."""
    code: str
    device_id: Optional[str] = None  # VK ID requires device_id
    code_verifier: Optional[str] = None  # VK ID PKCE
    state: Optional[str] = None  # CSRF-токен (Yandex): фронт возвращает то, что выдал /url


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
    code_verifier: Optional[str] = None  # VK PKCE: фронт сохраняет и отправляет при обмене
    device_id: Optional[str] = None  # VK ID: фронт сохраняет и отправляет при обмене
    state: Optional[str] = None  # Yandex CSRF: фронт сохраняет, сверяет с echo и шлёт на callback


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
    display_name: Optional[str] = None,
    allow_registration: bool = True,
) -> tuple[User, bool]:
    """
    Ищет пользователя по OAuth провайдеру+ID.
    Если не найден и allow_registration=True — создаёт нового.
    Если не найден и allow_registration=False — 403 (провайдер не создаёт новых юзеров).

    Returns:
        (user, is_new) — пользователь и флаг "новый ли"
    """
    # 1. Ищем по oauth_provider + oauth_id
    user = db.query(User).filter(
        User.oauth_provider == provider,
        User.oauth_id == oauth_id,
    ).first()

    if user:
        # Обновляем аватар и display_name если изменились
        changed = False
        if avatar_url and user.avatar_url != avatar_url:
            user.avatar_url = avatar_url
            changed = True
        if display_name and user.display_name != display_name:
            user.display_name = display_name
            changed = True
        if changed:
            db.commit()
        return user, False

    # 2. Если есть email — проверяем, может уже зареган через email+password
    #    НЕ привязываем OAuth автоматически к чужому аккаунту — это позволило бы
    #    захватить аккаунт через подмену email в OAuth провайдере.
    #    Вместо этого создаём отдельный OAuth-аккаунт.
    if email:
        existing = db.query(User).filter(User.email == email).first()
        if existing:
            # Аккаунт с таким email уже есть, но это может быть другой человек.
            # Если у существующего аккаунта есть пароль (не OAuth) — не привязываем.
            if existing.hashed_password:
                # Создаём OAuth-пользователя с уникальным email
                email = f"{provider}_{oauth_id}@oauth.local"
            else:
                # Аккаунт без пароля (уже OAuth) — безопасно привязать
                existing.oauth_provider = provider
                existing.oauth_id = oauth_id
                if avatar_url:
                    existing.avatar_url = avatar_url
                if display_name:
                    existing.display_name = display_name
                db.commit()
                return existing, False

    # 3. Создаём нового пользователя.
    if not allow_registration:
        raise HTTPException(
            status_code=403,
            detail=f"Регистрация через {provider.capitalize()} отключена. Зарегистрируйтесь через email или другой способ входа.",
        )

    # is_verified=True ТОЛЬКО если провайдер дал реальный email (Google/Yandex/
    # VK-с-email) — он уже подтверждён провайдером, код слать не нужно.
    # Нет email (Telegram всегда, VK часто) → synthetic @oauth.local →
    # is_verified=False: юзер обязан добавить и подтвердить реальный email
    # (через /add-email + код из письма) прежде чем сможет оплатить.
    email_final = email or f"{provider}_{oauth_id}@oauth.local"
    has_real_email = not email_final.endswith("@oauth.local")
    new_user = User(
        email=email_final,
        oauth_provider=provider,
        oauth_id=oauth_id,
        avatar_url=avatar_url,
        display_name=display_name,
        is_verified=has_real_email,
        is_active=True,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    log.info(f"Новый OAuth пользователь: {provider}/{oauth_id} ({email})")
    return new_user, True


def _make_token_response(user: User, is_new: bool, db=None) -> dict:
    """Формирует ответ с JWT токенами.

    db передаётся чтобы персистить refresh-токен при выдаче — иначе logout не
    может его отозвать и токен реиграется 7 дней (аудит 10.06)."""
    token_pair = create_token_pair(user_id=user.id, role=user.role or "user")
    if db is not None:
        from api.routers.auth import persist_refresh_token
        # OAuth-вход — тоже вход: без этой строки last_login_at заполнялся
        # только у парольных юзеров (у OAuth-аккаунтов вечный NULL).
        user.last_login_at = datetime.now(timezone.utc)
        persist_refresh_token(db, user.id, token_pair.refresh_token)
        db.commit()
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


# ── OAuth state (CSRF) для Yandex ───────────────────────────────────────────
# Yandex (в отличие от VK) не использует PKCE → нужен state против login-CSRF /
# account-fixation. Двойная защита: (1) фронт сохраняет выданный state и сверяет
# с тем, что Yandex вернул в redirect — привязка к браузеру, который начал вход;
# (2) сервер кладёт state в Redis (TTL 10мин, one-time) — отсекает подделку и
# повтор. Redis-зависимость МЯГКАЯ: если Redis недоступен на callback, вход НЕ
# падает (клиентская привязка уже защищает от CSRF) — только логируем.
_STATE_TTL = 600


def _issue_oauth_state(provider: str) -> str:
    import secrets
    from api.cache import set_cache
    state = secrets.token_urlsafe(32)
    try:
        set_cache(f"oauth:state:{provider}:{state}", "1", ttl=_STATE_TTL)
    except Exception as e:  # Redis недоступен — деградируем до клиентской привязки
        log.warning(f"oauth state: не удалось сохранить в Redis: {e}")
    return state


def _consume_oauth_state(provider: str, state: Optional[str]) -> None:
    """Сверяет и гасит one-time state. 400 если отсутствует/неизвестен.
    Если Redis недоступен — пропускаем (клиентская привязка на фронте уже сверила)."""
    if not state:
        raise HTTPException(400, "Отсутствует CSRF-токен (state)")
    try:
        from api.cache import _get_redis
        r = _get_redis()
        key = f"oauth:state:{provider}:{state}"
        if r.delete(key) == 0:
            raise HTTPException(400, "Недействительный или истёкший CSRF-токен")
    except HTTPException:
        raise
    except Exception as e:
        # Redis-сбой ≠ ошибка авторизации: не валим вход, фронт уже сверил state.
        log.warning(f"oauth state: Redis недоступен на проверке, пропускаю: {e}")


# ============================================================================
# ENDPOINTS — URL для редиректа
# ============================================================================

@router.get("/google/url")
async def google_auth_url():
    """Google-вход отключён (убран из провайдеров). Закрыто, чтобы заряженный
    но неиспользуемый Authorization-Code-Flow без state не оставался доступным."""
    raise HTTPException(status_code=410, detail="Вход через Google отключён")


@router.get("/vk/url")
async def vk_auth_url():
    """Получить URL для авторизации через VK ID (OAuth 2.1 + PKCE)."""
    _check_configured("VK", VK_CLIENT_ID, VK_REDIRECT_URI)

    import secrets
    import base64
    state = secrets.token_urlsafe(16)

    # PKCE: генерируем code_verifier и code_challenge
    code_verifier = secrets.token_urlsafe(64)  # 43-128 символов
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).rstrip(b"=").decode()

    # VK ID требует uuid (device_id)
    device_id = secrets.token_hex(16)

    from urllib.parse import quote
    url = (
        "https://id.vk.com/authorize?"
        f"client_id={VK_CLIENT_ID}"
        f"&redirect_uri={quote(VK_REDIRECT_URI, safe='')}"
        "&response_type=code"
        f"&state={state}"
        f"&code_challenge={code_challenge}"
        "&code_challenge_method=s256"
        f"&uuid={device_id}"
    )
    return OAuthURLResponse(
        url=url,
        provider="vk",
        code_verifier=code_verifier,
        device_id=device_id,
    )


@router.get("/yandex/url")
async def yandex_auth_url():
    """Получить URL для авторизации через Яндекс (с CSRF-state)."""
    _check_configured("Yandex", YANDEX_CLIENT_ID, YANDEX_REDIRECT_URI)

    state = _issue_oauth_state("yandex")
    url = (
        "https://oauth.yandex.ru/authorize?"
        f"client_id={YANDEX_CLIENT_ID}"
        f"&redirect_uri={YANDEX_REDIRECT_URI}"
        "&response_type=code"
        f"&state={state}"
        "&force_confirm=yes"
    )
    return OAuthURLResponse(url=url, provider="yandex", state=state)


# ============================================================================
# ENDPOINTS — Обмен code на токены
# ============================================================================

@router.post("/google")
async def google_oauth_callback(data: OAuthCodeRequest, db: Session = Depends(get_db)):
    """Google-вход отключён. Эндпоинт оставлен, чтобы старые редиректы не падали
    500, но обмен кода больше не выполняется (раньше — без state-проверки)."""
    raise HTTPException(status_code=410, detail="Вход через Google отключён")


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
        # 1. Обмениваем code на access_token (VK ID OAuth 2.1 + PKCE)
        token_payload = {
            "grant_type": "authorization_code",
            "code": data.code,
            "client_id": VK_CLIENT_ID,
            "redirect_uri": VK_REDIRECT_URI,
            "device_id": data.device_id or "",
            "state": "",
        }
        if data.code_verifier:
            token_payload["code_verifier"] = data.code_verifier

        async with httpx.AsyncClient() as client:
            token_resp = await client.post(
                "https://id.vk.com/oauth2/auth",
                data=token_payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

        token_data = token_resp.json()
        log.info(f"VK token response keys: {list(token_data.keys())}")

        if "error" in token_data:
            log.error(f"VK token error: {token_data}")
            raise HTTPException(400, f"VK: {token_data.get('error_description', token_data.get('error'))}")

        access_token = token_data.get("access_token")
        vk_user_id = token_data.get("user_id")
        email = token_data.get("email")

        if not access_token:
            log.error(f"VK: no access_token in response: {token_data}")
            raise HTTPException(400, "VK: не получен access_token")

        # 2. Получаем профиль через VK ID user_info
        async with httpx.AsyncClient() as client:
            profile_resp = await client.post(
                "https://id.vk.com/oauth2/user_info",
                data={
                    "access_token": access_token,
                    "client_id": VK_CLIENT_ID,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

        profile = profile_resp.json().get("user", profile_resp.json())

        avatar = profile.get("avatar")
        first = profile.get("first_name", "")
        last = profile.get("last_name", "")
        display_name = f"{first} {last}".strip() or None
        if not email:
            email = profile.get("email")

        # 3. Создаём/находим пользователя
        user, is_new = _find_or_create_oauth_user(
            db=db,
            provider="vk",
            oauth_id=str(vk_user_id or profile.get("user_id") or profile.get("id")),
            email=email,
            avatar_url=avatar,
            display_name=display_name,
        )

        return _make_token_response(user, is_new, db)

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"VK OAuth error: {e}", exc_info=True)
        raise HTTPException(500, "Ошибка авторизации через VK")


@router.post("/yandex")
async def yandex_oauth_callback(
    data: OAuthCodeRequest,
    db: Session = Depends(get_db),
):
    """
    Обмен Yandex authorization code на JWT.
    """
    _check_configured("Yandex", YANDEX_CLIENT_ID, YANDEX_CLIENT_SECRET)

    # 0. CSRF: сверяем и гасим state ДО обмена кода (raise 400 при невалидном).
    _consume_oauth_state("yandex", data.state)

    try:
        # 1. Обмениваем code на access_token
        async with httpx.AsyncClient() as client:
            token_resp = await client.post(
                "https://oauth.yandex.ru/token",
                data={
                    "code": data.code,
                    "client_id": YANDEX_CLIENT_ID,
                    "client_secret": YANDEX_CLIENT_SECRET,
                    "grant_type": "authorization_code",
                },
            )

        if token_resp.status_code != 200:
            log.error(f"Yandex token error: {token_resp.text}")
            raise HTTPException(400, "Не удалось получить токен от Яндекс")

        token_data = token_resp.json()
        access_token = token_data.get("access_token")

        # 2. Получаем профиль
        async with httpx.AsyncClient() as client:
            profile_resp = await client.get(
                "https://login.yandex.ru/info",
                headers={"Authorization": f"OAuth {access_token}"},
                params={"format": "json"},
            )

        if profile_resp.status_code != 200:
            raise HTTPException(400, "Не удалось получить профиль от Яндекс")

        profile = profile_resp.json()

        avatar_id = profile.get("default_avatar_id")
        avatar_url = (
            f"https://avatars.yandex.net/get-yapic/{avatar_id}/islands-200"
            if avatar_id and avatar_id != "0/0-0"
            else None
        )

        # 3. Создаём/находим пользователя
        user, is_new = _find_or_create_oauth_user(
            db=db,
            provider="yandex",
            oauth_id=str(profile["id"]),
            email=profile.get("default_email"),
            avatar_url=avatar_url,
            display_name=profile.get("display_name"),
        )

        return _make_token_response(user, is_new, db)

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Yandex OAuth error: {e}", exc_info=True)
        raise HTTPException(500, "Ошибка авторизации через Яндекс")


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

        # compare_digest — constant-time сравнение HMAC (без утечки через тайминг).
        if not hmac.compare_digest(expected_hash, data.hash):
            log.warning(f"Telegram auth: invalid hash for user {data.id}")
            raise HTTPException(400, "Невалидная подпись Telegram")

        # 3. Создаём/находим пользователя
        tg_display = f"@{data.username}" if data.username else data.first_name
        user, is_new = _find_or_create_oauth_user(
            db=db,
            provider="telegram",
            oauth_id=str(data.id),
            email=None,  # Telegram не даёт email
            avatar_url=data.photo_url,
            display_name=tg_display,
            allow_registration=False,  # новую регистрацию через Telegram отключили — только вход уже привязанным
        )

        return _make_token_response(user, is_new, db)

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
                "id": "yandex",
                "name": "Яндекс",
                "icon": "yandex",
                "configured": bool(YANDEX_CLIENT_ID and YANDEX_CLIENT_SECRET),
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
