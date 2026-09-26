"""
Ранний доступ к тестовым индикаторам для отдельных пользователей.

Тестовые индикаторы (пункты под «+» в шапке, страницы /admin/*, их API) видят
админы. Иногда индикатор надо показать одному-двум людям снаружи до релиза,
не давая им роль admin. Список — в .env, по фиче:

    REPAINT_EARLY_ACCESS=a@b.ru,c@d.ru

Переменную нужно прокинуть в environment сервиса api в docker-compose.yml
(из .env сама не долетает) и пересоздать api. Админы проходят всегда.
Профиль (/api/auth/me) отдаёт early_access: ["repaint", ...] — по нему фронт
показывает пункт в шапке и пускает на страницу.
"""
import os

from fastapi import Depends, HTTPException, status

from api.models import User
from api.routers.auth import get_current_user

# фича → переменная окружения со списком email через запятую
_FEATURE_ENV = {
    "repaint": "REPAINT_EARLY_ACCESS",
}


def _emails(feature: str) -> set[str]:
    raw = os.getenv(_FEATURE_ENV.get(feature, ""), "") or ""
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


def has_feature(user: User | None, feature: str) -> bool:
    if user is None:
        return False
    if user.role == "admin":
        return True
    return (user.email or "").lower() in _emails(feature)


def features_for(user: User | None) -> list[str]:
    """Фичи раннего доступа пользователя — для профиля. Админам список не нужен:
    фронт и так пускает их по роли."""
    if user is None or user.role == "admin":
        return []
    return [f for f in _FEATURE_ENV if has_feature(user, f)]


def require_feature(feature: str):
    """Dependency: админ или email из списка раннего доступа фичи."""
    async def _dep(user: User = Depends(get_current_user)) -> User:
        if not has_feature(user, feature):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                                detail="Требуются права администратора")
        return user
    return _dep
