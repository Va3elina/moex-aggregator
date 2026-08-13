"""
Синхронизация настроек юзера между устройствами.

Endpoints:
  GET /api/settings — снапшот настроек юзера (data=null, если ещё не сохранял).
  PUT /api/settings — полная замена снапшота (last-write-wins целиком блобом).

data — плоский словарь {localStorage-ключ: сырая строка значения}. Какие ключи
отслеживать, решает фронт (services/settingsSync.ts): настройки индикаторов
frame:*, раскладка песочницы, панели embed, избранное, тема. Бэкенд хранит
блоб как есть и не интерпретирует ключи — лимиты ниже только против мусора
(раскладка песочницы с рисовалками укладывается в лимит с большим запасом).
"""
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.models.user_settings import UserSettings
from api.routers.auth import get_current_user

router = APIRouter(prefix="/api/settings", tags=["settings"])

# 512 КБ сериализованного JSON — потолок блоба; типичный снапшот — единицы КБ.
MAX_BYTES = 512 * 1024
MAX_KEYS = 1000
MAX_KEY_LEN = 200


class SettingsPutRequest(BaseModel):
    data: dict[str, str]


@router.get("")
def get_settings(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = db.get(UserSettings, user.id)
    if row is None:
        return {"data": None, "updated_at": None}
    return {"data": row.data, "updated_at": row.updated_at}


@router.put("")
def put_settings(
    body: SettingsPutRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if len(body.data) > MAX_KEYS:
        raise HTTPException(status_code=400, detail=f"Слишком много ключей (лимит {MAX_KEYS})")
    if any(len(k) > MAX_KEY_LEN for k in body.data):
        raise HTTPException(status_code=400, detail="Слишком длинный ключ настройки")
    size = len(json.dumps(body.data, ensure_ascii=False).encode("utf-8"))
    if size > MAX_BYTES:
        raise HTTPException(status_code=413, detail="Снапшот настроек слишком большой")

    now = datetime.now(timezone.utc)
    row = db.get(UserSettings, user.id)
    if row is None:
        row = UserSettings(user_id=user.id, data=body.data, updated_at=now)
        db.add(row)
    else:
        row.data = body.data
        row.updated_at = now
    db.commit()
    return {"updated_at": now}
