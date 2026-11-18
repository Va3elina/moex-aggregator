"""
Синхронизация настроек юзера между устройствами.

Endpoints:
  GET /api/settings — снапшот настроек юзера (data=null, если ещё не сохранял).
  PUT /api/settings — полная замена снапшота (last-write-wins целиком блобом).
  GET/PUT /api/settings/signals — настройки сигналов бота (мини-приложение /tg).

Ключи «srv:*» — серверные: их пишут только свои ручки (сейчас /signals), синхронизация фронта их не видит (GET их не
отдаёт) и не может затереть (PUT их сохраняет). Иначе вкладка сайта со старым снимком снесла бы настройки, сделанные
в Telegram (ревью PR #1840).

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
# Ключей бывает много: у песочницы настройки неймспейсятся по каждой панели
# (frame:embed:<инд>:p:<pid>:*). Лимит 1000 у активного юзера выбивался —
# PUT молча получал 400, сервер замерзал, а pull при загрузке откатывал
# свежие тумблеры к старому снимку. Главный потолок — MAX_BYTES.
MAX_BYTES = 512 * 1024
MAX_KEYS = 5000
MAX_KEY_LEN = 200


SERVER_PREFIX = "srv:"
SIGNALS_KEY = "srv:signals:prefs"     # читает бот: signals/signal_prefs.py (PREFS_KEY)
SIGNALS_MAX_BYTES = 32 * 1024


class SettingsPutRequest(BaseModel):
    data: dict[str, str]


def _client_view(data: dict | None) -> dict | None:
    return None if data is None else {k: v for k, v in data.items() if not k.startswith(SERVER_PREFIX)}


@router.get("")
def get_settings(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = db.get(UserSettings, user.id)
    if row is None:
        return {"data": None, "updated_at": None}
    return {"data": _client_view(row.data), "updated_at": row.updated_at}


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
        keep = {k: v for k, v in (row.data or {}).items() if k.startswith(SERVER_PREFIX)}
        row.data = {**{k: v for k, v in body.data.items() if not k.startswith(SERVER_PREFIX)}, **keep}
        row.updated_at = now
    db.commit()
    return {"updated_at": now}


class SignalPrefsBody(BaseModel):
    prefs: dict


@router.get("/signals")
def get_signal_prefs(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Настройки сигналов бота; {} — не настраивал (приходит всё)."""
    row = db.get(UserSettings, user.id)
    raw = (row.data or {}).get(SIGNALS_KEY) if row else None
    try:
        return {"prefs": json.loads(raw) if raw else {}}
    except ValueError:
        return {"prefs": {}}


@router.put("/signals")
def put_signal_prefs(body: SignalPrefsBody, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Сохранить настройки сигналов целиком (один ключ блоба; остальные настройки юзера не трогаются)."""
    raw = json.dumps(body.prefs, ensure_ascii=False)
    if len(raw.encode("utf-8")) > SIGNALS_MAX_BYTES:
        raise HTTPException(status_code=413, detail="Настройки сигналов слишком большие")
    now = datetime.now(timezone.utc)
    row = db.get(UserSettings, user.id)
    if row is None:
        db.add(UserSettings(user_id=user.id, data={SIGNALS_KEY: raw}, updated_at=now))
    else:
        row.data = {**(row.data or {}), SIGNALS_KEY: raw}      # новый dict — JSONB замечает изменение
        row.updated_at = now
    db.commit()
    return {"prefs": body.prefs}
