"""
Watchlist API — личный список избранных тикеров для каждого юзера.

Endpoints:
  GET    /api/watchlist            — список тикеров (с метаданными актива)
  POST   /api/watchlist            — добавить тикер (body: {"ticker": "SBER"})
  DELETE /api/watchlist/{ticker}   — убрать тикер
  PATCH  /api/watchlist/reorder    — переупорядочить (body: ["SBER","GAZP",...])

Все требуют auth (get_current_user). Если юзер не залогинен → 401.

Гейтинг по тарифу НЕ применяется на самом watchlist — юзер может
добавить любой тикер. Гейт срабатывает при click-through на индикатор
(там работает useTierAccess как для всех остальных мест).
"""
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, delete, func
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User, UserWatchlist, Instrument
from api.routers.auth import get_current_user
from api.schemas.validators import validate_safe_id

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])

# Лимит на размер watchlist (защита от abuse + UX — больше 50 не вмещается
# в нормальный widget на overview без скролла).
MAX_WATCHLIST_SIZE = 50


class WatchlistItemIn(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=20)


class WatchlistReorderIn(BaseModel):
    tickers: List[str]


class WatchlistItemOut(BaseModel):
    ticker: str
    name: str | None = None
    sectype: str | None = None
    inst_type: str | None = None
    group: str | None = None
    position: int

    class Config:
        from_attributes = True


@router.get("", response_model=List[WatchlistItemOut])
def list_watchlist(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Возвращает список тикеров юзера + metadata из таблицы instruments
    (name, sectype, group) для UI-отображения.
    """
    # LEFT JOIN — если тикер удалили из instruments (delisting), показываем
    # без metadata вместо 500. UI отрисует с заголовком "тикер" без подзаголовка.
    rows = db.execute(
        select(
            UserWatchlist.ticker,
            UserWatchlist.position,
            Instrument.name,
            Instrument.sectype,
            Instrument.type,
            Instrument.group,
        )
        .outerjoin(Instrument, Instrument.sectype == UserWatchlist.ticker)
        .where(UserWatchlist.user_id == user.id)
        .order_by(UserWatchlist.position.asc(), UserWatchlist.added_at.asc())
    ).all()

    return [
        WatchlistItemOut(
            ticker=r.ticker,
            name=r.name,
            sectype=r.sectype,
            inst_type=r.type,
            group=r.group,
            position=r.position,
        )
        for r in rows
    ]


@router.post("", response_model=WatchlistItemOut, status_code=status.HTTP_201_CREATED)
def add_to_watchlist(
    body: WatchlistItemIn,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Добавляет тикер в watchlist. Если уже есть — возвращает существующий
    (idempotent). При достижении MAX_WATCHLIST_SIZE — 400.
    """
    try:
        ticker = validate_safe_id(body.ticker.strip().upper(), "ticker")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Проверяем что тикер вообще есть в instruments. Это не критично, но
    # защищает от опечаток в API. Если нет — 404.
    inst = db.execute(
        select(Instrument).where(Instrument.sectype == ticker)
    ).scalar_one_or_none()
    if not inst:
        raise HTTPException(status_code=404, detail=f"Инструмент {ticker} не найден")

    # Дубликат — возвращаем существующий.
    existing = db.execute(
        select(UserWatchlist).where(
            UserWatchlist.user_id == user.id,
            UserWatchlist.ticker == ticker,
        )
    ).scalar_one_or_none()
    if existing:
        return WatchlistItemOut(
            ticker=existing.ticker, name=inst.name, sectype=inst.sectype,
            inst_type=inst.type, group=inst.group, position=existing.position,
        )

    # Лимит на размер.
    count = db.execute(
        select(func.count(UserWatchlist.id)).where(UserWatchlist.user_id == user.id)
    ).scalar_one()
    if count >= MAX_WATCHLIST_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Достигнут лимит watchlist: {MAX_WATCHLIST_SIZE} тикеров. Удалите ненужные."
        )

    # Position = max(position) + 1 — добавляем в конец списка.
    max_pos = db.execute(
        select(func.coalesce(func.max(UserWatchlist.position), -1))
        .where(UserWatchlist.user_id == user.id)
    ).scalar_one()

    item = UserWatchlist(
        user_id=user.id,
        ticker=ticker,
        position=max_pos + 1,
    )
    db.add(item)
    db.commit()
    db.refresh(item)

    return WatchlistItemOut(
        ticker=item.ticker, name=inst.name, sectype=inst.sectype,
        inst_type=inst.type, group=inst.group, position=item.position,
    )


@router.delete("/{ticker}", status_code=status.HTTP_204_NO_CONTENT)
def remove_from_watchlist(
    ticker: str,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Убирает тикер из watchlist. Idempotent — нет тикера = 204."""
    try:
        ticker = validate_safe_id(ticker.strip().upper(), "ticker")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    db.execute(
        delete(UserWatchlist).where(
            UserWatchlist.user_id == user.id,
            UserWatchlist.ticker == ticker,
        )
    )
    db.commit()
    return None


@router.patch("/reorder")
def reorder_watchlist(
    body: WatchlistReorderIn,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Переупорядочивает watchlist. body.tickers — массив в новом порядке.
    Тикеры не из watchlist игнорируются. Не указанные тикеры остаются
    в конце в исходном порядке.
    """
    # Текущие тикеры юзера.
    current = {
        row.ticker: row for row in db.execute(
            select(UserWatchlist).where(UserWatchlist.user_id == user.id)
        ).scalars()
    }

    # Применяем новый порядок только для известных тикеров.
    position = 0
    for ticker in body.tickers:
        try:
            ticker_norm = validate_safe_id(ticker.strip().upper(), "ticker")
        except ValueError:
            continue
        item = current.pop(ticker_norm, None)
        if item is not None:
            item.position = position
            position += 1

    # Остальные (не указанные) сохраняют относительный порядок, но
    # положения смещаются после reorder'нутых.
    for item in sorted(current.values(), key=lambda x: x.position):
        item.position = position
        position += 1

    db.commit()
    return {"ok": True, "count": position}
