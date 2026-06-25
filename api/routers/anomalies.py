"""
/api/anomalies/* — лента всплывающих аномалий (тосты в углу + колокол в хедере).

Источник — таблица `anomalies` (см. db/migrations/015): публичный маркет-вайд скан
(signals/anomaly_scan.py) + личный write-through из alerts_run (Phase 2b). Обе
ветки переиспользуют те же детекторы, что шлют пуши в Telegram-бот.

  GET  /api/anomalies/feed       — лента (гость ИЛИ юзер; tier-вердикт диплинка)
  POST /api/anomalies/seen       — пометить просмотренным до last_id (auth)
  POST /api/anomalies/toggle     — вкл/выкл всплывающие тосты (auth; гость — localStorage)
  POST /api/anomalies/{id}/subscribe — создать алерт из аномалии (auth + квота)

ВИДИМОСТЬ ленты НЕ делится по силе/тарифу — все видят одно (решение Вадима).
Тариф влияет ТОЛЬКО на КЛИК: «Открыть график» по gated-цели → апселл + дефолт
(вердикт `link_required_tier` считается сервером по features.py, фронту не доверяем);
«Поставить сигнал» → квота telegram_alerts_quota.

НЕ ЗАДВОИТЬ: `/feed` отдаёт DISTINCT ON (type·asset·clgroup·direction·день) с
приоритетом личной строки → событие приходит ровно раз (mine=true если личная).
"""
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User, Alert
from api.routers.auth import get_current_user, get_current_user_optional
from api.billing.tiers import user_tier
from api.billing.features import get_common_features, get_indicator_limits
from api.routers.alerts import _log_alert_event, _alert_source

router = APIRouter(prefix="/api/anomalies", tags=["anomalies"])


# ─── Вердикт диплинка по тарифу (сервер — источник истины) ───────────────────
# Только OI-аномалии могут упереться в whitelist активов Free. Фонды — категорийный
# диплинк (/funds-money?category=), он не гейтится. interval=24 разрешён всем
# тарифам, 5-минутку в аномалиях не используем.
_OI_TYPES = ("oi_move", "oi_zscore", "oi_participants")


def _link_required_tier(tier: str, atype: str, asset_id: str) -> Optional[str]:
    """Какой тариф нужен, чтобы ОТКРЫТЬ цель диплинка (или None если текущий пускает).
    Считаем по той же матрице features.py, что и страницы/TierLock — полная синхрония.
    Для наших аномалий гейтит лишь whitelist OI-активов на Free → 'basic'."""
    if atype in _OI_TYPES:
        wl = get_indicator_limits(tier, "open_interest").get("assets_whitelist")
        if wl is not None and asset_id not in wl:
            return "basic"     # basic/pro снимают whitelist (assets_whitelist=None)
    return None


# ─── Дефолты подписки из аномалии (кнопка «🔔 Получать») ──────────────────────
# (indicator, metric, threshold) по типу аномалии. op='gt', timeframe='1d',
# mode='repeat'. Порог чуть выше публичного ×3 — чтобы личный сигнал не частил;
# юзер потом крутит в кабинете.
_SUBSCRIBE_DEFAULTS = {
    "oi_move":         ("oi_move", "atr", 4.0),
    "oi_participants": ("oi_participants", "atr", 4.0),
    "oi_zscore":       ("oi_zscore", "zscore", 2.5),
    "funds_flow":      ("funds_flow", "net_flow", 4.0),
}


class AnomalyOut(BaseModel):
    id: int
    type: str
    asset_id: str
    asset_name: Optional[str] = None
    clgroup: Optional[str] = None
    direction: Optional[str] = None
    headline: str
    context: Optional[str] = None
    severity_value: Optional[float] = None
    signal_date: Optional[str] = None
    created_at: Optional[str] = None
    deep_link: dict
    mine: bool = False                        # личная строка (твой алерт сработал)
    link_required_tier: Optional[str] = None  # None → открыть как есть; иначе апселл+дефолт


class ChannelPostOut(BaseModel):
    id: int
    channel: str
    channel_name: Optional[str] = None
    text: Optional[str] = None
    photo_url: Optional[str] = None
    link: str
    posted_at: Optional[str] = None


class FeedOut(BaseModel):
    items: list[AnomalyOut]
    last_seen_id: Optional[int] = None   # серверный для залогиненных; None для гостя
    toasts_enabled: bool = True          # users.anomaly_toasts_enabled (гость → true)
    channel_posts: list[ChannelPostOut] = []   # новости каналов — секция колокола


_FEED_SQL = text("""
  SELECT * FROM (
    SELECT DISTINCT ON (type, asset_id, COALESCE(clgroup,''), COALESCE(direction,''), signal_date)
      id, scope, type, asset_id, asset_name, clgroup, direction, headline, context,
      severity_value, signal_date, created_at, deep_link
    FROM anomalies
    WHERE (scope = 'public' OR user_id = :uid)
      AND created_at >= now() - make_interval(hours => :max_age_hours)
      AND (:since_id = 0 OR id > :since_id)
    ORDER BY type, asset_id, COALESCE(clgroup,''), COALESCE(direction,''), signal_date,
             (scope = 'personal') DESC, created_at DESC
  ) t
  ORDER BY created_at DESC
  LIMIT :limit
""")

_CHANNEL_POSTS_SQL = text("""
  SELECT id, channel, channel_name, text, photo_url, link, posted_at
  FROM channel_posts
  ORDER BY posted_at DESC NULLS LAST, post_id DESC
  LIMIT 12
""")


@router.get("/feed", response_model=FeedOut)
def feed(
    since: int = Query(0, ge=0, description="вернуть только id > since (для реконнекта/дедупа)"),
    limit: int = Query(50, ge=1, le=200),
    max_age_hours: int = Query(168, ge=1, le=720, description="окно свежести (тост ~48ч, колокол ~168ч)"),
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    """Лента аномалий. Гость видит публичные; залогиненный — публичные + свои личные
    (свёрнутые в одно событие). Каждая строка несёт серверный вердикт диплинка."""
    tier = user_tier(user)
    rows = db.execute(_FEED_SQL, {
        "uid": user.id if user else None,
        "since_id": since, "limit": limit, "max_age_hours": max_age_hours,
    }).mappings().all()

    items = [
        AnomalyOut(
            id=r["id"], type=r["type"], asset_id=r["asset_id"], asset_name=r["asset_name"],
            clgroup=r["clgroup"], direction=r["direction"], headline=r["headline"],
            context=r["context"],
            severity_value=float(r["severity_value"]) if r["severity_value"] is not None else None,
            signal_date=r["signal_date"].isoformat() if r["signal_date"] else None,
            created_at=r["created_at"].isoformat() if r["created_at"] else None,
            deep_link=r["deep_link"] or {},
            mine=(r["scope"] == "personal"),
            link_required_tier=_link_required_tier(tier, r["type"], r["asset_id"]),
        )
        for r in rows
    ]
    # Новости каналов — отдельная секция колокола (таблица channel_posts).
    # try/except: если миграция 016 ещё не применена — лента аномалий не падает.
    posts: list[ChannelPostOut] = []
    try:
        prows = db.execute(_CHANNEL_POSTS_SQL).mappings().all()
        posts = [ChannelPostOut(
            id=p["id"], channel=p["channel"], channel_name=p["channel_name"],
            text=p["text"], photo_url=p["photo_url"], link=p["link"],
            posted_at=p["posted_at"].isoformat() if p["posted_at"] else None,
        ) for p in prows]
    except Exception:
        db.rollback()   # сброс failed-tx (на случай отсутствия таблицы)
    return FeedOut(
        items=items,
        last_seen_id=(user.last_seen_anomaly_id if user else None),
        toasts_enabled=(bool(user.anomaly_toasts_enabled) if user else True),
        channel_posts=posts,
    )


class SeenIn(BaseModel):
    last_id: int


@router.post("/seen")
def mark_seen(
    body: SeenIn,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Сдвинуть маркер «просмотрено» до last_id (только вперёд — max со старым).
    Гость не зовёт это (хранит seen в localStorage)."""
    cur = user.last_seen_anomaly_id or 0
    if body.last_id > cur:
        user.last_seen_anomaly_id = body.last_id
        db.commit()
    return {"ok": True, "last_seen_id": user.last_seen_anomaly_id}


class ToggleIn(BaseModel):
    enabled: bool


@router.post("/toggle")
def toggle_toasts(
    body: ToggleIn,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Вкл/выкл всплывающие тосты для залогиненного (кнопка «выключить» + тумблер
    профиля). Гость хранит флаг в localStorage — сюда не ходит."""
    user.anomaly_toasts_enabled = bool(body.enabled)
    db.commit()
    return {"ok": True, "enabled": user.anomaly_toasts_enabled}


@router.post("/{anomaly_id}/subscribe")
def subscribe(
    anomaly_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Создать личный алерт из аномалии (кнопка «🔔 Получать»). Квота по тарифу —
    как в create_alert: free=0 → апгрейд Basic; лимит достигнут → апгрейд Pro
    (403-сообщения с «тарифе»+Basic/Pro под фронтовый handleTierError)."""
    quota = get_common_features(user_tier(user)).get("telegram_alerts_quota", 0)
    if quota == 0:
        raise HTTPException(status_code=403, detail="Сигналы доступны на тарифе Basic и Pro")
    if isinstance(quota, int):
        used = db.query(Alert).filter(Alert.user_id == user.id).count()
        if used >= quota:
            raise HTTPException(
                status_code=403,
                detail=f"Достигнут лимит {quota} сигналов на вашем тарифе — перейдите на Pro для безлимита",
            )

    a = db.execute(
        text("SELECT type, asset_id, asset_name, clgroup FROM anomalies WHERE id = :id"),
        {"id": anomaly_id},
    ).mappings().first()
    if not a:
        raise HTTPException(status_code=404, detail="Аномалия не найдена")
    spec = _SUBSCRIBE_DEFAULTS.get(a["type"])
    if not spec:
        raise HTTPException(status_code=422, detail="Этот тип аномалии нельзя превратить в сигнал")
    indicator, metric, threshold = spec

    alert = Alert(
        user_id=user.id,
        indicator=indicator,
        asset=a["asset_id"],
        asset_name=a["asset_name"],
        metric=metric,
        clgroup=a["clgroup"] if indicator in _OI_TYPES else None,
        fund_ids=None,
        op="gt",
        threshold=threshold,
        mode="repeat",
        cooldown_hours=24,
        timeframe="1d",
        source=_alert_source(indicator),
        status="active",
    )
    db.add(alert)
    db.flush()
    _log_alert_event(db, alert_id=alert.id, user_id=user.id, event="created",
                     asset=alert.asset, indicator=alert.indicator, source=alert.source)
    db.commit()
    return {"ok": True, "alert_id": alert.id}
