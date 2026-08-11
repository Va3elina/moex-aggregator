"""
/api/alerts/* — Telegram alert-bot.

Phase 1 (привязка Telegram):
  POST   /api/alerts/telegram/link    — сгенерировать deep-link токен привязки (auth)
  GET    /api/alerts/telegram/status  — привязан ли Telegram у юзера (auth)
  DELETE /api/alerts/telegram         — отвязать Telegram (auth)

Phase 2 добавит CRUD алертов (создать/список/пауза/удалить) с квота-гейтом по тарифу
(free=0 → апгрейд, basic=20, pro=∞ — см. features.py telegram_alerts_quota).

Привязка работает через одноразовый токен (telegram_link_tokens) + deep-link
t.me/<bot>?start=<token>: бот не может писать юзеру первым, поэтому юзер сам жмёт /start.
"""
import os
import secrets
from datetime import datetime, timedelta, timezone

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.services.market_delay import price_cutoff
from api.database import get_db
from api.models import User, Alert, AlertEvent
from api.models.telegram_link_token import TelegramLinkToken
from api.routers.auth import get_current_user
from api.billing.tiers import user_tier
from api.billing.features import get_common_features

router = APIRouter(prefix="/api/alerts", tags=["alerts"])

LINK_TOKEN_TTL_MIN = 10
# @username бота (без @) для deep-link t.me/<username>. Дефолт = реальный бот
# (публичный стабильный факт, не секрет); env ALERT_BOT_USERNAME может переопределить.
ALERT_BOT_USERNAME = os.getenv("ALERT_BOT_USERNAME", "framesignalbot")


class LinkResponse(BaseModel):
    deep_link: str
    token: str
    expires_at: str


class TelegramStatus(BaseModel):
    linked: bool
    username: str | None = None


@router.post("/telegram/link", response_model=LinkResponse)
def create_link(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Одноразовый токен привязки. Старые токены этого юзера чистим (один активный)."""
    db.query(TelegramLinkToken).filter(TelegramLinkToken.user_id == user.id).delete()
    token = secrets.token_urlsafe(24)
    expires = datetime.now(timezone.utc) + timedelta(minutes=LINK_TOKEN_TTL_MIN)
    db.add(TelegramLinkToken(token=token, user_id=user.id, expires_at=expires))
    db.commit()
    bot = ALERT_BOT_USERNAME or "framesignalbot"
    return LinkResponse(
        deep_link=f"https://t.me/{bot}?start={token}",
        token=token,
        expires_at=expires.isoformat(),
    )


@router.get("/telegram/status", response_model=TelegramStatus)
def telegram_status(user: User = Depends(get_current_user)):
    return TelegramStatus(
        linked=user.telegram_chat_id is not None,
        username=user.telegram_username,
    )


@router.delete("/telegram")
def unlink_telegram(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user.telegram_chat_id = None
    user.telegram_username = None
    user.telegram_linked_at = None
    db.commit()
    return {"ok": True}


# ─── Настройки доставки (дефолт-каналы юзера + доступность каналов) ──────────
class NotifySettings(BaseModel):
    # Дефолт-каналы для новых алертов (пресет модалки).
    default_channels: list[str] = Field(default_factory=lambda: ["telegram", "site"])
    # Доступность каналов для этого юзера — для рендера чекбоксов в модалке.
    telegram_linked: bool = False
    email_available: bool = False   # подтверждённый non-oauth email
    email: Optional[str] = None


def _email_available(user: User) -> bool:
    email = (getattr(user, "email", "") or "")
    return bool(getattr(user, "is_verified", False)) and not email.endswith("@oauth.local")


@router.get("/settings", response_model=NotifySettings)
def get_notify_settings(user: User = Depends(get_current_user)):
    email_ok = _email_available(user)
    return NotifySettings(
        default_channels=_channels_from_csv(getattr(user, "notify_channels_default", None)),
        telegram_linked=user.telegram_chat_id is not None,
        email_available=email_ok,
        email=(user.email if email_ok else None),
    )


class NotifySettingsUpdate(BaseModel):
    default_channels: list[str] = Field(min_length=1)


@router.put("/settings", response_model=NotifySettings)
def put_notify_settings(
    body: NotifySettingsUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    csv, err = _normalize_channels(body.default_channels, user=user, default_csv=None)
    if err:
        raise HTTPException(status_code=422, detail=err.capitalize())
    user.notify_channels_default = csv
    db.commit()
    return get_notify_settings(user)


# ─── Контекст для окна создания (текущая цена + intraday-доступность) ────────
# Лёгкий read-эндпоинт: фронт подтягивает свежую цену фьючерса, чтобы показать
# «Сейчас: <value> руб», задать placeholder порога и решить, есть ли у актива
# внутридневные данные (intraday) — от этого зависит частота проверки алерта.

class AlertPriceContext(BaseModel):
    value: Optional[float] = None
    ts: Optional[str] = None
    interval: Optional[int] = None
    intraday: bool = False


class AlertContextOut(BaseModel):
    price: AlertPriceContext


@router.get("/context", response_model=AlertContextOut)
def alert_context(
    indicator: str = Query(...),
    asset: str = Query(...),
    clgroup: str = Query("FIZ"),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Свежайшая цена фьючерса для окна создания алерта.

    Быстрый индексный запрос (sec_id IN (...), НЕ secid LIKE — тот даёт Seq-Scan
    по 8ГБ candles). Окно begin_time >= now()-14д ложится на индекс
    idx_candles_sec_interval_time. interval ∈ {5,60} → intraday=true.
    """
    row = db.execute(
        text("""
            SELECT close, begin_time, interval
            FROM candles
            WHERE sec_id IN (SELECT sec_id FROM instruments
                             WHERE sectype = :asset AND type = 'futures')
              AND interval IN (5, 60, 24)
              AND close > 0
              AND begin_time >= now() - interval '14 days'
              -- Лицензия MOEX: задержка только у 5-минуток (решение владельца
              -- 2026-08-11). Часовую и дневную свечу пропускаем как есть,
              -- поэтому предикат условный, а не общий.
              AND (interval <> 5 OR begin_time <= :cutoff)
            ORDER BY begin_time DESC, volume DESC NULLS LAST
            LIMIT 1
        """),
        {"asset": asset, "cutoff": price_cutoff()},
    ).first()

    if row is None:
        return AlertContextOut(price=AlertPriceContext())

    close, begin_time, interval = row[0], row[1], row[2]
    interval = int(interval) if interval is not None else None
    return AlertContextOut(price=AlertPriceContext(
        value=float(close) if close is not None else None,
        ts=begin_time.isoformat() if begin_time is not None else None,
        interval=interval,
        intraday=interval in (5, 60),
    ))


# ─── Алерты (CRUD) ──────────────────────────────────────────────────────────
# Квота по тарифу (features.py telegram_alerts_quota): free=0, basic=20, pro=∞(None).
# Гейт на СОЗДАНИИ: 0 → апгрейд до Basic; лимит достигнут → апгрейд до Pro.
# 403-сообщения содержат «тарифе»+«Basic/Pro» (под фронтовый tierError.ts).

class AlertCreate(BaseModel):
    # max_length зеркалит лимиты колонок БД — иначе длинная строка даёт 500
    # (Postgres DataError) вместо чистого 422.
    indicator: str = Field(max_length=24)        # 'price'|'oi_zscore'|'oi_move'|'funds_flow'
    # asset для funds_flow: 'all' (все фонды) | 'custom' (набор fund_ids) |
    # категория (money_market|stocks|bonds|gold). Для OI/цены — sectype.
    asset: str = Field(max_length=20)            # sectype | категория/режим фондов
    asset_name: Optional[str] = Field(default=None, max_length=128)
    metric: str = Field(max_length=24)           # 'close'|'zscore'|'atr'|'net_flow'
    clgroup: Optional[str] = Field(default=None, max_length=3)   # OI: 'FIZ'|'YUR'
    # funds_flow: явный набор фондов (заморожённый). Задан → asset форсится в
    # 'custom', сериализуется в CSV в колонку alerts.fund_ids. None → таргет по asset.
    fund_ids: Optional[list[int]] = None
    op: str = Field(max_length=16)               # 'cross'|'cross_up'|'cross_down' (+legacy 'gt'|'lt')
    threshold: float
    mode: str = "once"                   # 'once'|'repeat'
    cooldown_hours: int = Field(default=24, ge=1, le=720)
    timeframe: str = Field(default="1d", max_length=4)   # '5m'|'1h'|'1d' (OI-метрики)
    # Каналы доставки: список из {telegram,site,email}. None → берём дефолт юзера
    # (notify_channels_default) на сервере. email требует подтверждённый email.
    channels: Optional[list[str]] = None


class AlertOut(BaseModel):
    id: int
    indicator: str
    asset: str
    asset_name: Optional[str] = None
    metric: str
    clgroup: Optional[str] = None
    # funds_flow: набор фондов набора (распарсенный из CSV-колонки), None если
    # таргет задаётся через asset (категория/'all'). Для OI/цены всегда None.
    fund_ids: Optional[list[int]] = None
    op: str
    threshold: float
    mode: str
    status: str
    timeframe: str = "1d"
    source: str = "oi"
    channels: list[str] = Field(default_factory=lambda: ["telegram"])
    # Сектор инструмента (instruments.group) джойном по asset — для группировки
    # в кабинете. None если инструмент не найден.
    sector: Optional[str] = None
    # Число срабатываний за всю жизнь алерта (COUNT по alert_fires).
    fires_count: int = 0
    last_fired_at: Optional[str] = None
    created_at: Optional[str] = None


def _fund_ids_to_csv(fund_ids: Optional[list[int]]) -> Optional[str]:
    """list[int] → CSV для колонки alerts.fund_ids; пусто/None → None (таргет по asset).
    Дедуп + сохранение порядка, отрицательные/нечисловые отсекаются на уровне типа."""
    if not fund_ids:
        return None
    seen: set = set()
    ordered: list[int] = []
    for fid in fund_ids:
        if fid in seen:
            continue
        seen.add(fid)
        ordered.append(int(fid))
    return ",".join(str(x) for x in ordered) if ordered else None


def _fund_ids_from_csv(raw: Optional[str]) -> Optional[list[int]]:
    """CSV из колонки alerts.fund_ids → list[int]; пусто/None → None."""
    if not raw:
        return None
    out: list[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out or None


# Каналы доставки алерта. 'site' бесплатен и не требует привязок; 'telegram'
# работает если привязан chat_id (иначе просто пропустится в eval-loop);
# 'email' требует подтверждённый email. Хранится CSV в alerts.channels.
ALERT_CHANNELS = ("telegram", "site", "email")


def _channels_from_csv(raw: Optional[str]) -> list[str]:
    """CSV из колонки alerts.channels → list[str] в каноническом порядке.
    Пусто → ['telegram'] (бэкенд-дефолт, обратная совместимость)."""
    if not raw:
        return ["telegram"]
    present = {p.strip() for p in str(raw).split(",") if p.strip()}
    ordered = [c for c in ALERT_CHANNELS if c in present]
    return ordered or ["telegram"]


def _normalize_channels(channels: Optional[list[str]], *, user: User,
                        default_csv: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """(csv | None, error | None). None channels → дефолт юзера. Валидирует
    поднабор {telegram,site,email}, непустой; email → нужен подтверждённый email."""
    if channels is None:
        raw = default_csv or getattr(user, "notify_channels_default", None) or "telegram,site"
        sel = _channels_from_csv(raw)
    else:
        sel = [c for c in ALERT_CHANNELS if c in set(channels)]
        if any(c not in ALERT_CHANNELS for c in channels):
            return None, "неизвестный канал доставки"
        if not sel:
            return None, "нужен хотя бы один канал доставки"
    if "email" in sel:
        email = (getattr(user, "email", "") or "")
        if not getattr(user, "is_verified", False) or email.endswith("@oauth.local"):
            return None, "для канала e-mail нужен подтверждённый адрес"
    return ",".join(sel), None


def _to_out(a: Alert, sector: Optional[str] = None, fires_count: int = 0) -> AlertOut:
    return AlertOut(
        id=a.id, indicator=a.indicator, asset=a.asset, asset_name=a.asset_name,
        metric=a.metric, clgroup=a.clgroup,
        fund_ids=_fund_ids_from_csv(getattr(a, "fund_ids", None)),
        op=a.op, threshold=float(a.threshold),
        mode=a.mode, status=a.status, timeframe=getattr(a, "timeframe", "1d") or "1d",
        source=getattr(a, "source", "oi") or "oi",
        channels=_channels_from_csv(getattr(a, "channels", None)),
        sector=sector, fires_count=int(fires_count or 0),
        last_fired_at=a.last_fired_at.isoformat() if a.last_fired_at else None,
        created_at=a.created_at.isoformat() if a.created_at else None,
    )


def _log_alert_event(db: Session, *, alert_id: Optional[int], user_id: int,
                     event: str, asset: Optional[str], indicator: Optional[str],
                     source: Optional[str]) -> None:
    """Записать событие жизненного цикла алерта (в той же сессии — коммит у вызывающего)."""
    db.add(AlertEvent(
        alert_id=alert_id, user_id=user_id, event=event,
        asset=asset, indicator=indicator, source=source,
    ))


# Категории фонд-алертов (asset для funds_flow) — ключи разделов «Деньги в фондах».
# Юань включён (2026-06-19): раздел «Юань» на /funds-money рабочий, NAV в ₽ →
# поток считается той же математикой, что у остальных категорий (см. signals/db.py).
FUNDS_CATEGORIES = ("money_market", "stocks", "bonds", "gold", "yuan")
# Допустимые значения asset для funds_flow: режимы 'all'/'custom' + категории.
#   'all'    → все фонды всех категорий (динамически), fund_ids NULL;
#   'custom' → произвольный набор (fund_ids обязателен);
#   категория → вся категория (динамически, backward-compat), fund_ids NULL.
FUNDS_ASSETS = ("all", "custom") + FUNDS_CATEGORIES
# Потолок на размер заморожённого набора фондов (защита от мусорного payload).
MAX_FUND_IDS = 500


def _alert_source(indicator: str) -> str:
    """Источник алерта для разделения в кабинете: funds_flow → 'funds', иначе 'oi'.

    source задаём здесь (а не из тела запроса) — фронт его не присылает, он
    однозначно выводится из индикатора. Так нельзя подсунуть funds-алерт под
    'oi'-секцию и наоборот.
    """
    return "funds" if indicator == "funds_flow" else "oi"


# Валидация значений (op/mode/indicator/clgroup) — общая для одиночного и
# пакетного создания. Возвращает текст ошибки или None если всё ок.
def _validate_alert_body(b: AlertCreate) -> Optional[str]:
    if b.op not in ("gt", "lt", "cross", "cross_up", "cross_down",
                    "new_high", "new_low", "new_extreme"):
        return "некорректное условие"
    if b.mode not in ("once", "repeat"):
        return "некорректный режим"
    if b.indicator not in ("price", "oi_zscore", "oi_move", "oi_participants", "oi_level",
                           "oi_extreme", "buffett_ratio", "buffett_cap", "strength_level",
                           "funds_flow"):
        return "неизвестный индикатор"
    # funds_flow: asset — режим/категория фондов (не sectype); clgroup у фондов
    # нет; таймфрейм потоков всегда дневной. fund_ids задан → набор «custom».
    if b.indicator == "funds_flow":
        if b.asset not in FUNDS_ASSETS:
            return "неизвестная категория фондов"
        if b.timeframe != "1d":
            return "у потоков фондов только дневной таймфрейм"
        # 'custom' без набора бессмыслен; набор без 'custom' (на категории/all)
        # тоже не допускаем — иначе двойная семантика таргета.
        has_ids = bool(b.fund_ids)
        if b.asset == "custom" and not has_ids:
            return "для набора фондов нужно указать fund_ids"
        if has_ids and b.asset != "custom":
            return "fund_ids допустим только при asset='custom'"
        if has_ids and len(b.fund_ids) > MAX_FUND_IDS:
            return f"слишком много фондов в наборе (макс {MAX_FUND_IDS})"
        if has_ids and any((not isinstance(x, int)) or x <= 0 for x in b.fund_ids):
            return "некорректные fund_ids"
        return None
    # fund_ids неприменим к не-fund алертам — игнорируем (не валим запрос).
    if b.clgroup not in (None, "FIZ", "YUR", "ALL"):
        return "некорректная группа участников"
    if b.timeframe not in ("5m", "1h", "1d"):
        return "некорректный таймфрейм"
    # oi_level: «нога» величины ОИ (что показано на графике).
    if b.indicator == "oi_level" and b.metric not in ("net", "long", "short", "oi", "npart"):
        return "некорректная величина ОИ"
    # oi_extreme: новый максимум/минимум перекоса. op = new_high/new_low/new_extreme
    # (любой); threshold = окно-дни (180 полгода / 365 год / 0 всё время). clgroup:
    # FIZ/YUR — отдельно по группе, ALL — «в целом» (физ и юр зеркальны, считаем от FIZ).
    if b.indicator == "oi_extreme":
        if b.op not in ("new_high", "new_low", "new_extreme"):
            return "для нового экстремума op = new_high/new_low/new_extreme"
        # Горизонты-дни: 365 (год) / 730 (2г) / 1095 (3г) / 1460 (4г) / 1825 (5л) /
        # 0 (всё время). Короткие мес/полугода убраны — охотимся за редкими рекордами.
        if int(b.threshold) not in (0, 365, 730, 1095, 1460, 1825):
            return "период экстремума: 365 / 730 / 1095 / 1460 / 1825 / 0 (всё время)"
        if b.clgroup not in ("FIZ", "YUR", "ALL"):
            return "укажите группу физлица/юрлица/в целом"
    elif b.op in ("new_high", "new_low", "new_extreme"):
        return "new_high/new_low/new_extreme только для нового экстремума"
    # buffett_ratio: режим коэффициента (Cap/ВВП или Cap/M2).
    if b.indicator == "buffett_ratio" and b.metric not in ("cap_gdp", "cap_m2"):
        return "некорректный режим индикатора Баффета"
    # strength_level: metric = период EMA; asset = вселенная.
    if b.indicator == "strength_level":
        if b.metric not in ("20", "50", "100", "200"):
            return "некорректный период EMA"
        if b.asset not in ("all", "imoex", "all_usd", "imoex_usd"):
            return "некорректная вселенная силы рынка"
    return None


@router.get("", response_model=list[AlertOut])
def list_alerts(
    response: Response,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Список алертов юзера. Тело — ПЛОСКИЙ массив AlertOut (обратносовместимо с
    фронтовым listAlerts). Общее число — в заголовке X-Total-Count.

    Без N+1 и без фан-аута: sectype в instruments НЕ уникален (фьючерс 'Si' =
    несколько контрактов SiH/SiM/... с одним sectype), поэтому сектор берём
    скалярным коррелированным подзапросом (LIMIT 1), а НЕ JOIN'ом — иначе строки
    алертов размножились бы. Число срабатываний — коррелированным COUNT по
    alert_fires. Один запрос.
    """
    total = db.query(Alert).filter(Alert.user_id == user.id).count()

    rows = db.execute(
        text("""
            SELECT a.*,
                   (SELECT i."group" FROM instruments i
                    WHERE i.sectype = a.asset AND i."group" IS NOT NULL
                    LIMIT 1) AS sector,
                   (SELECT count(*) FROM alert_fires f WHERE f.alert_id = a.id) AS fires_count
            FROM alerts a
            WHERE a.user_id = :uid
            ORDER BY a.created_at DESC
            LIMIT :limit OFFSET :offset
        """),
        {"uid": user.id, "limit": limit, "offset": offset},
    ).mappings().all()

    response.headers["X-Total-Count"] = str(total)

    out: list[AlertOut] = []
    for r in rows:
        out.append(AlertOut(
            id=r["id"], indicator=r["indicator"], asset=r["asset"],
            asset_name=r["asset_name"], metric=r["metric"], clgroup=r["clgroup"],
            fund_ids=_fund_ids_from_csv(r.get("fund_ids")),
            op=r["op"], threshold=float(r["threshold"]), mode=r["mode"],
            status=r["status"], timeframe=r["timeframe"] or "1d",
            source=r["source"] or "oi",
            channels=_channels_from_csv(r.get("channels")),
            sector=r["sector"],
            fires_count=int(r["fires_count"] or 0),
            last_fired_at=r["last_fired_at"].isoformat() if r["last_fired_at"] else None,
            created_at=r["created_at"].isoformat() if r["created_at"] else None,
        ))
    return out


@router.post("", response_model=AlertOut)
def create_alert(
    body: AlertCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    quota = get_common_features(user_tier(user)).get("telegram_alerts_quota", 0)
    if quota == 0:
        raise HTTPException(status_code=403, detail="Алерты доступны на тарифе Basic и Pro")
    if isinstance(quota, int):  # не None (безлимит) → проверяем лимит
        used = db.query(Alert).filter(Alert.user_id == user.id).count()
        if used >= quota:
            raise HTTPException(
                status_code=403,
                detail=f"Достигнут лимит {quota} алертов на вашем тарифе — перейдите на Pro для безлимита",
            )
    err = _validate_alert_body(body)
    if err:
        raise HTTPException(status_code=422, detail=err.capitalize())

    channels_csv, ch_err = _normalize_channels(body.channels, user=user, default_csv=None)
    if ch_err:
        raise HTTPException(status_code=422, detail=ch_err.capitalize())

    # funds_flow → source='funds' (отдельная секция кабинета), clgroup у фондов
    # неприменим — обнуляем, чтобы не протёк случайный 'FIZ' из формы. Набор
    # фондов: fund_ids задан → asset форсим в 'custom' и сериализуем CSV; иначе
    # fund_ids=NULL (таргет по asset: 'all' / категория).
    is_funds = body.indicator == "funds_flow"
    fund_ids_csv = _fund_ids_to_csv(body.fund_ids) if is_funds else None
    asset = "custom" if fund_ids_csv else body.asset
    alert = Alert(
        user_id=user.id,
        indicator=body.indicator,
        asset=asset,
        asset_name=body.asset_name,
        metric=body.metric,
        clgroup=None if is_funds else body.clgroup,
        fund_ids=fund_ids_csv,
        op=body.op,
        threshold=body.threshold,
        mode=body.mode,
        cooldown_hours=body.cooldown_hours,
        timeframe=body.timeframe,
        source=_alert_source(body.indicator),
        channels=channels_csv,
        status="active",
    )
    db.add(alert)
    db.flush()   # получить alert.id до коммита для лога события
    _log_alert_event(
        db, alert_id=alert.id, user_id=user.id, event="created",
        asset=alert.asset, indicator=alert.indicator, source=alert.source,
    )
    db.commit()
    db.refresh(alert)
    return _to_out(alert)


class AlertBatchCreate(BaseModel):
    # Пакетное создание (группа активов) — ОДИН HTTP-запрос вместо N, иначе
    # N параллельных POST'ов бьются о nginx rate-limit (burst=20) и часть
    # падает с 503. Квота проверяется один раз — без гонки счётчика.
    alerts: list[AlertCreate] = Field(min_length=1, max_length=200)


class AlertBatchResult(BaseModel):
    created: int
    skipped: int = 0      # пропущено как дубли уже существующих
    errors: list[str]


def _alert_key(indicator, asset, clgroup, metric, op, threshold, timeframe="1d",
               fund_ids=None):
    # timeframe в ключе: 1d и 5m/1h на один актив — РАЗНЫЕ алерты (разный источник
    # «net сейчас»), не дубли. Дефолт '1d' для существующих строк без таймфрейма.
    # fund_ids в ключе: два custom-fund-алерта (asset='custom') с разными наборами
    # — РАЗНЫЕ алерты. Нормализуем CSV/list → отсортированный tuple (порядок и
    # дубли не влияют на «тот же набор»). Для не-fund/категорий fund_ids пуст → ().
    if isinstance(fund_ids, str):
        norm = tuple(sorted(_fund_ids_from_csv(fund_ids) or ()))
    elif fund_ids:
        norm = tuple(sorted(set(int(x) for x in fund_ids)))
    else:
        norm = ()
    return (indicator, asset, clgroup or "", metric, op,
            round(float(threshold), 6), timeframe or "1d", norm)


@router.post("/batch", response_model=AlertBatchResult)
def create_alerts_batch(
    body: AlertBatchCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    quota = get_common_features(user_tier(user)).get("telegram_alerts_quota", 0)
    if quota == 0:
        raise HTTPException(status_code=403, detail="Алерты доступны на тарифе Basic и Pro")
    existing = db.query(Alert).filter(Alert.user_id == user.id).all()
    used = len(existing)
    # remaining=None → безлимит (Pro). Иначе сколько ещё можно создать.
    remaining = None if quota is None else max(0, quota - used)
    # Дедуп: повторный «Создать» на той же группе не плодит дубли.
    seen = {_alert_key(x.indicator, x.asset, x.clgroup, x.metric, x.op, x.threshold,
                       getattr(x, "timeframe", "1d"), getattr(x, "fund_ids", None))
            for x in existing}

    created = 0
    skipped = 0
    errors: list[str] = []
    for a in body.alerts:
        err = _validate_alert_body(a)
        if err:
            errors.append(f"{a.asset}: {err}")
            continue
        channels_csv, ch_err = _normalize_channels(a.channels, user=user, default_csv=None)
        if ch_err:
            errors.append(f"{a.asset}: {ch_err}")
            continue
        is_funds = a.indicator == "funds_flow"
        fund_ids_csv = _fund_ids_to_csv(a.fund_ids) if is_funds else None
        asset_val = "custom" if fund_ids_csv else a.asset
        k = _alert_key(a.indicator, asset_val, a.clgroup, a.metric, a.op, a.threshold,
                       a.timeframe, fund_ids_csv)
        if k in seen:
            skipped += 1
            continue
        if remaining is not None and created >= remaining:
            errors.append(f"{a.asset}: достигнут лимит {quota} алертов (Pro — безлимит)")
            continue
        new_alert = Alert(
            user_id=user.id,
            indicator=a.indicator, asset=asset_val, asset_name=a.asset_name,
            metric=a.metric, clgroup=None if is_funds else a.clgroup,
            fund_ids=fund_ids_csv,
            op=a.op, threshold=a.threshold,
            mode=a.mode, cooldown_hours=a.cooldown_hours, timeframe=a.timeframe,
            source=_alert_source(a.indicator),
            channels=channels_csv,
            status="active",
        )
        db.add(new_alert)
        db.flush()   # alert.id для лога события
        _log_alert_event(
            db, alert_id=new_alert.id, user_id=user.id, event="created",
            asset=new_alert.asset, indicator=new_alert.indicator, source=new_alert.source,
        )
        seen.add(k)
        created += 1
    db.commit()
    return AlertBatchResult(created=created, skipped=skipped, errors=errors)


# Индикаторы, у которых timeframe реально меняет расчёт (см. alerts_run.py::compute_value
# — только они читают a.timeframe через _TF_INTERVAL). oi_extreme/price/funds_flow/
# strength_level/buffett_* его игнорируют — редактирование для них бессмысленно.
TIMEFRAME_INDICATORS = ("oi_move", "oi_participants", "oi_level")


@router.patch("/{alert_id}", response_model=AlertOut)
def update_alert(
    alert_id: int,
    status: Optional[str] = None,
    timeframe: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    a = db.query(Alert).filter(Alert.id == alert_id, Alert.user_id == user.id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Алерт не найден")
    if status in ("active", "paused") and status != a.status:
        a.status = status
        _log_alert_event(
            db, alert_id=a.id, user_id=user.id,
            event="resumed" if status == "active" else "paused",
            asset=a.asset, indicator=a.indicator, source=a.source,
        )
    if timeframe is not None and timeframe != a.timeframe:
        if timeframe not in ("5m", "1h", "1d"):
            raise HTTPException(status_code=400, detail="некорректный таймфрейм")
        if a.indicator not in TIMEFRAME_INDICATORS:
            raise HTTPException(status_code=400, detail="для этого индикатора таймфрейм нельзя менять")
        a.timeframe = timeframe
        _log_alert_event(
            db, alert_id=a.id, user_id=user.id, event="tf_changed",
            asset=a.asset, indicator=a.indicator, source=a.source,
        )
    db.commit()
    db.refresh(a)
    return _to_out(a)


@router.delete("")
def delete_all_alerts(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Удалить ВСЕ алерты пользователя — массовая очистка (если случайно создал
    группу из 100). Возвращает число удалённых."""
    # Лог 'deleted' по каждому до удаления (alert_id переживает удаление — без FK).
    for a in db.query(Alert).filter(Alert.user_id == user.id).all():
        _log_alert_event(
            db, alert_id=a.id, user_id=user.id, event="deleted",
            asset=a.asset, indicator=a.indicator, source=a.source,
        )
    n = db.query(Alert).filter(Alert.user_id == user.id).delete(synchronize_session=False)
    db.commit()
    return {"deleted": int(n)}


@router.delete("/{alert_id}")
def delete_alert(
    alert_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    a = db.query(Alert).filter(Alert.id == alert_id, Alert.user_id == user.id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Алерт не найден")
    # Лог 'deleted' до удаления; alert_id переживёт удаление (alert_events без FK).
    _log_alert_event(
        db, alert_id=a.id, user_id=user.id, event="deleted",
        asset=a.asset, indicator=a.indicator, source=a.source,
    )
    db.delete(a)
    db.commit()
    return {"ok": True}


# ─── История срабатываний одного алерта ─────────────────────────────────────

class AlertFireOut(BaseModel):
    id: int
    fired_at: Optional[str] = None
    value: Optional[float] = None
    message_text: Optional[str] = None


class RecentFireOut(BaseModel):
    """Срабатывание для центра уведомлений (вкладка «Мои алерты» + тост).
    Несёт контекст алерта, чтобы отрисовать карточку без доп. запросов."""
    id: int
    alert_id: int
    fired_at: Optional[str] = None
    value: Optional[float] = None
    message_text: Optional[str] = None
    indicator: str
    asset: str
    asset_name: Optional[str] = None
    source: str = "oi"


@router.get("/recent-fires", response_model=list[RecentFireOut])
def recent_fires(
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    since: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Последние срабатывания по ВСЕМ алертам юзера (user-scoped). Источник тоста
    на сайте (SSE 'alert_fire' даёт только user_id → фронт тянет контент отсюда) и
    вкладки «Мои алерты» в колоколе. since>0 → только новее этого fire.id."""
    rows = db.execute(
        text("""
            SELECT f.id, f.alert_id, f.fired_at, f.value, f.message_text,
                   a.indicator, a.asset, a.asset_name, a.source
            FROM alert_fires f
            JOIN alerts a ON a.id = f.alert_id
            WHERE a.user_id = :uid AND f.id > :since
            ORDER BY f.id DESC
            LIMIT :limit
        """),
        {"uid": user.id, "since": since, "limit": limit},
    ).mappings().all()
    return [
        RecentFireOut(
            id=r["id"], alert_id=r["alert_id"],
            fired_at=r["fired_at"].isoformat() if r["fired_at"] else None,
            value=float(r["value"]) if r["value"] is not None else None,
            message_text=r["message_text"], indicator=r["indicator"],
            asset=r["asset"], asset_name=r["asset_name"], source=r["source"] or "oi",
        )
        for r in rows
    ]


@router.get("/{alert_id}/fires", response_model=list[AlertFireOut])
def list_alert_fires(
    alert_id: int,
    response: Response,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """История срабатываний одного алерта (alert_fires), новые сверху.

    Доступ: владелец алерта или admin. Общее число — в X-Total-Count.
    """
    a = db.query(Alert).filter(Alert.id == alert_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Алерт не найден")
    if a.user_id != user.id and user.role != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа к этому алерту")

    total = db.execute(
        text("SELECT count(*) FROM alert_fires WHERE alert_id = :aid"),
        {"aid": alert_id},
    ).scalar() or 0
    response.headers["X-Total-Count"] = str(int(total))

    rows = db.execute(
        text("""
            SELECT id, fired_at, value, message_text
            FROM alert_fires
            WHERE alert_id = :aid
            ORDER BY fired_at DESC
            LIMIT :limit OFFSET :offset
        """),
        {"aid": alert_id, "limit": limit, "offset": offset},
    ).mappings().all()

    return [
        AlertFireOut(
            id=r["id"],
            fired_at=r["fired_at"].isoformat() if r["fired_at"] else None,
            value=float(r["value"]) if r["value"] is not None else None,
            message_text=r["message_text"],
        )
        for r in rows
    ]
