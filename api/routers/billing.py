"""
Billing endpoints — оплата и подписки.

Routes:
  GET  /api/billing/plans       — список тарифов (публичный, без auth)
  GET  /api/billing/status      — моя текущая подписка (auth required)
  POST /api/billing/checkout    — создать платёж → URL редиректа (auth required)
  POST /api/billing/webhook     — callback от провайдера (без auth, но верифицируется)
  POST /api/billing/cancel      — отменить авто-продление (auth required)
  POST /api/billing/stub/simulate — ТОЛЬКО для stub-провайдера: ручная симуляция
                                     успеха платежа (для тестирования без ключей)

Провайдер выбирается автоматически в api/billing/factory.py через env:
  - TBANK_TERMINAL_KEY + TBANK_PASSWORD → T-Bank (приоритет)
  - YOOKASSA_SHOP_ID + YOOKASSA_SECRET_KEY → ЮKassa (legacy)
  - не заданы → Stub (все платежи fake, для прогона инфраструктуры)
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.billing import service as billing_service
from api.billing import invites as invite_service
from api.billing.factory import get_payment_provider
from api.billing.plans import get_plan, list_public_plans, tiers_grouped
from api.billing.tiers import user_tier
from api.database import get_db
from api.models.subscription import Subscription
from api.models.user import User
from api.routers.auth import get_current_user, require_admin

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/billing", tags=["billing"])


# ═══════════════════════════════════════════════════════════════════════════════
#  Request/Response schemas
# ═══════════════════════════════════════════════════════════════════════════════

class CheckoutRequest(BaseModel):
    plan_id: str                  # 'pro_monthly' / 'premium_yearly' / ...
    return_url: str | None = None # куда вернуть после оплаты (optional)


class CheckoutResponse(BaseModel):
    subscription_id: int
    payment_id: str
    confirmation_url: str
    plan_id: str
    tier: str
    amount: float


class StatusResponse(BaseModel):
    tier: str                             # 'guest' / 'free' / 'basic' / 'pro' / 'premium' / 'admin'
    is_active: bool
    subscription_id: int | None = None
    plan_id: str | None = None
    started_at: str | None = None
    expires_at: str | None = None
    cancelled_at: str | None = None      # NULL → активна и продлится, NOT NULL → отменена, доступ до expires_at


# ═══════════════════════════════════════════════════════════════════════════════
#  1. GET /plans — список тарифов (публичный)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/plans")
async def list_plans():
    """
    Возвращает все тарифы для отображения на Pricing-странице.
    Структура — сгруппировано по tier (free / basic / pro / premium),
    внутри каждого — monthly и yearly варианты.
    """
    return {
        "provider": get_payment_provider().name,  # 'stub' или 'yookassa' — фронт может показать баннер
        "currency": "RUB",
        "tiers": tiers_grouped(),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  2. GET /status — моя подписка
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/status", response_model=StatusResponse)
async def my_status(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Вернёт текущий tier пользователя + активную подписку (если есть).
    """
    sub = billing_service.current_subscription(db, user)
    return StatusResponse(
        tier=user_tier(user),
        is_active=sub is not None,
        subscription_id=sub.id if sub else None,
        plan_id=sub.plan_id if sub else None,
        started_at=sub.started_at.isoformat() if sub and sub.started_at else None,
        expires_at=sub.expires_at.isoformat() if sub and sub.expires_at else None,
        cancelled_at=sub.cancelled_at.isoformat() if sub and sub.cancelled_at else None,
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  3. POST /checkout — создать платёж
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/checkout", response_model=CheckoutResponse)
async def checkout(
    body: CheckoutRequest,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Создаёт платёж у провайдера + запись subscription(pending).
    Клиент после получения ответа делает window.location = confirmation_url.
    """
    plan = get_plan(body.plan_id)
    if not plan:
        raise HTTPException(400, f"Unknown plan_id: {body.plan_id}")

    # Определяем return_url — куда ЮKassa вернёт пользователя после оплаты.
    # Если клиент не прислал — строим из Referer.
    return_url = body.return_url
    if not return_url:
        origin = request.headers.get("origin") or request.headers.get("referer") or "/"
        return_url = f"{origin.rstrip('/')}/billing/success"

    try:
        result = billing_service.create_checkout_for_user(
            db=db,
            user=user,
            plan_id=body.plan_id,
            return_url=return_url,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        log.error("checkout failed: %s", e)
        raise HTTPException(502, "Не удалось создать платёж. Попробуйте позже.")

    return CheckoutResponse(**result)


# ═══════════════════════════════════════════════════════════════════════════════
#  4. POST /webhook — callback от провайдера
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/webhook", status_code=status.HTTP_200_OK)
async def webhook(request: Request, db: Session = Depends(get_db)):
    """
    Принимает webhook-уведомления от платёжного провайдера.

    ЮKassa шлёт JSON на наш URL — должен быть публичным, без auth.
    Безопасность: провайдер делает дополнительный GET /payments/{id} внутри
    service.py (см. _verify_with_provider) — даже если webhook подделают,
    активация не произойдёт.

    Возвращаем 200 всегда (даже если не нашли подписку), иначе ЮKassa будет
    ретраить с экспоненциальным бэкоффом.
    """
    raw_body = await request.body()
    headers = dict(request.headers)

    provider = get_payment_provider()
    event = provider.parse_webhook(raw_body, headers)

    # Формат ответа:
    #   T-Bank ожидает plain text "OK" (иначе ретраит)
    #   ЮKassa принимает любой 2xx (мы возвращали JSON)
    # Чтобы не плодить ветвлений, для всех провайдеров отдаём plain text "OK" —
    # это совместимо с обоими.
    ok_response = PlainTextResponse("OK", status_code=200)

    if not event:
        log.warning("Webhook: unparseable body or bad signature (%d bytes)", len(raw_body))
        return ok_response

    log.info("Webhook event=%s payment_id=%s", event.event_type, event.payment_id)

    try:
        if event.event_type == "payment.succeeded":
            billing_service.activate_from_webhook(db, event)
        elif event.event_type in ("payment.canceled", "refund.succeeded"):
            billing_service.cancel_by_webhook(db, event)
        else:
            log.info("Webhook: unknown event type, ignoring: %s", event.event_type)
    except Exception as e:
        log.error("Webhook processing error: %s", e, exc_info=True)
        # НЕ пробрасываем — чтобы провайдер не ретраил. Ошибки смотрим в логах.

    return ok_response


# ═══════════════════════════════════════════════════════════════════════════════
#  5. POST /cancel — отменить авто-продление
# ═══════════════════════════════════════════════════════════════════════════════

class CancelRequest(BaseModel):
    subscription_id: int


@router.post("/cancel")
async def cancel(
    body: CancelRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    SOFT cancel — отключает авто-продление, но **доступ остаётся** до expires_at.
    Пользователь уже заплатил за период — несправедливо забирать access сразу.

    Что происходит:
    - cancelled_at = NOW() (флаг "пользователь нажал отмену")
    - status остаётся 'active' (current_subscription продолжает возвращать sub)
    - После expires_at < NOW() → cron expire_due_subscriptions переведёт status='expired'
    - UI показывает badge "Отменена" + дату окончания

    Можно undo через POST /resume пока expires_at > NOW().
    """
    sub = db.query(Subscription).filter(
        Subscription.id == body.subscription_id,
        Subscription.user_id == user.id,
    ).first()
    if not sub:
        raise HTTPException(404, "Подписка не найдена")
    if sub.status != "active":
        raise HTTPException(400, f"Нельзя отменить подписку в статусе {sub.status}")
    if sub.cancelled_at is not None:
        raise HTTPException(400, "Подписка уже отменена")

    # Soft cancel: только флаг, status не трогаем
    sub.cancelled_at = datetime.now(timezone.utc)
    db.commit()

    log.info("User #%s soft-cancelled subscription #%s (access until %s)",
             user.id, sub.id, sub.expires_at)
    return {
        "ok": True,
        "cancelled_at": sub.cancelled_at.isoformat(),
        "expires_at": sub.expires_at.isoformat() if sub.expires_at else None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  5b. POST /resume — отменить отмену (undo soft-cancel)
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/resume")
async def resume(
    body: CancelRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Отменяет soft-cancel — подписка снова считается активной для авто-продления.

    Работает только пока подписка ещё не истекла (status='active', cancelled_at IS NOT NULL).
    """
    sub = db.query(Subscription).filter(
        Subscription.id == body.subscription_id,
        Subscription.user_id == user.id,
    ).first()
    if not sub:
        raise HTTPException(404, "Подписка не найдена")
    if sub.status != "active":
        raise HTTPException(400, "Подписка уже не активна")
    if sub.cancelled_at is None:
        raise HTTPException(400, "Подписка не отменена — нечего возобновлять")

    sub.cancelled_at = None
    db.commit()

    log.info("User #%s resumed subscription #%s", user.id, sub.id)
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════════════════════
#  6. STUB ONLY — /stub/simulate (удобство для разработки без ключей)
# ═══════════════════════════════════════════════════════════════════════════════

class StubSimulateRequest(BaseModel):
    payment_id: str
    status: str = "succeeded"   # succeeded / canceled / refunded


@router.post("/stub/simulate")
async def stub_simulate(body: StubSimulateRequest, db: Session = Depends(get_db)):
    """
    Симулирует webhook от Stub-провайдера.
    Активен ТОЛЬКО когда factory возвращает StubPaymentProvider (нет реальных ключей).
    Frontend в stub-режиме может показать кнопку "Симулировать успех оплаты".
    """
    provider = get_payment_provider()
    if provider.name != "stub":
        raise HTTPException(404)

    from api.billing.provider import WebhookEvent
    event = WebhookEvent(
        payment_id=body.payment_id,
        event_type={
            "succeeded": "payment.succeeded",
            "canceled": "payment.canceled",
            "refunded": "refund.succeeded",
        }.get(body.status, "payment.succeeded"),
        payment_method="stub",
    )

    if event.event_type == "payment.succeeded":
        sub = billing_service.activate_from_webhook(db, event)
    else:
        sub = billing_service.cancel_by_webhook(db, event)

    if not sub:
        raise HTTPException(404, f"No subscription for payment_id={body.payment_id}")
    return {"ok": True, "subscription_id": sub.id, "status": sub.status}


# ═══════════════════════════════════════════════════════════════════════════════
#  7. REDEEM — user применяет invite-токен
# ═══════════════════════════════════════════════════════════════════════════════

class RedeemRequest(BaseModel):
    token: str


@router.post("/redeem")
async def redeem_invite(
    body: RedeemRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Применяет invite-токен для текущего user'а.
    Активирует подписку с указанным в invite tier'ом и duration_days.
    Идемпотентна: повторный redeem того же токена тем же user'ом вернёт
    уже созданную подписку.
    """
    try:
        sub = invite_service.redeem_invite(db, user, body.token.strip())
    except invite_service.RedeemError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        log.error("redeem_invite failed: %s", e, exc_info=True)
        raise HTTPException(500, "Не удалось применить ссылку")

    return {
        "ok": True,
        "tier": sub.tier,
        "subscription_id": sub.id,
        "expires_at": sub.expires_at.isoformat() if sub.expires_at else None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  8. ADMIN — /api/billing/admin/invites (CRUD для invite-токенов)
# ═══════════════════════════════════════════════════════════════════════════════

class CreateInvitesRequest(BaseModel):
    tier: str                  # 'basic' / 'pro' / 'premium'
    duration_days: int         # срок подписки после применения
    count: int = 1             # сколько токенов создать
    expires_in_days: int = 30  # когда истекает сам токен (если не использован)
    max_uses_per_token: int = 1  # сколько раз можно применить один токен
    note: str | None = None


@router.post("/admin/invites")
async def admin_create_invites(
    body: CreateInvitesRequest,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Создаёт N invite-токенов. Только admin'у."""
    try:
        invites = invite_service.create_invites(
            db=db,
            admin=admin,
            tier=body.tier,
            duration_days=body.duration_days,
            count=body.count,
            expires_in_days=body.expires_in_days,
            max_uses_per_token=body.max_uses_per_token,
            note=body.note,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))

    return {
        "tokens": [inv.token for inv in invites],
        "count": len(invites),
        "tier": body.tier,
        "duration_days": body.duration_days,
        "expires_in_days": body.expires_in_days,
    }


@router.get("/admin/invites")
async def admin_list_invites(
    only_active: bool = False,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Список всех invite-токенов."""
    return {"invites": invite_service.list_invites(db, admin, only_active=only_active)}


@router.delete("/admin/invites/{token}")
async def admin_revoke_invite(
    token: str,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Отозвать токен."""
    ok = invite_service.revoke_invite(db, admin, token)
    if not ok:
        raise HTTPException(404, "Токен не найден")
    return {"ok": True}
