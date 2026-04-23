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
  - YOOKASSA_SHOP_ID + YOOKASSA_SECRET_KEY заданы → реальная ЮKassa
  - не заданы → Stub (все платежи fake, для прогона инфраструктуры)
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.billing import service as billing_service
from api.billing.factory import get_payment_provider
from api.billing.plans import get_plan, list_public_plans, tiers_grouped
from api.billing.tiers import user_tier
from api.database import get_db
from api.models.subscription import Subscription
from api.models.user import User
from api.routers.auth import get_current_user

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
    if not event:
        log.warning("Webhook: unparseable body (%d bytes)", len(raw_body))
        return {"ok": True, "message": "ignored"}

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
        # НЕ пробрасываем — чтобы ЮKassa не ретраила. Ошибки смотрим в логах.

    return {"ok": True}


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
    Отменяет подписку — статус становится 'cancelled'.
    Доступ остаётся до expires_at (пользователь уже заплатил).
    Авто-продление (если реализовано в будущем) отключается.
    """
    sub = db.query(Subscription).filter(
        Subscription.id == body.subscription_id,
        Subscription.user_id == user.id,
    ).first()
    if not sub:
        raise HTTPException(404, "Подписка не найдена")
    if sub.status != "active":
        raise HTTPException(400, f"Нельзя отменить подписку в статусе {sub.status}")

    sub.status = "cancelled"
    sub.cancelled_at = datetime.now(timezone.utc)
    db.commit()

    log.info("User #%s cancelled subscription #%s", user.id, sub.id)
    return {"ok": True, "expires_at": sub.expires_at.isoformat() if sub.expires_at else None}


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
