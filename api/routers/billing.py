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
  - TBANK_TERMINAL_KEY + TBANK_PASSWORD → T-Bank
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
from api.models.payment_method import UserPaymentMethod
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
    widget_mode: bool = False     # True если инициирован через T-Bank JS SDK (SpeedPay)
    recurrent: bool = False       # True → сохранить карту для auto-renewal (T-Bank Recurrent="Y")


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

    Для провайдера tbank дополнительно отдаём terminal_key — он публичный
    идентификатор магазина (не секрет), фронт использует его для
    PaymentIntegration.init() при подключении JS SDK (SpeedPay кнопок).
    Password остаётся серверным секретом.
    """
    provider = get_payment_provider()
    response: dict = {
        "provider": provider.name,
        "currency": "RUB",
        "tiers": tiers_grouped(),
    }
    if provider.name == "tbank":
        # terminalKey — публичный (зашит в каждый Init request, отображается
        # в URL платежей). Безопасно отдавать на фронт для SDK init.
        response["terminal_key"] = getattr(provider, "terminal_key", None)
    return response


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

    # Определяем return_url — куда T-Bank вернёт пользователя после оплаты.
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
            widget_mode=body.widget_mode,
            recurrent=body.recurrent,
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
    Принимает webhook-уведомления от платёжного провайдера (T-Bank).

    T-Bank шлёт JSON на наш URL — должен быть публичным, без auth.
    Безопасность: верифицируем Token подписью, плюс service.py делает
    дополнительный GET /v2/GetState (см. _verify_with_provider) — даже
    если webhook подделают, активация не произойдёт.

    Возвращаем 200 + plain text "OK" всегда — это формат T-Bank.
    """
    raw_body = await request.body()
    headers = dict(request.headers)

    provider = get_payment_provider()
    event = provider.parse_webhook(raw_body, headers)

    # T-Bank ожидает plain text "OK" (иначе ретраит exponential backoff).
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
#  5a. POST /sync — fallback на случай задержки webhook
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/sync")
async def sync_pending(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Принудительно проверяет все pending подписки текущего user'а через
    GetState у провайдера. CONFIRMED активируется, REJECTED/CANCELED отменяется.

    Frontend дёргает это:
    1) Сразу при открытии /billing/success — чтобы не ждать webhook
    2) По кнопке «Проверить статус» если polling вышел в timeout

    Покрывает кейсы:
    - Webhook URL не настроен в кабинете провайдера
    - Webhook задержался / упал
    - Сеть/IP заблокированы временно

    Безопасно: идемпотентно, без side-effects если статус не изменился.
    """
    summary = billing_service.sync_pending_for_user(db, user)
    # Возвращаем актуальный статус user'а после sync
    sub = billing_service.current_subscription(db, user)
    return {
        "ok": True,
        "summary": summary,  # {activated, cancelled, skipped, checked}
        "tier": sub.tier if sub else "free",
        "is_active": sub is not None,
        "subscription_id": sub.id if sub else None,
        "expires_at": sub.expires_at.isoformat() if sub and sub.expires_at else None,
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
#  5c. POST /refund — пользовательский возврат (в течение 14 дней)
# ═══════════════════════════════════════════════════════════════════════════════

class RefundRequest(BaseModel):
    subscription_id: int
    amount: float | None = None  # частичный возврат, по умолчанию полный
    reason: str | None = None


# По закону "О защите прав потребителей" + ст. 26.1 ГК — дистанционно купленные
# нематериальные услуги можно вернуть в течение 14 дней с момента оплаты.
USER_REFUND_WINDOW_DAYS = 14


@router.post("/refund")
async def user_refund(
    body: RefundRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Пользователь запрашивает возврат собственной подписки.

    Условия:
    - Подписка принадлежит этому user'у
    - status в ('active', 'pending')  — не уже refunded/cancelled
    - С момента оплаты прошло не более USER_REFUND_WINDOW_DAYS дней
    - Возврат полный (amount=None) — частичные только админом

    Возврат частичный (amount != None) — на текущий момент только admin может,
    user через это endpoint всегда делает полный.
    """
    sub = db.query(Subscription).filter(
        Subscription.id == body.subscription_id,
        Subscription.user_id == user.id,
    ).first()
    if not sub:
        raise HTTPException(404, "Подписка не найдена")
    if sub.status not in ("active", "pending"):
        raise HTTPException(400, f"Подписка в статусе {sub.status} — возврат недоступен")

    # Окно 14 дней от created_at (или started_at если есть)
    started = sub.started_at or sub.created_at
    if started:
        # started_at из БД — timezone-aware (timestamptz)
        age_days = (datetime.now(timezone.utc) - started).days
        if age_days > USER_REFUND_WINDOW_DAYS:
            raise HTTPException(
                400,
                f"Срок возврата истёк (прошло {age_days} дней, окно {USER_REFUND_WINDOW_DAYS} дней). "
                "Для исключения напиши в поддержку."
            )

    # User может только полный возврат (amount=None). Игнорируем body.amount.
    result = billing_service.refund_subscription(
        db, sub, amount=None, reason=body.reason or "user request",
    )
    if not result["ok"]:
        raise HTTPException(502, result.get("message", "Не удалось оформить возврат"))
    return result


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


# ═══════════════════════════════════════════════════════════════════════════════
#  9. ADMIN — refund любой подписки (без 14-дневного ограничения)
# ═══════════════════════════════════════════════════════════════════════════════

class AdminRefundRequest(BaseModel):
    subscription_id: int
    amount: float | None = None  # частичный возврат, по умолчанию полный
    reason: str | None = None


@router.post("/admin/refund")
async def admin_refund(
    body: AdminRefundRequest,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Админский возврат — без проверки срока, любая подписка любого user'а.
    Поддерживает частичный возврат (передать amount меньше sub.amount).

    Используется для:
    - Возврат по запросу клиента после 14-дневного окна (DSGV / 26.1 ГК)
    - Решение споров (chargeback prevention)
    - Корректировка ошибочных платежей
    """
    sub = db.query(Subscription).filter(
        Subscription.id == body.subscription_id,
    ).first()
    if not sub:
        raise HTTPException(404, "Подписка не найдена")
    if sub.status in ("refunded", "cancelled"):
        return {
            "ok": True,
            "status": sub.status,
            "message": f"Подписка уже {sub.status} — повторный возврат не нужен",
        }

    result = billing_service.refund_subscription(
        db, sub,
        amount=body.amount,
        reason=body.reason or f"admin #{admin.id}",
    )
    if not result["ok"]:
        raise HTTPException(502, result.get("message", "Не удалось оформить возврат"))
    return {**result, "subscription_id": sub.id, "user_id": sub.user_id}


# ═══════════════════════════════════════════════════════════════════════════════
#  10. PAYMENT METHODS — управление сохранёнными картами
# ═══════════════════════════════════════════════════════════════════════════════

def _serialize_pm(pm: UserPaymentMethod) -> dict:
    """JSON-сериализация карты для UI «Сохранённые карты»."""
    return {
        "id": pm.id,
        "provider": pm.provider,
        "card_last4": pm.card_last4,
        "card_brand": pm.card_brand,
        "display_name": pm.display_name,
        "is_default": pm.is_default,
        "last_used_at": pm.last_used_at.isoformat() if pm.last_used_at else None,
        "created_at": pm.created_at.isoformat() if pm.created_at else None,
    }


@router.get("/payment_methods")
async def list_my_payment_methods(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Список сохранённых карт текущего user'а (только активные, не удалённые)."""
    methods = billing_service.list_payment_methods(db, user)
    return {"items": [_serialize_pm(pm) for pm in methods]}


@router.delete("/payment_methods/{pm_id}")
async def delete_my_payment_method(
    pm_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Soft-delete карты юзера. Не отвязываем у T-Bank — она просто скрывается
    из нашего списка. Подписки которые на неё ссылаются остаются работать
    до expires_at, но auto-renewal на этой карте не сработает.
    """
    ok = billing_service.soft_delete_payment_method(db, user, pm_id)
    if not ok:
        raise HTTPException(404, "Карта не найдена")
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════════════════════
#  11. ADMIN — Charge recurrent (auto-renewal trigger; test #6 T-Bank certification)
# ═══════════════════════════════════════════════════════════════════════════════

class AdminChargeRequest(BaseModel):
    user_id: int                          # на какого user'а списать
    plan_id: str                          # 'pro_monthly' / 'basic_yearly' / ...
    payment_method_id: int | None = None  # конкретная карта; если None — default


@router.post("/admin/charge_recurrent")
async def admin_charge_recurrent(
    body: AdminChargeRequest,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Списать с сохранённой карты юзера принудительно (admin only).

    Использование:
    - Тест №6 сертификации T-Bank (требует Init + Charge с RebillId из теста №5)
    - Manual trigger auto-renewal'а если cron не сработал
    - Помощь юзеру со стороны саппорта ("спишите за следующий месяц сейчас")

    Flow:
    1. Берём UserPaymentMethod (по pm_id или default юзера)
    2. provider.charge → Init + /v2/Charge мгновенно
    3. activate_from_webhook → подписка active, user.role обновлена

    Возвращает результат списания + новый subscription_id.
    """
    user = db.query(User).filter(User.id == body.user_id).first()
    if not user:
        raise HTTPException(404, f"User #{body.user_id} not found")

    if body.payment_method_id is not None:
        pm = db.query(UserPaymentMethod).filter(
            UserPaymentMethod.id == body.payment_method_id,
            UserPaymentMethod.user_id == body.user_id,
            UserPaymentMethod.deleted_at.is_(None),
        ).first()
        if not pm:
            raise HTTPException(404, "Payment method не найден")
    else:
        pm = billing_service.get_default_payment_method(db, user)
        if not pm:
            raise HTTPException(
                400,
                f"У user #{body.user_id} нет сохранённых карт — сначала "
                "нужен Init с Recurrent='Y' (тест №5)",
            )

    try:
        result = billing_service.charge_recurrent(db, user, body.plan_id, pm)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))

    return {
        **result,
        "user_id": body.user_id,
        "payment_method_id": pm.id,
        "rebill_id_masked": pm.rebill_id[:4] + "***" + pm.rebill_id[-4:] if pm.rebill_id else None,
    }


@router.get("/admin/payment_methods/{user_id}")
async def admin_list_user_payment_methods(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Список карт любого юзера (admin only). Полезно при сертификации T-Bank
    и для саппорта.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, f"User #{user_id} not found")
    methods = billing_service.list_payment_methods(db, user)
    # Admin видит rebill_id (но не полностью — маскируем для безопасности)
    items = []
    for pm in methods:
        d = _serialize_pm(pm)
        d["rebill_id_masked"] = (
            pm.rebill_id[:4] + "***" + pm.rebill_id[-4:] if pm.rebill_id else None
        )
        d["customer_key"] = pm.customer_key
        items.append(d)
    return {"user_id": user_id, "email": user.email, "items": items}
