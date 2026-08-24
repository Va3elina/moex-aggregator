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
from api.billing import trial as trial_service
from api.billing.factory import (
    get_payment_provider,
    get_provider_for,
    get_yookassa_provider,
)
from api.billing.plans import TRIAL_DAYS, get_plan, list_public_plans, tiers_grouped
from api.billing.tiers import user_tier
from api.database import get_db
from api.models.payment_method import UserPaymentMethod
from api.models.subscription import Subscription
from api.models.user import User
from api.routers.auth import get_current_user, get_current_user_optional, require_admin

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
    rail: str = "card"            # 'card' (redirect) | 'sbp' (QR, рекуррентный СБП)


class CheckoutResponse(BaseModel):
    subscription_id: int
    payment_id: str
    confirmation_url: str
    plan_id: str
    tier: str
    amount: float
    rail: str = "card"
    # Заполнены только при rail='sbp': картинка QR (data-URL) и СБП-ссылка
    # для мобильного диплинка. Для карты — None (фронт идёт по confirmation_url).
    qr_image: str | None = None
    qr_payload: str | None = None


class StatusResponse(BaseModel):
    tier: str                             # 'guest' / 'free' / 'basic' / 'pro' / 'premium' / 'admin'
    is_active: bool
    subscription_id: int | None = None
    plan_id: str | None = None
    started_at: str | None = None
    expires_at: str | None = None
    cancelled_at: str | None = None      # NULL → активна и продлится, NOT NULL → отменена, доступ до expires_at
    retention_eligible: bool = False     # можно ли предложить retention-скидку 40% (active + автопродление + не использована)
    # === Trial ===
    is_trial: bool = False               # текущая подписка — пробный период
    trial_ends_at: str | None = None     # когда кончится триал (= expires_at) и спишется next_charge
    trial_eligible: bool = False         # можно ли предложить начать триал (только если нет подписки)
    next_charge_amount: float | None = None  # сколько спишется по окончании триала (полная цена plan_id)
    next_charge_at: str | None = None    # дата первого платного списания (= trial_ends_at)
    # === Founder offer (персональный подарочный период, whitelist через env) ===
    # None у всех, кроме whitelisted юзеров без активной подписки/триала. Управляет
    # персональным баннером FounderOfferBanner. {tier, period, days, amount}.
    founder_offer: dict | None = None


# ═══════════════════════════════════════════════════════════════════════════════
#  1. GET /plans — список тарифов (публичный)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/features")
async def features_matrix():
    """
    Возвращает tier × indicator × feature_flags матрицу для frontend.

    Frontend кэширует ответ в AuthContext и использует для:
      - TierGate компонент (замочки на UI)
      - условный рендеринг (показ/скрытие режимов индикаторов)
      - PricingPage сравнительная таблица фич

    Источник истины — api/billing/features.py. Никаких изменений вне этого файла.
    """
    from api.billing.features import matrix_for_frontend
    return matrix_for_frontend()


@router.get("/plans")
async def list_plans(user: User | None = Depends(get_current_user_optional)):
    """
    Возвращает все тарифы для отображения на Pricing-странице.
    Структура — сгруппировано по tier (free / basic / pro),
    внутри каждого — monthly и yearly варианты.

    Для провайдера tbank дополнительно отдаём terminal_key — он публичный
    идентификатор магазина (не секрет), фронт использует его для
    PaymentIntegration.init() при подключении JS SDK (SpeedPay кнопок).
    Password остаётся серверным секретом.
    """
    # Роутинг как на checkout: новому юзеру отдаём terminal_key ДЕМО-терминала,
    # иначе SpeedPay-кнопки инициализировались бы боевым ключом, а Init шёл бы
    # на демо-терминал. Гостю — дефолт (оплатить он всё равно не может).
    provider = get_provider_for(user) if user is not None else get_payment_provider()
    response: dict = {
        "provider": provider.name,
        "currency": "RUB",
        "tiers": tiers_grouped(),
        # Публичный флаг для гостевого trial-тизера (гость не может звать /status).
        # trial_eligible для залогиненных по-прежнему считается в /status.
        "trial_enabled": trial_service.TRIAL_ENABLED,
        "trial_days": dict(TRIAL_DAYS),
    }
    if provider.name in ("tbank", "tbank_demo"):
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
    # Retention-оффер доступен только если: подписка активна, не отменена, есть
    # автопродление (payment_method_id) и скидку ещё не использовали.
    retention_eligible = bool(
        sub is not None
        and sub.status == "active"
        and sub.cancelled_at is None
        and sub.payment_method_id is not None
        and not getattr(user, "retention_used", False)
    )

    # Trial-поля: если активная подписка — триал, отдаём дату окончания и сумму
    # будущего списания; если подписки нет — считаем eligibility под CTA «попробовать».
    is_trial = bool(sub and getattr(sub, "is_trial", False))
    next_amount = None
    next_at = None
    if is_trial:
        plan = get_plan(sub.plan_id)
        next_amount = float(plan.amount) if plan else None
        next_at = sub.expires_at.isoformat() if sub.expires_at else None
    trial_eligible = bool(
        sub is None
        and trial_service.TRIAL_ENABLED
        and trial_service.check_trial_eligibility(db, user)["eligible"]
    )
    # Founder-оффер (whitelist через env) — отдельно от публичного trial_eligible,
    # чтобы у обычных юзеров CTA не появлялся. None для всех, кроме whitelisted.
    founder_offer = trial_service.founder_offer_state(db, user)

    return StatusResponse(
        tier=user_tier(user),
        is_active=sub is not None,
        subscription_id=sub.id if sub else None,
        plan_id=sub.plan_id if sub else None,
        started_at=sub.started_at.isoformat() if sub and sub.started_at else None,
        expires_at=sub.expires_at.isoformat() if sub and sub.expires_at else None,
        cancelled_at=sub.cancelled_at.isoformat() if sub and sub.cancelled_at else None,
        retention_eligible=retention_eligible,
        is_trial=is_trial,
        trial_ends_at=next_at,
        trial_eligible=trial_eligible,
        next_charge_amount=next_amount,
        next_charge_at=next_at,
        founder_offer=founder_offer,
    )


@router.get("/history")
async def my_history(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = 20,
):
    """
    История платежей текущего user'а — последние N подписок (любого статуса:
    active / pending / cancelled / refunded / expired / failed).

    Используется на странице /profile в блоке «История платежей».
    """
    subs = billing_service.subscription_history(db, user, limit=min(limit, 100))
    return {
        "items": [
            {
                "id": s.id,
                "tier": s.tier,
                "plan_id": s.plan_id,
                "amount": float(s.amount) if s.amount is not None else None,
                "currency": s.currency,
                "status": s.status,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "started_at": s.started_at.isoformat() if s.started_at else None,
                "expires_at": s.expires_at.isoformat() if s.expires_at else None,
                "cancelled_at": s.cancelled_at.isoformat() if s.cancelled_at else None,
                "yk_payment_id": s.yk_payment_id,  # для саппорта (показывать masked в UI)
            }
            for s in subs
        ]
    }


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

    if body.rail not in ("card", "sbp"):
        raise HTTPException(400, f"Unknown rail: {body.rail}")

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
            rail=body.rail,
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
            # Триал: 1₽-привязка принадлежит is_trial-строке → активируем ТРИАЛ
            # (N дней, 0₽) + сохраняем карту + возврат 1₽, а не платную подписку.
            from api.models.subscription import Subscription as _Sub
            sub = db.query(_Sub).filter(_Sub.yk_payment_id == event.payment_id).first()
            if sub is not None and getattr(sub, "is_trial", False):
                trial_service.complete_trial_binding(db, sub, event)
            else:
                billing_service.activate_from_webhook(db, event)
        elif event.event_type in ("payment.canceled", "refund.succeeded"):
            billing_service.cancel_by_webhook(db, event)
        else:
            log.info("Webhook: unknown event type, ignoring: %s", event.event_type)
    except Exception as e:
        log.error("Webhook processing error: %s", e, exc_info=True)
        # НЕ пробрасываем — чтобы провайдер не ретраил. Ошибки смотрим в логах.

    return ok_response


@router.post("/webhook/yookassa", status_code=status.HTTP_200_OK)
async def webhook_yookassa(request: Request, db: Session = Depends(get_db)):
    """
    Webhook ЮKassa (отдельный путь — на проде живут ДВА провайдера: T-Bank
    дефолтом, ЮKassa для тест-юзеров; общий /webhook разбирает T-Bank Token).

    ЮKassa НЕ подписывает уведомления: parse_webhook берёт из тела только id
    платежа и строит событие из ответа GET /payments/{id} — подделка тела
    бессильна. ЮKassa ждёт HTTP 200 (иначе ретраит до суток).

    URL прописывается в ЛК: Интеграция → HTTP-уведомления → события
    payment.succeeded / payment.canceled / refund.succeeded.
    """
    provider = get_yookassa_provider()
    if provider is None:
        # env не заданы — фича спит; 404 чтобы случайные POST не выглядели ок
        raise HTTPException(404)

    raw_body = await request.body()
    event = provider.parse_webhook(raw_body, dict(request.headers))
    if not event:
        log.warning("YooKassa webhook: не распарсилось/не подтвердилось (%d bytes)", len(raw_body))
        return {"ok": True}  # 200 — чтобы не ретраила мусор

    log.info("YooKassa webhook event=%s payment_id=%s", event.event_type, event.payment_id)
    try:
        if event.event_type == "payment.succeeded":
            billing_service.activate_from_webhook(db, event)
        elif event.event_type in ("payment.canceled", "refund.succeeded"):
            billing_service.cancel_by_webhook(db, event)
    except Exception as e:
        log.error("YooKassa webhook processing error: %s", e, exc_info=True)
        # НЕ пробрасываем — ошибки смотрим в логах, ретрай ЮKassa не поможет.
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
#  4b. POST /retention/accept — скидка 40% вместо отмены (1 раз на аккаунт)
# ═══════════════════════════════════════════════════════════════════════════════

RETENTION_DISCOUNT_PCT = 40  # скидка на 1 следующее продление при отказе от отмены


class RetentionRequest(BaseModel):
    subscription_id: int


@router.post("/retention/accept")
async def retention_accept(
    body: RetentionRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Принять retention-предложение вместо отмены: скидка 40% на СЛЕДУЮЩЕЕ продление.
    Подписка НЕ отменяется (cancelled_at не ставится) — остаётся активной и
    продлится со скидкой. Одноразово на аккаунт (user.retention_used). Скидка
    применяется в charge_recurrent при ближайшем рекуррентном списании.
    """
    if getattr(user, "retention_used", False):
        raise HTTPException(400, "Скидка уже была использована ранее")
    sub = db.query(Subscription).filter(
        Subscription.id == body.subscription_id,
        Subscription.user_id == user.id,
    ).first()
    if not sub:
        raise HTTPException(404, "Подписка не найдена")
    if sub.status != "active" or sub.cancelled_at is not None:
        raise HTTPException(400, "Скидка доступна только для активной неотменённой подписки")
    if sub.payment_method_id is None:
        raise HTTPException(400, "Нет автопродления — скидку не к чему применить")

    sub.discount_pct = RETENTION_DISCOUNT_PCT
    user.retention_used = True
    db.commit()
    log.info("User #%s accepted retention: %d%% off next renewal of sub #%s",
             user.id, RETENTION_DISCOUNT_PCT, sub.id)
    return {"ok": True, "discount_pct": RETENTION_DISCOUNT_PCT, "subscription_id": sub.id}


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
#  5d. TRIAL — старт пробного периода (привязка карты) + завершение
# ═══════════════════════════════════════════════════════════════════════════════

class TrialStartRequest(BaseModel):
    tier: str                     # 'basic' / 'pro'
    period: str = "monthly"       # что списать по окончании: monthly / yearly
    consent: bool = False         # явное согласие на автосписание (ГК 438) — обязательно


@router.post("/trial/start")
async def trial_start(
    body: TrialStartRequest,
    request: Request,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Старт бесплатного пробного периода: создаёт pending триал-строку и инициирует
    привязку карты через T-Bank AddCard (0₽ авторизация, без расчёта). Возвращает
    payment_url — фронт редиректит туда; после возврата зовёт POST /trial/complete.

    Гейтится TRIAL_ENABLED + check_trial_eligibility (анти-абуз). Требует consent=true.
    """
    origin = request.headers.get("origin") or request.headers.get("referer") or "https://framedata.ru"
    ip = request.client.host if request.client else None
    try:
        return trial_service.start_trial(
            db, user, body.tier, body.period, origin.rstrip("/"),
            consent=body.consent, ip=ip,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        log.error("trial_start failed user=%s: %s", user.id, e, exc_info=True)
        raise HTTPException(502, "Не удалось начать пробный период. Попробуйте позже.")


@router.post("/trial/complete")
async def trial_complete(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Завершение привязки карты: GetAddCardState → RebillId → активация триала.
    Фронт зовёт на /billing/trial-success. Идемпотентно.
    """
    try:
        return trial_service.complete_trial_for_user(db, user)
    except Exception as e:
        log.error("trial_complete failed user=%s: %s", user.id, e, exc_info=True)
        raise HTTPException(502, "Не удалось завершить привязку карты.")


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
    tier: str                  # только 'basic' / 'pro' — premium выведен из TIER_LEVELS
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


@router.delete("/admin/invites/{token}/purge")
async def admin_delete_invite(
    token: str,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Удалить отработавший токен из списка навсегда."""
    try:
        ok = invite_service.delete_invite(db, admin, token)
    except invite_service.InviteDeleteError as e:
        raise HTTPException(400, str(e))
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
    """JSON-сериализация способа оплаты для UI «Способы оплаты» в ЛК."""
    return {
        "id": pm.id,
        "provider": pm.provider,
        "method_type": pm.method_type,  # 'card' / 'sbp' — UI ветвит бейдж и подпись
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
    Отвязка способа оплаты юзером: soft-delete + стирание рекуррент-токенов
    (rebill_id/account_token) из БД — компл-требование ЮKassa/T-Bank.
    Подписки, которые на него ссылаются, работают до expires_at, но
    auto-renewal больше не сработает.
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


# ═══════════════════════════════════════════════════════════════════════════════
#  12. ADMIN — AddCard probe (тест привязки карты без оплаты для пробного периода)
# ═══════════════════════════════════════════════════════════════════════════════

class AddCardTestRequest(BaseModel):
    check_type: str = "3DS"   # NO / HOLD / 3DS / 3DSHOLD


@router.post("/admin/addcard_test")
async def admin_addcard_test(
    body: AddCardTestRequest,
    request: Request,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    ИЗОЛИРОВАННЫЙ ТЕСТ привязки карты через T-Bank AddCard (для пробного периода).

    Проверяет блокеры до постройки полного триала:
      - включён ли AddCard на терминале (если нет — T-Bank вернёт ошибку);
      - возвращается ли RebillId (через GetAddCardState);
      - формируется ли кассовый чек (НЕ должен при HOLD/3DS — это холд+отмена).

    Только admin. Деньги за подписку не списываются (HOLD/3DS делают холд+возврат
    1₽). CustomerKey = str(admin.id). Открой PaymentURL, введи карту, затем дёрни
    GET /admin/addcard_state/{request_key}.
    """
    provider = get_payment_provider()
    if provider.name != "tbank":
        raise HTTPException(400, f"AddCard доступен только на T-Bank (сейчас {provider.name})")
    if body.check_type not in ("NO", "HOLD", "3DS", "3DSHOLD"):
        raise HTTPException(400, "check_type: NO | HOLD | 3DS | 3DSHOLD")

    ck = str(admin.id)
    # AddCustomer идемпотентен; ошибку «уже существует» проглатываем
    try:
        provider.add_customer(ck, email=admin.email)  # type: ignore[attr-defined]
    except Exception as e:
        log.warning("addcard_test: add_customer warning: %s", e)

    # URL НЕ передаём: T-Bank AddCard их не honor-ит из запроса (204/9) и не
    # откатывается на кабинет, если они присланы. Редирект — из настроек терминала.
    try:
        res = provider.add_card(ck, check_type=body.check_type)  # type: ignore[attr-defined]
    except Exception as e:
        log.error("addcard_test: add_card failed: %s", e)
        raise HTTPException(502, f"AddCard error: {e}")

    log.info("addcard_test: admin=%s check_type=%s -> %s", admin.id, body.check_type, res.get("RequestKey"))
    return {
        "ok": bool(res.get("Success")),
        "request_key": res.get("RequestKey"),
        "payment_url": res.get("PaymentURL"),
        "raw": res,
    }


@router.get("/admin/addcard_state/{request_key}")
async def admin_addcard_state(
    request_key: str,
    admin: User = Depends(require_admin),
):
    """
    Статус привязки карты по RequestKey (T-Bank GetAddCardState).
    При COMPLETED содержит RebillId/CardId/Pan — это и проверяем.
    """
    provider = get_payment_provider()
    if provider.name != "tbank":
        raise HTTPException(400, f"AddCard доступен только на T-Bank (сейчас {provider.name})")
    try:
        res = provider.get_add_card_state(request_key)  # type: ignore[attr-defined]
    except Exception as e:
        raise HTTPException(502, f"GetAddCardState error: {e}")
    return {
        "ok": bool(res.get("Success")),
        "status": res.get("Status"),
        "rebill_id": res.get("RebillId"),
        "card_id": res.get("CardId"),
        "pan": res.get("Pan"),
        "raw": res,
    }
