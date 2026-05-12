"""
Бизнес-логика подписок — независимая от конкретного провайдера.

Основные операции:
  create_checkout_for_user  — создать платёж + subscription(pending) и вернуть URL
  activate_from_webhook     — после webhook.payment.succeeded → активировать подписку + поднять role
  cancel_by_webhook         — после payment.canceled / refund.succeeded → отменить
  sync_user_role            — посчитать tier по активным подпискам и записать в users.role
  expire_overdue            — для cron: найти и expire-нуть подписки с expires_at < now

Принципы:
  - Все мутации состояния проходят через этот файл (не через router напрямую).
  - Router только валидирует ввод и вызывает service.
  - Для idempotency — lookup по yk_payment_id (webhook могут прийти дважды).
"""
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from api.billing.factory import get_payment_provider
from api.billing.plans import TIER_LEVELS, get_plan
from api.billing.provider import WebhookEvent
from api.models.subscription import Subscription
from api.models.user import User

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  1. CHECKOUT — создание платёжной сессии для пользователя
# ═══════════════════════════════════════════════════════════════════════════════

def create_checkout_for_user(
    db: Session,
    user: User,
    plan_id: str,
    return_url: str,
    widget_mode: bool = False,
) -> dict:
    """
    Создаёт subscription(pending) + платёж у провайдера.

    Возвращает {subscription_id, payment_id, confirmation_url, plan_id, amount}.
    Frontend редиректит пользователя на confirmation_url.
    """
    plan = get_plan(plan_id)
    if not plan:
        raise ValueError(f"Unknown plan_id: {plan_id}")

    # 1. Создаём запись subscription со статусом 'pending'
    sub = Subscription(
        user_id=user.id,
        tier=plan.tier,
        period=plan.period,
        plan_id=plan.plan_id,
        amount=plan.amount,
        currency="RUB",
        status="pending",
    )
    db.add(sub)
    db.flush()  # нужен sub.id до commit'а

    # 2. Зовём провайдера. user.email/phone используется T-Bank провайдером
    # для построения Receipt (54-ФЗ), остальные провайдеры эти параметры игнорируют.
    provider = get_payment_provider()
    customer_email = getattr(user, "email", None) or None
    customer_phone = getattr(user, "phone", None) or None
    try:
        session = provider.create_checkout(
            amount=plan.amount,
            currency="RUB",
            description=f"{plan.title} — user #{user.id}",
            return_url=return_url,
            metadata={
                "user_id": str(user.id),
                "subscription_id": str(sub.id),
                "plan_id": plan.plan_id,
            },
            customer_email=customer_email,
            customer_phone=customer_phone,
            widget_mode=widget_mode,
        )
    except Exception as e:
        # Платёж не создался — помечаем fail и пробрасываем
        sub.status = "failed"
        db.commit()
        log.error("create_checkout_for_user: provider failed: %s", e)
        raise

    # 3. Сохраняем payment_id от провайдера в нашу подписку
    sub.yk_payment_id = session.payment_id
    db.commit()

    log.info(
        "Created subscription #%s (pending) for user #%s: plan=%s amount=%.2f payment_id=%s",
        sub.id, user.id, plan.plan_id, plan.amount, session.payment_id,
    )

    return {
        "subscription_id": sub.id,
        "payment_id": session.payment_id,
        "confirmation_url": session.confirmation_url,
        "plan_id": plan.plan_id,
        "tier": plan.tier,
        "amount": plan.amount,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  2. WEBHOOK — активация / отмена
# ═══════════════════════════════════════════════════════════════════════════════

def _verify_with_provider(sub: Subscription) -> bool:
    """
    Дополнительная проверка: делаем GET /payments/{id} у провайдера, чтобы
    убедиться что платёж реально успешен. Защита от подделки webhook'ов
    (ЮKassa их не подписывает).
    """
    provider = get_payment_provider()
    if provider.name == "stub":
        return True  # stub доверяем всегда
    if provider.name not in ("yookassa", "tbank"):
        return True  # другие провайдеры — настраивать отдельно
    # YooKassaProvider и TBankProvider имеют verify_payment
    info = provider.verify_payment(sub.yk_payment_id)  # type: ignore[attr-defined]
    if info is None:
        log.warning(
            "verify failed: payment %s not found in %s",
            sub.yk_payment_id, provider.name,
        )
        return False
    # YooKassa: status == "succeeded"  /  T-Bank: Status == "CONFIRMED"
    if provider.name == "yookassa":
        return info.get("status") == "succeeded"
    # T-Bank
    return info.get("Status", "").upper() == "CONFIRMED"


def activate_from_webhook(db: Session, event: WebhookEvent) -> Subscription | None:
    """
    Активирует подписку после webhook.payment.succeeded.
    Идемпотентна — повторный вызов с тем же event ничего не сломает.
    """
    sub = db.query(Subscription).filter(
        Subscription.yk_payment_id == event.payment_id
    ).first()
    if not sub:
        log.warning("activate_from_webhook: no subscription for payment_id=%s", event.payment_id)
        return None

    if sub.status == "active":
        log.info("activate_from_webhook: already active (idempotent) sub=%s", sub.id)
        return sub  # идемпотентно

    # Double-check с провайдером
    if not _verify_with_provider(sub):
        log.warning("activate_from_webhook: verification failed for sub=%s", sub.id)
        return None

    # Активируем
    plan = get_plan(sub.plan_id)
    now = datetime.now(timezone.utc)
    sub.status = "active"
    sub.started_at = now
    sub.expires_at = now + timedelta(days=plan.duration_days) if plan else None
    sub.yk_method = event.payment_method

    db.flush()

    # Поднимаем роль user'а если нужно
    user = db.query(User).filter(User.id == sub.user_id).first()
    if user:
        sync_user_role(db, user)

    db.commit()
    log.info(
        "Activated subscription #%s for user #%s: tier=%s expires=%s method=%s",
        sub.id, sub.user_id, sub.tier, sub.expires_at, event.payment_method,
    )
    return sub


def cancel_by_webhook(db: Session, event: WebhookEvent) -> Subscription | None:
    """Обработка webhook.payment.canceled / refund.succeeded."""
    sub = db.query(Subscription).filter(
        Subscription.yk_payment_id == event.payment_id
    ).first()
    if not sub:
        return None

    now = datetime.now(timezone.utc)
    if event.event_type == "refund.succeeded":
        sub.status = "refunded"
    else:
        sub.status = "failed" if sub.status == "pending" else "cancelled"
    sub.cancelled_at = now

    db.flush()

    user = db.query(User).filter(User.id == sub.user_id).first()
    if user:
        sync_user_role(db, user)

    db.commit()
    log.info("Cancelled subscription #%s (event=%s)", sub.id, event.event_type)
    return sub


# ═══════════════════════════════════════════════════════════════════════════════
#  3. Sync user.role — пересчёт роли по активным подпискам
# ═══════════════════════════════════════════════════════════════════════════════

def sync_user_role(db: Session, user: User) -> str:
    """
    Пересчитывает users.role по активным подпискам:
      — если есть active subscription с expires_at > now → берём max tier среди активных
      — если нет → role = 'free' (было 'user' — мигрируем на 'free')
    admin НЕ трогаем (это ручная роль, не от подписки).
    """
    if user.role == "admin":
        return "admin"

    now = datetime.now(timezone.utc)
    active_subs = db.query(Subscription).filter(
        Subscription.user_id == user.id,
        Subscription.status == "active",
        Subscription.expires_at > now,
    ).all()

    if not active_subs:
        new_role = "free"
    else:
        # Берём наивысший tier среди активных
        best_tier = max(active_subs, key=lambda s: TIER_LEVELS.get(s.tier, 0)).tier
        new_role = best_tier

    if user.role != new_role:
        log.info("sync_user_role: user #%s %s → %s", user.id, user.role, new_role)
        user.role = new_role
        db.flush()

    return new_role


# ═══════════════════════════════════════════════════════════════════════════════
#  4. Cron — expire просроченные подписки
# ═══════════════════════════════════════════════════════════════════════════════

def expire_overdue(db: Session) -> int:
    """
    Ищет подписки со status='active' AND expires_at < now и переводит в 'expired'.
    Пересчитывает role у затронутых пользователей.
    Запускать раз в час cron'ом.
    """
    now = datetime.now(timezone.utc)
    overdue = db.query(Subscription).filter(
        Subscription.status == "active",
        Subscription.expires_at < now,
    ).all()

    affected_users = set()
    for sub in overdue:
        sub.status = "expired"
        affected_users.add(sub.user_id)

    if affected_users:
        db.flush()
        for uid in affected_users:
            u = db.query(User).filter(User.id == uid).first()
            if u:
                sync_user_role(db, u)

    db.commit()
    log.info("expire_overdue: %d subscriptions expired, %d users updated",
             len(overdue), len(affected_users))
    return len(overdue)


# ═══════════════════════════════════════════════════════════════════════════════
#  5. Для UI — текущий статус пользователя
# ═══════════════════════════════════════════════════════════════════════════════

def current_subscription(db: Session, user: User) -> Subscription | None:
    """Активная подписка пользователя (или None если Free)."""
    now = datetime.now(timezone.utc)
    return db.query(Subscription).filter(
        Subscription.user_id == user.id,
        Subscription.status == "active",
        Subscription.expires_at > now,
    ).order_by(Subscription.expires_at.desc()).first()


def subscription_history(db: Session, user: User, limit: int = 20) -> list[Subscription]:
    """Последние N подписок пользователя (для ProfilePage → История платежей)."""
    return db.query(Subscription).filter(
        Subscription.user_id == user.id
    ).order_by(Subscription.created_at.desc()).limit(limit).all()
