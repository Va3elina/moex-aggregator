"""
Уведомление о предстоящем автосписании платной подписки (за 3 дня до списания).

Зачем: Единые правила защиты прав потребителей Союзного государства (ст. 43,
на них ссылается Руководство Роспотребнадзора от 02.03.2026) — уведомлять не
позднее чем за 3 дня до каждого списания абонентской платы и сообщать порядок
отказа. Плюс это главный аргумент против чарджбэков на годовых продлениях.

Когда: renew_expiring_subs списывает за RENEW_WINDOW_HOURS (24ч) до expires_at,
поэтому письмо уходит, когда до СПИСАНИЯ остаётся ≤ REMINDER_LEAD_HOURS (3 дня
+ 12ч запаса на часовой крон и простои). Ровно одно письмо на строку подписки
(каждое продление — новая строка → новое письмо к следующему продлению).

Анти-дубль и журнал — отдельная таблица renewal_reminders (создаётся на лету,
как tg_joins.ensure_tables; миграция 119 — та же DDL для порядка). Колонку в
subscriptions намеренно НЕ добавляем: ORM-модель с новой колонкой уронила бы
весь биллинг, если код задеплоят раньше ручной миграции.
"""

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from api.billing.plans import get_plan
from api.billing.service import apply_renewal_discount, renewal_terms, test_price_for
from api.models.payment_method import UserPaymentMethod
from api.models.subscription import Subscription
from api.models.user import User

log = logging.getLogger("moex_api")

MSK = ZoneInfo("Europe/Moscow")

# Должно совпадать с hours_window по умолчанию в renew_expiring_subs.
RENEW_WINDOW_HOURS = 24
# Минимум по закону — 3 дня до списания; +12ч, чтобы часовой крон и простои
# не съели запас.
NOTICE_MIN_HOURS = 72
REMINDER_LEAD_HOURS = NOTICE_MIN_HOURS + 12

_RU_MONTHS = (
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
)

_DDL = """
CREATE TABLE IF NOT EXISTS renewal_reminders (
    subscription_id INTEGER PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    charge_at       TIMESTAMPTZ NOT NULL,
    amount          NUMERIC(12, 2),
    email           VARCHAR(255),
    status          VARCHAR(16) NOT NULL
)
"""


def ensure_table(db: Session) -> None:
    db.execute(text(_DDL))
    db.commit()


def charge_at_for(sub: Subscription) -> datetime:
    """Когда renew_expiring_subs впервые попробует списать эту подписку."""
    return sub.expires_at - timedelta(hours=RENEW_WINDOW_HOURS)


def reminder_due(sub: Subscription, now: datetime) -> bool:
    """Пора ли слать письмо: до списания ≤ REMINDER_LEAD_HOURS и оно ещё впереди."""
    if not sub.expires_at:
        return False
    left = charge_at_for(sub) - now
    return timedelta(0) < left <= timedelta(hours=REMINDER_LEAD_HOURS)


def ru_date(dt: datetime) -> str:
    """'26 ноября' по Москве."""
    local = dt.astimezone(MSK)
    return f"{local.day} {_RU_MONTHS[local.month]}"


def _record(db: Session, sub: Subscription, charge_at: datetime, amount, email, status: str) -> None:
    db.execute(text("""
        INSERT INTO renewal_reminders (subscription_id, user_id, charge_at, amount, email, status)
        VALUES (:sid, :uid, :charge_at, :amount, :email, :status)
        ON CONFLICT (subscription_id) DO NOTHING
    """), {
        "sid": sub.id, "uid": sub.user_id, "charge_at": charge_at,
        "amount": amount, "email": email, "status": status,
    })
    db.commit()


def send_renewal_reminders(db: Session, now: datetime | None = None) -> dict:
    """Разослать уведомления о ближайших автосписаниях. Идемпотентно, best-effort."""
    now = now or datetime.now(timezone.utc)
    ensure_table(db)

    horizon = now + timedelta(hours=RENEW_WINDOW_HOURS + REMINDER_LEAD_HOURS)
    candidates = db.query(Subscription).filter(
        Subscription.is_trial.is_(False),           # у триала своё письмо (trial.py)
        Subscription.status == "active",
        Subscription.cancelled_at.is_(None),
        Subscription.payment_method_id.isnot(None),
        Subscription.expires_at > now,
        Subscription.expires_at <= horizon,
    ).all()

    summary = {"checked": 0, "sent": 0, "late": 0, "skipped": 0, "failed": 0}
    if not candidates:
        return summary

    already = {
        r[0] for r in db.execute(
            text("SELECT subscription_id FROM renewal_reminders WHERE subscription_id = ANY(:ids)"),
            {"ids": [s.id for s in candidates]},
        ).fetchall()
    }

    from api.services.email import send_renewal_notice_email

    for sub in candidates:
        if sub.id in already or not reminder_due(sub, now):
            continue
        summary["checked"] += 1

        # Уже продлили / купили заново этот tier — списания по этой строке не будет
        # (тот же Guard 1, что в renew_expiring_subs).
        follow_up = db.query(Subscription.id).filter(
            Subscription.user_id == sub.user_id,
            Subscription.tier == sub.tier,
            Subscription.id != sub.id,
            Subscription.status.in_(("pending", "active")),
            Subscription.created_at > sub.created_at,
        ).first()
        pm = db.query(UserPaymentMethod).filter(UserPaymentMethod.id == sub.payment_method_id).first()
        user = db.query(User).filter(User.id == sub.user_id).first()
        if follow_up or not pm or pm.deleted_at is not None or not user:
            summary["skipped"] += 1
            continue

        plan_id, discount_pct, discount_rub, _ = renewal_terms(sub)
        plan = get_plan(plan_id)
        if not plan:
            summary["skipped"] += 1
            continue
        amount = apply_renewal_discount(test_price_for(user, plan.amount), discount_pct, discount_rub)
        charge_at = charge_at_for(sub)
        email = getattr(user, "email", None)

        if not email:
            log.warning("renewal notice: sub_%s user_%s без email — уведомить нечем", sub.id, user.id)
            _record(db, sub, charge_at, amount, None, "no_email")
            summary["skipped"] += 1
            continue

        late = (charge_at - now) < timedelta(hours=NOTICE_MIN_HOURS)
        try:
            ok = send_renewal_notice_email(
                email,
                tier_ru="Pro" if plan.tier == "pro" else plan.tier.capitalize(),
                period=plan.period,
                amount=float(amount),
                charge_date=ru_date(charge_at),
                paid_until=ru_date(sub.expires_at),
                payment_label=pm.display_name,
                display_name=getattr(user, "display_name", None),
            )
        except Exception as e:  # send_* не должен бросать, но крон не роняем
            log.error("renewal notice: sub_%s send failed: %s", sub.id, e)
            ok = False

        if not ok:
            # Не записываем — следующий час попробует снова.
            summary["failed"] += 1
            continue

        _record(db, sub, charge_at, amount, email, "late" if late else "sent")
        summary["sent"] += 1
        if late:
            summary["late"] += 1
            log.warning(
                "renewal notice: sub_%s отправлено позже 3 дней до списания (%s)",
                sub.id, charge_at - now,
            )

    if summary["sent"] or summary["failed"]:
        log.info("send_renewal_reminders: %s", summary)
    return summary
