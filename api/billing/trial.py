"""
Бесплатный пробный период — оркестрация (eligibility / start / complete / reminders).

Механика (см. trial_system memory):
  - Карта привязывается через T-Bank AddCard (CheckType=3DS): авторизация-холд на
    0₽ БЕЗ расчёта → RebillId (чек 54-ФЗ не нужен, проверено на проде 2026-06-23).
  - Триал = ОТДЕЛЬНАЯ Subscription(is_trial=true, amount=0, plan_id=целевой план).
    sync_user_role даёт role=basic/pro; по окончании renew_expiring_subs списывает
    ПОЛНУЮ цену plan_id (charge_recurrent). Триал-строка с активным статусом.
  - Анти-абуз: один триал на идентичность (oauth/email — солёные SHA-256, 152-ФЗ).
  - Всё за флагом TRIAL_ENABLED (по умолчанию выкл).

Зависит от миграции db/migrations/014_trial.sql (новые колонки + trial_redemptions).
"""
import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import or_ as sa_or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.billing import service as billing_service
from api.billing.factory import get_payment_provider
from api.billing.plans import TRIAL_CONSENT_VERSION, get_plan, trial_days
from api.billing.provider import WebhookEvent
from api.models.subscription import Subscription
from api.models.trial_redemption import TrialRedemption
from api.models.user import User

log = logging.getLogger(__name__)

# Фиче-флаг: триал выключен по умолчанию (включаем после миграции + публикации оферты).
TRIAL_ENABLED = os.getenv("TRIAL_ENABLED", "").lower() in ("1", "true", "yes")
# Соль для хешей идентификаторов анти-абуза (152-ФЗ: храним только хеш).
_HASH_SALT = os.getenv("TRIAL_HASH_SALT", "")


# ─────────────────────────── Идентичность / хеши ───────────────────────────
def _hash(value: str) -> str:
    return hashlib.sha256((value + _HASH_SALT).encode("utf-8")).hexdigest()


def normalize_email(email: str | None) -> str | None:
    """lowercase + срез +алиаса (для всех доменов) + срез точек (gmail).

    Plus-адресација почти универсальна → режем '+suffix' везде. Точки игнорирует
    в локальной части только Gmail → убираем точки только для gmail/googlemail.
    Это смягчение; жёсткий барьер — DB-UNIQUE на email_hash (миграция 014).
    """
    if not email:
        return None
    email = email.strip().lower()
    if "@" not in email:
        return email
    local, _, domain = email.partition("@")
    local = local.split("+", 1)[0]  # +алиас режем для всех доменов
    if domain in ("gmail.com", "googlemail.com"):
        local = local.replace(".", "")
        domain = "gmail.com"
    return f"{local}@{domain}"


def _require_salt() -> None:
    """Фейл-фаст: при включённом триале соль обязательна (152-ФЗ, иначе хеши
    обратимы и анти-абуз оголён). См. ревью находки #7/#11/#15."""
    if not _HASH_SALT:
        raise ValueError(
            "TRIAL_HASH_SALT не задан — пробный период отключён в целях безопасности"
        )


def oauth_key_for(user: User) -> str | None:
    prov = getattr(user, "oauth_provider", None)
    oid = getattr(user, "oauth_id", None)
    return f"{prov}:{oid}" if prov and oid else None


def _identity_hashes(user: User) -> tuple[str | None, str | None]:
    ok = oauth_key_for(user)
    em = normalize_email(getattr(user, "email", None))
    return (_hash(ok) if ok else None, _hash(em) if em else None)


# ─────────────────────────── Eligibility ───────────────────────────
def check_trial_eligibility(db: Session, user: User) -> dict:
    """{eligible, reason}. Серверная проверка — клиенту не доверяем."""
    if getattr(user, "role", None) == "admin":
        return {"eligible": False, "reason": "admin"}
    email = getattr(user, "email", "") or ""
    if (not user.is_verified) or email.endswith("@oauth.local"):
        return {"eligible": False, "reason": "Подтвердите email перед пробным периодом"}
    if getattr(user, "trial_used", False):
        return {"eligible": False, "reason": "Пробный период уже был использован"}
    if billing_service.current_subscription(db, user) is not None:
        return {"eligible": False, "reason": "У вас уже есть активная подписка"}
    # Триал — инструмент привлечения: только тем, кто никогда не платил
    paid = db.query(Subscription).filter(
        Subscription.user_id == user.id,
        Subscription.is_trial.is_(False),
        Subscription.status.in_(("active", "expired", "cancelled", "refunded")),
    ).first()
    if paid is not None:
        return {"eligible": False, "reason": "Пробный период доступен только новым пользователям"}
    # Дедуп по кросс-аккаунтной идентичности
    oauth_h, email_h = _identity_hashes(user)
    clauses = []
    if oauth_h:
        clauses.append(TrialRedemption.oauth_hash == oauth_h)
    if email_h:
        clauses.append(TrialRedemption.email_hash == email_h)
    if clauses and db.query(TrialRedemption).filter(sa_or_(*clauses)).first() is not None:
        return {"eligible": False, "reason": "Пробный период уже был использован"}
    return {"eligible": True, "reason": None}


# ─────────────────────────── Start ───────────────────────────
def start_trial(
    db: Session, user: User, tier: str, period: str,
    base_url: str, consent: bool, ip: str | None = None,
) -> dict:
    """Создаёт pending триал-строку + инициирует привязку карты (AddCard).

    Возвращает {payment_url, request_key, ...} — фронт редиректит на payment_url.
    Завершение — complete_trial_for_user (фронт зовёт на /billing/trial-success).
    """
    if not TRIAL_ENABLED:
        raise ValueError("Пробный период временно недоступен")
    _require_salt()
    if not consent:
        raise ValueError("Требуется согласие на условия пробного периода")
    days = trial_days(tier)
    if not days:
        raise ValueError(f"Пробный период недоступен для тарифа {tier}")
    if period not in ("monthly", "yearly"):
        raise ValueError("period должен быть monthly или yearly")
    plan_id = f"{tier}_{period}"
    if not get_plan(plan_id):
        raise ValueError(f"Неизвестный план {plan_id}")

    elig = check_trial_eligibility(db, user)
    if not elig["eligible"]:
        raise ValueError(elig["reason"] or "Пробный период недоступен")

    provider = get_payment_provider()
    if provider.name != "tbank":
        raise ValueError("Пробный период доступен только с T-Bank провайдером")

    # Подчистить прошлые незавершённые привязки (повторный старт)
    for o in db.query(Subscription).filter(
        Subscription.user_id == user.id,
        Subscription.is_trial.is_(True),
        Subscription.status == "pending",
    ).all():
        o.status = "failed"
    db.flush()

    now = datetime.now(timezone.utc)
    sub = Subscription(
        user_id=user.id, tier=tier, period=period, plan_id=plan_id,
        amount=Decimal("0.00"), currency="RUB", status="pending",
        is_trial=True, trial_consent_at=now, trial_consent_version=TRIAL_CONSENT_VERSION,
    )
    db.add(sub)
    db.flush()

    base = base_url.rstrip("/")
    try:
        provider.add_customer(str(user.id), email=user.email)  # type: ignore[attr-defined]
    except Exception as e:
        log.warning("start_trial add_customer warning user=%s: %s", user.id, e)

    try:
        res = provider.add_card(  # type: ignore[attr-defined]
            str(user.id), check_type="3DS",
            success_url=f"{base}/billing/trial-success",
            fail_url=f"{base}/billing/trial-fail",
        )
    except Exception as e:
        sub.status = "failed"
        db.commit()
        log.error("start_trial add_card failed user=%s: %s", user.id, e)
        raise

    if not res.get("Success") or not res.get("RequestKey"):
        sub.status = "failed"
        db.commit()
        raise ValueError(f"AddCard error: {res.get('Message') or res.get('Details') or res}")

    # RequestKey — в ОТДЕЛЬНУЮ колонку, НЕ в yk_payment_id (тот остаётся NULL →
    # триал-строка невидима для sync_pending_for_user / cancel_by_webhook).
    sub.trial_request_key = str(res["RequestKey"])
    db.commit()
    log.info("start_trial user=%s tier=%s/%s request_key=%s", user.id, tier, period, sub.trial_request_key)
    return {
        "subscription_id": sub.id,
        "request_key": sub.trial_request_key,
        "payment_url": res.get("PaymentURL"),
        "tier": tier, "period": period, "trial_days": days,
    }


# ─────────────────────────── Complete ───────────────────────────
def complete_trial_for_user(db: Session, user: User) -> dict:
    """Завершение привязки: GetAddCardState → RebillId → активация триала.

    Идемпотентно. Фронт зовёт на /billing/trial-success после возврата от T-Bank.
    """
    _require_salt()
    sub = db.query(Subscription).filter(
        Subscription.user_id == user.id,
        Subscription.is_trial.is_(True),
        Subscription.status == "pending",
        Subscription.trial_request_key.isnot(None),
    ).order_by(Subscription.created_at.desc()).first()
    if not sub:
        # Уже активирован (повторный вызов) ИЛИ юзер уже оформил платную — мягкий ok.
        active = billing_service.current_subscription(db, user)
        if active:
            return {"ok": True, "status": "active", "subscription_id": active.id}
        return {"ok": False, "reason": "Нет ожидающей привязки карты"}

    # Повторный eligibility-гейт (TOCTOU между start и complete): юзер мог за это
    # время оформить платную подписку / стать неэлигибельным. См. ревью #8/#16.
    elig = check_trial_eligibility(db, user)
    if not elig["eligible"]:
        sub.status = "failed"
        db.commit()
        return {"ok": False, "reason": elig["reason"] or "Пробный период недоступен"}

    provider = get_payment_provider()
    state = provider.get_add_card_state(sub.trial_request_key)  # type: ignore[attr-defined]
    status = (state.get("Status") or "").upper()
    rebill_id = state.get("RebillId")
    if status != "COMPLETED" or not rebill_id:
        return {"ok": False, "status": status, "reason": "Привязка карты не завершена"}

    pan = state.get("Pan") or ""
    event = WebhookEvent(
        payment_id=str(sub.yk_payment_id), event_type="addcard",
        rebill_id=str(rebill_id), customer_key=str(user.id),
        card_last4=(pan[-4:] if len(pan) >= 4 else None),
        card_brand=state.get("CardType"),
    )
    pm = billing_service._upsert_payment_method(db, user.id, event)
    if not pm:
        return {"ok": False, "reason": "Не удалось сохранить карту"}

    # Анти-абуз: фиксируем редемпшн. UNIQUE(user_id) → повтор ловим IntegrityError.
    oauth_h, email_h = _identity_hashes(user)
    db.add(TrialRedemption(
        user_id=user.id, subscription_id=sub.id, tier=sub.tier,
        oauth_hash=oauth_h, email_hash=email_h, ip=None,
    ))
    sub_id = sub.id
    try:
        db.flush()
    except IntegrityError:
        # UNIQUE на user_id/oauth_hash/email_hash → эта идентичность уже брала
        # триал (тот же юзер = повтор/идемпотентно, ИЛИ другой аккаунт с тем же
        # oauth/email = мульти-аккаунт абуз). Триал НЕ выдаём.
        db.rollback()
        log.warning("complete_trial: identity already redeemed (user=%s) — abuse/идемпотентно", user.id)
        active = billing_service.current_subscription(db, user)
        if active:
            return {"ok": True, "status": "active", "subscription_id": active.id}
        stale = db.query(Subscription).filter(Subscription.id == sub_id).first()
        if stale and stale.status == "pending":
            stale.status = "failed"
            db.commit()
        return {"ok": False, "status": "used", "reason": "Пробный период уже был использован"}

    now = datetime.now(timezone.utc)
    sub.status = "active"
    sub.started_at = now
    sub.expires_at = now + timedelta(days=trial_days(sub.tier) or 7)
    sub.payment_method_id = pm.id
    user.trial_used = True
    db.flush()
    billing_service.sync_user_role(db, user)
    db.commit()
    log.info("complete_trial user=%s sub=%s active until %s", user.id, sub.id, sub.expires_at)
    return {
        "ok": True, "status": "active", "subscription_id": sub.id,
        "tier": sub.tier, "expires_at": sub.expires_at.isoformat(),
    }


# ─────────────────────────── Reminders (T-1) ───────────────────────────
def send_trial_reminders(db: Session, within_days: int = 3) -> dict:
    """Уведомление об окончании триала (best practice, не буква 376-ФЗ).

    Активные триалы, истекающие в ближайшие within_days, без отправленного
    напоминания и без отмены. Идемпотентно через trial_reminder_sent.
    """
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=within_days)
    subs = db.query(Subscription).filter(
        Subscription.is_trial.is_(True),
        Subscription.status == "active",
        Subscription.cancelled_at.is_(None),
        Subscription.trial_reminder_sent.is_(False),
        Subscription.expires_at > now,
        Subscription.expires_at <= cutoff,
    ).all()
    summary = {"checked": len(subs), "sent": 0}
    for sub in subs:
        user = db.query(User).filter(User.id == sub.user_id).first()
        try:
            _notify_trial_ending(user, sub, get_plan(sub.plan_id))
            sub.trial_reminder_sent = True
            summary["sent"] += 1
        except Exception as e:
            log.error("send_trial_reminders sub=%s: %s", sub.id, e)
    db.commit()
    if summary["sent"]:
        log.info("send_trial_reminders: %s", summary)
    return summary


_RU_MONTHS = (
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
)


def _ru_date(dt) -> str:
    """'30 июня' из datetime (для письма). Пусто если dt=None."""
    return f"{dt.day} {_RU_MONTHS[dt.month]}" if dt else ""


def _notify_trial_ending(user: User | None, sub: Subscription, plan) -> None:
    """Отправка уведомления «триал заканчивается, спишем X₽» на email.

    Email у триал-юзера всегда есть и верифицирован (eligibility-гейт требует
    реальный email для чека 54-ФЗ). Best-effort — исключения не роняют джобу
    (send_trial_ending_email сам ловит SMTP-ошибки).
    """
    amount = float(plan.amount) if plan else None
    email = getattr(user, "email", None)
    tier_ru = "Pro" if sub.tier == "pro" else "Basic"
    charge_date = _ru_date(sub.expires_at)
    log.info(
        "TRIAL ENDING notify: user=%s email=%s ends=%s charge=%s plan=%s",
        sub.user_id, email, sub.expires_at, amount, sub.plan_id,
    )
    if email and amount:
        from api.services.email import send_trial_ending_email
        send_trial_ending_email(
            email, tier_ru, amount, charge_date,
            display_name=getattr(user, "display_name", None),
        )
