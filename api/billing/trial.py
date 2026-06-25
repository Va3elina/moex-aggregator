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
from api.billing.plans import TRIAL_BIND_AMOUNT, TRIAL_CONSENT_VERSION, get_plan, trial_days
from api.billing.provider import WebhookEvent
from api.models.subscription import Subscription
from api.models.trial_redemption import TrialRedemption
from api.models.user import User

log = logging.getLogger(__name__)

# Фиче-флаг: триал выключен по умолчанию (включаем после миграции + публикации оферты).
TRIAL_ENABLED = os.getenv("TRIAL_ENABLED", "").lower() in ("1", "true", "yes")
# Соль для хешей идентификаторов анти-абуза (152-ФЗ: храним только хеш).
_HASH_SALT = os.getenv("TRIAL_HASH_SALT", "")
# Способ привязки карты: "init" — платёж 1₽ + возврат (текущий, рабочий);
# "addcard" — T-Bank AddCard (0₽, без чеков). AddCard включать ТОЛЬКО после того
# как поддержка T-Bank задаст Success/Fail Add Card URL на терминале (иначе
# редирект не вернёт клиента). Дефолт init — переключаемо env без передеплоя.
TRIAL_BIND_METHOD = os.getenv("TRIAL_BIND_METHOD", "init").lower()

# ─────────────────────────── Founder offer (персональный жест) ───────────────
# Точечный обход «триал только новым пользователям» для конкретных user_id
# (напр. первый подписчик, у кого подписка истекла, т.к. при покупке автопродление
# ещё не было настроено). Whitelist через env (как TRIAL_ENABLED — dark launch):
# пока FOUNDER_OFFER_USER_IDS пуст, поведение прода = ноль изменений.
#   FOUNDER_OFFER_USER_IDS — "21" или "21,42" (пусто = выключено)
#   FOUNDER_OFFER_DAYS     — длительность бесплатного периода (по умолч. 30)
#   FOUNDER_OFFER_TIER     — какой tier дарим (по умолч. pro)
# Founder НЕ трогает публичную check_trial_eligibility: обход живёт отдельной
# веткой в start_trial/complete_trial_for_user, поэтому /status.trial_eligible
# для остальных юзеров не меняется (никаких утечек CTA).
FOUNDER_OFFER_USER_IDS = {
    int(x) for x in os.getenv("FOUNDER_OFFER_USER_IDS", "").replace(" ", "").split(",")
    if x.strip().isdigit()
}
FOUNDER_OFFER_DAYS = int(os.getenv("FOUNDER_OFFER_DAYS", "30") or "30")
FOUNDER_OFFER_TIER = os.getenv("FOUNDER_OFFER_TIER", "pro") or "pro"


def is_founder_offer(user: User | None) -> bool:
    """User в founder-whitelist (имеет право на персональный подарочный период)."""
    return bool(user is not None and getattr(user, "id", None) in FOUNDER_OFFER_USER_IDS)


def founder_offer_state(db: Session, user: User) -> dict | None:
    """Состояние founder-оффера для баннера. None = не показывать.

    Показываем если: юзер в whitelist, фича-триал включён, триал ещё не брал,
    нет активной подписки, email верифицирован (нужен для чека 54-ФЗ при
    конверсии). После активации (trial_used=True / появилась активная подписка)
    → None → баннер сам исчезает, серверного dismiss-состояния не требуется.
    """
    if not TRIAL_ENABLED or not is_founder_offer(user):
        return None
    if getattr(user, "trial_used", False):
        return None
    if billing_service.current_subscription(db, user) is not None:
        return None
    email = getattr(user, "email", "") or ""
    if (not user.is_verified) or email.endswith("@oauth.local"):
        return None
    plan = get_plan(f"{FOUNDER_OFFER_TIER}_monthly")
    return {
        "tier": FOUNDER_OFFER_TIER,
        "period": "monthly",
        "days": FOUNDER_OFFER_DAYS,
        "amount": float(plan.amount) if plan else None,
    }


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
    """Создаёт pending триал-строку + инициирует привязку карты через Init 1₽.

    Привязка идёт обычным платежом /Init на TRIAL_BIND_AMOUNT (1₽) с Recurrent="Y"
    — у него редирект работает штатно (в отличие от AddCard, чей redirect нерабочий:
    204/ErrorCode 9). 1₽ возвращается после получения RebillId (complete_trial_binding
    в webhook). Возвращает {payment_url, ...} — фронт редиректит на payment_url
    (pay.tbank.ru). Завершение — в webhook (payment.succeeded → complete_trial_binding).
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

    # Founder: обходим только «триал доступен новым пользователям» (он уже платил).
    # Базовые гарантии держим: нет активной подписки + верифицированный email
    # (для чека 54-ФЗ при конверсии) + consent/salt (проверены выше). Длительность
    # — FOUNDER_OFFER_DAYS (30), а не стандартные trial_days(tier)=7.
    if is_founder_offer(user):
        if billing_service.current_subscription(db, user) is not None:
            raise ValueError("У вас уже есть активная подписка")
        em = getattr(user, "email", "") or ""
        if (not user.is_verified) or em.endswith("@oauth.local"):
            raise ValueError("Подтвердите email перед активацией")
        days = FOUNDER_OFFER_DAYS
    else:
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

    # AddCard-путь (за флагом TRIAL_BIND_METHOD=addcard): 0₽-привязка, без чеков.
    # Init-путь ниже — текущий рабочий по умолчанию.
    if TRIAL_BIND_METHOD == "addcard":
        return _start_addcard_bind(db, sub, user, tier, period, days)

    base = base_url.rstrip("/")
    # Привязка карты через Init 1₽ + Recurrent="Y" — редирект на /billing/trial-success
    # работает штатно. 1₽ возвращается в complete_trial_binding после получения
    # RebillId. Чек прихода 54-ФЗ бьётся автоматически (create_checkout→_build_receipt),
    # на возврат — чек возврата. yk_payment_id хранит 1₽-платёж до активации;
    # complete_trial_binding обнуляет его (чтобы refund-webhook не погасил триал).
    try:
        session = provider.create_checkout(  # type: ignore[attr-defined]
            amount=TRIAL_BIND_AMOUNT, currency="RUB",
            description=f"Привязка карты для пробного периода — user #{user.id}",
            return_url=f"{base}/billing/trial-success",
            metadata={
                "user_id": str(user.id),
                "subscription_id": str(sub.id),
                "plan_id": plan_id,
                "trial_bind": "1",
            },
            customer_email=getattr(user, "email", None) or None,
            customer_phone=getattr(user, "phone", None) or None,
            recurrent=True,
            customer_key=str(user.id),
        )
    except Exception as e:
        sub.status = "failed"
        db.commit()
        log.error("start_trial create_checkout (1₽ bind) failed user=%s: %s", user.id, e)
        raise

    sub.yk_payment_id = session.payment_id
    db.commit()
    log.info("start_trial user=%s tier=%s/%s payment_id=%s (1₽ bind)", user.id, tier, period, sub.yk_payment_id)
    return {
        "subscription_id": sub.id,
        "payment_id": session.payment_id,
        "payment_url": session.confirmation_url,
        "tier": tier, "period": period, "trial_days": days,
    }


# ─────────────── AddCard-привязка (за флагом, 0₽, без чеков) ───────────────
def _start_addcard_bind(db: Session, sub: Subscription, user: User,
                        tier: str, period: str, days: int) -> dict:
    """Инициировать привязку карты через T-Bank AddCard (CheckType=3DS, 0₽, без
    расчёта/чеков). RequestKey хранится в sub.trial_request_key (yk_payment_id
    остаётся None → возврат в complete_trial_binding = no-op). Success/Fail Add Card
    URL заданы НА ТЕРМИНАЛЕ поддержкой T-Bank (не в запросе). Завершение —
    complete_trial_for_user → _complete_addcard (GetAddCardState)."""
    provider = get_payment_provider()
    try:
        provider.add_customer(str(user.id), email=getattr(user, "email", None))  # type: ignore[attr-defined]
    except Exception as e:
        log.warning("start_trial addcard add_customer user=%s: %s", user.id, e)
    try:
        res = provider.add_card(str(user.id), check_type="3DS")  # type: ignore[attr-defined]
    except Exception as e:
        sub.status = "failed"
        db.commit()
        log.error("start_trial add_card failed user=%s: %s", user.id, e)
        raise
    if not res.get("Success") or not res.get("RequestKey") or not res.get("PaymentURL"):
        sub.status = "failed"
        db.commit()
        raise ValueError(f"AddCard error: {res.get('Message') or res.get('Details') or res}")
    sub.trial_request_key = str(res["RequestKey"])
    db.commit()
    log.info("start_trial user=%s tier=%s/%s request_key=%s (AddCard)", user.id, tier, period, sub.trial_request_key)
    return {
        "subscription_id": sub.id,
        "request_key": sub.trial_request_key,
        "payment_url": res.get("PaymentURL"),
        "tier": tier, "period": period, "trial_days": days,
    }


def _complete_addcard(db: Session, sub: Subscription, user: User) -> dict:
    """Завершить AddCard-привязку: GetAddCardState → RebillId → активировать триал
    (через complete_trial_binding; yk_payment_id=None → 1₽-возврат не делается)."""
    provider = get_payment_provider()
    state = provider.get_add_card_state(sub.trial_request_key)  # type: ignore[attr-defined]
    status = (state.get("Status") or "").upper()
    rebill_id = state.get("RebillId")
    if status != "COMPLETED" or not rebill_id:
        return {"ok": False, "status": "pending", "reason": "Привязка карты не завершена"}
    pan = state.get("Pan") or ""
    ev = WebhookEvent(
        payment_id="", event_type="addcard",
        rebill_id=str(rebill_id), customer_key=str(user.id),
        card_last4=(pan[-4:] if len(pan) >= 4 else None),
        card_brand=str(state.get("CardType") or ""),
        amount=0,
    )
    res = complete_trial_binding(db, sub, ev)
    if res is not None:
        return {"ok": True, "status": "active", "subscription_id": res.id, "tier": res.tier,
                "expires_at": res.expires_at.isoformat() if res.expires_at else None}
    return {"ok": False, "status": "pending", "reason": "Привязка ещё обрабатывается"}


# ─────────────── Активация триала после 1₽-привязки ───────────────
def _refund_bind(payment_id: str | None) -> None:
    """Вернуть 1₽-привязку (после получения RebillId). Best-effort: ошибка возврата
    не должна ломать активный триал. Чек возврата 54-ФЗ — внутри provider.refund."""
    if not payment_id:
        return
    try:
        get_payment_provider().refund(payment_id, amount=TRIAL_BIND_AMOUNT)  # type: ignore[attr-defined]
        log.info("trial: возврат 1₽ payment=%s инициирован", payment_id)
    except Exception as e:
        log.error("trial: возврат 1₽ payment=%s не удался: %s", payment_id, e)


def _rebill_from_cardlist(provider, customer_key: str) -> WebhookEvent | None:
    """Восстановить RebillId из GetCardList (fallback если webhook не принёс).
    Берём самую свежую активную карту клиента."""
    try:
        lst = provider._post_signed("GetCardList", {"CustomerKey": customer_key})  # type: ignore[attr-defined]
    except Exception as e:
        log.warning("trial: GetCardList(%s) failed: %s", customer_key, e)
        return None
    if not isinstance(lst, list):
        return None
    active = [c for c in lst if (c.get("Status") == "A" and c.get("RebillId"))]
    if not active:
        return None
    c = active[-1]  # последняя в списке — самая свежая привязка
    pan = c.get("Pan") or ""
    return WebhookEvent(
        payment_id="", event_type="payment.succeeded",
        rebill_id=str(c["RebillId"]), customer_key=customer_key,
        card_last4=(pan[-4:] if len(pan) >= 4 else None),
        card_brand=str(c.get("CardType") or ""),
        amount=TRIAL_BIND_AMOUNT,
    )


def complete_trial_binding(db: Session, sub: Subscription, event: WebhookEvent) -> Subscription | None:
    """Webhook payment.succeeded по 1₽-привязке: сохранить карту (RebillId),
    активировать триал (N дней, 0₽), вернуть 1₽. Идемпотентно.

    Зовётся из webhook-роутера, когда платёж принадлежит is_trial-строке.
    """
    if sub.status == "active":
        return sub  # уже активирован (идемпотентно)
    if sub.status != "pending":
        return None
    user = db.query(User).filter(User.id == sub.user_id).first()
    if not user:
        return None
    if not event.rebill_id:
        # Без RebillId нечем списывать после триала → привязка бессмысленна.
        log.warning("complete_trial_binding: нет rebill_id sub=%s — fail + возврат 1₽", sub.id)
        bind_pid = sub.yk_payment_id
        sub.status = "failed"
        db.commit()
        _refund_bind(bind_pid)
        return None

    # Анти-двойная-активация: если за это время оформлена платная подписка — не выдаём.
    active = billing_service.current_subscription(db, user)
    if active is not None and not getattr(active, "is_trial", False):
        bind_pid = sub.yk_payment_id
        sub.status = "failed"
        db.commit()
        _refund_bind(bind_pid)
        return None

    pm = billing_service._upsert_payment_method(db, user.id, event)
    if not pm:
        return None  # не коммитим — webhook может прийти повторно

    # Анти-абуз: фиксируем редемпшн (UNIQUE по идентичности → повтор = IntegrityError).
    oauth_h, email_h = _identity_hashes(user)
    db.add(TrialRedemption(
        user_id=user.id, subscription_id=sub.id, tier=sub.tier,
        oauth_hash=oauth_h, email_hash=email_h, ip=None,
    ))
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        log.warning("complete_trial_binding: идентичность уже брала триал user=%s", user.id)
        bind_pid = sub.yk_payment_id
        fresh = db.query(Subscription).filter(Subscription.id == sub.id).first()
        if fresh and fresh.status == "pending":
            fresh.status = "failed"
        db.commit()
        _refund_bind(bind_pid)  # деньги назад в любом случае
        return None

    now = datetime.now(timezone.utc)
    days = FOUNDER_OFFER_DAYS if is_founder_offer(user) else (trial_days(sub.tier) or 7)
    sub.status = "active"
    sub.started_at = now
    sub.expires_at = now + timedelta(days=days)
    sub.payment_method_id = pm.id
    user.trial_used = True
    # Обнуляем yk_payment_id (1₽-платёж): иначе refund-webhook на возврат 1₽ найдёт
    # эту строку в cancel_by_webhook и погасит активный триал. Возврат делаем по
    # сохранённому bind_pid.
    bind_pid = sub.yk_payment_id
    sub.yk_payment_id = None
    db.flush()
    billing_service.sync_user_role(db, user)
    db.commit()
    log.info("complete_trial_binding: триал активен user=%s sub=%s до %s", user.id, sub.id, sub.expires_at)
    _refund_bind(bind_pid)  # вернуть 1₽
    return sub


# ─────────────────────────── Status-чек / recovery ───────────────────────────
def complete_trial_for_user(db: Session, user: User) -> dict:
    """Фронт зовёт на /billing/trial-success (поллинг). Основная активация — в
    webhook (complete_trial_binding). Здесь: если триал уже активен — ok; если
    pending и 1₽-платёж CONFIRMED, но webhook ещё не пришёл — восстанавливаем
    RebillId через GetCardList и активируем сами (recovery)."""
    active = billing_service.current_subscription(db, user)
    if active and getattr(active, "is_trial", False):
        return {"ok": True, "status": "active", "subscription_id": active.id,
                "tier": active.tier,
                "expires_at": active.expires_at.isoformat() if active.expires_at else None}

    # AddCard-привязка (за флагом TRIAL_BIND_METHOD=addcard): завершаем через
    # GetAddCardState. trial_request_key есть только у AddCard-строк (Init=NULL).
    ac = db.query(Subscription).filter(
        Subscription.user_id == user.id,
        Subscription.is_trial.is_(True),
        Subscription.status == "pending",
        Subscription.trial_request_key.isnot(None),
    ).order_by(Subscription.created_at.desc()).first()
    if ac is not None:
        return _complete_addcard(db, ac, user)

    sub = db.query(Subscription).filter(
        Subscription.user_id == user.id,
        Subscription.is_trial.is_(True),
        Subscription.status == "pending",
        Subscription.yk_payment_id.isnot(None),
    ).order_by(Subscription.created_at.desc()).first()
    if not sub:
        return {"ok": False, "status": "none", "reason": "Нет ожидающей привязки карты"}

    provider = get_payment_provider()
    info = provider.verify_payment(sub.yk_payment_id) if sub.yk_payment_id else None  # type: ignore[attr-defined]
    if not info or (info.get("Status") or "").upper() != "CONFIRMED":
        return {"ok": False, "status": "pending", "reason": "Ожидаем подтверждение оплаты"}

    # 1₽ оплачен, но webhook ещё не активировал → достаём RebillId из GetCardList.
    ev = _rebill_from_cardlist(provider, str(user.id))
    if not ev:
        return {"ok": False, "status": "pending", "reason": "Карта ещё привязывается"}
    ev.payment_id = str(sub.yk_payment_id)
    res = complete_trial_binding(db, sub, ev)
    if res is not None:
        return {"ok": True, "status": "active", "subscription_id": res.id,
                "tier": res.tier,
                "expires_at": res.expires_at.isoformat() if res.expires_at else None}
    return {"ok": False, "status": "pending", "reason": "Привязка ещё обрабатывается"}


# ─────────────── Fallback: добить pending-привязки (оркестратор) ───────────────
def complete_pending_trials(db: Session, max_age_hours: int = 6) -> dict:
    """Оркестратор-фоллбэк (run_expire_only, 15 мин): на случай потерянного webhook
    проходит pending is_trial-строки с оплаченным 1₽ и активирует их через
    complete_trial_for_user (verify + GetCardList). Идемпотентно, no-op без pending."""
    if not TRIAL_ENABLED:
        return {"checked": 0, "completed": 0}
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=max_age_hours)
    subs = db.query(Subscription).filter(
        Subscription.is_trial.is_(True),
        Subscription.status == "pending",
        Subscription.yk_payment_id.isnot(None),
        Subscription.created_at > cutoff,
    ).all()
    summary = {"checked": len(subs), "completed": 0}
    seen_users: set[int] = set()
    for sub in subs:
        if sub.user_id in seen_users:
            continue
        seen_users.add(sub.user_id)
        user = db.query(User).filter(User.id == sub.user_id).first()
        if not user:
            continue
        try:
            res = complete_trial_for_user(db, user)
            if res.get("ok") and res.get("status") == "active":
                summary["completed"] += 1
        except Exception as e:
            log.error("complete_pending_trials sub=%s: %s", sub.id, e, exc_info=True)
    if summary["completed"]:
        log.info("complete_pending_trials: %s", summary)
    return summary


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
