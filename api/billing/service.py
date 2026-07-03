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
from decimal import Decimal

from sqlalchemy import or_ as sa_or_
from sqlalchemy.orm import Session

from api.billing.factory import get_payment_provider
from api.billing.plans import TIER_LEVELS, get_plan, monthly_fallback
from api.billing.provider import WebhookEvent
from api.models.payment_method import UserPaymentMethod
from api.models.subscription import Subscription
from api.models.user import User

log = logging.getLogger(__name__)


def _upsert_payment_method(
    db: Session,
    user_id: int,
    event: WebhookEvent,
) -> UserPaymentMethod | None:
    """
    Создаёт или находит UserPaymentMethod на основе данных из webhook.

    Идемпотентно: если карта (user_id + rebill_id) уже есть (NOT deleted),
    обновляем last_used_at + возможно недостающие поля (last4, brand) и
    возвращаем существующую запись. Иначе создаём новую.

    Вызывается из activate_from_webhook когда event.rebill_id != None.
    """
    # Карта → match по rebill_id; СБП → по account_token (rebill_id у СБП пуст).
    is_sbp = bool(event.account_token) and not event.rebill_id
    if not event.rebill_id and not event.account_token:
        return None

    provider = get_payment_provider()

    if is_sbp:
        existing = db.query(UserPaymentMethod).filter(
            UserPaymentMethod.user_id == user_id,
            UserPaymentMethod.account_token == event.account_token,
            UserPaymentMethod.deleted_at.is_(None),
        ).first()
    else:
        existing = db.query(UserPaymentMethod).filter(
            UserPaymentMethod.user_id == user_id,
            UserPaymentMethod.rebill_id == event.rebill_id,
            UserPaymentMethod.deleted_at.is_(None),
        ).first()

    now = datetime.now(timezone.utc)

    if existing:
        # Обновляем недостающие поля (если webhook принёс свежие данные)
        if event.card_last4 and not existing.card_last4:
            existing.card_last4 = event.card_last4
        if event.card_brand and not existing.card_brand:
            existing.card_brand = event.card_brand
        existing.last_used_at = now
        db.flush()
        log.info(
            "Payment method already exists: user=%s pm=%s (last_used updated)",
            user_id, existing.id,
        )
        return existing

    # Если у юзера ещё нет активных карт — новая будет default
    has_default = db.query(UserPaymentMethod).filter(
        UserPaymentMethod.user_id == user_id,
        UserPaymentMethod.deleted_at.is_(None),
        UserPaymentMethod.is_default.is_(True),
    ).first() is not None

    pm = UserPaymentMethod(
        user_id=user_id,
        provider=provider.name,
        method_type="sbp" if is_sbp else "card",
        rebill_id=event.rebill_id,
        account_token=event.account_token,
        bank_member_id=event.bank_member_id,
        customer_key=event.customer_key,
        card_last4=event.card_last4,
        card_brand=event.card_brand,
        is_default=not has_default,
        last_used_at=now,
    )
    db.add(pm)
    db.flush()
    log.info(
        "Created payment method: user=%s pm=%s type=%s brand=%s last4=%s default=%s",
        user_id, pm.id, pm.method_type, pm.card_brand, pm.card_last4, pm.is_default,
    )
    return pm


# ═══════════════════════════════════════════════════════════════════════════════
#  1. CHECKOUT — создание платёжной сессии для пользователя
# ═══════════════════════════════════════════════════════════════════════════════

def create_checkout_for_user(
    db: Session,
    user: User,
    plan_id: str,
    return_url: str,
    widget_mode: bool = False,
    recurrent: bool = False,
    rail: str = "card",
) -> dict:
    """
    Создаёт subscription(pending) + платёж у провайдера.

    rail='card' (по умолчанию) → возвращает confirmation_url (redirect на оплату).
    rail='sbp' → СБП по QR: возвращает qr_image (картинка) + qr_payload (ссылка),
    confirmation_url пуст; фронт ведёт на /billing/sbp.

    Возвращает {subscription_id, payment_id, confirmation_url, qr_image,
    qr_payload, rail, plan_id, tier, amount}.
    """
    plan = get_plan(plan_id)
    if not plan:
        raise ValueError(f"Unknown plan_id: {plan_id}")

    # Защита от дубликата: нельзя купить тот же или более низкий tier,
    # если у user'а уже active подписка (без cancelled_at).
    #
    # Правила (зеркало UI-логики на PricingPage):
    #   - admin → блокируем все покупки (у него full access)
    #   - active sub без cancelled_at:
    #       cardLevel < activeLevel → блок (downgrade недоступен)
    #       cardLevel == activeLevel && plan_id == active → блок (тот же план)
    #       иначе → разрешаем (period switch / upgrade)
    #   - active sub с cancelled_at (юзер отменил, доступ до expires) →
    #       разрешаем продление того же плана и всё что выше
    if getattr(user, "role", None) == "admin":
        raise ValueError("У админа полный доступ — оформление подписок не требуется")

    # === Гейт верификации email (ANTI-BYPASS, серверная проверка из БД) ===
    # Оплатить можно только с подтверждённым РЕАЛЬНЫМ email: иначе чек 54-ФЗ
    # уйдёт в никуда, и email юзеру не принадлежит. Фронт прячет кнопку оплаты
    # при !is_verified, но прямой вызов POST /api/billing/checkout с валидным JWT
    # закрывается здесь (is_verified читается живьём из user — в JWT его нет,
    # подделать нельзя). Покрывает: synthetic @oauth.local, неподтверждённый
    # добавленный email, неподтверждённую регистрацию по почте.
    user_email = (getattr(user, "email", "") or "")
    if (not user.is_verified) or user_email.endswith("@oauth.local"):
        raise ValueError("Подтвердите email перед оплатой")

    active_sub = current_subscription(db, user)
    # is_trial исключаем из дедуп-гейта: во время пробного периода юзер ДОЛЖЕН
    # иметь возможность оформить платную подписку досрочно (купленная active того
    # же tier через Guard 1 в renew_expiring_subs заглушит конвертацию триала).
    if active_sub and not active_sub.cancelled_at and not active_sub.is_trial:
        active_plan = get_plan(active_sub.plan_id)
        if active_plan:
            active_level = TIER_LEVELS.get(active_plan.tier, 0)
            card_level = TIER_LEVELS.get(plan.tier, 0)
            if card_level < active_level:
                raise ValueError(
                    f"У вас уже активен более высокий тариф ({active_plan.tier}). "
                    "Понижение тарифа возможно после окончания текущего периода."
                )
            if card_level == active_level and active_sub.plan_id == plan.plan_id:
                raise ValueError(
                    "Этот план уже активен. Сменить период — после окончания текущего."
                )

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
    metadata = {
        "user_id": str(user.id),
        "subscription_id": str(sub.id),
        "plan_id": plan.plan_id,
    }
    try:
        if rail == "sbp":
            # СБП всегда recurrent (привязка счёта во время первой оплаты).
            # customer_key обязателен; sub помечаем как оплаченную через СБП.
            sub.yk_method = "sbp"
            session = provider.create_sbp_checkout(  # type: ignore[attr-defined]
                amount=plan.amount,
                currency="RUB",
                description=f"{plan.title} — user #{user.id}",
                metadata=metadata,
                customer_email=customer_email,
                customer_phone=customer_phone,
                recurrent=True,
                customer_key=str(user.id),
            )
        else:
            session = provider.create_checkout(
                amount=plan.amount,
                currency="RUB",
                description=f"{plan.title} — user #{user.id}",
                return_url=return_url,
                metadata=metadata,
                customer_email=customer_email,
                customer_phone=customer_phone,
                widget_mode=widget_mode,
                recurrent=recurrent,
                # CustomerKey — обязательный когда recurrent=True. Уникальный
                # идентификатор клиента у эквайера; у нас str(user.id).
                customer_key=str(user.id) if recurrent else None,
            )
    except Exception as e:
        # Платёж не создался — помечаем fail и пробрасываем
        sub.status = "failed"
        db.commit()
        log.error("create_checkout_for_user: provider failed (rail=%s): %s", rail, e)
        raise

    # 3. Сохраняем payment_id от провайдера в нашу подписку
    sub.yk_payment_id = session.payment_id
    # СБП: RequestKey привязки — reconciler по нему дотянет AccountToken+BankMemberId
    # после оплаты (resolve_sbp_bindings в orchestrator).
    if rail == "sbp":
        sub.sbp_request_key = getattr(session, "request_key", None)
    db.commit()

    log.info(
        "Created subscription #%s (pending) for user #%s: plan=%s amount=%.2f payment_id=%s",
        sub.id, user.id, plan.plan_id, plan.amount, session.payment_id,
    )

    return {
        "subscription_id": sub.id,
        "payment_id": session.payment_id,
        "confirmation_url": session.confirmation_url,
        "qr_image": getattr(session, "qr_image", None),
        "qr_payload": getattr(session, "qr_payload", None),
        "rail": rail,
        "plan_id": plan.plan_id,
        "tier": plan.tier,
        "amount": plan.amount,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  2. WEBHOOK — активация / отмена
# ═══════════════════════════════════════════════════════════════════════════════

def _verify_with_provider(sub: Subscription) -> bool:
    """
    Дополнительная проверка: GET /v2/GetState у T-Bank, чтобы убедиться
    что платёж реально CONFIRMED. Защита от подделки webhook'ов (даже
    если злоумышленник угадал Token, GetState вернёт реальный статус).
    """
    provider = get_payment_provider()
    if provider.name == "stub":
        return True  # stub доверяем всегда
    if provider.name != "tbank":
        return True  # другие провайдеры — настраивать отдельно
    info = provider.verify_payment(sub.yk_payment_id)  # type: ignore[attr-defined]
    if info is None:
        log.warning(
            "verify failed: payment %s not found in %s",
            sub.yk_payment_id, provider.name,
        )
        return False
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

    # Guard 2 (анти-double-charge для card-1): НЕ реанимировать поздним
    # webhook'ом failed-попытку, если юзер уже перешёл на более новую активную
    # подписку того же tier (напр. годовой провалился по NSF → списан месячный
    # того же уровня). Активировать обе = двойное списание. При таймауте без
    # fallback (нет более новой active того же tier) реанимация остаётся
    # корректной — гард срабатывает ТОЛЬКО при наличии superseding-подписки.
    if sub.status == "failed":
        superseded_by = db.query(Subscription).filter(
            Subscription.user_id == sub.user_id,
            Subscription.tier == sub.tier,
            Subscription.id != sub.id,
            Subscription.status == "active",
            Subscription.created_at > sub.created_at,
        ).first()
        if superseded_by:
            log.warning(
                "activate_from_webhook: skip late reactivation of failed sub=%s "
                "— superseded by newer active sub=%s (same tier=%s)",
                sub.id, superseded_by.id, sub.tier,
            )
            return None

    # Double-check с провайдером
    if not _verify_with_provider(sub):
        log.warning("activate_from_webhook: verification failed for sub=%s", sub.id)
        return None

    # Сверка суммы: webhook должен принести ровно ту сумму, что мы выставили в
    # подписке (sub.amount уже учитывает retention-скидку). Подпись Token и так
    # покрывает Amount, так что это defense-in-depth — ловит рассинхрон сумм /
    # манипуляцию на стороне эквайера, не даёт активировать подписку, оплаченную
    # на другую сумму. Допуск 1 коп. на float-погрешность.
    if event.amount is not None and sub.amount is not None:
        if abs(float(event.amount) - float(sub.amount)) > 0.01:
            log.warning(
                "activate_from_webhook: amount mismatch sub=%s — ожидали %.2f, "
                "webhook принёс %.2f. НЕ активирую.",
                sub.id, float(sub.amount), float(event.amount),
            )
            return None

    # Активируем
    plan = get_plan(sub.plan_id)
    now = datetime.now(timezone.utc)
    sub.status = "active"
    sub.started_at = now
    sub.expires_at = now + timedelta(days=plan.duration_days) if plan else None
    sub.yk_method = event.payment_method

    # Если webhook принёс RebillId (карта) ИЛИ AccountToken (СБП) — это был
    # recurrent-платёж: сохраняем привязку в user_payment_methods и линкуем к sub.
    if event.rebill_id or event.account_token:
        pm = _upsert_payment_method(db, sub.user_id, event)
        if pm:
            sub.payment_method_id = pm.id

    db.flush()

    # Апгрейд гасит авто-продление старых подписок НИЖЕ нового tier'а: иначе
    # старая basic с payment_method_id остаётся active+не-cancelled, а дедуп в
    # renew_expiring_subs матчит только ТОТ ЖЕ tier → крон продолжил бы
    # списывать basic параллельно с pro (двойная оплата). Оплаченный период
    # они дослуживают (expires_at не трогаем) — выключается только продление.
    new_level = TIER_LEVELS.get(sub.tier, 0)
    lower_subs = db.query(Subscription).filter(
        Subscription.user_id == sub.user_id,
        Subscription.id != sub.id,
        Subscription.status == "active",
        Subscription.cancelled_at.is_(None),
    ).all()
    for old_sub in lower_subs:
        # Гасим авто-продление: (а) подписок строго НИЖЕ нового tier (апгрейд),
        # (б) ЛЮБОГО активного ТРИАЛА — покупка/конверсия закрывает пробный период,
        # чтобы он не списался повторно (ревью #1/#3). is_trial есть только у
        # триал-строк → реальные платные подписки тем же/выше tier не затрагиваются.
        if old_sub.is_trial or TIER_LEVELS.get(old_sub.tier, 0) < new_level:
            old_sub.cancelled_at = now
            log.info(
                "activate_from_webhook: %s — отключено авто-продление подписки #%s "
                "(tier=%s, is_trial=%s, дослужит до %s)",
                "конверсия/покупка поверх триала" if old_sub.is_trial else f"upgrade to {sub.tier}",
                old_sub.id, old_sub.tier, old_sub.is_trial, old_sub.expires_at,
            )

    # Поднимаем роль user'а если нужно
    user = db.query(User).filter(User.id == sub.user_id).first()
    if user:
        sync_user_role(db, user)

    db.commit()
    log.info(
        "Activated subscription #%s for user #%s: tier=%s expires=%s method=%s pm=%s",
        sub.id, sub.user_id, sub.tier, sub.expires_at,
        event.payment_method, sub.payment_method_id,
    )
    return sub


def cancel_by_webhook(db: Session, event: WebhookEvent) -> Subscription | None:
    """Обработка webhook.payment.canceled / refund.succeeded."""
    sub = db.query(Subscription).filter(
        Subscription.yk_payment_id == event.payment_id
    ).first()
    if not sub:
        return None

    # Триал: возврат 1₽-привязки — ОЖИДАЕМЫЙ (инициируем сами после активации) и
    # НЕ должен гасить активный триал. У активного триала yk_payment_id обнулён в
    # complete_trial_binding, поэтому сюда он обычно не попадёт; гард — на случай гонки.
    if getattr(sub, "is_trial", False) and sub.status == "active":
        log.info("cancel_by_webhook: пропускаем активный триал sub=%s (event=%s)", sub.id, event.event_type)
        return sub

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


def renew_expiring_subs(db: Session, hours_window: int = 24) -> dict:
    """
    Auto-renewal: для каждой active sub с привязанной картой (payment_method_id
    NOT NULL) и без cancelled_at, у которой expires_at < now + hours_window —
    запускаем charge_recurrent. Это создаёт новую sub того же plan_id и
    продлевает доступ.

    Защита от дублей:
    - Если для того же user + plan_id уже есть pending/active sub созданная
      после текущей — пропускаем (значит уже продлили в прошлом запуске или
      юзер сам купил)
    - payment_method.deleted_at IS NOT NULL — пропускаем (карта удалена)

    Запускать раз в час из orchestrator'а. hours_window=24 даёт буфер в случае
    задержки cron'а или временных T-Bank проблем (попытаемся ещё раз через
    час, до 24 раз пока not expired).

    Возвращает {checked, renewed, failed, skipped}.
    """
    from api.models.payment_method import UserPaymentMethod  # avoid cycle при import

    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=hours_window)

    # Grace 48ч НАЗАД: если крон простоял >24ч (рестарт сервера, недоступность
    # T-Bank), подписка успевает истечь и expire_overdue (он идёт ПЕРЕД renew в
    # оркестраторе) переводит её в expired — строгий фильтр status='active'
    # оставлял клиента с валидной картой без единой попытки списания. Включаем
    # недавно-expired: Guard 1 ниже защищает от двойного продления.
    grace = now - timedelta(hours=48)
    expiring = db.query(Subscription).filter(
        Subscription.status.in_(("active", "expired")),
        Subscription.cancelled_at.is_(None),
        Subscription.payment_method_id.isnot(None),
        Subscription.expires_at > grace,
        Subscription.expires_at < cutoff,
    ).all()

    summary = {"checked": 0, "renewed": 0, "failed": 0, "skipped": 0}
    if not expiring:
        return summary

    log.info("renew_expiring_subs: found %d candidates (window=%dh)", len(expiring), hours_window)

    for sub in expiring:
        summary["checked"] += 1

        # Триал: заряжаем ТОЛЬКО по факту окончания (expires_at <= now), а НЕ за
        # 24ч вперёд (как платные продления) — иначе спишем раньше конца
        # бесплатного периода. expire_overdue идёт перед renew, grace-48ч ниже
        # ловит недавно-истёкший триал → конвертация в течение ~1ч после конца.
        if sub.is_trial and sub.expires_at and sub.expires_at > now:
            summary["skipped"] += 1
            continue

        # Guard 1 (анти-double-charge): дедуп по TIER, а не по plan_id. После
        # card-1 fallback follow-up имеет ДРУГОЙ plan_id (tier_monthly вместо
        # tier_yearly) — матч по plan_id его бы не увидел → крон списал бы год
        # ПОВЕРХ уже успешного месяца. Любая более новая pending/active подписка
        # того же tier = «этот уровень уже продлили» → skip.
        existing_renewal = db.query(Subscription).filter(
            Subscription.user_id == sub.user_id,
            Subscription.tier == sub.tier,
            Subscription.id != sub.id,
            Subscription.status.in_(("pending", "active")),
            Subscription.created_at > sub.created_at,
        ).first()
        if existing_renewal:
            log.info(
                "renew: sub_%s skip — already has follow-up sub_%s (tier=%s)",
                sub.id, existing_renewal.id, sub.tier,
            )
            summary["skipped"] += 1
            continue

        # Карта существует и не удалена?
        pm = db.query(UserPaymentMethod).filter(
            UserPaymentMethod.id == sub.payment_method_id
        ).first()
        if not pm or pm.deleted_at is not None:
            log.warning(
                "renew: sub_%s skip — payment_method %s deleted/missing",
                sub.id, sub.payment_method_id,
            )
            summary["skipped"] += 1
            continue

        user = db.query(User).filter(User.id == sub.user_id).first()
        if not user:
            summary["skipped"] += 1
            continue

        # Card-1: если годовой уже окончательно провалился по NSF (fallback_done)
        # — списываем МЕСЯЧНЫЙ план того же tier, а не годовой. Retention-скидку,
        # рассчитанную под годовую цену, на месяц НЕ переносим (discount=0).
        charge_plan_id = sub.plan_id
        charge_discount = sub.discount_pct or 0
        if sub.fallback_done:
            fb = monthly_fallback(sub.plan_id)
            if fb:
                charge_plan_id = fb.plan_id
                charge_discount = 0

        try:
            # discount: retention-скидка истекающей подписки переносится на это
            # (одно) продление; новая запись внутри charge_recurrent создаётся с
            # discount_pct=0 → дальше полная цена.
            result = charge_recurrent(db, user, charge_plan_id, pm, discount_pct=charge_discount)
            if result.get("ok"):
                summary["renewed"] += 1
                log.info(
                    "renew: sub_%s OK → new sub_%s for user_%s plan=%s%s",
                    sub.id, result.get("subscription_id"), user.id, charge_plan_id,
                    " (monthly fallback)" if sub.fallback_done else "",
                )
                # Триал сконвертирован в платную строку → закрываем триал-строку
                # ТЕРМИНАЛЬНО (cancelled_at), чтобы она НАВСЕГДА выпала из
                # expiring-запроса и не списалась повторно, если платная строка
                # позже уйдёт из active/pending (возврат). Не полагаемся на Guard 1
                # (ревью #1). Касается ТОЛЬКО is_trial — платные не трогаем.
                if sub.is_trial and sub.cancelled_at is None:
                    sub.cancelled_at = now
                    db.commit()
            else:
                summary["failed"] += 1
                kind = result.get("failure_kind")
                err32 = (str(result.get("error_code"))[:32]
                         if result.get("error_code") is not None else None)
                # Card-1 (N=1): ПЕРВЫЙ NSF-отказ ГОДОВОГО → переключаем подписку на
                # месячный того же tier. Сам месячный спишется на СЛЕДУЮЩЕМ цикле
                # крона (см. charge_plan_id выше) — renew работает в 24ч-окне,
                # попыток (раз в час) хватает. fallback_done ставим ДО любого
                # месячного списания: терминальный маркер, чтобы (а) renew больше
                # не дёргал год, (б) поздний webhook по годовому PaymentId не
                # реанимировал годовую подписку (см. activate_from_webhook).
                if (kind == "nsf" and not sub.fallback_done
                        and sub.period == "yearly" and monthly_fallback(sub.plan_id)):
                    sub.fallback_done = True
                    sub.renewal_last_error = err32
                    db.commit()
                    log.warning(
                        "renew: sub_%s yearly NSF (code=%s) → switch to monthly fallback (next cycle)",
                        sub.id, result.get("error_code"),
                    )
                else:
                    sub.renewal_last_error = err32
                    db.commit()
                    log.warning(
                        "renew: sub_%s FAILED user_%s kind=%s: %s",
                        sub.id, user.id, kind, result.get("message"),
                    )
        except Exception as e:
            summary["failed"] += 1
            log.error("renew: sub_%s exception: %s", sub.id, e, exc_info=True)

    log.info("renew_expiring_subs summary: %s", summary)
    return summary


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
    """
    Последние N подписок пользователя для ProfilePage → История платежей.

    Фильтрация:
    - Все «конечные» статусы (active/expired/refunded/failed/cancelled) — всегда
    - pending — только если создан в последний час (юзер прямо сейчас платит,
      webhook ещё не пришёл). Старые pending'и — это брошенные checkout'ы
      («открыл форму, закрыл вкладку») и в истории не показываются.
    """
    pending_cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    return db.query(Subscription).filter(
        Subscription.user_id == user.id,
        sa_or_(
            Subscription.status != "pending",
            Subscription.created_at > pending_cutoff,
        ),
    ).order_by(Subscription.created_at.desc()).limit(limit).all()


# ═══════════════════════════════════════════════════════════════════════════════
#  6. Sync pending — fallback на случай задержки/отсутствия webhook
# ═══════════════════════════════════════════════════════════════════════════════

def refund_subscription(
    db: Session,
    sub: Subscription,
    amount: float | None = None,
    reason: str | None = None,
) -> dict:
    """
    Инициирует возврат денег за подписку через провайдера.

    Логика:
    1. Зовём provider.refund(payment_id, amount). T-Bank /v2/Cancel:
       - AUTHORIZED → reverse (холд снят, ничего не списано)
       - CONFIRMED  → refund (возврат на карту, до 7 дней зачисление)
       - Если amount меньше списанного → частичный возврат
    2. Если провайдер ОК — сразу обновляем status в БД (не ждём webhook):
       - 'active'  → 'refunded'
       - 'pending' → 'cancelled' (платёж не успел подтвердиться)
    3. sync_user_role пересчитает users.role — если у user'а больше нет
       активных подписок более высокого tier'а, role понизится

    Возвращает {ok, status, refunded_amount, message}.
    Если провайдер вернул False — оставляем sub без изменений.
    """
    provider = get_payment_provider()

    if provider.name == "stub":
        # Stub — просто маркируем refunded, без обращения наружу
        sub.status = "refunded"
        sub.cancelled_at = datetime.now(timezone.utc)
        db.flush()
        user = db.query(User).filter(User.id == sub.user_id).first()
        if user:
            sync_user_role(db, user)
        db.commit()
        return {"ok": True, "status": "refunded", "refunded_amount": sub.amount, "message": "stub refund"}

    if not sub.yk_payment_id:
        return {"ok": False, "status": sub.status, "message": "no payment_id"}

    ok = False
    try:
        ok = provider.refund(sub.yk_payment_id, amount=amount)  # type: ignore[attr-defined]
    except Exception as e:
        log.error("refund_subscription sub=%s provider.refund failed: %s", sub.id, e)
        return {"ok": False, "status": sub.status, "message": str(e)}

    if not ok:
        return {
            "ok": False,
            "status": sub.status,
            "message": "Провайдер отклонил возврат (см. логи)",
        }

    # Провайдер принял запрос — обновляем БД
    prev_status = sub.status
    if sub.status == "active":
        sub.status = "refunded"
    elif sub.status == "pending":
        sub.status = "cancelled"
    # для refunded/cancelled/expired/failed — не трогаем

    sub.cancelled_at = datetime.now(timezone.utc)
    db.flush()

    user = db.query(User).filter(User.id == sub.user_id).first()
    if user:
        sync_user_role(db, user)
    db.commit()

    log.info(
        "refund_subscription sub=%s user=%s: %s → %s (amount=%s, reason=%r)",
        sub.id, sub.user_id, prev_status, sub.status, amount or "FULL", reason,
    )
    return {
        "ok": True,
        "status": sub.status,
        "refunded_amount": amount if amount is not None else sub.amount,
        "message": "Возврат инициирован у Т-Банка (до 7 дней зачисления)",
    }


def sync_pending_for_user(
    db: Session,
    user: User,
    max_age_minutes: int = 60,
) -> dict:
    """
    Спрашивает провайдера актуальный статус для:
    - pending sub'ов user'а младше max_age_minutes — активируем CONFIRMED,
      отменяем REJECTED/CANCELED/REVERSED/DEADLINE_EXPIRED.
    - active sub'ов user'а — проверяем не были ли они REFUNDED у провайдера
      (refund могли инициировать через кабинет T-Bank, не через наш API).
      Если REFUNDED → status='refunded', sync_user_role понизит роль.

    Используется когда webhook задерживается или вообще не пришёл (URL
    не зарегистрирован у провайдера, проблемы с сетью). Идемпотентно.

    Возвращает summary {activated, cancelled, skipped, checked}.
    """
    provider = get_payment_provider()
    summary = {"activated": 0, "cancelled": 0, "skipped": 0, "checked": 0}

    # Stub провайдер не имеет внешнего state — нет смысла верифицировать.
    if provider.name == "stub":
        return summary

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
    # pending OR active (для подхвата REFUNDED через кабинет провайдера).
    # active без yk_payment_id (например invite-redeem подписки) исключаем —
    # их нечего сверять, они не от провайдера.
    subs = db.query(Subscription).filter(
        Subscription.user_id == user.id,
        Subscription.status.in_(["pending", "active"]),
        Subscription.yk_payment_id.isnot(None),
        # Триал-строки исключаем явно: у них yk_payment_id=NULL (RequestKey лежит в
        # trial_request_key), поэтому они и так не попадают, но фильтр —
        # defense-in-depth против ложной отмены триала через GetState (ревью #4/#9).
        Subscription.is_trial.is_(False),
        # pending — узкое окно; active — без ограничения, проверяем все
        # (refund может прийти через месяц после оплаты).
        sa_or_(
            Subscription.status != "pending",
            Subscription.created_at >= cutoff,
        ),
    ).all()
    pending = subs

    for sub in pending:
        summary["checked"] += 1
        info = provider.verify_payment(sub.yk_payment_id) if sub.yk_payment_id else None  # type: ignore[attr-defined]
        if not info:
            summary["skipped"] += 1
            continue

        # Маппинг провайдер-специфичных статусов в наш event_type
        event_type: str | None = None
        amount: float | None = None
        method: str | None = None

        if provider.name == "tbank":
            status = info.get("Status", "").upper()
            if status == "CONFIRMED":
                event_type = "payment.succeeded"
            elif status in ("REJECTED", "CANCELED", "REVERSED", "DEADLINE_EXPIRED"):
                event_type = "payment.canceled"
            elif status in ("REFUNDED", "PARTIAL_REFUNDED"):
                event_type = "refund.succeeded"
            # Иначе NEW/AUTHORIZED/*_ING — оставляем pending
            try:
                amount = float(info.get("Amount", 0)) / 100.0  # копейки → рубли
            except (ValueError, TypeError):
                amount = None
            method = info.get("PaymentMethod") or "bank_card"

        if not event_type:
            summary["skipped"] += 1
            continue

        # Формируем синтетический WebhookEvent и проводим через стандартный flow.
        # activate_from_webhook сам делает _verify_with_provider повторно —
        # двойная проверка, но это безопасно и быстро.
        from api.billing.provider import WebhookEvent
        event = WebhookEvent(
            payment_id=sub.yk_payment_id,  # type: ignore[arg-type]
            event_type=event_type,
            payment_method=method,
            amount=amount,
        )

        try:
            if event_type == "payment.succeeded":
                if activate_from_webhook(db, event):
                    summary["activated"] += 1
            else:
                if cancel_by_webhook(db, event):
                    summary["cancelled"] += 1
        except Exception as e:
            log.error("sync_pending_for_user sub=%s failed: %s", sub.id, e)
            summary["skipped"] += 1

    if summary["activated"] or summary["cancelled"]:
        log.info(
            "sync_pending_for_user user=%s: %s",
            user.id, summary,
        )
    return summary


# ═══════════════════════════════════════════════════════════════════════════════
#  7. Charge recurrent — списать с сохранённой карты (auto-renewal + test #6)
# ═══════════════════════════════════════════════════════════════════════════════

def charge_recurrent(
    db: Session,
    user: User,
    plan_id: str,
    payment_method: UserPaymentMethod,
    discount_pct: int = 0,
) -> dict:
    """
    Списать с сохранённой карты по plan_id — auto-renewal flow.

    Шаги:
    1. Проверки: payment_method принадлежит user'у, не удалён, plan валиден
    2. Создаём Subscription(pending) — для аудита
    3. provider.charge(amount, rebill_id) → провайдер делает Init + Charge
       без участия юзера (T-Bank списывает мгновенно, без 3DS)
    4. Если CONFIRMED → activate_from_webhook (синтетический WebhookEvent)
       → подписка active, role обновлена, payment_method.last_used_at = now
    5. Если REJECTED → cancel_by_webhook → status='failed'

    Возвращает {ok, status, subscription_id, payment_id, message?}.

    Используется:
    - Admin endpoint /api/billing/admin/charge_recurrent (для теста #6)
    - Auto-renewal cron в orchestrator (будет позже)
    """
    if payment_method.user_id != user.id:
        raise ValueError("payment_method не принадлежит этому user'у")
    if payment_method.deleted_at is not None:
        raise ValueError("payment_method удалён")

    plan = get_plan(plan_id)
    if not plan:
        raise ValueError(f"Unknown plan_id: {plan_id}")

    # Retention-скидка: применяется к ЭТОМУ списанию (1 период). discount_pct
    # приходит из истекающей подписки; новая запись ниже создаётся с
    # discount_pct=0 (default) → скидка одноразовая, дальше полная цена.
    # T-Bank принимает любую сумму >0, чек 54-ФЗ бьётся на эту же сумму
    # (tbank.charge → _build_receipt(amount)).
    # plan.amount — float (plans.py), у float нет .quantize(): без конверсии в
    # Decimal первое же продление с retention-скидкой падало AttributeError →
    # клиент не списывался и молча терял доступ.
    effective_amount = plan.amount
    if discount_pct and 0 < discount_pct < 100:
        effective_amount = (
            Decimal(str(plan.amount)) * (100 - discount_pct) / 100
        ).quantize(Decimal("0.01"))

    provider = get_payment_provider()
    # Ветвление card/СБП: карта → /v2/Charge(RebillId), СБП → ChargeQr(AccountToken).
    is_sbp = (payment_method.method_type == "sbp")
    needed = "charge_qr" if is_sbp else "charge"
    if not hasattr(provider, needed):
        raise RuntimeError(f"Provider {provider.name} не поддерживает рекурренты ({needed})")

    # 1. Sub(pending)
    sub = Subscription(
        user_id=user.id,
        tier=plan.tier,
        period=plan.period,
        plan_id=plan.plan_id,
        amount=effective_amount,
        currency="RUB",
        status="pending",
        payment_method_id=payment_method.id,
    )
    db.add(sub)
    db.flush()

    # 2. Init + Charge через провайдера
    # customer_key: T-Bank требует ТУ ЖЕ связку CustomerKey+RebillId что и при
    # первом платеже. Сохранён в pm.customer_key. Fallback на str(user.id)
    # для старых записей (до 2026-05) где customer_key не сохранялся.
    # customer_email: для Receipt-блока (54-ФЗ). Терминал-с-фискализацией без
    # Receipt вернёт 309 «expected.receipt». Берём из user.email.
    charge_kwargs = dict(
        amount=effective_amount,
        currency="RUB",
        description=f"{plan.title} — auto-renewal user #{user.id}" + (f" (скидка {discount_pct}%)" if discount_pct else ""),
        customer_key=payment_method.customer_key or str(user.id),
        customer_email=user.email,
        metadata={
            "user_id": str(user.id),
            "subscription_id": str(sub.id),
            "plan_id": plan.plan_id,
            "auto_renewal": "1",
        },
    )
    try:
        if is_sbp:
            result = provider.charge_qr(  # type: ignore[attr-defined]
                account_token=payment_method.account_token,
                bank_member_id=payment_method.bank_member_id,
                **charge_kwargs,
            )
        else:
            result = provider.charge(  # type: ignore[attr-defined]
                rebill_id=payment_method.rebill_id, **charge_kwargs
            )
    except Exception as e:
        sub.status = "failed"
        db.commit()
        log.error("charge_recurrent failed for sub=%s: %s", sub.id, e)
        return {
            "ok": False,
            "status": "failed",
            "subscription_id": sub.id,
            "message": str(e),
            # Сетевой/Init-фейл/таймаут — структурированного кода нет, деньги
            # МОГЛИ уйти. НЕ 'nsf' → card-1 fallback НЕ срабатывает (безопасно).
            "failure_kind": "unknown",
        }

    sub.yk_payment_id = result["payment_id"]
    db.flush()

    # 3. Активация или fail
    if result.get("success") and result.get("status") == "CONFIRMED":
        event = WebhookEvent(
            payment_id=result["payment_id"],
            event_type="payment.succeeded",
            payment_method="sbp" if is_sbp else "bank_card",
            amount=result.get("amount"),
        )
        activated = activate_from_webhook(db, event)
        # last_used_at обновится через _upsert_payment_method? Нет — там path
        # только когда RebillId в event. Charge webhook не присылает RebillId
        # (карта уже привязана). Обновляем вручную.
        payment_method.last_used_at = datetime.now(timezone.utc)
        db.commit()
        return {
            "ok": True,
            "status": "active",
            "subscription_id": sub.id,
            "payment_id": result["payment_id"],
            "tier": activated.tier if activated else plan.tier,
            "expires_at": activated.expires_at.isoformat() if activated and activated.expires_at else None,
            "message": "Списание прошло, подписка активирована",
        }
    elif result.get("pending"):
        # СБП-списание async и ещё не дожато до CONFIRMED. Оставляем sub в pending
        # (yk_payment_id уже проставлен) — webhook CONFIRMED активирует её позже
        # через activate_from_webhook. Renew-крон не перепродлит: дедуп по pending-
        # подписке того же tier. НЕ помечаем failed (деньги могут списаться).
        db.commit()
        log.info("charge_recurrent: sub=%s СБП-списание PENDING (pid=%s) — ждём webhook",
                 sub.id, result.get("payment_id"))
        return {
            "ok": False,
            "status": "pending",
            "subscription_id": sub.id,
            "payment_id": result.get("payment_id"),
            "message": "СБП-списание в обработке",
            "failure_kind": "pending",
        }
    else:
        # REJECTED / AUTHORIZED (ждёт) / etc — помечаем sub failed
        sub.status = "failed"
        sub.cancelled_at = datetime.now(timezone.utc)
        db.commit()
        return {
            "ok": False,
            "status": result.get("status", "failed"),
            "subscription_id": sub.id,
            "payment_id": result.get("payment_id"),
            "message": result.get("message", "Списание отклонено банком"),
            "error_code": result.get("error_code"),
            # 'nsf' (недостаточно средств) → card-1 fallback; иначе 'other'.
            "failure_kind": result.get("failure_kind", "other"),
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  7b. RECONCILER СБП-привязок — дотягивание AccountToken после оплаты
# ═══════════════════════════════════════════════════════════════════════════════

def resolve_sbp_bindings(db: Session, max_age_minutes: int = 30) -> dict:
    """
    Для СБП-подписок с sbp_request_key, но без payment_method_id — тянет статус
    привязки счёта через GetAddAccountQrState и:
      - ACTIVE → сохраняет AccountToken+BankMemberId в user_payment_methods,
        линкует к подписке → автопродление включено.
      - INACTIVE/REJECTED или старше max_age (give-up) → пробуем переиспользовать
        ПРОШЛУЮ живую заряжаемую СБП-привязку юзера (кейс «вернувшийся уже-
        привязанный»: банк не переспрашивает привязку → новый ключ висит
        PROCESSING, но старый токен всё ещё ACTIVE). Нашли → линкуем, автопродление
        сохранено; нет → подписка РАЗОВАЯ. В обоих случаях чистим sbp_request_key.
        НЕ ошибка («оплатил, не привязал»).
      - PROCESSING и не старо → ждём следующий цикл.

    Запускать из orchestrator каждые ~15 мин (run_expire_only). Возвращает счётчики.

    Это шаги 3-5 сценария autopay T-Bank (Вариант №2): надёжный способ получить
    AccountToken (вместо ненадёжного webhook о привязке).
    """
    provider = get_payment_provider()
    summary = {"checked": 0, "bound": 0, "reused": 0, "gave_up": 0, "pending": 0}
    if not hasattr(provider, "get_account_qr_state"):
        return summary

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=max_age_minutes)

    subs = db.query(Subscription).filter(
        Subscription.sbp_request_key.isnot(None),
        Subscription.payment_method_id.is_(None),
    ).all()

    for sub in subs:
        summary["checked"] += 1
        st = provider.get_account_qr_state(sub.sbp_request_key)  # type: ignore[attr-defined]
        status = (st or {}).get("status")
        token = (st or {}).get("account_token")
        bank = (st or {}).get("bank_member_id")

        if status == "ACTIVE" and token:
            # Сохраняем привязку через _upsert_payment_method (синтетический event).
            ev = WebhookEvent(
                payment_id=sub.yk_payment_id or "",
                event_type="payment.succeeded",
                payment_method="sbp",
                account_token=str(token),
                bank_member_id=str(bank) if bank else None,
                customer_key=str(sub.user_id),
            )
            pm = _upsert_payment_method(db, sub.user_id, ev)
            if pm:
                sub.payment_method_id = pm.id
            sub.sbp_request_key = None  # готово — больше не поллим
            if sub.yk_method != "sbp":
                sub.yk_method = "sbp"
            db.commit()
            summary["bound"] += 1
            log.info(
                "resolve_sbp_bindings: sub=%s привязка ACTIVE → pm=%s, автопродление включено",
                sub.id, pm.id if pm else None,
            )
        elif status in ("INACTIVE", "REJECTED") or (sub.created_at and sub.created_at < cutoff):
            # Новый RequestKey не дал привязки (перебито/таймаут). Частый кейс —
            # «вернувшийся уже-привязанный юзер»: банк не переспрашивает привязку,
            # новый ключ висит PROCESSING, ЗАТО прошлая привязка всё ещё ACTIVE
            # (токен становится INACTIVE только при ПОДТВЕРЖДЕНИИ новой привязки).
            # Поэтому переиспользуем последнюю живую ЗАРЯЖАЕМУЮ СБП-привязку юзера,
            # если есть. Money-safe: мёртвый токен лишь провалит ОДНО продление
            # (ChargeQr 3015/5031 → not nsf → без card-1 fallback) — как и сегодня;
            # зато в типичном кейсе автопродление сохраняется. Оптимистично, БЕЗ
            # повторной проверки ACTIVE: GetAddAccountQrState по СТАРОМУ RequestKey
            # может вернуть INACTIVE для ЗАПРОСА, хотя ТОКЕН ещё заряжается
            # (request-superseded ≠ token-dead) → проверка дала бы ложный отказ и
            # навсегда потеряла бы живую привязку. bank_member_id обязателен для
            # ChargeQr (иначе 5031) → отсекаем legacy-017 строки без него.
            # ⚠️ Линковка без row-lock корректна при ОДНОМ orchestrator-процессе
            # (15-мин слоты не пересекаются, см. run_expire_only); второй
            # параллельный процесс мог бы сдвоить — это деплой-инвариант, не гард.
            reuse_pm = db.query(UserPaymentMethod).filter(
                UserPaymentMethod.user_id == sub.user_id,
                UserPaymentMethod.method_type == "sbp",
                UserPaymentMethod.deleted_at.is_(None),
                UserPaymentMethod.account_token.isnot(None),
                UserPaymentMethod.bank_member_id.isnot(None),
            ).order_by(
                UserPaymentMethod.created_at.desc(), UserPaymentMethod.id.desc()
            ).first()
            sub.sbp_request_key = None  # больше не поллим (и не оставляем stale-ключ)
            if reuse_pm:
                sub.payment_method_id = reuse_pm.id
                if sub.yk_method != "sbp":
                    sub.yk_method = "sbp"
                db.commit()
                summary["reused"] += 1
                log.info(
                    "resolve_sbp_bindings: sub=%s новый ключ не привязался (status=%s) → "
                    "переиспользуем прошлую СБП-привязку pm=%s, автопродление сохранено",
                    sub.id, status, reuse_pm.id,
                )
            else:
                db.commit()
                summary["gave_up"] += 1
                log.info(
                    "resolve_sbp_bindings: sub=%s привязки нет (status=%s) → разовая подписка",
                    sub.id, status,
                )
        else:
            summary["pending"] += 1  # PROCESSING, ждём следующий цикл

    if summary["checked"]:
        log.info("resolve_sbp_bindings: %s", summary)
    return summary


# ═══════════════════════════════════════════════════════════════════════════════
#  8. Helpers для UI «Сохранённые карты»
# ═══════════════════════════════════════════════════════════════════════════════

def list_payment_methods(db: Session, user: User) -> list[UserPaymentMethod]:
    """Все активные (не удалённые) карты юзера, default первой."""
    return db.query(UserPaymentMethod).filter(
        UserPaymentMethod.user_id == user.id,
        UserPaymentMethod.deleted_at.is_(None),
    ).order_by(
        UserPaymentMethod.is_default.desc(),
        UserPaymentMethod.created_at.desc(),
    ).all()


def get_default_payment_method(db: Session, user: User) -> UserPaymentMethod | None:
    """Default карта или первая активная если default не отмечен."""
    pms = list_payment_methods(db, user)
    if not pms:
        return None
    return next((p for p in pms if p.is_default), pms[0])


def soft_delete_payment_method(
    db: Session, user: User, payment_method_id: int
) -> bool:
    """
    Отвязать способ оплаты: soft-delete строки + СТИРАНИЕ рекуррент-токенов.
    Возвращает True если удалено, False если не нашли.

    Токены (rebill_id / account_token) обнуляются, а не только скрываются:
    требование платёжных провайдеров (ЮKassa при подключении автоплатежей,
    T-Bank RemoveCustomer-compliance) — при самостоятельной отвязке юзером
    магазин обязан удалить токен повторных списаний из своей системы.
    Провайдеру ничего сообщать не нужно. Строка остаётся для аудита
    (card_last4/brand), но списать по ней уже нечем.
    """
    pm = db.query(UserPaymentMethod).filter(
        UserPaymentMethod.id == payment_method_id,
        UserPaymentMethod.user_id == user.id,
        UserPaymentMethod.deleted_at.is_(None),
    ).first()
    if not pm:
        return False

    pm.deleted_at = datetime.now(timezone.utc)
    pm.rebill_id = None
    pm.account_token = None
    was_default = pm.is_default
    pm.is_default = False
    db.flush()

    # Если удалили default — назначим default'ом следующую активную (если есть)
    if was_default:
        next_pm = db.query(UserPaymentMethod).filter(
            UserPaymentMethod.user_id == user.id,
            UserPaymentMethod.deleted_at.is_(None),
        ).order_by(UserPaymentMethod.created_at.desc()).first()
        if next_pm:
            next_pm.is_default = True
            db.flush()

    db.commit()
    log.info("Soft-deleted payment method id=%s user=%s", payment_method_id, user.id)
    return True
