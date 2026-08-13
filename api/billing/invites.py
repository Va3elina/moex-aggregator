"""
Сервис redeem-ссылок.

Три операции:
  create_invites  — админ создаёт N токенов (разовые или многоразовые)
  redeem_invite   — user применяет токен → создаётся Subscription + role поднимается
  list_invites    — админ видит все токены + статистика использований
  revoke_invite   — админ отзывает токен

Токен — криптографически случайная строка 32 символа (24 байта → base64url).
"""
import logging
import secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy.orm import Session

from api.billing.plans import TIER_LEVELS
from api.billing.service import sync_user_role
from api.models.subscription import Subscription
from api.models.subscription_invite import SubscriptionInvite, InviteRedemption
from api.models.user import User

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  Create — админ генерирует invite-токены
# ═══════════════════════════════════════════════════════════════════════════════

def create_invites(
    db: Session,
    admin: User,
    tier: str,
    duration_days: int,
    count: int = 1,
    expires_in_days: int = 30,
    max_uses_per_token: int = 1,
    note: str | None = None,
) -> list[SubscriptionInvite]:
    """
    Создаёт N invite-токенов. Один токен может быть применён max_uses_per_token раз
    (по умолчанию 1 — одноразовая ссылка для одного человека).

    duration_days — сколько дней подписки получит user после redeem'а.
    expires_in_days — через сколько сам токен истекает (если его не применят).
    """
    # premium удалён из TIER_LEVELS (plans.py) — инвайт с tier=premium дал бы
    # уровень 0 (= ничего). Ограничиваем создание реально существующими тирами,
    # чтобы админ случайно не выпустил «пустые» инвайты.
    if tier not in {"basic", "pro"}:
        raise ValueError(f"Invalid tier: {tier}")
    if duration_days < 1 or duration_days > 3650:  # до 10 лет
        raise ValueError("duration_days must be 1..3650")
    if count < 1 or count > 1000:
        raise ValueError("count must be 1..1000")
    if max_uses_per_token < 1:
        raise ValueError("max_uses_per_token must be >= 1")

    expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)

    invites: list[SubscriptionInvite] = []
    for _ in range(count):
        token = secrets.token_urlsafe(24)  # ~32 chars base64url
        invite = SubscriptionInvite(
            token=token,
            tier=tier,
            duration_days=duration_days,
            max_uses=max_uses_per_token,
            uses_count=0,
            expires_at=expires_at,
            created_by_user_id=admin.id,
            note=note,
        )
        db.add(invite)
        invites.append(invite)

    db.commit()
    log.info(
        "Admin #%s created %d invite(s): tier=%s days=%d uses=%d expires=%s note=%r",
        admin.id, count, tier, duration_days, max_uses_per_token, expires_at, note,
    )
    return invites


# ═══════════════════════════════════════════════════════════════════════════════
#  Redeem — user применяет токен
# ═══════════════════════════════════════════════════════════════════════════════

class RedeemError(Exception):
    """Базовый класс ошибок при redeem'е."""


def redeem_invite(db: Session, user: User, token: str) -> Subscription:
    """
    Применяет invite-токен для user'а.
    Идемпотентна — если этот же user уже редеемил этот токен, просто возвращает
    ранее созданную подписку (UNIQUE constraint защищает от дублей).

    Raises RedeemError с понятным сообщением если токен невалидный / истёк /
    исчерпан / отозван.
    """
    # with_for_update: блокируем строку инвайта на время транзакции, чтобы
    # параллельные redeem'ы одного multi-use токена сериализовались. Без этого
    # два одновременных запроса оба проходили проверку uses_count < max_uses и
    # оба инкрементили → лимит можно было превысить. Идемпотентность (один юзер
    # повторно) защищена отдельно ниже, тут — про РАЗНЫХ юзеров в гонке.
    invite = db.query(SubscriptionInvite).filter(
        SubscriptionInvite.token == token
    ).with_for_update().first()

    if not invite:
        raise RedeemError("Ссылка недействительна")

    now = datetime.now(timezone.utc)
    if invite.revoked_at:
        raise RedeemError("Ссылка отозвана администратором")
    if invite.expires_at < now:
        raise RedeemError("Срок действия ссылки истёк")
    if invite.uses_count >= invite.max_uses:
        raise RedeemError("Лимит использований ссылки исчерпан")

    # Идемпотентность — если уже редеемил, возвращаем старую подписку
    existing = db.query(InviteRedemption).filter(
        InviteRedemption.token == token,
        InviteRedemption.user_id == user.id,
    ).first()
    if existing and existing.subscription_id:
        sub = db.query(Subscription).filter(
            Subscription.id == existing.subscription_id
        ).first()
        if sub:
            return sub  # уже применял — просто отдаём

    # Создаём подписку
    expires_at = now + timedelta(days=invite.duration_days)
    sub = Subscription(
        user_id=user.id,
        tier=invite.tier,
        period="invite",                              # отличаем от платных monthly/yearly
        plan_id=f"invite_{invite.tier}",
        amount=Decimal("0.00"),
        currency="RUB",
        status="active",
        started_at=now,
        expires_at=expires_at,
    )
    db.add(sub)
    db.flush()

    # Запись redemption
    redemption = InviteRedemption(
        token=token,
        user_id=user.id,
        subscription_id=sub.id,
    )
    db.add(redemption)

    # Инкремент счётчика
    invite.uses_count = invite.uses_count + 1

    # Синхронизируем users.role
    sync_user_role(db, user)

    db.commit()

    log.info(
        "User #%s redeemed invite %s... → sub #%s (tier=%s, expires=%s)",
        user.id, token[:8], sub.id, invite.tier, expires_at,
    )
    return sub


# ═══════════════════════════════════════════════════════════════════════════════
#  List / Revoke — админ управляет
# ═══════════════════════════════════════════════════════════════════════════════

def list_invites(db: Session, admin: User, only_active: bool = False) -> list[dict]:
    """Возвращает список invite'ов, созданных этим админом (или всех если admin-super).
    Для простоты — показываем все."""
    q = db.query(SubscriptionInvite).order_by(SubscriptionInvite.created_at.desc())
    rows = q.all()
    result = []
    now = datetime.now(timezone.utc)
    for inv in rows:
        is_active = (
            inv.revoked_at is None
            and inv.expires_at > now
            and inv.uses_count < inv.max_uses
        )
        if only_active and not is_active:
            continue
        result.append({
            "token": inv.token,
            "tier": inv.tier,
            "duration_days": inv.duration_days,
            "max_uses": inv.max_uses,
            "uses_count": inv.uses_count,
            "expires_at": inv.expires_at.isoformat() if inv.expires_at else None,
            "revoked_at": inv.revoked_at.isoformat() if inv.revoked_at else None,
            "note": inv.note,
            "created_at": inv.created_at.isoformat() if inv.created_at else None,
            "created_by_user_id": inv.created_by_user_id,
            "is_active": is_active,
        })
    return result


def revoke_invite(db: Session, admin: User, token: str) -> bool:
    """Отозвать токен — он больше не работает. История использования сохраняется."""
    invite = db.query(SubscriptionInvite).filter(
        SubscriptionInvite.token == token
    ).first()
    if not invite:
        return False
    if invite.revoked_at:
        return True  # уже отозван
    invite.revoked_at = datetime.now(timezone.utc)
    db.commit()
    log.info("Admin #%s revoked invite %s...", admin.id, token[:8])
    return True


class InviteDeleteError(Exception):
    """Удаление невозможно (например, токен ещё живой)."""


def delete_invite(db: Session, admin: User, token: str) -> bool:
    """
    Насовсем удаляет отработавший токен из БД — чтобы список не зарастал.

    Только для НЕактивных (истёк / исчерпан / отозван). Живую ссылку сначала
    надо отозвать: удаление живого токена молча убило бы рабочую ссылку.

    Записи InviteRedemption уезжают по ON DELETE CASCADE, но выданные подписки
    не трогаются — они живут отдельными строками в subscriptions.
    """
    invite = db.query(SubscriptionInvite).filter(
        SubscriptionInvite.token == token
    ).first()
    if not invite:
        return False

    now = datetime.now(timezone.utc)
    is_active = (
        invite.revoked_at is None
        and invite.expires_at > now
        and invite.uses_count < invite.max_uses
    )
    if is_active:
        raise InviteDeleteError("Ссылка ещё активна — сначала отзови её")

    db.delete(invite)
    db.commit()
    log.info("Admin #%s deleted invite %s... permanently", admin.id, token[:8])
    return True
