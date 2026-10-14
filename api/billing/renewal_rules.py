"""
Правила продления подписок без автосписания.

Чистые функции без БД и провайдера: их используют assert_can_purchase,
activate_from_webhook и send_expiry_reminders (api/billing/service.py), а тесты
(tests/test_renewal_rules.py) гоняют их без Postgres.

Зачем отдельно. Подписка, оплаченная без привязки (СБП на форме банка,
SberPay), или та, где юзер сам выключил автопродление, сама не продлится.
Таким юзерам шлём письмо за 3 дня до конца и даём продлить тот же план
досрочно, не теряя оставшиеся дни.
"""
from datetime import datetime


def will_auto_renew(sub) -> bool:
    """Продлится ли подписка сама: способ оплаты сохранён и автопродление не выключено."""
    return sub.payment_method_id is not None and sub.cancelled_at is None


def renewal_start(now: datetime, user_subs, tier: str, exclude_id: int | None = None) -> datetime:
    """С какого момента считать новый оплаченный период того же tier.

    Если у юзера есть активная подписка того же tier, которая сама не продлится,
    новый период начинается с её конца: досрочное продление не съедает оплаченные
    дни. Подписки с автопродлением не учитываем: крон продлевает их за сутки до
    конца, и эту механику не меняем. Триалы тоже не учитываем.
    """
    start = now
    for s in user_subs:
        if s.id == exclude_id or s.status != "active" or s.tier != tier or s.is_trial:
            continue
        if will_auto_renew(s):
            continue
        if s.expires_at and s.expires_at > start:
            start = s.expires_at
    return start
