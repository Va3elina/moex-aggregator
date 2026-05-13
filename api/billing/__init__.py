"""
Billing subsystem — обработка платежей и подписок.

Архитектура:
  plans.py     — справочник тарифных планов (monthly / yearly)
  provider.py  — абстрактный протокол PaymentProvider (create_checkout, parse_webhook, ...)
  tbank.py     — реализация для T-Bank Acquiring v2 (единственный production-провайдер)
  stub.py      — заглушка для dev (когда нет TBANK ключей) — fake URL-ы
  factory.py   — выбор провайдера по env (T-Bank или stub)
  service.py   — бизнес-логика: создание/активация/отмена/refund подписок + sync users.role
  tiers.py     — функции проверки tier-уровня user'а
  invites.py   — invite-токены для admin'а (выдать pro/premium бесплатно)

YooKassa удалена 2026-05-13 — не использовалась несколько недель, и
наличие двух code paths мешало развитию (любой fix в sync нужно было
синхронизировать). См. git history если нужно вернуть.

Webhook handler:
  api/routers/billing.py::webhook — принимает уведомления от T-Bank,
  делегирует service.activate_from_webhook / cancel_by_webhook.
"""
from api.billing.factory import get_payment_provider

__all__ = ["get_payment_provider"]
