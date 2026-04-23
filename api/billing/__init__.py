"""
Billing subsystem — обработка платежей и подписок.

Архитектура:
  plans.py     — справочник тарифных планов (monthly / yearly / lifetime)
  provider.py  — абстрактный протокол PaymentProvider (create_checkout, parse_webhook, ...)
  yookassa.py  — реализация провайдера через ЮKassa REST API
  stub.py      — заглушка для разработки (когда нет ключей) — возвращает фейковые URL-ы
  factory.py   — выбор провайдера по env (реальный или stub)
  service.py   — бизнес-логика: создание/активация/отмена подписок + синхронизация users.role

Webhook handler:
  api/routers/billing.py::webhook  — принимает уведомления от ЮKassa,
  делегирует service.activate_subscription / cancel_subscription.
"""
from api.billing.factory import get_payment_provider

__all__ = ["get_payment_provider"]
