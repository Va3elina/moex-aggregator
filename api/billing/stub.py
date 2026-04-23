"""
Заглушка платёжного провайдера — используется когда в .env нет ключей ЮKassa.

Логика:
- create_checkout → возвращает фейковый URL вида /billing/stub-checkout/{payment_id}
  (frontend может показать "тестовый режим" и кнопку "Симулировать оплату")
- parse_webhook → принимает любые JSON с полями {payment_id, status}
  (можно руками дернуть curl'ом для тестирования)
- refund → логирует и возвращает True

Позволяет пройти полный flow "запрос → редирект → webhook → активация подписки"
БЕЗ реальных ключей ЮKassa. Когда коллега зарегистрируется и даст ключи —
просто меняется провайдер в factory.py.
"""
import uuid
import json
import logging

from api.billing.provider import CheckoutSession, WebhookEvent

log = logging.getLogger(__name__)


class StubPaymentProvider:
    name = "stub"

    def create_checkout(
        self,
        *,
        amount: float,
        currency: str,
        description: str,
        return_url: str,
        metadata: dict | None = None,
    ) -> CheckoutSession:
        payment_id = f"stub_{uuid.uuid4().hex[:16]}"
        # В stub-режиме фронт редиректит на свою же страницу /billing/stub
        # которая покажет "Тестовый режим" и кнопку "Симулировать успех".
        confirmation_url = (
            f"/billing/stub?payment_id={payment_id}"
            f"&amount={amount}&return_url={return_url}"
        )
        log.info(
            "StubProvider.create_checkout: payment_id=%s amount=%s %s description=%r",
            payment_id, amount, currency, description,
        )
        return CheckoutSession(payment_id=payment_id, confirmation_url=confirmation_url)

    def parse_webhook(self, raw_body: bytes, headers: dict) -> WebhookEvent | None:
        """В stub принимаем сырой JSON без проверки подписи."""
        try:
            data = json.loads(raw_body.decode("utf-8"))
        except Exception as e:
            log.warning("StubProvider.parse_webhook: can't parse body: %s", e)
            return None

        payment_id = data.get("payment_id") or data.get("object", {}).get("id")
        status = data.get("status") or data.get("event", "").split(".")[-1]
        if not payment_id:
            return None

        event_type = {
            "succeeded": "payment.succeeded",
            "canceled": "payment.canceled",
            "refunded": "refund.succeeded",
        }.get(status, "payment.succeeded")

        return WebhookEvent(
            payment_id=payment_id,
            event_type=event_type,
            payment_method=data.get("method", "bank_card"),
            amount=data.get("amount"),
            metadata=data.get("metadata") or {},
        )

    def refund(self, payment_id: str, amount: float | None = None) -> bool:
        log.info("StubProvider.refund: payment_id=%s amount=%s (mock)", payment_id, amount)
        return True
