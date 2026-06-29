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
        customer_email: str | None = None,
        customer_phone: str | None = None,
        widget_mode: bool = False,
        recurrent: bool = False,
        customer_key: str | None = None,
    ) -> CheckoutSession:
        # все доп-параметры игнорируются stub'ом
        _ = (customer_email, customer_phone, widget_mode, recurrent, customer_key)
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

    def create_sbp_checkout(
        self,
        *,
        amount: float,
        currency: str,
        description: str,
        metadata: dict | None = None,
        customer_email: str | None = None,
        customer_phone: str | None = None,
        recurrent: bool = False,
        customer_key: str | None = None,
    ) -> CheckoutSession:
        """Фейковый СБП-QR для dev: плейсхолдер-картинка + локальный payload."""
        _ = (customer_email, customer_phone, recurrent, customer_key, metadata)
        payment_id = f"stub_{uuid.uuid4().hex[:16]}"
        # Inline-SVG плейсхолдер вместо реального QR (рендерится в <img>).
        qr_image = (
            "data:image/svg+xml,"
            "%3Csvg%20xmlns='http://www.w3.org/2000/svg'%20width='200'%20height='200'%3E"
            "%3Crect%20width='200'%20height='200'%20fill='%23eee'/%3E"
            "%3Ctext%20x='100'%20y='105'%20text-anchor='middle'%20font-size='16'%3ESTUB%20QR%3C/text%3E%3C/svg%3E"
        )
        qr_payload = f"/billing/stub?payment_id={payment_id}&amount={amount}&sbp=1"
        log.info("StubProvider.create_sbp_checkout: payment_id=%s amount=%s", payment_id, amount)
        return CheckoutSession(
            payment_id=payment_id,
            confirmation_url="",
            qr_image=qr_image,
            qr_payload=qr_payload,
        )

    def charge_qr(self, *, amount: float, account_token: str, **kwargs) -> dict:
        """Фейковое рекуррентное СБП-списание для dev — всегда успех."""
        _ = (account_token, kwargs)
        payment_id = f"stub_{uuid.uuid4().hex[:16]}"
        log.info("StubProvider.charge_qr: payment_id=%s amount=%s (mock success)", payment_id, amount)
        return {"payment_id": payment_id, "status": "CONFIRMED", "success": True, "amount": amount}

    def get_account_qr_state(self, request_key: str) -> dict | None:
        """Фейковый статус СБП-привязки для dev — сразу ACTIVE с токеном."""
        _ = request_key
        return {
            "status": "ACTIVE",
            "account_token": f"stub_acct_{uuid.uuid4().hex[:12]}",
            "bank_member_id": "100000000004",
        }

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
            # dev может симулировать СБП-привязку, прислав account_token в body.
            account_token=data.get("account_token"),
            rebill_id=data.get("rebill_id"),
        )

    def refund(self, payment_id: str, amount: float | None = None) -> bool:
        log.info("StubProvider.refund: payment_id=%s amount=%s (mock)", payment_id, amount)
        return True
