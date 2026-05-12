"""
Адаптер для ЮKassa REST API.

Документация: https://yookassa.ru/developers/api

Этот класс активируется когда в .env заданы YOOKASSA_SHOP_ID + YOOKASSA_SECRET_KEY.
До этого момента factory.py возвращает StubPaymentProvider.

Метод create_checkout:
  POST https://api.yookassa.ru/v3/payments
  Basic Auth: SHOP_ID:SECRET_KEY
  Idempotency-Key: UUID (защита от дубликатов)

Webhook:
  ЮKassa присылает JSON на наш URL /api/billing/webhook
  В body — {type, event, object: {id, status, amount, metadata, payment_method, ...}}
  Проверка подлинности — либо через white-list IP, либо через event.id проверка
  в ответ на получение (по API GET /payments/{id}). Мы делаем lookup по id —
  если платёж реально succeeded в ЮKassa, доверяем.
"""
import base64
import json
import logging
import uuid
from typing import Any

import httpx

from api.billing.provider import CheckoutSession, WebhookEvent

log = logging.getLogger(__name__)

YOOKASSA_API_BASE = "https://api.yookassa.ru/v3"


class YooKassaProvider:
    name = "yookassa"

    def __init__(self, shop_id: str, secret_key: str):
        if not shop_id or not secret_key:
            raise ValueError("YooKassaProvider requires shop_id + secret_key")
        self.shop_id = shop_id
        self.secret_key = secret_key
        # Basic auth header — SHOP_ID:SECRET_KEY в base64
        auth_raw = f"{shop_id}:{secret_key}".encode("utf-8")
        self._auth_header = "Basic " + base64.b64encode(auth_raw).decode("ascii")

    def _headers(self, idempotency_key: str | None = None) -> dict:
        h = {
            "Authorization": self._auth_header,
            "Content-Type": "application/json",
        }
        if idempotency_key:
            h["Idempotency-Key"] = idempotency_key
        return h

    # ---------------------------------------------------------------------
    #  create_checkout — создание платёжной сессии
    # ---------------------------------------------------------------------
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
    ) -> CheckoutSession:
        """
        Создаёт платёж в ЮKassa. Возвращает id + confirmation_url (embedded widget
        или страница ЮKassa).

        customer_email/phone и widget_mode — игнорируются (ЮKassa использует
        внешнюю кассу для фискализации, и SpeedPay-like виджеты у неё иначе).
        """
        _ = (customer_email, customer_phone, widget_mode)
        body: dict[str, Any] = {
            "amount": {
                "value": f"{amount:.2f}",
                "currency": currency,
            },
            "confirmation": {
                "type": "redirect",
                "return_url": return_url,
            },
            "capture": True,  # автосписание (не двухэтапка)
            "description": description[:128],  # ЮKassa ограничение
        }
        if metadata:
            body["metadata"] = {k: str(v) for k, v in metadata.items()}

        # Idempotency-Key — UUID чтобы при ретрае не создать дубликат
        idem_key = str(uuid.uuid4())

        try:
            with httpx.Client(timeout=15) as client:
                resp = client.post(
                    f"{YOOKASSA_API_BASE}/payments",
                    headers=self._headers(idempotency_key=idem_key),
                    json=body,
                )
        except httpx.HTTPError as e:
            log.error("YooKassa.create_checkout: HTTP error: %s", e)
            raise

        if resp.status_code >= 400:
            log.error("YooKassa.create_checkout: %s %s", resp.status_code, resp.text)
            resp.raise_for_status()

        data = resp.json()
        payment_id = data["id"]
        confirmation_url = data["confirmation"]["confirmation_url"]
        log.info(
            "YooKassa.create_checkout OK: payment_id=%s amount=%.2f %s",
            payment_id, amount, currency,
        )
        return CheckoutSession(payment_id=payment_id, confirmation_url=confirmation_url)

    # ---------------------------------------------------------------------
    #  parse_webhook — приём уведомления
    # ---------------------------------------------------------------------
    def parse_webhook(self, raw_body: bytes, headers: dict) -> WebhookEvent | None:
        """
        ЮKassa НЕ подписывает webhook'и (на 2026). Защита — либо white-list IP
        (185.71.76.0/27, 185.71.77.0/27, 77.75.153.0/25, 77.75.156.11/32, ...),
        либо проверка через GET /payments/{id} (двойной запрос, но 100% надёжно).

        Мы делаем второй вариант — после парсинга делаем дополнительный GET
        по id, чтобы убедиться что статус реально такой. Это живёт в service.py,
        здесь только парсим body.
        """
        try:
            data = json.loads(raw_body.decode("utf-8"))
        except Exception as e:
            log.warning("YooKassa.parse_webhook: invalid JSON: %s", e)
            return None

        event_type = data.get("event")
        obj = data.get("object", {})
        payment_id = obj.get("id")
        if not event_type or not payment_id:
            log.warning("YooKassa.parse_webhook: missing event or payment_id")
            return None

        amount_raw = obj.get("amount", {}).get("value")
        try:
            amount = float(amount_raw) if amount_raw else None
        except (ValueError, TypeError):
            amount = None

        return WebhookEvent(
            payment_id=payment_id,
            event_type=event_type,  # 'payment.succeeded' / 'payment.canceled' / 'refund.succeeded'
            payment_method=(obj.get("payment_method") or {}).get("type"),
            amount=amount,
            metadata=obj.get("metadata") or {},
        )

    # ---------------------------------------------------------------------
    #  Проверка статуса (используется после webhook как double-check)
    # ---------------------------------------------------------------------
    def verify_payment(self, payment_id: str) -> dict | None:
        """GET /payments/{id} — получить актуальное состояние из ЮKassa."""
        try:
            with httpx.Client(timeout=15) as client:
                resp = client.get(
                    f"{YOOKASSA_API_BASE}/payments/{payment_id}",
                    headers=self._headers(),
                )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            log.error("YooKassa.verify_payment(%s): %s", payment_id, e)
            return None

    # ---------------------------------------------------------------------
    #  refund — возврат средств
    # ---------------------------------------------------------------------
    def refund(self, payment_id: str, amount: float | None = None) -> bool:
        body: dict[str, Any] = {"payment_id": payment_id}
        if amount is not None:
            body["amount"] = {"value": f"{amount:.2f}", "currency": "RUB"}

        idem_key = str(uuid.uuid4())
        try:
            with httpx.Client(timeout=15) as client:
                resp = client.post(
                    f"{YOOKASSA_API_BASE}/refunds",
                    headers=self._headers(idempotency_key=idem_key),
                    json=body,
                )
            resp.raise_for_status()
            log.info("YooKassa.refund: payment_id=%s → refund.id=%s",
                     payment_id, resp.json().get("id"))
            return True
        except httpx.HTTPError as e:
            log.error("YooKassa.refund(%s): %s", payment_id, e)
            return False
