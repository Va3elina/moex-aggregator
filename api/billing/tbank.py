"""
Адаптер для эквайринга Т-Банка (T-Bank Acquiring v2).

Документация:
  - JS Acquiring intro:  https://developer.tbank.ru/eacq/intro/developer/setup_js/
  - REST API:            https://developer.tbank.ru/eacq/api/v2

Активируется когда в .env заданы TBANK_TERMINAL_KEY + TBANK_PASSWORD.
До этого момента factory.py возвращает StubPaymentProvider.

КРАТКАЯ СПЕЦИФИКА T-Bank:
  - Сумма передаётся в копейках (а не рублях). 1400.00 RUB = 140000.
  - Подпись Token = SHA256(concat sorted-by-key values + Password). hex lowercase.
  - bool параметры (Recurrent, PayType=...) сериализуются как 'true'/'false'.
  - OrderId должен быть уникальным в рамках терминала (для идемпотентности).
  - Receipt и DATA — НЕ участвуют в подсчёте Token.
  - Webhook нужно отвечать plain text "OK" (HTTP 200) — иначе ретраи.

МАППИНГ Status → event_type (наш контракт):
  CONFIRMED                          → 'payment.succeeded'
  REJECTED / CANCELED / DEADLINE_EXPIRED / REVERSED → 'payment.canceled'
  REFUNDED / PARTIAL_REFUNDED        → 'refund.succeeded'
  Остальные (NEW, AUTHORIZED, *_ING) — интермедиаты, возвращаем None.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from typing import Any

import httpx

from api.billing.provider import CheckoutSession, WebhookEvent

log = logging.getLogger(__name__)

TBANK_API_BASE = "https://securepay.tinkoff.ru/v2"

# Маппинг T-Bank Status → наш контракт event_type
_STATUS_MAP = {
    "CONFIRMED": "payment.succeeded",
    "REJECTED": "payment.canceled",
    "CANCELED": "payment.canceled",
    "DEADLINE_EXPIRED": "payment.canceled",
    "REVERSED": "payment.canceled",
    "REFUNDED": "refund.succeeded",
    "PARTIAL_REFUNDED": "refund.succeeded",
}


class TBankProvider:
    name = "tbank"

    def __init__(self, terminal_key: str, password: str):
        if not terminal_key or not password:
            raise ValueError("TBankProvider requires terminal_key + password")
        self.terminal_key = terminal_key
        self.password = password

    # ─────────────────────────────────────────────────────────────────────
    #  Подпись Token
    # ─────────────────────────────────────────────────────────────────────
    def _make_token(self, params: dict[str, Any]) -> str:
        """
        T-Bank Token algorithm:
          1. Берём все top-level скалярные параметры (исключая Receipt, DATA, Token).
          2. Добавляем поле Password со значением shared secret.
          3. Сортируем по ключу в алфавитном порядке.
          4. Конкатенируем значения (без разделителей).
          5. SHA256 → hex lowercase.
        """
        # bool/None/dict исключаем — Token считается только по простым скалярам
        flat: dict[str, str] = {}
        for k, v in params.items():
            if k in ("Receipt", "DATA", "Token"):
                continue
            if v is None:
                continue
            if isinstance(v, dict | list):
                continue  # вложенные структуры не участвуют в Token
            if isinstance(v, bool):
                flat[k] = "true" if v else "false"
            else:
                flat[k] = str(v)
        flat["Password"] = self.password

        sorted_values = [flat[k] for k in sorted(flat.keys())]
        concat = "".join(sorted_values)
        return hashlib.sha256(concat.encode("utf-8")).hexdigest()

    # ─────────────────────────────────────────────────────────────────────
    #  create_checkout — POST /Init
    # ─────────────────────────────────────────────────────────────────────
    def create_checkout(
        self,
        *,
        amount: float,
        currency: str,
        description: str,
        return_url: str,
        metadata: dict | None = None,
    ) -> CheckoutSession:
        """
        Создаёт платёж через T-Bank API. Возвращает PaymentId + PaymentURL.

        currency: T-Bank по-умолчанию работает в RUB на terminal'е, доп. валюты
        требуют отдельной настройки. Передаём только для совместимости с
        интерфейсом — если currency != 'RUB', логируем warning.
        """
        if currency and currency.upper() != "RUB":
            log.warning(
                "TBankProvider: terminal обычно настроен на RUB, передан currency=%s",
                currency,
            )

        # OrderId — короткий уникальный идентификатор в рамках терминала.
        # 32 hex символа UUID без дефисов — укладывается в лимит T-Bank (36).
        order_id = uuid.uuid4().hex

        body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "Amount": int(round(amount * 100)),  # копейки!
            "OrderId": order_id,
            # Description до 250 символов
            "Description": description[:250] if description else "Подписка",
            # SuccessURL/FailURL — возврат пользователя в браузер после оплаты
            "SuccessURL": return_url,
            "FailURL": return_url,
        }

        # Token — считаем до добавления DATA (она в подписи не участвует)
        body["Token"] = self._make_token(body)

        # DATA — произвольные key-value, до 20 пар, строки. Передаём наш metadata
        # для последующего матчинга в webhook'е (подписка, тариф, и т.п.).
        if metadata:
            body["DATA"] = {
                str(k): str(v)[:256]  # значение до 256 символов
                for k, v in metadata.items()
                if v is not None
            }

        try:
            with httpx.Client(timeout=15) as client:
                resp = client.post(f"{TBANK_API_BASE}/Init", json=body)
        except httpx.HTTPError as e:
            log.error("TBank.create_checkout: HTTP error: %s", e)
            raise

        if resp.status_code >= 400:
            log.error("TBank.create_checkout: %s %s", resp.status_code, resp.text)
            resp.raise_for_status()

        data = resp.json()
        if not data.get("Success"):
            log.error(
                "TBank.create_checkout: Success=false ErrorCode=%s Message=%s Details=%s",
                data.get("ErrorCode"),
                data.get("Message"),
                data.get("Details"),
            )
            raise RuntimeError(
                f"T-Bank Init failed: {data.get('Message', 'unknown error')} "
                f"(code {data.get('ErrorCode')})"
            )

        payment_id = str(data["PaymentId"])
        payment_url = data["PaymentURL"]
        log.info(
            "TBank.create_checkout OK: payment_id=%s order_id=%s amount=%.2f RUB",
            payment_id, order_id, amount,
        )
        return CheckoutSession(payment_id=payment_id, confirmation_url=payment_url)

    # ─────────────────────────────────────────────────────────────────────
    #  parse_webhook — приём нотификации от T-Bank
    # ─────────────────────────────────────────────────────────────────────
    def parse_webhook(self, raw_body: bytes, headers: dict) -> WebhookEvent | None:
        """
        T-Bank присылает POST с подписанным JSON (Content-Type: application/json).
        Проверяем Token — если не совпадает, отбрасываем.

        После приёма нужно вернуть plain text "OK" — это делает router-level код
        (см. routers/billing.py — он смотрит provider.name).
        """
        try:
            data = json.loads(raw_body.decode("utf-8"))
        except Exception as e:
            log.warning("TBank.parse_webhook: invalid JSON: %s", e)
            return None

        # Проверка подписи
        received_token = data.get("Token", "")
        if not received_token:
            log.warning("TBank.parse_webhook: no Token in body")
            return None

        # Считаем ожидаемый Token из остальных полей
        expected = self._make_token(data)
        if received_token != expected:
            log.warning(
                "TBank.parse_webhook: Token mismatch — likely fake/replay attack"
            )
            return None

        # TerminalKey должен совпадать с нашим (защита от отправки чужих
        # уведомлений на наш endpoint, если злоумышленник знает наш URL)
        if data.get("TerminalKey") != self.terminal_key:
            log.warning(
                "TBank.parse_webhook: TerminalKey mismatch (got %s, expected %s)",
                data.get("TerminalKey"), self.terminal_key,
            )
            return None

        status = data.get("Status", "").upper()
        payment_id = str(data.get("PaymentId", ""))
        if not payment_id or not status:
            log.warning("TBank.parse_webhook: missing PaymentId or Status")
            return None

        event_type = _STATUS_MAP.get(status)
        if not event_type:
            log.info(
                "TBank.parse_webhook: intermediate Status=%s — ignoring",
                status,
            )
            return None

        # Сумма в копейках → рубли
        amount_kopeks = data.get("Amount")
        try:
            amount = float(amount_kopeks) / 100.0 if amount_kopeks else None
        except (ValueError, TypeError):
            amount = None

        return WebhookEvent(
            payment_id=payment_id,
            event_type=event_type,
            payment_method=data.get("PaymentMethod") or "bank_card",
            amount=amount,
            metadata=data.get("DATA") or {},
        )

    # ─────────────────────────────────────────────────────────────────────
    #  verify_payment — POST /GetState (double-check после webhook)
    # ─────────────────────────────────────────────────────────────────────
    def verify_payment(self, payment_id: str) -> dict | None:
        """
        Запрашивает актуальный статус платежа у T-Bank. Используется
        billing.service._verify_with_provider — защита от подделанных webhook'ов.
        """
        body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "PaymentId": str(payment_id),
        }
        body["Token"] = self._make_token(body)

        try:
            with httpx.Client(timeout=15) as client:
                resp = client.post(f"{TBANK_API_BASE}/GetState", json=body)
        except httpx.HTTPError as e:
            log.error("TBank.verify_payment(%s): %s", payment_id, e)
            return None

        if resp.status_code >= 400:
            log.error(
                "TBank.verify_payment(%s): %s %s",
                payment_id, resp.status_code, resp.text,
            )
            return None

        data = resp.json()
        if not data.get("Success"):
            log.warning(
                "TBank.verify_payment(%s): Success=false (%s)",
                payment_id, data.get("Message"),
            )
            return None
        return data

    # ─────────────────────────────────────────────────────────────────────
    #  refund — POST /Cancel
    # ─────────────────────────────────────────────────────────────────────
    def refund(self, payment_id: str, amount: float | None = None) -> bool:
        """
        Полный или частичный возврат.

        В T-Bank API единый endpoint /Cancel работает на всех стадиях:
          - AUTHORIZED   → отменяет холд (reverse)
          - CONFIRMED    → возврат на карту (refund)
          - PARTIAL      → если передать Amount меньше списанного
        """
        body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "PaymentId": str(payment_id),
        }
        if amount is not None:
            body["Amount"] = int(round(amount * 100))  # копейки
        body["Token"] = self._make_token(body)

        try:
            with httpx.Client(timeout=15) as client:
                resp = client.post(f"{TBANK_API_BASE}/Cancel", json=body)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            log.error("TBank.refund(%s): %s", payment_id, e)
            return False

        if not data.get("Success"):
            log.error(
                "TBank.refund(%s): Success=false ErrorCode=%s Message=%s",
                payment_id, data.get("ErrorCode"), data.get("Message"),
            )
            return False

        log.info(
            "TBank.refund(%s): new Status=%s",
            payment_id, data.get("Status"),
        )
        return True
