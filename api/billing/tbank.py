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
import os
import uuid
from typing import Any

import httpx

from api.billing.provider import CheckoutSession, WebhookEvent

log = logging.getLogger(__name__)

TBANK_API_BASE = "https://securepay.tinkoff.ru/v2"

# ─── Receipt (54-ФЗ) ────────────────────────────────────────────────────
# Включать ТОЛЬКО когда:
#   1) у ИП/ООО реальный (не DEMO) терминал
#   2) в кабинете T-Bank в настройках "Касса" → "Не использую онлайн-кассу"
#      (T-Bank сам фискализирует и шлёт в ОФД)
#   3) или подключена внешняя онлайн-касса (атол / эвотор / штрих-м)
#
# Для DEMO terminal'а Receipt не нужен (T-Bank его игнорирует, но логирует
# warning). Включай через env: TBANK_RECEIPT_ENABLED=1
TBANK_RECEIPT_ENABLED = os.getenv("TBANK_RECEIPT_ENABLED", "").strip() in ("1", "true", "yes")

# Система налогообложения. Допустимые значения T-Bank API:
#   osn / usn_income / usn_income_outcome / envd / esn / patent
TBANK_TAXATION = os.getenv("TBANK_TAXATION", "usn_income").strip()

# НДС для подписки. Допустимые: none / vat0 / vat10 / vat20 / vat110 / vat120
#   - УСН (доходы / доходы-расходы) → 'none' (НДС не платим)
#   - ОСН → 'vat20' (или 'vat10' для льготных категорий)
TBANK_VAT = os.getenv("TBANK_VAT", "none").strip()

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
    #  Receipt (54-ФЗ) helper
    # ─────────────────────────────────────────────────────────────────────
    def _build_receipt(
        self,
        amount: float,
        description: str,
        email: str | None,
        phone: str | None,
    ) -> dict | None:
        """
        Конструирует Receipt-блок для Init-запроса по правилам T-Bank Acquiring +
        требованиям 54-ФЗ. Возвращает dict или None если фискализация отключена.

        Spec: https://developer.tbank.ru/eacq/api/v2#init-receipt
        """
        if not TBANK_RECEIPT_ENABLED:
            return None

        # Хотя бы один из контактов обязателен — иначе T-Bank вернёт 1010 ошибку.
        if not email and not phone:
            log.warning(
                "TBank.Receipt: no email/phone provided — пропускаем Receipt "
                "(payment пройдёт без фискализации; для production нужен contact)"
            )
            return None

        amount_kopeks = int(round(amount * 100))
        receipt: dict[str, Any] = {
            "Taxation": TBANK_TAXATION,
            "Items": [
                {
                    "Name": (description or "Подписка")[:128],  # max 128 chars
                    "Price": amount_kopeks,        # копейки
                    "Quantity": 1.0,
                    "Amount": amount_kopeks,       # копейки
                    "PaymentMethod": "full_prepayment",
                    "PaymentObject": "service",    # подписка = service
                    "Tax": TBANK_VAT,
                }
            ],
        }
        if email:
            receipt["Email"] = email[:64]
        if phone:
            # T-Bank ожидает E.164 без '+' (+79991234567 → 79991234567) или с '+'
            phone_norm = phone.strip()
            if phone_norm.startswith("8"):
                phone_norm = "+7" + phone_norm[1:]
            elif phone_norm.startswith("7") and not phone_norm.startswith("+"):
                phone_norm = "+" + phone_norm
            receipt["Phone"] = phone_norm[:19]
        return receipt

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
        customer_email: str | None = None,
        customer_phone: str | None = None,
        widget_mode: bool = False,
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

        # SuccessURL — куда T-Bank вернёт пользователя при успешной оплате.
        # FailURL — куда при отказе/ошибке. Если оба одинаковы, пользователь
        # увидит "Оплата прошла!" даже при fail, потому что polling в этой
        # точке начнёт долбить /api/billing/status. Выводим fail_url из success
        # подстановкой /billing/success → /billing/fail (frontend всегда
        # передаёт return_url ведущий на success).
        success_url = return_url
        if "/billing/success" in return_url:
            fail_url = return_url.replace("/billing/success", "/billing/fail")
        elif return_url.endswith("/success"):
            fail_url = return_url[:-len("/success")] + "/fail"
        else:
            # неизвестный шаблон — fail_url = success_url (старый поведение)
            fail_url = return_url

        body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "Amount": int(round(amount * 100)),  # копейки!
            "OrderId": order_id,
            # Description до 250 символов
            "Description": description[:250] if description else "Подписка",
            "SuccessURL": success_url,
            "FailURL": fail_url,
        }

        # Token — считаем до добавления DATA и Receipt (они в подписи не участвуют)
        body["Token"] = self._make_token(body)

        # DATA — произвольные key-value, до 20 пар, строки. Передаём наш metadata
        # для последующего матчинга в webhook'е (подписка, тариф, и т.п.).
        data_block: dict[str, str] = {}
        if metadata:
            data_block = {
                str(k): str(v)[:256]  # значение до 256 символов
                for k, v in metadata.items()
                if v is not None
            }
        if widget_mode:
            # Обязательный маркер для T-Bank JS SDK (SpeedPay / Integration.js).
            # Без него T-Bank не гарантирует корректную работу виджета.
            # https://developer.tbank.ru/eacq/intro/developer/setup_js/setup_speedpay/
            data_block["connection_type"] = "Widget"
        if data_block:
            body["DATA"] = data_block

        # Receipt — 54-ФЗ фискализация. T-Bank сам пробивает чек в ОФД и отправляет
        # пользователю по email/SMS. Включается через TBANK_RECEIPT_ENABLED=1.
        receipt = self._build_receipt(amount, description, customer_email, customer_phone)
        if receipt is not None:
            body["Receipt"] = receipt

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
