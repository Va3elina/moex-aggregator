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

import base64
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from typing import Any

import httpx

from api.billing.provider import CheckoutSession, WebhookEvent
from api.ru_tls import RU_TLS_VERIFY

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

# Коды отказа эмитента «недостаточно средств» (NSF) — единственные, при которых
# имеет смысл списать МЕНЬШУЮ месячную сумму (card-1 fallback годовой→месячный).
# Источник: developer.tbank.ru/eacq/intro/errors/error-codes (сверено 2026-06-07).
# КОНСЕРВАТИВНО: только явные insufficient-funds. Прочие отказы (карта истекла/
# заблокирована, лимит, «повторите позже», generic-decline) НЕ считаем NSF —
# для них месячный fallback не поможет, поведение остаётся прежним (без fallback).
TBANK_NSF_CODES = {"103", "116", "1051", "5060"}


def classify_charge_failure(error_code: Any) -> str:
    """Классификация отказа Charge для card-1 fallback.
    'nsf'   — недостаточно средств (можно попробовать меньшую месячную сумму);
    'other' — всё прочее (терминальный отказ / retry-later / неизвестный код) →
              fallback НЕ делаем (безопасный дефолт)."""
    if error_code is not None and str(error_code).strip() in TBANK_NSF_CODES:
        return "nsf"
    return "other"


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
    def _make_token(self, params: dict[str, Any], *, extra_exclude: tuple[str, ...] = ()) -> str:
        """
        T-Bank Token algorithm:
          1. Берём все top-level скалярные параметры (исключая Receipt, DATA, Token).
          2. Добавляем поле Password со значением shared secret.
          3. Сортируем по ключу в алфавитном порядке.
          4. Конкатенируем значения (без разделителей).
          5. SHA256 → hex lowercase.

        extra_exclude — доп. поля, НЕ участвующие в подписи для конкретного метода.
        Для AddCard T-Bank считает токен БЕЗ SuccessURL/FailURL/NotificationURL
        (эмпирически проверено: с URL в подписи → ErrorCode 204 «Неверный токен»,
        URL только в теле → Success). У Init, наоборот, URL в подписи участвуют —
        поэтому исключение точечное (через _post_signed), а не глобальное.
        """
        # bool/None/dict исключаем — Token считается только по простым скалярам
        flat: dict[str, str] = {}
        for k, v in params.items():
            if k in ("Receipt", "DATA", "Token") or k in extra_exclude:
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

        # Защита: synthetic OAuth email (*@oauth.local) несуществующий — в чек
        # 54-ФЗ его слать нельзя. Гейт верификации в billing/service.py не пустит
        # сюда неверифицированного юзера, но дублируем на всякий случай.
        if email and email.endswith("@oauth.local"):
            email = None

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
        recurrent: bool = False,
        customer_key: str | None = None,
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

        # Рекуррентный платёж: сохраняем карту для будущих /v2/Charge без 3DS.
        # T-Bank требует:
        #   - Recurrent = "Y"
        #   - CustomerKey — уникальный идентификатор клиента (у нас str(user.id))
        # После CONFIRMED в webhook'е придёт RebillId — сохраняем его в
        # user_payment_methods (см. service.activate_from_webhook).
        # https://developer.tbank.ru/eacq/api/v2#init-recurrent
        if recurrent:
            if not customer_key:
                raise ValueError(
                    "TBankProvider.create_checkout: recurrent=True требует customer_key"
                )
            body["Recurrent"] = "Y"
            body["CustomerKey"] = customer_key[:36]  # T-Bank лимит 36 символов

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
            with httpx.Client(timeout=15, verify=RU_TLS_VERIFY) as client:
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
    #  create_sbp_checkout — Init(Recurrent,QR) + GetQr (рекуррентный СБП)
    #  Вариант №2 T-Bank: привязка счёта во время первой оплаты по QR.
    #  https://developer.tbank.ru/eacq/scenarios/payments/PCI_DSS/autopay/
    # ─────────────────────────────────────────────────────────────────────
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
        """
        СБП по QR с привязкой счёта (Вариант №2):
          1. Init(Recurrent="Y", DATA={"QR":"true"}, Description) → PaymentId
          2. GetQr(DataType=IMAGE) → картинка QR; GetQr(DataType=PAYLOAD) → СБП-ссылка

        confirmation_url пуст — оплата завершается в приложении банка по QR, без
        redirect'а на платёжку (это и обходит недоверенный РФ-серт хостед-страницы).
        AccountToken для будущих ChargeQr приходит ПОЗЖЕ в webhook на
        NotificationURL терминала (не здесь).
        """
        if currency and currency.upper() != "RUB":
            log.warning("TBank.create_sbp_checkout: terminal обычно RUB, передан %s", currency)
        if not customer_key:
            raise ValueError("create_sbp_checkout: customer_key обязателен (рекуррентный СБП)")

        order_id = uuid.uuid4().hex

        # ── Init ──────────────────────────────────────────────────────
        body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "Amount": int(round(amount * 100)),  # копейки!
            "OrderId": order_id,
            # Description ОБЯЗАТЕЛЕН для QR-привязки (Вариант №2 T-Bank).
            "Description": (description or "Подписка")[:250],
            # Recurrent + DATA={"QR":"true"} создают привязку счёта для ChargeQr.
            "Recurrent": "Y",
            "CustomerKey": customer_key[:36],
        }
        body["Token"] = self._make_token(body)  # DATA/Receipt в подписи не участвуют
        data_block: dict[str, str] = {"QR": "true"}
        if metadata:
            data_block.update({
                str(k): str(v)[:256] for k, v in metadata.items() if v is not None
            })
        body["DATA"] = data_block
        receipt = self._build_receipt(amount, description, customer_email, customer_phone)
        if receipt is not None:
            body["Receipt"] = receipt

        try:
            with httpx.Client(timeout=15, verify=RU_TLS_VERIFY) as client:
                resp = client.post(f"{TBANK_API_BASE}/Init", json=body)
        except httpx.HTTPError as e:
            log.error("TBank.create_sbp_checkout: Init HTTP error: %s", e)
            raise RuntimeError(f"T-Bank Init(QR) failed: {e}") from e
        if resp.status_code >= 400:
            log.error("TBank.create_sbp_checkout: Init %s %s", resp.status_code, resp.text)
            resp.raise_for_status()
        init_data = resp.json()
        if not init_data.get("Success"):
            err = (
                f"T-Bank Init(QR) failed: {init_data.get('Message', 'unknown')} "
                f"(ErrorCode {init_data.get('ErrorCode')}) Details: {init_data.get('Details')}"
            )
            log.error("TBank.create_sbp_checkout: %s", err)
            raise RuntimeError(err)
        payment_id = str(init_data["PaymentId"])
        log.info(
            "TBank.create_sbp_checkout: Init OK payment_id=%s amount=%.2f RUB",
            payment_id, amount,
        )

        # ── GetQr: PAYLOAD (ссылка + RequestKey) + IMAGE (картинка) ──
        # RequestKey приходит в ответе GetQr — это id СБП-привязки, по нему потом
        # reconciler тянет AccountToken+BankMemberId через GetAddAccountQrState.
        # T-Bank GetQr DataType=IMAGE отдаёт СЫРОЙ SVG-markup (<svg ...>), НЕ base64
        # (проверено вживую 2026-06-29). Кодируем в base64 для data-URL.
        qr_payload, request_key = self._get_qr(payment_id, "PAYLOAD")
        qr_image, _ = self._get_qr(payment_id, "IMAGE")
        if qr_image:
            if qr_image.lstrip().startswith("<"):
                b64 = base64.b64encode(qr_image.encode("utf-8")).decode("ascii")
                qr_image = f"data:image/svg+xml;base64,{b64}"
            elif not qr_image.startswith("data:"):
                # defensive: если вдруг вернётся уже base64
                qr_image = f"data:image/svg+xml;base64,{qr_image}"

        return CheckoutSession(
            payment_id=payment_id,
            confirmation_url="",
            qr_image=qr_image,
            qr_payload=qr_payload,
            request_key=request_key,
        )

    def _get_qr(self, payment_id: str, data_type: str) -> tuple[str | None, str | None]:
        """POST /GetQr → (Data, RequestKey). data_type: 'IMAGE' (SVG-картинка QR)
        или 'PAYLOAD' (СБП-ссылка qr.nspk.ru/...). RequestKey — id привязки счёта.
        (None, None) если запрос не удался."""
        body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "PaymentId": str(payment_id),
            "DataType": data_type,
        }
        body["Token"] = self._make_token(body)
        try:
            with httpx.Client(timeout=15, verify=RU_TLS_VERIFY) as client:
                resp = client.post(f"{TBANK_API_BASE}/GetQr", json=body)
        except httpx.HTTPError as e:
            log.error("TBank.GetQr(%s,%s): HTTP error: %s", payment_id, data_type, e)
            return None, None
        if resp.status_code >= 400:
            log.error(
                "TBank.GetQr(%s,%s): %s %s",
                payment_id, data_type, resp.status_code, resp.text,
            )
            return None, None
        data = resp.json()
        if not data.get("Success"):
            log.error(
                "TBank.GetQr(%s,%s): Success=false %s",
                payment_id, data_type, data.get("Message"),
            )
            return None, None
        rk = data.get("RequestKey")
        return data.get("Data"), (str(rk) if rk else None)

    def get_account_qr_state(self, request_key: str) -> dict | None:
        """POST /GetAddAccountQrState — статус СБП-привязки счёта по RequestKey.
        Возвращает {status, account_token, bank_member_id} или None при ошибке.

        status: 'PROCESSING' (ещё формируется) / 'ACTIVE' (привязка готова, токен
        чарджабельный) / 'INACTIVE' (перебита более новой привязкой) / 'REJECTED'.
        account_token + bank_member_id заполнены при ACTIVE — оба нужны для ChargeQr.
        """
        body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "RequestKey": str(request_key),
        }
        body["Token"] = self._make_token(body)
        try:
            with httpx.Client(timeout=15, verify=RU_TLS_VERIFY) as client:
                resp = client.post(f"{TBANK_API_BASE}/GetAddAccountQrState", json=body)
        except httpx.HTTPError as e:
            log.error("TBank.get_account_qr_state(%s): HTTP error: %s", request_key, e)
            return None
        if resp.status_code >= 400:
            log.error(
                "TBank.get_account_qr_state(%s): %s %s",
                request_key, resp.status_code, resp.text,
            )
            return None
        data = resp.json()
        if not data.get("Success"):
            log.warning(
                "TBank.get_account_qr_state(%s): Success=false %s",
                request_key, data.get("Message"),
            )
            return None
        return {
            "status": data.get("Status"),
            "account_token": data.get("AccountToken"),
            "bank_member_id": data.get("BankMemberId"),
        }

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

        # Считаем ожидаемый Token из остальных полей.
        # compare_digest — constant-time, чтобы не утекать инфу о подписи через
        # тайминг побайтового сравнения (стандартный hardening для HMAC-проверок).
        expected = self._make_token(data)
        if not hmac.compare_digest(received_token, expected):
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

        # === Рекуррент: достаём RebillId + Pan + CardType если есть ===
        # T-Bank webhook содержит эти поля только для платежей с Recurrent="Y"
        # на стадии CONFIRMED (первое успешное списание создаёт привязку).
        # Для обычных (не recurrent) платежей RebillId будет None.
        rebill_id = data.get("RebillId")
        if rebill_id is not None:
            rebill_id = str(rebill_id)

        # CustomerKey — мы его слали в Init, T-Bank возвращает обратно.
        customer_key = data.get("CustomerKey")

        # Pan: T-Bank присылает "430000******0333" — берём только last4.
        pan = data.get("Pan") or ""
        card_last4 = pan[-4:] if len(pan) >= 4 else None

        # card_fingerprint — sha256 маскированного PAN. Маска (first6+last4) у
        # одной карты одинакова между разными CustomerKey/RebillId, поэтому это
        # рабочий кросс-аккаунтный анти-абуз ключ для триала (rebill_id — нет, он
        # per-привязка). 152-ФЗ: храним хеш, не сам PAN. Требуется ≥10 значащих
        # символов маски, иначе слишком слабо — тогда None.
        card_fingerprint = None
        if len(pan) >= 10:
            card_fingerprint = hashlib.sha256(pan.encode("utf-8")).hexdigest()

        # CardType: 'VISA' / 'MASTERCARD' / 'MIR' / 'MAESTRO' / 'JCB' / ...
        card_brand = data.get("CardType")

        # === СБП-QR: AccountToken приходит при успешной привязке счёта по QR ===
        # (Вариант №2 T-Bank) — СБП-аналог RebillId. RequestKey — id GetQr-привязки.
        # AccountToken — top-level скаляр → автоматически участвует в Token-подписи
        # (verify выше уже его проверил).
        account_token = data.get("AccountToken")
        if account_token is not None:
            account_token = str(account_token)
        request_key = data.get("RequestKey")
        if request_key is not None:
            request_key = str(request_key)

        # payment_method: T-Bank шлёт явно; иначе выводим (СБП если есть AccountToken)
        payment_method = data.get("PaymentMethod")
        if not payment_method:
            payment_method = "sbp" if account_token else "bank_card"

        return WebhookEvent(
            payment_id=payment_id,
            event_type=event_type,
            payment_method=payment_method,
            amount=amount,
            metadata=data.get("DATA") or {},
            rebill_id=rebill_id,
            customer_key=customer_key,
            card_last4=card_last4,
            card_brand=card_brand,
            card_fingerprint=card_fingerprint,
            account_token=account_token,
            request_key=request_key,
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
            with httpx.Client(timeout=15, verify=RU_TLS_VERIFY) as client:
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
    #  AddCard — привязка карты БЕЗ оплаты подписки (для пробного периода)
    #  Поток: AddCustomer → AddCard(CheckType) → клиент проходит форму/3DS →
    #         GetAddCardState(RequestKey) отдаёт RebillId для будущих Charge.
    #  CheckType: NO (без проверки) | HOLD (холд+отмена, без расчёта) |
    #             3DS (3DS + холд+отмена) | 3DSHOLD (проверка поддержки 3DS).
    #  https://developer.tbank.ru/eacq/api/v2
    # ─────────────────────────────────────────────────────────────────────
    def _post_signed(self, path: str, body: dict[str, Any]) -> dict:
        """POST к T-Bank: добавляет TerminalKey + Token, парсит JSON.

        Только для запросов БЕЗ Receipt/DATA (AddCustomer/AddCard/GetAddCardState) —
        _make_token считает подпись по плоским скалярам, что здесь и нужно.
        """
        payload = dict(body)
        payload["TerminalKey"] = self.terminal_key
        # AddCard: URL-поля идут в теле, но НЕ в подписи (иначе ErrorCode 204).
        # Для AddCustomer/GetAddCardState URL-полей нет → исключение безвредно.
        payload["Token"] = self._make_token(
            payload, extra_exclude=("SuccessURL", "FailURL", "NotificationURL")
        )
        try:
            with httpx.Client(timeout=15, verify=RU_TLS_VERIFY) as client:
                resp = client.post(f"{TBANK_API_BASE}/{path}", json=payload)
        except httpx.HTTPError as e:
            log.error("TBank.%s: HTTP error: %s", path, e)
            raise
        if resp.status_code >= 400:
            log.error("TBank.%s: %s %s", path, resp.status_code, resp.text)
            resp.raise_for_status()
        return resp.json()

    def add_customer(self, customer_key: str, email: str | None = None) -> dict:
        """Регистрирует покупателя у эквайера (нужно до AddCard). Идемпотентно."""
        body: dict[str, Any] = {"CustomerKey": str(customer_key)[:36]}
        if email and not email.endswith("@oauth.local"):
            body["Email"] = email[:64]
        return self._post_signed("AddCustomer", body)

    def add_card(
        self,
        customer_key: str,
        check_type: str = "3DS",
        *,
        success_url: str | None = None,
        fail_url: str | None = None,
        notification_url: str | None = None,
    ) -> dict:
        """Инициирует привязку карты. Возвращает {Success, PaymentURL, RequestKey, ...}.

        Клиента редиректим на PaymentURL (ввод карты/3DS). Деньги НЕ списываются
        (HOLD/3DS делают авторизацию-холд без расчёта). RebillId забираем через
        get_add_card_state.

        Success/Fail/NotificationURL — куда T-Bank вернёт клиента после привязки.
        Если НЕ передать и в настройках терминала они пусты → ErrorCode 9
        «Переадресовываемый url пуст» (привязка проходит, но редиректить некуда).
        Переданные значения перекрывают настройки терминала.
        """
        body: dict[str, Any] = {
            "CustomerKey": str(customer_key)[:36],
            "CheckType": check_type,
        }
        if success_url:
            body["SuccessURL"] = success_url
        if fail_url:
            body["FailURL"] = fail_url
        if notification_url:
            body["NotificationURL"] = notification_url
        return self._post_signed("AddCard", body)

    def get_add_card_state(self, request_key: str) -> dict:
        """Статус привязки по RequestKey. При COMPLETED содержит RebillId/CardId/Pan."""
        return self._post_signed("GetAddCardState", {"RequestKey": str(request_key)})

    # ─────────────────────────────────────────────────────────────────────
    #  charge — POST /Init + POST /Charge (рекуррентное списание)
    # ─────────────────────────────────────────────────────────────────────
    def charge(
        self,
        *,
        amount: float,
        currency: str,
        description: str,
        rebill_id: str,
        customer_key: str,
        customer_email: str | None = None,
        customer_phone: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """
        Списать средства с сохранённой карты по RebillId без участия юзера.

        T-Bank требует двухшаговый flow:
        1. POST /v2/Init с TerminalKey/Amount/OrderId/CustomerKey → PaymentId
           ⚠️ CustomerKey ОБЯЗАТЕЛЕН — это связка с привязкой карты, без неё
           T-Bank вернёт ErrorCode 309 «Неверные параметры». Recurrent="Y" НЕ
           передаём (это только для первого платежа, который создаёт привязку).
        2. POST /v2/Charge с PaymentId + RebillId → T-Bank списывает мгновенно
           (без 3DS, без формы — карта уже верифицирована при первом платеже)

        Используется auto-renewal cron'ом и admin endpoint'ом для теста №6.

        Возвращает {'payment_id', 'status', 'success', 'amount', 'message'?}.
        status: 'CONFIRMED' (deposited) / 'REJECTED' / 'AUTHORIZED' (ожидает) / ...
        success: True если status=CONFIRMED, иначе False.

        ВЫЗЫВАЕТ RuntimeError если /Init или /Charge провалились на уровне HTTP/API.
        """
        if currency and currency.upper() != "RUB":
            log.warning(
                "TBankProvider.charge: terminal обычно RUB, передан %s",
                currency,
            )

        order_id = uuid.uuid4().hex

        # ── Шаг 1: Init ─────────────────────────────────────────────────
        # CustomerKey обязателен: T-Bank по {CustomerKey+RebillId} находит
        # привязку карты и понимает что это рекуррент. Без него /Charge на шаге 2
        # вернёт 308 «Не найдено сохранённой карты».
        init_body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "Amount": int(round(amount * 100)),  # копейки!
            "OrderId": order_id,
            "CustomerKey": customer_key[:36],
            "Description": (description or "Авто-продление подписки")[:250],
        }
        init_body["Token"] = self._make_token(init_body)

        # DATA — для матчинга в webhook (наша metadata: subscription_id, plan_id)
        if metadata:
            init_body["DATA"] = {
                str(k): str(v)[:256]
                for k, v in metadata.items()
                if v is not None
            }

        # Receipt (54-ФЗ) — если фискализация включена и есть контакт, T-Bank
        # требует Receipt-блок ДЛЯ ВСЕХ /Init, включая рекуррентные. Без него
        # терминал-с-фискализацией возвращает 309 «expected.receipt».
        receipt = self._build_receipt(amount, description, customer_email, customer_phone)
        if receipt is not None:
            init_body["Receipt"] = receipt

        try:
            with httpx.Client(timeout=15, verify=RU_TLS_VERIFY) as client:
                resp = client.post(f"{TBANK_API_BASE}/Init", json=init_body)
        except httpx.HTTPError as e:
            log.error("TBank.charge: Init HTTP error: %s", e)
            raise RuntimeError(f"T-Bank Init failed: {e}") from e

        if resp.status_code >= 400:
            log.error("TBank.charge: Init %s %s", resp.status_code, resp.text)
            resp.raise_for_status()

        init_data = resp.json()
        if not init_data.get("Success"):
            err = (
                f"T-Bank Init failed: {init_data.get('Message', 'unknown')} "
                f"(ErrorCode {init_data.get('ErrorCode')}) "
                f"Details: {init_data.get('Details')}"
            )
            log.error("TBank.charge: %s", err)
            raise RuntimeError(err)

        payment_id = str(init_data["PaymentId"])
        log.info(
            "TBank.charge: Init OK payment_id=%s amount=%.2f RUB",
            payment_id, amount,
        )

        # ── Шаг 2: Charge ───────────────────────────────────────────────
        charge_body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "PaymentId": payment_id,
            "RebillId": str(rebill_id),
        }
        charge_body["Token"] = self._make_token(charge_body)

        try:
            with httpx.Client(timeout=30, verify=RU_TLS_VERIFY) as client:
                # Charge может ждать обработки карты дольше чем Init
                resp = client.post(f"{TBANK_API_BASE}/Charge", json=charge_body)
        except httpx.HTTPError as e:
            log.error("TBank.charge: Charge HTTP error pid=%s: %s", payment_id, e)
            raise RuntimeError(f"T-Bank Charge failed: {e}") from e

        if resp.status_code >= 400:
            log.error(
                "TBank.charge: Charge pid=%s %s %s",
                payment_id, resp.status_code, resp.text,
            )
            resp.raise_for_status()

        charge_data = resp.json()
        success = bool(charge_data.get("Success"))
        status = charge_data.get("Status", "").upper()

        result = {
            "payment_id": payment_id,
            "status": status,
            "success": success,
            "amount": amount,
        }
        if not success:
            result["message"] = charge_data.get("Message", "T-Bank Charge rejected")
            result["error_code"] = charge_data.get("ErrorCode")
            # Классификация для card-1 fallback: 'nsf' (недостаточно средств,
            # можно попробовать месячный) vs 'other' (без fallback).
            result["failure_kind"] = classify_charge_failure(charge_data.get("ErrorCode"))
            log.warning(
                "TBank.charge: REJECTED pid=%s status=%s code=%s msg=%s",
                payment_id, status,
                charge_data.get("ErrorCode"),
                charge_data.get("Message"),
            )
        else:
            log.info(
                "TBank.charge: OK pid=%s status=%s amount=%.2f RUB",
                payment_id, status, amount,
            )

        return result

    # ─────────────────────────────────────────────────────────────────────
    #  charge_qr — POST /Init + POST /ChargeQr (рекуррентный СБП)
    #  СБП-аналог charge(): списание по AccountToken без участия юзера.
    # ─────────────────────────────────────────────────────────────────────
    def charge_qr(
        self,
        *,
        amount: float,
        currency: str,
        description: str,
        account_token: str,
        customer_key: str,
        bank_member_id: str | None = None,
        customer_email: str | None = None,
        customer_phone: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """
        Рекуррентное списание по СБП-привязке.
        1. POST /v2/Init с Recurrent="Y" + DATA={"QR":"true"} + Description + CustomerKey
           + Receipt → PaymentId (сценарий autopay T-Bank, шаг «init» перед charge-qr).
        2. POST /v2/ChargeQr с PaymentId + AccountToken + **BankMemberId** → списание.

        ⚠️ Найдено живым тестом 2026-06-29:
          - BankMemberId ОБЯЗАТЕЛЕН, иначе ErrorCode 5031 «привязка не найдена».
          - Токен должен быть ACTIVE, иначе 3015 «Неверный статус AccountToken».
          - ChargeQr АСИНХРОННЫЙ: возвращает FORM_SHOWED, финал CONFIRMED приходит
            позже → дожимаем GetState. Если за окно не дожали → status='PENDING'
            (вызывающий оставляет подписку pending, webhook CONFIRMED её активирует).

        Контракт возврата как у charge(): {payment_id, status, success}; при отказе
        + message/error_code/failure_kind. status='PENDING' → ещё в обработке.
        ВЫЗЫВАЕТ RuntimeError если Init/ChargeQr провалились на уровне HTTP/API.
        """
        if currency and currency.upper() != "RUB":
            log.warning("TBank.charge_qr: terminal обычно RUB, передан %s", currency)

        order_id = uuid.uuid4().hex

        # ── Шаг 1: Init ───────────────────────────────────────────────
        init_body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "Amount": int(round(amount * 100)),  # копейки!
            "OrderId": order_id,
            "CustomerKey": customer_key[:36],
            "Description": (description or "Авто-продление подписки (СБП)")[:250],
            "Recurrent": "Y",
        }
        init_body["Token"] = self._make_token(init_body)
        data_block: dict[str, str] = {"QR": "true"}
        if metadata:
            data_block.update({
                str(k): str(v)[:256] for k, v in metadata.items() if v is not None
            })
        init_body["DATA"] = data_block
        receipt = self._build_receipt(amount, description, customer_email, customer_phone)
        if receipt is not None:
            init_body["Receipt"] = receipt

        try:
            with httpx.Client(timeout=15, verify=RU_TLS_VERIFY) as client:
                resp = client.post(f"{TBANK_API_BASE}/Init", json=init_body)
        except httpx.HTTPError as e:
            log.error("TBank.charge_qr: Init HTTP error: %s", e)
            raise RuntimeError(f"T-Bank Init(QR charge) failed: {e}") from e
        if resp.status_code >= 400:
            log.error("TBank.charge_qr: Init %s %s", resp.status_code, resp.text)
            resp.raise_for_status()
        init_data = resp.json()
        if not init_data.get("Success"):
            err = (
                f"T-Bank Init(QR charge) failed: {init_data.get('Message', 'unknown')} "
                f"(ErrorCode {init_data.get('ErrorCode')}) Details: {init_data.get('Details')}"
            )
            log.error("TBank.charge_qr: %s", err)
            raise RuntimeError(err)
        payment_id = str(init_data["PaymentId"])
        log.info("TBank.charge_qr: Init OK payment_id=%s amount=%.2f RUB", payment_id, amount)

        # ── Шаг 2: ChargeQr (BankMemberId ОБЯЗАТЕЛЕН) ──────────────────
        charge_body: dict[str, Any] = {
            "TerminalKey": self.terminal_key,
            "PaymentId": payment_id,
            "AccountToken": str(account_token),
        }
        if bank_member_id:
            charge_body["BankMemberId"] = str(bank_member_id)
        charge_body["Token"] = self._make_token(charge_body)
        try:
            with httpx.Client(timeout=30, verify=RU_TLS_VERIFY) as client:
                resp = client.post(f"{TBANK_API_BASE}/ChargeQr", json=charge_body)
        except httpx.HTTPError as e:
            log.error("TBank.charge_qr: ChargeQr HTTP error pid=%s: %s", payment_id, e)
            raise RuntimeError(f"T-Bank ChargeQr failed: {e}") from e
        if resp.status_code >= 400:
            log.error("TBank.charge_qr: ChargeQr pid=%s %s %s", payment_id, resp.status_code, resp.text)
            resp.raise_for_status()
        charge_data = resp.json()

        # Синхронный отказ ChargeQr (напр. 3015 INACTIVE-токен, 5031 без BankMemberId).
        if not charge_data.get("Success"):
            code = charge_data.get("ErrorCode")
            log.warning(
                "TBank.charge_qr: ChargeQr REJECTED(sync) pid=%s code=%s msg=%s",
                payment_id, code, charge_data.get("Message"),
            )
            return {
                "payment_id": payment_id, "status": "REJECTED", "success": False,
                "amount": amount,
                "message": charge_data.get("Message", "T-Bank ChargeQr rejected"),
                "error_code": code,
                "failure_kind": classify_charge_failure(code),
            }

        # ── Шаг 3: дожать async-статус ────────────────────────────────
        # СБП ChargeQr возвращает FORM_SHOWED, реальный исход — позже
        # (FORM_SHOWED→AUTHORIZING→CONFIRMED). Поллим GetState до терминального.
        TERMINAL = {"CONFIRMED", "REJECTED", "AUTH_FAIL", "CANCELED", "REVERSED"}
        status = (charge_data.get("Status") or "").upper()
        for _ in range(18):  # ~18×5с = 90с
            if status in TERMINAL:
                break
            time.sleep(5)
            info = self.verify_payment(payment_id)
            if info:
                status = (info.get("Status") or "").upper()

        if status == "CONFIRMED":
            log.info("TBank.charge_qr: OK pid=%s CONFIRMED amount=%.2f RUB", payment_id, amount)
            return {"payment_id": payment_id, "status": "CONFIRMED", "success": True, "amount": amount}

        if status in TERMINAL:  # REJECTED/AUTH_FAIL/CANCELED/REVERSED
            log.warning("TBank.charge_qr: pid=%s терминальный отказ status=%s", payment_id, status)
            # Код отказа СБП приходит в webhook, не в GetState → failure_kind='other'
            # (безопасно: card-1 fallback не триггерим без явного NSF).
            return {
                "payment_id": payment_id, "status": status, "success": False,
                "amount": amount, "message": f"ChargeQr {status}", "failure_kind": "other",
            }

        # Не дожали за окно — оставляем PENDING. Вызывающий не помечает fail;
        # webhook CONFIRMED активирует подписку по yk_payment_id позже.
        log.info("TBank.charge_qr: pid=%s ещё в обработке (status=%s) → PENDING", payment_id, status)
        return {"payment_id": payment_id, "status": "PENDING", "success": False,
                "amount": amount, "pending": True}

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
            with httpx.Client(timeout=15, verify=RU_TLS_VERIFY) as client:
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


class TBankDemoProvider(TBankProvider):
    """
    Демо-терминал T-Bank (витрина оплаты без реального приёма денег).

    Отличается от боевого TBankProvider ТОЛЬКО именем: сеть, подпись Token,
    Init/GetState — всё то же самое, просто на тестовом терминале, где деньги
    не списываются. Имя выделено, чтобы биллинг мог отличить такой платёж от
    боевого и НИКОГДА не активировать по нему подписку:

      • service._verify_with_provider → False для 'tbank_demo';
      • service.activate_from_webhook → ранний выход;
      • service.sync_pending_for_user → закрывает pending как неоплаченный.

    Продления существующих карт сюда не попадают: они роутятся по
    pm.provider ('tbank') через factory.get_provider_by_name.
    """

    name = "tbank_demo"
