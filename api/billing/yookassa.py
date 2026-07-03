"""
Адаптер ЮKassa REST API (v3) — второй заход (первый удалён 88b7ef1).

Документация: https://yookassa.ru/developers/api
Магазин: shopId + секретный ключ (Basic auth), env YOOKASSA_SHOP_ID /
YOOKASSA_SECRET_KEY. Активируется НЕ глобально, а через factory-роутинг
по юзеру (env YOOKASSA_TEST_USER_IDS) — фаза теста, T-Bank остаётся
дефолтом. См. factory.get_provider_for_user.

Чем модель проще T-Bank:
  • рекуррент: платёж с save_payment_method=true → webhook приносит
    payment_method.{id, saved:true} → автосписание = НОВЫЙ платёж с
    payment_method_id. ЕДИНЫЙ механизм для карт/СБП/SberPay/T-Pay —
    charge() и charge_qr() здесь один и тот же вызов.
  • СБП идёт через общую redirect-форму ЮKassa (confirmation_url) —
    отдельный QR-флоу (GetQr/ChargeQr/BankMemberId) не нужен.
  • фискализация yoo_receipt: чек пробивает сама ЮKassa, мы кладём
    receipt-блок в каждый платёж (как T-Bank Receipt).

Верификация webhook'ов: ЮKassa НЕ подписывает уведомления (в отличие от
T-Bank Token). Поэтому parse_webhook НЕ доверяет телу: берёт из него только
id и делает GET /payments/{id} — событие строится из ответа API. Подделка
webhook'а максимум заставит нас лишний раз спросить ЮKassa.

Маппинг в наши структуры:
  • payment_method.id (сохранённый) → карта: WebhookEvent.rebill_id;
    СБП: WebhookEvent.account_token. Так существующий upsert в service.py
    сам проставит method_type='card'/'sbp', а charge_recurrent разветвится
    в charge()/charge_qr() — оба бьют в один POST /payments.
  • verify_payment() возвращает T-Bank-подобный dict {"Status": ...,
    "Amount": копейки, "PaymentMethod": ...} — чтобы sync_pending_for_user
    и _verify_with_provider работали одной веткой для обоих провайдеров.
"""
import json
import logging
import uuid

import httpx

from api.billing.provider import CheckoutSession, WebhookEvent

log = logging.getLogger(__name__)

YOOKASSA_API_BASE = "https://api.yookassa.ru/v3"
_TIMEOUT = 30.0

# ЮKassa payment.status → T-Bank-подобный Status (для sync/verify единой веткой).
# refunded_amount>0 перекрывает на REFUNDED (у ЮKassa возврат НЕ меняет status).
_STATUS_MAP = {
    "succeeded": "CONFIRMED",
    "canceled": "REJECTED",
    "pending": "NEW",
    "waiting_for_capture": "AUTHORIZED",
}

# cancellation_details.reason → failure_kind для card-1 NSF-fallback
# (см. service.renew_expiring_subs). Консервативно: только явный
# insufficient_funds считаем NSF.
_NSF_REASONS = {"insufficient_funds"}


class YooKassaProvider:
    name = "yookassa"

    def __init__(self, shop_id: str, secret_key: str):
        if not shop_id or not secret_key:
            raise ValueError("YooKassaProvider requires shop_id + secret_key")
        self.shop_id = shop_id
        self.secret_key = secret_key

    # ------------------------------------------------------------------ #
    #  HTTP helpers
    # ------------------------------------------------------------------ #
    def _request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        idempotence_key: str | None = None,
    ) -> dict:
        """Запрос к API. Кидает httpx.HTTPStatusError на не-2xx (с телом ЮKassa)."""
        headers = {"Content-Type": "application/json"}
        if idempotence_key:
            headers["Idempotence-Key"] = idempotence_key
        resp = httpx.request(
            method,
            f"{YOOKASSA_API_BASE}{path}",
            content=json.dumps(body) if body is not None else None,
            headers=headers,
            auth=(self.shop_id, self.secret_key),
            timeout=_TIMEOUT,
        )
        if resp.status_code >= 400:
            # Тело ошибки ЮKassa содержит code/description — тащим в лог целиком
            log.error(
                "YooKassa %s %s -> %s: %s", method, path, resp.status_code, resp.text[:500]
            )
            resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _build_receipt(
        amount: float,
        description: str,
        email: str | None,
        phone: str | None,
    ) -> dict | None:
        """
        Receipt-блок 54-ФЗ (фискализация yoo_receipt включена на магазине —
        без receipt платёж не создастся). vat_code=1 — без НДС (УСН доходы).
        """
        customer: dict = {}
        if email:
            customer["email"] = email
        elif phone:
            customer["phone"] = phone
        else:
            return None  # чекаут гейтит verified email — сюда попадать не должны
        return {
            "customer": customer,
            "items": [{
                "description": description[:128],
                "quantity": "1.00",
                "amount": {"value": f"{amount:.2f}", "currency": "RUB"},
                "vat_code": 1,
                "payment_subject": "service",
                "payment_mode": "full_payment",
            }],
        }

    # ------------------------------------------------------------------ #
    #  Checkout
    # ------------------------------------------------------------------ #
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
        widget_mode: bool = False,   # T-Bank-специфично, игнорируем
        recurrent: bool = False,
        customer_key: str | None = None,  # у ЮKassa связка через metadata.user_id
    ) -> CheckoutSession:
        """
        POST /payments → redirect на форму ЮKassa (все методы магазина: карта,
        СБП, SberPay, T-Pay). recurrent=True → save_payment_method=true
        (безусловная привязка): форма сама покажет только методы/банки,
        поддерживающие сохранение.
        """
        body: dict = {
            "amount": {"value": f"{amount:.2f}", "currency": currency or "RUB"},
            "capture": True,
            "confirmation": {"type": "redirect", "return_url": return_url},
            "description": description[:128],
            "metadata": metadata or {},
        }
        if recurrent:
            body["save_payment_method"] = True
        receipt = self._build_receipt(amount, description, customer_email, customer_phone)
        if receipt:
            body["receipt"] = receipt

        data = self._request("POST", "/payments", body, idempotence_key=str(uuid.uuid4()))
        confirmation_url = (data.get("confirmation") or {}).get("confirmation_url", "")
        log.info(
            "YooKassa checkout created: id=%s amount=%.2f recurrent=%s",
            data["id"], amount, recurrent,
        )
        return CheckoutSession(payment_id=data["id"], confirmation_url=confirmation_url)

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
        СБП у ЮKassa — через ту же redirect-форму (payment_method_data.type=sbp
        фиксирует метод). QR рисует сама форма; наш T-Bank-овский QR-флоу
        (qr_image/qr_payload) не нужен → возвращаем confirmation_url.
        """
        body: dict = {
            "amount": {"value": f"{amount:.2f}", "currency": currency or "RUB"},
            "capture": True,
            "confirmation": {
                "type": "redirect",
                # return_url в create_sbp_checkout не передаётся по Protocol —
                # ведём на страницу успеха, поллинг статуса там уже есть.
                "return_url": "https://xn--80aklbnczmv.xn--p1ai/billing/success",
            },
            "description": description[:128],
            "metadata": metadata or {},
            "payment_method_data": {"type": "sbp"},
        }
        if recurrent:
            body["save_payment_method"] = True
        receipt = self._build_receipt(amount, description, customer_email, customer_phone)
        if receipt:
            body["receipt"] = receipt

        data = self._request("POST", "/payments", body, idempotence_key=str(uuid.uuid4()))
        confirmation_url = (data.get("confirmation") or {}).get("confirmation_url", "")
        return CheckoutSession(payment_id=data["id"], confirmation_url=confirmation_url)

    # ------------------------------------------------------------------ #
    #  Рекуррентные списания
    # ------------------------------------------------------------------ #
    def _charge_saved(
        self,
        *,
        amount: float,
        currency: str,
        description: str,
        payment_method_id: str,
        customer_email: str | None,
        customer_phone: str | None,
        metadata: dict | None,
    ) -> dict:
        """
        Автосписание сохранённым методом: POST /payments с payment_method_id,
        без confirmation. Возвращает charge()-контракт
        {payment_id, status, success[, pending, message, error_code, failure_kind]}.
        """
        body: dict = {
            "amount": {"value": f"{amount:.2f}", "currency": currency or "RUB"},
            "capture": True,
            "payment_method_id": payment_method_id,
            "description": description[:128],
            "metadata": metadata or {},
        }
        receipt = self._build_receipt(amount, description, customer_email, customer_phone)
        if receipt:
            body["receipt"] = receipt

        try:
            data = self._request("POST", "/payments", body, idempotence_key=str(uuid.uuid4()))
        except httpx.HTTPStatusError as e:
            # Синхронный отказ API (400/403…) — до создания платежа
            return {
                "payment_id": None,
                "status": "REJECTED",
                "success": False,
                "message": f"YooKassa error: {e.response.status_code}",
                "error_code": str(e.response.status_code),
                "failure_kind": "other",
            }

        status = data.get("status", "")
        if status == "succeeded":
            return {"payment_id": data["id"], "status": "CONFIRMED", "success": True}
        if status in ("pending", "waiting_for_capture"):
            # Асинхронное списание (типично для СБП-автоплатежей) — дожмётся
            # webhook'ом payment.succeeded. Контракт как у tbank ChargeQr.
            return {
                "payment_id": data["id"],
                "status": "PENDING",
                "success": False,
                "pending": True,
                "message": f"async charge in progress ({status})",
            }
        # canceled — разбираем причину для NSF-fallback
        details = data.get("cancellation_details") or {}
        reason = details.get("reason", "unknown")
        return {
            "payment_id": data.get("id"),
            "status": "REJECTED",
            "success": False,
            "message": f"canceled: {reason}",
            "error_code": reason,
            "failure_kind": "nsf" if reason in _NSF_REASONS else "other",
        }

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
        """Карточный рекуррент: rebill_id = сохранённый payment_method.id ЮKassa."""
        return self._charge_saved(
            amount=amount, currency=currency, description=description,
            payment_method_id=rebill_id,
            customer_email=customer_email, customer_phone=customer_phone,
            metadata=metadata,
        )

    def charge_qr(
        self,
        *,
        amount: float,
        currency: str,
        description: str,
        account_token: str,
        customer_key: str,
        bank_member_id: str | None = None,  # T-Bank-специфично, не нужен
        customer_email: str | None = None,
        customer_phone: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """СБП-рекуррент: account_token = тот же payment_method.id ЮKassa."""
        return self._charge_saved(
            amount=amount, currency=currency, description=description,
            payment_method_id=account_token,
            customer_email=customer_email, customer_phone=customer_phone,
            metadata=metadata,
        )

    def get_account_qr_state(self, request_key: str) -> dict | None:
        """T-Bank-специфичный поллинг СБП-привязки — у ЮKassa привязка приходит
        сразу в объекте платежа (payment_method.saved). Не используется."""
        return None

    # ------------------------------------------------------------------ #
    #  Verify / webhook / refund
    # ------------------------------------------------------------------ #
    def verify_payment(self, payment_id: str) -> dict | None:
        """
        GET /payments/{id} → T-Bank-подобный dict {"Status", "Amount" (копейки),
        "PaymentMethod", "raw"} — сознательно та же форма, что tbank.verify_payment,
        чтобы sync_pending_for_user/_verify_with_provider работали одной веткой.
        """
        try:
            data = self._request("GET", f"/payments/{payment_id}")
        except Exception as e:
            log.warning("YooKassa verify_payment(%s) failed: %s", payment_id, e)
            return None

        status = _STATUS_MAP.get(data.get("status", ""), "NEW")
        # Возврат у ЮKassa НЕ меняет status платежа — смотрим refunded_amount
        refunded = float((data.get("refunded_amount") or {}).get("value", 0) or 0)
        if refunded > 0:
            total = float((data.get("amount") or {}).get("value", 0) or 0)
            status = "REFUNDED" if refunded >= total else "PARTIAL_REFUNDED"

        try:
            amount_kopecks = round(float(data["amount"]["value"]) * 100)
        except (KeyError, ValueError, TypeError):
            amount_kopecks = 0
        return {
            "Status": status,
            "Amount": amount_kopecks,
            "PaymentMethod": (data.get("payment_method") or {}).get("type"),
            "raw": data,
        }

    def parse_webhook(self, raw_body: bytes, headers: dict) -> WebhookEvent | None:
        """
        Уведомление ЮKassa: {type: "notification", event, object}. Подписи НЕТ →
        из тела берём ТОЛЬКО id, событие строим из ответа GET /payments/{id}.
        """
        try:
            notification = json.loads(raw_body)
        except (ValueError, UnicodeDecodeError):
            return None
        if notification.get("type") != "notification":
            return None
        event_name = notification.get("event", "")
        obj = notification.get("object") or {}

        if event_name == "refund.succeeded":
            # object = Refund → сверяем по родительскому платежу
            payment_id = obj.get("payment_id")
            if not payment_id:
                return None
            info = self.verify_payment(payment_id)
            if not info or info["Status"] not in ("REFUNDED", "PARTIAL_REFUNDED"):
                log.warning("YooKassa webhook refund.succeeded не подтвердился API: %s", payment_id)
                return None
            return WebhookEvent(
                payment_id=payment_id,
                event_type="refund.succeeded",
                amount=info["Amount"] / 100.0,
                provider_name=self.name,
            )

        if event_name not in ("payment.succeeded", "payment.canceled", "payment.waiting_for_capture"):
            log.info("YooKassa webhook: игнорируем событие %s", event_name)
            return None

        payment_id = obj.get("id")
        if not payment_id:
            return None
        info = self.verify_payment(payment_id)
        if not info:
            return None
        data = info["raw"]
        real_status = data.get("status")

        # Событие должно совпадать с реальным статусом (анти-подделка)
        if event_name == "payment.succeeded" and real_status != "succeeded":
            log.warning(
                "YooKassa webhook: событие succeeded, но API говорит %s (id=%s) — игнор",
                real_status, payment_id,
            )
            return None
        if event_name == "payment.canceled" and real_status != "canceled":
            return None

        pm = data.get("payment_method") or {}
        pm_type = pm.get("type")
        card = pm.get("card") or {}
        saved_id = pm.get("id") if pm.get("saved") else None
        is_sbp = pm_type == "sbp"

        metadata = data.get("metadata") or {}
        return WebhookEvent(
            payment_id=payment_id,
            event_type="payment.succeeded" if event_name == "payment.succeeded" else "payment.canceled",
            payment_method=pm_type,
            amount=float((data.get("amount") or {}).get("value", 0) or 0),
            metadata=metadata,
            # Сохранённый метод: карта → rebill_id, СБП → account_token.
            # upsert в service.py по этому признаку выставит method_type.
            rebill_id=saved_id if not is_sbp else None,
            account_token=saved_id if is_sbp else None,
            customer_key=metadata.get("user_id"),
            card_last4=card.get("last4"),
            card_brand=card.get("card_type"),
            provider_name=self.name,
        )

    def refund(self, payment_id: str, amount: float | None = None) -> bool:
        """
        POST /refunds. Если amount не задан — полный возврат (сумма из платежа).
        Чек возврата: фискализация yoo_receipt сама строит чек по данным
        платежа при полном возврате; если API потребует явный receipt
        (частичный возврат) — увидим ошибку в логе и доработаем.
        """
        try:
            if amount is None:
                info = self.verify_payment(payment_id)
                if not info:
                    return False
                amount = info["Amount"] / 100.0
            self._request(
                "POST", "/refunds",
                {
                    "payment_id": payment_id,
                    "amount": {"value": f"{amount:.2f}", "currency": "RUB"},
                },
                idempotence_key=str(uuid.uuid4()),
            )
            return True
        except Exception as e:
            log.error("YooKassa refund(%s) failed: %s", payment_id, e)
            return False
