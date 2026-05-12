"""
Factory — выбирает провайдера по env-переменным.

Приоритет:
  1. T-Bank (если заданы TBANK_TERMINAL_KEY + TBANK_PASSWORD) — основной провайдер
  2. YooKassa (если заданы YOOKASSA_SHOP_ID + YOOKASSA_SECRET_KEY) — legacy/fallback
  3. Stub (для разработки и «инфраструктура готова, ключей нет»)

Singleton через lru_cache — один экземпляр на процесс.
"""
import logging
import os
from functools import lru_cache

from api.billing.provider import PaymentProvider
from api.billing.stub import StubPaymentProvider
from api.billing.tbank import TBankProvider
from api.billing.yookassa import YooKassaProvider

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_payment_provider() -> PaymentProvider:
    """
    Возвращает провайдера. Singleton.
    """
    # 1. T-Bank — приоритетный провайдер (с 2026)
    tbank_terminal = os.getenv("TBANK_TERMINAL_KEY", "").strip()
    tbank_password = os.getenv("TBANK_PASSWORD", "").strip()
    if tbank_terminal and tbank_password:
        log.info(
            "Billing: using T-Bank provider (terminal=%s...)",
            tbank_terminal[:6],
        )
        return TBankProvider(terminal_key=tbank_terminal, password=tbank_password)

    # 2. YooKassa — оставлен как legacy fallback на случай возврата
    shop_id = os.getenv("YOOKASSA_SHOP_ID", "").strip()
    secret = os.getenv("YOOKASSA_SECRET_KEY", "").strip()
    if shop_id and secret:
        log.info("Billing: using YooKassa provider (shop_id=%s...)", shop_id[:4])
        return YooKassaProvider(shop_id=shop_id, secret_key=secret)

    # 3. Stub — dev / pre-onboarding
    log.warning(
        "Billing: using STUB provider (no TBANK_TERMINAL_KEY/PASSWORD or "
        "YOOKASSA_SHOP_ID/SECRET_KEY in env). Payments won't actually charge — "
        "use for infra testing only."
    )
    return StubPaymentProvider()
