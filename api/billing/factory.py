"""
Factory — выбирает провайдера по env-переменным.

Если YOOKASSA_SHOP_ID + YOOKASSA_SECRET_KEY заданы → YooKassa.
Иначе → Stub (для разработки и "инфраструктура готова, ключей нет").

Singleton через lru_cache — один экземпляр на процесс.
"""
import logging
import os
from functools import lru_cache

from api.billing.provider import PaymentProvider
from api.billing.stub import StubPaymentProvider
from api.billing.yookassa import YooKassaProvider

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_payment_provider() -> PaymentProvider:
    """
    Возвращает провайдера. Singleton.
    """
    shop_id = os.getenv("YOOKASSA_SHOP_ID", "").strip()
    secret = os.getenv("YOOKASSA_SECRET_KEY", "").strip()

    if shop_id and secret:
        log.info("Billing: using YooKassa provider (shop_id=%s...)", shop_id[:4])
        return YooKassaProvider(shop_id=shop_id, secret_key=secret)

    log.warning(
        "Billing: using STUB provider (no YOOKASSA_SHOP_ID/SECRET_KEY in env). "
        "Payments won't actually charge — use for infra testing only."
    )
    return StubPaymentProvider()
