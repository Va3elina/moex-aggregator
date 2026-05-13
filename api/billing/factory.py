"""
Factory — выбирает провайдера по env-переменным.

Приоритет:
  1. T-Bank (если заданы TBANK_TERMINAL_KEY + TBANK_PASSWORD) — единственный
     рабочий провайдер.
  2. Stub (для разработки / «инфраструктура готова, ключей нет»).

YooKassa удалена 2026-05-13 (commit с этой правкой). Все production-платежи
идут через T-Bank. YK provider не использовался уже несколько недель,
существование двух code paths мешало развитию (требовало синхронизировать
любое изменение в logic, типа sync_pending). См. git history если нужно
вернуть.

Singleton через lru_cache — один экземпляр на процесс.
"""
import logging
import os
from functools import lru_cache

from api.billing.provider import PaymentProvider
from api.billing.stub import StubPaymentProvider
from api.billing.tbank import TBankProvider

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_payment_provider() -> PaymentProvider:
    """
    Возвращает провайдера. Singleton.
    """
    # 1. T-Bank — единственный production-провайдер
    tbank_terminal = os.getenv("TBANK_TERMINAL_KEY", "").strip()
    tbank_password = os.getenv("TBANK_PASSWORD", "").strip()
    if tbank_terminal and tbank_password:
        log.info(
            "Billing: using T-Bank provider (terminal=%s...)",
            tbank_terminal[:6],
        )
        return TBankProvider(terminal_key=tbank_terminal, password=tbank_password)

    # 2. Stub — dev / pre-onboarding
    log.warning(
        "Billing: using STUB provider (no TBANK_TERMINAL_KEY/PASSWORD in env). "
        "Payments won't actually charge — use for infra testing only."
    )
    return StubPaymentProvider()
