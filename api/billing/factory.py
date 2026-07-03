"""
Factory — выбирает провайдера по env-переменным.

Дефолтный провайдер (get_payment_provider):
  1. T-Bank (если заданы TBANK_TERMINAL_KEY + TBANK_PASSWORD) — production.
  2. Stub (для разработки / «инфраструктура готова, ключей нет»).

ЮKassa (2026-07-03, второй заход — миграция с T-Bank): подключается НЕ
глобально, а пер-юзерным роутингом на время теста:
  • env YOOKASSA_SHOP_ID + YOOKASSA_SECRET_KEY → провайдер существует;
  • env YOOKASSA_TEST_USER_IDS="2" (csv) → ЭТИ юзеры чекаутятся через
    ЮKassa (get_provider_for_user), остальные — через дефолт (T-Bank);
  • продления идут по pm.provider (get_provider_by_name) — у кого карта
    привязана в T-Bank, тот продлевается в T-Bank, независимо от роутинга.
Без env-переменных всё поведение байт-в-байт прежнее.

Singleton'ы через lru_cache — по экземпляру на процесс.
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
    Дефолтный провайдер. Singleton.
    """
    # 1. T-Bank — основной production-провайдер
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


@lru_cache(maxsize=1)
def get_yookassa_provider() -> PaymentProvider | None:
    """ЮKassa-провайдер, если env заданы. None → фича спит."""
    shop_id = os.getenv("YOOKASSA_SHOP_ID", "").strip()
    secret_key = os.getenv("YOOKASSA_SECRET_KEY", "").strip()
    if shop_id and secret_key:
        log.info("Billing: YooKassa provider available (shop=%s)", shop_id)
        return YooKassaProvider(shop_id=shop_id, secret_key=secret_key)
    return None


@lru_cache(maxsize=1)
def yookassa_test_user_ids() -> frozenset[int]:
    """user_id'ы, чекаутящиеся через ЮKassa (env YOOKASSA_TEST_USER_IDS='2,37')."""
    raw = os.getenv("YOOKASSA_TEST_USER_IDS", "").strip()
    ids: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return frozenset(ids)


def get_provider_for_user(user_id: int | None) -> PaymentProvider:
    """
    Провайдер для НОВЫХ платежей юзера: ЮKassa для тест-юзеров (env),
    иначе дефолт. Роутинг только на checkout/sync — продления идут
    по pm.provider через get_provider_by_name.
    """
    yk = get_yookassa_provider()
    if yk is not None and user_id is not None and user_id in yookassa_test_user_ids():
        return yk
    return get_payment_provider()


def get_provider_by_name(name: str | None) -> PaymentProvider:
    """
    Провайдер по имени из БД (pm.provider / event.provider_name).
    Для продлений и возвратов: платёж обслуживает ТОТ провайдер, через
    которого прошла привязка — независимо от текущего роутинга юзеров.
    """
    if name == "yookassa":
        yk = get_yookassa_provider()
        if yk is None:
            raise RuntimeError(
                "payment_method привязан через yookassa, но YOOKASSA_SHOP_ID/"
                "YOOKASSA_SECRET_KEY не заданы в env"
            )
        return yk
    return get_payment_provider()
