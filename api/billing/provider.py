"""
Абстрактный протокол платёжного провайдера.

Любой адаптер (YooKassa, Stub, в будущем CloudPayments/Tinkoff) должен
реализовывать этот интерфейс. Бизнес-логика в service.py работает только
через него — провайдер можно заменить одной строкой в factory.py.
"""
from dataclasses import dataclass
from typing import Protocol


@dataclass
class CheckoutSession:
    """Результат создания платёжной сессии — пользователя редиректим на URL."""
    payment_id: str        # id платежа у провайдера (сохраняем в subscriptions.yk_payment_id)
    confirmation_url: str  # URL, на который отправить пользователя для оплаты


@dataclass
class WebhookEvent:
    """Распарсенное событие от провайдера."""
    payment_id: str                      # id платежа (для лукапа подписки в БД)
    event_type: str                      # 'payment.succeeded' / 'payment.canceled' / 'refund.succeeded'
    payment_method: str | None = None    # 'bank_card' / 'sbp' / ...
    amount: float | None = None
    metadata: dict | None = None         # произвольные данные, которые мы прокинули при создании


class PaymentProvider(Protocol):
    """Контракт платёжного провайдера."""

    name: str

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
        Создать платёжную сессию у провайдера.

        customer_email / customer_phone — для отправки кассового чека по 54-ФЗ
        (T-Bank пробивает чек в ОФД и отправляет по email/SMS). Опциональны для
        провайдеров которые не делают фискализацию (Stub, YooKassa с внешней
        кассой). Хотя бы один из двух желателен, иначе T-Bank Receipt не построит.

        widget_mode — если True, payment инициирован через T-Bank JS SDK
        (SpeedPay кнопки). Провайдер добавит в DATA маркер connection_type=Widget
        чтобы T-Bank корректно обработал. Frontend получает только PaymentURL,
        дальше SDK сам редиректит/открывает QR. Для не-T-Bank провайдеров параметр
        игнорируется.

        Возвращает payment_id (для сохранения в нашу БД) и confirmation_url (для редиректа).
        """
        ...

    def parse_webhook(self, raw_body: bytes, headers: dict) -> WebhookEvent | None:
        """
        Распарсить webhook-уведомление от провайдера.
        Проверяет подпись (если провайдер её присылает).
        Возвращает None если это не наше событие / подпись не прошла.
        """
        ...

    def refund(self, payment_id: str, amount: float | None = None) -> bool:
        """
        Инициировать возврат. Если amount не указан — полный возврат.
        Возвращает True если запрос принят провайдером (не факт что возврат уже случился).
        """
        ...
