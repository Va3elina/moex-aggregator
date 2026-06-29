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
    """Результат создания платёжной сессии.

    Карта: пользователя редиректим на confirmation_url.
    СБП-QR: confirmation_url пуст, вместо него qr_image (картинка для скана) и
    qr_payload (СБП-ссылка qr.nspk.ru/... для мобильного диплинка «открыть банк»).
    """
    payment_id: str               # id платежа у провайдера (→ subscriptions.yk_payment_id)
    confirmation_url: str = ""     # URL оплаты (карта). Пусто для СБП.
    qr_image: str | None = None    # data-URL картинки QR (СБП, GetQr DataType=IMAGE)
    qr_payload: str | None = None  # СБП-ссылка для мобильного диплинка (GetQr DataType=PAYLOAD)
    request_key: str | None = None # СБП RequestKey (GetQr) — для дотягивания AccountToken


@dataclass
class WebhookEvent:
    """Распарсенное событие от провайдера."""
    payment_id: str                      # id платежа (для лукапа подписки в БД)
    event_type: str                      # 'payment.succeeded' / 'payment.canceled' / 'refund.succeeded'
    payment_method: str | None = None    # 'bank_card' / 'sbp' / 'tpay' / ...
    amount: float | None = None
    metadata: dict | None = None         # произвольные данные, которые мы прокинули при создании
    # === Поля для рекуррентных платежей (T-Bank Recurrent flow) ===
    # Приходят в webhook'е CONFIRMED платежа созданного с Recurrent="Y".
    # service.py использует их для создания записи в user_payment_methods.
    rebill_id: str | None = None         # T-Bank токен карты для будущих Charge
    customer_key: str | None = None      # CustomerKey которым была привязана
    card_last4: str | None = None        # последние 4 цифры PAN для UI
    card_brand: str | None = None        # 'VISA' / 'MASTERCARD' / 'MIR' / ...
    # === Поля для рекуррентного СБП (T-Bank QR-flow) ===
    # account_token приходит в нотификации при успешной привязке счёта по QR
    # (СБП-аналог rebill_id). request_key — id запроса GetQr/AddAccountQR.
    account_token: str | None = None     # СБП-токен привязки для будущих ChargeQr
    request_key: str | None = None       # RequestKey QR-привязки
    bank_member_id: str | None = None    # id банка плательщика (нужен в ChargeQr)
    # card_fingerprint — sha256 маскированного PAN (first6+last4). Стабилен между
    # разными CustomerKey/rebill_id (та же карта → тот же отпечаток), поэтому
    # годится как кросс-аккаунтный анти-абуз ключ для триала. 152-ФЗ: это ХЕШ.
    card_fingerprint: str | None = None


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
        recurrent: bool = False,
        customer_key: str | None = None,
    ) -> CheckoutSession:
        """
        Создать платёжную сессию у провайдера.

        customer_email / customer_phone — для отправки кассового чека по 54-ФЗ
        (T-Bank пробивает чек в ОФД и отправляет по email/SMS). Опциональны для
        провайдеров которые не делают фискализацию (Stub, YooKassa с внешней
        кассой). Хотя бы один из двух желателен, иначе T-Bank Receipt не построит.

        widget_mode — если True, payment инициирован через T-Bank JS SDK
        (SpeedPay кнопки). Провайдер добавит в DATA маркер connection_type=Widget.

        recurrent — если True, после успешной оплаты карта будет сохранена для
        последующих списаний (Recurrent="Y" в T-Bank Init). В CONFIRMED webhook'е
        провайдер вернёт RebillId который сохраним в user_payment_methods.

        customer_key — обязательный когда recurrent=True. Уникальный идентификатор
        клиента у провайдера. У нас обычно str(user.id).

        Возвращает payment_id (для сохранения в нашу БД) и confirmation_url.
        """
        ...

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
        Создать платёж по СБП-QR (Вариант №2 T-Bank: привязка во время первой
        оплаты). Init с Recurrent="Y" + DATA={"QR":"true"} → GetQr.

        В отличие от create_checkout НЕ возвращает redirect-URL: возвращает
        CheckoutSession с qr_image (картинка QR для скана) и qr_payload (СБП-
        ссылка для мобильного диплинка). confirmation_url пуст.

        После оплаты T-Bank присылает в webhook (на NotificationURL терминала)
        AccountToken — сохраняем его в user_payment_methods (method_type='sbp')
        для последующих ChargeQr.

        recurrent — должен быть True (иначе нет смысла в привязке); customer_key
        обязателен. ВЫЗЫВАЕТ ИСКЛЮЧЕНИЕ если провайдер не поддерживает СБП.
        """
        ...

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
        Рекуррентное списание по СБП-привязке (ChargeQr) — СБП-аналог charge().
        Init с Recurrent="Y" + DATA={"QR":"true"} → ChargeQr с AccountToken +
        BankMemberId (обязателен, иначе ErrorCode 5031).

        Возвращает тот же dict-контракт, что charge(): минимум {payment_id, status,
        success}; при отказе — message/error_code/failure_kind. status='PENDING'
        (+ pending=True) — async-списание ещё в обработке (дожмётся webhook'ом).
        """
        ...

    def get_account_qr_state(self, request_key: str) -> dict | None:
        """
        Статус СБП-привязки счёта по RequestKey (GetAddAccountQrState).
        Возвращает {status, account_token, bank_member_id} или None при ошибке.
        status: 'PROCESSING' / 'ACTIVE' / 'INACTIVE' / 'REJECTED'.
        account_token + bank_member_id заполнены при ACTIVE.
        """
        ...

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
        Списать средства с привязанной карты без участия юзера (auto-renewal).

        Flow для T-Bank:
        1. Делается Init с обычными параметрами + ВНУТРИ провайдера сохраняется
           PaymentId
        2. Делается /v2/Charge с PaymentId + RebillId — провайдер списывает
           без 3DS / без формы

        Возвращает dict с минимум {payment_id, status, success}. Конкретный
        формат — провайдер-специфичный.

        ВЫЗЫВАЕТ ИСКЛЮЧЕНИЕ если провайдер не поддерживает рекурренты
        (Stub, YooKassa без specific setup'а).
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
