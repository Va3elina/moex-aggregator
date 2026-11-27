"""
Журнал входящих вебхуков банка (T-Bank, POST /api/billing/webhook).

Зачем: логи API теряются при каждом деплое, а сырых уведомлений банка у нас не
было нигде. Когда оплата прошла, а карта не сохранилась (sub 342, 20.11.2026),
восстановить, что и когда присылал банк, было не по чему.

Что пишем на каждый вебхук: время, PaymentId, Status, ErrorCode, сумму, БЫЛ ЛИ
в теле RebillId / AccountToken и чем кончилась обработка. Сами RebillId и
AccountToken НЕ храним — это ключи к списанию денег, в журнале им делать
нечего; подпись (Token), маска карты и прочее тело тоже не сохраняются.

Запись не должна ронять обработку: record() зовётся ПОСЛЕ неё и глотает любые
ошибки. Таблица создаётся на лету (как renewal_reminders), миграция
121_bank_webhook_log.sql — та же DDL для истории схемы.
"""
import json
import logging

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    func,
)
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

# Больше этого тело не разбираем: настоящие уведомления банка — сотни байт,
# а ручка публичная.
MAX_BODY_BYTES = 64 * 1024

_meta = MetaData()

bank_webhook_log = Table(
    "bank_webhook_log", _meta,
    # sqlite (тесты) делает автоинкремент только для INTEGER PRIMARY KEY.
    Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
    Column("received_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("payment_id", String(64)),
    Column("status", String(32)),
    Column("error_code", String(16)),
    Column("amount_kopecks", BigInteger),
    Column("has_rebill_id", Boolean, nullable=False, default=False),
    Column("has_account_token", Boolean, nullable=False, default=False),
    Column("subscription_id", Integer),
    Column("result", String(32), nullable=False),
    Column("detail", String(200)),
    Index("bank_webhook_log_payment_idx", "payment_id", "received_at"),
    Index("bank_webhook_log_time_idx", "received_at"),
)

_table_ready = False


def _ensure_table(db: Session) -> None:
    global _table_ready
    if _table_ready:
        return
    bank_webhook_log.create(db.connection(), checkfirst=True)
    db.commit()
    _table_ready = True


def _clip(value, size: int) -> str | None:
    if value is None or isinstance(value, (dict, list)):
        return None
    return str(value)[:size] or None


def safe_fields(raw_body: bytes) -> dict | None:
    """
    Поля уведомления, которые можно хранить. None — тело не JSON-объект.

    Тело ещё не проверено подписью (пишем и отбитые уведомления — по ним видно,
    например, что сменился пароль терминала), поэтому всё режем по длине.
    """
    if len(raw_body) > MAX_BODY_BYTES:
        return None
    try:
        data = json.loads(raw_body.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    try:
        amount = int(data.get("Amount")) if data.get("Amount") is not None else None
    except (ValueError, TypeError):
        amount = None
    if amount is not None and not (-2**62 < amount < 2**62):
        amount = None
    return {
        "payment_id": _clip(data.get("PaymentId"), 64),
        "status": _clip(data.get("Status"), 32),
        "error_code": _clip(data.get("ErrorCode"), 16),
        "amount_kopecks": amount,
        "has_rebill_id": bool(data.get("RebillId")),
        "has_account_token": bool(data.get("AccountToken")),
    }


def record(
    db: Session, raw_body: bytes, result: str,
    detail: str | None = None, subscription_id: int | None = None,
) -> None:
    """
    Одна строка журнала. Зовётся после обработки вебхука, никогда не бросает.

    result — чем кончилась обработка (значения см. в роутере, _webhook_outcome):
    bad_body / rejected / skipped_status / no_subscription / activated /
    binding_saved / duplicate / not_activated / trial_bound / cancelled /
    refunded / noop / error.
    """
    try:
        # Обработка могла оставить сессию в сломанной транзакции (исключение в
        # сервисе) — всё, что должно было сохраниться, сервис уже закоммитил.
        db.rollback()
        _ensure_table(db)
        fields = safe_fields(raw_body) or {}
        db.execute(bank_webhook_log.insert().values(
            **fields,
            subscription_id=subscription_id,
            result=result[:32],
            detail=(detail or None) and detail[:200],
        ))
        db.commit()
    except Exception as e:
        log.warning("bank webhook log: запись не удалась (%s): %s", result, e)
        try:
            db.rollback()
        except Exception:
            pass
