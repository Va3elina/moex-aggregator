"""Журнал вебхуков банка (api/billing/webhook_log.py + POST /api/billing/webhook).

Что проверяем:
  - каждый вебхук оставляет строку: PaymentId, Status, был ли RebillId /
    AccountToken, итог обработки;
  - сами RebillId и AccountToken в журнал не попадают;
  - отбитые уведомления (подпись, промежуточный статус, мусор) тоже пишутся;
  - сбой обработки и сбой самой записи не меняют ответ банку и не мешают
    активации подписки.
"""
import os

# api.database создаёт engine при импорте; к этой БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

pytest.importorskip("fastapi")

from api.billing import service, webhook_log
from api.billing.tbank import TBankProvider
from api.routers import billing as router

NOW = datetime.now(timezone.utc).replace(microsecond=0)
REBILL = "9900112233"
ACCOUNT_TOKEN = "sbp-token-very-secret"


@pytest.fixture()
def db(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from api.models.payment_method import UserPaymentMethod
    from api.models.subscription import Subscription
    from api.models.user import User

    engine = create_engine("sqlite:///:memory:")
    for model in (User, UserPaymentMethod, Subscription):
        model.__table__.create(engine)
    monkeypatch.setattr(webhook_log, "_table_ready", False)   # новая БД — новая таблица
    s = sessionmaker(bind=engine)()
    s.add(User(id=1, email="u@example.com", hashed_password="x", role="user", is_verified=True))
    s.commit()
    yield s
    s.close()


class Bank(TBankProvider):
    """Настоящая подпись вебхука, GetState — из словаря."""

    def __init__(self):
        super().__init__("TestTerminal", "test-password")
        self.states = {}

    def verify_payment(self, payment_id):
        st = self.states.get(payment_id)
        return None if st is None else {"Status": st, "Amount": 290000}


@pytest.fixture()
def bank(monkeypatch):
    b = Bank()
    monkeypatch.setattr(router, "get_payment_provider", lambda: b)
    monkeypatch.setattr(service, "get_provider_for", lambda user: b)
    monkeypatch.setattr(service, "get_provider_by_name", lambda name: b)
    monkeypatch.setattr(service, "get_payment_provider", lambda: b)
    monkeypatch.setattr(service, "_log_double_charge", lambda db, sub: None)
    return b


def _sub(db, status="pending", pid="pay-1", started_h=None, pm=None):
    from api.models.subscription import Subscription
    r = Subscription(
        user_id=1, tier="basic", period="monthly", plan_id="basic_monthly", amount=2900,
        status=status, yk_payment_id=pid, payment_method_id=pm,
        created_at=NOW - timedelta(minutes=5),
        started_at=NOW + timedelta(hours=started_h) if started_h is not None else None,
        expires_at=NOW + timedelta(days=30) if status == "active" else None,
    )
    db.add(r)
    db.commit()
    return r


def _body(bank, sign=True, **fields):
    data = {"TerminalKey": bank.terminal_key, "OrderId": "o-1", "Success": True,
            "Status": "CONFIRMED", "PaymentId": "pay-1", "ErrorCode": "0", "Amount": 290000}
    data.update(fields)
    data["Token"] = bank._make_token(data) if sign else "0" * 64
    return json.dumps(data).encode("utf-8")


class _Request:
    def __init__(self, raw):
        self._raw = raw
        self.headers = {"content-type": "application/json"}

    async def body(self):
        return self._raw


def _post(db, raw):
    resp = asyncio.run(router.webhook(_Request(raw), db=db))
    assert resp.status_code == 200 and resp.body == b"OK"    # банку всегда OK


def _rows(db):
    return db.execute(text(
        "SELECT payment_id, status, error_code, amount_kopecks, has_rebill_id, "
        "has_account_token, subscription_id, result, detail, received_at "
        "FROM bank_webhook_log ORDER BY id"
    )).mappings().all()


# ── Успешные сценарии ────────────────────────────────────────────────────────

def test_confirmed_webhook_is_logged_with_binding_flag(db, bank):
    sub = _sub(db)
    bank.states["pay-1"] = "CONFIRMED"
    _post(db, _body(bank, RebillId=REBILL, CustomerKey="1", Pan="430000******0333", CardType="VISA"))

    db.refresh(sub)
    assert sub.status == "active" and sub.payment_method_id is not None
    (row,) = _rows(db)
    assert row["payment_id"] == "pay-1" and row["status"] == "CONFIRMED"
    assert row["error_code"] == "0" and row["amount_kopecks"] == 290000
    assert bool(row["has_rebill_id"]) is True and bool(row["has_account_token"]) is False
    assert row["subscription_id"] == sub.id and row["result"] == "activated"
    assert row["received_at"] is not None


def test_keys_are_never_stored(db, bank):
    _sub(db)
    bank.states["pay-1"] = "CONFIRMED"
    _post(db, _body(bank, RebillId=REBILL, AccountToken=ACCOUNT_TOKEN, CustomerKey="1"))

    (row,) = _rows(db)
    assert bool(row["has_rebill_id"]) and bool(row["has_account_token"])
    dump = json.dumps({k: str(v) for k, v in row.items()})
    assert REBILL not in dump and ACCOUNT_TOKEN not in dump
    cols = {c.name for c in webhook_log.bank_webhook_log.columns}
    assert not cols & {"rebill_id", "account_token", "token", "pan", "raw_body", "body"}


def test_repeat_webhook_is_duplicate(db, bank):
    _sub(db)
    bank.states["pay-1"] = "CONFIRMED"
    raw = _body(bank, RebillId=REBILL, CustomerKey="1")
    _post(db, raw)
    _post(db, raw)
    assert [r["result"] for r in _rows(db)] == ["activated", "duplicate"]


def test_late_webhook_after_sync_is_binding_saved(db, bank):
    # /sync уже включил подписку без карты; вебхук с RebillId её досохраняет.
    sub = _sub(db, status="active", started_h=-0.1)
    _post(db, _body(bank, RebillId=REBILL, CustomerKey="1"))
    db.refresh(sub)
    assert sub.payment_method_id is not None
    assert _rows(db)[0]["result"] == "binding_saved"


def test_confirmed_without_binding_shows_missing_rebill(db, bank):
    # Тот самый случай: оплата прошла, RebillId банк не прислал.
    sub = _sub(db)
    bank.states["pay-1"] = "CONFIRMED"
    _post(db, _body(bank))
    db.refresh(sub)
    (row,) = _rows(db)
    assert sub.status == "active" and sub.payment_method_id is None
    assert row["result"] == "activated" and not row["has_rebill_id"] and not row["has_account_token"]


def test_cancel_and_refund_and_unknown_payment(db, bank):
    _sub(db, status="active", started_h=-1, pid="pay-1")
    _post(db, _body(bank, Status="REFUNDED"))
    _post(db, _body(bank, Status="REFUNDED"))            # повтор — ничего не меняет
    _post(db, _body(bank, Status="REJECTED", PaymentId="nobody"))
    assert [r["result"] for r in _rows(db)] == ["refunded", "noop", "no_subscription"]


def test_not_activated_when_bank_does_not_confirm(db, bank):
    sub = _sub(db)
    bank.states["pay-1"] = "REJECTED"                    # GetState не подтвердил
    _post(db, _body(bank, RebillId=REBILL))
    db.refresh(sub)
    assert sub.status == "pending"
    assert _rows(db)[0]["result"] == "not_activated"


# ── Отбитые уведомления ──────────────────────────────────────────────────────

def test_rejected_skipped_and_garbage_are_logged(db, bank):
    sub = _sub(db)
    _post(db, _body(bank, sign=False, RebillId=REBILL))  # чужая подпись
    _post(db, _body(bank, Status="AUTHORIZED"))          # промежуточный статус
    _post(db, b"not json at all")
    _post(db, b'["list", "not", "object"]')
    _post(db, b"x" * (webhook_log.MAX_BODY_BYTES + 1))

    db.refresh(sub)
    assert sub.status == "pending"                       # ничего не активировалось
    rows = _rows(db)
    assert [r["result"] for r in rows] == [
        "rejected", "skipped_status", "bad_body", "bad_body", "bad_body"]
    assert rows[0]["payment_id"] == "pay-1" and bool(rows[0]["has_rebill_id"])
    assert rows[1]["status"] == "AUTHORIZED"
    assert rows[2]["payment_id"] is None


def test_oversized_fields_are_clipped(db, bank):
    _post(db, _body(bank, sign=False, PaymentId="9" * 500, Status="S" * 500,
                    ErrorCode={"nested": 1}, Amount="abc"))
    (row,) = _rows(db)
    assert len(row["payment_id"]) == 64 and len(row["status"]) == 32
    assert row["error_code"] is None and row["amount_kopecks"] is None


# ── Журнал не мешает обработке ───────────────────────────────────────────────

def test_processing_error_is_logged_without_exception_text(db, bank, monkeypatch):
    _sub(db)

    def boom(db_, event):
        raise RuntimeError(f"db failure with params rebill_id={event.rebill_id}")

    monkeypatch.setattr(service, "activate_from_webhook", boom)
    _post(db, _body(bank, RebillId=REBILL))              # ответ банку всё равно OK
    (row,) = _rows(db)
    assert row["result"] == "error" and row["detail"] == "RuntimeError"
    assert REBILL not in json.dumps({k: str(v) for k, v in row.items()})


def test_log_failure_does_not_break_activation(db, bank, monkeypatch):
    sub = _sub(db)
    bank.states["pay-1"] = "CONFIRMED"

    def broken(db_):
        raise RuntimeError("no rights to create table")

    monkeypatch.setattr(webhook_log, "_ensure_table", broken)
    _post(db, _body(bank, RebillId=REBILL, CustomerKey="1"))

    db.refresh(sub)
    assert sub.status == "active" and sub.payment_method_id is not None
    # и сессия после сбоя журнала рабочая
    assert db.execute(text("SELECT count(*) FROM subscriptions")).scalar() == 1


def test_record_never_raises_on_broken_session(db):
    class Broken:
        def rollback(self):
            raise RuntimeError("connection lost")

    webhook_log.record(Broken(), b"{}", "activated")     # не бросает
