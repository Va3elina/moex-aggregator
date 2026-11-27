"""Алерт «оплата без карты» (api/billing/admin_notify.py: flag_paid_without_method).

Что проверяем:
  - оплаченная на форме подписка без способа оплаты через 15 минут даёт одно
    событие paid_no_method, повторные проходы его не дублируют;
  - триал, инвайт, нулевая сумма, продление с картой, свежая оплата, старая
    история, выключенное автопродление и СБП с незакрытой привязкой — мимо;
  - сообщение админам: кто, что, когда, что присылал банк; если привязка
    успела доехать — сообщения нет.
"""
import os

# api.database создаёт engine при импорте; к этой БД тест не ходит.
os.environ.setdefault("DB_URL", "sqlite:///unused-test.db")

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from api.billing import admin_notify, webhook_log

NOW = datetime.now(timezone.utc).replace(microsecond=0)
GRACE = admin_notify.NO_METHOD_GRACE_MINUTES
KIND = admin_notify.KIND_PAID_NO_METHOD

# Как в миграциях 106 + 122, в диалекте sqlite.
_EVENTS_DDL = """
CREATE TABLE billing_events (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    kind              TEXT NOT NULL,
    user_id           INTEGER,
    subscription_id   INTEGER,
    payment_method_id INTEGER,
    old_status        TEXT,
    new_status        TEXT,
    amount            NUMERIC(10, 2),
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notified_at       TIMESTAMP,
    skipped           BOOLEAN NOT NULL DEFAULT 0
)
"""
_EVENTS_UQ = """
CREATE UNIQUE INDEX billing_events_paid_no_method_uq
    ON billing_events (subscription_id) WHERE kind = 'paid_no_method'
"""


@pytest.fixture()
def db():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from api.models.payment_method import UserPaymentMethod
    from api.models.subscription import Subscription
    from api.models.user import User

    engine = create_engine("sqlite:///:memory:")
    for model in (User, UserPaymentMethod, Subscription):
        model.__table__.create(engine)
    s = sessionmaker(bind=engine)()
    s.execute(text(_EVENTS_DDL))
    s.execute(text(_EVENTS_UQ))
    s.add(User(id=1, email="u@example.com", hashed_password="x", role="basic", is_verified=True))
    s.add(UserPaymentMethod(id=10, user_id=1, provider="tbank", method_type="card", rebill_id="r1"))
    s.commit()
    yield s
    s.close()


def _sub(db, *, paid_min_ago=GRACE + 5, status="active", pid="pay-1", pm=None, amount=2900,
         is_trial=False, cancelled=False, sbp_key=None, method="bank_card"):
    from api.models.subscription import Subscription
    started = NOW - timedelta(minutes=paid_min_ago)
    r = Subscription(
        user_id=1, tier="basic", period="monthly", plan_id="basic_monthly", amount=amount,
        status=status, yk_payment_id=pid, payment_method_id=pm, is_trial=is_trial,
        yk_method=method, sbp_request_key=sbp_key,
        created_at=started - timedelta(minutes=2), started_at=started,
        expires_at=started + timedelta(days=30),
        cancelled_at=NOW if cancelled else None,
    )
    db.add(r)
    db.commit()
    return r


def _events(db):
    return db.execute(text(
        "SELECT kind, user_id, subscription_id, new_status, amount, notified_at "
        "FROM billing_events ORDER BY id"
    )).mappings().all()


# ── Кого ловим ───────────────────────────────────────────────────────────────

def test_paid_sub_without_method_is_flagged_once(db):
    sub = _sub(db)
    assert admin_notify.flag_paid_without_method(db, now=NOW) == {"checked": 1, "flagged": 1}
    (ev,) = _events(db)
    assert ev["kind"] == KIND and ev["subscription_id"] == sub.id and ev["user_id"] == 1
    assert float(ev["amount"]) == 2900 and ev["notified_at"] is None      # уйдёт обычной доставкой

    # Следующие проходы оркестратора (каждые 15 минут) — без повторов.
    for step in (15, 30, 600):
        out = admin_notify.flag_paid_without_method(db, now=NOW + timedelta(minutes=step))
        assert out["flagged"] == 0
    assert len(_events(db)) == 1


def test_already_delivered_event_is_not_repeated(db):
    _sub(db)
    admin_notify.flag_paid_without_method(db, now=NOW)
    db.execute(text("UPDATE billing_events SET notified_at = CURRENT_TIMESTAMP"))
    db.commit()
    assert admin_notify.flag_paid_without_method(db, now=NOW)["flagged"] == 0
    assert len(_events(db)) == 1


def test_waits_for_grace_period(db):
    _sub(db, paid_min_ago=GRACE - 1)
    assert admin_notify.flag_paid_without_method(db, now=NOW)["flagged"] == 0
    assert admin_notify.flag_paid_without_method(db, now=NOW + timedelta(minutes=2))["flagged"] == 1


def test_each_subscription_gets_its_own_event(db):
    a = _sub(db, pid="pay-a")
    b = _sub(db, pid="pay-b")
    assert admin_notify.flag_paid_without_method(db, now=NOW)["flagged"] == 2
    assert sorted(e["subscription_id"] for e in _events(db)) == sorted([a.id, b.id])


# ── Кого не трогаем ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("kw", [
    {"pm": 10},                                   # карта привязана / продление крона
    {"is_trial": True, "amount": 0, "pid": None}, # триал
    {"pid": None},                                # инвайт: платежа нет
    {"amount": 0},                                # бесплатно
    {"status": "pending"},                        # ещё не оплачена
    {"status": "refunded"},                       # деньги вернули
    {"cancelled": True},                          # автопродление выключено
    {"sbp_key": "rk-1", "method": "sbp"},         # СБП-привязку ещё дотягивает реконсайлер
    {"paid_min_ago": admin_notify.NO_METHOD_LOOKBACK_HOURS * 60 + 5},   # старая история
])
def test_not_flagged(db, kw):
    _sub(db, **kw)
    assert admin_notify.flag_paid_without_method(db, now=NOW)["flagged"] == 0
    assert _events(db) == []


def test_sbp_is_flagged_after_reconciler_gave_up(db):
    _sub(db, method="sbp", sbp_key=None, paid_min_ago=40)
    assert admin_notify.flag_paid_without_method(db, now=NOW)["flagged"] == 1


# ── Сообщение в FRAME · Касса ────────────────────────────────────────────────

def _render(db, sub):
    ev = {"kind": KIND, "user_id": 1, "subscription_id": sub.id, "payment_method_id": None,
          "old_status": None, "new_status": "active", "amount": sub.amount, "created_at": NOW}
    return admin_notify._render_event(db, ev)


def test_message_names_client_payment_and_bank_trail(db):
    sub = _sub(db)
    webhook_log.bank_webhook_log.create(db.connection())
    db.execute(webhook_log.bank_webhook_log.insert().values(
        payment_id="pay-1", status="CONFIRMED", has_rebill_id=False,
        has_account_token=False, result="activated"))
    db.commit()

    msg = _render(db, sub)
    assert "способ оплаты не привязан" in msg
    assert "u@example.com" in msg and f"sub #{sub.id}" in msg and "pay-1" in msg
    assert "2 900 ₽" in msg
    assert "CONFIRMED (без привязки, activated)" in msg
    assert "автопродления нет" in msg and f"{GRACE} минут" in msg


def test_message_without_webhooks_and_without_log_table(db):
    sub = _sub(db)
    msg = _render(db, sub)                                  # таблицы журнала нет вовсе
    assert "способ оплаты не привязан" in msg and "Вебхуки банка" not in msg
    assert db.execute(text("SELECT count(*) FROM subscriptions")).scalar() == 1   # сессия жива

    webhook_log.bank_webhook_log.create(db.connection())
    db.commit()
    assert "по этому платежу не приходили" in _render(db, sub)


def test_no_message_if_binding_arrived_or_refunded(db):
    sub = _sub(db)
    assert _render(db, sub) is not None
    sub.payment_method_id = 10                              # опоздавший вебхук досохранил карту
    db.commit()
    assert _render(db, sub) is None

    other = _sub(db, pid="pay-2")
    other.status = "refunded"
    db.commit()
    assert _render(db, other) is None
