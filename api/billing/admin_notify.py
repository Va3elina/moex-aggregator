"""
Уведомления о биллинге в @frameadminbot.

События пишет триггер БД в billing_events (db/migrations/106_billing_events.sql),
здесь их рендер и доставка:

  dispatch_pending  — забрать неотправленные события и отправить в чат.
                      Зовут: NOTIFY-листенер API (сразу после коммита) и
                      оркестратор каждые 15 мин (страховка, если листенер лежал).
  send_daily_digest — утренняя сводка: деньги за вчера и месяц, активные
                      подписки, MRR, ожидаемые продления. Раз в сутки из оркестратора.

Строки забираются FOR UPDATE SKIP LOCKED: три воркера API и оркестратор могут
звать одновременно, одно событие уйдёт один раз. Если Телеграм не ответил,
notified_at не ставится и событие уйдёт следующим проходом.
"""
import html
import logging
import os
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.billing.plans import get_plan
from api.models.payment_method import UserPaymentMethod
from api.models.subscription import Subscription
from api.models.user import User

log = logging.getLogger(__name__)

MSK = ZoneInfo("Europe/Moscow")

# Старше этого событие не шлём (помечаем skipped): после долгого простоя
# доставки чат не должен получить пачку вчерашних новостей.
MAX_EVENT_AGE_HOURS = 48

# Продление ретраится кроном каждый час до суток: о провале сообщаем один раз,
# дальше молчим до истечения подписки (придёт «закончилась, не продлена»).
RENEW_FAIL_QUIET_HOURS = 20

DIGEST_HOUR_MSK = 9

TIER_NAMES = {"basic": "Basic", "pro": "Pro", "premium": "Premium"}
PERIOD_NAMES = {"monthly": "месяц", "yearly": "год", "trial": "пробный период"}


# ═══════════════════════════════════════════════════════════════════════════════
#  Телеграм
# ═══════════════════════════════════════════════════════════════════════════════

def _configured() -> bool:
    return bool(os.environ.get("BOT_TOKEN") and os.environ.get("ADMIN_CHAT_ID"))


def _send(text_msg: str) -> bool:
    """Та же связка, что у mandate_scan._notify_admin: BOT_TOKEN/ADMIN_CHAT_ID +
    TELEGRAM_API_ROOT (прямой api.telegram.org с прода закрыт)."""
    token = os.environ.get("BOT_TOKEN", "")
    chat_id = os.environ.get("ADMIN_CHAT_ID", "")
    if not token or not chat_id:
        return False
    api_root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
    try:
        resp = requests.post(
            f"{api_root}/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text_msg[:4000],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        log.warning("billing admin notify: send failed: %s", e)
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  Форматирование
# ═══════════════════════════════════════════════════════════════════════════════

def _rub(v) -> str:
    if v is None:
        return "—"
    v = float(v)
    s = f"{v:,.2f}".replace(",", " ").replace(".00", "")
    return f"{s} ₽"


def _d(dt: datetime | None) -> str:
    return dt.astimezone(MSK).strftime("%d.%m.%Y") if dt else "—"


def _is_test_user(user_id: int | None) -> bool:
    ids = {c.strip() for c in (os.getenv("BILLING_TEST_USER_IDS") or "").split(",") if c.strip()}
    return user_id is not None and str(user_id) in ids


def _who(user: User | None, user_id: int | None) -> str:
    if user is None:
        return f"user #{user_id}"
    parts = [html.escape(user.email or "")]
    if user.telegram_username:
        parts.append("@" + html.escape(user.telegram_username))
    parts.append(f"#{user.id}")
    line = " · ".join(p for p in parts if p)
    if _is_test_user(user.id):
        line += " · 🧪 тест"
    return line


def _plan_name(sub: Subscription) -> str:
    plan = get_plan(sub.plan_id)
    tier = TIER_NAMES.get(sub.tier, sub.tier)
    if sub.yk_payment_id is None and not sub.is_trial:
        return f"{tier} по инвайту"
    if plan and not sub.is_trial:
        return plan.title
    return f"{tier} — {PERIOD_NAMES.get(sub.period, sub.period)}"


def _method_line(pm: UserPaymentMethod | None, sub: Subscription | None = None) -> str:
    if pm is None:
        if sub is not None and sub.yk_method == "sbp":
            return "СБП, разовая оплата (счёт не привязан)"
        return "разовая оплата, карта не привязана"
    if pm.method_type == "sbp":
        name, bound, unbound = "счёт СБП", "привязан", "отвязан"
    else:
        name = "карта"
        if pm.card_brand:
            name += " " + html.escape(pm.card_brand.upper())
        if pm.card_last4:
            name += f" •{pm.card_last4}"
        bound, unbound = "привязана", "отвязана"
    if pm.deleted_at is not None:
        return f"{name} ({unbound})"
    return f"{name}, {bound}"


def _paid_history(db: Session, user_id: int, before_id: int | None = None) -> list[Subscription]:
    """Оплаченные подписки юзера (реальные деньги, без триалов и инвайтов)."""
    q = db.query(Subscription).filter(
        Subscription.user_id == user_id,
        Subscription.status.in_(("active", "expired", "cancelled", "refunded")),
        Subscription.is_trial.is_(False),
        Subscription.yk_payment_id.isnot(None),
        Subscription.amount > 0,
    )
    if before_id is not None:
        q = q.filter(Subscription.id < before_id)
    return q.order_by(Subscription.id).all()


def _ltv_line(db: Session, user_id: int) -> str:
    subs = [s for s in _paid_history(db, user_id) if s.status != "refunded"]
    if not subs:
        return "Оплат пока нет"
    total = sum(float(s.amount) for s in subs)
    first = min((s.started_at or s.created_at) for s in subs)
    return f"Всего оплат: {len(subs)} на {_rub(total)}, клиент с {_d(first)}"


def _other_active(db: Session, sub: Subscription) -> Subscription | None:
    """Другая действующая подписка юзера (продлили/апгрейдили поверх этой)."""
    now = datetime.now(timezone.utc)
    return db.query(Subscription).filter(
        Subscription.user_id == sub.user_id,
        Subscription.id != sub.id,
        Subscription.status == "active",
        Subscription.expires_at > now,
    ).order_by(Subscription.id.desc()).first()


def _render_payment(db: Session, sub: Subscription, user, pm) -> str:
    prev = _paid_history(db, sub.user_id, before_id=sub.id)
    had_trial = db.query(Subscription.id).filter(
        Subscription.user_id == sub.user_id,
        Subscription.is_trial.is_(True),
        Subscription.id < sub.id,
    ).first() is not None
    if not prev:
        title = "🎉 <b>Новая подписка</b>" if not had_trial else "🎉 <b>Триал сконвертирован в оплату</b>"
    elif any(p.tier == sub.tier for p in prev):
        title = "🔁 <b>Продление</b>"
    else:
        title = "⬆️ <b>Смена тарифа</b>"
    amount = _rub(sub.amount)
    # Скидка удержания лежит на продлеваемой подписке, новая строка создаётся
    # с discount_pct=0: скидку видно только по сумме ниже цены плана.
    plan = get_plan(sub.plan_id)
    if plan and not _is_test_user(sub.user_id) and float(sub.amount) < plan.amount - 0.01:
        amount += f" (скидка {round((1 - float(sub.amount) / plan.amount) * 100)}%)"
    lines = [
        title,
        _who(user, sub.user_id),
        "",
        f"Тариф: <b>{_plan_name(sub)}</b>",
        f"Сумма: <b>{amount}</b>",
        f"Срок: {_d(sub.started_at)} → {_d(sub.expires_at)}",
        f"Оплата: {_method_line(pm, sub)}",
        "Автопродление: " + ("включено" if pm is not None and pm.deleted_at is None else "нет"),
        "",
        _ltv_line(db, sub.user_id),
    ]
    return "\n".join(lines)


def _render_payment_failed(db: Session, ev, sub: Subscription, user, pm) -> str | None:
    if ev["old_status"] == "binding":
        return None  # заявка на привязку СБП брошена, денег не двигалось
    if pm is None:
        return None  # чекаут брошен или отклонён на форме — только в сводку
    if sub.is_trial:
        return None
    prev = _paid_history(db, sub.user_id, before_id=sub.id)
    is_renewal = any(p.tier == sub.tier for p in prev)
    if is_renewal:
        quiet = db.execute(text("""
            SELECT 1 FROM billing_events
            WHERE user_id = :uid AND kind = 'payment_failed' AND NOT skipped
              AND notified_at IS NOT NULL
              AND created_at > now() - make_interval(hours => :h)
            LIMIT 1
        """), {"uid": sub.user_id, "h": RENEW_FAIL_QUIET_HOURS}).first()
        if quiet:
            return None
    # Код ошибки банка кладётся на продлеваемую (старую) подписку.
    code = db.query(Subscription.renewal_last_error).filter(
        Subscription.user_id == sub.user_id,
        Subscription.renewal_last_error.isnot(None),
    ).order_by(Subscription.updated_at.desc()).limit(1).scalar()
    base = db.query(Subscription).filter(
        Subscription.user_id == sub.user_id,
        Subscription.id < sub.id,
        Subscription.tier == sub.tier,
        Subscription.status.in_(("active", "expired")),
    ).order_by(Subscription.id.desc()).first()
    title = "⚠️ <b>Продление не прошло</b>" if is_renewal else "⚠️ <b>Первое списание не прошло</b>"
    lines = [
        title,
        _who(user, sub.user_id),
        "",
        f"Тариф: {_plan_name(sub)}, {_rub(sub.amount)}",
        f"Оплата: {_method_line(pm, sub)}",
    ]
    if code:
        lines.append(f"Код банка: {html.escape(str(code))}")
    if is_renewal and base is not None and base.expires_at:
        lines.append(f"Доступ до {_d(base.expires_at)}, крон пробует каждый час в течение суток")
    return "\n".join(lines)


def _render_autorenew_off(db: Session, sub: Subscription, user, pm) -> str | None:
    if sub.is_trial:
        return None  # триал гасится покупкой/конверсией
    other = _other_active(db, sub)
    if other is not None and other.id > sub.id:
        return None  # апгрейд: старой подписке выключили продление автоматически
    lines = [
        "🚪 <b>Отказ от автопродления</b>",
        _who(user, sub.user_id),
        "",
        f"Тариф: {_plan_name(sub)}, {_rub(sub.amount)}",
        f"Доступ сохранится до {_d(sub.expires_at)}",
    ]
    if user is not None and user.retention_used:
        lines.append("Скидку за удержание уже использовал")
    lines += ["", _ltv_line(db, sub.user_id)]
    return "\n".join(lines)


def _render_expired(db: Session, sub: Subscription, user, pm) -> str | None:
    if _other_active(db, sub) is not None:
        return None  # продлили заранее, истекла старая строка
    if sub.is_trial:
        reason = "пробный период кончился без оплаты"
    elif sub.yk_payment_id is None:
        reason = "кончился срок инвайта"
    elif sub.cancelled_at is not None:
        reason = "клиент отказался от автопродления"
    elif pm is None:
        reason = "автопродления не было"
    elif pm.deleted_at is not None:
        reason = "клиент отвязал способ оплаты"
    else:
        err = f" (код {html.escape(sub.renewal_last_error)})" if sub.renewal_last_error else ""
        reason = f"списание не прошло{err}"
    return "\n".join([
        "📉 <b>Подписка закончилась</b>",
        _who(user, sub.user_id),
        "",
        f"Тариф: {_plan_name(sub)}",
        f"Причина: {reason}",
        "",
        _ltv_line(db, sub.user_id),
    ])


def _render_event(db: Session, ev) -> str | None:
    """Текст сообщения или None, если событие системное и в чат не нужно."""
    kind = ev["kind"]
    user = db.query(User).filter(User.id == ev["user_id"]).first() if ev["user_id"] else None
    pm = None
    if ev["payment_method_id"]:
        pm = db.query(UserPaymentMethod).filter(UserPaymentMethod.id == ev["payment_method_id"]).first()

    if kind == "method_unlinked":
        if pm is None:
            return None
        relying = db.query(Subscription).filter(
            Subscription.payment_method_id == pm.id,
            Subscription.status == "active",
            Subscription.cancelled_at.is_(None),
        ).first()
        lines = ["🔌 <b>Отвязан способ оплаты</b>", _who(user, ev["user_id"]), "",
                 _method_line(pm).split(" (")[0]]
        if relying is not None:
            lines.append(f"Автопродление {_plan_name(relying)} не сработает, доступ до {_d(relying.expires_at)}")
        return "\n".join(lines)

    sub = db.query(Subscription).filter(Subscription.id == ev["subscription_id"]).first()
    if sub is None:
        return None
    if pm is None and sub.payment_method_id:
        pm = db.query(UserPaymentMethod).filter(UserPaymentMethod.id == sub.payment_method_id).first()

    if kind == "payment":
        return _render_payment(db, sub, user, pm)
    if kind == "payment_failed":
        return _render_payment_failed(db, ev, sub, user, pm)
    if kind == "autorenew_off":
        return _render_autorenew_off(db, sub, user, pm)
    if kind == "expired":
        return _render_expired(db, sub, user, pm)
    if kind == "trial_start":
        plan = get_plan(sub.plan_id)
        return "\n".join([
            "🧪 <b>Начат пробный период</b>", _who(user, sub.user_id), "",
            f"Тариф: {_plan_name(sub)}, до {_d(sub.expires_at)}",
            f"Оплата: {_method_line(pm, sub)}",
            f"Спишется по окончании: {_rub(plan.amount) if plan else '—'}",
        ])
    if kind == "grant":
        return "\n".join([
            "🎁 <b>Доступ по инвайту</b>", _who(user, sub.user_id), "",
            f"Тариф: {TIER_NAMES.get(sub.tier, sub.tier)}, до {_d(sub.expires_at)}",
        ])
    if kind == "autorenew_on":
        return "\n".join([
            "↩️ <b>Автопродление снова включено</b>", _who(user, sub.user_id), "",
            f"Тариф: {_plan_name(sub)}, следующее списание {_d(sub.expires_at)}",
        ])
    if kind == "retention":
        return "\n".join([
            "🤝 <b>Удержан скидкой</b>", _who(user, sub.user_id), "",
            f"Хотел отменить {_plan_name(sub)}, принял скидку {sub.discount_pct}% на продление {_d(sub.expires_at)}",
        ])
    if kind == "autopay_bound":
        return "\n".join([
            "🔗 <b>Привязан способ оплаты</b>", _who(user, sub.user_id), "",
            f"Тариф: {_plan_name(sub)}", f"Оплата: {_method_line(pm, sub)}",
            f"Автопродление {_d(sub.expires_at)}",
        ])
    if kind == "refund":
        return "\n".join([
            "💸 <b>Возврат</b>", _who(user, sub.user_id), "",
            f"Тариф: {_plan_name(sub)}, {_rub(sub.amount)}",
            f"Оплачено {_d(sub.started_at or sub.created_at)}, доступ отозван",
        ])
    if kind == "reversed":
        return "\n".join([
            "⛔️ <b>Платёж отменён банком</b>", _who(user, sub.user_id), "",
            f"Тариф: {_plan_name(sub)}, {_rub(sub.amount)}, доступ отозван",
        ])
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  Доставка
# ═══════════════════════════════════════════════════════════════════════════════

def dispatch_pending(db: Session, limit: int = 50) -> dict:
    """Отправить неотправленные события. Идемпотентно, безопасно параллельно."""
    summary = {"sent": 0, "skipped": 0, "failed": 0}
    if not _configured():
        return summary
    rows = db.execute(text("""
        SELECT id, kind, user_id, subscription_id, payment_method_id,
               old_status, new_status, amount, created_at
        FROM billing_events
        WHERE notified_at IS NULL AND kind <> 'digest_sent'
        ORDER BY id
        LIMIT :lim
        FOR UPDATE SKIP LOCKED
    """), {"lim": limit}).mappings().all()
    if not rows:
        db.rollback()
        return summary

    stale_before = datetime.now(timezone.utc) - timedelta(hours=MAX_EVENT_AGE_HOURS)
    for ev in rows:
        msg = None
        if ev["created_at"] >= stale_before:
            try:
                msg = _render_event(db, ev)
            except Exception as e:
                log.error("billing admin notify: render event %s failed: %s", ev["id"], e, exc_info=True)
                msg = f"💳 Событие биллинга <b>{html.escape(ev['kind'])}</b>, sub #{ev['subscription_id']}, user #{ev['user_id']} (не удалось оформить, см. логи)"
        if msg is None:
            db.execute(text("UPDATE billing_events SET notified_at = now(), skipped = TRUE WHERE id = :id"),
                       {"id": ev["id"]})
            summary["skipped"] += 1
            continue
        if _send(msg):
            db.execute(text("UPDATE billing_events SET notified_at = now() WHERE id = :id"), {"id": ev["id"]})
            summary["sent"] += 1
        else:
            # Телеграм недоступен: остальное тоже не уйдёт, пробуем следующим проходом.
            summary["failed"] += 1
            break
    db.commit()
    if summary["sent"] or summary["failed"]:
        log.info("billing admin notify: %s", summary)
    return summary


# ═══════════════════════════════════════════════════════════════════════════════
#  Утренняя сводка
# ═══════════════════════════════════════════════════════════════════════════════

def _day_bounds_msk(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=MSK)
    return start, start + timedelta(days=1)


def build_daily_digest(db: Session, day: date) -> str:
    """Сводка за день `day` (МСК) + срез на текущий момент."""
    d0, d1 = _day_bounds_msk(day)
    m0 = datetime(day.year, day.month, 1, tzinfo=MSK)
    now = datetime.now(timezone.utc)
    test_ids = [int(c) for c in (os.getenv("BILLING_TEST_USER_IDS") or "").split(",") if c.strip().isdigit()]
    p = {"d0": d0, "d1": d1, "m0": m0, "now": now, "week": now + timedelta(days=7),
         "test": test_ids or [-1]}

    ev = dict(db.execute(text("""
        SELECT kind, count(*) FROM billing_events
        WHERE created_at >= :d0 AND created_at < :d1 AND NOT skipped
          AND (user_id IS NULL OR NOT (user_id = ANY(:test)))
        GROUP BY kind
    """), p).all())

    paid = """
        s.is_trial = FALSE AND s.yk_payment_id IS NOT NULL AND s.amount > 0
        AND NOT (s.user_id = ANY(:test))
    """
    day_pay = db.execute(text(f"""
        SELECT count(*), COALESCE(sum(s.amount), 0) FROM subscriptions s
        WHERE {paid} AND s.status IN ('active','expired','cancelled')
          AND s.started_at >= :d0 AND s.started_at < :d1
    """), p).one()
    month_pay = db.execute(text(f"""
        SELECT count(*), COALESCE(sum(s.amount), 0) FROM subscriptions s
        WHERE {paid} AND s.status IN ('active','expired','cancelled')
          AND s.started_at >= :m0 AND s.started_at < :d1
    """), p).one()
    month_refund = db.execute(text(f"""
        SELECT count(*), COALESCE(sum(s.amount), 0) FROM subscriptions s
        WHERE {paid} AND s.status = 'refunded'
          AND s.cancelled_at >= :m0 AND s.cancelled_at < :d1
    """), p).one()
    abandoned = db.execute(text("""
        SELECT count(*) FROM subscriptions s
        WHERE s.status = 'failed' AND s.payment_method_id IS NULL AND s.is_trial = FALSE
          AND s.created_at >= :d0 AND s.created_at < :d1 AND NOT (s.user_id = ANY(:test))
    """), p).scalar()

    active = db.execute(text(f"""
        SELECT s.tier, s.period,
               count(*) AS n,
               count(*) FILTER (WHERE s.cancelled_at IS NULL AND s.payment_method_id IS NOT NULL) AS auto,
               COALESCE(sum(CASE WHEN s.period = 'yearly' THEN s.amount / 12 ELSE s.amount END)
                        FILTER (WHERE s.cancelled_at IS NULL AND s.payment_method_id IS NOT NULL), 0) AS mrr
        FROM subscriptions s
        WHERE {paid} AND s.status = 'active' AND s.expires_at > :now
        GROUP BY s.tier, s.period ORDER BY s.tier, s.period
    """), p).all()
    grants = db.execute(text("""
        SELECT count(*) FROM subscriptions s
        WHERE s.status = 'active' AND s.expires_at > :now AND s.yk_payment_id IS NULL
          AND s.is_trial = FALSE
    """), p).scalar()
    upcoming = db.execute(text(f"""
        SELECT count(*), COALESCE(sum(s.amount), 0) FROM subscriptions s
        WHERE {paid} AND s.status = 'active' AND s.cancelled_at IS NULL
          AND s.payment_method_id IS NOT NULL
          AND s.expires_at > :now AND s.expires_at < :week
    """), p).one()
    ending = db.execute(text(f"""
        SELECT count(*) FROM subscriptions s
        WHERE {paid} AND s.status = 'active'
          AND (s.cancelled_at IS NOT NULL OR s.payment_method_id IS NULL)
          AND s.expires_at > :now AND s.expires_at < :week
          AND NOT EXISTS (SELECT 1 FROM subscriptions o WHERE o.user_id = s.user_id
                          AND o.id > s.id AND o.status = 'active')
    """), p).scalar()

    lines = [f"📊 <b>Биллинг за {day:%d.%m.%Y}</b>", ""]
    lines.append(f"Оплат: <b>{day_pay[0]}</b> на <b>{_rub(day_pay[1])}</b>")
    lines.append(
        f"новых {_count_new(db, d0, d1, test_ids)}, "
        f"отказов от автопродления {ev.get('autorenew_off', 0)}, "
        f"закончилось {ev.get('expired', 0)}"
    )
    extra = []
    if ev.get("payment_failed"):
        extra.append(f"провалов списания {ev['payment_failed']}")
    if abandoned:
        extra.append(f"брошенных оформлений {abandoned}")
    if ev.get("refund"):
        extra.append(f"возвратов {ev['refund']}")
    if ev.get("method_unlinked"):
        extra.append(f"отвязок карт {ev['method_unlinked']}")
    if extra:
        lines.append(", ".join(extra))

    lines += ["", f"С начала месяца: <b>{_rub(month_pay[1])}</b> ({month_pay[0]} оплат)"]
    if month_refund[0]:
        lines.append(f"Возвраты за месяц: {_rub(month_refund[1])} ({month_refund[0]})")

    total_n = sum(r.n for r in active)
    total_auto = sum(r.auto for r in active)
    mrr = sum(float(r.mrr) for r in active)
    lines += ["", f"Платных подписок сейчас: <b>{total_n}</b>, с автопродлением {total_auto}"]
    for r in active:
        lines.append(f"  {TIER_NAMES.get(r.tier, r.tier)} {PERIOD_NAMES.get(r.period, r.period)}: {r.n}")
    if grants:
        lines.append(f"  по инвайтам: {grants}")
    lines.append(f"MRR с автопродлением: <b>{_rub(round(mrr))}</b>")
    lines += ["", f"Ближайшие 7 дней: продлений {upcoming[0]} на {_rub(upcoming[1])}, закончатся без продления {ending}"]
    return "\n".join(lines)


def _count_new(db: Session, d0, d1, test_ids) -> int:
    """Первые оплаты за день: у юзера нет более ранних оплаченных подписок."""
    return db.execute(text("""
        SELECT count(*) FROM subscriptions s
        WHERE s.is_trial = FALSE AND s.yk_payment_id IS NOT NULL AND s.amount > 0
          AND s.status IN ('active','expired','cancelled')
          AND s.started_at >= :d0 AND s.started_at < :d1
          AND NOT (s.user_id = ANY(:test))
          AND NOT EXISTS (
              SELECT 1 FROM subscriptions o
              WHERE o.user_id = s.user_id AND o.id < s.id AND o.is_trial = FALSE
                AND o.yk_payment_id IS NOT NULL AND o.amount > 0
                AND o.status IN ('active','expired','cancelled','refunded'))
    """), {"d0": d0, "d1": d1, "test": test_ids or [-1]}).scalar()


def send_daily_digest(db: Session, force: bool = False) -> bool:
    """Отправить сводку за вчера (МСК), один раз в сутки. Маркер — строка
    digest_sent в billing_events: переживает рестарт оркестратора."""
    if not _configured():
        return False
    now_msk = datetime.now(MSK)
    if not force and now_msk.hour < DIGEST_HOUR_MSK:
        return False
    day = now_msk.date() - timedelta(days=1)
    marker_start, _ = _day_bounds_msk(now_msk.date())
    if not force:
        # Лок на маркер: два параллельных прохода не отправят сводку дважды.
        db.execute(text("SELECT pg_advisory_xact_lock(hashtext('billing_digest'))"))
        done = db.execute(text("""
            SELECT 1 FROM billing_events
            WHERE kind = 'digest_sent' AND created_at >= :t LIMIT 1
        """), {"t": marker_start}).first()
        if done:
            db.rollback()
            return False
    msg = build_daily_digest(db, day)
    ok = _send(msg)
    if ok and not force:
        db.execute(text("""
            INSERT INTO billing_events (kind, notified_at, skipped) VALUES ('digest_sent', now(), TRUE)
        """))
    db.commit()
    return ok
