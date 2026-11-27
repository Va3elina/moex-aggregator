"""
Уведомления о биллинге в FRAME · Касса (@frame_billing_bot, BILLING_BOT_TOKEN; с 03.11.2026,
до этого — @frameadminbot, он остался запасным).

События пишет триггер БД в billing_events (db/migrations/106_billing_events.sql),
здесь их рендер и доставка:

  dispatch_pending  — забрать неотправленные события и отправить в чат.
                      Зовут: NOTIFY-листенер API (сразу после коммита) и
                      оркестратор каждые 15 мин (страховка, если листенер лежал).

Строки забираются FOR UPDATE SKIP LOCKED: три воркера API и оркестратор могут
звать одновременно, одно событие уйдёт один раз. Если Телеграм не ответил,
notified_at не ставится и событие уйдёт следующим проходом.

  flag_paid_without_method — найти оплаты на форме, у которых через 15 минут
                      так и нет способа оплаты, и завести по каждой событие
                      paid_no_method (одно на подписку). Зовёт оркестратор.
"""
import html
import logging
import os
from datetime import datetime, timedelta, timezone
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

# Продление: не больше 3 попыток (service.RENEW_*), обычно с паузой ≥23ч, но
# месячный после годового NSF-отказа идёт уже через час. Тишина 30 мин меньше
# любой паузы — в чат уходит каждая попытка («попытка N из 3»), а дубли одной
# попытки (отказ + поздний webhook) глушатся.
RENEW_FAIL_QUIET_MINUTES = 30

# «Оплата без карты»: сколько ждём привязку после активации и как далеко назад
# смотрим. Привязку приносит только вебхук банка, обычно в ту же минуту; /sync
# со страницы «Оплата прошла» включает подписку без неё. Окно назад — чтобы
# первый запуск после выкладки не поднял всю историю разовых оплат.
NO_METHOD_GRACE_MINUTES = 15
NO_METHOD_LOOKBACK_HOURS = 24
KIND_PAID_NO_METHOD = "paid_no_method"

TIER_NAMES = {"basic": "Basic", "pro": "Pro", "premium": "Premium"}
PERIOD_NAMES = {"monthly": "месяц", "yearly": "год", "trial": "пробный период"}


# ═══════════════════════════════════════════════════════════════════════════════
#  Телеграм
# ═══════════════════════════════════════════════════════════════════════════════

def _tokens() -> list[str]:
    """Касса; нет её токена или получатель её не запустил — @frameadminbot, как раньше."""
    toks = [os.environ.get(k, "").strip() for k in ("BILLING_BOT_TOKEN", "BOT_TOKEN")]
    return list(dict.fromkeys(t for t in toks if t))


def _configured() -> bool:
    return bool(_tokens() and os.environ.get("ADMIN_CHAT_ID"))


def _send(text_msg: str) -> bool:
    """Получатели ADMIN_CHAT_ID + TELEGRAM_API_ROOT (прямой api.telegram.org с прода закрыт)."""
    tokens = _tokens()
    # ADMIN_CHAT_ID — один id или список через запятую (всем админам бота).
    chat_ids = [c.strip() for c in os.environ.get("ADMIN_CHAT_ID", "").split(",") if c.strip()]
    if not tokens or not chat_ids:
        return False
    # Успех, если дошло хотя бы одному: иначе недоступный второй получатель
    # зациклил бы повторную отправку первому. Каждому — первым ботом, который принял.
    return any([any(_send_one(t, chat_id, text_msg) for t in tokens) for chat_id in chat_ids])


def _send_one(token: str, chat_id: str, text_msg: str) -> bool:
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
        # Релей может ответить 200 с ошибкой Телеграма в теле: верим только ok=true,
        # иначе событие помечается отправленным, а в чат не попадает.
        body = resp.json()
        if not body.get("ok"):
            log.warning("billing admin notify: telegram rejected chat %s: %s", chat_id, str(body)[:300])
            return False
        return True
    except Exception as e:
        log.warning("billing admin notify: send failed: %s", str(e).replace(token, "<REDACTED>"))
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
        # Оплатой считаем только подписку, которая хоть раз активировалась
        # (started_at ставит activate_from_webhook). Неудачное автопродление
        # получает failed, а следом вебхук банка об отмене переводил его в
        # cancelled: каждая попытка списания засчитывалась как оплата.
        Subscription.started_at.isnot(None),
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
              AND created_at > now() - make_interval(mins => :m)
            LIMIT 1
        """), {"uid": sub.user_id, "m": RENEW_FAIL_QUIET_MINUTES}).first()
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
        from api.billing.service import (
            RENEW_MAX_ATTEMPTS, next_renew_attempt_at, renewal_attempts,
        )
        # Сама эта попытка — последняя в списке; если её статус ещё не
        # долетел до failed/cancelled, считаем её вручную.
        done = [a for a in renewal_attempts(db, base) if a.id <= sub.id] or [sub]
        n = len(done)
        nxt = next_renew_attempt_at(base, done)
        if nxt is None:
            tail = "больше не пробуем"
        elif base.fallback_done and done[-1].period == "yearly":
            tail = "дальше месячный тариф в течение часа"
        else:
            tail = f"следующая не раньше {nxt.astimezone(MSK):%d.%m %H:%M} МСК"
        lines.append(f"Доступ до {_d(base.expires_at)}")
        lines.append(f"Попытка {n} из {RENEW_MAX_ATTEMPTS}, {tail}")
    return "\n".join(lines)


def _render_double_charge(db: Session, sub: Subscription, user) -> str | None:
    """Две оплаты за один тариф (service.find_double_charge). Возврат — решение админа."""
    from api.billing.service import find_double_charge

    twin = find_double_charge(db, sub)
    if twin is None:
        return None  # вторую оплату уже вернули/отменили
    pair = sorted((twin, sub), key=lambda s: s.started_at)

    def _row(n: int, s: Subscription) -> str:
        pm = None
        if s.payment_method_id:
            pm = db.query(UserPaymentMethod).filter(UserPaymentMethod.id == s.payment_method_id).first()
        renew = "автопродление включено" if (
            s.cancelled_at is None and pm is not None and pm.deleted_at is None
        ) else "автопродление выключено"
        return (
            f"{n}. {_plan_name(s)}, <b>{_rub(s.amount)}</b>, "
            f"{s.started_at.astimezone(MSK):%d.%m %H:%M} МСК\n"
            f"   sub #{s.id}, платёж {html.escape(str(s.yk_payment_id))}, {renew}"
        )

    paid_until = max(s.expires_at for s in pair if s.expires_at)
    return "\n".join([
        "🚨 <b>Двойное списание за один тариф</b>",
        _who(user, sub.user_id),
        "",
        _row(1, pair[0]),
        _row(2, pair[1]),
        "",
        f"Сроки сложились, доступ до {_d(paid_until)}.",
        "Возврат автоматически НЕ сделан. Если клиент платить дважды не собирался, "
        "верните одну из оплат вручную (админка → возврат по номеру sub).",
    ])


def _webhook_trail(db: Session, payment_id: str | None) -> str | None:
    """Что банк присылал по платежу (bank_webhook_log). None — журнал недоступен."""
    if not payment_id:
        return None
    try:
        with db.begin_nested():   # нет таблицы → не ломаем транзакцию рассылки
            rows = db.execute(text("""
                SELECT status, has_rebill_id, has_account_token, result
                FROM bank_webhook_log
                WHERE payment_id = :pid
                ORDER BY id DESC LIMIT 4
            """), {"pid": payment_id}).mappings().all()
    except Exception:
        return None
    if not rows:
        return "Вебхуки банка: по этому платежу не приходили"
    parts = []
    for r in reversed(rows):
        has_key = r["has_rebill_id"] or r["has_account_token"]
        parts.append(
            f"{html.escape(r['status'] or '?')} "
            f"({'с привязкой' if has_key else 'без привязки'}, {html.escape(r['result'])})"
        )
    return "Вебхуки банка: " + " → ".join(parts)


def _render_paid_no_method(db: Session, sub: Subscription, user, pm) -> str | None:
    """Оплата на форме прошла, а способ оплаты за NO_METHOD_GRACE_MINUTES не привязался."""
    if pm is not None or sub.payment_method_id is not None:
        return None  # привязка доехала, пока событие ждало отправки
    if sub.status != "active" or sub.cancelled_at is not None:
        return None  # вернули деньги или автопродление уже не нужно
    paid_at = sub.started_at or sub.created_at
    lines = [
        "🚨 <b>Оплата прошла, способ оплаты не привязан</b>",
        _who(user, sub.user_id),
        "",
        f"Тариф: {_plan_name(sub)}, <b>{_rub(sub.amount)}</b>",
        f"Оплачено {paid_at.astimezone(MSK):%d.%m %H:%M} МСК, "
        f"sub #{sub.id}, платёж {html.escape(str(sub.yk_payment_id))}",
        "Способ: " + ("СБП" if sub.yk_method == "sbp" else html.escape(sub.yk_method or "карта")),
    ]
    trail = _webhook_trail(db, sub.yk_payment_id)
    if trail:
        lines.append(trail)
    lines += [
        "",
        f"За {NO_METHOD_GRACE_MINUTES} минут после оплаты привязка от банка не сохранилась: "
        f"автопродления нет, доступ кончится {_d(sub.expires_at)}.",
        "Проверьте платёж в кабинете T-Bank: есть ли у него привязка (RebillId / счёт СБП).",
    ]
    return "\n".join(lines)


def _render_autorenew_off(db: Session, sub: Subscription, user, pm) -> str | None:
    if sub.is_trial:
        return None  # триал гасится покупкой/конверсией
    other = _other_active(db, sub)
    if other is not None and other.id > sub.id:
        return None  # апгрейд: старой подписке выключили продление автоматически
    # Покупка поверх того же тарифа: продление переехало на другую действующую
    # подписку (её id может быть и меньше — форму открыли до автосписания).
    now = datetime.now(timezone.utc)
    moved = db.query(Subscription.id).filter(
        Subscription.user_id == sub.user_id,
        Subscription.id != sub.id,
        Subscription.status == "active",
        Subscription.cancelled_at.is_(None),
        Subscription.expires_at > now,
    ).first()
    if moved is not None:
        return None
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
    if kind == "double_charge":
        return _render_double_charge(db, sub, user)
    if kind == "promo_twice":
        return "\n".join([
            "🚨 <b>Вторая покупка по акции</b>", _who(user, sub.user_id), "",
            f"Тариф: {_plan_name(sub)}, <b>{_rub(sub.amount)}</b>, sub #{sub.id}",
            f"Доступ до {_d(sub.expires_at)}",
            "По правилам акции — одна покупка на аккаунт. Клиент оплатил две формы, "
            "открытые одновременно. Возврат автоматически НЕ сделан — решите вручную.",
        ])
    if kind == KIND_PAID_NO_METHOD:
        return _render_paid_no_method(db, sub, user, pm)
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
            f"Хотел отменить {_plan_name(sub)}, принял скидку "
            + (f"{_rub(sub.discount_rub)} (месяц в подарок)" if (sub.discount_rub or 0) > 0 else f"{sub.discount_pct}%")
            + (f" на {sub.discount_periods_left} продления с {_d(sub.expires_at)}"
               if (sub.discount_periods_left or 0) > 1 else f" на продление {_d(sub.expires_at)}"),
        ])
    if kind == "autopay_bound":
        # Вебхук первой оплаты в одной транзакции активирует подписку и пишет карту:
        # триггер даёт payment и autopay_bound с одним created_at (now() транзакции).
        # «Новая подписка» уже говорит «карта, привязана», второе сообщение лишнее.
        # Поздняя привязка (СБП-резолв, карта к действующей подписке) идёт отдельно.
        same_tx_payment = db.execute(text("""
            SELECT 1 FROM billing_events
            WHERE kind = 'payment' AND subscription_id = :sid AND created_at = :ts
            LIMIT 1
        """), {"sid": sub.id, "ts": ev["created_at"]}).first()
        if same_tx_payment is not None:
            return None
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
#  Алерт «оплата без карты»
# ═══════════════════════════════════════════════════════════════════════════════

def flag_paid_without_method(db: Session, now: datetime | None = None) -> dict:
    """
    Оплаченные на форме подписки, у которых через NO_METHOD_GRACE_MINUTES после
    активации нет payment_method_id → событие paid_no_method в billing_events
    (дальше обычная доставка dispatch_pending). Одно событие на подписку.

    Кого берём: active, не триал, не инвайт (есть платёж), amount > 0,
    автопродление не выключено. Продления крона сюда не попадают — их строка
    создаётся уже с payment_method_id. СБП с незакрытой привязкой
    (sbp_request_key) ждём: её ещё дотягивает resolve_sbp_bindings.

    С мая по сентябрь 2026 таких оплат было 7, и ни одну не заметили.
    """
    now = now or datetime.now(timezone.utc)
    subs = db.query(Subscription).filter(
        Subscription.status == "active",
        Subscription.is_trial.is_(False),
        Subscription.yk_payment_id.isnot(None),
        Subscription.amount > 0,
        Subscription.payment_method_id.is_(None),
        Subscription.cancelled_at.is_(None),
        Subscription.sbp_request_key.is_(None),
        Subscription.started_at <= now - timedelta(minutes=NO_METHOD_GRACE_MINUTES),
        Subscription.started_at > now - timedelta(hours=NO_METHOD_LOOKBACK_HOURS),
    ).order_by(Subscription.id).all()

    flagged = 0
    for sub in subs:
        seen = db.execute(text(
            "SELECT 1 FROM billing_events WHERE kind = :k AND subscription_id = :sid LIMIT 1"
        ), {"k": KIND_PAID_NO_METHOD, "sid": sub.id}).first()
        if seen is not None:
            continue
        # ON CONFLICT — на случай параллельного прохода: в миграции 122 на
        # (subscription_id) WHERE kind='paid_no_method' стоит уникальный индекс.
        res = db.execute(text("""
            INSERT INTO billing_events (kind, user_id, subscription_id, new_status, amount)
            VALUES (:k, :uid, :sid, 'active', :amount)
            ON CONFLICT DO NOTHING
        """), {"k": KIND_PAID_NO_METHOD, "uid": sub.user_id, "sid": sub.id,
              "amount": float(sub.amount) if sub.amount is not None else None})
        if res.rowcount:
            flagged += 1
            log.warning(
                "paid_no_method: sub=%s user=%s оплачена %s, способ оплаты не привязан",
                sub.id, sub.user_id, sub.started_at,
            )
    if flagged and db.get_bind().dialect.name == "postgresql":
        # Будим листенер API (как триггер миграции 106) — сообщение уйдёт сразу.
        db.execute(text("SELECT pg_notify('billing_event', :p)"), {"p": KIND_PAID_NO_METHOD})
    db.commit()
    return {"checked": len(subs), "flagged": flagged}
