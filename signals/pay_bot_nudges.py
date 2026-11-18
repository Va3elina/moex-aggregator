"""
Дожимы бота оплаты @FrameDataPayBot (этап 3, docs/tg-pay-bot.md).

Host-cron раз в 15 минут (signals/pay_bot_nudges.sh):
  */15 * * * * /bin/bash /opt/frame/signals/pay_bot_nudges.sh >> /opt/frame/logs/pay_bot_nudges.log 2>&1
Ручной прогон без отправки: --dry-run.

Коды (каждый не больше одного раза на лида, n4/n5 на каждую подписку):
  n1_noclaim     — /start больше суток назад, на сайт так и не вошёл;
  n2_nopay       — привязан больше суток назад, активной подписки нет;
  n4_expiring    — подписка без автопродления кончается в ближайшие 3 дня;
  n5_renew_fail  — автопродление не прошло (renewal_last_error), новой подписки нет.
n3_discount (скидка на первую оплату) — этап 5, после решения о скидке.

Правила: не пишем с 22:00 до 09:00 МСК; не больше 3 дожимов на лида за 7 дней
и не чаще раза в сутки; за один прогон одному лиду максимум один дожим (по
приоритету n5 > n4 > n2 > n1). /stop отключает всё, кроме n5 (сервисное).
Заблокировавшим бот не пишем. Админов сайта не дожимаем.
Дожимы n1/n2 это ответ на /start человека, не реклама: согласие не нужно.
"""
import argparse
import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from signals import pay_bot as pb
from api.database import SessionLocal

MSK = timezone(timedelta(hours=3))
QUIET_FROM, QUIET_TO = 22, 9          # с 22:00 до 09:00 МСК молчим
CAP_PER_WEEK = 3
MIN_GAP = timedelta(hours=24)
PRIORITY = ["n5_renew_fail", "n4_expiring", "n2_nopay", "n1_noclaim"]
SITE = pb.SITE


def in_quiet_hours(now: datetime) -> bool:
    h = now.astimezone(MSK).hour
    return h >= QUIET_FROM or h < QUIET_TO


def pick(candidates: list[dict], history: dict[int, list[datetime]], now: datetime) -> list[dict]:
    """По одному дожиму на лида: высший приоритет, если лид не упёрся в лимиты.
    history: chat_id → времена прошлых дожимов."""
    best: dict[int, dict] = {}
    for c in candidates:
        cur = best.get(c["chat_id"])
        if cur is None or PRIORITY.index(c["code"]) < PRIORITY.index(cur["code"]):
            best[c["chat_id"]] = c
    out = []
    for chat_id, c in best.items():
        past = [t for t in history.get(chat_id, []) if t > now - timedelta(days=7)]
        if len(past) >= CAP_PER_WEEK:
            continue
        if past and max(past) > now - MIN_GAP:
            continue
        out.append(c)
    return sorted(out, key=lambda c: PRIORITY.index(c["code"]))


def message(c: dict) -> tuple[str, list]:
    code = c["code"]
    tier = pb.TIER_NAMES.get(c.get("tier") or "", c.get("tier") or "")
    if code == "n1_noclaim":
        return ("Вы заглядывали в Фрейм, но до тарифов не дошли. Если остались вопросы, "
                "напишите прямо сюда, ответим. Тарифы по кнопке:",
                [[{"text": "💳 Выбрать тариф", "url": c["url"]}]])
    if code == "n2_nopay":
        return ("Вы вошли на сайт, но подписку не оформили. Если что-то помешало при "
                "оплате, напишите сюда, разберёмся. Оформить можно по кнопке:",
                [[{"text": "💳 Выбрать тариф", "url": c["url"]}]])
    if code == "n4_expiring":
        return (f"Подписка {tier} заканчивается {pb.fmt_date(c['expires_at'])}, автопродления "
                "нет. Чтобы доступ не прервался, продлите заранее:",
                [[{"text": "🔄 Продлить", "url": c["url"]}]])
    if code == "n5_renew_fail":
        return (f"Не получилось продлить подписку {tier}: банк отклонил списание. Проверьте "
                "баланс карты или привяжите другой способ оплаты в профиле.",
                [[{"text": "👤 Открыть профиль", "url": f"{SITE}/profile"}]])
    raise ValueError(code)


# ── выборка ─────────────────────────────────────────────────────────────────
_BASE = """
    FROM tg_pay_leads l
    LEFT JOIN users u ON u.id = l.user_id
    WHERE l.blocked_at IS NULL
      AND COALESCE(u.role, '') <> 'admin'
      AND NOT EXISTS (SELECT 1 FROM tg_pay_events e WHERE e.chat_id = l.chat_id
                      AND e.kind = 'nudge_' || :code {sub_filter})
"""

SQL = {
    "n1_noclaim": """
        SELECT l.chat_id, l.user_id, l.start_param, NULL::int AS sub_id, NULL AS tier,
               NULL::timestamptz AS expires_at
    """ + _BASE.format(sub_filter="") + """
      AND l.opted_out_at IS NULL
      AND l.user_id IS NULL
      AND l.first_seen_at < now() - interval '24 hours'
      AND l.first_seen_at > now() - interval '7 days'
    """,
    "n2_nopay": """
        SELECT l.chat_id, l.user_id, l.start_param, NULL::int AS sub_id, NULL AS tier,
               NULL::timestamptz AS expires_at
    """ + _BASE.format(sub_filter="") + """
      AND l.opted_out_at IS NULL
      AND l.user_id IS NOT NULL
      AND l.linked_at < now() - interval '24 hours'
      AND l.linked_at > now() - interval '7 days'
      AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.user_id = l.user_id
                      AND s.status IN ('active', 'pending', 'bound'))
    """,
    "n4_expiring": """
        SELECT l.chat_id, l.user_id, l.start_param, s.id AS sub_id, s.tier, s.expires_at
    """ + _BASE.format(sub_filter="AND (e.meta->>'sub_id')::int = s.id").replace(
        "LEFT JOIN users u ON u.id = l.user_id",
        "JOIN users u ON u.id = l.user_id JOIN subscriptions s ON s.user_id = l.user_id") + """
      AND l.opted_out_at IS NULL
      AND s.status = 'active' AND NOT s.is_trial
      AND s.expires_at BETWEEN now() AND now() + interval '3 days'
      AND (s.payment_method_id IS NULL OR s.cancelled_at IS NOT NULL)
      AND NOT EXISTS (SELECT 1 FROM subscriptions s2 WHERE s2.user_id = s.user_id
                      AND s2.id <> s.id AND s2.status = 'active' AND s2.expires_at > s.expires_at)
    """,
    "n5_renew_fail": """
        SELECT l.chat_id, l.user_id, l.start_param, s.id AS sub_id, s.tier, s.expires_at
    """ + _BASE.format(sub_filter="AND (e.meta->>'sub_id')::int = s.id").replace(
        "LEFT JOIN users u ON u.id = l.user_id",
        "JOIN users u ON u.id = l.user_id JOIN subscriptions s ON s.user_id = l.user_id") + """
      AND s.renewal_last_error IS NOT NULL
      AND s.status IN ('active', 'expired')
      AND s.expires_at > now() - interval '3 days'
      AND NOT EXISTS (SELECT 1 FROM subscriptions s2 WHERE s2.user_id = s.user_id
                      AND s2.id <> s.id AND s2.status = 'active' AND s2.expires_at > s.expires_at)
    """,
}


def collect(db) -> list[dict]:
    out = []
    for code, sql in SQL.items():
        for r in db.execute(text(sql), {"code": code}).mappings().all():
            out.append({**dict(r), "code": code})
    return out


def history(db) -> dict[int, list[datetime]]:
    h: dict[int, list[datetime]] = {}
    for chat_id, at in db.execute(text(
            "SELECT chat_id, at FROM tg_pay_events WHERE kind LIKE 'nudge\\_%' "
            "AND at > now() - interval '7 days'")).all():
        h.setdefault(chat_id, []).append(at)
    return h


def run(dry: bool) -> None:
    now = datetime.now(timezone.utc)
    if in_quiet_hours(now) and not dry:
        return
    db = SessionLocal()
    try:
        chosen = pick(collect(db), history(db), now)
        for c in chosen:
            if c["code"] == "n1_noclaim":
                c["url"] = pb.pricing_url(pb.new_claim_token(db, c["chat_id"]), c["code"])
            else:
                c["url"] = f"{SITE}/pricing?src=tg_bot&feature={c['code']}"
            msg, kb = message(c)
            if dry:
                print(f"[dry] {c['chat_id']} {c['code']} sub={c['sub_id']}: {msg[:70]}")
                db.rollback()
                continue
            ok = pb.send(c["chat_id"], msg, kb)
            db.execute(text("INSERT INTO tg_pay_events (chat_id, user_id, kind, meta) "
                            "VALUES (:c, :u, :k, CAST(:m AS JSONB))"),
                       {"c": c["chat_id"], "u": c["user_id"], "k": "nudge_" + c["code"],
                        "m": json.dumps({"sub_id": c["sub_id"], "delivered": ok})})
            db.commit()
            print(f"{now:%Y-%m-%d %H:%M} {c['chat_id']} {c['code']} delivered={ok}", flush=True)
    finally:
        db.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(ap.parse_args().dry_run)
