"""
Бот оплаты Фрейма @FrameDataPayBot — host-side long-poll поллер. ТЗ: docs/tg-pay-bot.md.

Зачем: из канала люди идут на сайт и регистрируются через VK/Яндекс, почту там
не читают, и контакт теряется. Кто нажал /start здесь, тому бот может написать.

Чего бот НЕ делает:
  - не принимает платёж (цифровые подписки внутри Telegram только за Stars);
    кнопка ведёт на /pricing с развилкой карта / привязка СБП, НЕ на форму T-Bank,
    иначе СБП пройдёт без привязки счёта;
  - не логинит на сайт (406-ФЗ): чат привязывается к аккаунту claim-токеном
    (tg_pay_claims) уже ПОСЛЕ входа через VK/Яндекс/почту, POST /api/tg-pay/claim.

ЗАПУСК НА ХОСТЕ (как alert_bot): /opt/frame/signals/.venv/bin/python -m signals.pay_bot
Под systemd (signals/frame-pay-bot.service). ENV (.env корня):
  PAY_BOT_TOKEN       — токен бота;
  PAY_BOT_ADMIN_IDS   — Telegram id админов через запятую (/stats, уведомления об оплатах);
  TELEGRAM_API_ROOT   — релей в обход блокировок (общий с alert_bot).

Команды: /start [кампания], /status, /stop, /stats (админ).
"""
import json
import os
import re
import secrets
import time
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

# ── .env + DB_URL override ДО импорта api.database ──────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /opt/frame
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:  # host-side: docker-hostname → localhost
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from sqlalchemy import text  # noqa: E402
from api.database import SessionLocal  # noqa: E402  (импорт ПОСЛЕ DB_URL override)

BOT_TOKEN = os.environ.get("PAY_BOT_TOKEN", "")
API_ROOT = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
API_BASE = f"{API_ROOT}/bot{BOT_TOKEN}"
ADMIN_IDS = {int(x) for x in re.findall(r"-?\d+", os.environ.get("PAY_BOT_ADMIN_IDS", ""))}

SITE = "https://framedata.ru"
CLAIM_TTL = timedelta(hours=24)
PAID_SCAN_EVERY_S = 60

TIER_NAMES = {"basic": "Basic", "pro": "Pro", "premium": "Premium"}
PERIOD_NAMES = {"monthly": "месяц", "yearly": "год"}

# Кампания из /start: Telegram пускает только [A-Za-z0-9_-]{1,64}.
_PARAM_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

CB_CONSENT = "consent"

WELCOME = (
    "👋 Это бот подписки Фрейма.\n\n"
    "Фрейм показывает, что делают крупные игроки на МосБирже: открытый интерес "
    "юрлиц и физлиц, деньги в фондах, покупки фондов по компаниям, сезонность "
    "и силу рынка.\n\n"
    "Здесь можно оформить подписку, а потом получать сообщения об оплате и "
    "продлении."
)
CONSENT_PROMPT = "Хотите получать скидки и акции? Пишу редко, отписаться можно командой /stop."
HELP = (
    "/start — тарифы\n"
    "/status — моя подписка\n"
    "/stop — не присылать скидки и напоминания"
)


# ── чистые функции (покрыты тестами) ────────────────────────────────────────
def clean_start_param(arg: str | None) -> str | None:
    arg = (arg or "").strip()
    return arg if _PARAM_RE.match(arg) else None


def pricing_url(token: str, campaign: str | None) -> str:
    q = f"src=tg_bot&tgc={token}"
    if campaign:
        q += f"&feature={campaign}"
    return f"{SITE}/pricing?{q}"


def fmt_date(dt: datetime | None) -> str:
    return dt.astimezone(timezone(timedelta(hours=3))).strftime("%d.%m.%Y") if dt else "?"


def paid_text(tier: str, period: str, expires_at, first: bool) -> str:
    name = TIER_NAMES.get(tier, tier)
    if first:
        return (f"✅ Доступ открыт: {name}, до {fmt_date(expires_at)}.\n\n"
                f"Всё уже работает на {SITE.replace('https://', '')}. "
                "Если что-то не так, просто ответьте сюда.")
    return f"🔄 Подписка {name} продлена до {fmt_date(expires_at)}."


# ── Telegram ────────────────────────────────────────────────────────────────
def _redact(s) -> str:
    return str(s).replace(BOT_TOKEN, "<REDACTED>") if BOT_TOKEN else str(s)


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] [pay_bot] {_redact(msg)}", flush=True)


def tg(method: str, payload: dict) -> dict | None:
    try:
        r = requests.post(f"{API_BASE}/{method}", json=payload, timeout=15)
        return r.json()
    except (requests.RequestException, ValueError) as e:
        _log(f"{method} error: {e}")
        return None


def send(chat_id: int, text_msg: str, keyboard: list | None = None) -> bool:
    """True — доставлено. 403 (бот заблокирован) помечаем в лиде."""
    payload = {"chat_id": chat_id, "text": text_msg, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    if keyboard:
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    res = tg("sendMessage", payload)
    if res and res.get("ok"):
        return True
    if res and res.get("error_code") == 403:
        _mark_blocked(chat_id)
    return False


def _setup_bot_profile() -> None:
    """Команды и описание бота. Идемпотентно, раз при старте."""
    tg("setMyCommands", {"commands": [
        {"command": "start", "description": "Тарифы"},
        {"command": "status", "description": "Моя подписка"},
        {"command": "stop", "description": "Не присылать скидки и напоминания"},
    ]})
    tg("setMyShortDescription", {"short_description":
        "Подписка на Фрейм: аналитика МосБиржи. Оформление и напоминания."})
    tg("setMyDescription", {"description":
        "Бот подписки Фрейма (framedata.ru): открытый интерес, деньги в фондах, "
        "покупки фондов, сезонность и сила рынка. Здесь можно оформить подписку "
        "и получать сообщения об оплате и продлении."})


# ── БД ──────────────────────────────────────────────────────────────────────
def _event(db, chat_id: int, kind: str, user_id=None, meta: dict | None = None) -> None:
    db.execute(text("INSERT INTO tg_pay_events (chat_id, user_id, kind, meta) "
                    "VALUES (:c, :u, :k, CAST(:m AS JSONB))"),
               {"c": chat_id, "u": user_id, "k": kind,
                "m": json.dumps(meta, ensure_ascii=False) if meta else None})


def _mark_blocked(chat_id: int) -> None:
    db = SessionLocal()
    try:
        res = db.execute(text("UPDATE tg_pay_leads SET blocked_at = now() "
                              "WHERE chat_id = :c AND blocked_at IS NULL"), {"c": chat_id})
        if res.rowcount:
            _event(db, chat_id, "blocked")
        db.commit()
    finally:
        db.close()


def upsert_lead(db, chat: dict, campaign: str | None) -> dict:
    """Лид по /start. Кампания пишется только при первом входе; повторный /start
    снимает blocked_at (раз пишет — значит разблокировал)."""
    row = db.execute(text("""
        INSERT INTO tg_pay_leads (chat_id, username, first_name, start_param)
        VALUES (:c, :un, :fn, :sp)
        ON CONFLICT (chat_id) DO UPDATE SET
            username = EXCLUDED.username, first_name = EXCLUDED.first_name,
            last_seen_at = now(), blocked_at = NULL
        RETURNING start_param, user_id, consent_marketing, (xmax = 0) AS is_new
    """), {"c": chat["id"], "un": chat.get("username"),
           "fn": (chat.get("first_name") or "")[:128] or None, "sp": campaign}).mappings().one()
    return dict(row)


def new_claim_token(db, chat_id: int) -> str:
    token = secrets.token_urlsafe(24)
    db.execute(text("INSERT INTO tg_pay_claims (token, chat_id, expires_at) VALUES (:t, :c, :e)"),
               {"t": token, "c": chat_id, "e": datetime.now(timezone.utc) + CLAIM_TTL})
    return token


def _lead(db, chat_id: int) -> dict | None:
    row = db.execute(text("SELECT * FROM tg_pay_leads WHERE chat_id = :c"),
                     {"c": chat_id}).mappings().first()
    return dict(row) if row else None


# ── обработчики ─────────────────────────────────────────────────────────────
def _pricing_keyboard(db, chat_id: int, campaign: str | None, consent: bool) -> list:
    kb = [[{"text": "💳 Выбрать тариф", "url": pricing_url(new_claim_token(db, chat_id), campaign)}]]
    if not consent:
        kb.append([{"text": "🔔 Присылать скидки и акции", "callback_data": CB_CONSENT}])
    return kb


def handle_start(chat: dict, arg: str | None) -> None:
    campaign = clean_start_param(arg)
    db = SessionLocal()
    try:
        lead = upsert_lead(db, chat, campaign)
        _event(db, chat["id"], "start", lead["user_id"], {"param": campaign} if campaign else None)
        kb = _pricing_keyboard(db, chat["id"], lead["start_param"], lead["consent_marketing"])
        db.commit()
    finally:
        db.close()
    send(chat["id"], WELCOME + ("" if lead["consent_marketing"] else "\n\n" + CONSENT_PROMPT), kb)
    if lead["is_new"] and ADMIN_IDS:
        who = f"@{chat['username']}" if chat.get("username") else chat.get("first_name") or chat["id"]
        for a in ADMIN_IDS:
            if a != chat["id"]:
                send(a, f"👤 Новый лид в боте оплаты: {who}, кампания {campaign or 'нет'}")


def handle_status(chat_id: int) -> None:
    db = SessionLocal()
    try:
        lead = _lead(db, chat_id)
        sub = None
        if lead and lead["user_id"]:
            sub = db.execute(text("""
                SELECT tier, period, expires_at, payment_method_id IS NOT NULL AS autorenew,
                       cancelled_at IS NOT NULL AS cancelled
                FROM subscriptions
                WHERE user_id = :u AND status = 'active'
                ORDER BY expires_at DESC NULLS LAST LIMIT 1
            """), {"u": lead["user_id"]}).mappings().first()
        if not lead or not lead["user_id"]:
            msg = ("Аккаунт Фрейма пока не привязан. Нажмите «Выбрать тариф» и войдите "
                   "на сайте, после этого бот узнает вашу подписку.")
        elif not sub:
            msg = "Активной подписки нет."
        else:
            renew = ("продлится автоматически" if sub["autorenew"] and not sub["cancelled"]
                     else "без автопродления")
            msg = (f"Подписка {TIER_NAMES.get(sub['tier'], sub['tier'])} до "
                   f"{fmt_date(sub['expires_at'])}, {renew}.")
        kb = _pricing_keyboard(db, chat_id, lead["start_param"] if lead else None, True) if lead else None
        db.commit()
    finally:
        db.close()
    send(chat_id, msg, kb)


def handle_stop(chat_id: int) -> None:
    db = SessionLocal()
    try:
        db.execute(text("UPDATE tg_pay_leads SET opted_out_at = now(), consent_marketing = false "
                        "WHERE chat_id = :c"), {"c": chat_id})
        _event(db, chat_id, "opt_out")
        db.commit()
    finally:
        db.close()
    send(chat_id, "Хорошо, скидки и напоминания больше не пришлю. "
                  "Сообщения об оплате и продлении останутся. Вернуть: /start.")


def handle_consent(cb: dict) -> None:
    chat_id = cb["message"]["chat"]["id"]
    db = SessionLocal()
    try:
        db.execute(text("UPDATE tg_pay_leads SET consent_marketing = true, consent_at = now(), "
                        "opted_out_at = NULL WHERE chat_id = :c"), {"c": chat_id})
        _event(db, chat_id, "consent")
        db.commit()
    finally:
        db.close()
    tg("answerCallbackQuery", {"callback_query_id": cb["id"], "text": "Готово, буду присылать"})
    # убрать кнопку согласия, оставить кнопку тарифов
    kb = [row for row in (cb["message"].get("reply_markup") or {}).get("inline_keyboard", [])
          if not any(b.get("callback_data") == CB_CONSENT for b in row)]
    tg("editMessageReplyMarkup", {"chat_id": chat_id, "message_id": cb["message"]["message_id"],
                                  "reply_markup": {"inline_keyboard": kb}})


def handle_stats(chat_id: int) -> None:
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT COALESCE(l.start_param, '—') AS camp,
                   count(*) AS starts,
                   count(*) FILTER (WHERE l.user_id IS NOT NULL) AS linked,
                   count(DISTINCT p.chat_id) AS paid
            FROM tg_pay_leads l
            LEFT JOIN tg_pay_events p ON p.chat_id = l.chat_id AND p.kind = 'paid'
            WHERE l.first_seen_at > now() - interval '30 days'
            GROUP BY 1 ORDER BY 2 DESC LIMIT 20
        """)).mappings().all()
    finally:
        db.close()
    if not rows:
        send(chat_id, "За 30 дней лидов нет.")
        return
    lines = ["<b>Воронка за 30 дней</b>", "кампания: старт → привязка → оплата"]
    lines += [f"{r['camp']}: {r['starts']} → {r['linked']} → {r['paid']}" for r in rows]
    send(chat_id, "\n".join(lines))


def process_update(update: dict) -> None:
    cb = update.get("callback_query")
    if cb:
        if cb.get("data") == CB_CONSENT and cb.get("message"):
            handle_consent(cb)
        else:
            tg("answerCallbackQuery", {"callback_query_id": cb["id"]})
        return
    msg = update.get("message")
    if not msg or msg["chat"].get("type") != "private":
        return
    chat = msg["chat"]
    parts = (msg.get("text") or "").strip().split(maxsplit=1)
    cmd = parts[0].split("@", 1)[0].lower() if parts else ""
    if cmd == "/start":
        handle_start(chat, parts[1] if len(parts) == 2 else None)
    elif cmd == "/status":
        handle_status(chat["id"])
    elif cmd == "/stop":
        handle_stop(chat["id"])
    elif cmd == "/stats" and chat["id"] in ADMIN_IDS:
        handle_stats(chat["id"])
    elif cmd == "/help":
        send(chat["id"], HELP)
    else:
        # Свободный текст: пересылаем админам как обращение (ответ вручную).
        for a in ADMIN_IDS:
            if a != chat["id"]:
                tg("forwardMessage", {"chat_id": a, "from_chat_id": chat["id"],
                                      "message_id": msg["message_id"]})
        send(chat["id"], "Спасибо, передал. Ответим здесь же.\n\n" + HELP)


# ── сообщения об оплате ─────────────────────────────────────────────────────
PAID_SQL = text("""
    SELECT l.chat_id, l.user_id, l.username, l.start_param, s.id AS sub_id, s.tier,
           s.period, s.expires_at, s.source,
           EXISTS (SELECT 1 FROM tg_pay_events e
                   WHERE e.chat_id = l.chat_id AND e.kind = 'paid') AS had_paid
    FROM tg_pay_leads l
    JOIN subscriptions s ON s.user_id = l.user_id
    WHERE l.user_id IS NOT NULL
      AND l.blocked_at IS NULL
      AND s.status = 'active'
      AND NOT s.is_trial
      AND s.created_at >= l.first_seen_at
      AND NOT EXISTS (SELECT 1 FROM tg_pay_events e
                      WHERE e.chat_id = l.chat_id AND e.kind = 'paid'
                        AND (e.meta->>'sub_id')::int = s.id)
    ORDER BY s.created_at
""")


def scan_paid() -> int:
    """Новые оплаченные подписки привязанных лидов → сообщение в бот. Подписки,
    созданные до первого /start, не трогаем: это старые подписчики."""
    db = SessionLocal()
    sent = 0
    try:
        for r in db.execute(PAID_SQL).mappings().all():
            ok = send(r["chat_id"], paid_text(r["tier"], r["period"], r["expires_at"],
                                              first=not r["had_paid"]))
            # Событие пишем и при неудаче: blocked уже помечен, повторять не надо.
            _event(db, r["chat_id"], "paid", r["user_id"],
                   {"sub_id": r["sub_id"], "delivered": ok, "source": r["source"]})
            db.commit()
            sent += ok
            if not r["had_paid"]:
                who = f"@{r['username']}" if r["username"] else r["chat_id"]
                for a in ADMIN_IDS:
                    send(a, f"💳 Оплата из бота: {who}, "
                            f"{TIER_NAMES.get(r['tier'], r['tier'])} "
                            f"{PERIOD_NAMES.get(r['period'], r['period'])}, "
                            f"кампания {r['start_param'] or 'нет'}")
        db.execute(text("DELETE FROM tg_pay_claims WHERE expires_at < now() - interval '7 days'"))
        db.commit()
    finally:
        db.close()
    return sent


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("PAY_BOT_TOKEN не задан")
    _log(f"started, admins={sorted(ADMIN_IDS)}")
    _setup_bot_profile()
    offset = None
    next_scan = 0.0
    while True:
        try:
            if time.monotonic() >= next_scan:
                next_scan = time.monotonic() + PAID_SCAN_EVERY_S
                try:
                    n = scan_paid()
                    if n:
                        _log(f"paid messages sent: {n}")
                except Exception as e:
                    _log(f"scan_paid error: {e}")
            # long-poll 20с (как alert_bot): через релей 30с упирается в таймауты.
            params = {"timeout": 20,
                      "allowed_updates": json.dumps(["message", "callback_query"])}
            if offset:
                params["offset"] = offset
            data = requests.get(f"{API_BASE}/getUpdates", params=params, timeout=30).json()
            if not data.get("ok"):
                time.sleep(5)
                continue
            for update in data["result"]:
                offset = update["update_id"] + 1
                try:
                    process_update(update)
                except Exception as e:
                    _log(f"update error: {e}")
        except (requests.RequestException, ValueError) as e:
            _log(f"polling error: {e}")
            time.sleep(5)
        except Exception as e:
            _log(f"loop error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
