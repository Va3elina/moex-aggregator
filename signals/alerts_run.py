"""
Alert eval-loop (Phase 3) — один проход: проверяет активные алерты, шлёт пуши.
Запуск по cron на ХОСТЕ (Telegram через IPv6). Каждые ~N минут:
  /opt/frame/signals/alerts_run.sh   (cron */10 * * * *)

Для каждого active-алерта считает текущее значение (цена фьючерса = последняя
свеча непрерывного контракта; OI z-score = compute_oi_z), сравнивает с условием
(с учётом last_value для cross), при срабатывании шлёт пуш в user.telegram_chat_id,
пишет alert_fires, обновляет last_fired_at / last_value; mode='once' → status='fired'.

MVP: метрики 'price' (фьючерсы) + 'oi_zscore'. Когда 🔔 появится на акциях/индексах —
добавить ветки цены (candles по sec_id / index_data) в compute_value.
"""
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

# ── .env + DB_URL override ДО импорта api.database (host-side, как alert_bot) ──
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from api.database import SessionLocal               # noqa: E402
from api.models import User, Alert, AlertFire        # noqa: E402
from signals.db import get_latest_price              # noqa: E402
from signals.detectors.oi import compute_oi_z        # noqa: E402
from signals.alert_notify import send_message        # noqa: E402

SITE = "https://xn--80aklbnczmv.xn--p1ai"  # punycode таймфрейм.рф (надёжно в TG)

_OP_PRICE = {"cross_up": "↑ пересекла", "cross_down": "↓ пересекла",
             "gt": "выше", "lt": "ниже"}


def eval_op(op: str, value: float, prev, threshold: float) -> bool:
    if op == "gt":
        return value > threshold
    if op == "lt":
        return value < threshold
    if op == "cross_up":
        return prev is not None and prev < threshold <= value
    if op == "cross_down":
        return prev is not None and prev > threshold >= value
    return False


def compute_value(a: Alert):
    """(value, ctx) или (None, None). ctx — доп. данные для текста уведомления."""
    if a.indicator == "price":
        # Самая свежая цена (intraday по 5м/60м, EOD по дневной у неликвидных).
        r = get_latest_price(a.asset)
        if not r:
            return None, None
        close, ts, interval = r
        return close, {"interval": interval, "price_ts": ts}
    if a.indicator == "oi_zscore":
        r = compute_oi_z(a.asset, a.clgroup or "FIZ")
        if not r:
            return None, None
        z, last_diff, current_net = r
        return float(z), {"last_diff": last_diff, "current_net": current_net}
    return None, None


def format_msg(a: Alert, value: float, ctx: dict) -> str:
    name = a.asset_name or a.asset
    link = f'<a href="{SITE}/oi?instrument={a.asset}">открыть график →</a>'
    thr = float(a.threshold)
    if a.indicator == "price":
        word = _OP_PRICE.get(a.op, a.op)
        eod = "\n<i>(дневная свеча — у актива нет внутридневных данных)</i>" \
            if ctx.get("interval") == 24 else ""
        return (f"🔔 <b>{name} · {a.asset}</b>\n"
                f"Цена {word} {thr:g} ₽ → сейчас {value:g} ₽{eod}\n{link}")
    if a.indicator == "oi_zscore":
        clg = "физлица" if (a.clgroup or "FIZ") == "FIZ" else "юрлица"
        diff = ctx.get("last_diff", 0)
        arrow = "↑" if diff > 0 else "↓"
        return (f"🔔 <b>{name} · {a.asset}</b> — Открытый интерес\n"
                f"Аномалия позиций ({clg}): z = {value:+g}σ (порог {thr:g})\n"
                f"Δ чистой позиции за день: {diff:+,} контрактов {arrow}\n{link}")
    return f"🔔 {name}: алерт сработал"


def run_once() -> dict:
    summary = {"checked": 0, "fired": 0, "skipped": 0, "unlinked": 0, "errors": 0}
    db = SessionLocal()
    try:
        alerts = db.query(Alert).filter(Alert.status == "active").all()
        now = datetime.now(timezone.utc)
        for a in alerts:
            summary["checked"] += 1
            try:
                user = db.query(User).filter(User.id == a.user_id).first()
                if not user or not user.telegram_chat_id:
                    summary["skipped"] += 1   # не привязан Telegram — доставить некуда
                    continue
                value, ctx = compute_value(a)
                if value is None:
                    summary["skipped"] += 1
                    continue
                prev = float(a.last_value) if a.last_value is not None else None
                if eval_op(a.op, value, prev, float(a.threshold)):
                    text = format_msg(a, value, ctx or {})
                    result = send_message(user.telegram_chat_id, text)
                    if result == "ok":
                        db.add(AlertFire(alert_id=a.id, value=value, message_text=text))
                        a.last_fired_at = now
                        if a.mode == "once":
                            a.status = "fired"
                        summary["fired"] += 1
                    elif result == "blocked":
                        # юзер забанил бота / чат мёртв → авто-отвязка, чтобы не
                        # долбить впустую. Алерт остаётся, но без chat_id eval его
                        # скипнет до повторной привязки на сайте.
                        user.telegram_chat_id = None
                        user.telegram_username = None
                        user.telegram_linked_at = None
                        summary["unlinked"] += 1
                        print(f"[alerts_run] user {user.id} blocked bot → auto-unlinked")
                    # 'error' → не помечаем сработавшим, повтор на следующем цикле
                a.last_value = value
            except Exception as e:
                summary["errors"] += 1
                print(f"[alerts_run] alert {a.id} error: {e}")
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[alerts_run] fatal: {e}")
    finally:
        db.close()
    return summary


def main():
    s = run_once()
    print(f"[{datetime.now(timezone.utc)}] alerts_run: {s}")


if __name__ == "__main__":
    main()
