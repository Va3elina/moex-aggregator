"""Подборка дня: самые репостнутые новости за 24 часа из уже отобранных хайп-ботом → FRAME · Редакция.

Источник только tg_channel_watch.promoted (то, что signals/tg_hype_scan.py уже пропустил в @news_framedata), новых
новостей подборка не ищет. Репосты в таблице замерены в первые 3-8 минут, к вечеру они другие, поэтому в 17:00
скрипт перечитывает текущие репосты через ту же MTProto-сессию. Не вышло (нет сессии, Telegram не ответил) — берёт
последний сохранённый замер и пишет об этом в подборке.

Каналы сравниваются не по абсолютным репостам: у markettwits их в десятки раз больше, чем у newssmartlab, и вторая
никогда не попала бы в топ. Счёт = репосты / медиана репостов отобранных новостей того же канала за эти сутки.

    python3 -m signals.hype_digest --dry-run    # напечатать подборку, ничего не слать
"""
import os

_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import html  # noqa: E402
import re  # noqa: E402
import statistics  # noqa: E402
from datetime import datetime, timedelta, timezone  # noqa: E402

import requests  # noqa: E402
from sqlalchemy import text  # noqa: E402

from signals.news_similarity import _DIGEST_RE  # noqa: E402

WINDOW_HOURS = 24
TOP_N = 5
TITLE_LEN = 160
MSK = timezone(timedelta(hours=3))
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SESSION_PATH = os.path.join(_ROOT, "signals", "mtp_session")   # та же, что у tg_hype_scan

_PROMOTED = text("""
    SELECT channel, message_id, posted_at, msg_text,
           GREATEST(fwd_3, fwd_5, fwd_6, fwd_7, fwd_8) AS fwd
    FROM tg_channel_watch
    WHERE promoted AND posted_at > :since AND msg_text <> ''
""")

_MD_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_MD_MARK = re.compile(r"\*\*|__|~~|`")


def title_of(msg_text: str) -> str:
    """Первая содержательная строка без markdown Telethon: посты часто начинаются со строки эмодзи-тегов."""
    clean = _MD_MARK.sub("", _MD_LINK.sub(r"\1", msg_text or ""))
    line = next((s.strip() for s in clean.split("\n") if len(s.strip()) >= 15), clean.strip())
    return line[:TITLE_LEN].rstrip() + ("…" if len(line) > TITLE_LEN else "")


def rank(rows: list) -> list:
    rows = [r for r in rows if not _DIGEST_RE.search(r["msg_text"] or "")]
    by_channel = {}
    for r in rows:
        by_channel.setdefault(r["channel"], []).append(r["fwd"] or 0)
    medians = {ch: max(statistics.median(v), 1) for ch, v in by_channel.items()}
    scored = [dict(r, score=(r["fwd"] or 0) / medians[r["channel"]]) for r in rows]
    scored.sort(key=lambda r: (-r["score"], -(r["fwd"] or 0)))
    return scored[:TOP_N]


def build_message(top: list, total: int, live: bool, now: datetime) -> str:
    day = now.astimezone(MSK).strftime("%d.%m")
    lines = [f"<b>Новости дня {day}</b>", f"Отобрано за 24 часа: {total}, топ по репостам."]
    if not live:
        lines.append("⚠️ Текущие репосты не прочитались, взят ранний замер (первые минуты после выхода).")
    lines.append("")
    if not top:
        lines.append("За сутки отобранных новостей нет.")
    for i, r in enumerate(top, 1):
        url = f"https://t.me/{r['channel']}/{r['message_id']}"
        when = r["posted_at"].astimezone(MSK).strftime("%H:%M")
        lines.append(f"{i}. <a href=\"{html.escape(url)}\">{html.escape(title_of(r['msg_text']))}</a>")
        lines.append(f"   {r['fwd'] or 0} репостов, ×{r['score']:.1f} к обычному, {r['channel']}, {when}")
    return "\n".join(lines)


def refresh_forwards(rows: list) -> bool:
    """Текущие репосты через MTProto. False — оставили сохранённые замеры."""
    api_id, api_hash = os.environ.get("MTP_API_ID", ""), os.environ.get("MTP_API_HASH", "")
    if not rows or not api_id or not api_hash:
        return False
    from telethon.sync import TelegramClient
    try:
        with TelegramClient(SESSION_PATH, int(api_id), api_hash, use_ipv6=True) as client:
            for channel in {r["channel"] for r in rows}:
                mine = [r for r in rows if r["channel"] == channel]
                msgs = client.get_messages(client.get_entity(channel), ids=[r["message_id"] for r in mine])
                for r, m in zip(mine, msgs):
                    if m is not None:
                        r["fwd"] = max(m.forwards or 0, r["fwd"] or 0)
        return True
    except Exception as e:
        print(f"[hype_digest] репосты не обновлены: {type(e).__name__}: {e}")
        return False


def send(body: str) -> None:
    from signals import config
    api_root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
    r = requests.post(f"{api_root}/bot{config.EDITOR_BOT_TOKEN}/sendMessage",
                      data={"chat_id": config.ADMIN_USER_ID, "text": body, "parse_mode": "HTML",
                            "disable_web_page_preview": True}, timeout=20)
    if not r.ok:
        print(f"[hype_digest] отправка: {r.status_code} {r.text[:200]}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="напечатать подборку, ничего не слать")
    ap.add_argument("--no-refresh", action="store_true", help="не ходить в Telegram, взять сохранённые замеры")
    args = ap.parse_args()

    from dotenv import load_dotenv
    load_dotenv(os.path.join(_ROOT, ".env"))
    from api.database import SessionLocal

    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        rows = [dict(r) for r in db.execute(_PROMOTED, {"since": now - timedelta(hours=WINDOW_HOURS)}).mappings()]
    finally:
        db.close()
    live = False if args.no_refresh else refresh_forwards(rows)
    body = build_message(rank(rows), len(rows), live, now)
    print(f"[hype_digest] отобрано {len(rows)}, репосты живые: {live}")
    if args.dry_run:
        print(body)
    else:
        send(body)


if __name__ == "__main__":
    main()
