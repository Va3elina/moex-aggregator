"""
Ридер постов публичных Telegram-каналов → таблица channel_posts (секция «Новости
каналов» в колоколе сайта). Парсит t.me/s/<channel> (web-превью; достижимо с прода
НАПРЯМУЮ — РКН блокирует только api.telegram.org, бот идёт через релей). Host-cron,
как anomaly_scan:
  /opt/frame/signals/channel_scan.sh   (cron, напр. */30 * * * *)

Апсертит последние посты каждого канала (ON CONFLICT channel+post_id). Ничего не
шлёт; фронт тянет их в /api/anomalies/feed (поле channel_posts).
"""
import os
import re
import json
import html as _html
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from sqlalchemy import text

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from api.database import SessionLocal  # noqa: E402

# (username, запасное имя). Реальное имя парсим со страницы; это fallback.
CHANNELS = [
    ("FrameTool", "Фрейм"),
    # Thor_INV удалён 2026-07-03 по решению Вадима — не отслеживаем;
    # посты вычищены из channel_posts. НЕ возвращать без явной просьбы.
]
MAX_POSTS_PER_CHANNEL = 12
HTTP_TIMEOUT = 20
_UA = "Mozilla/5.0 (compatible; FrameBot/1.0; +https://xn--80aklbnczmv.xn--p1ai)"

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _clean_text(raw: str) -> str:
    """HTML-фрагмент поста → плоский текст: <br>→пробел, срезаем теги, раскрываем
    сущности, схлопываем пробелы."""
    t = raw.replace("<br/>", " ").replace("<br>", " ")
    t = _TAG_RE.sub(" ", t)
    t = _html.unescape(t)
    return _WS_RE.sub(" ", t).strip()


def _channel_title(page: str, fallback: str) -> str:
    m = re.search(r'tgme_channel_info_header_title"[^>]*>\s*<[^>]*>(.*?)</', page, re.S)
    if not m:
        m = re.search(r'<meta property="og:title" content="([^"]+)"', page)
    return _clean_text(m.group(1)) if m else fallback


def _parse_posts(page: str, channel: str) -> list:
    """Последние посты канала из HTML t.me/s. Разбиваем по data-post-якорям —
    первый text/date/photo в чанке принадлежит этому посту."""
    posts = []
    for chunk in page.split('data-post="')[1:]:
        m = re.match(r'([^/"]+)/(\d+)"', chunk)
        if not m or m.group(1) != channel:
            continue
        post_id = int(m.group(2))
        tm = re.search(r'tgme_widget_message_text[^>]*>(.*?)</div>', chunk, re.S)
        textval = _clean_text(tm.group(1)) if tm else ""
        posted_at = None
        dm = re.search(r'<time[^>]*datetime="([^"]+)"', chunk)
        if dm:
            try:
                posted_at = datetime.fromisoformat(dm.group(1))
            except ValueError:
                posted_at = None
        # Берём фон ИМЕННО у photo_wrap (собственное hi-res фото поста), а не
        # первый попавшийся background-image: у постов-комментариев первым идёт
        # tgme_widget_message_reply_thumb — крошечная мыльная превьюшка цитируемого
        # поста. Нет своего photo_wrap → photo=None (рисуем текст без картинки),
        # а не блёрнутый thumb.
        pm = re.search(
            r"tgme_widget_message_photo_wrap[^>]*?background-image:url\('([^']+)'\)",
            chunk)
        photo = pm.group(1) if pm else None
        if not textval and not photo:
            continue   # сервисное/пустое сообщение
        posts.append({
            "post_id": post_id, "text": textval[:600], "photo_url": photo,
            "link": f"https://t.me/{channel}/{post_id}", "posted_at": posted_at,
        })
    posts.sort(key=lambda p: p["post_id"])      # post_id монотонно растёт
    return posts[-MAX_POSTS_PER_CHANNEL:]


_UPSERT = text("""
  INSERT INTO channel_posts (channel, channel_name, post_id, text, photo_url, link, posted_at)
  VALUES (:channel, :channel_name, :post_id, :text, :photo_url, :link, :posted_at)
  ON CONFLICT (channel, post_id) DO UPDATE
    SET text = EXCLUDED.text, photo_url = EXCLUDED.photo_url,
        channel_name = EXCLUDED.channel_name
  RETURNING (xmax = 0) AS inserted
""")


def run_once() -> dict:
    summary = {"channels": 0, "posts": 0, "errors": 0}
    rows = []
    for username, fallback in CHANNELS:
        summary["channels"] += 1
        try:
            r = requests.get(f"https://t.me/s/{username}",
                             headers={"User-Agent": _UA}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            title = _channel_title(r.text, fallback)
            for p in _parse_posts(r.text, username):
                rows.append(dict(p, channel=username, channel_name=title))
        except Exception as e:
            summary["errors"] += 1
            print(f"[channel_scan] {username} failed: {type(e).__name__}: {e}")

    if not rows:
        return summary
    db = SessionLocal()
    try:
        new_posts = 0
        for row in rows:
            res = db.execute(_UPSERT, row).fetchone()
            if res and res[0]:        # xmax=0 → строка ВСТАВЛЕНА (новый пост), не апдейт
                new_posts += 1
            summary["posts"] += 1
        # Новый пост → SSE-нудж (source:'anomaly'): фронт сразу освежит ленту
        # колокола/новостей, не дожидаясь 90с-поллинга. notify_listener форвардит
        # канал 'anomaly' в sse_manager (как anomaly_scan).
        if new_posts:
            db.execute(text("SELECT pg_notify('anomaly', :p)"),
                       {"p": json.dumps({"source": "anomaly", "kind": "channel_post"})})
            summary["new"] = new_posts
        db.commit()
    except Exception as e:
        db.rollback()
        summary["errors"] += 1
        print(f"[channel_scan] db error: {e}")
    finally:
        db.close()
    return summary


def main():
    s = run_once()
    print(f"[{datetime.now(timezone.utc)}] channel_scan: {s}")


if __name__ == "__main__":
    main()
