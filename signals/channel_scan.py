"""
Ридер постов публичных Telegram-каналов → таблица channel_posts (секция «Новости
каналов» в колоколе сайта). Парсит t.me/s/<channel> (web-превью; достижимо с прода
НАПРЯМУЮ — РКН блокирует только api.telegram.org, бот идёт через релей). Host-cron,
как anomaly_scan:
  /opt/frame/signals/channel_scan.sh   (cron, напр. */30 * * * *)

Апсертит последние посты каждого канала (ON CONFLICT channel+post_id). Ничего не
шлёт; фронт тянет их в /api/anomalies/feed (поле channel_posts).

Текст — ЦЕЛИКОМ (28.09.2026): раньше резался до 600 знаков, и хэштег рубрики в конце поста
(#открытыепозиции, #деньгивфондах, #cделкифондов) до базы не доходил — завод постов по нему
ищет повторы (insight_scan.repeat_of) и берёт примеры голоса (content_ai), и 23–24.09 фильтр
повторов ошибался. Сниппет для колокола режет API (anomalies.py, left(text, 600)) — сайт
видит то же, что раньше. Дозалить старые посты: channel_scan.sh --backfill
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
BACKFILL_PAGES = 30          # страниц t.me/s по ~20 постов при дозаливе (--backfill)
HTTP_TIMEOUT = 20
_UA = "Mozilla/5.0 (compatible; FrameBot/1.0; +https://framedata.ru)"

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

# ── Тихий режим при сетевой недоступности t.me (добавлено 2026-08-11) ──
# 11.08 Telegram целиком перестал открываться с прода на уровне L4 (см. память
# telegram_egress_block): t.me тоже, хотя раньше был достижим напрямую в обход
# релея — на это прямо рассчитывал докстринг выше. Крон раз в 5 минут писал по
# две одинаковые строки на прогон, лог рос ~0.5 МБ/сутки чистым шумом.
#
# Подавляем ТОЛЬКО связность (ConnectionError/Timeout): HTTP 4xx/5xx, ошибки
# парсинга и БД по-прежнему шумят каждый прогон — они означают, что сломались
# МЫ или разметка t.me, и это надо видеть сразу.
#
# Состояние живёт в файле, потому что каждый прогон — отдельный процесс: пишем
# одну строку при ПЕРЕХОДЕ в офлайн и одну при восстановлении (с длительностью
# простоя), между ними — полная тишина, включая итоговую строку прогона.
_OFFLINE_FLAG = os.path.join(_ROOT, "logs", ".channel_scan_offline")


def _offline_since():
    """Момент ухода в офлайн, либо None если сейчас считаемся живыми."""
    try:
        with open(_OFFLINE_FLAG) as f:
            return datetime.fromisoformat(f.read().strip())
    except (OSError, ValueError):
        return None


def _mark_offline(now):
    try:
        os.makedirs(os.path.dirname(_OFFLINE_FLAG), exist_ok=True)
        with open(_OFFLINE_FLAG, "w") as f:
            f.write(now.isoformat())
    except OSError:
        pass          # не смогли записать флаг — переживём, просто пошумим ещё


def _clear_offline():
    try:
        os.remove(_OFFLINE_FLAG)
    except OSError:
        pass


def _human(delta) -> str:
    mins = int(delta.total_seconds() // 60)
    if mins < 60:
        return f"{mins} мин"
    h, m = divmod(mins, 60)
    return f"{h} ч {m} мин" if h < 24 else f"{h // 24} сут {h % 24} ч"


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


def _parse_posts(page: str, channel: str, limit: int | None = MAX_POSTS_PER_CHANNEL) -> list:
    """Последние посты канала из HTML t.me/s. Разбиваем по data-post-якорям —
    первый text/date/photo в чанке принадлежит этому посту. Текст — целиком."""
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
            "post_id": post_id, "text": textval, "photo_url": photo,
            "link": f"https://t.me/{channel}/{post_id}", "posted_at": posted_at,
        })
    posts.sort(key=lambda p: p["post_id"])      # post_id монотонно растёт
    return posts[-limit:] if limit else posts


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
    net_failures = 0            # из них — именно недоступность сети
    net_reason = ""
    for username, fallback in CHANNELS:
        summary["channels"] += 1
        try:
            r = requests.get(f"https://t.me/s/{username}",
                             headers={"User-Agent": _UA}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            title = _channel_title(r.text, fallback)
            for p in _parse_posts(r.text, username):
                rows.append(dict(p, channel=username, channel_name=title))
        except (requests.ConnectionError, requests.Timeout) as e:
            # Связность: не шумим на каждый прогон — решение ниже, после цикла.
            summary["errors"] += 1
            net_failures += 1
            net_reason = type(e).__name__
        except Exception as e:
            summary["errors"] += 1
            print(f"[channel_scan] {username} failed: {type(e).__name__}: {e}")

    # Офлайн = ВСЕ каналы отпали именно по сети. Частичный сбой (один канал из
    # нескольких) молчанием не заметаем — он попадёт в errors и в строку прогона.
    now = datetime.now(timezone.utc)
    was_offline = _offline_since()
    all_net_down = net_failures and net_failures == summary["channels"]
    if all_net_down:
        if was_offline is None:
            print(f"[channel_scan] t.me недоступен ({net_reason}) — ухожу в тихий "
                  f"режим, однотипные ошибки связности подавляю до восстановления")
            _mark_offline(now)
        summary["quiet"] = True       # main() не печатает итог, пока молчим
        return summary
    if was_offline is not None:
        print(f"[channel_scan] t.me снова доступен — молчал {_human(now - was_offline)}")
        _clear_offline()

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


# Дозалив полного текста уже лежащих постов: только text и только если в базе он короче (обрезан до 600
# знаков старым ридером); post_id, posted_at, ссылка и фото не трогаются, новых строк не создаёт.
_FILL_TEXT = text("""
  UPDATE channel_posts SET text = CAST(:text AS text)
   WHERE channel = :channel AND post_id = :post_id
     AND coalesce(length(text), 0) < length(CAST(:text AS text))
""")


def backfill(channel: str = "FrameTool", pages: int = BACKFILL_PAGES, get=requests.get, sleep=None) -> dict:
    """Листает t.me/s/<channel>?before=N от свежих постов к старым до самого старого поста в базе и
    дописывает полный текст там, где он был обрезан."""
    import time
    sleep = sleep or time.sleep
    db = SessionLocal()
    summary = {"pages": 0, "seen": 0, "filled": 0}
    try:
        oldest = db.execute(text("SELECT min(post_id) FROM channel_posts WHERE channel = :c"),
                            {"c": channel}).scalar()
        if oldest is None:
            return summary
        before = None
        for _ in range(pages):
            url = f"https://t.me/s/{channel}" + (f"?before={before}" if before else "")
            r = get(url, headers={"User-Agent": _UA}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            posts = _parse_posts(r.text, channel, limit=None)
            summary["pages"] += 1
            if not posts:
                break
            for p in posts:
                if p["text"]:
                    summary["seen"] += 1
                    summary["filled"] += db.execute(_FILL_TEXT, {"text": p["text"], "channel": channel,
                                                                "post_id": p["post_id"]}).rowcount or 0
            db.commit()
            low = min(p["post_id"] for p in posts)
            if low <= oldest or (before is not None and low >= before):
                break
            before = low
            sleep(1)
    finally:
        db.close()
    return summary


def main():
    import sys
    if "--backfill" in sys.argv:
        print(f"[{datetime.now(timezone.utc)}] channel_scan backfill: {backfill()}")
        return
    s = run_once()
    if s.pop("quiet", False):
        return          # тихий режим: переход в офлайн уже залогирован один раз
    print(f"[{datetime.now(timezone.utc)}] channel_scan: {s}")


if __name__ == "__main__":
    main()
