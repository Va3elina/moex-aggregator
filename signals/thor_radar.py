"""Радар боли для постов @Thor_INV: что разогрелось в Пульсе и какие факты накопил автор.

Первый этап завода личных постов. Раз в день собирает ленту Пульса (публичное API, без логина), считает,
какие бумаги обсуждают и какие из них выросли к обычному, и шлёт сводку в личку ревьюерам @frameadminbot.
Вместе с ней идут факты из копилки: команда `/факт текст` в том же боте (signals/content_review_bot.py).
Завод берёт личные истории только оттуда, выдумывать сделки и бытовые детали нельзя.

Сравнение по числу авторов, а не комментариев: у свежих постов комментарии ещё не набрались, и база за неделю
всегда обгоняла бы окно 48 часов. Посты с пачкой тикеров в счёт бумаг не идут (иначе один пост МГКЛ на 9 выпусков
поднимает все соседние бумаги), официальные аккаунты тоже.

    python3 -m signals.thor_radar --dry-run    # напечатать сводку, ничего не слать
"""
import os

_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import html  # noqa: E402
import re  # noqa: E402
import time  # noqa: E402
from collections import defaultdict  # noqa: E402
from datetime import datetime, timedelta, timezone  # noqa: E402
from pathlib import Path  # noqa: E402

import requests  # noqa: E402
from sqlalchemy import text  # noqa: E402

FEED_URL = "https://www.tbank.ru/api/invest-gw/social/post/feed/v1/feed"
POST_URL = "https://www.tbank.ru/invest/social/profile/{nick}/{id}/"
# tbank.ru на корне Минцифры, в certifi его нет. Доверие только на этот запрос (политика api/ru_tls.py), бандла на хосте нет.
RU_ROOT_CA = Path(__file__).resolve().parent.parent / "certs" / "russian_trusted_root_ca.pem"
HOT_HOURS = 48
BASE_DAYS = 7                  # база сравнения: сколько дней до окна HOT_HOURS
MAX_PAGES = 80                 # по 30 постов, ~200 постов в сутки
PAGE_PAUSE_SEC = 0.2
MAX_TICKERS_IN_POST = 6        # больше — пост-пачка, в счёт бумаг не идёт
ASSET_TYPES = {"share", "bond"} # фьючерсы и фонды это фон обзоров, а не тема
MIN_AUTHORS = 5
MIN_HEAT = 1.5                 # во сколько раз авторов в сутки больше обычного
TOP_ASSETS = 8
TOP_POSTS = 6
FACT_DAYS = 3
FACTS_SHOWN = 10
OFFICIAL = {"T-Investments", "Pulse_Authors"}
TG_LIMIT = 4000

_ENSURE = text("""
    CREATE TABLE IF NOT EXISTS thor_facts (
        id          SERIAL PRIMARY KEY,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        chat_id     BIGINT NOT NULL,
        body        TEXT NOT NULL
    )
""")


def ensure_tables(db) -> None:
    db.execute(_ENSURE)
    db.commit()


def add_fact(db, chat_id: int, body: str) -> int:
    fid = db.execute(text("INSERT INTO thor_facts (chat_id, body) VALUES (:c, :b) RETURNING id"),
                     {"c": chat_id, "b": body}).scalar_one()
    db.commit()
    return fid


def recent_facts(db, days: int = FACT_DAYS, limit: int = FACTS_SHOWN) -> list:
    rows = db.execute(text("""
        SELECT id, created_at, body FROM thor_facts
        WHERE created_at >= NOW() - make_interval(days => :d)
        ORDER BY id DESC LIMIT :n
    """), {"d": days, "n": limit}).fetchall()
    return list(rows)


def asset_name(ins: dict) -> str:
    """Выпуск облигации сводим к эмитенту: «ГК Самолет БО-П14» и акция «ГК Самолет» это одна тема."""
    name = (ins.get("briefName") or ins.get("ticker") or "").strip()
    if ins.get("type") != "bond":
        return name
    words = []
    for w in name.split():
        if re.search(r"\d", w) or w.upper().startswith("БО"):
            break
        words.append(w)
    return " ".join(words) or name


def _nick(p: dict) -> str:
    return p.get("nickname") or (p.get("owner") or {}).get("nickname") or ""


def _when(p: dict) -> datetime:
    return datetime.fromisoformat(p["inserted"].replace("Z", "+00:00"))


def fetch_feed(now: datetime) -> list:
    floor = now - timedelta(hours=HOT_HOURS) - timedelta(days=BASE_DAYS)
    posts, cursor = [], None
    for _ in range(MAX_PAGES):
        params = {"limit": 30}
        if cursor:
            params["cursor"] = cursor
        r = requests.get(FEED_URL, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=20,
                         verify=str(RU_ROOT_CA))
        if r.status_code >= 500 and posts:      # в глубине ленты API отдаёт 500, база просто будет короче
            print(f"[thor_radar] лента оборвалась: {r.status_code}, постов {len(posts)}")
            break
        r.raise_for_status()
        payload = r.json()["payload"]
        items = payload["items"]
        posts.extend(p for p in items if _when(p) >= floor)
        cursor = payload.get("nextCursor")
        if not payload.get("hasNext") or not items or _when(items[-1]) < floor:
            break
        time.sleep(PAGE_PAUSE_SEC)
    return posts


def _is_heated(row: dict) -> bool:
    return row["heat"] is None or row["heat"] >= MIN_HEAT


def rank_assets(posts: list, now: datetime) -> list:
    """Самые обсуждаемые бумаги, выросшие к обычному наверху. База короче BASE_DAYS, если лента неглубокая."""
    hot_from = now - timedelta(hours=HOT_HOURS)
    oldest = min((_when(p) for p in posts), default=now)
    base_days = max((hot_from - oldest).total_seconds() / 86400, 0)
    hot, base, comments, day_move = defaultdict(set), defaultdict(set), defaultdict(int), {}
    for p in posts:
        nick = _nick(p)
        ins = (p.get("content") or {}).get("instruments") or []
        if nick in OFFICIAL or not ins or len(ins) > MAX_TICKERS_IN_POST:
            continue
        is_hot = _when(p) >= hot_from
        for name in {asset_name(i) for i in ins if i.get("type") in ASSET_TYPES}:
            (hot if is_hot else base)[name].add(nick)
            if is_hot:
                comments[name] += p.get("commentsCount") or 0
        for i in ins:
            if i.get("type") == "share" and i.get("relativeDailyYield") is not None:
                day_move[asset_name(i)] = i["relativeDailyYield"]
    rows = []
    for name, authors in hot.items():
        rate = len(authors) / (HOT_HOURS / 24)
        usual = len(base[name]) / base_days if base_days >= 1 else None
        heat = rate / usual if usual else None      # нет базы — бумага всплыла впервые
        if len(authors) < MIN_AUTHORS:
            continue
        rows.append({"name": name, "authors": len(authors), "comments": comments[name],
                     "heat": heat, "move": day_move.get(name)})
    # Жёсткий порог по росту оставлял пустой список: база всего 2-3 дня, в выходные она выше окна.
    # Поэтому рост только поднимает бумагу наверх, дальше порядок по комментариям.
    rows.sort(key=lambda r: (not _is_heated(r), -r["comments"]))
    return rows[:TOP_ASSETS]


def top_posts(posts: list, now: datetime) -> list:
    hot_from = now - timedelta(hours=HOT_HOURS)
    fresh = [p for p in posts if _when(p) >= hot_from and _nick(p) not in OFFICIAL
             and (p.get("content") or {}).get("text")]
    return sorted(fresh, key=lambda p: -(p.get("commentsCount") or 0))[:TOP_POSTS]


def _snippet(body: str, n: int = 90) -> str:
    s = re.sub(r"\{\$[^}]*\}|\*\*", "", body)
    s = " ".join(s.split())
    return s[:n].rstrip() + ("…" if len(s) > n else "")


def build_digest(assets: list, posts: list, facts: list) -> str:
    lines = ["<b>Радар для @Thor_INV</b>", "", "<b>Что обсуждают за 48 часов</b>"]
    if not assets:
        lines.append("Заметных тем нет.")
    for a in assets:
        heat = ""
        if _is_heated(a):
            heat = f"🔥 ×{a['heat']:.1f}, " if a["heat"] else "🔥 новая тема, "
        move = f", день {a['move']:+.1f}%" if a["move"] is not None else ""
        lines.append(f"• {html.escape(a['name'])}: {heat}авторов {a['authors']}, "
                     f"комментариев {a['comments']}{move}")
    lines += ["", "<b>Где спорят</b>"]
    for p in posts:
        url = POST_URL.format(nick=_nick(p), id=p["id"])
        lines.append(f"• {p.get('commentsCount') or 0} комм.: "
                     f"<a href=\"{html.escape(url)}\">{html.escape(_snippet(p['content']['text']))}</a>")
    lines += ["", f"<b>Твои факты за {FACT_DAYS} дня</b>"]
    if facts:
        lines += [f"• {html.escape(_snippet(f.body, 200))}" for f in facts]
    else:
        lines.append("Пусто. Запиши сделку или историю: /факт текст")
    out, size = [], 0
    for line in lines:                  # режем по строкам, чтобы не разорвать тег
        size += len(line) + 1
        if size > TG_LIMIT:
            break
        out.append(line)
    return "\n".join(out)


def send_digest(body: str) -> None:
    from signals import config
    api_root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
    for chat_id in {config.ADMIN_USER_ID, *config.CONTENT_DRAFT_EXTRA_CHAT_IDS}:
        r = requests.post(f"{api_root}/bot{config.BOT_TOKEN}/sendMessage",
                          data={"chat_id": chat_id, "text": body, "parse_mode": "HTML",
                                "disable_web_page_preview": True}, timeout=20)
        if not r.ok:
            print(f"[thor_radar] отправка {chat_id}: {r.status_code}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="напечатать сводку, ничего не слать")
    args = ap.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    from api.database import SessionLocal

    now = datetime.now(timezone.utc)
    posts = fetch_feed(now)
    db = SessionLocal()
    try:
        ensure_tables(db)
        facts = recent_facts(db)
    finally:
        db.close()
    body = build_digest(rank_assets(posts, now), top_posts(posts, now), facts)
    print(f"[thor_radar] постов {len(posts)}, знаков {len(body)}")
    if args.dry_run:
        print(body)
    else:
        send_digest(body)


if __name__ == "__main__":
    main()
