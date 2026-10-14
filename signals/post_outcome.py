"""Итоги постов канала → @framesignalbot (личка админов): что сделала цена актива после публикации.

Посты канала уже лежат в channel_posts (signals/channel_scan.py, крон каждые 5 минут, парсер t.me/s/FrameTool),
отдельный поллер channel_post боту не нужен. Прогнозы в постах неявные («рекордный шорт нефти, посмотрим, как
сработает корреляция»), а LLM с хоста недоступен (api.anthropic.com отдаёт 403 с российских IP, signals/content_ai.py),
поэтому извлечения тезиса нет: трекер находит в тексте активы по словарю имён (акции из instruments + ручные
синонимы + макро: индексы, валюты, сырьё) и через неделю, месяц и 3 месяца присылает движение цены каждого с даты
поста. Сбылось или нет, решает человек по тексту.

Цены: акции — дневные свечи candles по secid (sec_id у истории до 2025-03 пустой), остальное — index_data (IMOEX,
RTSI, RGBI, USD000UTSTOM, CNYRUB_TOM, BRENT, GOLD…). База — последнее закрытие не позже дня поста (МСК), итог —
последнее закрытие не позже дня поста + горизонт.

Крон раз в день после открытия (`20 7 * * *`, 10:20 МСК); отправленное помнит STATE_DIR/post_outcome.sent.json
(пост|горизонт). Посты раньше FLOOR не трогаем, чтобы первый запуск не выслал весь архив.

    python3 -m signals.post_outcome --dry-run            # что ушло бы сегодня
    python3 -m signals.post_outcome --report 40          # таблица по последним 40 постам: активы и ходы
    python3 -m signals.post_outcome --since 2026-09-01   # сдвинуть нижнюю границу постов
"""
import os

_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import html  # noqa: E402
import json  # noqa: E402
import re  # noqa: E402
import time  # noqa: E402
from datetime import date, datetime, timedelta, timezone  # noqa: E402

from sqlalchemy import text  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from signals.framesignal import send  # noqa: E402
from signals.insights import fresh  # noqa: E402

NAME = "post_outcome"
CHANNEL = "FrameTool"
SENT_FILE = os.path.join(fresh.STATE_DIR, f"{NAME}.sent.json")
FLOOR = date(2026, 10, 1)
MSK = timezone(timedelta(hours=3))
HORIZONS = ((7, "Неделя"), (30, "Месяц"), (91, "3 месяца"))
MAX_ASSETS = 4                 # в сообщении — не больше стольких активов, в порядке появления в тексте
STALE_DAYS = 5                 # итог не старше стольких дней от даты горизонта, иначе ждём данные

# Макро-активы: regex по тексту (после вырезания имён акций — «Газпром нефть» не должен давать нефть) → ряд
# index_data. Порядок важен только для чтения.
MACRO = (
    (r"индекс\w*\s+мосбирж|imoex|российск\w+\s+(?:фондов\w+\s+)?рын\w+|рынок\s+акций|рынк\w*\s+рф", "IMOEX",
     "Индекс МосБиржи"),
    (r"(?<![а-яё])ртс(?![а-яё])|rtsi|в\s+долларах", "RTSI", "Индекс РТС"),
    (r"облигац|(?<![а-яё])офз|rgbi|рын\w*\s+долг|долгов\w+\s+рын", "RGBI", "Индекс RGBI"),
    (r"доллар(?!ах)|валют|usd", "USD000UTSTOM", "Доллар"),   # «в долларах» — это РТС
    (r"(?<![а-яё])юан|cny", "CNYRUB_TOM", "Юань"),
    (r"(?<![а-яё])евро(?![а-яё])", "EUR_RUB__TOM", "Евро"),
    (r"(?<![а-яё])нефт|brent|бочк", "BRENT", "Нефть Brent"),
    (r"золот", "GOLD", "Золото"),
    (r"серебр", "SILVER", "Серебро"),
    (r"платин", "PLATINUM", "Платина"),
    (r"паллад", "PALLADIUM", "Палладий"),
    (r"(?<![а-яё])мед[ьи](?![а-яё])", "COPPER", "Медь"),
    (r"природн\w+\s+газ|(?<![а-яё])газ[а-яё]{0,2}(?![а-яё])", "NATGAS_HH", "Газ"),
    (r"пшениц", "WHEAT", "Пшеница"),
)
# Ручные синонимы акций: разговорные имена, которых нет в instruments. Проверяются раньше автословаря.
ALIASES = (
    (r"сбер\w*", "SBER", "Сбербанк"),
    (r"яндекс\w*", "YDEX", "Яндекс"),
    (r"норникел\w*|норильск\w+\s+никел\w*", "GMKN", "Норильский никель"),
    (r"хэдхантер\w*|хедхантер\w*|headhunter", "HEAD", "HeadHunter"),
    (r"т-технолог\w*|тинькофф\w*|т-банк\w*|тбанк\w*", "T", "Т-Технологии"),
    (r"икс\s*5|x5|пятёрочк\w*|пятерочк\w*", "X5", "X5 Group"),
    (r"сургут\w*", "SNGS", "Сургутнефтегаз"),
    (r"московск\w+\s+бирж\w*|акци\w+\s+мосбирж\w*", "MOEX", "Московская биржа"),
    (r"газпром\s+нефт\w*|газпромнефт\w*", "SIBN", "Газпром нефть"),
)
# Акции instruments, которые автословарю брать нельзя: имя совпадает с обычным словом или с другим активом
SKIP_AUTO = {"MOEX", "SBERP", "SNGSP", "TATNP", "TRNFP", "X5", "T"}


def _word_pattern(name: str) -> str | None:
    """Regex по имени акции из instruments: «Магнит» ловит «Магнита», «Магните»; короткие имена (ПИК, ВТБ) — только
    как есть, иначе «пик» в «исторический пик» станет застройщиком."""
    name = norm(re.sub(r"\s*\((прив|вечн|мини)\)\s*", "", name)).strip()
    if not name or name.lower().startswith("i"):
        return None
    words = []
    for w in name.split():
        if re.fullmatch(r"[а-яёА-ЯЁ]+", w):
            if len(w) <= 4:
                words.append(re.escape(w))
                continue
            stem = w[:-1] if w[-1].lower() in "аеёиоуыэюя" else w
            words.append(re.escape(stem) + r"[а-яё]{0,3}")
        else:
            words.append(re.escape(w))
    return r"(?<![а-яёa-z0-9])" + r"\s+".join(words) + r"(?![а-яёa-z0-9])"


def _dictionary(db) -> list:
    """[(compiled regex, ключ ряда, подпись)] — ручные синонимы, акции instruments (длинные имена раньше коротких,
    чтобы «Газпром нефть» съедалась до «Газпром»), затем макро. Ключ: ('stock', secid) или ('index', secid)."""
    out = [(re.compile(norm(p), re.I), ("stock", sec), label) for p, sec, label in ALIASES]
    rows = db.execute(text("""SELECT name, sec_id FROM instruments
                               WHERE type = 'stock' AND NOT coalesce(hidden, false) AND sec_id IS NOT NULL""")).fetchall()
    auto = []
    for name, sec in rows:
        if sec in SKIP_AUTO or "(" in name:
            continue
        p = _word_pattern(name)
        if p:
            flags = re.I if len(name) > 4 else 0
            auto.append((len(name), re.compile(p, flags), ("stock", sec), name))
    out += [(rx, key, label) for _, rx, key, label in sorted(auto, key=lambda a: -a[0])]
    out += [(re.compile(norm(p), re.I), ("index", sec), label) for p, sec, label in MACRO]
    return out


def norm(s: str) -> str:
    """ё → е: в instruments «Самолет», в посте «Самолёт»."""
    return s.replace("ё", "е").replace("Ё", "Е")


def find_assets(txt: str, dictionary: list) -> list:
    """Активы поста в порядке первого упоминания; найденное имя вырезается из текста, чтобы не ловить его частями."""
    found = {}
    txt = norm(txt)
    for rx, key, label in dictionary:
        m = rx.search(txt)
        if not m:
            continue
        if key not in found or m.start() < found[key][0]:
            found[key] = (m.start(), label)
        if key[0] == "stock":
            txt = rx.sub(" ", txt)
    return [(k, v[1]) for k, v in sorted(found.items(), key=lambda kv: kv[1][0])][:MAX_ASSETS]


def headline(txt: str) -> str:
    first = txt.strip().split("\n", 1)[0]
    first = first.split("◽️", 1)[0]
    first = re.sub(r"[\U0001F000-\U0001FAFF☀-➿️]+", " ", first).strip(" ·-")
    if len(first) > 90:
        first = first[:90].rsplit(" ", 1)[0] + "…"
    return first or "Пост без заголовка"


# ── цены ──────────────────────────────────────────────────────────────────────────
_SQL = {
    "stock": """SELECT secid, CAST(begin_time AS date) AS d, close FROM candles
                WHERE interval = 24 AND type = 'stock' AND close > 0 AND secid IN :ids AND begin_time >= :since""",
    "index": """SELECT secid, trade_date AS d, close FROM index_data
                WHERE close > 0 AND secid IN :ids AND trade_date >= :since""",
}


def load_prices(db, keys: set, since: date) -> dict:
    """{(тип, secid): [(день, close)] по возрастанию}."""
    out = {}
    for kind in ("stock", "index"):
        ids = tuple(sorted({s for k, s in keys if k == kind}))
        if not ids:
            continue
        # secid — только буквы и цифры из instruments/index_data, подстановка в текст безопасна
        q = text(_SQL[kind].replace(":ids", "(" + ",".join(f"'{i}'" for i in ids) + ")"))
        for sec, d, close in db.execute(q, {"since": since - timedelta(days=10)}).fetchall():
            out.setdefault((kind, sec), []).append((d, float(close)))
    for v in out.values():
        v.sort()
    return out


def close_on(series: list, day: date):
    """Последнее закрытие не позже дня → (день, цена) или None."""
    best = None
    for d, c in series:
        if d > day:
            break
        best = (d, c)
    return best


def outcome(series: list, post_day: date, horizon: int, today: date):
    """Ход цены от дня поста до горизонта; None — рано или нет данных."""
    end = post_day + timedelta(days=horizon)
    if end > today:
        return None
    base, cur = close_on(series, post_day), close_on(series, end)
    if not base or not cur or cur[0] <= base[0] or (end - cur[0]).days > STALE_DAYS:
        return None
    return {"base": base[1], "cur": cur[1], "pct": cur[1] / base[1] * 100 - 100, "from": base[0], "to": cur[0]}


# ── сообщение ─────────────────────────────────────────────────────────────────────
def fmt_num(x: float) -> str:
    digits = 0 if x >= 1000 else 1 if x >= 100 else 2
    s = f"{x:,.{digits}f}".replace(",", " ").replace(".", ",")
    return s


def fmt_pct(p: float) -> str:
    sign = "+" if p > 0 else "−" if p < 0 else ""
    return f"{sign}{abs(p):.1f}".replace(".", ",") + "%"


def message(post: dict, label: str, lines: list) -> str:
    body = "\n".join(f"{html.escape(name)}: <b>{fmt_pct(o['pct'])}</b> ({fmt_num(o['base'])} → {fmt_num(o['cur'])})"
                     for name, o in lines)
    return (f"📬 <b>{label}</b> после поста от {post['day']:%d.%m}\n"
            f"<i>{html.escape(headline(post['text']))}</i>\n{body}\n"
            f'<a href="{html.escape(post["link"])}">Пост →</a>')


# ── данные ────────────────────────────────────────────────────────────────────────
def load_posts(db, since: date) -> list:
    rows = db.execute(text("""SELECT post_id, posted_at, text, link FROM channel_posts
                               WHERE channel = :ch AND posted_at >= :since AND coalesce(text, '') <> ''
                               ORDER BY posted_at"""), {"ch": CHANNEL, "since": since}).fetchall()
    return [{"id": pid, "day": at.astimezone(MSK).date(), "text": t, "link": link} for pid, at, t, link in rows]


def _load_sent() -> dict:
    try:
        with open(SENT_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}


def _save_sent(sent: dict) -> None:
    os.makedirs(fresh.STATE_DIR, exist_ok=True)
    with open(SENT_FILE, "w", encoding="utf-8") as fh:
        json.dump(dict(sorted(sent.items())), fh, ensure_ascii=False)


def collect(db, since: date, today: date) -> list:
    """[(пост, горизонт, подпись, [(имя, итог)])] — всё, что можно посчитать на сегодня."""
    posts = load_posts(db, since)
    dic = _dictionary(db)
    for p in posts:
        p["assets"] = find_assets(p["text"], dic)
    keys = {k for p in posts for k, _ in p["assets"]}
    prices = load_prices(db, keys, since)
    out = []
    for p in posts:
        if not p["assets"]:
            continue
        for h, label in HORIZONS:
            lines = [(name, o) for key, name in p["assets"]
                     if (o := outcome(prices.get(key, []), p["day"], h, today))]
            if lines:
                out.append((p, h, label, lines))
    return out


def run_once(since: date, dry_run: bool) -> dict:
    today = datetime.now(MSK).date()
    db = SessionLocal()
    try:
        items = collect(db, since, today)
    finally:
        db.close()
    sent = _load_sent()
    new = [it for it in items if f"{it[0]['id']}|{it[1]}" not in sent]
    summary = {"posts": len({it[0]["id"] for it in items}), "ready": len(items), "new": len(new), "sent": 0,
               "failed": 0}
    for post, h, label, lines in new:
        msg = message(post, label, lines)
        if dry_run:
            print(msg + "\n")
            continue
        if send(msg, NAME):
            sent[f"{post['id']}|{h}"] = today.isoformat()
            summary["sent"] += 1
        else:
            summary["failed"] += 1
        time.sleep(1.1)                   # лимит Telegram — около сообщения в секунду на чат
    if not dry_run and summary["sent"]:
        _save_sent(sent)
    return summary


def report(n: int) -> None:
    """Таблица по последним n постам: дата, активы, ход за неделю/месяц/3 месяца."""
    today = datetime.now(MSK).date()
    db = SessionLocal()
    try:
        posts = load_posts(db, today - timedelta(days=400))[-n:]
        dic = _dictionary(db)
        for p in posts:
            p["assets"] = find_assets(p["text"], dic)
        prices = load_prices(db, {k for p in posts for k, _ in p["assets"]}, today - timedelta(days=400))
    finally:
        db.close()
    for p in posts:
        print(f"{p['day']} #{p['id']} {headline(p['text'])}")
        if not p["assets"]:
            print("    (актив не найден)")
        for key, name in p["assets"]:
            cells = []
            for h, _ in HORIZONS:
                o = outcome(prices.get(key, []), p["day"], h, today)
                cells.append(f"{h:>3}д {fmt_pct(o['pct']):>7}" if o else f"{h:>3}д       —")
            print(f"    {name:<22} " + "  ".join(cells))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать сообщения, ничего не слать")
    ap.add_argument("--since", type=date.fromisoformat, default=FLOOR, help="нижняя граница даты поста")
    ap.add_argument("--report", type=int, nargs="?", const=40, help="таблица по последним N постам, без отправки")
    a = ap.parse_args()
    if a.report:
        report(a.report)
    else:
        print(f"[{NAME}] {run_once(a.since, a.dry_run)}")
