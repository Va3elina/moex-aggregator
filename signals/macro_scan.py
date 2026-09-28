"""Макро: новость важности 5 без компании → пост «как отреагировали наши данные» (Вадим 28.09: «делай по очереди, вводи
в эксплуатацию», шаг 3).

Шаг А такие новости отбрасывает — у завода не было пути для макро: за 14–27.09 13 новостей важности 5 («адские
санкции», перемирие) и ни одного поста. Здесь, спустя MACRO_WAIT_H часа после новости, собирается карточка реакции
(cards.macro_card): цена фьючерсов на индекс и доллар (при санкциях и нефти — ещё Лукойл и Роснефть) и 5-минутные
позиции физлиц за два часа после новости и к концу дня против обычного хода. Черновик — только если цена или толпа ушли
хотя бы в MACRO_MIN раза сильнее обычного; не больше одного в день, одна тема — раз в MACRO_THEME_DAYS дня. Дальше путь
тот же, что у находок: писатель по находке (content_ai), проверка кодом при приёмке, судья, бот.

Запуск — из дневного прохода связок (combo_scan --mode news, каждые 20 минут), своего крона нет. Ночная новость
(«Трамп подписал» в 00:50) разбирается утром: окно реакции — открытие торгов.

    python3 -m signals.macro_scan --dry-run                         # что создал бы сейчас
    python3 -m signals.macro_scan --dry-run --at 2026-09-17T08:30Z  # как выглядел бы момент в прошлом
"""
import os

_url = os.environ.get("DB_URL", "")
if "@db:" in _url:
    os.environ["DB_URL"] = _url.replace("@db:", "@127.0.0.1:")

import argparse  # noqa: E402
import json  # noqa: E402
import re  # noqa: E402
from datetime import datetime, timedelta, timezone  # noqa: E402

import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from signals.insights import cards  # noqa: E402

MACRO_WAIT_H = 2           # часов после новости: реакция должна успеть случиться
MACRO_LOOKBACK_H = 26      # новость старше — уже не повод
MACRO_MIN = 2.0            # цена или толпа — хотя бы в 2 раза сильнее обычного двухчасового хода
MACRO_THEME_DAYS = 3       # одна тема — не чаще раза в три дня: 10 новостей про «адские санкции» — один пост
MEDIA_DIR = os.environ.get("CONTENT_MEDIA_DIR", "/opt/frame/data/content_media")
CACHE = os.path.join(os.environ.get("COMBO_CACHE_DIR", "/opt/frame/data/combo_cache"), "macro_checked.json")
MSK = timezone(timedelta(hours=3))
# тема словаря мозга → набор бумаг карточки (cards.MACRO_EXTRA)
THEME = {"санкции": "санкции", "нефть, газ и топливо": "нефть_газ", "удары по инфраструктуре": "нефть_газ",
         "геополитика и переговоры": "геополитика"}

_CANDS = text("""
    SELECT c.id, coalesce(c.raw_text, c.headline, '') AS text, c.created_at, a.posted_at
      FROM content_candidates c
      LEFT JOIN news_archive a
        ON a.message_id = CAST(NULLIF(substring(c.source_url from :rx_id), '') AS bigint)
       AND lower(a.channel) = lower(substring(c.source_url from :rx_ch))
     WHERE c.status = 'discarded' AND c.importance_1_5 >= 5 AND coalesce(cardinality(c.tickers), 0) = 0
       AND c.source IN ('markettwits', 'newssmartlab') AND c.created_at BETWEEN :since AND :until
     ORDER BY c.created_at
""")
_DONE = text("""
    SELECT thread_key, created_at FROM content_candidates
     WHERE source = 'insight' AND event_type = 'insight_macro' AND created_at > :since
""")
_INSERT = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, futures_ticker, event_type,
         importance_1_5, reasoning, media_filename, thread_key)
    VALUES ('draft_ready', 'insight', :headline, :raw_text, CAST(:tickers AS text[]), 'IMOEXF',
            'insight_macro', 5, :reasoning, :media_filename, :thread_key)
    RETURNING id
""")


def headline_of(raw: str) -> str:
    """Суть новости без хэштегов, эмодзи и разметки — первая фраза, не длиннее 200 знаков."""
    t = re.sub(r"#\S+|\*\*|https?://\S+", " ", raw or "")
    t = re.sub(r"[^\w\s.,:;!?«»\"'()%$€₽\-–—/]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    first = re.split(r"(?<=[.!?])\s", t)[0]
    return (first if len(first) <= 200 else re.sub(r"\s+\S*$", "", first[:200]) + "…").strip(" -–—")


def theme_of(db, raw: str) -> str:
    """Тема новости — единым словарём мозга (как у Шага А и связок), не своим классификатором."""
    try:
        from api.brain_core import тип_текста
        tp = тип_текста(db, raw, [x.lower() for x in re.findall(r"#[0-9A-Za-zА-Яа-яЁё_]+", raw or "")],
                        без_компании=True) or ""
    except Exception as e:  # noqa: BLE001 — без темы карточка берёт только индекс и доллар
        print(f"[macro_scan] тема не определилась: {type(e).__name__}: {e}")
        tp = ""
    return THEME.get(tp, tp or "макро")


def pick_event(rows: list, done: list, now_msk, themes: dict) -> list:
    """Какие новости разбирать: по одной в день (МСК), тема — раз в MACRO_THEME_DAYS дня; от старых к новым."""
    today = now_msk.date()
    if any(pd.Timestamp(d).tz_convert(MSK).date() == today for _, d in done):
        return []
    recent = {k.split(":")[2] for k, d in done
              if k and k.count(":") >= 3 and (now_msk - pd.Timestamp(d).tz_convert(MSK)).days < MACRO_THEME_DAYS}
    return [r for r in rows if themes.get(r["id"]) not in recent]


def lookback_hours(now_msk) -> int:
    """Новость выходных («Трамп подписал» в ночь на субботу) разбирается в понедельник: реакция — открытие торгов."""
    return MACRO_LOOKBACK_H + (48 if now_msk.weekday() == 0 and now_msk.hour < 14 else 0)


def _cache() -> dict:
    try:
        return json.load(open(CACHE, encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def _remember(c: dict, cid: int, k: float) -> None:
    """Двухчасовое окно реакции после конца не меняется: слабую новость не пересчитываем каждые 20 минут."""
    c[str(cid)] = round(k, 2)
    try:
        os.makedirs(os.path.dirname(CACHE), exist_ok=True)
        json.dump(dict(list(c.items())[-500:]), open(CACHE, "w", encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        print(f"[macro_scan] кэш не записан: {e}")


def run_once(dry_run: bool = False, at: str | None = None) -> dict:
    now = pd.Timestamp(at) if at else pd.Timestamp(datetime.now(timezone.utc))
    now = now.tz_localize("UTC") if now.tzinfo is None else now
    now_msk = now.tz_convert(MSK)
    summary = {"checked": 0, "created": 0}
    seen = {} if (dry_run and at) else _cache()     # пишет его только боевой прогон
    db = SessionLocal()
    try:
        rows = [dict(r._mapping) for r in db.execute(_CANDS, {
            "rx_id": r"/(\d+)$", "rx_ch": r"t\.me/([^/]+)/",
            "since": now - pd.Timedelta(hours=lookback_hours(now_msk)),
            "until": now - pd.Timedelta(hours=MACRO_WAIT_H)})]
        rows = [r for r in rows if seen.get(str(r["id"]), MACRO_MIN) >= MACRO_MIN]   # слабые уже проверенные — мимо
        done = [(r[0], r[1]) for r in db.execute(_DONE, {"since": now - pd.Timedelta(days=MACRO_THEME_DAYS)})]
        themes = {r["id"]: theme_of(db, r["text"]) for r in rows}
        for r in pick_event(rows, done, now_msk, themes):
            summary["checked"] += 1
            ev = pd.Timestamp(r["posted_at"] or r["created_at"])
            ev = (ev.tz_localize("UTC") if ev.tzinfo is None else ev).tz_convert(MSK).tz_localize(None)
            if now_msk.tz_localize(None) - ev < pd.Timedelta(hours=MACRO_WAIT_H):
                continue
            head = headline_of(r["text"])
            try:
                card = cards.macro_card(ev, head, themes[r["id"]], now_msk.tz_localize(None))
            except Exception as e:  # noqa: BLE001
                print(f"[macro_scan] карточка #{r['id']} не собралась: {type(e).__name__}: {e}")
                continue
            k = float(card.get("strength") or 0)
            if not dry_run:
                _remember(seen, r["id"], k)
            if k < MACRO_MIN:
                print(f"[macro_scan] #{r['id']} {ev:%d.%m %H:%M}: реакция {k:.1f}× обычного — ниже порога "
                      f"{MACRO_MIN}×: {head[:70]}")
                continue
            brief = cards.brief_text(card, focus=True)
            thread = f"insight:macro:{themes[r['id']]}:{r['id']}"
            print(f"[macro_scan] #{r['id']} {ev:%d.%m %H:%M} ({themes[r['id']]}): реакция {k:.1f}× обычного — "
                  f"черновик: {head[:80]}")
            if dry_run:
                print(brief[:1500] + "\n")
                summary["created"] += 1
                break
            media = None
            if card.get("chart"):
                os.makedirs(MEDIA_DIR, exist_ok=True)
                media = f"insight_{ev:%Y%m%d_%H%M}_macro_{r['id']}.png"
                cards.draw_chart(card, os.path.join(MEDIA_DIR, media))
            row = db.execute(_INSERT, {
                "headline": card["headline"][:300], "raw_text": brief, "tickers": ["MIX"],
                "reasoning": f"макро: новость #{r['id']} (важность 5, Шаг А отбросил — нет компании) → реакция наших "
                             f"данных {k:.1f}× обычного", "media_filename": media, "thread_key": thread,
            }).first()
            db.commit()
            summary["created"] += 1
            print(f"[macro_scan] кандидат {row[0]} создан")
            break
    finally:
        db.close()
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="показать карточку, ничего не писать")
    ap.add_argument("--at", help="момент в прошлом (ISO, UTC); только с --dry-run")
    a = ap.parse_args()
    if a.at and not a.dry_run:
        ap.error("--at только вместе с --dry-run")
    print(f"[macro_scan] {run_once(dry_run=a.dry_run, at=a.at)}")
