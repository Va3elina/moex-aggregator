"""
Интерфакс, раздел «Экономика/бизнес» → news_archive (канал interfax) → кандидаты в посты
(source='interfax').

⚠️ ИСТОЧНИК ПОВОДОВ, А НЕ ЧИСЕЛ. Числа у нас из FinanceMarker и MOEX; Интерфакс даёт
то, чего в раскрытии ещё нет: «ПИК проведёт делистинг», «Аэрофлот допускает дивиденды».
Список раздела interfax.ru/business/ и страницы новостей отдаются с сервера целиком
(проверено 06.09.2026 с прод-сервера: 42 новости за двое суток, тело статьи в <article>).
Общий RSS не годится: 25 последних новостей всех рубрик, бизнеса там три.

⚠️ АВТОРСКОЕ ПРАВО. Тело статьи — контекст для агента, а не материал для поста:
в пост идёт наш текст и ссылка, цитировать Интерфакс нельзя (правило в промпте Шага В).

⚠️ ТЕМП ЩАДЯЩИЙ: сайт за Qrator. Один запрос списка в 5 минут и статьи только по новым
id (30–40 в день), пауза между статьями. Никаких повторных обходов истории.

Кандидатом становится новость, в заголовке или лиде которой правила имён второго мозга
нашли нашу компанию (1–3 компании; обзоры «рынок акций РФ…» имён не содержат и
отсеиваются сами). Тикеры кандидату НЕ ставим — их выбирает Шаг А по known_tickers,
как у телеграм-новостей; подсказка мозга покажет ему те же имена. Хайп-фильтр (Шаг Н)
обходится: это не пост в канале, репостов у него нет.

Крон (хост):  */5 * * * * /opt/frame/signals/interfax_scan.sh >> /opt/frame/logs/interfax_scan.log 2>&1
"""

import argparse
import html as _html
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from sqlalchemy import text

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat                  # noqa: E402
from api.database import SessionLocal      # noqa: E402

КАНАЛ = "interfax"
СПИСОК = "https://www.interfax.ru/business/"
СТАТЬЯ = "https://www.interfax.ru/business/{id}"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
ТАЙМАУТ = (8, 30)
ПАУЗА_МЕЖДУ_СТАТЬЯМИ = 1.5
МАКС_СТАТЕЙ_ЗА_ПРОГОН = 40
МАКС_ТЕКСТ = 3500
МСК = timezone(timedelta(hours=3))

_СПИСОК_RE = re.compile(r'<a href="/business/(\d+)"[^>]*>(.*?)</a>', re.S)
_ВРЕМЯ_RE = re.compile(r'<time[^>]*datetime="([^"]+)"')
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S)
_ARTICLE_RE = re.compile(r"<article[^>]*>(.*?)</article>", re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S)

_SELECT_ИЗВЕСТНЫЕ = text("SELECT message_id FROM news_archive WHERE channel = :ch AND message_id = ANY(:ids)")
_INSERT_NEWS = text("""
    INSERT INTO news_archive (channel, message_id, posted_at, text, views, hashtags, entities, tickers, source)
    VALUES (:ch, :mid, :posted_at, :text, NULL, ARRAY[]::text[], CAST(:entities AS jsonb), ARRAY[]::text[], 'interfax_web')
    ON CONFLICT (channel, message_id) DO NOTHING
""")
# Те же правила имён, что подсказывают Шагу А (brain_name_rules, полнотекст с морфологией).
_SELECT_NAME_HITS = text("""
    SELECT DISTINCT r.company_id, n.title
      FROM brain_name_rules r JOIN brain_nodes n ON n.id = r.company_id
     WHERE r.enabled AND NOT r.ambiguous
       AND to_tsvector('russian', :t) @@ phraseto_tsquery('russian', r.pattern)
     LIMIT 6
""")
_EXISTS_CANDIDATE = text("""
    SELECT 1 FROM content_candidates
     WHERE source = 'interfax' AND (source_url = :url OR (headline = :headline AND created_at > now() - interval '3 days'))
     LIMIT 1
""")
# ⚠️ ОДНА КОМПАНИЯ — ОДИН КАНДИДАТ ЗА 6 ЧАСОВ. Пуск «Восток Ойла» 05.09 дал три новости
# про Роснефть за час; три кандидата — это три разбора одного повода. Остальные
# новости остаются в архиве и попадают в бриф как «о компании писали».
# Окно считается по времени ПУБЛИКАЦИИ новости, а не по времени создания кандидата:
# первый прогон залил двое суток разом, и «Аэрофлот допускает дивиденды» (05.09) ушёл
# в тень «Аэрофлот закупает керосин» (04.09) только потому, что оба создались в одну минуту.
_SAME_COMPANY_RECENT = text("""
    SELECT c.id FROM content_candidates c
      JOIN news_archive n ON n.channel = 'interfax' AND c.source_url = 'https://www.interfax.ru/business/' || n.message_id
     WHERE c.source = 'interfax'
       AND n.posted_at BETWEEN CAST(:posted_at AS timestamptz) - interval '6 hours' AND CAST(:posted_at AS timestamptz) + interval '6 hours'
       AND n.entities->'companies' ?| CAST(:companies AS text[])
     LIMIT 1
""")
_INSERT_CANDIDATE = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, source_url, hype_filter_result, created_at, updated_at)
    VALUES ('candidate', 'interfax', :headline, :raw_text, ARRAY[]::text[], :url, TRUE, now(), now())
    RETURNING id
""")


def _get(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA, "Accept-Language": "ru"}, timeout=ТАЙМАУТ)
    r.raise_for_status()
    raw = r.content
    enc = "cp1251" if b"windows-1251" in raw[:600].lower() or b"charset=windows-1251" in raw[:600].lower() else "utf-8"
    return raw.decode(enc, errors="replace")


def _чисто(s: str) -> str:
    s = _SCRIPT_RE.sub(" ", s)
    s = _TAG_RE.sub(" ", s)
    return re.sub(r"\s+", " ", _html.unescape(s)).strip()


def список() -> list[tuple[int, str]]:
    """[(id, заголовок)] из раздела, свежие первыми, без дублей."""
    page = _get(СПИСОК)
    out, seen = [], set()
    for mid, inner in _СПИСОК_RE.findall(page):
        t = _чисто(inner)
        mid = int(mid)
        if len(t) < 20 or mid in seen:
            continue
        seen.add(mid)
        out.append((mid, t))
    return out


def статья(mid: int) -> dict | None:
    page = _get(СТАТЬЯ.format(id=mid))
    h1 = _H1_RE.search(page)
    art = _ARTICLE_RE.search(page)
    tm = _ВРЕМЯ_RE.search(page)
    if not art:
        return None
    body = _чисто(art.group(1))
    title = _чисто(h1.group(1)) if h1 else ""
    if title and body.startswith(title):
        body = body[len(title):].strip()
    posted = None
    if tm:
        try:
            posted = datetime.fromisoformat(tm.group(1)[:16]).replace(tzinfo=МСК).astimezone(timezone.utc)
        except ValueError:
            posted = None
    return {"title": title, "body": body, "posted_at": posted or datetime.now(timezone.utc)}


def run_once(dry_run: bool = False, limit: int = МАКС_СТАТЕЙ_ЗА_ПРОГОН) -> dict:
    итог = {"в_списке": 0, "новых": 0, "статей": 0, "кандидатов": 0, "ошибок": 0}
    db = SessionLocal()
    try:
        try:
            элементы = список()
        except Exception as e:  # noqa: BLE001
            итог["ошибок"] += 1
            print(f"[interfax_scan] список: {type(e).__name__}: {e}")
            return итог
        итог["в_списке"] = len(элементы)
        ids = [m for m, _ in элементы]
        известные = {r[0] for r in db.execute(_SELECT_ИЗВЕСТНЫЕ, {"ch": КАНАЛ, "ids": ids}).all()}
        новые = [(m, t) for m, t in элементы if m not in известные]
        итог["новых"] = len(новые)
        # старые первыми: если прогон оборвётся, хвост доберём в следующий
        for mid, заголовок in sorted(новые)[-limit:]:
            try:
                ст = статья(mid)
            except Exception as e:  # noqa: BLE001
                итог["ошибок"] += 1
                print(f"[interfax_scan] {mid}: {type(e).__name__}: {e}")
                time.sleep(ПАУЗА_МЕЖДУ_СТАТЬЯМИ)
                continue
            if not ст:
                print(f"[interfax_scan] {mid}: нет <article> — пропускаю")
                continue
            title = ст["title"] or заголовок
            url = СТАТЬЯ.format(id=mid)
            текст = (title + "\n\n" + ст["body"])[:6000]
            попадания = db.execute(_SELECT_NAME_HITS, {"t": (title + " " + ст["body"][:600])[:1500]}).all()
            кандидат = 1 <= len(попадания) <= 3
            if dry_run:
                print(f"  {ст['posted_at']:%d.%m %H:%M} {mid} | {title[:80]} | тело {len(ст['body'])} зн. | "
                      f"{'КАНДИДАТ ' + ', '.join(t for _, t in попадания) if кандидат else ('компаний ' + str(len(попадания)))}")
            else:
                db.execute(_INSERT_NEWS, {"ch": КАНАЛ, "mid": mid, "posted_at": ст["posted_at"], "text": текст,
                                          "entities": json.dumps({"url": url, "companies": [c for c, _ in попадания]}, ensure_ascii=False)})
                if кандидат and not db.execute(_EXISTS_CANDIDATE, {"url": url, "headline": title}).first():
                    ветка = db.execute(_SAME_COMPANY_RECENT, {"companies": [c for c, _ in попадания], "posted_at": ст["posted_at"]}).scalar()
                    if ветка:
                        print(f"[interfax_scan] {mid}: та же компания уже в кандидате #{ветка} (<6 ч) — только в архив")
                    else:
                        cid = db.execute(_INSERT_CANDIDATE, {"headline": title, "raw_text": ст["body"][:МАКС_ТЕКСТ], "url": url}).scalar()
                        итог["кандидатов"] += 1
                        print(f"[interfax_scan] кандидат #{cid}: {title[:90]} ← {', '.join(t for _, t in попадания)}")
                db.commit()
            итог["статей"] += 1
            time.sleep(ПАУЗА_МЕЖДУ_СТАТЬЯМИ)
    except Exception as e:  # noqa: BLE001
        db.rollback()
        итог["ошибок"] += 1
        print(f"[interfax_scan] сбой: {type(e).__name__}: {e}")
    finally:
        db.close()
    return итог


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=МАКС_СТАТЕЙ_ЗА_ПРОГОН)
    a = ap.parse_args()
    итог = run_once(a.dry_run, a.limit)
    print(f"[interfax_scan] итог: {итог}")
    if not a.dry_run:
        pipeline_heartbeat.record_pipeline_run(
            "interfax_scan", success=итог["в_списке"] > 0,
            note=f"в списке {итог['в_списке']}, новых {итог['новых']}, статей {итог['статей']}, кандидатов {итог['кандидатов']}",
            degraded=итог["ошибок"] > 0 and итог["в_списке"] > 0)


if __name__ == "__main__":
    main()
