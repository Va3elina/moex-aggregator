#!/usr/bin/env python3
"""Лента объявлений Московской биржи (ISS /iss/sitenews) → кандидаты в посты.

Зачем эта ветка вообще есть. 09.09.2026 биржа в 14:00 МСК объявила включение
ДОМ.РФ в Индекс МосБиржи создания стоимости. Мы узнали об этом в 14:06 из
MarketTwits, и то мимо конвейера: у поста было мало репостов, порог хайпа он бы
не прошёл, а фьючерса на DOMRF у нас нет — Шаг Б всё равно не подтвердил бы.
При этом первоисточник открыт, бесплатен, без токена и отдаёт JSON.

Что берём. `https://iss.moex.com/iss/sitenews.json` — последние 50 объявлений
(id, tag, title, published_at). Тело новости лежит отдельно, в
`/iss/sitenews/{id}.json`, и тянем мы его ТОЛЬКО для интересных рубрик: в ленте
90% — служебные сообщения о допуске облигаций к РЕПО и ценовых коридорах, и
качать их тела значило бы 50 лишних запросов каждые 15 минут.

Что считаем интересным. Две рубрики, обе про вынужденные потоки:
  • «индекс» — изменение базы расчёта индексов (включение/исключение бумаги,
    пересмотр весов). Индексные фонды обязаны отзеркалить это сделками;
  • «листинг» — перевод уровня листинга, делистинг, приостановка торгов. Тоже
    механическое: фонды с мандатом по уровню листинга обязаны реагировать.
Остальное (РЕПО, ценовые коридоры, дискретные аукционы, премии, курсы) в
кандидаты не идёт — оно либо служебное, либо не влечёт вынужденной сделки.

⚠️ Кандидат заводится с `hype_filter_result = TRUE`, как у ленты раскрытия FM:
это объявление биржи, а не пост из канала, репосты ему мерить нечем и незачем.
Дальше — обычный Шаг А (тикер и значимость) и Шаг Б (подтверждение данными).

⚠️ Тикеры достаём двумя способами — по коду и ПО ИМЕНИ КОМПАНИИ. Замер на живой
ленте 09.09: у объявления про ДОМ.РФ поиск по коду нашёл «MOEX» (из английского
названия индекса «MOEX Value Building Index») и не нашёл сам DOMRF, потому что в
русском пресс-релизе компания названа «ДОМ.РФ», а не тикером. Неверный тикер хуже
пустого: Шаг Б пошёл бы искать аномалию не по той бумаге. Поэтому:
  • коды ищем как отдельные слова (иначе «LENT» находится внутри «MOEXTELECOM»);
  • имена берём из issuers.name_short с нормализацией точек и регистра;
  • MOEX не ставим НИКОГДА, ни по коду, ни по имени: биржа — ИЗДАТЕЛЬ этой
    ленты и упоминает себя в каждом объявлении (в подписи, в названиях индексов
    вроде «MOEX Value Building Index»). Новости про акции самой биржи приходят
    к нам лентой раскрытия FM, там она обычный эмитент, а не издатель.
Если бумага не наша — кандидат всё равно заводим (индекс мог поменяться из-за
чужой бумаги, но веса поехали у наших), просто без тикера: Шаг А разберётся сам.

Запуск:
  python -m signals.moex_sitenews_scan              # штатный прогон
  python -m signals.moex_sitenews_scan --dry-run    # показать, ничего не писать
  python -m signals.moex_sitenews_scan --limit 100  # глубже по ленте
"""
import argparse
import os
import re
import sys
import time

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

СПИСОК = "https://iss.moex.com/iss/sitenews.json"
НОВОСТЬ = "https://iss.moex.com/iss/sitenews/{id}.json"
ТАЙМАУТ = (10, 30)
ПОВТОРОВ = 3
МАКС_ТЕЛО = 3500          # знаков в raw_text: у пресс-релизов бывают простыни цитат

# Рубрики. Порядок важен: делистинг часто упомянут вместе с индексом, и тогда
# сюжет всё-таки про индекс (бумагу исключают из базы расчёта из-за делистинга).
_РУБРИКИ = (
    ("индекс", re.compile(r"индекс|баз[аыу] расч[её]т|состав\w* индекс", re.I)),
    ("листинг", re.compile(r"листинг|делистинг|уров\w+ листинга|приостанов\w+ торг|"
                            r"прекращ\w+ торг|исключен\w+ из списка", re.I)),
)
# Служебный шум ленты: допуск к РЕПО, ценовые коридоры, дискретные аукционы,
# риск-параметры. Отсекаем ДО рубрикации — иначе «изменены значения нижней
# границы ценового коридора» ловится на слово «индекс» в теле.
_ШУМ = re.compile(r"РЕПО с ЦК|ценов\w+ коридор|дискретн\w+ аукцион|риск-параметр|"
                   r"ставк\w+ риска|о регистрации выпуска|дополнительн\w+ услови\w+ проведения торгов", re.I)

_UPSERT = text("""
    INSERT INTO moex_sitenews (id, published_at, modified_at, tag, title, body, rubric, tickers)
    VALUES (:id, CAST(:published_at AS timestamptz), CAST(:modified_at AS timestamptz),
            :tag, :title, :body, :rubric, CAST(:tickers AS text[]))
    ON CONFLICT (id) DO UPDATE SET
        modified_at = EXCLUDED.modified_at,
        body = COALESCE(EXCLUDED.body, moex_sitenews.body),
        rubric = COALESCE(EXCLUDED.rubric, moex_sitenews.rubric),
        tickers = CASE WHEN cardinality(EXCLUDED.tickers) > 0
                       THEN EXCLUDED.tickers ELSE moex_sitenews.tickers END
    RETURNING candidate_id, (xmax = 0) AS inserted
""")
_INSERT_CANDIDATE = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, source_url, hype_filter_result, created_at, updated_at)
    VALUES
        ('candidate', 'moex_sitenews', :headline, :raw_text, CAST(:tickers AS text[]),
         :source_url, TRUE, now(), now())
    RETURNING id
""")
_LINK = text("UPDATE moex_sitenews SET candidate_id = :cid WHERE id = :nid")
_ВСЕЛЕННАЯ = text("""
    SELECT smartlab_ticker, name_short FROM issuers
     WHERE smartlab_ticker IS NOT NULL AND is_active
""")
# Издатель ленты. Биржа называет себя в каждом своём объявлении, поэтому её
# тикер здесь не признак темы, а шум: замер 09.09 на новости про ДОМ.РФ дал
# «MOEX» из английского названия индекса. Про акции самой биржи мы узнаём из
# ленты раскрытия FM, где она обычный эмитент.
_ИЗДАТЕЛЬ = {"MOEX"}
МАКС_ТИКЕРОВ = 3


def _вычистить(s: str) -> str:
    """HTML-сущности и теги: ISS отдаёт тело как размеченный фрагмент страницы."""
    s = re.sub(r"<br\s*/?>|</p>", "\n", s or "")
    s = re.sub(r"<[^>]+>", "", s)
    for a, b in (("&quot;", '"'), ("&mdash;", "—"), ("&ndash;", "–"), ("&nbsp;", " "),
                 ("&laquo;", "«"), ("&raquo;", "»"), ("&amp;", "&"), ("&#39;", "'")):
        s = s.replace(a, b)
    return re.sub(r"\n{3,}", "\n\n", s).strip()


def _запрос(url: str, params: dict = None) -> dict:
    последняя = None
    for попытка in range(ПОВТОРОВ):
        try:
            r = requests.get(url, params=params, timeout=ТАЙМАУТ)
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout, ValueError) as e:
            последняя = e
            time.sleep(2 * (попытка + 1))
    raise последняя


def _таблица(ответ: dict, ключ: str) -> list[dict]:
    блок = ответ.get(ключ) or {}
    колонки = блок.get("columns") or []
    return [dict(zip(колонки, строка)) for строка in (блок.get("data") or [])]


def рубрика(текст: str):
    if _ШУМ.search(текст or ""):
        return None
    for имя, шаблон in _РУБРИКИ:
        if шаблон.search(текст or ""):
            return имя
    return None


def _норм(s: str) -> str:
    """Имя компании к сравнимому виду: «Дом.РФ» и «ДОМ.РФ» — одно и то же."""
    return re.sub(r"[^0-9a-zа-яё]", "", (s or "").lower())


def _шаблон_имени(имя: str):
    """Регулярка «имя как отдельное слово», терпимая к точкам и дефисам внутри.

    ⚠️ Почему не подстрока по нормализованному тексту (первая версия): короткие
    имена тогда приходилось отбрасывать целиком, и «ПИК» не находился в «ПАО
    ПИК» — а это ровно тот делистинг, который мы и хотим ловить. Границы слова
    решают: «ПИК» matched в «ПАО ПИК», но не в «пиковый».
    """
    части = [ч for ч in re.split(r"[^0-9A-Za-zА-Яа-яЁё]+", имя or "") if ч]
    if not части or sum(len(ч) for ч in части) < 3:
        return None            # «ЭН+» → «ЭН»: слишком коротко, ловится только по коду
    тело = r"[\s.\-–—]*".join(re.escape(ч) for ч in части)
    return re.compile(r"(?<![0-9A-Za-zА-Яа-яЁё])%s(?![0-9A-Za-zА-Яа-яЁё])" % тело, re.I)


def тикеры(текст: str, вселенная: dict) -> list:
    """вселенная: {тикер: короткое имя}. Возвращает коды наших бумаг из текста."""
    текст = текст or ""
    найдено = set()
    for код, имя in вселенная.items():
        if код in _ИЗДАТЕЛЬ:
            continue
        if re.search(r"\b%s\b" % re.escape(код), текст):
            найдено.add(код)
            continue
        шаблон = _шаблон_имени(имя)
        if шаблон and шаблон.search(текст):
            найдено.add(код)
    return sorted(найдено)[:МАКС_ТИКЕРОВ]


def run_once(limit: int = 50, dry_run: bool = False) -> dict:
    итог = {"записей": 0, "новых": 0, "интересных": 0, "кандидатов": 0, "ошибок": 0}
    db = SessionLocal()
    try:
        вселенная = {r[0]: r[1] for r in db.execute(_ВСЕЛЕННАЯ).all()}
        новости = _таблица(_запрос(СПИСОК, {"iss.meta": "off", "limit": limit}), "sitenews")
        итог["записей"] = len(новости)
        for n in новости:
            заголовок = _вычистить(n.get("title") or "")
            если_наша = рубрика(заголовок)
            # Тело качаем ТОЛЬКО у интересных: в ленте 9 из 10 — служебные сообщения.
            тело = None
            if если_наша:
                итог["интересных"] += 1
                try:
                    строки = _таблица(_запрос(НОВОСТЬ.format(id=n["id"]), {"iss.meta": "off"}), "content")
                    тело = _вычистить((строки[0].get("body") if строки else "") or "")
                except Exception as e:  # noqa: BLE001 — заголовка хватит, чтобы завести кандидата
                    итог["ошибок"] += 1
                    print(f"[moex_sitenews_scan] тело {n['id']}: {type(e).__name__}: {e}")
            полный = f"{заголовок}\n\n{тело or ''}".strip()
            коды = тикеры(полный, вселенная) if если_наша else []
            if dry_run:
                if если_наша:
                    print(f"  {n['published_at']} [{если_наша}] {коды or '—'} {заголовок[:80]}")
                continue
            row = db.execute(_UPSERT, {
                "id": int(n["id"]), "published_at": n["published_at"],
                "modified_at": n.get("modified_at"), "tag": (n.get("tag") or "")[:32],
                "title": заголовок, "body": (тело or None), "rubric": если_наша,
                "tickers": коды,
            }).mappings().first()
            if row["inserted"]:
                итог["новых"] += 1
            if _пропустить(если_наша, row):
                continue
            cid = db.execute(_INSERT_CANDIDATE, {
                "headline": заголовок[:500],
                "raw_text": полный[:МАКС_ТЕЛО],
                "tickers": коды,
                "source_url": f"https://www.moex.com/n{n['id']}",
            }).scalar()
            db.execute(_LINK, {"cid": cid, "nid": int(n["id"])})
            итог["кандидатов"] += 1
            print(f"[moex_sitenews_scan] кандидат #{cid} [{если_наша}] {коды}: {заголовок[:80]}")
        db.commit()
    except Exception as e:  # noqa: BLE001
        db.rollback()
        итог["ошибок"] += 1
        print(f"[moex_sitenews_scan] сбой: {type(e).__name__}: {e}")
    finally:
        db.close()
    return итог


def _пропустить(рубрика_новости, строка) -> bool:
    """Кандидат нужен один раз и только для интересных рубрик.

    ⚠️ Проверяем candidate_id ИЗ БАЗЫ, а не факт вставки: ISS правит уже
    опубликованные новости (у объявления про ДОМ.РФ modified_at на 8 минут
    позже published_at), и после правки строка приходит как обновление — без
    этой проверки мы завели бы второго кандидата на то же объявление.
    """
    return not рубрика_новости or строка["candidate_id"] is not None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50, help="сколько последних объявлений смотреть")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    итог = run_once(a.limit, a.dry_run)
    print(f"[moex_sitenews_scan] итог: {итог}")
    if not a.dry_run:
        pipeline_heartbeat.record_pipeline_run(
            "moex_sitenews_scan", success=итог["записей"] > 0,
            note=f"записей {итог['записей']}, новых {итог['новых']}, "
                 f"интересных {итог['интересных']}, кандидатов {итог['кандидатов']}",
            degraded=итог["ошибок"] > 0 and итог["записей"] > 0)


if __name__ == "__main__":
    main()
