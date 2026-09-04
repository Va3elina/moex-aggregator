#!/usr/bin/env python3
"""
Карточки компаний из FinanceMarker: один запрос на компанию, все разделы сразу.

⚠️ ЗАМЕНЯЕТ ПАРСЕР SMART-LAB, и вот почему это не вкусовщина. Замер 04.09.2026 на
Сбербанке: у FM отчётность 2011–2026 (98 записей, 99 полей) против 5 лет + 5 кварталов
у smart-lab; дивиденды с 2007 против 2017; у КАЖДОГО отчёта ссылка на PDF через их CDN
(content-type: application/pdf), тогда как половина ссылок smart-lab вела на
e-disclosure, который нам недоступен.

Но главное не глубина, а класс ошибок. Парсер HTML за один день дал: ноль вместо
пропуска, сдвиг колонок НА ГОД, кириллическую «К» вместо латинской Q, двойное
экранирование, задвоение Татнефти. Каждая родилась из разбора вёрстки. В JSON их нет
по построению.

⚠️ ОДНА КОМПАНИЯ = ОДИН ЗАПРОС. В ответе всегда все десять разделов, незапрошенные
приходят пустыми массивами: `include` управляет наполнением, а не структурой. Значит
просить разделы по очереди — тратить запросы впустую.

⚠️ КОДЫ ПОКАЗАТЕЛЕЙ — РОДНЫЕ FM, не переведённые в коды smart-lab. Source входит в
уникальный ключ company_metrics, поэтому два источника лежат параллельными рядами и
сверяются друг с другом; перевод кодов «на лету» превратил бы сверку в угадайку.

Использование:
    python fetch_fm_cards.py --limit 3 --dry-run     # проба
    python fetch_fm_cards.py                         # обход вселенной
"""

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(BASE_DIR.parent))

for _p in [BASE_DIR] + list(BASE_DIR.parents):
    if (_p / ".env").exists():
        load_dotenv(_p / ".env")
        break
# ⚠️ Адрес базы в .env записан для сети докера («@db:»). На ХОСТЕ такого имени нет и
# нужен 127.0.0.1, а ВНУТРИ контейнера — наоборот, «db» единственно верный. Слепая
# замена ломает один из двух случаев, поэтому проверяем, резолвится ли имя.
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    import socket
    try:
        socket.gethostbyname("db")
    except OSError:
        os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from fm_client import (FMClient, ЗАПАС_ЗАПРОСОВ, ИсточникПросилУйти,  # noqa: E402
                       ЛимитИсчерпан, log, setup_logging)

ИСТОЧНИК = "financemarker"
РАЗДЕЛЫ = "info,reports,ratios,dividends,owners,shares,summary,operations"

# Поля отчёта, которые НЕ являются показателями: служебные и текстовые. Всё
# остальное из 99 полей уезжает в company_metrics как есть.
НЕ_ПОКАЗАТЕЛИ = {"code", "exchange", "year", "month", "period", "type", "curr",
                 "amount", "changed_at", "link", "link_press", "link_update",
                 "preliminary"}


def notify(текст: str):
    """Уведомление тем же каналом, что и весь проект (CF-релей: прямой TG закрыт)."""
    chat = os.getenv("FRAME_ADMIN_CHAT_ID")
    if not chat:
        log.info("уведомление не отправлено (нет FRAME_ADMIN_CHAT_ID): %s",
                 текст.replace("\n", " ")[:160])
        return
    try:
        from signals.alert_notify import send_message
        send_message(chat, текст)
    except Exception as ex:
        log.warning("уведомление не ушло: %s", ex)


def период_в_метку(r: dict) -> tuple:
    """
    Из (year, period, month) FM — наши period_type и period_label.

    ⚠️ МЕСЯЦ ОБЯЗАТЕЛЕН В МЕТКЕ, И ЭТО НЕ ПЕДАНТИЗМ. У FM период «6M» встречается
    ДВАЖДЫ в году: с month=6 (первое полугодие) и month=12 (второе). Первая версия
    склеивала их в одну метку — и 1 830 значений на трёх компаниях молча перезаписали
    друг друга. Заметно это было только по счётчику «изменилось» на ПУСТОЙ таблице:
    изменяться там было нечему.

    Метки: 2025 · 2025Q3 · 2025H1 · 2025H2 · 2025-9M · LTM-2026-06.
    """
    p = (r.get("period") or "").upper()
    y = r.get("year")
    try:
        m = int(r.get("month") or 0)
    except (TypeError, ValueError):
        m = 0
    if p == "Y":
        return "year", str(y)
    if p == "YTM":
        # Скользящие 12 месяцев — их аналог нашего LTM. Год и месяц в метке, иначе
        # все годы схлопнутся в одну строку «LTM».
        return "ltm", "LTM-%s-%02d" % (y, m)
    if p == "Q":
        return "quarter", "%sQ%d" % (y, (m + 2) // 3 if m else 0)
    if p == "6M":
        return "quarter", "%sH%d" % (y, 2 if m >= 12 else 1)
    if p:
        return "quarter", "%s-%s" % (y, p)
    return "year", str(y)


def загрузить_компанию(db, secid: str, payload: dict, today: date) -> dict:
    итог = {"метрик": 0, "новых": 0, "изменилось": 0, "акционеров": 0,
            "дивидендов": 0, "документов": 0, "столкновений": 0}
    # ⚠️ СТОРОЖ ПРОТИВ СТОЛКНОВЕНИЙ. Если два разных периода из одного ответа дают
    # одну метку, второй молча затрёт первый — ровно это и случилось с «6M». Ошибку
    # видно только здесь: в базе останется правдоподобное значение не за тот период.
    ключи_прогона = set()

    # ── показатели: reports + ratios, коды родные
    for раздел in ("reports", "ratios"):
        for r in (payload.get(раздел) or []):
            ptype, plabel = период_в_метку(r)
            std = (r.get("type") or "").upper() or "NA"
            for код, знач in r.items():
                if код in НЕ_ПОКАЗАТЕЛИ or знач is None or знач == "":
                    continue
                try:
                    v = float(знач)
                except (TypeError, ValueError):
                    continue
                ключ = (secid, код, std, ptype, plabel)
                if ключ in ключи_прогона:
                    log.error("СТОЛКНОВЕНИЕ МЕТОК: %s %s %s %s — два периода из одного "
                              "ответа дали одну метку", secid, код, std, plabel)
                    итог["столкновений"] += 1
                ключи_прогона.add(ключ)

                cur = db.execute(text("""
                    SELECT id, value FROM company_metrics
                    WHERE secid=:s AND metric_code=:c AND standard=:st
                      AND period_type=:pt AND period_label=:pl AND source=:src
                    ORDER BY first_seen DESC LIMIT 1
                """), {"s": secid, "c": код, "st": std, "pt": ptype,
                       "pl": plabel, "src": ИСТОЧНИК}).fetchone()
                if cur and cur[1] is not None and abs(float(cur[1]) - v) < 1e-9:
                    db.execute(text("UPDATE company_metrics SET last_seen=:d WHERE id=:i"),
                               {"d": today, "i": cur[0]})
                    итог["метрик"] += 1
                    continue
                db.execute(text("""
                    INSERT INTO company_metrics (secid, metric_code, standard, period_type,
                        period_label, value, source, first_seen, last_seen)
                    VALUES (:s,:c,:st,:pt,:pl,:v,:src,:d,:d)
                    ON CONFLICT (secid, metric_code, standard, period_type, period_label,
                                 source, first_seen)
                    DO UPDATE SET value=EXCLUDED.value, last_seen=EXCLUDED.last_seen
                """), {"s": secid, "c": код, "st": std, "pt": ptype, "pl": plabel,
                       "v": v, "src": ИСТОЧНИК, "d": today})
                итог["изменилось" if cur else "новых"] += 1
                итог["метрик"] += 1

    # ── акционеры: у FM с годом и месяцем, то есть ИСТОРИЯ, а не один снимок
    for o in (payload.get("owners") or []):
        доля = o.get("own")
        try:
            доля = float(доля)
        except (TypeError, ValueError):
            доля = None
        y, m = o.get("year"), o.get("month")
        снимок = None
        if y:
            try:
                снимок = date(int(y), int(m or 12), 1)
            except (TypeError, ValueError):
                снимок = None
        db.execute(text("""
            INSERT INTO company_shareholders (issuer_id, holder, share_pct,
                   structure_as_of, source)
            SELECT s.issuer_id, :h, :p, :d, :src FROM issuer_securities s WHERE s.secid=:sec
            ON CONFLICT (issuer_id, holder, COALESCE(structure_as_of, DATE '0001-01-01'))
            DO UPDATE SET share_pct=EXCLUDED.share_pct, updated_at=now()
        """), {"h": (o.get("owner") or "")[:200], "p": доля, "d": снимок,
               "src": ИСТОЧНИК, "sec": secid})
        итог["акционеров"] += 1

    # ── документы: у КАЖДОГО отчёта ссылка на PDF через их CDN
    for r in (payload.get("reports") or []):
        for поле, тип in (("link", "financial_report"), ("link_press", "presentation")):
            u = r.get(поле)
            if not u or not u.startswith("http"):
                continue
            db.execute(text("""
                INSERT INTO company_documents (issuer_id, doc_type, period, url, source)
                SELECT s.issuer_id, :t, :p, :u, :src FROM issuer_securities s WHERE s.secid=:sec
                ON CONFLICT DO NOTHING
            """), {"t": тип, "p": str(r.get("year") or "")[:12], "u": u,
                   "src": ИСТОЧНИК, "sec": secid})
            итог["документов"] += 1
    return итог


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="сколько компаний (для пробы)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--once", action="store_true", help="совместимость с оркестратором")
    ap.add_argument("--force", action="store_true", help="совместимость с оркестратором")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()
    setup_logging(a.verbose)

    engine = create_engine(os.environ["DB_URL"])
    today = date.today()
    начало = time.time()
    итог = {"компаний": 0, "метрик": 0, "новых": 0, "изменилось": 0,
            "акционеров": 0, "документов": 0, "пусто": 0, "упало": 0,
            "столкновений": 0}
    остановлен = None

    with engine.begin() as db:
        client = FMClient(db=db)
        остаток = client.остаток()
        компании = [r[0] for r in db.execute(text("""
            SELECT s.secid FROM issuer_securities s
            WHERE s.share_class = 'common' ORDER BY s.secid""")).fetchall()]
        if a.limit:
            компании = компании[:a.limit]

        log.info("остаток запросов: %d · компаний к обходу: %d", остаток, len(компании))
        # ⚠️ Проверяем ДО начала, а не по ходу. Упереться в лимит на середине значит
        # получить половину компаний обновлённой, а половину нет — молча, без ошибок.
        if остаток < len(компании) + ЗАПАС_ЗАПРОСОВ:
            сообщение = ("⛔️ Карточки FinanceMarker: обход НЕ запущен. Остаток %d, "
                         "нужно %d (+%d запаса)." % (остаток, len(компании), ЗАПАС_ЗАПРОСОВ))
            log.error(сообщение)
            notify(сообщение)
            print(json.dumps({"остановлен": "лимит", "остаток": остаток}, ensure_ascii=False))
            return

        for i, secid in enumerate(компании, 1):
            try:
                payload = client.get("stocks/MOEX:%s" % secid, {"include": РАЗДЕЛЫ},
                                     doc_key="stocks/MOEX:%s" % secid)
            except ЛимитИсчерпан as ex:
                остановлен = "лимит исчерпан на %s (%d из %d)" % (secid, i, len(компании))
                log.error(остановлен)
                break
            except ИсточникПросилУйти as ex:
                остановлен = "источник попросил остановиться на %s: %s" % (secid, ex)
                log.error(остановлен)
                break
            except Exception:
                log.exception("%s: запрос не удался", secid)
                итог["упало"] += 1
                continue

            if not (payload.get("reports") or payload.get("ratios")):
                log.warning("%s: ответ пришёл, но отчётности в нём нет", secid)
                итог["пусто"] += 1
                continue

            try:
                r = загрузить_компанию(db, secid, payload, today)
            except Exception:
                log.exception("%s: данные получены, но не загружены", secid)
                итог["упало"] += 1
                continue

            итог["компаний"] += 1
            for k in ("метрик", "новых", "изменилось", "акционеров", "документов",
                      "столкновений"):
                итог[k] += r.get(k, 0)
            if i % 20 == 0:
                log.info("%d/%d · %.0f сек · метрик %d", i, len(компании),
                         time.time() - начало, итог["метрик"])

        if a.dry_run:
            db.rollback()
            log.info("DRY-RUN, откат")

    заняло = time.time() - начало
    log.info("ГОТОВО за %.0f сек: %s", заняло, json.dumps(итог, ensure_ascii=False))

    строки = ["📊 Карточки FinanceMarker за %d мин" % max(1, round(заняло / 60)),
              "Компаний: %d · метрик: %d (новых %d, изменилось %d)"
              % (итог["компаний"], итог["метрик"], итог["новых"], итог["изменилось"]),
              "Акционеров: %d · документов: %d" % (итог["акционеров"], итог["документов"])]
    if остановлен:
        строки.insert(0, "⛔️ ПРОГОН ОСТАНОВЛЕН: %s" % остановлен)
    if итог["упало"] or итог["пусто"] or итог["столкновений"]:
        строки.append("⚠️ Упало: %d, пусто: %d, столкновений меток: %d"
                      % (итог["упало"], итог["пусто"], итог["столкновений"]))
    # Тихий успех не спамим: уведомление имеет смысл, когда что-то не так.
    if остановлен or итог["упало"] or итог["пусто"] or итог["столкновений"]:
        notify("\n".join(строки))
    else:
        log.info("уведомление не требуется — прогон чистый")

    print(json.dumps(итог, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
