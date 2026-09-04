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

from fm_client import (FMClient, ДОЛЯ_ДРУГИМ, ЗАПАС_ЗАПРОСОВ,  # noqa: E402
                       СУТОЧНАЯ_КВОТА, ЦЕНА_ВЫЗОВА, ИсточникПросилУйти,
                       ЛимитИсчерпан, log, setup_logging)

ИСТОЧНИК = "financemarker"
РАЗДЕЛЫ = "info,reports,ratios,dividends,owners,shares,summary,operations"

# Поля отчёта, которые НЕ являются показателями: служебные и текстовые. Всё
# остальное из 99 полей уезжает в company_metrics как есть.
НЕ_ПОКАЗАТЕЛИ = {"code", "exchange", "year", "month", "period", "type", "curr",
                 "amount", "changed_at", "link", "link_press", "link_update",
                 "preliminary"}


def notify(текст: str) -> bool:
    """
    Пуш в @frameadminbot — та же связка, что у mandate_scan и tg_bot.

    ⚠️ ЗДЕСЬ БЫЛО ДВЕ ОШИБКИ СРАЗУ, И ОБЕ МОЛЧАЛИ. Переменную звали
    FRAME_ADMIN_CHAT_ID — такой в проекте нет вообще, настоящая ADMIN_CHAT_ID; а
    отправка шла через `from signals.alert_notify`, которого нет ни в одном образе
    (signals/ живёт на хосте под cron). То есть уведомление не ушло бы, даже если бы
    переменную назвали верно, — и об этом сообщала строка уровня INFO, которую никто
    не читает. Поэтому теперь: отсутствие настроек — ERROR, а не INFO.

    ⚠️ ТОЛЬКО ЧЕРЕЗ TELEGRAM_API_ROOT. Прямой api.telegram.org с прод-сервера
    закрыт (РКН, инцидент 2026-07-15) — без релея запрос просто виснет.
    """
    token = os.environ.get("BOT_TOKEN", "")
    chat = os.environ.get("ADMIN_CHAT_ID", "")
    if not token or not chat:
        log.error("УВЕДОМЛЕНИЕ НЕ УШЛО (нет BOT_TOKEN/ADMIN_CHAT_ID): %s",
                  текст.replace("\n", " ")[:200])
        return False
    api_root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
    try:
        import requests
        r = requests.post("%s/bot%s/sendMessage" % (api_root, token),
                          json={"chat_id": chat, "text": текст[:4000],
                                "parse_mode": "HTML", "disable_web_page_preview": True},
                          timeout=15)
        r.raise_for_status()
        # ⚠️ Пишем и УСПЕХ тоже. Без этой строки лог молчит одинаково и когда пуш
        # ушёл, и когда его не было вовсе, — а «тихий успех» неотличим от «тихого
        # ничего», и именно так уведомления в этом проекте уже терялись.
        log.info("уведомление отправлено в ТГ")
        return True
    except Exception as ex:
        # Токен в текст ошибки не попадает: он в URL, а мы печатаем только код.
        log.error("УВЕДОМЛЕНИЕ НЕ УШЛО (%s): %s", type(ex).__name__,
                  текст.replace("\n", " ")[:200])
        return False


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
        # ⚠️ МЕСЯЦ В МЕТКЕ ОБЯЗАТЕЛЕН. У ЕвроТранса за первое полугодие 2023 ДВЕ
        # записи: 6M с месяцем 5 и с месяцем 6, выручка отличается на 1000 — правка,
        # записанная источником дважды. Схлопывание по «месяц < 12 → H1» теряло одну
        # из них молча: 86 столкновений на одной компании. Канонический месяц (6 и 12)
        # даёт красивую метку, любой другой — с пометкой, чтобы аномалия была видна.
        if m in (6, 12):
            return "quarter", "%sH%d" % (y, 2 if m == 12 else 1)
        return "quarter", "%sH?-м%d" % (y, m)
    if p:
        return "quarter", "%s-%s" % (y, p)
    return "year", str(y)


def загрузить_компанию(db, secid: str, payload: dict, today: date) -> dict:
    итог = {"метрик": 0, "новых": 0, "изменилось": 0, "акционеров": 0,
            "дивидендов": 0, "документов": 0, "столкновений": 0,
            "операционных": 0, "инсайдеров": 0, "сводок": 0, "акций": 0}
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

    # ── операционные показатели: своя таблица, потому что свои единицы
    for o in (payload.get("operations") or []):
        ptype, plabel = период_в_метку(o)
        try:
            v = float(o.get("value")) if o.get("value") is not None else None
        except (TypeError, ValueError):
            v = None
        try:
            ov = float(o.get("original_value")) if o.get("original_value") is not None else None
        except (TypeError, ValueError):
            ov = None
        db.execute(text("""
            INSERT INTO company_operations (secid, metric_id, period_type, period_label,
                   value, unit, orig_value, orig_unit, source, first_seen, last_seen)
            VALUES (:s,:m,:pt,:pl,:v,:u,:ov,:ou,:src,:d,:d)
            ON CONFLICT (secid, metric_id, period_type, period_label, source, first_seen)
            DO UPDATE SET value=EXCLUDED.value, last_seen=EXCLUDED.last_seen
        """), {"s": secid, "m": str(o.get("operation_metric_id") or "")[:24],
               "pt": ptype, "pl": plabel, "v": v, "u": (o.get("unit") or "")[:40],
               "ov": ov, "ou": (o.get("original_unit") or "")[:40],
               "src": ИСТОЧНИК, "d": today})
        итог["операционных"] += 1

    # ── сделки инсайдеров: с долей ДО и ПОСЛЕ — этого нет ни в одном другом источнике
    for t in (payload.get("insiderTransactions") or []):
        d0 = t.get("transaction_date")
        if not d0:
            continue
        def _f(x):
            try:
                return float(x)
            except (TypeError, ValueError):
                return None
        db.execute(text("""
            INSERT INTO company_insider_trades (secid, transaction_date, filling_date,
                   insider, insider_role, insider_title, transaction_type, amount, price,
                   value, own_before, own_after, market_trade, approximate, link, source)
            VALUES (:s,:td,:fd,:i,:ir,:it,:tt,:a,:p,:v,:ob,:oa,:mt,:ap,:l,:src)
            ON CONFLICT (secid, transaction_date, insider, transaction_type)
            DO UPDATE SET amount=EXCLUDED.amount, own_after=EXCLUDED.own_after,
                          updated_at=now()
        """), {"s": secid, "td": d0[:10], "fd": (t.get("filling_date") or None),
               "i": (t.get("insider") or "?")[:300], "ir": (t.get("insider_role") or "")[:120],
               "it": (t.get("insider_title") or "")[:200],
               "tt": (t.get("transaction_type") or "")[:40],
               "a": _f(t.get("amount")), "p": _f(t.get("price")), "v": _f(t.get("value")),
               "ob": _f(t.get("own_before")), "oa": _f(t.get("own_after")),
               "mt": t.get("market_trade"), "ap": t.get("approximate"),
               "l": t.get("link"), "src": ИСТОЧНИК})
        итог["инсайдеров"] += 1

    # ── сводка: снимок, а не ряд (рост за 5 лет пересчитывается каждый раз заново)
    сводка = payload.get("summary")
    if сводка:
        db.execute(text("""
            INSERT INTO company_summary (secid, payload, source, updated_at)
            VALUES (:s, CAST(:p AS jsonb), :src, now())
            ON CONFLICT (secid) DO UPDATE SET payload=EXCLUDED.payload, updated_at=now()
        """), {"s": secid, "p": json.dumps(сводка, ensure_ascii=False), "src": ИСТОЧНИК})
        итог["сводок"] += 1

    # ── история числа акций: делает сопоставимыми показатели «на акцию»
    for sh in (payload.get("shares") or []):
        try:
            y, m2 = int(sh.get("year")), int(sh.get("month") or 12)
        except (TypeError, ValueError):
            continue
        try:
            num = int(float(sh.get("num"))) if sh.get("num") is not None else None
        except (TypeError, ValueError):
            num = None
        db.execute(text("""
            INSERT INTO company_shares (secid, year, month, num, source)
            VALUES (:s,:y,:m,:n,:src)
            ON CONFLICT (secid, year, month, source)
            DO UPDATE SET num=EXCLUDED.num, updated_at=now()
        """), {"s": secid, "y": y, "m": m2, "n": num, "src": ИСТОЧНИК})
        итог["акций"] += 1

    # ── дивиденды: у FM с 2007 года против 2017 у smart-lab
    for dv in (payload.get("dividends") or []):
        rd = dv.get("reestr_close_date")
        if not rd:
            continue
        def _f2(x):
            try:
                return float(x)
            except (TypeError, ValueError):
                return None
        db.execute(text("""
            INSERT INTO company_dividends (secid, period, record_date, dividend, price,
                   div_yield, source)
            VALUES (:s,:p,:rd,:d,:px,:y,:src)
            ON CONFLICT (secid, record_date, period)
            DO UPDATE SET dividend=EXCLUDED.dividend, div_yield=EXCLUDED.div_yield,
                          updated_at=now()
        """), {"s": secid, "p": str(dv.get("year") or "")[:40], "rd": rd[:10],
               "d": _f2(dv.get("div_amount")), "px": _f2(dv.get("last_buy_price")),
               "y": _f2(dv.get("div_percent")), "src": ИСТОЧНИК})
        итог["дивидендов"] += 1

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
    ap.add_argument("--dry-run", action="store_true",
                    help="показать план и выйти, НЕ ходя в сеть (ни одного платного запроса)")
    ap.add_argument("--once", action="store_true", help="совместимость с оркестратором")
    ap.add_argument("--force", action="store_true", help="совместимость с оркестратором")
    ap.add_argument("--все", dest="все", action="store_true",
                    help="перезапросить всех, не глядя на свежесть карточки")
    ap.add_argument("--свежесть", dest="свежесть", type=int, default=7,
                    help="сколько дней карточка считается свежей (по умолчанию 7)")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()
    setup_logging(a.verbose)

    engine = create_engine(os.environ["DB_URL"])
    today = date.today()
    начало = time.time()
    итог = {"компаний": 0, "метрик": 0, "новых": 0, "изменилось": 0,
            "акционеров": 0, "документов": 0, "пусто": 0, "упало": 0,
            "столкновений": 0, "операционных": 0, "инсайдеров": 0,
            "сводок": 0, "акций": 0, "дивидендов": 0}
    остановлен = None

    # ⚠️ ТРАНЗАКЦИЯ НА КОМПАНИЮ, А НЕ НА ОБХОД. Первая версия держала одну транзакцию
    # на все 123 компании: 700 тысяч вставок, пятнадцать минут блокировок, и — главное —
    # падение на сотой откатило бы все девяносто девять. Повезло, что первый прогон
    # остановился штатно. Ровно эту защиту я закладывал в smart-лабовский фетчер
    # («грузим сразу, чтобы прерывание не обесценило собранное») и не перенёс сюда.
    with engine.connect() as db:
        client = FMClient(db=db, engine=engine)
        # ⚠️ У ИСТОЧНИКА ОСТАТОК НЕ СПРАШИВАЕМ. Во-первых, сам вопрос стоит запроса.
        # Во-вторых, его day_limit показал 388 при пустом пуле — на этом числе
        # 04.09.2026 и была потрачена вся месячная квота. Считаем по api_budget.
        # Потолок месяца хранится в БД (это запись, а не догадка), но сменить тариф
        # должно быть можно переменной окружения, а не ручным SQL по проду.
        if os.environ.get("FM_СУТ_КВОТА") or os.environ.get("FM_DAILY_QUOTA"):
            client.установить_квоту(СУТОЧНАЯ_КВОТА)
            log.info("потолок суток задан окружением: %d", СУТОЧНАЯ_КВОТА)
        списано, квота = client.бюджет()
        db.commit()

        # ⚠️ ПОРЯДОК — ПО ЗАПУЩЕННОСТИ, А НЕ ПО АЛФАВИТУ. Бюджета хватает не на всех,
        # значит порядок решает, кого мы обновим, а кого нет. По алфавиту это значило
        # бы вечно свежий AFLT и вечно протухший YDEX. Сначала те, у кого карточки нет
        # вовсе, потом самые давно не обновлявшиеся.
        компании = [r[0] for r in db.execute(text("""
            SELECT s.secid
              FROM issuer_securities s
              LEFT JOIN (
                   SELECT secid, MAX(last_seen) AS свежесть
                     FROM company_metrics WHERE source = 'financemarker'
                    GROUP BY secid
              ) m ON m.secid = s.secid
             WHERE s.share_class = 'common'
             ORDER BY m.свежесть ASC NULLS FIRST, s.secid""")).fetchall()]
        # ⚠️ ОТБОР ПО СВЕЖЕСТИ, А НЕ «ВСЕХ ПОДРЯД». Суточной квоты хватает на полный
        # обход каждый день — но обходить каждый день нечего: отчётность выходит
        # раз в квартал. Ежедневный полный обход сжёг бы 344 из 400 единиц ради
        # неизменившихся чисел и не оставил бы места сканеру раскрытия (96/сутки),
        # ради которого всё и затевалось. Берём тех, чья карточка старше интервала.
        if not a.все:
            свежие = {r[0] for r in db.execute(text("""
                SELECT secid FROM company_metrics
                 WHERE source = 'financemarker'
                 GROUP BY secid
                HAVING MAX(last_seen) > now() - make_interval(days => :д)
            """), {"д": a.свежесть}).fetchall()}
            компании = [c for c in компании if c not in свежие]
            if свежие:
                log.info("свежих (моложе %d дн.), пропускаю: %d", a.свежесть, len(свежие))
        # ⚠️ ВСЯ АРИФМЕТИКА В ЕДИНИЦАХ КВОТЫ, А НЕ В ВЫЗОВАХ. Первая версия вычитала
        # бронь (единицы) из числа вызовов и давала 6 компаний вместо 86 — величины
        # разной размерности выглядят одинаково, пока их не сложишь.
        доступно = квота - ЗАПАС_ЗАПРОСОВ - ДОЛЯ_ДРУГИМ - списано
        по_карману = int(доступно // ЦЕНА_ВЫЗОВА) if доступно > 0 else 0

        # ⚠️ ДОЛЮ ОТ СУТОЧНОГО ЛИМИТА НЕ РЕЖЕМ. Квота обнуляется каждые сутки, значит
        # растягивать обход не на что: то, что не взято сегодня, не «экономится» —
        # оно просто сгорает в полночь. Ограничение одно: доля, оставленная другим
        # потребителям той же квоты (сканер раскрытия, ручные вызовы).
        порция = по_карману
        if a.limit:
            порция = a.limit
        log.info("бюджет за сегодня: списано %.0f из %d (бронь другим %d, запас %d) "
                 "· хватит на %d компаний · кандидатов %d",
                 списано, квота, ДОЛЯ_ДРУГИМ, ЗАПАС_ЗАПРОСОВ, по_карману, len(компании))

        if not компании:
            log.info("нечего делать: все компании уже загружены")
            print(json.dumps({"обход": "нечего делать"}, ensure_ascii=False))
            return

        if по_карману <= 0:
            сообщение = ("⛔️ Карточки FinanceMarker: обход НЕ запущен. Суточный бюджет "
                         "исчерпан: списано %.0f из %d." % (списано, квота))
            log.error(сообщение)
            # ⚠️ ПУШ ТОЛЬКО НА СМЕНУ СОСТОЯНИЯ. Задача стоит в дневном расписании, и
            # пока квота выбрана, она будет упираться в потолок каждый день. Слать
            # об этом пуш ежедневно значит за месяц выдать 27 одинаковых сообщений и
            # приучить пролистывать канал не читая — вместе с теми, что важны.
            if client.состояние_уведомления() != 'исчерпан':
                notify(сообщение)
                client.запомнить_уведомление('исчерпан')
            else:
                log.info("о исчерпании бюджета уже сообщали — пуш не повторяю")
            print(json.dumps({"остановлен": "бюджет", "списано": списано}, ensure_ascii=False))
            return

        # ⚠️ НЕ ОТКАЗЫВАЕМСЯ ЦЕЛИКОМ, ЕСЛИ ХВАТАЕТ НЕ НА ВСЕХ. Раньше здесь стоял
        # отказ «нужно N, есть меньше» — при платной квоте это значит не забрать
        # ничего. Берём сколько влезает, остальные останутся на следующий прогон:
        # дозаливка выше сама их подхватит.
        # ⚠️ --dry-run НЕ ХОДИТ В СЕТЬ. Раньше он отключал только запись в БД, а
        # запросы всё равно уходили — то есть «проба» стоила денег ровно столько же,
        # сколько боевой прогон. Для платного источника это ловушка в чистом виде.
        if a.dry_run:
            план = компании[:min(порция, по_карману)] if по_карману > 0 else []
            log.info("ПЛАН (сеть не трогаем): взяли бы %d — %s",
                     len(план), ", ".join(план[:15]) + ("…" if len(план) > 15 else ""))
            print(json.dumps({"план": план, "списано": списано, "квота": квота},
                             ensure_ascii=False))
            return

        # ⚠️ ТОЛЬКО ЗДЕСЬ, ПОСЛЕ --dry-run. Первая версия слала «бюджет снова есть» и
        # переписывала состояние ДО проверки на пробу: --dry-run отправлял пуш и врал
        # о том, что докачка идёт. Проба не имеет права менять ничего вообще.
        if client.состояние_уведомления() == 'исчерпан':
            notify("🟢 Карточки FinanceMarker: суточный бюджет обновился, докачка идёт "
                   "(списано %.0f из %d)." % (списано, квота))
            client.запомнить_уведомление('ок')

        порция = min(порция, по_карману)
        if порция < len(компании):
            log.info("беру %d из %d (самые давние) — остальные в следующие прогоны",
                     порция, len(компании))
            компании = компании[:порция]

        for i, secid in enumerate(компании, 1):
            try:
                client.проверить_бюджет()   # штатная остановка вместо 403 от источника
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
                if a.dry_run:
                    db.rollback()
                else:
                    db.commit()   # компания закончена — она в базе, что бы ни было дальше
            except Exception:
                db.rollback()
                log.exception("%s: данные получены, но не загружены", secid)
                итог["упало"] += 1
                continue

            итог["компаний"] += 1
            for k in ("метрик", "новых", "изменилось", "акционеров", "документов",
                      "столкновений", "операционных", "инсайдеров", "сводок",
                      "акций", "дивидендов"):
                итог[k] += r.get(k, 0)
            if i % 20 == 0:
                log.info("%d/%d · %.0f сек · метрик %d", i, len(компании),
                         time.time() - начало, итог["метрик"])

        if a.dry_run:
            log.info("DRY-RUN: ничего не записано")

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
