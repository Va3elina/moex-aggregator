#!/usr/bin/env python3
"""
Прогон парсера по всей вселенной: карточка каждой компании → отдельный JSON.

Это будущий ежедневный фетчер, поэтому устроен как фетчеры проекта: один сбой не роняет
прогон, всё пишется в лог, повтор безопасен. На каждую компанию — 7 страниц smart-lab
(годовая и квартальная МСФО, годовая и квартальная РСБУ, архив документов, дивиденды,
акционеры), между запросами вежливая пауза.

Использование:
    python parse_universe.py --out-dir cards/
    python parse_universe.py --out-dir cards/ --cache-dir cache/ --limit 5
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR.parents[1]))   # чтобы был виден пакет signals/

from smartlab_parser import ThrottledError, parse_company, setup_logging
from load_card import load_card


def notify(text: str):
    """
    Уведомление в Telegram тем же каналом, что и весь проект (signals/alert_notify,
    ходит через CF-релей — прямой Telegram с сервера закрыт с 11.08.2026).
    Молча пропускается, если токен или чат не заданы: прогон данных не должен падать
    из-за того, что некому написать.
    """
    chat = os.getenv("FRAME_ADMIN_CHAT_ID")
    if not chat:
        log.info("уведомление не отправлено (FRAME_ADMIN_CHAT_ID не задан): %s",
                 text.replace("\n", " ")[:160])
        return
    try:
        from signals.alert_notify import send_message
        send_message(chat, text)
    except Exception as ex:
        log.warning("уведомление в Telegram не ушло: %s", ex)

log = logging.getLogger("smartlab_parser")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--draft", type=Path, default=BASE_DIR / "issuer_universe.json")
    ap.add_argument("--out-dir", type=Path,
                    help="куда класть разобранные карточки; без --no-load не обязателен")
    ap.add_argument("--no-load", action="store_true",
                    help="только разобрать, в базу не грузить (отладка)")
    ap.add_argument("--cache-dir", type=Path)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--light", action="store_true",
                    help="только дивиденды и акционеры (ежедневный проход, 2 страницы из 7)")
    ap.add_argument("--once", action="store_true", help="совместимость с оркестратором")
    ap.add_argument("--force", action="store_true", help="совместимость с оркестратором")
    ap.add_argument("--skip-existing", action="store_true",
                    help="не перекачивать то, что уже сохранено сегодня")
    a = ap.parse_args()
    setup_logging()
    if a.out_dir:
        a.out_dir.mkdir(parents=True, exist_ok=True)
    if a.no_load and not a.out_dir:
        sys.exit("--no-load без --out-dir бессмыслен: разобрать и выбросить")

    # Справочник и список компаний читаются из БАЗЫ: источник истины после сида —
    # issuers и issuer_aliases, файл лишь их первое наполнение.
    engine = None if a.no_load else create_engine(os.environ["DB_URL"])

    # ⚠️ СПИСОК КОМПАНИЙ — ИЗ БАЗЫ, А НЕ ИЗ ФАЙЛА. Вселенная расширяется сидом
    # (seed_issuers --from-instruments), и она уже была расширена до 123 эмитентов,
    # когда фетчер продолжал читать замороженный JSON с 80 тикерами: 39 компаний
    # просто не качались. Поймано на первом же ручном прогоне по логу «40/80».
    #
    # Тот же список читает и карта «тикер smart-lab → эмитент» ниже — значит источник
    # у них теперь ОДИН, и разойтись они больше не могут.
    #
    # Файл остаётся запасным путём: пустая база (первый запуск, свежая копия) не
    # должна приводить к прогону по нулю компаний, который выглядит как успех.
    tickers = []
    if engine is not None:
        with engine.connect() as c:
            tickers = sorted({r[0] for r in c.execute(text(
                "SELECT smartlab_ticker FROM issuers WHERE smartlab_ticker IS NOT NULL"
            )).fetchall() if r[0]})
    if not tickers:
        rows = json.loads(a.draft.read_text(encoding="utf-8"))["кандидаты"]
        tickers = sorted({r["smartlab_ticker"] for r in rows if r.get("smartlab_ticker")})
        log.warning("список компаний взят из файла (%d) — в базе справочник пуст",
                    len(tickers))
    if a.limit:
        tickers = tickers[:a.limit]

    смартлаб_к_эмитенту = {}
    if engine is not None:
        with engine.connect() as c:
            смартлаб_к_эмитенту = {r[0]: r[1] for r in c.execute(text(
                "SELECT a.alias_value, i.issuer_key FROM issuer_aliases a "
                "JOIN issuers i USING(issuer_id) WHERE a.alias_type = 'smartlab'"
            )).fetchall()}
        log.info("справочник smart-lab → эмитент: %d записей", len(смартлаб_к_эмитенту))

    started = time.time()
    итог = {"успех": 0, "пусто": 0, "упало": 0, "метрик": 0, "документов": 0,
            "загружено": 0, "не_загружено": 0}
    сломались = []

    log.info("прогон вселенной: %d компаний%s", len(tickers),
             " (лёгкий режим: дивиденды и акционеры)" if a.light else "")
    for i, t in enumerate(tickers, 1):
        dest = (a.out_dir / f"{t}.json") if a.out_dir else None
        if a.skip_existing and dest and dest.exists():
            continue
        try:
            card = parse_company(t, ("MSFO", "RSBU"), a.cache_dir, light=a.light)
        except ThrottledError as ex:
            # ⚠️ НЕ продолжаем обход. Источник попросил остановиться, и долбить его
            # дальше по оставшимся компаниям — прямой путь к бану IP. Один раз мы
            # так уже получили блокировку от MOEX; повторять урок незачем.
            log.error("ПРОГОН ОСТАНОВЛЕН на %s (%d из %d): %s", t, i, len(tickers), ex)
            notify("⛔️ Карточки компаний: прогон остановлен на %s (%d из %d).\n"
                   "smart-lab вернул %s. Обход прекращён, чтобы не получить бан IP."
                   % (t, i, len(tickers), ex))
            итог["остановлен_на"] = t
            break
        except Exception:
            # Одна упавшая компания не должна ронять прогон по остальным 79.
            log.exception("%s: парсер упал", t)
            итог["упало"] += 1
            сломались.append(t)
            continue

        filled = sum(1 for m in card["metrics"] if m["value"] is not None)
        # ⚠️ Критерий успеха зависит от режима. В лёгком проходе метрик НЕТ по
        # замыслу — страницы отчётности не запрашивались. Считать это «пусто»
        # значило бы слать тревогу каждое утро на все 80 компаний, а тревога,
        # которая приходит всегда, перестаёт быть тревогой.
        if a.light:
            есть = bool(card.get("dividend_payments") or card.get("shareholders"))
        else:
            есть = bool(filled)
        if есть:
            итог["успех"] += 1
        else:
            log.warning("%s: страницы отдались, но данных в них нет", t)
            итог["пусто"] += 1
        итог["метрик"] += filled
        итог["документов"] += len(card["documents"])
        if dest:
            dest.write_text(json.dumps(card, ensure_ascii=False), encoding="utf-8")

        if not a.no_load:
            # ⚠️ Грузим СРАЗУ, а не после всего прогона. Если обход прервётся на
            # середине (429 от источника, обрыв сети), уже собранные компании
            # окажутся в базе, а не в подвешенном состоянии «разобрано, но нигде».
            try:
                key = смартлаб_к_эмитенту.get(t)
                if not key:
                    log.warning("%s: нет такого тикера smart-lab в справочнике", t)
                    итог["не_загружено"] += 1
                else:
                    load_card(card, key, a.draft, engine=engine)
                    итог["загружено"] += 1
            except Exception:
                log.exception("%s: карточка разобрана, но не загружена", t)
                итог["не_загружено"] += 1

        if i % 10 == 0:
            прошло = time.time() - started
            log.info("%d/%d, %.0f сек, метрик %d", i, len(tickers), прошло, итог["метрик"])

    заняло = time.time() - started
    log.info("ГОТОВО за %.0f сек: %s", заняло, json.dumps(итог, ensure_ascii=False))
    if сломались:
        log.error("не разобраны: %s", ", ".join(сломались))

    строки = ["📇 Карточки компаний: прогон закончен за %d мин" % round(заняло / 60),
              "Успешно: %d, пусто: %d, упало: %d" % (итог["успех"], итог["пусто"], итог["упало"]),
              "Загружено в базу: %d, не загружено: %d" % (итог["загружено"], итог["не_загружено"]),
              ("Метрик: %d, документов: %d" % (итог["метрик"], итог["документов"])
               if not a.light else "Лёгкий проход: дивиденды и акционеры")]
    if сломались:
        строки.append("⚠️ Не разобраны: " + ", ".join(сломались))
    if итог["упало"] or итог["пусто"] or итог["не_загружено"] or "остановлен_на" in итог:
        notify("\n".join(строки))
    else:
        # Тихий успех не спамим: уведомление имеет смысл, только когда что-то не так
        # или когда прогон прошёл впервые.
        log.info("уведомление не требуется — прогон чистый")
    print(json.dumps(итог, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
