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

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR.parents[1]))   # чтобы был виден пакет signals/

from parse_smartlab_company import ThrottledError, parse_company, setup_logging


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
    ap.add_argument("--draft", type=Path, default=BASE_DIR / "issuer_draft.json")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--cache-dir", type=Path)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--skip-existing", action="store_true",
                    help="не перекачивать то, что уже сохранено сегодня")
    a = ap.parse_args()
    setup_logging()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    rows = json.loads(a.draft.read_text(encoding="utf-8"))["кандидаты"]
    tickers = sorted({r["smartlab_ticker"] for r in rows if r.get("smartlab_ticker")})
    if a.limit:
        tickers = tickers[:a.limit]

    started = time.time()
    итог = {"успех": 0, "пусто": 0, "упало": 0, "метрик": 0, "документов": 0}
    сломались = []

    log.info("прогон вселенной: %d компаний", len(tickers))
    for i, t in enumerate(tickers, 1):
        dest = a.out_dir / f"{t}.json"
        if a.skip_existing and dest.exists():
            continue
        try:
            card = parse_company(t, ("MSFO", "RSBU"), a.cache_dir)
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
        if not filled:
            log.warning("%s: ни одного заполненного значения", t)
            итог["пусто"] += 1
        else:
            итог["успех"] += 1
        итог["метрик"] += filled
        итог["документов"] += len(card["documents"])
        dest.write_text(json.dumps(card, ensure_ascii=False), encoding="utf-8")

        if i % 10 == 0:
            прошло = time.time() - started
            log.info("%d/%d, %.0f сек, метрик %d", i, len(tickers), прошло, итог["метрик"])

    заняло = time.time() - started
    log.info("ГОТОВО за %.0f сек: %s", заняло, json.dumps(итог, ensure_ascii=False))
    if сломались:
        log.error("не разобраны: %s", ", ".join(сломались))

    строки = ["📇 Карточки компаний: прогон закончен за %d мин" % round(заняло / 60),
              "Успешно: %d, пусто: %d, упало: %d" % (итог["успех"], итог["пусто"], итог["упало"]),
              "Метрик: %d, документов: %d" % (итог["метрик"], итог["документов"])]
    if сломались:
        строки.append("⚠️ Не разобраны: " + ", ".join(сломались))
    if итог["упало"] or итог["пусто"] or "остановлен_на" in итог:
        notify("\n".join(строки))
    else:
        # Тихий успех не спамим: уведомление имеет смысл, только когда что-то не так
        # или когда прогон прошёл впервые.
        log.info("уведомление не требуется — прогон чистый")
    print(json.dumps(итог, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
