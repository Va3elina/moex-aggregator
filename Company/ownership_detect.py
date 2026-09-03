#!/usr/bin/env python3
"""
Детектор смены владения: ищет в архиве новостей поводы посмотреть на граф.

⚠️ ЭТО НЕ ДОБЫЧА РЁБЕР. Скрипт НИЧЕГО не пишет в world_facts ни при каком уровне
уверенности. Он наполняет очередь ownership_signals — «сходи посмотри». Причина в
данных: дочерняя компания в новостях почти никогда не названа тикером («дочерней
компании Новатэка», «ООО Башенная»), и попытка вывести ребро автоматически даёт
уверенный мусор. Ложное ребро дороже отсутствующего: агент объяснит движение одной
бумаги событиями чужой компании и сделает это убедительно.

ДВА ФИЛЬТРА, и они про разное:
  строгий — формулировки существенного факта по 714-П («право распоряжаться»,
            «голосующих акций»). Почти всегда настоящее раскрытие. 174 сообщения
            за всю историю архива, 91 за полгода;
  широкий — доля/пакет/акционер рядом с продал/купил/залог. Ловит и события, и
            планы, и слухи: «Минфин планирует продать госдолю в НМТП» — не факт,
            но повод. 1 564 за историю, 133 за полгода.

Использование:
    python ownership_detect.py --days 7          # ежедневный проход
    python ownership_detect.py --days 3650       # разовый разбор архива
    python ownership_detect.py --days 7 --dry-run
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
log = logging.getLogger("ownership_detect")

# ⚠️ Полнотекст по-русски, а не ILIKE: словоформы («долю», «доли», «долей») иначе
# пришлось бы перечислять руками, а GIN-индекс to_tsvector уже есть.
_ШИРОКИЙ = "(долю | доли | пакет | акционер) & (продал | продаёт | купил | приобрёл | залог | выкуп)"
_СТРОГИЙ = "(text ILIKE '%право распоряжаться%' OR text ILIKE '%голосующих акци%')"

# Процент в тексте отличает «сократил с 0,3625% до 0,3409%» от «планирует продать».
_ПРОЦЕНТ = re.compile(r"\d{1,3}([.,]\d+)?\s?%")

_SELECT = text("""
    SELECT channel, message_id, posted_at, tickers,
           left(regexp_replace(text, E'\\\\s+', ' ', 'g'), 400) AS snippet,
           CASE WHEN {strict} THEN 'строгий' ELSE 'широкий' END AS strength
    FROM news_archive
    WHERE tickers <> '{{}}'
      AND posted_at >= now() - CAST(:days || ' days' AS interval)
      AND (to_tsvector('russian', text) @@ to_tsquery('russian', :broad) OR {strict})
    ORDER BY posted_at DESC
""".format(strict=_СТРОГИЙ))

# ⚠️ СХЛОПЫВАНИЕ ПОВТОРОВ. Раскрытия приходят пачками: МГКЛ 24.08.2026 дал ШЕСТЬ
# почти одинаковых сообщений за день, и очередь заставила бы человека прочитать
# шесть копий одного события. Ключ схлопывания — компания + день + начало текста:
# разные события одной компании в один день начинаются по-разному, а копии — нет.
# Уникальность по (channel, message_id) от этого не спасает: сообщения РАЗНЫЕ.
_DUP = text("""
    SELECT 1 FROM ownership_signals
    WHERE tickers = CAST(:tk AS text[])
      AND posted_at::date = CAST(:d AS date)
      AND left(snippet, 80) = :head
    LIMIT 1
""")

_INSERT = text("""
    INSERT INTO ownership_signals (channel, message_id, posted_at, tickers, snippet,
                                   strength, has_percent, edge_state)
    VALUES (:ch, :mid, :at, CAST(:tk AS text[]), :sn, :st, :pc, :es)
    ON CONFLICT (channel, message_id) DO NOTHING
""")


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    day = date.today().strftime("%Y%m")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    for path, level in ((LOG_DIR / f"ownership_detect_{day}.log", logging.DEBUG),
                        (LOG_DIR / f"ownership_detect_errors_{day}.log", logging.WARNING)):
        h = logging.FileHandler(path, encoding="utf-8")
        h.setLevel(level); h.setFormatter(fmt); log.addHandler(h)
    c = logging.StreamHandler(sys.stderr)
    c.setLevel(logging.INFO); c.setFormatter(fmt); log.addHandler(c)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--once", action="store_true", help="совместимость с оркестратором")
    ap.add_argument("--force", action="store_true", help="совместимость с оркестратором")
    a = ap.parse_args()
    setup_logging()

    engine = create_engine(os.environ["DB_URL"])
    with engine.begin() as db:
        # Тикеры, у которых связь уже есть: сигнал по ним — скорее об ИЗМЕНЕНИИ
        # доли, а не о новой связи. Разные вещи и разная срочность.
        с_ребром = {r[0] for r in db.execute(text(
            "SELECT DISTINCT unnest(entities) FROM world_facts WHERE kind = 'связь'"
        )).fetchall()}

        rows = db.execute(_SELECT, {"days": a.days, "broad": _ШИРОКИЙ}).fetchall()
        итог = {"найдено": len(rows), "новых": 0, "строгих": 0, "с_процентом": 0,
                "повторов": 0}

        for ch, mid, at, tickers, snippet, strength in rows:
            есть = bool(set(tickers or []) & с_ребром)
            pc = bool(_ПРОЦЕНТ.search(snippet or ""))

            if db.execute(_DUP, {"tk": list(tickers or []), "d": at.date(),
                                 "head": (snippet or "")[:80]}).first():
                итог["повторов"] += 1
                continue
            res = db.execute(_INSERT, {
                "ch": ch, "mid": mid, "at": at, "tk": list(tickers or []),
                "sn": snippet, "st": strength, "pc": pc,
                "es": "есть_ребро" if есть else "нет_ребра",
            })
            if res.rowcount:
                итог["новых"] += 1
                итог["строгих"] += (strength == "строгий")
                итог["с_процентом"] += pc

        if a.dry_run:
            db.rollback()
            log.info("DRY-RUN, откат")

    log.info("итог: %s", json.dumps(итог, ensure_ascii=False))
    print(json.dumps(итог, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
