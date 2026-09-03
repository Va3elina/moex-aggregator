#!/usr/bin/env python3
"""
Загрузка рёбер владения в world_facts (kind='связь').

ДВЕ РАЗНЫЕ ДАТЫ, и их нельзя складывать в одно поле:
  • КОГДА ФАКТ СТАЛ ВЕРЕН — про мир, живёт в valid_from. Берётся из строки
    «Дата последнего обновления этой структуры» на странице акционеров;
  • КОГДА МЫ ЕГО УВИДЕЛИ — про нас, нужно для отслеживания изменений. Это
    created_at / updated_at, изобретать ничего не надо.

⚠️ ПОЧЕМУ У РЁБЕР БЕЗ ДАТЫ НЕЛЬЗЯ СТАВИТЬ СЕГОДНЯШНЮЮ. Главный запрос второго мозга
фильтрует `valid_from <= дата_новости`. Сегодняшняя дата спрятала бы факт из ВСЕХ
вопросов о прошлом: спросим «что действовало 24.08.2026» — и связь Газпром → Газпром
нефть не вернётся, хотя она верна пятнадцать лет. Плюс valid_from читается буквально:
агент напишет «Газпром нарастил долю до 95,68%». Незнание кодируется не датой, а
confidence 0.60 и прямым предупреждением в тексте факта.

⚠️ ПИШЕМ ТОЛЬКО ПРИ РЕАЛЬНОМ ИЗМЕНЕНИИ. Если гнать UPDATE на каждом прогоне, updated_at
показывает «когда скрипт последний раз бегал» — и как детектор смены владельца
бесполезен. Неизменившиеся строки не трогаем вовсе.

Использование:
    python ownership_edges.py --all --out edges.json
    python load_ownership_edges.py edges.json --dry-run
    python load_ownership_edges.py edges.json
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
for _p in BASE_DIR.parents:
    if (_p / ".env").exists():
        load_dotenv(_p / ".env")
        break
DB_URL = os.getenv("DB_URL")

# См. блок про даты в докстроке: сюда НЕ ставится сегодняшняя дата.
UNKNOWN_SINCE = date(2020, 1, 1)

# Имена компаний для текста факта. Берутся у ОБЫКНОВЕННОЙ бумаги: у префа подпись
# «Сбербанк (прив)», и владельцем в тексте оказался бы преф, которым никто не владеет.
EXTRA_NAMES = {
    "FEES": "ФСК-Россети", "ENPG": "Эн+", "SFIN": "ЭсЭфАй", "ETLN": "Эталон",
    "OZON": "Озон", "MSRS": "Россети Московский регион", "SGZH": "Сегежа",
    "RTKM": "Ростелеком", "MSNG": "Мосэнерго", "SBER": "Сбербанк", "IRAO": "Интер РАО",
    "HYDR": "РусГидро", "GMKN": "Норникель", "BANE": "Башнефть", "SIBN": "Газпром нефть",
    "RUAL": "Русал", "MTSS": "МТС", "MOEX": "Московская биржа", "VTBR": "ВТБ",
    "GAZP": "Газпром", "ROSN": "Роснефть", "AFKS": "АФК Система",
}

log = logging.getLogger("ownership_loader")


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    day = date.today().strftime("%Y%m")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    for path, level in ((LOG_DIR / f"ownership_load_{day}.log", logging.DEBUG),
                        (LOG_DIR / f"ownership_load_errors_{day}.log", logging.WARNING)):
        h = logging.FileHandler(path, encoding="utf-8")
        h.setLevel(level); h.setFormatter(fmt); log.addHandler(h)
    c = logging.StreamHandler(sys.stderr)
    c.setLevel(logging.INFO); c.setFormatter(fmt); log.addHandler(c)


def build_names(draft_path: Path):
    names = {}
    data = json.loads(draft_path.read_text(encoding="utf-8"))
    for row in data["кандидаты"]:
        if row.get("class") == "common" and row.get("oi_display_name"):
            names.setdefault(row["issuer_key"], row["oi_display_name"])
    names.update(EXTRA_NAMES)
    return names


def statement_for(edge, names):
    pct = ("%.2f" % edge["доля"]).replace(".", ",")
    owner, target = edge["владелец_тикер"], edge["эмитент"]
    when = (("Доля по структуре акционеров, обновлённой %s." % edge["структура_от"])
            if edge["структура_от"] else
            "ВНИМАНИЕ: источник не указал дату обновления структуры, доля может быть устаревшей.")
    return ("%s (#%s) владеет долей %s%% в компании %s (#%s). %s"
            % (names.get(owner, owner), owner, pct, names.get(target, target), target, when))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("edges_json", type=Path)
    ap.add_argument("--draft", type=Path, default=BASE_DIR / "issuer_draft.json")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    setup_logging()

    edges = json.loads(a.edges_json.read_text(encoding="utf-8"))["рёбра"]
    names = build_names(a.draft)
    engine = create_engine(DB_URL)
    stats = {"новых": 0, "изменилось": 0, "без_изменений": 0}

    with engine.begin() as conn:
        for edge in edges:
            key = "own:%s:%s" % (edge["владелец_тикер"], edge["эмитент"])
            as_of = (datetime.strptime(edge["структура_от"], "%d.%m.%Y").date()
                     if edge["структура_от"] else None)
            st = statement_for(edge, names)
            vf = as_of or UNKNOWN_SINCE

            cur = conn.execute(text(
                "SELECT statement, valid_from FROM world_facts WHERE fact_key=:k"),
                {"k": key}).fetchone()
            if cur and cur[0] == st and cur[1] == vf:
                stats["без_изменений"] += 1
                continue
            if cur:
                # Смена доли или даты снимка — событие: возможно, пакет продали.
                log.info("ИЗМЕНЕНИЕ %s\n  было:  %s\n  стало: %s", key, cur[0], st)
                stats["изменилось"] += 1
            else:
                stats["новых"] += 1

            conn.execute(text("""
              INSERT INTO world_facts (fact_key, statement, kind, entities, valid_from,
                                       source, source_url, confidence)
              VALUES (:k,:s,'связь',:ents,:vf,'smartlab',:url,:conf)
              ON CONFLICT (fact_key) DO UPDATE SET statement=EXCLUDED.statement,
                entities=EXCLUDED.entities, valid_from=EXCLUDED.valid_from,
                source_url=EXCLUDED.source_url, confidence=EXCLUDED.confidence,
                updated_at=now()
            """), {"k": key, "s": st,
                   "ents": [edge["владелец_тикер"], edge["эмитент"]], "vf": vf,
                   "url": "https://smart-lab.ru/q/%s/shareholders/" % edge["эмитент"],
                   "conf": 0.85 if as_of else 0.60})

        if a.dry_run:
            conn.rollback()
            log.info("DRY-RUN, откат")

    log.info("итог: %s", json.dumps(stats, ensure_ascii=False))
    print(json.dumps(stats, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
