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
    "BAZA": "Базис", "MVID": "М.Видео", "MTLR": "Мечел", "LEAS": "Европлан",
    "NVTK": "НОВАТЭК", "SFIN": "ЭсЭфАй",
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


def statement_for(edge, names, institutions):
    """
    Текст факта. У института владелец подписывается БЕЗ решётки-тикера и с явным
    предупреждением: Газпромбанк не Газпром, и агент не должен их склеить.
    """
    pct = ("%.2f" % edge["доля"]).replace(".", ",")
    target = edge["эмитент"]
    is_inst = edge.get("владелец_kind") == "institution"
    owner = edge.get("владелец_код") if is_inst else edge["владелец_тикер"]
    owner_label = (institutions.get(owner, {}).get("имя", owner) if is_inst
                   else "%s (#%s)" % (names.get(owner, owner), owner))
    как = (" косвенно (через %s)" % edge["через"]) if edge.get("через") else ""
    when = (("Доля по структуре акционеров, обновлённой %s." % edge["структура_от"])
            if edge.get("структура_от") else
            "ВНИМАНИЕ: источник не указал дату обновления структуры, доля может быть устаревшей.")
    warn = ""
    if is_inst:
        warn = " " + institutions.get(owner, {}).get("предупреждение", "")
    return ("%s владеет%s долей %s%% в компании %s (#%s). %s%s"
            % (owner_label, как, pct, names.get(target, target), target, when, warn))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("edges_json", type=Path)
    ap.add_argument("--draft", type=Path, default=BASE_DIR / "issuer_draft.json")
    ap.add_argument("--curated", type=Path, default=BASE_DIR / "ownership_curated.json",
                    help="рёбра, проверенные вручную (цепочки, дочки, институты)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    setup_logging()

    edges = json.loads(a.edges_json.read_text(encoding="utf-8"))["рёбра"]
    names = build_names(a.draft)

    # Курируемые рёбра идут В ТОТ ЖЕ проход: автомат их не находит (владелец назван
    # цепочкой «ЦХД->РТК ЦОД->Ростелеком», дочкой «Газпром капитал» или вообще не
    # является эмитентом), но обновляться они должны по тем же правилам.
    institutions = {}
    if a.curated and a.curated.exists():
        cur_data = json.loads(a.curated.read_text(encoding="utf-8"))
        institutions = cur_data.get("институты", {})
        edges = edges + cur_data["рёбра"]
        log.info("курируемых рёбер добавлено: %d", len(cur_data["рёбра"]))
    engine = create_engine(DB_URL)
    stats = {"новых": 0, "изменилось": 0, "без_изменений": 0}

    with engine.begin() as conn:
        for edge in edges:
            is_inst = edge.get("владелец_kind") == "institution"
            owner = edge.get("владелец_код") if is_inst else edge["владелец_тикер"]
            key = "own:%s:%s" % (owner, edge["эмитент"])
            as_of = (datetime.strptime(edge["структура_от"], "%d.%m.%Y").date()
                     if edge.get("структура_от") else None)
            st = statement_for(edge, names, institutions)
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

            # Косвенное владение и институты — менее твёрдые факты, чем прямая доля
            # с датой снимка, и confidence это отражает.
            conf = 0.85 if as_of else 0.60
            if edge.get("связь") == "косвенная":
                conf = min(conf, 0.70)
            if is_inst:
                conf = min(conf, 0.65)

            conn.execute(text("""
              INSERT INTO world_facts (fact_key, statement, kind, entities, valid_from,
                                       source, source_url, confidence)
              VALUES (:k,:s,'связь',:ents,:vf,'smartlab',:url,:conf)
              ON CONFLICT (fact_key) DO UPDATE SET statement=EXCLUDED.statement,
                entities=EXCLUDED.entities, valid_from=EXCLUDED.valid_from,
                source_url=EXCLUDED.source_url, confidence=EXCLUDED.confidence,
                updated_at=now()
            """), {"k": key, "s": st, "ents": [owner, edge["эмитент"]], "vf": vf,
                   "url": "https://smart-lab.ru/q/%s/shareholders/" % edge["эмитент"],
                   "conf": conf})

        if a.dry_run:
            conn.rollback()
            log.info("DRY-RUN, откат")

    log.info("итог: %s", json.dumps(stats, ensure_ascii=False))
    print(json.dumps(stats, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
