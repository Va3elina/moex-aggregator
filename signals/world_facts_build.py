"""Сборка world_facts из СТРУКТУРНЫХ данных своей БД.

Первый источник фактов для второго мозга — не новости, а `macro_data`: ключевая
ставка ЦБ, денежная масса, ВВП. Причина в надёжности: у структурных данных даты и
значения точные, конфликтовать нечему, извлекать нечего. Новости (news_archive,
486 тыс. постов) — второй заход, там нужны и извлечение, и разбор противоречий.

⚠️ ГЛАВНАЯ ИДЕЯ — СХЛОПЫВАНИЕ РЯДА В ИНТЕРВАЛЫ. Ключевая ставка лежит ежедневным
рядом: 3 247 точек с 2013 года, но РАЗНЫХ уровней всего 38. Хранить 3 247 фактов
«ставка 14%» бессмысленно — нужен один факт на период неизменности, с честными
valid_from и valid_until. Так «что действовало на дату» становится одной строкой,
а не поиском по ряду.

Для месячных и квартальных рядов (M2, ВВП) интервал другой по смыслу: значение
действует как последнее известное, пока не вышло следующее. Поэтому valid_until =
день перед следующим наблюдением, а у самого свежего — NULL.

Идемпотентно: fact_key детерминирован ('KEY_RATE:2026-07-24'), повторный прогон
обновляет, а не размножает.

Запуск:
  /opt/frame/signals/world_facts_build.sh
  /opt/frame/signals/world_facts_build.sh --dry-run
"""
import argparse
import os
from decimal import Decimal

from dotenv import load_dotenv
from sqlalchemy import text

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

from api.database import SessionLocal      # noqa: E402

# Что берём и как называем человеческим языком. Формулировка важна: она уходит
# прямо в бриф генератора, и «KEY_RATE=14.00» там читаться не должно.
SPECS = [
    {
        "indicator": "KEY_RATE",
        "kind": "ключевая ставка",
        "entities": ["ЦБ", "RGBI", "RUB"],
        "collapse": True,   # ежедневный ряд с редкими изменениями → интервалы
        "fmt": lambda v: f"Ключевая ставка ЦБ — {_num(v)}%",
    },
    {
        "indicator": "M2_MONTHLY",
        "kind": "денежная масса",
        "entities": ["ЦБ", "RUB"],
        "collapse": False,  # каждое наблюдение — свой факт до следующего
        "fmt": lambda v: f"Денежная масса M2 — {_num(v)} млрд руб.",
    },
    {
        "indicator": "GDP_QUARTERLY",
        "kind": "ввп",
        "entities": ["Росстат"],
        "collapse": False,
        "fmt": lambda v: f"ВВП России за квартал — {_num(v)} млрд руб.",
    },
]

_SELECT = text("""
    SELECT period_date, value FROM macro_data
    WHERE indicator = :ind ORDER BY period_date
""")

_UPSERT = text("""
    INSERT INTO world_facts
        (fact_key, statement, kind, entities, valid_from, valid_until,
         source, source_url, confidence)
    VALUES (:fact_key, :statement, :kind, :entities, :valid_from, :valid_until,
            'structured', NULL, 1.00)
    ON CONFLICT (fact_key) DO UPDATE
       SET statement = EXCLUDED.statement,
           valid_until = EXCLUDED.valid_until,
           updated_at = now()
""")


def _num(v) -> str:
    """Убираем хвостовые нули и ставим запятую: 14.00 → «14», 14.50 → «14,5».
    Это текст для человека, а не для парсера."""
    d = Decimal(str(v)).normalize()
    s = format(d, "f")
    return s.replace(".", ",")


def _intervals(rows, collapse: bool):
    """(valid_from, valid_until, value) по ряду наблюдений.

    collapse=True — соседние точки с ОДИНАКОВЫМ значением сливаются в один период
    (ставка держится месяцами, и 3 247 строк «ставка 14%» никому не нужны).
    collapse=False — каждое наблюдение действует до дня перед следующим.
    У последнего интервала valid_until = NULL: он действует до сих пор."""
    out = []
    for i, (d, v) in enumerate(rows):
        if collapse and out and out[-1][2] == v:
            continue                      # продолжение того же уровня
        start = d
        nxt = None
        for j in range(i + 1, len(rows)):
            if not collapse or rows[j][1] != v:
                nxt = rows[j][0]
                break
        out.append((start, nxt, v))
    # valid_until = день ПЕРЕД началом следующего интервала, а не сам этот день:
    # иначе на стыке два факта действовали бы одновременно и запрос «на дату»
    # вернул бы противоречие.
    from datetime import timedelta
    return [(s, (n - timedelta(days=1)) if n else None, v) for s, n, v in out]


def run_once(dry_run: bool = False) -> dict:
    summary = {}
    db = SessionLocal()
    try:
        for spec in SPECS:
            rows = [(r[0], r[1]) for r in db.execute(_SELECT, {"ind": spec["indicator"]})]
            if not rows:
                summary[spec["indicator"]] = "нет данных"
                continue
            ivs = _intervals(rows, spec["collapse"])
            summary[spec["indicator"]] = f"{len(rows)} точек → {len(ivs)} фактов"
            if dry_run:
                for s, u, v in ivs[-3:]:
                    print(f"  [{spec['indicator']}] {s} … {u or 'сейчас'}: {spec['fmt'](v)}")
                continue
            for s, u, v in ivs:
                db.execute(_UPSERT, {
                    "fact_key": f"{spec['indicator']}:{s}",
                    "statement": spec["fmt"](v),
                    "kind": spec["kind"],
                    "entities": spec["entities"],
                    "valid_from": s,
                    "valid_until": u,
                })
            db.commit()
    except Exception as e:
        db.rollback()
        summary["error"] = f"{type(e).__name__}: {e}"
    finally:
        db.close()
    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    s = run_once(a.dry_run)
    for k, v in s.items():
        print(f"[world_facts] {k}: {v}")


if __name__ == "__main__":
    main()
