"""Регрессия правил редактора: ловит ли завод СЕЙЧАС ошибки, которые Вадим уже находил.

Для каждого правила из editor_rules.yaml берёт кандидатов из `regression` и прогоняет их через то, что
сейчас стоит в коде: отсев повтора, устаревшей новости, экспирации, малоактивных контрактов и проверку
текста (insight_check) на исходном черновике ИИ. Только чтение БД.

Запуск на сервере (окружение как у content_ai.sh):
    cd /opt/frame && DB_URL=... signals/.venv/bin/python research/content_pipeline_v2/regression.py

Итог — таблица «правило · кандидат · чем проверено · поймано». «н/п» — у правила нет проверки кодом
(prompt_only) или она зависит от момента (сверка с утром): такие правила регрессия не защищает.
"""
import os
import pathlib
import sys

ROOT = os.environ.get("FRAME_ROOT", "/opt/frame")   # копия кода до деплоя: FRAME_ROOT=/tmp/<копия>
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "signals"))

import yaml  # noqa: E402
from sqlalchemy import text  # noqa: E402

from api.database import SessionLocal  # noqa: E402
from api.services import insight_check  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent

# правило → подстрока дефекта insight_check, которой правило ловится в тексте
TEXT_MARK = {"R02": "контракт", "R03": "объяснение индикатора", "R04": "писали", "R05": "медиан",
             "R07": "заготовка", "R18": "аналитик", "R23": "абзацев"}

_ROW = text("""
    SELECT c.id, c.source, c.tickers, c.futures_ticker, c.created_at, c.updated_at, c.raw_text,
           coalesce(c.draft_text_ai, c.draft_text) AS draft_ai, a.signal_date
    FROM content_candidates c LEFT JOIN anomalies a ON a.id = c.matched_anomaly_id
    WHERE c.id = :id
""")


def main() -> int:
    from signals import content_ai as ca
    from signals.insights.expiry import near_expiry
    from api.services.oi_screener import low_activity_set

    rules = yaml.safe_load((HERE / "editor_rules.yaml").read_text("utf-8"))["rules"]
    db = SessionLocal()
    low = low_activity_set(db)
    out, caught, total = [], 0, 0
    try:
        for r in rules:
            for cid in r.get("regression") or []:
                row = db.execute(_ROW, {"id": cid}).mappings().first()
                if not row:
                    out.append((r["id"], cid, "—", "нет в базе"))
                    continue
                how, hit = [], None
                if r["id"] in TEXT_MARK and row["draft_ai"]:
                    d = insight_check.check(row["draft_ai"], row["raw_text"] or "")
                    how.append("текст")
                    hit = any(TEXT_MARK[r["id"]] in x for x in d["defects"] + d["failed"])
                elif r["id"] == "R14":
                    how.append("повтор")
                    hit = bool(ca._repeat_of_ticker(db, cid))
                elif r["id"] == "R24":
                    how.append("возраст новости к черновику")
                    age = (row["updated_at"] - row["created_at"]).total_seconds() / 3600
                    hit = age > ca.STALE_NEWS_HOURS
                elif r["id"] == "R19":
                    day = row["signal_date"] or row["created_at"].date()
                    how.append(f"экспирация на {day}")
                    hit = near_expiry(day)
                elif r["id"] == "R20":
                    how.append("малоактивный контракт")
                    hit = (row["futures_ticker"] or "") in low
                if hit is not None:
                    total += 1
                    caught += bool(hit)
                out.append((r["id"], cid, " + ".join(how) or "—",
                            "н/п" if hit is None else ("поймано" if hit else "❌ НЕ поймано")))
    finally:
        db.close()
    w = max(len(x[2]) for x in out) if out else 10
    for rid, cid, how, res in out:
        print(f"{rid:<4} #{cid:<5} {how:<{w}}  {res}")
    print(f"\nпроверяемых кодом случаев: {total}, поймано: {caught}")
    return 0 if caught == total else 1


if __name__ == "__main__":
    sys.exit(main())
