"""Прогон второго мозга на эталоне — «вернёт ли он нужное, не пропустит ли, не придёт ли лишнее» (Вадим 27.09).

Часть 1 — типы новостей без компании (правила Brain/news_types.py) против ручной разметки research/brain/eval/types_gold.tsv:
по каждому типу — взято верно / лишнее / пропущено, отдельно по каналам (у СмартЛаба хэштегов нет — держится на словах).
Классификация — тем же SQL, что в синке мозга, прямо в базе. Только чтение.

Запуск на сервере (окружение как у content_ai.sh):
    cd /opt/frame && DB_URL=... signals/.venv/bin/python research/brain/eval_brain.py
Любая правка правил — прогон до и после; итог сравнивать по строке «ИТОГ».
"""
import os
import pathlib
import sys
from collections import Counter, defaultdict

ROOT = pathlib.Path(os.environ.get("FRAME_ROOT", pathlib.Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / "Brain"))
sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text  # noqa: E402

from news_types import sql_relevant, sql_type  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
_ID = ("CASE a.channel WHEN 'MarketTwits' THEN 'markettwits' WHEN 'СМАРТЛАБ НОВОСТИ' THEN 'newssmartlab' "
       "ELSE a.channel END || '/' || a.message_id")


def gold(name: str = "types_gold.tsv") -> list:
    out = []
    for line in (HERE / "eval" / name).read_text("utf-8").splitlines():
        if line.startswith("#") or not line.strip():
            continue
        nid, exp, snippet = (line.split("\t") + ["", ""])[:3]
        # «a|b» — годится любой: у поста два равноправных повода
        out.append((nid, None if exp == "—" else tuple(exp.split("|")), snippet))
    return out


def classify(conn, ids: list) -> dict:
    p = {"ids": ids}
    typ = sql_type("a", p)
    rel = sql_relevant("a", p)
    rows = conn.execute(text(f"""
        SELECT DISTINCT ON (1) {_ID} AS id, CASE WHEN {rel} THEN {typ} END AS тип
          FROM news_archive a WHERE {_ID} = ANY(CAST(:ids AS text[]))
         ORDER BY 1
    """), p).all()
    return {r[0]: r[1] for r in rows}


def report(conn, name: str, title: str) -> tuple:
    g = gold(name)
    got = classify(conn, [x[0] for x in g])
    per = defaultdict(Counter)
    by_channel = defaultdict(Counter)
    errors = []
    for nid, exp, snippet in g:
        pred = got.get(nid)
        ch = nid.split("/")[0]
        if exp and pred in exp:
            exp = pred
        elif exp:
            exp = exp[0]
        if pred == exp:
            verdict = "верно"
            if exp:
                per[exp]["верно"] += 1
        elif exp is None:
            verdict = "лишнее"
            per[pred]["лишнее"] += 1
        elif pred is None:
            verdict = "пропущено"
            per[exp]["пропущено"] += 1
        else:
            verdict = "не тот тип"
            per[exp]["пропущено"] += 1
            per[pred]["лишнее"] += 1
        by_channel[ch][verdict] += 1
        if verdict != "верно":
            errors.append((verdict, nid, exp or "—", pred or "—", snippet))
    print(f"===== {title} ({name}, {len(g)} новостей)")
    print(f"{'тип':<26} {'верно':>6} {'лишнее':>7} {'пропущено':>10}")
    for typ in sorted(per, key=lambda t: -sum(per[t].values())):
        c = per[typ]
        print(f"{typ:<26} {c['верно']:>6} {c['лишнее']:>7} {c['пропущено']:>10}")
    for ch, c in sorted(by_channel.items()):
        print(f"  {ch:<14} " + " · ".join(f"{k} {v}" for k, v in c.most_common()))
    for verdict, nid, exp, pred, snippet in sorted(errors):
        print(f"  {verdict:<10} {nid:<22} ждали «{exp}», правила «{pred}» | {snippet[:80]}")
    total = Counter(v for v, *_ in errors)
    line = (f"ИТОГ {title}: {len(g) - len(errors)} из {len(g)} верно; лишнее {total['лишнее']}, "
            f"пропущено {total['пропущено']}, не тот тип {total['не тот тип']}")
    print(line + "\n")
    return line


def main() -> int:
    url = os.environ["DB_URL"].replace("@db:", "@127.0.0.1:")
    with create_engine(url).connect() as conn:
        lines = [report(conn, "types_gold.tsv", "подгонка"), report(conn, "types_holdout.tsv", "отложенная-1"),
                 report(conn, "types_holdout2.tsv", "отложенная-2")]
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
