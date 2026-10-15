"""Перечитать агентом справки фонда за прошлые месяцы — те, что в базе испорчены или отсутствуют.

    /opt/frame/signals/.venv/bin/python -m signals.scha_reread TMOS 2022-07 2023-04
    /opt/frame/signals/.venv/bin/python -m signals.scha_reread TMOS 2021-06..2023-09

Находит справки на сайте УК (Funds/scha_collector.py), качает в архив и ставит задания scha_jobs.
Дальше — обычный конвейер signals/scha_pipeline.py work: текст → общая очередь → агент → заливка,
которая ЗАМЕНЯЕТ месяц целиком (копия старых строк — /opt/frame/backups/scha_replaced/) → сводка.
Сканер такие месяцы сам не берёт (только новее последнего залитого) — поэтому отдельный инструмент.

Зачем (15.10.2026): у TMOS 2021–2023 старый парсер занизил стоимость расписок в 10 раз (сумма в PDF
разорвана на две строки) и путал их штуки с кодами стран; 04.2023 не прочитал вовсе. Агент читает
верно (эталон research/scha_pipeline/eval/truth/TMOS_2022-07.json).
"""
from __future__ import annotations

import hashlib
import sys

from signals.scha_pipeline import ARCHIVE, col, event, get_engine, log, text


def months_arg(args: list[str]) -> list[str]:
    out = []
    for a in args:
        if ".." in a:
            lo, hi = a.split("..")
            y, m = map(int, lo.split("-"))
            while f"{y:04d}-{m:02d}" <= hi:
                out.append(f"{y:04d}-{m:02d}")
                y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        else:
            out.append(a)
    return sorted(set(out))


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    tk, months = sys.argv[1], months_arg(sys.argv[2:])
    if tk not in col.SOURCES:
        print(f"{tk}: нет в Funds/scha_collector.py SOURCES")
        return 2
    kind, arg = col.SOURCES[tk]
    docs = col.one_per_month(col.ADAPTERS[kind](tk, arg))
    eng = get_engine()
    queued, missing = [], []
    for p in months:
        d = docs.get(p)
        if not d:
            missing.append(p)
            continue
        (ARCHIVE / tk).mkdir(parents=True, exist_ok=True)
        tmp = col.download(d, ARCHIVE / tk)
        sha = hashlib.sha256(tmp.read_bytes()).hexdigest()
        path = tmp.with_name(f"{tmp.stem}_{sha[:8]}{tmp.suffix}")
        tmp.replace(path)
        with eng.begin() as conn:
            job = conn.execute(text("""
                INSERT INTO scha_jobs (ticker, period, url, sha256, file_path)
                VALUES (:t, :p, :u, :s, :f) ON CONFLICT (sha256) DO NOTHING RETURNING id
            """), {"t": tk, "p": p, "u": d.url, "s": sha, "f": str(path)}).first()
            if job:
                event(conn, job.id, "reread", d.url)
                queued.append(f"{p}#{job.id}")
            else:
                log(f"{tk} {p}: этот файл уже есть в заданиях — пропуск")
        col.time.sleep(col.PAUSE)
    log(f"{tk}: поставлено {len(queued)}: {', '.join(queued)}"
        + (f"; на сайте нет: {', '.join(missing)}" if missing else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
