"""Справка о СЧА: вынуть текст и залить прочитанное агентом. Вызывается конвейером
signals/scha_pipeline.py через `docker exec <api> python3 -m Funds.scha_job_tools ...`.

Почему в api-контейнере: здесь есть pdfplumber, pandas + xlrd/openpyxl (в окружении хост-скриптов
их нет; Word читаем как zip+XML) и тот же save_assets, что у ручной заливки manual_scha_backfill.py.
Архив справок смонтирован сюда только на чтение (/data/manual_scha).

    python3 -m Funds.scha_job_tools text   /data/manual_scha/auto/TMOS/TMOS_2026-09-30_ab12cd34.pdf
    python3 -m Funds.scha_job_tools import /data/manual_scha/auto/TMOS/TMOS_2026-09-30_ab12cd34.agent.json TMOS 2026-09

text   — весь текст файла с сохранённой раскладкой, БЕЗ угадывания: страницы/листы/таблицы,
         ячейки через « | ». Читает агент-чтец (рутина frame-scha-reader). Последняя строка stdout —
         JSON {"text": ...} или {"error": ...}.
import — ответ агента → fund_holdings_history (source=interfax_manual). Строки одной бумаги (разные
         места хранения) складываются: ключ таблицы — (фонд, ISIN, имя, дата), вторая строка с тем же
         ISIN иначе затёрла бы первую. Последняя строка stdout — JSON с итогом.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import date
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

ARCHIVE = Path("/data/manual_scha/auto")


def _safe(path: str) -> Path:
    """Только файлы архива — путь приходит из БД, но лишняя проверка дешевле сюрприза."""
    p = Path(path).resolve()
    if ARCHIVE.resolve() not in p.parents:
        raise ValueError(f"файл не из архива справок: {path}")
    return p


def _cells(values) -> str:
    out = []
    for c in values:
        s = str(c).strip()
        if s and s not in ("nan", "None", "NaT"):
            out.append(s)
    return " | ".join(out)


def extract_text(p: Path) -> str:
    ext = p.suffix.lower()
    out: list[str] = []
    if ext == ".pdf":
        import pdfplumber
        with pdfplumber.open(p) as pdf:
            for i, page in enumerate(pdf.pages, 1):
                out.append(f"===== страница {i} =====")
                out.append(page.extract_text(layout=True) or "")
    elif ext in (".xls", ".xlsx"):
        import pandas as pd
        for name, df in pd.read_excel(p, sheet_name=None, header=None, dtype=str).items():
            rows = [r for r in (_cells(row) for row in df.itertuples(index=False)) if r]
            if rows:
                out.append(f"===== лист {name} =====")
                out += rows
    elif ext == ".docx":
        # Word без python-docx (его нет в образе): docx — zip, внутри word/document.xml —
        # так же читает Funds/parsers/scha_docx_parser.py.
        import zipfile
        from xml.etree import ElementTree as ET
        w = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
        with zipfile.ZipFile(p) as z:
            body = ET.fromstring(z.read("word/document.xml")).find(w + "body")
        for child in body:
            if child.tag == w + "p":
                t = "".join(x.text or "" for x in child.iter(w + "t")).strip()
                if t:
                    out.append(t)
            elif child.tag == w + "tbl":
                out.append("----- таблица -----")
                for tr in child.iter(w + "tr"):
                    cells = [" ".join((x.text or "") for x in tc.iter(w + "t")).strip()
                             for tc in tr if tc.tag == w + "tc"]
                    out.append(" | ".join(cells))
    else:
        raise ValueError(f"формат {ext} не поддерживается (нужен PDF, XLS/XLSX или DOCX)")
    return "\n".join(out)


def cmd_text(path: str) -> dict:
    p = _safe(path)
    try:
        return {"text": extract_text(p)}
    except Exception as e:  # noqa: BLE001 — сообщить конвейеру, а не упасть без ответа
        return {"error": f"{type(e).__name__}: {e}"[:500]}


def _num(v) -> float | None:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(" ", "").replace(" ", "").replace(",", ".")
    return float(s)


def cmd_import(agent_json: str, ticker: str, period: str) -> dict:
    import Funds.manual_scha_backfill as mb
    a = json.loads(_safe(agent_json).read_text())
    snap = a.get("snapshot_date") or ""
    if snap[:7] != period:   # не «проверка» чтения, а предохранитель записи: не в чужой месяц
        return {"error": f"дата справки у агента {snap!r}, а задание за {period} — не залито"}
    by_isin: dict[str, dict] = {}
    for r in a.get("rows") or []:
        isin = (r.get("isin") or "").strip().upper()
        if not isin:
            continue
        cur = by_isin.setdefault(isin, {"isin": isin, "asset_name": r.get("name") or isin,
                                        "positions": 0.0, "value_rub": 0.0})
        cur["positions"] += _num(r.get("quantity")) or 0.0
        cur["value_rub"] += _num(r.get("value_rub")) or 0.0
    assets = list(by_isin.values())
    for x in assets:   # positions в базе — bigint; в справке бывают доли (ФосАгро 22 406,66) → до целого, как парсер
        x["positions"] = int(round(x["positions"])) if x["positions"] else None
    if not assets:
        return {"error": "в ответе агента нет ни одной бумаги — не залито"}
    eng = mb.get_engine()
    fund_id, how = mb.find_fund_id(eng, None, ticker)
    if not fund_id:
        return {"error": f"фонд {ticker} не найден в funds"}
    n = mb.save_assets(eng, fund_id, date.fromisoformat(snap), assets,
                       gross_assets=_num(a.get("gross_assets_rub")))
    return {"inserted": n, "fund_id": fund_id, "snapshot_date": snap, "assets": len(assets)}


def main() -> int:
    cmd, *args = sys.argv[1:]
    try:
        out = cmd_text(*args) if cmd == "text" else cmd_import(*args) if cmd == "import" \
            else {"error": f"нет команды {cmd}"}
    except Exception as e:  # noqa: BLE001
        out = {"error": f"{type(e).__name__}: {e}"[:500]}
    print(json.dumps(out, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
