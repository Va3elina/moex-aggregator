"""
Manual SCHA backfill — парсит PDF'ки из папки и пишет в fund_holdings_history.

Workflow:
  1. Пользователь скачивает SCHA-PDF с e-disclosure.ru (Интерфакс) через VPN
     или с сайтов УК. Складывает в папку `~/Downloads/scha_manual/`.
  2. Именует файлы по одному из двух правил:
     a. Для БПИФ — любое имя; парсер автоматически найдёт фонд по
        `isin_pif` извлечённому из метаданных PDF.
     b. Для ОПИФ — имя файла должно начинаться с тикера и даты:
        `OPIF-54_2026-04-30_scha.pdf` или `OPIF-54-2026-04-30.pdf`.
        Допустимые разделители: `_`, `-`, ` `.
  3. Запускает скрипт. Парсер:
     - читает каждый PDF
     - определяет fund_id (по ISIN из PDF или из имени файла)
     - определяет snapshot_date (из метаданных PDF; fallback на имя файла)
     - резолвит имена эмитентов через MOEX ISS API
     - пишет в `fund_holdings_history` с `source='interfax_manual'`

Запуск локально:
    python3 Funds/manual_scha_backfill.py ~/Downloads/scha_manual/

На проде (если PDF загружены на сервер):
    docker exec frame-api-1 python3 /app/Funds/manual_scha_backfill.py /data/manual_scha/

Идемпотентность: ON CONFLICT DO UPDATE — повторный запуск обновит данные
если PDF был перепарсен (например после фикса парсера).
"""
import io
import logging
import re
import sys
import time
import urllib.request
import urllib.parse
import json
from datetime import date as _date
from pathlib import Path

# Добавим parent в path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from api.database import get_engine
from Funds.parsers.scha_parser import parse_scha

SOURCE = "interfax_manual"
SLEEP_BETWEEN_MOEX = 0.15  # сек между MOEX ISS запросами для резолва имён

# In-memory кеш ISIN → SHORTNAME (одна сессия одного запуска)
_name_cache: dict[str, str] = {}


def setup_logging():
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(fmt)
    root.addHandler(h)


log = logging.getLogger(__name__)


# ─── Резолверы ─────────────────────────────────────────────────


def resolve_isin_name(isin: str) -> str:
    """ISIN → SHORTNAME через MOEX ISS. Кеширует. Fallback = сам ISIN."""
    if not isin:
        return "(no ISIN)"
    if isin in _name_cache:
        return _name_cache[isin]
    try:
        url = (
            f"https://iss.moex.com/iss/securities.json?q={urllib.parse.quote(isin)}"
            "&iss.meta=off&limit=5"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "frame/manual-backfill"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        secs = data.get("securities", {})
        cols = secs.get("columns", [])
        rows = secs.get("data", [])
        if not cols or not rows:
            _name_cache[isin] = isin
            return isin
        ci = {c: i for i, c in enumerate(cols)}
        # Предпочитаем traded=1
        for prefer in (True, False):
            for row in rows:
                if "isin" in ci and row[ci["isin"]] != isin:
                    continue
                if prefer and "is_traded" in ci and not row[ci["is_traded"]]:
                    continue
                if "shortname" in ci and row[ci["shortname"]]:
                    name = row[ci["shortname"]]
                    _name_cache[isin] = name
                    return name
    except Exception:
        pass
    _name_cache[isin] = isin
    return isin


# Regex для ISO даты в имени файла: 2026-04-30 / 26_04_30 / 30.04.2026
_DATE_PATTERNS = [
    (re.compile(r"(\d{4})[-_.](\d{2})[-_.](\d{2})"), lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),
    (re.compile(r"(\d{2})[-_.](\d{2})[-_.](\d{2})\b"), lambda m: f"20{m.group(1)}-{m.group(2)}-{m.group(3)}"),
    (re.compile(r"(\d{2})[-_.](\d{2})[-_.](\d{4})"), lambda m: f"{m.group(3)}-{m.group(2)}-{m.group(1)}"),
]

# Regex для извлечения ticker из имени файла:
# OPIF-54_..., OPIF-9165_..., EQMX-... и т.д.
_TICKER_RE = re.compile(r"\b(OPIF-\d+|[A-Z]{4,6})\b")


def parse_filename(filename: str) -> tuple[str | None, str | None]:
    """
    Извлекает (ticker, snapshot_date_iso) из имени файла.
    Возвращает (None, None) если не удалось распарсить.
    """
    stem = Path(filename).stem
    # Попытка найти тикер
    ticker = None
    m = _TICKER_RE.search(stem)
    if m:
        ticker = m.group(1)

    # Попытка найти дату
    snap_date = None
    for pattern, formatter in _DATE_PATTERNS:
        m = pattern.search(stem)
        if m:
            try:
                snap_date = formatter(m)
                _date.fromisoformat(snap_date)  # validation
                break
            except (ValueError, AttributeError):
                continue
    return ticker, snap_date


def find_fund_id(engine, isin_pif: str | None, ticker: str | None) -> tuple[int | None, str]:
    """Возвращает (fund_id, match_method). method = 'isin' | 'ticker' | None."""
    with engine.connect() as conn:
        # 1. ISIN-пай (для БПИФ)
        if isin_pif:
            row = conn.execute(
                text("SELECT fund_id, ticker FROM funds WHERE isin_pif = :isin"),
                {"isin": isin_pif},
            ).first()
            if row:
                return row[0], f"isin_pif={isin_pif} → {row[1]}"
        # 2. По тикеру
        if ticker:
            row = conn.execute(
                text("SELECT fund_id FROM funds WHERE UPPER(ticker) = UPPER(:t)"),
                {"t": ticker},
            ).first()
            if row:
                return row[0], f"ticker={ticker}"
    return None, ""


def save_assets(engine, fund_id: int, snap_date: _date, assets: list[dict], resolve_names: bool = True) -> int:
    """Сохраняет позиции с резолвом имён через MOEX ISS."""
    if not assets:
        return 0

    total_nav = sum(a.get("value_rub") or 0 for a in assets)
    inserted = 0
    with engine.connect() as conn:
        for a in assets:
            name = a.get("asset_name") or "(unknown)"
            isin = a.get("isin")
            # Резолвим имя если placeholder и есть ISIN
            if name in ("(name from ISIN)", "(имя не извлечено)") and isin and resolve_names:
                name = resolve_isin_name(isin)
                time.sleep(SLEEP_BETWEEN_MOEX)

            weight = None
            if total_nav > 0 and a.get("value_rub"):
                weight = round(a["value_rub"] / total_nav * 100, 4)

            result = conn.execute(
                text("""
                    INSERT INTO fund_holdings_history
                        (fund_id, asset_name, isin, weight, positions, amount_rub,
                         snapshot_date, source, created_at)
                    VALUES (:fid, :name, :isin, :weight, :positions, :amount, :d, :src, NOW())
                    ON CONFLICT (fund_id, asset_name, snapshot_date) DO UPDATE SET
                        isin = COALESCE(EXCLUDED.isin, fund_holdings_history.isin),
                        positions = COALESCE(EXCLUDED.positions, fund_holdings_history.positions),
                        amount_rub = COALESCE(EXCLUDED.amount_rub, fund_holdings_history.amount_rub),
                        weight = COALESCE(EXCLUDED.weight, fund_holdings_history.weight)
                """),
                {
                    "fid": fund_id,
                    "name": name[:255],
                    "isin": isin,
                    "weight": weight,
                    "positions": a.get("positions"),
                    "amount": a.get("value_rub"),
                    "d": snap_date,
                    "src": SOURCE,
                },
            )
            if result.rowcount > 0:
                inserted += 1
        conn.commit()
    return inserted


def process_pdf(engine, pdf_path: Path) -> dict:
    """Обрабатывает один PDF. Возвращает summary dict."""
    summary = {
        "file": pdf_path.name,
        "fund_id": None,
        "match": "",
        "snapshot_date": None,
        "parser_strategy": None,
        "total_assets": 0,
        "inserted": 0,
        "error": None,
    }

    try:
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        result = parse_scha(pdf_bytes)
    except Exception as e:
        summary["error"] = f"parse failed: {e}"
        return summary

    summary["parser_strategy"] = result.get("parser_strategy")
    summary["total_assets"] = result.get("total_assets", 0)

    # Снапшот дата: из PDF метаданных, fallback из имени файла
    isin_pif = result.get("isin_pif")
    snap_date_iso = result.get("snapshot_date")
    file_ticker, file_date = parse_filename(pdf_path.name)
    if not snap_date_iso:
        snap_date_iso = file_date
    if not snap_date_iso:
        summary["error"] = "no snapshot_date in PDF or filename"
        return summary

    try:
        snap_date = _date.fromisoformat(snap_date_iso)
    except ValueError:
        summary["error"] = f"invalid date {snap_date_iso}"
        return summary
    summary["snapshot_date"] = snap_date.isoformat()

    # Resolve fund_id
    fund_id, match_method = find_fund_id(engine, isin_pif, file_ticker)
    if not fund_id:
        summary["error"] = (
            f"fund not resolved: isin_pif={isin_pif}, file_ticker={file_ticker}. "
            "Переименуй файл в формат TICKER_YYYY-MM-DD.pdf или добавь isin_pif в БД."
        )
        return summary
    summary["fund_id"] = fund_id
    summary["match"] = match_method

    if not result["assets"]:
        summary["error"] = f"no assets parsed (strategy={summary['parser_strategy']})"
        return summary

    summary["inserted"] = save_assets(engine, fund_id, snap_date, result["assets"])
    return summary


def main(directory: str):
    setup_logging()
    path = Path(directory)
    if not path.is_dir():
        log.error(f"Папка {directory} не найдена")
        sys.exit(1)

    pdfs = sorted([p for p in path.iterdir() if p.suffix.lower() == ".pdf"])
    if not pdfs:
        log.warning(f"В папке {directory} нет PDF файлов")
        sys.exit(0)

    log.info(f"Найдено {len(pdfs)} PDF файлов в {directory}")
    engine = get_engine()

    stats = {"ok": 0, "skip": 0, "err": 0, "rows": 0}
    failures = []
    for pdf in pdfs:
        log.info(f"")
        log.info(f"=== {pdf.name} ===")
        s = process_pdf(engine, pdf)
        if s["error"]:
            log.warning(f"  ❌ {s['error']}")
            failures.append((pdf.name, s["error"]))
            stats["err"] += 1
        elif s["inserted"] == 0:
            log.info(f"  ⚠️  fund_id={s['fund_id']} match={s['match']} "
                     f"date={s['snapshot_date']} strategy={s['parser_strategy']} "
                     f"assets={s['total_assets']} inserted=0 (уже было в БД?)")
            stats["skip"] += 1
        else:
            log.info(f"  ✅ fund_id={s['fund_id']} match={s['match']} "
                     f"date={s['snapshot_date']} strategy={s['parser_strategy']} "
                     f"assets={s['total_assets']} inserted={s['inserted']}")
            stats["ok"] += 1
            stats["rows"] += s["inserted"]

    log.info("")
    log.info("=" * 60)
    log.info(f"FINAL SUMMARY")
    log.info("=" * 60)
    log.info(f"  PDF обработано: {len(pdfs)}")
    log.info(f"  Успешно вставлено: {stats['ok']} (всего {stats['rows']} строк)")
    log.info(f"  Скипнуто (уже в БД): {stats['skip']}")
    log.info(f"  Ошибок: {stats['err']}")
    if failures:
        log.info("")
        log.info("Проблемные файлы:")
        for fname, err in failures:
            log.info(f"  - {fname}: {err}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python manual_scha_backfill.py <path_to_pdf_directory>")
        print("Пример: python manual_scha_backfill.py ~/Downloads/scha_manual/")
        sys.exit(1)
    main(sys.argv[1])
