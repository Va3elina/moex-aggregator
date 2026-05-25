"""
Backfill SCHA snapshot'ов для ВИМ-БПИФ из сайта wealthim.ru.

Для каждого из 3 фондов (LQDT, EQMX, GOLD):
  1. list_vim_scha_for_ticker(ticker) → список SCHA-документов с датами
  2. Для каждой даты которой нет в БД:
     a. download_pdf(url)
     b. parse_scha(pdf_bytes)
     c. INSERT в fund_holdings_history с source='vim_sdr'

Идемпотентность через UNIQUE (fund_id, asset_name, snapshot_date) +
ON CONFLICT DO NOTHING. Повторный запуск ничего не дублирует.

Запуск (на проде):
    docker exec frame-api-1 python3 /app/Funds/vim_sdr_backfill.py
"""
import io
import logging
import sys
import time
import zipfile
from pathlib import Path

# Добавим parent в path чтобы импортировать наши модули
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from api.database import get_engine
from Funds.parsers.scha_parser import parse_scha
from Funds.parsers.vim_sdr_listener import (
    VIM_TICKER_TO_SLUG,
    download_pdf,
    list_vim_scha_for_ticker,
)


# Цель backfill — 3 БПИФ-фонда у которых есть архив на сайте ВИМ.
BACKFILL_TICKERS = ["LQDT", "EQMX", "GOLD"]

# Sleep между запросами чтобы не вызвать rate-limit ВИМ.
SLEEP_BETWEEN_DOWNLOADS = 1.5  # секунды

SOURCE = "vim_sdr"


def setup_logging():
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s",
                             datefmt="%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)


log = logging.getLogger(__name__)


def _extract_pdf(content: bytes) -> bytes | None:
    """Если content = ZIP, распакуем и вернём первый PDF. Если PDF — вернём как есть."""
    if content.startswith(b"%PDF-"):
        return content
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            pdfs = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
            if pdfs:
                return zf.read(pdfs[0])
    except zipfile.BadZipFile:
        pass
    return None


def get_fund_id(engine, ticker: str) -> int | None:
    """Lookup fund_id by ticker."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT fund_id FROM funds WHERE ticker = :t"),
            {"t": ticker},
        ).fetchone()
    return row[0] if row else None


def existing_snapshot_dates(engine, fund_id: int, source: str = SOURCE) -> set:
    """Возвращает set дат для которых уже есть snapshot этого фонда из этого источника."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT snapshot_date FROM fund_holdings_history
            WHERE fund_id = :fid AND source = :src
        """), {"fid": fund_id, "src": source}).fetchall()
    return {r[0] for r in rows}


def save_assets(engine, fund_id: int, snapshot_date, assets: list[dict]) -> int:
    """Сохраняет список assets в fund_holdings_history. ON CONFLICT DO NOTHING."""
    if not assets:
        return 0
    inserted = 0
    with engine.connect() as conn:
        for a in assets:
            # weight в SCHA не указан явно, но мы можем посчитать его post-hoc
            # как value_rub / total_NAV. Для now — оставим NULL и положим
            # positions + amount_rub. weight можно вычислить SQL'ом потом.
            result = conn.execute(text("""
                INSERT INTO fund_holdings_history
                    (fund_id, asset_name, weight, positions, amount_rub,
                     snapshot_date, source, created_at)
                VALUES
                    (:fid, :name, NULL, :positions, :amount_rub,
                     :snap_date, :source, NOW())
                ON CONFLICT (fund_id, asset_name, snapshot_date) DO NOTHING
                RETURNING id
            """), {
                "fid": fund_id,
                "name": a["asset_name"][:255],
                "positions": a.get("positions"),
                "amount_rub": a.get("value_rub"),
                "snap_date": snapshot_date,
                "source": SOURCE,
            })
            if result.rowcount > 0:
                inserted += 1
        conn.commit()
    return inserted


def backfill_ticker(engine, ticker: str) -> dict:
    """Backfill одного фонда. Возвращает статистику."""
    log.info("=" * 60)
    log.info(f"Backfill {ticker}")
    log.info("=" * 60)

    fund_id = get_fund_id(engine, ticker)
    if not fund_id:
        log.error(f"  {ticker}: fund_id не найден в БД, пропуск")
        return {"ticker": ticker, "error": "no_fund_id"}

    log.info(f"  fund_id={fund_id}")

    # Listing.
    docs = list_vim_scha_for_ticker(ticker)
    schas = [d for d in docs if d["doc_type"] == "scha"]
    log.info(f"  Доступно SCHA на сайте: {len(schas)}")

    # Что уже в БД.
    existing = existing_snapshot_dates(engine, fund_id)
    log.info(f"  Уже в БД (source={SOURCE}): {len(existing)}")

    new_schas = [s for s in schas if s["snapshot_date"] not in existing]
    log.info(f"  Будем скачивать: {len(new_schas)}")

    stats = {
        "ticker": ticker,
        "available": len(schas),
        "already_in_db": len(existing),
        "to_download": len(new_schas),
        "downloaded": 0,
        "parsed": 0,
        "inserted": 0,
        "failed": [],
    }

    # Сортируем по дате убывание — свежие первыми (на случай если cron
    # прервётся, у нас будут самые новые snapshot'ы).
    new_schas.sort(key=lambda d: d["snapshot_date"], reverse=True)

    for i, doc in enumerate(new_schas, 1):
        snap = doc["snapshot_date"]
        log.info(f"  [{i}/{len(new_schas)}] {snap} downloading...")
        content = download_pdf(doc["download_url"])
        if not content:
            log.warning(f"    download failed")
            stats["failed"].append(str(snap))
            time.sleep(SLEEP_BETWEEN_DOWNLOADS)
            continue
        stats["downloaded"] += 1

        pdf_bytes = _extract_pdf(content)
        if not pdf_bytes:
            log.warning(f"    not a PDF (bad ZIP?)")
            stats["failed"].append(f"{snap}-bad-zip")
            time.sleep(SLEEP_BETWEEN_DOWNLOADS)
            continue

        try:
            result = parse_scha(pdf_bytes)
        except Exception as e:
            log.warning(f"    parse failed: {e}")
            stats["failed"].append(f"{snap}-parse")
            time.sleep(SLEEP_BETWEEN_DOWNLOADS)
            continue
        stats["parsed"] += 1

        # Используем snapshot_date из PDF если есть, иначе из имени файла.
        scha_date = result.get("snapshot_date") or snap.isoformat()
        if isinstance(scha_date, str):
            from datetime import date as _date
            scha_date = _date.fromisoformat(scha_date)

        inserted = save_assets(engine, fund_id, scha_date, result["assets"])
        stats["inserted"] += inserted
        log.info(
            f"    parsed {len(result['assets'])} активов, "
            f"inserted {inserted} новых строк (date={scha_date})"
        )

        time.sleep(SLEEP_BETWEEN_DOWNLOADS)

    log.info(f"\n  Итого {ticker}: downloaded={stats['downloaded']} "
             f"parsed={stats['parsed']} inserted={stats['inserted']}")
    return stats


def main():
    setup_logging()
    engine = get_engine()
    log.info(f"VIM SDR backfill starting, tickers: {BACKFILL_TICKERS}")
    log.info(f"Sleep between downloads: {SLEEP_BETWEEN_DOWNLOADS}s")
    log.info("")
    all_stats = []
    for ticker in BACKFILL_TICKERS:
        stats = backfill_ticker(engine, ticker)
        all_stats.append(stats)
    log.info("")
    log.info("=" * 60)
    log.info("FINAL SUMMARY")
    log.info("=" * 60)
    for s in all_stats:
        if "error" in s:
            log.info(f"  {s['ticker']}: SKIPPED ({s['error']})")
        else:
            log.info(f"  {s['ticker']}: {s['inserted']} rows inserted ({s['parsed']} SCHA parsed)")
    engine.dispose()


if __name__ == "__main__":
    main()
