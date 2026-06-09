"""
Пересчёт `fund_holdings_history.weight` на знаменатель «Раздел 3. Подраздел 10.
Общая стоимость активов» (gross), извлекаемый из самих форм СЧА (0420502).

ЗАЧЕМ: исторически доля считалась как value_rub / Σ(value_rub по бумагам) — то
есть без денег/прочих активов. Это завышало доли на ~0.5–0.6% относительно
формы и investfunds (которые делят на ПОЛНУЮ стоимость активов). Этот скрипт
правит ТОЛЬКО поле weight для уже импортированных снапшотов, in-place:

    weight = ROUND(amount_rub / gross_assets * 100, 4)

Хирургический UPDATE (не INSERT) → ноль риска дублей и ре-резолва имён. Трогаем
только source ∈ (vim_sdr, interfax_manual) — те, что показываются в фиче.

Запуск (dry-run, ничего не пишет):
    docker exec frame-api-1 python3 /app/Funds/recompute_weights_gross.py /data/funds_archive/
Запись:
    docker exec frame-api-1 python3 /app/Funds/recompute_weights_gross.py /data/funds_archive/ --commit

Идемпотентно: weight зависит только от amount_rub (в БД) и gross (из формы).
Если в форме нет Подраздела 10 (старые форматы) — снапшот ПРОПУСКАЕТСЯ (weight
остаётся прежним, на базе Σ бумаг). Guard: gross должен быть ≥ Σ(amount_rub) в БД.
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from api.database import get_engine
from Funds.manual_scha_backfill import parse_any, find_fund_id, parse_filename, setup_logging

log = logging.getLogger(__name__)

SOURCES = ("vim_sdr", "interfax_manual")
SCHA_EXTS = (".pdf", ".xls", ".xlsx", ".zip")


def folder_ticker(path: Path) -> str | None:
    """Тикер из имени родительской папки архива: «AKME — Управляемые…» → AKME."""
    name = path.parent.name
    head = name.split("—")[0].split()[0].strip()
    return head or None


def recompute_one(engine, path: Path, commit: bool) -> dict:
    s = {"file": path.name, "fund_id": None, "date": None, "gross": None,
         "rows": 0, "sum_before": None, "sum_after": None, "status": ""}
    try:
        result = parse_any(path)
    except Exception as e:
        s["status"] = f"parse_failed: {e}"
        return s

    gross = result.get("gross_assets_rub")
    if not gross or gross <= 0:
        s["status"] = "no_gross (Подраздел 10 не найден) — SKIP"
        return s
    s["gross"] = gross

    # snapshot_date: из формы, fallback на имя файла.
    snap = result.get("snapshot_date")
    if not snap:
        _, snap = parse_filename(path.name)
    if not snap:
        snap = path.stem  # архив именует файлы как YYYY-MM-DD
    try:
        from datetime import date as _date
        snap_date = _date.fromisoformat(snap)
    except (ValueError, TypeError):
        s["status"] = f"bad_date: {snap}"
        return s
    s["date"] = snap_date.isoformat()

    # fund_id: по isin_pif (надёжно), fallback на тикер из папки/имени.
    fid, _ = find_fund_id(engine, result.get("isin_pif"), folder_ticker(path) or parse_filename(path.name)[0])
    if not fid:
        s["status"] = f"fund_not_resolved (isin_pif={result.get('isin_pif')})"
        return s
    s["fund_id"] = fid

    with engine.connect() as conn:
        # Фича показывает только акционерные фонды — облигационные/деньги/золото
        # не нужны (Вадим). Пересчитываем weight ТОЛЬКО для category='stocks'.
        cat = conn.execute(text("SELECT category FROM funds WHERE fund_id = :f"),
                           {"f": fid}).scalar()
        if cat != "stocks":
            s["status"] = f"not_stocks ({cat}) — SKIP"
            return s

        agg = conn.execute(text("""
            SELECT COALESCE(SUM(amount_rub), 0), COALESCE(SUM(weight), 0), COUNT(*)
            FROM fund_holdings_history
            WHERE fund_id = :f AND snapshot_date = :d AND source = ANY(:src)
              AND amount_rub IS NOT NULL
        """), {"f": fid, "d": snap_date, "src": list(SOURCES)}).fetchone()
        sum_amt, sum_w_before, n = float(agg[0]), float(agg[1]), int(agg[2])
        s["sum_before"] = round(sum_w_before, 2)
        if n == 0:
            s["status"] = "no_rows_in_db — SKIP"
            return s
        # Guard: gross обязан быть ≥ суммы бумаг (gross = бумаги + деньги/прочее).
        # Если меньше — извлечение Подраздела 10 кривое, не трогаем.
        if gross < sum_amt * 0.98:
            s["status"] = f"GUARD: gross {gross:.0f} < Σamount {sum_amt:.0f} — SKIP"
            return s

        if commit:
            res = conn.execute(text("""
                UPDATE fund_holdings_history
                SET weight = ROUND((amount_rub / :g * 100)::numeric, 4)
                WHERE fund_id = :f AND snapshot_date = :d AND source = ANY(:src)
                  AND amount_rub IS NOT NULL
            """), {"g": gross, "f": fid, "d": snap_date, "src": list(SOURCES)})
            conn.commit()
            s["rows"] = res.rowcount
            after = conn.execute(text("""
                SELECT COALESCE(SUM(weight), 0) FROM fund_holdings_history
                WHERE fund_id = :f AND snapshot_date = :d AND source = ANY(:src)
                  AND amount_rub IS NOT NULL
            """), {"f": fid, "d": snap_date, "src": list(SOURCES)}).scalar()
            s["sum_after"] = round(float(after), 2)
            s["status"] = "UPDATED"
        else:
            s["rows"] = n
            s["sum_after"] = round(sum_amt / gross * 100, 2)  # прогноз Σ долей
            s["status"] = "dry-run"
    return s


def main(directory: str, commit: bool):
    setup_logging()
    path = Path(directory)
    if not path.is_dir():
        log.error(f"Папка {directory} не найдена")
        sys.exit(1)

    files = sorted(p for p in path.rglob("*") if p.suffix.lower() in SCHA_EXTS)
    log.info(f"Найдено {len(files)} SCHA-файлов в {directory} (commit={commit})")
    engine = get_engine()

    stats = {"updated": 0, "rows": 0, "skip_nogross": 0, "skip_other": 0, "guard": 0}
    suspicious = []
    for p in files:
        s = recompute_one(engine, p, commit)
        st = s["status"]
        if st in ("UPDATED", "dry-run"):
            stats["updated"] += 1
            stats["rows"] += s["rows"]
            flag = ""
            # Σ долей бумаг должна быть < 100 (остаток = деньги). >100.5 подозрительно.
            if s["sum_after"] and s["sum_after"] > 100.5:
                flag = "  ⚠️ Σ>100!"
                suspicious.append((s["file"], s["sum_after"]))
            log.info(f"  {st:8s} fid={s['fund_id']} {s['date']} "
                     f"gross={s['gross']/1e9:.3f}млрд rows={s['rows']} "
                     f"Σw {s['sum_before']}→{s['sum_after']}%{flag}")
        elif st.startswith("no_gross"):
            stats["skip_nogross"] += 1
            log.warning(f"  SKIP no_gross: {s['file']}")
        elif st.startswith("GUARD"):
            stats["guard"] += 1
            log.warning(f"  {s['status']}: {s['file']}")
        else:
            stats["skip_other"] += 1
            log.warning(f"  SKIP {s['status']}: {s['file']}")

    log.info("=" * 60)
    log.info(f"FINAL: обработано {stats['updated']} снапшотов "
             f"({stats['rows']} строк), skip_nogross={stats['skip_nogross']}, "
             f"guard={stats['guard']}, skip_other={stats['skip_other']}")
    if suspicious:
        log.warning(f"ПОДОЗРИТЕЛЬНЫЕ (Σ долей >100): {len(suspicious)}")
        for f, sw in suspicious[:20]:
            log.warning(f"  {f}: Σ={sw}%")
    if not commit:
        log.info("DRY-RUN — ничего не записано. Добавь --commit для записи.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python recompute_weights_gross.py <dir> [--commit]")
        sys.exit(1)
    main(sys.argv[1], "--commit" in sys.argv)
