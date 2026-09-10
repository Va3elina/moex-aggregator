#!/usr/bin/env python3
"""Тест миграции 093: один источник на срез состава фонда.

Гоняет сам файл db/migrations/093_fund_holdings_one_source.sql на SQLite
(синтаксис миграции общий с Postgres) против среза, повторяющего прод OBLG
на 30.04.2026: одна справка о СЧА лежит дважды — vim_sdr (битые штуки) и
interfax_manual. Карточка фонда отдавала обе копии: 135 строк на 70 ISIN,
сумма долей 184 %.

  • в срезе остаётся только interfax_manual, двойников по ISIN нет;
  • vim_sdr без пары (другой фонд, даты до прихода interfax) не тронут;
  • прочие источники (ежедневный vim) не тронуты;
  • удалённое лежит в fund_holdings_history_shadowed и возвращается откатом;
  • повторный прогон ничего не меняет.

Запуск:
    python tests/test_fund_holdings_one_source.py
    pytest tests/test_fund_holdings_one_source.py
"""
import sqlite3
import sys
from pathlib import Path

MIGRATION = (Path(__file__).parent.parent / "db" / "migrations"
             / "093_fund_holdings_one_source.sql")

OBLG, EQMX = 12000, 6073
COLS = "id, fund_id, snapshot_date, asset_name, isin, weight, positions, amount_rub, source"
ROWS = [
    # OBLG 30.04.2026 — одна справка дважды. У vim_sdr штуки битые (баг ×1000),
    # у interfax_manual есть ВТБ Т2-3, которого vim_sdr не видит.
    (1, OBLG, "2026-04-30", "Славнеф1Р3", "RU000A1013U1", 9.6078, 15664564, 696921621.16, "vim_sdr"),
    (2, OBLG, "2026-04-30", "ФосАгро2П6", "RU000A10ER66", 7.0519, 16500, 511525000.0, "vim_sdr"),
    (3, OBLG, "2026-04-30", "Славнеф1Р3", "RU000A1013U1", 7.0254, 664564, 696921621.16, "interfax_manual"),
    (4, OBLG, "2026-04-30", "ФосАгро2П6", "RU000A10ER66", 5.1565, 500000, 511525000.0, "interfax_manual"),
    (5, OBLG, "2026-04-30", "ВТБ Т2-3", "RU000A1014J2", 8.0406, 797600, 797600000.0, "interfax_manual"),
    # Ежедневный ВИМ на ту же дату — не месячный источник, не трогаем.
    (6, OBLG, "2026-04-30", "Славнеф1Р3", "RU000A1013U1", 7.0, 664564, 696921621.16, "vim"),
    # До прихода interfax (2023-10) vim_sdr — единственная копия среза.
    (7, OBLG, "2023-09-29", "Славнеф1Р3", "RU000A1013U1", 4.2, 500000, 5.0e8, "vim_sdr"),
    # EQMX: vim_sdr без пары — остаётся.
    (8, EQMX, "2026-04-30", "Сбербанк", "RU0009029540", 12.3, 1000000, 3.1e8, "vim_sdr"),
]
ROLLBACK = "INSERT INTO fund_holdings_history SELECT * FROM fund_holdings_history_shadowed"


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE fund_holdings_history (
        id INTEGER PRIMARY KEY, fund_id INTEGER, snapshot_date TEXT, asset_name TEXT,
        isin TEXT, weight REAL, positions INTEGER, amount_rub REAL, source TEXT)""")
    conn.executemany(f"INSERT INTO fund_holdings_history ({COLS}) VALUES ({','.join('?' * 9)})", ROWS)
    conn.commit()
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    conn.executescript(MIGRATION.read_text(encoding="utf-8"))


def _rows(conn: sqlite3.Connection, table: str = "fund_holdings_history") -> set[tuple]:
    return set(conn.execute(f"SELECT {COLS} FROM {table}"))


def test_one_source_per_snapshot() -> None:
    conn = _db()
    _migrate(conn)
    mixed = conn.execute("""
        SELECT fund_id, snapshot_date FROM fund_holdings_history
        WHERE source IN ('vim_sdr', 'interfax_manual')
        GROUP BY 1, 2 HAVING count(DISTINCT source) > 1""").fetchall()
    assert mixed == [], f"срезы с двумя источниками: {mixed}"
    card = conn.execute("""
        SELECT isin, weight, positions FROM fund_holdings_history
        WHERE fund_id = ? AND snapshot_date = '2026-04-30'
          AND source IN ('vim_sdr', 'interfax_manual')
        ORDER BY isin""", (OBLG,)).fetchall()
    assert card == [("RU000A1013U1", 7.0254, 664564),
                     ("RU000A1014J2", 8.0406, 797600),
                     ("RU000A10ER66", 5.1565, 500000)], card


def test_only_shadowed_vim_sdr_removed() -> None:
    conn = _db()
    _migrate(conn)
    assert _rows(conn) == {r for r in ROWS if r[0] not in (1, 2)}
    assert _rows(conn, "fund_holdings_history_shadowed") == {ROWS[0], ROWS[1]}


def test_idempotent() -> None:
    conn = _db()
    _migrate(conn)
    after_first = _rows(conn), _rows(conn, "fund_holdings_history_shadowed")
    _migrate(conn)
    assert (_rows(conn), _rows(conn, "fund_holdings_history_shadowed")) == after_first


def test_rollback_restores() -> None:
    conn = _db()
    _migrate(conn)
    conn.execute(ROLLBACK)
    assert _rows(conn) == set(ROWS)


if __name__ == "__main__":
    failed = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  PASS  {name}")
            except AssertionError as e:
                failed += 1
                print(f"  FAIL  {name}: {e}")
    print()
    if failed:
        print(f"FAILED: {failed}")
        sys.exit(1)
    print("Все проверки пройдены.")
