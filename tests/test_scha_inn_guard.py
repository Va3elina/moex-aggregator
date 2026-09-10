#!/usr/bin/env python3
"""Тест защиты импорта справок о СЧА от ИНН эмитента в поле «количество».

Ловит регрессию 30.04.2026: у субординированных выпусков номиналом 10 млн ₽ (OBLG,
OPIF-54) парсер записал в количество ИНН ВТБ 7702070139 и Сбера 7707083893. Стоимость
была верной и крупной, цена за штуку выходила 0,03–0,27 ₽ — is_implausible_row с
порогом 0,0001 ₽ такое пропускал. Реальные крупные позиции (Элемент ~1,6 млрд штук,
ВТБ ао до обратного сплита ~2,5 млрд) при этом не должны задеваться.

Запуск:
    python tests/test_scha_inn_guard.py
    pytest tests/test_scha_inn_guard.py
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
# Модуль при импорте создаёт движок api.database из DB_URL. Движок ленивый — к базе
# никто не подключается, но без URL create_engine падает (нет .env в CI/worktree).
os.environ.setdefault("DB_URL", "postgresql+pg8000://test@localhost/test")

from Funds.manual_scha_backfill import is_implausible_row, is_inn_as_qty  # noqa: E402
from Funds.parsers.scha_parser import (  # noqa: E402
    _parse_assets_from_tables_rowwise, looks_like_issuer_code,
)


def test_rowwise_takes_quantity_not_inn() -> None:
    """Корень: строка облигации номиналом 10 млн ₽ — ИНН эмитента левее количества,
    цена за штуку 10,8 млн ₽ выше прежнего потолка 500 000 ₽ → парсер брал ИНН."""
    table = [
        ["Банк ВТБ (ПАО)", "7702070139", "1027739609391", "RU000A1014J2", "74",
         "797 625 117,94", "Рыночные котировки (уровень 1)"],
        # обычная акция: ОГРН и ИНН левее количества, цена ~130 ₽ — как было
        ["ПАО Сбербанк", "7707083893", "1027700132195", "RU0009029540", "3894300",
         "504 467 622,00", "Московская биржа"],
    ]
    got = {a["isin"]: a["positions"] for a in _parse_assets_from_tables_rowwise([table])}
    assert got == {"RU000A1014J2": 74, "RU0009029540": 3894300}, got


def test_rowwise_skips_receivables() -> None:
    """Дебиторка по облигации — не позиция (ячейки из справки ВИМ «Казначейский» 31.10.2023
    и Т-Капитал TBRU 31.03.2026). Старый парсер давал «41 185 000 шт. по 1 ₽»."""
    table = [
        ["2", "Погашение купона по облигации, сумма к получению", "643-RUB",
         "Условия выпуска ценных бумаг", "2023.10.31", "ПАО «Якутская топливно-энергетическая компания»",
         "1435032049", "-", "1021401062187", "Российская Федерация", "643",
         "1 745 420,30", "1 745 420,30", "Модель оценки", "-", "", "-", "RU000A102B48", "", "-"],
        ["", "Погашени е облигации, сумма к погашени ю", "", "643-RUB", "", "31.03.202 6 0:00:00",
         "4705081 944", "11947040 13350", "643", "58 633 01 3,85", "58 633 01 3,85",
         "Рыночны е котировк и (уровень 1)", "RU000A1 060Y4"],
    ]
    assert _parse_assets_from_tables_rowwise([table]) == []


def test_rowwise_gdr_not_country_code() -> None:
    """VK: в строке код BVI 092 — с потолком 100 млн ₽/шт он давал «1,1 млн ₽ за расписку».
    Настоящее количество 161 620 стоит левее регномера эмитента 655058 и позиционно не
    отличимо — это известный пробел (см. скилл moex-fund-scha-backfill), здесь — только
    что код страны не выбирается."""
    table = [["", "RCS Issuer Services S.AR.L.", "442", "B1372 39", "", "", "US5603 172082",
              "EDSXFR", "161 620", "VK COMPANY LIMITED", "", "655058", "Акции", "",
              "VGG576 3H1192", "092", "840-USD", "105 570 184", "Оценщик", "Модель оценки"]]
    got = _parse_assets_from_tables_rowwise([table])
    assert got and got[0]["positions"] != 92, got


def test_issuer_codes() -> None:
    for n in (7702070139, 7707083893, 7708503727,       # ИНН ВТБ, Сбера, РЖД
              1027739609391, 1027700132195):            # ОГРН ВТБ, Сбера
        assert looks_like_issuer_code(n), n
    for n in (74, 3894300, 8797190000, 1221840000, 7702070130):
        assert not looks_like_issuer_code(n), n


def test_catches_issuer_inn() -> None:
    # Строки прода 30.04.2026: (количество из справки, стоимость ₽)
    for pos, amt in [(7702070139, 797625117.94),    # OBLG ВТБ Т2-3
                     (7702070139, 2091071255.14),   # OPIF-54 ВТБ Т2-3
                     (7707083893, 251995548.00),    # OPIF-54 Сбер2СУБ2R
                     (7702070139, 88359378.94),     # OPIF-54 ВТБСУБТ1-1
                     (7708503727, 986807168.70)]:   # OPIF-54 РЖД 1Б-06
        assert is_inn_as_qty(pos, amt), (pos, amt)
        assert not is_implausible_row(pos, amt), "старая защита это и не ловила"


def test_keeps_real_positions() -> None:
    for pos, amt in [(74, 797625117.94),            # восстановленное количество
                     (1641319000, 1.97e8),          # OPIF-432 Элемент, ~0,12 ₽ за акцию
                     (2489520000, 4.5e7),           # EQMX ВТБ ао 2023, до обратного сплита
                     (8797190000, 1.988e8),         # SBMX ВТБ ао 2024: контрольная сумма ИНН
                     (1221840000, 1.616e8),         # SBMX Россети 2023 сходится случайно
                     (7702070139, 8.0e9),           # ИНН, но цена ≥ 1 ₽ — не наш случай
                     (7702070139, None),
                     (None, 797625117.94)]:
        assert not is_inn_as_qty(pos, amt), (pos, amt)


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
