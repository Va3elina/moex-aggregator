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
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from Funds.manual_scha_backfill import is_implausible_row, is_inn_as_qty  # noqa: E402


def test_catches_issuer_inn() -> None:
    # Строки прода 30.04.2026: (количество из справки, стоимость ₽)
    for pos, amt in [(7702070139, 797625117.94),    # OBLG ВТБ Т2-3
                     (7702070139, 2091071255.14),   # OPIF-54 ВТБ Т2-3
                     (7707083893, 251995548.00),    # OPIF-54 Сбер2СУБ2R
                     (7702070139, 88359378.94)]:    # OPIF-54 ВТБСУБТ1-1
        assert is_inn_as_qty(pos, amt), (pos, amt)
        assert not is_implausible_row(pos, amt), "старая защита это и не ловила"


def test_keeps_real_positions() -> None:
    for pos, amt in [(74, 797625117.94),            # восстановленное количество
                     (1641319000, 1.97e8),          # OPIF-432 Элемент, ~0,12 ₽ за акцию
                     (2489520000, 4.5e7),           # EQMX ВТБ ао 2023, до обратного сплита
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
