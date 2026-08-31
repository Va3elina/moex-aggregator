"""Округление и формулировки брифа v4.

Зачем отдельные тесты на «как звучит фраза». Три разных дефекта пайплайна имели
ОДНУ природу: модель Шага В дословно переносит содержимое брифа в текст.
  • поле называлось `перекос_net_gross`  → в посте появился «перекос net/gross»;
  • поле называлось `..._диапазон_за_ряд` → модель написала «максимум за всё время»;
  • значение было `3.03`                  → в посте «выросло в 3,03 раза».
Ни один из трёх не лечится запретом в промпте — только на входе. Значит формат
значения такой же контракт, как и схема, и его надо фиксировать тестом.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from signals.content_ai import _money_ru, _pct, _raz, _times, _window_ru  # noqa: E402


def test_times_snaps_to_round_values():
    assert _times(3.03) == "в 3 раза"        # ← ровно тот случай из черновика 1104
    assert _times(2.0) == "в 2 раза"
    assert _times(5.0) == "в 5 раз"
    assert _times(2.4) == "в 2,5 раза"       # половинка круглее, чем «почти в 3»
    assert _times(3.5) == "в 3,5 раза"


def test_times_uses_words_for_small_multiples():
    assert _times(1.46) == "примерно в полтора раза"   # было «на 46,2%»
    assert _times(2.2) == "более чем вдвое"
    assert not _times(1.2), "рост на 20% кратностью не описывают"


def test_times_never_overstates():
    """Каждая фраза должна быть ПРАВДОЙ про своё r — на всём диапазоне 1,4…20×.

    Инвариант, а не набор примеров: «почти в N» допустимо только при r < N,
    «более чем в N» — только при r > N, голое «в N» — только при близком r.
    Словесные формы («вдвое», «полтора») проверяются наравне с цифровыми: пост
    читает человек, и для него «почти вдвое» при трёхкратном росте — такая же
    ложь, как «почти в 2 раза».
    """
    WORDS = {"примерно в полтора раза": lambda r: 1.4 <= r <= 1.75,
             "почти вдвое": lambda r: r < 2.0,
             "более чем вдвое": lambda r: r > 2.0}
    for r in [x / 20 for x in range(28, 400)]:
        phrase = _times(r)
        assert phrase, f"{r}: кратность не описана"
        if phrase in WORDS:
            assert WORDS[phrase](r), f"{r}: «{phrase}» — неправда"
            continue
        n = float(re.search(r"[\d,]+", phrase).group().replace(",", "."))
        if phrase.startswith("почти"):
            assert r < n, f"{r}: «{phrase}» — завышение"
        elif phrase.startswith("более чем"):
            assert r > n, f"{r}: «{phrase}» — занижение до неправды"
        else:
            assert abs(r - n) <= 0.15, f"{r}: округлено до {n} — слишком далеко"


def test_russian_numeral_agreement():
    assert _raz(2) == "раза" and _raz(5) == "раз"
    assert _raz(21) == "раз" and _raz(22) == "раза"
    assert _raz(12) == "раз", "12 — исключение, не «12 раза»"
    assert _times(22.0) == "в 22 раза" and _times(21.0) == "в 21 раз"


def test_pct_keeps_precision_only_where_it_is_honest():
    assert _pct(46.2) == "примерно на 45%"
    assert _pct(3.2) == "на 3,2%", "из 3,2% нельзя делать «примерно на 5%» — это вранье"
    assert _pct(7.4) == "на 7%"


def test_no_latin_or_stray_dots_anywhere():
    """Запятая вместо точки (_ru) и никакого латинского жаргона в значениях."""
    vals = [f(x) for f, xs in ((_times, (1.46, 3.03, 12.7)), (_pct, (3.2, 46.2)),
                               (_window_ru, (30, 400)), (_money_ru, (91.81, 1234.7)))
            for x in xs]
    for v in vals:
        assert not re.search(r"[A-Za-z]", v), v
        assert not re.search(r"\d\.\d", v), f"точка в числе: {v}"


def test_window_spoken_not_counted_in_days():
    assert _window_ru(400) == "год", "«за последние 400 дней» протекало в текст"
    assert _window_ru(180) == "полгода"
    assert "дн" not in _window_ru(400)


def test_money_drops_meaningless_kopecks():
    assert _money_ru(91.81) == "около 92"       # было «₽91,81»
    assert _money_ru(1234.7) == "около 1 230"
    assert _money_ru(4.55) == "4,55", "у дешёвой бумаги копейки — основная часть цены"
