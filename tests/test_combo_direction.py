"""Направление ставки в карточке связки: сторона позиции не должна переворачиваться
из-за пояснений «без вычета шортов» / «лонги минус шорты»."""
from signals.insights.combos import direction, legible as ready_title


def _d(raw: str) -> str:
    return direction(ready_title(raw))


def test_long_side_with_clarification_is_long():
    d = _d("USD/RUB: покупки физлиц (длинная сторона) сократились на 9% за день")
    assert "ставка на рост валюты" in d and "меньше" in d


def test_short_side_is_short():
    assert "ставка на крепкий рубль" in _d("USD/RUB: шорт физлиц (короткая сторона) вырос на 12%")


def test_net_long_and_net_short():
    assert "ставка на рост рынка" in _d("Индекс МосБиржи: чистый лонг физлиц вырос до максимума")
    assert "ставка на падение рынка" in _d("Индекс МосБиржи: чистый шорт физлиц сократился")


def test_net_position_is_long_bet():
    assert "ставка на рост цены" in _d("Золото: чистая позиция физлиц выросла")


def test_people_count_in_short():
    assert "ставка на падение цены" in direction("Золото: число физлиц в шорте выросло на 40%")
