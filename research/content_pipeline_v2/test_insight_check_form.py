"""Форма находки в проверке кодом и словарь тем связок (разбор завода 24.09).

python3 -m pytest research/content_pipeline_v2/test_insight_check_form.py -q
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from api.services import insight_check  # noqa: E402

CARD = "21 марта 2025 года; 1 июля 2025 года; 4 июля 2025 года; 81%; 8500; 1600; 30"


def _post(paras: int, body: str = "Шорт у исторического максимума.") -> str:
    return "Заголовок 📣\n" + "\n".join(f"◽️ {body} " + "слово " * 40 for _ in range(paras)) + "\n#открытыепозиции"


def test_dates_do_not_count_toward_number_cap():
    body = ("Так было с 21 марта 2025 года по 1 июля 2025 года, разворот 4 июля 2025 года. "
            "Бумага упала на 81%, с 8500 до 1600, минус 30 за месяц.")
    post = "Заголовок 📣\n◽️ " + body + "\n◽️ " + "слово " * 80 + "\n#открытыепозиции"
    r = insight_check.check(post, CARD)
    assert r["numbers"] == 4, r
    assert not any("чисел" in d for d in r["defects"]), r["defects"]


def test_paragraphs_two_to_four_by_topic():
    for n in (2, 3, 4):
        assert not any("абзацев" in d for d in insight_check.check(_post(n), CARD)["defects"]), n
    for n in (1, 5):
        assert any("абзацев" in d for d in insight_check.check(_post(n), CARD)["defects"]), n


def _lex():
    src = (ROOT / "signals/insights/combos.py").read_text(encoding="utf-8")
    return eval(re.search(r"^LEX = (\{.*?\})\n\n", src, re.S | re.M).group(1))


def test_currency_theme_ignores_plain_rubles():
    rx = _lex()["валюта"]
    # пост канала про ребаланс фондов 22.09 — не валютный, хотя в нём «млрд рублей»
    assert not re.search(rx, "Фонды продадут бумаги на 1,2 млрд рублей и докупят другие", re.I)
    for t in ("Спекулянты уходят из валюты", "курс рубля к доллару", "рубль укрепился", "отток из юаневых фондов"):
        assert re.search(rx, t, re.I), t
