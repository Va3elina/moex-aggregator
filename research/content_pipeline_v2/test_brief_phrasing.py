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


# ─────────────────────────────────────────────────────────────────────────────
# Инвариант «судья видит то же, что писатель»
# ─────────────────────────────────────────────────────────────────────────────

import datetime as _dt  # noqa: E402
import json as _json  # noqa: E402

from signals import content_ai as CA  # noqa: E402

_ROW = {
    "id": 1104, "headline": "Газпром отчитался по РСБУ", "raw_text": "Газпром…",
    "tickers": ["GAZP"], "event_type": "earnings", "asset_id": "GAZPF",
    "asset_name": "Газпром", "anomaly_clgroup": "FIZ", "severity_value": 4.69,
    "signal_date": _dt.date(2026, 7, 30), "created_at": None,
    "thread_key": "GAZP:earnings", "draft_text": "Черновик",
}


def _stub_brief_sources(monkeypatch):
    monkeypatch.setattr(CA, "_story_frame", lambda *a: "РЕАКЦИЯ")
    monkeypatch.setattr(CA, "_position_phrases", lambda *a, **k: {"ГЛАВНОЕ_ЧИСЛО": "в 3 раза"})
    monkeypatch.setattr(CA, "_price_context", lambda *a: {"цена_сейчас": "около 92"})
    monkeypatch.setattr(CA, "_prior_post_line", lambda *a: "(нет)")


def _brief_of(payload: str) -> dict:
    return _json.loads(payload.rsplit("\ninternal_token:", 1)[0])


def test_judge_receives_exactly_the_writers_brief(monkeypatch):
    """Судья обязан видеть РОВНО тот бриф, по которому написан черновик.

    Регрессия кандидата 1104: бриф собирался дважды руками, и в версию судьи не
    попал блок цена_акции из v3. Судья честно назвал верные числа «выдуманными»
    — он не ошибся, он отвечал на другой вопрос. Асимметрия контекста в
    LLM-as-judge не видна по вердикту: он выглядит осмысленным и обоснованным.
    Поэтому проверяем совпадение входов кодом, а не глазами по логу.
    """
    _stub_brief_sources(monkeypatch)
    writer = _brief_of(CA._step_c_payload(None, _ROW, "tok"))
    judge = _brief_of(CA._step_g_payload(None, _ROW, "tok"))["бриф"]
    assert judge == writer, (
        "бриф судьи разошёлся с брифом писателя по полям: "
        + ", ".join(sorted(set(writer) ^ set(judge)))
    )


def test_price_block_is_in_both_briefs(monkeypatch):
    """Именно этого поля не хватало судье — фиксируем отдельно и явно."""
    _stub_brief_sources(monkeypatch)
    for name, payload in (("писатель", CA._step_c_payload(None, _ROW, "tok")),
                          ("судья", CA._step_g_payload(None, _ROW, "tok"))):
        assert "цена_акции" in payload, f"{name} не получил блок цена_акции"


def test_judge_payload_carries_the_draft(monkeypatch):
    _stub_brief_sources(monkeypatch)
    out = _json.loads(CA._step_g_payload(None, _ROW, "tok").rsplit("\ninternal_token:", 1)[0])
    assert out["черновик_на_проверку"] == "Черновик"
    assert out["candidate_id"] == 1104


# ─────────────────────────────────────────────────────────────────────────────
# Парная связка «позиция ↔ цена»
# ─────────────────────────────────────────────────────────────────────────────

_PRICE = {"цена_сейчас": "около 92", "цена_за_месяц": "упала на 7%",
          "цена_за_полгода": "упала примерно на 30%", "цена_за_год": "упала примерно на 25%"}


def test_pair_uses_the_same_window_as_the_lead_number():
    """Одинаковое окно у позиции и цены — иначе сравнение не сравнение."""
    out = CA._pair_price_with_position(dict(_PRICE), {
        "_код_период_главного_числа": "за_год",
        "_код_фраза_главного_числа": "чистый лонг вырос в 3 раза"})
    assert out["ГЛАВНОЕ_СРАВНЕНИЕ"].startswith(
        "за год: акция упала примерно на 25%, а чистый лонг вырос в 3 раза")
    assert "за_полгода" in out["остальные_горизонты_упоминать_не_обязательно"]
    assert "цена_за_год" not in out, "ведущий горизонт не должен дублироваться"


def test_pair_names_both_windows_when_they_differ():
    """Ведущее окно позиции — сутки; у цены суточного горизонта нет.

    Нельзя выдавать разные окна за одно: «за сутки лонг втрое, акция вдвое» —
    ложь, которую читатель не поймает. Проговариваем оба срока явно.
    """
    out = CA._pair_price_with_position(dict(_PRICE), {
        "_код_период_главного_числа": "за_сутки",
        "_код_фраза_главного_числа": "толпа перевернулась из чистого лонга в чистый шорт"})
    pair = out["ГЛАВНОЕ_СРАВНЕНИЕ"]
    assert "за сутки" in pair and "за год" in pair, pair


def test_pair_reduces_number_count():
    """Смысл правки — плотность. Было 4 равноправных значения, стало 1 связка."""
    before = len([k for k in _PRICE if k.startswith("цена_за_")]) + 1
    out = CA._pair_price_with_position(dict(_PRICE), {
        "_код_период_главного_числа": "за_год", "_код_фраза_главного_числа": "лонг вырос в 3 раза"})
    top = [k for k in out if not k.startswith("остальные")]
    assert len(top) < before, f"{len(top)} против {before}"


def test_pair_survives_missing_price_data():
    """У фьючерса без акции блок цены пустой — связки просто нет, падать нельзя."""
    assert CA._pair_price_with_position({}, {"_код_период_главного_числа": "за_год",
                                             "_код_фраза_главного_числа": "x"}) == {}
    assert CA._pair_price_with_position(dict(_PRICE), {}) == _PRICE


def test_service_keys_never_reach_the_model(monkeypatch):
    """Ключи «_код_» — для кода. В брифе их быть не должно: любое видимое поле
    модель считает обязанной израсходовать (тот же механизм, что убил
    market_rank и recent_signals)."""
    monkeypatch.setattr(CA, "_story_frame", lambda *a: "РЕАКЦИЯ")
    monkeypatch.setattr(CA, "_position_phrases", lambda *a, **k: {
        "_код_период_главного_числа": "за_год", "_код_фраза_главного_числа": "лонг вырос в 3 раза",
        "ГЛАВНОЕ_ЧИСЛО": "за_год: лонг вырос в 3 раза"})
    monkeypatch.setattr(CA, "_price_context", lambda *a: dict(_PRICE))
    monkeypatch.setattr(CA, "_prior_post_line", lambda *a: "(нет)")
    for name, payload in (("писатель", CA._step_c_payload(None, _ROW, "tok")),
                          ("судья", CA._step_g_payload(None, _ROW, "tok"))):
        assert "_код_" not in payload, f"служебный ключ утёк в бриф {name}"
        assert "ГЛАВНОЕ_СРАВНЕНИЕ" in payload, f"{name} не получил связку"


# ─────────────────────────────────────────────────────────────────────────────
# Рамка сюжета: один день ≠ упреждение (правка Вадима по 1638)
# ─────────────────────────────────────────────────────────────────────────────

def _frame(days_offset: int) -> str:
    """days_offset < 0 — сигнал РАНЬШЕ новости."""
    news = _dt.date(2026, 8, 4)
    return CA._story_frame(news + _dt.timedelta(days=days_offset), news)


def test_one_day_lead_is_not_anticipation():
    """Кандидат 1638 (АКРА/AFKS): отрыв в один день, а пост заявил предвидение.

    Прежняя рамка при d=-1 выдавала «только в этой рамке можно говорить, что толпа
    встала заранее» — бриф САМ выдавал лицензию, и модель ей воспользовалась.
    Вадим: «фьючерс поменялся за день и спрогнозировало — спорное заявление».
    """
    assert _frame(-1).startswith("СОВПАДЕНИЕ")
    assert "НЕЛЬЗЯ" in _frame(-1)


def test_anticipation_needs_at_least_two_days():
    assert _frame(-2).startswith("УПРЕЖДЕНИЕ")
    assert _frame(0).startswith("СОВПАДЕНИЕ")
    assert _frame(3).startswith("РЕАКЦИЯ")


def test_no_frame_ever_licenses_foresight():
    """Даже при большом отрыве порядок дат можно КОНСТАТИРОВАТЬ, но не толковать
    как предвидение: «и кто знает» — это про отсутствие причинности в данных,
    сколько бы дней ни было."""
    for off in (-30, -5, -2, -1, 0, 1, 10):
        f = _frame(off)
        licensed = "заранее" in f or "спрогнозир" in f
        forbidden = any(w in f for w in ("НЕЛЬЗЯ", "нельзя", "запрещены", "не доказывает"))
        assert not licensed or forbidden, f
    assert "не доказывает предвидение" in _frame(-5)


def test_reversal_phrase_has_no_size_clause():
    """Вадим по 1638: «ну и извращенское заявление, предыдущего хватает более чем».
    Сам факт разворота самодостаточен; сравнение размеров позиций разного знака
    читателю ничего не добавляет."""
    from signals.db import get_position_series  # noqa: F401  (документируем зависимость)
    # Ветка разворота живёт в замыкании phrase() внутри _position_phrases, поэтому
    # проверяем по исходнику: фраза не должна содержать сравнения размеров.
    import inspect
    src = inspect.getsource(CA._position_phrases)
    rev = src[src.index("if (old_v > 0) != (new_v > 0):"):src.index("grew = abs")]
    assert "крупнее прежнего" not in rev and "меньше прежнего" not in rev, rev
    assert "того же размера" not in rev
    assert 'толпа перевернулась из чистого {was}а в чистый {now}' in rev


# ─────────────────────────────────────────────────────────────────────────────
# Защита кнопки «Править» в ревью-боте
# ─────────────────────────────────────────────────────────────────────────────

from signals.content_review_bot import _looks_like_post  # noqa: E402

_REAL_POST = ("АКРА режет рейтинг, толпа уже в лонге⚡\n\n"
              "◽️АКРА понизило рейтинг АФК Системы до A+(RU).\n\n"
              "◽️Розница развернулась из шорта в лонг.\n\n#открытыепозиции")
# Дословно то, что Вадим отправил в поле правки кандидата 1638.
_REAL_CRITIQUE = ("фьючерс поменялся за день  и спрогнозировало - спорное заявление и кто знает \n\n"
                  "Хотя новая вышла в два раза меньше прежней короткой - ну и извращенское "
                  "заявление предыдущего хватате более чем \n\n"
                  "в рф нет культуры рейтинговых агентств - всем будет пофиг и это надо проверять")


def test_real_critique_is_not_mistaken_for_a_post():
    """Регрессия 1638: этот текст стал текстом поста и мог уйти в канал."""
    assert not _looks_like_post(_REAL_CRITIQUE, _REAL_POST)


def test_real_post_passes_unchallenged():
    assert _looks_like_post(_REAL_POST, _REAL_POST)


def test_post_recognised_by_format_even_when_short():
    """Короткая, но оформленная переписка — это пост, лишний вопрос не нужен."""
    assert _looks_like_post("◽️Коротко и по делу.\n\n#открытыйинтерес", _REAL_POST)
    assert _looks_like_post("Заголовок\n\nОдин абзац.\n\n#открытыепозиции", _REAL_POST)


def test_length_alone_never_decides():
    """Длина не отличает разбор от поста — на этом и падал первый вариант.

    Реальная критика 1638 длиннее, чем короткий оформленный пост. Любой запас по
    длине пропустил бы её. Признак только один — формат.
    """
    assert not _looks_like_post("а" * 5000, _REAL_POST), "длина не аргумент"
    assert len(_REAL_CRITIQUE) > len("◽️Коротко.\n\n#открытыйинтерес")
    assert not _looks_like_post(_REAL_CRITIQUE, "◽️Коротко.\n\n#открытыйинтерес")


def test_guard_survives_missing_current_draft():
    assert not _looks_like_post("короткий комментарий", None)
    assert not _looks_like_post("   ", _REAL_POST)
