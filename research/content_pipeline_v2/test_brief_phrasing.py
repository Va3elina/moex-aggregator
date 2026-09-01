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
    # ⚠️ Каждый НОВЫЙ источник данных брифа обязан попасть в эту заглушку. Именно
    # так тест паритета и поймал добавление _related_context: без подмены он полез
    # в db=None. Это и есть польза от проверки инварианта, а не набора полей.
    monkeypatch.setattr(CA, "_related_context", lambda *a: {})


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
    monkeypatch.setattr(CA, "_related_context", lambda *a: {})
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
    f = _frame(-1)
    assert f.startswith("СОВПАДЕНИЕ")
    assert "НЕ заявляй предвидение" in f


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


# ─────────────────────────────────────────────────────────────────────────────
# Связанные компании: выбор фрагмента и ловушка Озон / ОзонФарма
# ─────────────────────────────────────────────────────────────────────────────

# Реальный дайджест «Итоги дня» из news_archive 24.08.2026 — именно в таком посте
# лежала вся фактура по Озону, и именно такой пост нельзя отдавать в бриф целиком.
_DIGEST = """Атаки БПЛА на Озон⚡️Итоги дня

📉Сбербанк -0.5% Многие сомневались, что дивиденды утвердят

📉Система -12% 📉Сегежа -9.5% Озон тянет за собой вниз Систему. Проблемы у Системы могут стать проблемами для Сегежи🧐

📈НЛМК +3% Металлурги дружно растут без явных новостей

📊Полная карта рынка"""

_SINGLE = """📉Озон -21%

Уже третий день подряд идут атаки БПЛА на логистические центры Озона."""


def test_digest_yields_only_the_relevant_paragraph():
    """Из дайджеста берём строку про нужную компанию, а не весь пост."""
    sn = CA._pick_snippet(_DIGEST, nt=12, name="Система")
    assert "Система -12%" in sn
    assert "НЛМК" not in sn and "Сбербанк" not in sn


def test_single_ticker_post_taken_from_the_top():
    sn = CA._pick_snippet(_SINGLE, nt=1, name="Озон")
    assert "Озон -21%" in sn and "БПЛА" in sn


def test_ozon_does_not_match_ozonfarma():
    """Ловушка, на которую я наступил при разведке: поиск словом «озон» тащил
    Озон Фармацевтику (OZPH) — другую компанию. Совпадение по слову целиком."""
    assert CA._pick_snippet("📈ОзонФарма +2% отчёт за полугодие", 12, "Озон") == ""
    assert CA._pick_snippet("📈Озон +2% выросли на новостях", 12, "Озон") != ""


def test_snippet_is_capped_and_safe_on_junk():
    assert len(CA._pick_snippet("а" * 5000, 1, "Озон")) <= 220
    assert CA._pick_snippet("", 1, "Озон") == ""
    assert CA._pick_snippet(None, 1, "Озон") == ""
    assert CA._pick_snippet(_DIGEST, 12, "Лукойл") == "", "нет упоминания — нет фрагмента"


def test_related_block_absent_when_no_links(monkeypatch):
    """Пустое «связанные_компании: {}» в брифе провоцирует придумать связь —
    поле должно исчезать целиком (тот же механизм, что убил market_rank)."""
    monkeypatch.setattr(CA, "_story_frame", lambda *a: "РЕАКЦИЯ")
    monkeypatch.setattr(CA, "_position_phrases", lambda *a, **k: {"ГЛАВНОЕ_ЧИСЛО": "x"})
    monkeypatch.setattr(CA, "_price_context", lambda *a: {})
    monkeypatch.setattr(CA, "_prior_post_line", lambda *a: "(нет)")
    monkeypatch.setattr(CA, "_related_context", lambda *a: {})
    for payload in (CA._step_c_payload(None, _ROW, "t"), CA._step_g_payload(None, _ROW, "t")):
        assert "связанные_компании" not in payload


def test_related_block_carries_context_warning(monkeypatch):
    """Если связь есть — рядом обязана быть подпись «контекст, не причина»."""
    monkeypatch.setattr(CA, "_story_frame", lambda *a: "РЕАКЦИЯ")
    monkeypatch.setattr(CA, "_position_phrases", lambda *a, **k: {"ГЛАВНОЕ_ЧИСЛО": "x"})
    monkeypatch.setattr(CA, "_price_context", lambda *a: {})
    monkeypatch.setattr(CA, "_prior_post_line", lambda *a: "(нет)")
    monkeypatch.setattr(CA, "_related_context", lambda *a: {
        "OZON": {"связь": "крупный акционер"},
        "ПОЯСНЕНИЕ": "КОНТЕКСТ, НЕ ПРИЧИНА. …"})
    payload = CA._step_c_payload(None, _ROW, "t")
    assert "связанные_компании" in payload and "НЕ ПРИЧИНА" in payload


def test_links_block_states_where_it_goes(monkeypatch):
    """Место блока в тексте — часть смысла, и указание живёт рядом с данными.

    Судья по черновику 1638: «соседство с абзацем о позициях толпы может подтолкнуть
    читателя додумать связь, которая прямо не утверждается». Причинность возникала
    не из слов, а из порядка абзацев — значит и лечить это надо там, где данные, а
    не очередным правилом в промпте.
    """
    calls = {}

    class _DB:
        def execute(self, q, params=None):
            sql = str(q)
            calls["sql"] = sql

            class _R:
                @staticmethod
                def fetchall():
                    if "world_facts" in sql:
                        return [(["AFKS", "OZON"], "Система — крупный акционер Озона")]
                    return []

                @staticmethod
                def scalar():
                    return "Озон"
            return _R()

    import datetime as _d
    blk = CA._related_context(_DB(), ["AFKS"], _d.date(2026, 9, 1))
    assert "МЕСТО В ТЕКСТЕ" in blk["ПОЯСНЕНИЕ"]
    assert "РАНЬШЕ" in blk["ПОЯСНЕНИЕ"]
    assert "НЕ ПРИЧИНА" in blk["ПОЯСНЕНИЕ"], "старое предупреждение не должно пропасть"


# ─────────────────────────────────────────────────────────────────────────────
# Прежний уровень рейтинга из архива
# ─────────────────────────────────────────────────────────────────────────────

# Дословные строки из news_archive по AFKS.
_L_ACRA = 'АКРА подтвердило рейтинг АФК "Система" и ее облигаций на уровне AA-(RU)*, прогноз негативный'
_L_EXPERT = 'Эксперт РА понизил кредитный рейтинг АФК «Система» и её облигаций с ruAA- до ruA+ и изменило прогноз'
_L_SUBSIDIARY = 'Эксперт РА присвоил кредитный рейтинг агрохолдингу «Степь» на уровне ruBBB+ - АФК "Система"'


def _levels(line):
    return [x for x in CA._RE_SCALE.findall(line) if CA._plausible_level(x)]


def test_last_level_in_a_prior_action_is_the_level_in_force():
    """У подтверждения уровень один; у понижения последний — тот, что действовал ДО
    нынешней новости. Именно он и есть «прежний уровень»."""
    assert _levels(_L_ACRA) == ["AA-(RU)"]
    assert _levels(_L_EXPERT)[-1] == "ruA+"
    assert _levels('S&P ПОВЫСИЛО РЕЙТИНГИ С "B+" ДО "BB-"')[-1] == "BB-"


def test_stray_latin_letters_are_not_levels():
    """«B» из «B2B» проходит границы слова и стала бы «прежним уровнем» — цифра
    выглядела бы правдоподобно, и ошибку никто бы не заметил."""
    assert _levels("АКРА про сегмент B2B и рейтинг AA(RU)") == ["AA(RU)"]
    assert not CA._plausible_level("B") and not CA._plausible_level("C")
    assert CA._plausible_level("ruA") and CA._plausible_level("AA-") and CA._plausible_level("A+")


def test_name_stems_survive_declension_and_quotes():
    assert CA._name_stems("АФК Система") == ["Систе"]
    for form in ('АФК "Система"', "АФК «Системы»", "СИСТЕМЫ"):
        assert any(st.lower() in form.lower() for st in CA._name_stems("АФК Система")), form


def _rating_db(lines, name="АФК Система"):
    class _DB:
        def execute(self, q, params=None):
            sql = str(q)

            class _R:
                @staticmethod
                def scalar():
                    return name

                @staticmethod
                def fetchall():
                    return lines if "news_archive" in sql else []
            return _R()
    return _DB()


def test_previous_level_comes_from_the_same_agency():
    """⚠️ Ключевая защита. У АКРА уровень AA-(RU), у Эксперт РА — ruAA-: разные шкалы.
    Подставить прежний уровень другого агентства = фактическая ошибка, которую никто
    не заметит, потому что цифра выглядит правдоподобно.
    """
    import datetime as _d
    db = _rating_db([(_d.date(2026, 6, 30), _L_EXPERT), (_d.date(2025, 12, 30), _L_ACRA)])
    out = CA._rating_history(db, "АКРА ПОНИЗИЛО КРЕДИТНЫЙ РЕЙТИНГ АФК СИСТЕМА ДО A+(RU)",
                             "", ["AFKS"], _d.date(2026, 9, 1))
    assert "AA-(RU)" in out["ПРЕЖНИЙ_УРОВЕНЬ"], out["ПРЕЖНИЙ_УРОВЕНЬ"]
    assert "ruA+" not in out["ПРЕЖНИЙ_УРОВЕНЬ"], "взят уровень другого агентства"
    assert "АКРА" in out["ПРЕЖНИЙ_УРОВЕНЬ"]


def test_no_previous_level_when_agency_never_acted_before():
    """Нет прошлого действия того же агентства — поля нет, и промпт запрещает
    называть уровень. Молчание честнее подстановки."""
    import datetime as _d
    db = _rating_db([(_d.date(2026, 6, 30), _L_EXPERT)])
    out = CA._rating_history(db, "НКР понизило рейтинг АФК Система", "", ["AFKS"],
                             _d.date(2026, 9, 1))
    assert "ПРЕЖНИЙ_УРОВЕНЬ" not in out
    assert out["прошлые_действия"], "сами действия всё равно полезны как контекст"
    assert "НЕЛЬЗЯ" in out["ПОЯСНЕНИЕ"]


def test_subsidiary_action_is_filtered_out():
    """В старых постах под тем же тикером попадаются действия по ДОЧКАМ."""
    import datetime as _d
    db = _rating_db([(_d.date(2026, 5, 1), _L_SUBSIDIARY)])
    out = CA._rating_history(db, "АКРА понизило рейтинг Системы", "", ["AFKS"],
                             _d.date(2026, 9, 1))
    assert "ruBBB+" not in json_dumps(out), out


def json_dumps(o):
    import json
    return json.dumps(o, ensure_ascii=False)


def test_block_absent_for_non_rating_news():
    """История рейтингов в брифе про отчётность — насыпанное поле, которое модель
    обязана израсходовать."""
    import datetime as _d
    db = _rating_db([(_d.date(2025, 12, 30), _L_ACRA)])
    assert CA._rating_history(db, "Убыток Газпрома по РСБУ во 2кв 2026", "", ["GAZP"],
                              _d.date(2026, 9, 1)) == {}


def test_issuer_must_be_named_before_the_level():
    """Признак настоящего действия — порядок: «агентство … рейтинг КОМУ … уровень X».

    На строке про дочку «Система» стоит в подписи, уже ПОСЛЕ уровня — простая
    проверка «имя есть в строке» её пропускала, и ruBBB+ уходил бы в бриф как
    прежний уровень Системы.
    """
    stems = CA._name_stems("АФК Система")
    assert CA._issuer_named_before_level(_L_ACRA, stems)
    assert CA._issuer_named_before_level(_L_EXPERT, stems)
    assert CA._issuer_named_before_level('S&P ПОВЫСИЛО РЕЙТИНГИ "СИСТЕМЫ" С "B+" ДО "BB-"', stems)
    assert not CA._issuer_named_before_level(_L_SUBSIDIARY, stems)


def test_share_range_is_marked_not_for_text():
    """Диапазон доли — проверка на рекорд, а не содержание поста.

    Вадим 01.09 про «а за год доля доходила до 50%»: «сложная формулировка и она не
    понятна сразу, нужно проще». Поле существует, чтобы модель НЕ соврала про
    рекорд, — значит имя поля должно это и говорить (имя поля есть интерфейс).
    """
    import inspect
    src = inspect.getsource(CA._position_phrases)
    assert "не_для_текста_проверка_рекорда_за_" in src
    assert "эта_доля_за_" not in src, "прежнее имя приглашало вынести диапазон в текст"
    assert "В ТЕКСТ ЭТОТ ДИАПАЗОН НЕ ВЫНОСИТЬ" in src


def test_context_blocks_declare_volume_limit():
    """Оба новых блока — фон. Без границы модель тратит по абзацу на каждый пункт,
    и пост распухает: 1105 знаков против медианы жанра 661."""
    import inspect
    for fn in (CA._related_context, CA._rating_history):
        src = inspect.getsource(fn)
        assert "ОБЪЁМ" in src, fn.__name__
        assert "ФОН" in src, fn.__name__


def test_peak_comparison_is_gone_from_the_brief():
    """Вадим 01.09 про «текущий лонг в 11 раз меньше пикового значения за год»:
    «такие факты нам не нужны — если есть с чем сравнить глобально, это для
    глобального поста про макродвижения, а тут достаточно круглое число за период».

    Поле вычеркнуто, а не переформулировано: оно добавляло ВТОРОЕ сравнение того же
    рода и тянуло пост в макро-разговор. Костяк — ГЛАВНОЕ_СРАВНЕНИЕ (цена ↔ позиция
    за одно названное окно), и его достаточно.
    """
    import inspect
    src = inspect.getsource(CA._position_phrases)
    assert "размер_позиции_против_пика" not in src
    assert "доля_чистой_позиции_в_ои" not in src
    assert not hasattr(CA, "_size_vs_peak")


def test_previous_rating_level_names_month_and_year_in_words():
    """Дату 30.12.2025 модель пересказала как «в конце декабря» — без года, и в
    сентябре 2026 это двусмысленно. Готовую фразу модель копирует, цифровую дату —
    пересказывает, поэтому месяц и год пишем словами."""
    import datetime as _d
    db = _rating_db([(_d.date(2025, 12, 30), _L_ACRA)])
    out = CA._rating_history(db, "АКРА ПОНИЗИЛО РЕЙТИНГ АФК СИСТЕМА ДО A+(RU)", "",
                             ["AFKS"], _d.date(2026, 9, 1))
    assert out["ПРЕЖНИЙ_УРОВЕНЬ"] == "до этого, в декабре 2025 года, у АКРА было AA-(RU)", \
        out["ПРЕЖНИЙ_УРОВЕНЬ"]


def test_context_blocks_ask_for_one_short_sentence():
    """Было «по одной фразе на компанию, обе в одном абзаце» — модель втиснула три
    факта в одно предложение со вставкой в скобках. Замер: канал 11 слов в
    предложении и скобки в 3% абзацев; тот черновик — 17 слов и скобки в трети."""
    import inspect
    for fn in (CA._related_context, CA._rating_history):
        src = inspect.getsource(fn)
        assert "ОДНИМ КОРОТКИМ ПРЕДЛОЖЕНИЕМ" in src, fn.__name__
        assert "скобк" in src, fn.__name__


def test_frame_tells_the_model_to_stay_silent_about_the_one_day_gap():
    """Модель вынесла оговорку рамки в пост дословно: «Разворот случился за день до
    новости — от шума такой срок почти не отличить». Вадим: «вот эта часть уже не
    нужна, это просто шум». Запрет, попавший в бриф, модель проговаривает вслух."""
    f = _frame(-1)
    assert f.startswith("СОВПАДЕНИЕ")
    assert "НЕ пиши в посте" in f and "промолчи" in f


def test_current_price_key_says_how_to_use_it():
    """«Акция сейчас стоит 7,30 рубля» отдельным предложением повисает в конце. В
    понравившемся варианте цена стояла в одной фразе с изменением."""
    import inspect
    src = inspect.getsource(CA._price_context)
    assert "цена_сейчас_только_вместе_с_изменением" in src
    assert '"цена_сейчас"' not in src


def test_links_block_defines_paragraph_order_and_disputed_handling():
    import inspect
    src = inspect.getsource(CA._related_context)
    assert "ПОРЯДОК" in src and "КТО КОМУ КЕМ ЯВЛЯЕТСЯ" in src
    assert "СПОРНО" in src


def test_no_frame_leaks_its_own_instruction_into_the_post():
    """Утечка указаний рамки в текст — повторяющийся класс ошибки, найденный дважды.

    Сначала СОВПАДЕНИЕ дало «от шума такой срок почти не отличить» (Вадим: «это
    просто шум»). Починил одну ветку — батч 01.09 показал ту же утечку в другой:
    черновик 1104 напечатал «Это отклик на новость, не опережение», то есть
    формулировку РЕАКЦИИ. Значит молчать должны ВСЕ ветки, а не та, где поймали.
    """
    for off in (-30, -5, -2, -1, 0, 1, 3, 10):
        f = _frame(off)
        assert ("не проговаривай" in f or "НЕ пиши в посте" in f
                or "промолчи" in f), f"утечка возможна при отрыве {off}: {f}"
