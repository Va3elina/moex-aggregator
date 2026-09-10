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


class _НетСледа:
    """След в тестах брифа не проверяем — но и ронять сборку он не должен."""

    def record(self, *a, **k):
        pass


_НЕТ_СЛЕДА = _НетСледа()


def _stub_brief_sources(monkeypatch):
    monkeypatch.setattr(CA, "_story_frame", lambda *a: "РЕАКЦИЯ")
    monkeypatch.setattr(CA, "_position_phrases", lambda *a, **k: {"ГЛАВНОЕ_ЧИСЛО": "в 3 раза"})
    monkeypatch.setattr(CA, "_price_context", lambda *a: {"цена_сейчас": "около 92"})
    monkeypatch.setattr(CA, "_prior_post_line", lambda *a: "(нет)")
    # ⚠️ Каждый НОВЫЙ источник данных брифа обязан попасть в эту заглушку. Именно
    # так тест паритета и поймал добавление _related_context: без подмены он полез
    # в db=None. Это и есть польза от проверки инварианта, а не набора полей.
    #
    # ⚠️ И заглушка обязана брать **kwargs: сборщик зовёт источники по именам
    # (trace=, мозг=). Заглушка на одних *a молча падает TypeError и роняет тест
    # паритета там, где брифы на самом деле совпадают.
    monkeypatch.setattr(CA, "_related_context", lambda *a, **k: ({}, {}))
    monkeypatch.setattr(CA, "_rating_history", lambda *a, **k: {})
    monkeypatch.setattr(CA, "_brain_contexts", lambda *a, **k: {})
    monkeypatch.setattr(CA, "_brain_block", lambda *a, **k: {})
    monkeypatch.setattr(CA, "_company_fundamentals", lambda *a, **k: {})
    monkeypatch.setattr(CA, "трассировать", lambda *a, **k: _НЕТ_СЛЕДА)


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
    _stub_brief_sources(monkeypatch)
    monkeypatch.setattr(CA, "_position_phrases", lambda *a, **k: {
        "_код_период_главного_числа": "за_год", "_код_фраза_главного_числа": "лонг вырос в 3 раза",
        "ГЛАВНОЕ_ЧИСЛО": "за_год: лонг вырос в 3 раза"})
    monkeypatch.setattr(CA, "_price_context", lambda *a: dict(_PRICE))
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
    _stub_brief_sources(monkeypatch)
    for payload in (CA._step_c_payload(None, _ROW, "t"), CA._step_g_payload(None, _ROW, "t")):
        assert "связанные_компании" not in payload


def test_related_block_carries_context_warning(monkeypatch):
    """Если связь есть — рядом обязана быть подпись «контекст, не причина»."""
    _stub_brief_sources(monkeypatch)
    monkeypatch.setattr(CA, "_related_context", lambda *a, **k: (
        {"OZON": {"связь": "крупный акционер"},
         "ПОЯСНЕНИЕ": "КОНТЕКСТ, НЕ ПРИЧИНА. …"}, {}))
    payload = CA._step_c_payload(None, _ROW, "t")
    assert "связанные_компании" in payload and "НЕ ПРИЧИНА" in payload


_LINK_ROW = {"id": 1638, "tickers": ["AFKS"],
             "headline": "АКРА понизило рейтинг АФК «Система»",
             "raw_text": "Среди активов холдинга — Озон"}


class _LinksDB:
    """Фейк ровно под три запроса блока связей.

    ⚠️ Колонок у _SELECT_ENTITY_LINKS ЧЕТЫРЕ (entities, statement, лет,
    confidence). Фейк на двух колонках однажды уже отгнил молча: тест падал
    ValueError и выглядел «сломанным тестом», хотя описывал живое правило.
    """

    def __init__(self, pair_date=None, name="Озон", лет=1, conf=0.9,
                 entities=("AFKS", "OZON"), statement="Система — крупный акционер Озона"):
        self.pair_date, self.name, self.лет, self.conf = pair_date, name, лет, conf
        self.entities, self.statement = list(entities), statement

    def execute(self, q, params=None):
        sql = str(q)
        внешний = self

        class _R:
            @staticmethod
            def fetchall():
                if "world_facts" in sql:
                    return [(внешний.entities, внешний.statement,
                             внешний.лет, внешний.conf)]
                return []

            @staticmethod
            def first():
                if "news_archive" in sql and внешний.pair_date:
                    return (внешний.pair_date, 2)
                return None

            @staticmethod
            def scalar():
                return внешний.name if "instruments" in sql else None
        return _R()


def test_links_block_states_where_it_goes(monkeypatch):
    """Место блока в тексте — часть смысла, и указание живёт рядом с данными.

    Судья по черновику 1638: «соседство с абзацем о позициях толпы может подтолкнуть
    читателя додумать связь, которая прямо не утверждается». Причинность возникала
    не из слов, а из порядка абзацев — значит и лечить это надо там, где данные, а
    не очередным правилом в промпте.
    """
    blk, _ = CA._related_context(_LinksDB(), _LINK_ROW, _dt.date(2026, 9, 1))
    assert "МЕСТО В ТЕКСТЕ" in blk["ПОЯСНЕНИЕ"]
    assert "РАНЬШЕ" in blk["ПОЯСНЕНИЕ"]
    assert "НЕ ПРИЧИНА" in blk["ПОЯСНЕНИЕ"], "старое предупреждение не должно пропасть"


# ─────────────────────────────────────────────────────────────────────────────
# Решение «упоминать ли связь» принимает агент, а не наличие ребра в графе
# ─────────────────────────────────────────────────────────────────────────────

def test_link_only_in_the_graph_never_reaches_the_writer():
    """Кандидат 1933: НОВАТЭК ведёт переговоры с Petrovietnam — а в черновике
    абзац про долю Газпрома по снимку 25.03.2021.

    Модель его не выдумала: ребро владения есть, значит поле в брифе есть, значит
    поле надо израсходовать. Лечится не запретом в промпте, а тем, что связь без
    признаков присутствия в сюжете до писателя не доезжает ВОВСЕ.
    """
    db = _LinksDB(pair_date=None, name="Газпром", entities=("NVTK", "GAZP"),
                  statement="Газпром владеет косвенно долей 10% в НОВАТЭК")
    row = {"id": 1933, "tickers": ["NVTK"],
           "headline": "НОВАТЭК рассматривает с Petrovietnam проект во Вьетнаме",
           "raw_text": "Переговоры вышли на продвинутую стадию, поставки СПГ с 2027 года"}
    assert CA._related_context(db, row, _dt.date(2026, 9, 9)) == ({}, {})


def test_link_named_in_the_news_itself_reaches_the_writer():
    """Обратная сторона: если связанная компания названа в самой новости, связь в
    сюжете есть — и писатель обязан увидеть, ПОЧЕМУ она здесь."""
    db = _LinksDB(pair_date=None, name="Озон")
    row = {"id": 1638, "tickers": ["AFKS"],
           "headline": "АКРА понизило рейтинг АФК «Система»",
           "raw_text": "Среди активов холдинга — Озон и Сегежа"}
    blk, спорные = CA._related_context(db, row, _dt.date(2026, 9, 1))
    assert "OZON" in blk and not спорные
    признаки = blk["OZON"]["почему_она_здесь"]
    assert any("названа в самой новости" in p for p in признаки), признаки


def test_recent_shared_story_also_counts_as_a_live_link():
    """Второй признак — общий сюжет: обе компании были в одной новости недавно."""
    db = _LinksDB(pair_date=_dt.date(2026, 8, 20), name="Озон")
    row = {"id": 1, "tickers": ["AFKS"], "headline": "Рейтинг", "raw_text": "Без имён"}
    blk, спорные = CA._related_context(db, row, _dt.date(2026, 9, 1))
    assert any("в одной новости" in p for p in blk["OZON"]["почему_она_здесь"])
    assert not спорные, "компании из разных секторов — общий сюжет, а не отраслевой фон"


def test_shared_story_inside_one_sector_is_only_a_question(monkeypatch):
    """Кандидат 1933 во второй раз: NVTK и GAZP попадали в одну новость десять раз
    за месяц — «экспорт СПГ вырос», «Новак о поставках в Китай». Обе компании там
    подлежащие, но к переговорам во Вьетнаме это отношения не имеет.

    Значит совместное упоминание однокашников по сектору — не фактура, а вопрос: без
    цены, без архива и БЕЗ указаний, как писать абзац."""
    monkeypatch.setattr(CA, "_sector_of", lambda *a, **k: "Нефть и газ")
    db = _LinksDB(pair_date=_dt.date(2026, 9, 4), name="Газпром",
                  entities=("NVTK", "GAZP"),
                  statement="Газпром владеет косвенно долей 10% в НОВАТЭК")
    row = {"id": 1933, "tickers": ["NVTK"],
           "headline": "НОВАТЭК рассматривает с Petrovietnam проект во Вьетнаме",
           "raw_text": "Переговоры вышли на продвинутую стадию"}
    blk, спорные = CA._related_context(db, row, _dt.date(2026, 9, 9))
    assert blk == {}, "слабая связь не должна ехать фактурой"
    assert "ОТВЕТ ПО УМОЛЧАНИЮ — НЕТ" in спорные["ВОПРОС"]
    assert "отраслевой обзор" in " ".join(спорные["GAZP"]["почему_она_здесь"])
    assert "цена_за_месяц" not in спорные["GAZP"]
    assert "МЕСТО В ТЕКСТЕ" not in _json.dumps(спорные, ensure_ascii=False), (
        "указания «как писать абзац» рядом со спорной связью — то, от чего "
        "писателю нечем отказаться")


def test_writer_is_told_the_decision_is_his():
    """Карта отсеивает мёртвые связи; уместность живой — вопрос смысла, и решает
    его писатель. Если этой строки нет, «связь в брифе» снова читается как
    «связь в посте»."""
    blk, _ = CA._related_context(_LinksDB(), _LINK_ROW, _dt.date(2026, 9, 1))
    п = blk["ПОЯСНЕНИЕ"]
    assert "РЕШАЕШЬ ТЫ" in п and "почему_она_здесь" in п
    assert "не упоминай её вовсе" in п


def test_neighbourhood_in_news_is_a_hint_not_a_link():
    """«Рядом в новостях» — уровень D, соседство, а не отношение: как признак
    показываем, но живой связь одно только соседство не делает."""
    мозг = {"AFKS": {"вместе_в_новостях": {"элементы": [{"заголовок": "Озон"}]}}}
    row = {"id": 1, "tickers": ["AFKS"], "headline": "Рейтинг", "raw_text": "Без имён"}
    db = _LinksDB(pair_date=None, name="Озон")
    blk, спорные = CA._related_context(db, row, _dt.date(2026, 9, 1), мозг=мозг)
    assert blk == {}, "соседство в новостях — не фактура"
    признаки, сила = CA._link_evidence(db, row, ["AFKS"], "OZON", "Озон",
                                       _dt.date(2026, 9, 1), мозг)
    assert сила == "слабая" and any("[D" in p for p in признаки)


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
    src = inspect.getsource(CA._related_context)
    assert "ОБЪЁМ" in src
    assert "ФОН" in src
    # ⚠️ История рейтинга — тоже фон, но БЕЗ указания, как её верстать (06.09,
    # кандидат 1638): «одним предложением в том же абзаце» модель прочла как
    # «вставить обязательно», и в пост уехали коды шкалы A+(RU)/AA-(RU).
    rat = inspect.getsource(CA._rating_history)
    assert "ФОН" in rat
    assert "ОДНИМ КОРОТКИМ ПРЕДЛОЖЕНИЕМ" not in rat


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


def test_context_blocks_ask_for_short_sentences_not_crammed_phrases():
    """Граница объёма измеряется ПРЕДЛОЖЕНИЯМИ, и её пришлось калибровать дважды.

    Сначала «по одной ФРАЗЕ на компанию, обе в одном абзаце» — модель втиснула три
    факта в одно предложение со вставкой в скобках (17 слов против 11 у канала,
    скобки в трети абзацев против 3%). Ужал до одного предложения — оказалось слишком
    туго: в абзаце, который Вадим назвал лучшим, про Озон было ДВА факта (сорванная
    сделка со Сбером и залог пакета). Итог — одно-два коротких предложения.
    """
    import inspect
    src = inspect.getsource(CA._related_context)
    assert "ОДНИМ-ДВУМЯ КОРОТКИМИ ПРЕДЛОЖЕНИЯМИ" in src
    assert "скобк" in src
    # История рейтинга границы по предложениям больше не задаёт — см.
    # test_context_blocks_declare_volume_limit.


def test_fundamentals_boundary_has_no_example_sentence():
    """Вадим 06.09 о черновике 1638: «долговая нагрузка ни к селу ни к городу».
    Пример в границе («рейтинг понизили, а долговая нагрузка снизилась») писатель
    воспроизвёл дословно — пример для модели равен инструкции."""
    import inspect
    src = inspect.getsource(CA._company_fundamentals)
    assert "например" not in src.split('out["ГРАНИЦА"]')[-1]
    assert "долговая нагрузка" not in src.split('out["ГРАНИЦА"]')[-1]


def test_fundamentals_reach_the_writer_only_for_numeric_events():
    """«Если говорим про цифры, то про цифры» (Вадим 06.09): блок фундамента в брифе
    только под отчёт/дивиденды/корпдействие. Под рейтинг или санкции числа уходят
    лишь подписью под постом."""
    assert {"earnings", "dividend", "register_closing", "corporate_action"} <= CA._FUND_NUMERIC_EVENTS
    assert "regulatory" not in CA._FUND_NUMERIC_EVENTS
    assert "sanctions" not in CA._FUND_NUMERIC_EVENTS
    import inspect
    src = inspect.getsource(CA._build_brief)
    assert "_FUND_NUMERIC_EVENTS" in src
    # ⚠️ Проверено на проде после v19: под «regulatory» блок доезжал как {} — пустое
    # поле модель считает обязанной заполнить, поэтому его надо убирать целиком.
    assert '"фундамент_компании")' in src.replace("\n", "").replace(" ", "") or \
        '"фундамент_компании"' in src.split("for empty in")[1].split(")")[0]


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
    assert "ПОРЯДОК АБЗАЦА" in src
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


# ─────────────────────────────────────────────────────────────────────────────
# Профиль стиля (идея Вадима: «размытое ощущение» вместо жёстких правил)
# ─────────────────────────────────────────────────────────────────────────────

from api.services import style_profile as SP  # noqa: E402


def test_signature_is_stripped_before_measuring():
    """⚠️ Наступал на это 01.09: без снятия подписи @FrameTool «последним абзацем» у
    опубликованных постов оказывается она, и весь замер даёт нули."""
    t = "Заголовок\n\n◽️Акции за год упали вдвое.\n\n😀😀😀/@FrameTool\n\n#открытыйинтерес"
    paras = SP._paras(t)
    assert all("@FrameTool" not in p for p in paras), paras
    assert any("упали вдвое" in p for p in paras)


def test_profile_measures_number_density_not_just_count():
    """Плотность, а не количество: короткий пост с теми же числами плотнее. Именно так
    сегодняшние черновики стали ХУЖЕ канала (1,35 против 0,40), хотя каждый отдельный
    запрет был верным — вырезалась проза, а числа оставались."""
    dense = "З\n\n◽️Рост 8%, до 121,28. День +6%. Лонг вырос в 3 раза.\n\n#открытыйинтерес"
    airy = ("З\n\n◽️Акции заметно выросли за считанные минуты. Толпа отреагировала "
            "почти сразу, но осторожно.\n\n#открытыйинтерес")
    assert SP.profile(dense)["чисел_на_100зн"] > SP.profile(airy)["чисел_на_100зн"]


def test_score_is_deterministic():
    """Главное преимущество перед судьёй: один и тот же текст всегда даёт один ответ.
    Судья 01.09 на неизменном тексте выдал сначала «брак», потом «годится»."""
    t = "Заголовок\n\n◽️Акции за год упали вдвое. Толпа перешла в лонг.\n\n#открытыйинтерес"
    a, b = SP.score(t), SP.score(t)
    assert a["среднее_отклонение"] == b["среднее_отклонение"]


def test_rejected_features_are_not_in_the_score():
    """Температура и эмодзи померены и ОТБРОШЕНЫ: у канала 1,00, у черновиков 0,80 —
    не разделяют. Гипотеза Вадима про температуру проверена и не подтвердилась."""
    assert "температура" not in SP.KEYS
    assert "эмодзи" not in SP.KEYS
    assert "температура" in SP.profile("З\n\n◽️Текст подлиннее для порога.\n\n#х")


def test_guidance_talks_about_text_not_about_the_score():
    """⚠️ «Плотность +1,4σ» модель исправила бы механически — выкинула бы число и
    сломала мысль. Совет обязан говорить, ЧТО делать: добавить связок, а не срезать
    факты. Иначе самопроверка станет причиной новых дефектов."""
    dense = ("Заголовок\n\n◽️Рост 8%, до 121,28 рубля. День +6%. Лонг вырос в 3 раза "
             "за месяц, за сутки ещё на 35%.\n\n#открытыйинтерес")
    res = SP.score(dense)
    assert res and res["что_поправить"], res
    g = " ".join(res["что_поправить"])
    assert "σ" not in g, "совет не должен оперировать сигмами"
    assert "добавь связок" in g
    assert "Не срезай факты" in g


def test_short_post_is_not_told_to_pad_with_numbers():
    """Совет про короткий пост обязан прямо запрещать дотягивание: именно требование
    «дотяни до 550» заставило модель добирать фоном и врать (черновик 1185)."""
    short = "Заголовок\n\n◽️Коротко и по делу, без цифр совсем.\n\n#открытыйинтерес"
    res = SP.score(short)
    g = " ".join(res["что_поправить"])
    if "КОРОТКИЙ" in g:
        assert "НЕ повод дописать" in g


def test_snapshot_is_compact_and_has_no_advice():
    """В базу идут только признаки и отклонения: советы — для писателя в моменте, а
    не исторические данные."""
    import json
    from api.routers.content_news import _style_snapshot
    snap = json.loads(_style_snapshot(
        "Заголовок\n\n◽️Акции за год упали вдвое. Толпа перешла в лонг.\n\n#х"))
    assert set(snap) == {"профиль", "отклонение_сигм", "среднее_отклонение"}
    assert "что_поправить" not in snap


def test_prose_share_separates_where_number_share_does_not():
    """Пятый признак объяснил остаток разрыва (01.09).

    Доля предложений С ЧИСЛАМИ у наших черновиков уже совпадала с каналом (0,30
    против 0,29) — то есть по числам мы попали. А доля ПРОЗЫ (без чисел И без
    тикеров) была 0,38 против 0,65: вместо слов у нас стоят обозначения вроде GAZPF
    и A+(RU). Без этого признака перекос не виден вообще.
    """
    tickers = ("З\n\n◽️Позиции физлиц во фьючерсе GAZPF выросли. Рейтинг AFKS "
               "понижен до уровня ruB.\n\n#х")
    prose = ("З\n\n◽️Толпа продолжает набирать длинную позицию. Рынок пока не верит "
             "в разворот.\n\n#х")
    assert SP.profile(tickers)["доля_прозы"] < SP.profile(prose)["доля_прозы"]
    # По доле предложений с числом эти два текста НЕ различаются — в обоих чисел нет.
    assert (SP.profile(tickers)["доля_предл_с_числом"]
            == SP.profile(prose)["доля_предл_с_числом"] == 0.0)


def test_guidance_is_symmetric_on_every_feature():
    """⚠️ Первая версия ругалась только на ДЛИННЫЕ предложения и КОРОТКИЙ пост. Итог:
    батч дал 7 слов в предложении против 11 у канала, и писатель этого не видел.
    Односторонняя метрика толкает в свою сторону до упора.
    """
    chopped = "З\n\n◽️Акция упала. Толпа в лонге. Рейтинг снижен. Прогноз плохой.\n\n#х"
    g = " ".join(SP.score(chopped)["что_поправить"])
    assert "РУБЛЕНЫЕ" in g, g
    assert "Соедини пары коротких" in g
    long_s = ("З\n\n◽️" + ("очень длинное предложение из множества слов подряд " * 4)
              + "и ещё немного.\n\n#х")
    assert "ДЛИННЫЕ" in " ".join(SP.score(long_s)["что_поправить"])


def test_no_machine_precision_in_any_guidance_branch():
    """Эталон в подсказке не должен выглядеть машинным: «661.0», «11.0», «0.645» —
    ровно та болезнь, которую мы вычищали из самих постов. Проверяем ВСЕ ветки, а не
    те, что случайно сработали: одну ветку я уже пропустил при первой правке.
    """
    import re as _re
    cases = ("З\n\n◽️Акция упала. Толпа в лонге. Рейтинг снижен.\n\n#х",
             "З\n\n◽️Рост 8%, до 121,28. День +6%. Лонг втрое, за сутки ещё на 35%.\n\n#х",
             "З\n\n◽️" + ("очень длинное предложение из множества слов подряд " * 4) + "конец.\n\n#х",
             "З\n\n◽️Рейтинг понизили, прогноз негативный. Толпа развернулась в лонг.\n\n#х")
    seen = 0
    for t in cases:
        res = SP.score(t)
        for g in res["что_поправить"]:
            seen += 1
            assert not _re.search(r"\d+\.\d{3,}", g), g
            assert not _re.search(r"\b\d+\.0\b", g), f"машинный ноль: {g}"
    assert seen >= 5, f"ветки не покрыты, сработало только {seen}"


def test_reference_values_in_guidance_are_rounded():
    """«против 0.645 у канала» — машинный след; в подсказке эталон должен быть
    округлён так же, как остальные числа брифа."""
    import re as _re
    for txt in ("З\n\n◽️Акция упала. Толпа в лонге. Рейтинг снижен.\n\n#х",
                "З\n\n◽️Рост 8%, до 121,28. День +6%. Лонг втрое.\n\n#х"):
        for g in SP.score(txt)["что_поправить"]:
            assert not _re.search(r"\d\.\d{3,}", g), g


def test_zero_numbers_always_warns_even_when_sigma_says_ok():
    """⚠️ Живой случай 01.09. Писатель выпустил черновик БЕЗ ЕДИНОЙ цифры: пропали
    «₽320 млрд» и «28 августа», вместо них «привлекла деньги» и «в конце августа».

    Сигма это не поймала: у канала медиана 0,40 при разбросе 0,46, поэтому ноль чисел
    даёт всего −0,87σ — выше порога −1,0. Широкий разброс эталона делает сигму слепой
    у нуля, там нужен абсолютный пол по существу.
    """
    no_nums = ("Заголовок 📉\n\n◽️Рейтинг компании понизили, прогноз негативный. "
               "Раньше агентство держало оценку выше.\n\n◽️За год акции упали больше "
               "чем вдвое. Толпа развернулась в длинную позицию.\n\n#открытыепозиции")
    res = SP.score(no_nums)
    assert res["профиль"]["чисел_на_100зн"] == 0.0
    assert res["отклонение_сигм"]["чисел_на_100зн"] > -1.0, "сигма тут не срабатывает"
    g = " ".join(res["что_поправить"])
    assert "ЦИФР СЛИШКОМ МАЛО" in g, g
    assert "потеря фактуры" in g


def test_normal_number_level_does_not_trigger_the_floor():
    ok = ("Заголовок 📉\n\n◽️Рейтинг понизили до A+(RU), раньше было AA-(RU). "
          "Прогноз остался негативным.\n\n◽️За год акции упали вдвое, до 7,30 рубля. "
          "Толпа за это же время перешла в лонг.\n\n#открытыепозиции")
    assert "ЦИФР СЛИШКОМ МАЛО" not in " ".join(SP.score(ok)["что_поправить"])


def test_links_paragraph_asks_for_chronology_and_one_company_per_sentence():
    """Правило порядка переписано второй раз, и первая версия была слишком буквальной.

    Она говорила «сначала кто кому кем является, потом что у кого произошло» — и
    получилось: «Сегежа входит в группу Системы, а сама Система — крупный акционер
    Озона. 28 августа… В июне…». Два родства в одном предложении и обратная
    хронология. Вадим: «как-то не складно, будто просто факты накидал, но нет начала
    связи».
    """
    import inspect
    src = inspect.getsource(CA._related_context)
    assert "ПОРЯДОК АБЗАЦА" in src
    assert "зачем эти компании вообще в посте" in src
    assert "порядке ВРЕМЕНИ" in src
    assert "по ОДНОЙ компании на предложение" in src
    # Прежняя формулировка не должна вернуться
    assert "КТО КОМУ КЕМ ЯВЛЯЕТСЯ" not in src


def test_homoglyph_in_rating_level_is_caught_by_code():
    """⚠️ Тихая ошибка, которую не увидит ни человек, ни модель-судья.

    Живой случай 01.09: черновик написал уровень рейтинга «ruВ» с КИРИЛЛИЧЕСКОЙ «В»
    вместо латинской B. Визуально неотличимо, значение неверное — и судья подтвердил
    numbers_traceable, потому что глазами уровень совпадал с брифом. Ловится только
    кодом, поэтому проверка живёт в сервисе, а не в рубрике.
    """
    assert SP.technical_warnings("рейтинг срезали до ruВ")  # кириллическая В
    assert not SP.technical_warnings("рейтинг срезали до ruB")  # латинская B
    w = SP.technical_warnings("уровень AА-(RU)")  # кириллическая А во второй позиции
    assert w and "СМЕШАННЫЙ АЛФАВИТ" in w[0]


def test_homoglyph_check_does_not_cry_wolf():
    """Ложные срабатывания сделали бы проверку бесполезной: обычный русский текст,
    чистые тикеры и хэштеги алфавит не смешивают."""
    for ok in ("Акции Системы упали больше чем вдвое за год. Толпа в лонге.",
               "#открытыепозиции", "фьючерс AFKS вырос, рейтинг A+(RU) подтверждён",
               "Сегеже срезали рейтинг до ruB 28 августа"):
        assert SP.technical_warnings(ok) == [], ok


def test_warnings_are_separate_from_style_advice():
    """Смешанный алфавит — не «непохоже на канал», а прямая ошибка. Поэтому отдельное
    поле: стилевой совет можно проигнорировать, эту — нет."""
    res = SP.score("Заголовок 📉\n\n◽️Рейтинг срезали до ruВ за год вдвое.\n\n#х")
    assert res["предупреждения"], res
    assert "СМЕШАННЫЙ" not in " ".join(res["что_поправить"])


def test_reference_is_genre_aware():
    """⚠️ Выяснилось на живом посте. Разбор мандата по фондам получил замечание «пост
    длинный: 1239 знаков против 661» — а 1239 это РОВНО медиана жанра «фонды».

    Замер по срезам: длина 679 (ОИ) против 1239 (фонды) при почти одинаковой плотности
    чисел (0,38 / 0,43) и прозе (0,67 / 0,73). Отличается ВДВОЕ именно длина, поэтому
    сравнивать пост про фонды с эталоном «ОИ» значит требовать сократить его вдвое без
    причины.
    """
    assert SP.detect_genre("текст\n\n#открытыйинтерес") == "ои"
    assert SP.detect_genre("текст\n\n#cделкифондов") == "фонды"
    assert SP.detect_genre("текст\n\n#сезонность") is None
    oi = SP.channel_reference("ои")
    fund = SP.channel_reference("фонды")
    assert fund["медиана"]["знаков"] > 1.5 * oi["медиана"]["знаков"], (
        oi["медиана"]["знаков"], fund["медиана"]["знаков"])


def test_thin_genre_falls_back_to_whole_channel():
    """Порог 15 постов: у «#сезонность» их 8, и эталон по ним увёл бы сильнее, чем
    общий по каналу. Незнакомая рубрика тоже уходит на общий срез."""
    ref = SP.channel_reference("сезонность")
    assert ref["эталон"] == "весь канал"
    assert SP.channel_reference(None)["эталон"] == "весь канал"


def test_score_picks_genre_from_the_text_itself():
    """Рубрика стоит последней строкой поста — отдельный параметр не нужен."""
    fund_post = ("Заголовок ⚠️\n\n◽️" + ("обычный текст про фонды и потоки денег " * 12)
                 + "\n\n#cделкифондов")
    assert SP.score(fund_post)["срез_эталона"] == "жанр «фонды»"
