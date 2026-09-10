"""Официальные события во втором мозге (10.09.2026).

Вадим: «раскрытия, объявления биржи, отчёты с цифрами должны попадать в мозг независимо
от того, кандидат это или нет». Раньше из 78 отчётных раскрытий за 4 дня в карту не
попал ни один узел, из 93 объявлений МосБиржи — три.
"""
import importlib.util
import inspect
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from api import brain_core as core  # noqa: E402
from signals import content_ai as CA  # noqa: E402


def _sync():
    spec = importlib.util.spec_from_file_location("brain_sync_events", ROOT / "Brain" / "brain_sync.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_new_sources_run_every_sync_and_have_trust_levels():
    s = _sync()
    main = inspect.getsource(s.main)
    for fn in ("раскрытия(conn", "отчёты(conn", "объявления_биржи(conn"):
        assert fn in main, fn
    assert s.УРОВНИ["раскрытие_о"] == ("B", "раскрытие_fm")
    assert s.УРОВНИ["объявление_о"] == ("A", "moex")
    assert s.УРОВНИ["отчёт_о"] == ("A", "документ")


def test_disclosures_go_in_regardless_of_candidates():
    """Никакого фильтра по candidate_id или категории — всё, что пришло."""
    src = inspect.getsource(_sync().раскрытия)
    assert "candidate_id" not in src and "category IN" not in src


def test_exchange_feed_skips_only_trading_noise_and_never_tags_moex_by_name():
    s = _sync()
    src = inspect.getsource(s.объявления_биржи)
    assert "company_id <> 'company:MOEX'" in src
    import re
    шум = re.compile(s._БИРЖА_ШУМ, re.I)
    assert шум.search("изменены значения нижней границы ценового коридора РЕПО")
    assert not шум.search("Московская биржа включила акции ДОМ.РФ в Индекс МосБиржи создания стоимости")


def test_name_check_is_shared_and_longest_name_wins():
    s = _sync()
    rx = s._проверка_имени("Газпром", "company:GAZP", None,
                           [("газпром", "company:GAZP"), ("газпром нефть", "company:SIBN")])
    assert "(?!\\s+(нефт))" in rx and rx.startswith("\\mГазпром")
    assert s._проверка_имени("Мосбиржа", "company:MOEX", "мосбирж", []) == "мосбирж"
    assert "_проверка_имени(pattern, company_id, verify, все_имена)" in inspect.getsource(s.новости_по_имени)


def test_writer_context_has_the_new_blocks():
    src = inspect.getsource(core.контекст)
    for k in ('"раскрытия"', '"объявления_биржи"', '"отчёты"'):
        assert k in src, k


def _ctx(**extra):
    пусто = {"элементы": [], "всего": 0}
    c = {"компания": {"заголовок": "Новатэк"}, "сектор": dict(пусто), "владельцы": dict(пусто),
         "владеет": dict(пусто), "фонды_держатели": dict(пусто), "индексы": dict(пусто),
         "новости": dict(пусто), "кандидаты": dict(пусто), "аномалии": dict(пусто),
         "вместе_в_новостях": dict(пусто)}
    c.update(extra)
    return {"NVTK": c}


def test_brief_shows_disclosures_exchange_news_and_report_figures():
    ctx = _ctx(
        раскрытия={"всего": 2, "элементы": [{"время": "2026-09-09T10:00", "заголовок": "Отчётность · Новатэк: МСФО за 6 мес."}]},
        объявления_биржи={"всего": 1, "элементы": [{"время": "2026-09-10T09:00", "заголовок": "Изменение базы расчёта индексов", "уровень": "A"}]},
        отчёты={"всего": 1, "элементы": [{"время": "2026-09-08T06:30", "заголовок": "Отчёт МСФО NVTK за 6 мес. 2026",
                                           "сводка": "Выручка 700 млрд руб (стр. 5)"}]},
    )
    блок = CA._brain_block(None, {}, ctx)["NVTK"]
    days = CA.BRAIN_CONTEXT_DAYS
    assert блок[f"раскрытия_компании_за_{days}_дней"].startswith("2 [B, FinanceMarker]")
    assert "[A]" in блок[f"объявления_биржи_за_{days}_дней"]
    assert "Выручка 700 млрд руб (стр. 5)" in блок["отчёты_с_цифрами"][0]


def test_old_context_without_new_blocks_still_renders():
    блок = CA._brain_block(None, {}, _ctx())["NVTK"]
    assert not any(k.startswith(("раскрытия_", "объявления_биржи_", "отчёты_")) for k in блок)
