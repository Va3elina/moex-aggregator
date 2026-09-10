"""История индексов и фондов во втором мозге (10.09.2026).

Вадим: «входы и выходы из индексов, покупки фондов — событиями в мозг; главное, чтобы
всё, что мы добавляем, было автоматизировано и отфильтровано». Мозг знал только
последний состав индекса и последний срез фонда — когда бумага вошла и когда фонд её
набрал, терялось.
"""
import importlib.util
import inspect
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from api import brain_core as core  # noqa: E402
from signals import content_ai as CA  # noqa: E402


def _sync():
    spec = importlib.util.spec_from_file_location("brain_sync_history", ROOT / "Brain" / "brain_sync.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_history_runs_every_sync_with_level_a():
    s = _sync()
    main = inspect.getsource(s.main)
    assert "события_индексов(conn" in main and "события_фондов(conn" in main
    assert s.УРОВНИ["событие_индекса"] == ("A", "moex")
    assert s.УРОВНИ["событие_фонда"] == ("A", "раскрытие_ук")


def test_fund_events_are_filtered():
    """Без фильтра «новых» и «закрытых» почти поровну — хвост топ-списков мигает."""
    s = _sync()
    assert s._ФОНД_СРЕЗ_МИН >= 5 and s._ФОНД_ВЕС_МИН >= 1.0 and s._ФОНД_ИЗМ >= 0.5
    assert s._ФОНД_ДНЕЙ <= 730
    sql = s._СОБЫТИЯ_ФОНДОВ
    assert "HAVING COUNT(*) >= CAST(:срез AS bigint)" in sql, "только полные срезы"
    assert "JOIN brain_ticker_map" in sql, "только наши компании"
    assert "COALESCE(c.weight, 0) >= CAST(:вес AS numeric)" in sql, "вход — только весомой бумаги"
    assert "COALESCE(p.weight, 0) >= CAST(:вес AS numeric)" in sql, "выход — только весомой бумаги"
    # pg8000 выводит тип параметра из соседа: без CAST 0,5 рядом с bigint падал на проде
    assert ":изм * p.positions" not in sql and "CAST(:изм AS numeric)" in sql


def test_fund_wording_says_snapshots_not_trades():
    src = inspect.getsource(_sync().события_фондов)
    assert "между срезами" in src
    # pg8000: одиночный % — плейсхолдер, а «%%» доезжал до текста как есть («доля 5.35%%»)
    assert "chr(37)" in src and "'%%'" not in src and "'%'" not in src


def test_fund_snapshots_are_compared_only_at_similar_size():
    """Срезы разной полноты — не сделки: сравниваем только похожие по размеру."""
    sql = _sync()._СОБЫТИЯ_ФОНДОВ
    assert "LEAST(n, prev_n) >= 0.7 * GREATEST(n, prev_n)" in sql


def test_snapshot_size_counts_only_matched_securities():
    """EQMX до 25.08 слал 47 строк без ISIN, с 26.08 — с ISIN: весь портфель
    (Лукойл 15,7 %) выходил «новыми позициями». Размер среза — по сопоставленным бумагам."""
    sql = _sync()._СОБЫТИЯ_ФОНДОВ
    полный = sql[sql.index("full_snap AS"):sql.index("s0 AS")]
    assert "FROM h" in полный and "fund_holdings_history" not in полный
    assert sql.index("h AS (SELECT") < sql.index("full_snap AS")


def test_fund_events_use_site_sources_only():
    """10.09: реконструкция cbonds лежала в те же даты рядом с документами УК, доли в срезе
    складывались до 160 % — треть событий была мнимой. Источник — тот же, что у /fund-trades."""
    s = _sync()
    ft = (ROOT / "api" / "routers" / "fund_trades.py").read_text(encoding="utf-8")
    строка = re.search(r"^MONTHLY_SOURCES = \((.*?)\)", ft, re.M).group(1)
    assert s._ФОНД_ИСТОЧНИКИ == re.findall(r'"([^"]+)"', строка)
    sql = s._СОБЫТИЯ_ФОНДОВ
    assert "source = ANY(:источники)" in sql and "src.source = h.source" in sql
    assert "DISTINCT ON (fund_id, snapshot_date)" in sql, "один источник на срез"
    assert "<= CAST(:сумма AS numeric)" in sql and 100 < s._ФОНД_СУММА_МАКС <= 130


def test_fund_rebuild_starts_clean():
    """Смена правил отбора: мнимые события старых правил не должны пережить пересборку."""
    s = _sync()
    src = inspect.getsource(s.события_фондов)
    assert "_водяной(conn, _ФОНД_ВЕРСИЯ)" in src and "_отметить(conn, _ФОНД_ВЕРСИЯ" in src
    assert s._ФОНД_ВЕРСИЯ != "fund_events", "новая версия ключа — один полный прогон"
    чистка = src[src.index("if вод is None"):src.index("п = {")]
    assert "kind = 'событие_фонда'" in чистка and "kind = 'fund_event'" in чистка


def test_index_events_compare_neighbouring_dates():
    sql = _sync()._СОБЫТИЯ_ИНДЕКСОВ
    assert "lag(trade_date)" in sql and "'вход'" in sql and "'выход'" in sql


def test_writer_context_and_brief_show_history():
    src = inspect.getsource(core.контекст)
    assert '"индексы_события"' in src and '"фонды_события"' in src
    пусто = {"элементы": [], "всего": 0}
    c = {k: dict(пусто) for k in ("сектор", "владельцы", "владеет", "фонды_держатели", "индексы",
                                  "новости", "кандидаты", "аномалии", "вместе_в_новостях")}
    c["компания"] = {"заголовок": "ДОМ.РФ"}
    c["индексы_события"] = {"всего": 1, "элементы": [
        {"время": "2026-09-14T00:00", "заголовок": "ДОМ.РФ вошла в индекс MVBI"}]}
    c["фонды_события"] = {"всего": 2, "элементы": [
        {"время": "2026-08-31T00:00", "заголовок": "БПИФ Индекс МосБиржи — новая позиция: ДОМ.РФ"}]}
    блок = CA._brain_block(None, {}, {"DOMRF": c})["DOMRF"]
    assert блок["входы_и_выходы_из_индексов"] == ["2026-09-14 ДОМ.РФ вошла в индекс MVBI [A]"]
    фонды = блок[f"фонды_меняли_позицию_за_{CA.BRAIN_CONTEXT_DAYS}_дней"]
    assert "месячные срезы, не сделки" in фонды and "новая позиция: ДОМ.РФ" in фонды
