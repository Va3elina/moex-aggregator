"""Сборщик справок о СЧА (Funds/scha_collector.py): разбор дат, выбор справки за месяц, адаптер Атона.
Офлайн — сайты не трогаем."""
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from Funds import scha_collector as C  # noqa: E402


def test_date_formats():
    assert C.date_from_text("Справка о СЧА на 31.08.2026 и отчёт за август 2026") == ("2026-08", date(2026, 8, 31))
    # Герои 2024: дата в начале через пробелы
    assert C.date_from_text("28 12 2024 Справка о стоимости чистых активов Фонда") == ("2024-12", date(2024, 12, 28))
    assert C.date_from_text("Справка о СЧА за III квартал 2025") == ("2025-09", None)
    # числа через пробел не в начале строки — не дата (номер выпуска, суммы)
    assert C.date_from_text("Справка о СЧА 12 34 5678 без даты") == (None, None)


def _doc(period, day=None, corrected=False, url="u"):
    return C.Doc("X", period, day, url, "t", corrected)


def test_corrected_wins_over_original_and_later_date():
    docs = [_doc("2021-04", url="orig"), _doc("2021-04", corrected=True, url="fixed"), _doc("2021-04", url="orig2")]
    assert C.one_per_month(docs)["2021-04"].url == "fixed"
    docs = [_doc("2026-08", date(2026, 8, 28), url="a"), _doc("2026-08", date(2026, 8, 31), url="b")]
    assert C.one_per_month(docs)["2026-08"].url == "b"


def test_aton_next_data(monkeypatch):
    data = {"props": {"pageProps": {"data": [
        {"code": "otchet_amgb", "items": [
            {"name": "Справка о стоимости чистых активов на 31.08.2026 и Отчет о приросте за август 2026",
             "file": {"url": "/upload/iblock/79a/a.xlsx"}},
            {"name": "Справка о стоимости чистых активов на 28.12.2024", "file": {"url": "/upload/b.xlsx"}},
            {"name": "Правила расчёта СЧА", "file": {"url": "/upload/c.pdf"}},
        ]},
        {"code": "raskrytie_amgb", "items": [
            {"name": "Справка о стоимости чистых активов на 30.09.2026", "file": {"url": "/upload/kid.pdf"}}]},
    ]}}}
    html = '<html><script id="__NEXT_DATA__" type="application/json">' + json.dumps(data) + "</script></html>"
    monkeypatch.setattr(C, "http_get", lambda url, timeout=60: (html.encode(), {}))
    docs = C.list_aton("AMGB", "https://www.am-aton.ru/legaldocs/docpif/amgb")
    assert [(d.period, d.url) for d in docs] == [
        ("2026-08", "https://www.am-aton.ru/upload/iblock/79a/a.xlsx"),
        ("2024-12", "https://www.am-aton.ru/upload/b.xlsx"),
    ]


def test_vim_pagination_stops_when_page_repeats(monkeypatch):
    pages = {1: ["2026-09", "2026-08"], 2: ["2026-07"], 3: ["2026-09", "2026-08"]}  # за последней — снова первая
    calls = []

    def fake(ticker, url):
        n = int(url.split("PAGEN_1=")[1]) if "PAGEN_1=" in url else 1
        calls.append(n)
        return [_doc(p) for p in pages.get(n, pages[1])]
    monkeypatch.setattr(C, "_list_html_page", fake)
    monkeypatch.setattr(C, "PAUSE", 0)
    docs = C.list_html("OBLG", "https://www.wealthim.ru/x/reports/")
    assert sorted({d.period for d in docs}) == ["2026-07", "2026-08", "2026-09"] and calls == [1, 2, 3]
    # не ВИМ — одна страница
    calls.clear()
    C.list_html("TBRU", "https://t-capital-funds.ru/documents/mutual_funds/TBRU/")
    assert calls == [1]


def test_every_bond_fund_has_source():
    bonds = {"AMGB", "RU000A109SR1", "AKMB", "OPIF-33", "OPIF-9113", "OPIF-11259", "TBRU", "TOFZ", "OBLG",
             "WOFZ", "WFLO", "OPIF-54", "OPIF-9165", "SBGB", "SBLB", "SBRB", "SBFR", "SAFE", "OPIF-47",
             "OPIF-8119", "OPIF-4995"}
    assert bonds <= set(C.SOURCES)
    for tk, (kind, arg) in C.SOURCES.items():
        assert kind in C.ADAPTERS, tk
        if arg.startswith("http"):
            C._check_url(arg)
