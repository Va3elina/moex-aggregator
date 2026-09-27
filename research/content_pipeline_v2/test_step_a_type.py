"""Тип события кандидата — из единого словаря мозга, а не своё мнение Шага А (Вадим 27.09: «закрыть тему второго мозга»).

Код завода (event_type) — производный: по нему выбираются цифры фундамента и строится ключ треда, поэтому таблица
перехода обязана покрывать все типы словаря.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "Brain"))

import vocab  # noqa: E402

КОДЫ_ЗАВОДА = {"earnings", "dividend", "corporate_action", "sanctions", "regulatory", "macro", "other"}


def test_every_vocabulary_type_maps_to_a_factory_code():
    типы = {t for t, _, _ in vocab.ТИПЫ}
    assert типы <= set(vocab.ТИП_В_КОД_ЗАВОДА), типы - set(vocab.ТИП_В_КОД_ЗАВОДА)
    assert set(vocab.ТИП_В_КОД_ЗАВОДА.values()) <= КОДЫ_ЗАВОДА


def test_step_a_route_is_still_on_the_handler():
    """Помощник, вставленный между декоратором и функцией, забрал бы маршрут Шага А себе (поймано 27.09 до коммита)."""
    from api.routers import content_news as cn
    ручки = {r.path: r.endpoint.__name__ for r in cn.internal_router.routes if r.path.endswith("/{candidate_id}/step-a")}
    assert list(ручки.values()) == ["apply_step_a"], ручки


def test_vocabulary_type_overrides_step_a_code(monkeypatch):
    from api import brain_core
    from api.routers import content_news as cn
    monkeypatch.setattr(brain_core, "тип_текста", lambda *a, **k: "дивиденды")
    body = cn.StepAResult(relevant=True, tickers=["SBER"], event_type="other", importance_1_5=4, reasoning="Сбер")
    cn._единый_тип(None, {"headline": "Сбер: дивиденды", "raw_text": ""}, body)
    assert body.event_type == "dividend" and "тип по словарю: дивиденды" in body.reasoning


def test_step_a_code_stays_when_vocabulary_is_silent(monkeypatch):
    from api import brain_core
    from api.routers import content_news as cn
    monkeypatch.setattr(brain_core, "тип_текста", lambda *a, **k: None)
    body = cn.StepAResult(relevant=True, tickers=[], event_type="macro", importance_1_5=3, reasoning="x")
    cn._единый_тип(None, {"headline": "что-то", "raw_text": ""}, body)
    assert body.event_type == "macro" and body.reasoning == "x"
