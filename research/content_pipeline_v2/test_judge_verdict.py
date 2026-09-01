"""Вердикт судьи: поабзацный разбор и два новых пункта ворот B.

Кандидат 1638 прошёл ВСЕ ворота — вердикт «годится», ноль провалов, ноль дефектов —
и был отклонён человеком с тремя замечаниями. Каждое относилось к своему абзацу:
абзац 2 держал утверждение о предвидении на отрыве в один день, абзац 3 повторял
абзац 2 теми же словами, абзац 4 подавал догадку о мотиве толпы как факт.

Вывод не «рубрика слишком мягкая», а «рубрика плоская»: пост целиком модель
просматривает, абзац за абзацем — разбирает. Поэтому тесты здесь проверяют не
пороги, а то, что поабзацный сигнал ДОХОДИТ до вердикта и считается поштучно.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from api.routers.content_news import (  # noqa: E402
    _JUDGE_GATES_A, _JUDGE_GATES_B, _JUDGE_CHECKLIST_C,
    JudgeParagraph, _derive_judge_verdict,
)

_ALL_OK = {k: True for k in (*_JUDGE_GATES_A, *_JUDGE_GATES_B, *_JUDGE_CHECKLIST_C)}


def _p(n, supported=True):
    return JudgeParagraph(n=n, claim="c", supported_by="s", supported=supported,
                          doubt="d")


def test_clean_draft_passes():
    v, failed, defects = _derive_judge_verdict(dict(_ALL_OK), [_p(1), _p(2), _p(3)])
    assert (v, failed, defects) == ("годится", [], [])


def test_one_unsupported_paragraph_makes_it_disputable():
    v, failed, _ = _derive_judge_verdict(dict(_ALL_OK), [_p(1), _p(2, False), _p(3)])
    assert v == "спорно"
    assert failed == ["абзац_2_без_опоры"], failed


def test_two_unsupported_paragraphs_make_it_reject():
    v, failed, _ = _derive_judge_verdict(
        dict(_ALL_OK), [_p(1), _p(2, False), _p(3), _p(4, False)])
    assert v == "брак"
    assert failed == ["абзац_2_без_опоры", "абзац_4_без_опоры"], failed


def test_paragraphs_are_counted_individually_not_collapsed():
    """Три абзаца без опоры хуже одного — вердикт обязан различать.

    Соблазн был свернуть их в один синтетический провал «есть абзацы без опоры»:
    тогда пост, где не держится вообще ничего, получил бы «спорно».
    """
    _, failed, _ = _derive_judge_verdict(
        dict(_ALL_OK), [_p(n, False) for n in (1, 2, 3)])
    assert len(failed) == 3, failed


def test_paragraph_failure_combines_with_item_failure():
    """Один провал смысла + один абзац без опоры = два провала ворот B = брак."""
    items = dict(_ALL_OK, no_redundancy=False)
    v, failed, _ = _derive_judge_verdict(items, [_p(1), _p(2, False)])
    assert v == "брак"
    assert "no_redundancy" in failed and "абзац_2_без_опоры" in failed


def test_gate_a_still_dominates():
    v, _, _ = _derive_judge_verdict(dict(_ALL_OK, numbers_traceable=False), [_p(1)])
    assert v == "брак"


def test_new_gates_are_in_group_b():
    """claim_falsifiable и event_matters — суждение, а не фактура: по одному дают
    «спорно», а не «брак». Ворота A остаются про проверяемые ошибки."""
    for key in ("claim_falsifiable", "event_matters"):
        assert key in _JUDGE_GATES_B and key not in _JUDGE_GATES_A
        v, failed, _ = _derive_judge_verdict(dict(_ALL_OK, **{key: False}), [_p(1)])
        assert (v, failed) == ("спорно", [key]), (key, v, failed)


def test_missing_items_default_to_pass():
    """Забытый в ответе ключ не должен превращаться в провал ворот и ложный «брак»."""
    v, failed, defects = _derive_judge_verdict({}, [])
    assert (v, failed, defects) == ("годится", [], [])


def test_works_without_paragraphs_at_all():
    """Старый ответ без paragraphs обязан обрабатываться: судья в облаке может
    выполняться по прежней инструкции, пока триггер не перечитал файл."""
    v, failed, _ = _derive_judge_verdict(dict(_ALL_OK, has_thesis=False))
    assert (v, failed) == ("спорно", ["has_thesis"])


# ─────────────────────────────────────────────────────────────────────────────
# Сомнения в карточке ревью
# ─────────────────────────────────────────────────────────────────────────────

from signals.content_review_bot import _doubts_line  # noqa: E402

# Реальный разбор кандидата 1638, каким его должен был выдать судья.
_P1638 = [
    {"n": 1, "claim": "АКРА режет рейтинг, толпа уже в лонге", "supported_by": "headline",
     "supported": True, "doubt": "заголовок обещает связь, которой в данных нет"},
    {"n": 3, "claim": "розница развернулась за день до решения агентства",
     "supported_by": "рамка_сюжета", "supported": False,
     "doubt": "один день от шума не отличим"},
    {"n": 4, "claim": "розница выбрала противоположный сценарий заранее",
     "supported_by": "", "supported": False, "doubt": "мысли толпы недоступны"},
]


def test_unsupported_paragraphs_are_always_shown():
    out = _doubts_line(_P1638)
    assert "абз.3" in out and "абз.4" in out
    assert out.count("⛔") == 2, out


def test_doubts_shown_even_for_supported_paragraphs():
    """Иначе «годится» снова превратится в одну строку без зацепок."""
    out = _doubts_line(_P1638)
    assert "абз.1" in out and "которой в данных нет" in out


def test_accepts_json_string_from_db():
    """psycopg может отдать jsonb строкой — карточка не должна из-за этого пустеть."""
    import json as _j
    assert _doubts_line(_j.dumps(_P1638, ensure_ascii=False)) == _doubts_line(_P1638)


def test_silent_when_nothing_to_say():
    assert _doubts_line(None) == ""
    assert _doubts_line([]) == ""
    assert _doubts_line("не json") == ""
    assert _doubts_line([{"n": 1, "supported": True, "doubt": ""}]) == ""


def test_survives_garbage_rows():
    assert "абз.2" in _doubts_line([None, 42, {"n": 2, "supported": False, "claim": "x"}])


# ─────────────────────────────────────────────────────────────────────────────
# Журнал обратной связи (content_feedback, миграция 062)
# ─────────────────────────────────────────────────────────────────────────────

import inspect  # noqa: E402
import re as _re  # noqa: E402

from signals import content_review_bot as BOT  # noqa: E402


def test_journal_is_append_only():
    """Перезапись по месту и была корнем проблемы — в журнале её быть не может."""
    sql = str(BOT._INSERT_FEEDBACK)
    assert "INSERT INTO content_feedback" in sql
    assert not _re.search(r"\bUPDATE\b|\bDELETE\b|ON CONFLICT", sql, _re.I), sql


def test_snapshot_is_taken_by_sql_not_by_caller():
    """Снимок черновика берётся выражением из самого кандидата.

    Если бы его передавали аргументом, вызывающий код обязан был бы помнить, что
    снять состояние ДО своего UPDATE — и рано или поздно забыл бы. Тут это
    структурно невозможно.
    """
    sql = str(BOT._INSERT_FEEDBACK)
    assert "FROM content_candidates" in sql
    assert "draft_text_ai" in sql and "judge_verdict" in sql
    params = set(inspect.signature(BOT._log_feedback).parameters)
    assert "draft_ai" not in params, "снимок не должен приходить снаружи"


def test_snapshot_logged_before_every_overwrite():
    """ПОРЯДОК: _log_feedback обязан идти ДО изменения черновика или статуса.

    Иначе журнал запишет уже перезаписанное состояние, и мы снова потеряем текст,
    к которому относится причина — та же потеря, только теперь молча.
    """
    src = inspect.getsource(BOT)
    for stmt in ("_UPDATE_DRAFT", "_TO_PUBLISHED", "_TO_REJECTED"):
        for m in _re.finditer(rf"db\.execute\({stmt}\b", src):
            before = src[max(0, m.start() - 400):m.start()]
            assert "_log_feedback(db" in before, (
                f"{stmt} исполняется без предшествующего _log_feedback")


def test_every_decision_point_is_logged():
    """Все четыре события человека попадают в журнал."""
    src = inspect.getsource(BOT)
    events = set(_re.findall(r'_log_feedback\(db, cid, "(\w+)"', src))
    assert events == {"approved", "rejected", "edited", "comment"}, events


def test_journal_failure_does_not_break_review():
    """Человек нажал кнопку — решение обязано примениться. Потеря строки журнала
    обратима, неприменённое решение оставляет карточку подвешенной."""
    class _Boom:
        def execute(self, *a, **k):
            raise RuntimeError("нет таблицы")
    BOT._log_feedback(_Boom(), 1638, "rejected")  # не должно бросить


def test_endpoint_reads_journal_not_candidate_columns():
    from api.routers import content_news as CN
    sql = str(CN._SELECT_REAL_REJECTIONS)
    assert "FROM content_feedback" in sql
    assert "f.judge_verdict" in sql, "вердикт нужен НА МОМЕНТ решения, а не сегодняшний"
