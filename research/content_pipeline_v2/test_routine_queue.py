"""Завод постов на общей очереди рутин (signals/routine_queue.py, 15.10.2026).

ROUTINE_QUEUE_CONTENT=1 — content_ai._fire кладёт задание в очередь с ключом «ШАГ:кандидат»
и НЕ ходит в /fire; без флага — прежний прямой /fire. Пауза между запусками в режиме очереди —
дело диспетчера, не завода.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
os.environ.setdefault("DB_URL", "postgresql+pg8000://offline@127.0.0.1:1/none")

from signals import content_ai as CA  # noqa: E402


class _Conn:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def begin(self):
        return _Conn()


def _patch_queue(monkeypatch):
    calls = []
    import api.database
    import signals.routine_queue as RQ
    monkeypatch.setattr(api.database, "get_engine", lambda: _Engine())
    monkeypatch.setattr(RQ, "enqueue", lambda conn, routine, payload, **kw: calls.append((routine, payload, kw)) or 1)
    monkeypatch.setattr(CA.requests, "post", lambda *a, **k: (_ for _ in ()).throw(AssertionError("прямой /fire")))
    return calls


def test_queue_mode_enqueues_with_step_and_candidate(monkeypatch):
    monkeypatch.setenv("ROUTINE_QUEUE_CONTENT", "1")
    calls = _patch_queue(monkeypatch)
    CA._fire(CA.TRIGGER_ID_STEP_A, "tok", "payload-a", dedup=123)
    CA._fire(CA.TRIGGER_ID_STEP_C, "tok", "payload-c", dedup=124)
    assert [(r, kw["dedup_key"]) for r, _, kw in calls] == [("STEP_A", "STEP_A:123"), ("STEP_C", "STEP_C:124")]
    routine, payload, kw = calls[0]
    assert payload == "payload-a" and kw["source"] == "content"
    # повторы — у бэкстопа завода, не у очереди (иначе задвоятся); посты важнее справок
    assert kw["max_attempts"] == 1 and kw["priority"] < 100


def test_direct_mode_still_fires(monkeypatch):
    monkeypatch.delenv("ROUTINE_QUEUE_CONTENT", raising=False)
    sent = []

    class _Resp:
        def raise_for_status(self):
            pass

    monkeypatch.setattr(CA.requests, "post", lambda url, **k: sent.append((url, k["json"])) or _Resp())
    CA._fire(CA.TRIGGER_ID_STEP_A, "tok", "payload-a", dedup=1)
    assert sent and sent[0][1] == {"text": "payload-a"} and CA.TRIGGER_ID_STEP_A in sent[0][0]


def test_stagger_only_in_direct_mode(monkeypatch):
    slept = []

    class _T:
        @staticmethod
        def sleep(s):
            slept.append(s)
    monkeypatch.setattr(CA, "time", _T)
    monkeypatch.setenv("ROUTINE_QUEUE_CONTENT", "1")
    CA._stagger()
    monkeypatch.setenv("ROUTINE_QUEUE_CONTENT", "0")
    CA._stagger()
    assert slept == [CA.FIRE_STAGGER_SEC]


def test_every_fire_call_passes_candidate():
    """Каждый запуск шага по кандидату передаёт dedup — иначе колбэк не найдёт задание
    очереди, и оно займёт место в общем лимите до таймаута."""
    import re
    root = Path(__file__).resolve().parents[2] / "signals"
    for name in ("content_ai.py", "content_match.py", "tg_hype_scan.py"):   # файлами: telethon в CI нет
        src = (root / name).read_text()
        calls = list(re.finditer(r"\b_fire\(TRIGGER_ID_\w+,.*?\)\n", src, re.S))
        assert calls, name
        for m in calls:
            assert "dedup=" in m.group(0), f"{name}: {m.group(0)[:120]}"


def test_routine_rate_limit_is_per_routine():
    """429 «This routine's fire rate limit» — пауза только этой рутины на указанное время."""
    from signals import routine_dispatch as RD
    err = ('HTTPError: 429 {"error":{"message":"This routine\'s fire rate limit has been reached. '
           'Try again in 39m58s.","type":"rate_limit_error"}}')
    assert RD._is_routine_rate_limit(err)
    assert RD._retry_after_sec(err) == 39 * 60 + 58
    assert RD._retry_after_sec("try again in 1h2m") == 3720
    assert RD._retry_after_sec("usage limit reached") is None
    assert not RD._is_routine_rate_limit("HTTPError: 429 usage limit reached for your plan")
