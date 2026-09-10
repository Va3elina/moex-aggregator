"""Ярлыки новостей во втором мозге: тип события и роль компании (10.09.2026).

Вадим: «ярлыки новостей — тип события и роль компании; сначала правилами по хэштегам,
остаток — ночным агентом; главное, чтобы всё было автоматизировано и отфильтровано».
Замер на 4 510 новостях мозга за 90 дней: правила размечают 47 %, остальное — агенту.
"""
import contextlib
import importlib.util
import inspect
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from api import brain_core as core  # noqa: E402
from signals import brain_audit_fire as F  # noqa: E402

MSK = timezone(timedelta(hours=3))
THU_0220 = datetime(2026, 9, 10, 2, 20, tzinfo=MSK)
THU_0420 = datetime(2026, 9, 10, 4, 20, tzinfo=MSK)


def _sync():
    spec = importlib.util.spec_from_file_location("brain_sync_labels", ROOT / "Brain" / "brain_sync.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_rule_types_and_agent_types_are_the_same_list():
    s = _sync()
    правила = [t for t, _, _ in s._ТИПЫ_НОВОСТЕЙ]
    assert tuple(правила) + ("прочее",) == core._ТИПЫ_НОВОСТЕЙ


def test_labels_run_every_sync_only_for_90_days_and_only_once():
    s = _sync()
    assert "ярлыки_новостей(conn" in inspect.getsource(s.main)
    assert s._ЯРЛЫКИ_ДНЕЙ == 90
    src = inspect.getsource(s.ярлыки_новостей)
    assert "NOT EXISTS (SELECT 1 FROM brain_news_labels" in src, "по одному разу на новость"
    assert "b.ts > :с" in src
    # регэкспы и теги — параметрами, без «%» и экранирования в тексте запроса
    assert "CAST(:h{i} AS text[])" in src and "~* :r{i}" in src


def test_labels_table_survives_news_rebuild():
    """Ярлыки — не в payload узла: новости() перезаписывает payload при импорте."""
    s = _sync()
    assert "brain_news_labels" in inspect.getsource(s.таблицы_аудита)
    assert "ADD COLUMN IF NOT EXISTS role" in inspect.getsource(s.таблицы_аудита)
    assert "тип" not in inspect.getsource(s.новости)


def test_role_rule_single_company_or_named_in_the_head():
    src = inspect.getsource(_sync().ярлыки_новостей)
    assert "cnt.n = 1" in src and "left(a.text, :голова)" in src
    assert "'главная'" in src and "'упоминание'" in src


def test_labels_window_during_first_pass_and_every_night_after():
    many = F.LABELS_MIN + 10
    # первый проход аудита: 02:20 — основная партия, 04:20 — ярлыки
    assert F.решение(THU_0220, 3000, 0, None, None, 0, None, many) == (None, 0, "main")
    assert F.решение(THU_0420, 3000, 0, None, None, 0, None, many) == (None, 0, "labels")
    # первый проход закончен — ярлыки в любое ночное окно
    assert F.решение(THU_0220, 10, 0, None, None, 0, None, many) == (None, 0, "labels")
    # второе мнение всё равно первым
    assert F.решение(THU_0420, 3000, 0, None, None, F.SECOND_MIN, None, many) == (None, 0, "second")
    # мало — не стреляем ради пары новостей
    assert F.решение(THU_0220, 10, 0, None, None, 0, None, F.LABELS_MIN - 1)[0]


def test_labels_run_is_a_batch_too_for_the_overlap_guard():
    last = THU_0220 - timedelta(minutes=30)
    assert F.решение(THU_0220, 10, 0, None, None, 0, None, 500, last)[0] == "прошлая партия ещё не вернулась"


class _DB:
    def __init__(self, rowcount=1):
        self.sql, self.committed, self.rc = [], False, rowcount

    def execute(self, stmt, params=None):
        self.sql.append((str(stmt), params or {}))
        return type("R", (), {"rowcount": self.rc})()

    def begin_nested(self):
        return contextlib.nullcontext()

    def commit(self):
        self.committed = True


def test_agent_labels_are_validated_and_written_once():
    db = _DB()
    out = core.ярлыки_решения({"партия": "p", "ярлыки": [
        {"id": "news:markettwits/1", "тип": "дивиденды", "роли": {"company:SBER": "главная"}},
        {"id": "news:markettwits/2", "тип": "слухи"},
        {"id": "news:markettwits/3", "тип": "суд", "роли": {"company:SBER": "главнее всех"}},
        {"id": "мусор", "тип": "прочее"},
    ]}, db=db)
    assert out == {"принято": 1, "отброшено": 3}
    sql = " ".join(s for s, _ in db.sql)
    assert "тип IS NULL" in sql, "тип правил агент не перетирает"
    assert "'{}'::jsonb" not in sql, "pg8000 портит jsonb-литералы"
    assert db.sql[0][1]["who"] == "агент"


def test_agent_label_for_already_labelled_news_is_dropped():
    out = core.ярлыки_решения({"ярлыки": [{"id": "news:markettwits/1", "тип": "прочее"}]}, db=_DB(rowcount=0))
    assert out == {"принято": 0, "отброшено": 1}
