"""
След агента: запись того, что он искал в данных, что нашлось и что дошло до текста.

ЗАЧЕМ. Между новостью на входе и черновиком на выходе сейчас не видно ничего.
Воронка говорит, что из 1 117 кандидатов опубликовано 5, но не почему именно эти.
След делает работу агента наблюдаемой: одна строка на обращение к данным.

⚠️ СЛЕД НИКОГДА НЕ РОНЯЕТ ПАЙПЛАЙН. Наблюдение за работой не может быть причиной,
по которой работа не сделана. Любая ошибка записи гасится и уходит в лог; функции
здесь ничего не бросают наружу и ничего не возвращают, кроме идентификатора.

⚠️ ЛЕЖИТ В api/, А НЕ В signals/, НАМЕРЕННО. След пишут обе стороны: конвейер
(signals/content_ai.py, сбор брифа) и приёмка результатов агентов (api/routers/
content_news.py, судья и писатель). В образе API каталога signals/ НЕТ — импорт
оттуда уронил бы приложение на старте. Обратное направление работает: signals уже
импортирует api.database, значит api виден обеим сторонам, а signals — только одной.

⚠️ ПУСТОЙ ОТВЕТ ТОЖЕ ПИШЕТСЯ. Соблазн писать только удачные обращения велик, но
именно отказы объясняют черновик: «структура MTSS → пусто» говорит больше, чем
любое рассуждение о том, почему МТС не попал в текст.
"""

import json
import logging

from sqlalchemy import text

log = logging.getLogger(__name__)

_INSERT = text("""
    INSERT INTO agent_trace (candidate_id, step, seq, source, question, params,
                             result_count, result_note, outcome, outcome_reason,
                             duration_ms)
    VALUES (:cid, :step, :seq, :src, :q, CAST(:params AS jsonb), :n, :note,
            :outcome, :reason, :ms)
""")

# Три исхода, и третий не менее важен первых двух.
ВЗЯТО = "взято"        # попало в бриф и дошло до модели
НЕ_ВЗЯТО = "не_взято"  # нашлось, но агент отбросил — причина обязательна
ПУСТО = "пусто"        # источник ничего не вернул


class Trace:
    """
    Счётчик обращений для одного кандидата на одном шаге.

    Порядковый номер ведётся здесь, а не в базе: он должен отражать порядок, в
    котором агент реально ходил в данные, а не порядок, в котором строки успели
    записаться.
    """

    def __init__(self, db, candidate_id, step: str):
        self.db = db
        self.candidate_id = candidate_id
        self.step = step
        self.seq = 0

    def record(self, source: str, question: str, *, outcome: str,
               result_count: int = 0, result_note: str = None,
               reason: str = None, params: dict = None, duration_ms: int = None):
        self.seq += 1
        try:
            self.db.execute(_INSERT, {
                "cid": self.candidate_id, "step": self.step, "seq": self.seq,
                "src": source[:40], "q": question,
                "params": json.dumps(params or {}, ensure_ascii=False, default=str),
                "n": int(result_count or 0), "note": result_note,
                "outcome": outcome, "reason": reason, "ms": duration_ms,
            })
        except Exception as ex:
            # Осознанно проглатываем: наблюдение не может ломать наблюдаемое.
            log.warning("след агента не записан (%s / %s): %s", source, outcome, ex)


def трассировать(db, candidate_id, step: str = "бриф") -> Trace:
    return Trace(db, candidate_id, step)
