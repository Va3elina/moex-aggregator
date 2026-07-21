"""Кастомный uvicorn-worker: принудительно закрывает вечные соединения (SSE)
при graceful shutdown.

Без timeout_graceful_shutdown рециклинг воркера (--max-requests) не может
завершиться: /api/events/stream живёт вечно (keepalive каждые 25с в
api/sse.py), uvicorn ждёт закрытия всех соединений, gunicorn-heartbeat
замирает → через --timeout 60с master шлёт SIGABRT («WORKER TIMEOUT …
was sent code 134!»), разом роняя всех SSE-клиентов воркера (nginx:
«upstream prematurely closed» на /api/events/stream). Наблюдалось до
14 раз/сутки, частота росла с адопцией ленты аномалий.

С лимитом 20с uvicorn сам закрывает оставшиеся стримы (CancelledError в
генераторе → cleanup в sse.py), воркер выходит штатно в пределах
--graceful-timeout 30, а EventSource на клиенте переподключается прозрачно.
"""
from uvicorn.workers import UvicornWorker


class GracefulSSEWorker(UvicornWorker):
    CONFIG_KWARGS = {**UvicornWorker.CONFIG_KWARGS, "timeout_graceful_shutdown": 20}
