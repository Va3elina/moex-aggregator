"""
Снимок состояния проекта для админского дашборда: /api/admin/dashboard/*

⚠️ ДАШБОРД НИЧЕГО НЕ СЧИТАЕТ НА ЛЕТУ. Экран, который каждые пять секунд честно
пересчитывает 24 процесса и 65 таблиц, — это два десятка запросов в 17-гигабайтную базу
каждые пять секунд. От этого он и виснет. Снимок собирается раз в 30 секунд и лежит в
Redis; открытие экрана — чтение из кэша.

⚠️ ТРИ СОСТОЯНИЯ, А НЕ ДВА. Работает / работает, но данные устарели / не работает.
Второе — самое частое и самое опасное: пайплайн зелёный, а цифры недельной давности.
Именно поэтому у каждого блока рядом с «жив ли» стоит «когда обновлялся».

Только чтение, только админ. Ничего из этого не выходит на публичные страницы.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.cache import get_or_set
from api.database import get_db
from api.models import User
from api.routers.auth import require_admin

router = APIRouter(prefix="/api/admin/dashboard", tags=["admin-dashboard"])

# 30 секунд: «реальное время» здесь — это секунды, а не миллисекунды. Данные
# обновляются пайплайнами раз в пять минут и реже, поэтому чаще опрашивать нечего,
# а редкий опрос делает экран мёртвым на глаз.
_TTL = 30

# Пайплайн, переставший ЗАПУСКАТЬСЯ, вечно хранит последний last_status='ok' —
# heartbeat пишет сам скрипт, поэтому мёртвый cron выглядит зелёным. Тот же порог,
# что в health_monitor: сутки с запасом, недельным — свой.
_MAX_AGE_H = {"distributions": 9 * 24, "mandate_scan": 9 * 24,
              "company_cards": 9 * 24, "ownership_scan": 9 * 24}
_DEFAULT_MAX_AGE_H = 48


# ⚠️ ЗАГЛУШКА «НИКОГДА». record_pipeline_start вставляет новую строку с
# last_run_at = 1970-01-01, потому что колонка NOT NULL, а прогон ещё не
# завершался. Наружу это обязано выходить как «никогда», иначе панель напишет
# «20805 дн назад» — дата-заглушка, притворяющаяся измерением.
_НИКОГДА_ДО = datetime(2000, 1, 1, tzinfo=timezone.utc)


def _когда(v):
    """Время с UTC-меткой, или None для заглушки «никогда»."""
    if v is None:
        return None
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return None if v < _НИКОГДА_ДО else v


def _снимок(db: Session) -> dict:
    now = datetime.now(timezone.utc)

    # ── процессы: жив ли, когда бегал, сколько длился
    процессы = []
    for r in db.execute(text("""
            SELECT pipeline, last_run_at, last_success_at, last_status,
                   last_duration_sec, last_note, started_at
            FROM pipeline_runs ORDER BY last_run_at DESC NULLS LAST""")).mappings():
        run_at = _когда(r["last_run_at"])
        часов = (now - run_at).total_seconds() / 3600 if run_at else None
        молчит = (часов is not None
                  and часов > _MAX_AGE_H.get(r["pipeline"], _DEFAULT_MAX_AGE_H))
        нач = _когда(r["started_at"])
        # «Идёт сейчас» — производное: старт позже последнего финиша. Отдельного
        # статуса 'running' в базе нет намеренно (см. миграцию 079): убитый процесс
        # навсегда застрял бы в нём, а так он сам выдаёт себя возрастом старта.
        идёт = bool(нач and (run_at is None or нач > run_at))
        процессы.append({
            "имя": r["pipeline"],
            "идёт": идёт,
            "идёт_сек": round((now - нач).total_seconds(), 1) if идёт and нач else None,
            "состояние": "молчит" if молчит else (r["last_status"] or "неизвестно"),
            "часов_назад": round(часов, 1) if часов is not None else None,
            "длился_сек": (round(r["last_duration_sec"], 1)
                           if r["last_duration_sec"] is not None else None),
            "заметка": (r["last_note"] or "")[:120],
        })

    # ── конвейер постов: воронка целиком, одним запросом
    воронка = {k: v for k, v in db.execute(text(
        "SELECT status, COUNT(*) FROM content_candidates GROUP BY status")).all()}

    # ── второй мозг: справочник, карточки, граф
    мозг = db.execute(text("""
        SELECT (SELECT COUNT(*) FROM issuers)                            AS эмитентов,
               (SELECT COUNT(*) FROM issuer_securities)                  AS бумаг,
               (SELECT COUNT(*) FROM issuer_aliases)                     AS алиасов,
               (SELECT COUNT(*) FROM company_metrics)                    AS метрик,
               (SELECT COUNT(DISTINCT secid) FROM company_metrics)       AS бумаг_с_карточкой,
               (SELECT COUNT(*) FROM company_documents)                  AS документов,
               (SELECT COUNT(*) FROM world_facts WHERE kind='связь')     AS рёбер,
               (SELECT COUNT(*) FROM world_facts
                 WHERE kind='казначейский пакет')                        AS казначейских,
               (SELECT COUNT(*) FROM ownership_signals WHERE status='новый') AS сигналов_в_очереди
    """)).mappings().first()

    # ── ⚠️ ВТОРОЙ ВИД ПРОТУХАНИЯ: возраст САМИХ данных, а не нашей записи. Структуру
    # акционеров мы перезаписываем еженедельно исправно, а у источника она может быть
    # пятилетней. По первой оси это вечное «ok».
    старение = db.execute(text("""
        SELECT (SELECT COUNT(DISTINCT issuer_id) FROM company_shareholders
                 WHERE structure_as_of < CURRENT_DATE - INTERVAL '2 years') AS акционеры_старше_2лет,
               (SELECT COUNT(DISTINCT issuer_id) FROM company_shareholders)  AS акционеры_всего,
               (SELECT COUNT(*) FROM world_facts
                 WHERE kind='связь' AND valid_from < CURRENT_DATE - INTERVAL '2 years') AS рёбра_старше_2лет
    """)).mappings().first()

    # ── хранилища: вес и строки. pg_total_relation_size по имени — дешёвая операция
    # по системному каталогу, без чтения самих таблиц.
    хранилища = [dict(r) for r in db.execute(text("""
        SELECT c.relname AS таблица,
               pg_size_pretty(pg_total_relation_size(c.oid)) AS размер,
               pg_total_relation_size(c.oid) AS байт,
               COALESCE(s.n_live_tup, 0) AS строк
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE n.nspname = 'public' AND c.relkind = 'r'
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT 12""")).mappings()]

    молчат = [p["имя"] for p in процессы if p["состояние"] == "молчит"]
    упали = [p["имя"] for p in процессы
             if p["состояние"] not in ("ok", "молчит", "неизвестно")]
    return {
        "снято": now.isoformat(),
        # ⚠️ Стареющие данные в общий вердикт НЕ входят: у 40 компаний из 78 структура
        # акционеров старше двух лет — это норма жизни рынка, а не авария. Тревога,
        # которая горит всегда, гасит внимание ко всем остальным.
        "вердикт": "сломано" if (упали or молчат) else "работает",
        "молчат": молчат,
        "упали": упали,
        "процессы": процессы,
        "воронка_постов": воронка,
        "второй_мозг": dict(мозг or {}),
        "стареющие_данные": dict(старение or {}),
        "хранилища": хранилища,
    }


@router.get("/live")
def live(
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """
    Живое состояние процессов. БЕЗ КЭША и без тяжёлых запросов.

    ⚠️ ЭТО НЕ УМЕНЬШЕННАЯ КОПИЯ /overview. Тот снимок стоит полсекунды и лежит в
    Redis 30 секунд — опрашивать его чаще бессмысленно и дорого. Здесь одна
    лёгкая выборка по 27 строкам: её не жалко дёргать раз в пару секунд и, что
    важнее, отдавать сразу после SSE-события, не дожидаясь протухания кэша.

    Панель живёт на событиях (SSE 'pipeline'), а эта ручка нужна для первой
    отрисовки и для восстановления после разрыва соединения — иначе экран,
    открытый в момент паузы между событиями, показывал бы пустоту.
    """
    now = datetime.now(timezone.utc)
    итог = []
    for r in db.execute(text("""
            SELECT pipeline, last_run_at, last_status, last_duration_sec,
                   last_note, started_at
            FROM pipeline_runs""")).mappings():
        run_at, нач = _когда(r["last_run_at"]), _когда(r["started_at"])
        идёт = bool(нач and (run_at is None or нач > run_at))
        итог.append({
            "имя": r["pipeline"],
            "идёт": идёт,
            "идёт_сек": round((now - нач).total_seconds(), 1) if идёт and нач else None,
            "состояние": r["last_status"] or "неизвестно",
            "закончил_сек_назад": (round((now - run_at).total_seconds(), 1)
                                   if run_at else None),
            "длился_сек": (round(r["last_duration_sec"], 1)
                           if r["last_duration_sec"] is not None else None),
            "заметка": (r["last_note"] or "")[:120],
        })
    итог.sort(key=lambda x: (not x["идёт"], x["закончил_сек_назад"] or 1e12))
    return {"снято": now.isoformat(), "процессы": итог,
            "идут": [p["имя"] for p in итог if p["идёт"]]}


@router.get("/overview")
def overview(
    fresh: bool = Query(False, description="пересобрать снимок, минуя кэш"),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Карта проекта одним запросом: процессы, воронка, второй мозг, хранилища."""
    ключ = "dashboard:overview"
    if not fresh:
        cached = get_or_set(ключ)
        if cached is not None:
            cached["из_кэша"] = True
            return cached
    снимок = _снимок(db)
    get_or_set(ключ, снимок, ttl=_TTL)
    снимок["из_кэша"] = False
    return снимок
