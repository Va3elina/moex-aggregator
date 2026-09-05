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
from api.pipeline_notes import человеческая
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

# ⚠️ ЗАМЕТКУ БОЛЬШЕ НЕ РЕЖЕМ. Здесь стоял срез до 120 символов, и он обрывал
# итог прогона на полуслове: «content_match {'checked': 6, … 'step_c_» — владелец
# видел не сокращение, а обрубок. Сам heartbeat уже режет до 500 символов при
# записи, второй раз укорачивать нечего; решение «сколько показать» принадлежит
# экрану, а не выдаче.


def _давно(t, now) -> str:
    """«3 мин назад» / «2 ч назад» / «4 дн назад» — для подписи у живой цифры."""
    if isinstance(t, str):          # из журнала приходит уже isoformat
        t = datetime.fromisoformat(t)
    t = _когда(t) if not isinstance(t, datetime) else t
    if t is None:
        return "никогда"
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    сек = (now - t).total_seconds()
    if сек < 90:
        return "только что"
    if сек < 3600:
        return f"{int(сек // 60)} мин назад"
    if сек < 48 * 3600:
        return f"{int(сек // 3600)} ч назад"
    return f"{int(сек // 86400)} дн назад"


def _дата(d) -> str:
    return d.strftime("%d.%m") if d else "—"


def _скл(n: int, один: str, два: str, пять: str) -> str:
    """131 бумага, 2 бумаги, 50 бумаг — «131 бумаг» читается как машинный текст."""
    n = int(n)
    сотня, ед = n % 100, n % 10
    слово = пять if 11 <= сотня <= 14 else один if ед == 1 else два if 2 <= ед <= 4 else пять
    return f"{n} {слово}"


def _журнал_сутки(db: Session) -> dict:
    """Сколько раз бегал и сколько строк записал каждый процесс за сутки — из журнала."""
    return {r["pipeline"]: {
        "прогонов": int(r["прогонов"]),
        "строк": int(r["строк"]) if r["строк"] is not None else None,
        "последний": r["последний"].isoformat() if r["последний"] else None,
    } for r in db.execute(text("""
        SELECT pipeline, COUNT(*) AS прогонов, SUM(rows_written) AS строк, MAX(finished_at) AS последний
          FROM pipeline_run_log
         WHERE finished_at > NOW() - INTERVAL '24 hours'
         GROUP BY pipeline
    """)).mappings()}


def _источники(db: Session, now, журнал: dict) -> dict:
    """
    Живые цифры у узлов-источников карты: не «процесс отработал», а что именно
    приехало и насколько оно свежее. Ключи — id узлов из topology.ts.

    ⚠️ ВОЗРАСТ ДАННЫХ, А НЕ ВОЗРАСТ ЗАПИСИ. У Банка России ряд ZCYC «обновлялся»
    каждый день и при этом стоял на 11.06 три месяца — потому что источник его
    не отдавал. Здесь смотрится дата последней ТОЧКИ ряда, и порог свой у каждой
    частоты: дневному ряду неделя — тревога, месячному — полтора месяца,
    квартальному — четыре.
    """
    def факт(ярлык, значение, подпись=None, тревога=False):
        return {"ярлык": ярлык, "значение": значение, "подпись": подпись, "тревога": bool(тревога)}

    def строк_за_сутки(*пайплайны):
        всего = 0
        есть = False
        for п in пайплайны:
            ж = журнал.get(п)
            if ж and ж["строк"] is not None:
                всего += ж["строк"]
                есть = True
        return всего if есть else None

    def дней(d):
        return (now.date() - d).days if d else None

    ист: dict[str, dict] = {}

    # ── FinanceMarker: суточная квота из нашего счётчика
    б = db.execute(text("""
        SELECT spent, limit_total, calls, updated_at FROM api_budget
         WHERE source = 'financemarker' AND period_day = CURRENT_DATE
    """)).mappings().first()
    if б:
        остаток = max(0, int(б["limit_total"]) - int(б["spent"]))
        ист["fm"] = {
            "заголовок": f"квота {int(б['spent'])} из {б['limit_total']} · остаток {остаток}",
            "факты": [факт("квота сегодня", f"{int(б['spent'])} из {б['limit_total']}",
                          f"{б['calls']} вызовов · последний {_давно(б['updated_at'], now)}",
                          тревога=остаток == 0)],
        }
    else:
        ист["fm"] = {"заголовок": "сегодня ещё не ходили", "факты": [факт("квота сегодня", "не тронута")]}

    # ── Telegram: по каналам — последний пост и сколько за сутки
    факты = []
    сутки = 0
    for r in db.execute(text("""
        SELECT channel, MAX(posted_at) AS последний,
               COUNT(*) FILTER (WHERE posted_at > NOW() - INTERVAL '24 hours') AS за_сутки
          FROM tg_channel_watch GROUP BY channel ORDER BY channel
    """)).mappings():
        # В выходные каналы молчат по полдня — шесть часов тишины тревога только в будни.
        порог_ч = 30 if now.weekday() >= 5 else 6
        тихо = (now - r["последний"]).total_seconds() > порог_ч * 3600 if r["последний"] else True
        сутки += int(r["за_сутки"])
        факты.append(факт(f"@{r['channel']}", f"{_скл(r['за_сутки'], 'пост', 'поста', 'постов')} за сутки",
                          f"последний {_давно(r['последний'], now)}", тревога=тихо))
    ист["tg"] = {"заголовок": f"{_скл(сутки, 'пост', 'поста', 'постов')} за сутки из двух каналов", "факты": факты}

    # ── МосБиржа: объём за сутки по журналу
    свечи = строк_за_сутки("candles_spot", "candles_futures")
    ои = строк_за_сутки("oi_5min")
    ист["moex"] = {
        "заголовок": (f"{свечи:,} свечей · {ои:,} строк ОИ за сутки".replace(",", " ")
                      if свечи is not None and ои is not None else "объём за сутки ещё не набрался"),
        "факты": [
            факт("свечи акций и фьючерсов", f"{свечи:,} строк за сутки".replace(",", " ") if свечи is not None else "—",
                 f"последний прогон {_давно(журнал.get('candles_spot', {}).get('последний'), now)}"),
            факт("открытый интерес 5 мин", f"{ои:,} строк за сутки".replace(",", " ") if ои is not None else "—",
                 f"последний прогон {_давно(журнал.get('oi_5min', {}).get('последний'), now)}"),
        ],
    }

    # ── Cbonds: СЧА
    ф = db.execute(text("""
        SELECT MAX(trade_date) AS д,
               (SELECT COUNT(*) FROM fund_data WHERE trade_date = (SELECT MAX(trade_date) FROM fund_data)) AS фондов
          FROM fund_data""")).mappings().first()
    ист["cbonds"] = {
        "заголовок": f"СЧА {_скл(ф['фондов'], 'фонда', 'фондов', 'фондов')} на {_дата(ф['д'])}",
        "факты": [факт("СЧА и паи", f"{_скл(ф['фондов'], 'фонд', 'фонда', 'фондов')} на {_дата(ф['д'])}",
                      "Cbonds отдаёт дату расчёта УК: сдвиг на день — норма", тревога=(дней(ф["д"]) or 0) > 4)],
    }

    # ── Банк России и Росстат: дата последней точки каждого ряда
    ряды = {r["indicator"]: r for r in db.execute(text("""
        SELECT m.indicator, m.name, m.frequency,
               (SELECT MAX(period_date) FROM macro_data d WHERE d.indicator = m.indicator) AS д
          FROM macro m
         WHERE m.indicator IN ('KEY_RATE', 'M2_MONTHLY', 'GDP_QUARTERLY', 'ZCYC_10Y')
    """)).mappings()}
    # ⚠️ Квартальному ряду — 175 дней, не 130. Росстат обновляет квартальный xlsx
    # не с первой оценкой (середина августа), а с уточнённой — во второй половине
    # сентября: I квартал (31.03) появляется в файле через ~170 дней. Порог 130
    # горел ложно весь август и полсентября.
    порог = {"daily": 7, "monthly": 45, "quarterly": 175}

    def ряд(код, ярлык):
        r = ряды.get(код)
        if not r or not r["д"]:
            return факт(ярлык, "ряда нет", тревога=True)
        дн = дней(r["д"])
        return факт(ярлык, f"на {_дата(r['д'])}", f"{дн} дн назад · ряд {r['frequency']}",
                    тревога=дн > порог.get(r["frequency"], 30))

    цб = [ряд("KEY_RATE", "ключевая ставка"), ряд("M2_MONTHLY", "денежная масса M2"), ряд("ZCYC_10Y", "доходность ОФЗ 10 лет")]
    отстали = [x["ярлык"] for x in цб if x["тревога"]]
    ист["cbr"] = {
        "заголовок": (f"отстал: {', '.join(отстали)}" if отстали
                      else f"ставка на {_дата(ряды['KEY_RATE']['д'])} · M2 на {_дата(ряды['M2_MONTHLY']['д'])}"),
        "факты": цб,
    }
    ввп = ряд("GDP_QUARTERLY", "ВВП, квартальный")
    ист["rosstat"] = {"заголовок": f"ВВП {ввп['значение']}", "факты": [ввп]}

    # ── smart-lab: капитализация и карточки
    к = db.execute(text("""
        SELECT MAX(period_date) AS д,
               (SELECT COUNT(*) FROM stock_market_cap WHERE period_date = (SELECT MAX(period_date) FROM stock_market_cap)) AS бумаг
          FROM stock_market_cap""")).mappings().first()
    # ⚠️ Не «новых за неделю»: архив документов пересобирается целиком, и все 8 221
    # строки имеют свежий created_at. Честная цифра — размер и когда обновлён.
    док = db.execute(text("""
        SELECT COUNT(*) AS всего, MAX(created_at) AS последний FROM company_documents""")).mappings().first()
    ист["smartlab"] = {
        "заголовок": f"капитализация {_скл(к['бумаг'], 'бумаги', 'бумаг', 'бумаг')} на {_дата(к['д'])}",
        "факты": [
            факт("капитализация", f"{_скл(к['бумаг'], 'бумага', 'бумаги', 'бумаг')} на {_дата(к['д'])}",
                 f"{дней(к['д'])} дн назад" if к["д"] else None, тревога=(дней(к["д"]) or 0) > 3),
            факт("документы компаний", f"{_скл(док['всего'], 'документ', 'документа', 'документов')} в архиве",
                 f"обновлён {_давно(док['последний'], now)}"),
        ],
    }

    # ── сырьё
    сырьё = строк_за_сутки("commodity_daily")
    ист["commodity"] = {
        "заголовок": f"{сырьё} строк за сутки" if сырьё is not None else "за сутки не бегал",
        "факты": [факт("Yahoo через релей", f"{сырьё} строк за сутки" if сырьё is not None else "—",
                      f"последний прогон {_давно(журнал.get('commodity_daily', {}).get('последний'), now)}")],
    }
    return ист


def _перевод(r) -> dict:
    """Человеческая фраза и флаг «зелёный, но пустой» для строки pipeline_runs."""
    ч = человеческая(r["pipeline"], r["last_status"], r["last_note"], r["last_duration_sec"])
    return {"фраза": ч["фраза"], "тревога": ч["тревога"]}


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
            "заметка": r["last_note"] or "",
            # Перевод рядом с сырой заметкой, а не вместо неё: экран показывает
            # фразу, оригинал остаётся под рукой — перевод не должен прятать факт.
            **_перевод(r),
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

    # ── живые цифры у источников + журнал за сутки (что и сколько приехало)
    журнал = _журнал_сутки(db)
    try:
        источники = _источники(db, now, журнал)
    except Exception as e:  # noqa: BLE001 — одна упавшая цифра не должна валить снимок
        источники = {"_ошибка": str(e)[:200]}

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
        "источники": источники,
        "журнал_сутки": журнал,
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
    # Медиана длительности за 14 дней по журналу (миграция 080). У процесса без
    # истории — NULL, и экран ничего не утверждает. Это замена снятой эвристике
    # «меньше секунды — ранний выход»: та врала у штатно быстрых скриптов.
    типично = {r[0]: float(r[1]) for r in db.execute(text("""
        SELECT pipeline, percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_sec)
          FROM pipeline_run_log
         WHERE finished_at > now() - interval '14 days' AND duration_sec IS NOT NULL
           AND status = 'ok'
         GROUP BY pipeline HAVING COUNT(*) >= 5
    """)).all()}
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
            "заметка": r["last_note"] or "",
            # Перевод рядом с сырой заметкой, а не вместо неё: экран показывает
            # фразу, оригинал остаётся под рукой — перевод не должен прятать факт.
            **_перевод(r),
            "типично_сек": (round(типично[r["pipeline"]], 1) if r["pipeline"] in типично else None),
        })
    итог.sort(key=lambda x: (not x["идёт"], x["закончил_сек_назад"] or 1e12))
    return {"снято": now.isoformat(), "процессы": итог,
            "идут": [p["имя"] for p in итог if p["идёт"]]}


@router.get("/pulse")
def pulse(
    hours: int = Query(24, ge=1, le=168),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """
    Пульс: что и сколько писалось по часам, лента последних прогонов, ковёр шагов.

    Всё из pipeline_run_log (миграция 080). Первые сутки после деплоя гистограмма
    неполная — журнал наполняется с этого момента, истории задним числом нет.
    """
    now = datetime.now(timezone.utc)
    часы = [{
        "час": r["час"].isoformat(),
        "прогонов": r["прогонов"],
        "записей": int(r["записей"]) if r["записей"] is not None else 0,
        "сбоев": r["сбоев"],
    } for r in db.execute(text("""
        SELECT date_trunc('hour', finished_at) AS час,
               COUNT(*) AS прогонов,
               SUM(rows_written) AS записей,
               COUNT(*) FILTER (WHERE status NOT IN ('ok', 'degraded')) AS сбоев
          FROM pipeline_run_log
         WHERE finished_at > now() - make_interval(hours => :h)
         GROUP BY 1 ORDER BY 1
    """), {"h": hours}).mappings()]

    лента = []
    for r in db.execute(text("""
        SELECT pipeline, finished_at, status, duration_sec, rows_written, note
          FROM pipeline_run_log
         ORDER BY finished_at DESC LIMIT 40
    """)).mappings():
        ч = человеческая(r["pipeline"], r["status"], r["note"], r["duration_sec"])
        лента.append({
            "имя": r["pipeline"],
            "когда": r["finished_at"].isoformat(),
            "сек_назад": round((now - r["finished_at"]).total_seconds(), 1),
            "статус": r["status"],
            "длился_сек": round(r["duration_sec"], 1) if r["duration_sec"] is not None else None,
            "записей": r["rows_written"],
            "фраза": ч["фраза"],
            "тревога": ч["тревога"],
        })

    return {
        "снято": now.isoformat(),
        "часов": hours,
        "по_часам": часы,
        "лента": лента,
        # Ковёр — те же последние прогоны, но в порядке выполнения: квадрат на шаг.
        "ковёр": [{"имя": x["имя"], "статус": x["статус"]} for x in reversed(лента[:32])],
        "всего_в_журнале": db.execute(text("SELECT COUNT(*) FROM pipeline_run_log")).scalar(),
    }


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
