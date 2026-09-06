"""
Разбор кандидата в посты: /api/admin/dashboard/posts/*

⚠️ СМОТРИМ И ОПУБЛИКОВАННЫЕ, И ОТКАЗЫ. Опубликованных всего пять, и все они
сделаны до появления судьи — у них пустой вердикт и нет поабзацного разбора.
Соблазн показывать только те кандидаты, где след полный, велик, но тогда экран
теряет главное: из 1163 кандидатов 1069 отсеяны, и вопрос «почему именно так»
задаётся как раз к ним. Поэтому список отдаёт все статусы, а карточка честно
пишет, чего в следе нет, вместо пустого блока.

⚠️ СВЯЗЬ С НОВОСТЬЮ — ЧЕРЕЗ РАЗБОР URL, А НЕ ПО КЛЮЧУ. Формального внешнего
ключа на news_archive нет: у кандидата лежит только source_url вида
`https://t.me/newssmartlab/130070`. Пара (канал, id сообщения) совпадает с
первичным ключом архива, поэтому разбираем URL здесь, а не заводим join по
тексту. Как появится колонка — заменить.

⚠️ ДВА ИМЕНИ ОДНОГО КАНАЛА. Исторический экспорт лежит под `MarketTwits` и
`СМАРТЛАБ НОВОСТИ`, живой ингест — под `markettwits` и `newssmartlab`. Кандидаты
ссылаются на живые имена, поэтому здесь совпадёт; но любой фильтр по каналу
обязан помнить про обе формы.

Только чтение, только админ.
"""

import re
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import require_admin

router = APIRouter(prefix="/api/admin/dashboard/posts", tags=["admin-dashboard"])

# Путь кандидата: восемь машинных статусов сворачиваются в пять этапов воронки.
# ⚠️ Порядок здесь — не рейтинг по величине, а путь: сначала вход, потом потери.
ЭТАПЫ = [
    ("pending", "ждут разбора"),
    ("candidate", "только пришли"),
    ("no_data", "нечем подтвердить"),
    ("discarded", "отсеяны судьёй релевантности"),
    ("rejected", "отклонены человеком"),
    ("draft_ready", "черновик готов"),
    ("in_review", "на ревью"),
    ("published", "опубликованы"),
]
ПОДПИСЬ_СТАТУСА = dict(ЭТАПЫ)

_URL_ПОСТА = re.compile(r"t\.me/(?:s/)?([A-Za-z0-9_]+)/(\d+)")


def _разобрать_url(url: str | None):
    """(канал, id сообщения) из ссылки на пост, либо (None, None)."""
    if not url:
        return None, None
    m = _URL_ПОСТА.search(url)
    return (m.group(1), int(m.group(2))) if m else (None, None)


def _возраст(t, now):
    if t is None:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return round((now - t).total_seconds(), 1)


@router.get("")
def список(
    status: str | None = Query(None, description="фильтр по статусу"),
    ticker: str | None = Query(None, description="тикер в списке кандидата"),
    verdict: str | None = Query(None, description="вердикт судьи: годится/спорно/брак"),
    q: str | None = Query(None, description="поиск по заголовку"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Список кандидатов с фильтрами + разбивка по статусам."""
    условия, параметры = ["1 = 1"], {"limit": limit, "offset": offset}
    if status:
        условия.append("c.status = :status")
        параметры["status"] = status
    if ticker:
        # Пересечение массивов, а не ANY: под tickers лежит GIN, и обратиться к
        # нему может только `&&`. Проверено: на нынешних 1163 строках планировщик
        # всё равно выбирает полный проход — таблица слишком мала, чтобы индекс
        # окупился. Оператор оставлен правильный, чтобы он заработал сам, когда
        # кандидатов станет на порядок больше.
        условия.append("c.tickers && ARRAY[:ticker]::text[]")
        параметры["ticker"] = ticker.upper()
    if verdict:
        условия.append("c.judge_verdict = :verdict")
        параметры["verdict"] = verdict
    if q:
        условия.append("c.headline ILIKE :q")
        параметры["q"] = f"%{q}%"
    где = " AND ".join(условия)

    строки = db.execute(text(f"""
        SELECT c.id, c.status, c.source, c.headline, c.tickers, c.futures_ticker,
               c.event_type, c.importance_1_5, c.judge_verdict, c.brief_version,
               c.created_at, c.published_at, c.matched_anomaly_id,
               (c.draft_text IS NOT NULL)      AS есть_черновик,
               (c.judge_paragraphs IS NOT NULL) AS есть_разбор,
               (SELECT COUNT(*) FROM agent_trace t WHERE t.candidate_id = c.id) AS шагов
          FROM content_candidates c
         WHERE {где}
         ORDER BY c.created_at DESC
         LIMIT :limit OFFSET :offset
    """), параметры).mappings().all()

    по_статусам = {r[0]: r[1] for r in db.execute(text(
        "SELECT status, COUNT(*) FROM content_candidates GROUP BY status")).all()}

    всего = db.execute(text(f"""
        SELECT COUNT(*) FROM content_candidates c WHERE {где}
    """), {k: v for k, v in параметры.items() if k not in ("limit", "offset")}).scalar()

    return {
        "всего_по_фильтру": всего,
        "по_статусам": по_статусам,
        "этапы": [{"код": к, "подпись": п, "сколько": по_статусам.get(к, 0)}
                  for к, п in ЭТАПЫ],
        "кандидаты": [{
            "id": r["id"],
            "статус": r["status"],
            "статус_подпись": ПОДПИСЬ_СТАТУСА.get(r["status"], r["status"]),
            "источник": r["source"],
            "заголовок": r["headline"],
            "тикеры": list(r["tickers"] or []),
            "фьючерс": r["futures_ticker"],
            "тип_события": r["event_type"],
            "важность": r["importance_1_5"],
            "вердикт": r["judge_verdict"],
            "версия_брифа": r["brief_version"],
            "создан": r["created_at"].isoformat() if r["created_at"] else None,
            "опубликован": r["published_at"].isoformat() if r["published_at"] else None,
            "есть_аномалия": r["matched_anomaly_id"] is not None,
            "есть_черновик": r["есть_черновик"],
            "есть_разбор": r["есть_разбор"],
            "шагов_следа": r["шагов"],
        } for r in строки],
    }


@router.get("/{candidate_id}")
def карточка(
    candidate_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Полный путь кандидата: новость → судья релевантности → данные → черновик → судья → человек."""
    now = datetime.now(timezone.utc)

    c = db.execute(text("""
        SELECT * FROM content_candidates WHERE id = :id
    """), {"id": candidate_id}).mappings().first()
    if c is None:
        raise HTTPException(status_code=404, detail="кандидат не найден")

    # ── исходная новость и разгон репостов
    канал, mid = _разобрать_url(c["source_url"])
    новость = разгон = None
    if канал and mid:
        новость = db.execute(text("""
            SELECT channel, message_id, posted_at, text, views, tickers, hashtags
              FROM news_archive WHERE channel = :ch AND message_id = :mid
        """), {"ch": канал, "mid": mid}).mappings().first()
        разгон = db.execute(text("""
            SELECT fwd_3, fwd_5, fwd_6, fwd_7, fwd_8, fwd_15, fwd_90, promoted
              FROM tg_channel_watch WHERE channel = :ch AND message_id = :mid
        """), {"ch": канал, "mid": mid}).mappings().first()

    # ── аномалия, на которой стоит пост
    аномалия = None
    if c["matched_anomaly_id"]:
        аномалия = db.execute(text("""
            SELECT id, type, asset_id, asset_name, clgroup, direction,
                   headline, context, severity_value, signal_date, deep_link
              FROM anomalies WHERE id = :id
        """), {"id": c["matched_anomaly_id"]}).mappings().first()

    # ── что агент спрашивал у базы
    след = db.execute(text("""
        SELECT step, seq, source, question, params, result_count, result_note,
               outcome, outcome_reason, duration_ms, created_at
          FROM agent_trace WHERE candidate_id = :id ORDER BY seq, id
    """), {"id": candidate_id}).mappings().all()

    # ── решения человека и правки судьи
    журнал = db.execute(text("""
        SELECT event, reason_code, reason_text, judge_verdict, created_at
          FROM content_feedback WHERE candidate_id = :id ORDER BY created_at
    """), {"id": candidate_id}).mappings().all()

    # ⚠️ Чего в следе НЕТ — говорим прямо. Пустой блок читается как «всё хорошо,
    # просто ничего не было», а на деле у постов до 01.09 судьи ещё не существовало,
    # и снимок брифа не сохраняется вовсе. Молчать об этом — врать умолчанием.
    пробелы = []
    if not след:
        пробелы.append("Следа агента нет: он пишется с 04.09.2026, более ранние "
                       "кандидаты проходили конвейер до его появления")
    if c["judge_verdict"] is None and c["draft_text"]:
        пробелы.append("Судья черновика не выносил вердикт: он появился 31.08.2026, "
                       "а этот черновик написан раньше")
    if c["draft_text"] and not c["annotation"]:
        пробелы.append("Аннотации с числами нет — она появилась позже")
    пробелы.append("Снимок брифа не сохраняется: числа, на которых стоял пост "
                   "(позиции физлиц, цена), собирались на лету и не остались нигде")

    return {
        "кандидат": {
            "id": c["id"],
            "статус": c["status"],
            "статус_подпись": ПОДПИСЬ_СТАТУСА.get(c["status"], c["status"]),
            "источник": c["source"],
            "ссылка": c["source_url"],
            "заголовок": c["headline"],
            "текст_новости": c["raw_text"],
            "тикеры": list(c["tickers"] or []),
            "фьючерс": c["futures_ticker"],
            "тип_события": c["event_type"],
            "важность": c["importance_1_5"],
            "версия_брифа": c["brief_version"],
            "тред": c["thread_key"],
            "родитель": c["parent_candidate_id"],
            "создан": c["created_at"].isoformat() if c["created_at"] else None,
            "опубликован": c["published_at"].isoformat() if c["published_at"] else None,
            "возраст_сек": _возраст(c["created_at"], now),
        },
        "шаг_н_хайп": {
            "решение": c["hype_filter_result"],
            "когда": c["hype_filter_checked_at"].isoformat() if c["hype_filter_checked_at"] else None,
            "сдался": c["hype_filter_gave_up_at"] is not None,
        },
        "шаг_а_релевантность": {
            "обоснование": c["reasoning"],
            "истекает": c["pending_expires_at"].isoformat() if c["pending_expires_at"] else None,
        },
        "шаг_б_данные": {
            "проверен": c["step_b_checked_at"].isoformat() if c["step_b_checked_at"] else None,
            "аномалия": dict(аномалия) | {
                "signal_date": аномалия["signal_date"].isoformat() if аномалия["signal_date"] else None,
                "severity_value": (float(аномалия["severity_value"])
                                   if аномалия["severity_value"] is not None else None),
            } if аномалия else None,
        },
        "шаг_в_черновик": {
            "текст": c["draft_text"],
            "текст_ии": c["draft_text_ai"],
            "правил_человек": bool(c["draft_text"] and c["draft_text_ai"]
                                   and c["draft_text"] != c["draft_text_ai"]),
            "отказ": c["synth_declined_reason"],
            "аннотация": c["annotation"],
            "профиль_стиля": c["style_profile"],
        },
        "шаг_г_судья": {
            "вердикт": c["judge_verdict"],
            "провалено": list(c["judge_failed"] or []),
            "замечания": list(c["judge_defects"] or []),
            "заметка": c["judge_note"],
            "пункты": c["judge_items"],
            "абзацы": c["judge_paragraphs"],
            "правил": c["judge_fixed_at"].isoformat() if c["judge_fixed_at"] else None,
            "что_поправил": c["judge_fix_note"],
            "когда": c["judge_checked_at"].isoformat() if c["judge_checked_at"] else None,
        },
        "человек": {
            "решение": c["reviewer_action"],
            "причина": c["reviewer_reason"],
            "код_причины": c["reviewer_reason_code"],
            "уведомлён": c["reviewer_notified_at"].isoformat() if c["reviewer_notified_at"] else None,
        },
        "новость": (dict(новость) | {
            "posted_at": новость["posted_at"].isoformat() if новость["posted_at"] else None,
            "tickers": list(новость["tickers"] or []),
            "hashtags": list(новость["hashtags"] or []),
        }) if новость else None,
        "разгон_репостов": (
            [{"минута": м, "репостов": разгон[f"fwd_{м}"]}
             for м in (3, 5, 6, 7, 8, 15, 90) if разгон[f"fwd_{м}"] is not None]
            if разгон else []
        ),
        "признан_хайпом": bool(разгон["promoted"]) if разгон else None,
        "мозг": [{
            "шаг": t["step"], "номер": t["seq"], "источник": t["source"], "вопрос": t["question"],
            "нашлось": t["result_count"], "результат": t["result_note"], "исход": t["outcome"],
            "почему": t["outcome_reason"], "мс": t["duration_ms"],
        } for t in след if t["step"] == "мозг"],
        "след": [{
            "шаг": t["step"],
            "номер": t["seq"],
            "источник": t["source"],
            "вопрос": t["question"],
            "нашлось": t["result_count"],
            "результат": t["result_note"],
            "исход": t["outcome"],
            "почему": t["outcome_reason"],
            "мс": t["duration_ms"],
        } for t in след if t["step"] != "мозг"],
        "журнал": [{
            "событие": j["event"],
            "код": j["reason_code"],
            "причина": j["reason_text"],
            "вердикт_тогда": j["judge_verdict"],
            "когда": j["created_at"].isoformat() if j["created_at"] else None,
        } for j in журнал],
        "чего_нет": пробелы,
        "рассуждение": _рассказ(c, новость, аномалия, след, журнал),
    }


# ── «Как рассуждал агент» — путь кандидата простыми фразами
#
# ⚠️ ЭТО НЕ ЛОГ, А РАССКАЗ. Сырой след («context ROSN days=14 · нашлось 37 · 112 мс»)
# читался как шум: непонятно, что спросили, что узнали и на что это повлияло. Здесь
# каждый шаг — заголовок и две-три фразы человеческим языком; уровни доверия A/B/C/D
# переведены в слова («надёжно», «наша разметка», «только подсказка»). Числа берутся из
# params следа (структура), а не из разбора текста заметки; для старых строк без
# структуры показывается заметка как есть.

def _д(iso) -> str:
    """'2026-09-01T16:12:05+00:00' → '01.09 16:12'."""
    if not iso:
        return ""
    s = str(iso)
    try:
        return f"{s[8:10]}.{s[5:7]} {s[11:16]}".strip()
    except Exception:  # noqa: BLE001
        return s[:16]


def _перечень(xs, n=3) -> str:
    xs = [x for x in xs if x]
    if not xs:
        return ""
    return ", ".join(xs[:n]) + (f" и ещё {len(xs) - n}" if len(xs) > n else "")


def _мозг_шаги(след) -> list[dict]:
    """Подсказка Шагу А и «что мозг знал о компании» — из строк шага «мозг»."""
    import json as _json
    шаги = []
    подсказки = [t for t in след if t["step"] == "мозг" and (t["question"] or "").startswith("подсказка")]
    контексты = [t for t in след if t["step"] == "мозг" and (t["question"] or "").startswith("context ")]
    поиски = [t for t in след if t["step"] == "мозг" and (t["question"] or "").startswith("search")]

    def п(t):
        x = t["params"]
        if isinstance(x, str):
            try:
                x = _json.loads(x)
            except Exception:  # noqa: BLE001
                x = {}
        return x or {}

    if подсказки:
        t = подсказки[-1]
        x = п(t)
        строки = []
        for тк in x.get("по_хэштегу") or []:
            строки.append(f"Автор поставил хэштег #{тк} — это тикер из нашего справочника. Надёжный признак.")
        по_имени = x.get("по_имени") or []
        if по_имени and not isinstance(по_имени[0], dict):
            # старый формат следа (до 06.09): имён в params нет — берём заметку как есть
            строки.append("Подсказка мозга: " + (t["result_note"] or ""))
            по_имени = []
        for h in по_имени:
            строки.append(f"В самом тексте названа компания «{h['имя']}» ({h['тикер']}). Это наша разметка по именам, она права примерно в девяти случаях из десяти.")
        for m in x.get("по_смыслу") or []:
            строки.append(f"Имён наших компаний в тексте нет, но по смыслу новость похожа на «{m['имя']}» ({m['тикер']}, {m['сходство']}). Это только подсказка: тикер по ней не ставится.")
        if not строки:
            строки.append(t["result_note"] or "Мозг не нашёл в тексте ни одной нашей компании и ничего похожего по смыслу.")
        шаги.append({"код": "мозг_подсказка", "заголовок": "Второй мозг прочитал новость",
                     "строки": строки, "тон": "ok" if x.get("по_хэштегу") or x.get("по_имени") else "neutral"})
    elif поиски:
        t = поиски[-1]
        шаги.append({"код": "мозг_подсказка", "заголовок": "Второй мозг прочитал новость",
                     "строки": [("Имён наших компаний в тексте нет; по смыслу похоже на: " + t["result_note"]) if t["result_note"]
                                else "Имён наших компаний в тексте нет, и по смыслу ничего близкого не нашлось."],
                     "тон": "neutral"})

    # что знал о компании — последний контекст на каждый тикер
    по_тикеру = {}
    for t in контексты:
        по_тикеру[п(t).get("ticker") or t["question"].split()[1]] = t
    for тк, t in по_тикеру.items():
        x = п(t).get("структура")
        if not x:
            шаги.append({"код": "мозг_знал", "заголовок": f"Что мозг знал о {тк}",
                         "строки": [t["result_note"] or "итог не сохранён"], "тон": "neutral"})
            continue
        строки = []
        имя = x.get("компания") or тк
        if x.get("сектор"):
            строки.append(f"{имя}: сектор «{x['сектор']}»" + (" (сектор задан нами, у smart-lab другой)." if x.get("сектор_наш") else "."))
        вл = x.get("владельцы") or []
        if вл:
            части = []
            for в in вл[:3]:
                доля = f" {в['доля']:g}%" if в.get("доля") is not None else ""
                части.append(f"{в['имя']}{доля}")
            дата = next((в.get("на_дату") for в in вл if в.get("на_дату")), None)
            старый = дата and str(дата)[:4].isdigit() and int(str(дата)[:4]) <= 2024
            строки.append(f"Крупные акционеры по последнему снимку: {', '.join(части)}"
                          + (f" (снимок на {str(дата)[:7]}" + (", старше года — в тексте только «по данным на»" if старый else "") + ")." if дата else "."))
        if x.get("владеет"):
            строки.append(f"Сама владеет долями в: {_перечень(x['владеет'], 4)}.")
        if x.get("фондов"):
            строки.append(f"Бумагу держат {x['фондов']} фондов" + (f", крупнейшие доли у: {_перечень(x.get('фонды_топ') or [], 3)}" if x.get("фонды_топ") else "") + ".")
        if x.get("индексы"):
            строки.append(f"Входит в индексы: {_перечень(x['индексы'], 4)}.")
        н = x.get("новостей", 0); к = x.get("кандидатов_60", 0); а = x.get("аномалий_60", 0)
        строки.append(f"За {x.get('дней', 14)} дней о компании {н} новостей, за 60 дней {к} кандидатов в посты"
                      + (f" и {а} аномалий позиций по нашему детектору ({_перечень(x.get('аномалии') or [], 2)})." if а else ", аномалий позиций не было."))
        if x.get("рядом"):
            строки.append(f"Часто упоминается рядом с: {_перечень(x['рядом'], 4)}. Это соседство в новостях, не связь.")
        шаги.append({"код": "мозг_знал", "заголовок": f"Что мозг знал о {имя}", "строки": строки, "тон": "neutral"})
    return шаги


def _рассказ(c, новость, аномалия, след, журнал) -> list[dict]:
    шаги = []
    # 1. новость / раскрытие
    if c["source"] == "fm_disclosure":
        шаги.append({"код": "новость", "заголовок": "Пришло раскрытие эмитента",
                     "строки": ["Лента раскрытия FinanceMarker (страница сайта, не API): заголовок и текст сообщения "
                                "легли в кандидата напрямую. Хайп-фильтр для раскрытия не нужен — это не пост в канале."],
                     "тон": "neutral"})
    elif новость:
        теги = " ".join("#" + str(h).lstrip("#") for h in (новость["hashtags"] or [])[:6])
        просмотров = (", " + f"{int(новость['views']):,}".replace(",", " ") + " просмотров") if новость["views"] is not None else ""
        шаги.append({"код": "новость", "заголовок": "Пришла новость",
                     "строки": [f"Канал {новость['channel']}, {_д(новость['posted_at'])}{просмотров}."
                                + (f" Хэштеги автора: {теги}." if теги else " Хэштегов у автора нет.")],
                     "тон": "neutral"})
    # 2. хайп-фильтр
    if c["hype_filter_result"] is not None or c["hype_filter_gave_up_at"]:
        if c["hype_filter_result"] is None:
            строка = "Хайп-фильтр не дождался данных о репостах и отпустил новость дальше без оценки."
            тон = "warn"
        elif c["hype_filter_result"]:
            строка = "Хайп-фильтр решил: это новость, а не шутка или спам. Пропущена дальше."
            тон = "ok"
        else:
            строка = "Хайп-фильтр решил: это не новость (шутка, реклама или пересказ). Дальше не пошла."
            тон = "bad"
        шаги.append({"код": "хайп", "заголовок": "Хайп-фильтр (Шаг Н)", "строки": [строка], "тон": тон})
    # 3. мозг
    мозг = _мозг_шаги(след)
    if мозг:
        шаги.extend(мозг)
    elif c["reasoning"]:
        шаги.append({"код": "мозг_нет", "заголовок": "Второй мозг",
                     "строки": ["К этому кандидату мозг не подключался: он прошёл судью релевантности до 05.09.2026, когда мозг появился в конвейере."],
                     "тон": "neutral"})
    # 4. судья релевантности
    if c["reasoning"]:
        строки = []
        тикеры = list(c["tickers"] or [])
        если_событие = f", тип события «{c['event_type']}»" if c["event_type"] else ""
        строки.append((f"Тикер: {', '.join(тикеры)}" if тикеры else "Тикер не назначен") + если_событие
                      + (f", важность {c['importance_1_5']} из 5." if c["importance_1_5"] is not None else "."))
        строки.append(f"Обоснование: {c['reasoning']}")
        отсеян = c["status"] == "discarded"
        строки.append("Решение: отсеять — писать пост не о чем." if отсеян else "Решение: пропустить дальше, к данным.")
        шаги.append({"код": "шаг_а", "заголовок": "Судья релевантности (Шаг А)", "строки": строки, "тон": "bad" if отсеян else "ok"})
    # 5. данные
    if аномалия:
        шаги.append({"код": "шаг_б", "заголовок": "Данные нашлись (Шаг Б)",
                     "строки": [f"{аномалия['headline']} — {аномалия['context']}, на {аномалия['signal_date']}."], "тон": "ok"})
    elif c["step_b_checked_at"] and c["status"] == "no_data":
        шаги.append({"код": "шаг_б", "заголовок": "Данных не нашлось (Шаг Б)",
                     "строки": ["Ни аномалии позиций, ни движения фондов, которыми можно подтвердить новость. Пост без данных не пишем."], "тон": "warn"})
    # 6. писатель
    бриф = [t for t in след if t["step"] == "бриф"]
    if c["draft_text"] or c["synth_declined_reason"] or бриф:
        строки = []
        if бриф:
            взято = [t for t in бриф if t["outcome"] == "взято"]
            пусто = [t for t in бриф if t["outcome"] != "взято"]
            строки.append(f"Бриф собрал данные из {len(бриф)} источников: взято {len(взято)}, пусто или отброшено {len(пусто)}."
                          + (f" Пустыми оказались: {_перечень(list(dict.fromkeys(t['source'] for t in пусто)), 4)}." if пусто else ""))
        if c["synth_declined_reason"]:
            строки.append(f"Писатель отказался писать: {c['synth_declined_reason']}")
            тон = "warn"
        elif c["draft_text"]:
            строки.append(f"Писатель написал черновик на {len(c['draft_text'])} знаков — он ниже.")
            тон = "ok"
        else:
            тон = "neutral"
        шаги.append({"код": "шаг_в", "заголовок": "Писатель (Шаг В)", "строки": строки, "тон": тон})
    # 7. судья черновика
    if c["judge_verdict"]:
        строки = [f"Вердикт: {c['judge_verdict']}."]
        if c["judge_failed"]:
            строки.append(f"Провалены проверки: {', '.join(c['judge_failed'])}.")
        if c["judge_note"]:
            строки.append(c["judge_note"])
        if c["judge_fix_note"]:
            строки.append(f"Судья поправил текст: {c['judge_fix_note']}")
        шаги.append({"код": "шаг_г", "заголовок": "Судья черновика (Шаг Г)", "строки": строки,
                     "тон": {"годится": "ok", "спорно": "warn", "брак": "bad"}.get(c["judge_verdict"], "neutral")})
    # 8. человек
    if c["reviewer_action"]:
        одобрил = c["reviewer_action"] == "approved"
        шаги.append({"код": "человек", "заголовок": "Решение человека",
                     "строки": [("Одобрено." if одобрил else "Отклонено.") + (f" Причина: {c['reviewer_reason']}" if c["reviewer_reason"] else "")],
                     "тон": "ok" if одобрил else "bad"})
    # итог
    шаги.append({"код": "итог", "заголовок": "Где путь закончился",
                 "строки": [ПОДПИСЬ_СТАТУСА.get(c["status"], c["status"]).capitalize() + "."],
                 "тон": "ok" if c["status"] == "published" else "neutral"})
    return шаги
