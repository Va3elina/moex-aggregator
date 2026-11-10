"""Сигналы «Денег в фондах» из @framesignalbot → поводы для завода постов (Вадим 10.11.2026: «чтобы поводы появлялись
по тем сигналам, что присылаются в бот сигналов по деньгам в фондах»).

Детектор находок (signals/insights/detect.py) видит в фондах только крайности по пятидневным и двадцатидневным
суммам: в спокойные недели находок нет, и за 2–9 ноября завод не написал по фондам ничего, хотя бот присылал рекорды
и серии. Здесь каждый ОТПРАВЛЕННЫЙ сигнал бота — рекорд потока (signals/funds_flow_notify.py) или серия и разворот
(signals/funds_flow_streak.py) — становится кандидатом content_candidates тем же путём, что находки insight_scan:
карточка категории (cards.funds_card) + факт сигнала первым пунктом, писатель Шага В, проверка insight_check, бот ревью.

Срез сигнала бывает подкатегорией («Облигации · Короткие»), а карточка считается по категории целиком: цифры среза
идут фактами в начало карточки, остальное — фон категории.

Фильтры:
  • один повод на категорию за прогон — самый сильный сигнал (рекорд старше окна, потом старший горизонт);
  • канал писал о той же категории за SKIP_DAYS дня — пропуск (insight_scan.repeat_of), раньше — «продолжение темы»;
  • тикер кандидата — «funds:<категория>», как у находок детектора: правило «один тикер — один пост за три дня»
    (content_ai._repeat_of_ticker) разводит поводы бота и детектора между собой;
  • тот же заголовок за 14 дней — пропуск (insight_scan.save_job).
Сбой здесь не мешает боту: сигнал уже отправлен, ошибка печатается в лог.
"""
from datetime import date

import pandas as pd

TF_WORD = {"1d": "за день", "1w": "за неделю", "1m": "за месяц"}
TF_RANK = {"1d": 0, "1w": 1, "1m": 2}
SIDE_WORD = {"in": "приток", "out": "отток"}
SIDE_GEN = {"in": "притока", "out": "оттока"}


def _bn(v_bln: float) -> str:
    from signals.insights import cards
    return f"{cards.n_ru(abs(v_bln) * 1e9)} ₽"


def _bar_when(tf: str, start: str | None, end: str, ref) -> str:
    from signals.insights import cards
    if tf == "1m":
        return cards.mn_ru(pd.Timestamp(end), ref)
    if tf == "1w" and start:
        return f"неделя {cards.d_ru(start, ref)} - {cards.d_ru(end, ref)}"
    return cards.d_ru(end, ref)


def lead_record(x: dict) -> dict:
    """Рекорд потока (funds_flow_notify.records): заголовок и факты среза."""
    side, tf, ref = x["side"], x["tf"], pd.Timestamp(x["day"])
    head = f"{x['name']}: {SIDE_WORD[side]} {TF_WORD[tf]} - рекорд {x['window']}"
    facts = [f"{SIDE_WORD[side]} {TF_WORD[tf]} в срезе «{x['name']}» - {_bn(x['flow'])}, "
             f"больше не было {x['window']} ({_bar_when(tf, x.get('start'), str(x['day']), ref)})"]
    if x.get("prev_day"):
        facts.append(f"прошлый раз {SIDE_WORD[side]} {TF_WORD[tf]} был больше - "
                     f"{_bar_when(tf, x.get('prev_start'), x['prev_day'], ref)}: {_bn(x['prev_flow'])}")
    return {"headline": head, "facts": facts, "rank": 10 + _WINDOW_RANK(x["window"])}


def lead_streak(x: dict) -> dict:
    """Серия или разворот (funds_flow_streak.pending): заголовок и факты среза."""
    from signals.funds_flow_streak import span, span_gen
    side, tf, n, ref = x["side"], x["tf"], x["n"], pd.Timestamp(x["day"])
    if x["kind"] == "break":
        opp = "out" if side == "in" else "in"
        head = f"{x['name']}: {SIDE_WORD[side]} после {span_gen(n, tf)} {SIDE_GEN[opp]}"
        facts = [f"в срезе «{x['name']}» {SIDE_WORD[side]} {TF_WORD[tf]} - {_bn(x['flow'])} "
                 f"({_bar_when(tf, x.get('start'), str(x['day']), ref)}) после {span_gen(n, tf)} {SIDE_GEN[opp]} подряд"]
        if x.get("run_sum") is not None:
            facts.append(f"за те {span_gen(n, tf)} {SIDE_GEN[opp]} всего - {_bn(x['run_sum'])}")
        rank = 5
    else:
        head = f"{x['name']}: {SIDE_WORD[side]} идёт {span(n, tf)} подряд"
        facts = [f"в срезе «{x['name']}» {SIDE_WORD[side]} идёт {span(n, tf)} подряд, последний "
                 f"столбец ({_bar_when(tf, x.get('start'), str(x['day']), ref)}) - {_bn(x['flow'])}"]
        if x.get("run_sum") is not None:
            facts.append(f"за всю серию {SIDE_WORD[side]} - {_bn(x['run_sum'])}")
        if x.get("record"):
            head += f", самая долгая серия с {x['since']} года"
            facts.append(f"это самая долгая серия {SIDE_GEN[side]} на графике с {x['since']} года")
        rank = 8 if x.get("record") else 4
    return {"headline": head, "facts": facts, "rank": rank}


def _WINDOW_RANK(w: str) -> int:  # noqa: N802 — окно рекорда: старше — сильнее
    from signals.record_notify import RANK
    return RANK.get(w, 0)


MIN_RUB, MIN_FRAC = 3e9, 0.03       # порог insight_scan.fund_significant: 3 млрд ₽ или 3% активов категории


def significant(x: dict) -> bool:
    """Бот шлёт серии без порога по величине (Вадим 10.11: «без порога СЧА»), а пост — нет: «приток в золото 10 дней
    подряд» на 642 млн ₽ из 63 млрд — 1% активов, не новость для читателя (холостой прогон 10.11). Поток столбца или
    сумма серии ≥ min(3 млрд ₽, 3% активов категории); рекорд за всё время и самая долгая серия — без порога."""
    if x.get("window") == "за всё время" or x.get("record"):
        return True
    from signals.insights import cards
    v = max(abs(x.get("flow") or 0), abs(x.get("run_sum") or 0)) * 1e9
    try:
        aum = float(cards.upto(cards.funds_data()[1][x["cat"]], pd.Timestamp(x["day"])).iloc[-1])
    except Exception:  # noqa: BLE001 — нет активов категории: порог в рублях
        aum = 0.0
    return v >= (min(MIN_RUB, MIN_FRAC * aum) if aum > 0 else MIN_RUB)


def submit(items: list, kind: str, dry_run: bool = False, log=print) -> dict:
    """Отправленные ботом сигналы → кандидаты завода. kind: «record» или «streak». Возвращает сводку."""
    summary = {"leads": 0, "created": 0, "skipped_repeat": 0, "skipped_exists": 0}
    if not items:
        return summary
    from api.database import SessionLocal
    from signals import insight_scan as ins

    best: dict = {}
    for x in items:
        lead = lead_record(x) if kind == "record" else lead_streak(x)
        if not significant(x):
            log(f"[funds_signal_leads] пропуск, мало для читателя: {lead['headline'][:90]}")
            summary["skipped_small"] = summary.get("skipped_small", 0) + 1
            continue
        key = (lead["rank"], TF_RANK[x["tf"]], x["sub"] is None)
        if x["cat"] not in best or key > best[x["cat"]][0]:
            best[x["cat"]] = (key, x, lead)
    now_iso = pd.Timestamp.now(tz="UTC").isoformat()
    db = SessionLocal()
    try:
        for n, (_, x, lead) in enumerate(best.values(), 1):
            summary["leads"] += 1
            day = min(pd.Timestamp(x["day"]), pd.Timestamp(date.today()))
            spec = {"kind": "funds", "cat": x["cat"], "leg": None}
            rep = ins.repeat_of(spec, day)
            if rep and (day - rep["date"]).days <= ins.SKIP_DAYS:
                log(f"[funds_signal_leads] пропуск, канал писал {rep['date']:%d.%m} «{rep['title'][:50]}»: "
                    f"{lead['headline'][:70]}")
                summary["skipped_repeat"] += 1
                continue
            if x["sub"]:
                lead["note"] = (f"сигнал - по срезу «{x['name']}», а остальная карточка и график - по категории "
                                f"целиком: цифры среза бери только из фактов сигнала, не смешивай их с категорией")
            job = {"kind": "funds", "spec": spec, "date": day, "title": lead["headline"], "repeat": rep,
                   "instrument": f"funds:{x['cat']}", "score": lead["rank"], "lead": lead,
                   "thread_key": f"insight:funds_signal:{x['cat']}|{x['sub'] or '*'}|{x['side']}|{x['tf']}"}
            b = ins.build(job, now_iso)
            ins.save_job(db, job, b, f"s{kind[0]}{n}", dry_run, summary, tag="funds_signal_leads")
    finally:
        db.close()
    return summary
