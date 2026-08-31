#!/usr/bin/env python3
"""Бриф Шага В версии 2 — с числами, которые можно высказать честно.

ЗАЧЕМ. Старый бриф отдавал ATR-множитель («multiplier: x4.69»), означающий
«дневное изменение в 4,69 раза крупнее обычного дневного». Канал же оперирует
отношением «было → стало» за период («лонг вырос в 5 раз»). Это РАЗНЫЕ величины, и
из-за подмены в двух опубликованных постах оказались фактические ошибки:
793 написал «позиция выросла в 3,53 раза за один день», хотя за день она выросла на
37%, а в 5,2 раза — за месяц; 748 читается как «сократили втрое» при реальных 19%.
Подробно — METRIC_MISMATCH.md.

Что делает этот модуль: считает из ряда позиций те величины, которыми канал реально
пользуется, и НЕ пускает ATR-множитель в текстовую часть брифа. Множитель остаётся
внутренним — детектору он нужен, чтобы понять, что движение аномально; тексту нет.

Плюс два поля, которых не было вовсе:
  • рамка сюжета (упреждение / реакция / совпадение) — из порядка дат. Её отсутствие
    дало черновик 845 с развёрнутой стрелкой времени;
  • смена знака позиции — сюжет сам по себе («физики развернулись из лонга в шорт»),
    который старый бриф терял целиком.
"""
import argparse
import json
import os
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal


def _ru(x, digits=1) -> str:
    """Русская типографика: запятая, без хвостовых нулей."""
    d = Decimal(str(round(float(x), digits))).normalize()
    return format(d, "f").replace(".", ",")


def _pct(new, old) -> str:
    if not old:
        return "нет базы для сравнения"
        # деление на ноль тут не «ошибка», а честный ответ: позиции не было
    return f"{'+' if new >= old else ''}{_ru((new - old) / abs(old) * 100)}%"


def _ratio(new, old):
    """Отношение «было → стало» ПО МОДУЛЮ. Знак несёт отдельное поле: «вырос в 5 раз»
    про шорт и про лонг звучит одинаково, а направление читатель узнаёт из слова."""
    if not old:
        return None
    return abs(new) / abs(old)


def load_series(path: str) -> dict:
    """series.tsv: id|asset|clgroup|signal_date|direction|severity|news_date|name|
    tradedate|net|pos_long|pos_short"""
    by_id = defaultdict(lambda: {"meta": None, "rows": []})
    with open(path, encoding="utf-8") as f:
        for line in f:
            p = line.rstrip("\n").split("|")
            if len(p) < 12:
                continue
            cid = int(p[0])
            if by_id[cid]["meta"] is None:
                by_id[cid]["meta"] = {
                    "asset_id": p[1], "clgroup": p[2],
                    "signal_date": date.fromisoformat(p[3]), "direction": p[4],
                    "severity": float(p[5]), "news_date": date.fromisoformat(p[6]),
                    "asset_name": p[7],
                }
            by_id[cid]["rows"].append((date.fromisoformat(p[8]), int(p[9]),
                                        int(p[10]), int(p[11])))
    return by_id


def frame_of(signal_date: date, news_date: date) -> str:
    """Рамка сюжета из порядка дат. Ровно это поле отсутствовало в старом брифе, и
    модель разворачивала стрелку времени (черновик 845: «толпа шла в шорт ещё до
    ралли» при сигнале на три дня ПОЗЖЕ новости)."""
    d = (signal_date - news_date).days
    if d < 0:
        return (f"УПРЕЖДЕНИЕ: позиции менялись за {abs(d)} дн. ДО новости. "
                f"Только в этой рамке можно говорить, что толпа встала заранее.")
    if d == 0:
        return ("СОВПАДЕНИЕ: позиции и новость в один день. Утверждать, что толпа "
                "встала ЗАРАНЕЕ, нельзя — данных на это нет.")
    return (f"РЕАКЦИЯ: позиции менялись на {d} дн. ПОЗЖЕ новости. Это отклик на "
            f"событие, а не предвидение. Фразы «знали заранее», «встали до» — "
            f"запрещены как факт.")


def _delta(new, old) -> float:
    """Сила относительного движения — по ней бриф решает, какой период главный.
    Через смену знака берём сумму модулей: разворот всегда сильнее любого роста
    внутри одного знака, и это правда — разворот и есть событие."""
    if not old:
        return 0.0
    if (old > 0) != (new > 0):
        return (abs(new) + abs(old)) / abs(old)
    return abs(abs(new) - abs(old)) / abs(old)


def _phrase(new, old, direction_word: str) -> str:
    """Изменение позиции ЧЕЛОВЕЧЕСКОЙ фразой, а не голым процентом.

    ⚠️ Голый процент по отрицательной позиции читается неверно. Живой случай: VK,
    чистый шорт −7 231 → −33 981 давало «−369,9%», и модель написала «позиция
    изменилась на −369,9%». Человек прочтёт это как «упало на 370%», чего быть не
    может. Канал в такой ситуации говорит «чистый шорт вырос в 4,7 раза» — то есть
    описывает рост САМОЙ ПОЗИЦИИ в её собственном направлении. Та же логика, что и
    с ATR-множителем: число надо отдавать в той форме, в какой его можно произнести.
    """
    if not old:
        return "не с чем сравнить — позиции не было"
    if (old > 0) != (new > 0):
        return (f"развернулась: было {'лонг' if old > 0 else 'шорт'} {abs(old)}, "
                f"стало {'лонг' if new > 0 else 'шорт'} {abs(new)}")
    grew = abs(new) > abs(old)
    r = abs(new) / abs(old)
    verb = "вырос" if grew else "сократился"
    if grew and r >= 1.5:
        return f"чистый {direction_word} {verb} в {_ru(r, 2)} раза"
    change = abs(abs(new) - abs(old)) / abs(old) * 100
    return f"чистый {direction_word} {verb} на {_ru(change)}%"


def compute(meta: dict, rows: list) -> dict:
    """Величины, которыми пользуется канал, а не детектор."""
    rows = sorted(rows)
    if len(rows) < 2:
        return {"ошибка": "слишком короткий ряд позиций"}
    dates = [r[0] for r in rows]
    net = {r[0]: r[1] for r in rows}
    last_d = dates[-1]
    last = net[last_d]
    prev = net[dates[-2]]
    word = "лонг" if last > 0 else "шорт"

    # ⚠️ ГЛАВНОЕ ЧИСЛО ВЫБИРАЕТ БРИФ, А НЕ МОДЕЛЬ. Замер после первой версии брифа:
    # плотность осталась 5 показателей на абзац против 1-2 у канала — потому что
    # бриф выкладывал шесть равноправных чисел, и модель добросовестно брала все.
    # Это тот же провал, что с промптом: «дай всё и надейся, что выберет» не
    # работает. Поэтому одно число помечено главным, остальные — фоном, который
    # упоминать НЕ обязательно.
    periods = {}
    for days, label in ((1, "за_сутки"), (7, "за_неделю"), (30, "за_месяц")):
        if days == 1:
            periods[label] = (_phrase(last, prev, word), abs(_delta(last, prev)))
            continue
        target = last_d - timedelta(days=days)
        base_d = min((d for d in dates if d >= target), default=None)
        if base_d is None or base_d == last_d:
            continue
        periods[label] = (_phrase(last, net[base_d], word),
                          abs(_delta(last, net[base_d])))

    # Главное — период с самым сильным относительным движением: именно он и есть
    # новость про позиции.
    lead = max(periods, key=lambda k: periods[k][1]) if periods else None
    out = {
        "направление_позиции": f"чистый {word.upper()}",
        "ГЛАВНОЕ_ЧИСЛО": f"{lead}: {periods[lead][0]}" if lead else "нет",
        "фон_упоминать_не_обязательно": {
            k: v[0] for k, v in periods.items() if k != lead},
        "чистая_позиция_контрактов": abs(last),
    }

    pcts = []
    for d, n, pl, ps in rows:
        gross = (pl or 0) - (ps or 0)
        pcts.append((n / gross * 100) if gross else 0.0)
    out["перекос_net_gross"] = f"{_ru(pcts[-1])}%"
    # ⚠️ Окно указано В НАЗВАНИИ поля. Прежнее «перекос_диапазон_за_ряд» модель
    # прочитала как «за всё время наблюдений» и написала «максимум за всё время» —
    # хотя ряд тут всего несколько недель. Название поля это тоже интерфейс.
    span = (dates[-1] - dates[0]).days
    out[f"перекос_диапазон_только_за_{span}_дней"] = (
        f"от {_ru(min(pcts))}% до {_ru(max(pcts))}% — это НЕ исторический экстремум, "
        f"а лишь окно в {span} дн.")
    return out


def build(cid: int, meta: dict, rows: list, news: dict | None = None) -> dict:
    numbers = compute(meta, rows)
    return {
        "candidate_id": cid,
        "инструмент": f"{meta['asset_id']} ({meta['asset_name']})",
        "дата_новости": str(meta["news_date"]),
        "дата_сигнала": str(meta["signal_date"]),
        "рамка_сюжета": frame_of(meta["signal_date"], meta["news_date"]),
        "позиции_физлиц": numbers,
        "новость": news or {},
        "служебное": {
            "atr_множитель": meta["severity"],
            "пояснение": ("ВНУТРЕННЕЕ. Это отношение дневного изменения к обычному "
                          "дневному, а НЕ рост позиции. В текст поста не выносить: "
                          "читатель поймёт «вырос в N раз», и это будет неправдой. "
                          "Служит только признаком того, что движение аномально."),
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True)
    ap.add_argument("--ids", help="через запятую; по умолчанию все")
    a = ap.parse_args()
    data = load_series(a.series)
    ids = [int(x) for x in a.ids.split(",")] if a.ids else sorted(data)
    for cid in ids:
        d = data.get(cid)
        if not d or not d["meta"]:
            print(f"# {cid}: нет данных"); continue
        print(json.dumps(build(cid, d["meta"], d["rows"]), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
