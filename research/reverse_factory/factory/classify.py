"""Стадия 2. Тип стратегии (архетип) + «продаёт/покупает страховку» + бот или человек.

Каталог архетипов и их подписи — KNOWLEDGE.md. Здесь правила; при расхождении
с описанием самого трейдера — править пороги здесь и записывать в KNOWLEDGE.md.
"""
from __future__ import annotations

import re


CLAIMS = [  # что трейдер пишет о себе → какие типы завода этому соответствуют
    ("докупка/сетка", r"\bdca\b|grid|сетк|martingale|мартингейл|averag|усредн|accumulat|накоп",
     {"докупка/мартингейл", "сетка", "долгая докупка (накопление)", "возврат к среднему"}),
    ("контртренд", r"counter.?trend|контр.?тренд|mean.?revers|возврат к средн|reversal|разворот|ловл|catch|knife|нож",
     {"контртренд (вход против движения)", "возврат к среднему", "докупка/мартингейл", "сетка", "скальпер", "сессионный скальпер"}),
    # «тренд», но не «контртренд»: у Syndicate/FlowAI/Knife Catcher слово «контртренд» засчитывалось как «тренд» (26.09)
    ("тренд", r"(?<!контр)(?<!контр-)тренд|(?<!counter)(?<!counter-)(?<!counter )trend|momentum|моментум|breakout|пробо",
     {"тренд", "тренд (вход по ходу движения)", "сканер с фиксированными тейком и стопом", "фиксированные тейк и стоп", "долгосрочный держатель"}),
    ("скальпинг", r"scalp|скальп|\bhft\b|high.?frequency", {"скальпер", "сессионный скальпер"}),
]


def self_check(meta: dict, archetype: str) -> dict | None:
    """Сверка найденного типа с самоописанием — счётчик ошибок классификатора (колонка registry)."""
    text = " ".join(str(meta.get(k) or "") for k in ["description", "tags", "name"])
    claims = [(name, ok) for name, rx, ok in CLAIMS if re.search(rx, text, re.I)]
    if not claims:
        return None
    match = any(archetype in ok for _, ok in claims)
    return dict(claimed=", ".join(n for n, _ in claims), match=match)


def run(fp: dict, meta: dict) -> dict:
    b, t, bt, ex, av, lv = (fp[k] for k in ["basics", "timing", "batching", "exits", "averaging", "levels"])
    win, payoff = b["win"], b.get("payoff")
    hold50 = (b.get("hold_h") or {}).get("p50") or 0
    reasons, arch = [], None

    grid = (lv or {}).get("grid")
    avg_evidence = (av["multi_order_share"] >= 0.2 and av.get("add_dir") in ("усреднение вниз", None)) \
        or (av.get("cost_mult_max") or 0) >= 4
    if grid and (lv.get("repeat_price_share") or 0) >= 0.3:
        arch = "сетка"
        reasons.append(f"заявки на повторяющихся уровнях {grid['sym']} с шагом {grid['step']} (~{grid['step_pct']}%)")
    # при явной доливке (половина позиций из нескольких ордеров) порог побед мягче: Papai 6/7 = 86% (калибровка 26.09)
    elif (win >= 0.88 or (win >= 0.8 and av["multi_order_share"] >= 0.5)) and (avg_evidence or ex.get("fixed_tp")) and not ex.get("fixed_sl"):
        arch = "докупка/мартингейл"
        if av.get("size_ratio"):
            reasons.append(f"доливки с множителем ×{av['size_ratio']}")
        if av.get("cost_mult_max"):
            reasons.append(f"объём позиции растёт до ×{av['cost_mult_max']} от базового")
        if ex.get("fixed_tp"):
            tps = ", ".join(f"+{m['level']}% ({int(100 * m['share'])}%)" for m in ex["tp_modes"] if m["share"] >= 0.08)
            reasons.append(f"выход на фиксированном проценте от средней: {tps} прибыльных")
        reasons.append(f"{int(100 * win)}% сделок в плюс, стопа нет")
    elif ex.get("fixed_tp") and ex.get("fixed_sl"):
        arch = "сканер с фиксированными тейком и стопом" if bt["batch_share"] >= 0.25 else "фиксированные тейк и стоп"
        reasons.append(f"тейк +{ex['fixed_tp']['level']}% / стоп {ex['fixed_sl']['level']}%")
        if bt["batch_share"] >= 0.25:
            reasons.append(f"{int(100 * bt['batch_share'])}% входов пачками по разным монетам")
    elif payoff and win <= 0.45 and payoff >= 1.8:
        arch = "тренд"
        reasons.append(f"в плюс {int(100 * win)}% сделок, средний плюс в {payoff} раза больше минуса")
    elif hold50 < 1 and b["per_week"] > 30:
        arch = "сессионный скальпер" if t["top4_hours_share"] >= 0.6 else "скальпер"
        reasons.append(f"сделка держится ~{hold50} ч, {b['per_week']} сделок в неделю")
    elif hold50 > 24 * 14 and av["multi_order_share"] >= 0.5 and av.get("add_dir") == "усреднение вниз":
        arch = "долгая докупка (накопление)"      # Papai: держит месяцами, доливается вниз, общий выход (26.09)
        reasons.append(f"держит ~{round(hold50 / 24)} дней, {int(100 * av['multi_order_share'])}% позиций набраны частями с доливкой вниз")
    elif hold50 > 24 * 14:
        arch = "долгосрочный держатель"
        reasons.append(f"держит в среднем {round(hold50 / 24)} дней")
    elif payoff and win >= 0.6 and payoff < 1:
        arch = "возврат к среднему"
        reasons.append(f"часто в плюс ({int(100 * win)}%), но плюс меньше минуса")
    else:
        arch = "смешанный"
    ctx = fp.get("context") or {}
    ws = ctx.get("with_share")
    if ws is not None:
        reasons.append(f"контекст входа: {ctx.get('verdict')} ({int(100 * ws)}% входов по ходу 4–24 ч движения)")
        if arch in ("смешанный", "возврат к среднему") and ws <= 0.38:
            arch = "контртренд (вход против движения)"
        elif arch == "смешанный" and ws >= 0.62:
            arch = "тренд (вход по ходу движения)"
    if meta.get("netted_suspect"):
        reasons.append("⚠️ по описанию несколько стратегий в одном счёте — позиции неттируются, сделки = изменения суммарной позиции")

    if arch in ("докупка/мартингейл", "сетка", "долгая докупка (накопление)") or (win >= 0.75 and payoff is not None and payoff < 1):
        insurance = "продаёт страховку"
    elif (win <= 0.45 and (payoff or 0) >= 1.5) or (win <= 0.6 and (payoff or 0) >= 2):
        insurance = "покупает страховку"
    else:
        insurance = "нейтрально"

    bot, human = [], []
    if t.get("grid_tf_min"):
        bot.append(f"входы на закрытии свечи {t['grid_tf_min']} мин ({int(100 * t['grid']['share'])}%)")
    if (t.get("sec_le10_share") or 0) >= 0.6:
        bot.append(f"{int(100 * t['sec_le10_share'])}% входов в первые 10 секунд минуты")
    if bt["batch_share"] >= 0.25:
        bot.append("пачки входов по разным монетам")
    if ex.get("fixed_tp") or ex.get("fixed_sl"):
        bot.append("фиксированные уровни выхода")
    if meta.get("bybit_is_bot"):
        bot.append("Bybit помечает как бота")
    if meta.get("apps"):
        bot.append(f"подключён через {', '.join(meta['apps'])}")
    tags = " ".join(meta.get("tags") or []) if isinstance(meta.get("tags"), list) else str(meta.get("tags") or "")
    if re.search(r"алго|algo", tags, re.I):
        bot.append("площадка помечает как алгоритм")
    if re.search(r"\bbot\b|бот|robot|робот|algorithm|алгоритм|automat|автомат|webhook|вебхук|\bcode\b|backtest|бэктест", meta.get("description") or "", re.I):
        bot.append("по описанию — автоматическая торговля")
    if (lv.get("round_price_share") or 0) >= 0.3 and not grid:
        human.append(f"{int(100 * lv['round_price_share'])}% цен круглые — лимитки руками")
    # отсутствие сетки свечей — НЕ улика (боты по тикам тоже без сетки; у invvo время в минутах): Quant Hill 26.09
    who = ("бот" if bot and not human else "человек" if human and not bot else
           "скорее бот" if len(bot) > len(human) else "скорее человек" if human else "не ясно, бот или человек")
    return dict(archetype=arch, insurance=insurance, reasons=reasons, who=who, bot_signs=bot, human_signs=human,
                self_check=self_check(meta, arch))
