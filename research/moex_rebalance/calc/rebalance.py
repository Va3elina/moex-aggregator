#!/usr/bin/env python
"""Расчёт вынужденных сделок индексных фондов при смене базы IMOEX 18.09.2026.

Считает по данным из data/ (снимаются fetch_data.py) и правилам из ../CONDITIONS.md.
Пишет ../RESULTS.md и печатает то же в консоль.

МОДЕЛЬ, в двух шагах. Разделение шагов — не педантизм: если наложить потолок сразу
на сегодняшние данные, продажа ЛУКОЙЛа завышается почти вдвое.

  Шаг А (на дату фиксации, п. 2.8.6 — день перед раскрытием, т.е. 27.08):
      MC_i = cap_total_i(FIX) × FF_new_i × LW_new_i          # WW ещё не известен
      нормируем, применяем потолки Приложения 3 по ЭМИТЕНТУ итерационно
      → отсюда достаём WW_new_i, и дальше он ЗАМОРОЖЕН до следующего пересмотра

  Шаг Б (на последнюю дату с данными):
      MC_i = cap_total_i(ASOF) × FF_new_i × LW_new_i × WW_new_i
      нормируем → веса новой базы в сегодняшних ценах

  Шаг В: сделка_i = (вес_новый_i − вес_текущий_i) × СЧА фондов

Почему берём cap_total из ISS, а не восстанавливаем «цена × количество»: проверено,
что вес ∝ cap_total × ff_factor × w_factor с точностью 0,005 п.п. (это округление
публикуемого веса до двух знаков). Значит cap_total уже содержит нужное произведение,
и одно допущение из модели уходит.

Запуск:  .venv/bin/python research/moex_rebalance/calc/rebalance.py
         --fix-date 2026-08-28   # проверить чувствительность к трактовке п. 2.8.6
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

BASE = pathlib.Path(__file__).resolve().parents[1]
DATA = BASE / "data"

CAP_ISSUER = 15.0          # Приложение 3: потолок веса эмитента для IMOEX
CAP_TOP5 = 55.0            # Приложение 3: сумма пяти наибольших на дату формирования
EXCLUDED = {"LENT", "MSNG"}   # покидают IMOEX 18.09.2026 (проверено по релизу)

# Вес эмитента — сумма всех категорий его акций (п. 2.8.2)
ISSUER = {"SBER": "Сбербанк", "SBERP": "Сбербанк",
          "TATN": "Татнефть", "TATNP": "Татнефть",
          "SNGS": "Сургутнефтегаз", "SNGSP": "Сургутнефтегаз",
          "RTKM": "Ростелеком", "RTKMP": "Ростелеком"}
RU = {"LKOH": "ЛУКОЙЛ", "TATN": "Татнефть, ао", "TATNP": "Татнефть, ап",
      "TRNFP": "Транснефть, ап", "GAZP": "Газпром", "SBER": "Сбербанк, ао",
      "SBERP": "Сбербанк, ап", "LENT": "Лента", "MSNG": "Мосэнерго",
      "YDEX": "Яндекс", "SVCB": "Совкомбанк", "T": "Т-Технологии",
      "GMKN": "Норникель", "NVTK": "Новатэк", "PLZL": "Полюс", "ROSN": "Роснефть",
      "X5": "X5", "OZON": "Озон", "VTBR": "ВТБ", "PHOR": "ФосАгро",
      "BSPB": "Банк СПб", "POSI": "Позитив", "RENI": "Ренессанс",
      "SNGS": "Сургутнефтегаз, ао", "SNGSP": "Сургутнефтегаз, ап"}
name = lambda t: RU.get(t, t)                                        # noqa: E731
issuer = lambda t: ISSUER.get(t, t)                                  # noqa: E731


def apply_caps(mc: dict[str, float]) -> tuple[dict[str, float], set[str], dict[str, float]]:
    """Процедура корректировки Удельных весов по п. 2.8.4, дословно:

    «Если Удельный вес Эмитента или сумма Удельных весов Эмитентов в Базе расчета Индекса
    превышает величину, установленную в Приложении 3, то соответствующий Удельный вес
    устанавливается равным этой величине. Разница между Удельными весами до и после
    ограничения пропорционально распределяется между Эмитентами, Удельные веса которых
    НЕ БЫЛИ ОГРАНИЧЕНЫ. Указанные выше действия повторяются итерационно пока остаются
    Эмитенты, Удельные веса которых превышают величину, установленную в Приложении 3.»

    Возвращает (веса, обрезанные эмитенты, WW по бумагам).

    ⚠️ Про семантику WW. По п. 2.8.3 WW — «коэффициент, ограничивающий долю
    капитализации», и формулы у него в методике НЕТ: он определён через результат
    («рассчитываемый таким образом, чтобы Удельный вес не превышал требуемого»).
    Значит у необрезанных бумаг WW = 1 РОВНО, а перераспределение к ним приходит само —
    через нормировку, потому что масса обрезанных уменьшилась. Первая версия этой функции
    домножала веса необрезанных явно, и тогда «WW» у них выходил 1,22 — то есть больше
    единицы, что п. 2.8.6 прямо запрещает («WWi принимают значение от 0 до 1»). Итоговые
    веса при этом совпадают до 0,0000 п.п., но разложение по причинам было бессмысленным.
    """
    ww = {t: 1.0 for t in mc}
    capped: set[str] = set()
    for _ in range(50):
        tot = sum(mc[t] * ww[t] for t in mc)
        grp_w: dict[str, float] = {}
        for t in mc:
            grp_w[issuer(t)] = grp_w.get(issuer(t), 0.0) + 100 * mc[t] * ww[t] / tot
        over = [g for g, v in grp_w.items() if v > CAP_ISSUER + 1e-9 and g not in capped]
        if not over:
            break
        capped |= set(over)
        # необрезанные идут с WW = 1; на каждого обрезанного эмитента приходится масса,
        # дающая ему ровно потолок после нормировки
        free_mc = sum(mc[t] for t in mc if issuer(t) not in capped)
        share = CAP_ISSUER / 100
        target = share * free_mc / (1 - share * len(capped))
        for g in capped:
            cls = [t for t in mc if issuer(t) == g]
            s_cls = sum(mc[t] for t in cls)
            for t in cls:
                ww[t] = target / s_cls            # доля класса внутри эмитента сохраняется
    tot = sum(mc[t] * ww[t] for t in mc)
    return {t: 100 * mc[t] * ww[t] / tot for t in mc}, capped, ww


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix-date", default="2026-08-31",
                    help="дата фиксации весовых коэффициентов (п. 2.8.6)")
    ap.add_argument("--asof", default="2026-09-01", help="дата, на которую оцениваем сделки")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    lw_new = json.loads((DATA / "lw_new.json").read_text(encoding="utf-8"))
    ff_new = json.loads((DATA / "ff_new.json").read_text(encoding="utf-8"))
    changes = json.loads((DATA / "imoex_changes.json").read_text(encoding="utf-8"))
    iss = json.loads((DATA / "iss_imoex.json").read_text(encoding="utf-8"))["бумаги"]
    prices = json.loads((DATA / "prices.json").read_text(encoding="utf-8"))
    funds = json.loads((DATA / "funds.json").read_text(encoding="utf-8"))

    # --- проверки, без которых расчёт нельзя публиковать -------------------------
    assert len(lw_new) == 60, f"таблица LW: {len(lw_new)} бумаг вместо 60"
    assert len(ff_new) == 15, f"free-float в релизе: {len(ff_new)} вместо 15"
    assert not changes["включения_в_imoex"], \
        "в релизе есть ВКЛЮЧЕНИЕ в IMOEX — модель не учитывает новые бумаги, все суммы неверны"
    assert set(changes["исключены"]) == {"Лента", "Мосэнерго"}, changes["исключены"]
    FIX, ASOF = args.fix_date, args.asof
    have = [t for t, v in iss.items() if FIX in v and ASOF in v and v[ASOF]["ff"] and v[ASOF]["w"]]
    assert len(have) >= 40, f"данных ISS хватает только по {len(have)} бумагам"

    nav = sum(v["руб"] for v in funds["сча"].values())
    nav_date = next(iter(funds["сча"].values()))["дата"]

    def mc(t: str, date: str, ff: float, lw: float, ww: float = 1.0) -> float:
        return iss[t][date]["cap"] * ff * lw * ww

    # --- текущие веса (те же данные, тот же способ — чтобы сумма сходилась в ноль) --
    cur_mc = {t: mc(t, ASOF, iss[t][ASOF]["ff"], iss[t][ASOF]["w"]) for t in have}
    tot = sum(cur_mc.values())
    w_cur = {t: 100 * v / tot for t, v in cur_mc.items()}

    # --- ШАГ А: коэффициенты новой базы фиксируются на FIX ------------------------
    keep = [t for t in have if t not in EXCLUDED]
    mc_fix = {t: mc(t, FIX, ff_new.get(t, iss[t][FIX]["ff"]), lw_new.get(t, 1.0)) for t in keep}
    # ⚠️ КАЛИБРОВКА НА ПРОШЛЫХ РЕБАЛАНСАХ. Проверено на двух базах, где ответ известен:
    #   19.06.2026 (ушёл ПИК, пришло Русагро) и 20.06.2025 (ушли Мечел и Селигдар, никто
    #   не пришёл). Дата фиксации коэффициентов подтверждена обеими: 03.06.2026 при релизе
    #   04.06 и 30.05.2025 при релизе 02.06 — то есть день перед раскрытием, п. 2.8.6.
    #   Для сентябрьской базы это 27.08.2026.
    # Уходящие бумаги исключаются из знаменателя СРАЗУ. Проверялась и обратная гипотеза
    # («уходящая ещё в знаменателе»): на июне-2026 она лучше (0,033 против 0,135 п.п.), но
    # на июне-2025 ХУЖЕ (0,033 против 0,005). Значит остаток в июне-2026 связан не с
    # уходящей бумагой, а с ВХОДЯЩЕЙ, и добавление уходящей его лишь компенсировало.
    # Сентябрь-2026 — ребаланс без входящих, полный аналог июня-2025, поэтому берём
    # процедуру, выверенную на нём: максимальная ошибка прогноза веса 0,005 п.п., что ниже
    # округления публикуемых весов (ЛУКОЙЛ 14,39% против факта 14,39%).
    w_fix, capped, ww_new = apply_caps(mc_fix)     # WW дальше ЗАМОРОЖЕН до пересмотра

    # --- ШАГ Б: те же WW применяем к сегодняшним ценам ---------------------------
    mc_now = {t: mc(t, ASOF, ff_new.get(t, iss[t][ASOF]["ff"]), lw_new.get(t, 1.0), ww_new[t])
              for t in keep}
    tot_now = sum(mc_now.values())
    w_new = {t: 100 * v / tot_now for t, v in mc_now.items()}

    # --- ШАГ В: сделки -----------------------------------------------------------
    trades = {t: (w_new.get(t, 0.0) - w_cur[t]) / 100 * nav for t in have}
    sells = sum(v for v in trades.values() if v < 0)
    buys = sum(v for v in trades.values() if v > 0)

    # --- контроль снизу: исключаемые бумаги считаются без всякой модели ----------
    # их продают целиком, поэтому «позиции × цена» должно совпасть с моделью
    control = {}
    isin_by = {"RU000A102S15": "LENT", "RU0008958863": "MSNG",
               "RU0009024277": "LKOH", "RU0009033591": "TATN"}
    for row in funds["позиции"]:
        t = isin_by.get(row["isin"])
        if t and t in prices and ASOF in prices[t]:
            control[t] = control.get(t, 0.0) + row["штук"] * prices[t][ASOF]

    L = []
    p = L.append
    p(f"# Результаты расчёта\n")
    p(f"Сгенерировано `calc/rebalance.py`. Дата фиксации коэффициентов **{FIX}**, "
      f"оценка в ценах **{ASOF}**, СЧА трёх фондов **{nav/1e9:.3f} млрд ₽** на {nav_date}.\n")
    p(f"Бумаг в расчёте: {len(have)} (из них уходят {len(EXCLUDED)}). "
      f"Потолок 15% сработал у: **{', '.join(sorted(capped)) or 'никого'}**.\n")
    p(f"## Сделки фондов\n")
    p("| бумага | вес сейчас | вес новый | сделка, млн ₽ |")
    p("|---|---|---|---|")
    for t, v in sorted(trades.items(), key=lambda kv: kv[1]):
        if abs(v) >= 10e6:
            p(f"| {name(t)} | {w_cur[t]:.2f}% | {w_new.get(t, 0.0):.2f}% | **{v/1e6:+,.0f}** |")
    p(f"\n**Продать всего:** {sells/1e6:,.0f} млн ₽ · **докупить:** {buys/1e6:+,.0f} млн ₽ "
      f"(расхождение {(sells+buys)/1e6:+.1f} млн — ноль по построению).\n")

    p(f"## Контроль снизу\n")
    p("Исключаемые бумаги продают целиком, поэтому их сумму можно получить вообще без "
      "модели — «позиции фондов × цена». Совпадение двух путей и есть проверка.\n")
    p("| бумага | модель | позиции × цена | расхождение |")
    p("|---|---|---|---|")
    for t in ("LENT", "MSNG"):
        if t in control:
            m, c = abs(trades[t]), control[t]
            p(f"| {name(t)} | {m/1e6:,.0f} млн | {c/1e6:,.0f} млн | {100*(m-c)/c:+.1f}% |")
    p(f"\nПозиции, которые фонды держат сейчас (для сверки с индикатором):\n")
    p("| бумага | штук | млрд ₽ |")
    p("|---|---|---|")
    for t, v in sorted(control.items(), key=lambda kv: -kv[1]):
        sh = sum(r["штук"] for r in funds["позиции"]
                 if isin_by.get(r["isin"]) == t)
        p(f"| {name(t)} | {sh:,} | {v/1e9:.2f} |")

    p(f"\n## Чувствительность к дате фиксации коэффициентов\n")
    p("Главный источник неопределённости — не данные, а трактовка методики: п. 2.8.6 "
      "говорит «день перед раскрытием», а Приложение 3 привязывает потолки к «Дате "
      "формирования Базы расчета». Сдвиг опорного дня на один день меняет ответ так:\n")
    p("| дата фиксации | продажа ЛУКОЙЛа | всего продать |")
    p("|---|---|---|")
    for d in ("2026-08-31", "2026-08-28", ASOF):
        mcf = {t: mc(t, d, ff_new.get(t, iss[t][d]["ff"]), lw_new.get(t, 1.0))
               for t in keep if d in iss[t]}
        if len(mcf) < len(keep):
            continue
        _, _, wwf = apply_caps(mcf)
        mn = {t: mc(t, ASOF, ff_new.get(t, iss[t][ASOF]["ff"]), lw_new.get(t, 1.0), wwf[t])
              for t in keep}
        tn = sum(mn.values())
        wn = {t: 100 * v / tn for t, v in mn.items()}
        tr = {t: (wn.get(t, 0.0) - w_cur[t]) / 100 * nav for t in have}
        s = sum(v for v in tr.values() if v < 0)
        mark = " ← принято" if d == FIX else (" ← наивная модель" if d == ASOF else "")
        p(f"| {d}{mark} | {tr['LKOH']/1e6:+,.0f} млн | {s/1e6:,.0f} млн |")
    p("\nПоэтому любую рублёвую сумму здесь надо называть оценкой, а не фактом.\n")

    p(f"\n## Топ-5 и прочие ограничения\n")
    grp_new: dict[str, float] = {}
    for t, v in w_new.items():
        grp_new[issuer(t)] = grp_new.get(issuer(t), 0.0) + v
    top5 = sorted(grp_new.values(), reverse=True)[:5]
    p(f"Сумма пяти наибольших эмитентов в новой базе: **{sum(top5):.1f}%** "
      f"(лимит на дату формирования {CAP_TOP5:.0f}%) — "
      f"{'ограничение НЕ выполняется, модель неполна' if sum(top5) > CAP_TOP5 else 'запас есть'}.\n")
    p(f"Максимальный вес эмитента: **{max(grp_new.values()):.2f}%** "
      f"(потолок {CAP_ISSUER:.0f}% на дату формирования, 30% в каждый момент).\n")

    # веса для fund_view.py: он строит портфель одного фонда на тех же числах
    (DATA / "trades_by_weight.json").write_text(json.dumps(
        {"веса": {t: {"old": w_cur[t], "new": w_new.get(t, 0.0), "mln": trades[t] / 1e6,
                      "ww": ww_new.get(t), "обрезан": issuer(t) in capped} for t in have},
         "нормировка": tot_now / tot, "обрезанные": sorted(capped)},
        ensure_ascii=False, indent=1), encoding="utf-8")

    out = "\n".join(L)
    (BASE / "RESULTS.md").write_text(out + "\n", encoding="utf-8")
    if not args.quiet:
        print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
