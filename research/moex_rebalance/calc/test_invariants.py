#!/usr/bin/env python
"""Инварианты расчёта ребаланса: то, что обязано быть верным.

Зачем файл. За две сессии в расчёте нашлось девять ошибок, и все — в производных
величинах, ни одной в прочитанном из документа или измеренном. Значит нужен не
«поверь мне», а исполняемая проверка: каждый тест здесь ловит одну КОНКРЕТНУЮ
ошибку, которая уже была допущена. Если правило нарушится снова — упадёт тест,
а не публикация.

Запуск:  .venv/bin/python research/moex_rebalance/calc/test_invariants.py
Тесты офлайновые: читают только sources/ и data/, в сеть не ходят.
"""
from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys

BASE = pathlib.Path(__file__).resolve().parents[1]
SRC, DATA = BASE / "sources", BASE / "data"
ASOF, FIX = "2026-09-01", "2026-08-31"

ok_count = 0
fails: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global ok_count
    if cond:
        ok_count += 1
        print(f"  ✓ {name}")
    else:
        fails.append(f"{name}: {detail}")
        print(f"  ✗ {name}  {detail}")


def txt(f: str) -> str:
    return re.sub(r"[ \t]+", " ", (SRC / f).read_text(encoding="utf-8", errors="replace"))


def main() -> int:
    lw = json.loads((DATA / "lw_new.json").read_text(encoding="utf-8"))
    ff = json.loads((DATA / "ff_new.json").read_text(encoding="utf-8"))
    ch = json.loads((DATA / "imoex_changes.json").read_text(encoding="utf-8"))
    iss = json.loads((DATA / "iss_imoex.json").read_text(encoding="utf-8"))["бумаги"]
    funds = json.loads((DATA / "funds.json").read_text(encoding="utf-8"))
    prices = json.loads((DATA / "prices.json").read_text(encoding="utf-8"))

    print("\n1. Документы: та ли редакция (ловит ошибку 01.09 — цитировал редакцию 2021 года)")
    new = txt("methodology_from_18.09.2026.txt")[:400]
    cur = txt("methodology_current.txt")[:400]
    check("новая редакция утверждена 13.08.2026, Протокол № 57",
          "«13» августа 2026" in new and "№ 57" in new, new[:120])
    check("действующая редакция утверждена 04.04.2025, Протокол № 28",
          "4 апреля 2025" in cur and "№ 28" in cur, cur[:120])
    check("устаревшая редакция 2021 лежит отдельно и помечена",
          (SRC / "methodology_2021_obsolete.docx").exists())

    print("\n2. Правила веса: не изменились ли между редакциями")
    def sect(t: str) -> str:
        i = [m.start() for m in re.finditer(
            r"Определение коэффициента ликвидности и дополнительного весового", t)][-1]
        j = [m.start() for m in re.finditer(r"Расчет Делителя", t)][-1]
        return re.sub(r"\s+", " ", t[i:j]).strip()
    check("разделы 2.7 и 2.8 идентичны в обеих редакциях",
          sect(txt("methodology_from_18.09.2026.txt")) == sect(txt("methodology_current.txt")))
    check("формула Wi = WWi ∙ LWi присутствует дословно",
          "Wi=WWi∙LWi" in txt("methodology_from_18.09.2026.txt").replace(" ", ""))
    check("LW определён через free-float (доля в НРД к Коэффициенту free-float)",
          "к Коэффициенту free-float" in txt("methodology_from_18.09.2026.txt"))
    check("коэффициенты считаются по итогам дня перед раскрытием (п. 2.8.6)",
          "предшествующего дню раскрытия информации об изменении Базы расчета"
          in txt("methodology_from_18.09.2026.txt"))

    print("\n3. Потолки: 15% для IMOEX на дату формирования")
    n = txt("methodology_from_18.09.2026.txt")
    i = n.find("не должен превышать установленную величину ограничения")
    app3 = re.sub(r"\s+", " ", n[i:i + 300])
    check("формулировка «на Дату формирования Базы расчета» на месте",
          "на Дату формирования Базы расчета" in app3, app3[:100])
    check("IMOEX в группе с ограничением 15", re.search(r"IMOEX[^|]{0,80}?15", app3) is not None, app3[:160])
    check("постоянный лимит 30% на каждый момент существует",
          re.search(r"на каждый момент расчета[^.]{0,120}30%", n) is not None)
    check("лимит топ-5: 55% на дату формирования", "не должна превышать 55%" in n)

    print("\n4. Таблица LW: полнота разбора (ловит потерю односимвольного тикера T)")
    check("ровно 60 бумаг", len(lw) == 60, str(len(lw)))
    check("односимвольный тикер T на месте со значением 0,8", lw.get("T") == 0.8, str(lw.get("T")))
    check("TATN 0,5 и TRNFP 0,3", lw.get("TATN") == 0.5 and lw.get("TRNFP") == 0.3)
    check("ЛУКОЙЛа, Татнефти-ап и Мосэнерго в таблице НЕТ (⇒ LW = 1)",
          not any(k in lw for k in ("LKOH", "TATNP", "MSNG")))
    pdf = subprocess.run(["pdftotext", "-layout", str(SRC / "lw_table_18.09.2026.pdf"), "-"],
                         capture_output=True, text=True, timeout=60).stdout
    nums = [int(m.group(1)) for m in re.finditer(r"^\s*(\d+)\s+[A-Z]", pdf, re.M)]
    check("нумерация в PDF без дыр, 1..60", sorted(nums) == list(range(1, 61)),
          f"{len(nums)} строк")

    print("\n5. Релиз: состав изменений (ловит допущение «включений вроде не было»)")
    check("ровно 15 бумаг с новым free-float", len(ff) == 15, str(len(ff)))
    check("исключены ровно Лента и Мосэнерго", set(ch["исключены"]) == {"Лента", "Мосэнерго"},
          str(ch["исключены"]))
    check("включений в IMOEX нет", ch["включения_в_imoex"] is False)
    check("TATN 49%, LKOH 59%, TRNFP 58%, TATNP 79%",
          ff.get("TATN") == .49 and ff.get("LKOH") == .59
          and ff.get("TRNFP") == .58 and ff.get("TATNP") == .79)

    print("\n6. Данные ISS: тождество веса (ловит смешение индексов и несортированные строки)")
    for d in (FIX, ASOF):
        num = {t: v[d]["cap"] * v[d]["ff"] * v[d]["w"] for t, v in iss.items()
               if d in v and v[d]["ff"] and v[d]["w"] and v[d]["cap"]}
        tot = sum(num.values())
        worst = max(abs(100 * x / tot - iss[t][d]["weight"]) for t, x in num.items())
        check(f"вес ∝ cap×ff×w на {d} (расхождение ≤ 0,006 п.п.)", worst <= 0.006,
              f"максимум {worst:.4f}")
    check("вес ЛУКОЙЛа на 01.09 равен 17,17%", iss["LKOH"][ASOF]["weight"] == 17.17,
          str(iss["LKOH"][ASOF]["weight"]))

    print("\n7. LW у бумаг под потолком (ловит «границу приняли за значение»)")
    LW_CAPPED = {"LKOH": 1.0, "SBER": 0.3, "SBERP": 0.6}
    for t, val in LW_CAPPED.items():
        w = iss[t][ASOF]["w"]
        check(f"{t}: принятый LW {val} не меньше w_factor {w:.7f} (иначе WW > 1)",
              val >= w - 1e-9)
    round_w = [t for t, v in iss.items()
               if v[ASOF]["w"] and abs(v[ASOF]["w"] * 10 - round(v[ASOF]["w"] * 10)) < 1e-9]
    check("нецелый w_factor ровно у трёх бумаг — тех, что под потолком",
          set(iss) - set(round_w) == set(LW_CAPPED), str(sorted(set(iss) - set(round_w))))

    print("\n8. Фонды: измеренное, а не выведенное")
    nav = sum(v["руб"] for v in funds["сча"].values())
    check("СЧА трёх фондов 43,2 млрд (не 43,3)", 43.1e9 < nav < 43.3e9, f"{nav/1e9:.3f}")
    check("все три СЧА на одну дату",
          len({v["дата"] for v in funds["сча"].values()}) == 1)
    check("состав взят только из справок о СЧА, не из парсера",
          funds.get("источник") == "interfax_manual", str(funds.get("источник")))
    check("у всех трёх фондов справка на одну дату",
          len({r["снимок"] for r in funds["позиции"]}) == 1,
          str(sorted({r["снимок"] for r in funds["позиции"]})))
    # Количества — из СПРАВОК О СЧА (source='interfax_manual', снимок 31.07.2026), а не
    # из парсера сайта ВИМ. Прежние значения (LKOH 1 529 996, TATN 4 189 844) были
    # смесью: по EQMX туда попадал WIP-парсер 'vim' за 26.08.
    isin = {"RU000A102S15": ("LENT", 81_694), "RU0008958863": ("MSNG", 45_044_000),
            "RU0009024277": ("LKOH", 1_545_355), "RU0009033591": ("TATN", 4_220_182)}
    for code, (tk, expect) in isin.items():
        got = sum(r["штук"] for r in funds["позиции"] if r["isin"] == code)
        check(f"{tk}: {expect:,} акций у трёх фондов", got == expect, f"получено {got:,}")

    print("\n9. Контроль снизу: модель против прямого измерения")
    res = (BASE / "RESULTS.md").read_text(encoding="utf-8") if (BASE / "RESULTS.md").exists() else ""
    for tk, code in (("Лента", "RU000A102S15"), ("Мосэнерго", "RU0008958863")):
        m = re.search(rf"\| {tk} \| ([\d ,]+) млн \| ([\d ,]+) млн \| ([+-][\d.,]+)%", res)
        if not m:
            check(f"{tk}: строка контроля есть в RESULTS.md", False, "не найдена")
            continue
        dev = abs(float(m.group(3).replace(",", ".")))
        check(f"{tk}: модель и «позиции × цена» расходятся ≤ 6% (кэш в СЧА)", dev <= 6.0,
              f"{dev}%")
    m = re.search(r"расхождение ([+-][\d.,]+) млн", res)
    check("сумма продаж равна сумме покупок",
          m is not None and abs(float(m.group(1).replace(",", "."))) < 1.0,
          m.group(1) if m else "не найдено")

    print(f"\n{'=' * 62}")
    print(f"пройдено {ok_count}, провалено {len(fails)}")
    for f in fails:
        print(f"  ✗ {f}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
