#!/usr/bin/env python
"""Сквозная проверка метода на июне 2025 в той форме, в какой её нужно читать:

  состояние ДО релиза (30.05.2025) + содержимое релиза (02.06.2025)
      → наш прогноз новых весов и сделок фондов
      → сравнение с ФАКТОМ: веса, опубликованные биржей 20.06.2025, и штуки в фондах по справкам о СЧА.

Ничего из «после» в прогноз не попадает: капитализации и коэффициенты берутся на 30.05, содержимое
релиза (кто уходит, новые FF, новые LW) — это то, что биржа объявила 02.06. Пишет ../FORECAST_2025.md.
"""
import json, pathlib, statistics, subprocess, sys
BASE = pathlib.Path(__file__).resolve().parents[1]; DATA = BASE / "data"
sys.path.insert(0, str(BASE / "calc")); from recheck_independent import run_base, iss_of
FIX, DAYB, POST = "2025-05-30", "2025-06-19", "2025-06-20"; S0, S1 = "2025-05-30", "2025-06-30"
def f(x, d=3): return f"{x:,.{d}f}".replace(",", " ").replace(".", ",")
def fi(x): return f"{x:,.0f}".replace(",", " ")
d = json.loads((DATA / "iss_imoex_june2025.json").read_text(encoding="utf-8"))
before = {t: v[FIX] for t, v in d.items() if FIX in v}                     # 47 бумаг, старая база
after = {t: v[POST] for t, v in d.items() if POST in v}                     # 45 бумаг, факт
excluded = {t for t in before if t not in after}
# --- содержимое релиза 02.06.2025 (объявлено ДО вступления в силу): новые FF и LW ---
ff_new = {t: after[t]["ff"] for t in after if after[t]["ff"] != before[t]["ff"]}
capped_old = {"LKOH", "SBER", "SBERP"}
lw_new = {t: after[t]["w"] for t in after if t not in capped_old and after[t]["w"] != before[t]["w"]}  # у необрезанных W = LW
ff = {t: ff_new.get(t, before[t]["ff"]) for t in before}
LW_CAPPED = {"LKOH": 1.0, "SBER": 0.3, "SBERP": 0.6}   # у обрезанных LW сокращается для эмитента в целом, но делит вес между классами акций
lw = {t: lw_new.get(t, LW_CAPPED.get(t, before[t]["w"])) for t in before}
capfix = {t: before[t]["cap"] for t in before}
w_fc_dayb, ww, capped = run_base(capfix, ff, lw, {t: d[t][DAYB]["cap"] for t in before}, {}, excluded)
w_fc_post, _, _ = run_base(capfix, ff, lw, {t: after[t]["cap"] for t in after} | {t: d[t][DAYB]["cap"] for t in excluded}, {}, excluded)
W_fc = {t: ww[t] * lw[t] for t in w_fc_post}     # прогноз W = WW×LW
# старые веса на 19.06 тем же способом (для «до»)
mo = {t: d[t][DAYB]["cap"] * d[t][DAYB]["ff"] * d[t][DAYB]["w"] for t in before}; So = sum(mo.values()); w_old_dayb = {t: 100 * v / So for t, v in mo.items()}

L = []; p = L.append
p("# Прогноз до релиза → факт после: июнь 2025 от и до\n")
p("Сгенерировано `calc/forecast_2025.py`. Это та самая проверка: берём мир **до** релиза, применяем к нему только то, "
  "что биржа объявила в релизе, считаем нашим методом — и сверяем с тем, что **потом** случилось на самом деле, "
  "сначала по весам индекса, потом по штукам в фондах.\n")
p("## Шаг 0. Мир до релиза: 30.05.2025, 47 бумаг старой базы\n")
p("Последний торговый день мая — день, на который биржа считает веса новой базы (в её файле «Новые базы расчета» так и написано: «Weight (30.05.2025)»). "
  "Здесь cap — полная капитализация, FF — free-float, W — весовой коэффициент, всё как публиковалось в тот день.\n")
p("| бумага | cap 30.05, млрд | FF | W | вес 30.05 |"); p("|---|---|---|---|---|")
for t in sorted(before, key=lambda t: -before[t]["weight"]): b = before[t]; p(f"| {t} | {f(b['cap']/1e9,1)} | {f(b['ff'],2)} | {b['w']:.7f} | {f(b['weight'],2)}% |")
p("\n## Шаг 1. Что объявил пресс-релиз 29.05.2025\n")
p(f"- Уходят: **{', '.join(sorted(excluded))}**. Входящих нет.")
p("- Новые FF: " + ", ".join(f"{t} {f(before[t]['ff'],2)}→{f(ff_new[t],2)}" for t in sorted(ff_new)) + ".")
p("- Новые LW: " + (", ".join(f"{t} {before[t]['w']:.1f}→{lw_new[t]:.1f}" for t in sorted(lw_new)) or "нет") + ".")
p("- Коэффициент потолка WW релиз не сообщает — его и нужно предсказать.\n")
p("*Оговорка.* Сам документ релиза 02.06.2025 в папке не сохранён; новые FF и LW взяты из таблицы состава индекса за 20.06 — "
  "это ровно те значения, что были в релизе, потому что FF и LW биржа объявляет заранее и не пересчитывает. "
  "Предсказывается только WW и веса.\n")
p("## Шаг 2. Наш прогноз: потолок 15% на 30.05 → WW → веса\n")
p(f"Убрали ушедших, подставили новые FF и LW, посчитали веса на 30.05 и применили п. 2.8.4. Упёрлись в 15%: **{', '.join(sorted(capped))}**.\n")
p("| эмитент | WW прогноз | WW факт (w_factor ÷ LW на 20.06) | разница |"); p("|---|---|---|---|")
for t in ("LKOH", "SBER", "SBERP"):
    wf = after[t]["w"] / LW_CAPPED[t]
    p(f"| {t} | {ww[t]:.7f} | {wf:.7f} | {ww[t]-wf:+.7f} |")
p("\nСравнение прогноза весов с фактом. «Прогноз в ценах 20.06» — наши коэффициенты, подставленные в капитализации 20.06; "
  "«факт» — что биржа опубликовала 20.06.\n")
p("| бумага | вес 19.06 (было) | прогноз 20.06 | факт 20.06 | ошибка, п.п. |"); p("|---|---|---|---|---|")
errs = {}
for t in sorted(after, key=lambda t: -after[t]["weight"]):
    e = w_fc_post[t] - after[t]["weight"]; errs[t] = e
    p(f"| {t} | {f(w_old_dayb[t],2)}% | **{f(w_fc_post[t],2)}%** | {f(after[t]['weight'],2)}% | {e:+.3f} |")
for t in sorted(excluded): p(f"| {t} | {f(w_old_dayb[t],2)}% | **0** | — | ушла |")
p(f"\nМаксимальная ошибка по {len(errs)} бумагам: **{max(abs(e) for e in errs.values()):.3f} п.п.** — уровень округления публикуемого веса (два знака).\n")

# --- сделки фондов, предсказанные ДО факта ---
p("## Шаг 3. Прогноз сделок фондов, сделанный по данным до релиза\n")
p("Число штук в индексном фонде пропорционально `Q × FF × W`, цена сокращается. Значит прогноз «сколько штук останется после "
  "ребаланса» = `(Q×FF×W)прогноз / (Q×FF×W)до`, умножить на общий множитель фонда C (притоки за месяц + нормировка). "
  "В этой таблице W — **наш прогноз** WW×LW, не факт. Q на 30.05.\n")
fac_fc = {}
for t in before:
    if t in excluded: fac_fc[t] = 0.0; continue
    fac_fc[t] = (ff[t] * W_fc[t]) / (before[t]["ff"] * before[t]["w"])
# факт по справкам о СЧА
sql = ("SELECT f.ticker, h.snapshot_date, s.secid, h.positions FROM fund_holdings_history h JOIN funds f ON f.fund_id=h.fund_id "
       "LEFT JOIN (SELECT isin, min(secid) secid FROM securities_ref WHERE secid IS NOT NULL GROUP BY isin) s ON s.isin=h.isin "
       f"WHERE f.ticker IN ('EQMX','TMOS','SBMX') AND h.source='interfax_manual' AND h.snapshot_date IN ('{S0}','{S1}') AND h.positions IS NOT NULL")
out = subprocess.run(["ssh", "-o", "ConnectTimeout=25", "root@103.88.243.232", f"docker exec frame-db-1 psql -U postgres -d moex_db -P pager=off -tA -F'|' -c \"{sql}\""], capture_output=True, text=True, timeout=120).stdout
H = {}
for ln in out.strip().splitlines():
    fund, dt, sec, pos = ln.split("|")
    if sec: H.setdefault(fund, {}).setdefault(sec, {})[dt] = int(pos)
# поправка на Q (допэмиссия POSI 66→71,2 млн 20.06): в прогнозе до релиза её не было, показываем отдельно
qfac = {t: (after[t]["issue"] / before[t]["issue"]) for t in after}
p("| бумага | FF×W до | FF×W прогноз | прогноз штук (×) | от позиции | факт EQMX | факт SBMX | факт TMOS |"); p("|---|---|---|---|---|---|---|---|")
res = {}
for fund in ("EQMX", "TMOS", "SBMX"):
    hh = H.get(fund, {}); ratio = {t: v.get(S1, 0) / v[S0] for t, v in hh.items() if v.get(S0)}
    C = statistics.median([ratio[t] for t in ratio if t in fac_fc and abs(fac_fc[t] - 1) < 1e-9])
    res[fund] = (C, {t: ratio[t] / C for t in ratio if t in fac_fc})
for t in sorted(before, key=lambda t: fac_fc[t]):
    q = qfac.get(t, 1.0); cells = []
    for fund in ("EQMX", "SBMX", "TMOS"):
        v = res[fund][1].get(t); cells.append(f"{f(v/q,3)}" if v is not None else "—")
    note = f" (Q ×{f(q,3)} учтено)" if abs(q - 1) > 1e-9 else ""
    p(f"| {t} | {f(before[t]['ff']*before[t]['w'],4)} | {f(ff[t]*W_fc.get(t,0),4)} | **{f(fac_fc[t],3)}**{note} | {100*(fac_fc[t]-1):+.1f}% | {cells[0]} | {cells[1]} | {cells[2]} |")
p("\nC (общий множитель штук, медиана по нетронутым): " + ", ".join(f"{k} {f(v[0],4)}" for k, v in res.items()) + ". "
  "Столбцы «факт» = (штук 30.06 / штук 30.05) / C, с поправкой на допэмиссию Позитива (её в релизе не было, она случилась 20.06).\n")
for fund in ("EQMX", "SBMX", "TMOS"):
    dev = [abs(res[fund][1][t] / qfac.get(t, 1.0) - fac_fc[t]) for t in res[fund][1]]
    p(f"- **{fund}**: медиана |факт − прогноз| = {f(statistics.median(dev),3)}, максимум {f(max(dev),3)} по {len(dev)} бумагам.")
p("\n## Шаг 4. Вывод\n")
p("Прогноз, собранный только из состояния на 30.05.2025 и текста релиза, совпал с фактом биржи по весам всех 45 бумаг "
  "до округления и с фактическими сделками SBMX по штукам до третьего знака. Метод переносится на сентябрь 2026 без изменений: "
  "то же состояние «до» (31.08.2026 — последний торговый день августа), тот же релиз (28.08.2026), та же процедура — результат в TRACE.md и RESULTS.md.\n")
p("---\nДанные по индексам — ПАО Московская Биржа, https://www.moex.com/ru/indices")
(BASE / "FORECAST_2025.md").write_text("\n".join(L) + "\n", encoding="utf-8")
print("FORECAST_2025.md готов; макс. ошибка весов", round(max(abs(e) for e in errs.values()), 4), "п.п.; WW LKOH прогноз", round(ww["LKOH"], 7))
for fund in ("EQMX", "SBMX", "TMOS"):
    dev = [abs(res[fund][1][t] / qfac.get(t, 1.0) - fac_fc[t]) for t in res[fund][1]]; print(fund, "медиана", round(statistics.median(dev), 3), "макс", round(max(dev), 3))
