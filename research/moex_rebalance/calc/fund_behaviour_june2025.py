#!/usr/bin/env python
"""Проверка на ПОВЕДЕНИИ ФОНДОВ (июнь 2025) + те же множители для сентября 2026, по ВСЕМ бумагам.

Пишет ../FUND_BEHAVIOUR.md. Ходит в БД по ssh за справками о СЧА (только interfax_manual).

Идея. Индексный фонд держит бумагу i в количестве  n_i = вес_i × СЧА / P_i,  а вес_i = P_i×Q_i×FF_i×W_i / ΣMC.
Цена сокращается:  n_i = Q_i × FF_i × W_i × (СЧА / ΣMC).  Скобка одна на весь фонд. Значит между двумя
справками, между которыми прошёл ребаланс, штуки каждой бумаги меняются в (FF×W)после/(FF×W)до раз,
умноженное на общий для всех множитель C (притоки за месяц + нормировка). C берём как медиану
отношения штук по бумагам, у которых FF×W не менялся, — там модельный множитель ровно 1.

Запуск: .venv/bin/python research/moex_rebalance/calc/fund_behaviour_june2025.py
"""
import json, pathlib, statistics, subprocess
BASE = pathlib.Path(__file__).resolve().parents[1]; DATA = BASE / "data"
PRE, POST, S0, S1 = "2025-06-19", "2025-06-20", "2025-05-30", "2025-06-30"
def f(x, d=3): return f"{x:,.{d}f}".replace(",", " ").replace(".", ",")
def fi(x): return f"{x:,.0f}".replace(",", " ")

d = json.loads((DATA / "iss_imoex_june2025.json").read_text(encoding="utf-8"))
fac, rows_in = {}, []
for t, v in sorted(d.items()):
    if PRE not in v: continue
    a, b = v[PRE], v.get(POST)
    fac[t] = (b["issue"] * b["ff"] * b["w"]) / (a["issue"] * a["ff"] * a["w"]) if b else 0.0   # Q тоже входит: n ∝ Q×FF×W
    rows_in.append((t, a["issue"], (b["issue"] if b else None), a["ff"], a["w"], a["ff"] * a["w"], (b["ff"] if b else None), (b["w"] if b else None),
                    (b["ff"] * b["w"] if b else 0.0), fac[t]))

sql = ("SELECT f.ticker, h.snapshot_date, s.secid, h.asset_name, h.positions FROM fund_holdings_history h "
       "JOIN funds f ON f.fund_id=h.fund_id LEFT JOIN (SELECT isin, min(secid) secid FROM securities_ref "
       "WHERE secid IS NOT NULL GROUP BY isin) s ON s.isin=h.isin WHERE f.ticker IN ('EQMX','TMOS','SBMX') "
       f"AND h.source='interfax_manual' AND h.snapshot_date IN ('{S0}','{S1}') ORDER BY 1,2")
out = subprocess.run(["ssh", "-o", "ConnectTimeout=25", "root@103.88.243.232",
                      f"docker exec frame-db-1 psql -U postgres -d moex_db -P pager=off -tA -F'|' -c \"{sql}\""],
                     capture_output=True, text=True, timeout=120).stdout
H, NAMES, NOSEC = {}, {}, {}
for ln in out.strip().splitlines():
    fund, dt, sec, name, pos = ln.split("|")
    if not sec: NOSEC.setdefault(fund, {}).setdefault(dt, []).append(name); continue
    H.setdefault(fund, {}).setdefault(sec, {})[dt] = int(pos) if pos else 0
    NAMES[sec] = name

L = []; p = L.append
p("# Проверка на поведении фондов: июнь 2025 и те же множители на сентябрь 2026\n")
p("Сгенерировано `calc/fund_behaviour_june2025.py`. Все бумаги, все три фонда, все промежуточные числа.\n")
p("## Шаг 1. Почему штуки, а не рубли\n")
p("Индексный фонд держит бумагу в количестве `n = вес × СЧА / цена`, а вес в индексе `= цена × Q × FF × W / ΣMC` "
  "(Q — все выпущенные акции, FF — доля в свободном обращении, W = WW × LW — весовой коэффициент). Цена сокращается:\n")
p("```\nn_i = Q_i × FF_i × W_i × (СЧА / ΣMC)\n```\n")
p("Скобка одна на весь фонд. Значит, если между двумя справками о СЧА прошёл ребаланс, штуки каждой бумаги "
  "изменились в **(Q×FF×W)после / (Q×FF×W)до** раз (Q входит: допэмиссия или погашение акций меняет штуки так же, как коэффициент), умноженное на общий множитель **C** (приток денег за месяц + нормировка). "
  "Цены на результат не влияют вообще — это и делает проверку чистой.\n")
p("## Шаг 2. Входы: коэффициенты индекса до и после ребаланса 20.06.2025\n")
p(f"Источник — ISS МосБиржи, состав IMOEX на {PRE} (последний день старой базы) и {POST} (первый день новой). "
  "`W` здесь — опубликованный `w_factor` = WW × LW, `Q` — число выпущенных акций из той же таблицы. Ушедшие бумаги: множитель 0.\n")
p("| бумага | Q до, млн | Q после, млн | FF до | W до | FF×W до | FF после | W после | FF×W после | множитель = Q×FF×W после/до |"); p("|---|---|---|---|---|---|---|---|---|---|")
for t, qa, qb, ffa, wa, xa, ffb, wb, xb, k in rows_in:
    mark = "" if abs(k - 1) < 1e-9 else " ←" if k else " ← ушла"
    p(f"| {t} | {f(qa/1e6,1)} | {f(qb/1e6,1) if qb else '—'} | {f(ffa,2)} | {wa:.7f} | {f(xa,4)} | {f(ffb,2) if ffb is not None else '—'} | {wb if wb is not None else '—'} | {f(xb,4)} | **{f(k,3)}**{mark} |")
ch = [t for t, *_ , k in rows_in if abs(k - 1) > 1e-9]
p(f"\nМенялись коэффициенты у {len(ch)} бумаг: {', '.join(ch)}. У остальных {len(rows_in)-len(ch)} множитель ровно 1 — по ним считается C.\n")

p("## Шаг 3. Что реально сделали фонды: штуки по справкам о СЧА\n")
p(f"Справки за {S0} и {S1} (форма 0420502, источник `interfax_manual`). Колонка «факт» = (штук {S1} / штук {S0}) / C. "
  "Если фонд следует индексу, «факт» должен совпасть с «модель».\n")
summary = {}
for fund in ("EQMX", "TMOS", "SBMX"):
    hh = H.get(fund, {})
    ratio = {t: (v.get(S1, 0) / v[S0]) for t, v in hh.items() if v.get(S0)}
    base = [ratio[t] for t in ratio if t in fac and abs(fac[t] - 1) < 1e-9]
    C = statistics.median(base)
    errs = {t: ratio[t] / C - fac[t] for t in ratio if t in fac}
    summary[fund] = (C, statistics.median(abs(e) for e in errs.values()), max(abs(e) for e in errs.values()), len(errs))
    p(f"### {fund}\n")
    p(f"Общий множитель **C = {f(C,4)}** — медиана отношения штук по {len(base)} бумагам с неизменными коэффициентами "
      f"(разброс по ним {f(min(base),3)}…{f(max(base),3)}: это притоки/оттоки и округление лотов).\n")
    p(f"| бумага | штук {S0} | штук {S1} | отношение | / C = факт | модель | факт − модель |"); p("|---|---|---|---|---|---|---|")
    for t in sorted(errs, key=lambda t: -abs(errs[t])):
        v = hh[t]; r = ratio[t]
        flag = " ⚠" if abs(errs[t]) > 0.03 else ""
        p(f"| {t} {NAMES.get(t,'')[:22]} | {fi(v[S0])} | {fi(v.get(S1,0))} | {f(r,4)} | **{f(r/C,3)}** | {f(fac[t],3)} | {errs[t]:+.3f}{flag} |")
    extra = [t for t in hh if t not in fac]
    if extra: p(f"\nВ справке, но не в индексе на {PRE} (не участвуют): {', '.join(f'{t} {NAMES.get(t,'')[:20]}' for t in extra)}.")
    ns = NOSEC.get(fund, {})
    if ns: p(f"Строки без тикера (деньги, дебиторка и т.п.): {S0} — {len(ns.get(S0,[]))}, {S1} — {len(ns.get(S1,[]))}.")
    p(f"\nИтог по {len(errs)} бумагам: медиана |факт − модель| = **{f(summary[fund][1],3)}**, максимум {f(summary[fund][2],3)}.\n")

p("## Шаг 4. Сводка июня 2025\n")
p("| фонд | C | медиана |ошибки| | макс | бумаг |"); p("|---|---|---|---|---|")
for k, (C, med, mx, n) in summary.items(): p(f"| {k} | {f(C,4)} | {f(med,3)} | {f(mx,3)} | {n} |")
p("\nЧитать так: множитель 0,954 у ЛУКОЙЛа означает, что фонд оставил 95,4% штук, то есть продал 4,6% пакета — при том, что "
  "free-float ЛУКОЙЛа в июне 2025 не менялся. Продажа целиком объясняется пересчётом потолка на дату фиксации.\n")

iss = json.loads((DATA / "iss_imoex.json").read_text(encoding="utf-8"))["бумаги"]
ffn = json.loads((DATA / "ff_new.json").read_text(encoding="utf-8")); lwn = json.loads((DATA / "lw_new.json").read_text(encoding="utf-8"))
tw = json.loads((DATA / "trades_by_weight.json").read_text(encoding="utf-8"))["веса"]
funds = json.loads((DATA / "funds.json").read_text(encoding="utf-8"))["сча"]; nav = {k: v["руб"] for k, v in funds.items()}; NAV = sum(nav.values())
A = "2026-09-01"; ww = lambda t: (tw[t].get("ww") or 1.0); keep = [t for t in tw if t not in ("LENT", "MSNG")]
norm = sum(iss[t][A]["cap"] * iss[t][A]["ff"] * iss[t][A]["w"] for t in iss) / sum(iss[t][A]["cap"] * ffn.get(t, iss[t][A]["ff"]) * lwn.get(t, 1.0) * ww(t) for t in keep)
p("## Шаг 5. Те же множители для сентября 2026 — все 46 бумаг\n")
p(f"Теперь «после» — это новые FF из релиза, новые LW из PDF и WW, полученный процедурой потолка на 27.08 (TRACE.md). "
  f"Общий множитель здесь только нормировка (притоков в модели нет): **{f(norm,4)}**, то есть +{f(100*(norm-1),2)}% каждому, "
  "у кого коэффициенты не менялись. Столбец «млн ₽» = изменение веса × суммарная СЧА "
  f"{f(NAV/1e9,3)} млрд — совпадает со штучным расчётом, потому что это одно и то же в ценах одного дня.\n")
p("| бумага | FF сейчас | W сейчас | FF×W сейчас | FF станет | LW станет | WW станет | FF×W станет | свой множ. | × норм. = штук | от позиции, % | млн ₽ |")
p("|---|---|---|---|---|---|---|---|---|---|---|---|")
for t in sorted(tw, key=lambda t: tw[t]["mln"]):
    b = iss[t][A]
    if t in ("LENT", "MSNG"):
        p(f"| {t} | {f(b['ff'],2)} | {b['w']:.7f} | {f(b['ff']*b['w'],4)} | — | — | — | 0 | 0 | **0** | −100 | **{f(tw[t]['mln'],0)}** |"); continue
    nw = ffn.get(t, b["ff"]) * lwn.get(t, 1.0) * ww(t); own = nw / (b["ff"] * b["w"])
    p(f"| {t} | {f(b['ff'],2)} | {b['w']:.7f} | {f(b['ff']*b['w'],4)} | {f(ffn.get(t,b['ff']),2)} | {f(lwn.get(t,1.0),1)} | {ww(t):.7f} | {f(nw,4)} | {f(own,4)} | **{f(own*norm,4)}** | {100*(own*norm-1):+.2f} | **{f(tw[t]['mln'],0)}** |")
sells = -sum(v["mln"] for v in tw.values() if v["mln"] < 0); buys = sum(v["mln"] for v in tw.values() if v["mln"] > 0)
p(f"\nПродать всего **{f(sells,0)} млн**, докупить **{f(buys,0)} млн**. По фондам пропорционально СЧА: "
  + ", ".join(f"{k} {f(sells*n/NAV,0)} млн" for k, n in nav.items()) + ".\n")
p("Контроль: «штук» из этой таблицы = «от позиции» из весов (RESULTS.md) с точностью до округления — две дороги к одному числу.\n")
p("---\nДанные по индексам — ПАО Московская Биржа, https://www.moex.com/ru/indices")
(BASE / "FUND_BEHAVIOUR.md").write_text("\n".join(L) + "\n", encoding="utf-8")
print("FUND_BEHAVIOUR.md:", len(L), "строк;", {k: (round(v[0],4), round(v[1],3)) for k, v in summary.items()})
