#!/usr/bin/env python
"""Три таблицы по всем 46 бумагам → ../TABLES.md:
 1. вес было/станет, позиция фондов сейчас/станет, разница (млн ₽);
 2. вынужденная сделка против нового free-float (сколько % свободного обращения фонды двинут);
 3. новая база: Q, FF, LW, WW, W, учитываемая капитализация, вес.
Плюс разбор Газпрома по шагам. Читает data/*.json, ничего не качает."""
import json, pathlib
BASE = pathlib.Path(__file__).resolve().parents[1]; D = BASE / "data"; A = "2026-09-01"
def f(x, d=2): return f"{x:,.{d}f}".replace(",", " ").replace(".", ",")
def m(x): return ("+" if x>0 else "−" if x<0 else "") + f"{abs(x):,.0f}".replace(",", " ")
iss = json.loads((D/"iss_imoex.json").read_text(encoding="utf-8"))["бумаги"]
ffn = json.loads((D/"ff_new.json").read_text(encoding="utf-8")); lwn = json.loads((D/"lw_new.json").read_text(encoding="utf-8"))
tw = json.loads((D/"trades_by_weight.json").read_text(encoding="utf-8")); norm = 1/tw.get("нормировка"); tw = tw["веса"]   # множитель веса нетронутых = 1/(ΣMC_new/ΣMC_old)
tvp = D/"turnover_20d.json"; TV = json.loads(tvp.read_text(encoding="utf-8"))["средний_дневной_оборот_руб"] if tvp.exists() else {}
pr = json.loads((D/"prices.json").read_text(encoding="utf-8")); funds = json.loads((D/"funds.json").read_text(encoding="utf-8"))["сча"]
NAV = sum(v["руб"] for v in funds.values()); LWC = {"LKOH": 1.0, "SBER": 0.3, "SBERP": 0.6}
EXCL = {"LENT", "MSNG"}
L = []; p = L.append
p("# Три таблицы: было → станет, сделка против free-float, новая база\n")
p(f"Сгенерировано `calc/tables.py`. Дата фиксации весов **31.08.2026** (последний торговый день августа — так подписывает "
  f"свои файлы биржа), цены и капитализации на {A}, СЧА трёх фондов на 31.08: **{f(NAV/1e9,3)} млрд ₽** "
  + "(" + ", ".join(f"{k} {f(v['руб']/1e9,2)}" for k, v in funds.items()) + "). Позиция фондов = вес × СЧА.\n")

# ---------- 1 ----------
p("## 1. Вес и позиция фондов: было → станет\n")
p("| бумага | вес сейчас | вес станет | Δ, п.п. | позиция сейчас, млн | позиция станет, млн | разница, млн | % позиции |")
p("|---|---|---|---|---|---|---|---|")
for t in sorted(tw, key=lambda t: tw[t]["mln"]):
    v = tw[t]; now = v["old"]/100*NAV/1e6; new = v["new"]/100*NAV/1e6
    p(f"| {t} | {f(v['old'],3)}% | {f(v['new'],3)}% | {v['new']-v['old']:+.3f} | {f(now,0)} | {f(new,0)} | **{m(v['mln'])}** | {100*(v['new']-v['old'])/v['old']:+.1f}% |")
sells = -sum(v["mln"] for v in tw.values() if v["mln"] < 0); buys = sum(v["mln"] for v in tw.values() if v["mln"] > 0)
p(f"\nПродать **{f(sells,0)} млн**, докупить **{f(buys,0)} млн** ({f(100*sells/NAV*1e6,2)}% СЧА). Сумма весов «сейчас» {f(sum(v['old'] for v in tw.values()),2)}%, «станет» {f(sum(v['new'] for v in tw.values()),2)}%.\n")

# ---------- 2 ----------
p("## 2. Вынужденная сделка против нового free-float\n")
p("Free-float-капитализация = cap × FF новый: столько акций компании реально в свободном обращении по оценке биржи. "
  "Последняя колонка — какую долю этого свободного обращения три фонда должны продать (−) или купить (+) в один день.\n")
p("| бумага | cap, млрд | FF сейчас | FF станет | free-float, млрд | сделка, млн | % от free-float | сделка, тыс. штук | ср. дневной оборот, млн | сделка, дней оборота |")
p("|---|---|---|---|---|---|---|---|---|---|")
for t in sorted(tw, key=lambda t: -abs(tw[t]["mln"]/(iss[t][A]["cap"]*ffn.get(t, iss[t][A]["ff"])))):
    b = iss[t][A]; ffnew = ffn.get(t, b["ff"]); ffcap = b["cap"]*ffnew; price = pr.get(t, {}).get(A)
    mm = tw[t]["mln"]; sh = m(mm*1e6/price/1e3) if price else "—"; tv = TV.get(t); days = f"{abs(mm)*1e6/tv:.2f}".replace(".", ",") if tv else "—"
    p(f"| {t} | {f(b['cap']/1e9,0)} | {f(b['ff'],2)} | {f(ffnew,2)} | {f(ffcap/1e9,1)} | {m(mm)} | **{100*mm*1e6/ffcap:+.3f}%** | {sh} | {f(tv/1e6,0) if tv else '—'} | {days} |")
p("\nУ Ленты и Мосэнерго свободное обращение считается по старому FF (нового нет — бумаги уходят). Оборот — средний дневной за 04.08–01.09.2026 по нашей базе свечей.\n")

# ---------- 3 ----------
p("## 3. Новая база с 18.09.2026: коэффициенты и веса\n")
p("`Q` — все выпущенные акции (cap / цена), `FF` — free-float, `LW` — дополнительный коэффициент (PDF к релизу; кого нет в PDF — 1), "
  "`WW` — коэффициент потолка (наш расчёт, у всех кроме ЛУКОЙЛа и Сбербанка = 1), `W = WW × LW`, `MC = cap × FF × W`. Вес = MC / ΣMC в ценах 01.09.\n")
p("| № | бумага | Q, млн шт | FF было → станет | LW было → станет | WW | W станет | MC станет, млрд | вес станет |")
p("|---|---|---|---|---|---|---|---|---|")
rows = []
for t in tw:
    if t in EXCL: continue
    b = iss[t][A]; ffnew = ffn.get(t, b["ff"]); lw_old = LWC.get(t, b["w"]); lw_new = lwn.get(t, 1.0); ww = tw[t].get("ww") or 1.0
    W = ww*lw_new; mc = b["cap"]*ffnew*W; q = b["cap"]/pr[t][A] if pr.get(t, {}).get(A) else None
    rows.append((tw[t]["new"], t, q, b["ff"], ffnew, lw_old, lw_new, ww, W, mc))
S = sum(r[-1] for r in rows)
for i, (wn, t, q, ffo, ffnew, lwo, lwnw, ww, W, mc) in enumerate(sorted(rows, reverse=True), 1):
    p(f"| {i} | {t} | {f(q/1e6,1) if q else '—'} | {f(ffo,2)} → {f(ffnew,2)}{' ←' if ffnew!=ffo else ''} | {f(lwo,1)} → {f(lwnw,1)}{' ←' if abs(lwnw-lwo)>1e-9 else ''} | {ww:.7f} | {W:.7f} | {f(mc/1e9,1)} | **{f(100*mc/S,3)}%** |")
p(f"\nΣ MC новой базы = {f(S/1e12,3)} трлн ₽. Ушли: Лента (уровень листинга), Мосэнерго (вес < 0,2%).\n")

# ---------- Газпром ----------
g = tw["GAZP"]; freed = tw["LENT"]["old"] + tw["MSNG"]["old"] + (tw["LKOH"]["old"]-tw["LKOH"]["new"]) + (tw["SBER"]["old"]-tw["SBER"]["new"]) + (tw["SBERP"]["old"]-tw["SBERP"]["new"])
unc = [t for t in tw if t not in EXCL and t not in ("LKOH", "SBER", "SBERP")]; unc_w = sum(tw[t]["old"] for t in unc)
p("## Почему Газпром докупают: по шагам\n")
p(f"1. У Газпрома ничего не меняли: FF {f(iss['GAZP'][A]['ff'],2)} → {f(ffn.get('GAZP', iss['GAZP'][A]['ff']),2)}, LW {iss['GAZP'][A]['w']:.1f} → {lwn.get('GAZP',1.0):.1f}. Его собственный множитель ровно 1.")
p(f"2. Освободился вес, п.п.: Лента {f(tw['LENT']['old'],2)} + Мосэнерго {f(tw['MSNG']['old'],2)} + ЛУКОЙЛ (17,17 → {f(tw['LKOH']['new'],2)}) {f(tw['LKOH']['old']-tw['LKOH']['new'],2)} + Сбербанк {f(tw['SBER']['old']-tw['SBER']['new']+tw['SBERP']['old']-tw['SBERP']['new'],2)} = **{f(freed,2)} п.п.**")
p(f"3. Этот вес по п. 2.8.4 достаётся всем, кого потолок не режет, пропорционально их весу. Таких {len(unc)} бумаг общим весом {f(unc_w,1)}%.")
p(f"4. Нетронутой бумаге это даёт ×{norm:.4f} к весу, то есть **+{f(100*(norm-1),2)} % к позиции** (у бумаг с новыми коэффициентами — свой множитель × {norm:.4f}).")
p(f"5. Газпром — самая большая позиция среди тех, кого не режет потолок: {f(g['old'],3)}% × {f(100*(norm-1),2)}% = **+{g['new']-g['old']:.3f} п.п.** × СЧА {f(NAV/1e9,2)} млрд = **+{g['mln']:.0f} млн ₽**.")
p(f"6. Для сравнения: Яндекс {f(tw['YDEX']['old'],2)}% → +{tw['YDEX']['mln']:.0f} млн, Т-Технологии {f(tw['T']['old'],2)}% → +{tw['T']['mln']:.0f} млн. Тот же процент, меньше база.\n")
p("То есть Газпром покупают не потому, что ему что-то улучшили, а потому, что при перераспределении освободившегося веса «каждому по размеру» он самый крупный из получателей.\n")
p("---\nДанные по индексам — ПАО Московская Биржа, https://www.moex.com/ru/indices")
(BASE/"TABLES.md").write_text("\n".join(L)+"\n", encoding="utf-8"); print("TABLES.md:", len(L), "строк; освободилось", round(freed,2), "п.п.; норм.", norm)
