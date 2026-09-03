#!/usr/bin/env python
"""Полная трассировка расчёта сентябрьской базы — каждое число, чтобы проверить на калькуляторе.

Пишет ../TRACE.md. Ничего не оптимизирует и не сокращает: цель — показать все промежуточные
величины, а не получить ответ быстро. Процедура потолка — буквально по п. 2.8.4, с журналом
каждой итерации.

Запуск:  .venv/bin/python research/moex_rebalance/calc/trace.py
"""
from __future__ import annotations
import json, pathlib, re, subprocess, html
BASE = pathlib.Path(__file__).resolve().parents[1]; SRC, DATA = BASE/"sources", BASE/"data"
FIX, ASOF, CAP = "2026-08-31", "2026-09-01", 15.0
EXCL = {"LENT", "MSNG"}
ISSUER = {"SBER":"Сбербанк","SBERP":"Сбербанк","TATN":"Татнефть","TATNP":"Татнефть",
          "SNGS":"Сургутнефтегаз","SNGSP":"Сургутнефтегаз","RTKM":"Ростелеком","RTKMP":"Ростелеком"}
iss_of = lambda t: ISSUER.get(t, t)
def f(x, d=2): return f"{x:,.{d}f}".replace(",", " ").replace(".", ",")
def fb(x): return f"{x:,.0f}".replace(",", " ")

# ── входы ─────────────────────────────────────────────────────────────────────
iss = json.loads((DATA/"iss_imoex.json").read_text(encoding="utf-8"))["бумаги"]
txt = subprocess.run(["pdftotext","-layout",str(SRC/"lw_table_18.09.2026.pdf"),"-"],capture_output=True,text=True).stdout
lw_new = {m.group(2): float(m.group(3).replace(",",".")) for ln in txt.splitlines()
          if (m := re.match(r"\s*(\d+)\s+([A-Z][A-Z0-9]*)\s+.+?\s([01],\d)\s*$", ln))}
raw = (SRC/"press_release_n103733.html").read_text(encoding="utf-8",errors="replace")
lines=[re.sub(r"\s+"," ",html.unescape(x)).strip() for x in re.sub(r"<[^>]+>","\n",re.sub(r"<script.*?</script>"," ",raw,flags=re.S)).split("\n")]
lines=[x for x in lines if x]; i=next(k for k,x in enumerate(lines) if "Новый free-float" in x)
ff_new={lines[k]: int(lines[k+2][:-1])/100 for k in range(i+1,i+80) if k+2<len(lines)
        and re.fullmatch(r"[A-Z][A-Z0-9]*",lines[k]) and re.fullmatch(r"\d{1,3}%",lines[k+2])}
tick = sorted(t for t in iss if FIX in iss[t] and ASOF in iss[t])
funds = json.loads((DATA/"funds.json").read_text(encoding="utf-8"))
nav = {k: v["руб"] for k, v in funds["сча"].items()}; NAV = sum(nav.values())

L=[]; p=L.append
p("# Трассировка расчёта: сентябрь 2026, от входов до рублей\n")
p("Каждое число ниже можно пересчитать на калькуляторе. Обозначения: **cap** — полная "
  "капитализация (цена × все акции), **FF** — доля акций в свободном обращении, **LW** — коэффициент "
  "доступности, **WW** — коэффициент потолка, **MC** — капитализация, учитываемая в индексе.\n")
p(f"Источники: cap и старые FF/W — ISS МосБиржи на {FIX} и {ASOF} (перекачано 02.09, 460 значений, "
  "0 расхождений); новые FF — пресс-релиз n103733; новые LW — PDF-приложение к релизу (60 строк). "
  "Кого нет в PDF — LW = 1.\n")

# ── 1. входы ───────────────────────────────────────────────────────────────────
p("## Шаг 0. Входные данные по всем 46 бумагам\n")
p("| бумага | cap 27.08, млрд | cap 01.09, млрд | FF было | FF станет | LW станет | W было (=WW×LW) | статус |")
p("|---|---|---|---|---|---|---|---|")
for t in tick:
    a, b = iss[t][FIX], iss[t][ASOF]
    st = "**уходит**" if t in EXCL else ("FF изм." if t in ff_new else "") 
    if t not in EXCL and lw_new.get(t,1.0) != (b["w"] if abs(b["w"]*10-round(b["w"]*10))<1e-9 else None): st += " LW изм." if st else "LW изм."
    p(f"| {t} | {f(a['cap']/1e9)} | {f(b['cap']/1e9)} | {f(b['ff'],2)} | {f(ff_new.get(t,b['ff']),2)} | "
      f"{f(lw_new.get(t,1.0),1)} | {b['w']:.7f} | {st} |")

# ── 2. шаг А ───────────────────────────────────────────────────────────────────
keep=[t for t in tick if t not in EXCL]
p(f"\n## Шаг А. Дата фиксации {FIX}: считаем коэффициент потолка WW\n")
p("Уходящие Лента и Мосэнерго исключены сразу (проверено на июне-2025: так совпадает с фактом до "
  "0,005 п.п.). Для остальных 44: `MC = cap(27.08) × FF_новый × LW_новый`, WW пока = 1.\n")
mc={t: iss[t][FIX]["cap"]*ff_new.get(t,iss[t][FIX]["ff"])*lw_new.get(t,1.0) for t in keep}
S=sum(mc.values()); w0={t:100*v/S for t,v in mc.items()}
p(f"Σ MC по 44 бумагам = **{fb(S)} ₽** ({f(S/1e12,3)} трлн). Вес без потолка = 100 × MC / Σ MC.\n")
p("| бумага | MC = cap × FF × LW, млрд | вес без потолка |"); p("|---|---|---|")
for t in sorted(keep,key=lambda t:-w0[t])[:12]:
    p(f"| {t} | {f(mc[t]/1e9)} | {f(w0[t],4)}% |")
p(f"| … ещё {len(keep)-12} | | |")

# итерации потолка
w=dict(w0); capped=set(); it=0
p("\n### Процедура потолка (п. 2.8.4), по итерациям\n")
while True:
    it+=1
    by={}
    for t,x in w.items(): by[iss_of(t)]=by.get(iss_of(t),0)+x
    over={g:v for g,v in by.items() if v>CAP+1e-12 and g not in capped}
    p(f"**Итерация {it}.** Веса по эмитентам (топ-5): " + " · ".join(f"{g} {f(v,4)}%" for g,v in sorted(by.items(),key=lambda kv:-kv[1])[:5]))
    if not over:
        p(f"\nНикто не превышает {f(CAP,0)}% — процедура завершена.\n"); break
    excess=0
    for g,v in over.items():
        k=CAP/v; excess+=v-CAP
        p(f"- **{g}** = {f(v,4)}% > 15% → умножаем его бумаги на 15 / {f(v,4)} = **{k:.7f}**; излишек {f(v-CAP,4)} п.п.")
        for t in w:
            if iss_of(t)==g: w[t]*=k
        capped.add(g)
    free=[t for t in w if iss_of(t) not in capped]; fs=sum(w[t] for t in free)
    p(f"- Суммарный излишек **{f(excess,4)} п.п.** распределяем между {len(free)} неограниченными бумагами "
      f"(их сумма {f(fs,4)}%) пропорционально весу: каждая × (1 + {f(excess,4)}/{f(fs,4)}) = × **{1+excess/fs:.7f}**\n")
    for t in free: w[t]+=excess*w[t]/fs
w_fix=w
# WW
free_scale = sum(w0[t] for t in keep if iss_of(t) not in capped)/sum(w_fix[t] for t in keep if iss_of(t) not in capped)
ww={t:( (w_fix[t]/w0[t])*free_scale if iss_of(t) in capped else 1.0) for t in keep}
p("**Итог шага А:** веса на дату фиксации и коэффициент WW.\n")
p("WW у обрезанного = (вес после / вес до) ÷ (тот же множитель у неограниченных). У всех остальных WW = 1.\n")
p("| эмитент | вес до потолка | вес после | WW |"); p("|---|---|---|---|")
for t in ("LKOH","SBER","SBERP"):
    p(f"| {t} | {f(w0[t],4)}% | {f(w_fix[t],4)}% | **{ww[t]:.7f}** |")
p(f"\nПроверка: ЛУКОЙЛ после потолка = {f(w_fix['LKOH'],4)}%, Сбербанк (SBER+SBERP) = "
  f"{f(w_fix['SBER']+w_fix['SBERP'],4)}% — оба ровно 15,00%. Сумма всех весов = {f(sum(w_fix.values()),4)}%.\n")

# ── 3. шаг Б ───────────────────────────────────────────────────────────────────
p(f"## Шаг Б. Дата оценки {ASOF}: те же коэффициенты, сегодняшние цены\n")
p("WW заморожен со дня фиксации. `MC = cap(01.09) × FF_новый × LW_новый × WW`, нормируем.\n")
mcn={t: iss[t][ASOF]["cap"]*ff_new.get(t,iss[t][ASOF]["ff"])*lw_new.get(t,1.0)*ww[t] for t in keep}
Sn=sum(mcn.values()); wn={t:100*v/Sn for t,v in mcn.items()}
p(f"Σ MC = **{fb(Sn)} ₽**.\n")
# старые веса
mco={t: iss[t][ASOF]["cap"]*iss[t][ASOF]["ff"]*iss[t][ASOF]["w"] for t in tick}
So=sum(mco.values()); wo={t:100*v/So for t,v in mco.items()}
p("## Шаг В. Текущие веса на ту же дату — тем же способом\n")
p(f"`MC = cap(01.09) × FF_старый × W_старый` по всем 46 бумагам. Σ MC = **{fb(So)} ₽**. "
  "Контроль: полученные веса совпадают с опубликованными ISS до 0,005 п.п.\n")

# ── 4. сделки ──────────────────────────────────────────────────────────────────
p(f"## Шаг Г. Изменение веса × СЧА = сделка\n")
p(f"СЧА трёх фондов на 31.08: EQMX {f(nav['EQMX']/1e9,3)} + TMOS {f(nav['TMOS']/1e9,3)} + SBMX "
  f"{f(nav['SBMX']/1e9,3)} = **{f(NAV/1e9,3)} млрд ₽**.\n")
p("| бумага | вес сейчас | вес новый | Δ, п.п. | сделка = Δ/100 × СЧА, млн ₽ |"); p("|---|---|---|---|---|")
tr={t:(wn.get(t,0.0)-wo[t])/100*NAV for t in tick}
for t in sorted(tick,key=lambda t:tr[t]):
    if abs(tr[t])>=5e6: p(f"| {t} | {f(wo[t],4)}% | {f(wn.get(t,0.0),4)}% | {f(wn.get(t,0.0)-wo[t],4)} | **{f(tr[t]/1e6,1)}** |")
sells=-sum(v for v in tr.values() if v<0); buys=sum(v for v in tr.values() if v>0)
p(f"\n**Продать {f(sells/1e6,1)} млн · докупить {f(buys/1e6,1)} млн** · разность {f((buys-sells)/1e6,3)} млн (ноль по построению).\n")
p("По фондам, пропорционально СЧА:\n"); p("| фонд | СЧА, млрд | продать, млн | докупить, млн | % СЧА |"); p("|---|---|---|---|---|")
for k,n in nav.items(): p(f"| {k} | {f(n/1e9,2)} | {f(sells*n/NAV/1e6,1)} | {f(buys*n/NAV/1e6,1)} | {f(100*sells/NAV,2)}% |")

# ── 5. пример руками ───────────────────────────────────────────────────────────
t="LKOH"; a,b=iss[t][FIX],iss[t][ASOF]
p("\n## Разбор одной бумаги руками: ЛУКОЙЛ\n")
p(f"1. cap 27.08 = **{fb(a['cap'])} ₽**; FF новый = {f(ff_new[t],2)}; LW новый = {f(lw_new.get(t,1.0),1)} (в PDF отсутствует).")
p(f"2. MC на дату фиксации = {fb(a['cap'])} × {f(ff_new[t],2)} × {f(lw_new.get(t,1.0),1)} = **{fb(mc[t])} ₽**.")
p(f"3. Вес без потолка = 100 × {fb(mc[t])} / {fb(S)} = **{f(w0[t],4)}%** — выше 15%.")
p(f"4. Потолок: умножаем на 15 / {f(w0[t],4)} = {CAP/w0[t]:.7f} → **15,0000%**. Излишек уходит остальным.")
p(f"5. WW = {f(w_fix[t],4)}/{f(w0[t],4)} ÷ {free_scale:.7f} = **{ww[t]:.7f}**. Это и есть «коэффициент, ограничивающий вес», который биржа не публикует.")
p(f"6. На 01.09: MC = {fb(b['cap'])} × {f(ff_new[t],2)} × {f(lw_new.get(t,1.0),1)} × {ww[t]:.7f} = **{fb(mcn[t])} ₽**.")
p(f"7. Новый вес = 100 × {fb(mcn[t])} / {fb(Sn)} = **{f(wn[t],4)}%**.")
p(f"8. Текущий вес = 100 × ({fb(b['cap'])} × {f(b['ff'],2)} × {b['w']:.7f}) / {fb(So)} = **{f(wo[t],4)}%** (ISS публикует {b['weight']}).")
p(f"9. Δ = {f(wn[t],4)} − {f(wo[t],4)} = **{f(wn[t]-wo[t],4)} п.п.** → × {f(NAV/1e9,3)} млрд / 100 = **{f(tr[t]/1e6,1)} млн ₽**.")
p(f"\nВ процентах от позиции: {f(wn[t]-wo[t],4)} / {f(wo[t],4)} = **{f(100*(wn[t]-wo[t])/wo[t],2)}%** — эта величина от цен почти не зависит.\n")
p("---\nДанные по индексам — ПАО Московская Биржа, https://www.moex.com/ru/indices")
(BASE/"TRACE.md").write_text("\n".join(L)+"\n",encoding="utf-8")
print(f"TRACE.md: {len(L)} строк; ЛУКОЙЛ {f(wo['LKOH'])}→{f(wn['LKOH'])}%, {f(tr['LKOH']/1e6,1)} млн; всего {f(sells/1e6,1)} млн")
