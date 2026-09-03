#!/usr/bin/env python
"""Разведка smart-lab: что можно забрать и по каким компаниям. Без БД, пишет survey.json + SURVEY.md."""
import urllib.request, re, html, json, time, pathlib, collections, datetime as dt, sys
OUT = pathlib.Path(__file__).resolve().parent; UA = {"User-Agent": "Mozilla/5.0 (Macintosh) FrameResearch/1.0"}
def get(u, tries=3):
    for i in range(tries):
        try:
            r = urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=40); return r.read().decode("utf-8", "replace")
        except Exception as e:
            if i == tries - 1: raise
            time.sleep(3 * (i + 1))
def clean(c): return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html.unescape(c).replace("&nbsp;", " "))).strip()
def rows(raw):
    out = []
    for r in re.findall(r"<tr[^>]*>(.*?)</tr>", raw, flags=re.S):
        cells = [clean(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", r, flags=re.S)]
        if cells: out.append(cells)
    return out
def parse_table(raw):
    """→ (periods, {label: {period: raw_value}}, report_dates)"""
    rs = rows(raw); hdr = next((c for c in rs if any(re.fullmatch(r"20\d\d(Q\d)?", x) for x in c)), None)
    if not hdr: return [], {}, {}
    periods = {k: (x.split()[0]) for k, x in enumerate(hdr) if re.fullmatch(r"20\d\d(Q\d)?|LTM \?|LTM", x)}
    data = {}
    for c in rs:
        if len(c) == len(hdr) + 1 and c[0] and c[0] != hdr[0]:
            data[c[0]] = {periods[k]: c[k + 1] for k in periods if c[k + 1] not in ("", "?")}
    return [periods[k] for k in sorted(periods)], data, data.get("Дата отчета", {})
# 1. вселенная: сводная таблица
raw = get("https://smart-lab.ru/q/shares_fundamental2/"); rs = rows(raw)
universe = []
for c in rs:
    if len(c) >= 6 and re.fullmatch(r"[A-Z0-9]{2,6}", c[2] or ""): universe.append({"ticker": c[2], "name": c[1], "cap": c[3], "report": c[-1]})
seen = set(); universe = [u for u in universe if not (u["ticker"] in seen or seen.add(u["ticker"]))]
print("в сводной таблице тикеров:", len(universe), flush=True)
# наши вселенные
def jget(u): return json.load(urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=60))
imoex = set(); bmi = set()
try:
    for idx, tgt in (("IMOEX", imoex), ("MOEXBMI", bmi)):
        for start in (0, 100, 200):
            d = jget(f"https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/{idx}.json?limit=100&start={start}")["analytics"]
            cols = d["columns"]; [tgt.add(dict(zip(cols, r))["ticker"]) for r in d["data"]]
except Exception as e: print("ISS:", e)
funds_assets = set()
try:
    a = jget("https://xn--80aklbnczmv.xn--p1ai/api/fund-trades/assets"); items = a.get("data", a)["assets"]
    # тикер по ISIN через ISS
    for it in items:
        try:
            s = jget(f"https://iss.moex.com/iss/securities.json?q={it['isin']}&iss.meta=off")["securities"]; cols = s["columns"]
            for r in s["data"]:
                rr = dict(zip(cols, r))
                if rr.get("isin") == it["isin"] and rr.get("is_traded") in (1, "1", True): funds_assets.add(rr["secid"]); break
        except Exception: pass
        time.sleep(0.15)
except Exception as e: print("fund assets:", e)
print("IMOEX", len(imoex), "MOEXBMI", len(bmi), "в фондах", len(funds_assets), flush=True)
# 2. по каждому тикеру
res = []; label_freq = collections.Counter(); t0 = time.time()
for i, u in enumerate(universe, 1):
    t = u["ticker"]; rec = dict(u); rec.update({"imoex": t in imoex, "bmi": t in bmi, "in_funds": t in funds_assets})
    try:
        py, dy, rdy = parse_table(get(f"https://smart-lab.ru/q/{t}/f/y/")); time.sleep(0.5)
        pq, dq, rdq = parse_table(get(f"https://smart-lab.ru/q/{t}/f/q/")); time.sleep(0.5)
        years = [p for p in py if re.fullmatch(r"20\d\d", p)]; quarters = [p for p in pq if "Q" in p]
        filled = {k: v for k, v in dy.items() if any(p in v for p in years)}
        ops = [k for k in dy if k in list(dy)[:list(dy).index("Выручка, млрд руб")] and k not in ("Дата отчета", "Валюта отчета", "Финансовый отчет", "Годовой отчет")] if "Выручка, млрд руб" in dy else [k for k in dy if any(w in k for w in ("Добыча", "Производство", "Выработка", "Продажи", "Перевозки", "Выпуск", "Отгруз"))]
        rec.update({"years": years, "n_years": len(years), "quarters": quarters, "n_quarters": len(quarters),
                    "n_indicators": len(filled), "ops": ops, "last_report": max(rdy.values(), key=lambda s: dt.datetime.strptime(s, "%d.%m.%Y") if re.fullmatch(r"\d\d\.\d\d\.\d{4}", s) else dt.datetime.min) if rdy else None,
                    "has_dividend": any(dy.get("Дивиденд, руб/акцию", {}).values()), "is_bank": "Чист. проц. доходы, млрд руб" in dy,
                    "msfo_empty": len(filled) == 0})
        for k in filled: label_freq[k] += 1
        if rec["msfo_empty"]:
            pr, dr, _ = parse_table(get(f"https://smart-lab.ru/q/{t}/f/y/RSBU/")); time.sleep(0.5)
            rec["rsbu_years"] = [p for p in pr if re.fullmatch(r"20\d\d", p)]; rec["rsbu_indicators"] = sum(1 for k, v in dr.items() if v)
    except Exception as e: rec["error"] = str(e)[:120]
    res.append(rec)
    if i % 25 == 0: print(f"{i}/{len(universe)} за {time.time()-t0:.0f}с", flush=True)
json.dump({"universe": res, "label_freq": label_freq.most_common(), "imoex": sorted(imoex), "bmi": sorted(bmi), "funds": sorted(funds_assets)}, open(OUT / "survey.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
# 3. отчёт
L = []; p = L.append
ok = [r for r in res if not r.get("error")]
p("# Разведка smart-lab: что можно забрать и по каким компаниям\n"); p(f"Снято {dt.date.today()}. Бумаг в сводной таблице: {len(res)}, ошибок при обходе: {len(res)-len(ok)}.\n")
def grp(name, sel):
    rs_ = [r for r in ok if sel(r)]
    if not rs_: return
    deep = sum(1 for r in rs_ if r["n_years"] >= 5); q = sum(1 for r in rs_ if r["n_quarters"] >= 4); e = sum(1 for r in rs_ if r["msfo_empty"]); o = sum(1 for r in rs_ if r["ops"]); d = sum(1 for r in rs_ if r["has_dividend"])
    p(f"| {name} | {len(rs_)} | {deep} | {q} | {o} | {d} | {e} |")
p("## Покрытие по группам\n"); p("| группа | бумаг | ≥5 лет МСФО | ≥4 кварталов | есть операционные | есть дивиденды | МСФО пусто |"); p("|---|---|---|---|---|---|---|")
grp("Индекс МосБиржи (IMOEX)", lambda r: r["imoex"]); grp("Широкий рынок (MOEXBMI)", lambda r: r["bmi"]); grp("Держат наши фонды", lambda r: r["in_funds"]); grp("Все на smart-lab", lambda r: True)
p("\n## Какие показатели встречаются (частота среди бумаг с данными)\n"); p("| показатель | у скольких бумаг |"); p("|---|---|")
for k, n in label_freq.most_common(80): p(f"| {k} | {n} |")
p("\n## По бумагам\n"); p("| тикер | название | IMOEX | BMI | в фондах | лет МСФО | кварталов | показателей | операционные | последний отчёт | банк | МСФО пусто → РСБУ лет |"); p("|---|---|---|---|---|---|---|---|---|---|---|---|")
for r in sorted(ok, key=lambda r: (-r["imoex"], -r["bmi"], -r["in_funds"], r["ticker"])):
    p(f"| {r['ticker']} | {r['name'][:22]} | {'✓' if r['imoex'] else ''} | {'✓' if r['bmi'] else ''} | {'✓' if r['in_funds'] else ''} | {r['n_years']} ({r['years'][0] if r['years'] else ''}–{r['years'][-1] if r['years'] else ''}) | {r['n_quarters']} | {r['n_indicators']} | {', '.join(x.split(',')[0] for x in r['ops'][:3])} | {r['last_report'] or ''} | {'банк' if r['is_bank'] else ''} | {('РСБУ ' + str(len(r.get('rsbu_years', [])))) if r['msfo_empty'] else ''} |")
bad = [r for r in res if r.get("error")]
if bad: p("\nОшибки: " + ", ".join(f"{r['ticker']} ({r['error'][:40]})" for r in bad))
(OUT / "SURVEY.md").write_text("\n".join(L) + "\n", encoding="utf-8"); print("готово:", OUT / "SURVEY.md")
