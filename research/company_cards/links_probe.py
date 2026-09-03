#!/usr/bin/env python
"""Проверка всех ?field= ссылок Вадима: работает ли, какой заголовок показателя, сколько компаний."""
import urllib.request, re, html, json, time, pathlib
UA={"User-Agent":"Mozilla/5.0 FrameResearch/1.0"}
FIELDS_1=["oil_production","oil_refining","gas_production","operating_income","net_income_ns","ocf","capex",
 "dividend_payout","dividend","div_yield","div_payout_ratio","opex","amortization","employment_expenses",
 "interest_expenses","net_assets","common_share","number_of_shares","free_float","market_cap","ev","book_value",
 "fcf_share","bv_share","fcf_yield","roe","roa","p_fcf","employees","labour_productivity","expenses_per_employee",
 "r_and_d_capex","capex_revenue"]
FIELDS_2=["oil_production","oil_refining","revenue","ebitda","net_income","cost_of_production","assets","debt",
 "cash","net_debt","ebitda_margin","net_margin","p_e","p_s","p_bv","ev_ebitda","debt_ebitda"]
def get(u):
    for i in range(3):
        try: return urllib.request.urlopen(urllib.request.Request(u,headers=UA),timeout=40).read().decode("utf-8","replace")
        except Exception:
            if i==2: raise
            time.sleep(2)
def probe(page, field):
    raw=get(f"https://smart-lab.ru/q/{page}/?field={field}")
    t=re.search(r"<title>(.*?)</title>",raw,flags=re.S)
    title=re.sub(r"\s+"," ",html.unescape(t.group(1))).strip() if t else ""
    ths=[re.sub(r"\s+"," ",re.sub(r"<[^>]+>","",html.unescape(x))).strip() for x in re.findall(r"<th[^>]*>(.*?)</th>",raw,flags=re.S)]
    col=next((x for x in ths if x and x not in ("№","Название","Тикер","отчет","Изм. %, г/г","")),"")
    n=len(set(re.findall(r"<td[^>]*>\s*<a[^>]*href=\"/q/([A-Z0-9]{2,6})/",raw)))
    tot=re.search(r"Всего:.{0,200}?([\d\s.,-]{2,})",re.sub(r"<[^>]+>"," ",raw),flags=re.S)
    return {"field":field,"page":page,"title":title[:80],"column":col,"tickers":n,
            "total":re.sub(r"\s+"," ",tot.group(1)).strip()[:20] if tot else None}
out=[]
for page,fields in (("shares_fundamental",FIELDS_1),("shares_fundamental2",FIELDS_2)):
    for f in fields:
        try: r=probe(page,f); print(f"{page:20} {f:22} {r['column'][:34]:36} бумаг {r['tickers']:>3}",flush=True)
        except Exception as e: r={"field":f,"page":page,"error":str(e)[:60]}; print(f"{page:20} {f:22} ОШИБКА {r['error']}",flush=True)
        out.append(r); time.sleep(0.4)
pathlib.Path("field_codes.json").write_text(json.dumps(out,ensure_ascii=False,indent=1),encoding="utf-8")
ok=[r for r in out if not r.get("error") and r.get("column")]
print(f"\nработают: {len(ok)}/{len(out)}; уникальных показателей: {len({r['column'] for r in ok})}")
