"""Замер векторов на постах: находит ли смысл компанию без тикера, и похожи ли «похожие»."""
import sys, time, json
sys.path.insert(0, "/app")
from sqlalchemy import text
from api.database import SessionLocal
from api.routers import brain as m
db = SessionLocal()
канд = db.execute(text("""
    SELECT id, headline, tickers, status FROM content_candidates
     WHERE cardinality(tickers) > 0 AND headline IS NOT NULL AND created_at > NOW() - INTERVAL '60 days'
     ORDER BY id DESC LIMIT 120""")).all()
карта = {r[0]: r[1] for r in db.execute(text("SELECT ticker, company_id FROM brain_ticker_map")).all()}
итог = {"кандидатов": 0, "топ1": 0, "топ3": 0, "топ5": 0, "похожие_с_тем_же_тикером_доля": [], "мс": []}
примеры = []
for cid, headline, tickers, status in канд:
    цели = {карта.get(t) for t in tickers if карта.get(t)}
    if not цели: continue
    итог["кандидатов"] += 1
    t0 = time.time()
    r = m.поиск(q=headline[:300], kind="company", mode="meaning", limit=5, candidate_id=None, db=db, _who="admin")["найдено"]
    итог["мс"].append((time.time()-t0)*1000)
    ids = [x["id"] for x in r]
    hit = [i for i, x in enumerate(ids) if x in цели]
    if hit:
        if hit[0] == 0: итог["топ1"] += 1
        if hit[0] < 3: итог["топ3"] += 1
        итог["топ5"] += 1
    try:
        s = m.похожие(id=f"candidate:{cid}", kind="candidate", limit=10, candidate_id=None, db=db, _who="admin")["похожие"]
        if s:
            же = sum(1 for x in s if set((x["данные"] or {}).get("tickers") or []) & set(tickers))
            итог["похожие_с_тем_же_тикером_доля"].append(же / len(s))
    except Exception as e:
        pass
    if len(примеры) < 8:
        примеры.append({"id": cid, "тикеры": list(tickers), "заголовок": headline[:80], "топ": [(x["id"].split(":")[1], x["почему"]) for x in r[:3]]})
n = итог["кандидатов"]
д = итог["похожие_с_тем_же_тикером_доля"]
print(json.dumps({"кандидатов": n, "компания_в_топ1": итог["топ1"], "компания_в_топ3": итог["топ3"], "компания_в_топ5": итог["топ5"],
                  "похожие_кандидаты_с_тем_же_тикером_средняя_доля": round(sum(д)/len(д), 2) if д else None,
                  "медиана_мс": round(sorted(итог["мс"])[len(итог["мс"])//2]) if итог["мс"] else None, "примеры": примеры}, ensure_ascii=False))
