"""Сухой прогон: что агент получил бы из мозга по реальным кандидатам — до правки промптов."""
import sys, json, time
sys.path.insert(0, "/app")
from sqlalchemy import text
from api.database import SessionLocal
from api.routers import brain as m
db = SessionLocal()
канд = db.execute(text("""
    SELECT id, status, tickers, headline, created_at::date FROM content_candidates
     WHERE cardinality(tickers) > 0 AND (id IN (1638, 1703, 1727) OR status IN ('draft_ready','pending','in_review') OR created_at > NOW() - INTERVAL '10 days')
     ORDER BY id DESC LIMIT 14""")).all()
out = []
for cid, status, tickers, headline, dt in канд:
    t = tickers[0]
    try:
        t0 = time.time(); c = m.контекст(ticker=t, days=14, candidate_id=None, db=db, _who="admin"); ms = (time.time() - t0) * 1000
    except Exception as e:
        out.append({"id": cid, "тикер": t, "ошибка": str(e)[:80]}); continue
    def эл(k, n=4, поле="заголовок"):
        return [(x[поле][:60], x.get("уровень"), x.get("вес"), x.get("на_дату")) for x in c[k]["элементы"][:n]]
    след = [(r[0], r[1], r[2][:50]) for r in db.execute(text("SELECT source, outcome, question FROM agent_trace WHERE candidate_id=:c ORDER BY seq"), {"c": cid}).all()]
    out.append({"id": cid, "статус": status, "тикеры": list(tickers), "заголовок": headline[:90], "мс": round(ms),
                "сектор": [x[0] for x in эл("сектор", 1)], "владеет": эл("владеет", 4), "владельцы": эл("владельцы", 4),
                "фондов": len(c["фонды_держатели"]["элементы"]), "топ_фонды": [(x[0], x[2]) for x in эл("фонды_держатели", 3)],
                "индексы": [x[0] for x in эл("индексы", 3)], "новостей14": len(c["новости"]["элементы"]), "новости": [x[0] for x in эл("новости", 3)],
                "кандидатов60": len(c["кандидаты"]["элементы"]), "аномалий60": len(c["аномалии"]["элементы"]), "аномалии": [x[0] for x in эл("аномалии", 2)],
                "вместе": [x[0] for x in эл("вместе_в_новостях", 4)], "след_агента": след})
print(json.dumps(out, ensure_ascii=False, default=str))
