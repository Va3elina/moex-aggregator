"""Замер: что второй мозг даёт кандидату в пост — и покрывает ли он то, что агент спрашивал у таблиц."""
import importlib.util, sys, time, json
sys.path.insert(0, "/app")
spec = importlib.util.spec_from_file_location("brain_probe", "/tmp/brain_measure_s14/brain.py"); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
from sqlalchemy import text
from api.database import SessionLocal
db = SessionLocal()
# соответствие «источник в следе агента» → «кольцо мозга»
СООТВ = {"world_facts": {"владеет", "владеет_долей", "факт_о"}, "news_archive": {"упоминает"}, "company_metrics": set(), "judge": None, "writer": None}
кандидаты = db.execute(text("""
    SELECT id, status, tickers, headline, created_at::date FROM content_candidates
     WHERE cardinality(tickers) > 0 AND (id IN (1638, 1703, 1727) OR created_at > NOW() - INTERVAL '21 days')
     ORDER BY id DESC LIMIT 40""")).all()
строки = []; сумм = {"кандидатов": 0, "с_узлом": 0, "с_сектором": 0, "со_связями": 0, "с_фондами": 0, "с_новостями14": 0, "с_прошлыми_кандидатами30": 0, "мс": []}
for cid, status, tickers, headline, dt in кандидаты:
    сумм["кандидатов"] += 1
    t0 = time.time()
    якоря = {"сектор": set(), "связи": set(), "фонды": 0, "новостей14": 0, "кандидатов30": 0, "аномалий30": 0, "документов": 0}
    узлы = 0
    for t in tickers:
        r = m.поиск(q=t, kind="company", limit=1, candidate_id=None, db=db, _who="admin")["найдено"]
        if not r: continue
        узлы += 1; nid = r[0]["id"]
        if r[0]["данные"] and r[0]["данные"].get("sector"): якоря["сектор"].add(r[0]["данные"]["sector"])
        for k in ("владеет", "владеет_долей"):
            for s in m.соседи(id=nid, kind=k, since=None, limit=20, offset=0, candidate_id=None, db=db, _who="admin")["соседи"]:
                якоря["связи"].add(s["заголовок"][:30])
        якоря["фонды"] += m.соседи(id=nid, kind="держит", since=None, limit=1, offset=0, candidate_id=None, db=db, _who="admin")["всего"]
        якоря["новостей14"] += m.соседи(id=nid, kind="упоминает", since=14, limit=1, offset=0, candidate_id=None, db=db, _who="admin")["всего"]
        якоря["кандидатов30"] += max(0, m.соседи(id=nid, kind="о", since=30, limit=1, offset=0, candidate_id=None, db=db, _who="admin")["всего"] - 1)
        якоря["аномалий30"] += m.соседи(id=nid, kind="аномалия_по", since=30, limit=1, offset=0, candidate_id=None, db=db, _who="admin")["всего"]
        якоря["документов"] += m.соседи(id=nid, kind="отчитался", since=None, limit=1, offset=0, candidate_id=None, db=db, _who="admin")["всего"]
    ms = (time.time() - t0) * 1000; сумм["мс"].append(ms)
    if узлы: сумм["с_узлом"] += 1
    if якоря["сектор"]: сумм["с_сектором"] += 1
    if якоря["связи"]: сумм["со_связями"] += 1
    if якоря["фонды"]: сумм["с_фондами"] += 1
    if якоря["новостей14"]: сумм["с_новостями14"] += 1
    if якоря["кандидатов30"]: сумм["с_прошлыми_кандидатами30"] += 1
    след = db.execute(text("SELECT source, outcome, question FROM agent_trace WHERE candidate_id = :c ORDER BY seq"), {"c": cid}).all()
    покрытие = []
    for src, outc, q in след:
        кольца = СООТВ.get(src)
        if кольца is None: continue
        if not кольца: покрытие.append(f"{src}: в карте нет (фундамент — в company_metrics)"); continue
        есть = якоря["связи"] if src == "world_facts" else якоря["новостей14"]
        покрытие.append(f"{src}→{'/'.join(sorted(кольца))}: агент {outc}, мозг {'даёт' if есть else 'пусто'}")
    строки.append({"id": cid, "статус": status, "тикеры": list(tickers), "узлов": узлы, "мс": round(ms), **{k: (sorted(v) if isinstance(v, set) else v) for k, v in якоря.items()}, "след": покрытие})
n = сумм["кандидатов"]
print(json.dumps({"итог": {**{k: v for k, v in сумм.items() if k != "мс"}, "медиана_мс": sorted(сумм["мс"])[len(сумм["мс"])//2] if сумм["мс"] else None}, "кандидаты": строки}, ensure_ascii=False, default=str))
