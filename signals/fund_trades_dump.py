"""Данные «Сделок фондов» для карточек бота — запускается ВНУТРИ api-контейнера (код сайта + доступ к Redis/БД):

    docker exec -i <api> python - <команда> ... < signals/fund_trades_dump.py

Считает тем же кодом, что страница /fund-trades (api/routers/fund_trades), но без тирной задержки: бот — служебный.
Печатает одну строку JSON.

  complete             — по разделам (акции, облигации): последний месяц и залит ли он целиком (у каждого фонда
                         раздела с полным срезом прошлого месяца есть полный срез этого — как data_release_scan)
  month CAT YYYY-MM    — сделки раздела за месяц: топ-5 покупок и продаж (млн ₽), число фондов, топ-10 портфеля
  fund TICKER YYYY-MM  — сделки фонда за месяц: топ-3 покупок и продаж
  imported DAYS        — справки, залитые агентом за последние DAYS дней (для сообщений «фонд отчитался»)
"""
import calendar
import json
import sys
from datetime import date

from sqlalchemy import text

import api.routers.fund_trades as T
from api.database import SessionLocal

# бот служебный: без тарифных ограничений гостя (задержка среза, свой набор фондов, произвольный диапазон) —
# иначе гостю подставляется набор «без индексных», а облигационные фонды отбрасываются
T.get_indicator_limits = lambda tier, indicator: {}


def month_end(ym: str) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    return f"{ym}-{calendar.monthrange(y, m)[1]:02d}"


def complete(db) -> dict:
    rows = db.execute(text("""
      WITH snap_ok AS (
        SELECT h.fund_id, h.snapshot_date FROM fund_holdings_history h
        WHERE h.source = ANY(:s) AND h.snapshot_date >= :floor
        GROUP BY h.fund_id, h.snapshot_date
        HAVING (COALESCE(SUM(h.weight), 0) >= :wmin
            OR EXISTS (SELECT 1 FROM scha_jobs sj JOIN funds sf ON sf.ticker = sj.ticker
                       WHERE sf.fund_id = h.fund_id AND sj.status = 'imported'
                         AND sj.period = to_char(h.snapshot_date, 'YYYY-MM'))))
      SELECT to_char(s.snapshot_date, 'YYYY-MM') AS m, f.category, f.ticker
      FROM snap_ok s JOIN funds f ON f.fund_id = s.fund_id
      WHERE f.category IN ('stocks', 'bonds')
      GROUP BY 1, 2, 3
    """), {"s": list(T.MONTHLY_SOURCES), "floor": date(2021, 9, 1), "wmin": T.FT_COMPLETE_WSUM_MIN}).all()
    by: dict = {}
    for m, cat, t in rows:
        by.setdefault(cat, {}).setdefault(m, set()).add(t)
    out = {}
    for cat, months in by.items():
        ms = sorted(months)
        cur = ms[-1]
        prev = months[ms[-2]] if len(ms) > 1 else set()
        missing = sorted(prev - months[cur])
        # последний ПОЛНЫЙ месяц среди трёх свежих: отстающий фонд может донести октябрь, когда другие уже сдали
        # ноябрь — тогда итог октября всё равно должен уйти
        full = None
        for k in range(len(ms) - 1, max(len(ms) - 4, 0), -1):
            if not (months[ms[k - 1]] - months[ms[k]]):
                full = ms[k]
                break
        out[cat] = {"month": cur, "complete": not missing, "have": len(months[cur]), "missing": missing,
                    "complete_month": full}
    return out


SHARES = ("common_share", "preferred_share", "depositary_receipt")


def sec_types(db, isins: list) -> dict:
    rows = db.execute(text("SELECT isin, sec_type FROM securities_ref WHERE isin = ANY(:i)"), {"i": list(isins)}).all()
    return {r[0]: r[1] for r in rows}


def movers(db, cat: str, ym: str, limit: int = 5) -> dict:
    """Топ сделок раздела. У фондов акций — только акции и расписки (ОФЗ и облигации в их сделках есть, но в
    рубрику не идут — как в посте «Сделки фондов»)."""
    # без набора фондов страница берёт только фонды акций; облигационные — явным списком (как пикер на сайте)
    funds = None
    if cat != "stocks":
        funds = ",".join(r[0] for r in db.execute(text("SELECT ticker FROM funds WHERE category = :c"), {"c": cat}))
    r = T.top_movers(period="1m", category=cat, as_of=month_end(ym), range_from=None, range_to=None, manager=None,
                     funds=funds, sort="amount", limit=60, scope="movers", user=None, db=db)
    xs = (r.get("top_accumulated") or []) + (r.get("top_reduced") or [])
    st = sec_types(db, [x["akey"] for x in xs])
    ok = (lambda x: st.get(x["akey"]) in SHARES) if cat == "stocks" else (lambda x: True)
    pick = lambda xs: [{"asset": x["asset_name"], "isin": x["akey"], "amount": x["total_delta_amount"]}  # noqa: E731
                       for x in xs if ok(x)][:limit]
    return {"resolved": (r.get("resolved_month") or "")[:7], "funds": r.get("funds_in_month") or 0,
            "buy": pick(r.get("top_accumulated") or []), "sell": pick(r.get("top_reduced") or [])}


def month(db, cat: str, ym: str) -> dict:
    out = movers(db, cat, ym)
    if cat == "stocks":
        p = T.combined_portfolio(manager=None, funds=None, as_of=month_end(ym), user=None, db=db)
        hs = sorted(p.get("holdings") or [], key=lambda h: -(h.get("weight_rub") or 0))[:40]
        st = sec_types(db, [h.get("isin") or h["akey"] for h in hs])       # только акции — как в посте
        out["portfolio"] = [{"asset": h["asset_name"], "isin": h.get("isin") or h["akey"], "weight": h["weight_rub"]}
                            for h in hs if st.get(h.get("isin") or h["akey"]) in SHARES][:10]
        out["portfolio_month"] = (p.get("resolved_month") or "")[:7]
    return out


def fund(db, ticker: str, ym: str) -> dict:
    y, m = int(ym[:4]), int(ym[5:7])
    prev = f"{y - (m == 1)}-{(m - 2) % 12 + 1:02d}"
    d = T.fund_trades_detail(ticker=ticker, period="1m", range_from=prev, range_to=ym, user=None, db=db)
    diff = [x for x in d.get("diff") or [] if x.get("delta_amount_rub")]
    f = d.get("fund") or {}
    uk = db.execute(text("SELECT uk_id, category FROM funds WHERE ticker = :t"), {"t": ticker}).first()
    if uk:
        f = {**f, "uk_id": f.get("uk_id") or uk[0], "category": f.get("category") or uk[1]}
    if f.get("category") == "stocks":                       # у фонда акций — только акции, как в рубрике
        st = sec_types(db, [x.get("isin") for x in diff if x.get("isin")])
        diff = [x for x in diff if st.get(x.get("isin")) in SHARES]
    buy = sorted([x for x in diff if x["delta_amount_rub"] > 0], key=lambda x: -x["delta_amount_rub"])[:3]
    sell = sorted([x for x in diff if x["delta_amount_rub"] < 0], key=lambda x: x["delta_amount_rub"])[:3]
    pick = lambda xs: [{"asset": x["asset_name"], "isin": x.get("isin"), "amount": x["delta_amount_rub"]} for x in xs]  # noqa: E731
    return {"ticker": ticker, "name": f.get("name"), "uk_id": f.get("uk_id"), "category": f.get("category"),
            "current": d.get("current_snapshot_date"), "previous": d.get("previous_snapshot_date"),
            "buy": pick(buy), "sell": pick(sell)}


def imported(db, days: int) -> list:
    """Справки, залитые агентом за последние days дней. По времени заливки, а не по номеру: агент отвечает не по
    порядку (19.10: #938 залита раньше #937) — курсор по id терял бы фонды (ревью 18.11)."""
    rows = db.execute(text("""
        SELECT j.id, j.ticker, j.period, f.name, f.uk_id, f.category
        FROM scha_jobs j JOIN funds f ON f.ticker = j.ticker
        WHERE j.status = 'imported' AND j.updated_at > now() - make_interval(days => :d) ORDER BY j.updated_at, j.id
    """), {"d": days}).all()
    return [{"id": r[0], "ticker": r[1], "period": r[2], "name": r[3], "uk_id": r[4], "category": r[5]} for r in rows]


if __name__ == "__main__":
    cmd, args = sys.argv[1], sys.argv[2:]
    db = SessionLocal()
    try:
        res = {"complete": lambda: complete(db), "month": lambda: month(db, *args),
               "fund": lambda: fund(db, *args), "imported": lambda: imported(db, int(args[0]))}[cmd]()
    finally:
        db.close()
    print(json.dumps(res, ensure_ascii=False, default=str))
