"""Разовая выгрузка истории облигаций из фондов с MOEX ISS → bond_history / bond_ref / bond_events.

Запускать С МАКА (не с сервера): бан IP сервера кладёт все фетчеры МосБиржи, а предупреждений перед
баном ISS не даёт (memory: moex_ip_ban). Один поток, ~1 запрос в секунду, каждая страница — в кэш на
диск: прерванный прогон продолжается с того же места, повторный ничего не перекачивает.

    python3 research/bonds_iss/backfill.py list                  # ISIN облигаций из фондов (прод, только чтение)
    python3 research/bonds_iss/backfill.py fetch --only RU000A1014J2,RU000A10D533   # проба
    python3 research/bonds_iss/backfill.py fetch                 # всё (~1 150 бумаг, ~6–7 ч)
    python3 research/bonds_iss/backfill.py load [--only …]       # кэш → прод (UPSERT)

По каждой бумаге три вида запросов:
  history/engines/stock/markets/bonds/securities/<SECID>.json   (по 100 строк, все режимы торгов)
  securities/<SECID>.json?iss.only=description,boards                               (справочник выпуска)
  statistics/engines/stock/markets/bonds/bondization/<SECID>.json?limit=unlimited   (купоны/амортизации/оферты)
ISIN → SECID: securities_ref.secid (у ОФЗ свой SECID вида SU26254RMFS1), иначе поиск securities.json?q=ISIN.
Ежедневное обновление — отдельный шаг оркестратора (не этот скрипт).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ISS = "https://iss.moex.com/iss"
CACHE = Path.home() / ".cache/frame/bonds_iss"
UA = {"User-Agent": "frame-bonds-backfill/1.0"}
PAUSE = 1.0
SSH = ["ssh", "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "ConnectTimeout=30",
       "-i", str(Path.home() / ".ssh/id_ed25519"), "root@103.88.243.232"]
BOND_TYPES = ("exchange_bond", "corporate_bond", "ofz_bond", "subfederal_bond", "municipal_bond", "cb_bond")

HIST_COLS = ["BOARDID", "TRADEDATE", "SHORTNAME", "SECID", "NUMTRADES", "VALUE", "LOW", "HIGH", "CLOSE",
             "LEGALCLOSEPRICE", "ACCINT", "WAPRICE", "YIELDCLOSE", "OPEN", "VOLUME", "MARKETPRICE2", "MARKETPRICE3",
             "ADMITTEDQUOTE", "MP2VALTRD", "MARKETPRICE3TRADESVALUE", "ADMITTEDVALUE", "MATDATE", "DURATION",
             "YIELDATWAP", "IRICPICLOSE", "BEICLOSE", "COUPONPERCENT", "COUPONVALUE", "BUYBACKDATE", "LASTTRADEDATE",
             "FACEVALUE", "CURRENCYID", "CBRCLOSE", "YIELDTOOFFER", "YIELDLASTCOUPON", "OFFERDATE", "FACEUNIT",
             "TRADINGSESSION", "CALLOPTIONDATE", "CALLOPTIONYIELD", "CALLOPTIONDURATION", "PUTOPTIONDATE",
             "DATEYIELDFROMISSUER", "TRADE_SESSION_DATE", "ZSPREAD", "ZSPREADATWAPRICE", "BONDTYPE", "BONDSUBTYPE",
             "COUPON_DETAILS", "FACEVALUE_TYPE"]


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


_last = [0.0]
_fails = [0]


def get(url: str):
    """Один запрос к ISS с паузой PAUSE от предыдущего. 5 сбоев подряд — стоп (похоже на бан/блэкхол)."""
    for attempt in range(3):
        wait = PAUSE - (time.monotonic() - _last[0])
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.monotonic()
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=40) as r:
                _fails[0] = 0
                return json.load(r)
        except Exception as e:  # noqa: BLE001
            _fails[0] += 1
            log(f"  сбой ({attempt + 1}/3): {type(e).__name__}: {str(e)[:100]}")
            if _fails[0] >= 5:
                sys.exit("5 сбоев подряд — останавливаюсь (возможен бан ISS). Перезапуск продолжит с кэша.")
            time.sleep(10 * (attempt + 1))
    raise RuntimeError(url)


def cached(path: Path, url: str):
    if path.exists():
        return json.loads(path.read_text())
    d = get(url)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(d, ensure_ascii=False))
    return d


def psql(sql: str) -> str:
    r = subprocess.run(SSH + ["docker exec -i frame-db-1 psql -v ON_ERROR_STOP=1 -U postgres -d moex_db -At -F'\t'"],
                       input=sql, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"psql: {r.stderr.strip()[-800:]}")
    return r.stdout


# ── list ──────────────────────────────────────────────────────────────────

def cmd_list() -> None:
    out = psql(f"""
        SELECT h.isin, coalesce(s.secid, ''), coalesce(s.sec_type, '')
        FROM (SELECT DISTINCT isin FROM fund_holdings_history
              WHERE source IN ('interfax_manual','vim_sdr') AND isin ~ '^(RU|SU)') h
        LEFT JOIN securities_ref s ON s.isin = h.isin
        WHERE s.sec_type IN ({','.join(repr(t) for t in BOND_TYPES)}) OR s.sec_type IS NULL
        ORDER BY 1;
    """)
    items = [dict(zip(("isin", "secid", "sec_type"), ln.split("\t"))) for ln in out.splitlines() if ln.strip()]
    CACHE.mkdir(parents=True, exist_ok=True)
    (CACHE / "isins.json").write_text(json.dumps(items, ensure_ascii=False, indent=0))
    no_secid = sum(1 for i in items if not i["secid"])
    log(f"облигаций в фондах: {len(items)} (без SECID в securities_ref: {no_secid}) → {CACHE / 'isins.json'}")


def resolve_secid(item: dict) -> str | None:
    if item["secid"]:
        return item["secid"]
    p = CACHE / "q" / f"{item['isin']}.json"
    d = cached(p, f"{ISS}/securities.json?iss.meta=off&q={item['isin']}")
    rows = [dict(zip(d["securities"]["columns"], r)) for r in d["securities"]["data"]]
    rows = [r for r in rows if r.get("isin") == item["isin"] and str(r.get("type") or "").endswith("bond")]
    return rows[0]["secid"] if rows else None


# ── fetch ─────────────────────────────────────────────────────────────────

def fetch_one(secid: str) -> int:
    base = CACHE / "sec" / secid
    cached(base / "desc.json", f"{ISS}/securities/{secid}.json?iss.meta=off&iss.only=description,boards")
    cached(base / "bondization.json",
           f"{ISS}/statistics/engines/stock/markets/bonds/bondization/{secid}.json?iss.meta=off&limit=unlimited")
    start, rows = 0, 0
    while True:
        # БЕЗ marketprice_board=1: фильтр отрезает старые режимы (до 2020 облигации шли в EQOB — у ЛСР
        # история начиналась бы с 2020-05 вместо 2017-10). Берём все режимы, режим — часть ключа таблицы.
        d = cached(base / f"ha_{start:05d}.json",
                   f"{ISS}/history/engines/stock/markets/bonds/securities/{secid}.json"
                   f"?iss.meta=off&start={start}")
        n = len(d["history"]["data"])
        rows += n
        cur = dict(zip(d["history.cursor"]["columns"], d["history.cursor"]["data"][0]))
        start += cur["PAGESIZE"]
        if n == 0 or start >= cur["TOTAL"]:
            return rows


def cmd_fetch(only: set[str] | None) -> None:
    items = json.loads((CACHE / "isins.json").read_text())
    if only:
        items = [i for i in items if i["isin"] in only]
    t0, total_rows = time.time(), 0
    for k, item in enumerate(items, 1):
        secid = resolve_secid(item)
        if not secid:
            log(f"[{k}/{len(items)}] {item['isin']}: SECID не найден в ISS — пропуск")
            (CACHE / "missing.txt").open("a").write(item["isin"] + "\n")
            continue
        item["secid"] = secid
        rows = fetch_one(secid)
        total_rows += rows
        if k % 25 == 0 or k == len(items) or only:
            el = time.time() - t0
            log(f"[{k}/{len(items)}] {item['isin']} → {secid}: {rows} дней · всего {total_rows} · {el / 60:.0f} мин")
    # запомнить найденные SECID (для load) — в общем списке, не только в пробной выборке
    allitems = {i["isin"]: i for i in json.loads((CACHE / "isins.json").read_text())}
    for i in items:
        allitems[i["isin"]] = i
    (CACHE / "isins.json").write_text(json.dumps(list(allitems.values()), ensure_ascii=False, indent=0))
    log(f"готово: {len(items)} бумаг, {total_rows} дней истории")


# ── load ──────────────────────────────────────────────────────────────────

def _rows(block: dict) -> list[dict]:
    return [dict(zip(block["columns"], r)) for r in block.get("data") or []]


def _csv(rows: list[list]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    for r in rows:
        w.writerow(["" if v is None else v for v in r])
    return buf.getvalue()


def build(only: set[str] | None):
    items = [i for i in json.loads((CACHE / "isins.json").read_text()) if i.get("secid")]
    if only:
        items = [i for i in items if i["isin"] in only]
    hist, ref, ev = [], [], []
    for it in items:
        base = CACHE / "sec" / it["secid"]
        if not (base / "desc.json").exists():
            continue
        for p in sorted(base.glob("ha_*.json")):
            for r in _rows(json.loads(p.read_text())["history"]):
                hist.append([r.get("SECID"), r.get("TRADEDATE"), r.get("BOARDID"), it["isin"]]
                            + [r.get(c) for c in HIST_COLS if c not in ("SECID", "TRADEDATE", "BOARDID")])
        d = json.loads((base / "desc.json").read_text())
        desc = {r["name"]: r["value"] for r in _rows(d["description"])}
        boards = _rows(d.get("boards") or {"columns": [], "data": []})
        hfrom = min((b["history_from"] for b in boards if b.get("history_from")), default=None)
        htill = max((b["history_till"] for b in boards if b.get("history_till")), default=None)
        b = lambda k: None if desc.get(k) in (None, "") else desc.get(k) == "1"  # noqa: E731
        ref.append([it["isin"], it["secid"], desc.get("SHORTNAME"), desc.get("NAME"), desc.get("REGNUMBER"),
                    desc.get("EMITTER_ID"), desc.get("ISSUEDATE"), desc.get("MATDATE"), desc.get("INITIALFACEVALUE"),
                    desc.get("FACEUNIT"), desc.get("ISSUESIZE"), desc.get("COUPONFREQUENCY"), desc.get("BOND_TYPE"),
                    desc.get("BOND_SUBTYPE"), desc.get("SECSUBTYPE"), desc.get("COUPON_BENCHMARK"),
                    desc.get("COUPON_BENCHMARK_SPREAD"), desc.get("LISTLEVEL"), b("ISQUALIFIEDINVESTORS"),
                    b("SUBORDBOND"), b("HASDEFAULT"), b("HASTECHNICALDEFAULT"), hfrom, htill,
                    json.dumps(desc, ensure_ascii=False)])
        bz = json.loads((base / "bondization.json").read_text())
        for kind, block, dk, vk, pk in (("coupon", "coupons", "coupondate", "value", "valueprc"),
                                        ("amortization", "amortizations", "amortdate", "value", "valueprc"),
                                        ("offer", "offers", "offerdate", "value", "price")):
            seen = set()
            for r in _rows(bz.get(block) or {"columns": [], "data": []}):
                dt = r.get(dk)
                if not dt or dt in seen or dt.startswith("0000"):
                    continue
                seen.add(dt)
                ev.append([it["secid"], kind, dt, it["isin"], r.get(vk), r.get(pk), r.get("value_rub"),
                           r.get("facevalue"), json.dumps(r, ensure_ascii=False)])
    return hist, ref, ev


def cmd_load(only: set[str] | None) -> None:
    hist, ref, ev = build(only)
    log(f"к заливке: история {len(hist)} строк, справочник {len(ref)}, события {len(ev)}")
    hcols = ["secid", "trade_date", "boardid", "isin"] + [c.lower() for c in HIST_COLS
                                                          if c not in ("SECID", "TRADEDATE", "BOARDID")]
    rcols = ["isin", "secid", "shortname", "name", "regnumber", "emitter_id", "issuedate", "matdate",
             "initialfacevalue", "faceunit", "issuesize", "couponfrequency", "bond_type", "bond_subtype",
             "secsubtype", "coupon_benchmark", "coupon_benchmark_spread", "listlevel", "is_qualified", "is_subord",
             "has_default", "has_technical_default", "history_from", "history_till", "description"]
    ecols = ["secid", "kind", "event_date", "isin", "value", "value_prc", "value_rub", "facevalue", "raw"]
    for table, cols, rows, key in (("bond_history", hcols, hist, "secid, trade_date, boardid"),
                                   ("bond_ref", rcols, ref, "isin"),
                                   ("bond_events", ecols, ev, "secid, kind, event_date")):
        for i in range(0, len(rows), 100_000):
            part = rows[i:i + 100_000]
            upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key.split(", "))
            sql = (f"BEGIN;\nCREATE TEMP TABLE t (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP;\n"
                   f"COPY t ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv);\n{_csv(part)}\\.\n"
                   f"INSERT INTO {table} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM t "
                   f"ON CONFLICT ({key}) DO UPDATE SET {upd};\nCOMMIT;\n")
            out = psql(sql)
            log(f"  {table}: {i + len(part)}/{len(rows)} {out.strip().splitlines()[-2:] if out.strip() else ''}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["list", "fetch", "load"])
    ap.add_argument("--only", help="ISIN через запятую")
    a = ap.parse_args()
    only = set(a.only.split(",")) if a.only else None
    {"list": lambda: cmd_list(), "fetch": lambda: cmd_fetch(only), "load": lambda: cmd_load(only)}[a.cmd]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
