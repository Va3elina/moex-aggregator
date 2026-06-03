"""
fetch_sber_options_history.py
=============================
Download full historical options chain for SBER (ASSETCODE=SBRF) and the
underlying SBRF futures (SRH/SRM/SRU/SRZ series) from MOEX ISS for the
window 2018-01-01 to 2026-06-03.

Output (in this folder):
    sber_options_history.parquet   (one row per SECID per trading day)
        SECID, TRADEDATE, STRIKE, OPTIONTYPE (C/P), LASTDELDATE (expiry),
        SETTLEPRICE, WAPRICE, VOLUME, OPENPOSITION, NUMTRADES,
        OPEN, HIGH, LOW, CLOSE, VALUE, CHANGE, SHORTNAME, ASSETCODE
        + parquet fallback to *.csv if pyarrow unavailable.

    sber_futures_history.parquet   (one row per SBRF futures contract per day)
        SECID, TRADEDATE, SHORTNAME, ASSETCODE, SETTLEPRICE, WAPRICE,
        OPEN, HIGH, LOW, CLOSE, VOLUME, OPENPOSITION, NUMTRADES

    sber_options_progress.json     (checkpoint — resume from last good day)

API endpoints used:
    options market history (per day, paginated 100/page):
        https://iss.moex.com/iss/history/engines/futures/markets/options/
        securities.json?date=YYYY-MM-DD&iss.meta=off&start=N

    futures market history (per day, paginated):
        https://iss.moex.com/iss/history/engines/futures/markets/forts/
        securities.json?date=YYYY-MM-DD&iss.meta=off&start=N
        (this block already carries ASSETCODE & SHORTNAME)

    security description (used once per new SBRF option SECID to recover
    STRIKE / OPTIONTYPE / LASTDELDATE for already-expired contracts that
    the live securities snapshot no longer lists):
        https://iss.moex.com/iss/securities/{SECID}.json?iss.meta=off

Rate-limiting: ~5 req/s (0.2s sleep) plus exponential-backoff retries.
"""

from __future__ import annotations

import json
import os
import re
import ssl
import sys
import time
import urllib.parse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE = "https://iss.moex.com/iss"
HERE = Path(__file__).resolve().parent

OUT_OPTIONS = HERE / "sber_options_history.parquet"
OUT_OPTIONS_CSV = HERE / "sber_options_history.csv"
OUT_FUTURES = HERE / "sber_futures_history.parquet"
OUT_FUTURES_CSV = HERE / "sber_futures_history.csv"
PROGRESS_FILE = HERE / "sber_options_progress.json"
META_CACHE = HERE / "sber_options_secid_meta.json"

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 fetch_sber_options"
REQ_SLEEP = 0.20            # ~5 req/s
PAGE_SIZE = 100
LOG_EVERY_DAYS = 30

# silence urllib3 warning when verify=False
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass


# ---------------------------------------------------------------------------
# HTTP session with retry adapter
# ---------------------------------------------------------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    retry = Retry(
        total=4,
        backoff_factor=1.5,             # 1.5, 3, 6, 12s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = make_session()


def get_json(url: str) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=45, verify=False)
            if r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}")
            # ISS sometimes uses BOM
            return json.loads(r.content.decode("utf-8-sig"))
        except Exception as exc:
            last_exc = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"GET failed after retries: {url}  ({last_exc})")


def block_rows(block: dict | None) -> list[dict]:
    if not block:
        return []
    cols = block.get("columns") or []
    return [dict(zip(cols, row)) for row in block.get("data", [])]


# ---------------------------------------------------------------------------
# Per-SECID metadata: STRIKE / OPTIONTYPE / LASTDELDATE / SHORTNAME
# ---------------------------------------------------------------------------
# We parse the SHORTNAME because per-secid fetches are slow at scale.
# Two formats on SBER:
#   Margin on futures (the standard SBRF option for VRP work, all years):
#       "SBRF-3.24M200324CA10000"  ->  base "SBRF-3.24", M=margin,
#       expiry YYMMDD=200324, CA=Call American (PA=Put American),
#       strike=10000 (units = futures price; SBER futures lot=100 shares).
#   Premium European on stock (started appearing ~2025):
#       "SBERP200324CE100"  ->  expiry 200324, CE=Call European, strike=100.

_SHORT_RE_FUT = re.compile(
    r"^SBRF-[\d.]+M(\d{6})([CP])([AE])(\d+(?:\.\d+)?)$"
)
_SHORT_RE_STK = re.compile(r"^SBERP(\d{6})(C|P)E(\d+(?:\.\d+)?)$")


def parse_shortname(shortname: str) -> dict | None:
    if not shortname:
        return None
    m = _SHORT_RE_FUT.match(shortname)
    if m:
        yymmdd, cp, ae, strike = m.groups()
        yy = int(yymmdd[:2]); mm = int(yymmdd[2:4]); dd = int(yymmdd[4:6])
        return {
            "STRIKE": float(strike),
            "OPTIONTYPE": cp,
            "EXERCISE": ae,                 # A=American, E=European
            "UNDERLYINGTYPE": "F",          # futures
            "LASTDELDATE": f"{2000+yy:04d}-{mm:02d}-{dd:02d}",
        }
    m = _SHORT_RE_STK.match(shortname)
    if m:
        yymmdd, cp, strike = m.groups()
        yy = int(yymmdd[:2]); mm = int(yymmdd[2:4]); dd = int(yymmdd[4:6])
        return {
            "STRIKE": float(strike),
            "OPTIONTYPE": cp,
            "EXERCISE": "E",
            "UNDERLYINGTYPE": "S",          # stock
            "LASTDELDATE": f"{2000+yy:04d}-{mm:02d}-{dd:02d}",
        }
    return None


def load_meta_cache() -> dict[str, dict]:
    if META_CACHE.exists():
        try:
            return json.loads(META_CACHE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_meta_cache(cache: dict[str, dict]) -> None:
    META_CACHE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")


def fetch_secid_meta(secid: str) -> dict:
    """Look up STRIKE/OPTIONTYPE/LASTDELDATE via the securities description
    endpoint (works for delisted contracts too)."""
    url = f"{BASE}/securities/{urllib.parse.quote(secid)}.json?iss.meta=off"
    try:
        d = get_json(url)
    except Exception:
        return {"SHORTNAME": None}
    desc_rows = block_rows(d.get("description"))
    by_name = {r["name"]: r["value"] for r in desc_rows}
    shortname = by_name.get("SHORTNAME")
    out: dict[str, Any] = {"SHORTNAME": shortname}
    parsed = parse_shortname(shortname) if shortname else None
    if parsed:
        out.update(parsed)
    elif "STRIKE" in by_name:
        # rare fallback
        try:
            out["STRIKE"] = float(by_name["STRIKE"])
        except Exception:
            pass
        out["OPTIONTYPE"] = by_name.get("OPTIONTYPE")
        out["LASTDELDATE"] = by_name.get("LSTTRADE") or by_name.get("LASTDELDATE")
    time.sleep(REQ_SLEEP)
    return out


# ---------------------------------------------------------------------------
# Trading-day iterator (Mon-Fri; daily endpoint returns empty on holidays)
# ---------------------------------------------------------------------------
def trading_days(start: date, end: date) -> list[str]:
    out: list[str] = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # 0=Mon..4=Fri
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


# ---------------------------------------------------------------------------
# Page-through one full day of options
# ---------------------------------------------------------------------------
def fetch_options_day(date_str: str, assetcode: str = "SBRF") -> list[dict]:
    """Pull all option rows for a given trading day, server-side filtered by
    ASSETCODE (much faster than client-side filter — ~15 pages vs ~350)."""
    rows: list[dict] = []
    start = 0
    while True:
        url = (
            f"{BASE}/history/engines/futures/markets/options/securities.json"
            f"?iss.meta=off&assetcode={assetcode}&date={date_str}&start={start}"
        )
        d = get_json(url)
        chunk = block_rows(d.get("history"))
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(REQ_SLEEP)
    return rows


def fetch_futures_day(date_str: str, assetcode: str = "SBRF") -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        url = (
            f"{BASE}/history/engines/futures/markets/forts/securities.json"
            f"?iss.meta=off&assetcode={assetcode}&date={date_str}&start={start}"
        )
        d = get_json(url)
        chunk = block_rows(d.get("history"))
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(REQ_SLEEP)
    return rows


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def write_table(rows: list[dict], parquet_path: Path, csv_path: Path) -> Path:
    import pandas as pd
    df = pd.DataFrame(rows)
    try:
        df.to_parquet(parquet_path, index=False)
        return parquet_path
    except Exception:
        df.to_csv(csv_path, index=False)
        return csv_path


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"last_done": None, "option_rows": 0, "futures_rows": 0}


def save_progress(p: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(p, ensure_ascii=False, indent=2),
                             encoding="utf-8")


def append_jsonl(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")


def run(start_str: str, end_str: str, label: str = "main") -> dict:
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()
    days = trading_days(start, end)

    # Per-window staging files (jsonl, streamed) to survive crashes.
    stage_opts = HERE / f"sber_options_stage_{label}.jsonl"
    stage_futs = HERE / f"sber_futures_stage_{label}.jsonl"

    progress = load_progress()
    resume_after = progress.get("last_done") if label == "main" else None
    if resume_after and label == "main":
        days = [d for d in days if d > resume_after]
        print(f"[resume] skipping through {resume_after}, "
              f"{len(days)} trading days left")

    meta = load_meta_cache()
    opt_total = progress.get("option_rows", 0) if label == "main" else 0
    fut_total = progress.get("futures_rows", 0) if label == "main" else 0
    empty_days: list[str] = []

    t0 = time.time()
    for i, day in enumerate(days, 1):
        try:
            opts = fetch_options_day(day)
            futs = fetch_futures_day(day)
        except Exception as exc:
            print(f"[error] {day}: {exc} — pausing 10s and continuing")
            time.sleep(10)
            continue

        # Server-side filter is by ASSETCODE=SBRF so every row is SBER.  Enrich
        # with metadata (STRIKE / OPTIONTYPE / LASTDELDATE) from per-SECID
        # description lookups, cached on disk.
        sber_opts: list[dict] = []
        for r in opts:
            sid = r.get("SECID") or ""
            if not sid:
                continue
            if sid not in meta:
                meta[sid] = fetch_secid_meta(sid)
            m = meta[sid] or {}
            r2 = dict(r)
            r2["SHORTNAME"] = m.get("SHORTNAME")
            r2["STRIKE"] = m.get("STRIKE")
            r2["OPTIONTYPE"] = m.get("OPTIONTYPE")
            r2["LASTDELDATE"] = m.get("LASTDELDATE")
            r2["EXERCISE"] = m.get("EXERCISE")
            r2["UNDERLYINGTYPE"] = m.get("UNDERLYINGTYPE")
            r2["ASSETCODE"] = "SBRF"
            sber_opts.append(r2)

        # Futures endpoint already returns ASSETCODE in rows; assetcode filter
        # in URL above only loosely narrows the set so re-filter just in case.
        sber_futs = [r for r in futs if (r.get("ASSETCODE") == "SBRF")]

        append_jsonl(stage_opts, sber_opts)
        append_jsonl(stage_futs, sber_futs)

        opt_total += len(sber_opts)
        fut_total += len(sber_futs)
        if not sber_opts and not sber_futs:
            empty_days.append(day)

        if label == "main":
            progress.update(
                last_done=day,
                option_rows=opt_total,
                futures_rows=fut_total,
                empty_days=empty_days,
            )
            save_progress(progress)
            if i % 10 == 0:
                save_meta_cache(meta)

        if i % LOG_EVERY_DAYS == 0:
            dt = time.time() - t0
            rate = i / dt if dt else 0
            print(f"[{label}] {i}/{len(days)} days  ({day})  "
                  f"opts={opt_total} futs={fut_total} cache={len(meta)} "
                  f"{rate:.2f} day/s")

    save_meta_cache(meta)

    summary = {
        "label": label,
        "window": (start_str, end_str),
        "days_processed": len(days),
        "option_rows": opt_total,
        "futures_rows": fut_total,
        "empty_days": empty_days,
        "stage_opts": str(stage_opts),
        "stage_futs": str(stage_futs),
    }
    return summary


def finalise(stage_opts: Path, stage_futs: Path,
             opts_pq: Path, opts_csv: Path,
             futs_pq: Path, futs_csv: Path) -> dict:
    import pandas as pd
    out: dict[str, Any] = {}
    if stage_opts.exists():
        opts = [json.loads(l) for l in stage_opts.read_text(encoding="utf-8").splitlines() if l.strip()]
        path = write_table(opts, opts_pq, opts_csv)
        out["options_path"] = str(path)
        out["options_rows"] = len(opts)
        out["options_size"] = path.stat().st_size
    if stage_futs.exists():
        futs = [json.loads(l) for l in stage_futs.read_text(encoding="utf-8").splitlines() if l.strip()]
        path = write_table(futs, futs_pq, futs_csv)
        out["futures_path"] = str(path)
        out["futures_rows"] = len(futs)
        out["futures_size"] = path.stat().st_size
    return out


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"

    if cmd == "verify":
        # Small-window sanity check (January 2024).
        print(">>> verify: downloading Jan-2024 only")
        summary = run("2024-01-01", "2024-01-31", label="verify")
        stage_o = Path(summary["stage_opts"])
        stage_f = Path(summary["stage_futs"])
        v_opts_pq = HERE / "verify_sber_options.parquet"
        v_opts_csv = HERE / "verify_sber_options.csv"
        v_futs_pq = HERE / "verify_sber_futures.parquet"
        v_futs_csv = HERE / "verify_sber_futures.csv"
        info = finalise(stage_o, stage_f, v_opts_pq, v_opts_csv,
                        v_futs_pq, v_futs_csv)
        print("VERIFY SUMMARY:", json.dumps({**summary, **info}, indent=2,
                                            default=str))

        import pandas as pd
        if "options_path" in info:
            df = pd.read_parquet(info["options_path"]) if info["options_path"].endswith(".parquet") else pd.read_csv(info["options_path"])
            print(f"\nrows={len(df)} unique secids={df['SECID'].nunique()} "
                  f"unique expiries={df['LASTDELDATE'].nunique()}")
            print("Sample 5 rows:")
            print(df.head(5).to_string())
            print("Expiries:", sorted(df["LASTDELDATE"].dropna().unique()))
        if "futures_path" in info:
            df = pd.read_parquet(info["futures_path"]) if info["futures_path"].endswith(".parquet") else pd.read_csv(info["futures_path"])
            print(f"\nFUTURES rows={len(df)} unique secids={df['SECID'].nunique()}")
            print(df.head(5).to_string())

    elif cmd == "full":
        print(">>> full: downloading 2018-01-01 → 2026-06-03")
        summary = run("2018-01-01", "2026-06-03", label="main")
        stage_o = Path(summary["stage_opts"])
        stage_f = Path(summary["stage_futs"])
        info = finalise(stage_o, stage_f, OUT_OPTIONS, OUT_OPTIONS_CSV,
                        OUT_FUTURES, OUT_FUTURES_CSV)
        print("FINAL SUMMARY:", json.dumps({**summary, **info}, indent=2,
                                           default=str))

    elif cmd == "finalise":
        stage_o = HERE / "sber_options_stage_main.jsonl"
        stage_f = HERE / "sber_futures_stage_main.jsonl"
        info = finalise(stage_o, stage_f, OUT_OPTIONS, OUT_OPTIONS_CSV,
                        OUT_FUTURES, OUT_FUTURES_CSV)
        print("FINALISE:", json.dumps(info, indent=2, default=str))

    else:
        print("Usage: fetch_sber_options_history.py [verify|full|finalise]")
        sys.exit(1)
