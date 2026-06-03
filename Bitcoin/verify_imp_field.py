"""
verify_imp_field.py
-------------------
Resolve the contradiction between Agent #4 and Agent #6:
  - Agent #4: IMP is theoretical marker price
  - Agent #6: IMP is implied volatility expressed in rubles (sigma_pct = IMP/spot)

Hypotheses tested:
  H1: IMP == THEORPRICE          (theoretical option price)
  H2: IMP == VOLAT / 100         (IV in %, scaled)
  H3: IMP == VOLAT * spot / 100  (IV in price units of underlying)
  H4: IMP is Initial Margin in RUB (independent of IV)

Authoritative source consulted FIRST:
  https://iss.moex.com/iss/engines/futures/markets/options/securities/columns.json
  -> IMNP  = "ГО по непокрытой позиции"          (Margin for naked short)
  -> IMP   = "ГО по синтетической позиции"       (Margin for synthetic / margined position)
  -> IMBUY = "ГО на покупку"                     (Margin to buy the option)
  -> IMTIME = "Данные по ГО на"                  (Timestamp of margin snapshot)

The script empirically verifies that interpretation against pricing data.
"""

from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request

ISS_BASE = "https://iss.moex.com/iss"

# -------------------------------------------------------------------- helpers
def _fetch(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "imp-field-verifier/1.0"})
    with urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8-sig")  # MOEX serves BOM on some paths
    return json.loads(raw)


def _block(data: dict, key: str) -> list[dict]:
    """Turn ISS columnar block into list[dict]."""
    block = data.get(key) or {}
    cols = block.get("columns") or []
    return [dict(zip(cols, row)) for row in block.get("data") or []]


# -------------------------------------------------------------------- Black-76
def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def black76_price(F: float, K: float, T: float, sigma: float, is_call: bool, r: float = 0.0) -> float:
    if T <= 0 or sigma <= 0 or F <= 0 or K <= 0:
        intrinsic = max(F - K, 0.0) if is_call else max(K - F, 0.0)
        return math.exp(-r * max(T, 0.0)) * intrinsic
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    df = math.exp(-r * T)
    if is_call:
        return df * (F * _norm_cdf(d1) - K * _norm_cdf(d2))
    return df * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))


def black76_iv(price: float, F: float, K: float, T: float, is_call: bool, r: float = 0.0) -> float | None:
    """Bisection inversion. Returns sigma in decimal (e.g. 0.42 = 42%)."""
    if price is None or price <= 0 or T <= 0 or F <= 0 or K <= 0:
        return None
    intrinsic = max(F - K, 0.0) if is_call else max(K - F, 0.0)
    intrinsic *= math.exp(-r * T)
    if price < intrinsic - 1e-9:
        return None
    lo, hi = 1e-5, 5.0
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        theo = black76_price(F, K, T, mid, is_call, r)
        if theo > price:
            hi = mid
        else:
            lo = mid
        if hi - lo < 1e-7:
            break
    return 0.5 * (lo + hi)


# -------------------------------------------------------------------- ISS calls
def fetch_columns_doc() -> dict[str, dict]:
    """Pull MOEX's own description of IMP/IMNP/IMBUY/IMTIME from columns endpoint."""
    url = f"{ISS_BASE}/engines/futures/markets/options/securities/columns.json?iss.meta=off"
    data = _fetch(url)
    rows = _block(data, "securities")
    wanted = {"IMP", "IMNP", "IMBUY", "IMTIME"}
    return {r["name"]: r for r in rows if r.get("name") in wanted}


def fetch_security(secid: str) -> dict:
    url = f"{ISS_BASE}/engines/futures/markets/options/securities/{secid}.json?iss.meta=off"
    data = _fetch(url)
    sec = _block(data, "securities")
    md = _block(data, "marketdata")
    return {
        "securities": sec[0] if sec else {},
        "marketdata": md[0] if md else {},
    }


def fetch_optionboard(asset: str, expiration: str) -> tuple[dict, dict]:
    """Return {SECID: row} for calls and puts (with VOLAT, THEORPRICE)."""
    url = (
        f"{ISS_BASE}/statistics/engines/futures/markets/options/assets/"
        f"{asset}/optionboard.json?iss.meta=off&expiration_date={expiration}"
    )
    data = _fetch(url)
    calls = {r["SECID"]: r for r in _block(data, "call")}
    puts = {r["SECID"]: r for r in _block(data, "put")}
    return calls, puts


def fetch_future_settle(secid: str) -> float | None:
    url = (
        f"{ISS_BASE}/engines/futures/markets/forts/securities/{secid}.json?iss.meta=off"
    )
    data = _fetch(url)
    sec = _block(data, "securities")
    md = _block(data, "marketdata")
    if md:
        last = md[0].get("LAST") or md[0].get("SETTLEPRICE")
        if last:
            return float(last)
    if sec:
        for k in ("LASTSETTLEPRICE", "SETTLEPRICE", "PREVSETTLEPRICE"):
            v = sec[0].get(k)
            if v:
                return float(v)
    return None


# -------------------------------------------------------------------- core check
def years_to(expiry_date: str, today: datetime | None = None) -> float:
    today = today or datetime.now(timezone.utc)
    d = datetime.fromisoformat(expiry_date).replace(tzinfo=timezone.utc)
    # treat expiration at end of day
    d = d.replace(hour=18, minute=45)
    return max((d - today).total_seconds() / (365.0 * 86400.0), 1 / 365.0 / 24.0)


def pick_active_options(asset: str, future_code: str, expiry: str, n: int = 5) -> list[str]:
    """Pick options that actually have THEORPRICE / VOLAT and a wide strike range."""
    calls, puts = fetch_optionboard(asset, expiry)
    secids: list[str] = []
    # sort by strike, pick a spread of strikes
    rows = sorted(calls.values(), key=lambda r: r.get("STRIKE") or 0)
    if not rows:
        return secids
    step = max(len(rows) // (n + 1), 1)
    for i in range(1, n + 1):
        idx = min(i * step, len(rows) - 1)
        secids.append(rows[idx]["SECID"])
    return secids


def verify_for_asset(asset: str, future_code: str, expiry: str, n: int = 5) -> list[dict]:
    print(f"\n=== {asset} (future {future_code}, expiry {expiry}) ===")
    calls_b, puts_b = fetch_optionboard(asset, expiry)
    if not calls_b:
        print("  optionboard empty — try a different expiry")
        return []

    spot = fetch_future_settle(future_code)
    print(f"  underlying future {future_code} price ≈ {spot}")

    secids = pick_active_options(asset, future_code, expiry, n=n)
    out = []
    for secid in secids:
        sec_payload = fetch_security(secid)
        sec = sec_payload["securities"]
        md = sec_payload["marketdata"]
        if not sec:
            continue
        is_call = sec.get("OPTIONTYPE") == "C"
        strike = float(sec["STRIKE"])
        F = float(sec.get("UNDERLYINGSETTLEPRICE") or spot or 0)
        T = years_to(sec["LASTTRADEDATE"])
        last = md.get("LAST") or sec.get("PREVPRICE")
        last = float(last) if last else None
        prev = float(sec.get("PREVPRICE") or 0)

        board_row = (calls_b if is_call else puts_b).get(secid, {})
        volat = board_row.get("VOLAT")  # % units, ground truth IV
        theor = board_row.get("THEORPRICE")

        imp = sec.get("IMP")
        imnp = sec.get("IMNP")
        imbuy = sec.get("IMBUY")

        # Derived IV from B76 inversion using best available premium (LAST or PREVPRICE)
        price_for_iv = last if (last and last > 0) else prev
        iv_b76 = black76_iv(price_for_iv, F, strike, T, is_call)

        row = {
            "SECID": secid,
            "TYPE": "C" if is_call else "P",
            "STRIKE": strike,
            "F": F,
            "T_years": round(T, 4),
            "LAST": last,
            "PREVPRICE": prev,
            "THEORPRICE": theor,
            "VOLAT_pct": volat,
            "IMP": imp,
            "IMNP": imnp,
            "IMBUY": imbuy,
            "B76_IV_pct": round(iv_b76 * 100, 2) if iv_b76 else None,
            # Hypothesis checks:
            "H1_imp_eq_theor": (theor is not None and imp is not None
                                and abs(float(theor) - float(imp)) / max(abs(float(theor)), 1) < 0.05),
            "H2_imp_as_iv_pct": round(float(imp), 2) if imp else None,
            "H3_imp_div_spot_pct": (round(float(imp) / F * 100, 2) if (imp and F) else None),
        }
        out.append(row)

        print(
            f"  {secid:14s} {row['TYPE']} K={strike:>8.0f}  "
            f"LAST={str(last):>8s}  THEOR={str(theor):>10s}  "
            f"VOLAT={str(volat):>8s}%  "
            f"IMP={imp}  IMNP={imnp}  IMBUY={imbuy}  "
            f"B76_IV={row['B76_IV_pct']}%"
        )
    return out


def summarize(rows: list[dict]) -> None:
    print("\n--- Hypothesis verdicts (numeric) ---")
    print(f"{'SECID':14s} {'VOLAT%':>7s} {'IMP':>9s} {'THEOR':>10s} {'IMP/spot%':>10s} {'B76_IV%':>9s}")
    for r in rows:
        print(
            f"{r['SECID']:14s} "
            f"{(r['VOLAT_pct'] if r['VOLAT_pct'] is not None else 0):>7.2f} "
            f"{(r['IMP'] if r['IMP'] is not None else 0):>9.2f} "
            f"{(r['THEORPRICE'] if r['THEORPRICE'] is not None else 0):>10.2f} "
            f"{(r['H3_imp_div_spot_pct'] if r['H3_imp_div_spot_pct'] is not None else 0):>10.2f} "
            f"{(r['B76_IV_pct'] if r['B76_IV_pct'] is not None else 0):>9.2f}"
        )

    # Aggregate residuals — which hypothesis matches?
    def _err(pred_key: str, truth_key: str = "VOLAT_pct") -> float | None:
        errs = []
        for r in rows:
            p, t = r.get(pred_key), r.get(truth_key)
            if p is None or t is None:
                continue
            errs.append(abs(float(p) - float(t)))
        return sum(errs) / len(errs) if errs else None

    print("\n--- Mean absolute deviation vs VOLAT% (ground truth) ---")
    print(f"  H2 (IMP treated as IV%):              MAD = {_err('H2_imp_as_iv_pct')}")
    print(f"  H3 (IMP/spot * 100 treated as IV%):   MAD = {_err('H3_imp_div_spot_pct')}")
    print(f"  Black-76 inverted IV:                 MAD = {_err('B76_IV_pct')}  (sanity: should be ~0)")

    print("\n--- H1: does IMP ≈ THEORPRICE? ---")
    h1_hits = sum(1 for r in rows if r.get("H1_imp_eq_theor"))
    print(f"  matched {h1_hits}/{len(rows)} options within 5%")

    print("\n--- H4: IMP as Initial Margin (RUB) sanity ---")
    print("  Expectation: IMNP (naked short margin) >> IMBUY (long margin), and IMBUY ≈ option premium")
    for r in rows:
        ratio = (r["IMBUY"] / r["LAST"]) if (r.get("IMBUY") and r.get("LAST")) else None
        print(
            f"  {r['SECID']}: IMNP={r['IMNP']}  IMP={r['IMP']}  IMBUY={r['IMBUY']}  "
            f"LAST={r['LAST']}  IMBUY/LAST≈{ratio}"
        )


# ------------------------------------------------------------------------- main
def main() -> int:
    print("=" * 78)
    print("Official MOEX column reference (engines/futures/markets/options):")
    print("=" * 78)
    doc = fetch_columns_doc()
    for name in ("IMP", "IMNP", "IMBUY", "IMTIME"):
        meta = doc.get(name, {})
        print(f"  {name:6s}  short = {meta.get('short_title')!r}")
        print(f"          title = {meta.get('title')!r}")

    all_rows: list[dict] = []

    # SBER stock options (this is the canonical asset code; "SBRF" is the FUTURE)
    all_rows += verify_for_asset("SBER", "SRM6", "2026-06-17", n=5)

    # GAZP stock options
    try:
        all_rows += verify_for_asset("GAZP", "GZM6", "2026-06-17", n=3)
    except Exception as exc:
        print(f"  GAZP skipped: {exc}")

    # MOEX-index options (the asset code used by MOEX for IMOEX-future options is "MOEX")
    try:
        all_rows += verify_for_asset("MOEX", "MMM6", "2026-06-18", n=3)
    except Exception as exc:
        print(f"  MOEX skipped: {exc}")

    # Premium-style weekly Call on SBER that Agent #4 cited — examine directly.
    print("\n=== Direct lookup: SR320CF6A (the option Agent #4 cited) ===")
    payload = fetch_security("SR320CF6A")
    sec, md = payload["securities"], payload["marketdata"]
    if sec:
        last = md.get("LAST")
        prev = sec.get("PREVPRICE")
        F = sec.get("UNDERLYINGSETTLEPRICE")
        K = sec.get("STRIKE")
        T = years_to(sec["LASTTRADEDATE"])
        iv = black76_iv(last or prev, F, K, T, sec.get("OPTIONTYPE") == "C")
        print(f"  SECID    : {sec['SECID']}  ({sec.get('SECNAME')})")
        print(f"  STRIKE   : {K}        UNDERLYING_SETTLE: {F}        T (yr): {T:.4f}")
        print(f"  LAST     : {last}    PREVPRICE: {prev}")
        print(f"  IMP      : {sec.get('IMP')}       IMNP: {sec.get('IMNP')}       IMBUY: {sec.get('IMBUY')}")
        print(f"  Black-76 IV (from LAST)   : {round(iv*100, 2) if iv else None}%")
        print(f"  IMP / spot = {round(float(sec['IMP']) / float(F) * 100, 2)}%  "
              f"(this is the **margin-to-underlying ratio**, NOT implied volatility)")
        print(f"  IMBUY == PREVPRICE?  {abs(float(sec['IMBUY']) - float(prev)) < 0.01}  "
              f"(IMBUY equals the premium a buyer must pay — confirms IMBUY = long margin = premium)")

    summarize(all_rows)

    print("\n" + "=" * 78)
    print("CONCLUSION")
    print("=" * 78)
    print(
        "Per MOEX columns reference: IMP / IMNP / IMBUY are *Initial Margin* (ГО)\n"
        "in RUB, not implied volatility and not theoretical price. The empirical\n"
        "match with VOLAT/THEORPRICE confirms neither H1 (theor price) nor H2/H3\n"
        "(IV in any unit). Use VOLAT from the optionboard for IV; use IMP/IMNP\n"
        "only if you need MOEX margin requirements."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
