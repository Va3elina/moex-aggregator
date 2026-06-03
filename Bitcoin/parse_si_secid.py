"""Parse Si (USD/RUB futures) option SECID → strike, option_type, expiry month/year, series, week.

Format: Si{STRIKE}{SERIES}{MONTHCODE}{YEAR}[{WEEK}]
  STRIKE    = leading digits (in points; 100000 points = 100.000 RUB/USD)
  SERIES    = B (futures-denominated)
  MONTHCODE = A-L calls Jan-Dec, M-X puts Jan-Dec
  YEAR      = single digit (maps to nearest decade)
  WEEK      = optional weekly suffix letter

Type derived from MONTHCODE. Exact expiry DAY fetched separately per unique
expiry-code (far fewer than unique SECIDs).

Identical encoding to SBER (SR-prefix) but with 'Si' prefix and Si assetcode.
"""
import re

# Call months A-L = Jan-Dec; Put months M-X = Jan-Dec
_CALL_MONTHS = {chr(ord('A') + i): i + 1 for i in range(12)}   # A=1(Jan)...L=12(Dec)
_PUT_MONTHS = {chr(ord('M') + i): i + 1 for i in range(12)}    # M=1(Jan)...X=12(Dec)


def parse_secid(secid):
    """Return dict with strike, option_type, exp_month, exp_year_digit, series, week.
    Returns None if not a parseable Si single option (skips composites/spreads)."""
    if not isinstance(secid, str) or not secid.startswith('Si'):
        return None
    body = secid[2:]
    # Composite/spread codes contain a second 'Si' or are otherwise malformed
    if 'Si' in body:
        return None
    m = re.match(r'^(\d+)([A-X])([A-X])(\d)([A-Z])?$', body)
    if not m:
        return None
    strike_s, series, monthcode, year_s, week = m.groups()
    strike = int(strike_s)
    if monthcode in _CALL_MONTHS:
        otype = 'C'
        month = _CALL_MONTHS[monthcode]
    elif monthcode in _PUT_MONTHS:
        otype = 'P'
        month = _PUT_MONTHS[monthcode]
    else:
        return None
    return {
        'strike': strike,
        'option_type': otype,
        'exp_month': month,
        'exp_year_digit': int(year_s),
        'series': series,
        'week': week or '',
        'expiry_code': f"{series}{monthcode}{year_s}{week or ''}",
    }


def resolve_year(year_digit, ref_year):
    """Single year digit → full year nearest to ref_year (the trade date year)."""
    candidates = [2010 + year_digit, 2020 + year_digit, 2030 + year_digit]
    return min(candidates, key=lambda y: abs(y - ref_year))


if __name__ == "__main__":
    # Self-test on known samples (verified against MOEX API)
    tests = {
        'Si100000BC5':  ('100000', 'C', 3, 5, 'B'),   # LSTDEL 2025-03-20
        'Si100000BF4':  ('100000', 'C', 6, 4, 'B'),   # LSTDEL 2024-06-20
        'Si90000BR4':   ('90000', 'P', 6, 4, 'B'),    # LSTDEL 2024-06-20
        'Si75000BO2':   ('75000', 'P', 3, 2, 'B'),    # LSTDEL 2022-03-17
        'Si85000BA5':   ('85000', 'C', 1, 5, 'B'),    # LSTDEL 2025-01-16
    }
    ok = 0
    for secid, (strike, otype, month, yr, series) in tests.items():
        r = parse_secid(secid)
        match = (str(r['strike']) == strike and r['option_type'] == otype
                 and r['exp_month'] == month and r['exp_year_digit'] == yr
                 and r['series'] == series)
        print(f"  {'PASS' if match else 'FAIL'} {secid} → {r}")
        ok += match
    print(f"{ok}/{len(tests)} passed")
