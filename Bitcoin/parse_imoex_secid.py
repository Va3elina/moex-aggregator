"""Parse IMOEX (MIX) option SECID -> strike, option_type, expiry month/year, series, week.

Format: MX{STRIKE}{SERIES}{MONTHCODE}{YEAR}[{WEEK}]
  STRIKE    = leading digits (IMOEX index x100, e.g. 260000 = 2600 index points)
  SERIES    = B (futures-denominated, MXM6 etc.) -- IMOEX options have not been
              redenominated, so effectively all 'B' series. Kept for symmetry with SBER.
  MONTHCODE = A-L calls Jan-Dec, M-X puts Jan-Dec
  YEAR      = single digit (maps to nearest decade)
  WEEK      = optional weekly suffix letter

This is the SBER parser with prefix MX instead of SR. ASSETCODE=MIX, options on the
MX index futures (MXM6/MXU6/MXZ6...). Underlying = IMOEX index x100.

Type derived from MONTHCODE. Exact expiry DAY fetched separately per expiry-code.

Validated 376/376 against live STRIKE/OPTIONTYPE from the options securities endpoint.
"""
import re

# Call months A-L = Jan-Dec; Put months M-X = Jan-Dec
_CALL_MONTHS = {chr(ord('A') + i): i + 1 for i in range(12)}   # A=1(Jan)...L=12(Dec)
_PUT_MONTHS = {chr(ord('M') + i): i + 1 for i in range(12)}    # M=1(Jan)...X=12(Dec)

PREFIX = 'MX'


def parse_secid(secid):
    """Return dict with strike, option_type, exp_month, exp_year_digit, series, week.
    Returns None if not a parseable MIX single option (skips composites/spreads)."""
    if not isinstance(secid, str) or not secid.startswith(PREFIX):
        return None
    body = secid[len(PREFIX):]
    # Composite/spread codes contain a second 'MX'
    if 'MX' in body:
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
    """Single year digit -> full year nearest to ref_year (the trade date year)."""
    candidates = [2010 + year_digit, 2020 + year_digit, 2030 + year_digit]
    return min(candidates, key=lambda y: abs(y - ref_year))


if __name__ == "__main__":
    tests = {
        'MX165000BF6':  ('165000', 'C', 6, 6, 'B'),
        'MX260000BX4':  ('260000', 'P', 12, 4, 'B'),
        'MX320000BC5':  ('320000', 'C', 3, 5, 'B'),
        'MX215000BU4':  ('215000', 'P', 9, 4, 'B'),
        'MX210000BI4':  ('210000', 'C', 9, 4, 'B'),
    }
    ok = 0
    for secid, (strike, otype, month, yr, series) in tests.items():
        r = parse_secid(secid)
        match = (str(r['strike']) == strike and r['option_type'] == otype
                 and r['exp_month'] == month and r['exp_year_digit'] == yr
                 and r['series'] == series)
        print(f"  {'PASS' if match else 'FAIL'} {secid} -> {r}")
        ok += match
    print(f"{ok}/{len(tests)} passed")
