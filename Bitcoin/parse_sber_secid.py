"""Parse SBER option SECID → strike, option_type, expiry month/year, series, week.

Format: SR{STRIKE}{SERIES}{MONTHCODE}{YEAR}[{WEEK}]
  STRIKE    = leading digits
  SERIES    = B (futures-denominated) or C (stock-denominated, post-2022 redenom)
  MONTHCODE = A-L calls Jan-Dec, M-X puts Jan-Dec
  YEAR      = single digit (maps to nearest decade)
  WEEK      = optional weekly suffix letter

Type derived from MONTHCODE. Exact expiry DAY fetched separately per unique
expiry-code (far fewer than unique SECIDs).
"""
import re

# Call months A-L = Jan-Dec; Put months M-X = Jan-Dec
_CALL_MONTHS = {chr(ord('A') + i): i + 1 for i in range(12)}   # A=1(Jan)...L=12(Dec)
_PUT_MONTHS = {chr(ord('M') + i): i + 1 for i in range(12)}    # M=1(Jan)...X=12(Dec)


def parse_secid(secid):
    """Return dict with strike, option_type, exp_month, exp_year_digit, series, week.
    Returns None if not a parseable SBER single option (skips composites)."""
    if not isinstance(secid, str) or not secid.startswith('SR'):
        return None
    body = secid[2:]
    # Composite/spread codes contain a second 'SR' or are too long
    if 'SR' in body:
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
    # Self-test on known samples
    tests = {
        'SR100CA3':   ('100', 'C', 1, 3, 'C'),
        'SR110CK2A':  ('110', 'C', 11, 2, 'C'),
        'SR14250BF1': ('14250', 'C', 6, 1, 'B'),
        'SR23000BR3B':('23000', 'P', 6, 3, 'B'),
        'SR370CO4D':  ('370', 'P', 3, 4, 'C'),
        'SR53000BF2': ('53000', 'C', 6, 2, 'B'),
        'SR8750BT2':  ('8750', 'P', 8, 2, 'B'),
        'SR30750BU3': ('30750', 'P', 9, 3, 'B'),
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
