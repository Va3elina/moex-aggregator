#!/usr/bin/env python3
"""Справочник «ISIN облигации → иконка» для /fund-trades и /funds.

Генерит frontend/src/config/bondIssuers.generated.ts:
  BOND_GOV_ISINS    — гособлигации (ОФЗ, субъекты РФ, муниципальные) → герб;
  BOND_ISSUER_LOGO  — корпоблигация → тикер акции эмитента (или ключ лого
                      эмитента без акций), только если у ключа ЕСТЬ лого.

Источник — MOEX ISS (наш обычный источник, без скрейпинга):
  securities.json?q=<ISIN>               → emitent_id / emitent_inn / type;
  securities.json?group_by=stock_shares  → emitent_id → тикер акции.
Дочки-эмитенты без своих акций (Газпром капитал, ИКС 5 ФИНАНС, СФО Сбера…)
сводятся к родителю ручной картой PARENT_BY_INN.

Когда запускать: раз в месяц после залива справок СЧА (новые выпуски).
Без прогона новые бумаги просто остаются с кружком-буквой; ОФЗ с ISIN SU…
получают герб и без справочника (isGovBond на фронте).

  # ISIN-ы с прода (только чтение), затем генерация:
  python3 scripts/build_bond_issuers.py --ssh root@<прод-хост>
  # или из файла (первый столбец — ISIN, второй опц. — объём в ₽ для отчёта):
  python3 scripts/build_bond_issuers.py --isins isins.csv

Ответы ISS кэшируются (--cache), повторный прогон спрашивает только новые ISIN.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import time
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'frontend/src/config/bondIssuers.generated.ts'
SPRITE_MANIFEST = ROOT / 'frontend/public/logos/sprite-manifest.json'
INSTRUMENT_ICON = ROOT / 'frontend/src/components/InstrumentIcon.tsx'
TICKER_LOGO = ROOT / 'frontend/src/components/TickerLogo.tsx'
ISS = 'https://iss.moex.com/iss'

GOV_TYPES = {'ofz_bond', 'subfederal_bond', 'municipal_bond', 'cb_bond'}
MINFIN_INN = '7710168360'

# Эмитент облигаций без своих акций → тикер акции группы (лого бренда).
# Только очевидные «финансовые дочки» с тем же брендом.
PARENT_BY_INN = {
    '7726588547': 'GAZP',   # Газпром капитал
    '9703163693': 'SBER',   # СФО СБ Секьюритизация 5
    '9704267960': 'SBER',   # СФО СБ Секьюритизация 4
    '9704044804': 'DOMRF',  # СОПФ ДОМ.РФ
    '7705619586': 'ETLN',   # Эталон-Финанс
    '7714431630': 'VKCO',   # Мэйл.Ру Финанс (VK)
    '7715630469': 'X5',     # ИКС 5 ФИНАНС
    '7724365841': 'PRMD',   # ПРОМОМЕД ДМ
    '3812142445': 'ENPG',   # ЭН+ ГИДРО
    '7709378229': 'VTBR',   # ВТБ Лизинг
    '9717068640': 'WUSH',   # ООО «ВУШ»
    '7722753969': 'VSEH',   # ВсеИнструменты.ру
    '7709780434': 'SVCB',   # Совкомбанк Лизинг
    '7710140679': 'T',      # АО «ТБанк»
    '7826087713': 'OKEY',   # О'КЕЙ
}

# Тикер без своего PNG, но знак есть в стикерпаке (raw_NNN).
LOGO_BY_TICKER = {
    'SLAV': 'raw_149',   # Славнефть (тот же знак, что JNOS/MFGS)
    'ROSB': 'raw_146',   # Росбанк
    'OKEY': 'raw_045',   # О'КЕЙ
}


def get(url: str):
    for i in range(4):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.load(r)
        except Exception:  # noqa: BLE001 — ISS иногда рвёт соединение
            time.sleep(2 + 3 * i)
    raise RuntimeError(url)


def rows(block: dict) -> list[dict]:
    return [dict(zip(block['columns'], r)) for r in block['data']]


def fetch_isins_ssh(host: str) -> dict[str, float]:
    """ISIN-ы из месячных справок (vim_sdr/interfax_manual) + объём на последнюю дату фонда."""
    sql = (
        "\\copy (WITH last AS (SELECT fund_id, max(snapshot_date) d FROM fund_holdings_history "
        "WHERE source IN ('vim_sdr','interfax_manual') GROUP BY fund_id) "
        "SELECT h.isin, sum(CASE WHEN h.snapshot_date = l.d THEN h.amount_rub ELSE 0 END) "
        "FROM fund_holdings_history h JOIN last l USING (fund_id) "
        "WHERE h.source IN ('vim_sdr','interfax_manual') AND h.isin ~ '^(RU|SU)' "
        "GROUP BY h.isin) TO STDOUT WITH CSV"
    )
    out = subprocess.run(
        ['ssh', '-o', 'IdentitiesOnly=yes', '-i', str(Path.home() / '.ssh/id_ed25519'), host,
         'docker exec -i frame-db-1 psql -U postgres -d moex_db'],
        input=sql, capture_output=True, text=True, check=True,
    ).stdout
    return {r[0]: float(r[1] or 0) for r in csv.reader(out.splitlines()) if r}


def read_isins_file(path: Path) -> dict[str, float]:
    res: dict[str, float] = {}
    for r in csv.reader(path.read_text(encoding='utf-8').splitlines()):
        if r and re.fullmatch(r'[A-Z]{2}[A-Z0-9]{10}', r[0]):
            res[r[0]] = float(r[1]) if len(r) > 1 and r[1] else 0.0
    return res


def logo_keys() -> set[str]:
    """Ключи, у которых InstrumentIcon нарисует картинку, а не инициалы."""
    keys = set(json.loads(SPRITE_MANIFEST.read_text())['logos'])
    src = INSTRUMENT_ICON.read_text(encoding='utf-8')
    m = re.search(r'STOCK_LOGO_OVERRIDE[^{]*\{(.*?)\n\};', src, re.S)
    keys |= set(re.findall(r'^\s*(\w+):', m.group(1), re.M)) if m else set()
    m = re.search(r'INDIVIDUAL_LOGOS[^{]*\{(.*?)\n\};', TICKER_LOGO.read_text(encoding='utf-8'), re.S)
    keys |= set(re.findall(r'^\s*(\w+):', m.group(1), re.M)) if m else set()
    return keys


def share_ticker_by_emitent() -> dict[int, str]:
    shares, start = [], 0
    while True:
        part = rows(get(f'{ISS}/securities.json?iss.meta=off&group_by=group'
                        f'&group_by_filter=stock_shares&limit=100&start={start}')['securities'])
        shares += part
        if len(part) < 100:
            break
        start += 100
    best: dict[int, tuple] = {}
    for s in shares:
        sec = s['secid'] or ''
        if s['type'] not in ('common_share', 'preferred_share') or not re.fullmatch(r'[A-Z0-9]+', sec):
            continue
        # торгуется > обыкновенная > основной режим TQBR > короче
        rank = (-(s['is_traded'] or 0), s['type'] != 'common_share', s['primary_boardid'] != 'TQBR', len(sec))
        cur = best.get(s['emitent_id'])
        if cur is None or rank < cur[0]:
            best[s['emitent_id']] = (rank, sec)
    return {k: v[1] for k, v in best.items()}


def lookup(isins: list[str], cache_path: Path) -> dict[str, dict | None]:
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    todo = [i for i in isins if i not in cache]

    def one(isin: str):
        found = [r for r in rows(get(f'{ISS}/securities.json?iss.meta=off&q={isin}')['securities'])
                 if r['isin'] == isin]
        return isin, (found[0] if found else None)

    with ThreadPoolExecutor(4) as ex:
        for n, (isin, r) in enumerate(ex.map(one, todo), 1):
            cache[isin] = r
            if n % 100 == 0:
                print(f'  ISS {n}/{len(todo)}', file=sys.stderr)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, ensure_ascii=False))
    return cache


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument('--ssh', help='user@host прод-сервера (read-only psql)')
    src.add_argument('--isins', type=Path, help='CSV: ISIN[,объём ₽]')
    ap.add_argument('--cache', type=Path, default=Path.home() / '.cache/frame/iss_bond_isins.json')
    ap.add_argument('--top', type=int, default=25, help='сколько эмитентов без лого показать')
    a = ap.parse_args()

    volume = fetch_isins_ssh(a.ssh) if a.ssh else read_isins_file(a.isins)
    info = lookup(sorted(volume), a.cache)
    by_emitent = share_ticker_by_emitent()
    has_logo = logo_keys()

    gov: list[str] = []
    issuer: dict[str, str] = {}
    missing: dict[str, list] = defaultdict(lambda: [0.0, 0])
    for isin in sorted(volume):
        r = info.get(isin)
        if not r or not (r['type'] or '').endswith('_bond'):
            continue  # акции/ДР/фонды — это ISIN_TO_TICKER; неизвестные ISS — пропуск
        if r['type'] in GOV_TYPES or r['emitent_inn'] == MINFIN_INN:
            gov.append(isin)
            continue
        ticker = PARENT_BY_INN.get(r['emitent_inn'] or '') or by_emitent.get(r['emitent_id'])
        key = LOGO_BY_TICKER.get(ticker or '', ticker)
        if key and key in has_logo:
            issuer[isin] = key
        else:
            m = missing[r['emitent_title'] or isin]
            m[0] += volume[isin]
            m[1] += 1

    lines = [
        '// АВТОГЕНЕРАЦИЯ scripts/build_bond_issuers.py (MOEX ISS) — НЕ редактировать руками.',
        '// Гособлигации (ОФЗ, субъекты РФ, муниципальные) → герб; корпоблигации → лого эмитента.',
        '// Обновлять раз в месяц после залива справок СЧА; см. docstring скрипта.',
        '',
        'export const BOND_GOV_ISINS: ReadonlySet<string> = new Set([',
        *[f"    '{i}'," for i in gov],
        ']);',
        '',
        'export const BOND_ISSUER_LOGO: Readonly<Record<string, string>> = {',
        *[f"    {i}: '{t}'," for i, t in sorted(issuer.items())],
        '};',
        '',
    ]
    OUT.write_text('\n'.join(lines), encoding='utf-8')

    vol_issuer = sum(volume[i] for i in issuer)
    vol_gov = sum(volume[i] for i in gov)
    vol_miss = sum(v[0] for v in missing.values())
    print(f'{OUT.relative_to(ROOT)}: герб {len(gov)} ISIN ({vol_gov / 1e9:.0f} млрд ₽), '
          f'лого эмитента {len(issuer)} ({vol_issuer / 1e9:.0f} млрд ₽), '
          f'без лого {sum(v[1] for v in missing.values())} ({vol_miss / 1e9:.0f} млрд ₽)')
    print(f'Эмитенты без лого, топ-{a.top} по объёму в фондах на последнюю справку:')
    for title, (v, n) in sorted(missing.items(), key=lambda x: -x[1][0])[:a.top]:
        print(f'  {v / 1e9:7.1f} млрд ₽  {n:3d} вып.  {title}')


if __name__ == '__main__':
    main()
