#!/usr/bin/env python
"""Портфель ОДНОГО фонда сейчас и как он изменится при смене базы 18.09.2026.

Пишет ../FUND_<тикер>.md. По умолчанию EQMX — у него снимок состава ежедневный
(парсер сайта ВИМ), у TMOS и SBMX месячный ручной ингест, то есть данные старее.

⚠️ ПОРТФЕЛЬ — ЭТО ОДИН СНИМОК, а не «последняя строка по каждой бумаге». Первая версия
этого скрипта брала DISTINCT ON (фонд, isin) и втащила в портфель Petropavlovsk,
Полиметалл и Мечел — бумаги, которых в фонде нет годами, просто их последние строки
были самыми свежими для них самих. Здесь берётся последний снимок, у которого сумма
весов близка к 100% (признак полноты), и только его строки.

⚠️ ТРИ ДАТЫ НЕ СОВПАДАЮТ, и это надо держать в голове:
  • состав фонда — дата снимка;
  • цены         — последний торговый день в нашей БД;
  • СЧА          — дата из fund_data (источник Cbonds, своя дата).
Стоимость позиции считается как штуки × цена, а не берётся из amount_rub: у строк
источника 'vim' он пустой, и сумма молча потеряла бы фонд целиком.

Расхождение между Σ(штуки × цена) и СЧА — не ошибка, а кэш и прочие активы. Печатается
явно: на эту величину оценка «Δвес × СЧА» завышает реальную сделку.

Запуск:  .venv/bin/python research/moex_rebalance/calc/fund_view.py [--fund EQMX]
"""
from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys

BASE = pathlib.Path(__file__).resolve().parents[1]
DATA = BASE / "data"
ASOF = "2026-09-01"
SSH = ["ssh", "-o", "ConnectTimeout=25", "root@103.88.243.232"]
PSQL = "docker exec frame-db-1 psql -U postgres -d moex_db -P pager=off -tA -F'|' -c"


def db(sql: str) -> list[list[str]]:
    out = subprocess.run(SSH + [f'{PSQL} "{sql}"'], capture_output=True, text=True, timeout=180)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip()[:400])
    return [ln.split("|") for ln in out.stdout.strip().splitlines() if ln.strip()]


def num(x: float, d: int = 2) -> str:
    """Число по-русски: пробел как разделитель тысяч, запятая как десятичная."""
    s = f"{x:,.{d}f}"
    return s.replace(",", " ").replace(".", ",")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fund", default="EQMX")
    args = ap.parse_args()
    F = args.fund

    prices = json.loads((DATA / "prices.json").read_text(encoding="utf-8"))
    funds = json.loads((DATA / "funds.json").read_text(encoding="utf-8"))
    wpath = DATA / "trades_by_weight.json"
    if not wpath.exists():
        print("нет data/trades_by_weight.json — сначала запусти rebalance.py")
        return 2
    tw = json.loads(wpath.read_text(encoding="utf-8"))
    # rebalance.py стал писать веса вложенным блоком «веса» вместе с нормировкой и
    # списком обрезанных — поддерживаем оба формата, иначе фонд молча выходит нулевым
    if "веса" in tw:
        tw = tw["веса"]

    nav = funds["сча"][F]["руб"]
    nav_date = funds["сча"][F]["дата"]

    # Последний полный снимок. Порог полноты берём НЕ на глаз, а тот, что использует
    # сам продукт: FT_COMPLETE_WSUM_MIN = 80 в api/routers/fund_trades.py. Мой прежний
    # порог 98% молча выбрасывал июльские снимки TMOS и SBMX (сумма весов 96,4% — там
    # просто не отражён кэш отдельной строкой) и откатывал расчёт на июнь.
    # ⚠️ ТОЛЬКО ОФИЦИАЛЬНОЕ РАСКРЫТИЕ: source='interfax_manual' — точный SCHA (справка
    # о СЧА, форма ЦБ № 0420502). НЕ 'vim': это WIP-парсер сайта ВИМ, и в его семействе
    # документирован баг ×1000 по количествам, затронувший EQMX.
    WSUM_MIN = 80          # порог полноты снимка = FT_COMPLETE_WSUM_MIN продукта
    SCHA = "interfax_manual"
    snap = db(f"SELECT h.snapshot_date FROM fund_holdings_history h "
              f"JOIN funds f ON f.fund_id=h.fund_id WHERE f.ticker='{F}' "
              f"AND h.source='{SCHA}' AND h.positions IS NOT NULL "
              f"GROUP BY 1 HAVING sum(h.weight) >= {WSUM_MIN} ORDER BY 1 DESC LIMIT 1")
    if not snap:
        print(f"у {F} нет ни одного снимка с суммой весов около 100%")
        return 2
    sdate = snap[0][0]

    rows = db(f"SELECT h.isin, h.asset_name, h.positions, h.weight, s.secid "
              f"FROM fund_holdings_history h JOIN funds f ON f.fund_id=h.fund_id "
              f"LEFT JOIN (SELECT DISTINCT isin, min(secid) AS secid FROM securities_ref "
              f"           WHERE secid IS NOT NULL GROUP BY isin) s ON s.isin = h.isin "
              f"WHERE f.ticker='{F}' AND h.source='{SCHA}' AND h.snapshot_date='{sdate}' "
              f"ORDER BY h.weight DESC NULLS LAST")

    items, unpriced = [], []
    for isin, name, pos, weight, secid in rows:
        w_snap = float(weight) if weight else None
        n = int(pos) if pos else None
        t = secid or None
        if t and n and t in prices and ASOF in prices[t]:
            items.append({"т": t, "имя": name, "шт": n, "руб": n * prices[t][ASOF],
                          "вес_сн": w_snap,
                          "вес_инд": tw.get(t, {}).get("old"),
                          "вес_нов": tw.get(t, {}).get("new")})
        else:
            unpriced.append({"имя": name, "вес_сн": w_snap, "шт": n, "т": t})

    total = sum(x["руб"] for x in items)
    cash = nav - total

    L, p = [], None
    L.append(f"# Портфель {F}: сейчас и после 18 сентября\n")
    p = L.append
    p(f"СЧА фонда **{num(nav / 1e9)} млрд ₽** на {nav_date} · состав — **справка о СЧА "
      f"за {sdate}** ({len(rows)} строк) · цены на {ASOF}.\n")
    p(f"Источник состава — только официальное раскрытие УК (`{SCHA}`, форма ЦБ № 0420502). "
      f"Ежедневный парсер сайта ВИМ (`vim`) сознательно НЕ используется.\n")
    p(f"Сгенерировано `calc/fund_view.py`.\n")

    p("## Что в портфеле сейчас\n")
    import datetime as _dt
    gap = (_dt.date.fromisoformat(nav_date) - _dt.date.fromisoformat(sdate)).days
    # Остаток — это НЕ обязательно кэш. Даты трёх величин разные, и знак подсказывает,
    # что именно перевесило: положительный — фонд вырос после снимка (приток),
    # отрицательный — бумаги подорожали относительно даты СЧА.
    if gap <= 7:
        what = "деньги и прочие активы"
    elif cash >= 0:
        what = (f"НЕ только кэш: справка о СЧА старше даты СЧА на {gap} дней, и за это "
                f"время фонд вырос на притоке — позиции из справки занижают текущий портфель")
    else:
        what = (f"остаток ОТРИЦАТЕЛЬНЫЙ: справка о СЧА за {sdate}, а цены на {ASOF} — "
                f"бумаги подорожали, и портфель по текущим ценам дороже СЧА на дату отчёта. "
                f"Кэш при этом тоже есть, просто он меньше переоценки")
    p(f"Оценено {len(items)} позиций на **{num(total / 1e9)} млрд ₽** — "
      f"**{num(100 * total / nav, 1)}%** от СЧА. Остаток **{num(cash / 1e6, 0)} млн ₽** "
      f"({num(100 * cash / nav, 1)}%) — {what}.\n")
    if unpriced:
        p(f"Без оценки {len(unpriced)} строк: "
          f"{', '.join((x['имя'] or '?')[:30] for x in unpriced)} — "
          f"это денежная позиция и/или бумаги вне нашего набора цен.\n")

    p("| # | бумага | штук | млн ₽ | доля в фонде | вес в индексе |")
    p("|---|---|---|---|---|---|")
    for i, x in enumerate(sorted(items, key=lambda z: -z["руб"]), 1):
        wi = f"{num(x['вес_инд'])}%" if x["вес_инд"] is not None else "—"
        p(f"| {i} | {(x['имя'] or '')[:36]} | {num(x['шт'], 0)} | {num(x['руб'] / 1e6, 0)} "
          f"| {num(100 * x['руб'] / total)}% | {wi} |")

    p("\n## Как изменится\n")
    p("Фонд обязан следовать индексу, поэтому сделка = изменение веса × СЧА. Числа — "
      "**оценка в ценах одного дня**; верить стоит порядку и знаку, а не точному значению. "
      "Разброс от трактовки даты фиксации коэффициентов — почти вдвое, см. `OPEN.md`.\n")
    p("| бумага | вес в индексе сейчас → новый | оценка сделки, млн ₽ | от позиции |")
    p("|---|---|---|---|")
    moves = [(x, (x["вес_нов"] - x["вес_инд"]) / 100 * nav) for x in items
             if x["вес_инд"] is not None and x["вес_нов"] is not None]
    for x, d in sorted(moves, key=lambda z: z[1]):
        if abs(d) < 2e6:
            continue
        share = 100 * d / x["руб"] if x["руб"] else 0
        p(f"| {(x['имя'] or '')[:36]} | {num(x['вес_инд'])}% → {num(x['вес_нов'])}% "
          f"| **{num(d / 1e6, 0)}** | {num(share, 1)}% |")
    s = sum(d for _, d in moves if d < 0)
    b = sum(d for _, d in moves if d > 0)
    p(f"\nПродать примерно **{num(abs(s) / 1e6, 0)} млн ₽** "
      f"({num(100 * abs(s) / nav, 2)}% СЧА), докупить **{num(b / 1e6, 0)} млн ₽** "
      f"({num(100 * b / nav, 2)}% СЧА).\n")
    p(f"Поправка на кэш: бумаги занимают {num(100 * total / nav, 1)}% СЧА, значит реальные "
      f"сделки примерно на {num(100 - 100 * total / nav, 1)}% меньше этих оценок.\n")

    (BASE / f"FUND_{F}.md").write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))
    return 0


if __name__ == "__main__":
    sys.exit(main())
