#!/usr/bin/env python
"""Снять все данные, нужные для расчёта ребаланса, в data/.

Разделение простое: fetch_sources.py тянет ДОКУМЕНТЫ (правила), этот скрипт —
ЧИСЛА (состав индекса, коэффициенты, цены, фонды). Оба идемпотентны.

Что кладётся в data/:
  lw_new.json        LW новой базы, разобранный из PDF-приложения к релизу (ровно 60 бумаг)
  ff_new.json        новые коэффициенты free-float, разобранные из релиза (ровно 15 бумаг)
  imoex_changes.json кого исключают/включают в Индекс МосБиржи (по тексту релиза)
  iss_imoex.json     по каждой бумаге IMOEX: вес, ff_factor, w_factor, выпуск — по датам
  prices.json        цены закрытия из нашей БД на опорные даты
  funds.json         СЧА трёх индексных фондов + их позиции по бумагам

⚠️ ISS не любит частых запросов — между обращениями пауза, иначе банит IP сервера
(см. память moex_ip_ban). Здесь ~50 запросов с паузой 0,6 с.

Запуск:  .venv/bin/python research/moex_rebalance/calc/fetch_data.py
"""
from __future__ import annotations

import html
import json
import pathlib
import re
import subprocess
import sys
import time
import urllib.request

BASE = pathlib.Path(__file__).resolve().parents[1]
SRC, DATA = BASE / "sources", BASE / "data"
UA = {"User-Agent": "Mozilla/5.0 (compatible; FrameBot/1.0)"}

# Опорные даты. FIX_DATE — день, по итогам которого биржа зафиксировала весовые
# коэффициенты новой базы (п. 2.8.6: день, предшествующий дню раскрытия; релиз 28.08).
# ASOF — последняя дата, на которую у нас есть данные; на ней и оцениваем сделки.
FIX_DATE = "2026-08-31"
ASOF = "2026-09-01"

SSH = ["ssh", "-o", "ConnectTimeout=25", "root@103.88.243.232"]
PSQL = "docker exec frame-db-1 psql -U postgres -d moex_db -P pager=off -tA -F'|' -c"
FUNDS = ("EQMX", "TMOS", "SBMX")          # индексные фонды на Индекс МосБиржи


def db(sql: str) -> list[list[str]]:
    """Выполнить SQL на прод-БД, вернуть строки как списки строк."""
    out = subprocess.run(SSH + [f'{PSQL} "{sql}"'], capture_output=True, text=True, timeout=180)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip()[:400])
    return [ln.split("|") for ln in out.stdout.strip().splitlines() if ln.strip()]


def iss(url: str, tries: int = 4) -> dict:
    for k in range(tries):
        try:
            return json.load(urllib.request.urlopen(
                urllib.request.Request(url, headers=UA), timeout=40))
        except Exception:                              # noqa: BLE001
            if k == tries - 1:
                raise
            time.sleep(2 * (k + 1))
    return {}


def parse_lw() -> dict[str, float]:
    """LW новой базы из PDF-приложения.

    ⚠️ Нижняя граница длины тикера — ОДИН символ: в таблице есть строка 52 «T»
    (Т-Технологии, LW 0,8). Регулярка на 2+ символа её теряет, бумага молча получает
    LW = 1, и её оценка сделки раздувается в шесть раз. Проверка len == 60 обязательна.
    """
    txt = subprocess.run(["pdftotext", "-layout", str(SRC / "lw_table_18.09.2026.pdf"), "-"],
                         capture_output=True, text=True, timeout=60).stdout
    lw = {}
    for line in txt.splitlines():
        m = re.match(r"\s*(\d+)\s+([A-Z][A-Z0-9]{0,7})\s+.*?([01],\d)\s*$", line.rstrip())
        if m:
            lw[m.group(2)] = float(m.group(3).replace(",", "."))
    if len(lw) != 60:
        raise AssertionError(f"в таблице LW разобрано {len(lw)} бумаг вместо 60 — проверь парсер")
    return lw


def release_text() -> str:
    raw = (SRC / "press_release_n103733.html").read_text(encoding="utf-8", errors="replace")
    t = re.sub(r"<script.*?</script>|<style.*?</style>", " ", raw, flags=re.S)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "\n", t)))


def parse_ff() -> dict[str, float]:
    """Новые free-float из таблицы релиза. Ровно 15 бумаг, иначе релиз изменился."""
    raw = (SRC / "press_release_n103733.html").read_text(encoding="utf-8", errors="replace")
    t = re.sub(r"<script.*?</script>|<style.*?</style>", " ", raw, flags=re.S)
    lines = [re.sub(r"\s+", " ", html.unescape(x)).strip()
             for x in re.sub(r"<[^>]+>", "\n", t).split("\n")]
    lines = [x for x in lines if x]
    i = next(k for k, x in enumerate(lines) if "Новый free-float" in x)
    tail, ff = lines[i + 1:i + 80], {}
    for k in range(len(tail) - 2):
        if re.fullmatch(r"[A-Z][A-Z0-9]{0,7}", tail[k]) and re.fullmatch(r"\d{1,3}%", tail[k + 2]):
            ff[tail[k]] = int(tail[k + 2].rstrip("%")) / 100
    if len(ff) != 15:
        raise AssertionError(f"в релизе разобрано {len(ff)} free-float вместо 15")
    return ff


def parse_imoex_changes() -> dict:
    """Исключения и включения по Индексу МосБиржи.

    Критично именно ВКЛЮЧЕНИЕ: любая новая бумага в базе перераспределяет веса, и все
    рублёвые оценки становятся неверны. Поэтому проверяем явно, а не «вроде не было».
    """
    t = release_text()
    m = re.search(r"Индекс[аовы]{0,3} МосБиржи и Индекс[аовы]{0,3} РТС(.{0,220})", t)
    seg = (m.group(1) if m else "").strip()
    # фраза вида «покинут обыкновенные акции МКПАО "Лента" и ПАО "Мосэнерго"» —
    # кавычки в релизе ПРЯМЫЕ, не «ёлочки»; ищем и те, и другие
    excl = re.search(r"покин\w+(.{0,160}?)(?:\.|База расчета)", seg)
    names = re.findall(r"[«\"]([^»\"]{2,40})[»\"]", excl.group(1)) if excl else []
    return {"фрагмент_релиза": seg[:220],
            "исключены": names,
            # ищем включение ИМЕННО в этом предложении, до перехода к другому индексу
            "включения_в_imoex": bool(re.search(r"войдут|включен", seg.split("База расчета")[0]))}


def main() -> int:
    DATA.mkdir(parents=True, exist_ok=True)

    print("1. таблица LW из PDF")
    lw = parse_lw()
    (DATA / "lw_new.json").write_text(json.dumps(lw, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"   разобрано {len(lw)} бумаг, T={lw['T']}, TATN={lw['TATN']}, "
          f"LKOH={'нет→1,0' if 'LKOH' not in lw else lw['LKOH']}")

    print("2. free-float из релиза")
    ff = parse_ff()
    (DATA / "ff_new.json").write_text(json.dumps(ff, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"   разобрано {len(ff)} бумаг")

    print("3. изменения состава IMOEX")
    ch = parse_imoex_changes()
    (DATA / "imoex_changes.json").write_text(json.dumps(ch, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"   исключены: {ch['исключены']} | включения упомянуты: {ch["включения_в_imoex"]}")

    print("4. состав IMOEX и коэффициенты из ISS")
    comp = iss("https://iss.moex.com/iss/statistics/engines/stock/markets/index/"
               "analytics/IMOEX.json?iss.meta=off&limit=100")["analytics"]
    rows = [dict(zip(comp["columns"], r)) for r in comp["data"]]
    last = max(x["tradedate"] for x in rows)
    tickers = sorted({x["ticker"] for x in rows if x["tradedate"] == last})
    print(f"   состав на {last}: {len(tickers)} бумаг")
    detail = {}
    for n, t in enumerate(tickers, 1):
        b = iss("https://iss.moex.com/iss/statistics/engines/stock/markets/index/"
                f"analytics/IMOEX/tickers/{t}.json?iss.meta=off&from={FIX_DATE}&till={ASOF}")["ticker"]
        # ⚠️ ISS отдаёт строки НЕ по порядку дат — сортируем сами, иначе «последняя»
        # строка окажется не последней (я на этом уже ошибся на весе ЛУКОЙЛа).
        got = sorted((dict(zip(b["columns"], r)) for r in b["data"]), key=lambda r: r["tradedate"])
        detail[t] = {r["tradedate"]: {"weight": r["weight"], "ff": r["ff_factor"],
                                      "w": r["w_factor"], "issue": r["issue_size_total"],
                                      "cap": r["cap_total"]} for r in got}
        if n % 10 == 0:
            print(f"   ...{n}/{len(tickers)}")
        time.sleep(0.6)
    (DATA / "iss_imoex.json").write_text(
        json.dumps({"состав_на": last, "бумаги": detail}, ensure_ascii=False, indent=1),
        encoding="utf-8")

    print("5. цены из нашей БД")
    q = ",".join(f"'{t}'" for t in tickers)
    pr = db(f"SELECT secid, begin_time::date, close FROM candles WHERE interval = 24 "
            f"AND begin_time::date IN ('{FIX_DATE}','{ASOF}') AND secid IN ({q}) ORDER BY 1,2")
    prices: dict[str, dict[str, float]] = {}
    for secid, d, c in pr:
        prices.setdefault(secid, {})[d] = float(c)
    (DATA / "prices.json").write_text(json.dumps(prices, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"   цен: {len(prices)} бумаг × {len(next(iter(prices.values())))} даты")

    print("6. фонды: СЧА и позиции")
    f = ",".join(f"'{t}'" for t in FUNDS)
    nav = db(f"SELECT f.ticker, d.trade_date, d.nav FROM funds f JOIN fund_data d "
             f"ON d.fund_id = f.fund_id WHERE f.ticker IN ({f}) AND d.trade_date = "
             f"(SELECT max(trade_date) FROM fund_data WHERE nav IS NOT NULL) ORDER BY 1")
    # ⚠️ ТОЛЬКО ОФИЦИАЛЬНОЕ РАСКРЫТИЕ. source='interfax_manual' — это точный SCHA
    # (справка о СЧА, форма ЦБ № 0420502) из официального раскрытия УК; так помечен
    # источник в .claude/skills/moex-fund-scha-backfill/SKILL.md.
    # НЕ БРАТЬ source='vim': это ежедневный HTML-парсинг сайта ВИМ, помеченный в
    # api/routers/fund_trades.py как WIP, и в семействе vim документирован баг ×1000
    # (срезалась группа «000» в количестве), затронувший как раз EQMX. Первая версия
    # этого скрипта брала «последнюю строку по каждому ISIN» по всем источникам и
    # смешивала парсер с документами — числа по EQMX оказывались из парсера.
    # Заодно это даёт всем трём фондам ОДНУ дату снимка, что честнее для сравнения.
    SCHA = "interfax_manual"
    pos = db(f"SELECT f.ticker, h.isin, h.snapshot_date, h.positions, h.asset_name, h.amount_rub "
             f"FROM fund_holdings_history h JOIN funds f ON f.fund_id = h.fund_id "
             f"WHERE f.ticker IN ({f}) AND h.source = '{SCHA}' AND h.positions IS NOT NULL "
             f"AND h.snapshot_date = (SELECT max(h2.snapshot_date) "
             f"    FROM fund_holdings_history h2 WHERE h2.fund_id = h.fund_id "
             f"      AND h2.source = '{SCHA}' AND h2.positions IS NOT NULL) "
             f"ORDER BY f.ticker, h.weight DESC NULLS LAST")
    funds = {"сча": {t: {"дата": d, "руб": float(v)} for t, d, v in nav},
             "источник": SCHA,
             "позиции": [{"фонд": a, "isin": b, "снимок": c, "штук": int(d), "имя": e,
                          "руб_в_справке": float(g) if g else None}
                         for a, b, c, d, e, g in pos]}
    (DATA / "funds.json").write_text(json.dumps(funds, ensure_ascii=False, indent=1), encoding="utf-8")
    tot = sum(x["руб"] for x in funds["сча"].values())
    parts = [f"{k} {v['руб']/1e9:.2f}" for k, v in funds["сча"].items()]
    print(f"   СЧА: {', '.join(parts)} млрд, сумма {tot/1e9:.3f} млрд "
          f"на {next(iter(funds['сча'].values()))['дата']}")
    print(f"   позиций: {len(funds['позиции'])} строк")

    print(f"\nготово, данные в {DATA}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
