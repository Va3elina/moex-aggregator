"""Плечо мастера ITEKCrypto Kamaz и расчёт плеча для нашего «спокойного Kamaz». Только исследование: живой бот, сервер
и заявки не трогаем (ручки Bybit — только публичные, на чтение).

Разделы (печатаются и пишутся в kamaz_leverage.md рядом):
  1. Мастер: что пишет и что показывает Bybit (кошелёк, AUM, доходность 90 дн.), плечо на бирже по монетам, первая заявка,
     докупки, номинал кампаний; одновременный номинал позиций / капитал мастера = настоящая нагрузка на счёт;
     запас до ликвидации isolated по каждой кампании.
  2. Наш демо-бот: что выставлено на бирже (по коду и снимку счёта 02.10).
  3. Бэктест: ежедневная десятка 07.2021–27.09.2026, $100k (10 ячеек по $10k), рабочий вариант A и вариант B (вход внутри
     минуты), цель обычная и 3.9/3.0, плечо стратегии x1/x1.5/x2/x3 (лесенка целиком = L × ячейка, кусок от стартовой
     ячейки и не больше текущего счёта ячейки). Два учёта ликвидации: «ячейка» (каждая монета сама за себя, как раньше)
     и «общий счёт» (cross: ячейка может уйти в минус, ликвидация — только если весь счёт ниже поддерживающей маржи).
     + худшее исполнение (лимитки при проходе 0.1%, комиссии ×2), фандинг и комиссии отдельно,
     + плечо только на монетах с сильной связью с биткоином (корреляция ≥ 0.6 за 60 дн.) или только на «ядре»,
     + что было бы при isolated xN на бирже (сколько кампаний ушло бы ниже цены ликвидации).
Движок — копия grid_engine.run с плечом по минутам, учётом фандинга/комиссий, худшей точки кампании и без ликвидации
ячейки для режима «общий счёт»; при постоянном плече и с ликвидацией совпадает с grid_engine.run (проверка в начале).

Запуск: .venv/bin/python experiments/kamaz_leverage.py [--fetch] [--skip-bt]
  --fetch  — заново снять публичные ручки мастера (кошелёк, доходность, кривая 90 дн.) через Chrome без входа
Процессы — spawn (fork после чтения parquet виснет).
"""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

os.environ.setdefault("UNIV_SET", "one")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))
from factory import market  # noqa: E402
import grid_engine as ge  # noqa: E402
import kamaz_missed_entries as kme  # noqa: E402

MASTER = ROOT / "inbox/private/kamaz_master"
OUT = Path(__file__).with_suffix(".md")
LINES: list[str] = []
E0 = 10_000.0
CAP = 100_000.0
T0 = pd.Timestamp("2021-06-01")
BLK = 5                                        # минут в блоке для сумм по счёту (берём худшее значение блока)
MMR = 0.005                                    # поддерживающая маржа ~0.5% (у альтов на малых объёмах 0.5–1%)
CORR_HI = 0.6
CORR_LO = 0.4
CORE = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"}
LEVS = [1.0, 1.5, 2.0, 3.0]
TGT = {"цель 1.94/1.6": {}, "цель 3.9/3.0": kme.LONG}
BASE = dict(kme.BASE)                          # btc_filter=False, L=1, cap_to_equity=True, crash_skip=25
REAL = dict(kme.REAL)                          # through=0.1, maker/taker ×2

# Снимок нашего демо-бота 02.10.2026 13:22 UTC (ssh на сервер, только чтение; ключи не выводились)
BOT_SNAPSHOT = dict(
    execstart="run.py run --mode demo --sizing slot --capital 50000 --max-camps 10",
    margin_mode="REGULAR_MARGIN (cross, весь единый счёт — залог всех позиций)",
    usdt_balance=99_902.19, total_equity=189_018.59, open_positions=0,
    set_leverage="3 (run.py при старте: set_leverage(s, \"3\") по монетам десятки, где первая покупка ≥ мин. заявки)",
)


SUMMARY = """
## Коротко

**У мастера плечо x5 только на бирже. По счёту оно около x1, в пике x2.** «Плечо Х5» — это настройка каждой позиции на Bybit:
isolated x5 (у ZEC x2, у SUI x4.8), стопа нет. Первая заявка $1 775 — это позиция на $1 775, под неё биржа берёт залог $355.
Его собственных денег в копи-счёте $10.7k (публичная ручка wallet-info). По кривой за 90 дней — $9.4–14.9k, прибыль он выводит
(22→23.09 оценка капитала ~$15.2k → ~$10.4k). Подписчиков 188, их денег $274k, ему идёт 15% их прибыли.

| мастер, 06–10.2026 | значение |
|---|---|
| плечо на бирже / режим | x5 (ZEC x2, SUI x4.8) / isolated, стопа нет |
| первая заявка: позиция / залог | $1 775 / $355 (PEPE, DOGE, SOL, XRP); $1 420 / $284 (ADA, AVAX, SUI, HYPE, ETH); $700 (SHIB); $460 (ZEC, x2) |
| докупок в кампании | обычно 0–3; BCH 40 и SUI 18 — недели набора без стопа |
| **настоящая нагрузка: все позиции / капитал** | **медиана x1.06, 90% времени ≤ x1.39, максимум x1.99** (17.08: ADA $7.0k + BCH $10.3k + PEPE $1.4k) |
| занятый залог, максимум | $4.7k = 37% капитала |
| худший открытый убыток | −$1.8k = −14.7% капитала (15.09; Bybit пишет просадку −15.9%) |
| ближе всего к ликвидации | SUI 15.09: −14.2% от средней при ликвидации на −20.3% |

**Номинальное плечо ≠ нагрузка на счёт.** Плечо на бирже решает только, сколько денег биржа запирает под позицию.
Isolated x5 при позиции $1 775 значит: залог $355, и если цена уйдёт на ≈19.5% ниже средней, биржа закроет эту позицию.
Мастер потеряет $355 плюс залоги докупок, остальной счёт не пострадает. По сути это встроенный стоп на −19.5%.
В режиме cross (общий счёт) x5 вообще не двигает цену ликвидации: биржа закроет всё, только когда весь счёт упадёт ниже
поддерживающей маржи (~0.5–1% от всех позиций). Нагрузку на деньги показывает другое: сумма позиций / капитал.

**Наш демо-бот.** Лесенка ячейки = $5 000, 10 ячеек = бюджет $50k, это x1 по стратегии. На бирже стоит x3 (так в коде
`run.py`), режим счёта cross (REGULAR_MARGIN), на демо-счёте $99.9k USDT. Значит, нагрузка не больше x0.5 от счёта
и x1 от бюджета. x3 на бирже лишь запирает до $16.7k залога, ликвидация при такой нагрузке практически невозможна.
Наш x1 по нагрузке ≈ обычный уровень мастера (x1.06), наш x2 ≈ его пик.

**Расчёт плеча (десятка дня, 07.2021–27.09.2026, $100k = 10 ячеек по $10k).** Кусок — от стартовой ячейки, не растёт с прибылью,
после убытков не больше денег ячейки. Фандинг и комиссии внутри итога.

| вариант A (как бот), цель 1.94/1.6 | итог | CAGR | 2025 | 2026 | просадка | 10.10.25 | ликвидаций | нужно свободных на счёте |
|---|---|---|---|---|---|---|---|---|
| x1 (сейчас) | +$106k | 14.7% | +8.9% | +6.5% | −4.9% | −$9.1k | 0 | $45k |
| x1.5 | +$157k | 19.7% | +13.2% | +9.4% | −5.9% | −$13.6k | 0 | $68k |
| x2 | +$184k | 22.1% | −1.5% | +7.8% | −12.4% | −$35.5k | 4 | $71k |
| x3 | +$252k | 27.1% | −26.0% | +20.0% | −18.9% | −$71.4k | 7 | $100k |
| x2 + фильтр BTC, на монетах со слабой связью с BTC (корр. < 0.4) x1 | +$151k | 19.2% | +33.6% | +1.6% | −5.7% | +$0.1k | 0 | $16k |
| то же, худшее исполнение (проход 0.1%, комиссии ×2) | +$106k | 14.8% | +28.7% | −9.2% | −8.3% | +$0.1k | 0 | — |
| x1, худшее исполнение (для сравнения) | +$64k | 9.9% | +4.2% | −5.1% | −7.0% | −$9.0k | 0 | — |
| x2 только на «ядре» (BTC, ETH, SOL, XRP, DOGE), без фильтра | +$140k | 18.2% | +9.9% | +8.4% | −5.9% | −$13.6k | 0 | $63k |

Ликвидации при x2–x3 — это 10.10.25 (FARTCOIN, PUMPFUN, DOGE, SUI, HYPE), а ещё SIREN 17.04.26, BANK 30.07.26 и LUNA2 14.09.22.
Учёт «ячейка сама за себя». Вариант B и цель 3.9/3.0 растут от плеча так же: B 3.9/3.0 x1 +$202k / −8.0%, x2 +$390k / −13.4%,
но 3 ликвидации и −$33.6k за 10.10.25. Это пока гипотезы из разбора пропусков мастера, на демо не проверены.

Что ещё показал расчёт:
- **Общий счёт (cross) против «ячейки».** В cross прокол 10.10.25 не ликвидирует отдельные монеты, и они отрастают.
  Поэтому x2 теряет за день −$18k, а не −$35k, и прибыль выше (+$208k). Но худший открытый убыток x2 −$89k, x3 −$134k
  на $100k. Счёт, открытый с $100k прямо перед 10.10.25, при x3 ликвидировало бы целиком, при x2 остался бы запас ~$9k.
  Сейчас пережить это позволяет только прибыль прошлых лет, лежащая на счёте.
- **Isolated xN, как у мастера, нашей стратегии вредит.** Это стоп на −(1/N − 0.5%) от средней, и он срабатывает именно
  на проколах, на которых лесенка потом зарабатывает. При x1 по деньгам и isolated x5 на бирже ниже −19.5% ушли бы
  42 кампании из 9 739, это −$37k к итогу. При x10: 175 кампаний и −$76k, при x3: 12 и −$15k.
- **10.10.25 в 21:20 все 10 ячеек стояли с полной лесенкой** (6 докупок у каждой). Это главный риск плеча: одна минута бьёт
  по всем монетам сразу. При x1 открытый убыток был −$45k.
- **Фандинг не издержка, а небольшой доход:** +$4.0k при x1 и +$7.9k при x2 за 5 лет. Мы покупаем после обвалов, когда ставка
  отрицательная и платят шорты. Комиссии: $10.6k при x1 и $21k при x2.
- **Защиты.** Пропуск монеты после −25% за сутки включён везде. **Фильтр BTC** («не входить, пока биткоин ниже минимума 12 ч»)
  сильнее всех снижает риск плеча. Худший открытый убыток x2 падает с −$69k до −$16k, 10.10.25 из −$35k становится ≈ 0.
  Цена — недобор в спокойные годы: 2024 при x2 +48.8% против +74.2%. Оговорка: фильтр выбран 27.09 после того, как видели
  2025 год, часть его защиты на 10.10.25 — знание задним числом. Остальные ликвидации с фильтром (SIREN, BANK) — мем-монеты,
  которые падают без биткоина. Их убирает правило «x1 на монетах с корр. < 0.4». **Корреляция ≥ 0.6 как допуск к плечу
  не работает:** FARTCOIN и SUI на 10.10.25 шли вместе с BTC и всё равно ликвидированы. **Лимит одновременных кампаний**
  (оценка задним числом): x2 при лимите 5 даёт худшую точку 10.10.25 около −$41k — как x1 без лимита (−$45k), —
  и примерно +50% к прибыли. Но это по сути «5 ячеек по $20k», и фильтр BTC даёт больше за меньшую цену.

**Вывод (это расчёт стратегии, не совет).**
- **Демо.** На бирже менять ничего не нужно. Плечо не меняет сигналы и исполнение, только размер, поэтому его можно считать
  по журналу x1: результат × L, плюс проверка «нужно на счёте». Если хотим видеть плечо вживую на демо, разумный вариант —
  L = 2 на том же демо-счёте $100k (нагрузка x1 к счёту), с фильтром BTC и x1 на монетах с корр. < 0.4. На бирже оставить
  cross и x3–x5, чтобы заранее выставленные лесенки помещались в свободную маржу. Тогда главная проверка демо — исполнение
  лимиток — смешается с плечом. Чище держать x1 на демо и считать плечо отдельно.
- **Реальные деньги.** Пока демо не подтвердило исполнение — x1. Плечо умножает и ошибку исполнения: при худшем исполнении
  x2 + фильтр в 2026 году −19%, x2 + фильтр с x1 на слабых монетах −9%. Если потом плечо, то не выше x1.5–2 и только вместе
  с правилами:
  1. cross, а не isolated;
  2. фильтр BTC;
  3. x1 на монетах с корр. < 0.4 и на молодых монетах (корреляция не посчитана — тоже x1);
  4. пропуск монеты после −25%/сутки;
  5. кусок от стартового капитала, прибыль выводить;
  6. на торговом счёте держать свободными не меньше строки «нужно свободных на счёте» (x2 без фильтра ≈ $71k на $100k,
     с фильтром ≈ $16k), а не в стейкинге.
  x3 по 2021–2026: −26% в 2025 году и −$71k за 10.10.25 (ячейка) или полная ликвидация счёта, начатого без запаса прибыли (cross).
"""


def say(s: str = ""):
    print(s, flush=True)
    LINES.append(s)


def md_table(df: pd.DataFrame, floatfmt=2) -> str:
    """Как в kamaz_missed_entries, но «|» в названиях вариантов → «·» (иначе ломает таблицу)."""
    d = df.copy()
    for c in d.columns:
        if d[c].dtype == object or pd.api.types.is_string_dtype(d[c]):
            d[c] = d[c].map(lambda v: v.replace(" | ", " · ") if isinstance(v, str) else v)
    return kme.md_table(d, floatfmt)


# ================================================================ 1. мастер

def fetch_leader() -> Path:
    """Публичные ручки страницы мастера (без входа, только чтение) → inbox/private/kamaz_master/leader_<дата>.json."""
    import kamaz_master_sync as km
    from playwright.sync_api import sync_playwright
    js = """async (m) => {
      const e = encodeURIComponent(m); const out = {};
      const urls = {info: 'private/v1/pub-leader/info?leaderMark=' + e, income: 'public/v1/common/leader-income?leaderMark=' + e,
                    wallet: 'public/v1/leader/wallet-info?leaderMark=' + e,
                    trend90: 'public/v2/leader/yield-trend?dayCycleType=DAY_CYCLE_TYPE_NINETY_DAY&period=PERIOD_DAY&leaderMark=' + e,
                    positions: 'public/v1/common/position/list?leaderMark=' + e};
      for (const [k, u] of Object.entries(urls)) {
        const r = await fetch('/x-api/fapi/beehive/' + u); const j = await r.json();
        if (j.retCode !== 0) throw new Error(k + ': ' + j.retMsg); out[k] = j.result; await new Promise(r => setTimeout(r, 300)); }
      return out; }"""
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, channel="chrome", args=["--disable-blink-features=AutomationControlled"])
        pg = b.new_context(user_agent=km.UA, locale="en-US", viewport={"width": 1280, "height": 900}).new_page()
        pg.goto(km.PAGE, wait_until="domcontentloaded", timeout=60000)
        time.sleep(4)
        data = pg.evaluate(js, km.MARK)
        b.close()
    data["fetched_at"] = str(pd.Timestamp.now(tz="UTC").tz_localize(None).floor("s"))
    f = MASTER / f"leader_{pd.Timestamp.now():%Y-%m-%d}.json"
    f.write_text(json.dumps(data, ensure_ascii=False, indent=1))
    return f


def load_leader() -> dict:
    fs = sorted(MASTER.glob("leader_*.json"))
    if not fs:
        raise SystemExit("нет снимка мастера — запустить с --fetch")
    return json.loads(fs[-1].read_text())


def e8(x) -> float:
    return int(x) / 1e8


def capital_series(L: dict) -> pd.Series:
    """Капитал мастера по дням ≈ доход дня / доходность дня (Bybit считает доходность дня от капитала на начало дня).
    Берём дни с |доходностью| ≥ 0.5% (иначе округление E4 шумит), сглаживаем медианой 7 дней."""
    rows = []
    for r in L["trend90"]["yieldTrend"]:
        y, rt = e8(r["yieldE8"]), int(r["yieldRateE4"]) / 1e4
        rows.append((pd.to_datetime(int(r["statisticDate"]), unit="ms"), y, rt, e8(r["totalYieldE8"])))
    T = pd.DataFrame(rows, columns=["d", "y", "rate", "cum"]).set_index("d")
    base = (T.y / T.rate).where(T.rate.abs() >= 0.005)
    T["raw"] = base
    T["base"] = base.rolling(7, min_periods=1, center=True).median().ffill().bfill()
    return T


def section_master():
    L = load_leader()
    inf, inc = L["info"], L["income"]
    wallet = e8(L["wallet"]["walletBalanceE8"])
    T = capital_series(L)
    say("## 1. Мастер ITEKCrypto Kamaz: какое у него плечо\n")
    say(f"*Снимок публичной страницы {L['fetched_at']} UTC (кошелёк, доходность, кривая 90 дн.) + накопитель сделок "
        f"`closed.jsonl` (ордера 08.06–02.10.2026) и открытая позиция.*\n")
    say("### 1а. Что пишет он и что показывает Bybit\n")
    T1 = pd.DataFrame([
        ("Описание мастера", inf["leaderUserIntroduction"].split(":")[0].strip() + " : Stop Loss OFF"),
        ("Метка Bybit", "Low Leverage — «среднее плечо за 30 дней ниже 5X» (это настройка плеча на бирже, а не нагрузка счёта)"),
        ("Кошелёк мастера (его деньги в копи-счёте)", f"${wallet:,.0f}"),
        ("Деньги подписчиков (AUM)", f"${e8(inf['aumE8']):,.0f}, подписчиков {inf['currentFollowerCount']}, доля мастеру {e8(inf['shareProfitRateE8']) * 100:.0f}%"),
        ("Его прибыль за всё время / за 90 дн.", f"${e8(inc['cumYieldE8']):,.0f} / ${e8(inc['ninetyDayProfitE8']):,.0f} (+{int(inc['ninetyDayYieldRateE4']) / 100:.1f}% за 90 дн.)"),
        ("Прибыль подписчиков за 90 дн.", f"${e8(inc['ninetyDayFollowerYieldE8']):,.0f}"),
        ("Макс. просадка (Bybit, 90 дн.)", f"−{int(inc['ninetyDayDrawDownE4']) / 100:.1f}%"),
        ("Капитал по кривой 90 дн. (доход дня / доходность дня)", f"${T.base.min():,.0f}…${T.base.max():,.0f}, медиана ${T.base.median():,.0f}"),
    ], columns=["что", "значение"])
    say(md_table(T1)); say()
    rw = T.raw.dropna()
    big = [(d0, v0, d1, v1) for (d0, v0), (d1, v1) in zip(rw.items(), list(rw.items())[1:]) if v1 - v0 < -2500]
    if big:
        say("Капитал не растёт вместе с прибылью — выводит: " + ", ".join(f"{d0:%d.%m} ~${v0:,.0f} → {d1:%d.%m} ~${v1:,.0f}" for d0, v0, d1, v1 in big)
            + " (оценка капитала на начало дня падает сильнее, чем убыток дня). Кусок у него фактически «от стартового», ~$10–15k.\n")

    D = kme.master_orders()
    D["notional"] = D["size"] * D.order_price
    D["margin"] = D.notional / D.lev
    C = kme.campaigns(D)
    syms = sorted(D.sym.unique())
    a, b = D.t_open.min().floor("h") - pd.Timedelta(hours=1), kme.END
    H = {s: market.klines(s, "60", a - pd.Timedelta(days=1), b).astype(float) for s in syms}

    say("### 1б. Плечо на бирже, кусок и докупки по монетам (06–10.2026)\n")
    rows = []
    for s, g in D.groupby("sym"):
        cs = C[C.sym == s]
        first = cs.orders.apply(lambda o: o[0][1] * o[0][2])
        rows.append({"монета": s.replace("USDT", ""), "плечо на бирже": "/".join(f"x{v:g}" for v in sorted(g.lev.unique())),
                     "режим": "isolated", "ордеров": len(g), "кампаний": len(cs),
                     "первая заявка $": first.median(), "её маржа $": (first / g.lev.iloc[0]).median(),
                     "докупок макс": int(cs.n.max() - 1), "номинал кампании макс $": cs.usd.max(),
                     "маржа кампании макс $": (cs.usd / g.lev.iloc[0]).max()})
    T2 = pd.DataFrame(rows).sort_values("первая заявка $", ascending=False)
    say(md_table(T2, 0)); say()
    say(f"Все {len(D)} ордеров (закрытые + открытая часть SOL) — isolated (`isIsolated=true`). Первая заявка $1 775 = маржа $355 × 5; $1 420 = $284 × 5; "
        f"$700 = $140 × 5; ZEC $460 = $230 × 2. Кампаний {len(C)}, из них с докупками {int((C.n > 1).sum())}; две «вечные» "
        f"кампании набирались неделями: BCH 08.06–22.09 (ордеров: {int(C[C.sym == 'BCHUSDT'].n.max())}, суммарно ${C[C.sym == 'BCHUSDT'].usd.max():,.0f}) "
        f"и SUI 22.08–20.09 (ордеров: {int(C[C.sym == 'SUIUSDT'].n.max())}, суммарно ${C[C.sym == 'SUIUSDT'].usd.max():,.0f}) — "
        "не одновременно весь объём: ордера закрывались и открывались заново.\n")

    # одновременный номинал по часам (по рынку) и маржа
    grid = pd.date_range(a, b, freq="h")
    NT = pd.Series(0.0, grid); MG = pd.Series(0.0, grid); UR = pd.Series(0.0, grid)
    by_sym_nt = {}
    for s, g in D.groupby("sym"):
        px = H[s].c.reindex(grid, method="ffill"); lo = H[s].l.reindex(grid, method="ffill")
        q = pd.Series(0.0, grid); cost = pd.Series(0.0, grid); mg = pd.Series(0.0, grid)
        for r in g.itertuples():
            end = r.t_close if pd.notna(r.t_close) else b + pd.Timedelta(hours=1)
            m = (grid >= r.t_open.floor("h")) & (grid < end.floor("h"))
            q[m] += r.size; cost[m] += r.size * r.order_price; mg[m] += r.margin
        nt = q * px
        by_sym_nt[s] = nt
        NT += nt; MG += mg; UR += q * lo - cost
    cap = T.base.reindex(grid.floor("D")).values
    cap = pd.Series(cap, grid).bfill().ffill()
    first_known = T.index.min()
    EL = NT / cap
    inpos = NT > 0
    say("### 1в. Настоящая нагрузка на счёт: одновременный номинал позиций / капитал мастера (по часам)\n")
    say(f"Капитал до {first_known:%d.%m} неизвестен — взят первый известный (~${T.base.iloc[0]:,.0f}). Номинал — по закрытию часа.\n")
    q = lambda s, x: s[inpos].quantile(x)  # noqa: E731
    T3 = pd.DataFrame([
        ("в позиции, % времени", f"{inpos.mean() * 100:.0f}%"),
        ("номинал позиций, медиана / 90% / макс", f"${q(NT, .5):,.0f} / ${q(NT, .9):,.0f} / ${NT.max():,.0f} ({NT.idxmax():%d.%m %H:%M})"),
        ("маржа в позициях (заблокировано), медиана / макс", f"${q(MG, .5):,.0f} / ${MG.max():,.0f} = {MG.max() / cap[MG.idxmax()] * 100:.0f}% капитала"),
        ("**эффективное плечо = номинал / капитал**, медиана / 90% / макс", f"**x{q(EL, .5):.2f} / x{q(EL, .9):.2f} / x{EL.max():.2f}** ({EL.idxmax():%d.%m %H:%M})"),
        ("худший открытый убыток (по минимуму часа)", f"${UR.min():,.0f} ({UR.idxmin():%d.%m %H:%M}) = {UR.min() / cap[UR.idxmin()] * 100:.1f}% капитала"),
    ], columns=["что", "значение"])
    say(md_table(T3)); say()
    top = EL.resample("D").max().sort_values(ascending=False).head(5)
    say("Самые нагруженные дни (эфф. плечо, макс за день): " + ", ".join(f"{d:%d.%m} x{v:.2f}" for d, v in top.items()) + ".\n")
    mx = EL.idxmax()
    say("В момент максимума: " + ", ".join(f"{s.replace('USDT', '')} ${by_sym_nt[s][mx]:,.0f}" for s in syms if by_sym_nt[s][mx] > 0) + ".\n")

    say("### 1г. Запас до ликвидации isolated по кампаниям мастера\n")
    say("Isolated: залог позиции = номинал / плечо, остальной счёт не участвует. Ликвидация, когда цена ниже средней на "
        "≈ 1/плечо − 0.5%: x5 → −19.5%, x4.8 → −20.3%, x2 → −49.5%. Худшая точка — по минимуму часа против средней на тот момент.\n")
    rows = []
    for c in C.itertuples():
        g = sorted(c.orders)
        t_end = c.t1 if pd.notna(c.t1) else b
        hh = H[c.sym].loc[g[0][0].floor("h"):t_end]
        worst, wt, avg_w = 0.0, None, None
        for t, lo in hh.l.items():
            fill = [(px, sz) for (to, px, sz) in g if to.floor("h") <= t]
            if not fill:
                continue
            avg = sum(px * sz for px, sz in fill) / sum(sz for _, sz in fill)
            r_ = lo / avg - 1
            if r_ < worst:
                worst, wt, avg_w = r_, t, avg
        rows.append(dict(монета=c.sym.replace("USDT", ""), вход=c.t0, ордеров=c.n, плечо=c.lev, худшая=worst * 100, когда=wt,
                         ликвидация=-(1 / c.lev - MMR) * 100, запас=(worst + 1 / c.lev - MMR) * 100))
    Q = pd.DataFrame(rows).sort_values("запас").head(8)
    Q["вход"] = Q.вход.dt.strftime("%d.%m.%y"); Q["когда"] = Q.когда.map(lambda x: f"{x:%d.%m %H:%M}" if pd.notna(x) else "")
    Q = Q.rename(columns={"худшая": "худшая точка от средней %", "ликвидация": "ликвидация при %", "запас": "запас, п.п."})
    say(md_table(Q, 1)); say()
    return dict(wallet=wallet, cap_med=float(T.base.median()), el_med=float(q(EL, .5)), el_max=float(EL.max()), el_t=EL.idxmax(),
                nt_max=float(NT.max()), mg_max=float(MG.max()), ur_min=float(UR.min()), Q=Q)


# ================================================================ 2. наш бот

def section_bot():
    say("## 2. Что у нас: демо-бот на Bybit\n")
    piece = 50_000 / 10 / float(ge.SIZES.sum())
    T = pd.DataFrame([
        ("служба на сервере", BOT_SNAPSHOT["execstart"]),
        ("ячейки", "10 × $5 000 (бумажная книга рядом — 10 × $10 000)"),
        ("первая покупка / вся лесенка ячейки", f"${piece:,.0f} / $5 000 (1 : 3.08 : 4.19 : 4.82 : 5.54 : 6.37 : 7.33, сумма 32.33)"),
        ("плечо стратегии (лесенка / ячейка)", "x1 (L=1): полностью набранная лесенка = деньги ячейки"),
        ("плечо на бирже", BOT_SNAPSHOT["set_leverage"]),
        ("режим маржи счёта", BOT_SNAPSHOT["margin_mode"]),
        ("на демо-счёте (снимок 02.10 13:22 UTC)", f"USDT ${BOT_SNAPSHOT['usdt_balance']:,.0f} (всего с монетами ${BOT_SNAPSHOT['total_equity']:,.0f}), открытых позиций {BOT_SNAPSHOT['open_positions']}"),
        ("макс. номинал (все 10 лесенок набраны)", "$50 000 = x0.5 к USDT на счёте, x1.0 к бюджету бота"),
        ("заблокированная маржа при x3", "$1 667 на полную лесенку, $16 700 на все 10"),
    ], columns=["что", "значение"])
    say(md_table(T)); say()


# ================================================================ 3. движок с плечом

def run_lev(coin, p: ge.P, Larr: np.ndarray | None = None, liq_on: bool = True, blocks: bool = False):
    """Копия grid_engine.run (лонг, без стопа и простоев) + плечо по минутам входа, фандинг/комиссии, худшая точка кампании
    против средней (mae), режим без ликвидации ячейки (liq_on=False — «общий счёт»), суммы по 5-мин блокам."""
    assert p.side == 1 and p.hard_stop is None and not p.outages
    k = coin.k; o, h, l, c = (k[x].values for x in "ohlc"); n = len(c)
    sig = coin.signal(p)
    scale_arr = isinstance(p.scale, np.ndarray)
    FULL = p.sizes[:len(p.levels) + 1].sum()
    cash = E0; x = None; eq = np.empty(n); eq[0] = E0; liq = None; trades = 0; camps = []; cool = -1
    nt = np.zeros(n, np.float32); ur = np.zeros(n, np.float32); eqw = np.full(n, E0, np.float32)
    thr = p.through / 100; slip = p.slip / 100
    fund_paid = 0.0; fees = 0.0
    for i in range(1, n):
        if x is not None and p.funding_on and coin.fund[i]:
            f_ = x["Q"] * c[i - 1] * coin.fund[i]; cash -= f_; fund_paid += f_
        if x is None:
            if sig[i - 1] and i >= cool:
                Lx = float(Larr[i - 1]) if Larr is not None else p.L
                bu = min(E0, cash) * Lx / FULL if p.cap_to_equity else E0 * Lx / FULL
                if bu > 0:
                    px_in = o[i] * (1 + slip)
                    q = bu / px_in
                    sc = p.scale[i - 1] if scale_arr else p.scale
                    x = dict(Q=q, cost=q * px_in, p0=px_in, k=0, last=i, t0=i, c0=cash, bu=bu, sc=sc, lv=p.levels * sc / 100, mae=0.0, L=Lx)
                    fe = q * px_in * p.taker; cash -= fe; fees += fe; trades += 1
        else:
            while x["k"] < len(x["lv"]):
                lvl = x["p0"] * (1 - x["lv"][x["k"]])
                if not (l[i] <= lvl * (1 - thr)):
                    break
                q = x["bu"] / x["p0"] * p.sizes[x["k"] + 1]
                x["Q"] += q; x["cost"] += q * lvl; x["k"] += 1; x["last"] = i
                fe = q * lvl * p.maker; cash -= fe; fees += fe; trades += 1
            avg = x["cost"] / x["Q"]
            x["mae"] = min(x["mae"], l[i] / avg - 1)
            tp = x["p0"] * (1 + p.tp1 / 100) if x["k"] == 0 else avg * (1 + p.tpn / 100)
            if i > x["last"] and h[i] >= tp * (1 + thr):
                pnl = (tp - avg) * x["Q"]; fe = tp * x["Q"] * p.maker
                cash += pnl - fe; fees += fe; trades += 1
                camps.append((k.index[x["t0"]], k.index[i], pnl, x["k"], "цель", x["p0"], avg, tp, x["mae"], x["L"], x["Q"] * avg)); x = None
            elif (i - x["last"]) >= p.timer_h * 60:
                px_out = c[i] * (1 - slip)
                pnl = (px_out - avg) * x["Q"]; fe = px_out * x["Q"] * p.taker
                cash += pnl - fe; fees += fe; trades += 1
                camps.append((k.index[x["t0"]], k.index[i], pnl, x["k"], "таймер", x["p0"], avg, px_out, x["mae"], x["L"], x["Q"] * avg)); x = None
        if x is not None:
            worst = l[i]
            e_w = cash + (worst - x["cost"] / x["Q"]) * x["Q"]
            if liq_on and e_w <= 0.005 * x["Q"] * worst:
                camps.append((k.index[x["t0"]], k.index[i], -x["c0"], x["k"], "ликвидация", x["p0"], x["cost"] / x["Q"], worst, x["mae"], x["L"], x["cost"]))
                liq = k.index[i]; cash = 0.0; x = None; eq[i:] = 0.0; eqw[i:] = 0.0
                break
            nt[i] = x["Q"] * c[i]; ur[i] = (worst - x["cost"] / x["Q"]) * x["Q"]; eqw[i] = e_w
        else:
            eqw[i] = cash
        eq[i] = cash + ((c[i] - x["cost"] / x["Q"]) * x["Q"] if x is not None else 0.0)
    E = pd.Series(eq, k.index)
    C = pd.DataFrame(camps, columns=["t0", "t1", "pnl", "adds", "exit", "p0", "avg", "px_out", "mae", "lev", "notional"])
    extra = dict(fund=fund_paid, fees=fees)
    if blocks:
        bi = ((k.index - T0) // pd.Timedelta(minutes=BLK)).values.astype(np.int64)
        st = np.r_[0, np.flatnonzero(np.diff(bi)) + 1]
        extra["blk"] = bi[st].astype(np.int32)
        extra["eqd"] = (np.minimum.reduceat(eqw, st) - E0).astype(np.float32)
        extra["ur"] = np.minimum.reduceat(ur, st).astype(np.float32)
        extra["nt"] = np.maximum.reduceat(nt, st).astype(np.float32)
        extra["final"] = float(eq[-1] - E0)
    return E, C, liq, trades, extra


def corr_mask(sym: str, idx: pd.DatetimeIndex, thr: float = CORR_HI) -> np.ndarray:
    """Корреляция дневных доходностей монеты с BTC за 60 дней, известна на начало дня (до вчерашнего закрытия)."""
    if sym == "BTCUSDT":
        return np.ones(len(idx), bool)
    a, b = idx.min() - pd.Timedelta(days=90), idx.max()
    d = market.klines(sym, "D", a, b).astype(float).c
    bt = market.klines("BTCUSDT", "D", a, b).astype(float).c
    R = pd.concat([d, bt], axis=1, keys=["s", "b"]).pct_change(fill_method=None)
    cr = R.s.rolling(60, min_periods=40).corr(R.b).shift(1)
    return (cr.reindex(idx.floor("D")).fillna(0).values >= thr)


def variants():
    V = {}
    for e in ("A", "B"):
        for tn, tk in TGT.items():
            for L in LEVS:
                V[f"{e} | {tn} | x{L:g}"] = dict(coin=e, kw=dict(BASE, **tk, L=L), liq=True, blocks=True, grp="main")
                if L > 1:
                    V[f"{e} | {tn} | x{L:g} | общий счёт"] = dict(coin=e, kw=dict(BASE, **tk, L=L), liq=False, blocks=True, grp="cross")
                V[f"{e} | {tn} | x{L:g} | хуже исполнение"] = dict(coin=e + ("thr" if e == "B" else ""), kw=dict(BASE, **tk, **REAL, L=L),
                                                                  liq=True, blocks=False, grp="real")
            if tn == "цель 1.94/1.6":
                F_ = dict(BASE, **tk, btc_filter=True)
                for L in (1.0, 1.5, 2.0, 3.0):
                    V[f"{e} | {tn} | x{L:g} + фильтр BTC"] = dict(coin=e, kw=dict(F_, L=L), liq=True, blocks=True, grp="filt")
                V[f"{e} | {tn} | x2 только ядро + фильтр BTC"] = dict(coin=e, kw=dict(F_, L=1.0), sel=("ядро", 2.0), liq=True, blocks=True, grp="filt")
                V[f"{e} | {tn} | x2 + фильтр BTC, кроме корр. < 0.4"] = dict(coin=e, kw=dict(F_, L=1.0), sel=("корр. ≥ 0.4", 2.0), liq=True,
                                                                             blocks=True, grp="filt")
                V[f"{e} | {tn} | x2 + фильтр BTC | хуже исполнение"] = dict(coin=e + ("thr" if e == "B" else ""), kw=dict(F_, **REAL, L=2.0),
                                                                           liq=True, blocks=False, grp="filt")
                V[f"{e} | {tn} | x1 + фильтр BTC | хуже исполнение"] = dict(coin=e + ("thr" if e == "B" else ""), kw=dict(F_, **REAL, L=1.0),
                                                                           liq=True, blocks=False, grp="filt")
                V[f"{e} | {tn} | x2 + фильтр BTC, кроме корр. < 0.4 | хуже исполнение"] = dict(
                    coin=e + ("thr" if e == "B" else ""), kw=dict(F_, **REAL, L=1.0), sel=("корр. ≥ 0.4", 2.0), liq=True, blocks=False, grp="filt")
                V[f"{e} | {tn} | x1.5 + фильтр BTC | хуже исполнение"] = dict(coin=e + ("thr" if e == "B" else ""), kw=dict(F_, **REAL, L=1.5),
                                                                             liq=True, blocks=False, grp="filt")
                V[f"{e} | {tn} | x2 только ядро | хуже исполнение"] = dict(coin=e + ("thr" if e == "B" else ""), kw=dict(BASE, **tk, **REAL, L=1.0),
                                                                          sel=("ядро", 2.0), liq=True, blocks=False, grp="filt")
            for L in (2.0, 3.0):
                for sel in ("корр. ≥ 0.6", "ядро"):
                    V[f"{e} | {tn} | x{L:g} только {sel}"] = dict(coin=e, kw=dict(BASE, **tk, L=1.0), sel=(sel, L), liq=True, blocks=True, grp="sel")
    return V


def bt_task(sym_a_b):
    import grid_universe as gu
    sym, a, b = sym_a_b
    G = gu.glob_data()
    A_ = a; B_ = min(b + pd.Timedelta(days=3), gu.END)
    k = gu.load_1m(sym, A_ - pd.Timedelta(days=2), B_)
    if k.empty or len(k) < 3000:
        return dict(sym=sym, skipped=True)
    day_ok = G["U"][sym].reindex(k.index.floor("D")).fillna(0).values.astype(bool)
    sel = (k.index >= A_) & (k.index <= B_)
    scale_full = ge.daily_scale(sym, k.index, G["DOGE_RNG"])
    F = kme.feats(k)
    trig = kme.trigger_price(F, scale_full)
    crash_ok = np.nan_to_num(F.ch1440.values, nan=0.0) > -25.0 * scale_full
    t_s, ok_s, al = trig[sel], crash_ok[sel], day_ok[sel]
    l_s = k.l.values[sel]
    coins = {"A": gu.SegCoin(sym, k, A_, B_, day_ok[sel])}
    for name, thr in (("B", 0.0), ("Bthr", 0.001)):
        hit = l_s <= t_s * (1 - thr)
        sig = np.zeros(sel.sum(), bool)
        sig[:-1] = hit[1:] & al[1:] & ok_s[:-1]
        c_ = gu.SegCoin(sym, k, A_, B_, day_ok[sel])
        kB = c_.k.copy()
        kB["o"] = np.where(np.r_[False, sig[:-1]], np.minimum(kB.o.values, t_s), kB.o.values)
        c_.k = kB
        c_.signal = (lambda p, _s=sig, _c=c_: (_s & ~_c.btc_lo12[_c.sel]) if p.btc_filter else _s)
        coins[name] = c_
    scale = scale_full[sel]
    idx = coins["A"].k.index
    hi = {"корр. ≥ 0.6": corr_mask(sym, idx), "ядро": np.full(len(idx), sym in CORE), "корр. ≥ 0.4": corr_mask(sym, idx, CORR_LO)}
    res = {}
    for vn, v in variants().items():
        Larr = None
        if "sel" in v:
            nm, Lx = v["sel"]
            Larr = np.where(hi[nm], Lx, 1.0)
        E, Cm, liq, tr, ex = run_lev(coins[v["coin"]], ge.P(side=1, scale=scale, **v["kw"]), Larr=Larr, liq_on=v["liq"], blocks=v["blocks"])
        Cm["монета"] = sym.replace("USDT", "")
        d = E.resample("D").last().dropna()
        res[vn] = dict(days=d.diff().fillna(d.iloc[0] - E0), camps=Cm, liq=liq, ex=ex)
    return dict(sym=sym, skipped=False, res=res, hi_share=float(hi["корр. ≥ 0.6"].mean()), core=float(sym in CORE), minutes=int(sel.sum()))


def verify_engine(n_seg=3):
    """run_lev при постоянном плече и с ликвидацией = grid_engine.run (сделки и счёт)."""
    import grid_universe as gu
    segs = sorted(gu.segments(), key=lambda s: (s[2] - s[1]).days, reverse=True)
    picks = [segs[0], segs[len(segs) // 3], segs[len(segs) // 2]][:n_seg]
    picks += [s for s in segs if s[0] == "FARTCOINUSDT" and s[1] <= pd.Timestamp("2025-10-10") <= s[2]][:1]   # с ликвидацией x3
    ok = 0
    for sym, a, b in picks:
        G = gu.glob_data()
        B_ = min(b + pd.Timedelta(days=3), gu.END)
        k = gu.load_1m(sym, a - pd.Timedelta(days=2), B_)
        day_ok = G["U"][sym].reindex(k.index.floor("D")).fillna(0).values.astype(bool)
        sel = (k.index >= a) & (k.index <= B_)
        c = gu.SegCoin(sym, k, a, B_, day_ok[sel])
        scale = ge.daily_scale(sym, c.k.index, G["DOGE_RNG"])
        for L in (1.0, 3.0):
            p = ge.P(side=1, scale=scale, **dict(BASE, L=L))
            E1, C1, l1, t1 = ge.run(c, p)
            E2, C2, l2, t2, _ = run_lev(c, p)
            if l1 is not None:
                print(f"    ликвидация {l1:%d.%m.%y %H:%M} / {l2:%d.%m.%y %H:%M}" if l2 is not None else "    ликвидация только у движка", flush=True)
            same = (np.allclose(E1.values, E2.values, rtol=0, atol=1e-6) and len(C1) == len(C2) and t1 == t2 and l1 == l2
                    and np.allclose(C1.pnl.values, C2.pnl.values, atol=1e-6))
            ok += same
            print(f"  сверка {sym} {a:%d.%m.%y} x{L:g}: {'совпало' if same else 'РАСХОЖДЕНИЕ'} (сделок {t1}/{t2}, кампаний {len(C1)}/{len(C2)})", flush=True)
    return ok, 2 * len(picks)


def section_backtest(master: dict):
    import grid_universe as gu
    say("## 3. Бэктест плеча: ежедневная десятка, 07.2021–27.09.2026, $100k (10 ячеек по $10k)\n")
    ok, n = verify_engine()
    say(f"*Движок с плечом сверен с `grid_engine.run` (постоянное плечо x1 и x3, ликвидация ячейки): совпало {ok} из {n} прогонов "
        "(счёт по минутам, сделки, ликвидации).*\n")
    segs = gu.segments()
    V = variants()
    t0 = time.time()
    nb = int((gu.END + pd.Timedelta(days=4) - T0) / pd.Timedelta(minutes=BLK)) + 2
    acc = {vn: dict(eqd=np.zeros(nb, np.float64), ur=np.zeros(nb, np.float64), nt=np.zeros(nb, np.float64)) for vn, v in V.items() if v["blocks"]}
    days, camps, liqs, fund, fees = ({vn: [] for vn in V} for _ in range(5))
    hi_share = []
    with mp.get_context("spawn").Pool(6) as pool:
        for j, r in enumerate(pool.imap_unordered(bt_task, segs, chunksize=1), 1):
            if j % 40 == 0:
                print(f"  бэктест: {j}/{len(segs)} за {time.time() - t0:.0f} с", flush=True)
            if r["skipped"]:
                continue
            hi_share.append((r["hi_share"], r["minutes"], r["core"]))
            for vn, x in r["res"].items():
                days[vn].append(x["days"]); camps[vn].append(x["camps"]); liqs[vn].append((x["camps"].монета.iloc[0] if len(x["camps"]) else r["sym"], x["liq"]))
                fund[vn].append(x["ex"]["fund"]); fees[vn].append(x["ex"]["fees"])
                if vn in acc:
                    ex = x["ex"]; A_ = acc[vn]
                    A_["eqd"][ex["blk"]] += ex["eqd"]; A_["ur"][ex["blk"]] += ex["ur"]; A_["nt"][ex["blk"]] += ex["nt"]
                    A_["eqd"][ex["blk"][-1] + 1:] += ex["final"]
    print(f"бэктест {time.time() - t0:.0f} с", flush=True)
    hs = np.array(hi_share)
    corr_share = float((hs[:, 0] * hs[:, 1]).sum() / hs[:, 1].sum())
    core_share = float((hs[:, 2] * hs[:, 1]).sum() / hs[:, 1].sum())
    yrs_total = (gu.END - pd.Timestamp("2021-07-01")).days / 365.25
    blk_t = lambda b_: T0 + pd.Timedelta(minutes=BLK * int(b_))  # noqa: E731

    def row(vn):
        P = pd.concat(days[vn], axis=1, sort=True).sum(axis=1).sort_index()
        eq = CAP + P.cumsum()
        Cm = pd.concat([c for c in camps[vn] if len(c)], ignore_index=True); Cm["t0"] = pd.to_datetime(Cm.t0); Cm["t1"] = pd.to_datetime(Cm.t1)
        r = {"вариант": vn, "итог $k": (eq.iloc[-1] - CAP) / 1000,
             "CAGR %": ((eq.iloc[-1] / CAP) ** (1 / yrs_total) - 1) * 100 if eq.iloc[-1] > 0 else -100.0}
        for y in range(2021, 2027):
            e_ = eq[eq.index.year == y]
            st = eq[eq.index < e_.index[0]].iloc[-1] if (eq.index < e_.index[0]).any() else CAP
            r[f"{y} %"] = (e_.iloc[-1] - st) / CAP * 100
        r["просадка %"] = (eq / eq.cummax() - 1).min() * 100
        r["худший день $k"] = P.min() / 1000; r["когда"] = f"{P.idxmin():%d.%m.%y}"
        r["10.10.25 $k"] = P.get(pd.Timestamp("2025-10-10"), 0.0) / 1000
        lq = [(s, t) for s, t in liqs[vn] if t is not None]
        r["ликвид."] = len(lq)
        if vn in acc:
            A_ = acc[vn]
            need = -A_["ur"] + 0.01 * A_["nt"]
            r["худший откр. убыток $k"] = A_["ur"].min() / 1000; r["когда убыток"] = f"{blk_t(A_['ur'].argmin()):%d.%m.%y}"
            r["нужно на счёте $k"] = need.max() / 1000
            r["макс. номинал $k"] = A_["nt"].max() / 1000
            acct = CAP + A_["eqd"]
            m = A_["nt"] > 0
            r["эфф. плечо макс"] = float((A_["nt"][m] / np.maximum(acct[m], 1)).max()) if m.any() else 0.0
            r["счёт в худшую минуту $k"] = (acct - MMR * A_["nt"]).min() / 1000
            r["запас без прибыли $k"] = (CAP - need.max()) / 1000
        r["фандинг $k"] = -sum(fund[vn]) / 1000; r["комиссии $k"] = sum(fees[vn]) / 1000
        r["сделок"] = len(Cm); r["в плюс %"] = (Cm.pnl > 0).mean() * 100
        r["_liq"] = ", ".join(f"{s} {t:%d.%m.%y}" for s, t in sorted(lq, key=lambda z: z[1]))
        r["_eq"] = eq; r["_P"] = P; r["_C"] = Cm
        return r

    R = {vn: row(vn) for vn in V}
    pub = lambda rows, cols: pd.DataFrame([{k: v for k, v in r.items() if k in cols} for r in rows])  # noqa: E731
    main_cols = ["вариант", "итог $k", "CAGR %", "2021 %", "2022 %", "2023 %", "2024 %", "2025 %", "2026 %", "просадка %", "худший день $k",
                 "когда", "10.10.25 $k", "ликвид.", "худший откр. убыток $k", "нужно на счёте $k", "макс. номинал $k", "фандинг $k", "комиссии $k",
                 "сделок", "в плюс %"]
    say("### 3а. Плечо стратегии x1…x3, ячейка сама за себя (как в прежних расчётах)\n")
    say("L — во сколько раз лесенка больше ячейки: x1 = набранная лесенка $10k на ячейку $10k; x3 = $30k. Кусок от стартовой ячейки, "
        "после убытков — не больше её текущих денег (cap_to_equity); прибыль кусок не увеличивает. Ликвидация ячейки — когда её деньги "
        "+ открытый убыток ≤ 0.5% позиции (ячейка теряет всё, монета дальше не торгуется до нового отрезка в десятке). "
        "«Годы» — % от $100k; CAGR — на весь срок 5.2 года (без реинвеста). «нужно на счёте» — худший суммарный открытый убыток "
        "+ 1% номинала (поддерживающая маржа): столько свободных денег должно лежать на торговом счёте, чтобы ни одну позицию "
        "не ликвидировало. Фандинг и комиссии уже внутри итога.\n")
    main = [R[vn] for vn in V if V[vn]["grp"] == "main"]
    say(md_table(pub(main, main_cols), 1)); say()
    for r in main:
        if r["_liq"]:
            say(f"- {r['вариант'].replace(' | ', ' · ')}: ликвидации — {r['_liq']}")
    say()

    say("### 3б. Тот же расчёт, но общий счёт (cross, как у нашего демо): ячейка может уйти в минус\n")
    say("Bybit в режиме REGULAR_MARGIN ликвидирует только когда ВЕСЬ счёт ниже поддерживающей маржи. Здесь ячейки не "
        "ликвидируются; «счёт в худшую минуту» = $100k + итог до этой минуты + открытый убыток по минимуму минуты − 0.5% номинала. "
        "Если он > 0 — ликвидации счёта не было, но убыток одной монеты может съесть деньги других ячеек. Важно: к худшей минуте "
        "на счёте уже лежит прибыль прошлых лет. «запас без прибыли» = $100k − «нужно на счёте»: если отрицательный, то счёт, "
        "открытый с $100k прямо перед худшим днём, ликвидировало бы целиком.\n")
    cross_cols = ["вариант", "итог $k", "CAGR %", "2022 %", "2025 %", "2026 %", "просадка %", "худший день $k", "когда", "10.10.25 $k",
                  "худший откр. убыток $k", "когда убыток", "нужно на счёте $k", "запас без прибыли $k", "счёт в худшую минуту $k", "эфф. плечо макс",
                  "макс. номинал $k"]
    cr = []
    for e in ("A", "B"):
        for tn in TGT:
            cr.append(R[f"{e} | {tn} | x1"])
            for L in LEVS[1:]:
                cr.append(R[f"{e} | {tn} | x{L:g} | общий счёт"])
    say(md_table(pub(cr, cross_cols), 1)); say()

    say("### 3в. Худшее исполнение: лимитки (и вход B) только при проходе цены на 0.1%, комиссии ×2\n")
    real_cols = ["вариант", "итог $k", "CAGR %", "2024 %", "2025 %", "2026 %", "просадка %", "худший день $k", "когда", "10.10.25 $k", "ликвид.", "фандинг $k",
                 "комиссии $k", "сделок"]
    say(md_table(pub([R[vn] for vn in V if V[vn]["grp"] == "real"], real_cols), 1)); say()

    say(f"### 3г. Плечо только на монетах с сильной связью с биткоином или на «ядре», остальные x1\n")
    say(f"«корр. ≥ {CORR_HI}» — корреляция дневных доходностей с BTC за 60 дней (известна на начало дня); таких монето-дней "
        f"в десятке {corr_share * 100:.0f}%. «ядро» — BTC, ETH, SOL, XRP, DOGE (были крупными уже в 2021), {core_share * 100:.0f}% монето-минут "
        "десятки. Сравнивать с x1 и «везде» из 3а.\n")
    sel_rows = []
    for e in ("A", "B"):
        for tn in TGT:
            sel_rows.append(R[f"{e} | {tn} | x1"])
            for L in (2.0, 3.0):
                sel_rows += [R[f"{e} | {tn} | x{L:g}"], R[f"{e} | {tn} | x{L:g} только корр. ≥ 0.6"], R[f"{e} | {tn} | x{L:g} только ядро"]]
    sel_cols = ["вариант", "итог $k", "CAGR %", "2022 %", "2025 %", "2026 %", "просадка %", "худший день $k", "когда", "10.10.25 $k", "ликвид.",
                "худший откр. убыток $k", "нужно на счёте $k"]
    say(md_table(pub(sel_rows, sel_cols), 1)); say()
    for r in sel_rows:
        if r["_liq"] and "только" in r["вариант"]:
            say(f"- {r['вариант'].replace(' | ', ' · ')}: ликвидации — {r['_liq']}")
    say()

    say("### 3д. А если поставить на бирже isolated xN, как мастер (кусок как у нас, x1 по счёту)\n")
    say("Isolated: залог позиции = номинал / N, ликвидация при цене ≈ средняя × (1 − 1/N + 0.5%). Считаем по нашим кампаниям x1 "
        "(без ликвидаций), сколько из них хоть раз уходили ниже этой цены (худшая точка — минимум минуты против средней после "
        "докупок). Убыток при ликвидации ≈ залог (номинал / N) вместо фактического итога кампании; «стоило бы» — разница по всем "
        "таким кампаниям.\n")
    rows = []
    for e in ("A", "B"):
        for tn in TGT:
            Cm = R[f"{e} | {tn} | x1"]["_C"]
            for N in (10, 5, 3, 2):
                thr_ = -(1 / N - MMR)
                hit = Cm[Cm.mae <= thr_]
                cost = (-(hit.notional / N) - hit.pnl).sum()
                by_y = hit.groupby(pd.to_datetime(hit.t0).dt.year).size()
                rows.append({"вариант": f"{e} | {tn}", "isolated": f"x{N}", "ликвидация при %": thr_ * 100, "кампаний ниже": len(hit),
                             "из всех": len(Cm), "стоило бы $k": cost / 1000, "по годам (год:число)": " ".join(f"{y % 100}:{v}" for y, v in by_y.items())})
    say(md_table(pd.DataFrame(rows), 1)); say()
    allC = R["A | цель 1.94/1.6 | x1"]["_C"]
    deep = allC.sort_values("mae").head(8)
    say("Самые глубокие кампании A x1 (худшая точка от средней): " + ", ".join(f"{r_.монета} {r_.t0:%d.%m.%y} {r_.mae * 100:.0f}%" for r_ in deep.itertuples()) + ".\n")
    say(f"Худшая точка кампаний A x1 от средней: медиана {allC.mae.median() * 100:.1f}%, 1% худших ≤ {allC.mae.quantile(0.01) * 100:.1f}%, "
        f"минимум {allC.mae.min() * 100:.1f}% ({allC.loc[allC.mae.idxmin(), 'монета']} {pd.Timestamp(allC.loc[allC.mae.idxmin(), 't0']):%d.%m.%y}).\n")
    section_protect(R, V, main_cols, pub)
    return R


CRASH_T = pd.Timestamp("2025-10-10 21:20")


def open_at(C: pd.DataFrame, t: pd.Timestamp) -> pd.DataFrame:
    return C[(C.t0 <= t) & (C.t1 >= t)]


def limit_camps(C: pd.DataFrame, N: int) -> pd.DataFrame:
    """Грубая оценка лимита N одновременных кампаний по готовым кампаниям: берём по времени входа, пропускаем вход, если
    уже открыто N. (Не учитывает, что монета после пропуска могла войти позже — это пропущенный вход.)"""
    keep, ends = [], []
    for r in C.sort_values(["t0", "монета"], kind="stable").itertuples():
        ends = [e for e in ends if e > r.t0]
        if len(ends) < N:
            keep.append(r.Index); ends.append(r.t1)
    return C.loc[keep]


def section_protect(R: dict, V: dict, main_cols, pub):
    say("### 3е. Защиты при плече: фильтр по биткоину, лимит одновременных кампаний, пропуск −25%/сутки\n")
    say("Пропуск монеты после −25% за сутки уже включён во все варианты (рабочее правило). Фильтр BTC — не входить, пока биткоин "
        "ниже своего минимума за 12 ч (ячейка сама за себя). «кроме корр. < 0.4» — плечо x2 везде, кроме монет со слабой связью "
        "с биткоином (там x1). «хуже исполнение» — проход 0.1% + комиссии ×2:\n")
    rows = []
    for e in ("A", "B"):
        t = f"{e} | цель 1.94/1.6"
        rows += [R[f"{t} | x1"], R[f"{t} | x1 + фильтр BTC"], R[f"{t} | x1.5 + фильтр BTC"], R[f"{t} | x2"], R[f"{t} | x2 + фильтр BTC"],
                 R[f"{t} | x2 только ядро"], R[f"{t} | x2 только ядро + фильтр BTC"], R[f"{t} | x2 + фильтр BTC, кроме корр. < 0.4"],
                 R[f"{t} | x3"], R[f"{t} | x3 + фильтр BTC"],
                 R[f"{t} | x1 | хуже исполнение"], R[f"{t} | x1 + фильтр BTC | хуже исполнение"], R[f"{t} | x1.5 + фильтр BTC | хуже исполнение"],
                 R[f"{t} | x2 + фильтр BTC | хуже исполнение"], R[f"{t} | x2 + фильтр BTC, кроме корр. < 0.4 | хуже исполнение"],
                 R[f"{t} | x2 только ядро | хуже исполнение"]]
    cols = ["вариант", "итог $k", "CAGR %", "2022 %", "2024 %", "2025 %", "2026 %", "просадка %", "худший день $k", "когда", "10.10.25 $k", "ликвид.",
            "худший откр. убыток $k", "нужно на счёте $k"]
    say(md_table(pub(rows, cols), 1)); say()
    for r in rows:
        if r["_liq"] and ("фильтр" in r["вариант"] or "ядро" in r["вариант"]):
            say(f"- {r['вариант'].replace(' | ', ' · ')}: ликвидации — {r['_liq']}")
    say()

    C1 = R["A | цель 1.94/1.6 | x1"]["_C"]
    ev = pd.concat([pd.Series(1, C1.t0), pd.Series(-1, C1.t1)]).sort_index(kind="stable").cumsum()
    o = open_at(C1, CRASH_T)
    say(f"Одновременно открытых кампаний (A x1, 10 ячеек): максимум {int(ev.max())} ({ev.idxmax():%d.%m.%y %H:%M}); "
        f"10.10.25 в 21:20 открыто {len(o)}: " + ", ".join(f"{r.монета} ({r.adds} докуп., итог {r.pnl:+,.0f}$)" for r in o.itertuples()) + ".\n")
    say("Лимит одновременных кампаний — грубая оценка по готовым кампаниям (новый вход пропускается, если уже открыто N; "
        "в «общем счёте» без ликвидаций ячейки, чтобы сравнение было по одним и тем же кампаниям). Итог — по кампаниям, без "
        "комиссий и фандинга, поэтому чуть выше, чем в таблицах выше:\n")
    rows = []
    for L, vn in ((1.0, "A | цель 1.94/1.6 | x1"), (2.0, "A | цель 1.94/1.6 | x2 | общий счёт"), (3.0, "A | цель 1.94/1.6 | x3 | общий счёт"),
                  (2.0, "B | цель 3.9/3.0 | x2 | общий счёт")):
        C = R[vn]["_C"]
        for N in (10, 5, 3):
            K = limit_camps(C, N)
            oc = open_at(K, CRASH_T)
            yr = K.groupby(K.t1.dt.year).pnl.sum() / 1000
            rows.append({"вариант": vn, "лимит": N if N < 10 else "нет (10 ячеек)", "итог $k": K.pnl.sum() / 1000, "пропущено": len(C) - len(K),
                         "2025 $k": yr.get(2025, 0.0), "2026 $k": yr.get(2026, 0.0), "открыто 10.10 21:20": len(oc),
                         "итог этих кампаний $k": oc.pnl.sum() / 1000, "их худшая точка $k": (oc.mae * oc.notional).sum() / 1000})
    say(md_table(pd.DataFrame(rows), 1)); say()

    say("### 3ж. Дни-обвалы: результат дня по вариантам, $k\n")
    base3 = R["A | цель 1.94/1.6 | x3"]["_P"]
    days = sorted(set(base3.nsmallest(6).index) | {pd.Timestamp("2025-10-10")})
    show = ["A | цель 1.94/1.6 | x1", "A | цель 1.94/1.6 | x1.5", "A | цель 1.94/1.6 | x2", "A | цель 1.94/1.6 | x3",
            "A | цель 1.94/1.6 | x2 | общий счёт", "A | цель 1.94/1.6 | x3 | общий счёт", "A | цель 1.94/1.6 | x2 только ядро",
            "A | цель 1.94/1.6 | x2 + фильтр BTC", "A | цель 1.94/1.6 | x2 + фильтр BTC, кроме корр. < 0.4", "B | цель 3.9/3.0 | x1",
            "B | цель 3.9/3.0 | x2"]
    T = pd.DataFrame({vn: [R[vn]["_P"].get(d, 0.0) / 1000 for d in days] for vn in show}, index=[f"{d:%d.%m.%y}" for d in days]).T
    T.insert(0, "вариант", T.index)
    say(md_table(T.reset_index(drop=True), 1)); say()


if __name__ == "__main__":
    pd.set_option("display.width", 250)
    t_start = time.time()
    if "--fetch" in sys.argv:
        print("снимок мастера:", fetch_leader())
    say("# Плечо: у мастера ITEKCrypto Kamaz и у нас (02.10.2026)\n")
    say("*Скрипт `experiments/kamaz_leverage.py`. Только исследование: живой бот, сервер и заявки не трогали; ручки Bybit — "
        "публичные, на чтение. Время — UTC.*\n")
    say("@@SUMMARY@@\n")
    M = section_master()
    section_bot()
    if "--skip-bt" not in sys.argv:
        section_backtest(M)
    OUT.write_text("\n".join(LINES).replace("@@SUMMARY@@", SUMMARY.strip()) + "\n")
    print(f"готово за {time.time() - t_start:.0f} с → {OUT}")
