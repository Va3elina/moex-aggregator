"""Копитрейдинг Bybit — полуавтомат через браузер.

Почему не curl: Akamai режет любые запросы не из браузера (403 даже с имитацией
TLS Chrome через curl_cffi, проверено 26.09.2026). Страница запрещает fetch на
localhost (CSP), а window.open в панели браузера уводит ту же вкладку — данные теряются.

Поэтому: BUNDLE_JS выполняется во вкладке bybit.com (инструмент javascript_tool), выгружает
несколько трейдеров и отправляет их ОБЫЧНОЙ ФОРМОЙ на локальный приёмник factory/receiver.py
(форма — это навигация, CSP connect-src её не режет) → inbox/<name>.json →
`run.py add bybit-bundle inbox/<name>.json`. Один трейдер вручную: export_js + add bybit-file.

Что важно знать о данных Bybit:
  * история сделок — ТОЛЬКО последние 90 дней;
  * entryPrice — цена ордера, positionEntryPrice — средняя позиции (ордера одной
    позиции закрываются одним временем → pos_id = символ|сторона|время закрытия);
  * ROI в карточке = ход цены × плечо на марже ордера (косметика);
  * isBotLeader — Bybit сам помечает ботов; leaderUserIntroduction — описание;
  * cumResetRoi (dynamic-yield-trend) — накопленный ROI в сотых долях процента.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from ..schema import finalize

EXPORT_FN = r"""
async function exportOne(MARK) {
  const S = ms => new Promise(r => setTimeout(r, ms));
  const api = p => fetch('/x-api/fapi/beehive/' + p).then(r => r.json());
  const m = encodeURIComponent(MARK);
  let rows = [], cursor = '', action = 'first_page', hidden = 0;
  for (let k = 0; k < 80; k++) {
    const r = (await api(`public/v1/common/leader-history?leaderMark=${m}&pageSize=50&pageAction=${action}` + (cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''))).result || {};
    hidden = hidden || r.openTradeInfoProtection || 0;
    rows = rows.concat(r.data || []);
    if (!r.hasNext || !r.cursor) break;
    cursor = r.cursor; action = 'next'; await S(250);
  }
  const info = (await api(`private/v1/pub-leader/info?leaderMark=${m}`)).result || {};
  const tag = (await api(`public/v1/leader/identify-tag-info?leaderMark=${m}`)).result || {};
  const tr = (await api(`public/v2/leader/dynamic-yield-trend?dayCycleType=DAY_CYCLE_TYPE_NINETY_DAY&period=PERIOD_DAY&leaderMark=${m}`)).result || {};
  const roi = ((tr.metricList || []).find(x => x.line === 'cumResetRoi') || {}).metricLineValue || [];
  const meta = {leaderMark: MARK, name: info.leaderUserName, intro: info.leaderUserIntroduction, isBot: info.isBotLeader,
    tradeDays: info.tradeDays, profitCount: info.profitCount, lossCount: info.lossCount, hidden,
    apps: (tag.thirdPartyApplicationInfo || []).map(x => x.name), roi: roi.map(x => [+x.statisticDate, +x.value])};
  return '#meta ' + JSON.stringify(meta) + '\n' + 'sym,side,order_price,p_open,p_close,size,t_open_ms,t_close_ms,lev,iso\n' +
    rows.map(x => [x.symbol, x.side === 'Buy' ? 1 : -1, +x.entryPrice, +x.positionEntryPrice, +x.closedPrice, +x.size,
                   +x.startedTimeE3, +x.closedTimeE3, +x.leverageE2 / 100, x.isIsolated ? 1 : 0].join(',')).join('\n');
}
"""

# Пакет: выгружает несколько трейдеров и отправляет ОДНОЙ формой на локальный приёмник
# (factory/receiver.py, 127.0.0.1:8765). Вкладка после отправки уходит на страницу приёмника — это нормально.
BUNDLE_JS = EXPORT_FN + r"""
const LIST = __LIST__;   // [[slug, leaderMark], ...]
const out = {};
for (const [slug, mark] of LIST) { try { out[slug] = await exportOne(mark); } catch (e) { out[slug] = '#error ' + e; } }
const f = document.createElement('form'); f.method = 'POST'; f.action = 'http://127.0.0.1:8765/'; f.acceptCharset = 'utf-8';
for (const [k, v] of [['name', '__NAME__'], ['data', JSON.stringify(out)]]) { const t = document.createElement('textarea'); t.name = k; t.value = v; f.appendChild(t); }
document.body.appendChild(f); setTimeout(() => f.submit(), 50);
'отправлено: ' + Object.keys(out).length;
"""


def bundle_js(pairs: list[tuple[str, str]], name: str) -> str:
    return BUNDLE_JS.replace("__LIST__", json.dumps(pairs)).replace("__NAME__", name)


def export_js(leader_mark: str) -> str:
    return EXPORT_FN + f"\nawait exportOne({json.dumps(leader_mark)});"


def split_bundle(path: str | Path) -> list[Path]:
    """inbox/<bundle>.json → inbox/<slug>.bybit.txt для каждого трейдера."""
    d = json.loads(Path(path).read_text())
    out = []
    for slug, text in d.items():
        if text.startswith("#error"):
            print(f"  {slug}: {text}")
            continue
        p = Path(path).parent / f"{slug}.bybit.txt"
        p.write_text(text)
        out.append(p)
    return out


def from_export(path: str | Path, name: str | None = None) -> tuple[pd.DataFrame, dict, pd.Series]:
    text = Path(path).read_text()
    first, rest = text.split("\n", 1)
    meta_raw = json.loads(first.removeprefix("#meta ").strip())
    from io import StringIO
    R = pd.read_csv(StringIO(rest))
    t_close = pd.to_datetime(R.t_close_ms, unit="ms")
    df = pd.DataFrame(dict(
        sym=R.sym, side=R.side, t_open=pd.to_datetime(R.t_open_ms, unit="ms"), t_close=t_close,
        p_open=R.p_open, order_price=R.order_price, p_close=R.p_close, size=R["size"], cost=R["size"] * R.order_price,
        lev=R.lev, pnl_pct=np.nan,
        pos_id=[f"{a}|{b}|{c}" for a, b, c in zip(R.sym, R.side, R.t_close_ms)]))
    roi = meta_raw.get("roi") or []
    eq = pd.Series(dtype=float)
    if roi:
        s = pd.Series({pd.to_datetime(t, unit="ms"): v for t, v in roi}).sort_index()
        eq = 1 + s / 10000.0          # cumResetRoi в сотых долях процента
    meta = dict(name=name or meta_raw.get("name"), source="bybit", source_id=meta_raw.get("leaderMark"),
                url="https://www.bybit.com/copyTrade/trade-center/detail?leaderMark=" + str(meta_raw.get("leaderMark")),
                description=meta_raw.get("intro") or "", bybit_is_bot=meta_raw.get("isBot"),
                apps=meta_raw.get("apps"), trade_days=meta_raw.get("tradeDays"),
                history_hidden=bool(meta_raw.get("hidden")),
                time_note="UTC (метки времени в миллисекундах)",
                price_note="p_open = средняя позиции, order_price = цена ордера",
                history_note="только последние 90 дней — выводы о правилах ограничены",
                equity_note="кривая = cumResetRoi за 90 дней (ROI витрины, с плечом)")
    return finalize(df), meta, eq
