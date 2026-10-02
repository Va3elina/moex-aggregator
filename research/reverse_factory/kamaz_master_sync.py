"""Ежедневная выгрузка сделок мастера ITEKCrypto Kamaz с Bybit (только публичные ручки, без входа и заявок).

Как достаём: curl/curl_cffi Bybit (Akamai) режет 403, headless Chromium с родным UA «HeadlessChrome» — тоже.
Работает: Playwright + установленный Google Chrome в headless, UA обычного Chrome, без флага автоматизации;
открываем bybit.com/copyTrade и делаем fetch изнутри страницы (как вкладка в браузере).

Пишем в inbox/private/kamaz_master/ (не в git):
  closed.jsonl          — накопитель закрытых сделок, ключ = монета|время открытия|время закрытия (мс);
                          src=api (ручка leader-history) или src=top_export (выгрузка 26.09, 119 сделок);
                          при совпадении ключа запись api вытесняет top_export (у неё больше полей)
  open_YYYY-MM-DD.json  — снимки открытых позиций за день (каждый прогон дописывает снимок)
  raw/<UTC>.json        — сырые ответы Bybit за прогон
  sync.log, state.json  — журнал прогонов и время последнего успеха

Запуск:  .venv/bin/python kamaz_master_sync.py            (выгрузка + сверка с ботом)
         .venv/bin/python kamaz_master_sync.py --no-match
Расписание: launchd ~/Library/LaunchAgents/ru.framedata.kamaz-master-sync.plist.
Сбой: строка в sync.log, уведомление macOS, код 1; если успеха не было > 18 ч — сообщение в админ-чат
через @framesignalbot (ssh на прод, релей Telegram, токен остаётся на сервере).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
MARK = "k1cwzhKkWm/8mD/Pdp6nWw=="
NAME = "ITEKCrypto Kamaz"
OUT = HERE / "inbox" / "private" / "kamaz_master"
RAW = OUT / "raw"
CLOSED = OUT / "closed.jsonl"
STATE = OUT / "state.json"
LOG = OUT / "sync.log"
LEGACY = HERE / "inbox" / "top-itekcrypto-kamaz.bybit.txt"
PAGE = "https://www.bybit.com/en/copyTrade/trade-center/detail?leaderMark=" + "k1cwzhKkWm%2F8mD%2FPdp6nWw%3D%3D"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
SSH = ["ssh", "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "ConnectTimeout=30",
       "-i", str(Path.home() / ".ssh" / "id_ed25519"), "root@103.88.243.232"]
ALERT_AFTER_H = 18

FETCH_JS = r"""
async (MARK) => {
  const S = ms => new Promise(r => setTimeout(r, ms));
  const get = async p => {
    const r = await fetch('/x-api/fapi/beehive/' + p);
    const t = await r.text();
    if (r.status !== 200) throw new Error('HTTP ' + r.status + ' ' + p.split('?')[0] + ': ' + t.slice(0, 120));
    return JSON.parse(t);
  };
  const m = encodeURIComponent(MARK);
  const hist = [], open = [];
  let cursor = '', action = 'first_page';
  for (let k = 0; k < 40; k++) {
    const j = await get(`public/v1/common/leader-history?leaderMark=${m}&pageSize=50&pageAction=${action}` +
                        (cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''));
    hist.push(j);
    const r = j.result || {};
    if (!r.hasNext || !r.cursor) break;
    cursor = r.cursor; action = 'next'; await S(300);
  }
  for (let page = 1; page <= 10; page++) {
    const j = await get(`public/v1/common/order/list-detail?leaderMark=${m}&pageSize=50&page=${page}`);
    open.push(j);
    const r = j.result || {};
    if ((r.data || []).length < 50) break;
    await S(300);
  }
  return {hist, open};
}
"""


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def log(msg: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    line = f"{now_utc():%Y-%m-%d %H:%M:%S}Z {msg}"
    print(line)
    with LOG.open("a") as f:
        f.write(line + "\n")


def iso(ms) -> str:
    return dt.datetime.fromtimestamp(int(ms) / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fetch() -> dict:
    from playwright.sync_api import sync_playwright
    errors = []
    with sync_playwright() as p:
        import glob
        local = sorted(glob.glob(str(Path.home() / "Library/Caches/ms-playwright/chromium-*/chrome-mac-arm64/*.app/Contents/MacOS/*")))
        variants = [dict(channel="chrome"), dict(channel="chrome")] + ([dict(executable_path=local[-1])] if local else [])
        for attempt, kw in enumerate(variants, 1):
            b = None
            try:
                b = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"], **kw)
                ctx = b.new_context(user_agent=UA, locale="en-US", viewport={"width": 1280, "height": 900})
                pg = ctx.new_page()
                pg.goto(PAGE, wait_until="domcontentloaded", timeout=60000)
                data = pg.evaluate(FETCH_JS, MARK)
                for j in data["hist"] + data["open"]:
                    if j.get("retCode") != 0:
                        raise RuntimeError(f"retCode={j.get('retCode')} {j.get('retMsg')}")
                return data
            except Exception as e:  # noqa: BLE001
                errors.append(f"попытка {attempt} ({kw or 'chromium'}): {str(e).splitlines()[0][:200]}")
                time.sleep(10 * attempt)
            finally:
                if b:
                    b.close()
    raise RuntimeError("; ".join(errors))


def norm_api(d: dict, fetched_at: str) -> dict:
    side = 1 if d.get("side") == "Buy" else -1
    entry, close = float(d["entryPrice"]), float(d["closedPrice"])
    return dict(
        key=f"{d['symbol']}|{int(d['startedTimeE3'])}|{int(d['closedTimeE3'])}",
        src="api", fetched_at=fetched_at, order_id=d.get("orderId"),
        sym=d["symbol"], side=side, t_open=iso(d["startedTimeE3"]), t_close=iso(d["closedTimeE3"]),
        t_open_ms=int(d["startedTimeE3"]), t_close_ms=int(d["closedTimeE3"]),
        order_price=entry, p_open=float(d.get("positionEntryPrice") or entry), p_close=close,
        size=float(d["size"]), lev=int(d.get("leverageE2") or 0) / 100, iso=bool(d.get("isIsolated")),
        price_pct=round(side * (close / entry - 1) * 100, 3),
        roi_pct=int(d.get("orderNetProfitRateE4") or 0) / 100,
        pnl_usd=int(d.get("orderNetProfitE8") or 0) / 1e8,
        closed_type=d.get("closedType"), full_closed=d.get("fullClosed"),
        multi_close=d.get("hasMultiCloseOrder"), followers=int(d.get("followerNum") or 0),
        raw=d,
    )


def legacy_rows() -> list[dict]:
    if not LEGACY.exists():
        return []
    lines = LEGACY.read_text().splitlines()
    hdr = lines[1].split(",")
    out = []
    for ln in lines[2:]:
        if not ln.strip():
            continue
        r = dict(zip(hdr, ln.split(",")))
        side = int(r["side"])
        op, pc = float(r["order_price"]), float(r["p_close"])
        out.append(dict(
            key=f"{r['sym']}|{int(r['t_open_ms'])}|{int(r['t_close_ms'])}",
            src="top_export", fetched_at="2026-09-26", order_id=None,
            sym=r["sym"], side=side, t_open=iso(r["t_open_ms"]), t_close=iso(r["t_close_ms"]),
            t_open_ms=int(r["t_open_ms"]), t_close_ms=int(r["t_close_ms"]),
            order_price=op, p_open=float(r["p_open"]), p_close=pc, size=float(r["size"]),
            lev=float(r["lev"]), iso=r["iso"] == "1", price_pct=round(side * (pc / op - 1) * 100, 3)))
    return out


def load_closed() -> dict[str, dict]:
    rows = {}
    if CLOSED.exists():
        for ln in CLOSED.read_text().splitlines():
            if ln.strip():
                r = json.loads(ln)
                rows[r["key"]] = r
    return rows


def merge(rows: dict, new: list[dict]) -> int:
    added = 0
    for r in new:
        old = rows.get(r["key"])
        if old is None:
            rows[r["key"]] = r
            added += 1
        elif old["src"] == "top_export" and r["src"] == "api":
            rows[r["key"]] = r          # уточнение: api богаче
    return added


def save_closed(rows: dict) -> None:
    tmp = CLOSED.with_suffix(".tmp")
    with tmp.open("w") as f:
        for r in sorted(rows.values(), key=lambda r: (r["t_close_ms"], r["key"])):
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(CLOSED)


def state_get() -> dict:
    try:
        return json.loads(STATE.read_text())
    except Exception:  # noqa: BLE001
        return {}


def state_put(**kw) -> None:
    s = state_get()
    s.update(kw)
    STATE.write_text(json.dumps(s, ensure_ascii=False, indent=1))


def notify_mac(text: str) -> None:
    try:
        subprocess.run(["osascript", "-e", f'display notification "{text[:180]}" with title "Kamaz: выгрузка мастера"'],
                       timeout=10, check=False)
    except Exception:  # noqa: BLE001
        pass


ALERT_PY = r"""
import json, os, sys, urllib.parse, urllib.request
env = {}
for ln in open('/opt/frame/.env', encoding='utf-8'):
    if '=' in ln and not ln.lstrip().startswith('#'):
        k, v = ln.rstrip('\n').split('=', 1); env[k.strip()] = v.strip().strip('"').strip("'")
root = env.get('TELEGRAM_API_ROOT') or 'https://api.telegram.org'
import base64
text = base64.b64decode(sys.argv[1]).decode()
data = urllib.parse.urlencode({'chat_id': env['ADMIN_CHAT_ID'], 'text': text[:3500]}).encode()
req = urllib.request.Request(f"{root}/bot{env['ALERT_BOT_TOKEN']}/sendMessage", data=data,
                             headers={'User-Agent': 'frame-robots/1.0'})
print(json.load(urllib.request.urlopen(req, timeout=20)).get('ok'))
"""


def alert_telegram(text: str) -> None:
    try:
        import base64
        b64 = base64.b64encode(text.encode()).decode()
        r = subprocess.run(SSH + [f"python3 - {b64}"], input=ALERT_PY, text=True,
                           capture_output=True, timeout=60)
        log(f"алерт в Telegram: {r.stdout.strip() or r.stderr.strip()[:200]}")
    except Exception as e:  # noqa: BLE001
        log(f"алерт в Telegram не ушёл: {e}")


def run(do_match: bool) -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(exist_ok=True)
    stamp = now_utc()
    data = fetch()
    (RAW / f"{stamp:%Y-%m-%dT%H%M%SZ}.json").write_text(json.dumps(data, ensure_ascii=False))

    fetched_at = f"{stamp:%Y-%m-%d %H:%M:%S}"
    api = [norm_api(d, fetched_at) for j in data["hist"] for d in (j.get("result") or {}).get("data") or []]
    opened = [d for j in data["open"] for d in (j.get("result") or {}).get("data") or []]

    rows = load_closed()
    prev_last = max((r["t_close_ms"] for r in rows.values()), default=0)
    n_leg = merge(rows, legacy_rows())
    n_api = merge(rows, api)
    save_closed(rows)

    # дыра: самая старая сделка в ответе новее последней сохранённой → что-то между ними потеряно
    if api and prev_last and min(r["t_close_ms"] for r in api) > prev_last:
        log(f"ВНИМАНИЕ: возможна дыра в истории {iso(prev_last)} … {iso(min(r['t_close_ms'] for r in api))}")

    snap_f = OUT / f"open_{stamp:%Y-%m-%d}.json"
    snap = json.loads(snap_f.read_text()) if snap_f.exists() else {"leaderMark": MARK, "name": NAME, "snapshots": []}
    snap["snapshots"].append({"at": fetched_at, "positions": [dict(
        sym=d["symbol"], side=d["side"], t_open=iso(d["createdAtE3"]), order_price=float(d["entryPrice"]),
        p_open=float(d.get("positionEntryPrice") or d["entryPrice"]), size=int(d["sizeX"]) / 1e8,
        size_left=int(d.get("closeFreeQtyX") or 0) / 1e8, lev=int(d.get("leverageE2") or 0) / 100,
        raw=d) for d in opened]})
    snap_f.write_text(json.dumps(snap, ensure_ascii=False, indent=1))

    state_put(last_ok=fetched_at, last_api_rows=len(api), last_open=len(opened))
    log(f"ok: ответ {len(api)} закрытых, новых {n_api}, из старой выгрузки {n_leg}, всего {len(rows)}; "
        f"открыто {len(opened)}: " + ", ".join(f"{d['symbol']} {d['side']} {d['entryPrice']}" for d in opened))

    if do_match:
        try:
            import kamaz_master_match
            kamaz_master_match.main([])
        except Exception as e:  # noqa: BLE001
            log(f"сверка с ботом не удалась (выгрузка сохранена): {e!r}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-match", action="store_true", help="без сверки с журналами бота")
    a = ap.parse_args()
    sys.path.insert(0, str(HERE))
    try:
        return run(not a.no_match)
    except Exception as e:  # noqa: BLE001
        log(f"СБОЙ: {e!r}\n{traceback.format_exc()}")
        notify_mac(f"сбой: {str(e)[:120]}")
        last = state_get().get("last_ok")
        stale = (not last) or (now_utc() - dt.datetime.fromisoformat(last).replace(tzinfo=dt.timezone.utc)
                               > dt.timedelta(hours=ALERT_AFTER_H))
        if stale:
            alert_telegram(f"⚠️ Kamaz: выгрузка сделок мастера с Bybit не работает "
                           f"(последний успех: {last or 'никогда'} UTC).\n{str(e)[:500]}\n"
                           f"Лог: research/reverse_factory/inbox/private/kamaz_master/sync.log")
        return 1


if __name__ == "__main__":
    sys.exit(main())
