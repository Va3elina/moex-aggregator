"""Сверка сделок мастера Kamaz (inbox/private/kamaz_master/closed.jsonl) с нашим ботом kamaz-bot.

Журналы бота забираются одним rsync с прода (/opt/kamaz-bot/bot/state/journal/*.jsonl → kamaz_master/our_journal/).
Кампании бота собираются отдельно для бумаги (вход / докупка N / цель|таймер) и демо-счёта
(«реал»: исполнения от первого до «кампания закрыта»). Время журнала — UTC, как и у Bybit.

Сделка мастера = match, если у нас по той же монете есть покупка (вход или докупка) в пределах ±WIN минут
от его входа; иначе miss (no_coin — монеты не было в нашей «десятке дня»). Обратная сторона:
наши кампании без сделки мастера рядом.

Пишет kamaz_master/match.csv (все сделки мастера с начала журналов) и our_only.csv; печатает таблицу.
Запуск:  .venv/bin/python kamaz_master_match.py [--win 30] [--no-rsync]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "inbox" / "private" / "kamaz_master"
JDIR = OUT / "our_journal"
SSH = "ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 -i " + str(Path.home() / ".ssh" / "id_ed25519")
REMOTE = "root@103.88.243.232:/opt/kamaz-bot/bot/state/journal/"
F = "%Y-%m-%d %H:%M:%S"


def pt(s: str) -> dt.datetime:
    return dt.datetime.strptime(s[:19], F)


def sync_journals() -> None:
    JDIR.mkdir(parents=True, exist_ok=True)
    subprocess.run(["rsync", "-az", "--include=*.jsonl", "--exclude=*", "-e", SSH, REMOTE, str(JDIR) + "/"],
                   check=True, timeout=120)


def campaigns() -> tuple[list[dict], dict[str, set], dt.datetime | None]:
    """Кампании бота + монеты «десятки» по дням + начало журнала."""
    camps, cur, tops, first = [], {}, {}, None
    for f in sorted(JDIR.glob("*.jsonl")):
        for ln in f.read_text().splitlines():
            if not ln.startswith("{"):
                continue
            d = json.loads(ln)
            k, at = d.get("kind"), d.get("at")
            if at and first is None:
                first = pt(at)
            if k == "десятка дня":
                tops.setdefault(d["day"], set()).update(d.get("top") or [])
                continue
            src, sym = d.get("src"), d.get("sym")
            if src not in ("бумага", "реал") or not sym:
                continue
            key = (src, sym)
            t = d.get("t") or at
            c = cur.get(key)
            if src == "бумага":
                if k == "вход":
                    cur[key] = dict(src=src, sym=sym, t_in=t, buys=[(t, d["price"], d.get("usd") or 1)], exit=None, t_out=None, p_out=None)
                elif c and k.startswith("докупка"):
                    c["buys"].append((t, d["price"], d.get("usd") or 1))
                elif c and k in ("цель", "таймер"):
                    c.update(exit=k, t_out=t, p_out=d["price"])
                    camps.append(cur.pop(key))
            else:  # демо-счёт
                if k == "исполнение":
                    if c is None:
                        cur[key] = c = dict(src="демо", sym=sym, t_in=t, buys=[], exit=None, t_out=None, p_out=None)
                    c["buys"].append((t, d["price"], (d.get("qty") or 0) * d["price"]))
                elif c and k == "кампания закрыта":
                    last_t, last_p, _ = c["buys"].pop()          # последнее исполнение = закрытие
                    c.update(exit="закрыта", t_out=last_t, p_out=last_p)
                    camps.append(cur.pop(key))
    for c in cur.values():
        c["exit"] = "открыта"
        camps.append(c)
    for c in camps:
        tot = len(c["buys"])
        c["n_add"] = max(tot - 1, 0)
        w = sum(u for _, _, u in c["buys"])
        avg = w / sum(u / p for _, p, u in c["buys"]) if tot and w else None   # средняя по объёму
        c["first_p"] = c["buys"][0][1] if tot else None
        c["pct"] = round((c["p_out"] / avg - 1) * 100, 2) if (avg and c["p_out"]) else None
    return camps, tops, first


def main(argv=None) -> list[dict]:
    ap = argparse.ArgumentParser()
    ap.add_argument("--win", type=int, default=30)
    ap.add_argument("--no-rsync", action="store_true")
    a = ap.parse_args(argv)
    if not a.no_rsync:
        sync_journals()
    camps, tops, first = campaigns()
    if first is None:
        raise RuntimeError("журналы бота пусты")
    win = dt.timedelta(minutes=a.win)
    master = [json.loads(l) for l in (OUT / "closed.jsonl").read_text().splitlines() if l.strip()]
    master = [m for m in master if pt(m["t_open"]) >= first - win]
    master.sort(key=lambda m: m["t_open_ms"])

    used, rows = set(), []
    for m in master:
        coin = m["sym"].removesuffix("USDT")
        t0 = pt(m["t_open"])
        best = None
        for i, c in enumerate(camps):
            if c["sym"] != m["sym"]:
                continue
            for bt, _, _ in c["buys"]:
                dmin = (pt(bt) - t0).total_seconds() / 60
                if abs(dmin) <= a.win and (best is None or abs(dmin) < abs(best[1])):
                    best = (i, dmin)
        day_top = tops.get(t0.strftime("%Y-%m-%d"), set())
        row = dict(sym=coin, m_open=m["t_open"][:16], m_price=m["order_price"], m_close=m["t_close"][:16],
                   m_pct=m["price_pct"], m_full=m.get("full_closed"))
        if best:
            c = camps[best[0]]
            used.add(best[0])
            same = [j for j, cc in enumerate(camps) if cc["sym"] == c["sym"]
                    and abs((pt(cc["t_in"]) - pt(c["t_in"])).total_seconds()) <= 180]   # та же кампания на бумаге и демо
            used.update(same)
            srcs = "+".join(sorted({camps[j]["src"] for j in same}))
            row.update(match="match", d_min=round(best[1]), our=f"{srcs} вход {c['t_in'][5:16]}",
                       our_adds=c["n_add"], our_exit=f"{c['exit']} {str(c['t_out'] or '')[5:16]}", our_pct=c["pct"])
        else:
            row.update(match="miss" if coin in day_top else "miss/no_coin", d_min="", our="", our_adds="",
                       our_exit="", our_pct="")
        rows.append(row)

    ours = [c for i, c in enumerate(camps) if i not in used]
    with (OUT / "match.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["sym"])
        w.writeheader(); w.writerows(rows)
    with (OUT / "our_only.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["src", "sym", "t_in", "first_price", "adds", "exit", "t_out", "pct"])
        for c in ours:
            w.writerow([c["src"], c["sym"], c["t_in"][:16], c["first_p"], c["n_add"], c["exit"],
                        str(c["t_out"] or "")[:16], c["pct"]])

    print(f"сверка (±{a.win} мин, журнал бота с {first:%Y-%m-%d %H:%M} UTC): сделок мастера {len(rows)}, "
          f"match {sum(r['match'] == 'match' for r in rows)}, наших кампаний без мастера {len(ours)}")
    for r in rows:
        print(f"  {r['sym']:<9} {r['m_open']} @{r['m_price']:<10} → {r['m_close']} {r['m_pct']:+.2f}%  "
              f"{r['match']:<12} {r['d_min']!s:>4} {r['our']} {r['our_exit']} {r['our_pct']}")
    return rows


if __name__ == "__main__":
    main()
