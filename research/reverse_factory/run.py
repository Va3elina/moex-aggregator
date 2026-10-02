"""Завод обратного инжиниринга торговых стратегий. Карта — README.md, знания — KNOWLEDGE.md.

  .venv/bin/python run.py add invvo 16                  # стратегия invvo (все сделки + кривая)
  .venv/bin/python run.py add invvo all                 # все стратегии invvo
  .venv/bin/python run.py add bybit-file inbox/x.txt    # выгрузка из вкладки Bybit (см. factory/sources/bybit.py)
  .venv/bin/python run.py add tv trades.csv --name X --sym BTCUSDT [--tz 3]   # «Список сделок» TradingView
  .venv/bin/python run.py add csv f.csv --name X --map sym=s,side=d,p_open=e,p_close=c,t_open=t0,t_close=t1 [--time-unit s]
  .venv/bin/python run.py analyze <slug> [--tf 360] [--skip entry,replicate]
  .venv/bin/python run.py bybit-js <leaderMark>         # напечатать скрипт выгрузки для вкладки Bybit
  .venv/bin/python run.py bybit-bundle-js "slug=mark;slug2=mark2" --name b.json   # пакет → форма на приёмник
  .venv/bin/python -m factory.receiver &                # приёмник 127.0.0.1:8765 → inbox/
  .venv/bin/python run.py add bybit-bundle inbox/b.json
  .venv/bin/python run.py screen [--style bot|human] [--links] [--farms]   # отбор целей из скрининга витрины Bybit
  .venv/bin/python run.py analyze-all                   # перегнать все цели на текущем коде (после улучшений)
  .venv/bin/python run.py registry                      # сводка всех разобранных целей
"""
from __future__ import annotations

import argparse
import json
import sys

import pandas as pd

from factory import classify, context, entry_rules, equity, exits, fingerprint, replicate, report
from factory.schema import ROOT, load_target, positions, save_target, slugify


def analyze(slug: str, tf: int | None = None, skip: set[str] = frozenset()) -> dict:
    tr, meta, eq = load_target(slug)
    p = positions(tr)
    print(f"[{slug}] ордеров {len(tr)}, позиций {len(p)}")
    fp = fingerprint.run(tr, p)
    if "context" not in skip:
        fp["context"] = context.run(p)
    cl = classify.run(fp, meta)
    print(f"  тип: {cl['archetype']} · {cl['insurance']} · {cl['who']}")
    eq_approx = False
    if eq is None or len(eq) < 5:
        eq, eq_approx = equity.from_positions(p), True
    eqs = equity.stats(eq)
    # вывод о «страховке» уточняется кривой (докупщиков по дневным сделкам не всегда видно)
    if eqs and not eq_approx and eqs.get("tail_verdict", "").startswith("продаёт") and not cl["insurance"].startswith("продаёт"):
        cl["insurance"] = "продаёт страховку (по кривой)"
        cl["reasons"].append(f"по кривой: худший месяц = {eqs['worst_month_vs_avg_good']} средних хороших месяцев")
    rep = None
    if "replicate" not in skip:
        if eq_approx:
            rep = dict(skipped="у источника нет своей кривой доходности — по оценочной кривой копию не подбираем")
        else:
            rep = replicate.run(eq, p)
            print(f"  копия: {rep.get('skipped') or ('вне подбора ' + str(rep['corr_out']))}")
    ent = None
    if "entry" not in skip:
        ent = entry_rules.run(p, fp, meta, tf_min=tf)
        print(f"  вход: {ent.get('skipped') or ent['verdict'] + ' — ' + ent['best_by_train']['правило']}")
    exi = None if "exits" in skip else exits.run(p, fp, tf_min=tf or (fp["timing"] or {}).get("grid_tf_min"))
    passport = report.build(slug, meta, p, fp, cl, eqs, eq, eq_approx, rep, ent, exi)
    print(f"  → targets/{slug}/REPORT.md")
    return passport


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("add"); a.add_argument("kind"); a.add_argument("what"); a.add_argument("--name"); a.add_argument("--slug")
    a.add_argument("--sym"); a.add_argument("--tz", type=float, default=0.0); a.add_argument("--map"); a.add_argument("--time-unit")
    a.add_argument("--dump", help="готовая выгрузка invvo.json, чтобы не качать заново"); a.add_argument("--no-analyze", action="store_true")
    z = sub.add_parser("analyze"); z.add_argument("slug"); z.add_argument("--tf", type=int); z.add_argument("--skip", default="")
    j = sub.add_parser("bybit-js"); j.add_argument("mark")
    jb = sub.add_parser("bybit-bundle-js"); jb.add_argument("pairs", help="slug=leaderMark;slug2=mark2"); jb.add_argument("--name", default="bundle.json")
    sub.add_parser("registry")
    sc = sub.add_parser("screen", help="отбор целей из сохранённого скрининга Bybit")
    sc.add_argument("--file", default=str(ROOT / "inbox" / "bybit_screen_2026-09-26.json"))
    sc.add_argument("--style", default="any", choices=["any", "bot", "human"]); sc.add_argument("--max-per-week", type=float, default=10)
    sc.add_argument("--majors", type=float, default=0.6); sc.add_argument("--min-pos", type=int, default=10)
    sc.add_argument("--min-payoff", type=float, default=1.5); sc.add_argument("--allow-averaging", action="store_true")
    sc.add_argument("--links", action="store_true", help="только трейдеры со ссылками на внешнюю историю")
    sc.add_argument("--farms", action="store_true", help="найти фермы клонов"); sc.add_argument("--top", type=int, default=30)
    aa = sub.add_parser("analyze-all", help="перегнать все цели на текущем коде (после улучшения завода)"); aa.add_argument("--skip", default="")
    args = ap.parse_args()

    if args.cmd == "registry":
        r = pd.read_csv(ROOT / "registry.csv")
        pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
        print(r.drop(columns=["report"]).to_string(index=False))
        return
    if args.cmd == "bybit-js":
        from factory.sources import bybit
        print(bybit.export_js(args.mark))
        return
    if args.cmd == "bybit-bundle-js":
        from factory.sources import bybit
        pairs = [x.split("=", 1) for x in args.pairs.split(";") if x]
        print(bybit.bundle_js(pairs, args.name))
        return
    if args.cmd == "screen":
        from factory import screen
        S = screen.load(args.file)
        pd.set_option("display.width", 250); pd.set_option("display.max_colwidth", 40)
        if args.farms:
            for g in screen.clone_farms(S):
                print("ферма:", ", ".join(g))
            return
        R = screen.leads_with_links(S) if args.links else screen.pick(
            S, args.max_per_week, args.min_pos, args.majors, args.min_payoff, args.style, not args.allow_averaging, args.top)
        print(R.drop(columns=["mark"]).to_string(index=False))
        pairs = ";".join(f"{slugify(n, 'bybit')}={m}" for n, m in zip(R.name, R.mark))
        print(f"\nвыгрузить пакетом:\n.venv/bin/python run.py bybit-bundle-js \"{pairs}\" --name screen_pick.json > inbox/screen_pick.js")
        return
    if args.cmd == "analyze-all":
        for d in sorted((ROOT / "targets").iterdir()):
            if (d / "trades.parquet").exists():
                try:
                    analyze(d.name, None, set(filter(None, args.skip.split(","))))
                except Exception as e:
                    print(f"[{d.name}] ОШИБКА: {e!r}")
        return
    if args.cmd == "analyze":
        analyze(args.slug, args.tf, set(filter(None, args.skip.split(","))))
        return

    slugs = []
    if args.kind == "invvo":
        from factory.sources import invvo
        dump = json.load(open(args.dump)) if args.dump else None
        ids = [int(k) for k in dump] if (args.what == "all" and dump) else \
              [sid for sid, _ in invvo.list_ids()] if args.what == "all" else [int(args.what)]
        for sid in ids:
            tr, meta, eq = invvo.build(sid, dump)
            slug = args.slug or slugify(meta["name"], f"invvo-{sid}")
            save_target(slug, tr, meta, eq); slugs.append(slug)
            print(f"invvo {sid} → {slug}: {len(tr)} сделок, кривая {len(eq)} дн.")
    elif args.kind == "bybit-bundle":
        from factory.sources import bybit
        for f in bybit.split_bundle(args.what):
            tr, meta, eq = bybit.from_export(f)
            slug = f.name.removesuffix(".bybit.txt")
            if tr.empty:
                print(f"{slug}: сделок нет (история скрыта или пуста)"); continue
            save_target(slug, tr, meta, eq); slugs.append(slug)
    elif args.kind == "bybit-file":
        from factory.sources import bybit
        tr, meta, eq = bybit.from_export(args.what, args.name)
        slug = args.slug or slugify(meta["name"], "bybit-" + str(abs(hash(meta["source_id"])) % 10**6))
        save_target(slug, tr, meta, eq); slugs.append(slug)
    elif args.kind in ("tv", "csv"):
        from factory.sources import csvfile
        if args.kind == "tv":
            tr = csvfile.tradingview(args.what, args.sym, args.tz)
        else:
            mapping = dict(x.split("=") for x in args.map.split(","))
            tr = csvfile.generic(args.what, mapping, args.time_unit, args.tz)
        meta = dict(name=args.name, source=args.kind, source_id=args.what, url="", description="",
                    time_note=f"исходное время UTC{args.tz:+.0f} переведено в UTC")
        slug = args.slug or slugify(args.name, "csv")
        save_target(slug, tr, meta); slugs.append(slug)
    else:
        sys.exit(f"неизвестный источник {args.kind}")
    if not args.no_analyze:
        for s in slugs:
            try:
                analyze(s)
            except Exception as e:  # одна сломанная цель не должна останавливать пакет
                print(f"[{s}] ОШИБКА: {e!r}")


if __name__ == "__main__":
    main()
