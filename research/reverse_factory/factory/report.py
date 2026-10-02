"""Сборка REPORT.md + passport.json + графики, обновление registry.csv и JOURNAL.md."""
from __future__ import annotations

import json
from datetime import date

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .schema import ROOT, target_dir

P = lambda x: f"{100 * x:+.0f}%" if x is not None else "—"


def _fig_timing(fp, p, d):
    fig, ax = plt.subplots(1, 2, figsize=(12, 3.2))
    g = fp["timing"]["grid_all"]
    ax[0].bar([str(k) for k in g], list(g.values()), color="#5DA3E9")
    ax[0].set_title("доля входов сразу после закрытия свечи, по таймфреймам (мин)")
    ax[0].axhline(0.5, ls="--", color="gray", lw=.8)
    p.t_open.dt.hour.value_counts().sort_index().plot.bar(ax=ax[1], color="#E0A34E")
    ax[1].set_title("час входа, UTC")
    plt.tight_layout(); plt.savefig(d / "figs" / "timing.png", dpi=100); plt.close()


def _fig_exits(fp, p, d):
    c = p[p.t_close.notna()]
    if c.empty:
        return
    fig, ax = plt.subplots(figsize=(12, 3.2))
    x = c.pnl_pct.clip(c.pnl_pct.quantile(.01), c.pnl_pct.quantile(.99))
    ax.hist(x, bins=120, color="#888")
    for key, col in [("fixed_tp", "#2a9d55"), ("fixed_sl", "#d64545")]:
        if fp["exits"].get(key):
            ax.axvline(fp["exits"][key]["level"], color=col, lw=1.5)
    ax.set_title("ход цены на позицию, % (без плеча); линии — найденные фиксированные тейк/стоп")
    plt.tight_layout(); plt.savefig(d / "figs" / "exits.png", dpi=100); plt.close()


def _fig_equity(eq, d, approx):
    if eq is None or len(eq) < 5:
        return
    fig, ax = plt.subplots(2, 1, figsize=(12, 5), sharex=True, gridspec_kw={"height_ratios": [3, 1]})
    ax[0].plot(eq.index, eq.values, color="k", lw=1.2)
    ax[0].set_yscale("log")
    ax[0].set_title("кривая доходности" + (" (ОЦЕНКА по сделкам)" if approx else " (данные источника)"))
    dd = eq / eq.cummax() - 1
    ax[1].fill_between(dd.index, dd.values, 0, color="#d64545", alpha=.5)
    ax[1].set_title("просадка")
    plt.tight_layout(); plt.savefig(d / "figs" / "equity.png", dpi=100); plt.close()


def _fig_replica(rep, d):
    cv = rep.get("_curves")
    if cv is None:
        return
    fig, ax = plt.subplots(figsize=(12, 4.5))
    for col, c in zip(cv.columns, ["k", "#E0A34E", "#5DA3E9"]):
        ax.plot(cv.index, cv[col], label=col, color=c, lw=1.3)
    ax.axvline(pd.Timestamp(rep["split"]), ls="--", color="gray")
    ax.set_yscale("log"); ax.legend(); ax.set_title(f"копия из простых правил: корреляция вне подбора {rep['corr_out']}")
    plt.tight_layout(); plt.savefig(d / "figs" / "replica.png", dpi=100); plt.close()


def build(slug, meta, p, fp, cl, eqs, eq, eq_approx, rep, ent, exi) -> dict:
    d = target_dir(slug)
    _fig_timing(fp, p, d); _fig_exits(fp, p, d); _fig_equity(eq, d, eq_approx)
    if rep and "_curves" in rep:
        _fig_replica(rep, d)
    b, t, bt = fp["basics"], fp["timing"], fp["batching"]
    L = []
    L.append(f"# {meta.get('name')} — обратный инжиниринг\n")
    L.append(f"*Источник: {meta.get('source')} · {meta.get('url', '')} · разбор {date.today():%d.%m.%Y}*\n")
    if meta.get("description"):
        L.append(f"> {meta['description'][:600]}\n")
    # ── коротко
    L.append("## Коротко\n")
    L.append(f"- **Тип:** {cl['archetype']} · {cl['insurance']} · **{cl['who']}**")
    for r in cl["reasons"]:
        L.append(f"  - {r}")
    sc = cl.get("self_check")
    if sc:
        L.append(f"  - сверка с самоописанием: заявлено «{sc['claimed']}» → {'✅ совпало' if sc['match'] else '❌ НЕ совпало — разобрать, поправить classify.py, записать в KNOWLEDGE.md'}")
    tf = t.get("grid_tf_min")
    cx = fp.get("context") or {}
    if cx.get("verdict"):
        L.append(f"- **Контекст входа:** {cx['verdict']}: " + ", ".join(f"за {h} {int(100 * cx[h]['with_share'])}% по ходу (медиана {cx[h]['median']}%)" for h in ["4ч", "24ч", "72ч"] if h in cx))
    L.append(f"- **Когда входит:** " + (f"на закрытии свечи {tf} мин (≈{t['grid']['lag_min']} мин после закрытия, {int(100 * t['grid']['share'])}% входов)" if tf else "без привязки к свечам")
             + (f"; {int(100 * bt['batch_share'])}% входов пачками по разным монетам" if bt["batch_share"] >= 0.25 else ""))
    if ent and not ent.get("skipped"):
        bb = ent["best_by_train"]
        L.append(f"- **Правило входа:** {ent['verdict']} — лучшее: «{bb['правило']}», на проверке F1 {bb['F1_проверка']} "
                 f"(охват {bb['охват']}, точность {bb['точность']} на всей истории)")
    elif ent:
        L.append(f"- **Правило входа:** не искали — {ent['skipped']}")
    if exi:
        L.append(f"- **Как выходит:** " + "; ".join(exi.get("verdict", [])))
    if eqs:
        yrs = ", ".join(f"{k}: {P(v)}" for k, v in eqs["by_year"].items())
        L.append(f"- **Результат{' (оценка по сделкам)' if eq_approx else ''}:** ×{eqs['total_x']}, {P(eqs['cagr'])} в год, "
                 f"макс. просадка {P(eqs['max_dd'])}, сейчас {P(eqs['dd_now'])} ({eqs['days_in_dd_now']} дн.). По годам: {yrs}")
        L.append(f"- **Хвосты:** месяцев в плюс {int(100 * (eqs.get('months_up') or 0))}%, худший месяц = {eqs.get('worst_month_vs_avg_good')} средних хороших, "
                 f"просадка/волатильность {eqs.get('dd_to_vol')} → {eqs.get('tail_verdict')}")
    if rep and not rep.get("skipped"):
        fam = ", ".join(f"{k} {int(100 * v)}%" for k, v in rep["families"].items())
        L.append(f"- **Природа по кривой:** {fam} · совпадение копии вне подбора {rep['corr_out']} (на подборе {rep['corr_in']})"
                 + (" — ⚠️ кривая короткая, вывод слабый" if rep.get("weak") else ""))
        sh = rep.get("shape") or {}
        L.append(f"- **Форма доходности:** участие в росте рынка {sh.get('beta_up')}, в падении {sh.get('beta_down')} → {sh.get('verdict')}")
    notes = [meta.get(k) for k in ["history_note", "price_note", "equity_note"] if meta.get(k)]
    if notes:
        L.append("- **Оговорки:** " + "; ".join(notes))
    # ── подробно
    L.append("\n## Почерк\n")
    L.append(f"| | |\n|---|---|\n| период | {b['start']} → {b['end']} ({b['span_days']} дн.) |\n| позиций | {b['n_pos']} ({b['per_week']} в неделю), открыто сейчас {b['open_now']} |"
             f"\n| монет | {b['n_sym']}: {', '.join(f'{k} {v}' for k, v in b['top_sym'].items())} |\n| доля BTC/ETH/SOL/XRP/BNB/золото | {int(100 * b['majors_share'])}% |"
             f"\n| шорты | {int(100 * b['short_share'])}% |\n| в плюс | {int(100 * b['win'])}% |\n| средний плюс / минус | {b['avg_win']}% / {b['avg_loss']}% (отношение {b['payoff']}) |"
             f"\n| худшая / лучшая | {b['worst']}% / {b['best']}% |\n| удержание 10/50/90% | {b['hold_h']} ч |"
             f"\n| одновременно открыто макс. | {fp['concurrency']['max_open']} |\n| доливки | {fp['averaging']} |\n| уровни цен | {fp['levels']} |")
    L.append("\n![время входов](figs/timing.png)\n\n![выходы](figs/exits.png)\n")
    if eq is not None and len(eq):
        L.append("![кривая](figs/equity.png)\n")
    if rep and not rep.get("skipped"):
        L.append("## Копия по кривой\n")
        L.append(f"Подбор на {rep['days']} днях, разрез {rep['split']}. Корреляция дневной доходности: подбор {rep['corr_in']}, "
                 f"**проверка {rep['corr_out']}**, вся история {rep['corr_full']} (R² {rep['r2_full']}).\n")
        L.append("| компонента копии | вес |\n|---|---|\n" + "\n".join(f"| {k} | {v} |" for k, v in rep["top_weights"].items()))
        L.append("\nСильнее всего совпадает по одиночке: " + ", ".join(f"{k} ({v})" for k, v in rep["best_single"].items()))
        L.append("\nСильнее всего противоположна: " + ", ".join(f"{k} ({v})" for k, v in rep["worst_single"].items()))
        L.append("\n![копия](figs/replica.png)\n")
    elif rep:
        L.append(f"## Копия по кривой\n\nНе делали: {rep['skipped']}\n")
    if ent and not ent.get("skipped"):
        L.append("## Правило входа\n")
        L.append(f"Таймфрейм {ent['interval']}, монет {ent['symbols']}, входов в окне {ent['entries']}. "
                 f"Точность считается только по свечам, где цель вне позиции по монете.\n")
        R = ent["_rules"].head(12)
        L.append(R.to_markdown(index=False) if hasattr(R, "to_markdown") else R.to_string(index=False))
        S = ent["_signature"].head(14)
        if len(S):
            L.append("\n**Подпись свечи входа** (эффект = сдвиг медианы входов от обычных свечей в межквартильных размахах):\n")
            L.append(S.to_markdown(index=False) if hasattr(S, "to_markdown") else S.to_string(index=False))
        if ent.get("_tree"):
            L.append(f"\n**Дерево решений** (подбор на первых 2/3; на проверке {ent.get('tree_test')}):\n```\n{ent['_tree']}```")
    if exi:
        L.append("\n## Выходы\n")
        L.append("```\n" + json.dumps({k: v for k, v in exi.items()}, ensure_ascii=False, indent=1, default=str) + "\n```")
    (d / "REPORT.md").write_text("\n".join(L))
    # паспорт
    clean = lambda x: {k: v for k, v in (x or {}).items() if not str(k).startswith("_")}
    passport = dict(slug=slug, name=meta.get("name"), source=meta.get("source"), url=meta.get("url"), analyzed=str(date.today()),
                    classify=cl, fingerprint=fp, equity=eqs, equity_approx=eq_approx, replicate=clean(rep), entry=clean(ent), exits=exi)
    (d / "passport.json").write_text(json.dumps(passport, ensure_ascii=False, indent=1, default=str))
    if ent and "_rules" in ent:
        ent["_rules"].to_csv(d / "rules.csv", index=False)
    # реестр
    row = dict(slug=slug, name=meta.get("name"), source=meta.get("source"), period=f"{b['start']}…{b['end']}", positions=b["n_pos"],
               archetype=cl["archetype"], insurance=cl["insurance"], who=cl["who"], entry_tf_min=tf,
               tp=(fp["exits"].get("fixed_tp") or {}).get("level"), sl=(fp["exits"].get("fixed_sl") or {}).get("level"),
               win=b["win"], payoff=b["payoff"], cagr=(eqs or {}).get("cagr"), max_dd=(eqs or {}).get("max_dd"),
               replica_corr_out=(rep or {}).get("corr_out"), rule=(ent or {}).get("best_by_train", {}).get("правило") if ent and not ent.get("skipped") else None,
               rule_f1_test=(ent or {}).get("best_by_train", {}).get("F1_проверка") if ent and not ent.get("skipped") else None,
               self_claim=(cl.get("self_check") or {}).get("claimed"), self_match=(cl.get("self_check") or {}).get("match"),
               analyzed=str(date.today()), report=f"targets/{slug}/REPORT.md")
    import fcntl
    reg_path = ROOT / "registry.csv"
    with open(ROOT / ".registry.lock", "w") as lk:          # несколько прогонов параллельно не затирают друг друга
        fcntl.flock(lk, fcntl.LOCK_EX)
        reg = pd.read_csv(reg_path) if reg_path.exists() else pd.DataFrame()
        reg = pd.concat([reg[reg.slug != slug] if len(reg) else reg, pd.DataFrame([row])], ignore_index=True)
        reg.to_csv(reg_path, index=False)
        fcntl.flock(lk, fcntl.LOCK_UN)
    with open(ROOT / "JOURNAL.md", "a") as fh:
        fh.write(f"- {date.today():%d.%m.%Y} · **{meta.get('name')}** ({meta.get('source')}) → {cl['archetype']}, {cl['insurance']}, {cl['who']}"
                 + (f"; копия вне подбора {rep['corr_out']}" if rep and not rep.get('skipped') else "")
                 + (f"; правило «{row['rule']}» F1 {row['rule_f1_test']}" if row["rule"] else "") + f" → [отчёт](targets/{slug}/REPORT.md)\n")
    return passport
