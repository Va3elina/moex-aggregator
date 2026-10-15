"""Эталонный набор агента-чтеца справок о СЧА: насколько точно он читает.

Набор — 15 справок всех 7 УК и трёх форматов (docs/), для каждой выверенный правильный ответ
(truth/). Почему справка трудная и откуда взят правильный ответ — cases.json. Собран 15.10.2026 из
сверки всей истории: где агент и база спорили, правильным считается то, что сходится с итогами
разделов самой справки.

    # 1. тексты для прогона (тот же код, что у конвейера на сервере)
    .venv/bin/python research/scha_pipeline/eval/score.py texts /tmp/scha_eval
    # 2. дать агенту каждую /tmp/scha_eval/<ТИКЕР>_<ММ>.txt с BRIEF.md, ответы — /tmp/scha_eval/answers/<ТИКЕР>_<ММ>.json
    # 3. оценка
    .venv/bin/python research/scha_pipeline/eval/score.py score /tmp/scha_eval/answers

Метрики (по каждой справке и в сумме):
  найдено   — сколько бумаг эталона агент выписал (по ISIN, строки одной бумаги складываются)
  лишних    — бумаг, которых в эталоне нет
  ₽ верно   — у скольких найденных стоимость совпала до рубля
  шт верно  — у скольких найденных количество совпало (доли округляются, как в базе)
  дата / активы — совпали ли дата справки и «Общая стоимость активов» (до рубля)
Главная цифра — «полей верно»: (₽ верно + шт верно) / (2 × бумаг в эталоне).

Прогон после каждой правки брифа: стало лучше или хуже — видно цифрой, а не на глаз.
Новую трудную справку — в docs/ + truth/ + строку в cases.json.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _f(v) -> float:
    if v in (None, ""):
        return 0.0
    return float(str(v).replace(" ", "").replace(" ", "").replace(",", "."))


def _agg(rows: list) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        i = str(r.get("isin") or "").strip().upper()
        if i:
            x = out.setdefault(i, [0.0, 0.0])
            x[0] += _f(r.get("quantity"))
            x[1] += _f(r.get("value_rub"))
    return out


def texts(out_dir: str) -> None:
    sys.path.insert(0, str(HERE.parents[2]))
    from Funds.scha_job_tools import extract_text
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for c in json.loads((HERE / "cases.json").read_text()):
        t = extract_text(HERE / "docs" / c["file"])
        (out / f"{c['ticker']}_{c['period']}.txt").write_text(t)
        print(f"{c['ticker']}_{c['period']}.txt  {len(t)} символов")


def score(answers_dir: str) -> None:
    ans = Path(answers_dir)
    tot = dict(n=0, found=0, extra=0, val=0, qty=0, date=0, gross=0, cases=0, missing=0)
    print(f"{'справка':22} {'найдено':>9} {'лишних':>6} {'₽ верно':>8} {'шт верно':>8} {'дата':>4} {'активы':>6}")
    for c in json.loads((HERE / "cases.json").read_text()):
        key = f"{c['ticker']}_{c['period']}"
        truth = json.loads((HERE / "truth" / f"{key}.json").read_text())
        tr = {r["isin"]: (float(r["quantity"]), float(r["value_rub"])) for r in truth["rows"]}
        p = ans / f"{key}.json"
        tot["cases"] += 1
        tot["n"] += len(tr)
        if not p.exists():
            tot["missing"] += 1
            print(f"{key:22} нет ответа")
            continue
        a = json.loads(p.read_text())
        ag = {i: v for i, v in _agg(a.get("rows")).items() if v[0] or v[1]}
        tr_nz = {i: v for i, v in tr.items()}
        found = [i for i in tr_nz if i in ag]
        extra = [i for i in ag if i not in tr_nz]
        val = [i for i in found if abs(ag[i][1] - tr_nz[i][1]) <= 1]
        qty = [i for i in found if round(ag[i][0]) == round(tr_nz[i][0])]
        d_ok = str(a.get("snapshot_date") or "") == str(truth["snapshot_date"])
        g_ok = truth["gross_assets_rub"] is not None and abs(_f(a.get("gross_assets_rub")) - truth["gross_assets_rub"]) <= 1
        for k, v in (("found", len(found)), ("extra", len(extra)), ("val", len(val)), ("qty", len(qty)),
                     ("date", d_ok), ("gross", g_ok)):
            tot[k] += v
        print(f"{key:22} {len(found):>4}/{len(tr):<4} {len(extra):>6} {len(val):>8} {len(qty):>8} "
              f"{'да' if d_ok else 'НЕТ':>4} {'да' if g_ok else 'НЕТ':>6}")
        for i in [i for i in tr_nz if i not in ag][:3]:
            print(f"    пропущена {i}: {tr_nz[i][0]:,.0f} шт, {tr_nz[i][1]:,.2f} ₽".replace(",", " "))
        for i in extra[:3]:
            print(f"    лишняя    {i}: {ag[i][0]:,.0f} шт, {ag[i][1]:,.2f} ₽".replace(",", " "))
        for i in [i for i in found if i not in val][:3]:
            print(f"    ₽ не тот  {i}: агент {ag[i][1]:,.2f}, эталон {tr_nz[i][1]:,.2f}".replace(",", " "))
        for i in [i for i in found if i not in qty][:3]:
            print(f"    шт не те  {i}: агент {ag[i][0]:,.2f}, эталон {tr_nz[i][0]:,.0f}".replace(",", " "))
    n = tot["n"] or 1
    print(f"\nИТОГО {tot['cases']} справок, бумаг в эталоне {tot['n']}: найдено {tot['found']} "
          f"({tot['found'] / n:.1%}), лишних {tot['extra']}, полей верно "
          f"{(tot['val'] + tot['qty']) / (2 * n):.1%} (₽ {tot['val']}, шт {tot['qty']}), "
          f"дата {tot['date']}/{tot['cases']}, активы {tot['gross']}/{tot['cases']}"
          + (f", без ответа {tot['missing']}" if tot["missing"] else ""))


if __name__ == "__main__":
    cmd, arg = (sys.argv[1:] + ["", ""])[:2]
    if cmd == "texts" and arg:
        texts(arg)
    elif cmd == "score" and arg:
        score(arg)
    else:
        print(__doc__)
        sys.exit(2)
