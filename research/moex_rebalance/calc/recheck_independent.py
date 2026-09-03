#!/usr/bin/env python
"""Независимая перепроверка (Fable, 02.09.2026) — расчёт написан заново, с нуля.

Ничего из calc/rebalance.py не импортируется. Цель — не перезапустить чужой код, а
получить те же числа другим путём и сравнить. Что сделано иначе:

  1. LW и FF перечитаны из первоисточников (PDF и HTML релиза) своим парсером,
     а не взяты из data/*.json.
  2. Процедура потолка реализована БУКВАЛЬНО по тексту п. 2.8.4 — в пространстве ВЕСОВ:
     «вес устанавливается равным величине», «разница пропорционально распределяется
     между неограниченными», «повторяется итерационно». Существующий скрипт решает ту
     же задачу через массы (target = 0,15·A/(1−0,15·k)). Если оба дают одни веса —
     это подтверждение алгоритма, а не копия.
  3. Валидация на июне-2025 повторена этим же новым кодом.

Запуск:  .venv/bin/python research/moex_rebalance/calc/recheck_independent.py
"""
from __future__ import annotations

import html
import json
import pathlib
import re
import statistics as st
import subprocess

BASE = pathlib.Path(__file__).resolve().parents[1]
SRC, DATA = BASE / "sources", BASE / "data"

CAP = 15.0
ISSUER = {"SBER": "Сбербанк", "SBERP": "Сбербанк", "TATN": "Татнефть", "TATNP": "Татнефть",
          "SNGS": "Сургутнефтегаз", "SNGSP": "Сургутнефтегаз", "RTKM": "Ростелеком", "RTKMP": "Ростелеком"}
iss_of = lambda t: ISSUER.get(t, t)                                  # noqa: E731
is_round = lambda x: x is not None and abs(x * 10 - round(x * 10)) < 1e-9   # noqa: E731

verdicts: list[tuple[str, bool, str]] = []
def V(name: str, ok: bool, detail: str = "") -> None:
    verdicts.append((name, ok, detail))
    print(f"  {'✓' if ok else '✗'} {name}" + (f"  — {detail}" if detail else ""))


# ─── 1. первоисточники своим парсером ────────────────────────────────────────────
def parse_lw_pdf() -> dict[str, float]:
    txt = subprocess.run(["pdftotext", "-layout", str(SRC / "lw_table_18.09.2026.pdf"), "-"],
                         capture_output=True, text=True, timeout=60).stdout
    out = {}
    for ln in txt.splitlines():
        m = re.match(r"\s*(\d+)\s+([A-Z][A-Z0-9]*)\s+.+?\s([01],\d)\s*$", ln)
        if m:
            out[m.group(2)] = float(m.group(3).replace(",", "."))
    return out

def parse_ff_release() -> dict[str, float]:
    raw = (SRC / "press_release_n103733.html").read_text(encoding="utf-8", errors="replace")
    t = re.sub(r"<script.*?</script>|<style.*?</style>", " ", raw, flags=re.S)
    lines = [re.sub(r"\s+", " ", html.unescape(x)).strip() for x in re.sub(r"<[^>]+>", "\n", t).split("\n")]
    lines = [x for x in lines if x]
    i = next(k for k, x in enumerate(lines) if "Новый free-float" in x)
    out = {}
    seg = lines[i + 1:i + 80]
    for k in range(len(seg) - 2):
        if re.fullmatch(r"[A-Z][A-Z0-9]*", seg[k]) and re.fullmatch(r"\d{1,3}%", seg[k + 2]):
            out[seg[k]] = int(seg[k + 2][:-1]) / 100
    return out

def exclusions_release() -> list[str]:
    raw = (SRC / "press_release_n103733.html").read_text(encoding="utf-8", errors="replace")
    t = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>", " ", raw, flags=re.S))))
    m = re.search(r"Индекса МосБиржи и Индекса РТС покинут (.{0,200}?)\.", t)
    return re.findall(r'"([^"]+)"', m.group(1)) if m else []


# ─── 2. процедура потолка БУКВАЛЬНО по п. 2.8.4, в пространстве весов ────────────
def cap_procedure_literal(w0: dict[str, float]) -> tuple[dict[str, float], set[str]]:
    """w0 — веса без ограничений, в сумме 100. Возвращает (веса, обрезанные эмитенты)."""
    w = dict(w0)
    capped: set[str] = set()
    for _ in range(100):
        by_iss: dict[str, float] = {}
        for t, x in w.items():
            by_iss[iss_of(t)] = by_iss.get(iss_of(t), 0.0) + x
        over = {g: v for g, v in by_iss.items() if v > CAP + 1e-12 and g not in capped}
        if not over:
            break
        excess = 0.0
        for g, v in over.items():                       # «устанавливается равным этой величине»
            k = CAP / v
            for t in w:
                if iss_of(t) == g:
                    w[t] *= k
            excess += v - CAP
            capped.add(g)
        free = [t for t in w if iss_of(t) not in capped]  # «между Эмитентами … не были ограничены»
        fs = sum(w[t] for t in free)
        for t in free:                                     # «пропорционально распределяется»
            w[t] += excess * w[t] / fs
    return w, capped


def run_base(cap_fix: dict, ff: dict, lw: dict, cap_asof: dict, old_w_asof: dict,
             excluded: set[str]) -> tuple[dict, dict, set]:
    """Шаг А на дате фиксации → WW; шаг Б на ASOF → новые веса. Возвращает (веса, WW, обрезанные)."""
    keep = [t for t in cap_fix if t not in excluded]
    mc = {t: cap_fix[t] * ff[t] * lw[t] for t in keep}
    S = sum(mc.values())
    w0 = {t: 100 * v / S for t, v in mc.items()}
    w_fix, capped = cap_procedure_literal(w0)
    # WW: у обрезанных = отношение итоговой массы к исходной; у остальных = 1 (п. 2.8.6)
    ww = {}
    for t in keep:
        if iss_of(t) in capped:
            # доля обрезанного в итоге / доля до потолка, приведённая к общей нормировке
            ww[t] = (w_fix[t] / w0[t]) * (sum(w0[x] for x in keep if iss_of(x) not in capped)
                                          / sum(w_fix[x] for x in keep if iss_of(x) not in capped))
        else:
            ww[t] = 1.0
    mc_new = {t: cap_asof[t] * ff[t] * lw[t] * ww[t] for t in keep}
    S2 = sum(mc_new.values())
    w_new = {t: 100 * v / S2 for t, v in mc_new.items()}
    return w_new, ww, capped


def main() -> int:
    print("\n═══ 1. Первоисточники, перечитанные заново")
    lw_pdf = parse_lw_pdf()
    ff_rel = parse_ff_release()
    excl = exclusions_release()
    lw_json = json.loads((DATA / "lw_new.json").read_text(encoding="utf-8"))
    ff_json = json.loads((DATA / "ff_new.json").read_text(encoding="utf-8"))
    V("таблица LW из PDF: 60 строк", len(lw_pdf) == 60, str(len(lw_pdf)))
    V("LW из PDF совпадает с data/lw_new.json", lw_pdf == lw_json,
      f"расхождения: {[k for k in set(lw_pdf)|set(lw_json) if lw_pdf.get(k)!=lw_json.get(k)]}")
    V("free-float из релиза: 15 бумаг", len(ff_rel) == 15, str(len(ff_rel)))
    V("FF из релиза совпадает с data/ff_new.json", ff_rel == ff_json)
    V("из IMOEX уходят ровно Лента и Мосэнерго", set(excl) == {"Лента", "Мосэнерго"}, str(excl))
    V("T (Т-Технологии) в таблице LW = 0,8", lw_pdf.get("T") == 0.8)
    V("ЛУКОЙЛа, TATNP, MSNG в таблице LW нет → LW = 1", not any(k in lw_pdf for k in ("LKOH", "TATNP", "MSNG")))

    print("\n═══ 2. Тождество веса (ISS, 01.09): вес ∝ cap × ff × w")
    iss = json.loads((DATA / "iss_imoex.json").read_text(encoding="utf-8"))["бумаги"]
    ASOF, FIX = "2026-09-01", "2026-08-31"
    num = {t: v[ASOF]["cap"] * v[ASOF]["ff"] * v[ASOF]["w"] for t, v in iss.items() if ASOF in v}
    S = sum(num.values())
    worst = max(abs(100 * x / S - iss[t][ASOF]["weight"]) for t, x in num.items())
    V(f"46 бумаг, макс. расхождение {worst:.4f} п.п. ≤ 0,006", worst <= 0.006)

    print("\n═══ 3. Сентябрь-2026: расчёт заново, буквальной процедурой")
    tick = [t for t in iss if ASOF in iss[t] and FIX in iss[t]]
    ff_all = {t: ff_rel.get(t, iss[t][ASOF]["ff"]) for t in tick}
    lw_all = {t: lw_pdf.get(t, 1.0) for t in tick}
    cap_fix = {t: iss[t][FIX]["cap"] for t in tick}
    cap_asof = {t: iss[t][ASOF]["cap"] for t in tick}
    old_w = {t: 100 * num[t] / S for t in tick}
    w_new, ww, capped = run_base(cap_fix, ff_all, lw_all, cap_asof, old_w, {"LENT", "MSNG"})
    V("потолок сработал у ЛУКОЙЛа и Сбербанка", capped == {"LKOH", "Сбербанк"}, str(sorted(capped)))
    V("WW у всех необрезанных ровно 1,0", all(ww[t] == 1.0 for t in ww if iss_of(t) not in capped))
    V("WW у обрезанных ≤ 1 (п. 2.8.6)", all(0 < ww[t] <= 1 for t in ww))

    # сравнение с существующим расчётом
    ref = json.loads((DATA / "trades_by_weight.json").read_text(encoding="utf-8"))
    refw = ref["веса"] if "веса" in ref else ref
    diffs = {t: abs(w_new.get(t, 0.0) - refw[t]["new"]) for t in refw}
    V(f"новые веса совпали с calc/rebalance.py, макс. расхождение {max(diffs.values()):.6f} п.п.",
      max(diffs.values()) < 1e-6)
    ww_ref = {t: refw[t]["ww"] for t in refw if refw[t].get("ww")}
    dww = max(abs(ww[t] - ww_ref[t]) for t in ww if t in ww_ref)
    V(f"WW совпали с существующим расчётом, макс. расхождение {dww:.2e}", dww < 1e-6)

    funds = json.loads((DATA / "funds.json").read_text(encoding="utf-8"))
    nav = {k: v["руб"] for k, v in funds["сча"].items()}
    NAV = sum(nav.values())
    trades = {t: (w_new.get(t, 0.0) - old_w[t]) / 100 * NAV for t in tick}
    sells = -sum(v for v in trades.values() if v < 0)
    buys = sum(v for v in trades.values() if v > 0)
    print(f"\n  СЧА трёх фондов: {NAV/1e9:.3f} млрд ₽")
    print(f"  ЛУКОЙЛ:   {old_w['LKOH']:.2f}% → {w_new['LKOH']:.2f}%   сделка {trades['LKOH']/1e6:+.0f} млн")
    print(f"  Сбербанк: {old_w['SBER']:.2f}% → {w_new['SBER']:.2f}%   сделка {trades['SBER']/1e6:+.0f} млн")
    print(f"  Газпром:  {old_w['GAZP']:.2f}% → {w_new['GAZP']:.2f}%   сделка {trades['GAZP']/1e6:+.0f} млн")
    print(f"  продать {sells/1e6:.0f} млн · докупить {buys/1e6:.0f} млн · {100*sells/NAV:.2f}% СЧА")
    for f, n in nav.items():
        print(f"    {f}: {sells*n/NAV/1e6:.0f} млн ({100*sells/NAV:.2f}% СЧА)")
    V("продажи = покупки (кэш-нейтрально)", abs(sells - buys) < 1e-3)
    V("ЛУКОЙЛ: продажа ≈ 890 млн", abs(trades["LKOH"] / 1e6 + 890) < 3, f"{trades['LKOH']/1e6:+.0f}")
    V("всего продать ≈ 1 209 млн", abs(sells / 1e6 - 1209) < 3, f"{sells/1e6:.0f}")
    V("Газпром: покупка ≈ 134 млн", abs(trades["GAZP"] / 1e6 - 134) < 3, f"{trades['GAZP']/1e6:+.0f}")

    print("\n═══ 4. Валидация на июне-2025 (только уход) — тем же новым кодом")
    J = json.loads((DATA / "iss_imoex_june2025.json").read_text(encoding="utf-8"))
    EFF, FIX25 = "2025-06-20", "2025-05-30"
    new = {t: v[EFF] for t, v in J.items() if EFF in v}
    lw25 = {t: (v["w"] if is_round(v["w"]) else {"SBER": 0.3, "SBERP": 0.6}.get(t, 1.0)) for t, v in new.items()}
    ff25 = {t: v["ff"] for t, v in new.items()}
    capf = {t: J[t][FIX25]["cap"] for t in new}
    cape = {t: J[t][EFF]["cap"] for t in new}
    w25, _, capped25 = run_base(capf, ff25, lw25, cape, {}, set())
    err = {t: abs(w25[t] - new[t]["weight"]) for t in new}
    print(f"  бумаг {len(new)} · медиана ошибки {st.median(err.values()):.4f} · макс {max(err.values()):.3f} п.п."
          f" (у {max(err, key=err.get)})")
    print(f"  ЛУКОЙЛ прогноз {w25['LKOH']:.2f}% / факт {new['LKOH']['weight']:.2f}%"
          f" · Сбербанк {w25['SBER']:.2f}% / {new['SBER']['weight']:.2f}%")
    V("макс. ошибка ≤ 0,006 п.п. (округление публикуемых весов)", max(err.values()) <= 0.006)
    V("обрезаны ЛУКОЙЛ и Сбербанк", capped25 == {"LKOH", "Сбербанк"})

    print("\n═══ ИТОГ")
    bad = [n for n, ok, _ in verdicts if not ok]
    print(f"  проверок {len(verdicts)}, провалено {len(bad)}")
    for n in bad:
        print(f"  ✗ {n}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
