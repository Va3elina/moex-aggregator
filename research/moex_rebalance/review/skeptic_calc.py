#!/usr/bin/env python3
"""
Независимый пересчёт ребаланса IMOEX 18.09.2026 из сырых данных.
Написан БЕЗ чтения авторских calc/*.py.

Методика (п. 2.8 + Приложение 3, редакция от 18.09.2026):
  MC_i = cap_i * FF_i * W_i,   W_i = WW_i * LW_i   (7 знаков)
  Wght_i = MC_i / Σ MC
  Потолок на Дату формирования: эмитент <= 15%; сумма топ-5 эмитентов <= 55%.
  Процедура п.2.8.4: превысивший -> ставим = лимит, разница пропорционально
  распределяется между НЕограниченными, повторять итерационно.
  п.2.8.5: расчёт WW по итогам торгового дня, предшествующего дню раскрытия.
"""
import json, sys, os
from collections import defaultdict

BASE = '/Users/vadim/PyCharmMiscProject/MOEX/research/moex_rebalance/data'
def J(fn):
    return json.load(open(os.path.join(BASE, fn)))

ISS      = J('iss_imoex.json')['бумаги']
FF_NEW   = J('ff_new.json')
LW_NEW   = J('lw_new.json')
PRICES   = J('prices.json')
FUNDS    = J('funds.json')
CHANGES  = J('imoex_changes.json')
LEAVING  = {'LENT', 'MSNG'}          # imoex_changes.json: Лента, Мосэнерго
FIX_DATE = '2026-08-27'              # день перед релизом 28.08.2026 (п.2.8.5)
CUR_DATE = '2026-09-01'
LIMIT    = 0.15
TOP5     = 0.55
NAV      = sum(v['руб'] for v in FUNDS['сча'].values())

# --- эмитенты: ао+ап одного эмитента считаются вместе -----------------------
PAIRS = {'SBERP': 'SBER', 'TATNP': 'TATN', 'SNGSP': 'SNGS', 'MTLRP': 'MTLR',
         'BANEP': 'BANE', 'LSNGP': 'LSNG', 'RTKMP': 'RTKM', 'TRNFP': 'TRNF',
         'NKNCP': 'NKNC', 'KZOSP': 'KZOS'}
def emitter(t):
    return PAIRS.get(t, t)

# --- ядро: процедура ограничения весов п.2.8.4 -------------------------------
def cap_weights(mc, limit=LIMIT, top5=TOP5, verbose=False):
    """mc: {ticker: MC без WW}. Возвращает (WW по тикеру, итоговые веса по тикеру, лог)."""
    tot = sum(mc.values())
    w_sec = {t: v / tot for t, v in mc.items()}
    w_em = defaultdict(float)
    for t, w in w_sec.items():
        w_em[emitter(t)] += w
    uncapped = dict(w_em)                # веса эмитентов ДО ограничения
    capped = {}                          # эмитент -> фиксированный вес
    log = []
    cur = dict(w_em)
    it = 0
    while True:
        it += 1
        over = [e for e, w in cur.items() if e not in capped and w > limit + 1e-12]
        if not over:
            break
        for e in over:
            log.append(f'итерация {it}: {e} {cur[e]*100:.4f}% > {limit*100:.0f}% -> {limit*100:.0f}%')
            capped[e] = limit
        free = [e for e in cur if e not in capped]
        rest = 1.0 - sum(capped.values())
        s = sum(cur[e] for e in free)
        for e in free:
            cur[e] = cur[e] / s * rest
        for e in capped:
            cur[e] = capped[e]
    # проверка топ-5
    top = sorted(cur.values(), reverse=True)[:5]
    log.append(f'сумма топ-5 эмитентов после потолка 15%: {sum(top)*100:.3f}% (лимит {top5*100:.0f}%)'
               + ('  !!! НАРУШЕНО' if sum(top) > top5 + 1e-12 else '  ок'))
    # WW: итоговый вес = MC*WW нормированный. Для неограниченных WW=1 и вес = w_unc * k
    free = [e for e in cur if e not in capped]
    k = sum(cur[e] for e in free) / sum(uncapped[e] for e in free) if free else 1.0
    ww_em = {}
    for e in cur:
        ww_em[e] = 1.0 if e not in capped else round(cur[e] / (uncapped[e] * k), 7)
    ww = {t: ww_em[emitter(t)] for t in mc}
    # пересчёт весов с округлённым WW
    tot2 = sum(mc[t] * ww[t] for t in mc)
    final = {t: mc[t] * ww[t] / tot2 for t in mc}
    return ww, final, log, uncapped

# --- построение нового набора коэффициентов ----------------------------------
def lw_old_of(t, row):
    """LW действующий: если WW=1, то w == LW. Для обрезанных (LKOH, SBER*) LW из lw_new (ratio ап/ао=2 подтверждён)."""
    return row['w']

def new_params(t, row):
    ff = FF_NEW.get(t, row['ff'])
    lw = LW_NEW.get(t, 1.0)
    return ff, lw

def run(fix_date=FIX_DATE, cur_date=CUR_DATE, exclude_leaving_in_cap=True,
        cap_source='iss', verbose=True, out=print):
    """Полный расчёт. cap_source: 'iss' (cap из ISS) или 'price' (issue*close из prices.json)."""
    def cap_of(t, dt):
        r = ISS[t][dt]
        if cap_source == 'iss':
            return r['cap']
        return r['issue'] * PRICES[t][dt]

    tickers = sorted(ISS)
    stay = [t for t in tickers if t not in LEAVING]
    # 1) MC на дату фиксации с новыми FF/LW, WW=1
    universe = stay if exclude_leaving_in_cap else tickers
    mc_fix = {}
    for t in universe:
        r = ISS[t][fix_date]
        ff, lw = new_params(t, r)
        mc_fix[t] = cap_of(t, fix_date) * ff * lw
    ww, w_fix, log, unc = cap_weights(mc_fix)
    if verbose:
        out(f'\n--- фиксация {fix_date}, cap={cap_source}, уходящие {"исключены" if exclude_leaving_in_cap else "ОСТАВЛЕНЫ"} в знаменателе ---')
        for l in log: out('  ' + l)
        out(f'  WW LKOH={ww["LKOH"]:.7f}  WW SBER={ww["SBER"]:.7f}  (SBERP={ww["SBERP"]:.7f})')
        em = defaultdict(float)
        for t, w in w_fix.items(): em[emitter(t)] += w
        out('  топ-6 эмитентов на дату фиксации после потолка: ' + ', '.join(f'{e} {w*100:.3f}' for e, w in sorted(em.items(), key=lambda x: -x[1])[:6]))
    # 2) новые веса на cur_date: те же FF/LW/WW, cap на cur_date
    W_new = {t: round(ww[t] * new_params(t, ISS[t][cur_date])[1], 7) for t in stay}
    mc_new = {t: cap_of(t, cur_date) * new_params(t, ISS[t][cur_date])[0] * W_new[t] for t in stay}
    tot_new = sum(mc_new.values())
    w_new = {t: mc_new[t] / tot_new * 100 for t in stay}
    # текущие веса на cur_date
    mc_cur = {t: cap_of(t, cur_date) * ISS[t][cur_date]['ff'] * ISS[t][cur_date]['w'] for t in tickers}
    tot_cur = sum(mc_cur.values())
    w_cur = {t: mc_cur[t] / tot_cur * 100 for t in tickers}
    # 3) сделки
    trades = {t: (w_new.get(t, 0.0) - w_cur[t]) / 100 * NAV for t in tickers}
    return dict(ww=ww, W_new=W_new, w_new=w_new, w_cur=w_cur, trades=trades, log=log,
                w_fix=w_fix, unc=unc)

def fmt_m(x):
    return f'{x/1e6:+,.1f} млн'

def main():
    lines = []
    out = lambda s='': (print(s), lines.append(s))
    out(f'ΣСЧА = {NAV/1e9:.3f} млрд ₽  (' + ', '.join(f'{k} {v["руб"]/1e9:.3f}' for k, v in FUNDS['сча'].items()) + ')')

    # ---------- 3) тождество ----------
    out('\n=== 3. Тождество cap*ff*w (норм.) == опубликованный weight ===')
    for dt in ['2026-08-27', '2026-09-01']:
        tot = sum(ISS[t][dt]['cap'] * ISS[t][dt]['ff'] * ISS[t][dt]['w'] for t in ISS)
        errs = sorted(((abs(ISS[t][dt]['cap'] * ISS[t][dt]['ff'] * ISS[t][dt]['w'] / tot * 100 - ISS[t][dt]['weight']), t) for t in ISS), reverse=True)
        out(f'  {dt}: n={len(ISS)}, max|err| = {errs[0][0]:.4f} п.п. ({errs[0][1]}), Σweight = {sum(ISS[t][dt]["weight"] for t in ISS):.2f}')

    # ---------- 1) базовый расчёт ----------
    R = run(out=out)
    out('\n=== 1. Базовый расчёт (фиксация 27.08, cap ISS, LENT/MSNG исключены) ===')
    out(f'  WW ЛУКОЙЛ = {R["ww"]["LKOH"]:.7f}   (сейчас w={ISS["LKOH"][CUR_DATE]["w"]})')
    out(f'  WW Сбербанк = {R["ww"]["SBER"]:.7f} -> W ао = {R["W_new"]["SBER"]:.7f}, W ап = {R["W_new"]["SBERP"]:.7f}  (сейчас w ао={ISS["SBER"][CUR_DATE]["w"]}, ап={ISS["SBERP"][CUR_DATE]["w"]})')
    out(f'  {"тикер":6} {"вес сейчас":>10} {"вес новый":>10} {"Δ п.п.":>8} {"сделка":>16}')
    for t in ['LKOH', 'SBER', 'SBERP', 'TATN', 'TATNP', 'GAZP', 'LENT', 'MSNG', 'PHOR', 'TRNFP', 'POSI', 'BSPB', 'RENI', 'SVCB']:
        out(f'  {t:6} {R["w_cur"][t]:10.3f} {R["w_new"].get(t,0):10.3f} {R["w_new"].get(t,0)-R["w_cur"][t]:+8.3f} {fmt_m(R["trades"][t]):>16}')
    sells = sum(v for v in R['trades'].values() if v < 0)
    buys = sum(v for v in R['trades'].values() if v > 0)
    out(f'  Σ продаж = {sells/1e6:,.1f} млн, Σ покупок = {buys/1e6:,.1f} млн, оборот одной стороны = {buys/NAV*100:.3f}% СЧА')
    top_buys = sorted(R['trades'].items(), key=lambda x: -x[1])[:5]
    top_sells = sorted(R['trades'].items(), key=lambda x: x[1])[:5]
    out('  крупнейшие покупки: ' + ', '.join(f'{t} {v/1e6:+.1f}' for t, v in top_buys))
    out('  крупнейшие продажи: ' + ', '.join(f'{t} {v/1e6:+.1f}' for t, v in top_sells))
    out('  Все 46 бумаг (вес сейчас / новый / сделка, млн):')
    for t in sorted(ISS, key=lambda t: -R['w_cur'][t]):
        out(f'    {t:6} {R["w_cur"][t]:7.3f} {R["w_new"].get(t,0):7.3f} {R["trades"][t]/1e6:+9.1f}')

    # ---------- 4) утверждения поста ----------
    out('\n=== 4. Утверждения поста v24 ===')
    out(f'  LKOH сейчас: опубликовано {ISS["LKOH"][CUR_DATE]["weight"]}%, расчёт {R["w_cur"]["LKOH"]:.3f}%; новый {R["w_new"]["LKOH"]:.3f}%; сделка {fmt_m(R["trades"]["LKOH"])}')
    out(f'  TATN FF {ISS["TATN"][CUR_DATE]["ff"]} -> {FF_NEW["TATN"]}, W {ISS["TATN"][CUR_DATE]["w"]} -> {R["W_new"]["TATN"]}; '
        f'FF*W {ISS["TATN"][CUR_DATE]["ff"]*ISS["TATN"][CUR_DATE]["w"]:.4f} -> {FF_NEW["TATN"]*R["W_new"]["TATN"]:.4f}; сделка {fmt_m(R["trades"]["TATN"])} '
        f'(пакет фондов по весу сейчас {R["w_cur"]["TATN"]/100*NAV/1e9:.2f} млрд)')
    out('  Бумаги IMOEX с новым FF — доступная доля FF*LW (старая -> новая):')
    for t in sorted(FF_NEW):
        if t not in ISS:
            out(f'    {t:6} не в IMOEX')
            continue
        r = ISS[t][CUR_DATE]
        lw_old = r['w'] / (R['ww'][t] if False else 1.0)  # для необрезанных w==LW
        if t == 'LKOH':
            lw_old = 1.0
        elif t in ('SBER', 'SBERP'):
            lw_old = LW_NEW[t]
        old = r['ff'] * lw_old
        new = FF_NEW[t] * LW_NEW.get(t, 1.0)
        flag = 'НЕ прибавилось' if new <= old + 1e-12 else 'прибавилось'
        out(f'    {t:6} FF {r["ff"]:.2f}->{FF_NEW[t]:.2f}  LW {lw_old:.2f}->{LW_NEW.get(t,1.0):.2f}  FF*LW {old:.4f}->{new:.4f}  {flag}'
            f'   | c учётом WW: W {r["w"]:.7f}->{R["W_new"][t]:.7f}, FF*W {r["ff"]*r["w"]:.5f}->{FF_NEW[t]*R["W_new"][t]:.5f}')
    out(f'  GAZP: сделка {fmt_m(R["trades"]["GAZP"])}; крупнейшая покупка = {top_buys[0][0]} {fmt_m(top_buys[0][1])}')
    out(f'  "около миллиарда": Σ покупок {buys/1e6:,.0f} млн / Σ продаж {sells/1e6:,.0f} млн')
    # Мосэнерго vs ЛУКОЙЛ доля фондов
    msng_sh, lkoh_sh = 45_044_000, 1_545_355
    for t, sh in (('MSNG', msng_sh), ('LKOH', lkoh_sh)):
        r = ISS[t][CUR_DATE]
        issue_field = r['issue']
        issue_calc = r['cap'] / PRICES[t][CUR_DATE]
        ff = r['ff']
        out(f'  {t}: штук у фондов {sh:,}; выпуск (issue) {issue_field:,.0f} (cap/цена = {issue_calc:,.0f}); '
            f'доля от выпуска {sh/issue_field*100:.3f}%; от free-float (FF={ff}) {sh/(issue_field*ff)*100:.3f}%')
    # доля в штуках из funds.json позиций (сверка)
    pos = defaultdict(int)
    for p in FUNDS['позиции']:
        pos[p['isin']] += p['штук']
    out(f'  сверка funds.json: LKOH RU0009024277 = {pos.get("RU0009024277",0):,}; MSNG RU0008958863 = {pos.get("RU0008958863",0):,}')

    # ---------- 6) стресс ----------
    out('\n=== 6. Стресс ===')
    variants = [
        ('фиксация 27.08 (база)', dict()),
        ('фиксация 28.08', dict(fix_date='2026-08-28')),
        ('фиксация 31.08', dict(fix_date='2026-08-31')),
        ('фиксация 01.09', dict(fix_date='2026-09-01')),
        ('27.08, уходящие НЕ исключены из знаменателя потолка', dict(exclude_leaving_in_cap=False)),
        ('27.08, cap = issue*close из prices.json', dict(cap_source='price')),
        ('27.08, cap price, уходящие не исключены', dict(cap_source='price', exclude_leaving_in_cap=False)),
    ]
    out(f'  {"вариант":58} {"WW LKOH":>9} {"WW SBER":>9} {"LKOH new":>9} {"LKOH сделка":>12} {"SBER сделка":>12} {"GAZP":>9} {"TATN":>9} {"Σпродаж":>10}')
    for name, kw in variants:
        r = run(verbose=False, **kw)
        s = sum(v for v in r['trades'].values() if v < 0)
        out(f'  {name:58} {r["ww"]["LKOH"]:9.7f} {r["ww"]["SBER"]:9.7f} {r["w_new"]["LKOH"]:9.3f} {r["trades"]["LKOH"]/1e6:+12.1f} {r["trades"]["SBER"]/1e6:+12.1f} {r["trades"]["GAZP"]/1e6:+9.1f} {r["trades"]["TATN"]/1e6:+9.1f} {s/1e6:10.1f}')

    # ---------- 5) валидация июнь-2025 ----------
    out('\n=== 5. Валидация на июне-2025 ===')
    J25 = J('iss_imoex_june2025.json')
    dates = sorted({d for t in J25 for d in J25[t]})
    post = '2025-06-20'
    pre_last = [d for d in dates if d < post][-1]
    stay25 = sorted(t for t in J25 if post in J25[t])
    left25 = sorted(t for t in J25 if pre_last in J25[t] and post not in J25[t])
    out(f'  ушли {left25}; осталось {len(stay25)}; даты {dates[0]}..{dates[-1]}')
    # новые FF/LW берём из пост-ребалансных ISS-данных: ff = ff(post); LW = w(post) для необрезанных;
    # для обрезанных (WW<1): LKOH LW=1, SBER LW: ратио ап/ао = 2 (как в lw_new 0.3/0.6)
    def lw_post(t):
        w = J25[t][post]['w']
        if t == 'LKOH':
            return 1.0
        if t == 'SBER':
            return 0.3
        if t == 'SBERP':
            return 0.6
        return w
    best = None
    for fix in [d for d in dates if d < post]:
        mc = {t: J25[t][fix]['cap'] * J25[t][post]['ff'] * lw_post(t) for t in stay25}
        ww, wfix, log, unc = cap_weights(mc)
        W = {t: round(ww[t] * lw_post(t), 7) for t in stay25}
        # сравнение 1: W против фактического w на post
        dW = {t: W[t] - J25[t][post]['w'] for t in stay25}
        # сравнение 2: веса на post с моими W против опубликованных weight
        mcp = {t: J25[t][post]['cap'] * J25[t][post]['ff'] * W[t] for t in stay25}
        tp = sum(mcp.values())
        dwt = {t: mcp[t] / tp * 100 - J25[t][post]['weight'] for t in stay25}
        mx = max(abs(v) for v in dwt.values())
        out(f'  фиксация {fix}: WW LKOH {ww["LKOH"]:.7f} (факт w {J25["LKOH"][post]["w"]}), WW SBER {ww["SBER"]:.7f} -> W ао {W["SBER"]:.7f} (факт {J25["SBER"][post]["w"]}); '
            f'max|Δвес| на {post} = {mx:.4f} п.п.; топ-5 {log[-1].split(":")[1].strip()}')
        if best is None or mx < best[1]:
            best = (fix, mx)
    out(f'  лучшая дата фиксации: {best[0]}, max|Δвес| = {best[1]:.4f} п.п.')
    # тот же тест: уходящих НЕ исключать из знаменателя при потолке (на 30.05)
    fix = '2025-05-30'
    all25 = sorted(t for t in J25 if fix in J25[t])
    mc = {t: J25[t][fix]['cap'] * (J25[t][post]['ff'] if post in J25[t] else J25[t][fix]['ff']) * (lw_post(t) if post in J25[t] else J25[t][fix]['w']) for t in all25}
    ww, _, log, _ = cap_weights(mc)
    W = {t: round(ww[t] * lw_post(t), 7) for t in stay25}
    mcp = {t: J25[t][post]['cap'] * J25[t][post]['ff'] * W[t] for t in stay25}
    tp = sum(mcp.values())
    mx = max(abs(mcp[t] / tp * 100 - J25[t][post]['weight']) for t in stay25)
    out(f'  контр-тест 30.05 с уходящими в знаменателе: WW LKOH {ww["LKOH"]:.7f}, WW SBER {ww["SBER"]:.7f}, max|Δвес| = {mx:.4f} п.п.')

    # топ-5 эмитентов на 30.05.2025 с моими WW (проверка ограничения 55%)
    fix = '2025-05-30'
    mc = {t: J25[t][fix]['cap'] * J25[t][post]['ff'] * lw_post(t) for t in stay25}
    ww, wfix, log, unc = cap_weights(mc)
    em = defaultdict(float)
    for t, w in wfix.items(): em[emitter(t)] += w
    top = sorted(em.items(), key=lambda x: -x[1])[:5]
    out('  топ-5 ЭМИТЕНТОВ на фиксации 30.05.2025 (после потолка 15%): ' + ', '.join(f'{e} {w*100:.2f}' for e, w in top) + f' = {sum(w for _, w in top)*100:.2f}%')
    top_sec = sorted(wfix.items(), key=lambda x: -x[1])[:5]
    out('  (то же по БУМАГАМ, без объединения ао+ап: ' + ', '.join(f'{t} {w*100:.2f}' for t, w in top_sec) + f' = {sum(w for _, w in top_sec)*100:.2f}%)')
    em_post = defaultdict(float)
    for t in stay25: em_post[emitter(t)] += J25[t][post]['weight']
    out(f'  факт 20.06.2025, топ-5 эмитентов по опубликованным весам: {sum(sorted(em_post.values(), reverse=True)[:5]):.2f}%')

    # ---------- 5b: вторая валидация — июнь-2026 (ушёл PIKK, пришёл RAGR) ----------
    out('\n=== 5b. Валидация на июне-2026 (ушёл PIKK, пришёл RAGR; релиз 04.06 -> фиксация 03.06) ===')
    J26 = J('iss_imoex_june.json')
    if 'бумаги' in J26: J26 = J26['бумаги']
    PIKK = J('iss_pikk_june.json')
    RAGR = J('iss_ragr_bmi_june.json')
    post = '2026-06-19'
    stay26 = sorted(t for t in J26 if post in J26[t])          # 46 incl. RAGR
    def lw26(t):
        return {'LKOH': 1.0, 'SBER': 0.3, 'SBERP': 0.6}.get(t, J26[t][post]['w'])
    def cap26(t, d):
        if t == 'RAGR' and d not in J26['RAGR']:
            return RAGR[d]['cap']
        return J26[t][d]['cap']
    for fix in ['2026-06-02', '2026-06-03', '2026-06-04']:
        for excl in (True, False):
            mc = {t: cap26(t, fix) * J26[t][post]['ff'] * lw26(t) for t in stay26}
            if not excl:
                mc['PIKK'] = PIKK[fix]['cap'] * PIKK[fix]['ff'] * PIKK[fix]['w']
            ww, _, log, _ = cap_weights(mc)
            W = {t: round(ww[t] * lw26(t), 7) for t in stay26}
            mcp = {t: J26[t][post]['cap'] * J26[t][post]['ff'] * W[t] for t in stay26}
            tp = sum(mcp.values())
            mx = max(abs(mcp[t] / tp * 100 - J26[t][post]['weight']) for t in stay26)
            out(f'  фиксация {fix}, PIKK {"исключён" if excl else "в знаменателе"}: WW LKOH {ww["LKOH"]:.7f} (факт {J26["LKOH"][post]["w"]}), '
                f'W SBER {W["SBER"]:.7f} (факт {J26["SBER"][post]["w"]}), max|Δвес| {mx:.4f} п.п.')
    return lines

if __name__ == '__main__':
    main()
