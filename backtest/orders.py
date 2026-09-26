"""Стратегии на Python «заявками» — как strategy.entry / strategy.exit в Pine: объём в контрактах, лимитные и стоп-заявки,
доборы (пирамидинг), выход по средней цене позиции. Включается строкой MODE = "orders" в коде стратегии.

    MODE = "orders"
    PARAMS = {"qty": 1}
    def on_bar(i, b, s, p):                  # s — счёт и заявки, как strategy.* в Pine
        if s.position_size == 0:
            s.entry("L", LONG, p["qty"])                       # по рынку — по открытию следующей свечи
        else:
            s.exit("TP", "L", limit=s.position_avg_price * 1.003, stop=s.position_avg_price * 0.99)

Исполнение — как эмулятор брокера TradingView (calc_on_order_fills=false, без лупы):
- on_bar(i) вызывается на закрытии свечи i; заявка, поставленная на свече i, исполняется начиная со свечи i+1;
- рыночная — по открытию следующей свечи; лимитная — по своей цене, когда путь цены её коснулся, или по открытию,
  если свеча открылась уже за ней (лучше цены заявки); стоп — по своей цене, при гэпе — по открытию (хуже);
- путь внутри свечи: open→high→low→close, если high ближе к open, чем low; иначе open→low→high→close;
- заявка с тем же id заменяет прежнюю; exit(from_entry=X) закрывает сделки входа X и живёт, пока они открыты.

Квартальные фьючерсы: на смене контракта позиция ПЕРЕНОСИТСЯ (ROLL = True по умолчанию), как у живого трейдера:
старый закрыт, новый открыт в ту же минуту, цены входа и заявок сдвинуты на разницу контрактов (календарный спред),
поэтому средняя, сетка и тейк стратегии не ломаются; перенос стоит две комиссии и два полуспреда. Разница контрактов
(контанго) — это и есть плата за удержание квартального фьючерса, как фандинг у вечного. ROLL = False — закрывать.

Счёт считается здесь же, свеча за свечой (а не в account.simulate): объём задаёт стратегия, поэтому слоты не нужны.
Комиссия — тариф прогона; спред (половина круга) — только у рыночных и стоп-заявок, лимитная его не платит.
ГО: 'mr1' / 'snapshot' — вход без свободного ГО не исполняется, капитал < MM × ГО позиции → брокер закрывает всё;
'tv100' — как TradingView v6 по умолчанию: маржа 100 % стоимости позиции, при нехватке — маржин-колл на вчетверо
больший объём (правило TV); 'none' — без проверок. Фандинг вечных фьючерсов (data/funding.csv.gz, ISS SWAPRATE) — с
позиции, открытой к вечернему клирингу: лонг платит положительную своп-разницу, шорт получает.
"""
import math
import numpy as np, pandas as pd
from . import store, costs, account

CLEARING_MIN = 18 * 60 + 50       # вечерний клиринг — с позиции, открытой к нему, списывается фандинг
MM = 0.5                          # минимальная маржа = половина начальной (КСУР); ниже — принудительное закрытие
MAX_TRADES = 200_000
LONG, SHORT = 1, -1
_F = {}


def funding_series(st):
    """Series[дата → своп-разница в пунктах цены] для вечного фьючерса; None — у бумаги фандинга нет."""
    if 'f' not in _F:
        f = store.REF / 'funding.csv.gz'
        _F['f'] = {k: g.set_index('d').swaprate.sort_index() for k, g in pd.read_csv(f, parse_dates=['d']).groupby('st')} \
            if f.exists() else {}
    return _F['f'].get(st)


class Trade:
    """Открытая сделка: вход одной заявки. id — имя заявки входа (как strategy.opentrades.entry_id)."""
    __slots__ = ('id', 'side', 'qty', 'price', 'i', 'comm', 'spread', 'funding', 'go', 'equity_in', 'hi', 'lo', 'rpp')

    def __init__(self, **kw):
        for k, v in kw.items(): setattr(self, k, v)

    def __repr__(self):
        return f"Trade({self.id!r}, {'LONG' if self.side > 0 else 'SHORT'} {self.qty} @ {self.price})"


class Strategy:
    """То, что видит on_bar(i, b, s, p). Названия — как у strategy.* в Pine."""

    def __init__(self, eng):
        self._e = eng

    # — состояние
    @property
    def position_size(self):
        """Позиция в контрактах: + лонг, − шорт, 0 — нет."""
        return sum(t.side * t.qty for t in self._e.open)

    @property
    def position_avg_price(self):
        q = sum(t.qty for t in self._e.open)
        return sum(t.qty * t.price for t in self._e.open) / q if q else float('nan')

    @property
    def opentrades(self):
        return len(self._e.open)

    @property
    def open_trades(self):
        """Открытые сделки от старой к новой: .id .side .qty .price .i"""
        return list(self._e.open)

    @property
    def closedtrades(self):
        return len(self._e.closed)

    @property
    def equity(self):
        """Капитал с незакрытым результатом по закрытию текущей свечи, ₽."""
        return self._e.equity_at(self._e.C[self._e.i])

    @property
    def netprofit(self):
        return self._e.realized

    @property
    def point_value(self):
        """₽ за 1.0 цены на один контракт сегодня: стоимость контракта = цена × point_value."""
        return self._e.rpp

    # — заявки
    def entry(self, id, direction, qty, limit=None, stop=None):
        """Вход (или добор) direction = LONG | SHORT на qty контрактов. Без limit/stop — по рынку.
        Позиция в другую сторону закрывается этой же заявкой (разворот), как в Pine."""
        q = int(qty)
        if q < 1: raise ValueError(f'entry("{id}"): объём {qty} — нужно целое число контрактов ≥ 1')
        if direction not in (LONG, SHORT): raise ValueError(f'entry("{id}"): направление LONG или SHORT')
        self._e.orders[('E', str(id))] = {'kind': 'E', 'id': str(id), 'dir': int(direction), 'qty': q,
                                          'limit': self._e.rt(limit), 'stop': self._e.rt(stop), 'seq': self._e.seq()}

    def exit(self, id, from_entry=None, limit=None, stop=None):
        """Выход сделок входа from_entry (None — всей позиции) по лимиту и/или стопу — что сработает первым."""
        if limit is None and stop is None: raise ValueError(f'exit("{id}"): нужен limit или stop (по рынку — close)')
        self._e.orders[('X', str(id))] = {'kind': 'X', 'id': str(id), 'from': None if from_entry is None else str(from_entry),
                                          'limit': self._e.rt(limit), 'stop': self._e.rt(stop), 'seq': self._e.seq()}

    def close(self, from_entry):
        """Закрыть сделки входа from_entry по рынку — по открытию следующей свечи."""
        self._e.orders[('C', str(from_entry))] = {'kind': 'C', 'id': str(from_entry), 'from': str(from_entry), 'seq': self._e.seq()}

    def close_all(self):
        self._e.orders[('C', None)] = {'kind': 'C', 'id': None, 'from': None, 'seq': self._e.seq()}

    def cancel(self, id):
        for k in [k for k in self._e.orders if k[1] == str(id) and k[0] in ('E', 'X')]: del self._e.orders[k]

    def cancel_all(self):
        self._e.orders.clear()


class Engine:
    def __init__(self, b, st, acct):
        self.b, self.st = b, st
        self.O, self.H, self.L, self.C = (np.array(x, float) for x in (b.open, b.high, b.low, b.close))   # копии: перенос правит C
        self.D, self.M = np.asarray(b.date), np.asarray(b.minute)
        a = acct or {}
        self.capital = float(a.get('capital') or 1_000_000)
        self.tariff, self.spread_mode = a.get('tariff', 'trader'), a.get('spread', 'c3')
        self.go_mode, self.go_mult = a.get('go', 'mr1'), float(a.get('go_mult') or 1.0)
        self.pyramiding = int(a.get('pyramiding') or 1000)
        self.tick = float(store_step(st))
        self.orders, self.open, self.closed, self.skips = {}, [], [], {}
        self.realized, self.funding_paid, self.margin_calls, self.liquidated = 0.0, 0.0, 0, 0
        self.i, self._seq, self.day = 0, 0, None
        self.fund = funding_series(st)
        self.eq_rows, self._mc_day, self.rolls = [], 0, 0

    def seq(self):
        self._seq += 1
        return self._seq

    def rt(self, x):
        """Цена заявки, округлённая до шага цены (как в TradingView)."""
        if x is None: return None
        x = float(x)
        if not math.isfinite(x) or x <= 0: raise ValueError(f'цена заявки {x}')
        return round(round(x / self.tick) * self.tick, 10) if self.tick > 0 else x

    # — счёт
    def new_day(self, d):
        self.day = d
        self.rpp = account.rub_per_point(self.st, d)
        self.go_k = {s: (account.go_per_contract(self.st, d, 1.0, s, self.go_mode) * self.go_mult if self.go_mode in ('mr1', 'tv100') else 0.0)
                     for s in (1, -1)}
        self.go_c = {s: (account.go_per_contract(self.st, d, 1.0, s, 'snapshot') * self.go_mult if self.go_mode == 'snapshot' else 0.0)
                     for s in (1, -1)}
        for s in (1, -1):
            if self.go_k[s] != self.go_k[s]: self.go_k[s] = 0.0      # нет ставки на дату — не проверять
        self.half_spread = costs.spread_on(self.st, d, d, self.spread_mode) / 2 if self.spread_mode != 'none' else 0.0

    def go1(self, px, side):
        """ГО одного контракта при цене px."""
        return self.go_k[side] * px + self.go_c[side]

    def go_used(self, px):
        return sum(self.go1(px, t.side) * t.qty for t in self.open)

    def equity_at(self, px):
        return self.capital + self.realized + sum(t.side * (px - t.price) * t.qty * self.rpp for t in self.open)

    def comm(self, px, q):
        cv = px * self.rpp
        return costs.commission_side(self.tariff, self.day, self.st, cv) * q * cv

    def skip(self, reason):
        self.skips[reason] = self.skips.get(reason, 0) + 1

    # — исполнение
    def fill_entry(self, od, px, market):
        side = od['dir']
        if self.open and self.open[0].side != side:                       # разворот
            self.close_trades(list(self.open), px, 'разворот', market)
        elif len(self.open) >= self.pyramiding:
            return self.skip('пирамидинг')
        q = od['qty']
        if self.go_mode != 'none':
            if self.go_mode == 'tv100':
                need, used = q * px * self.rpp, sum(t.qty for t in self.open) * px * self.rpp
            else:
                need, used = self.go1(px, side) * q, self.go_used(px)
            if need > self.equity_at(px) - used:
                return self.skip('не хватает ГО')                       # как у брокера и TradingView: заявка снимается
        cm = self.comm(px, q)
        sp = (self.half_spread if market else 0.0) * q * px * self.rpp
        self.realized -= cm + sp
        self.open.append(Trade(id=od['id'], side=side, qty=q, price=px, i=self.i, comm=cm, spread=sp, funding=0.0,
                               go=self.go1(px, side) * q if self.go_mode != 'tv100' else q * px * self.rpp,
                               equity_in=self.equity_at(px), hi=px, lo=px, rpp=self.rpp))

    def close_trades(self, trades, px, reason, market, qty=None):
        """Закрыть сделки (qty — не больше стольких контрактов, по порядку: частичное закрытие при маржин-колле)."""
        left = qty
        for t in trades:
            if left is not None and left <= 0: break
            q = t.qty if left is None else min(left, t.qty)
            if left is not None: left -= q
            f = q / t.qty
            cm = self.comm(px, q); sp = (self.half_spread if market else 0.0) * q * px * self.rpp
            gross = t.side * (px - t.price) * q * self.rpp
            self.realized += gross - cm - sp
            self.closed.append({'side': t.side, 'qty': q, 'px_in': t.price, 'i_in': t.i, 'px_out': px, 'i_out': self.i,
                                'exit_reason': reason, 'entry_id': t.id, 'comm_rub': t.comm * f + cm, 'spread_rub': t.spread * f + sp,
                                'funding_rub': t.funding * f, 'notional': q * t.price * t.rpp, 'go': t.go * f, 'equity_in': t.equity_in,
                                'gross_rub': gross, 'hi': t.hi, 'lo': t.lo,
                                'pnl_rub': gross - t.comm * f - cm - t.spread * f - sp - t.funding * f})
            if q < t.qty:
                t.qty -= q; t.comm -= t.comm * f; t.spread -= t.spread * f; t.funding -= t.funding * f; t.go -= t.go * f
            else:
                self.open.remove(t)
        if len(self.closed) > MAX_TRADES:
            raise ValueError(f'больше {MAX_TRADES} сделок — стратегия торгует на каждой свече?')
        self.drop_dead_exits()

    def drop_dead_exits(self):
        """Выходы, которым больше нечего закрывать: сделок их входа нет и заявка на этот вход не стоит."""
        ids = {t.id for t in self.open}
        waiting = {od['id'] for od in self.orders.values() if od['kind'] == 'E'}
        for k in [k for k, od in self.orders.items() if od['kind'] in ('X', 'C') and
                  ((od['from'] is None and not self.open and not waiting) or
                   (od['from'] is not None and od['from'] not in ids and od['from'] not in waiting))]:
            del self.orders[k]

    def order_side(self, od):
        """+1 — заявка на покупку, −1 — на продажу, 0 — неактивна (выход без открытых сделок своего входа)."""
        if od['kind'] == 'E': return od['dir']
        tr = [t for t in self.open if od['from'] is None or t.id == od['from']]
        return -tr[0].side if tr else 0

    def exec_order(self, key, px, market):
        od = self.orders.pop(key)
        if od['kind'] == 'E':
            self.fill_entry(od, px, market)
        else:
            tr = [t for t in self.open if od['from'] is None or t.id == od['from']]
            if tr: self.close_trades(tr, px, od['id'] if od['kind'] == 'X' else 'по рынку', market)

    def at_open(self, o):
        for key in sorted([k for k, od in self.orders.items() if od['kind'] == 'C' or
                           (od['kind'] == 'E' and od['limit'] is None and od['stop'] is None)], key=lambda k: self.orders[k]['seq']):
            if key in self.orders: self.exec_order(key, o, True)
        while True:                                                     # гэп: заявка уже «за» ценой открытия
            hit = None
            for key, od in sorted(self.orders.items(), key=lambda kv: kv[1]['seq']):
                s = self.order_side(od)
                if not s: continue
                lim, stp = od.get('limit'), od.get('stop')
                if lim is not None and ((s > 0 and o <= lim) or (s < 0 and o >= lim)): hit = (key, False); break
                if stp is not None and ((s > 0 and o >= stp) or (s < 0 and o <= stp)): hit = (key, True); break
            if hit is None: return
            self.exec_order(hit[0], o, hit[1])

    def segment(self, a, b):
        """Цена идёт от a к b: исполняем заявки в том порядке, в каком цена до них доходит."""
        if a == b: return
        up = b > a
        while True:
            best = None
            for key, od in self.orders.items():
                s = self.order_side(od)
                if not s: continue
                for x, is_stop in ((od.get('limit'), False), (od.get('stop'), True)):
                    if x is None: continue
                    # вверх: продажа по лимиту, покупка по стопу; вниз: покупка по лимиту, продажа по стопу
                    ok = (a < x <= b) and ((s < 0) != is_stop) if up else (b <= x < a) and ((s > 0) != is_stop)
                    if ok and (best is None or (x < best[0] if up else x > best[0]) or (x == best[0] and od['seq'] < best[3])):
                        best = (x, key, is_stop, od['seq'])
            if best is None: return
            self.exec_order(best[1], best[0], best[2])
            a = best[0]

    def margin_check(self, px):
        if not self.open or self.go_mode == 'none': return
        eq = self.equity_at(px)
        if self.go_mode == 'tv100':
            used = sum(t.qty for t in self.open) * px * self.rpp
            if eq > used: return
            need = 4 * max(1, math.ceil((used - eq) / (px * self.rpp)))     # правило TradingView: вчетверо больше нехватки
            self.margin_calls += 1; self._mc_day = 1
            self.close_trades(list(self.open), px, 'маржин-колл', True, qty=need)
        else:
            if eq >= MM * self.go_used(px): return
            self.liquidated += 1; self._mc_day = 1
            self.close_trades(list(self.open), px, 'принудительное закрытие', True)
            self.orders.clear()

    def roll(self, gap):
        """Смена контракта: позиция переезжает в новый (цены входа, экстремумы и заявки — на разницу контрактов gap)."""
        old = self.C[self.i]; new = old + gap
        for t in self.open:
            cm = self.comm(old, t.qty) + self.comm(new, t.qty)
            sp = self.half_spread * t.qty * (old + new) * self.rpp
            self.realized -= cm + sp; t.comm += cm; t.spread += sp
            t.price += gap; t.hi += gap; t.lo += gap
        for od in self.orders.values():
            for k in ('limit', 'stop'):
                if od.get(k) is not None: od[k] = self.rt(od[k] + gap)
        self.C[self.i] = new                  # до конца свечи позиция оценивается уже по цене нового контракта
        self.rolls += 1

    def charge_funding(self, d):
        if self.fund is None or not self.open: return
        x = self.fund.get(pd.Timestamp(d))
        if x is None or x != x: return
        for t in self.open:
            pay = t.side * t.qty * float(x) * self.rpp
            t.funding += pay; self.realized -= pay; self.funding_paid += pay

    def bar(self, i):
        self.i = i
        o, h, l, c = self.O[i], self.H[i], self.L[i], self.C[i]
        self.at_open(o)
        path = (o, h, l, c) if (h - o) < (o - l) else (o, l, h, c)
        self.margin_check(o)
        for a, b in zip(path[:-1], path[1:]):
            self.segment(a, b)
            self.margin_check(b)
        for t in self.open:
            if t.i < i: t.hi = max(t.hi, h); t.lo = min(t.lo, l)


def run(ns, b, p, st, acct=None, guard=None, rolls=None, call_init=True):
    """Прогон стратегии заявками по одной бумаге → (сделки, дневная кривая капитала, сводка счёта).
    rolls — {индекс последней свечи контракта: цена нового − цена старого в ту же минуту} (pyengine.roll_gaps).
    call_init=False — init() уже вызван (проверка на заглядывание сама считает индикаторы и охраняет массивы)."""
    if call_init and callable(ns.get('init')): ns['init'](b, p)
    eng = Engine(b, st, {**(acct or {}), 'pyramiding': ns.get('PYRAMIDING')})
    do_roll = bool(ns.get('ROLL', True)) and rolls is not None
    S = Strategy(eng)
    on_bar, n = ns['on_bar'], len(b)
    last = np.asarray(b.last_of_contract)
    charged, mc_days = None, 0
    for i in range(n):
        d = eng.D[i]
        if d != eng.day:
            if eng.day is not None and charged != eng.day:           # в прошлый день клиринга не было в данных — списать сейчас
                eng.charge_funding(eng.day)
            eng.new_day(d)
        if eng.M[i] >= CLEARING_MIN and charged != d:                 # позиция дожила до вечернего клиринга
            eng.charge_funding(d); charged = d
        eng.bar(i)
        if last[i] and do_roll and i < n - 1 and i in rolls:        # смена контракта: позиция и заявки переезжают
            eng.roll(rolls[i])
            if guard is None or guard(i):
                on_bar(i, b, S, p)
        elif last[i]:                                                 # конец данных (или ROLL = False): закрыть, снять
            if eng.open: eng.close_trades(list(eng.open), eng.C[i], 'смена контракта' if i < n - 1 else 'конец данных', True)
            eng.orders.clear()
        elif guard is None or guard(i):
            on_bar(i, b, S, p)
        if i == n - 1 or eng.D[i + 1] != d:
            px = eng.C[i]
            eng.eq_rows.append({'d': str(d)[:10], 'equity': eng.equity_at(px), 'positions': len(eng.open),
                                'notional': sum(t.qty for t in eng.open) * px * eng.rpp, 'go_used': eng.go_used(px),
                                'margin_call': eng._mc_day})
            mc_days += eng._mc_day; eng._mc_day = 0
    rows = []
    for r in eng.closed:
        a, z = r['i_in'], r['i_out']
        sd, pi = r['side'], r['px_in']
        up, dn = r['hi'] / pi - 1, r['lo'] / pi - 1
        rows.append({'st': st, 'secid': str(b.secid[a]), 'side': sd, 'd': str(eng.D[a])[:10], 'm_in': int(eng.M[a]), 'px_in': pi,
                     'd_out': str(eng.D[z])[:10], 'm_out': int(eng.M[z]), 'px_out': r['px_out'], 'exit_reason': r['exit_reason'],
                     'mfe': max(0.0, up if sd > 0 else -dn), 'mae': min(0.0, dn if sd > 0 else -up),
                     'qty': r['qty'], 'notional': r['notional'], 'go': r['go'], 'equity_in': r['equity_in'],
                     'comm_rub': r['comm_rub'], 'spread_rub': r['spread_rub'], 'funding_rub': r['funding_rub'], 'pnl_rub': r['pnl_rub'],
                     'entry_id': r['entry_id']})
    summary = {'фандинг_руб': round(eng.funding_paid), 'принудительных_закрытий': eng.liquidated, 'переносов': eng.rolls,
               'маржин_коллов_TV': eng.margin_calls, 'пропущено': eng.skips, 'капитал': eng.capital}
    return rows, eng.eq_rows, summary


def is_orders_code(code):
    """MODE = "orders" в коде — через ast, БЕЗ исполнения (так можно в API и воркере)."""
    import ast
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return False
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'MODE' for t in node.targets):
            try:
                return str(ast.literal_eval(node.value)).lower() == 'orders'
            except Exception:
                return False
    return False


def store_step(st):
    try:
        return account.specs()[st]['price_step']
    except Exception:
        return 0.0


DCA_TEMPLATE = '''# DCA-мартингейл «как у копитрейдеров» — перенос Pine один в один (strategy.entry / strategy.exit).
# Заход по рынку, доборы лимитками: каждая ступень на step_pct % дальше и в mult раз больше, тейк от средней цены.
# Стопа нет. exit_all = 0 — выходы по входам, как в Pine (ловушка: если тейк и следующий добор исполнились в одной
# свече, новая сделка остаётся без тейка); exit_all = 1 — один тейк на всю позицию, как у настоящего DCA-бота.
MODE = "orders"                     # стратегия заявками: объём в контрактах, лимитки, доборы
PYRAMIDING = 20

PARAMS = {"side": 1, "base": 30, "levels": 5, "step_pct": 1.0, "mult": 2.0, "tp_pct": 0.3, "exit_all": 0}


def on_bar(i, b, s, p):
    side = LONG if p["side"] > 0 else SHORT
    if s.position_size == 0:        # позиции нет — снять старые заявки и зайти базовым объёмом по рынку
        s.cancel_all()
        s.entry("L0", side, p["base"])
        return
    base = s.open_trades[0].price   # цена первой открытой сделки (strategy.opentrades.entry_price(0))
    n = s.opentrades
    for k in range(n, int(p["levels"])):
        q = max(1, int(p["base"] * p["mult"] ** k + 0.5))
        s.entry(f"L{k}", side, q, limit=base * (1 - side * p["step_pct"] / 100 * k))
    tp = s.position_avg_price * (1 + side * p["tp_pct"] / 100)
    if p["exit_all"]:
        s.exit("TP", limit=tp)
    else:
        for k in range(n):
            s.exit(f"TP{k}", f"L{k}", limit=tp)
'''


DCA_VOL_TEMPLATE = '''# DCA-сетка под волатильность бумаги — чтобы сравнивать разные бумаги одной меркой.
# Шаг сетки и тейк — в долях дневной волатильности за прошлые 20 дней (у юаня 1.5 × 0.67 % ≈ 1 % и 0.45 × ≈ 0.3 %,
# как в Pine). Объём — от капитала: вся сетка (1 + 2 + 4 + 8 + 16 = 31 часть) = капитал × lev. Один тейк на всю
# позицию, стопа нет. side = 1 — усреднение покупками, -1 — продажами (шорт). Квартальные фьючерсы переносятся.
MODE = "orders"
PYRAMIDING = 20
PARAMS = {"side": 1, "levels": 5, "mult": 2.0, "step_vol": 1.5, "tp_vol": 0.45, "lev": 1.0, "vol_days": 20}
ST = {}


def init(b, p):
    last = np.r_[b.new_day[1:], True]                        # последняя свеча каждого дня
    dc, ds = b.close[last], b.secid[last]
    r = np.r_[np.nan, np.diff(np.log(dc))]
    r[1:][ds[1:] != ds[:-1]] = np.nan                         # через смену контракта доходность не считаем
    vol = pd.Series(r).rolling(int(p["vol_days"]), min_periods=10).std().values
    day = np.cumsum(b.new_day) - 1                            # номер дня у каждой свечи
    b.vol = np.r_[np.nan, vol[:-1]][day]                      # внутри дня — волатильность только по прошлым дням


def on_bar(i, b, s, p):
    side = LONG if p["side"] > 0 else SHORT
    if s.position_size == 0:
        s.cancel_all()
        v = b.vol[i]
        if not v > 0:
            return
        units = sum(p["mult"] ** k for k in range(int(p["levels"])))
        base = int(s.equity * p["lev"] / (units * b.close[i] * s.point_value))
        if base < 1:
            return
        ST.update(base=base, step=p["step_vol"] * v, tp=p["tp_vol"] * v)
        s.entry("L0", side, base)
        return
    first = s.open_trades[0].price
    for k in range(s.opentrades, int(p["levels"])):
        s.entry(f"L{k}", side, max(1, int(ST["base"] * p["mult"] ** k + 0.5)), limit=first * (1 - side * ST["step"] * k))
    s.exit("TP", limit=s.position_avg_price * (1 + side * ST["tp"]))
'''


_REGIME_INIT = '''
def init(b, p):
    """Режим и волатильность по ДНЕВНЫМ закрытиям непрерывного ряда (close − adj: без скачков на смене контракта).
    Всё считается по прошлым дням и действует со следующего дня (недельный режим — со следующей недели)."""
    last = np.r_[b.new_day[1:], True]                        # последняя свеча каждого дня
    dc = (b.close - b.adj)[last]
    dd = pd.to_datetime(b.date[last])
    r = np.r_[np.nan, np.diff(np.log(b.close[last]))]
    sec = b.secid[last]
    r[1:][sec[1:] != sec[:-1]] = np.nan                       # доходность через смену контракта не считаем
    vol = pd.Series(r).rolling(int(p["vol_days"]), min_periods=10).std()
    calm = (vol <= vol.rolling(250, min_periods=120).median()).astype(float).values
    n = int(p["ema"])
    if p["ema_tf"] == "W":                                    # EMA по недельным закрытиям
        wk = pd.Series(dc, index=dd).groupby(dd.to_period("W")).last()
        reg = np.sign(wk - wk.ewm(span=n, adjust=False, min_periods=n).mean()).shift(1)
        reg = reg.reindex(dd.to_period("W")).values
    else:                                                     # EMA по дневным закрытиям
        s = pd.Series(dc)
        reg = np.sign(s - s.ewm(span=n, adjust=False, min_periods=n).mean()).shift(1).values
    day = np.cumsum(b.new_day) - 1
    b.vol = np.r_[np.nan, vol.values[:-1]][day]
    b.calm = np.r_[np.nan, calm[:-1]][day]
    b.regime = np.nan_to_num(np.asarray(reg, float))[day]


def _want(i, b, p):
    """Куда можно торговать сегодня: +1 / −1 / 0."""
    if p["trade_from"] and str(b.date[i]) < p["trade_from"]:
        return 0
    reg = int(b.regime[i])
    return reg if p["sides"] == "both" else max(reg, 0)
'''

DCA_REGIME_TEMPLATE = '''# DCA-сетка по режиму рынка: сетка работает только по тренду. Режим — цена выше / ниже EMA по дневным (ema_tf = "D")
# или недельным ("W") закрытиям. sides = "both": выше EMA — сетка покупками, ниже — продажами; "long" — ниже не торгуем.
# on_flip = "close": режим сменился — открытая сетка закрывается по рынку; "wait": дожидается своего тейка.
# calm = 1: новые сетки только в спокойном рынке (волатильность за 20 дней не выше медианы за год).
# Шаг и тейк — в долях дневной волатильности, вся сетка (31 часть) = капитал × lev. trade_from — начать торговать с даты.
MODE = "orders"
PYRAMIDING = 20
PARAMS = {"ema": 200, "ema_tf": "D", "sides": "both", "on_flip": "close", "calm": 0,
          "levels": 5, "mult": 2.0, "step_vol": 1.5, "tp_vol": 0.45, "lev": 1.0, "vol_days": 20, "trade_from": ""}
ST = {}
''' + _REGIME_INIT + '''

def on_bar(i, b, s, p):
    want = _want(i, b, p)
    pos = s.position_size
    if pos and (1 if pos > 0 else -1) != want and p["on_flip"] == "close":
        s.cancel_all()
        s.close_all()
        return
    if pos == 0:
        s.cancel_all()
        v = b.vol[i]
        if want == 0 or not v > 0 or (p["calm"] and b.calm[i] != 1):
            return
        units = sum(p["mult"] ** k for k in range(int(p["levels"])))
        base = int(s.equity * p["lev"] / (units * b.close[i] * s.point_value))
        if base < 1:
            return
        ST.update(side=want, base=base, step=p["step_vol"] * v, tp=p["tp_vol"] * v)
        s.entry("L0", LONG if want > 0 else SHORT, base)
        return
    side = ST["side"]
    first = s.open_trades[0].price
    for k in range(s.opentrades, int(p["levels"])):
        s.entry(f"L{k}", LONG if side > 0 else SHORT, max(1, int(ST["base"] * p["mult"] ** k + 0.5)),
                limit=first * (1 - side * ST["step"] * k))
    s.exit("TP", limit=s.position_avg_price * (1 + side * ST["tp"]))
'''

TREND_TEMPLATE = '''# Позиция по режиму рынка БЕЗ сетки — «купить и вовремя продать»: выше EMA держим лонг на весь капитал × lev,
# ниже — шорт (sides = "both") или ничего (sides = "long"). Меняется только при смене режима. Для сравнения с сеткой.
MODE = "orders"
PARAMS = {"ema": 200, "ema_tf": "D", "sides": "both", "lev": 1.0, "vol_days": 20, "trade_from": ""}
''' + _REGIME_INIT + '''

def on_bar(i, b, s, p):
    want = _want(i, b, p)
    pos = s.position_size
    if (pos > 0) - (pos < 0) == want:
        return
    if pos:
        s.close_all()
        return
    q = int(s.equity * p["lev"] / (b.close[i] * s.point_value))
    if want and q >= 1:
        s.entry("T", LONG if want > 0 else SHORT, q)
'''
