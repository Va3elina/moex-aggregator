"""Декларативные правила «сессионного» семейства: ход за окно дня → вход → выход на следующий торговый день.

{
 "id": "hybrid7c", "name": "...",
 "signal": {"from": "10:30", "to": "17:00"},
 "long":  {"type": "quantile", "q": 0.67, "window": 250, "min_obs": 100},      # рост ≥ квантиля прошлых ростов
 "short": {"type": "fixed", "value": 0.02},                                     # падение ≥ 2 %
 "filters": [{"type": "straightness", "points": ["10:00", ...], "min": 0.58}],  # min — число или {тип: число}
 "entry": {"price": "close" | "next_open", "delay_min": 0},
 "exit":  {"at": "11:00", "price": "close" | "next_open", "delay_min": 0,
           "hold_days": 1,            # выход на N-й следующий торговый день (по одной позиции на бумагу)
           "stop": 0.02, "take": 0.04, "trail": 0.015}   # досрочный выход; доли от цены входа; любой можно опустить
}
Стоп/тейк/трейлинг проверяются по закрытию каждой 5-минутной свечи между входом и плановым выходом (вечерняя и
утренняя сессии, будни), выход — по открытию следующей свечи. Трейлинг — откат от лучшего закрытия в нашу сторону.
Сторона: {"type":"fixed","value":v,"strict":bool} — ход > v (strict) или ≥ v; value — число или {тип фьючерса: число}.
null — сторона выключена. Пороги квантиля считаются ТОЛЬКО по прошлым дням ряда.
price: close — закрытие свечи T (как в замороженных спецификациях); next_open — открытие первой свечи ≥ T+5 мин
(рыночная заявка в OsEngine / TradingView). delay_min сдвигает T (робот входит ~17:15 → delay 10 + next_open).
"""
import copy

Q_HYBRID = {"type": "quantile", "window": 250, "min_obs": 100}
UNIVERSE_21 = ['AF', 'AK', 'BR', 'CC', 'Eu', 'GK', 'GZ', 'LK', 'MN', 'MX', 'NM', 'PI', 'PT', 'RI', 'SN', 'SR', 'SS',
               'SZ', 'Si', 'TT', 'VB']
_V2_MOVE = {'Si': 0.00902, 'CR': 0.00862, 'BR': 0.01319, 'PT': 0.01272, 'CC': 0.02891, 'SS': 0.01939, 'SZ': 0.02040}
_V2_STRAIGHT = {'Si': 0.627, 'CR': 0.625, 'BR': 0.556, 'PT': 0.558, 'CC': 0.599, 'SS': 0.578, 'SZ': 0.586}

PRESETS = {
    'hybrid7c': {
        "id": "hybrid7c", "name": "Гибрид (spec7c, заморожен 08.09.2026)", "frozen": True,
        "universe": UNIVERSE_21,
        "signal": {"from": "10:30", "to": "17:00"},
        "long": {**Q_HYBRID, "q": 0.67}, "short": {**Q_HYBRID, "q": 0.90}, "filters": [],
        "entry": {"price": "close", "delay_min": 0}, "exit": {"at": "11:00", "price": "close", "delay_min": 0}},
    'algozavr': {
        "id": "algozavr", "name": "Алгозавр: лонг на любой рост, шорт при падении ≥ 2 %",
        "universe": UNIVERSE_21,
        "signal": {"from": "10:30", "to": "17:00"},
        "long": {"type": "fixed", "value": 0.0, "strict": True}, "short": {"type": "fixed", "value": 0.02},
        "filters": [],
        "entry": {"price": "close", "delay_min": 0}, "exit": {"at": "11:00", "price": "close", "delay_min": 0}},
    'straight_v2': {
        "id": "straight_v2", "name": "Прямой ход v2 (spec7b): ход 10:00→18:30 по прямой, выход 10:00",
        "frozen": True, "universe": list(_V2_MOVE),
        "signal": {"from": "10:00", "to": "18:30"},
        "long": {"type": "fixed", "value": _V2_MOVE}, "short": {"type": "fixed", "value": _V2_MOVE},
        "filters": [{"type": "straightness", "min": _V2_STRAIGHT, "max": 1.01,
                     "points": ["10:00", "11:30", "13:00", "14:30", "16:00", "17:15", "18:30"]}],
        "entry": {"price": "close", "delay_min": 0}, "exit": {"at": "10:00", "price": "close", "delay_min": 0}},
}
# как исполняют рыночную заявку OsEngine и TradingView: открытие следующей свечи
EXEC_NEXT_OPEN = {"entry": {"price": "next_open", "delay_min": 0}, "exit": {"price": "next_open", "delay_min": 0}}
# как исполняет робот на сервере: крон :00/:05 после 17:12 и 11:06 → ~17:15 и ~11:10
EXEC_ROBOT = {"entry": {"price": "next_open", "delay_min": 10}, "exit": {"price": "next_open", "delay_min": 5}}


def load(x):
    """Имя пресета | словарь → проверенное правило. Путь к файлу сюда НЕ принимается намеренно: правило приходит
    и из API, читать по нему произвольные файлы сервера нельзя (файл читает сам cli)."""
    if isinstance(x, str):
        if x not in PRESETS: raise ValueError(f'нет пресета «{x}»; есть: {", ".join(PRESETS)}')
        r = copy.deepcopy(PRESETS[x])
    else:
        r = copy.deepcopy(x)
    r.setdefault('filters', []); r.setdefault('universe', UNIVERSE_21)
    r.setdefault('entry', {}); r['entry'].setdefault('price', 'close'); r['entry'].setdefault('delay_min', 0)
    r['exit'].setdefault('price', 'close'); r['exit'].setdefault('delay_min', 0)
    for side in ('long', 'short'):
        s = r.get(side)
        if s is None: continue
        if s['type'] == 'quantile':
            assert 0 < s['q'] < 1; s.setdefault('window', 250); s.setdefault('min_obs', 100)
        elif s['type'] == 'fixed':
            s.setdefault('strict', False)
        else:
            raise ValueError(f"{side}.type = {s['type']}")
    for k in ('stop', 'take', 'trail'):
        v = r['exit'].get(k)
        if v is not None and not (0 < float(v) < 1): raise ValueError(f'exit.{k}: доля от цены входа, например 0.02 (= 2 %)')
    if not 1 <= int(r['exit'].get('hold_days') or 1) <= 20: raise ValueError('exit.hold_days: от 1 до 20')
    for p in (r['entry']['price'], r['exit']['price']):
        assert p in ('close', 'next_open'), p
    return r


def with_exec(rule, ex):
    r = load(rule)
    r['entry'].update(ex.get('entry', {})); r['exit'].update(ex.get('exit', {}))
    return r
