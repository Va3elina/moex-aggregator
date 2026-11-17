"""Робот скринера VB (объёмный пробой лонг + шорт, акции TQBR) в песочнице Т-Инвестиций — следует модели один в один.

Плечо — VB_LEVERAGE (по умолчанию ×3, с 16.11.2026): вход, если хватает начальной маржи по ставкам риска Т-Банка на сегодня
(S.account_margin, тот же код, что в Стенде); VB_GUARD=0.15 — защита «−15 % от пика → новые сделки ×1» (выкл.).
Решений сам не принимает: каждые 5 минут пересчитывает стратегию тем же кодом, что прогон Стенда
(backtest/screener_vb.py: данные из БД, сегодняшняя дневная свеча — та, что успела обновиться), и приводит счёт
к модели: модель открыла сделку — входим рыночной заявкой, модель её закрыла — выходим. Всю историю модель
считает первый раз за день и в последние 10 минут; между ними — только бумаги, чьи сделки может изменить
сегодняшняя свеча, остальное из утреннего кэша (см. «кэш модели на день», сверка robots/vb_light_check.py). Счёт 10 млн, идея = 1 млн
(тренд-сделки одного дня делят идею).

Когда входим (МСК): шорт-скальп — по открытию дня T+1 (первый запуск после 07:00); лонг-скальп и тренд-ноги — по
закрытию T+1, то есть в последние 10 минут сессии (до 23:50 в будни, до 19:00 в выходной), когда объём дня почти полный.
Выходы по стопам — в течение дня, как только сегодняшняя свеча пробила уровень модели; по времени, режиму IMOEX и
до отсечки — в последние 10 минут. Перед шортом — флаг Т-Банка «шорт разрешён»; запрещён — сделка пропускается
(в журнале). Песочница плечо не ограничивает (проверено 16.11.2026), поэтому «без плеча» держит модель.

На сервере (cron root, сервер в UTC, окна МСК робот проверяет сам):
    */5 3-20 * * * cd /opt/frame && BT_DATA=/var/lib/docker/volumes/frame_bt_data/_data VB_STATE=/opt/vb-bot/state \\
        flock -n /opt/vb-bot/state/run.lock /opt/hybrid-bot/.venv/bin/python -m robots.vb_bot run >> /opt/vb-bot/state/run.log 2>&1
Команды: setup (открыть счёт песочницы и зачислить 10 млн) · run · status · report (сделки робота против модели).
"""
import argparse, csv, datetime as dt, json, math, os, pathlib, ssl, sys, time, urllib.error, urllib.request, uuid
from zoneinfo import ZoneInfo

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backtest import screener_vb as S  # noqa: E402

MSK = ZoneInfo('Europe/Moscow')
STATE = pathlib.Path(os.environ.get('VB_STATE', ROOT / 'bt_data' / 'vb_bot'))
CAPITAL, SLOTS = 10_000_000, 10
SIZE = CAPITAL / SLOTS
LEVERAGE = float(os.environ.get('VB_LEVERAGE', '3'))         # плечо: идея = 1 млн × плечо, вход при достатке маржи
GUARD = float(os.environ.get('VB_GUARD', '0')) or None        # 0.15 = после −15 % от пика новые сделки ×1 (выкл. по умолчанию)
ACCOUNT_NAME = 'vb-screener-x1'
BASE = 'https://sandbox-invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.'
JCOLS = ['ts', 'action', 'key', 'ticker', 'leg', 'side', 'lots', 'qty', 'price', 'model_price', 'model_reason', 'note']


# ── T-Invest REST (песочница) ──
def _ctx():
    import certifi
    c = ssl.create_default_context(cafile=certifi.where())
    for f in ('russian_trusted_root_ca.pem', 'russian_trusted_sub_ca.pem'):
        p = ROOT / 'certs' / f
        if p.exists(): c.load_verify_locations(str(p))
    return c


CTX = None


def api(method, body, tries=3):
    global CTX
    CTX = CTX or _ctx()
    tok = os.environ.get('TINVEST_SANDBOX_TOKEN') or (pathlib.Path.home() / '.config/tinvest/sandbox_token').read_text().strip()
    req = urllib.request.Request(BASE + method, data=json.dumps(body).encode(),
                                 headers={'Authorization': 'Bearer ' + tok, 'Content-Type': 'application/json'})
    for k in range(tries):
        try:
            return json.load(urllib.request.urlopen(req, context=CTX, timeout=40))
        except urllib.error.HTTPError as e:
            msg = e.read().decode()[:500]
            if e.code < 500 or k == tries - 1: return {'_error': f'HTTP {e.code}: {msg}'}
        except Exception as e:
            if k == tries - 1: return {'_error': f'{type(e).__name__}: {e}'}
        time.sleep(3 + 7 * k)


q = lambda x: None if not x else float(x.get('units', 0)) + x.get('nano', 0) / 1e9


def log(msg):
    print(f'{dt.datetime.now(MSK):%Y-%m-%d %H:%M:%S} vb-bot  {msg}', flush=True)


# ── состояние и журнал ──
def load_state():
    f = STATE / 'state.json'
    return json.loads(f.read_text()) if f.exists() else {}


def save_state(st):
    STATE.mkdir(parents=True, exist_ok=True)
    tmp = STATE / 'state.json.tmp'; tmp.write_text(json.dumps(st, ensure_ascii=False, indent=1)); tmp.replace(STATE / 'state.json')


def journal(row):
    f = STATE / 'journal.csv'; new = not f.exists()
    with open(f, 'a', newline='', encoding='utf-8') as h:
        w = csv.DictWriter(h, fieldnames=JCOLS)
        if new: w.writeheader()
        w.writerow({k: row.get(k, '') for k in JCOLS})


def instruments(st):
    """Справочник акций TQBR раз в день: figi, лот, «шорт разрешён»."""
    today = dt.date.today().isoformat()
    f = STATE / 'instruments.json'
    if f.exists():
        d = json.loads(f.read_text())
        if d.get('date') == today and all('dl' in v for v in d['map'].values()): return d['map']   # старый кэш без ставок — перечитать
    r = api('InstrumentsService/Shares', {'instrumentStatus': 'INSTRUMENT_STATUS_BASE'})
    if '_error' in r:
        if f.exists(): return json.loads(f.read_text())['map']
        raise RuntimeError(r['_error'])
    m = {s['ticker']: {'figi': s['figi'], 'lot': int(s['lot']), 'short': bool(s.get('shortEnabledFlag')),
                       'dl': q(s.get('dlong')), 'ds': q(s.get('dshort'))}
         for s in r['instruments'] if s.get('classCode') == 'TQBR'}
    f.write_text(json.dumps({'date': today, 'map': m}))
    return m


def last_price(figi):
    r = api('MarketDataService/GetLastPrices', {'instrumentId': [figi]})
    p = r.get('lastPrices') or []
    return q(p[0].get('price')) if p else None


def order(st, key, figi, side, lots, tag):
    """Рыночная заявка; orderId детерминирован по сделке — повтор не задвоит (песочница отклонит дубликат)."""
    oid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{st['account']}|{key}|{tag}"))
    r = api('SandboxService/PostSandboxOrder', {'instrumentId': figi, 'quantity': str(lots), 'accountId': st['account'],
             'direction': 'ORDER_DIRECTION_BUY' if side > 0 else 'ORDER_DIRECTION_SELL', 'orderType': 'ORDER_TYPE_MARKET', 'orderId': oid})
    if r.get('executionReportStatus') == 'EXECUTION_REPORT_STATUS_FILL':
        return q(r.get('executedOrderPrice')), None              # средняя цена одной бумаги
    return None, r.get('_error') or r.get('executionReportStatus') or str(r)[:300]


# ── модель ──
def session(now):
    """(открытие, закрытие) торгового дня акций, МСК: будни 07:00–23:50, выходные сессии 10:00–19:00."""
    wk = now.weekday() >= 5
    return (dt.time(10, 0), dt.time(19, 0)) if wk else (dt.time(7, 0), dt.time(23, 50))


def rates_from(inst):
    """Ставки риска Т-Банка на сегодня: тикер → (лонг, шорт или None, если шорт запрещён)."""
    return {t: (i['dl'] if i.get('dl') else 1.0, i['ds'] if i['short'] and i.get('ds') else None) for t, i in inst.items()}


# ── кэш модели на день ──
# Полный прогон (все акции с 2011) — 9 с и ~325 МБ; раньше шёл каждые 5 минут и 17.11.2026 добил память прода.
# Внутри дня меняется только сегодняшний бар, а сделки бумаги зависят только от её свечей: у бумаги без открытой
# сделки, без входа/выхода сегодня и уже с сегодняшней свечой результат до последних 10 минут не меняется
# (сигнальный день ≤ вчера, средние — по барам до него, подтверждение входа до закрытия бара всегда «да»).
# Поэтому: полный прогон — первый за день и в последние 10 минут; между ними пересчитываются только «живые»
# бумаги, сделки остальных берутся из утреннего кэша. Решения те же — сверка: robots/vb_light_check.py.
def _code_sig():
    import hashlib
    h = hashlib.sha1()
    for f in (ROOT / 'backtest' / 'screener_vb.py', pathlib.Path(__file__)):
        h.update(f.read_bytes())
    return f'{h.hexdigest()[:12]}|{LEVERAGE}|{GUARD}'


def could_enter_today(x, divs, mk, today):
    """Бумага ещё без сегодняшней свечи: может ли с её появлением родиться вход сегодня? Сигнальный день — её
    последняя свеча (всё уже известно), от сегодняшнего бара до закрытия зависит только отмена по гэпу открытия.
    Пробуем открытие = вчерашнему закрытию (гэпа нет — отмены нет): сигнала нет и так — не будет ни при каком."""
    c = x.c[-1]
    rows = [[d, o, h, l, cc, v] for d, o, h, l, cc, v in zip(x.d, x.o, x.h, x.l, x.c, x.v)] + [[today, c, c, c, c, x.v[-1]]]
    probe = S.Ticker(x.secid, rows)
    return any(s.entryDate == today for dir_ in ('long', 'short')
               for s in S.scan_full(probe, divs.get(x.secid, []), dir_, mk, None, lambda d: d < today))


def live_tickers(sigs, tk, st, today, divs, mk):
    """Бумаги, чьи сделки могут измениться от сегодняшнего бара: открытые, со входом/выходом сегодня, сделки с даты
    старта робота (их цены нужны счёту с маржой), бумаги без сегодняшней свечи, у которых с её появлением может
    родиться вход сегодня, ноги робота, SBER (проверка свежести и календарь дней счёта)."""
    live = {s.ticker for s in sigs
            if s.res['reason'] in ('open', 'delisted') or s.entryDate >= min(today, st['start']) or s.res['exitDate'] >= today}
    live |= {t for t, x in tk.items() if x.d[-1] < today and t not in live and could_enter_today(x, divs, mk, today)}
    live |= {L['ticker'] for L in st.get('legs', {}).values()}
    return sorted(live | {'SBER'})


DIV_SQL = "SELECT secid, registry_close_date, value FROM dividends WHERE registry_close_date IS NOT NULL"


def data_print(today, had=None):
    """Отпечаток всего, от чего зависят замороженные сделки, кроме сегодняшних h/l/c/v: свечи последних 20 дней
    (загрузчик каждые 5 минут перезаливает 14 дней — ISS правит вчерашний объём/цену), сегодняшнее открытие бумаг
    из had (у кого свеча уже была при записи кэша; у остальных вход сегодня исключила could_enter_today, а открытие
    может только отменить вход по гэпу), дивиденды, файлы сплитов/ставок/снятых бумаг. → (отпечаток, had)"""
    import hashlib
    w0 = (dt.date.fromisoformat(today) - dt.timedelta(days=20)).isoformat()
    df = S._copy(f"""SELECT secid, begin_time::date AS d, open, high, low, close, volume FROM candles
                     WHERE interval = 24 AND type = 'stock' AND begin_time >= '{w0}' ORDER BY secid, begin_time""")
    df['d'] = df.d.astype(str).str[:10]
    tod = df[df.d == today]
    if had is None:
        had = sorted(set(tod.secid))
    h = hashlib.sha1()
    h.update(df[df.d < today].to_csv(index=False).encode())
    h.update(tod[tod.secid.isin(set(had))][['secid', 'open']].to_csv(index=False).encode())
    h.update(S._copy(DIV_SQL).sort_values(['secid', 'registry_close_date', 'value']).to_csv(index=False).encode())
    for p in (S.extra_path(), S.rusfar_path(), S.DATA / 'splits.csv.gz'):
        h.update((f'{p.name}:{p.stat().st_mtime_ns}:{p.stat().st_size}' if p.exists() else p.name).encode())
    return h.hexdigest(), had


def account_days(tk, start, today):
    """Прошлые дни счёта с маржой (как в S.account_margin) — у лёгкого прогона они должны совпасть с полным."""
    return sorted({d for x in tk.values() for d in x.d if start <= d < today and not S.wk(d)})


def _cache_path(today):
    return STATE / f'model_{today}.pkl'


def load_cache(today, st):
    """Кэш дня, если он ещё верен; иначе None (→ полный прогон). Любая ошибка — тоже None: торговлю кэш не роняет."""
    # pickle читает только файл, который сам робот пишет в свой каталог состояния (root, 755) — чужих данных тут нет;
    # нужен ради точных numpy-чисел в Sig (JSON-копия могла бы разойтись с полным прогоном).
    import pickle
    try:
        c = pickle.loads(_cache_path(today).read_bytes())
        if c.get('code') != _code_sig() or c.get('start') != st['start']:
            return None
        if not {L['ticker'] for L in st.get('legs', {}).values()} <= set(c['live']):
            return None
        return c if data_print(today, c['had'])[0] == c['fp'] else None
    except Exception as e:
        if _cache_path(today).exists(): log(f'кэш модели не прочитан ({type(e).__name__}: {e}) — полный прогон')
        return None


def save_cache(today, st, sigs, tk, divs, mk, fp):
    import pickle
    try:
        live = live_tickers(sigs, tk, st, today, divs, mk)
        keep = set(live)
        STATE.mkdir(parents=True, exist_ok=True)
        tmp = STATE / 'model.pkl.tmp'
        tmp.write_bytes(pickle.dumps({'code': _code_sig(), 'start': st['start'], 'live': live, 'fp': fp[0], 'had': fp[1],
                                      'days': account_days(tk, st['start'], today),
                                      'frozen': [s for s in sigs if s.ticker not in keep]}))
        tmp.replace(_cache_path(today))
        for f in STATE.glob('model_*.pkl'):
            if f != _cache_path(today): f.unlink(missing_ok=True)
    except Exception as e:
        log(f'кэш модели не записан ({type(e).__name__}: {e}) — следующий запуск снова полный')


def model(now, st, inst, cache='auto'):
    """cache: 'auto' — как в бою; 'off' — всегда полный прогон (сверка). model.light — был ли лёгкий прогон."""
    today = now.date().isoformat()
    t_open, t_close = session(now)
    last10 = now.time() >= (dt.datetime.combine(now.date(), t_close) - dt.timedelta(minutes=10)).time()
    final = lambda d: d < today or (d == today and last10)
    model.light = False
    if cache == 'auto':
        # Нерабочий день или данные встали: сегодняшней свечи SBER нет — полную модель не считаем (робот всё равно
        # ничего не делает), иначе это 200 полных прогонов за день.
        sb = S.load_db(secids=['SBER'])[0].get('SBER')
        if not sb or sb.d[-1] != today:
            return [], set(), False, last10
    c = load_cache(today, st) if cache == 'auto' and not last10 else None
    if c:
        try:
            tk, mk, dv, vv = S.load_db(secids=c['live'])
            if account_days(tk, st['start'], today) != c['days']:
                c = None
        except Exception as e:
            log(f'лёгкая загрузка не удалась ({type(e).__name__}: {e}) — полный прогон'); c = None
    if c:
        sigs = c['frozen'] + S.run(tk, mk, dv, vv, final)
    else:
        # Отпечаток — ДО загрузки: если данные поменяются между ними, отпечаток окажется старым и следующий запуск
        # просто пересчитает всё; в обратном порядке кэш мог бы молча прожить день на старых данных.
        fp = None
        if cache == 'auto' and not last10:
            try:
                fp = data_print(today)
            except Exception as e:
                log(f'отпечаток данных не снят ({type(e).__name__}: {e}) — кэш не пишу')
        tk, mk, dv, vv = S.load_db()
        sigs = S.run(tk, mk, dv, vv, final)
        if fp and 'SBER' in tk and tk['SBER'].d[-1] == today:
            save_cache(today, st, sigs, tk, dv, mk, fp)
    model.light = bool(c)
    sigs.sort(key=lambda s: (s.entryDate, s.ticker, s.exitMode))
    w = S.cluster_weights(sigs)
    fresh = tk['SBER'].d[-1] == today if 'SBER' in tk else False
    if LEVERAGE > 1:                                       # счёт с маржой с первого дня робота (тот же код, что в Стенде)
        nom, forced, _ = S.account_margin(sigs, w, tk, LEVERAGE, rates_from(inst), st['start'], CAPITAL, SIZE, GUARD)
    else:
        nom, forced = [x * SIZE for x in S.account(sigs, w, SLOTS)], {}
    return [(s, x) for s, x in zip(sigs, nom) if x], {key(sigs[i]) for i in forced}, fresh, last10


def key(s):
    return f'{s.ticker}|{s.signalDate}|{s.dir}|{s.exitMode}'


C = S.P['commission'] + S.P['slippage']
raw_in = lambda s: s.ep / (1 + C) if s.dir == 'long' else s.ep / (1 - C)          # цена модели без её издержек
raw_out = lambda s: s.res['exitPrice'] / (1 - C) if s.dir == 'long' else s.res['exitPrice'] / (1 + C)
LEG = lambda s: ('лонг' if s.dir == 'long' else 'шорт') + ('-тренд' if s.exitMode == 'trend' else '-скальп')


def run():
    st = load_state()
    if not st.get('account'): sys.exit('нет счёта: сначала python -m robots.vb_bot setup')
    now = dt.datetime.now(MSK)
    t_open, t_close = session(now)
    if not (t_open <= now.time() <= t_close):
        return
    inst = instruments(st)
    exe, forced, fresh, last10 = model(now, st, inst)
    if not fresh:
        log('сегодняшних свечей в БД нет — неторговый день или данные встали; ничего не делаю'); return
    today = now.date().isoformat()
    legs, skipped = st.setdefault('legs', {}), st.setdefault('skipped', {})
    allm = {key(s): (s, x) for s, x in exe}
    # выходы: модель закрыла сделку (или её больше нет)
    for k in list(legs):
        s = allm.get(k, (None, 0))[0]
        if s is not None and s.res['reason'] == 'open' and k not in forced: continue
        L = legs[k]
        px, err = order(st, k, L['figi'], -L['side'], L['lots'], 'out')
        if err:
            log(f'ВЫХОД {k} не исполнен: {err}'); continue
        journal({'ts': now.isoformat(timespec='seconds'), 'action': 'выход', 'key': k, 'ticker': L['ticker'], 'leg': L['leg'],
                 'side': -L['side'], 'lots': L['lots'], 'qty': L['qty'], 'price': px,
                 'model_price': round(raw_out(s), 4) if s else '', 'model_reason': S.REASON.get(s.res['reason'], s.res['reason']) if s else 'нет в модели'})
        log(f"выход {k} {L['lots']} лот. по {px}")
        legs.pop(k); save_state(st)
    # входы: только сделки, начатые сегодня и после запуска робота
    for k, (s, x) in allm.items():
        if s.res['reason'] != 'open' or k in legs or k in skipped or s.entryDate < st['start']: continue
        if s.entryDate != today:
            skipped[k] = 'вход пропущен: робот не успел в день входа'; save_state(st); continue
        if not s.entryAtOpen and not last10: continue            # лонг и тренд — только по закрытию дня
        side = 1 if s.dir == 'long' else -1
        i = inst.get(s.ticker)
        why = 'нет в справочнике Т-Банка' if not i else ('шорт запрещён брокером' if side < 0 and not i['short'] else None)
        if not why:
            p = last_price(i['figi'])
            lots = math.floor(x / (p * i['lot'])) if p else 0           # x — стоимость входа модели, ₽
            why = None if lots >= 1 else 'нет цены или меньше 1 лота'
        if why:
            skipped[k] = why; save_state(st)
            journal({'ts': now.isoformat(timespec='seconds'), 'action': 'пропуск', 'key': k, 'ticker': s.ticker, 'leg': LEG(s), 'note': why})
            log(f'пропуск {k}: {why}'); continue
        px, err = order(st, k, i['figi'], side, lots, 'in')
        if err:
            log(f'ВХОД {k} не исполнен: {err}'); continue
        legs[k] = {'ticker': s.ticker, 'figi': i['figi'], 'leg': LEG(s), 'side': side, 'lots': lots, 'qty': lots * i['lot'],
                   'price': px, 'model_ep': round(raw_in(s), 4), 'ts': now.isoformat(timespec='seconds')}
        save_state(st)
        journal({'ts': now.isoformat(timespec='seconds'), 'action': 'вход', 'key': k, 'ticker': s.ticker, 'leg': LEG(s), 'side': side,
                 'lots': lots, 'qty': lots * i['lot'], 'price': px, 'model_price': round(raw_in(s), 4)})
        log(f'вход {k} {lots} лот. по {px} (модель {raw_in(s):.2f})')
    # сверка с брокером: суммарная позиция по бумаге = сумма ног
    pos = api('SandboxService/GetSandboxPositions', {'accountId': st['account']})
    if '_error' not in pos:
        have = {p['figi']: int(float(p['balance'])) for p in pos.get('securities', [])}
        want = {}
        for L in legs.values(): want[L['figi']] = want.get(L['figi'], 0) + L['side'] * L['qty']
        bad = {f: (have.get(f, 0), want.get(f, 0)) for f in set(have) | set(want) if have.get(f, 0) != want.get(f, 0)}
        if bad: log(f'РАСХОЖДЕНИЕ позиций (брокер, ноги): {bad}')


def setup():
    st = load_state()
    if st.get('account'): sys.exit(f"счёт уже есть: {st['account']}")
    r = api('SandboxService/OpenSandboxAccount', {'name': ACCOUNT_NAME})
    if '_error' in r: sys.exit(r['_error'])
    aid = r['accountId']
    p = api('SandboxService/SandboxPayIn', {'accountId': aid, 'amount': {'currency': 'rub', 'units': str(CAPITAL), 'nano': 0}})
    st = {'account': aid, 'start': dt.datetime.now(MSK).date().isoformat(), 'legs': {}, 'skipped': {}}
    save_state(st)
    print(json.dumps({'счёт': aid, 'зачислено': p.get('balance'), 'старт': st['start']}, ensure_ascii=False))


def status():
    st = load_state()
    print(json.dumps({'счёт': st.get('account'), 'старт': st.get('start'), 'открыто ног': len(st.get('legs', {})),
                      'пропущено': len(st.get('skipped', {}))}, ensure_ascii=False))
    for k, L in st.get('legs', {}).items(): print(f"  {k:40s} {L['lots']:>7} лот. по {L['price']} (модель {L['model_ep']})")
    pf = api('SandboxService/GetSandboxPortfolio', {'accountId': st['account']}) if st.get('account') else {}
    if pf and '_error' not in pf: print('портфель:', q(pf.get('totalAmountPortfolio')), 'деньги:', q(pf.get('totalAmountCurrencies')))


def report():
    """Закрытые сделки: результат робота против модели, в % на сделку по ценам исполнения (без комиссий у обоих)."""
    f = STATE / 'journal.csv'
    if not f.exists(): print('сделок ещё нет'); return
    rows = list(csv.DictReader(open(f, encoding='utf-8')))
    ins = {r['key']: r for r in rows if r['action'] == 'вход'}
    for r in rows:
        if r['action'] != 'выход' or r['key'] not in ins: continue
        a = ins[r['key']]; side = int(a['side'])
        rob = side * (float(r['price']) / float(a['price']) - 1) * 100
        mod = side * (float(r['model_price']) / float(a['model_price']) - 1) * 100 if r['model_price'] else float('nan')
        print(f"{r['key']:40s} робот {rob:+6.2f}%  модель {mod:+6.2f}%  разница {rob - mod:+5.2f} п.п.  ({r['model_reason']})")
    for r in rows:
        if r['action'] == 'пропуск': print(f"пропуск {r['key']}: {r['note']}")


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('cmd', choices=['setup', 'run', 'status', 'report'])
    a = ap.parse_args()
    {'setup': setup, 'run': run, 'status': status, 'report': report}[a.cmd]()
