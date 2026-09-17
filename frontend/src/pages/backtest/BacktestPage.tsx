/* eslint-disable @typescript-eslint/no-explicit-any -- результат прогона и правило приходят как свободный JSON движка */
// Стенд — свой бэктест-терминал (/admin/backtest, только админы). Этап 2: один график, сделки, сводка, новый прогон.
// План и устройство: research/backtest_terminal/PLAN.md. Расчёт — контейнер bt-worker, страница только показывает.
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { btApi, type BtCandle, type BtEquity, type BtMeta, type BtRun, type BtTrade } from './api';
import { CandleChart, EquityChart, type BtMarker } from './BtChart';
import './bt.css';

const TFS: [number, string][] = [[5, '5м'], [15, '15м'], [60, '1ч'], [1440, 'Д']];
const SPAN_DAYS: Record<number, [number, number]> = { 5: [70, 25], 15: [220, 60], 60: [900, 200], 1440: [4000, 400] };
const DAY = 86400;

const iso = (d: Date) => d.toISOString().slice(0, 10);
const addDays = (s: string, n: number) => { const d = new Date(s + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return iso(d); };
const hm = (s: string) => Number(s.slice(0, 2)) * 60 + Number(s.slice(3, 5));
/** Секунды «МСК как UTC» — так же отдаёт /candles. */
const ts = (d: string, minutes: number) => Date.UTC(+d.slice(0, 4), +d.slice(5, 7) - 1, +d.slice(8, 10)) / 1000 + minutes * 60;
const fmtD = (s: string) => `${s.slice(8, 10)}.${s.slice(5, 7)}.${s.slice(2, 4)}`;
const num = (v: number | null | undefined, digits = 0) => v == null ? '—' : v.toLocaleString('ru-RU', { maximumFractionDigits: digits, minimumFractionDigits: digits });
const pct = (v: number | null | undefined, digits = 2) => v == null ? '—' : (v > 0 ? '+' : '') + (100 * v).toFixed(digits) + '%';
const sign = (v: number | null | undefined) => v == null ? '' : v > 0 ? 'bt-up' : v < 0 ? 'bt-down' : '';
const mmToStr = (m: number) => `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;

/** Минута входа и выхода по развёрнутому правилу прогона (как считает движок). */
function execMinutes(specFull: any): { tin: number; tout: number } {
  const r = specFull?.rule;
  if (!r) return { tin: 17 * 60, tout: 11 * 60 };
  const tin = hm(r.signal.to) + (r.entry?.delay_min ?? 0) + (r.entry?.price === 'next_open' ? 5 : 0);
  const tout = hm(r.exit.at) + (r.exit?.delay_min ?? 0) + (r.exit?.price === 'next_open' ? 5 : 0);
  return { tin, tout };
}

export default function BacktestPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [sp, setSp] = useSearchParams();
  const isAdmin = !!user && user.role === 'admin';

  useEffect(() => { if (!authLoading && !isAdmin) navigate('/', { replace: true }); }, [authLoading, isAdmin, navigate]);

  const [meta, setMeta] = useState<BtMeta | null>(null);
  const [runs, setRuns] = useState<BtRun[]>([]);
  const [run, setRun] = useState<BtRun | null>(null);
  const [trades, setTrades] = useState<BtTrade[]>([]);
  const [equity, setEquity] = useState<BtEquity[]>([]);
  const [candles, setCandles] = useState<BtCandle[]>([]);
  const [tf, setTf] = useState(5);
  const [center, setCenter] = useState<string>(iso(new Date()));
  const [focus, setFocus] = useState<{ from: number; to: number; nonce: number } | null>(null);
  const [selKey, setSelKey] = useState<string | null>(null);
  const [tab, setTab] = useState<'trades' | 'summary'>('trades');
  const [onlySt, setOnlySt] = useState(true);
  const [drawer, setDrawer] = useState(false);
  const [hover, setHover] = useState<BtCandle | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loadingCandles, setLoadingCandles] = useState(false);
  const [bottomPct, setBottomPct] = useState(40);

  const runId = Number(sp.get('run')) || null;
  const st = sp.get('st') || 'SS';
  const setParam = useCallback((k: string, v: string | null) => {
    setSp(prev => { const n = new URLSearchParams(prev); if (v == null) n.delete(k); else n.set(k, v); return n; }, { replace: true });
  }, [setSp]);

  // справочники и список прогонов
  const reloadRuns = useCallback(() => btApi.runs().then(setRuns).catch(e => setErr(String(e.message ?? e))), []);
  useEffect(() => { if (!isAdmin) return; btApi.meta().then(setMeta).catch(e => setErr(String(e.message ?? e))); reloadRuns(); }, [isAdmin, reloadRuns]);
  useEffect(() => {                                   // нет прогона в адресе — берём последний готовый
    if (!runId && runs.length) { const r = runs.find(x => x.status === 'done') ?? runs[0]; setParam('run', String(r.id)); }
  }, [runId, runs, setParam]);
  useEffect(() => {                                   // пока что-то считается — опрашиваем
    if (!runs.some(r => r.status === 'queued' || r.status === 'running')) return;
    const t = setInterval(reloadRuns, 2000); return () => clearInterval(t);
  }, [runs, reloadRuns]);

  // выбранный прогон
  const runStatus = runs.find(r => r.id === runId)?.status;
  useEffect(() => {
    if (!isAdmin || !runId) return;
    let dead = false;
    setRun(null); setTrades([]); setEquity([]);
    btApi.run(runId).then(r => {
      if (dead) return; setRun(r);
      if (r.status !== 'done') return;
      Promise.all([btApi.trades(runId), btApi.equity(runId)]).then(([t, e]) => { if (!dead) { setTrades(t); setEquity(e); } });
    }).catch(e => setErr(String(e.message ?? e)));
    return () => { dead = true; };
  }, [isAdmin, runId, runStatus]);

  // свечи вокруг «центра»
  useEffect(() => {
    if (!isAdmin) return;
    let dead = false; setLoadingCandles(true);
    const [back, fwd] = SPAN_DAYS[tf];
    btApi.candles(st, tf, addDays(center, -back), addDays(center, fwd))
      .then(r => { if (!dead) setCandles(r.candles); })
      .catch(e => setErr(String(e.message ?? e)))
      .finally(() => { if (!dead) setLoadingCandles(false); });
    return () => { dead = true; };
  }, [isAdmin, st, tf, center]);

  const { tin, tout } = useMemo(() => execMinutes(run?.spec_full), [run]);
  const stTrades = useMemo(() => trades.filter(t => t.st === st), [trades, st]);
  const bucket = useCallback((d: string, m: number) => tf === 1440 ? ts(d, 0) : ts(d, Math.floor(m / tf) * tf), [tf]);

  const markers = useMemo<BtMarker[]>(() => stTrades.flatMap(t => {
    const muted = !!t.account_skip; const key = `${t.st}|${t.d}`;
    const thr = t.thr != null ? ` / порог ${(100 * (t.side > 0 ? t.thr : -t.thr)).toFixed(2)}%` : '';
    return [
      { key: key + '|in', time: bucket(t.d, tin), side: t.side, kind: 'in' as const, muted,
        text: `${t.side > 0 ? 'Л' : 'Ш'} ${num(t.px_in, 2)} · ход ${(100 * t.move).toFixed(2)}%${tf <= 15 ? thr : ''}` },
      { key: key + '|out', time: bucket(t.d_out, tout), side: t.side, kind: 'out' as const, muted, good: t.net > 0,
        text: `${num(t.px_out, 2)} · ${pct(t.net)}` },
    ];
  }), [stTrades, bucket, tin, tout, tf]);

  const perInst = useMemo(() => {
    const m = new Map<string, { n: number; net: number; rub: number }>();
    for (const t of trades) {
      const x = m.get(t.st) ?? { n: 0, net: 0, rub: 0 }; x.n++; x.net += t.net; x.rub += t.pnl_rub ?? 0; m.set(t.st, x);
    }
    return m;
  }, [trades]);

  const goTrade = (t: BtTrade) => {
    setSelKey(`${t.st}|${t.d}`);
    if (t.st !== st) setParam('st', t.st);
    setCenter(t.d);
    const pad = tf === 5 ? 1.2 * DAY : tf === 15 ? 4 * DAY : tf === 60 ? 15 * DAY : 120 * DAY;
    setFocus({ from: ts(t.d, 0) - pad, to: ts(t.d_out, 24 * 60) + pad, nonce: Date.now() });
  };
  const pickInstrument = (s: string) => {
    setParam('st', s); setSelKey(null);
    const last = trades.filter(t => t.st === s).at(-1);
    setCenter(last ? last.d : iso(new Date())); setFocus(null);
  };

  const startDrag = (e: React.MouseEvent) => {
    e.preventDefault();
    const h = window.innerHeight - 44;
    const move = (ev: MouseEvent) => setBottomPct(Math.min(75, Math.max(15, 100 * (window.innerHeight - ev.clientY) / h)));
    const up = () => { window.removeEventListener('mousemove', move); window.removeEventListener('mouseup', up); };
    window.addEventListener('mousemove', move); window.addEventListener('mouseup', up);
  };

  if (authLoading || !isAdmin) return null;

  const names = new Map(meta?.instruments.map(i => [i.st, i.name]) ?? []);
  const universe: string[] = run?.spec_full?.universe ?? meta?.instruments.map(i => i.st) ?? [];
  const acc = run?.result?.account; const pt = run?.result?.per_trade;
  const shown = (onlySt ? stTrades : trades).slice().reverse();
  const last = hover ?? candles.at(-1) ?? null;

  return (
    <div className="bt-root">
      <div className="bt-top">
        <span className="bt-logo">СТЕНД</span>
        <select className="bt-select" style={{ maxWidth: 460 }} value={runId ?? ''} onChange={e => setParam('run', e.target.value)}>
          {!runs.length && <option value="">прогонов пока нет</option>}
          {runs.map(r => (
            <option key={r.id} value={r.id}>
              #{r.id} · {r.name || (typeof r.spec.rule === 'string' ? r.spec.rule : r.spec.rule?.id)} · {r.spec.exec}
              {r.status === 'done' && r.summary ? ` · ${num(r.summary['сделок'])} сд. · ${r.summary['годовых_%'] ?? '—'}% год.` : ` · ${r.status}`}
            </option>
          ))}
        </select>
        {run && <span className={`bt-status ${run.status}`}>{{ queued: 'в очереди', running: 'считается…', done: 'готов', error: 'ошибка' }[run.status]}</span>}
        {run?.status === 'done' && acc && (
          <span className="bt-dim" style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            годовых <b className={sign(acc['годовых_%'])}>{acc['годовых_%']}%</b> · просадка <b className="bt-down">{acc['просадка_%']}%</b> ·
            Шарп <b style={{ color: 'var(--bt-text)' }}>{acc['Шарп']}</b> · данные по {String(run.result?.data_until ?? '').slice(0, 16)}
          </span>
        )}
        <span style={{ flex: 1 }} />
        {err && <span className="bt-down" title={err} style={{ maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} onClick={() => setErr(null)}>{err}</span>}
        {run && <button className="bt-btn" onClick={() => { if (confirm(`Удалить прогон #${run.id}?`)) btApi.deleteRun(run.id).then(() => { setParam('run', null); reloadRuns(); }); }}>Удалить</button>}
        <button className="bt-btn primary" onClick={() => setDrawer(true)}>Новый прогон</button>
      </div>

      <div className="bt-body">
        <aside className="bt-side">
          <h4>Бумаги прогона</h4>
          {universe.map(s => {
            const x = perInst.get(s);
            return (
              <div key={s} className={`bt-inst ${s === st ? 'on' : ''}`} onClick={() => pickInstrument(s)}>
                <b>{s}</b>
                <span>{names.get(s) ?? s}<small>{x ? `${x.n} сделок` : 'нет сделок'}</small></span>
                <span className={sign(x?.net)} style={{ textAlign: 'right' }}>
                  {x ? pct(x.net / x.n) : ''}<small>{x && acc ? `${num(x.rub)} ₽` : ''}</small>
                </span>
              </div>
            );
          })}
        </aside>

        <main className="bt-main" style={{ ['--bt-bottom' as any]: `${bottomPct}%` }}>
          <section className="bt-chartwrap">
            <div className="bt-chartbar">
              <b>{names.get(st) ?? st}</b><span className="bt-dim">{last?.secid ?? ''}</span>
              {TFS.map(([v, l]) => <button key={v} className={`bt-btn sm ${tf === v ? 'on' : ''}`} onClick={() => { setTf(v); setFocus(null); }}>{l}</button>)}
              <button className="bt-btn sm" title="раньше" onClick={() => { setCenter(c => addDays(c, -SPAN_DAYS[tf][0] / 2)); setFocus(null); }}>◀</button>
              <button className="bt-btn sm" title="позже" onClick={() => { setCenter(c => addDays(c, SPAN_DAYS[tf][0] / 2)); setFocus(null); }}>▶</button>
              <button className="bt-btn sm" onClick={() => { setCenter(iso(new Date())); setFocus(null); }}>сегодня</button>
              {last && (
                <span className="bt-dim">
                  О <b className={sign(last.close - last.open)}>{num(last.open, 2)}</b> В <b className={sign(last.close - last.open)}>{num(last.high, 2)}</b>{' '}
                  Н <b className={sign(last.close - last.open)}>{num(last.low, 2)}</b> З <b className={sign(last.close - last.open)}>{num(last.close, 2)}</b> · объём {num(last.volume)}
                </span>
              )}
              {loadingCandles && <span className="bt-dim">загрузка…</span>}
            </div>
            <div className="bt-chart">
              <CandleChart candles={candles} markers={markers} focus={focus} onHover={setHover} />
              {run?.spec_full?.rule && (
                <div className="bt-note bt-dim">
                  {run.spec_full.rule.name} · вход {mmToStr(tin)}, выход {mmToStr(tout)} МСК · стрелка — вход, кружок — выход, серые — сигнал без сделки на счёте
                </div>
              )}
            </div>
          </section>
          <div className="bt-split" onMouseDown={startDrag} />
          <section className="bt-bottom">
            <div className="bt-tabs">
              <button className={`bt-tab ${tab === 'trades' ? 'on' : ''}`} onClick={() => setTab('trades')}>Сделки {trades.length ? `(${onlySt ? stTrades.length : trades.length})` : ''}</button>
              <button className={`bt-tab ${tab === 'summary' ? 'on' : ''}`} onClick={() => setTab('summary')}>Сводка</button>
              <span style={{ flex: 1 }} />
              {tab === 'trades' && (
                <label className="bt-dim" style={{ display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer' }}>
                  <input type="checkbox" checked={onlySt} onChange={e => setOnlySt(e.target.checked)} /> только {st}
                </label>
              )}
            </div>
            <div className="bt-scroll">
              {!run ? <div className="bt-empty">{runs.length ? 'Загрузка…' : 'Прогонов пока нет. Нажми «Новый прогон».'}</div>
                : run.status === 'error' ? <div className="bt-empty bt-down">Прогон упал: {run.error}</div>
                : run.status !== 'done' ? <div className="bt-empty">Прогон считается… обычно это несколько секунд (первый после перезапуска — пара минут: грузятся свечи).</div>
                : tab === 'trades' ? <TradesTable rows={shown.slice(0, 1500)} total={shown.length} withAccount={!!acc} selKey={selKey} onPick={goTrade} names={names} />
                : <Summary run={run} equity={equity} pt={pt} acc={acc} names={names} />}
            </div>
          </section>
        </main>
      </div>
      {drawer && meta && <NewRun meta={meta} base={run?.spec} onClose={() => setDrawer(false)}
        onCreated={id => { setDrawer(false); reloadRuns().then(() => setParam('run', String(id))); }} />}
    </div>
  );
}

function TradesTable({ rows, total, withAccount, selKey, onPick, names }: {
  rows: BtTrade[]; total: number; withAccount: boolean; selKey: string | null; onPick: (t: BtTrade) => void; names: Map<string, string>;
}) {
  if (!rows.length) return <div className="bt-empty">Сделок нет</div>;
  return (
    <table className="bt-table">
      <thead><tr>
        <th className="l">Бумага</th><th className="l">Тип</th><th className="l">Вход</th><th>Цена</th><th className="l">Выход</th><th>Цена</th>
        <th>Ход дня</th><th>Порог</th><th>Цена, %</th><th>Издержки, %</th><th>Итог, %</th>
        {withAccount && <><th>Контр.</th><th>Позиция, ₽</th><th>ГО, ₽</th><th>Комиссия+спред, ₽</th><th>Итог, ₽</th></>}
      </tr></thead>
      <tbody>
        {rows.map(t => {
          const k = `${t.st}|${t.d}`;
          return (
            <tr key={k} className={`${k === selKey ? 'on' : ''} ${t.account_skip ? 'skipped' : ''}`} onClick={() => onPick(t)}>
              <td className="l"><b>{t.st}</b> <span className="bt-dim">{names.get(t.st)} · {t.secid}</span></td>
              <td className={`l ${t.side > 0 ? 'bt-up' : 'bt-down'}`}>{t.side > 0 ? 'Длинная' : 'Короткая'}</td>
              <td className="l">{fmtD(t.d)}</td><td>{num(t.px_in, 2)}</td>
              <td className="l">{fmtD(t.d_out)}</td><td>{num(t.px_out, 2)}</td>
              <td>{pct(t.move)}</td><td className="bt-dim">{t.thr != null ? pct(t.side > 0 ? t.thr : -t.thr) : '—'}</td>
              <td className={sign(t.gross)}>{pct(t.gross)}</td><td className="bt-dim">−{(100 * (t.comm + t.spread)).toFixed(2)}%</td>
              <td className={sign(t.net)}><b>{pct(t.net)}</b></td>
              {withAccount && (t.account_skip
                ? <td colSpan={5} className="l">не исполнена: {t.account_skip}</td>
                : <><td>{num(t.qty)}</td><td>{num(t.notional)}</td><td>{num(t.go)}</td>
                    <td className="bt-dim">{num((t.comm_rub ?? 0) + (t.spread_rub ?? 0))}</td><td className={sign(t.pnl_rub)}><b>{num(t.pnl_rub)}</b></td></>)}
            </tr>
          );
        })}
        {total > rows.length && <tr><td className="l bt-dim" colSpan={16}>показаны последние {rows.length} из {total}</td></tr>}
      </tbody>
    </table>
  );
}

function Summary({ run, equity, pt, acc, names }: { run: BtRun; equity: BtEquity[]; pt: any; acc: any; names: Map<string, string> }) {
  const lines = useMemo(() => [{ name: 'капитал', color: '#FF5C2B', points: equity.map(e => ({ d: e.d, v: e.equity })) }], [equity]);
  const s = run.spec_full ?? {};
  const tile = (label: string, value: React.ReactNode, cls = '') => <div className="bt-tile"><small>{label}</small><b className={cls}>{value}</b></div>;
  const skipped = acc?.['пропущено'] ?? {};
  return (
    <>
      <div className="bt-tiles">
        {acc && <>
          {tile('Годовых', acc['годовых_%'] + '%', sign(acc['годовых_%']))}
          {tile('Макс. просадка', acc['просадка_%'] + '%', 'bt-down')}
          {tile('Шарп', acc['Шарп'])}
          {tile('Капитал', `${num(acc['капитал_старт'])} → ${num(acc['капитал_конец'])} ₽`)}
          {tile('Прибыль', num(acc['прибыль_руб']) + ' ₽', sign(acc['прибыль_руб']))}
          {tile('Комиссии / спред', `${num(acc['комиссии_руб'])} / ${num(acc['спред_руб'])} ₽`)}
          {tile('ГО, максимум', acc['ГО_макс_%'] + '% капитала')}
          {tile('Загрузка капитала', acc['загрузка_средняя_%'] + '% в среднем')}
        </>}
        {tile('Сделок (лонг / шорт)', `${num(pt?.['сделок'])} (${num(pt?.['лонгов'])} / ${num(pt?.['шортов'])})`)}
        {tile('На сделку: цена → итог', `${pt?.['валовыми_%']}% → ${pt?.['чистыми_%']}%`, sign(pt?.['чистыми_%']))}
        {tile('Винрейт / профит-фактор', `${pt?.['винрейт_%']}% / ${pt?.['профит_фактор'] ?? '—'}`)}
        {tile('t-статистика', pt?.['t'] ?? '—')}
        {acc && tile('Исполнено / пропущено', `${num(acc['сделок_исполнено'])} / ${num(Object.values(skipped).reduce((a: number, b: any) => a + Number(b), 0))}`)}
      </div>
      <div className="bt-cols">
        <div className="bt-card"><h5>Капитал, ₽</h5>{equity.length ? <EquityChart lines={lines} /> : <div className="bt-empty">счёт не считался</div>}</div>
        <div className="bt-card">
          <h5>По годам и условия прогона</h5>
          <table className="bt-table"><tbody>
            {acc && Object.entries(acc['по_годам_%'] ?? {}).map(([y, v]: any) => <tr key={y}><td className="l">{y}</td><td className={sign(v)}>{v > 0 ? '+' : ''}{v}%</td></tr>)}
            {Object.entries(skipped).map(([k, v]: any) => <tr key={k}><td className="l bt-dim">пропущено: {k}</td><td>{num(v)}</td></tr>)}
            <tr><td className="l bt-dim">период</td><td>{run.result?.period?.join(' — ')}</td></tr>
            <tr><td className="l bt-dim">тариф / спред / ГО</td><td>{s.tariff} / {s.spread} / {s.go}</td></tr>
            <tr><td className="l bt-dim">капитал, слотов</td><td>{num(s.capital)} ₽, {s.slots}</td></tr>
          </tbody></table>
        </div>
      </div>
      <div className="bt-cols" style={{ gridTemplateColumns: '1fr' }}>
        <div className="bt-card"><h5>По бумагам (на сделку, после издержек)</h5>
          <table className="bt-table"><thead><tr><th className="l">Бумага</th><th>Сделок</th><th>Итог на сделку</th><th>Винрейт</th></tr></thead><tbody>
            {(run.result?.by_instrument ?? []).map((r: any) => <tr key={r.st}><td className="l"><b>{r.st}</b> <span className="bt-dim">{names.get(r.st)}</span></td>
              <td>{num(r['сделок'])}</td><td className={sign(r['чистыми_%'])}>{r['чистыми_%'] > 0 ? '+' : ''}{r['чистыми_%']}%</td><td>{r['винрейт_%']}%</td></tr>)}
          </tbody></table>
        </div>
      </div>
    </>
  );
}

function NewRun({ meta, base, onClose, onCreated }: { meta: BtMeta; base?: any; onClose: () => void; onCreated: (id: number) => void }) {
  const baseRule = typeof base?.rule === 'string' ? base.rule : base?.rule?.id;
  const [rule, setRule] = useState<string>(meta.rules.some(r => r.id === baseRule) ? baseRule : meta.rules[0].id);
  const preset = meta.rules.find(r => r.id === rule)!;
  const [uni, setUni] = useState<string[]>(base?.universe ?? preset.universe);
  const [f, setF] = useState({ name: '', exec: base?.exec ?? 'robot', tariff: base?.tariff ?? 'trader', spread: base?.spread ?? 'c3',
    capital: base?.capital ?? 1_000_000, slots: base?.slots ?? 6, go: base?.go ?? 'mr1', since: base?.since ?? '', until: base?.until ?? '' });
  const [busy, setBusy] = useState(false); const [error, setError] = useState<string | null>(null);
  const first = useRef(true);
  useEffect(() => { if (first.current) { first.current = false; return; } setUni(preset.universe); }, [preset]);
  const set = (k: string, v: any) => setF(p => ({ ...p, [k]: v }));
  const submit = () => {
    setBusy(true); setError(null);
    btApi.createRun({ rule, name: f.name || null, exec: f.exec, universe: uni, tariff: f.tariff, spread: f.spread,
      capital: Number(f.capital) || null, slots: Number(f.slots) || 6, go: f.go, since: f.since || null, until: f.until || null })
      .then(r => onCreated(r.id)).catch(e => { setError(String(e.message ?? e)); setBusy(false); });
  };
  return (
    <div className="bt-drawer">
      <header><b>Новый прогон</b><button className="bt-btn sm" onClick={onClose}>✕</button></header>
      <div className="bt-form">
        <label>Правило
          <select className="bt-select" value={rule} onChange={e => setRule(e.target.value)}>
            {meta.rules.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
          </select>
        </label>
        <div className="hint">
          Сигнал: ход {preset.rule.signal.from} → {preset.rule.signal.to}. Выход: {preset.rule.exit.at} следующего торгового дня.
          {preset.frozen && ' Правило заморожено — параметры не меняются.'}
        </div>
        <label>Исполнение
          <select className="bt-select" value={f.exec} onChange={e => set('exec', e.target.value)}>
            {Object.entries(meta.exec).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
          </select>
        </label>
        <label>Бумаги ({uni.length})
          <div className="bt-chips">
            <button className="bt-chip" onClick={() => setUni(preset.universe)}>все</button>
            <button className="bt-chip" onClick={() => setUni([])}>ни одной</button>
            {meta.instruments.map(i => (
              <button key={i.st} title={i.name} className={`bt-chip ${uni.includes(i.st) ? 'on' : ''}`}
                onClick={() => setUni(u => u.includes(i.st) ? u.filter(x => x !== i.st) : [...u, i.st])}>{i.st}</button>
            ))}
          </div>
        </label>
        <div className="row">
          <label>Капитал, ₽<input className="bt-input" type="number" value={f.capital} onChange={e => set('capital', e.target.value)} /></label>
          <label>Сделок в день, максимум<input className="bt-input" type="number" min={1} max={30} value={f.slots} onChange={e => set('slots', e.target.value)} /></label>
        </div>
        <div className="hint">На сделку идёт капитал / число слотов, целыми контрактами, без плеча.</div>
        <div className="row">
          <label>Тариф брокера
            <select className="bt-select" value={f.tariff} onChange={e => set('tariff', e.target.value)}>
              {Object.entries(meta.tariffs).map(([k, v]) => <option key={k} value={k}>{k} — {v}% за сторону</option>)}
            </select>
          </label>
          <label>Спред стакана
            <select className="bt-select" value={f.spread} onChange={e => set('spread', e.target.value)}>
              <option value="c3">учитывать (3 уровня)</option><option value="c5">учитывать (5 уровней)</option><option value="none">не учитывать</option>
            </select>
          </label>
        </div>
        <label>Гарантийное обеспечение
          <select className="bt-select" value={f.go} onChange={e => set('go', e.target.value)}>
            {Object.entries(meta.go).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
          </select>
        </label>
        <div className="row">
          <label>С даты<input className="bt-input" type="date" value={f.since} onChange={e => set('since', e.target.value)} /></label>
          <label>По дату<input className="bt-input" type="date" value={f.until} onChange={e => set('until', e.target.value)} /></label>
        </div>
        <label>Название (необязательно)<input className="bt-input" value={f.name} onChange={e => set('name', e.target.value)} placeholder="например: гибрид, только акции" /></label>
        {error && <div className="bt-down">{error}</div>}
      </div>
      <footer><button className="bt-btn" onClick={onClose}>Отмена</button><button className="bt-btn primary" disabled={busy || !uni.length} onClick={submit}>{busy ? 'Отправляю…' : 'Запустить'}</button></footer>
    </div>
  );
}
