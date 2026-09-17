/* eslint-disable @typescript-eslint/no-explicit-any -- спецификация прогона — свободный JSON движка */
// Стенд — свой бэктест-терминал (/admin/backtest, только админы). Устройство как у TradingView: тонкая шапка
// (бумага · таймфрейм · индикаторы · редактор стратегии · сетка), графики, снизу «Тестер стратегий».
// План: backtest/PLAN.md. Расчёт — контейнер bt-worker; страница только показывает и помнит, как её оставили.
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { btApi, type BtEquity, type BtLiveTrade, type BtMeta, type BtRun, type BtTrade } from './api';
import ChartCell, { hoverStore, type Focus } from './ChartCell';
import { AddIndicatorMenu, useIndicators } from '../embed/EmbedIndicators';
import Editor from './Editor';
import SymbolSearch, { type SymRow } from './SymbolSearch';
import Tester, { tradeKey } from './Tester';
import { paramLabel, setPath } from './Sweep';
import { TFS, cls, num, pct, tfLabel } from './lib';
import { usePrefs, type Layout, type Prefs } from './usePrefs';
import { Logo, Menu, MenuItem } from './ui';
import './bt.css';

const LAYOUTS: [Layout, string, number][] = [['1', '▢  Один график', 1], ['2h', '◫  Два рядом', 2], ['2v', '⊟  Два друг под другом', 2], ['4', '⊞  Четыре', 4]];
const SHOW: [keyof Prefs['show'], string][] = [['markers', 'Стрелки сделок'], ['labels', 'Подписи у стрелок'], ['lines', 'Линии вход → выход'], ['window', 'Подсветка окна сигнала'], ['robot', 'Сделки робота (песочница)']];

/** Данные свечи под курсором активного графика — в шапке, как в TradingView. Свой компонент: обновляется без перерисовки страницы. */
function Ohlc() {
  const c = useSyncExternalStore(hoverStore.subscribe, hoverStore.get);
  if (!c) return null; const k = cls(c.close - c.open) || 'bt-up'; const ch = c.open ? c.close / c.open - 1 : 0;
  return <span className="bt-ohlc"><span className="bt-dim">{c.secid}</span> О<i className={k}>{num(c.open, 2)}</i> В<i className={k}>{num(c.high, 2)}</i> Н<i className={k}>{num(c.low, 2)}</i> З<i className={k}>{num(c.close, 2)}</i> <i className={k}>{pct(ch)}</i> <span className="bt-dim">объём {num(c.volume)}</span></span>;
}

export default function BacktestPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [sp, setSp] = useSearchParams();
  const isAdmin = !!user && user.role === 'admin';
  useEffect(() => { if (!authLoading && !isAdmin) navigate('/', { replace: true }); }, [authLoading, isAdmin, navigate]);

  const [prefs, set] = usePrefs(sp.get('ws'));
  const inds = useIndicators('frame:bt:indicators');     // движок индикаторов песочницы: настройка, перенос между панелями, профиль объёма
  const [meta, setMeta] = useState<BtMeta | null>(null);
  const [runs, setRuns] = useState<BtRun[]>([]);
  const [run, setRun] = useState<BtRun | null>(null);
  const [trades, setTrades] = useState<BtTrade[]>([]);
  const [equity, setEquity] = useState<BtEquity[]>([]);
  const [compare, setCompare] = useState<{ run: BtRun; equity: BtEquity[] } | null>(null);
  const [live, setLive] = useState<BtLiveTrade[]>([]);
  const [focus, setFocus] = useState<Focus | null>(null);
  const [selKey, setSelKey] = useState<string | null>(null);
  const [search, setSearch] = useState(false);
  const [editor, setEditor] = useState(false);
  const [busy, setBusy] = useState(false);
  const [runErr, setRunErr] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const fail = useCallback((e: unknown) => setErr(String((e as Error)?.message ?? e)), []);

  const nCells = LAYOUTS.find(l => l[0] === prefs.layout)?.[2] ?? 1;
  const active = Math.min(prefs.active, nCells - 1);
  const cell = prefs.cells[active];
  const setCell = useCallback((i: number, patch: Partial<Prefs['cells'][number]>) => set(p => ({ cells: p.cells.map((c, k) => k === i ? { ...c, ...patch } : c) })), [set]);

  // ссылка: ?run= и ?st= применяются один раз при открытии; дальше адрес только отражает состояние
  const applied = useRef(false);
  useEffect(() => {
    if (applied.current) return; applied.current = true;
    const r = Number(sp.get('run')); const s = sp.get('st');
    if (r) set({ runId: r }); if (s) setCell(0, { st: s });
  }, [sp, set, setCell]);
  useEffect(() => {
    setSp(prev => { const n = new URLSearchParams(prev); if (prefs.runId) n.set('run', String(prefs.runId)); n.set('st', cell.st); return n; }, { replace: true });
  }, [prefs.runId, cell.st, setSp]);

  const reloadRuns = useCallback(() => btApi.runs().then(setRuns).catch(fail), [fail]);
  useEffect(() => { if (!isAdmin) return; btApi.meta().then(setMeta).catch(fail); reloadRuns(); btApi.live().then(setLive).catch(() => undefined); }, [isAdmin, reloadRuns, fail]);
  useEffect(() => { if (!prefs.runId && runs.length) set({ runId: (runs.find(x => x.status === 'done') ?? runs[0]).id }); }, [prefs.runId, runs, set]);
  useEffect(() => {
    if (!runs.some(r => r.status === 'queued' || r.status === 'running')) return;
    const t = setInterval(reloadRuns, 2000); return () => clearInterval(t);
  }, [runs, reloadRuns]);

  // выбранный прогон: старые данные остаются на экране, пока не приехали новые — ничего не мигает и не прыгает
  const runStatus = runs.find(r => r.id === prefs.runId)?.status;
  useEffect(() => {
    if (!isAdmin || !prefs.runId) return; let dead = false;
    btApi.run(prefs.runId).then(r => {
      if (dead) return; setRun(r); if (r.status === 'done') setBusy(false); if (r.status === 'error') { setBusy(false); setRunErr(r.error); }
      if (r.status !== 'done') return;
      Promise.all([btApi.trades(r.id), btApi.equity(r.id)]).then(([t, e]) => { if (!dead) { setTrades(t); setEquity(e); } }).catch(fail);
    }).catch(e => { if (!dead) { fail(e); set({ runId: null }); } });
    return () => { dead = true; };
  }, [isAdmin, prefs.runId, runStatus, fail, set]);
  useEffect(() => {
    if (!prefs.compareRunId) { setCompare(null); return; } let dead = false;
    Promise.all([btApi.run(prefs.compareRunId), btApi.equity(prefs.compareRunId)]).then(([r, e]) => { if (!dead) setCompare({ run: r, equity: e }); }).catch(() => undefined);
    return () => { dead = true; };
  }, [prefs.compareRunId]);

  const names = useMemo(() => new Map(meta?.instruments.map(i => [i.st, i.name]) ?? []), [meta]);
  const tradesBySt = useMemo(() => { const m = new Map<string, BtTrade[]>(); for (const t of trades) { const a = m.get(t.st); if (a) a.push(t); else m.set(t.st, [t]); } return m; }, [trades]);
  const liveBySt = useMemo(() => { const m = new Map<string, BtLiveTrade[]>(); for (const t of live) { const a = m.get(t.st); if (a) a.push(t); else m.set(t.st, [t]); } return m; }, [live]);
  const symRows: SymRow[] = useMemo(() => (meta?.instruments ?? []).map(i => {
    const ts_ = tradesBySt.get(i.st) ?? []; const ex = ts_.filter(t => !t.account_skip);
    return { st: i.st, name: i.name, n: ts_.length, avg: ts_.length ? ts_.reduce((a, t) => a + t.net, 0) / ts_.length : null,
      rub: run?.result?.account ? ex.reduce((a, t) => a + (t.pnl_rub ?? 0), 0) : null, inRun: (run?.spec_full?.universe ?? []).includes(i.st) };
  }), [meta, tradesBySt, run]);

  const pickSymbol = (st: string) => { setCell(active, { st }); set(p => ({ recent: [st, ...p.recent.filter(x => x !== st)].slice(0, 8) })); setSearch(false); setSelKey(null); };
  const pickTrade = (t: BtTrade) => { setSelKey(tradeKey(t)); if (t.st !== cell.st) setCell(active, { st: t.st }); setFocus({ d: t.d, dOut: t.d_out, nonce: Date.now() }); if (prefs.panel === 'max') set({ panel: 'open' }); };
  const startRun = (spec: any) => { setBusy(true); setRunErr(null); btApi.createRun(spec).then(r => reloadRuns().then(() => set({ runId: r.id, view: 'overview' }))).catch(e => { setBusy(false); setRunErr(String(e.message ?? e)); }); };
  const deleteRun = (id: number) => btApi.deleteRun(id).then(() => { if (prefs.runId === id) { set({ runId: null }); setRun(null); setTrades([]); setEquity([]); } reloadRuns(); }).catch(fail);

  useEffect(() => {                                   // как в TradingView: начал печатать — открылся поиск бумаги
    const key = (e: KeyboardEvent) => {
      const el = e.target as HTMLElement; if (['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName) || e.metaKey || e.ctrlKey || e.altKey) return;
      if (/^[a-zA-Zа-яА-Я]$/.test(e.key)) setSearch(true);
    };
    window.addEventListener('keydown', key); return () => window.removeEventListener('keydown', key);
  }, []);

  const drag = (e: React.MouseEvent) => {
    e.preventDefault(); const move = (ev: MouseEvent) => set({ panelH: Math.max(140, Math.min(window.innerHeight - 160, window.innerHeight - ev.clientY)) });
    const up = () => { window.removeEventListener('mousemove', move); window.removeEventListener('mouseup', up); document.body.style.userSelect = ''; };
    document.body.style.userSelect = 'none'; window.addEventListener('mousemove', move); window.addEventListener('mouseup', up);
  };

  if (authLoading || !isAdmin) return null;
  const rule = run?.spec_full?.rule ?? null;

  return (
    <div className="bt-root">
      <div className="bt-toolbar">
        <button className="bt-tb sym" onClick={() => setSearch(true)} title="Выбрать инструмент (или просто начни печатать)"><Logo st={cell.st} size={20} /><b>{cell.st}</b><span className="bt-dim">{names.get(cell.st) ?? ''}</span></button>
        <i className="sep" />
        <Menu label={<><b>{tfLabel(cell.tf)}</b> ▾</>} title="Таймфрейм">{close => <>{TFS.map(([v, , full]) => <MenuItem key={v} on={cell.tf === v} onClick={() => { setCell(active, { tf: v }); close(); }}>{full}</MenuItem>)}</>}</Menu>
        <i className="sep" />
        <Menu label={<>Индикаторы{inds.list.length ? <sup>{inds.list.length}</sup> : null}</>}>{close => <div data-theme="editorial-dark" className="bt-indmenu"><AddIndicatorMenu api={inds} hasVolume onDone={close} /></div>}</Menu>
        <button className={`bt-tb ${editor ? 'on' : ''}`} onClick={() => setEditor(e => !e)} title="Редактор стратегии: код правила, свойства счёта, запуск">{'{ }'} Стратегия</button>
        <i className="sep" /><Ohlc />
        <span style={{ flex: 1 }} />
        {err && <span className="bt-err" title={err} onClick={() => setErr(null)}>{err}</span>}
        <Menu label="Вид" align="right">{() => <>
          <div className="bt-pop-h">На графике</div>
          {SHOW.map(([k, l]) => <MenuItem key={k} on={prefs.show[k]} onClick={() => set(p => ({ show: { ...p.show, [k]: !p.show[k] } }))}>{l}</MenuItem>)}
          <div className="bt-pop-sep" /><div className="bt-pop-h">Сетка графиков</div>
          {LAYOUTS.map(([k, l]) => <MenuItem key={k} on={prefs.layout === k} onClick={() => set({ layout: k, active: 0 })}>{l}</MenuItem>)}
          <div className="bt-pop-sep" /><MenuItem on={prefs.panel !== 'closed'} onClick={() => set({ panel: prefs.panel === 'closed' ? 'open' : 'closed' })}>Тестер стратегий</MenuItem>
        </>}</Menu>
      </div>

      <div className="bt-body">
        <div className="bt-col">
          {prefs.panel !== 'max' && (
            <div className={`bt-grid l${prefs.layout}`}>
              {prefs.cells.slice(0, nCells).map((c, i) => (
                <ChartCell key={i} st={c.st} tf={c.tf} active={i === active} multi={nCells > 1} rule={rule}
                  trades={tradesBySt.get(c.st) ?? EMPTY_T} live={liveBySt.get(c.st) ?? EMPTY_L} show={prefs.show} inds={inds}
                  focus={i === active ? focus : null} onActivate={() => { if (i !== active) { set({ active: i }); hoverStore.set(null); } }} />
              ))}
            </div>
          )}
          {prefs.panel === 'closed'
            ? <button className="bt-reopen" onClick={() => set({ panel: 'open' })}>Тестер стратегий {run?.result?.account ? `· ${run.result.account['годовых_%']}% годовых · просадка ${run.result.account['просадка_%']}%` : ''} ▴</button>
            : <>
              {prefs.panel === 'open' && <div className="bt-split" onMouseDown={drag} />}
              <div className="bt-panel" style={prefs.panel === 'max' ? { flex: 1 } : { height: prefs.panelH }}>
                <Tester runs={runs} run={run} trades={trades} equity={equity} st={cell.st} name={names.get(cell.st) ?? cell.st} names={names} prefs={prefs} set={set}
                  onPickTrade={pickTrade} selKey={selKey} onDeleteRun={deleteRun} onOpenEditor={() => setEditor(true)} compare={compare} live={live}
                  onRunChecks={() => { if (run) btApi.runChecks(run.id).then(reloadRuns).catch(fail); }}
                  onOpenVariant={params => {
                    if (!run?.spec_full) return; const s = run.spec_full; const rule = JSON.parse(JSON.stringify(s.rule));
                    for (const [k, v] of Object.entries(params)) setPath(rule, k, v);
                    const label = Object.entries(params).map(([k, v]) => `${paramLabel(k)} ${v ?? 'выкл.'}`).join(', ');
                    startRun({ rule, name: `${rule.name ?? rule.id} · ${label}`, exec: 'close', universe: s.universe, tariff: s.tariff, spread: s.spread, capital: s.capital, slots: s.slots, go: s.go, leverage: s.leverage, go_mult: s.go_mult, since: s.since, until: s.until });
                  }} />
              </div>
            </>}
        </div>
        {editor && meta && <Editor meta={meta} state={prefs.editor} saved={prefs.rules} onState={e => set({ editor: e })} onSaved={r => set({ rules: r })}
          onRun={startRun} onClose={() => setEditor(false)} busy={busy} error={runErr} />}
      </div>
      {search && <SymbolSearch rows={symRows} current={cell.st} recent={prefs.recent} onPick={pickSymbol} onClose={() => setSearch(false)} />}
    </div>
  );
}
const EMPTY_T: BtTrade[] = []; const EMPTY_L: BtLiveTrade[] = [];
