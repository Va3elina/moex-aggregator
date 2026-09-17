/* eslint-disable @typescript-eslint/no-explicit-any -- спецификация прогона — свободный JSON движка */
// Стенд — свой бэктест-терминал (/admin/backtest, только админы). Устройство как у TradingView: тонкая шапка
// (бумага · таймфрейм · индикаторы · редактор стратегии · сетка), графики, снизу «Тестер стратегий».
// План: backtest/PLAN.md. Расчёт — контейнер bt-worker; страница только показывает и помнит, как её оставили.
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { btApi, type BtEquity, type BtLiveTrade, type BtMeta, type BtRun, type BtTrade } from './api';
import ChartCell, { type Focus } from './ChartCell';
import Editor from './Editor';
import SymbolSearch, { type SymRow } from './SymbolSearch';
import Tester, { tradeKey } from './Tester';
import { paramLabel, setPath } from './Sweep';
import { TFS } from './lib';
import { usePrefs, type IndCfg, type IndKind, type Layout, type Prefs } from './usePrefs';
import { Logo, Menu, MenuItem } from './ui';
import './bt.css';

const IND_LIST: { kind: IndKind; label: string; length: number; mult?: number; where: string }[] = [
  { kind: 'sma', label: 'Скользящая средняя (SMA)', length: 20, where: 'на цене' }, { kind: 'ema', label: 'Экспоненциальная средняя (EMA)', length: 20, where: 'на цене' },
  { kind: 'wma', label: 'Взвешенная средняя (WMA)', length: 20, where: 'на цене' }, { kind: 'bb', label: 'Полосы Боллинджера', length: 20, mult: 2, where: 'на цене' },
  { kind: 'rsi', label: 'RSI', length: 14, where: 'панель' }, { kind: 'atr', label: 'ATR', length: 14, where: 'панель' },
];
const IND_COLORS = ['#f5a524', '#4c8dff', '#c678dd', '#56b6c2', '#e06c75', '#98c379'];
const LAYOUTS: [Layout, string, number][] = [['1', '▢  Один график', 1], ['2h', '◫  Два рядом', 2], ['2v', '⊟  Два друг под другом', 2], ['4', '⊞  Четыре', 4]];
const SHOW: [keyof Prefs['show'], string][] = [['markers', 'Стрелки сделок'], ['labels', 'Подписи у стрелок'], ['lines', 'Линии вход → выход'], ['window', 'Подсветка окна сигнала'], ['robot', 'Сделки робота (песочница)'], ['volume', 'Объём']];

export default function BacktestPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [sp, setSp] = useSearchParams();
  const isAdmin = !!user && user.role === 'admin';
  useEffect(() => { if (!authLoading && !isAdmin) navigate('/', { replace: true }); }, [authLoading, isAdmin, navigate]);

  const [prefs, set] = usePrefs(sp.get('ws'));
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
  const addIndicator = (k: typeof IND_LIST[number]) => set(p => ({ indicators: [...p.indicators, { id: `${k.kind}${Date.now()}`, kind: k.kind, length: k.length, mult: k.mult, color: IND_COLORS[p.indicators.length % IND_COLORS.length] }] }));
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
        {TFS.map(([v, l, full]) => <button key={v} title={full} className={`bt-tb ${cell.tf === v ? 'on' : ''}`} onClick={() => setCell(active, { tf: v })}>{l}</button>)}
        <i className="sep" />
        <Menu label={<>Индикаторы{prefs.indicators.length ? <sup>{prefs.indicators.length}</sup> : null}</>}>{close => <>
          <div className="bt-pop-h">Добавить на график</div>
          {IND_LIST.map(k => <MenuItem key={k.kind} onClick={() => { addIndicator(k); close(); }} hint={`${k.length} · ${k.where}`}>{k.label}</MenuItem>)}
          {!!prefs.indicators.length && <><div className="bt-pop-sep" /><MenuItem onClick={() => { set({ indicators: [] }); close(); }}>Убрать все</MenuItem></>}
        </>}</Menu>
        <button className={`bt-tb ${editor ? 'on' : ''}`} onClick={() => setEditor(e => !e)} title="Редактор стратегии: код правила, свойства счёта, запуск">{'{ }'} Стратегия</button>
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
                <ChartCell key={i} st={c.st} tf={c.tf} name={names.get(c.st) ?? c.st} active={nCells > 1 && i === active} rule={rule}
                  trades={tradesBySt.get(c.st) ?? EMPTY_T} live={liveBySt.get(c.st) ?? EMPTY_L} show={prefs.show} indicators={prefs.indicators}
                  focus={i === active ? focus : null} onActivate={() => { if (i !== active) set({ active: i }); }} onIndicators={(next: IndCfg[]) => set({ indicators: next })} />
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
