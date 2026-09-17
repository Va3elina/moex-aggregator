/* eslint-disable @typescript-eslint/no-explicit-any -- результат прогона — свободный JSON движка */
// Стенд: «Тестер стратегий» — нижняя панель. Состав как у TradingView (основные данные, динамика, анализ результатов,
// анализ сделок, список сделок) + наше: журнал решений, сравнение прогонов, загрузка ГО. Любой блок можно скрыть.
import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { btApi, type BtCandle, type BtEquity, type BtRealism, type BtRun, type BtSignal, type BtTrade } from './api';
import { EquityChart } from './ChartCell';
import { cls, fmtDate, money, num, pct, signed } from './lib';
import { byCalendar, byPeriod, compute, histogram, pnlOf, type Bucket, type Stats } from './stats';
import type { Prefs } from './usePrefs';
import { Bars, Donut, Logo, Menu, MenuItem } from './ui';

const BLOCKS: [string, string][] = [['main', 'Основные данные'], ['dyn', 'Динамика'], ['res', 'Анализ результатов'], ['trd', 'Анализ сделок'], ['inst', 'По бумагам'], ['cond', 'Условия прогона']];
const STATUS: Record<string, string> = { queued: 'в очереди', running: 'считается…', done: '', error: 'ошибка' };

function Metric({ label, children, sub, c }: { label: string; children: ReactNode; sub?: ReactNode; c?: string }) {
  return <div className="bt-metric"><small>{label}</small><b className={c}>{children}</b>{sub != null && <span className="bt-dim">{sub}</span>}</div>;
}
function Block({ id, title, hidden, onHide, right, children }: { id: string; title: string; hidden: string[]; onHide: (id: string) => void; right?: ReactNode; children: ReactNode }) {
  if (hidden.includes(id)) return null;
  return <section className="bt-block"><h3>{title}<span className="bt-block-r">{right}<button className="bt-x" title="скрыть блок" onClick={() => onHide(id)}>✕</button></span></h3>{children}</section>;
}
const Tabs = ({ items, value, onChange }: { items: [string, string][]; value: string; onChange: (v: string) => void }) =>
  <div className="bt-pills">{items.map(([k, l]) => <button key={k} className={k === value ? 'on' : ''} onClick={() => onChange(k)}>{l}</button>)}</div>;
const Seg = ({ items, value, onChange }: { items: [string, string][]; value: string; onChange: (v: string) => void }) =>
  <div className="bt-seg">{items.map(([k, l]) => <button key={k} className={k === value ? 'on' : ''} onClick={() => onChange(k)}>{l}</button>)}</div>;

export default function Tester({ runs, run, trades, equity, st, name, names, prefs, set, onPickTrade, selKey, onDeleteRun, onOpenEditor, compare }: {
  runs: BtRun[]; run: BtRun | null; trades: BtTrade[]; equity: BtEquity[]; st: string; name: string; names: Map<string, string>;
  prefs: Prefs; set: (p: Partial<Prefs>) => void; onPickTrade: (t: BtTrade) => void; selKey: string | null;
  onDeleteRun: (id: number) => void; onOpenEditor: () => void; compare: { run: BtRun; equity: BtEquity[] } | null;
}) {
  const acc = run?.result?.account; const money_ = !!acc; const capital = run?.spec_full?.capital ?? 1_000_000;
  const symbol = prefs.scope === 'symbol';
  const scoped = useMemo(() => symbol ? trades.filter(t => t.st === st) : trades, [trades, symbol, st]);
  const S: Stats = useMemo(() => compute(scoped, symbol ? null : equity, capital, money_), [scoped, equity, symbol, capital, money_]);
  const hide = (id: string) => set({ hidden: [...prefs.hidden, id] });
  const v = (x: number | null | undefined) => money_ ? money(x) : pct(x);
  const share = (x: number) => money_ ? pct(x / capital) : '';
  const runLabel = (r: BtRun) => r.name || (typeof r.spec.rule === 'string' ? r.spec.rule : r.spec.rule?.name ?? r.spec.rule?.id) || `прогон ${r.id}`;

  return (
    <div className="bt-tester">
      <div className="bt-thead">
        <Menu label={<><b>{run ? runLabel(run) : 'Прогонов пока нет'}</b> ▾</>} title="прогоны">{close => <>
          <div className="bt-pop-h">Прогоны</div>
          {runs.map(r => (
            <div key={r.id} className="bt-mi-row">
              <MenuItem on={r.id === run?.id} onClick={() => { set({ runId: r.id }); close(); }} hint={r.status === 'done' ? `${num(r.summary?.['сделок'])} сд. · ${r.summary?.['годовых_%'] ?? '—'}% год.` : STATUS[r.status]}>#{r.id} · {runLabel(r)} · {r.spec.exec}</MenuItem>
              <button className="bt-x" title="удалить прогон" onClick={() => { if (confirm(`Удалить прогон #${r.id}?`)) onDeleteRun(r.id); }}>✕</button>
            </div>
          ))}
          <div className="bt-pop-sep" /><MenuItem onClick={() => { onOpenEditor(); close(); }}>Новая стратегия в редакторе…</MenuItem>
        </>}</Menu>
        {run && run.status !== 'done' && <span className={`bt-status ${run.status}`}>{STATUS[run.status]}</span>}
        <Seg items={[['symbol', name], ['portfolio', 'Весь счёт']]} value={prefs.scope} onChange={k => set({ scope: k as Prefs['scope'] })} />
        {run?.result?.period && <span className="bt-chip2">{fmtDate(run.result.period[0])} — {fmtDate(run.result.period[1])}</span>}
        {money_ && <span className="bt-chip2">{num(capital / 1e6, 2)} млн ₽ · {run?.spec_full?.slots} сделок в день</span>}
        <span style={{ flex: 1 }} />
        <Seg items={[['overview', 'Обзор'], ['trades', `Список сделок`], ['signals', 'Сигналы']]} value={prefs.view} onChange={k => set({ view: k as Prefs['view'] })} />
        <Menu label="⚙" title="что показывать" align="right">{() => <>
          <div className="bt-pop-h">Блоки обзора</div>
          {BLOCKS.map(([id, l]) => <MenuItem key={id} on={!prefs.hidden.includes(id)} onClick={() => set({ hidden: prefs.hidden.includes(id) ? prefs.hidden.filter(x => x !== id) : [...prefs.hidden, id] })}>{l}</MenuItem>)}
          <div className="bt-pop-sep" /><div className="bt-pop-h">Сравнить с прогоном</div>
          <MenuItem on={!prefs.compareRunId} onClick={() => set({ compareRunId: null })}>не сравнивать</MenuItem>
          {runs.filter(r => r.status === 'done' && r.id !== run?.id).map(r => <MenuItem key={r.id} on={prefs.compareRunId === r.id} onClick={() => set({ compareRunId: r.id })}>#{r.id} · {runLabel(r)}</MenuItem>)}
        </>}</Menu>
        <button className="bt-tb" title={prefs.panel === 'max' ? 'обычный размер' : 'на весь экран'} onClick={() => set({ panel: prefs.panel === 'max' ? 'open' : 'max' })}>{prefs.panel === 'max' ? '⤡' : '⤢'}</button>
        <button className="bt-tb" title="свернуть" onClick={() => set({ panel: 'closed' })}>—</button>
      </div>
      <div className="bt-tbody">
        {!run ? <div className="bt-nodata big">Прогонов пока нет. Открой редактор <b>{'{ }'}</b> в шапке, выбери стратегию и нажми «Запустить».</div>
          : run.status === 'error' ? <div className="bt-nodata big bt-down">Прогон упал: {run.error}</div>
          : run.status !== 'done' ? <div className="bt-nodata big"><div className="bt-spinner" />Прогон считается… обычно 20–30 секунд.</div>
          : prefs.view === 'trades' ? <TradesList rows={scoped} money={money_} names={names} symbol={symbol} selKey={selKey} onPick={onPickTrade} onlyExec={prefs.tradesOnlyExecuted} setOnlyExec={x => set({ tradesOnlyExecuted: x })} />
          : prefs.view === 'signals' ? <Signals runId={run.id} st={st} name={name} />
          : <>
            <Block id="main" title="Основные данные" hidden={prefs.hidden} onHide={hide}>
              <div className="bt-metrics">
                <Metric label="Общие ПР/УБ" c={cls(S.total)} sub={share(S.total)}>{v(S.total)}</Metric>
                <Metric label="Макс. просадка" c="bt-down" sub={money_ ? pct(-S.maxDdPct) : S.maxDdDate ?? ''}>{money_ ? money(-S.maxDd) : pct(-S.maxDd)}</Metric>
                <Metric label="Прибыльные сделки" sub={`${S.wins} из ${S.n}`}>{S.n ? num(100 * S.wins / S.n, 2) + '%' : '—'}</Metric>
                <Metric label="Фактор прибыли">{num(S.pf, 2)}</Metric>
                {money_ && <Metric label="Годовая доходность" c={cls(S.cagr)}>{pct(S.cagr)}</Metric>}
                {money_ && <Metric label="Коэффициент Шарпа">{num(S.sharpe, 2)}</Metric>}
                <Metric label="В среднем на сделку" c={cls(S.avgRet)} sub="после издержек">{pct(S.avgRet, 3)}</Metric>
                <Metric label="Комиссии и спред" sub={money_ && S.grossProfit ? `${num(100 * S.costs / S.grossProfit, 0)}% валовой прибыли` : ''}>{money_ ? num(S.costs) + ' ₽' : pct(S.costs)}</Metric>
              </div>
            </Block>
            <Block id="dyn" title="Динамика" hidden={prefs.hidden} onHide={hide} right={compare && <span className="bt-dim">пунктир — #{compare.run.id} {runLabel(compare.run)}</span>}>
              <Dynamics S={S} compare={symbol ? null : compare} money={money_} />
            </Block>
            <Block id="res" title="Анализ результатов" hidden={prefs.hidden} onHide={hide}>
              <Tabs items={[['dist', 'Распределение'], ['period', 'За период'], ['cmp', 'Сравнение'], ['margin', 'Использование ГО'], ['real', 'Реализм'], ['dd', 'Рост и спад']]} value={prefs.resultTab} onChange={k => set({ resultTab: k })} />
              {prefs.resultTab === 'dist' && <>
                <div className="bt-metrics"><Metric label="Валовая прибыль" c="bt-up" sub={share(S.grossProfit)}>{v(S.grossProfit)}</Metric><Metric label="Валовый убыток" c="bt-down" sub={share(-S.grossLoss)}>{v(-S.grossLoss)}</Metric>
                  <Metric label="Профит-фактор">{num(S.pf, 2)}</Metric><Metric label="Комиссионная нагрузка" sub="комиссии и спред к валовой прибыли">{S.grossProfit ? num(100 * S.costs / S.grossProfit, 1) + '%' : '—'}</Metric></div>
                <h4>Прибыль и убыток по направлению</h4>
                <HBars rows={[['Все сделки', S.total, S.n], ['Шорт', S.shortPnl, S.shortN], ['Лонг', S.longPnl, S.longN]]} fmt={v} />
              </>}
              {prefs.resultTab === 'period' && <>
                <div className="bt-metrics"><Metric label="Годовая доходность (СГТР)" c={cls(S.cagr)}>{pct(S.cagr)}</Metric><Metric label="Общая рентабельность" c={cls(S.totalRet)}>{pct(S.totalRet)}</Metric>
                  <Metric label="Коэффициент Шарпа">{num(S.sharpe, 2)}</Metric><Metric label="Коэффициент Сортино">{num(S.sortino, 2)}</Metric></div>
                <h4>ПР/УБ за период<Seg items={[['day', 'День'], ['week', 'Неделя'], ['month', 'Месяц'], ['quarter', 'Квартал'], ['year', 'Год']]} value={prefs.bucket} onChange={k => set({ bucket: k })} /></h4>
                <Bars data={byPeriod(scoped, money_, prefs.bucket as Bucket).slice(-120)} fmt={v} />
              </>}
              {prefs.resultTab === 'cmp' && <Comparison S={S} run={run} compare={compare} st={st} name={name} symbol={symbol} money={money_} />}
              {prefs.resultTab === 'margin' && <Margin equity={equity} acc={acc} symbol={symbol} S={S} />}
              {prefs.resultTab === 'real' && <Realism st={st} name={name} run={run} />}
              {prefs.resultTab === 'dd' && <>
                <div className="bt-metrics"><Metric label="Средняя продолжительность роста">{S.avgUpDays != null ? num(S.avgUpDays) + ' дн.' : '—'}</Metric><Metric label="Средняя продолжительность просадки">{S.avgDdDays != null ? num(S.avgDdDays) + ' дн.' : '—'}</Metric>
                  <Metric label="Макс. просадка" c="bt-down" sub={S.maxDdDate ? 'дно ' + fmtDate(S.maxDdDate) : ''}>{money_ ? money(-S.maxDd) : pct(-S.maxDd)}</Metric><Metric label="Макс. просадка, % от пика" c="bt-down">{money_ ? pct(-S.maxDdPct) : '—'}</Metric></div>
                <h4>Самые глубокие просадки</h4>
                <table className="bt-table"><thead><tr><th className="l">Начало</th><th className="l">Восстановление</th><th>Дней</th><th>Глубина</th></tr></thead><tbody>
                  {S.ddSpans.slice(0, 8).map(d => <tr key={d.from + d.to}><td className="l">{fmtDate(d.from)}</td><td className="l">{fmtDate(d.to)}</td><td>{d.days}</td><td className="bt-down">{money_ ? money(-d.depth) : pct(-d.depth)}</td></tr>)}
                  {!S.ddSpans.length && <tr><td colSpan={4} className="l bt-dim">просадок не было</td></tr>}
                </tbody></table>
              </>}
            </Block>
            <Block id="trd" title="Анализ сделок" hidden={prefs.hidden} onHide={hide}>
              <Tabs items={[['dist', 'Распределение'], ['streak', 'Серии'], ['time', 'Временные закономерности']]} value={prefs.tradeTab} onChange={k => set({ tradeTab: k })} />
              {prefs.tradeTab === 'dist' && <>
                <div className="bt-metrics"><Metric label="Ожидание" c={cls(S.expectancy)} sub={pct(S.avgRet, 3)}>{v(S.expectancy)}</Metric><Metric label="Средняя прибыль / убыток">{v(S.avgWin)} / {v(S.avgLoss)}</Metric>
                  <Metric label="Макс. прибыль" c="bt-up">{v(S.maxWin)}</Metric><Metric label="Макс. убыток" c="bt-down">{v(S.maxLoss)}</Metric></div>
                <div className="bt-two"><div><h4>Распределение доходности сделок, %</h4><Hist trades={scoped} /></div>
                  <div><h4>Распределение сделок</h4><Donut parts={[{ label: 'Прибыльные', n: S.wins, color: '#26a69a' }, { label: 'Убыточные', n: S.losses, color: '#ef5350' }, { label: 'Безубыточные', n: S.flats, color: '#f5a524' }]} /></div></div>
              </>}
              {prefs.tradeTab === 'streak' && <>
                <div className="bt-metrics"><Metric label="Самая длинная прибыльная серия">{S.bestStreak} сделок</Metric><Metric label="Самая длинная убыточная серия">{S.worstStreak} сделок</Metric>
                  <Metric label="Средняя прибыльная серия">{num(S.avgWinStreak, 1)}</Metric><Metric label="Средняя убыточная серия">{num(S.avgLossStreak, 1)}</Metric></div>
                <h4>Серии прибыльных и убыточных сделок (последние 150)</h4>
                <Bars data={S.streaks.slice(-150).map((x, i) => ({ k: String(i + 1), v: x }))} fmt={x => `${Math.abs(x)} подряд`} />
              </>}
              {prefs.tradeTab === 'time' && <TimePatterns trades={scoped} money={money_} kind={prefs.calKind} setKind={k => set({ calKind: k })} S={S} fmt={v} />}
            </Block>
            <Block id="inst" title="По бумагам" hidden={prefs.hidden} onHide={hide}>
              <table className="bt-table"><thead><tr><th className="l">Бумага</th><th>Сделок</th><th>На сделку</th><th>Винрейт</th>{money_ && <th>Итог, ₽</th>}</tr></thead><tbody>
                {(run.result?.by_instrument ?? []).map((r: any) => { const rub = trades.filter(t => t.st === r.st).reduce((a, t) => a + (t.pnl_rub ?? 0), 0); return (
                  <tr key={r.st} className={r.st === st ? 'on' : ''}><td className="l"><span className="bt-inline"><Logo st={r.st} size={18} /><b>{r.st}</b> <span className="bt-dim">{names.get(r.st)}</span></span></td>
                    <td>{num(r['сделок'])}</td><td className={cls(r['чистыми_%'])}>{signed(r['чистыми_%'], 3)}%</td><td>{r['винрейт_%']}%</td>{money_ && <td className={cls(rub)}>{money(rub)}</td>}</tr>); })}
              </tbody></table>
            </Block>
            <Block id="cond" title="Условия прогона" hidden={prefs.hidden} onHide={hide}><Conditions run={run} S={S} money={money_} /></Block>
          </>}
      </div>
    </div>
  );
}

function Dynamics({ S, compare, money }: { S: Stats; compare: { run: BtRun; equity: BtEquity[] } | null; money: boolean }) {
  const lines = useMemo(() => {
    const l = [{ name: 'капитал', color: '#26a69a', points: S.curve }];
    if (compare) l.push({ name: `#${compare.run.id}`, color: '#f5a524', points: compare.equity.map(e => ({ d: e.d, v: e.equity })) });
    return l;
  }, [S.curve, compare]);
  if (S.curve.length < 2) return <div className="bt-nodata">Недостаточно данных</div>;
  return <><EquityChart lines={lines} dd={S.dd} /><div className="bt-dim bt-foot">{money ? 'Линия — капитал, ₽. ' : 'Линия — накопленный результат на 1 контракт. '}Красные столбики внизу — просадка от максимума, %.</div></>;
}

function HBars({ rows, fmt }: { rows: [string, number, number][]; fmt: (v: number) => string }) {
  const max = Math.max(...rows.map(r => Math.abs(r[1])), 1e-12);
  return <div className="bt-hbars">{rows.map(([l, v_, n]) => <div key={l}><span>{l}<small> · {n}</small></span><div><i className={v_ >= 0 ? 'up' : 'down'} style={{ width: `${100 * Math.abs(v_) / max}%` }} /></div><b className={cls(v_)}>{fmt(v_)}</b></div>)}</div>;
}

function Hist({ trades }: { trades: BtTrade[] }) {
  const h = histogram(trades); if (!h.length) return <div className="bt-nodata">Недостаточно данных</div>;
  return <Bars data={h.map(b => ({ k: `${num((b.from + b.to) / 2, 1)}%`, v: b.to <= 0 ? -b.n : b.n }))} fmt={x => `${Math.abs(x)} сделок`} />;
}

function TimePatterns({ trades, money, kind, setKind, S, fmt }: { trades: BtTrade[]; money: boolean; kind: string; setKind: (k: string) => void; S: Stats; fmt: (v: number) => string }) {
  const best = (k: 'weekday' | 'month') => { const a = byCalendar(trades, money, k).filter(x => x.n); return a.length ? a.reduce((x, y) => y.v > x.v ? y : x).k : '—'; };
  return <>
    <div className="bt-metrics"><Metric label="Лучший день недели для входа">{best('weekday')}</Metric><Metric label="Лучший месяц для входа">{best('month')}</Metric>
      <Metric label="Средняя продолжительность сделки" sub="календарных, с выходными">{num(S.avgDays, 1)} дн.</Metric><Metric label="Час входа" sub="задан правилом">по правилу</Metric></div>
    <h4>Результат по времени входа<Seg items={[['weekday', 'Дни недели'], ['month', 'Месяцы'], ['year', 'Годы']]} value={kind} onChange={setKind} /></h4>
    <Bars data={byCalendar(trades, money, kind as 'weekday')} fmt={fmt} />
  </>;
}

function Margin({ equity, acc, symbol, S }: { equity: BtEquity[]; acc: any; symbol: boolean; S: Stats }) {
  const lines = useMemo(() => [{ name: 'ГО', color: '#4c8dff', points: equity.map(e => ({ d: e.d, v: e.equity ? 100 * e.go_used / e.equity : 0 })) },
    { name: 'позиции', color: '#f5a524', points: equity.map(e => ({ d: e.d, v: e.equity ? 100 * e.notional / e.equity : 0 })) }], [equity]);
  if (!acc) return <div className="bt-nodata">Счёт в этом прогоне не считался</div>;
  const used = equity.filter(e => e.go_used > 0);
  return <>
    <div className="bt-metrics"><Metric label="ГО, максимум" sub="доля капитала под залогом">{acc['ГО_макс_%']}%</Metric><Metric label="ГО, в среднем в дни с позицией">{used.length ? num(used.reduce((a, e) => a + 100 * e.go_used / e.equity, 0) / used.length, 1) + '%' : '—'}</Metric>
      <Metric label="Загрузка капитала, в среднем" sub="стоимость позиций к капиталу">{acc['загрузка_средняя_%']}%</Metric>
      <Metric label="Маржин-коллы" c={acc['маржин_коллов'] ? 'bt-down' : ''} sub={acc['маржин_коллов'] ? 'утром капитал был меньше требуемого ГО' : 'капитал всегда покрывал ГО'}>{num(acc['маржин_коллов'] ?? 0)}</Metric>
      <Metric label="Сделок урезано по ГО" sub="объём меньше расчётного: не хватило свободного ГО">{num(acc['урезано_по_ГО'] ?? 0)}</Metric></div>
    <h4>Использование капитала, % {symbol && <span className="bt-dim">— показано по всему счёту</span>}</h4>
    <EquityChart lines={lines} height={220} /><div className="bt-dim bt-foot">Синяя — залог биржи (ГО), жёлтая пунктирная — стоимость открытых позиций. Прибыль на 1 ₽ среднего залога: {used.length ? num(S.total / (used.reduce((a, e) => a + e.go_used, 0) / used.length), 2) + ' ₽' : '—'}.</div>
  </>;
}

function Realism({ st, name, run }: { st: string; name: string; run: BtRun }) {
  const [r, setR] = useState<BtRealism | null>(null);
  useEffect(() => { let dead = false; setR(null); btApi.realism(st).then(x => { if (!dead) setR(x); }).catch(() => undefined); return () => { dead = true; }; }, [st]);
  const go = useMemo(() => r ? [{ name: 'ГО', color: '#4c8dff', points: r.go_rate.map(([d, v]) => ({ d, v })) }] : [], [r]);
  const rpp = useMemo(() => r ? [{ name: '₽', color: '#f5a524', points: r.rub_per_point.map(([d, v]) => ({ d, v })) }] : [], [r]);
  const sp = useMemo(() => r ? [{ name: 'спред', color: '#c678dd', points: r.spread.map(([d, v]) => ({ d, v })) }] : [], [r]);
  if (!r) return <div className="bt-nodata"><div className="bt-spinner" /></div>;
  const s = run.spec_full ?? {};
  return <>
    <div className="bt-metrics">
      <Metric label={`ГО сейчас, % стоимости · ${name}`} sub={`биржа × надбавка брокера ${num(r.broker_coef, 2)}`}>{r.go_rate.length ? num(r.go_rate[r.go_rate.length - 1][1], 1) + '%' : '—'}</Metric>
      <Metric label="ГО, максимум за историю" sub={r.go_rate.length ? fmtDate(r.go_rate.reduce((a, b) => b[1] > a[1] ? b : a)[0]) : ''}>{r.go_rate.length ? num(Math.max(...r.go_rate.map(x => x[1])), 1) + '%' : '—'}</Metric>
      <Metric label="Стоимость пункта цены" sub={r.rub_per_point.length ? 'по дням, из данных биржи' : 'постоянная: контракт в рублях'}>{num(r.rub_per_point.length ? r.rub_per_point[r.rub_per_point.length - 1][1] : r.rub_per_point_const, 2)} ₽</Metric>
      <Metric label="Спред стакана за круг" sub={r.spread.length ? 'по дням, Algopack' : 'одно число: медиана 09.2025–09.2026'}>{num(r.spread_const, 3)}%</Metric>
    </div>
    <h4>Ставка ГО, % от стоимости контракта <span className="bt-dim">ставка рыночного риска МосБиржи · в этом прогоне: {s.go}{s.go_mult > 1 ? `, стресс ×${s.go_mult}` : ''}{s.leverage > 1 ? `, плечо ×${s.leverage}` : ''}</span></h4>
    {go[0]?.points.length ? <EquityChart lines={go} height={200} /> : <div className="bt-nodata">нет данных</div>}
    {!!rpp[0]?.points.length && <><h4>Рублей за 1 пункт цены <span className="bt-dim">открытые позиции в рублях ÷ в контрактах ÷ расчётная цена — курс, по которому биржа считала вариационную маржу</span></h4><EquityChart lines={rpp} height={200} /></>}
    {!!sp[0]?.points.length && <><h4>Спред за круг (3 уровня стакана), %</h4><EquityChart lines={sp} height={200} /></>}
  </>;
}

function Comparison({ S, run, compare, st, name, symbol, money }: { S: Stats; run: BtRun; compare: { run: BtRun; equity: BtEquity[] } | null; st: string; name: string; symbol: boolean; money: boolean }) {
  const [bh, setBh] = useState<BtCandle[] | null>(null);
  const [a, b] = run.result?.period ?? ['2019-01-01', '2099-01-01'];
  useEffect(() => { let dead = false; btApi.candles(symbol ? st : 'MX', 1440, '2019-01-01', new Date().toISOString().slice(0, 10)).then(c => { if (!dead) setBh(c); }).catch(() => undefined); return () => { dead = true; }; }, [st, symbol]);
  const hold = useMemo(() => {
    if (!bh) return null; const from = Date.parse(a) / 1000, to = Date.parse(b) / 1000 + 86400; const x = bh.filter(c => c.time >= from && c.time <= to);
    return x.length > 1 ? x[x.length - 1].close / x[0].open - 1 : null;
  }, [bh, a, b]);
  const c = compare?.run.result;
  return <>
    <div className="bt-metrics"><Metric label="Доходность стратегии" c={cls(S.totalRet)}>{pct(S.totalRet)}</Metric>
      <Metric label={`Купить и держать: ${symbol ? name : 'индекс МосБиржи'}`} c={cls(hold)} sub="склеенный фьючерс, без плеча">{pct(hold)}</Metric>
      <Metric label="Опережение стратегии" c={cls(hold != null ? S.totalRet - hold : null)}>{hold != null ? pct(S.totalRet - hold) : '—'}</Metric>
      <Metric label="Макс. просадка стратегии" c="bt-down">{money ? pct(-S.maxDdPct) : pct(-S.maxDd)}</Metric></div>
    <h4>Сравнение с другим прогоном {!compare && <span className="bt-dim">— выбери прогон в меню ⚙ справа вверху</span>}</h4>
    {compare && c && <table className="bt-table"><thead><tr><th className="l">Показатель</th><th>#{run.id} этот</th><th>#{compare.run.id} {compare.run.name ?? ''}</th></tr></thead><tbody>
      {[['Сделок', 'per_trade', 'сделок'], ['На сделку после издержек, %', 'per_trade', 'чистыми_%'], ['Винрейт, %', 'per_trade', 'винрейт_%'], ['Профит-фактор', 'per_trade', 'профит_фактор'], ['Годовых, %', 'account', 'годовых_%'], ['Макс. просадка, %', 'account', 'просадка_%'], ['Шарп', 'account', 'Шарп'], ['Прибыль, ₽', 'account', 'прибыль_руб'], ['Комиссии, ₽', 'account', 'комиссии_руб'], ['Спред, ₽', 'account', 'спред_руб']]
        .map(([l, g, k]) => <tr key={l}><td className="l">{l}</td><td>{num(run.result?.[g]?.[k], 2)}</td><td>{num(c[g]?.[k], 2)}</td></tr>)}
    </tbody></table>}
  </>;
}

function Conditions({ run, S, money }: { run: BtRun; S: Stats; money: boolean }) {
  const s = run.spec_full ?? {}; const r = s.rule ?? {}; const acc = run.result?.account;
  const rows: [string, ReactNode][] = [['Стратегия', r.name ?? r.id], ['Сигнал', `ход ${r.signal?.from} → ${r.signal?.to}`], ['Вход / выход', `${r.entry?.price === 'next_open' ? 'открытие следующей свечи' : 'закрытие свечи'}, задержка ${r.entry?.delay_min ?? 0} / ${r.exit?.delay_min ?? 0} мин; выход в ${r.exit?.at}`],
    ['Тариф / спред / ГО', `${s.tariff} / ${s.spread} / ${s.go}`], ['Бумаги', (s.universe ?? []).join(', ')], ['Данные по', String(run.result?.data_until ?? '').slice(0, 16)], ['Сигналов / сделок', `${num(run.result?.signals)} / ${num(run.result?.per_trade?.['сделок'])}`]];
  if (money) rows.push(['Исполнено на счёте', num(acc?.['сделок_исполнено'])], ...Object.entries(S.skipped).map(([k, n]) => [`Пропущено: ${k}`, num(n)] as [string, ReactNode]), ...Object.entries(acc?.['по_годам_%'] ?? {}).map(([y, x]) => [`${y} год`, <span className={cls(x as number)}>{signed(x as number, 1)}%</span>] as [string, ReactNode]));
  return <table className="bt-table kv"><tbody>{rows.map(([k, x]) => <tr key={k}><td className="l bt-dim">{k}</td><td className="l">{x}</td></tr>)}</tbody></table>;
}

function TradesList({ rows, money, names, symbol, selKey, onPick, onlyExec, setOnlyExec }: { rows: BtTrade[]; money: boolean; names: Map<string, string>; symbol: boolean; selKey: string | null; onPick: (t: BtTrade) => void; onlyExec: boolean; setOnlyExec: (x: boolean) => void }) {
  const [limit, setLimit] = useState(300);
  const list = useMemo(() => { const x = (onlyExec && money ? rows.filter(t => !t.account_skip) : rows).slice().sort((a, b) => a.d < b.d ? 1 : a.d > b.d ? -1 : 0); let cum = 0; const asc = [...x].reverse(); const cumMap = new Map<string, number>(); for (const t of asc) { if (!(money && t.account_skip)) cum += pnlOf(t, money); cumMap.set(t.st + t.d, cum); } return x.map((t, i) => ({ t, no: x.length - i, cum: cumMap.get(t.st + t.d) ?? 0 })); }, [rows, onlyExec, money]);
  if (!rows.length) return <div className="bt-nodata big">Сделок нет</div>;
  return (
    <div className="bt-tradelist">
      <div className="bt-listbar"><b>Список сделок</b><span className="bt-dim">{list.length}</span>{money && <label><input type="checkbox" checked={onlyExec} onChange={e => setOnlyExec(e.target.checked)} /> только исполненные на счёте</label>}<span className="bt-dim">клик по сделке — показать на графике</span></div>
      <table className="bt-table tv"><thead><tr><th className="l">№ сделки</th>{!symbol && <th className="l">Бумага</th>}<th className="l">Тип</th><th className="l">Дата</th><th>Цена</th><th>Ход дня / порог</th>{money && <th>Размер</th>}<th>Чистая ПР/УБ</th><th>Доходность</th><th>Накопленная ПР/УБ</th></tr></thead>
        <tbody>{list.slice(0, limit).map(({ t, no, cum }) => { const k = `${t.st}|${t.d}`, sk = money && !!t.account_skip; return (
          <tr key={k} className={`${k === selKey ? 'on' : ''} ${sk ? 'skipped' : ''}`} onClick={() => onPick(t)}>
            <td className="l"><span className="bt-dim">{no}</span> <b className={t.side > 0 ? 'bt-long' : 'bt-down'}>{t.side > 0 ? 'Длинная' : 'Короткая'}</b></td>
            {!symbol && <td className="l"><span className="bt-inline"><Logo st={t.st} size={18} /><b>{t.st}</b> <span className="bt-dim">{names.get(t.st)}</span></span></td>}
            <td className="l two"><div>Выход</div><div>Вход</div></td><td className="l two"><div>{fmtDate(t.d_out)}</div><div>{fmtDate(t.d)}</div></td>
            <td className="two"><div>{num(t.px_out, 2)}</div><div>{num(t.px_in, 2)}</div></td>
            <td className="two"><div>{pct(t.move)}</div><div className="bt-dim">{t.thr != null ? pct(t.side > 0 ? t.thr : -t.thr) : '—'}</div></td>
            {money && <td className="two">{sk ? <div className="bt-dim">{t.account_skip}</div> : <><div>{num(t.qty)} контр.</div><div className="bt-dim">{num(t.notional)} ₽ · ГО {num(t.go)} ₽</div></>}</td>}
            <td className={`two ${cls(sk ? null : pnlOf(t, money))}`}>{sk ? '—' : <><div>{money ? money_(t.pnl_rub) : pct(t.net)}</div><div className="bt-dim">издержки {money ? num((t.comm_rub ?? 0) + (t.spread_rub ?? 0)) + ' ₽' : pct(-(t.comm + t.spread))}</div></>}</td>
            <td className={cls(t.net)}>{pct(t.net)}</td><td className={cls(cum)}>{money ? money_(cum) : pct(cum)}</td>
          </tr>); })}</tbody></table>
      {list.length > limit && <button className="bt-more" onClick={() => setLimit(l => l + 1000)}>показать ещё ({list.length - limit})</button>}
    </div>
  );
}
const money_ = (v: number | null | undefined) => money(v);

function Signals({ runId, st, name }: { runId: number; st: string; name: string }) {
  const [rows, setRows] = useState<BtSignal[] | null>(null); const [only, setOnly] = useState(true);
  useEffect(() => { let dead = false; btApi.signals(runId, st).then(r => { if (!dead) setRows(r); }).catch(() => { if (!dead) setRows([]); }); return () => { dead = true; }; }, [runId, st]);
  if (!rows) return <div className="bt-nodata big"><div className="bt-spinner" /></div>;
  const list = (only ? rows.filter(r => r.side !== 0) : rows).slice().reverse().slice(0, 1500);
  return (
    <div className="bt-tradelist">
      <div className="bt-listbar"><b>Журнал решений · {name}</b><span className="bt-dim">{list.length}</span><label><input type="checkbox" checked={only} onChange={e => setOnly(e.target.checked)} /> только дни с сигналом</label><span className="bt-dim">каждый торговый день: ход, пороги и почему есть или нет сделки</span></div>
      <table className="bt-table"><thead><tr><th className="l">День</th><th className="l">Контракт</th><th>Цена начала окна</th><th>Цена конца окна</th><th>Ход</th><th>Порог лонга</th><th>Порог шорта</th><th className="l">Решение</th></tr></thead>
        <tbody>{list.map(r => <tr key={r.d} className={r.side === 0 ? 'skipped' : ''}><td className="l">{fmtDate(r.d)}</td><td className="l">{r.secid}</td><td>{num(r.pa, 2)}</td><td>{num(r.pb, 2)}</td><td className={cls(r.move)}>{pct(r.move)}</td>
          <td>{r.thr_up != null ? pct(r.thr_up) : 'мало истории'}</td><td>{r.thr_dn != null ? pct(-r.thr_dn) : 'мало истории'}</td>
          <td className="l">{r.side === 0 ? <span className="bt-dim">нет сигнала</span> : <><b className={r.side > 0 ? 'bt-long' : 'bt-down'}>{r.side > 0 ? 'Лонг' : 'Шорт'}</b>{!r.tradable && <span className="bt-dim"> · без сделки: {r.skip}</span>}</>}</td></tr>)}</tbody></table>
    </div>
  );
}
