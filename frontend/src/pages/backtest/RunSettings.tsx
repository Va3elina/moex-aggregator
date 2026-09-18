/* eslint-disable @typescript-eslint/no-explicit-any -- спецификация прогона и правило — свободный JSON движка */
// Стенд: настройки стратегии прямо в шапке тестера — как окно «Настройки» стратегии в TradingView.
// Три кнопки: период · счёт (капитал, размер, издержки) · условия входа и риск. «Применить» = та же стратегия
// с новыми значениями считается заново; редактор кода получает те же значения, чтобы код и форма говорили одно.
import { useState } from 'react';
import type { BtMeta, BtRun } from './api';
import { fmtDate, iso, num } from './lib';
import { Menu } from './ui';

type Spec = Record<string, any>;
/** Спецификация, из которой прогон можно повторить: правило всегда целиком (а не именем пресета). */
// eslint-disable-next-line react-refresh/only-export-components
export const baseSpec = (run: BtRun): Spec => run.spec?.code ? { ...run.spec }
  : { ...run.spec, rule: run.spec?.rule && typeof run.spec.rule === 'object' ? run.spec.rule : run.spec_full?.rule };

const p100 = (v: any) => typeof v === 'number' ? +(100 * v).toFixed(4) : '';
const sideText = (s: any, word: string) => !s ? `${word} выкл.` : s.type === 'quantile' ? `${word} ${Math.round(100 * s.q)}%`
  : typeof s.value === 'object' ? `${word}: порог бумаги` : `${word} ${s.strict ? '>' : '≥'} ${num(100 * s.value, 2)}%`;

function Foot({ dirty, busy, onApply, close }: { dirty: boolean; busy: boolean; onApply: () => void; close: () => void }) {
  return <div className="bt-set-foot"><button className="bt-tb" onClick={close}>Отмена</button><button className="bt-run" disabled={!dirty || busy} onClick={() => { onApply(); close(); }}>{busy ? 'Считается…' : 'Применить и пересчитать'}</button></div>;
}

function Period({ spec, busy, onApply, close }: { spec: Spec; busy: boolean; onApply: (s: Spec) => void; close: () => void }) {
  const [since, setSince] = useState<string>(spec.since ?? ''); const [until, setUntil] = useState<string>(spec.until ?? '');
  const back = (years: number) => { const d = new Date(); d.setFullYear(d.getFullYear() - years); setSince(iso(d)); setUntil(''); };
  return (
    <div className="bt-form bt-set">
      <div className="bt-chips">
        <button onClick={() => { setSince(''); setUntil(''); }}>вся история</button>
        {[5, 3, 2, 1].map(y => <button key={y} onClick={() => back(y)}>{y === 1 ? 'год' : `${y} ${y === 5 ? 'лет' : 'года'}`}</button>)}
        <button onClick={() => { setSince(`${new Date().getFullYear()}-01-01`); setUntil(''); }}>с начала года</button>
        <button onClick={() => { setSince('2022-03-24'); setUntil(''); }} title="после возобновления торгов">с 24.03.2022</button>
      </div>
      <div className="row">
        <label>С даты<input type="date" value={since} onChange={e => setSince(e.target.value)} /></label>
        <label>По дату<input type="date" value={until} onChange={e => setUntil(e.target.value)} /></label>
      </div>
      <div className="hint">Пусто — с начала истории (2019) и по последнюю свечу. Пороги «сильнее N% дней» считаются по дням до начала периода тоже — разгон не теряется.</div>
      <Foot dirty={since !== (spec.since ?? '') || until !== (spec.until ?? '')} busy={busy} close={close} onApply={() => onApply({ ...spec, since: since || null, until: until || null })} />
    </div>
  );
}

function Account({ spec, meta, busy, onApply, close }: { spec: Spec; meta: BtMeta; busy: boolean; onApply: (s: Spec) => void; close: () => void }) {
  const [d, setD] = useState<Spec>({ capital: spec.capital ?? 1_000_000, slots: spec.slots ?? 6, leverage: spec.leverage ?? 1, go_mult: spec.go_mult ?? 1, tariff: spec.tariff ?? 'trader', spread: spec.spread ?? 'c3', go: spec.go ?? 'mr1', exec: spec.exec ?? 'close' });
  const [was] = useState(() => JSON.stringify(d)); const set = (k: string, v: any) => setD(x => ({ ...x, [k]: v }));
  const nUni = (spec.universe ?? []).length || 1;
  return (
    <div className="bt-form bt-set">
      <div className="row">
        <label>Капитал, ₽<input type="number" min={10000} step={100000} value={d.capital} onChange={e => set('capital', e.target.value)} /></label>
        <label>Сделок в день, максимум<input type="number" min={1} max={30} value={d.slots} onChange={e => set('slots', e.target.value)} /></label>
      </div>
      <div className="hint">Размер сделки = капитал × плечо ÷ число сделок в день, целыми контрактами. Бумаг в прогоне: {nUni}.</div>
      <div className="row">
        <label>Плечо<select value={d.leverage} onChange={e => set('leverage', e.target.value)}>{[1, 1.5, 2, 3, 4, 5].map(x => <option key={x} value={x}>{x === 1 ? 'без плеча' : `×${x}`}</option>)}</select></label>
        <label>Стресс: ГО выросло<select value={d.go_mult} onChange={e => set('go_mult', e.target.value)}>{[1, 1.5, 2, 3].map(x => <option key={x} value={x}>{x === 1 ? 'нет' : `в ${x} раза`}</option>)}</select></label>
      </div>
      <div className="row">
        <label>Тариф брокера<select value={d.tariff} onChange={e => set('tariff', e.target.value)}>{Object.entries(meta.tariffs).map(([k, v]) => <option key={k} value={k}>{meta.tariff_labels?.[k] ?? `${k} — ${v}% за сторону`}</option>)}</select></label>
        <label>Спред стакана<select value={d.spread} onChange={e => set('spread', e.target.value)}>{meta.spread_daily && <option value="daily">по дням, с глубиной стакана</option>}<option value="c3">учитывать (3 уровня)</option><option value="c5">учитывать (5 уровней)</option><option value="none">не учитывать</option></select></label>
      </div>
      <label>Гарантийное обеспечение<select value={d.go} onChange={e => set('go', e.target.value)}>{Object.entries(meta.go).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select></label>
      {!spec.code && <label>Исполнение заявок<select value={d.exec} onChange={e => set('exec', e.target.value)}>{Object.entries(meta.exec).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select></label>}
      <Foot dirty={JSON.stringify(d) !== was} busy={busy} close={close} onApply={() => onApply({ ...spec, ...d, capital: Number(d.capital) || 1_000_000, slots: Math.max(1, Math.min(30, Number(d.slots) || 1)), leverage: Number(d.leverage) || 1, go_mult: Number(d.go_mult) || 1 })} />
    </div>
  );
}

/** Порог стороны: выключен · «сильнее N% таких дней» · фиксированный ход. Пороги словарём по бумагам правятся только в коде. */
function Side({ word, s, onChange }: { word: string; s: any; onChange: (s: any) => void }) {
  const type = !s ? 'off' : s.type; const perSt = s?.type === 'fixed' && typeof s.value === 'object';
  const pick = (t: string) => onChange(t === 'off' ? null : t === 'quantile' ? { type: 'quantile', q: s?.q ?? (word === 'Лонг' ? 0.67 : 0.9), window: s?.window ?? 250, min_obs: s?.min_obs ?? 100 } : { type: 'fixed', value: typeof s?.value === 'number' ? s.value : 0.02, strict: false });
  return (<>
    <div className="bt-form-h">{word}</div>
    <div className="row">
      <label>Условие<select value={type} onChange={e => pick(e.target.value)}><option value="off">выключен</option><option value="quantile">ход сильнее, чем в N% таких дней</option><option value="fixed">ход не меньше X%</option></select></label>
      {type === 'quantile' && <label>N, % дней<input type="number" min={1} max={99} step={1} value={p100(s.q)} onChange={e => onChange({ ...s, q: Math.min(0.99, Math.max(0.01, Number(e.target.value) / 100)) })} /></label>}
      {type === 'fixed' && (perSt ? <label>X<input disabled value="свой у каждой бумаги — в коде" /></label> : <label>X, %<input type="number" min={0} step={0.1} value={p100(s.value)} onChange={e => onChange({ ...s, value: Number(e.target.value) / 100 })} /></label>)}
    </div>
    {type === 'quantile' && <div className="row">
      <label>Считать по последним, дней<input type="number" min={20} max={1000} value={s.window ?? 250} onChange={e => onChange({ ...s, window: Number(e.target.value) || 250 })} /></label>
      <label>Нужно таких дней, не меньше<input type="number" min={5} max={500} value={s.min_obs ?? 100} onChange={e => onChange({ ...s, min_obs: Number(e.target.value) || 100 })} /></label>
    </div>}
  </>);
}

function Conditions({ spec, busy, onApply, close }: { spec: Spec; busy: boolean; onApply: (s: Spec) => void; close: () => void }) {
  const [r, setR] = useState<any>(() => JSON.parse(JSON.stringify(spec.rule ?? {}))); const [was] = useState(() => JSON.stringify(spec.rule ?? {}));
  const upd = (path: string[], v: any) => setR((x: any) => { const n = JSON.parse(JSON.stringify(x)); let o = n; path.slice(0, -1).forEach(k => { o[k] = o[k] ?? {}; o = o[k]; }); if (v === undefined) delete o[path[path.length - 1]]; else o[path[path.length - 1]] = v; return n; });
  const ex = r.exit ?? {};
  return (
    <div className="bt-form bt-set wide">
      <div className="row3">
        <label>Ход цены с<input type="time" step={300} value={r.signal?.from ?? ''} onChange={e => upd(['signal', 'from'], e.target.value)} /></label>
        <label>по — и сразу вход<input type="time" step={300} value={r.signal?.to ?? ''} onChange={e => upd(['signal', 'to'], e.target.value)} /></label>
        <label>Выход завтра в<input type="time" step={300} value={ex.at ?? ''} onChange={e => upd(['exit', 'at'], e.target.value)} /></label>
      </div>
      <Side word="Лонг" s={r.long} onChange={s => upd(['long'], s)} />
      <Side word="Шорт" s={r.short} onChange={s => upd(['short'], s)} />
      <div className="bt-form-h">Риск <span>досрочный выход, % от цены входа</span></div>
      <div className="row3">
        {([['stop', 'Стоп, %'], ['take', 'Тейк, %'], ['trail', 'Трейлинг, %']] as const).map(([k, l]) => <label key={k}>{l}<input type="number" min={0} step={0.5} placeholder="нет" value={ex[k] ? p100(ex[k]) : ''} onChange={e => upd(['exit', k], e.target.value ? Number(e.target.value) / 100 : undefined)} /></label>)}
      </div>
      <label>Держать позицию, торговых дней<input type="number" min={1} max={20} value={ex.hold_days ?? 1} onChange={e => upd(['exit', 'hold_days'], Number(e.target.value) > 1 ? Number(e.target.value) : undefined)} /></label>
      <div className="hint">Стоп, тейк и трейлинг проверяются по закрытию каждой 5-минутной свечи, выход — по открытию следующей.{(r.filters ?? []).length ? ' Фильтры правила (прямота хода) меняются в редакторе кода.' : ''}</div>
      <Foot dirty={JSON.stringify(r) !== was} busy={busy} close={close} onApply={() => onApply({ ...spec, rule: { ...r, frozen: false } })} />
    </div>
  );
}

function PyParams({ spec, busy, onApply, close }: { spec: Spec; busy: boolean; onApply: (s: Spec) => void; close: () => void }) {
  const [p, setP] = useState<Record<string, any>>({ ...(spec.params ?? {}) }); const [was] = useState(() => JSON.stringify(spec.params ?? {}));
  return (
    <div className="bt-form bt-set">
      {Object.keys(p).length ? <div className="row">{Object.entries(p).map(([k, v]) => <label key={k}>{k}<input value={String(v)} onChange={e => setP(x => ({ ...x, [k]: typeof (spec.params ?? {})[k] === 'number' ? Number(e.target.value) : e.target.value }))} /></label>)}</div>
        : <div className="hint" style={{ marginTop: 0 }}>Прогон запущен со значениями PARAMS по умолчанию. Чтобы менять их здесь, задай значения в редакторе («Свойства») и запусти ещё раз.</div>}
      <Foot dirty={JSON.stringify(p) !== was} busy={busy} close={close} onApply={() => onApply({ ...spec, params: p })} />
    </div>
  );
}

export default function RunSettings({ run, meta, busy, onApply }: { run: BtRun; meta: BtMeta | null; busy: boolean; onApply: (spec: Spec) => void }) {
  if (!meta || run.kind === 'sweep' || !run.spec) return null;
  const spec = baseSpec(run); const full = run.spec_full ?? spec; const period = run.result?.period; const isPy = !!spec.code; const rule = spec.rule ?? {};
  const ex = rule.exit ?? {};
  const risk = [ex.stop && `стоп ${num(100 * ex.stop, 1)}%`, ex.take && `тейк ${num(100 * ex.take, 1)}%`, ex.trail && `трейл ${num(100 * ex.trail, 1)}%`, ex.hold_days > 1 && `держать ${ex.hold_days} дн.`].filter(Boolean);
  const cond = isPy ? `параметры${Object.keys(spec.params ?? {}).length ? `: ${Object.entries(spec.params).map(([k, v]) => `${k}=${v}`).join(', ')}` : ''}`
    : [sideText(rule.long, 'лонг'), sideText(rule.short, 'шорт'), ...(risk.length ? risk : ['без стопа и тейка'])].join(' · ');
  return (<>
    <Menu className="chip" tall label={<>{period ? `${fmtDate(period[0])} — ${fmtDate(period[1])}` : 'период'} ▾</>} title="Период теста — нажми, чтобы изменить">{close => <Period spec={spec} busy={busy} onApply={onApply} close={close} />}</Menu>
    {full.capital != null && <Menu className="chip" tall label={<>{num(full.capital / 1e6, 2)} млн ₽ · сделок в день: до {full.slots}{full.leverage > 1 ? ` · плечо ×${full.leverage}` : ''} ▾</>} title="Капитал, размер сделки, комиссии, спред, ГО — нажми, чтобы изменить">{close => <Account spec={spec} meta={meta} busy={busy} onApply={onApply} close={close} />}</Menu>}
    <Menu className="chip" tall label={<>{cond} ▾</>} title="Условия входа и риск-менеджмент — нажми, чтобы изменить">{close => isPy ? <PyParams spec={spec} busy={busy} onApply={onApply} close={close} /> : <Conditions spec={spec} busy={busy} onApply={onApply} close={close} />}</Menu>
  </>);
}
