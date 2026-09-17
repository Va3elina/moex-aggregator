/* eslint-disable @typescript-eslint/no-explicit-any -- правило — свободный JSON движка */
// Стенд: редактор стратегии. Слева код правила (JSON Стенда — см. backtest/rules.py), справа свойства счёта.
// «Запустить» = прогон на сервере; готовый прогон сам открывается в тестере.
import { useEffect, useMemo, useRef, useState } from 'react';
import type { BtMeta } from './api';
import type { Prefs, SavedRule } from './usePrefs';
import { Logo, Menu, MenuItem } from './ui';

export const DEFAULT_PROPS = { exec: 'robot', tariff: 'trader', spread: 'c3', capital: 1_000_000, slots: 6, go: 'mr1', since: '', until: '', name: '', universe: null as string[] | null };
/** JSON с отступами, но короткие списки и словари чисел — в одну строку (иначе список бумаг занимает пол-экрана). */
const pretty = (x: unknown) => JSON.stringify(x, null, 2)
  .replace(/\[\n\s+([^[\]{}]*?)\n\s+\]/g, (_, body: string) => `[${body.split(/,\n\s+/).join(', ')}]`)
  .replace(/\{\n\s+((?:"[^"]+": -?[\d.]+,?\n?\s*)+)\}/g, (_, body: string) => `{ ${body.trim().split(/,\n\s+/).join(', ')} }`);

function describe(r: any): string[] {
  try {
    const side = (s: any, word: string) => !s ? `${word}: выключен` : s.type === 'quantile'
      ? `${word}: ход сильнее, чем в ${Math.round(s.q * 100)}% таких дней за последние ${s.window ?? 250} (нужно ≥ ${s.min_obs ?? 100} дней)`
      : `${word}: ход ${s.strict ? '>' : '≥'} ${typeof s.value === 'object' ? 'порога бумаги' : (100 * s.value).toFixed(2) + '%'}`;
    const px = (p: any) => p?.price === 'next_open' ? 'по открытию следующей свечи' : 'по закрытию свечи';
    return [`Сигнал: ход цены ${r.signal.from} → ${r.signal.to}`, side(r.long, 'Лонг'), side(r.short, 'Шорт'),
      ...(r.filters ?? []).map((f: any) => `Фильтр: ${f.type === 'straightness' ? `прямота хода ≥ ${typeof f.min === 'object' ? 'порога бумаги' : f.min}` : f.type}`),
      `Вход: ${r.signal.to}${r.entry?.delay_min ? ` + ${r.entry.delay_min} мин` : ''}, ${px(r.entry)}`,
      `Выход: ${r.exit.at}${r.exit?.delay_min ? ` + ${r.exit.delay_min} мин` : ''} следующего торгового дня, ${px(r.exit)}`];
  } catch { return []; }
}

export default function Editor({ meta, state, saved, onState, onSaved, onRun, onClose, busy, error }: {
  meta: BtMeta; state: Prefs['editor']; saved: SavedRule[]; onState: (e: NonNullable<Prefs['editor']>) => void; onSaved: (r: SavedRule[]) => void;
  onRun: (spec: any) => void; onClose: () => void; busy: boolean; error: string | null;
}) {
  const code = state?.code ?? pretty(meta.rules[0].rule);
  const props = { ...DEFAULT_PROPS, ...(state?.props ?? {}) };
  const [tab, setTab] = useState<'code' | 'props' | 'help'>('code');
  const ta = useRef<HTMLTextAreaElement>(null); const gutter = useRef<HTMLDivElement>(null);
  const parsed = useMemo(() => { try { return { rule: JSON.parse(code), err: null as string | null }; } catch (e) { return { rule: null, err: String((e as Error).message) }; } }, [code]);
  const lines = useMemo(() => code.split('\n').length, [code]);
  const setCode = (c: string) => onState({ code: c, props });
  const setProp = (k: string, v: unknown) => onState({ code, props: { ...props, [k]: v } });
  useEffect(() => { if (!state) onState({ code, props }); /* первая загрузка */ // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const universe: string[] = props.universe ?? parsed.rule?.universe ?? meta.instruments.map(i => i.st);
  const onKey = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Tab') { e.preventDefault(); const el = e.currentTarget, s = el.selectionStart; setCode(code.slice(0, s) + '  ' + code.slice(el.selectionEnd)); requestAnimationFrame(() => { el.selectionStart = el.selectionEnd = s + 2; }); }
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); run(); }
    if ((e.metaKey || e.ctrlKey) && e.key === 's') { e.preventDefault(); save(); }
  };
  const run = () => {
    if (!parsed.rule || busy) return;
    onRun({ rule: parsed.rule, name: props.name || parsed.rule.name || null, exec: props.exec, universe, tariff: props.tariff, spread: props.spread,
      capital: Number(props.capital) || null, slots: Number(props.slots) || 6, go: props.go, since: props.since || null, until: props.until || null });
  };
  const save = () => {
    const name = prompt('Название стратегии', parsed.rule?.name ?? 'Моя стратегия'); if (!name) return;
    onSaved([{ name, code }, ...saved.filter(r => r.name !== name)]);
  };

  return (
    <aside className="bt-editor">
      <header>
        <Menu label={<>Открыть ▾</>}>{close => <>
          <div className="bt-pop-h">Готовые</div>
          {meta.rules.map(r => <MenuItem key={r.id} onClick={() => { setCode(pretty(r.rule)); setProp('universe', null); close(); }} hint={r.frozen ? 'заморожена' : undefined}>{r.name}</MenuItem>)}
          {!!saved.length && <div className="bt-pop-h">Мои</div>}
          {saved.map(r => <div key={r.name} className="bt-mi-row"><MenuItem onClick={() => { setCode(r.code); close(); }}>{r.name}</MenuItem><button className="bt-x" title="удалить" onClick={() => onSaved(saved.filter(x => x.name !== r.name))}>✕</button></div>)}
        </>}</Menu>
        <button className="bt-tb" onClick={save} title="⌘S">Сохранить</button>
        <button className="bt-tb" disabled={!parsed.rule} onClick={() => parsed.rule && setCode(pretty(parsed.rule))}>Формат</button>
        <span style={{ flex: 1 }} />
        <button className="bt-run" disabled={!parsed.rule || busy || !universe.length} onClick={run} title="⌘Enter">{busy ? 'Считается…' : '▶ Запустить'}</button>
        <button className="bt-x" onClick={onClose} title="закрыть редактор">✕</button>
      </header>
      <nav className="bt-subtabs">
        {([['code', 'Код правила'], ['props', 'Свойства'], ['help', 'Справка']] as const).map(([k, l]) => <button key={k} className={tab === k ? 'on' : ''} onClick={() => setTab(k)}>{l}</button>)}
      </nav>
      {tab === 'code' && <>
        <div className="bt-code">
          <div className="bt-gutter" ref={gutter}>{Array.from({ length: lines }, (_, i) => <div key={i}>{i + 1}</div>)}</div>
          <textarea ref={ta} spellCheck={false} value={code} onChange={e => setCode(e.target.value)} onKeyDown={onKey}
            onScroll={e => { if (gutter.current) gutter.current.scrollTop = e.currentTarget.scrollTop; }} />
        </div>
        <div className={`bt-codestatus ${parsed.err ? 'err' : ''}`}>{parsed.err ? `Ошибка JSON: ${parsed.err}` : '✓ JSON корректен · ⌘Enter — запустить'}</div>
        {error && <div className="bt-codestatus err">{error}</div>}
        {parsed.rule && <ul className="bt-describe">{describe(parsed.rule).map(s => <li key={s}>{s}</li>)}</ul>}
      </>}
      {tab === 'props' && (
        <div className="bt-form">
          <label>Исполнение<select value={props.exec} onChange={e => setProp('exec', e.target.value)}>{Object.entries(meta.exec).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select></label>
          <div className="row">
            <label>Капитал, ₽<input type="number" value={props.capital} onChange={e => setProp('capital', e.target.value)} /></label>
            <label>Сделок в день, максимум<input type="number" min={1} max={30} value={props.slots} onChange={e => setProp('slots', e.target.value)} /></label>
          </div>
          <div className="hint">На сделку идёт капитал ÷ число сделок, целыми контрактами, без плеча.</div>
          <div className="row">
            <label>Тариф брокера<select value={props.tariff} onChange={e => setProp('tariff', e.target.value)}>{Object.entries(meta.tariffs).map(([k, v]) => <option key={k} value={k}>{k} — {v}% за сторону</option>)}</select></label>
            <label>Спред стакана<select value={props.spread} onChange={e => setProp('spread', e.target.value)}><option value="c3">учитывать (3 уровня)</option><option value="c5">учитывать (5 уровней)</option><option value="none">не учитывать</option></select></label>
          </div>
          <label>Гарантийное обеспечение<select value={props.go} onChange={e => setProp('go', e.target.value)}>{Object.entries(meta.go).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select></label>
          <div className="row">
            <label>С даты<input type="date" value={props.since} onChange={e => setProp('since', e.target.value)} /></label>
            <label>По дату<input type="date" value={props.until} onChange={e => setProp('until', e.target.value)} /></label>
          </div>
          <label>Бумаги ({universe.length})
            <div className="bt-chips">
              <button onClick={() => setProp('universe', null)}>как в правиле</button><button onClick={() => setProp('universe', meta.instruments.map(i => i.st))}>все</button><button onClick={() => setProp('universe', [])}>ни одной</button>
              {meta.instruments.map(i => <button key={i.st} title={i.name} className={universe.includes(i.st) ? 'on' : ''} onClick={() => setProp('universe', universe.includes(i.st) ? universe.filter(x => x !== i.st) : [...universe, i.st])}><Logo st={i.st} size={14} />{i.st}</button>)}
            </div>
          </label>
          <label>Название прогона<input value={props.name} onChange={e => setProp('name', e.target.value)} placeholder="по умолчанию — имя правила" /></label>
        </div>
      )}
      {tab === 'help' && (
        <div className="bt-help">
          <p>Стратегия описывается правилом в формате JSON. Это язык Стенда, а не Pine Script: правило задаёт окно сигнала, пороги входа и время выхода — без программирования. Произвольный код (стопы, свои индикаторы) — следующий этап.</p>
          <pre>{`{
  "id": "my_rule", "name": "Моя стратегия",
  "signal": { "from": "10:30", "to": "17:00" },   // ход цены за это окно
  "long":  { "type": "quantile", "q": 0.67,       // рост сильнее 67% прошлых ростов
             "window": 250, "min_obs": 100 },
  "short": { "type": "fixed", "value": 0.02 },    // падение ≥ 2%;  null — сторона выключена
  "filters": [ { "type": "straightness", "min": 0.58,
      "points": ["10:30","12:00","13:30","15:00","17:00"] } ],
  "entry": { "price": "next_open", "delay_min": 10 },   // close | next_open
  "exit":  { "at": "11:00", "price": "next_open", "delay_min": 5 }
}`}</pre>
          <p><b>quantile</b> — порог считается по прошлым дням этой же бумаги (без заглядывания вперёд). <b>fixed</b> — число; можно словарём по бумагам: <code>{'{"Si": 0.009, "SS": 0.019}'}</code>. <b>strict</b>: true — строго больше.</p>
          <p>Время — начало 5-минутной свечи по Москве. <b>close</b> — цена закрытия этой свечи, <b>next_open</b> — открытие следующей (так исполняют рыночную заявку OsEngine и TradingView). Выход — всегда на следующий торговый день.</p>
        </div>
      )}
    </aside>
  );
}
