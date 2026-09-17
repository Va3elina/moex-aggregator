/* eslint-disable @typescript-eslint/no-explicit-any -- правило — свободный JSON движка */
// Стенд: редактор стратегии. Слева код правила (JSON Стенда — см. backtest/rules.py), справа свойства счёта.
// «Запустить» = прогон на сервере; готовый прогон сам открывается в тестере.
import { useEffect, useMemo, useRef, useState } from 'react';
import { btApi, type BtMeta } from './api';
import type { Prefs, SavedRule } from './usePrefs';
import { Logo, Menu, MenuItem } from './ui';
import { paramLabel, setPath } from './Sweep';

export const DEFAULT_PROPS = { exec: 'robot', tariff: 'trader', spread: 'c3', capital: 1_000_000, slots: 6, go: 'mr1', leverage: 1, go_mult: 1, since: '', until: '', name: '', universe: null as string[] | null };
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
      `Выход: ${r.exit.at}${r.exit?.delay_min ? ` + ${r.exit.delay_min} мин` : ''} ${(r.exit?.hold_days ?? 1) > 1 ? `через ${r.exit.hold_days} торговых дня` : 'следующего торгового дня'}, ${px(r.exit)}`,
      ...(['stop', 'take', 'trail'] as const).filter(k => r.exit?.[k]).map(k => `${{ stop: 'Стоп', take: 'Тейк', trail: 'Трейлинг' }[k]}: ${(100 * r.exit[k]).toFixed(2)}% от цены входа — проверка по закрытию каждой 5-минутной свечи, выход по открытию следующей`)];
  } catch { return []; }
}

export default function Editor({ meta, state, saved, onState, onSaved, onRun, onClose, busy, error }: {
  meta: BtMeta; state: Prefs['editor']; saved: SavedRule[]; onState: (e: NonNullable<Prefs['editor']>) => void; onSaved: (r: SavedRule[]) => void;
  onRun: (spec: any) => void; onClose: () => void; busy: boolean; error: string | null;
}) {
  const code = state?.code ?? pretty(meta.rules[0].rule);
  const props = { ...DEFAULT_PROPS, ...(state?.props ?? {}) };
  const [tab, setTab] = useState<'code' | 'props' | 'sweep' | 'help'>('code');
  const [grid, setGrid] = useState<Record<string, string>>({}); const [oos, setOos] = useState('2025-01-01');
  const ta = useRef<HTMLTextAreaElement>(null); const gutter = useRef<HTMLDivElement>(null);
  const parsed = useMemo(() => { try { return { rule: JSON.parse(code), err: null as string | null }; } catch (e) { return { rule: null, err: String((e as Error).message) }; } }, [code]);
  const lines = useMemo(() => (state?.lang === 'python' ? (state?.py ?? meta.python_template ?? '') : code).split('\n').length, [code, state?.lang, state?.py, meta.python_template]);
  const lang = state?.lang ?? 'rule'; const isPy = lang === 'python';
  const py = state?.py ?? meta.python_template ?? ''; const pyParams = state?.pyParams ?? {};
  const patch = (p: Partial<NonNullable<Prefs['editor']>>) => onState({ code, props, lang, py, pyParams, ...p });
  const setCode = (c: string) => patch(isPy ? { py: c } : { code: c });
  const setProp = (k: string, v: unknown) => patch({ props: { ...props, [k]: v } });
  useEffect(() => { if (!state) patch({}); /* первая загрузка */ // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  // Python: синтаксис и PARAMS проверяет сервер (только разбор кода, без исполнения) — с паузой после последнего нажатия
  const [pyInfo, setPyInfo] = useState<{ error: string | null; params: Record<string, any> }>({ error: null, params: {} });
  useEffect(() => {
    if (!isPy) return; let dead = false;
    const t = setTimeout(() => { btApi.inspect(py).then(r => { if (!dead) setPyInfo(r); }).catch(() => undefined); }, 500);
    return () => { dead = true; clearTimeout(t); };
  }, [isPy, py]);
  const shown = isPy ? py : code;

  const universe: string[] = props.universe ?? parsed.rule?.universe ?? meta.instruments.map(i => i.st);
  const onKey = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Tab') { e.preventDefault(); const el = e.currentTarget, s = el.selectionStart, pad = isPy ? '    ' : '  '; setCode(shown.slice(0, s) + pad + shown.slice(el.selectionEnd)); requestAnimationFrame(() => { el.selectionStart = el.selectionEnd = s + pad.length; }); }
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); run(); }
    if ((e.metaKey || e.ctrlKey) && e.key === 's') { e.preventDefault(); save(); }
  };
  const run = () => {
    if (isPy) {
      if (busy || pyInfo.error) return;
      onRun({ code: py, params: pyParams, name: props.name || 'Стратегия на Python', universe: props.universe ?? meta.instruments.map(i => i.st).filter(s => s !== 'CR'), tariff: props.tariff, spread: props.spread,
        capital: Number(props.capital) || null, slots: Number(props.slots) || 6, go: props.go, leverage: Number(props.leverage) || 1, go_mult: Number(props.go_mult) || 1, since: props.since || null, until: props.until || null });
      return;
    }
    if (!parsed.rule || busy) return;
    onRun({ rule: parsed.rule, name: props.name || parsed.rule.name || null, exec: props.exec, universe, tariff: props.tariff, spread: props.spread,
      capital: Number(props.capital) || null, slots: Number(props.slots) || 6, go: props.go, leverage: Number(props.leverage) || 1, go_mult: Number(props.go_mult) || 1, since: props.since || null, until: props.until || null });
  };
  /** Правка поля правила из формы: меняем JSON и переформатируем код — форма и код всегда говорят одно и то же. */
  const setRule = (path: string, value: number | null) => { if (!parsed.rule) return; const r = JSON.parse(JSON.stringify(parsed.rule)); setPath(r, path, value); setCode(pretty(r)); };
  const numericPaths = useMemo(() => {
    const out: string[] = []; const walk = (o: any, p: string) => { for (const [k, v] of Object.entries(o ?? {})) { const q = p ? `${p}.${k}` : k; if (typeof v === 'number') out.push(q); else if (v && typeof v === 'object' && !Array.isArray(v) && k !== 'universe') walk(v, q); else if (Array.isArray(v) && k === 'filters') v.forEach((f, i) => walk(f, `${q}.${i}`)); } };
    if (parsed.rule) walk(parsed.rule, ''); for (const k of ['exit.stop', 'exit.take', 'exit.trail', 'exit.hold_days']) if (!out.includes(k)) out.push(k);
    return out.filter(k => !/^(id|name)$/.test(k));
  }, [parsed.rule]);
  const parseVals = (s: string) => s.split(/[,;\s]+/).filter(Boolean).map(x => /^(нет|выкл|null|-)$/i.test(x) ? null : Number(x.replace('%', '')) / (x.includes('%') ? 100 : 1)).filter(x => x === null || isFinite(x as number));
  const sweepGrid = Object.fromEntries(Object.entries(grid).map(([k, v]) => [k, parseVals(v)] as const).filter(([, v]) => v.length));
  const nVariants = Object.values(sweepGrid).reduce((a, v) => a * v.length, 1);
  const runSweep = () => {
    if (!parsed.rule || busy || !Object.keys(sweepGrid).length) return;
    onRun({ rule: parsed.rule, name: `Перебор: ${Object.keys(sweepGrid).map(paramLabel).join(', ')}`, exec: props.exec, universe, tariff: props.tariff, spread: props.spread, since: props.since || null, until: props.until || null, sweep: { grid: sweepGrid, oos_from: oos } });
  };
  const save = () => {
    const name = prompt('Название стратегии', isPy ? 'Моя стратегия на Python' : parsed.rule?.name ?? 'Моя стратегия'); if (!name) return;
    onSaved([{ name, code: isPy ? '#python\n' + py : code }, ...saved.filter(r => r.name !== name)]);
  };

  return (
    <aside className="bt-editor">
      <header>
        <Menu label={<>Открыть ▾</>}>{close => <>
          <div className="bt-pop-h">Готовые</div>
          {meta.rules.map(r => <MenuItem key={r.id} onClick={() => { patch({ lang: 'rule', code: pretty(r.rule), props: { ...props, universe: null } }); close(); }} hint={r.frozen ? 'заморожена' : undefined}>{r.name}</MenuItem>)}
          <MenuItem onClick={() => { patch({ lang: 'python', py: meta.python_template ?? '' }); close(); }} hint="Python">Пример: пересечение двух EMA</MenuItem>
          {!!saved.length && <div className="bt-pop-h">Мои</div>}
          {saved.map(r => <div key={r.name} className="bt-mi-row"><MenuItem onClick={() => { if (r.code.startsWith('#python\n')) patch({ lang: 'python', py: r.code.slice(8) }); else patch({ lang: 'rule', code: r.code }); close(); }} hint={r.code.startsWith('#python\n') ? 'Python' : undefined}>{r.name}</MenuItem><button className="bt-x" title="удалить" onClick={() => onSaved(saved.filter(x => x.name !== r.name))}>✕</button></div>)}
        </>}</Menu>
        <button className="bt-tb" onClick={save} title="⌘S">Сохранить</button>
        {!isPy && <button className="bt-tb" disabled={!parsed.rule} onClick={() => parsed.rule && setCode(pretty(parsed.rule))}>Формат</button>}
        <div className="bt-seg"><button className={!isPy ? 'on' : ''} onClick={() => patch({ lang: 'rule' })}>Правило</button><button className={isPy ? 'on' : ''} onClick={() => patch({ lang: 'python' })}>Python</button></div>
        <span style={{ flex: 1 }} />
        <button className="bt-run" disabled={(isPy ? !!pyInfo.error : !parsed.rule) || busy || !universe.length} onClick={run} title="⌘Enter">{busy ? 'Считается…' : '▶ Запустить'}</button>
        <button className="bt-x" onClick={onClose} title="закрыть редактор">✕</button>
      </header>
      <nav className="bt-subtabs">
        {([['code', isPy ? 'Код стратегии' : 'Код правила'], ['props', 'Свойства'], ['sweep', 'Перебор'], ['help', 'Справка']] as const).map(([k, l]) => <button key={k} className={tab === k ? 'on' : ''} onClick={() => setTab(k)}>{l}</button>)}
      </nav>
      {tab === 'code' && <>
        <div className="bt-code">
          <div className="bt-gutter" ref={gutter}>{Array.from({ length: lines }, (_, i) => <div key={i}>{i + 1}</div>)}</div>
          <textarea ref={ta} spellCheck={false} value={shown} onChange={e => setCode(e.target.value)} onKeyDown={onKey}
            onScroll={e => { if (gutter.current) gutter.current.scrollTop = e.currentTarget.scrollTop; }} />
        </div>
        <div className={`bt-codestatus ${(isPy ? pyInfo.error : parsed.err) ? 'err' : ''}`}>{isPy ? (pyInfo.error ? `Ошибка: ${pyInfo.error}` : '✓ синтаксис в порядке · ⌘Enter — запустить · выполняется в изолированном контейнере') : parsed.err ? `Ошибка JSON: ${parsed.err}` : '✓ JSON корректен · ⌘Enter — запустить'}</div>
        {error && <div className="bt-codestatus err">{error}</div>}
        {!isPy && parsed.rule && <ul className="bt-describe">{describe(parsed.rule).map(s => <li key={s}>{s}</li>)}</ul>}
      </>}
      {tab === 'props' && (
        <div className="bt-form">
          {isPy && <><div className="bt-form-h" style={{ borderTop: 0, paddingTop: 0 }}>Параметры стратегии <span>PARAMS из кода</span></div>
            {Object.keys(pyInfo.params).length ? <div className="row">{Object.entries(pyInfo.params).map(([k, d]) => <label key={k}>{k}<input value={String(pyParams[k] ?? d)} onChange={e => patch({ pyParams: { ...pyParams, [k]: typeof d === 'number' ? (e.target.value === '' ? d : Number(e.target.value)) : e.target.value } })} /></label>)}</div> : <div className="hint" style={{ marginTop: 0 }}>В коде нет словаря PARAMS.</div>}</>}
          {!isPy && <label>Исполнение<select value={props.exec} onChange={e => setProp('exec', e.target.value)}>{Object.entries(meta.exec).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select></label>}
          <div className="row">
            <label>Капитал, ₽<input type="number" value={props.capital} onChange={e => setProp('capital', e.target.value)} /></label>
            <label>Сделок в день, максимум<input type="number" min={1} max={30} value={props.slots} onChange={e => setProp('slots', e.target.value)} /></label>
          </div>
          <div className="hint">На сделку идёт капитал × плечо ÷ число сделок, целыми контрактами.</div>
          <div className="row">
            <label>Плечо<select value={props.leverage} onChange={e => setProp('leverage', e.target.value)}>{[1, 1.5, 2, 3, 4, 5].map(x => <option key={x} value={x}>{x === 1 ? 'без плеча' : `×${x}`}</option>)}</select></label>
            <label>Стресс: ГО выросло<select value={props.go_mult} onChange={e => setProp('go_mult', e.target.value)}>{[1, 1.5, 2, 3].map(x => <option key={x} value={x}>{x === 1 ? 'нет' : `в ${x} раза (как в 02.2022)`}</option>)}</select></label>
          </div>
          <div className="hint">Без плеча ГО никогда не мешает. С плечом объём режется по свободному ГО, а при росте ГО появляются маржин-коллы — всё это видно в тестере, вкладка «Использование ГО».</div>
          <div className="row">
            <label>Тариф брокера<select value={props.tariff} onChange={e => setProp('tariff', e.target.value)}>{Object.entries(meta.tariffs).map(([k, v]) => <option key={k} value={k}>{k} — {v}% за сторону</option>)}</select></label>
            <label>Спред стакана<select value={props.spread} onChange={e => setProp('spread', e.target.value)}>{meta.spread_daily && <option value="daily">по дням, с учётом глубины стакана</option>}<option value="c3">учитывать (3 уровня)</option><option value="c5">учитывать (5 уровней)</option><option value="none">не учитывать</option></select></label>
          </div>
          {!isPy && <><div className="bt-form-h">Досрочный выход <span>записывается в код правила</span></div>
          <div className="row3">
            {([['stop', 'Стоп, %'], ['take', 'Тейк, %'], ['trail', 'Трейлинг, %']] as const).map(([k, l]) => <label key={k}>{l}<input type="number" min={0} step={0.5} placeholder="нет" value={parsed.rule?.exit?.[k] ? +(100 * parsed.rule.exit[k]).toFixed(4) : ''} onChange={e => setRule(`exit.${k}`, e.target.value ? Number(e.target.value) / 100 : null)} /></label>)}
          </div>
          <label>Держать позицию, торговых дней<input type="number" min={1} max={20} value={parsed.rule?.exit?.hold_days ?? 1} onChange={e => setRule('exit.hold_days', Number(e.target.value) > 1 ? Number(e.target.value) : null)} /></label>
          <div className="hint">Стоп, тейк и трейлинг проверяются по закрытию каждой 5-минутной свечи (вечерняя и утренняя сессии), выход — по открытию следующей. Так же видит рынок робот, который просыпается раз в 5 минут.</div></>}
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
      {tab === 'sweep' && isPy && <div className="bt-help"><p>Перебор параметров для стратегий на Python появится следующим шагом. Сейчас перебор работает для стратегий-правил.</p></div>}
      {tab === 'sweep' && !isPy && (
        <div className="bt-form">
          <div className="hint" style={{ marginTop: 0 }}>Перебор прогоняет правило со всеми сочетаниями значений. Чтобы не подогнать параметры под историю, она делится на обучение и контроль: варианты ранжируются по обучению, а контроль показывает, что было бы на самом деле.</div>
          {numericPaths.map(k => (
            <label key={k}>{paramLabel(k)} <span className="bt-dim">{k} · сейчас {String(k.split('.').reduce((o: any, x) => o?.[x], parsed.rule) ?? 'выкл.')}</span>
              <input value={grid[k] ?? ''} onChange={e => setGrid(g => ({ ...g, [k]: e.target.value }))} placeholder={/stop|take|trail|value/.test(k) ? 'например: нет, 2%, 3%, 5%' : k.endsWith('.q') ? 'например: 0.6, 0.67, 0.75, 0.8' : 'значения через запятую'} /></label>
          ))}
          <label>Контрольный период начинается с<input type="date" value={oos} onChange={e => setOos(e.target.value)} /></label>
          <div className="hint">Вариантов: <b>{Object.keys(sweepGrid).length ? nVariants : 0}</b> (не больше 150). Каждый считается около секунды.</div>
          <button className="bt-run" disabled={busy || !Object.keys(sweepGrid).length || nVariants > 150 || !parsed.rule} onClick={runSweep}>{busy ? 'Считается…' : `▶ Запустить перебор`}</button>
          {error && <div className="bt-codestatus err">{error}</div>}
        </div>
      )}
      {tab === 'help' && isPy && (
        <div className="bt-help">
          <p>Стратегия на Python выполняется по каждой бумаге отдельно, свеча за свечой (5 минут, время московское) — как скрипт Pine в TradingView. Заявка исполняется по <b>открытию следующей свечи</b>. Позиция по бумаге одна. Перед сменой контракта позиция закрывается автоматически. Объём, комиссии, спред и ГО считает счёт — как у правил.</p>
          <pre>{`PARAMS = {"n": 20}                 # параметры → форма в «Свойствах»\n\ndef init(b, p):                    # один раз на бумагу: индикаторы\n    b.ma = b.sma(b.close, p["n"])\n\ndef on_bar(i, b, pos, p):          # на каждой свече\n    return +1 | -1 | 0 | None      # желаемая позиция; None — не менять`}</pre>
          <p><b>b</b> — свечи бумаги, массивы numpy: <code>b.open b.high b.low b.close b.volume</code>, <code>b.minute</code> (минуты от полуночи, 17:00 = 1020), <code>b.date</code>, <code>b.new_day</code>, <code>b.day_open</code>, <code>b.prev_close</code>.</p>
          <p>Индикаторы: <code>b.sma(x, n)</code> <code>b.ema(x, n)</code> <code>b.rsi(x, n)</code> <code>b.atr(n)</code> <code>b.highest(x, n)</code> <code>b.lowest(x, n)</code>. Доступны <code>np</code> и <code>pd</code>.</p>
          <p><b>Заглядывание вперёд.</b> В <code>on_bar</code> смотреть можно только на свечи 0…i. Каждый прогон проверяется автоматически: индикаторы пересчитываются на обрезанной истории, а обращения к будущим свечам отслеживаются. Результат — первым блоком в тестере.</p>
          <p><b>Безопасность.</b> Код выполняется в отдельном контейнере без сети и без доступа к базе и паролям сайта, с потолком памяти и времени (10 минут процессора).</p>
        </div>
      )}
      {tab === 'help' && !isPy && (
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
  "exit":  { "at": "11:00", "price": "next_open", "delay_min": 5,
             "hold_days": 1,                            // выход на N-й торговый день
             "stop": 0.03, "take": 0.05, "trail": 0.02 }  // досрочный выход, доли от цены входа
}`}</pre>
          <p><b>quantile</b> — порог считается по прошлым дням этой же бумаги (без заглядывания вперёд). <b>fixed</b> — число; можно словарём по бумагам: <code>{'{"Si": 0.009, "SS": 0.019}'}</code>. <b>strict</b>: true — строго больше.</p>
          <p>Время — начало 5-минутной свечи по Москве. <b>close</b> — цена закрытия этой свечи, <b>next_open</b> — открытие следующей (так исполняют рыночную заявку OsEngine и TradingView). Выход — всегда на следующий торговый день.</p>
        </div>
      )}
    </aside>
  );
}
