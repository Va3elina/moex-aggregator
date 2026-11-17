// Мини-приложение Telegram «FRAME · Сигналы» (/tg) — настройка сигналов из бота @framesignalbot.
// ПОКА ТОЛЬКО ИНТЕРФЕЙС (Вадим 17.11.2026): данные демо, сервер не подключён, изменения живут до закрытия.
// Карта продукта и экраны: https://claude.ai/artifact/NDXgE94mSgqDgaTVzg2Zp9
// Внутри Telegram шапку, «Назад» и главную кнопку рисует сам Telegram (SDK); в обычном браузере — свои.
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import './tg.css';

interface TgButton { show(): void; hide(): void; onClick(cb: () => void): void; offClick(cb: () => void): void }
interface TgWebApp {
  platform: string;
  ready(): void;
  expand(): void;
  openLink(url: string): void;
  close(): void;
  setHeaderColor?(color: string): void;
  showConfirm?(message: string, cb: (ok: boolean) => void): void;
  BackButton: TgButton;
  MainButton: TgButton & { setParams(p: { text?: string; color?: string; text_color?: string }): void };
  HapticFeedback?: { selectionChanged(): void };
}

const SDK = 'https://telegram.org/js/telegram-web-app.js';
const SITE = 'https://framedata.ru';

/** SDK Telegram; null — открыто в обычном браузере (SDK там есть, но platform = unknown). */
function useTelegram(): TgWebApp | null {
  const [tg, setTg] = useState<TgWebApp | null>(null);
  useEffect(() => {
    const pick = () => {
      const w = (window as unknown as { Telegram?: { WebApp?: TgWebApp } }).Telegram?.WebApp;
      if (w && w.platform && w.platform !== 'unknown') {
        w.ready();
        w.expand();
        w.setHeaderColor?.('secondary_bg_color');
        setTg(w);
      }
    };
    if (document.querySelector(`script[src="${SDK}"]`)) { pick(); return; }
    const s = document.createElement('script');
    s.src = SDK;
    s.onload = pick;
    document.head.appendChild(s);
  }, []);
  return tg;
}

type Screen = 'login' | 'home' | 'rule' | 'asset' | 'sets' | 'history' | 'account' | 'quiet';
type Group = 'fiz' | 'yur' | 'all';
type Kind = 'level' | 'move' | 'record';
interface Rule {
  id: number; asset: string; group: Group; kind: Kind; power: 2 | 3 | 5; level: number; period: 'half' | 'year' | 'all';
  repeat: boolean; every: '5m' | '1h' | '1d'; paused: boolean; last: string;
}

const ASSETS: Record<string, { name: string; hint: string; mark: string; color: string }> = {
  Si: { name: 'Доллар/рубль', hint: 'фьючерс на курс доллара', mark: '$', color: '#2E7D5B' },
  CNY: { name: 'Юань/рубль', hint: 'фьючерс на курс юаня', mark: '¥', color: '#B23A48' },
  NG: { name: 'Газ', hint: 'фьючерс на природный газ', mark: 'NG', color: '#3A6EA5' },
  BR: { name: 'Нефть Brent', hint: 'фьючерс на нефть', mark: 'BR', color: '#6B4E2E' },
  GD: { name: 'Золото', hint: 'фьючерс на золото', mark: 'Au', color: '#B8892B' },
  VK: { name: 'VK', hint: 'фьючерс на акции VK', mark: 'VK', color: '#2A5BD7' },
  SR: { name: 'Сбербанк', hint: 'фьючерс на акции Сбера', mark: 'SR', color: '#1F9D55' },
  MX: { name: 'Индекс МосБиржи', hint: 'фьючерс на индекс', mark: 'MX', color: '#8A4831' },
};
const GROUPS: [Group, string][] = [['fiz', 'Физлица'], ['yur', 'Юрлица'], ['all', 'Все']];
const KINDS: [Kind, string][] = [['level', 'Пересечение уровня'], ['move', 'Резкое движение позиции'], ['record', 'Рекорд позиции за период']];
const POWERS: [2 | 3 | 5, string, string][] = [[2, 'Заметное · 2×', 'примерно раз в неделю'], [3, 'Сильное · 3×', 'примерно раз в 2–3 недели'], [5, 'Редкое · 5×', 'примерно раз в 2 месяца']];
const PERIODS: [Rule['period'], string][] = [['half', 'Полгода'], ['year', 'Год'], ['all', 'Всё время']];
const EVERY: [Rule['every'], string][] = [['5m', '5 минут'], ['1h', 'Час'], ['1d', 'День']];
const RULE_LIMIT = 20;

const DEMO_RULES: Rule[] = [
  { id: 1, asset: 'Si', group: 'fiz', kind: 'move', power: 3, level: 10000, period: 'year', repeat: true, every: '5m', paused: false, last: 'вчера' },
  { id: 2, asset: 'CNY', group: 'fiz', kind: 'level', power: 3, level: 12000, period: 'year', repeat: false, every: '1d', paused: false, last: 'ждёт' },
  { id: 3, asset: 'NG', group: 'fiz', kind: 'record', power: 3, level: 10000, period: 'year', repeat: true, every: '1d', paused: true, last: 'пауза' },
];
const HISTORY: { day: string; items: { asset: string; text: string; set: boolean; time: string; up: boolean }[] }[] = [
  { day: 'Сегодня', items: [
    { asset: 'Si', text: 'Физлица резко нарастили позицию · 3,4×', set: false, time: '14:35', up: true },
    { asset: 'VK', text: 'рекордный шорт за всё время', set: true, time: '10:55', up: false },
    { asset: 'GD', text: 'рекорд лонга за полгода', set: true, time: '10:00', up: true },
  ] },
  { day: 'Вчера', items: [
    { asset: 'NG', text: 'рекорд лонга за год', set: true, time: '17:20', up: true },
    { asset: 'Si', text: 'Физлица резко сократили позицию · 3,1×', set: false, time: '12:05', up: false },
    { asset: 'BR', text: 'рекорд лонга за полгода', set: true, time: '09:40', up: true },
  ] },
  { day: '15 ноября', items: [
    { asset: 'CNY', text: 'Позиция физлиц пересекла 12 000', set: false, time: '11:15', up: true },
  ] },
];

const title = (code: string) => (code === ASSETS[code].name ? code : `${ASSETS[code].name} · ${code}`);
const groupWord = (g: Group) => GROUPS.find(([k]) => k === g)![1].toLowerCase();
function ruleSub(r: Rule): string {
  if (r.paused) return `${KINDS.find(([k]) => k === r.kind)![1].split(' ')[0]} · на паузе`;
  if (r.kind === 'level') return `Уровень ${r.level.toLocaleString('ru-RU')} · ${r.repeat ? 'каждый раз' : 'один раз'}`;
  if (r.kind === 'move') return `Резкое движение · ${groupWord(r.group)} · ${r.power}×`;
  return `Рекорд · ${PERIODS.find(([k]) => k === r.period)![1].toLowerCase()} · ${groupWord(r.group)}`;
}

const Chevron = () => (
  <svg width="8" height="14" viewBox="0 0 8 14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" style={{ flex: 'none', opacity: 0.35 }}><path d="M1 1l6 6-6 6" /></svg>
);
const Check = () => (
  <svg width="15" height="12" viewBox="0 0 15 12" fill="none" stroke="var(--accent)" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" style={{ flex: 'none' }}><path d="M1.5 6.5l4 4 8-9" /></svg>
);
const Logo = ({ code, off }: { code: string; off?: boolean }) => (
  <span className={`tgs-lg${off ? ' off' : ''}`} style={{ background: ASSETS[code].color }}>{ASSETS[code].mark}</span>
);
const Thumb = ({ up }: { up: boolean }) => (
  <svg className="tgs-th" viewBox="0 0 48 36" aria-hidden="true">
    <path d={up ? 'M4 27L12 24 20 26 28 19 36 20 44 9' : 'M4 9L12 13 20 11 28 18 36 16 44 27'} fill="none" stroke="var(--accent)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
    <circle cx="44" cy={up ? 9 : 27} r="2.5" fill="var(--accent)" />
  </svg>
);

function Seg<T extends string | number>({ value, options, onChange }: { value: T; options: [T, string][]; onChange: (v: T) => void }) {
  return (
    <div className="tgs-seg">
      {options.map(([v, label]) => (
        <button key={String(v)} type="button" className={v === value ? 'on' : ''} aria-pressed={v === value} onClick={() => onChange(v)}>{label}</button>
      ))}
    </div>
  );
}

export default function TgSignalsApp() {
  const tg = useTelegram();
  const [stack, setStack] = useState<Screen[]>(() => (new URLSearchParams(window.location.search).get('screen') === 'login' ? ['login'] : ['home']));
  const screen = stack[stack.length - 1];
  const [enabled, setEnabled] = useState(true);
  const [sets, setSets] = useState({ records: true, moves: false, season: false, minWindow: 'half', groups: ['Акции', 'Валюта', 'Сырьё'], digest: false });
  const [muted, setMuted] = useState([{ asset: 'VK', until: 'тихо до 18 ноября, 14:35' }]);
  const [rules, setRules] = useState(DEMO_RULES);
  const [draft, setDraft] = useState<Rule | null>(null);
  const [filter, setFilter] = useState<'all' | 'mine' | 'sets'>('all');
  const [quiet, setQuiet] = useState({ on: true, from: '23:00', to: '08:00' });
  const [send, setSend] = useState({ telegram: true, site: true, news: false });
  const [toast, setToast] = useState('');

  const say = useCallback((text: string) => setToast(text), []);
  useEffect(() => {
    if (!toast) return;
    const id = window.setTimeout(() => setToast(''), 2200);
    return () => window.clearTimeout(id);
  }, [toast]);
  const tap = useCallback(() => tg?.HapticFeedback?.selectionChanged(), [tg]);
  const go = useCallback((s: Screen) => setStack((st) => [...st, s]), []);
  const back = useCallback(() => setStack((st) => (st.length > 1 ? st.slice(0, -1) : st)), []);
  const open = useCallback((url: string) => (tg ? tg.openLink(url) : window.open(url, '_blank', 'noopener')), [tg]);

  const isNew = draft !== null && !rules.some((r) => r.id === draft.id);
  const saveRule = useCallback(() => {
    if (!draft) return;
    setRules((rs) => (rs.some((r) => r.id === draft.id) ? rs.map((r) => (r.id === draft.id ? draft : r)) : [...rs, draft]));
    say(isNew ? 'Правило создано' : 'Сохранено');
    back();
  }, [draft, isNew, say, back]);
  const main = screen === 'rule' ? { text: isNew ? 'Создать правило' : 'Сохранить', run: saveRule } : null;

  // Кнопки самого Telegram: «Назад» в шапке и главная кнопка внизу
  useEffect(() => {
    if (!tg) return;
    if (stack.length > 1) tg.BackButton.show(); else tg.BackButton.hide();
    tg.BackButton.onClick(back);
    return () => tg.BackButton.offClick(back);
  }, [tg, stack.length, back]);
  const mainText = main?.text;
  const mainRun = main?.run;
  useEffect(() => {
    if (!tg) return;
    if (!mainText || !mainRun) { tg.MainButton.hide(); return; }
    tg.MainButton.setParams({ text: mainText, color: '#FF5C2B', text_color: '#FFFFFF' });
    tg.MainButton.show();
    tg.MainButton.onClick(mainRun);
    return () => tg.MainButton.offClick(mainRun);
  }, [tg, mainText, mainRun]);

  const sw = (on: boolean, label: string, set: (v: boolean) => void) => (
    <button type="button" className={`tgs-sw${on ? ' on' : ''}`} aria-pressed={on} aria-label={label} onClick={() => { tap(); set(!on); }} />
  );
  const editRule = (r: Rule) => { setDraft({ ...r }); go('rule'); };
  const newRule = () => {
    if (rules.length >= RULE_LIMIT) { say('На тарифе Basic — до 20 правил'); return; }
    setDraft({ id: Date.now(), asset: 'Si', group: 'fiz', kind: 'move', power: 3, level: 10000, period: 'year', repeat: false, every: '1d', paused: false, last: 'новое' });
    go('rule');
  };
  const removeRule = () => {
    if (!draft) return;
    const run = (ok: boolean) => { if (!ok) return; setRules((rs) => rs.filter((r) => r.id !== draft.id)); say('Правило удалено'); back(); };
    if (tg?.showConfirm) tg.showConfirm(`Удалить правило по «${title(draft.asset)}»?`, run);
    else run(window.confirm(`Удалить правило по «${title(draft.asset)}»?`));
  };
  const patch = (p: Partial<Rule>) => setDraft((d) => (d ? { ...d, ...p } : d));
  const history = useMemo(
    () => HISTORY.map((d) => ({ ...d, items: d.items.filter((i) => filter === 'all' || (filter === 'sets') === i.set) })).filter((d) => d.items.length),
    [filter],
  );

  let body: ReactNode = null;
  if (screen === 'login') {
    body = (
      <div className="tgs-login">
        <div className="tgs-card">
          <h1>Войти в аккаунт</h1>
          <p>Чтобы получать сигналы в этот чат</p>
          <button type="button" className="tgs-ob" onClick={() => say('Демо: вход пока не подключён')}>Войти через Яндекс</button>
          <button type="button" className="tgs-ob" onClick={() => say('Демо: вход пока не подключён')}>Войти через ВКонтакте</button>
          <div className="tgs-or">или по email</div>
          <label className="tgs-fl">Email<input type="email" placeholder="you@example.com" autoComplete="off" /></label>
          <label className="tgs-fl">Пароль<input type="password" placeholder="••••••••" autoComplete="off" /></label>
          <button type="button" className="tgs-pr" onClick={() => { say('Демо: вошли без проверки'); setStack(['home']); }}>Войти →</button>
          <button type="button" className="tgs-tl" onClick={() => open(`${SITE}/forgot-password`)}>Забыли пароль?</button>
          <p>Нет аккаунта? <button type="button" className="tgs-tl" style={{ margin: 0, height: 'auto', padding: 0 }} onClick={() => open(`${SITE}/login`)}>Зарегистрироваться</button></p>
        </div>
        <button type="button" className="tgs-alt" onClick={() => open(`${SITE}/profile`)}>Я уже вошёл в браузере</button>
        <div className="tgs-f" style={{ textAlign: 'center', padding: 0 }}>Откроем профиль на сайте: оттуда бот подключается одной кнопкой.</div>
      </div>
    );
  } else if (screen === 'home') {
    body = (
      <>
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g"><span>{enabled ? 'Сигналы включены' : 'Сигналы на паузе'}</span><span className="tgs-s">{enabled ? 'Выключите — всё встанет на паузу' : 'Ничего не приходит, настройки сохранены'}</span></span>{sw(enabled, 'Сигналы включены', setEnabled)}</div>
        </div>
        <div className="tgs-h">Подборки Фрейма</div>
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g">Рекорды позиций</span>{sw(sets.records, 'Рекорды позиций', (v) => setSets({ ...sets, records: v }))}</div>
          <div className="tgs-row"><span className="tgs-g">Резкие сдвиги</span>{sw(sets.moves, 'Резкие сдвиги', (v) => setSets({ ...sets, moves: v }))}</div>
          <button type="button" className="tgs-row" onClick={() => go('sets')}><span className="tgs-g">Настроить подборки</span><Chevron /></button>
        </div>
        <div className="tgs-h">Мои правила · {rules.length} из {RULE_LIMIT}</div>
        <div className="tgs-sec">
          {rules.map((r) => (
            <button key={r.id} type="button" className="tgs-row" onClick={() => editRule(r)}>
              <Logo code={r.asset} off={r.paused} />
              <span className="tgs-g"><span className={r.paused ? 'tgs-dim' : ''}>{title(r.asset)}</span><span className="tgs-s tgs-1l">{ruleSub(r)}</span></span>
              <span className="tgs-s">{r.paused ? 'пауза' : r.last}</span><Chevron />
            </button>
          ))}
          <button type="button" className="tgs-row tgs-accent" onClick={newRule}><span className="tgs-lg tgs-plus">+</span><span className="tgs-g">Новое правило</span></button>
        </div>
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <button type="button" className="tgs-row" onClick={() => go('history')}><span className="tgs-g">История</span><span className="tgs-v">3 сегодня</span><Chevron /></button>
          <button type="button" className="tgs-row" onClick={() => go('quiet')}><span className="tgs-g">Тихие часы</span><span className="tgs-v">{quiet.on ? `${quiet.from}–${quiet.to}` : 'выключены'}</span><Chevron /></button>
          <button type="button" className="tgs-row" onClick={() => go('account')}><span className="tgs-g">Аккаунт</span><span className="tgs-v">Basic</span><Chevron /></button>
        </div>
        <div className="tgs-demo">Демо-версия: данные условные, изменения не сохраняются.</div>
      </>
    );
  } else if (screen === 'rule' && draft) {
    body = (
      <>
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <button type="button" className="tgs-row" onClick={() => go('asset')}><Logo code={draft.asset} /><span className="tgs-g"><span>{title(draft.asset)}</span><span className="tgs-s">{ASSETS[draft.asset].hint}</span></span><Chevron /></button>
        </div>
        <div className="tgs-h">Чьи позиции</div>
        <Seg value={draft.group} options={GROUPS} onChange={(group) => patch({ group })} />
        <div className="tgs-h">Что ловить</div>
        <div className="tgs-sec">
          {KINDS.map(([k, label]) => (
            <button key={k} type="button" className="tgs-row" aria-pressed={draft.kind === k} onClick={() => { tap(); patch({ kind: k }); }}><span className="tgs-g">{label}</span>{draft.kind === k && <Check />}</button>
          ))}
        </div>
        {draft.kind === 'move' && (
          <>
            <div className="tgs-h">Сила сигнала</div>
            <Seg value={draft.power} options={POWERS.map(([v, l]) => [v, l] as [2 | 3 | 5, string])} onChange={(power) => patch({ power })} />
            <div className="tgs-f">По этому активу срабатывает {POWERS.find(([v]) => v === draft.power)![2]}.</div>
          </>
        )}
        {draft.kind === 'level' && (
          <>
            <div className="tgs-h">Уровень</div>
            <div className="tgs-sec">
              <label className="tgs-row"><span className="tgs-g">Чистая позиция, контрактов</span><input className="tgs-in" type="number" inputMode="numeric" value={draft.level} onChange={(e) => patch({ level: Number(e.target.value) || 0 })} /></label>
            </div>
            <div className="tgs-f">Сообщим, когда позиция пересечёт этот уровень в любую сторону.</div>
          </>
        )}
        {draft.kind === 'record' && (
          <>
            <div className="tgs-h">Рекорд за</div>
            <Seg value={draft.period} options={PERIODS} onChange={(period) => patch({ period })} />
            <div className="tgs-f">Сообщим, когда позиция станет самой большой за этот срок.</div>
          </>
        )}
        <div className="tgs-h">Как часто проверять</div>
        <Seg value={draft.every} options={EVERY} onChange={(every) => patch({ every })} />
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g">Присылать каждый раз</span>{sw(draft.repeat, 'Присылать каждый раз', (repeat) => patch({ repeat }))}</div>
          <div className="tgs-row"><span className="tgs-g">Правило на паузе</span>{sw(draft.paused, 'Правило на паузе', (paused) => patch({ paused }))}</div>
        </div>
        {!isNew && (
          <>
            <div className="tgs-gap" />
            <div className="tgs-sec"><button type="button" className="tgs-row tgs-center tgs-danger" onClick={removeRule}>Удалить правило</button></div>
          </>
        )}
      </>
    );
  } else if (screen === 'asset' && draft) {
    body = (
      <>
        <div className="tgs-h">Актив</div>
        <div className="tgs-sec">
          {Object.keys(ASSETS).map((code) => (
            <button key={code} type="button" className="tgs-row" onClick={() => { tap(); patch({ asset: code }); back(); }}>
              <Logo code={code} /><span className="tgs-g"><span>{title(code)}</span><span className="tgs-s">{ASSETS[code].hint}</span></span>{draft.asset === code && <Check />}
            </button>
          ))}
        </div>
        <div className="tgs-f">В демо — восемь активов; в рабочей версии будет поиск по всем фьючерсам.</div>
      </>
    );
  } else if (screen === 'sets') {
    const toggleGroup = (g: string) => setSets({ ...sets, groups: sets.groups.includes(g) ? sets.groups.filter((x) => x !== g) : [...sets.groups, g] });
    body = (
      <>
        <div className="tgs-h">Что присылать</div>
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g"><span>Рекорды позиций</span><span className="tgs-s">Такой позиции у физлиц давно не было</span></span>{sw(sets.records, 'Рекорды позиций', (v) => setSets({ ...sets, records: v }))}</div>
          <div className="tgs-row"><span className="tgs-g"><span>Резкие сдвиги</span><span className="tgs-s">Позиция изменилась сильнее обычного</span></span>{sw(sets.moves, 'Резкие сдвиги', (v) => setSets({ ...sets, moves: v }))}</div>
          <div className="tgs-row"><span className="tgs-g"><span>Сезонность</span><span className="tgs-s">Начался сильный сезонный период</span></span>{sw(sets.season, 'Сезонность', (v) => setSets({ ...sets, season: v }))}</div>
        </div>
        <div className="tgs-h">Рекорд не короче</div>
        <Seg value={sets.minWindow} options={[['3m', '3 месяца'], ['half', 'Полгода'], ['year', 'Год'], ['all', 'Всё время']]} onChange={(minWindow) => setSets({ ...sets, minWindow })} />
        <div className="tgs-f">С «3 месяцев» сообщений вдвое больше.</div>
        <div className="tgs-h">Активы</div>
        <div className="tgs-sec">
          {['Акции', 'Валюта', 'Индексы', 'Сырьё'].map((g) => (
            <button key={g} type="button" className="tgs-row" aria-pressed={sets.groups.includes(g)} onClick={() => { tap(); toggleGroup(g); }}><span className="tgs-g">{g}</span>{sets.groups.includes(g) && <Check />}</button>
          ))}
        </div>
        <div className="tgs-h">Как присылать</div>
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g"><span>Только сводка дня</span><span className="tgs-s">Одним сообщением в 19:00</span></span>{sw(sets.digest, 'Только сводка дня', (v) => setSets({ ...sets, digest: v }))}</div>
        </div>
        {muted.length > 0 && (
          <>
            <div className="tgs-h">На тишине</div>
            <div className="tgs-sec">
              {muted.map((m) => (
                <div key={m.asset} className="tgs-row"><Logo code={m.asset} /><span className="tgs-g"><span>{title(m.asset)}</span><span className="tgs-s">{m.until}</span></span><button type="button" className="tgs-lk" onClick={() => { setMuted(muted.filter((x) => x.asset !== m.asset)); say(`${title(m.asset)}: звук возвращён`); }}>Вернуть</button></div>
              ))}
            </div>
          </>
        )}
      </>
    );
  } else if (screen === 'history') {
    body = (
      <>
        <div className="tgs-gap" />
        <Seg value={filter} options={[['all', 'Все'], ['mine', 'Мои правила'], ['sets', 'Подборки']]} onChange={setFilter} />
        {history.map((d) => (
          <div key={d.day}>
            <div className="tgs-h">{d.day}</div>
            <div className="tgs-sec">
              {d.items.map((i) => (
                <button key={i.asset + i.time} type="button" className="tgs-row tgs-thumbed" onClick={() => open(`${SITE}/oi?instrument=${i.asset}&clgroup=FIZ`)}>
                  <Thumb up={i.up} />
                  <span className="tgs-g"><span>{title(i.asset)}</span><span className="tgs-s">{i.set && <><span className="tgs-accent">Подборка</span> · </>}{i.text}</span></span>
                  <span className="tgs-tm">{i.time}</span>
                </button>
              ))}
            </div>
          </div>
        ))}
        <div className="tgs-f">Хранится 30 дней. Нажмите на строку — откроется график на сайте.</div>
      </>
    );
  } else if (screen === 'quiet') {
    body = (
      <>
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g">Тихие часы</span>{sw(quiet.on, 'Тихие часы', (on) => setQuiet({ ...quiet, on }))}</div>
          {quiet.on && <label className="tgs-row"><span className="tgs-g">С</span><input className="tgs-in" type="time" value={quiet.from} onChange={(e) => setQuiet({ ...quiet, from: e.target.value })} /></label>}
          {quiet.on && <label className="tgs-row"><span className="tgs-g">До</span><input className="tgs-in" type="time" value={quiet.to} onChange={(e) => setQuiet({ ...quiet, to: e.target.value })} /></label>}
        </div>
        <div className="tgs-f">В это время сигналы приходят без звука. Время московское.</div>
      </>
    );
  } else if (screen === 'account') {
    body = (
      <>
        <div className="tgs-me">
          <div className="tgs-ava">Д</div>
          <div className="tgs-name">Демо-аккаунт</div>
          <div className="tgs-s" style={{ fontSize: 15, lineHeight: '20px' }}>здесь будут почта и Telegram</div>
        </div>
        <div className="tgs-sec">
          <button type="button" className="tgs-row" onClick={() => open(`${SITE}/pricing`)}><span className="tgs-g"><span>Тариф Basic</span><span className="tgs-s">{rules.length} из {RULE_LIMIT} правил</span></span><span className="tgs-v tgs-accent">Pro</span><Chevron /></button>
        </div>
        <div className="tgs-h">Куда присылать сигналы</div>
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g">Telegram</span>{sw(send.telegram, 'Telegram', (telegram) => setSend({ ...send, telegram }))}</div>
          <div className="tgs-row"><span className="tgs-g">Колокол на сайте</span>{sw(send.site, 'Колокол на сайте', (site) => setSend({ ...send, site }))}</div>
        </div>
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g">Новости Фрейма</span>{sw(send.news, 'Новости Фрейма', (news) => setSend({ ...send, news }))}</div>
        </div>
        <div className="tgs-f">Редкие сообщения о новых разделах и данных. Отключается здесь же.</div>
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <button type="button" className="tgs-row" onClick={() => open(SITE)}><span className="tgs-g">Открыть framedata.ru</span><Chevron /></button>
          <button type="button" className="tgs-row" onClick={() => open(`${SITE}/contacts`)}><span className="tgs-g">Написать в поддержку</span><Chevron /></button>
        </div>
        <div className="tgs-gap" />
        <div className="tgs-sec"><button type="button" className="tgs-row tgs-center tgs-danger" onClick={() => { say('Демо: показываем экран входа'); setStack(['login']); }}>Отвязать Telegram</button></div>
        <div className="tgs-f">Сигналы перестанут приходить в этот чат. Правила сохранятся.</div>
      </>
    );
  }

  return (
    <div className="tgs">
      <div className="tgs-col">
        {!tg && (
          <div className="tgs-bar">
            {stack.length > 1
              ? <button type="button" className="tgs-bar-l" onClick={back}><svg width="11" height="19" viewBox="0 0 11 19" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M9 2L2 9.5 9 17" /></svg>Назад</button>
              : <span />}
            <div className="tgs-bar-t">FRAME · Сигналы<small>мини-приложение</small></div>
            <span />
          </div>
        )}
        <div className="tgs-pg">{body}</div>
        {!tg && main && <div className="tgs-mb"><button type="button" onClick={main.run}>{main.text}</button></div>}
      </div>
      {toast && <div className="tgs-toast" role="status">{toast}</div>}
    </div>
  );
}
