// Мини-приложение Telegram «FRAME · Сигналы» (/tg) — кабинет Фрейма внутри бота @framesignalbot.
// Вход — ТОЛЬКО обычной страницей входа сайта (/login?next=/tg): входить «по Telegram» нельзя (406-ФЗ, см. oauth.py).
// Telegram здесь — канал уведомлений: привязка идёт прежней ссылкой с одноразовым кодом (/api/alerts/telegram/link).
// Кабинет (аккаунт, тариф, привязка) — настоящие данные; настройка сигналов пока демо, сервер к ней не подключён.
// Карта продукта: https://claude.ai/artifact/NDXgE94mSgqDgaTVzg2Zp9
// Внутри Telegram шапку, «Назад» и главную кнопку рисует сам Telegram (SDK); в обычном браузере — свои.
import { useCallback, useEffect, useState, type ReactNode } from 'react';
import { useAuth } from '../../contexts/AuthContext';
import { apiFetch } from '../../services/api';
import './tg.css';

interface TgButton { show(): void; hide(): void; onClick(cb: () => void): void; offClick(cb: () => void): void }
interface TgWebApp {
  platform: string;
  initData: string;
  ready(): void;
  expand(): void;
  openLink(url: string): void;
  openTelegramLink?(url: string): void;
  close(): void;
  setHeaderColor?(color: string): void;
  showConfirm?(message: string, cb: (ok: boolean) => void): void;
  BackButton: TgButton;
  MainButton: TgButton & { setParams(p: { text?: string; color?: string; text_color?: string }): void };
  HapticFeedback?: { selectionChanged(): void };
}

const SDK = 'https://telegram.org/js/telegram-web-app.js';
const SITE = 'https://framedata.ru';

/** SDK Telegram. tg = null — открыто в обычном браузере (SDK там есть, но platform = unknown);
 *  ready = false, пока SDK не загрузился: до этого не знаем, в Telegram мы или нет. */
function useTelegram(): { tg: TgWebApp | null; ready: boolean } {
  const [state, setState] = useState<{ tg: TgWebApp | null; ready: boolean }>({ tg: null, ready: false });
  useEffect(() => {
    const pick = () => {
      const w = (window as unknown as { Telegram?: { WebApp?: TgWebApp } }).Telegram?.WebApp;
      const inside = !!w && !!w.platform && w.platform !== 'unknown';
      if (inside) {
        w.ready();
        w.expand();
        w.setHeaderColor?.('secondary_bg_color');
      }
      setState({ tg: inside ? w : null, ready: true });
    };
    const old = document.querySelector<HTMLScriptElement>(`script[src="${SDK}"]`);
    if (old) {
      // Скрипт остался от прошлого захода на страницу: SDK уже в window
      const id = window.setTimeout(pick, 0);
      return () => window.clearTimeout(id);
    }
    const s = document.createElement('script');
    s.src = SDK;
    s.onload = pick;
    s.onerror = pick;          // SDK не загрузился (блокировка) — работаем как в браузере
    document.head.appendChild(s);
  }, []);
  return state;
}

/** Человек сам отвязал Telegram в этом окне — не привязываем обратно при следующем открытии. */
const NO_AUTOLINK = 'tg_no_autolink';
const LOGIN_URL = '/login?next=/tg';
/** Секрет попытки входа через браузер: переживает переоткрытие окна, пока код жив (10 минут). */
const WAIT_KEY = 'tg_login_wait';

type Screen = 'cabinet' | 'signals' | 'rule' | 'asset' | 'sets' | 'quiet';
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
const TIERS: Record<string, string> = { guest: 'Free', free: 'Free', basic: 'Basic', pro: 'Pro', premium: 'Premium', admin: 'Админ' };

const DEMO_RULES: Rule[] = [
  { id: 1, asset: 'Si', group: 'fiz', kind: 'move', power: 3, level: 10000, period: 'year', repeat: true, every: '5m', paused: false, last: 'вчера' },
  { id: 2, asset: 'CNY', group: 'fiz', kind: 'level', power: 3, level: 12000, period: 'year', repeat: false, every: '1d', paused: false, last: 'ждёт' },
  { id: 3, asset: 'NG', group: 'fiz', kind: 'record', power: 3, level: 10000, period: 'year', repeat: true, every: '1d', paused: true, last: 'пауза' },
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
  const { tg, ready } = useTelegram();
  const { user, loading, isAuthenticated, login, logout } = useAuth();
  const [stack, setStack] = useState<Screen[]>(['cabinet']);
  const screen = stack[stack.length - 1];
  const [enabled, setEnabled] = useState(true);
  const [sets, setSets] = useState({ records: true, moves: false, season: false, minWindow: 'half', groups: ['Акции', 'Валюта', 'Сырьё'], digest: false });
  const [muted, setMuted] = useState([{ asset: 'VK', until: 'тихо до 18 ноября, 14:35' }]);
  const [rules, setRules] = useState(DEMO_RULES);
  const [draft, setDraft] = useState<Rule | null>(null);
  const [quiet, setQuiet] = useState({ on: true, from: '23:00', to: '08:00' });
  const [plan, setPlan] = useState<{ tier: string; expires_at: string | null; cancelled_at: string | null; is_trial: boolean } | null>(null);
  const [link, setLink] = useState<{ linked: boolean; username: string | null } | null>(null);
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
    confirmThen(`Удалить правило по «${title(draft.asset)}»?`, () => { setRules((rs) => rs.filter((r) => r.id !== draft.id)); say('Правило удалено'); back(); });
  };
  const patch = (p: Partial<Rule>) => setDraft((d) => (d ? { ...d, ...p } : d));

  // Кабинет: тариф и привязка Telegram — настоящие. Перечитываем при возврате в окно (после привязки в боте).
  useEffect(() => {
    if (!isAuthenticated) return;
    let dead = false;
    const load = async () => {
      try {
        const [b, t] = await Promise.all([apiFetch('/api/billing/status'), apiFetch('/api/alerts/telegram/status')]);
        const [bj, tj] = [b.ok ? await b.json() : null, t.ok ? await t.json() : null];
        if (dead) return;
        if (bj) setPlan(bj);
        if (tj) setLink(tj);
      } catch { /* сеть: оставляем прежние значения */ }
    };
    (async () => { await load(); })();
    const onShow = () => { if (document.visibilityState === 'visible') void load(); };
    document.addEventListener('visibilitychange', onShow);
    window.addEventListener('focus', onShow);
    return () => { dead = true; document.removeEventListener('visibilitychange', onShow); window.removeEventListener('focus', onShow); };
  }, [isAuthenticated]);

  // Привязка чата. В Telegram — сразу по данным мини-приложения (без кода и «Запустить»);
  // в браузере — прежней ссылкой с одноразовым кодом.
  const linkHere = useCallback(async (): Promise<boolean> => {
    if (!tg?.initData) return false;
    const r = await apiFetch('/api/alerts/telegram/link-webapp', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ init_data: tg.initData }) });
    if (!r.ok) return false;
    setLink(await r.json());
    return true;
  }, [tg]);
  const connect = async () => {
    try {
      localStorage.removeItem(NO_AUTOLINK);
      if (tg?.initData) {
        if (await linkHere()) say('Telegram подключён'); else say('Не получилось. Откройте кабинет из бота заново');
        return;
      }
      const r = await apiFetch('/api/alerts/telegram/link', { method: 'POST' });
      if (!r.ok) throw new Error(String(r.status));
      const { deep_link } = await r.json();
      window.open(deep_link, '_blank', 'noopener');
    } catch { say('Не получилось. Попробуйте ещё раз'); }
  };
  // Вошёл в Telegram, а чат ещё не привязан — привязываем сами, один раз за открытие
  const needLink = !!tg?.initData && isAuthenticated && link !== null && !link.linked;
  useEffect(() => {
    if (!needLink || localStorage.getItem(NO_AUTOLINK)) return;
    let dead = false;
    (async () => { try { if (await linkHere() && !dead) say('Telegram подключён'); } catch { /* останется кнопка «Подключить» */ } })();
    return () => { dead = true; };
  }, [needLink, linkHere, say]);
  // Вход через обычный браузер: в окне Telegram нет сохранённых паролей и входа в Яндекс/VK, а там есть.
  // Берём одноразовый код, открываем с ним сайт в браузере и ждём, пока человек подтвердит (browser_login.py).
  const [wait, setWait] = useState<{ secret: string; code: string; pin: string } | null>(() => {
    try { return JSON.parse(sessionStorage.getItem(WAIT_KEY) ?? 'null'); } catch { return null; }
  });
  const stopWait = useCallback(() => { sessionStorage.removeItem(WAIT_KEY); setWait(null); }, []);
  const openApprove = useCallback((code: string) => open(`${SITE}/tg/approve?c=${code}`), [open]);
  const browserLogin = async () => {
    try {
      const r = await apiFetch('/api/tg-login/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ init_data: tg?.initData ?? '' }) });
      if (r.status === 429) { say('Слишком много попыток. Подождите 10 минут'); return; }
      if (!r.ok) throw new Error(String(r.status));
      const { code, secret, pin } = await r.json();
      sessionStorage.setItem(WAIT_KEY, JSON.stringify({ secret, code, pin }));
      setWait({ secret, code, pin });
      openApprove(code);
    } catch { say('Не получилось. Попробуйте ещё раз или введите логин здесь'); }
  };
  const waitSecret = !isAuthenticated ? wait?.secret : undefined;
  useEffect(() => {
    if (!waitSecret) return;
    let dead = false;
    const ask = async () => {
      try {
        const r = await apiFetch('/api/tg-login/poll', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ secret: waitSecret, init_data: tg?.initData ?? '' }) });
        const j = r.ok ? await r.json() : null;
        if (dead || !j) return;
        if (j.status === 'approved') { stopWait(); await login({ access_token: j.access_token, refresh_token: j.refresh_token }); }
        else if (j.status === 'expired') { stopWait(); say('Время вышло. Нажмите «Войти через браузер» ещё раз'); }
      } catch { /* сеть: спросим на следующем круге */ }
    };
    const id = window.setInterval(ask, 3000);
    const onShow = () => { if (document.visibilityState === 'visible') void ask(); };   // вернулся из браузера — спрашиваем сразу
    document.addEventListener('visibilitychange', onShow);
    window.addEventListener('focus', onShow);
    return () => { dead = true; window.clearInterval(id); document.removeEventListener('visibilitychange', onShow); window.removeEventListener('focus', onShow); };
  }, [waitSecret, tg, login, stopWait, say]);

  const confirmThen = (question: string, run: () => void) => {
    if (tg?.showConfirm) tg.showConfirm(question, (ok) => { if (ok) run(); });
    else if (window.confirm(question)) run();
  };
  const disconnect = () => confirmThen('Отвязать Telegram? Сигналы перестанут приходить в этот чат.', async () => {
    const r = await apiFetch('/api/alerts/telegram', { method: 'DELETE' });
    if (r.ok) { localStorage.setItem(NO_AUTOLINK, '1'); setLink({ linked: false, username: null }); say('Telegram отвязан'); } else say('Не получилось. Попробуйте ещё раз');
  });
  const signOut = () => confirmThen('Выйти из аккаунта?', () => { void logout(); setStack(['cabinet']); });

  let body: ReactNode = null;
  if (loading || !ready) {
    body = null;
  } else if (!isAuthenticated || !user) {
    const mark = <svg width="72" height="72" viewBox="0 0 32 32" aria-hidden="true"><rect width="32" height="32" rx="8" fill="var(--accent)" /><path d="M7 7h6.5v2.6H9.6v3.9H7zM25 7h-6.5v2.6h3.9v3.9H25zM7 25h6.5v-2.6H9.6v-3.9H7zM25 25h-6.5v-2.6h3.9v-3.9H25z" fill="#fff" /></svg>;
    body = wait ? (
      <div className="tgs-welcome">
        {mark}
        <h1>Подтвердите вход в браузере</h1>
        <p>Там откроется сайт Фрейма: нажмите «Подтвердить» и вернитесь сюда. Кабинет откроется сам.</p>
        <div className="tgs-pin"><span>Код для сверки</span><b>{wait.pin}</b></div>
        <div className="tgs-spin" role="status" aria-label="Ждём подтверждения" />
        <button type="button" className="tgs-alt" onClick={() => openApprove(wait.code)}>Открыть браузер ещё раз</button>
        <button type="button" className="tgs-tl" onClick={stopWait}>Отмена</button>
      </div>
    ) : (
      <div className="tgs-welcome">
        {mark}
        <h1>Кабинет Фрейма</h1>
        <p>Войдите в аккаунт — и сигналы будут приходить в этот чат.</p>
        {tg
          ? <button type="button" className="tgs-pr" onClick={browserLogin}>Войти через браузер</button>
          : <button type="button" className="tgs-pr" onClick={() => window.location.assign(LOGIN_URL)}>Войти</button>}
        {tg && <div className="tgs-f" style={{ textAlign: 'center', padding: 0 }}>Быстрее всего: в браузере уже есть ваши пароли и вход в Яндекс.</div>}
        {tg && <button type="button" className="tgs-alt" onClick={() => window.location.assign(LOGIN_URL)}>Ввести логин здесь</button>}
        <button type="button" className="tgs-tl" onClick={() => window.location.assign('/login?mode=register&next=/tg')}>Зарегистрироваться</button>
      </div>
    );
  } else if (screen === 'cabinet') {
    const name = user.display_name || user.email.split('@')[0];
    const tier = TIERS[plan?.tier ?? ''] ?? '—';
    const until = plan?.expires_at ? new Date(plan.expires_at).toLocaleDateString('ru-RU', { day: 'numeric', month: 'long', year: 'numeric' }) : null;
    body = (
      <>
        <div className="tgs-me">
          <div className="tgs-ava">{name[0].toUpperCase()}</div>
          <div className="tgs-name">{name}</div>
          <div className="tgs-s" style={{ fontSize: 15, lineHeight: '20px' }}>{user.requires_email_setup ? 'почта не указана' : user.email}</div>
        </div>
        <div className="tgs-sec">
          <button type="button" className="tgs-row" onClick={() => open(`${SITE}/pricing`)}>
            <span className="tgs-g"><span>Тариф {tier}</span><span className="tgs-s">{until ? `${plan?.is_trial ? 'пробный период до' : plan?.cancelled_at ? 'действует до' : 'продлится'} ${until}` : 'Подписки нет'}</span></span>
            <span className="tgs-v tgs-accent">{until ? 'Изменить' : 'Выбрать'}</span><Chevron />
          </button>
        </div>
        <div className="tgs-f">Оплата открывается на сайте.</div>
        <div className="tgs-h">Уведомления</div>
        <div className="tgs-sec">
          {link?.linked
            ? <div className="tgs-row"><span className="tgs-g"><span>Telegram подключён</span><span className="tgs-s">{link.username ? `@${link.username}` : 'сигналы приходят в этот чат'}</span></span><Check /></div>
            : <button type="button" className="tgs-row tgs-accent" onClick={connect}><span className="tgs-g"><span>Подключить Telegram</span><span className="tgs-s">{tg ? 'Сигналы будут приходить в этот чат' : 'Откроется бот — нажмите «Запустить»'}</span></span><Chevron /></button>}
          {/* Настройка сигналов пока демо — показываем только админам, клиентам кабинет без ненастоящего */}
          {user.role === 'admin' && <button type="button" className="tgs-row" onClick={() => go('signals')}><span className="tgs-g"><span>Настройка сигналов</span><span className="tgs-s">Демо: пока не сохраняется</span></span><Chevron /></button>}
        </div>
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <button type="button" className="tgs-row" onClick={() => open(`${SITE}/profile`)}><span className="tgs-g">Профиль на сайте</span><Chevron /></button>
          <button type="button" className="tgs-row" onClick={() => open(`${SITE}/contacts`)}><span className="tgs-g">Написать в поддержку</span><Chevron /></button>
        </div>
        <div className="tgs-gap" />
        <div className="tgs-sec">
          {link?.linked && <button type="button" className="tgs-row tgs-center tgs-danger" onClick={disconnect}>Отвязать Telegram</button>}
          <button type="button" className="tgs-row tgs-center tgs-danger" onClick={signOut}>Выйти из аккаунта</button>
        </div>
      </>
    );
  } else if (screen === 'signals') {
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
          <button type="button" className="tgs-row" onClick={() => go('quiet')}><span className="tgs-g">Тихие часы</span><span className="tgs-v">{quiet.on ? `${quiet.from}–${quiet.to}` : 'выключены'}</span><Chevron /></button>
        </div>
        <div className="tgs-demo">Демо: данные условные, изменения не сохраняются. Настоящая настройка — следующим шагом.</div>
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
