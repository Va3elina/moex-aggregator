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
  onEvent?(name: string, cb: () => void): void;
  offEvent?(name: string, cb: () => void): void;
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
/** Попытка входа через браузер. Лежит в localStorage, а не в sessionStorage: после возврата из браузера
 *  Telegram нередко открывает ДРУГОЕ окно кабинета, и оно должно знать о попытке (Вадим 17.11: «вернулся —
 *  а там снова „Войти"»). localStorage у окон кабинета общий. Живёт столько же, сколько код на сервере. */
const WAIT_KEY = 'tg_login_wait';
const WAIT_TTL_MS = 10 * 60 * 1000;
type Wait = { secret: string; code: string; at: number };
/** Один опрос за раз на всё окно: сервер отдаёт сессию ровно один раз, и второй одновременный опрос
 *  получил бы «попытка сгорела» поверх успешного первого. */
let pollBusy = false;
function readWait(): Wait | null {
  try {
    const w = JSON.parse(localStorage.getItem(WAIT_KEY) ?? 'null') as Wait | null;
    if (w && Date.now() - w.at < WAIT_TTL_MS) return w;
    localStorage.removeItem(WAIT_KEY);
  } catch { /* хранилище недоступно */ }
  return null;
}

type Screen = 'cabinet' | 'signals' | 'type' | 'hidden' | 'quiet';
type MinWindow = '3m' | '6m' | '1y' | 'all';
interface TypePrefs { on?: boolean; min?: MinWindow; markets?: string[]; modes?: string[] }
interface SignalPrefs { on?: boolean; types?: Record<string, TypePrefs>; hidden?: string[]; names?: Record<string, string>; quiet?: { on: boolean; from: string; to: string } }
interface AssetItem { key: string; code: string; name: string; kind: string }

/** Настройки сигналов — своя серверная ручка (api/routers/user_settings.py → ключ srv:signals:prefs, его читает бот
 *  signals/signal_prefs.py). Не localStorage + общая синхронизация: та затирала бы их со старых вкладок сайта. */
const PREFS_API = '/api/settings/signals';
/** Сохранения настроек строго по очереди (одно окно — одна очередь): частые нажатия не обгоняют друг друга. */
let saveChain: Promise<void> = Promise.resolve();
function queueSave(prefs: object, onError: (text: string) => void) {
  saveChain = saveChain.then(async () => {
    try {
      const r = await apiFetch(PREFS_API, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ prefs }) });
      if (!r.ok) onError('Не сохранилось — попробуйте ещё раз');
    } catch { onError('Нет связи — настройка не сохранилась'); }
  });
}
/** Виды сигналов бота — ключи как в signals/signal_prefs.TYPES; частота — по факту отправок. */
const SIGNAL_TYPES: { key: string; title: string; hint: string; window?: boolean; windowNote?: string; assets?: boolean }[] = [
  { key: 'oi_record', title: 'Рекорды позиций физлиц', hint: 'около 3 в торговый день', window: true, assets: true,
    windowNote: 'С «3 месяцев» сообщений примерно вчетверо больше, чем с «года». «Всё время» — самые редкие и громкие.' },
  { key: 'funds_flow', title: 'Рекорды притоков и оттоков фондов', hint: '1–2 в неделю', window: true,
    windowNote: 'Рекорд потока за неделю или месяц против прошлых столбиков того же среза.' },
  { key: 'funds_streak', title: 'Серии притоков и оттоков', hint: '1–2 в неделю' },
  { key: 'season', title: 'Сезонность', hint: 'около 20 в месяц', assets: true },
  { key: 'strength', title: 'Сила рынка', hint: 'около 5 в год' },
  { key: 'buffett', title: 'Индикатор Баффетта', hint: '2–3 в год' },
  { key: 'fund_trades', title: 'Сделки фондов', hint: 'в начале месяца, по мере отчётов' },
  { key: 'cbr_flows', title: 'Потоки капитала ЦБ', hint: 'раз в месяц' },
];
const MIN_WINDOWS: [MinWindow, string][] = [['3m', '3 мес'], ['6m', 'Полгода'], ['1y', 'Год'], ['all', 'Всё время']];
const MARKETS = ['Акции', 'Валюта', 'Индексы', 'Сырьё'];
const MODES: [string, string, string][] = [['positions', 'По объёму', 'сколько контрактов в лонге и шорте'], ['participants', 'По числу людей', 'сколько человек в лонге и шорте']];
const TIERS: Record<string, string> = { guest: 'Free', free: 'Free', basic: 'Basic', pro: 'Pro', premium: 'Premium', admin: 'Админ' };

const Chevron = () => (
  <svg width="8" height="14" viewBox="0 0 8 14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" style={{ flex: 'none', opacity: 0.35 }}><path d="M1 1l6 6-6 6" /></svg>
);
const Check = () => (
  <svg width="15" height="12" viewBox="0 0 15 12" fill="none" stroke="var(--accent)" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" style={{ flex: 'none' }}><path d="M1.5 6.5l4 4 8-9" /></svg>
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
  const { user, loading, isAuthenticated, login, logout, refreshUser } = useAuth();
  // Кнопка «Настройки» под сигналом открывает сразу экран сигналов (?s=signals) и говорит, откуда пришли (from, asset, name)
  const [ctx] = useState(() => {
    const q = new URLSearchParams(window.location.search);
    return { open: q.get('s') === 'signals', type: q.get('from'), asset: q.get('asset'), name: q.get('name') };
  });
  const [stack, setStack] = useState<Screen[]>(() => (ctx.open ? ['cabinet', 'signals'] : ['cabinet']));
  const screen = stack[stack.length - 1];
  // Настройки сигналов: с сервера (PREFS_API), каждое изменение сохраняется сразу. Пустые — приходит всё.
  const [prefs, setPrefsState] = useState<SignalPrefs>({});
  const [prefsReady, setPrefsReady] = useState(false);
  const [prefsError, setPrefsError] = useState(false);
  const [prefsTry, setPrefsTry] = useState(0);
  const [typeKey, setTypeKey] = useState<string>('oi_record');
  const [query, setQuery] = useState('');
  const [assets, setAssets] = useState<AssetItem[]>([]);
  const hidden = prefs.hidden ?? [];
  const hiddenNames = prefs.names ?? {};
  const setPrefs = (next: SignalPrefs) => {
    setPrefsState(next);
    queueSave(next, setToast);
  };
  const typeOn = (k: string) => prefs.types?.[k]?.on !== false;
  const setType = (k: string, patch: Partial<TypePrefs>) =>
    setPrefs({ ...prefs, types: { ...(prefs.types ?? {}), [k]: { ...(prefs.types?.[k] ?? {}), ...patch } } });
  /** Скрыть / вернуть актив; имя запоминаем рядом, чтобы список «Скрыты» был понятен и без поиска. */
  const toggleHidden = (key: string, hide: boolean, name?: string | null) => {
    const next = hide ? Array.from(new Set([...hidden, key])) : hidden.filter((k) => k !== key);
    setPrefs({ ...prefs, hidden: next, names: hide && name ? { ...hiddenNames, [key]: name } : hiddenNames });
    say(hide ? `${name ?? 'Актив'}: больше не присылаем` : `${name ?? 'Актив'}: снова присылаем`);
  };
  const typeSub = (k: string) => {
    const t = SIGNAL_TYPES.find((x) => x.key === k)!;
    if (!typeOn(k)) return 'не приходят';
    const mn = prefs.types?.[k]?.min;
    return t.window && mn && mn !== '3m' ? `от «${MIN_WINDOWS.find(([v]) => v === mn)?.[1].toLowerCase()}» · ${t.hint}` : t.hint;
  };
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

  // главная кнопка Telegram сейчас не нужна (настройки сохраняются сразу); место под неё оставлено
  const main = null as { text: string; run: () => void } | null;

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

  useEffect(() => {
    if (!isAuthenticated) return;
    let dead = false;
    (async () => {
      // не загрузились — редактировать нельзя: иначе первое нажатие сохранило бы «всё по умолчанию» поверх настоящих
      let ok = false;
      try {
        const r = await apiFetch(PREFS_API);
        if (r.ok) {
          const j = (await r.json()) as { prefs: SignalPrefs };
          if (!dead) setPrefsState(j.prefs ?? {});
          ok = true;
        }
      } catch { /* сеть — ниже */ }
      if (dead) return;
      setPrefsError(!ok);
      setPrefsReady(ok);
    })();
    return () => { dead = true; };
  }, [isAuthenticated, prefsTry]);

  // Список для поиска «Компании и активы»: фьючерсы (рекорды позиций, ключ oi:<базовый тикер>) и акции/индексы
  // (сезонность, ключ se:<тикер>) — тот же справочник инструментов, что у сайта.
  useEffect(() => {
    if (screen !== 'hidden' || assets.length) return;
    let dead = false;
    (async () => {
      try {
        const r = await apiFetch('/api/instruments');
        if (!r.ok) return;
        const j = (await r.json()) as { instruments: { sec_id: string; sectype: string; name: string; type: string; group?: string }[] };
        const seen = new Set<string>();
        const out: AssetItem[] = [];
        for (const i of j.instruments) {
          const fut = i.type === 'futures';
          const key = fut ? `oi:${i.sectype}` : `se:${i.sec_id}`;
          if (seen.has(key)) continue;
          seen.add(key);
          out.push({ key, code: fut ? i.sectype : i.sec_id, name: i.name, kind: fut ? `позиции · ${i.group ?? 'фьючерс'}` : 'сезонность' });
        }
        if (!dead) setAssets(out);
      } catch { /* сеть: поиск пустой, скрытые всё равно видны */ }
    })();
    return () => { dead = true; };
  }, [screen, assets.length]);

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
  const [wait, setWait] = useState<Wait | null>(readWait);
  const stopWait = () => { localStorage.removeItem(WAIT_KEY); setWait(null); };
  const openApprove = useCallback((code: string) => open(`${SITE}/tg/approve?c=${code}`), [open]);
  const browserLogin = async () => {
    try {
      const r = await apiFetch('/api/tg-login/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ init_data: tg?.initData ?? '' }) });
      if (r.status === 429) { say('Слишком много попыток. Подождите 10 минут'); return; }
      if (!r.ok) throw new Error(String(r.status));
      const { code, secret } = await r.json();
      const w = { secret, code, at: Date.now() };
      localStorage.setItem(WAIT_KEY, JSON.stringify(w));
      setWait(w);
      openApprove(code);
    } catch { say('Не получилось. Попробуйте ещё раз'); }
  };
  // Окон кабинета бывает несколько. Это могло не начинать вход — тогда при показе подхватываем попытку,
  // начатую в соседнем окне, или уже готовый вход (сосед получил сессию раньше нас).
  useEffect(() => {
    if (isAuthenticated) return;
    const sync = () => {
      if (document.visibilityState !== 'visible') return;
      if (localStorage.getItem('access_token') || localStorage.getItem('refresh_token')) { void refreshUser(); return; }
      setWait(readWait());
    };
    document.addEventListener('visibilitychange', sync);
    window.addEventListener('focus', sync);
    window.addEventListener('storage', sync);
    tg?.onEvent?.('activated', sync);
    return () => {
      document.removeEventListener('visibilitychange', sync); window.removeEventListener('focus', sync); window.removeEventListener('storage', sync);
      tg?.offEvent?.('activated', sync);
    };
  }, [isAuthenticated, refreshUser, tg]);
  // Опрашиваем только когда SDK Telegram готов: без подписи окна сервер отвечает 422 (17.11, видно в логах)
  const waitSecret = !isAuthenticated && ready ? wait?.secret : undefined;
  useEffect(() => {
    if (!waitSecret) return;
    let dead = false;
    const ask = async () => {
      // Вход мог уже забрать соседний экземпляр окна — тогда он лежит в хранилище, берём его сразу
      if (localStorage.getItem('access_token') || localStorage.getItem('refresh_token')) {
        localStorage.removeItem(WAIT_KEY); setWait(null); await refreshUser(); return;
      }
      if (pollBusy) return;
      pollBusy = true;
      try {
        // Короткий таймаут: запрос, «замёрзший» пока окно было в фоне, не должен держать опрос 20 секунд
        const r = await apiFetch('/api/tg-login/poll', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ secret: waitSecret, init_data: tg?.initData ?? '' }) }, 6000);
        const j = r.ok ? await r.json() : null;
        if (!j) return;
        if (j.status === 'approved') {
          // Сессию сервер выдаёт ОДИН раз, поэтому ответ нельзя выбрасывать, даже если этот эффект уже
          // пересоздан (так терялся вход: окно возвращалось из фона, эффект перезапускался, а пришедший
          // ответ отбрасывался как устаревший — 17.11, поймано отладкой на телефоне). Токены сразу в хранилище.
          localStorage.setItem('access_token', j.access_token);
          localStorage.setItem('refresh_token', j.refresh_token);
          localStorage.removeItem(WAIT_KEY);
          setWait(null);
          await login({ access_token: j.access_token, refresh_token: j.refresh_token });
          return;
        }
        if (dead) return;
        if (j.status === 'expired') {
          setWait(null);
          // Сессию мог только что забрать соседний экземпляр окна — тогда вход уже лежит в хранилище
          if (localStorage.getItem('access_token') || localStorage.getItem('refresh_token')) { await refreshUser(); return; }
          localStorage.removeItem(WAIT_KEY);
          say('Время вышло. Нажмите «Войти» ещё раз');
        }
      } catch { /* сеть: спросим на следующем круге */ } finally { pollBusy = false; }
    };
    (async () => { await ask(); })();          // окно открыли заново — спрашиваем сразу, не ждём круга
    const id = window.setInterval(ask, 2000);
    // Вернулся из браузера — спрашиваем сразу. В окне Telegram на Android надёжнее его событие «activated»
    const onShow = () => { if (document.visibilityState === 'visible') void ask(); };
    document.addEventListener('visibilitychange', onShow);
    window.addEventListener('focus', onShow);
    tg?.onEvent?.('activated', onShow);
    return () => {
      dead = true; window.clearInterval(id);
      document.removeEventListener('visibilitychange', onShow); window.removeEventListener('focus', onShow);
      tg?.offEvent?.('activated', onShow);
    };
  }, [waitSecret, tg, login, refreshUser, say]);
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
        <p>Потом вернитесь в Telegram.</p>
        <div className="tgs-spin" role="status" aria-label="Ждём подтверждения" />
        <button type="button" className="tgs-alt" onClick={() => openApprove(wait.code)}>Открыть браузер ещё раз</button>
        <button type="button" className="tgs-tl" onClick={stopWait}>Отмена</button>
      </div>
    ) : (
      <div className="tgs-welcome">
        {mark}
        {/* Только логотип, строка и одна кнопка (Вадим 17.11: заголовок и подсказки лишние). В Telegram вход
            идёт через обычный браузер — там сохранённые пароли; регистрация — на той же странице сайта. */}
        <p>Войдите, чтобы получать сигналы.</p>
        <button type="button" className="tgs-pr" onClick={tg ? browserLogin : () => window.location.assign(LOGIN_URL)}>Войти</button>
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
        {/* Админу тариф выбирать незачем: доступ даёт роль, а не подписка */}
        {user.role === 'admin' ? (
          <div className="tgs-sec">
            <div className="tgs-row"><span className="tgs-g"><span>Доступ администратора</span><span className="tgs-s">Все разделы открыты</span></span></div>
          </div>
        ) : (
          <>
          <div className="tgs-sec">
            <button type="button" className="tgs-row" onClick={() => open(`${SITE}/pricing`)}>
              <span className="tgs-g"><span>Тариф {tier}</span><span className="tgs-s">{until ? `${plan?.is_trial ? 'пробный период до' : plan?.cancelled_at ? 'действует до' : 'продлится'} ${until}` : 'Подписки нет'}</span></span>
              <span className="tgs-v tgs-accent">{until ? 'Изменить' : 'Выбрать'}</span><Chevron />
            </button>
          </div>
          <div className="tgs-f">Оплата открывается на сайте.</div>
          </>
        )}
        <div className="tgs-h">Уведомления</div>
        <div className="tgs-sec">
          {link?.linked
            ? <div className="tgs-row"><span className="tgs-g"><span>Telegram подключён</span><span className="tgs-s">{link.username ? `@${link.username}` : 'сигналы приходят в этот чат'}</span></span><Check /></div>
            : <button type="button" className="tgs-row tgs-accent" onClick={connect}><span className="tgs-g"><span>Подключить Telegram</span><span className="tgs-s">{tg ? 'Сигналы будут приходить в этот чат' : 'Откроется бот — нажмите «Запустить»'}</span></span><Chevron /></button>}
          {/* Сигналы бота пока приходят только админам — и настройку видят только они (до раскатки всем) */}
          {user.role === 'admin' && <button type="button" className="tgs-row" onClick={() => go('signals')}><span className="tgs-g"><span>Настройка сигналов</span><span className="tgs-s">Что присылать и что скрыть</span></span><Chevron /></button>}
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
  } else if ((screen === 'signals' || screen === 'type' || screen === 'hidden' || screen === 'quiet') && !prefsReady) {
    body = prefsError ? (
      <>
        <div className="tgs-f" style={{ paddingTop: 24 }}>Не получилось загрузить настройки. Пока они не загружены, менять их нельзя — чтобы не затереть сохранённые.</div>
        <div className="tgs-gap" />
        <div className="tgs-sec"><button type="button" className="tgs-row tgs-center tgs-accent" onClick={() => { setPrefsError(false); setPrefsTry((n) => n + 1); }}>Попробовать ещё раз</button></div>
      </>
    ) : <div className="tgs-f" style={{ paddingTop: 24 }}>Загружаем настройки…</div>;
  } else if (screen === 'signals') {
    const fromName = ctx.name || ctx.asset;
    const hiddenNow = !!ctx.asset && hidden.includes(ctx.asset);
    body = (
      <>
        {ctx.type && (
          <div className="tgs-ctx">
            <span className="tgs-g"><span>Вы пришли из сигнала{fromName ? ` по ${fromName}` : ''}</span><span className="tgs-s">{SIGNAL_TYPES.find((t) => t.key === ctx.type)?.title ?? ''}</span></span>
          </div>
        )}
        {ctx.asset && (
          <div className="tgs-sec">
            <div className="tgs-row"><span className="tgs-g">Не присылать по {fromName}</span>{sw(hiddenNow, `Не присылать по ${fromName}`, (v) => toggleHidden(ctx.asset!, v, fromName))}</div>
          </div>
        )}
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g"><span>{prefs.on !== false ? 'Сигналы включены' : 'Сигналы на паузе'}</span><span className="tgs-s">{prefs.on !== false ? 'Выключите — ничего не будет приходить' : 'Ничего не приходит, настройки сохранены'}</span></span>{sw(prefs.on !== false, 'Сигналы включены', (on) => setPrefs({ ...prefs, on }))}</div>
        </div>
        <div className="tgs-h">Что присылать</div>
        <div className="tgs-sec">
          {SIGNAL_TYPES.map((t) => (
            <div key={t.key} className="tgs-row">
              <button type="button" className="tgs-g tgs-btn" onClick={() => { setTypeKey(t.key); go('type'); }}><span>{t.title}</span><span className="tgs-s">{typeSub(t.key)}</span></button>
              {sw(typeOn(t.key), t.title, (on) => setType(t.key, { on }))}
            </div>
          ))}
        </div>
        <div className="tgs-f">Новые виды сигналов будут включаться сами.</div>
        <div className="tgs-h">Не присылать</div>
        <div className="tgs-sec">
          <button type="button" className="tgs-row" onClick={() => go('hidden')}><span className="tgs-g">Компании и активы</span><span className="tgs-v">{hidden.length ? `${hidden.length} скрыто` : 'все приходят'}</span><Chevron /></button>
          <button type="button" className="tgs-row" onClick={() => go('quiet')}><span className="tgs-g">Тихие часы</span><span className="tgs-v">{prefs.quiet?.on ? `${prefs.quiet.from}–${prefs.quiet.to}` : 'выключены'}</span><Chevron /></button>
        </div>
        <div className="tgs-f">Свои уведомления по уровням цены и позиций — на сайте, в разделе «Алерты».</div>
      </>
    );
  } else if (screen === 'type') {
    const t = SIGNAL_TYPES.find((x) => x.key === typeKey)!;
    const tp = prefs.types?.[t.key] ?? {};
    const markets = tp.markets ?? MARKETS;
    const modes = tp.modes ?? MODES.map(([k]) => k);
    const flip = <T extends string>(list: T[], v: T) => (list.includes(v) ? list.filter((x) => x !== v) : [...list, v]);
    body = (
      <>
        <div className="tgs-gap" />
        <div className="tgs-sec">
          <div className="tgs-row"><span className="tgs-g"><span>Присылать</span><span className="tgs-s">{t.hint}</span></span>{sw(typeOn(t.key), 'Присылать', (on) => setType(t.key, { on }))}</div>
        </div>
        {t.window && (
          <>
            <div className="tgs-h">Рекорд не короче</div>
            <Seg value={(tp.min ?? '3m') as MinWindow} options={MIN_WINDOWS} onChange={(min) => setType(t.key, { min })} />
            <div className="tgs-f">{t.windowNote}</div>
          </>
        )}
        {t.key === 'oi_record' && (
          <>
            <div className="tgs-h">Рынки</div>
            <div className="tgs-sec">
              {MARKETS.map((m) => (
                <button key={m} type="button" className="tgs-row" aria-pressed={markets.includes(m)} onClick={() => { tap(); setType(t.key, { markets: flip(markets, m) }); }}><span className="tgs-g">{m}</span>{markets.includes(m) && <Check />}</button>
              ))}
            </div>
            <div className="tgs-h">Позиция</div>
            <div className="tgs-sec">
              {MODES.map(([k, label, sub]) => (
                <button key={k} type="button" className="tgs-row" aria-pressed={modes.includes(k)} onClick={() => { tap(); setType(t.key, { modes: flip(modes, k) }); }}><span className="tgs-g"><span>{label}</span><span className="tgs-s">{sub}</span></span>{modes.includes(k) && <Check />}</button>
              ))}
            </div>
          </>
        )}
        {t.assets && (
          <>
            <div className="tgs-gap" />
            <div className="tgs-sec">
              <button type="button" className="tgs-row" onClick={() => go('hidden')}><span className="tgs-g">Компании и активы</span><span className="tgs-v">{hidden.length ? `все, кроме ${hidden.length}` : 'все'}</span><Chevron /></button>
            </div>
          </>
        )}
      </>
    );
  } else if (screen === 'hidden') {
    const q = query.trim().toLowerCase();
    const found = q ? assets.filter((a) => a.name.toLowerCase().includes(q) || a.code.toLowerCase().includes(q)).slice(0, 40) : [];
    const nameOf = (key: string) => assets.find((a) => a.key === key)?.name ?? hiddenNames[key] ?? key.split(':')[1] ?? key;
    body = (
      <>
        <div className="tgs-gap" />
        <input className="tgs-search" type="search" placeholder="Поиск: компания, валюта, товар" value={query} onChange={(e) => setQuery(e.target.value)} />
        {found.length > 0 && (
          <>
            <div className="tgs-h">Найдено</div>
            <div className="tgs-sec">
              {found.map((a) => (
                <button key={a.key} type="button" className="tgs-row" aria-pressed={hidden.includes(a.key)} onClick={() => { tap(); toggleHidden(a.key, !hidden.includes(a.key), a.name); }}>
                  <span className="tgs-g"><span>{a.name}</span><span className="tgs-s">{a.kind}</span></span>{hidden.includes(a.key) ? <span className="tgs-v">скрыт</span> : <span className="tgs-v tgs-accent">Скрыть</span>}
                </button>
              ))}
            </div>
          </>
        )}
        <div className="tgs-h">Скрыты</div>
        <div className="tgs-sec">
          {hidden.length === 0 && <div className="tgs-row"><span className="tgs-g tgs-dim">Ничего не скрыто — приходит всё</span></div>}
          {hidden.map((k) => (
            <div key={k} className="tgs-row"><span className="tgs-g"><span>{nameOf(k)}</span><span className="tgs-s">ни по одному сигналу</span></span><button type="button" className="tgs-lk" onClick={() => toggleHidden(k, false, nameOf(k))}>Вернуть</button></div>
          ))}
        </div>
        <div className="tgs-f">Фьючерс и акция компании — разные строки: фьючерс — рекорды позиций, акция — сезонность. Индексы и валюты сезонности скрываются кнопкой «Настройки» под их сигналом.</div>
      </>
    );
  } else if (screen === 'quiet') {
    const quiet = prefs.quiet ?? { on: false, from: '23:00', to: '08:00' };
    const setQuiet = (q: typeof quiet) => setPrefs({ ...prefs, quiet: q });
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
