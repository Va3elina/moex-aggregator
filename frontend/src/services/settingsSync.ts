import { apiFetch } from './api';
import { readPalette, applyPalette } from '../hooks/useChartPalette';

/**
 * Синхронизация настроек юзера между устройствами через /api/settings.
 *
 * Все пользовательские настройки фронта живут в localStorage (usePersistedState,
 * песочница frame:sandbox:v2, панели embed frame:embed:*, избранное, тема).
 * Здесь они собираются в один снапшот {ключ: сырая строка} и хранятся на
 * сервере per-user; на другом устройстве снапшот применяется ДО первого рендера
 * страниц (AuthContext блокирует рендер до конца loadUser → все
 * usePersistedState-инициализаторы читают уже гидрированный localStorage,
 * менять их не нужно).
 *
 * Механика записи: install() оборачивает localStorage.setItem/removeItem —
 * любая запись отслеживаемого ключа планирует дебаунс-PUT полного снапшота.
 * Записи из iframe-панелей песочницы (embed на том же origin) в своём
 * контексте setItem не патчат наш, но порождают storage-событие в top-окне —
 * его тоже слушаем.
 *
 * Неподтверждённые локальные изменения (PENDING_KEY): каждый изменённый ключ
 * помечается временем изменения и снимается с пометки только после успешного
 * PUT. Pull при загрузке НЕ перетирает помеченные ключи серверными значениями.
 * Без этого любой сбой пуша (протухший токен в keepalive при закрытии вкладки,
 * 400 от сервера, сеть) приводил к откату: на следующей загрузке старый
 * серверный снимок затирал свежий тумблер, и настройка «не сохранялась».
 *
 * Гварды против затирания серверных настроек мусором:
 *  - пуш только из top-окна и НЕ на /embed: embed в Т-терминале живёт в
 *    partitioned-хранилище (почти пустом) и получает access_token обменом
 *    ext-токена (EmbedPage) — пуш оттуда снёс бы настройки юзера;
 *  - пуш только при наличии refresh_token (живая сессия сайта, в embed его нет);
 *  - пуш только после успешного pull (hydrated): не перезаписывать сервер,
 *    пока не знаем его состояние.
 *
 * Конфликты: last-write-wins целиком блобом (один человек, устройства меняются
 * по очереди), кроме неподтверждённых локальных ключей — они побеждают.
 * Ключи, есть только локально, доливаются на сервер после pull.
 */

/** Отслеживаемые префиксы: настройки индикаторов/песочницы/embed (frame:*),
 *  флаги «тур показан» и сравнения сезонности. `frame_` (подчёркивание) сюда
 *  НЕ входит — это служебные ключи (session id, consent, api-docs key). */
const SYNC_PREFIXES = ['frame:', 'frame_tour_seen_', 'seasonality_compare_'];
/** Точечные ключи вне префиксов: тема, избранное. */
const SYNC_KEYS = new Set(['theme', 'accent', 'favoriteInstruments', 'favoriteFundTradeAssets']);
/** Девайс-локальные исключения из frame:*: guard перезагрузки чанков и
 *  межвкладочный leader-election аномалий — синкать их бессмысленно/вредно. */
const EXCLUDE_KEYS = new Set(['frame:chunk-reload-at', 'frame:embed:anomaly:leader']);

/** {ключ: время локального изменения, мс} — изменения, ещё не подтверждённые
 *  сервером. Префикс `frame_` → сам не синкается. */
const PENDING_KEY = 'frame_sync_pending';
/** Старый серверный лимит ключей. Снапшоты больше него сервер отклонял (400),
 *  то есть локальное состояние таких юзеров новее серверного — см. hydrate. */
const LEGACY_MAX_KEYS = 1000;
const LEGACY_DONE_KEY = 'frame_sync_legacy_done';

const DEBOUNCE_MS = 1500;

function isTracked(key: string): boolean {
  if (EXCLUDE_KEYS.has(key)) return false;
  if (SYNC_KEYS.has(key)) return true;
  return SYNC_PREFIXES.some((p) => key.startsWith(p));
}

/** Пуш разрешён только top-окну сайта (не iframe, не /embed). */
function isTopApp(): boolean {
  try {
    return window.self === window.top && !window.location.pathname.startsWith('/embed');
  } catch {
    return false; // cross-origin parent → мы точно в чужом iframe
  }
}

function hasSession(): boolean {
  try {
    return !!localStorage.getItem('refresh_token');
  } catch {
    return false;
  }
}

// Оригинальные методы — для применения серверного снапшота и служебных
// записей без ре-триггера пуша.
const rawSetItem = typeof window !== 'undefined'
  ? Storage.prototype.setItem.bind(window.localStorage)
  : null;
const rawRemoveItem = typeof window !== 'undefined'
  ? Storage.prototype.removeItem.bind(window.localStorage)
  : null;

let installed = false;
let hydrated = false;
let dirty = false;
let timer: number | undefined;

// Кэш в памяти: markPending зовётся на каждую запись, парсить JSON каждый раз
// незачем. Пишет пометки только top-окно (см. markPending), так что кэш не
// расходится с хранилищем из-за iframe-панелей.
let pendingCache: Record<string, number> | null = null;

function readPending(): Record<string, number> {
  if (pendingCache) return pendingCache;
  try {
    const raw = localStorage.getItem(PENDING_KEY);
    const v = raw ? JSON.parse(raw) : null;
    pendingCache = v && typeof v === 'object' && !Array.isArray(v) ? v as Record<string, number> : {};
  } catch {
    pendingCache = {};
  }
  return pendingCache;
}

function writePending(p: Record<string, number>): void {
  pendingCache = p;
  if (!rawSetItem || !rawRemoveItem) return;
  try {
    if (Object.keys(p).length === 0) rawRemoveItem(PENDING_KEY);
    else rawSetItem(PENDING_KEY, JSON.stringify(p));
  } catch { /* quota */ }
}

function markPending(key: string): void {
  // iframe-панели песочницы помечать не должны: их запись долетит до top-окна
  // storage-событием, и пометит уже оно (единственный писатель PENDING_KEY).
  if (!isTopApp()) return;
  const p = readPending();
  p[key] = Date.now();
  writePending(p);
}

/** Снять пометку с ключей, изменённых не позже `upTo` (их значение ушло в PUT). */
function clearPending(upTo: number): void {
  const p = readPending();
  let changed = false;
  for (const [k, ts] of Object.entries(p)) {
    if (ts <= upTo) { delete p[k]; changed = true; }
  }
  if (changed) writePending(p);
}

function collect(): Record<string, string> {
  const data: Record<string, string> = {};
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key == null || !isTracked(key)) continue;
      const v = localStorage.getItem(key);
      if (v != null) data[key] = v;
    }
  } catch { /* private mode */ }
  return data;
}

async function push(): Promise<void> {
  if (!hydrated || !isTopApp() || !hasSession()) return;
  dirty = false;
  const startedAt = Date.now();
  try {
    const resp = await apiFetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ data: collect() }),
    });
    if (resp.ok) clearPending(startedAt);
    else dirty = true; // повторим при следующем изменении / загрузке
  } catch {
    dirty = true;
  }
}

function schedule(): void {
  if (!hydrated || !isTopApp() || !hasSession()) return;
  dirty = true;
  window.clearTimeout(timer);
  timer = window.setTimeout(() => { void push(); }, DEBOUNCE_MS);
}

/** Синхронный best-effort пуш при уходе со страницы (keepalive переживает
 *  закрытие вкладки; apiFetch с refresh здесь не успеет). Результат неизвестен,
 *  поэтому пометки pending НЕ снимаем — следующая загрузка их дошлёт. */
function flushOnHide(): void {
  if (!dirty || !hydrated || !isTopApp() || !hasSession()) return;
  const token = localStorage.getItem('access_token');
  if (!token) return;
  dirty = false;
  try {
    void fetch('/api/settings', {
      method: 'PUT',
      keepalive: true,
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ data: collect() }),
    });
  } catch { /* best-effort */ }
}

/**
 * Тянет серверный снапшот и применяет к localStorage. Зовётся из
 * AuthContext.loadUser ПОСЛЕ успешного /me и ДО setLoading(false) — страницы
 * ещё не отрендерены. Никогда не бросает. Провал GET оставляет hydrated=false:
 * в этой сессии пушей не будет (лучше не синкнуть, чем затереть сервер).
 */
export async function hydrateSettingsFromServer(): Promise<void> {
  if (!isTopApp() || !rawSetItem) return;
  try {
    const resp = await apiFetch('/api/settings');
    if (!resp.ok) return;
    const body = await resp.json() as { data: Record<string, string> | null };
    const server = body.data;
    hydrated = true;

    const local = collect();
    if (!server || Object.keys(server).length === 0) {
      // Сервер пуст (первый вход с этого аккаунта) — заливаем локальное.
      if (Object.keys(local).length > 0) schedule();
      return;
    }

    // Локальные изменения, которые сервер ещё не подтвердил, — не трогаем.
    const pending = readPending();
    // Снапшот больше старого лимита сервер отклонял → сервер замёрз, а локальное
    // состояние новее. Один раз (флаг LEGACY_DONE_KEY) считаем всё локальное
    // неподтверждённым — дальше обычный режим.
    if (!localStorage.getItem(LEGACY_DONE_KEY)) {
      if (Object.keys(local).length > LEGACY_MAX_KEYS) {
        const now = Date.now();
        for (const k of Object.keys(local)) if (!(k in pending)) pending[k] = now;
        writePending(pending);
      }
      try { rawSetItem(LEGACY_DONE_KEY, '1'); } catch { /* quota */ }
    }

    let needPush = Object.keys(pending).length > 0;
    for (const key of Object.keys(local)) {
      if (!(key in server)) needPush = true;
    }
    for (const [key, value] of Object.entries(server)) {
      // Применяем только отслеживаемые строки: серверный блоб не должен уметь
      // писать произвольные ключи (access_token и пр.).
      if (typeof value !== 'string' || !isTracked(key)) continue;
      if (key in pending) { needPush = true; continue; }
      try { rawSetItem(key, value); } catch { /* quota */ }
    }

    // Живые сторы поверх localStorage (useChartSettings, ThemeProvider) уже
    // инициализировались до pull — будим их синтетическим storage-событием
    // (key=null у них означает «перечитай всё»). Палитру переприменяем сами:
    // applyPalette уже отработал в main.tsx со старым значением.
    try {
      window.dispatchEvent(new StorageEvent('storage', { key: null }));
      applyPalette(readPalette());
    } catch { /* не критично */ }

    // Локальные ключи, которых нет на сервере, и неподтверждённые изменения
    // доливаем (мердж-вверх).
    if (needPush) schedule();
  } catch { /* сеть — молча, синк отключён до следующей загрузки */ }
}

/**
 * Ставит перехват записей. Зовётся один раз из main.tsx до рендера.
 */
export function installSettingsSync(): void {
  if (installed || typeof window === 'undefined' || !rawSetItem || !rawRemoveItem) return;
  installed = true;

  // Собственная запись поверх метода прототипа: все вызовы
  // localStorage.setItem/removeItem(...) по проекту попадают сюда.
  window.localStorage.setItem = (key: string, value: string): void => {
    rawSetItem(key, value);
    if (isTracked(key)) { markPending(key); schedule(); }
  };
  window.localStorage.removeItem = (key: string): void => {
    rawRemoveItem(key);
    if (isTracked(key)) { markPending(key); schedule(); }
  };

  // Записи из iframe-панелей песочницы и других вкладок (storage-событие не
  // стреляет в контексте, который писал, — только в остальных). key=null
  // игнорируем: это clear() либо наше собственное синтетическое событие.
  window.addEventListener('storage', (e) => {
    if (e.key != null && isTracked(e.key)) { markPending(e.key); schedule(); }
  });

  window.addEventListener('pagehide', flushOnHide);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') flushOnHide();
  });
}
