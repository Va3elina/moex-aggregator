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
 * Механика записи: install() оборачивает localStorage.setItem — любая запись
 * отслеживаемого ключа планирует дебаунс-PUT полного снапшота. Записи из
 * iframe-панелей песочницы (embed на том же origin) в своём контексте setItem
 * не патчат наш, но порождают storage-событие в top-окне — его тоже слушаем.
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
 * по очереди). Ключи, есть только локально, доливаются на сервер после pull.
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

// Оригинальный setItem — для применения серверного снапшота без ре-триггера пуша.
const rawSetItem = typeof window !== 'undefined'
  ? Storage.prototype.setItem.bind(window.localStorage)
  : null;

let installed = false;
let hydrated = false;
let dirty = false;
let timer: number | undefined;

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
  try {
    const resp = await apiFetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ data: collect() }),
    });
    if (!resp.ok) dirty = true; // повторим при следующем изменении
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
 *  закрытие вкладки; apiFetch с refresh здесь не успеет). */
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

    if (!server || Object.keys(server).length === 0) {
      // Сервер пуст (первый вход с этого аккаунта) — заливаем локальное.
      if (Object.keys(collect()).length > 0) schedule();
      return;
    }

    let localOnly = false;
    for (const key of Object.keys(collect())) {
      if (!(key in server)) localOnly = true;
    }
    for (const [key, value] of Object.entries(server)) {
      // Применяем только отслеживаемые строки: серверный блоб не должен уметь
      // писать произвольные ключи (access_token и пр.).
      if (typeof value !== 'string' || !isTracked(key)) continue;
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

    // Локальные ключи, которых нет на сервере, доливаем (мердж-вверх).
    if (localOnly) schedule();
  } catch { /* сеть — молча, синк отключён до следующей загрузки */ }
}

/**
 * Ставит перехват записей. Зовётся один раз из main.tsx до рендера.
 */
export function installSettingsSync(): void {
  if (installed || typeof window === 'undefined' || !rawSetItem) return;
  installed = true;

  // Собственная запись поверх метода прототипа: все вызовы
  // localStorage.setItem(...) по проекту попадают сюда.
  window.localStorage.setItem = (key: string, value: string): void => {
    rawSetItem(key, value);
    if (isTracked(key)) schedule();
  };

  // Записи из iframe-панелей песочницы и других вкладок (storage-событие не
  // стреляет в контексте, который писал, — только в остальных). key=null
  // игнорируем: это clear() либо наше собственное синтетическое событие.
  window.addEventListener('storage', (e) => {
    if (e.key != null && isTracked(e.key)) schedule();
  });

  window.addEventListener('pagehide', flushOnHide);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') flushOnHide();
  });
}
