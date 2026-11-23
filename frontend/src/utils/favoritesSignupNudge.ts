/**
 * Предложение гостю зарегистрироваться, когда он собирает избранное.
 *
 * Избранное гостя живёт только в localStorage этого браузера: очистил данные
 * сайта или зашёл с телефона — списка нет. После регистрации settingsSync
 * заливает его в аккаунт. Поэтому повод честный: человек уже вложился в список.
 *
 * Правила показа (исследование 2026-11: регистрация в «момент ценности»,
 * одно мягкое окно, без повторов в той же сессии):
 *  - только гостям, только в top-окне сайта (не /embed, не Telegram-аппы);
 *  - со 2-го добавления актива в избранное (суммарно, между визитами) —
 *    дефолтный список и первая звёздочка окна не вызывают;
 *  - не чаще раза в сессию и раз в 7 дней, максимум 3 показа на браузер.
 *
 * Звёздочки зовут noteFavoriteAdded(); само окно (FavoritesSignupModal)
 * слушает событие FAV_SIGNUP_EVENT. Ключи с префиксом `frame_` (подчёркивание)
 * settingsSync не синкает — это состояние именно этого браузера.
 */

const STATE_KEY = 'frame_favsignup';
const SESSION_KEY = 'frame_favsignup_session';
export const FAV_SIGNUP_EVENT = 'frame:favorites-signup';

const MIN_ADDS = 2;
const MAX_SHOWS = 3;
const COOLDOWN_MS = 7 * 24 * 60 * 60 * 1000;

interface NudgeState {
  adds: number;
  shows: number;
  lastShownAt: number;
}

function readState(): NudgeState {
  try {
    const raw = localStorage.getItem(STATE_KEY);
    const v = raw ? JSON.parse(raw) as Partial<NudgeState> : {};
    return {
      adds: Number(v.adds) || 0,
      shows: Number(v.shows) || 0,
      lastShownAt: Number(v.lastShownAt) || 0,
    };
  } catch {
    return { adds: 0, shows: 0, lastShownAt: 0 };
  }
}

function writeState(s: NudgeState): void {
  try { localStorage.setItem(STATE_KEY, JSON.stringify(s)); } catch { /* quota / private mode */ }
}

function isGuest(): boolean {
  try {
    return !localStorage.getItem('refresh_token') && !localStorage.getItem('access_token');
  } catch {
    return false;
  }
}

/** Только сам сайт: в iframe терминала и в Telegram-аппах окно неуместно. */
function isTopSite(): boolean {
  try {
    if (window.self !== window.top) return false;
  } catch {
    return false;
  }
  const p = window.location.pathname;
  return !p.startsWith('/embed') && !p.startsWith('/tg') && !p.startsWith('/signal-export');
}

/** Звать при ДОБАВЛЕНИИ актива в избранное (не при снятии звёздочки). */
export function noteFavoriteAdded(): void {
  if (typeof window === 'undefined' || !isGuest() || !isTopSite()) return;
  const s = readState();
  s.adds += 1;
  writeState(s);

  if (s.adds < MIN_ADDS || s.shows >= MAX_SHOWS) return;
  if (Date.now() - s.lastShownAt < COOLDOWN_MS) return;
  try {
    if (sessionStorage.getItem(SESSION_KEY)) return;
  } catch { /* sessionStorage недоступен — полагаемся на cooldown */ }

  window.dispatchEvent(new CustomEvent(FAV_SIGNUP_EVENT));
}

/** Окно реально показано — учитываем для лимитов. */
export function markFavoritesSignupShown(): void {
  const s = readState();
  s.shows += 1;
  s.lastShownAt = Date.now();
  writeState(s);
  try { sessionStorage.setItem(SESSION_KEY, '1'); } catch { /* ignore */ }
}
