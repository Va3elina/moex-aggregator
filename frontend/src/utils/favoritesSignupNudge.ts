/**
 * Лимит избранного для гостя и предложение зарегистрироваться.
 *
 * Избранное гостя живёт только в localStorage этого браузера: очистил данные
 * сайта или зашёл с телефона — списка нет. После регистрации settingsSync
 * заливает его в аккаунт.
 *
 * Правило (решение владельца 2026-11-24, вместо мягкого окна раз в 7 дней,
 * которое гость просто закрывал и продолжал собирать список):
 *  - гость может добавить в избранное GUEST_FAVORITES_LIMIT своих активов;
 *    стартовый список (DEFAULT_FAVORITES) в лимит не входит;
 *  - следующая звёздочка НЕ ставится — вместо неё каждый раз открывается
 *    окно регистрации (FavoritesSignupModal слушает FAV_SIGNUP_EVENT);
 *  - снять звёздочку можно всегда — освободившийся слот снова доступен;
 *  - только в top-окне сайта: в /embed (терминал) и Telegram-аппах лимита нет.
 */

export const FAV_SIGNUP_EVENT = 'frame:favorites-signup';

export const GUEST_FAVORITES_LIMIT = 2;

/** Стартовое избранное пикеров (InstrumentSearchModal / MobileAssetSearch). */
const DEFAULT_FAVORITES = new Set(['SR', 'GZ', 'MX', 'SBER', 'GAZP', 'IMOEX']);

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

/**
 * Звать ПЕРЕД добавлением актива в избранное (не при снятии звёздочки).
 * `current` — текущий список избранного этого пикера.
 * true — добавлять можно; false — гость упёрся в лимит, окно регистрации
 * уже открыто, звёздочку не ставить.
 */
export function requestFavoriteAdd(current: readonly string[]): boolean {
  if (typeof window === 'undefined' || !isGuest() || !isTopSite()) return true;
  const own = current.filter((id) => !DEFAULT_FAVORITES.has(id)).length;
  if (own < GUEST_FAVORITES_LIMIT) return true;
  window.dispatchEvent(new CustomEvent(FAV_SIGNUP_EVENT));
  return false;
}
