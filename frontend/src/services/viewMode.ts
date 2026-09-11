/**
 * Вид сайта для админа: «как видит пользователь».
 *
 * С 2026-09-11 цена акций и фьючерсов на сайте — только на закрытие торгов в
 * 19:00 (бэкенд: api/services/session_close). Админам бэкенд отдаёт прежнюю,
 * незамедленную версию. Флаг ниже переключает админа в вид пользователя:
 * apiFetch шлёт заголовок X-Frame-View: user, и бэкенд отвечает как всем.
 *
 * Ключ без префикса `frame:` намеренно: такие ключи синкаются между
 * устройствами (services/settingsSync), а вид — настройка конкретного
 * браузера, админ может смотреть сайт по-разному на ноутбуке и на телефоне.
 */
const KEY = 'admin:viewAsUser';

export function isViewAsUser(): boolean {
  try {
    return localStorage.getItem(KEY) === '1';
  } catch {
    return false;
  }
}

export function setViewAsUser(on: boolean): void {
  try {
    if (on) localStorage.setItem(KEY, '1');
    else localStorage.removeItem(KEY);
  } catch {
    /* private mode — вид останется админским */
  }
}

/** Заголовок вида для apiFetch: пусто, если админ смотрит свою версию. */
export function viewHeaders(): Record<string, string> {
  return isViewAsUser() ? { 'X-Frame-View': 'user' } : {};
}
