/**
 * Возврат пользователя на нужную страницу после входа (`?next=...`).
 *
 * Email-вход и «уже залогинен» читают next прямо из URL (мы ещё на /login).
 * OAuth уходит через провайдера (round-trip теряет URL) → next кладётся в
 * localStorage перед редиректом и читается в AuthCallback.
 *
 * Защита от open-redirect: пускаем ТОЛЬКО внутренние пути (один ведущий '/',
 * не '//' и не '/\') — иначе можно было бы увести юзера на чужой домен.
 */
const KEY = 'post_login_next';

export function safeInternalPath(raw: string | null | undefined): string | null {
  if (!raw || raw[0] !== '/') return null;
  if (raw[1] === '/' || raw[1] === '\\') return null;  // //evil.com, /\evil.com
  return raw;
}

/** Сохранить next перед OAuth-редиректом (переживает round-trip провайдера). */
export function setPostLoginNext(raw: string | null | undefined): void {
  const safe = safeInternalPath(raw);
  try {
    if (safe) localStorage.setItem(KEY, safe);
    else localStorage.removeItem(KEY);
  } catch { /* localStorage недоступен — игнор */ }
}

/** Прочитать и УДАЛИТЬ сохранённый next (одноразовый). null если нет/невалиден. */
export function popPostLoginNext(): string | null {
  try {
    const v = localStorage.getItem(KEY);
    localStorage.removeItem(KEY);
    return safeInternalPath(v);
  } catch {
    return null;
  }
}
