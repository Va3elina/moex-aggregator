/**
 * Отложенный invite-токен (`/billing/redeem?token=X`) для гостя.
 *
 * Гость по инвайт-ссылке не может применить токен сразу — сначала регистрация.
 * Токен кладём сюда, а PendingRedeemApplier применяет его в момент, когда юзер
 * становится авторизованным, на какой бы странице это ни случилось.
 *
 * Раньше токен жил в sessionStorage, а возврат делался через `?next=`. Оба
 * механизма рвались:
 *   - `next` терялся после регистрации (LoginPage вёл на /verify-email) и после
 *     подтверждения почты;
 *   - sessionStorage НЕ шарится между вкладками, а VK ID на iOS Safari открывает
 *     авторизацию в новой вкладке (та же грабля, что с vk_code_verifier).
 * Поэтому localStorage + применение по факту авторизации, а не по маршруту.
 */
const KEY = 'pending_redeem_token';

/** Токен из хранилища. Читаем и legacy sessionStorage — ссылки уже в ходу. */
export function getPendingRedeemToken(): string | null {
  try {
    return localStorage.getItem(KEY) || sessionStorage.getItem(KEY) || null;
  } catch {
    return null;
  }
}

export function setPendingRedeemToken(token: string): void {
  try {
    localStorage.setItem(KEY, token);
  } catch { /* хранилище недоступно — применим токен прямо из URL */ }
}

export function clearPendingRedeemToken(): void {
  try {
    localStorage.removeItem(KEY);
    sessionStorage.removeItem(KEY);
  } catch { /* игнор */ }
}
