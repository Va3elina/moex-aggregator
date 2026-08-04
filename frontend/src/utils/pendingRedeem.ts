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

/**
 * Результат уже применённого токена.
 *
 * Нужен из-за гонки: авторизация случается ещё на /login, applier успевает
 * применить токен и стереть его, а BillingRedeemPage открывается следом уже
 * с пустым хранилищем и показывает «Ссылка не содержит токен» — хотя подписка
 * выдана. Applier кладёт сюда исход, страница его забирает и показывает успех.
 */
const RESULT_KEY = 'pending_redeem_result';

export type RedeemResult =
  | { ok: true; tier: string; expires_at: string | null }
  | { ok: false; message: string };

export function setRedeemResult(result: RedeemResult): void {
  try {
    localStorage.setItem(RESULT_KEY, JSON.stringify(result));
  } catch { /* игнор */ }
}

/** Забрать и удалить — исход показывается один раз. */
export function popRedeemResult(): RedeemResult | null {
  try {
    const raw = localStorage.getItem(RESULT_KEY);
    localStorage.removeItem(RESULT_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed.ok !== 'boolean') return null;
    if (parsed.ok) {
      if (typeof parsed.tier !== 'string') return null;
      return { ok: true, tier: parsed.tier, expires_at: parsed.expires_at ?? null };
    }
    return { ok: false, message: String(parsed.message || 'Не удалось применить ссылку') };
  } catch {
    return null;
  }
}
