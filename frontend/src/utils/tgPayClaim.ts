/**
 * Привязка чата бота оплаты @FrameDataPayBot к аккаунту (docs/tg-pay-bot.md).
 *
 * Бот ведёт на /pricing?src=tg_bot&tgc=<token>. Токен запоминаем, а когда
 * человек вошёл на сайт обычным способом (VK, Яндекс, почта), отправляем на
 * POST /api/tg-pay/claim. Это не вход через Telegram (406-ФЗ): бот только
 * узнаёт, кому писать об оплате.
 *
 * localStorage, а не только адрес: между кликом в боте и входом бывают
 * регистрация, OAuth в другой вкладке и подтверждение почты.
 */
import { apiFetch } from '../services/api';

const KEY = 'frame_tgpay_claim';
const TTL_MS = 24 * 3600 * 1000;
const TOKEN_RE = /^[A-Za-z0-9_-]{16,64}$/;

/** Токен из адреса. true — был и запомнен. */
export function captureTgPayToken(params: URLSearchParams): boolean {
  const token = params.get('tgc');
  if (!token || !TOKEN_RE.test(token)) return false;
  try {
    localStorage.setItem(KEY, JSON.stringify({ token, ts: Date.now() }));
  } catch { /* приватный режим: привязка просто не случится */ }
  return true;
}

/** Отправить сохранённый токен. Вызывать только для залогиненного. */
export async function claimTgPayToken(): Promise<void> {
  let token: string | null = null;
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return;
    const v = JSON.parse(raw);
    if (TOKEN_RE.test(v?.token) && Date.now() - Number(v.ts) < TTL_MS) token = v.token;
    else localStorage.removeItem(KEY);
  } catch {
    return;
  }
  if (!token) return;
  try {
    const r = await apiFetch('/api/tg-pay/claim', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }),
    });
    if (r.ok) localStorage.removeItem(KEY);
  } catch { /* сеть: попробуем при следующем заходе на тарифы */ }
}
