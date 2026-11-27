/**
 * Предпросмотр покупки — GET /api/billing/checkout/preview (api/billing/SALE.md).
 *
 * Сервер считает той же функцией, что выставляет сумму в платеже: сколько
 * спишется сейчас, с какого дня и до какого действует подписка (сложение
 * сроков, подарочные дни акции, дни за остаток младшего тарифа), и когда и на
 * сколько будет следующее автосписание. Окно оплаты показывает это ДО платежа.
 */
import { apiFetch } from '../services/api';
import { dateLocale } from '../i18n';

export interface CheckoutPreview {
  plan_id: string;
  /** Текст причины, если купить нельзя. */
  blocked: string | null;
  /** Действующая платная подписка (нет — null). */
  current: {
    tier: string;
    title: string;
    expires_at: string | null;
    autorenew: boolean;
    /** Действующий тариф выше покупаемого — это переход на тариф ниже. */
    is_higher: boolean;
  } | null;
  amount: number;
  full_amount: number;
  promo_code: string | null;
  sale_title: string | null;
  starts_at: string;
  expires_at: string;
  bonus_days: number;
  bonus_from: string | null;
  gift_days: number;
  renew_amount: number;
  renew_at: string;
}

export async function fetchCheckoutPreview(
  planId: string,
  switchDown = false,
): Promise<CheckoutPreview | null> {
  try {
    const q = new URLSearchParams({ plan_id: planId });
    if (switchDown) q.set('switch_down', 'true');
    const r = await apiFetch(`/api/billing/checkout/preview?${q}`);
    return r.ok ? ((await r.json()) as CheckoutPreview) : null;
  } catch {
    return null;
  }
}

/** «17.12.2026» — даты срока и списания в окне оплаты. */
export function fmtDay(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime())
    ? '—'
    : d.toLocaleDateString(dateLocale(), { day: '2-digit', month: '2-digit', year: 'numeric' });
}

/** «4 декабря» — срок действия цены акции. */
export function fmtDayLong(ms: number): string {
  return new Date(ms).toLocaleDateString(dateLocale(), { day: 'numeric', month: 'long' });
}

/** Подписка начнётся не сегодня: срок прибавляется к уже оплаченному. */
export function startsLater(p: CheckoutPreview | null): boolean {
  return !!p && Date.parse(p.starts_at) - Date.now() > 60 * 60 * 1000;
}

export const fmtRub = (n: number) => n.toLocaleString('ru-RU', { maximumFractionDigits: 0 });
