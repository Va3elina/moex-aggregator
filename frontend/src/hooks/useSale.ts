/**
 * useSale — идёт ли сейчас акция и по каким ценам (бэкенд: api/billing/sale.py,
 * контракт — api/billing/SALE.md).
 *
 * Единственный источник правды — поле `sale` в /api/billing/plans. Всё, что
 * относится к акции на сайте (баннер на главной, кнопка в шапке, всплывающее
 * окно, карточки тарифов, окно оплаты), рисуется ТОЛЬКО пока этот хук отдаёт
 * акцию. Сервер перестаёт её отдавать в момент окончания, а таймер ниже гасит
 * её и на уже открытой странице — поэтому после акции сайт возвращается к
 * обычному виду сам, без выкладки и без правки настроек.
 *
 * Запрос один на сессию (кэш на модуль) и отдельный для гостя и для вошедшего:
 * сервер решает по аккаунту, доступна ли ему ещё цена акции (`eligible`).
 */
import { useEffect, useState } from 'react';
import { apiFetch } from '../services/api';
import { useAuth } from '../contexts/AuthContext';

export interface SaleTier {
  tier: 'basic' | 'pro';
  title: string;
  planId: string;
  /** Обычная цена года — она же цена продления. */
  amount: number;
  /** Цена года по акции. */
  saleAmount: number;
  /** Процент на плашке. */
  pct: number;
  /** Зачёркнутая база: 12 × месячная цена. */
  listAmount: number;
  /** Обычная цена месяца (для «было / стало» в пересчёте на месяц). */
  monthlyAmount: number;
  /** Подарочные дни сверх года; 0 — подарка нет. */
  giftDays: number;
}

export interface Sale {
  code: string;
  title: string;
  pctMax: number;
  /** Момент окончания, мс. */
  endsAt: number;
  monthlyAvailable: boolean;
  /** false — этот аккаунт по акции уже покупал: ему обычные цены. */
  eligible: boolean;
  /** Тест-аккаунт оплаты (BILLING_TEST_USER_IDS). */
  tester: boolean;
  tiers: SaleTier[];
}

interface RawPlan {
  plan_id: string;
  amount: number;
  sale_amount?: number;
  sale_pct?: number;
  sale_list_amount?: number;
  sale_gift_days?: number;
}
interface RawTier { tier: string; title: string; monthly: RawPlan | null; yearly: RawPlan | null }
export interface RawPlans {
  tiers?: RawTier[];
  /** Тест-аккаунт оплаты: видит тарифы как покупатель, даже с ролью admin. */
  tester?: boolean;
  sale?: {
    code: string; title: string; pct_max: number; ends_at: string;
    monthly_available: boolean; eligible: boolean;
  } | null;
}

/** Разбор ответа /api/billing/plans. null — акции нет или в ответе нет цен акции. */
export function parseSale(data: RawPlans | null | undefined): Sale | null {
  const raw = data?.sale;
  if (!raw) return null;
  const endsAt = Date.parse(raw.ends_at);
  if (!Number.isFinite(endsAt) || endsAt <= Date.now()) return null;
  const tiers: SaleTier[] = [];
  for (const t of data?.tiers ?? []) {
    const y = t.yearly;
    if ((t.tier !== 'basic' && t.tier !== 'pro') || !y || !y.sale_amount) continue;
    // Из базы скидки, а не из monthly.amount: тест-аккаунту сервер подменяет
    // amount тест-ценой, а зачёркнутая цена месяца должна остаться настоящей.
    const monthly = y.sale_list_amount
      ? Math.round(y.sale_list_amount / 12)
      : t.monthly?.amount ?? Math.round(y.amount / 12);
    tiers.push({
      tier: t.tier,
      title: t.title,
      planId: y.plan_id,
      amount: y.amount,
      saleAmount: y.sale_amount,
      pct: y.sale_pct ?? 0,
      listAmount: y.sale_list_amount ?? monthly * 12,
      monthlyAmount: monthly,
      giftDays: y.sale_gift_days ?? 0,
    });
  }
  return {
    code: raw.code,
    title: raw.title,
    pctMax: raw.pct_max,
    endsAt,
    monthlyAvailable: raw.monthly_available,
    eligible: raw.eligible,
    tester: !!data?.tester,
    // Аккаунту, который лимит акции уже использовал, сервер цен акции не отдаёт.
    tiers,
  };
}

/** Акция, которую этому посетителю можно показывать: идёт, доступна, цены есть. */
export function isSaleLive(sale: Sale | null): sale is Sale {
  return !!sale && sale.eligible && sale.tiers.length > 0 && sale.endsAt > Date.now();
}

/** Это «Чёрная пятница» (метка bf…) — тогда в заголовках фирменное «Black Friday». */
export function isBlackFriday(sale: Sale): boolean {
  return sale.code.toLowerCase().startsWith('bf');
}

const cache = new Map<string, Promise<Sale | null>>();

function load(key: string): Promise<Sale | null> {
  let p = cache.get(key);
  if (!p) {
    p = apiFetch('/api/billing/plans')
      .then((r) => (r.ok ? r.json() : null))
      .then((d: RawPlans | null) => parseSale(d))
      .catch(() => null);
    cache.set(key, p);
  }
  return p;
}

/** Сбросить кэш: цена изменилась (ответ 409 на оплату) или прошла покупка. */
export function resetSaleCache(): void {
  cache.clear();
}

export function useSale(): Sale | null {
  const { isAuthenticated, loading, user } = useAuth();
  const [sale, setSale] = useState<Sale | null>(null);
  const key = loading ? null : isAuthenticated ? `u${user?.id ?? ''}` : 'guest';

  useEffect(() => {
    if (key === null) return;
    let alive = true;
    load(key).then((s) => { if (alive) setSale(s); });
    return () => { alive = false; };
  }, [key]);

  // Акция закончилась, пока страница открыта, — гасим её без перезагрузки.
  // setTimeout больше ~24,8 дня переполняется, поэтому длинные сроки не ждём.
  useEffect(() => {
    if (!sale) return;
    const left = sale.endsAt - Date.now();
    // Уже вышла — isSaleLive ниже её и так не отдаст.
    if (left <= 0 || left > 2 ** 31 - 1) return;
    const id = setTimeout(() => { resetSaleCache(); setSale(null); }, left);
    return () => clearTimeout(id);
  }, [sale]);

  // Админу тарифы не продаём — ему и акция не показывается (кроме тест-аккаунта).
  if (user?.role === 'admin' && !sale?.tester) return null;
  return isSaleLive(sale) ? sale : null;
}

/** Самый большой подарок среди тарифов акции — для общей строки в шапке. */
export function saleGiftLine(sale: Sale, t: (k: string, o?: Record<string, unknown>) => string): string {
  const days = Math.max(0, ...sale.tiers.map((x) => x.giftDays));
  if (days <= 0) return '';
  if (days >= 28 && days <= 31) return t('+ месяц в подарок');
  return giftLabel(days, t);
}

/** «+1 месяц в подарок» / «+N дней в подарок»; пусто, если подарка нет. */
export function giftLabel(days: number, t: (k: string, o?: Record<string, unknown>) => string): string {
  if (days <= 0) return '';
  if (days >= 28 && days <= 31) return t('+1 месяц в подарок');
  return t('+{{n}} дн. в подарок', { n: days });
}
