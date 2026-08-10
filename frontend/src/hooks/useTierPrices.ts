/**
 * useTierPrices — цены тарифов для апселл-виджетов.
 *
 * Апселлы показывают ГОДОВУЮ цену в пересчёте на месяц (yearly / 12) — так же,
 * как карточки на PricingPage (там годовой период — дефолт, цены сравнимы
 * «в лоб»). Источник — /api/billing/plans (публичный, доступен гостю): цены не
 * хардкодим (54-ФЗ/ЗоЗПП), но держим фолбэк на случай сетевой ошибки — виджет
 * замка не должен остаться без цены.
 *
 * Фетч один на сессию (module-level кэш) — апселлы открываются часто, дёргать
 * /plans на каждый показ незачем.
 */
import { useEffect, useState } from 'react';

export interface TierPrice {
    /** Годовая цена, пересчитанная на месяц: round(yearly / 12). */
    monthlyEquivalent: number;
    /** Полная сумма списания за год. */
    yearly: number;
}

export type TierPrices = Record<'basic' | 'pro', TierPrice>;

// Фолбэк = текущие цены (basic 2900, pro 5900 ₽/мес; годовая = 12 × 0.8,
// округление до 10 ₽ — зеркало api/billing/plans.py::_make_pair).
const FALLBACK: TierPrices = {
    basic: { monthlyEquivalent: 2320, yearly: 27840 },
    pro: { monthlyEquivalent: 4720, yearly: 56640 },
};

let cached: TierPrices | null = null;
let inflight: Promise<TierPrices> | null = null;

async function fetchPrices(): Promise<TierPrices> {
    try {
        const r = await fetch('/api/billing/plans');
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const data = await r.json();
        const find = (t: string) =>
            ((data.tiers || []).find((x: { tier: string }) => x.tier === t) || {}) as
                { yearly?: { amount: number } };
        const result: TierPrices = { ...FALLBACK };
        for (const tier of ['basic', 'pro'] as const) {
            const yearly = find(tier).yearly?.amount;
            if (yearly && yearly > 0) {
                result[tier] = { monthlyEquivalent: Math.round(yearly / 12), yearly };
            }
        }
        cached = result;
        return result;
    } catch {
        return FALLBACK;
    }
}

export function useTierPrices(): TierPrices {
    const [prices, setPrices] = useState<TierPrices>(cached ?? FALLBACK);

    useEffect(() => {
        if (cached) return;
        let cancelled = false;
        inflight = inflight ?? fetchPrices();
        inflight.then((p) => { if (!cancelled) setPrices(p); });
        return () => { cancelled = true; };
    }, []);

    return prices;
}

export function fmtRubMonthly(n: number): string {
    return `${n.toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽/мес`;
}
