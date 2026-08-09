/**
 * TierFeaturesContext — клиентский кэш матрицы tier × indicator × feature_flags.
 *
 * Источник истины — `api/billing/features.py` на бэкенде, отдаётся через
 * `GET /api/billing/features`. Кэшируется в localStorage (TTL 1 час) + в памяти.
 *
 * Использование:
 *
 *   const { canAccessAsset, requiredTierFor } = useTierAccess('open_interest');
 *   if (!canAccessAsset('CC')) {
 *     // показать замочек, призыв к апгрейду
 *     const upgradeTier = requiredTierFor({ asset: 'CC' });  // 'basic' | 'pro' | null
 *   }
 */
import {
    createContext, useContext, useEffect, useMemo, useState,
    type ReactNode,
} from 'react';
import { useAuth } from './AuthContext';


// ───────────────────────────────────────────────────────────────
// Типы matrix (зеркало api/billing/features.py)
// ───────────────────────────────────────────────────────────────
export type Tier = 'guest' | 'free' | 'basic' | 'pro' | 'admin';

export interface IndicatorLimits {
    assets_whitelist?: string[] | null;
    tickers_whitelist?: string[] | null;
    allowed_intervals?: number[] | null;
    allowed_timeframes?: string[] | null;
    allowed_modes?: string[] | null;
    modes?: string[] | null;
    universes?: string[] | null;
    clgroups?: string[] | null;
    max_history_days?: number | null;
    // max_history_years удалён 2026-07-30 вместе с мёртвым гейтом сезонности —
    // бэкенд его никогда не проверял (см. features.py, блок "seasonality").
    data_delay_hours?: number;
    // fund_trades: задержка меряется не часами, а снапшотами свежести (0 = свежий).
    snapshot_delay?: number;
    // Per-indicator flags:
    mode_imoex?: boolean;
    mode_all?: boolean;
    usd_mode?: boolean;
    custom_ranges?: boolean;
    filter_no_outliers?: boolean;
    filter_no_dividends?: boolean;
    category_filters_enabled?: boolean;
    settings_customizable?: boolean;
    // funds_money / fund_trades: выбор ПОДМНОЖЕСТВА фондов (пикер) — с Basic.
    fund_picker?: boolean;
    open?: boolean;
}

export interface CommonFeatures {
    watermark_on_export: boolean;
    csv_export: boolean;
    api_access: boolean;
    fund_trades_access: boolean;
    telegram_alerts_quota: number | null;
}

export interface FeaturesMatrix {
    indicators: Record<string, Record<string, IndicatorLimits>>;
    common: Record<string, CommonFeatures>;
    valid_tiers: string[];
}


// ───────────────────────────────────────────────────────────────
// Cache
// ───────────────────────────────────────────────────────────────
const CACHE_KEY = 'frame_features_matrix_v1';
const CACHE_TTL_MS = 60 * 60 * 1000;  // 1 час

function loadFromCache(): FeaturesMatrix | null {
    try {
        const raw = localStorage.getItem(CACHE_KEY);
        if (!raw) return null;
        const { ts, data } = JSON.parse(raw);
        if (Date.now() - ts > CACHE_TTL_MS) return null;
        return data;
    } catch {
        return null;
    }
}

function saveToCache(data: FeaturesMatrix): void {
    try {
        localStorage.setItem(CACHE_KEY, JSON.stringify({ ts: Date.now(), data }));
    } catch {
        // localStorage quota — игнорируем
    }
}


// ───────────────────────────────────────────────────────────────
// Context
// ───────────────────────────────────────────────────────────────
const TierFeaturesContext = createContext<FeaturesMatrix | null>(null);


export function TierFeaturesProvider({ children }: { children: ReactNode }) {
    const [matrix, setMatrix] = useState<FeaturesMatrix | null>(() => loadFromCache());

    useEffect(() => {
        let cancelled = false;

        // Резистентность к деплою и transient-сбоям. fetch резолвится и на 502/504
        // (nginx отдаёт HTML-страницу при пересоздании api-контейнера ~3с во время
        // деплоя). Прежний код звал r.json() без проверки r.ok → парсил «<html>…» →
        // «Unexpected token '<'» в консоли, и матрица оставалась null до перезагрузки.
        // Теперь: проверяем r.ok и ретраим с backoff (0.5/1/2с ≈ переживает окно
        // рестарта). При полном провале — оставляем кэш (loadFromCache в useState).
        async function load() {
            for (let attempt = 0; !cancelled && attempt < 4; attempt++) {
                try {
                    const r = await fetch('/api/billing/features');
                    if (!r.ok) throw new Error(`HTTP ${r.status}`);
                    const data: FeaturesMatrix = await r.json();
                    if (cancelled) return;
                    setMatrix(data);
                    saveToCache(data);
                    return;
                } catch (err) {
                    if (cancelled) return;
                    if (attempt === 3) {
                        console.warn('features matrix load failed:', err);
                        return;
                    }
                    await new Promise((res) => setTimeout(res, 500 * 2 ** attempt));
                }
            }
        }

        load();
        return () => { cancelled = true; };
    }, []);

    return (
        <TierFeaturesContext.Provider value={matrix}>
            {children}
        </TierFeaturesContext.Provider>
    );
}


// ───────────────────────────────────────────────────────────────
// Hooks
// ───────────────────────────────────────────────────────────────
const PERIOD_DAYS: Record<string, number> = {
    '1d': 1, '1w': 7, '1m': 30, '3m': 90, '6m': 180,
    '1y': 365, '2y': 730, '3y': 1095, '5y': 1825, '10y': 3650, '20y': 7300, 'all': 99999,
};

// Нормализация tier'а (admin/guest mapping)
function normalizeTier(role: string | undefined): Tier {
    if (!role) return 'guest';
    const r = role.toLowerCase();
    if (r === 'admin') return 'admin';
    if (r === 'user') return 'free';  // legacy
    if (r === 'guest') return 'guest';
    if (['free', 'basic', 'pro'].includes(r)) return r as Tier;
    return 'free';
}

function effectiveTierForLimits(tier: Tier): 'free' | 'basic' | 'pro' {
    if (tier === 'admin') return 'pro';
    if (tier === 'guest') return 'free';
    return tier as 'free' | 'basic' | 'pro';
}

export interface UseTierAccessResult {
    tier: Tier;
    limits: IndicatorLimits;
    isLoading: boolean;
    canAccessAsset: (asset: string) => boolean;
    canUseInterval: (interval: number) => boolean;
    canUseTimeframe: (tf: string) => boolean;
    canUseMode: (mode: string) => boolean;
    canUseUniverse: (u: string) => boolean;
    canUsePeriod: (period: string) => boolean;
    canUseFlag: (flagName: keyof IndicatorLimits) => boolean;
    requiredTierFor: (check: {
        asset?: string;
        interval?: number;
        timeframe?: string;
        mode?: string;
        universe?: string;
        period?: string;
        flag?: keyof IndicatorLimits;
    }) => 'basic' | 'pro' | null;
}

export function useTierAccess(indicator: string): UseTierAccessResult {
    const matrix = useContext(TierFeaturesContext);
    const { user, loading: authLoading } = useAuth();

    const tier = useMemo(() => normalizeTier(user?.role), [user?.role]);

    // Tier недостоверен, пока не разрешены ОБА источника: matrix (фичи) и auth (роль).
    // matrix===null — матрица ещё грузится. authLoading — AuthContext грузит /me.
    // (hasToken && !user) — критичный кейс: токен есть, но user не подгрузился,
    // например /me временно упал на старте (AuthContext.loadUser ловит сетевую
    // ошибку и всё равно снимает loading) → tier падал в 'guest' и Pro-юзер видел
    // замки до перезагрузки. Все потребители трактуют isLoading=true как «не
    // блокировать», поэтому достоверный tier раскроет реальные замки, а недогруз —
    // нет. Лучше на миг не запереть, чем запереть платника.
    const hasToken = typeof window !== 'undefined' && !!localStorage.getItem('access_token');
    const tierLoading = matrix === null || authLoading || (hasToken && !user);

    const limits = useMemo<IndicatorLimits>(() => {
        if (!matrix) return {};
        const effective = effectiveTierForLimits(tier);
        return matrix.indicators[indicator]?.[effective] || {};
    }, [matrix, indicator, tier]);

    function checkOnTier(t: 'free' | 'basic' | 'pro', check: Parameters<UseTierAccessResult['requiredTierFor']>[0]): boolean {
        if (!matrix) return false;
        const l = matrix.indicators[indicator]?.[t] || {};
        if (check.asset != null) {
            const wl = l.assets_whitelist ?? l.tickers_whitelist;
            if (wl != null && !wl.includes(check.asset)) return false;
        }
        if (check.interval != null) {
            const allowed = l.allowed_intervals;
            if (allowed != null && !allowed.includes(check.interval)) return false;
        }
        if (check.timeframe != null) {
            const allowed = l.allowed_timeframes;
            if (allowed != null && !allowed.includes(check.timeframe)) return false;
        }
        if (check.mode != null) {
            const allowed = l.allowed_modes ?? l.modes;
            if (allowed != null && !allowed.includes(check.mode)) return false;
        }
        if (check.universe != null) {
            const allowed = l.universes;
            if (allowed != null && !allowed.includes(check.universe)) return false;
        }
        if (check.period != null) {
            const max = l.max_history_days;
            if (max != null && (PERIOD_DAYS[check.period] ?? 0) > max) return false;
        }
        if (check.flag != null) {
            const v = (l as Record<string, unknown>)[check.flag];
            if (v === false) return false;
        }
        return true;
    }

    return {
        tier,
        limits,
        isLoading: tierLoading,
        canAccessAsset: (asset) => checkOnTier(effectiveTierForLimits(tier), { asset }),
        canUseInterval: (interval) => checkOnTier(effectiveTierForLimits(tier), { interval }),
        canUseTimeframe: (tf) => checkOnTier(effectiveTierForLimits(tier), { timeframe: tf }),
        canUseMode: (mode) => checkOnTier(effectiveTierForLimits(tier), { mode }),
        canUseUniverse: (u) => checkOnTier(effectiveTierForLimits(tier), { universe: u }),
        canUsePeriod: (period) => checkOnTier(effectiveTierForLimits(tier), { period }),
        canUseFlag: (flag) => checkOnTier(effectiveTierForLimits(tier), { flag }),

        requiredTierFor: (check) => {
            // Уже доступно на текущем tier? → не нужен апгрейд
            if (checkOnTier(effectiveTierForLimits(tier), check)) return null;
            // Иначе найти минимальный платный tier который даёт доступ
            for (const t of ['basic', 'pro'] as const) {
                if (checkOnTier(t, check)) return t;
            }
            return null;  // даже Pro не даёт — что-то странное
        },
    };
}


// ───────────────────────────────────────────────────────────────
// Common features hook
// ───────────────────────────────────────────────────────────────
export function useCommonFeatures(): CommonFeatures {
    const matrix = useContext(TierFeaturesContext);
    const { user } = useAuth();
    const tier = normalizeTier(user?.role);
    const effective = effectiveTierForLimits(tier);
    return matrix?.common[effective] ?? {
        watermark_on_export: true,
        csv_export: false,
        api_access: false,
        fund_trades_access: true,
        telegram_alerts_quota: 0,
    };
}


export function useCurrentTier(): Tier {
    const { user } = useAuth();
    return normalizeTier(user?.role);
}
