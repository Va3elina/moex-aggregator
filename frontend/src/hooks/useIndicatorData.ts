/**
 * useIndicatorData — generic-каркас загрузки данных индикатора (DESKTOP).
 *
 * Выносит повторяющийся boilerplate индикатор-страниц: триада loading/error/data
 * + loadData(useCallback по deps) + автозапуск useEffect + опц. SSE-подписка
 * (useRealtimeData) + опц. tier-403 разбор (handleTierError). Это РОВНО та логика,
 * что дублировалась на 7 desktop-страницах.
 *
 * Что хук НЕ делает (остаётся в странице): chart-useMemo (chartData/oiData/align),
 * holiday-фильтр, usePersistedState-контролы, chartAnchorRef/useFitToViewport,
 * любые производные снимки. Хук отдаёт РОВНО ту data-ссылку, что вернул fetcher
 * (без обёртки/spread) — иначе SimpleChart лишний раз перезапустит анимацию.
 *
 * Bit-identical-контракт (критично для пилота OI): onSuccess вызывается СИНХРОННО
 * сразу после setData(result) в том же async-теле → setData и любые setState внутри
 * onSuccess (напр. setDataInterval) попадают в ОДИН React-18 batch. Так производный
 * снимок (dataInterval) остаётся атомарным с data — точно как было в инлайн-loadData.
 *
 * Mobile/embed НЕ используют этот хук (другая state-модель + by-design другой рендер).
 */
import { useCallback, useEffect, useState, type DependencyList } from 'react';
import { useRealtimeData } from './useRealtimeData';
import { handleTierError, type UpgradeTier } from '../utils/tierError';

interface TierConfig {
    showUpgrade: (props: { tier: UpgradeTier; featureName?: string; indicator?: string }) => void;
    indicator: string;
    featureName: string | ((msg: string) => string);
    tierResolver?: (msg: string) => UpgradeTier;
}

export interface UseIndicatorDataOptions<T> {
    /** Забинденный фетчер — замыкает все api-аргументы снаружи (хук не знает про api). */
    fetcher: () => Promise<T>;
    /** То, по чему пересоздаётся loadData (как deps useCallback инлайн-loadData). */
    deps: DependencyList;
    /** SSE-каналы для авто-reload. Не задан/пусто → без realtime (opt-in). */
    channels?: string[];
    /** Debounce для realtime (по умолчанию как у useRealtimeData = 2000мс). */
    debounceMs?: number;
    /** Если false — fetcher не вызывается (напр. OI: `!!selectedInstrument`). */
    enabled?: boolean;
    /** Текст non-tier ошибки (по умолчанию 'Ошибка загрузки данных'). */
    errorMessage?: string;
    /** СИНХРОННЫЙ колбэк сразу после setData (один batch). Для производных снимков. */
    onSuccess?: (data: T) => void;
    /** Если задан — tier-403 в catch идёт через handleTierError (caller-агностичный util). */
    tier?: TierConfig;
}

export interface UseIndicatorDataResult<T> {
    data: T | null;
    loading: boolean;
    error: string | null;
    reload: () => void;
}

export function useIndicatorData<T>(opts: UseIndicatorDataOptions<T>): UseIndicatorDataResult<T> {
    const {
        fetcher, deps, channels, debounceMs, enabled = true,
        errorMessage = 'Ошибка загрузки данных', onSuccess, tier,
    } = opts;

    const [data, setData] = useState<T | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    const reload = useCallback(async () => {
        if (enabled === false) return;
        try {
            setLoading(true);
            setError(null);
            const result = await fetcher();
            setData(result);
            // СИНХРОННО в том же async-теле → один React-18 batch с setData.
            onSuccess?.(result);
        } catch (err) {
            // tier задан и это tier-ошибка → upgrade-modal (onTier гасит error-state);
            // иначе destructive error + лог. Поведение идентично инлайн-catch.
            if (tier && handleTierError(err, { ...tier, onTier: () => setError(null) })) {
                /* tier-обработан */
            } else {
                setError(errorMessage);
                console.error(err);
            }
        } finally {
            setLoading(false);
        }
        // deps управляют пересозданием loadData — намеренно, как инлайн useCallback.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, deps);

    useEffect(() => { reload(); }, [reload]);

    // SSE opt-in: пустой channels → no-op (useRealtimeData фильтрует по source).
    useRealtimeData(channels ?? [], reload, debounceMs);

    return { data, loading, error, reload };
}
