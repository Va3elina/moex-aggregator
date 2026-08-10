/**
 * useIndicatorData — generic-каркас загрузки данных индикатора (DESKTOP).
 *
 * Выносит повторяющийся boilerplate индикатор-страниц: триада loading/error/data
 * + loadData(useCallback по deps) + автозапуск useEffect + опц. SSE-подписка
 * (useRealtimeData) + опц. tier-403 разбор (handleTierError). Это РОВНО та логика,
 * что дублировалась на desktop-страницах индикаторов.
 *
 * Что хук НЕ делает (остаётся в странице): chart-useMemo (chartData/oiData/align),
 * holiday-фильтр, usePersistedState-контролы, chartAnchorRef/useFitToViewport,
 * любые производные снимки. Хук отдаёт РОВНО ту data-ссылку, что вернул fetcher
 * (без обёртки/spread) — иначе SimpleChart лишний раз перезапустит анимацию.
 *
 * Bit-identical-контракт: onSuccess вызывается СИНХРОННО сразу после setData(result)
 * в том же async-теле → setData и любые setState внутри onSuccess (напр.
 * setDataInterval) попадают в ОДИН React-18 batch. Так производный снимок остаётся
 * атомарным с data — точно как было в инлайн-loadData.
 *
 * NB про console.error: хук логирует только non-tier-ошибки (tier-403 ожидаем —
 * не шумим). На разных страницах инлайн-логика логировала по-разному (OI —
 * non-tier; Strength/Funds — всегда; CbrFlows — никогда) — это dev-only сайд-эффект,
 * НЕ видимый пользователю; user-visible state (data/loading/error/modal) идентичен.
 *
 * Mobile/embed НЕ используют этот хук (другая state-модель + by-design другой рендер).
 */
import { useCallback, useEffect, useRef, useState, type DependencyList } from 'react';
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
    /** Текст non-tier ошибки: строка ИЛИ функция от пойманной ошибки (напр. CbrFlows
     *  показывал `e?.message`). По умолчанию 'Ошибка загрузки данных'. */
    errorMessage?: string | ((err: unknown) => string);
    /** СИНХРОННЫЙ колбэк сразу после setData (один batch). Для производных снимков. */
    onSuccess?: (data: T) => void;
    /** Если задан — tier-403 в catch идёт через handleTierError (caller-агностичный util).
     */
    tier?: TierConfig;
    /** Дешёвый компаратор срезов, применяется ТОЛЬКО к realtime-рефетчу (SSE).
     *  true → пришло ровно то же, что уже в state: не вызываем setData, значит нет
     *  ре-рендера, сброса навигатора и перезапуска анимации графика. Нужен потому,
     *  что у тира с задержкой данных (Free: потолок = вчера) ответ на SSE-рефетч
     *  ИДЕНТИЧЕН предыдущему — график каждые 5 минут анимированно перерисовывал сам
     *  себя. Обычную загрузку (смена периода/инструмента) не трогает: там срез
     *  обязан доехать до state, даже если совпал по сигнатуре. */
    isSameData?: (prev: T, next: T) => boolean;
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
        errorMessage, onSuccess, tier, isSameData,
    } = opts;

    const [data, setData] = useState<T | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const reqIdRef = useRef(0);
    // Зеркало data для сравнения в reload (state там уже устарел бы на замыкание).
    const dataRef = useRef<T | null>(null);
    const isSameRef = useRef(isSameData);
    isSameRef.current = isSameData;

    const resolveError = (err: unknown): string =>
        typeof errorMessage === 'function' ? errorMessage(err)
            : (errorMessage ?? 'Ошибка загрузки данных');

    const reload = useCallback(async (opts?: { realtime?: boolean }) => {
        if (enabled === false) return;
        const realtime = opts?.realtime === true;
        const myId = ++reqIdRef.current;
        // Reqid-guard БЕЗУСЛОВНЫЙ: устаревший (не последний) ответ не пишет state.
        // Дёшево и корректно для всех индикаторов — без него быстрый перебор
        // фильтра/типа давал гонку (медленный ранний ответ перезаписывал свежий).
        const isStale = () => myId !== reqIdRef.current;
        try {
            // Realtime-рефетч — ТИХИЙ: спиннер «Обновление…» поверх графика на
            // каждое SSE-событие мигал каждые 5 минут, хотя данные на экране всё
            // это время валидны (ровно как silentRef в embed-панелях).
            if (!realtime) setLoading(true);
            setError(null);
            const result = await fetcher();
            if (isStale()) return;
            // Тот же срез по дешёвой сигнатуре → state не трогаем вовсе.
            if (realtime && dataRef.current && isSameRef.current?.(dataRef.current, result)) return;
            dataRef.current = result;
            setData(result);
            // СИНХРОННО в том же async-теле → один React-18 batch с setData.
            onSuccess?.(result);
        } catch (err) {
            if (isStale()) return;
            // tier задан и это tier-ошибка → upgrade-modal (onTier гасит error-state);
            // иначе destructive error + лог.
            if (tier && handleTierError(err, { ...tier, onTier: () => setError(null) })) {
                /* tier-обработан */
            } else {
                setError(resolveError(err));
                console.error(err);
            }
        } finally {
            // Устаревший ответ не трогает loading (новый reload уже выставил true).
            if (!isStale()) setLoading(false);
        }
        // deps управляют пересозданием loadData — намеренно, как инлайн useCallback.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, deps);

    useEffect(() => { reload(); }, [reload]);

    // SSE opt-in: пустой channels → no-op (useRealtimeData фильтрует по source).
    // realtime:true → тихий рефетч + сравнение среза (см. isSameData).
    const reloadRealtime = useCallback(() => { void reload({ realtime: true }); }, [reload]);
    useRealtimeData(channels ?? [], reloadRealtime, debounceMs);

    return { data, loading, error, reload };
}
