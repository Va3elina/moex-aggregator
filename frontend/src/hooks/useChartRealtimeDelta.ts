/**
 * useChartRealtimeDelta — SSE-обновление графика ОИ инкрементальной дельтой.
 *
 * Для поверхностей со СВОЕЙ state-моделью (embed-панели песочницы, мобильная
 * страница). Десктопный /oi ходит через useIndicatorData.realtimeMerge — там
 * тот же мердж, но встроенный в общий каркас загрузки.
 *
 * Без дельты каждая открытая панель на каждое SSE-событие перекачивала полный
 * ряд: 5м/6м — это 26 тыс. свечей и 6.2 МБ раз в пять минут, и все панели
 * стартовали одновременно, конкурируя за 4 ядра сервера.
 *
 * Если дельта не применима (пустой ряд, клиент сильно отстал, сеть) — зовём
 * onFallback: пусть поверхность сделает обычный тихий рефетч, как раньше.
 */
import { useRef } from 'react';
import type { ChartResponse } from '../types';
import { useRealtimeData } from './useRealtimeData';
import { fetchAndMergeChartDelta, matchesControls } from '../utils/chartDelta';

interface Options {
    /** Текущий срез. null (ещё не загрузились) → дельта пропускается. */
    data: ChartResponse | null;
    /** Применить обновлённый срез. Зовётся ТОЛЬКО когда что-то изменилось. */
    onMerged: (next: ChartResponse) => void;
    /** Полный тихий рефетч — когда дельта не применима. */
    onFallback: () => void;
    /** Текущие контролы: защита от гонки со сменой инструмента/ТФ/среза. */
    controls: { sectype: string; interval: number; clgroup: string };
    /** SSE-каналы (по умолчанию как у графика ОИ). */
    channels?: string[];
}

export function useChartRealtimeDelta({
    data, onMerged, onFallback, controls, channels = ['5min', 'hourly'],
}: Options) {
    // Всё через ref: колбэки и данные пересоздаются каждый рендер, а подписка
    // на SSE живёт один раз (useRealtimeData монтирует эффект с пустыми deps).
    const ref = useRef({ data, onMerged, onFallback, controls });
    ref.current = { data, onMerged, onFallback, controls };

    // Защита от наложения: SSE-события могут прийти пачкой, а запрос дельты
    // асинхронный — второй заход поверх незавершённого склеил бы точки дважды.
    const inFlight = useRef(false);

    useRealtimeData(channels, () => {
        const { data: cur, onFallback: fallback, controls: ctl } = ref.current;
        if (!cur) { fallback(); return; }
        if (!matchesControls(cur, ctl)) return;   // рефетч новых контролов уже в полёте
        if (inFlight.current) return;

        inFlight.current = true;
        fetchAndMergeChartDelta(cur)
            .then((next) => {
                // Контролы могли смениться, пока дельта летела — тогда результат
                // относится к прошлому срезу и применять его нельзя.
                if (next && matchesControls(ref.current.data ?? next, ref.current.controls)) {
                    ref.current.onMerged(next);
                }
            })
            .catch(() => { fallback(); })
            .finally(() => { inFlight.current = false; });
    });
}
