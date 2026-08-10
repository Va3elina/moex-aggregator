/**
 * useChartWindowLoader — оконная подача свечей (модель TradingView).
 *
 * Вместо «весь ряд одним куском» грузим только последнее окно, а прошлое
 * догружаем частями, когда пользователь долистывает к началу. На интрадее
 * разница принципиальная: 5м за полгода — это 26.7 тыс. свечей и 6.2 МБ, а
 * окно в месяц — 4.4 тыс. и ~1 МБ.
 *
 * Почему это вообще работает быстрее, а не просто «позже»: стоимость ответа у
 * нас линейна по числу баров на КАЖДОМ слое (SQL → Python → JSON → gzip →
 * JSON.parse в браузере). Окно сокращает все слои разом, а догрузка случается
 * лишь у тех, кто реально листает вглубь.
 *
 * Границы кусков считаем по календарю, а не по числу баров: у бэкенда ручка
 * принимает date_from/date_to, и запрос с ОБОИМИ концами кешируется отдельным
 * ключом — история неизменна, поэтому такой кусок можно держать в кеше долго.
 */
import { useCallback, useRef, useState } from 'react';
import type { ChartResponse } from '../../types';
import { getChartData } from '../../services/api';

/** Ширина одного куска истории в днях — по таймфрейму. Подобрано так, чтобы
 *  кусок был сопоставим по весу (~1-1.5 МБ) на любом ТФ. */
const CHUNK_DAYS: Record<number, number> = { 5: 30, 60: 180, 24: 1825 };
/** Докуда вообще пускаем прокрутку: дальше данных либо нет, либо они не нужны
 *  (5м на проде живут с 2026-02). Страховка от бесконечной догрузки. */
const MAX_CHUNKS = 8;

const iso = (d: Date) => d.toISOString().slice(0, 10);
const daysBefore = (dateStr: string, days: number) => {
    const d = new Date(dateStr + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() - days);
    return iso(d);
};

interface Params {
    instrument: string;
    interval: number;
    clgroup: string;
    /** Текущий срез — к нему пририсовываем прошлое. */
    data: ChartResponse | null;
    /** Отдать расширенный срез наружу. */
    onExtended: (next: ChartResponse) => void;
}

export function useChartWindowLoader({ instrument, interval, clgroup, data, onExtended }: Params) {
    const [loadingMore, setLoadingMore] = useState(false);
    // Всё через ref: колбэк уезжает в подписку графика, которая живёт один раз.
    const ref = useRef({ instrument, interval, clgroup, data, onExtended });
    ref.current = { instrument, interval, clgroup, data, onExtended };
    const busy = useRef(false);
    const chunks = useRef(0);
    // Докуда уже догрузили: повторный вызов на том же крае не должен слать
    // запрос заново (график дёргает подписку на каждый кадр прокрутки).
    const loadedFrom = useRef<string | null>(null);
    const exhausted = useRef(false);

    /** Сброс при смене инструмента/ТФ/среза — окно считается заново. */
    const reset = useCallback(() => {
        busy.current = false;
        chunks.current = 0;
        loadedFrom.current = null;
        exhausted.current = false;
        setLoadingMore(false);
    }, []);

    const loadMore = useCallback(() => {
        const cur = ref.current;
        if (busy.current || exhausted.current || !cur.data?.candles?.length) return;
        if (chunks.current >= MAX_CHUNKS) return;

        const firstTime = cur.data.candles[0].time.slice(0, 10);
        if (loadedFrom.current === firstTime) return;   // этот край уже просили
        loadedFrom.current = firstTime;

        const span = CHUNK_DAYS[cur.interval] ?? CHUNK_DAYS[24];
        const from = daysBefore(firstTime, span);
        // Верхняя граница — день ПЕРЕД первым имеющимся баром: куски не
        // пересекаются, склейка не даёт дублей.
        const to = daysBefore(firstTime, 1);

        busy.current = true;
        chunks.current += 1;
        setLoadingMore(true);

        getChartData(cur.instrument, cur.instrument, 'futures', cur.interval, cur.clgroup,
                     true, '6m', from, to)
            .then((older) => {
                const now = ref.current;
                // Пока летел запрос, могли сменить актив/ТФ — тогда результат чужой.
                if (now.instrument !== cur.instrument || now.interval !== cur.interval
                    || now.clgroup !== cur.clgroup || !now.data) return;

                const olderCandles = older.candles ?? [];
                if (!olderCandles.length) { exhausted.current = true; return; }

                // Страховка от нахлёста: берём только то, что строго раньше
                // текущего начала (бэкенд режет по датам, а бары внутри дня).
                const edge = now.data.candles[0].time;
                const candles = olderCandles.filter((c) => c.time < edge);
                const oi = (older.open_interest ?? []).filter((o) => o.time < edge);
                if (!candles.length) { exhausted.current = true; return; }

                now.onExtended({
                    ...now.data,
                    candles: [...candles, ...now.data.candles],
                    open_interest: [...oi, ...now.data.open_interest],
                    candles_count: candles.length + now.data.candles.length,
                    oi_count: oi.length + now.data.open_interest.length,
                    candles_start_date: candles[0].time.slice(0, 10),
                    data_start: candles[0].time.slice(0, 10),
                });
            })
            .catch(() => { exhausted.current = true; })   // нет данных/тариф — просто не тянем дальше
            .finally(() => { busy.current = false; setLoadingMore(false); });
    }, []);

    return { loadMore, loadingMore, reset };
}
