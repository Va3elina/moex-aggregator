/**
 * Инкрементальный догруз графика ОИ: общая логика для всех трёх поверхностей
 * (сайт /oi, embed-панели песочницы, мобильная страница).
 *
 * Зачем: по SSE-событию раньше перекачивался ПОЛНЫЙ ряд — на 5м/6м это 26 тыс.
 * свечей и 6.2 МБ каждые пять минут на каждую открытую панель. Дельта отдаёт
 * только точки после последней известной (килобайты).
 *
 * Контракт с бэкендом (GET /api/chart/{id}/delta):
 *   - клиент присылает время последних ЗАКРЫТЫХ точек (live срезает сам);
 *   - назад приходят строго более новые закрытые точки + свежая live-точка;
 *   - full_reload=true → клиент отстал слишком сильно, нужен полный рефетч.
 */
import type { ChartResponse } from '../types';
import { getChartDelta } from '../services/api';

/** Время последней НАСТОЯЩЕЙ точки ряда (её и шлём как since).
 *  Пропускаем и live, и stretched: растянутая цена — это заполнение отрезка,
 *  где свеча по лицензии MOEX ещё не раздаётся. Если принять её за since,
 *  настоящие свечи за этот отрезок уже не догрузятся никогда. */
function lastClosedTime<P extends { time: string; live?: boolean; stretched?: boolean }>(arr: P[]): string | null {
    for (let i = arr.length - 1; i >= 0; i--) if (!arr[i].live && !arr[i].stretched) return arr[i].time;
    return null;
}

export class ChartDeltaUnavailable extends Error {}

/**
 * Запросить дельту и вклеить её в текущий срез.
 *
 * @returns новый ChartResponse — или null, если ничего не изменилось (тогда
 *          state трогать НЕ надо: не будет ни ре-рендера, ни перезапуска
 *          анимации, ни сброса навигатора).
 * @throws  ChartDeltaUnavailable — дельта не применима (пустой ряд, клиент
 *          сильно отстал, сеть). Вызывающий делает обычный полный рефетч.
 */
export async function fetchAndMergeChartDelta(prev: ChartResponse): Promise<ChartResponse | null> {
    const sinceCandle = lastClosedTime(prev.candles);
    if (!sinceCandle) throw new ChartDeltaUnavailable('пустой ряд');
    const sinceOi = prev.open_interest.length ? lastClosedTime(prev.open_interest) : null;

    const showOi = prev.mode !== 'price_only';
    const d = await getChartDelta(
        prev.sec_id, prev.sectype, 'futures',
        prev.interval, prev.clgroup, showOi, sinceCandle, sinceOi,
    );
    if (d.full_reload) throw new ChartDeltaUnavailable('клиент отстал');

    // Новых ЗАКРЫТЫХ точек нет, и live не изменилась → срез тот же.
    const prevLive = prev.candles.at(-1)?.live ? prev.candles.at(-1) : null;
    const newLive = d.candles.at(-1)?.live ? d.candles.at(-1) : null;
    const onlyLive = d.candles.every((c) => c.live) && d.open_interest.every((o) => o.live);
    if (onlyLive && (!newLive || (prevLive?.time === newLive.time && prevLive?.close === newLive.close))) {
        return null;
    }

    const candles = [...prev.candles.filter((c) => !c.live && !c.stretched), ...d.candles];
    const open_interest = [...prev.open_interest.filter((o) => !o.live), ...d.open_interest];
    const endDate = candles.at(-1)?.time.slice(0, 10) ?? prev.candles_end_date;

    return {
        ...prev, candles, open_interest,
        candles_count: candles.length,
        oi_count: open_interest.length,
        candles_end_date: endDate,
        data_end: endDate,
    };
}

/**
 * Совпадает ли срез с текущими контролами. SSE может стрельнуть, пока полный
 * рефетч нового инструмента ещё в полёте — тогда prev относится к СТАРЫМ
 * контролам, и мердж склеил бы два разных ряда.
 */
export function matchesControls(
    data: ChartResponse,
    controls: { sectype: string; interval: number; clgroup: string },
): boolean {
    return data.sectype === controls.sectype
        && data.interval === controls.interval
        && data.clgroup === controls.clgroup;
}
