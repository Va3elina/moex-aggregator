/**
 * csvPeriod — helper для конвертации PeriodValue (CsvExportModal) в query string.
 *
 * Преобразует:
 *   { type: 'preset', value: '1y' } → "days=365" (через preset map)
 *   { type: 'range', from: '2024-01-01', to: '2024-12-31' } → "period_from=...&period_to=..."
 */
import type { PeriodValue } from '../components/export/CsvExportModal';

const DEFAULT_DAYS_MAP: Record<string, number> = {
    '1d': 2,
    '1w': 7,
    '1m': 30,
    '3m': 90,
    '6m': 180,
    '1y': 365,
    '2y': 730,
    '3y': 1095,
    '5y': 1825,
    '10y': 3650,
    '20y': 7300,
    'all': 7000,
};

/**
 * Конвертирует PeriodValue в query-string фрагмент для backend endpoint'а.
 * Параметры:
 *   value: PeriodValue из CsvExportModal state
 *   fallbackDays: значение по default если value undefined / preset не найден
 */
export function periodToQuery(value: unknown, fallbackDays: number = 365): string {
    if (!value || typeof value !== 'object') {
        return `days=${fallbackDays}`;
    }
    const v = value as PeriodValue;
    if (v.type === 'range' && v.from && v.to) {
        return `period_from=${encodeURIComponent(v.from)}&period_to=${encodeURIComponent(v.to)}`;
    }
    if (v.type === 'preset' && v.value) {
        const days = DEFAULT_DAYS_MAP[v.value] ?? fallbackDays;
        return `days=${days}`;
    }
    return `days=${fallbackDays}`;
}
