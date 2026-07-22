// Ограничения для неавторизованных пользователей

// Гости: только дневной таймфрейм
export const GUEST_ALLOWED_INTERVALS = new Set([24]);

// Гости: максимум 1 год данных
export const GUEST_MAX_PERIOD = '1y';

// Порядок периодов для сравнения
const PERIOD_ORDER: Record<string, number> = {
  '1d': 1, '1w': 7, '1m': 30, '3m': 90, '6m': 180,
  '1y': 365, '2y': 730, '3y': 1095, '5y': 1825, '10y': 3650, '20y': 7300, 'all': 99999,
};

export function isIntervalAllowed(interval: number, isAuthenticated: boolean): boolean {
  if (isAuthenticated) return true;
  return GUEST_ALLOWED_INTERVALS.has(interval);
}

export function isPeriodAllowed(period: string, isAuthenticated: boolean): boolean {
  if (isAuthenticated) return true;
  return (PERIOD_ORDER[period] ?? 0) <= PERIOD_ORDER[GUEST_MAX_PERIOD];
}

/** Возвращает дефолтный период с учётом авторизации */
export function getDefaultPeriod(preferredPeriod: string, isAuthenticated: boolean): string {
  if (isAuthenticated) return preferredPeriod;
  return isPeriodAllowed(preferredPeriod, false) ? preferredPeriod : GUEST_MAX_PERIOD;
}
