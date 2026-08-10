// Ограничения для неавторизованных пользователей.
//
// 2026-08-10 (решение владельца): гость = free. Отдельных гостевых
// ограничений больше НЕТ — всё, что открыто зарегистрированному на бесплатном
// тарифе, открыто и гостю. Гейтит только тарифная матрица
// (api/billing/features.py через useTierAccess), где guest мапится на free.
//
// Функции и константы оставлены (их зовут ~7 страниц): всегда разрешают.
// Вернуть гостевой гейт = вернуть прежние наборы и сравнения здесь, без
// перекройки страниц.

export const GUEST_ALLOWED_INTERVALS = new Set([5, 60, 24]);

export const GUEST_MAX_PERIOD = 'all';

export function isIntervalAllowed(_interval: number, _isAuthenticated: boolean): boolean {
  return true;
}

export function isPeriodAllowed(_period: string, _isAuthenticated: boolean): boolean {
  return true;
}

/** Возвращает дефолтный период с учётом авторизации (сейчас — как есть). */
export function getDefaultPeriod(preferredPeriod: string, _isAuthenticated: boolean): string {
  return preferredPeriod;
}
