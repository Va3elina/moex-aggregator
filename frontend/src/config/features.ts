/**
 * Глобальные feature-флаги (kill-switch).
 *
 * CSV_EXPORT_ENABLED — экспорт данных в CSV / Excel (кнопка на страницах
 * индикаторов). ВКЛЮЧЁН 01.09.2026. Бэк: api/billing/features.py
 * CSV_EXPORT_ENABLED (default on) + монтирование exports_router в main.py.
 *
 * PUBLIC_API_ENABLED — публичный API: ключи в профиле, страница /api-docs,
 * /api/v1/public/*. СКРЫТО до официального запуска: пока сервис не запущен
 * официально, не предлагаем эту функцию (конкуренты могут использовать
 * против нас).
 *
 * Раньше это был один флаг API_CSV_ENABLED на обе функции — разделён
 * 01.09.2026, чтобы вернуть экспорт, не открывая API.
 *
 * Чтобы ВЕРНУТЬ API на запуске:
 *   1. здесь  → export const PUBLIC_API_ENABLED = true;
 *   2. на бэке → env PUBLIC_API_ENABLED=1 (api/billing/features.py + main.py)
 *   3. rebuild фронта + recreate api
 */
export const CSV_EXPORT_ENABLED = true;
export const PUBLIC_API_ENABLED = false;

/**
 * PAYMENT_METHODS_UI_ENABLED — секция «Способы оплаты» в ЛК (/profile):
 * список привязок + кнопка «Отвязать» (стирает рекуррент-токен на бэке).
 * Построена под компл-требование ЮKassa (автоплатежи), СКРЫТА по решению
 * Вадима 2026-07-03 до запуска ЮKassa-теста. Бэкенд (GET/DELETE
 * /api/billing/payment_methods) живёт независимо от флага.
 *
 * Включить (при старте теста ЮKassa или по просьбе проверяющих):
 *   export const PAYMENT_METHODS_UI_ENABLED = true; + rebuild фронта.
 */
export const PAYMENT_METHODS_UI_ENABLED = false;
