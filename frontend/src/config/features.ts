/**
 * Глобальные feature-флаги (kill-switch).
 *
 * API_CSV_ENABLED — публичный API (ключи, /api-docs, /api/v1/public/*) и
 * экспорт данных в CSV. СКРЫТО до официального запуска: пока сервис не
 * запущен официально, не предлагаем эти функции (конкуренты могут
 * использовать против нас).
 *
 * Чтобы ВЕРНУТЬ на запуске:
 *   1. здесь  → export const API_CSV_ENABLED = true;
 *   2. на бэке → env PUBLIC_API_CSV_ENABLED=1 (api/billing/features.py + main.py)
 *   3. rebuild фронта + recreate api
 */
export const API_CSV_ENABLED = false;

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
