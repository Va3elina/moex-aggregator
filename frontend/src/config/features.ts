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
