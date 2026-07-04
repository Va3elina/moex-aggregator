/**
 * Маппинг "технического" sec_id (используется в БД и API) → читабельный тикер
 * для UI. Применяется в индикаторах, заголовках чартов и т.д.
 *
 * Валютные индексы MOEX имеют громоздкие sec_id вида `USD000UTSTOM` —
 * показываем пользователю FX-стандарт `USD/RUB`.
 */
const DISPLAY_TICKER_MAP: Record<string, string> = {
    USD000UTSTOM: 'USD/RUB',
    EUR_RUB__TOM: 'EUR/RUB',
    CNYRUB_TOM: 'CNY/RUB',
    GLDRUB_TOM: 'GLD/RUB',
    NATGAS_HH: 'NATGAS',
    TTF_GAS: 'TTF',
};

export function displayTicker(secId: string | null | undefined): string {
    if (!secId) return '';
    return DISPLAY_TICKER_MAP[secId] ?? secId;
}
