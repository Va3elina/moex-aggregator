/**
 * Аннотации аномальных событий фондов.
 * Отображаются как маркеры на графике «Притоки-Оттоки».
 */

export interface FundAnnotation {
    date: string;           // "2026-01-27"
    category: string;       // "stocks" | "bonds" | "gold" | "money_market"
    ticker: string;         // "OPIF-282"
    ukId: string;           // ключ в UK_LOGOS
    type: 'merger' | 'liquidation' | 'reorganization';
    description: string;    // Описание для тултипа
}

export const FUND_ANNOTATIONS: FundAnnotation[] = [
    // === АКЦИИ ===
    {
        date: '2014-12-15',
        category: 'stocks',
        ticker: 'OPIF-432',
        ukId: '5',
        type: 'merger',
        description: 'Присоединение ОПИФ «Альфа-Капитал Акции» к «Ликвидные акции» (+0.9 млрд, NAV +589%)',
    },
    {
        date: '2019-09-25',
        category: 'stocks',
        ticker: 'OPIF-43',
        ukId: '34',
        type: 'merger',
        description: 'Присоединение фондов «Малой капитализации» и «Активного управления» Сбербанка к «Добрыня Никитич» (+1.6 млрд, NAV +33%)',
    },
    {
        date: '2025-06-06',
        category: 'stocks',
        ticker: 'OPIF-1003',
        ukId: '7',
        type: 'merger',
        description: 'Присоединение ОПИФ «ВИМ - Портфель Акций» к «ВИМ - Акции» (+3.3 млрд, NAV +37%)',
    },
    {
        date: '2026-04-17',
        category: 'stocks',
        ticker: 'OPIF-43',
        ukId: '34',
        type: 'merger',
        description: 'Присоединение ОПИФ «Первая - Новые возможности» к «Фонд российских акций» (+2.4 млрд, NAV +19%)',
    },

    // === ЗОЛОТО ===
    {
        date: '2024-09-03',
        category: 'gold',
        ticker: 'SBGD',
        ukId: '34',
        type: 'liquidation',
        description: 'Частичная ликвидация «Первая - Доступное золото» (−1.5 млрд, NAV −48%)',
    },

    // === ОБЛИГАЦИИ ===
    {
        date: '2020-10-22',
        category: 'bonds',
        ticker: 'OPIF-33',
        ukId: '5',
        type: 'merger',
        description: 'Слияние: активы присоединены к «Альфа-Капитал Облигации Плюс» (+7.2 млрд, NAV +25%)',
    },
    {
        date: '2022-09-15',
        category: 'bonds',
        ticker: 'OPIF-47',
        ukId: '34',
        type: 'merger',
        description: 'Присоединение фонда к «Первая - Рублёвые сбережения» (+4.6 млрд, NAV +46%)',
    },
];
