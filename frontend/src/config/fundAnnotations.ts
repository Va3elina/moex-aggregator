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
        date: '2021-05-06',
        category: 'stocks',
        ticker: 'OPIF-281',
        ukId: '20',
        type: 'merger',
        description: 'Слияние: активы фонда Райффайзен присоединены к «Райффайзен - Акции» (+4.6 млрд, NAV +90%)',
    },
    {
        date: '2024-08-06',
        category: 'stocks',
        ticker: 'OPIF-282',
        ukId: '20',
        type: 'merger',
        description: 'Слияние: активы фонда Райффайзен присоединены к «Компании роста» (+2.9 млрд, NAV +71%)',
    },
    {
        date: '2025-06-06',
        category: 'stocks',
        ticker: 'OPIF-1003',
        ukId: '7',
        type: 'merger',
        description: 'Присоединение фонда ВИМ к «ВИМ - Акции» (+3.3 млрд, NAV +37%)',
    },
    {
        date: '2026-01-27',
        category: 'stocks',
        ticker: 'OPIF-282',
        ukId: '20',
        type: 'merger',
        description: 'Слияние: активы фонда Райффайзен присоединены к «Компании роста» (+7.5 млрд, NAV +164%)',
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
