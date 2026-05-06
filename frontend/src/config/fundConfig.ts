// Конфигурация фондов — общие константы для FundsMoneyPage и FundsCatalogPage

export const UK_LOGOS: Record<string, { letter: string; bg: string; color: string; name: string }> = {
    '3597': { letter: 'Т', bg: '#FFDD2D', color: '#000000', name: 'Т-Капитал' },
    '5':    { letter: 'А', bg: '#EF3124', color: '#FFFFFF', name: 'Альфа-Капитал' },
    '34':   { letter: 'П', bg: '#21A038', color: '#FFFFFF', name: 'Первая' },
    '7':    { letter: 'В', bg: '#009FDF', color: '#FFFFFF', name: 'ВИМ' },
    '20':   { letter: 'Р', bg: '#FEE600', color: '#000000', name: 'Райффайзен' },
    'aton': { letter: 'A', bg: '#1A3C6E', color: '#FFFFFF', name: 'АТОН' },
};

export const CATEGORY_LABELS: Record<string, string> = {
    'money_market': 'Денежный рынок',
    'stocks': 'Акции',
    'bonds': 'Облигации',
    'gold': 'Золото',
};

// Editorial-friendly палитра — deep desaturated tones, не неон.
// Подходит к paper bg на editorial-light + dark. Mirror FUND_PALETTE из
// chartTheme.ts (там тот же набор для chart-series). Раньше тут был неоновый
// набор #6366f1/#2EE59D/etc — anti-pattern editorial design system.
export const DONUT_COLORS = [
    '#3B4F7A', // deep indigo
    '#7A4332', // brick brown
    '#3F6B47', // forest dark
    '#6B4F8A', // muted purple
    '#8A5C3F', // warm clay
    '#2D5F6E', // deep teal
    '#7A3F5C', // rosewood
    '#5C5F2D', // olive
    '#3F5C6B', // slate blue
    '#6B5C2D', // dark gold
    '#7A5C3F', // bronze
    '#4F6B5C', // sage
];
