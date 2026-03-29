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

export const DONUT_COLORS = [
    '#6366f1', '#2EE59D', '#4DA3FF', '#FF4D4D', '#FFB020',
    '#00D9FF', '#9D4DFF', '#FF6B9D', '#FCD34D', '#14B8A6',
    '#F97316', '#818CF8',
];
