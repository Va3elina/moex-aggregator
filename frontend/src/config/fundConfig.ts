// Конфигурация фондов — общие константы для FundsMoneyPage и FundsCatalogPage

// `img` (опц.) — путь к фирменному лого УК (рендерится круглым вместо буквы).
// letter/bg/color остаются как fallback, если картинка не загрузится.
export const UK_LOGOS: Record<string, { letter: string; bg: string; color: string; name: string; img?: string }> = {
    '3597': { letter: 'Т', bg: '#FFDD2D', color: '#000000', name: 'Т-Капитал' },
    '5':    { letter: 'А', bg: '#EF3124', color: '#FFFFFF', name: 'Альфа-Капитал' },
    '34':   { letter: 'П', bg: '#21A038', color: '#FFFFFF', name: 'Первая' },
    '7':    { letter: 'В', bg: '#009FDF', color: '#FFFFFF', name: 'ВИМ' },
    '20':   { letter: 'Р', bg: '#FEE600', color: '#000000', name: 'Райффайзен' },
    'aton': { letter: 'A', bg: '#1A3C6E', color: '#FFFFFF', name: 'АТОН' },
    'record': { letter: 'А', bg: '#294C96', color: '#FFFFFF', name: 'Рекорд Капитал', img: '/uk-logos/alenka.svg' },
    'geroi':  { letter: 'Г', bg: '#6B3FA0', color: '#FFFFFF', name: 'ГЕРОИ', img: '/uk-logos/geroi.png' },
};

export const CATEGORY_LABELS: Record<string, string> = {
    'money_market': 'Денежный рынок',
    'stocks': 'Акции',
    'bonds': 'Облигации',
    'gold': 'Золото',
    'yuan': 'Юань',
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

// Фирменные цвета бумаг для пончика состава + списка позиций — узнаваемость
// (Сбер зелёный, Т-Техно жёлтый, Газпром синий и т.д.). Матч по подстроке имени
// (lowercase, имена приходят короткие: «Сбербанк», «Т-Техно ао», «ЯНДЕКС»…).
// Порядок важен — специфичное раньше общего. Нет в списке → fallback DONUT_COLORS.
const ASSET_COLOR_RULES: { kw: string[]; color: string }[] = [
    { kw: ['сбер'], color: '#1FA037' },                       // Сбербанк — зелёный
    { kw: ['т-техно', 'т-технолог', 'тинькоф', 'ткс'], color: '#FFDD2D' }, // Т-Технологии — жёлтый
    { kw: ['газпром нефт'], color: '#1A4FA0' },               // Газпром нефть
    { kw: ['газпром'], color: '#0079C2' },                    // Газпром — синий
    { kw: ['лукойл'], color: '#E2231A' },                     // Лукойл — красный
    { kw: ['роснефт'], color: '#F5C518' },                    // Роснефть — жёлтый
    { kw: ['яндекс'], color: '#FC3F1D' },                     // Яндекс — красный
    { kw: ['новатэк', 'новатек'], color: '#12305E' },         // Новатэк — тёмно-синий
    { kw: ['норникель', 'норильск', 'гмк'], color: '#11A6C9' }, // Норникель — бирюзовый
    { kw: ['полюс'], color: '#C7A452' },                      // Полюс — золото
    { kw: ['полиметалл'], color: '#8C9BA5' },                 // Полиметалл — серебро
    { kw: ['втб'], color: '#009FDF' },                        // ВТБ — синий
    { kw: ['магнит'], color: '#E2001A' },                     // Магнит — красный
    { kw: ['мтс'], color: '#E30611' },                        // МТС — красный
    { kw: ['татнефт'], color: '#D81E27' },                    // Татнефть — красный
    { kw: ['северсталь'], color: '#1B3A6B' },                 // Северсталь — синий
    { kw: ['нлмк'], color: '#0093D0' },                       // НЛМК — голубой
    { kw: ['ммк', 'магнитогорск'], color: '#E1251B' },        // ММК — красный
    { kw: ['фосагро'], color: '#5FA63B' },                    // ФосАгро — зелёный
    { kw: ['икс 5', 'x5', 'пятёроч', 'пятероч'], color: '#009A44' }, // X5 — зелёный
    { kw: ['озон', 'ozon'], color: '#005BFF' },               // Озон — синий
    { kw: ['совкомфлот'], color: '#0B4DA2' },                 // Совкомфлот — синий
    { kw: ['аэрофлот'], color: '#00529B' },                   // Аэрофлот — синий
    { kw: ['алроса'], color: '#00A3DA' },                     // Алроса — голубой
    { kw: ['русгидро'], color: '#00A4E3' },                   // РусГидро — голубой
    { kw: ['интер рао', 'интеррао'], color: '#ED1C24' },      // Интер РАО — красный
    { kw: ['сургут'], color: '#E8B400' },                     // Сургутнефтегаз — золото
    { kw: ['афк', 'систем'], color: '#C8102E' },              // АФК Система — красный
    { kw: ['пик'], color: '#FF5000' },                        // ПИК — оранжевый
    { kw: ['самолёт', 'самолет'], color: '#1E4BE5' },         // Самолёт — синий
    { kw: ['хэдхантер', 'хедхантер', 'headhunter'], color: '#D6001C' }, // HeadHunter — красный
    { kw: ['транснефт'], color: '#E30613' },                  // Транснефть — красный
    { kw: ['мосбирж', 'московская бирж', 'moex'], color: '#11A0E0' }, // Мосбиржа
    { kw: ['ростелеком'], color: '#7A2FF2' },                 // Ростелеком — фиолетовый
];

// Цвет бумаги: фирменный, если узнан по имени; иначе undefined → вызывающий берёт
// DONUT_COLORS по индексу. Прочее/неизвестное остаётся в editorial-палитре.
export function assetColor(name: string | null | undefined): string | undefined {
    if (!name) return undefined;
    const n = name.toLowerCase();
    for (const r of ASSET_COLOR_RULES) {
        if (r.kw.some((k) => n.includes(k))) return r.color;
    }
    return undefined;
}
