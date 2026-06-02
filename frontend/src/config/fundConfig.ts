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
// Цвета — ПРИГЛУШЁННЫЕ editorial-тона (та же насыщенность, что DONUT_COLORS),
// узнаваемые по hue, но не неон. Ключи включают СОКРАЩЁННЫЕ имена из данных
// («Транснф», «Татнфт», «Ростел», «ГМКНорНик») — иначе не матчатся.
const ASSET_COLOR_RULES: { kw: string[]; color: string }[] = [
    { kw: ['сбер'], color: '#3F7A50' },                       // Сбербанк — зелёный
    { kw: ['т-техно', 'т-технолог', 'тинькоф', 'ткс'], color: '#B89A3E' }, // Т-Технологии — золотой
    { kw: ['газпром нефт'], color: '#2C4E7E' },               // Газпром нефть
    { kw: ['газпром'], color: '#355F92' },                    // Газпром — синий
    { kw: ['лукойл'], color: '#A83A33' },                     // Лукойл — красный
    { kw: ['роснефт'], color: '#9A7E3E' },                    // Роснефть — бронза-золото
    { kw: ['яндекс'], color: '#A8473B' },                     // Яндекс — кирпичный
    { kw: ['новатэк', 'новатек'], color: '#34507A' },         // Новатэк — тёмно-синий
    { kw: ['норник', 'норильск', 'гмк'], color: '#3E8290' },  // Норникель/ГМКНорНик — бирюзовый
    { kw: ['полюс'], color: '#B8A04E' },                      // Полюс — золото
    { kw: ['полиметалл', 'югк', 'южуралзолото'], color: '#8A99A3' }, // драгметаллы — серебро
    { kw: ['втб'], color: '#3E86A8' },                        // ВТБ — голубой
    { kw: ['магнит'], color: '#BE3F3F' },                     // Магнит — красный
    { kw: ['мтс'], color: '#BE4B3E' },                        // МТС — красный
    { kw: ['татнфт', 'татнефт'], color: '#A6483C' },          // Татнефть — красно-коричн.
    { kw: ['северсталь'], color: '#3E5A82' },                 // Северсталь — синий
    { kw: ['нлмк'], color: '#3E7FA8' },                       // НЛМК — голубой
    { kw: ['ммк', 'магнитогорск'], color: '#BE4438' },        // ММК — красный
    { kw: ['фосагро'], color: '#5A8A4A' },                    // ФосАгро — зелёный
    { kw: ['икс 5', 'x5', 'пятёроч', 'пятероч'], color: '#4E8A4E' }, // X5 — зелёный
    { kw: ['озон', 'ozon'], color: '#3E63A8' },               // Озон — синий
    { kw: ['совкомфлот'], color: '#2F5288' },                 // Совкомфлот — синий
    { kw: ['аэрофлот'], color: '#3E5E96' },                   // Аэрофлот — синий
    { kw: ['алроса'], color: '#3E8CB0' },                     // Алроса — голубой
    { kw: ['русгидро'], color: '#3E8FA0' },                   // РусГидро — teal
    { kw: ['интер рао', 'интеррао'], color: '#B23E3A' },      // Интер РАО — красный
    { kw: ['сургут'], color: '#A8943E' },                     // Сургутнефтегаз — золото
    { kw: ['афк', 'систем'], color: '#A83E48' },              // АФК Система — красный
    { kw: ['пик'], color: '#B0653A' },                        // ПИК — оранж-глина
    { kw: ['самол'], color: '#4A5FA0' },                      // Самолёт — синий
    { kw: ['хэдхантер', 'хедхантер', 'хедхант', 'headhunter', 'ххру'], color: '#9A4A5C' }, // HeadHunter — роза
    { kw: ['транснф', 'транснефт'], color: '#8C5A3E' },       // Транснефть — бронза
    { kw: ['мосбирж', 'московская бирж', 'moex'], color: '#3E7C9E' }, // Мосбиржа — голубой
    { kw: ['ростел'], color: '#6B4F8A' },                     // Ростелеком — фиолетовый
    { kw: ['русал'], color: '#9A5246' },                      // Русал — медь
    { kw: ['русагро'], color: '#6B8A4A' },                    // Русагро — оливковый
    { kw: ['астра'], color: '#3E72A8' },                      // Астра — синий
    { kw: ['позитив', 'positive', 'group-ib'], color: '#5A7A3E' }, // Positive — зелёный
    { kw: ['эн+', 'en+', 'эн ', 'rusal'], color: '#7A5C3F' }, // Эн+ — бронза
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
