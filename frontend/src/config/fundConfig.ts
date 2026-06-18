// Конфигурация фондов — общие константы для FundsMoneyPage и FundsCatalogPage

// `img` (опц.) — путь к фирменному лого УК (рендерится круглым вместо буквы).
// letter/bg/color остаются как fallback, если картинка не загрузится.
export const UK_LOGOS: Record<string, { letter: string; bg: string; color: string; name: string; img?: string }> = {
    '3597': { letter: 'Т', bg: '#FFDD2D', color: '#000000', name: 'Т-Капитал',     img: '/uk-logos/tcap.png' },
    '5':    { letter: 'А', bg: '#EF3124', color: '#FFFFFF', name: 'Альфа-Капитал', img: '/uk-logos/alfa.png' },
    '34':   { letter: 'П', bg: '#21A038', color: '#FFFFFF', name: 'Первая',        img: '/uk-logos/pervaya.png' },
    '7':    { letter: 'В', bg: '#009FDF', color: '#FFFFFF', name: 'ВИМ',           img: '/uk-logos/vim.png' },
    'aton': { letter: 'A', bg: '#1A3C6E', color: '#FFFFFF', name: 'АТОН',          img: '/uk-logos/aton.png' },
    'record': { letter: 'А', bg: '#294C96', color: '#FFFFFF', name: 'Рекорд Капитал', img: '/uk-logos/alenka.svg' },
    'geroi':  { letter: 'Г', bg: '#6B3FA0', color: '#FFFFFF', name: 'ГЕРОИ', img: '/uk-logos/geroi.png' },
};

// Авторские фонды — ФОТО основателя вместо УК-лого (ключ = тикер/ISIN фонда).
// Аватары взяты с finuslugi.ru (карточки фондов), лежат в public/authors/
// (грузятся лениво по URL, постбилд чистит только dist/logos — их не трогает).
export const AUTHOR_LOGOS: Record<string, { name: string; img: string }> = {
    'RU000A10BZ69': { name: 'Евгений Коган',        img: '/authors/kogan.png' },       // Биткоган
    'RU000A10EBY8': { name: 'Илья Воробьёв',        img: '/authors/vorobiev.png' },    // Долгосрочные инвестиции
    'RU000A10B8Z2': { name: 'Назар Щетинин',        img: '/authors/shchetinin.png' },  // Блэк лайн
    'RU000A10B917': { name: 'Константин Кудрицкий', img: '/authors/vasilich.png' },    // Матрёшка а-ля Рус
    'RU000A10B909': { name: 'Иван Крейнин',         img: '/authors/kreinin.png' },     // Великолепная семёрка
    'RU000A10D1E0': { name: 'Игорь Шимко',          img: '/authors/shimko.png' },      // Консервативная стратегия
    'RU000A10D5D3': { name: 'Алексей Линецкий',     img: '/authors/linecki.png' },     // Сбалансированные Возможности
};

export type FundLogo = { letter: string; bg: string; color: string; name: string; img?: string };

/**
 * Лого фонда: для авторских фондов — фото основателя (в форме UK_LOGOS-записи,
 * drop-in замена `UK_LOGOS[uk_id]`), иначе обычное лого УК. ukKey = uk_id или ключ.
 * Места рендера, где у фонда есть тикер, должны звать это вместо UK_LOGOS[uk_id].
 */
export function resolveFundLogo(
    ticker?: string | null,
    ukKey?: string | number | null,
): FundLogo | undefined {
    if (ticker && AUTHOR_LOGOS[ticker]) {
        const a = AUTHOR_LOGOS[ticker];
        return { letter: a.name.charAt(0), bg: '#1A1D28', color: '#FFFFFF', name: a.name, img: a.img };
    }
    return ukKey != null ? UK_LOGOS[String(ukKey)] : undefined;
}

// Легаси/вариативные написания имени УК, встречающиеся в начале названия фонда
// (УК переименовывались, в данных встречаются старые формы). Ключ — uk_id, как в
// UK_LOGOS; каноничное имя из UK_LOGOS[uk_id].name добавляется автоматически.
const UK_NAME_ALIASES: Record<string, string[]> = {
    '34':   ['Сбер Управление Активами', 'Сбер'],   // «Первая» — бывш. Сбер
    '7':    ['ВТБ Капитал', 'ВТБ'],                 // «ВИМ» — бывш. ВТБ Капитал
    '3597': ['Тинькофф Капитал', 'Тинькофф'],       // «Т-Капитал» — бывш. Тинькофф
};

/**
 * Срезает ведущее имя УК (и следующий за ним разделитель) из названия фонда —
 * в списках иконка УК уже показывает компанию, и префикс «Первая — …», «АТОН …»
 * лишь дублируется при прокрутке. No-op, если префикса нет (ETF-имена вроде
 * «Ликвидность», авторские фонды, чужие УК). Полное имя стоит оставлять в title
 * и в заголовках детальных карточек — там фонд один и дублирования нет.
 *
 * Матч регистронезависимый («Атон - Петр Столыпин» vs «АТОН …»), имя срезается
 * целиком (важно для «Т-Капитал»/«Альфа-Капитал» с дефисом внутри), длиннейший
 * алиас проверяется первым. Если после среза пусто — возвращаем оригинал.
 */
export function stripUkName(name: string, ukKey?: string | number | null): string {
    if (!name || ukKey == null) return name;
    const key = String(ukKey);
    const aliases = [UK_LOGOS[key]?.name, ...(UK_NAME_ALIASES[key] ?? [])]
        .filter(Boolean)
        .sort((a, b) => (b as string).length - (a as string).length) as string[];
    const lower = name.toLowerCase();
    for (const a of aliases) {
        if (lower.startsWith(a.toLowerCase())) {
            const stripped = name.slice(a.length).replace(/^[\s\-–—·:.]+/, '').trim();
            if (stripped) return stripped;
        }
    }
    return name;
}

export const CATEGORY_LABELS: Record<string, string> = {
    'money_market': 'Денежный рынок',
    'stocks': 'Акции',
    'bonds': 'Облигации',
    'gold': 'Золото',
    'yuan': 'Юань',
};

// Подсказки к группам облигационных фондов (разбивка по дюрации). Показываются
// в кружке со знаком вопроса рядом с заголовком группы в каталоге (FundsTable).
// Текст для розничного инвестора, без жаргона. Ключ — значение funds.subcategory
// (оно же отображаемое имя группы). Нет ключа → иконка не рисуется.
export const SUBCATEGORY_HELP: Record<string, string> = {
    'Короткие': 'Минимальная чувствительность к ставке: флоатеры и короткие выпуски. Ведут себя около денежного рынка, почти как вклад, без сильных колебаний цены. Доходность идёт следом за ключевой ставкой.',
    'Средние': 'Корпоративные и средние гособлигации, дюрация примерно от 2 до 4 лет. Баланс купонного дохода и риска, умеренная реакция на изменение ставки.',
    'Длинные': 'Длинные ОФЗ дальнего конца кривой, дюрация примерно от 5 до 7 лет. Высокий потенциал при снижении ставки и самые сильные колебания цены. По сути ставка на снижение ключевой ставки.',
    'Авторские': 'Фонды под управлением авторов стратегий, а не классических управляющих компаний.',
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
// `ticker` (MOEX) — для рендера спрайт-лого через <TickerLogo>. assetTicker(name)
// возвращает его по той же keyword-логике; нет тикера → вызывающий рисует точку.
const ASSET_COLOR_RULES: { kw: string[]; color: string; ticker: string }[] = [
    { kw: ['сбер'], color: '#3F7A50', ticker: 'SBER' },        // Сбербанк — зелёный
    { kw: ['т-техно', 'т-технолог', 'тинькоф', 'ткс'], color: '#B89A3E', ticker: 'T' }, // Т-Технологии — золотой
    { kw: ['газпром нефт'], color: '#2C4E7E', ticker: 'SIBN' }, // Газпром нефть
    { kw: ['газпром'], color: '#355F92', ticker: 'GAZP' },     // Газпром — синий
    { kw: ['лукойл'], color: '#A83A33', ticker: 'LKOH' },      // Лукойл — красный
    { kw: ['роснефт'], color: '#9A7E3E', ticker: 'ROSN' },     // Роснефть — бронза-золото
    { kw: ['яндекс'], color: '#A8473B', ticker: 'YDEX' },      // Яндекс — кирпичный
    { kw: ['новатэк', 'новатек'], color: '#34507A', ticker: 'NVTK' }, // Новатэк — тёмно-синий
    { kw: ['норник', 'норильск', 'гмк'], color: '#3E8290', ticker: 'GMKN' }, // Норникель/ГМКНорНик — бирюзовый
    { kw: ['полюс'], color: '#B8A04E', ticker: 'PLZL' },       // Полюс — золото
    { kw: ['полиметалл', 'югк', 'южуралзолото'], color: '#8A99A3', ticker: 'POLY' }, // драгметаллы — серебро
    { kw: ['втб'], color: '#3E86A8', ticker: 'VTBR' },         // ВТБ — голубой
    { kw: ['магнит'], color: '#BE3F3F', ticker: 'MGNT' },      // Магнит — красный
    { kw: ['мтс'], color: '#BE4B3E', ticker: 'MTSS' },         // МТС — красный
    { kw: ['татнфт', 'татнефт'], color: '#A6483C', ticker: 'TATN' }, // Татнефть — красно-коричн.
    { kw: ['северсталь'], color: '#3E5A82', ticker: 'CHMF' },  // Северсталь — синий
    { kw: ['нлмк'], color: '#3E7FA8', ticker: 'NLMK' },        // НЛМК — голубой
    { kw: ['ммк', 'магнитогорск'], color: '#BE4438', ticker: 'MAGN' }, // ММК — красный
    { kw: ['фосагро'], color: '#5A8A4A', ticker: 'PHOR' },     // ФосАгро — зелёный
    { kw: ['икс 5', 'x5', 'пятёроч', 'пятероч'], color: '#4E8A4E', ticker: 'X5' }, // X5 — зелёный
    { kw: ['озон', 'ozon'], color: '#3E63A8', ticker: 'OZON' }, // Озон — синий
    { kw: ['совкомфлот'], color: '#2F5288', ticker: 'FLOT' },  // Совкомфлот — синий
    { kw: ['аэрофлот'], color: '#3E5E96', ticker: 'AFLT' },    // Аэрофлот — синий
    { kw: ['алроса'], color: '#3E8CB0', ticker: 'ALRS' },      // Алроса — голубой
    { kw: ['русгидро'], color: '#3E8FA0', ticker: 'HYDR' },    // РусГидро — teal
    { kw: ['интер рао', 'интеррао'], color: '#B23E3A', ticker: 'IRAO' }, // Интер РАО — красный
    { kw: ['сургут'], color: '#A8943E', ticker: 'SNGS' },      // Сургутнефтегаз — золото
    { kw: ['афк', 'систем'], color: '#A83E48', ticker: 'AFKS' }, // АФК Система — красный
    { kw: ['пик'], color: '#B0653A', ticker: 'PIKK' },         // ПИК — оранж-глина
    { kw: ['самол'], color: '#4A5FA0', ticker: 'SMLT' },       // Самолёт — синий
    { kw: ['хэдхантер', 'хедхантер', 'хедхант', 'headhunter', 'ххру'], color: '#9A4A5C', ticker: 'HEAD' }, // HeadHunter — роза
    { kw: ['транснф', 'транснефт'], color: '#8C5A3E', ticker: 'TRNFP' }, // Транснефть — бронза
    { kw: ['мосбирж', 'московская бирж', 'moex'], color: '#3E7C9E', ticker: 'MOEX' }, // Мосбиржа — голубой
    { kw: ['ростел'], color: '#6B4F8A', ticker: 'RTKM' },      // Ростелеком — фиолетовый
    { kw: ['русал'], color: '#9A5246', ticker: 'RUAL' },       // Русал — медь
    { kw: ['русагро'], color: '#6B8A4A', ticker: 'RAGR' },     // Русагро — оливковый
    { kw: ['астра'], color: '#3E72A8', ticker: 'ASTR' },       // Астра — синий
    { kw: ['позитив', 'positive', 'group-ib'], color: '#5A7A3E', ticker: 'POSI' }, // Positive — зелёный
    { kw: ['эн+', 'en+', 'эн ', 'rusal'], color: '#7A5C3F', ticker: 'ENPG' }, // Эн+ — бронза
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

// MOEX-тикер бумаги для спрайт-лого (<TickerLogo>): та же keyword-логика, что
// assetColor. Не узнано → undefined (вызывающий рисует только цветную точку).
export function assetTicker(name: string | null | undefined): string | undefined {
    if (!name) return undefined;
    const n = name.toLowerCase();
    for (const r of ASSET_COLOR_RULES) {
        if (r.kw.some((k) => n.includes(k))) return r.ticker;
    }
    return undefined;
}
