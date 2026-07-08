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
    'Длинные': 'Длинные ОФЗ дальнего конца кривой, дюрация примерно от 5 до 7 лет. Выигрывают при снижении ключевой ставки: цена растёт сильнее всех. Проигрывают при росте ставки или доходностей дальнего конца, просадка максимальная. Самые сильные колебания цены.',
    'Средние': 'Корпоративные и средние гособлигации, дюрация примерно от 2 до 4 лет. Баланс купона и риска. Выигрывают при плавном снижении ставки, при росте ставки просадка умеренная.',
    'Короткие': 'Флоатеры и короткие выпуски, чувствительность к ставке близка к нулю. Ведут себя около денежного рынка, почти как вклад. Выигрывают когда ставка высокая или растёт. При быстром снижении ставки доходность падает вслед за ней.',
    'Авторские': 'Фонды публичных личностей и блогеров (авторов стратегий), а не классических управляющих компаний.',
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
    // Добавлено из стикерпака (новые логотипы DATA/VSEH/GLRX.png): фонды держат
    // эти бумаги, имена в составе приходят без ISIN — резолвим по ключевому слову.
    { kw: ['аренадата'], color: '#2F8A6E', ticker: 'DATA' },              // Аренадата — зелёный
    { kw: ['ви.ру', 'все инструмент'], color: '#BE3F3F', ticker: 'VSEH' }, // ВИ.ру — красный
    { kw: ['глоракс'], color: '#5546A8', ticker: 'GLRX' },                // Глоракс — фиолетовый
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


// ─── Унификация с Сезонностью: резолв бумаги фонда → каноничный тикер/имя/цвет ───

// ─── АВТОГЕНЕРАЦИЯ (generate_fundconfig.py): унификация имён/лого с Сезонностью ───
// Источник: реальный скоуп пикера /assets × MOEX ISS (ISIN→тикер) × instruments
// (каталог Сезонности) × OpenFIGI (ГДР/АДР). НЕ редактировать руками — regenerate.
// 2026-07-08: имена приведены к единому стандарту ОИ (миграция БД
// 020_unify_asset_names.sql) — regenerate после миграции даст те же значения.
export const ISIN_TO_TICKER: Record<string, string> = {
    'RU0006944147': 'TATNP',
    'RU0007252813': 'ALRS',
    'RU0007288411': 'GMKN',
    'RU0007661625': 'GAZP',
    'RU0007775219': 'MTSS',
    'RU0007976965': 'BANEP',
    'RU0008926258': 'SNGS',
    'RU0008943394': 'RTKM',
    'RU0008958863': 'MSNG',
    'RU0008992318': 'FESH',
    'RU0009024277': 'LKOH',
    'RU0009029524': 'SNGSP',
    'RU0009029540': 'SBER',
    'RU0009029557': 'SBERP',
    'RU0009033591': 'TATN',
    'RU0009046452': 'NLMK',
    'RU0009046510': 'CHMF',
    'RU0009046700': 'RTKMP',
    'RU0009062285': 'AFLT',
    'RU0009062467': 'SIBN',
    'RU0009084396': 'MAGN',
    'RU0009084438': 'EVRZ',
    'RU0009084446': 'NMTP',
    'RU0009091573': 'TRNFP',
    'RU0009092134': 'LSNGP',
    'RU0009100945': 'BSPB',
    'RU000A0B6NK6': 'TRMK',
    'RU000A0B90N8': 'RASP',
    'RU000A0DKVS5': 'NVTK',
    'RU000A0DKXV5': 'MTLR',
    'RU000A0DQZE3': 'AFKS',
    'RU000A0ET7Y7': 'MSRS',
    'RU000A0HL5M1': 'BELU',
    'RU000A0J2Q06': 'ROSN',
    'RU000A0JKQU8': 'MGNT',
    'RU000A0JNAA8': 'PLZL',
    'RU000A0JNAB6': 'ABIO',
    'RU000A0JNGA5': 'UPRO',
    'RU000A0JP5V6': 'VTBR',
    'RU000A0JP7J7': 'PIKK',
    'RU000A0JPFP0': 'LSRG',
    'RU000A0JPGA0': 'MVID',
    'RU000A0JPKH7': 'HYDR',
    'RU000A0JPN96': 'MRKP',
    'RU000A0JPNM1': 'IRAO',
    'RU000A0JPNN9': 'FEES',
    'RU000A0JPP37': 'UGLD',
    'RU000A0JPPB9': 'MRKZ',
    'RU000A0JPPL8': 'MRKC',
    'RU000A0JPPN4': 'MRKV',
    'RU000A0JPPT1': 'MRKU',
    'RU000A0JPR50': 'SELG',
    'RU000A0JPV70': 'MTLRP',
    'RU000A0JQ9P9': 'SPBE',
    'RU000A0JQTS3': 'AQUA',
    'RU000A0JQUZ6': 'RAGR',
    'RU000A0JR4A1': 'MOEX',
    'RU000A0JRH43': 'MBNK',
    'RU000A0JRKT8': 'PHOR',
    'RU000A0JSE60': 'RNFT',
    'RU000A0JUG31': 'CBOM',
    'RU000A0JVBT9': 'UWGN',
    'RU000A0JVW89': 'SFIN',
    'RU000A0JXNU8': 'FLOT',
    'RU000A0ZZAC4': 'SVCB',
    'RU000A0ZZBC2': 'SOFL',
    'RU000A0ZZFS9': 'LEAS',
    'RU000A0ZZFU5': 'DOMRF',
    'RU000A0ZZG02': 'SMLT',
    'RU000A0ZZM04': 'RENI',
    'RU000A1002V2': 'EUTR',
    'RU000A100K72': 'ENPG',
    'RU000A1014L8': 'LQDT',
    'RU000A102093': 'ELMT',
    'RU000A1025V3': 'RUAL',
    'RU000A102S15': 'LENT',
    'RU000A102XG9': 'SGZH',
    'RU000A103RF1': 'SBMM',
    'RU000A103X66': 'POSI',
    'RU000A104U43': 'BTBR',
    'RU000A104X08': 'AKMM',
    'RU000A105EX7': 'WUSH',
    'RU000A106DL2': 'TMON',
    'RU000A106T36': 'ASTR',
    'RU000A106XF2': 'HNFG',
    'RU000A106YF0': 'VKCO',
    'RU000A107662': 'HEAD',
    'RU000A107ER5': 'DIAS',
    'RU000A107J11': 'DELI',
    'RU000A107JE2': 'GEMC',
    'RU000A107RM8': 'ZAYM',
    'RU000A107T19': 'YDEX',
    'RU000A107UL4': 'T',
    'RU000A108JF7': 'PRMD',
    'RU000A108K09': 'VSEH',
    'RU000A108KL3': 'MDMG',
    'RU000A108X38': 'X5',
    'RU000A108ZR8': 'DATA',
    'RU000A109B25': 'OZPH',
    'RU000A10ANA1': 'CNRU',
    'RU000A10B5G8': 'FIXR',
    'RU000A10C1L6': 'ETLN',
    'RU000A10CRQ4': 'GLRX',
    'RU000A10CTQ0': 'BAZA',
    'RU000A10CW95': 'OZON',
    'US42207L1061': 'HEAD',
    'US5603172082': 'VKCO',
    'US69269L1044': 'OZON',
    'US7496552057': 'RAGR',
    'US87238U2033': 'T',
    'US98387E2054': 'X5',
};

export const TICKER_TO_NAME: Record<string, string> = {
    'AFKS': 'АФК Система',
    'AFLT': 'Аэрофлот',
    'ALRS': 'АЛРОСА',
    'ASTR': 'Астра',
    'BAZA': 'iБАЗИС',
    'BELU': 'НоваБев',
    'BSPB': 'Банк Санкт-Петербург',
    'BTBR': 'В2В-РТС',
    'CBOM': 'МКБ',
    'CHMF': 'Северсталь',
    'CNRU': 'ЦИАН',
    'DATA': 'iАренадата',
    'DOMRF': 'Дом.РФ',
    'ELMT': 'Элемент',
    'ENPG': 'ЭН+',
    'FEES': 'ФСК Россети',
    'FESH': 'ДВМП',
    'FIXR': 'ФиксПрайс',
    'FLOT': 'Совкомфлот',
    'GAZP': 'Газпром',
    'GEMC': 'ЕМС',
    'GLRX': 'ГЛОРАКС',
    'GMKN': 'Норильский никель',
    'HEAD': 'HeadHunter',
    'HYDR': 'РусГидро',
    'IRAO': 'Интер РАО',
    'LEAS': 'Европлан',
    'LENT': 'Лента',
    'LKOH': 'Лукойл',
    'LSRG': 'ЛСР',
    'MAGN': 'ММК',
    'MBNK': 'МТСБанк',
    'MDMG': 'Мать и дитя',
    'MGNT': 'Магнит',
    'MOEX': 'Московская биржа',
    'MRKP': 'РоссетиЦП',
    'MRKU': 'РоссетиУрал',
    'MSNG': 'МосЭнерго',
    'MSRS': 'РоссетиМР',
    'MTSS': 'МТС',
    'NLMK': 'НЛМК',
    'NMTP': 'НМТП',
    'NVTK': 'НОВАТЭК',
    'OZON': 'Озон',
    'OZPH': 'ОзонФарма',
    'PHOR': 'ФосАгро',
    'PIKK': 'ПИК',
    'PLZL': 'Полюс',
    'POSI': 'Positive Technologies',
    'PRMD': 'Промомед',
    'RAGR': 'Русагро',
    'RASP': 'Распадская',
    'RENI': 'Ренессанс Страхование',
    'ROSN': 'Роснефть',
    'RTKM': 'Ростелеком',
    'RUAL': 'Русал',
    'SBER': 'Сбербанк',
    'SBERP': 'Сбербанк (прив)',
    'SELG': 'Селигдар',
    'SFIN': 'ЭсЭфАй',
    'SIBN': 'Газпром нефть',
    'SMLT': 'Самолет',
    'SNGS': 'Сургутнефтегаз',
    'SNGSP': 'Сургутнефтегаз (прив)',
    'SVCB': 'Совкомбанк',
    'T': 'Т-Технологии',
    'TATN': 'Татнефть',
    'TATNP': 'Татнефть (прив)',
    'TRMK': 'ТМК',
    'TRNFP': 'Транснефть (прив)',
    'UGLD': 'ЮГК',
    'UPRO': 'Юнипро',
    'UWGN': 'ОВК',
    'VKCO': 'ВК',
    'VSEH': 'ВИ.ру',
    'VTBR': 'ВТБ',
    'X5': 'X5 Group',
    'YDEX': 'Яндекс',
};

export const TICKER_TO_COLOR: Record<string, string> = {
    'AFKS': '#A83E48',
    'AFLT': '#3E5E96',
    'ALRS': '#3E8CB0',
    'ASTR': '#3E72A8',
    'CHMF': '#3E5A82',
    'ENPG': '#7A5C3F',
    'FLOT': '#2F5288',
    'GAZP': '#355F92',
    'GMKN': '#3E8290',
    'HEAD': '#9A4A5C',
    'HYDR': '#3E8FA0',
    'IRAO': '#B23E3A',
    'LKOH': '#A83A33',
    'MAGN': '#BE4438',
    'MGNT': '#BE3F3F',
    'MOEX': '#3E7C9E',
    'MTSS': '#BE4B3E',
    'NLMK': '#3E7FA8',
    'NVTK': '#34507A',
    'OZON': '#3E63A8',
    'PHOR': '#5A8A4A',
    'PIKK': '#B0653A',
    'PLZL': '#B8A04E',
    'POSI': '#5A7A3E',
    'RAGR': '#6B8A4A',
    'ROSN': '#9A7E3E',
    'RTKM': '#6B4F8A',
    'RTKMP': '#6B4F8A',
    'RUAL': '#9A5246',
    'SBER': '#3F7A50',
    'SBERP': '#3F7A50',
    'SIBN': '#2C4E7E',
    'SMLT': '#4A5FA0',
    'SNGS': '#A8943E',
    'SNGSP': '#A8943E',
    'T': '#B89A3E',
    'TATN': '#A6483C',
    'TATNP': '#A6483C',
    'TRNFP': '#8C5A3E',
    'VTBR': '#3E86A8',
    'X5': '#4E8A4E',
    'YDEX': '#A8473B',
};

// Имена для бумаг, которых нет в каталоге Сезонности (instruments type=stock), но
// которые есть в портфелях фондов и имеют логотип. Курируется ВРУЧНУЮ — не из БД.
// Стиль имени — единый стандарт ОИ (2026-07-08), префы — маркер «(прив)».
const TICKER_TO_NAME_EXTRA: Record<string, string> = {
    ABIO: 'Артген',
    BANEP: 'Башнефть (прив)',
    LSNGP: 'РоссетиЛенэн (прив)',
    MTLR: 'Мечел',
    MTLRP: 'Мечел (прив)',
    MVID: 'М.Видео',
    RNFT: 'РуссНефть',
    RTKMP: 'Ростелеком (прив)',
    SGZH: 'Сегежа Групп',
    SOFL: 'Софтлайн',
    SPBE: 'СПБ биржа',
    WUSH: 'Whoosh',
};

/**
 * resolveFundTicker — бумага фонда → каноничный MOEX-тикер АКЦИИ (sec_id), как в
 * Сезонности. НИКОГДА не возвращает фьючерсный код.
 *
 * Если у бумаги есть ISIN — доверяем ТОЛЬКО точной карте ISIN_TO_TICKER (выверена
 * по MOEX ISS + OpenFIGI). ISIN есть, но не в карте → бумага не наша акция (облига-
 * ция/ОФЗ/денежный рынок/иностранная без листинга MOEX) → undefined (точка, без
 * keyword-гадания: иначе «Газпром капитал»-облигация словила бы лого Газпрома).
 * Keyword-фолбэк по имени — только для строк БЕЗ ISIN (легаси-источники).
 */
export function resolveFundTicker(name?: string | null, isin?: string | null): string | undefined {
    if (isin) return ISIN_TO_TICKER[isin] || undefined;
    return assetTicker(name);
}

/** Каноничное имя бумаги (формат Сезонности); иначе — исходное имя из данных фонда. */
export function fundAssetName(name?: string | null, isin?: string | null): string {
    const t = resolveFundTicker(name, isin);
    return (t && (TICKER_TO_NAME[t] ?? TICKER_TO_NAME_EXTRA[t])) || name || '';
}

/** Фирменный цвет по резолвнутому тикеру; иначе keyword по имени; иначе undefined. */
export function fundAssetColor(name?: string | null, isin?: string | null): string | undefined {
    const t = resolveFundTicker(name, isin);
    return (t && TICKER_TO_COLOR[t]) || assetColor(name);
}
