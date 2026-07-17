/**
 * InstrumentIcon — единая иконка для тикеров.
 *
 * Cascade:
 *   1. Извлекаем «базу» из sectype (квартальный SRH→SR, perpetual SBERF→SBERF, mini W4F→W4)
 *   2. Если база в FUT_BASE_TO_LOGO → лого через TickerLogo
 *   3. Если sectype в INSTRUMENT_ICONS → текстовый badge (для редких валют/сырья)
 *   4. Иначе TickerLogo по самому sectype (для акций SBER, GAZP)
 */
import TickerLogo from './TickerLogo';

// Текстовые badge'и для тех инструментов где НЕТ файла-лого.
// Если потом докинем лого — удаляем оттуда соответствующую запись.
export const INSTRUMENT_ICONS: Record<string, { icon: string; bg: string; color: string }> = {
  // Крипто (нет лого в стикерпаке) — биткоин-фьючерсы IBIT (IB) и индекс МосБиржи (BT)
  IB: { icon: '₿', bg: '#F7931A', color: '#fff' },
  BT: { icon: '₿', bg: '#F7931A', color: '#fff' },
  // Секторные индексы МосБиржи (CS/FN/HO/MA/OG) — теперь через FUT_BASE_TO_LOGO
  // (секторные иконки-плитки TradingView, файлы SECT_*). Здесь остаётся только
  // RVI — индекс волатильности без лого в стикерпаке.
  VI: { icon: 'σ', bg: '#EF4444', color: '#fff' },
  // Spot инструменты валюты — теперь через FUT_BASE_TO_LOGO (USD/EUR/CNY/GOLD).
  // RGBI/RGBITR — через FUT_BASE_TO_LOGO (BONDS-герб).
  // Здесь только то для чего нет лого в стикерпаке:
  RVI: { icon: 'σ', bg: '#EF4444', color: '#fff' },
  RUSFAR3M: { icon: '%', bg: '#14B8A6', color: '#fff' },
};

// База фьючерса → имя лого-файла без расширения.
// Mapping проверен по реальной таблице instruments в БД (312 контрактов → 70 баз).
// См. agent run от 2026-04-26.
//
// Корректировки коллеги (frame-logo-corrections-2026-04-29.json) применены:
// 28 futures-баз получили правильные raw_NNN-логотипы из стикерпака.
// raw_NNN.png — оригинальные логотипы компаний из дизайнерского стикерпака.
export const FUT_BASE_TO_LOGO: Record<string, string> = {
  // Акции (квартальные)
  AF: 'AFLT',
  AK: 'raw_069', // АФК Система (corrected 2026-04-29)
  AL: 'raw_063', // АЛРОСА (corrected)
  AS: 'raw_012', // Астра (corrected)
  BN: 'raw_009', // Башнефть (corrected)
  BS: 'raw_028', // Банк СПб (corrected)
  CH: 'raw_059', // Северсталь (corrected)
  CK: 'raw_059', // Северсталь (мини) — тот же знак, что у стандартного фьючерса CH
  CM: 'CBOM', FE: 'FESH', FL: 'FLOT',
  FS: 'raw_123', // ФСК Россети (corrected — раньше был text-badge)
  GK: 'raw_062', // Норильский никель (corrected)
  GZ: 'GAZP', HD: 'HEAD',
  HY: 'raw_117', // РусГидро (corrected)
  IR: 'IRAO',
  IS: 'raw_087', // Артген (corrected)
  KM: 'KMAZ',
  LE: 'raw_094', // Европлан (corrected)
  LK: 'LKOH',
  MC: 'raw_071', // Мечел (corrected)
  ME: 'MOEX', MG: 'MAGN',
  MN: 'raw_042', // Магнит (corrected)
  MT: 'MTSS',
  MV: 'raw_093', // М.Видео (corrected)
  NB: 'raw_082', // НоваБев (бывш. Белуга Групп)
  NM: 'NLMK', NV: 'NVTK',
  PH: 'raw_076', // ФосАгро (corrected)
  PI: 'PIKK', PS: 'POSI', PX: 'PLZL',
  RA: 'raw_072', // Распадская (corrected)
  RD: 'raw_037', // Ренессанс Страхование (corrected)
  RL: 'raw_064', // Русал (corrected)
  RN: 'ROSN',
  RT: 'raw_034', // Ростелеком (corrected)
  RU: 'raw_066', // РуссНефть (corrected)
  S0: 'raw_018', // Софтлайн (raw_070 оказался логотипом SFI — ошибка старых правок)
  SC: 'SVCB',
  SE: 'raw_035', // СПБ Биржа (corrected)
  SG: 'SNGSP',
  SH: 'SFIN', SN: 'SNGS', SO: 'SIBN', SP: 'SBERP',
  SR: 'SBER', SS: 'SMLT',
  SZ: 'raw_068', // Сегежа (corrected)
  TB: 'T',
  TN: 'raw_010', // Транснефть (corrected)
  TP: 'TATNP',TT: 'TATN', UN: 'UPRO',
  VB: 'raw_027', // ВТБ (corrected)
  VK: 'VKCO',
  WU: 'raw_067', // Whoosh (corrected)
  X5: 'X5',   YD: 'YDEX',
  // Перпетуалы
  SBERF: 'SBER', GAZPF: 'GAZP',
  // Сырьё
  BR: 'BRENT', BM: 'BRENT',
  GD: 'GOLD',  GL: 'GOLD',  GLDRUBF: 'GOLD',
  SV: 'SILVER',
  PT: 'PLATINUM',
  PD: 'PALLADIUM',
  NG: 'GAS', NR: 'GAS', FF: 'GAS',
  W4: 'raw_174', // Пшеница (corrected — раньше был WHEAT generic icon)
  // Валюты
  Si: 'USD',   USDRUBF: 'USD', UC: 'USD',
  Eu: 'EUR',   EURRUBF: 'EUR', ED: 'EUR',
  CR: 'CNY',   CNYRUBF: 'CNY',
  JP: 'JPY',
  GU: 'GBP',
  // Индексы
  MX: 'MOEX_IDX', MM: 'MOEX_IDX', MY: 'MOEX_IDX',
  IP: 'MOEX_IDX', IMOEXF: 'MOEX_IDX',
  RI: 'RTS_IDX',  RM: 'RTS_IDX',
  SF: 'SP500_IDX',  // фьючерс на индекс S&P 500 (SPYF) — лого из стикерпака
  // Секторные индексы МосБиржи — секторные иконки-плитки TradingView
  // (s3-symbol-logo.tradingview.com/sector/*), растеризованы в SECT_*.png.
  CS: 'SECT_CONSUMER',  // Потребительский сектор
  FN: 'SECT_FINANCE',   // Финансы
  OG: 'SECT_OILGAS',    // Нефть и газ
  MA: 'SECT_METALS',    // Металлы и добыча
  HO: 'SECT_REALTY',    // Недвижимость ДомКлик
  // Spot валюта (с биржи MOEX) → лого валюты из стикерпака
  USD000UTSTOM: 'USD',
  EUR_RUB__TOM: 'EUR',
  CNYRUB_TOM: 'CNY',
  GLDRUB_TOM: 'GOLD',
  // Spot сырьё (continuous-ряды) → те же лого, что у фьючерсов-аналогов
  NATGAS_HH: 'GAS',
  TTF_GAS: 'GAS',
  // Spot индексы → лого MOEX/RTS из стикерпака
  IMOEX: 'MOEX_IDX',
  RTSI: 'RTS_IDX',
  MCFTR: 'MOEX_IDX',  // Индекс полной доходности MOEX
  // Облигации (ОФЗ) → золотой двуглавый орёл на зелёном (OFZ_GERB.png,
  // прислан владельцем 2026-07-17; раньше был серый орёл ЦБ raw_152)
  RGBI: 'OFZ_GERB',
  RGBITR: 'OFZ_GERB',
  RB: 'OFZ_GERB',  // Фьючерс на индекс RGBI
  // Дополнительные валюты (сгенерированные иконки в том же стиле)
  AE: 'AED',
  AU: 'AUD',
  CA: 'CAD',
  CF: 'CHF',
  HK: 'HKD',
  KZ: 'KZT',
  TR: 'TRY',
  EC: 'EUR_CAD',  // EUR/CAD кросс
  // Металлы и сырьё (сгенерированные иконки)
  AN: 'ALUMINUM',
  CE: 'COPPER',
  NC: 'NICKEL',
  ZC: 'ZINC',
  KC: 'COFFEE',
  CC: 'COCOA',
  OJ: 'ORANGE',
  SA: 'SUGAR',
};

/**
 * STOCK_LOGO_OVERRIDE — корректировки для конкретных тикеров акций.
 * Применяется ДО FUT_BASE_TO_LOGO (приоритет full ticker над futures base).
 *
 * Источник: frame-logo-corrections-2026-04-29.json (правки коллеги).
 *
 * Содержит:
 *  - 41 stock с raw_NNN-логотипами из дизайнерского стикерпака
 *  - 1 reuse существующего логотипа (AFKS → HYDR)
 *  - 2 тикера выпали из коррекций (SELG, RGSS) — без override, fallback на дефолт
 *
 * Логика fallback:
 *   sectype в STOCK_LOGO_OVERRIDE → используем override
 *   Иначе → existing FUT_BASE_TO_LOGO / direct sectype
 */
export const STOCK_LOGO_OVERRIDE: Record<string, string> = {
  AFKS: 'HYDR',     // Система — reuse HYDR logo per JSON
  ALRS: 'raw_063',  // Алроса
  ASTR: 'raw_012',  // Группа Астра
  BANE: 'raw_009',  // Башнефть
  BSPB: 'raw_028',  // БСП
  CHMF: 'raw_059',  // Северсталь
  BELU: 'raw_082',  // НоваБев
  ELMT: 'raw_020',  // Элемент
  FEES: 'raw_123',  // Россети
  GEMC: 'raw_090',  // ЕМС
  JNOS: 'raw_149',  // Славнефть-ЯНОС
  KAZT: 'raw_078',  // КуйбышевАзот
  KZOS: 'raw_080',  // Казаньоргсинт
  LEAS: 'raw_094',  // Европлан
  LSNG: 'raw_123',  // Россети Ленэнерго
  MGNT: 'raw_042',  // Магнит
  MFGS: 'raw_149',  // Славнефть-МНГЗ
  MRKK: 'raw_123',  // Россети СК
  MRKP: 'raw_123',  // Россети ЦП
  MRKU: 'raw_123',  // Россети Урал
  MRKS: 'raw_123',  // Россети Сибирь
  MSNG: 'raw_005',  // МосЭнерго
  MSRS: 'raw_123',  // Россети МР
  NKNC: 'raw_079',  // НКНХ
  OGKB: 'raw_005',  // ОГК-2
  OZPH: 'raw_086',  // ОзонФарма
  PHOR: 'raw_076',  // ФосАгро
  PLZL: 'PLZL',     // Полюс — новый знак через INDIVIDUAL_LOGOS в TickerLogo (не спрайт)
  RAGR: 'raw_073',  // Русагро
  RENI: 'raw_037',  // Ренессанс
  RASP: 'raw_072',  // Распадская
  RTKM: 'raw_034',  // Ростелеком
  RUAL: 'raw_064',  // Русал
  TNSE: 'raw_130',  // ТНС-Энерго
  UGLD: 'raw_058',  // ЮГК
  USBN: 'raw_143',  // Уралсиб
  UWGN: 'raw_103',  // ОВК
  VTBR: 'raw_027',  // ВТБ
  VSMO: 'raw_108',  // ВСМПО-АВИСМА
  VJGZ: 'raw_004',  // Варьеганнефтегаз
  TRNFP: 'raw_010', // Транснефть (прив)
  // Бумаги из портфелей фондов без собственного PNG — переиспользуем
  // существующие лого стикерпака (унификация эмоджи 2026-07-08).
  ABIO: 'raw_087',  // Артген — лого фьючерса IS
  BANEP: 'raw_009', // Башнефть (прив) — лого BANE
  LSNGP: 'raw_123', // РоссетиЛенэн (прив) — герб Россетей
  MRKC: 'raw_123',  // РоссетиЦентр — герб Россетей
  MRKV: 'raw_123',  // РоссетиВолга — герб Россетей
  MRKZ: 'raw_123',  // РоссетиСЗ — герб Россетей
  MTLRP: 'raw_071', // Мечел (прив) — лого Мечела (раньше указывал на заглушку MTLR.png)
  RNFT: 'raw_066',  // РуссНефть — лого фьючерса RU
  RTKMP: 'raw_034', // Ростелеком (прив) — лого обычки
  // Правки владельца через logos-checker (frame-logo-corrections-2026-07-10):
  // выбор из полного стикерпака для бумаг, стоявших на generic-заглушках
  // «кружок + буква» или без лого. GMKN — важно: файл AFKS.png на самом деле
  // логотип Норникеля (== raw_062), а GMKN.png — Северстали; берём каноничные raw_NNN.
  GMKN: 'raw_062',  // Норильский никель (не путать: AFKS.png/GMKN.png перепутаны местами)
  HYDR: 'raw_117',  // РусГидро — заглушка HYDR.png заменена на корректный знак из стикерпака (== логотип фьючерса HY)
  SELG: 'raw_055',  // Селигдар
  SGZH: 'raw_068',  // Сегежа Групп
  WUSH: 'raw_067',  // Whoosh
  ETLN: 'raw_050',  // Эталон
  HNFG: 'raw_041',  // Хэндерсон
  SPBE: 'raw_035',  // СПБ биржа
  SOFL: 'raw_018',  // Софтлайн
  MTLR: 'raw_071',  // Мечел
  MVID: 'raw_093',  // М.Видео
  AQUA: 'raw_074',  // Инарктика
  ZAYM: 'raw_040',  // Займер
  EUTR: 'raw_104',  // ЕвроТранс
  DIAS: 'raw_017',  // Диасофт
  DELI: 'raw_023',  // Делимобиль
};

/**
 * Извлекает «базу» из sectype:
 * - Perpetual: SBERF, USDRUBF, IMOEXF — оставляем целиком (мапятся как есть)
 * - Квартальный (3 буквы): SRH → SR, GZM → GZ, BRZ → BR
 * - Микро/мини: W4F → W4, NRH → NR, MMU → MM (первые 2 char)
 * - Уже база: остаётся
 */
function extractBase(sectype: string): string {
  // Perpetual ends with F, length > 4
  if (sectype.length > 4 && sectype.endsWith('F')) return sectype;
  // 3 chars = quarterly future или micro/mini → first 2
  if (sectype.length === 3) return sectype.slice(0, 2);
  return sectype;
}

interface Props {
  sectype: string;
  size?: number;
  rounded?: 'none' | 'sm' | 'md' | 'full';
  eager?: boolean;
}

export default function InstrumentIcon({ sectype, size = 28, rounded = 'full', eager = false }: Props) {
  // 0. Stock override — приоритет full ticker над FUT base mapping.
  //    Источник: frame-logo-corrections-2026-04-29.json (правки коллеги).
  if (sectype in STOCK_LOGO_OVERRIDE) {
    return <TickerLogo ticker={STOCK_LOGO_OVERRIDE[sectype]} size={size} rounded={rounded} eager={eager} />;
  }

  // 1. Маппинг через base → лого-файл
  const base = extractBase(sectype);
  const logoTicker = FUT_BASE_TO_LOGO[base] || FUT_BASE_TO_LOGO[sectype];
  if (logoTicker) {
    return <TickerLogo ticker={logoTicker} size={size} rounded={rounded} eager={eager} />;
  }

  // 2. Кастомные badge'и (валюты/сырьё/индексы без лого)
  const ic = INSTRUMENT_ICONS[base] || INSTRUMENT_ICONS[sectype];
  if (ic) {
    const radius = rounded === 'full' ? '50%' : rounded === 'md' ? '8px' : rounded === 'sm' ? '4px' : '0';
    return (
      <div
        className="flex items-center justify-center flex-shrink-0 font-bold"
        style={{
          width: size,
          height: size,
          borderRadius: radius,
          backgroundColor: ic.bg,
          color: ic.color,
          fontSize: Math.max(9, Math.round(size * 0.32)),
          lineHeight: 1,
        }}
      >
        {ic.icon}
      </div>
    );
  }

  // 3. Обычная акция — лого по тикеру
  return <TickerLogo ticker={sectype} size={size} rounded={rounded} eager={eager} />;
}
