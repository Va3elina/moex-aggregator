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
  // Узкие индексы (нет лого в стикерпаке)
  CS: { icon: 'ИП', bg: '#9333EA', color: '#fff' },
  FN: { icon: 'ИФ', bg: '#1E40AF', color: '#fff' },
  HO: { icon: 'НМ', bg: '#0F766E', color: '#fff' },
  MA: { icon: 'ИМ', bg: '#7C2D12', color: '#fff' },
  OG: { icon: 'ИНГ', bg: '#92400E', color: '#fff' },
  VI: { icon: 'σ', bg: '#EF4444', color: '#fff' },
  FS: { icon: 'ФСК', bg: '#10B981', color: '#fff' },
  // Spot инструменты валюты — теперь через FUT_BASE_TO_LOGO (USD/EUR/CNY/GOLD).
  // RGBI/RGBITR — через FUT_BASE_TO_LOGO (BONDS-герб).
  // Здесь только то для чего нет лого в стикерпаке:
  RVI: { icon: 'σ', bg: '#EF4444', color: '#fff' },
  RUSFAR3M: { icon: '%', bg: '#14B8A6', color: '#fff' },
};

// База фьючерса → имя лого-файла без расширения.
// Mapping проверен по реальной таблице instruments в БД (312 контрактов → 70 баз).
// См. agent run от 2026-04-26.
export const FUT_BASE_TO_LOGO: Record<string, string> = {
  // Акции (квартальные)
  AF: 'AFLT', AK: 'AFKS', AL: 'ALRS', AS: 'ASTR',
  BN: 'BANE',  // Башнефть
  BS: 'BSPB',  // Банк СПб
  CH: 'CHMF', CM: 'CBOM', FE: 'FESH', FL: 'FLOT',
  GK: 'GMKN', GZ: 'GAZP', HD: 'HEAD', HY: 'HYDR',
  IR: 'IRAO', KM: 'KMAZ', LE: 'LEAS', LK: 'LKOH',
  MC: 'MTLR',  // Мечел
  ME: 'MOEX', MG: 'MAGN', MN: 'MGNT', MT: 'MTSS',
  MV: 'MVID',  // М.Видео
  NB: 'BELU', NM: 'NLMK', NV: 'NVTK', PH: 'PHOR',
  PI: 'PIKK', PS: 'POSI', PX: 'PLZL',
  RA: 'RASP',  // Распадская
  RD: 'RENI',
  RL: 'RUAL', RN: 'ROSN',
  RT: 'RTKM',  // Ростелеком
  S0: 'SOFL',  // Софтлайн
  SC: 'SVCB',
  SE: 'SPBE',  // СПБ Биржа (ранее ошибочно SELG)
  SG: 'SNGSP',
  SH: 'SFIN', SN: 'SNGS', SO: 'SIBN', SP: 'SBERP',
  SR: 'SBER', SS: 'SMLT',
  SZ: 'SGZH',  // Сегежа
  TB: 'T',    TN: 'TRNFP',
  TP: 'TATNP',TT: 'TATN', UN: 'UPRO',
  VB: 'VTBR',  // ВТБ
  VK: 'VKCO',
  WU: 'WUSH',  // Whoosh
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
  W4: 'WHEAT',
  // Валюты
  Si: 'USD',   USDRUBF: 'USD', UC: 'USD',
  Eu: 'EUR',   EURRUBF: 'EUR', ED: 'EUR', EG: 'EUR',
  CR: 'CNY',   CNYRUBF: 'CNY',
  JP: 'JPY',
  GU: 'GBP',
  // Индексы
  MX: 'MOEX_IDX', MM: 'MOEX_IDX', MY: 'MOEX_IDX',
  IP: 'MOEX_IDX', IMOEXF: 'MOEX_IDX',
  RI: 'RTS_IDX',  RM: 'RTS_IDX',
  // Spot валюта (с биржи MOEX) → лого валюты из стикерпака
  USD000UTSTOM: 'USD',
  EUR_RUB__TOM: 'EUR',
  CNYRUB_TOM: 'CNY',
  GLDRUB_TOM: 'GOLD',
  // Spot индексы → лого MOEX/RTS из стикерпака
  IMOEX: 'MOEX_IDX',
  RTSI: 'RTS_IDX',
  MCFTR: 'MOEX_IDX',  // Индекс полной доходности MOEX
  // Облигации (ОФЗ) → герб Минфина
  RGBI: 'BONDS',
  RGBITR: 'BONDS',
  RB: 'BONDS',  // Фьючерс на индекс RGBI
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
