/**
 * futuresSpotAliases — поисковые алиасы фьючерсных серий по тикеру базового актива.
 *
 * Зачем: в пикере ОИ у квартального фьючерса sectype «SR» и имя «Сбербанк»,
 * а пользователь вбивает тикер спота «sber». Без алиасов находился только
 * вечный SBERF (его sectype содержит «SBER»). Теперь ввод «sber» показывает
 * все вариации: SR (квартальный), SBERF (вечный) и, если есть, мини.
 *
 * Ключ — «база» серии (см. extractBase в InstrumentIcon: SRH→SR, SBERF→SBERF),
 * значение — тикеры спота / понятные коды, по которым серию должно быть видно.
 * Матч — подстрока, case-insensitive (как у sectype/name в useInstrumentFilter).
 *
 * Новая серия в ОИ → добавить строку сюда (рядом с FUT_BASE_TO_LOGO в
 * InstrumentIcon для лого).
 */
import { extractBase } from '../components/InstrumentIcon';

export const FUT_BASE_TO_SPOT: Record<string, string[]> = {
  // Акции (квартальные)
  AF: ['AFLT'],
  AK: ['AFKS'],
  AL: ['ALRS'],
  AS: ['ASTR'],
  AB: ['ASTR'],      // Астра (мини)
  BN: ['BANE'],
  BS: ['BSPB'],
  CH: ['CHMF'],
  CK: ['CHMF'],      // Северсталь (мини)
  CM: ['CBOM'],
  FE: ['FESH'],
  FL: ['FLOT'],
  FS: ['FEES'],
  GK: ['GMKN'],
  GZ: ['GAZP'],
  HD: ['HEAD'],
  HY: ['HYDR'],
  IR: ['IRAO'],
  IS: ['ABIO'],      // Артген
  KM: ['KMAZ'],
  LE: ['LEAS'],      // Европлан
  LK: ['LKOH'],
  MC: ['MTLR'],
  ME: ['MOEX'],
  MG: ['MAGN'],
  MN: ['MGNT'],
  MT: ['MTSS'],
  MV: ['MVID'],
  NB: ['BELU'],      // НоваБев
  NE: ['BELU'],      // НоваБев (мини)
  NM: ['NLMK'],
  NV: ['NVTK'],
  ON: ['OZON'],
  PH: ['PHOR'],
  PI: ['PIKK'],
  PS: ['POSI'],
  PX: ['PLZL'],
  RA: ['RASP'],
  RD: ['RENI'],
  RL: ['RUAL'],
  RN: ['ROSN'],
  RT: ['RTKM'],
  RQ: ['RTKMP'],     // Ростелеком (прив)
  RU: ['RNFT'],
  S0: ['SOFL'],
  SC: ['SVCB'],
  SE: ['SPBE'],
  SG: ['SNGSP'],
  SH: ['SFIN'],
  SN: ['SNGS'],
  SO: ['SIBN'],
  SP: ['SBERP'],
  SR: ['SBER'],
  SS: ['SMLT'],
  SZ: ['SGZH'],
  TB: ['T', 'TCSG'], // Т-Технологии (бывш. Тинькофф)
  TN: ['TRNFP'],
  TP: ['TATNP'],
  TT: ['TATN'],
  UN: ['UPRO'],
  VB: ['VTBR'],
  VK: ['VKCO'],
  WU: ['WUSH'],
  X5: ['X5'],
  YD: ['YDEX'],
  EA: ['ENPG'],
  MD: ['MDMG'],
  LN: ['LENT'],
  FI: ['FIXR'],
  DR: ['DOMRF'],
  RZ: ['RAGR'],
  // Акции (вечные)
  SBERF: ['SBER'],
  GAZPF: ['GAZP'],
  // Валюты
  Si: ['USDRUB', 'USD'], UC: ['USDRUB', 'USD'], UM: ['USDRUB', 'USD'], USDRUBF: ['USDRUB', 'USD'],
  Eu: ['EURRUB', 'EUR'], ED: ['EURRUB', 'EUR'], ER: ['EURRUB', 'EUR'], EURRUBF: ['EURRUB', 'EUR'],
  CR: ['CNYRUB', 'CNY'], CNYRUBF: ['CNYRUB', 'CNY'],
  // Индексы
  MX: ['IMOEX'], MM: ['IMOEX'], MY: ['IMOEX'], IP: ['IMOEX'], IMOEXF: ['IMOEX'],
  RI: ['RTSI'], RM: ['RTSI'],
  // Сырьё
  BR: ['BRENT'], BM: ['BRENT'],
  GD: ['GOLD', 'GLDRUB'], GL: ['GOLD', 'GLDRUB'], GN: ['GOLD', 'GLDRUB'], GLDRUBF: ['GOLD', 'GLDRUB'],
  SV: ['SILVER', 'SLVRUB'], S1: ['SILVER', 'SLVRUB'], S2: ['SILVER', 'SLVRUB'], SLVRUBF: ['SILVER', 'SLVRUB'],
  NG: ['NATGAS', 'GAS'], NR: ['NATGAS', 'GAS'], FF: ['NATGAS', 'GAS'],
};

const EMPTY: readonly string[] = [];
const cache = new Map<string, readonly string[]>();

/** Алиасы (lower-case) для sectype; пусто, если серия не в карте. Мемоизировано. */
export function spotAliasesFor(sectype: string): readonly string[] {
  const hit = cache.get(sectype);
  if (hit) return hit;
  const list = FUT_BASE_TO_SPOT[sectype] ?? FUT_BASE_TO_SPOT[extractBase(sectype)] ?? EMPTY;
  const lowered = list.map((a) => a.toLowerCase());
  cache.set(sectype, lowered);
  return lowered;
}
