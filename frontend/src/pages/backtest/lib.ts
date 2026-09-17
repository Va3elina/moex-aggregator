// Стенд: даты, куски свечей, форматирование, справочник бумаг.
export const DAY = 86400;
export const TFS: [number, string, string][] = [[5, '5м', '5 минут'], [15, '15м', '15 минут'], [60, '1ч', '1 час'], [1440, 'Д', '1 день']];
export const tfLabel = (tf: number) => TFS.find(x => x[0] === tf)?.[1] ?? String(tf);

/** Тикер логотипа в /logos/sm/<тикер>.png для типа фьючерса. */
export const LOGO: Record<string, string> = {
  AF: 'AFLT', AK: 'AFKS', BR: 'BRENT', CC: 'COCOA', CR: 'CNY', Eu: 'EUR', GK: 'GMKN', GZ: 'GAZP', LK: 'LKOH', MN: 'MGNT',
  MX: 'MOEX_IDX', NM: 'NLMK', PI: 'PIKK', PT: 'PLATINUM', RI: 'RTS_IDX', SN: 'SNGS', SR: 'SBER', SS: 'SMLT', SZ: 'SGZH',
  Si: 'USD', TT: 'TATN', VB: 'VTBR',
};
export const GROUP: Record<string, string> = {
  AF: 'Акции', AK: 'Акции', GK: 'Акции', GZ: 'Акции', LK: 'Акции', MN: 'Акции', NM: 'Акции', PI: 'Акции', SN: 'Акции',
  SR: 'Акции', SS: 'Акции', SZ: 'Акции', TT: 'Акции', VB: 'Акции', MX: 'Индексы', RI: 'Индексы', Si: 'Валюты', Eu: 'Валюты',
  CR: 'Валюты', BR: 'Товары', PT: 'Товары', CC: 'Товары',
};

export const iso = (d: Date) => d.toISOString().slice(0, 10);
export const today = () => iso(new Date());
export const addDays = (s: string, n: number) => { const d = new Date(s + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return iso(d); };
export const hm = (s: string) => Number(s.slice(0, 2)) * 60 + Number(s.slice(3, 5));
/** Секунды «МСК как UTC» — так же отдаёт /candles. */
export const ts = (d: string, minutes = 0) => Date.UTC(+d.slice(0, 4), +d.slice(5, 7) - 1, +d.slice(8, 10)) / 1000 + minutes * 60;
export const tsToIso = (t: number) => iso(new Date(t * 1000));
export const mmToStr = (m: number) => `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;
const MONTHS = ['янв.', 'февр.', 'марта', 'апр.', 'мая', 'июня', 'июля', 'авг.', 'сент.', 'окт.', 'нояб.', 'дек.'];
export const fmtDate = (s: string) => `${+s.slice(8, 10)} ${MONTHS[+s.slice(5, 7) - 1]} ${s.slice(0, 4)} г.`;
export const fmtD = (s: string) => `${s.slice(8, 10)}.${s.slice(5, 7)}.${s.slice(2, 4)}`;

export const num = (v: number | null | undefined, digits = 0) =>
  v == null || !isFinite(v) ? '—' : v.toLocaleString('ru-RU', { maximumFractionDigits: digits, minimumFractionDigits: digits });
export const signed = (v: number | null | undefined, digits = 0) => v == null || !isFinite(v) ? '—' : (v > 0 ? '+' : '') + num(v, digits);
export const pct = (v: number | null | undefined, digits = 2) => v == null || !isFinite(v) ? '—' : (v > 0 ? '+' : '') + num(100 * v, digits) + '%';
export const cls = (v: number | null | undefined) => v == null ? '' : v > 0 ? 'bt-up' : v < 0 ? 'bt-down' : '';
export const money = (v: number | null | undefined) => v == null || !isFinite(v) ? '—' : signed(v) + ' ₽';

/** Куски свечей по календарным границам — чтобы ответы сервера кэшировались: 5м — месяц, 15м — квартал, 1ч — год, Д — всё. */
export function chunkOf(tf: number, d: string): string {
  const y = +d.slice(0, 4), m = +d.slice(5, 7);
  if (tf === 5) return `${y}-${String(m).padStart(2, '0')}-01`;
  if (tf === 15) return `${y}-${String(3 * Math.floor((m - 1) / 3) + 1).padStart(2, '0')}-01`;
  if (tf === 60) return `${y}-01-01`;
  return '2019-01-01';
}
export function chunkStep(tf: number, start: string, dir: 1 | -1): string | null {
  if (tf === 1440) return null;
  const d = new Date(start + 'T00:00:00Z');
  d.setUTCMonth(d.getUTCMonth() + dir * (tf === 5 ? 1 : tf === 15 ? 3 : 12));
  const s = iso(d);
  return s < '2019-01-01' || s > today() ? null : s;
}
export function chunkEnd(tf: number, start: string): string {
  if (tf === 1440) return today();
  const d = new Date(start + 'T00:00:00Z');
  d.setUTCMonth(d.getUTCMonth() + (tf === 5 ? 1 : tf === 15 ? 3 : 12)); d.setUTCDate(0);
  const s = iso(d); return s > today() ? today() : s;
}
