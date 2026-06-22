// Разбор диплинка на /oi из Telegram-сигналов. Ссылка несёт контекст сигнала
// (актив + физ/юр + таймфрейм + режим/вариант), чтобы страница — ВКЛЮЧАЯ мобилку —
// открылась ровно в том виде, о котором пришёл сигнал, а не на дефолте.
// Десктоп и мобилка применяют это один раз на маунте (см. OpenInterestPage /
// MobileOpenInterestPage). Невалидные значения тихо игнорируются.
export interface OiDeepLink {
  instrument?: string;
  clgroup?: 'FIZ' | 'YUR';
  interval?: number; // 5 | 60 | 24
  period?: '1w' | '1m' | '1y' | '5y' | 'all';
  displayMode?: 'price' | 'positions' | 'participants';
  oiVariant?: 'oi' | 'long' | 'short' | 'both' | 'net';
}

export function parseOiDeepLink(sp: URLSearchParams): OiDeepLink {
  const out: OiDeepLink = {};
  const inst = sp.get('instrument');
  if (inst) out.instrument = inst;

  const clg = sp.get('clgroup');
  if (clg === 'FIZ' || clg === 'YUR') out.clgroup = clg;

  const iv = Number(sp.get('interval'));
  if (iv === 5 || iv === 60 || iv === 24) out.interval = iv;

  // Период графика. Явный ?period= имеет приоритет; иначе для внутридневных
  // таймфреймов (5м/1ч) дефолтим на 1 месяц. Сохранённый период доходит до 5y —
  // а 5-минутный запрос на годы это сотни тысяч баров (тяжёлая загрузка). Сигнал
  // про свежее внутридневное движение, месяца контекста достаточно. Дневной
  // (24) интервал лёгкий на любом диапазоне — его период не навязываем.
  const pr = sp.get('period');
  if (pr === '1w' || pr === '1m' || pr === '1y' || pr === '5y' || pr === 'all') {
    out.period = pr;
  } else if (out.interval === 5 || out.interval === 60) {
    out.period = '1m';
  }

  const mode = sp.get('mode');
  if (mode === 'price' || mode === 'positions' || mode === 'participants') out.displayMode = mode;

  const v = sp.get('variant');
  if (v === 'oi' || v === 'long' || v === 'short' || v === 'both' || v === 'net') out.oiVariant = v;

  return out;
}
