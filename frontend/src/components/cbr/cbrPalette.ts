/**
 * Палитра для категорий участников ОРФР Банка России.
 *
 * 7 цветов для stocks/ofz + 5 для fx (один пересекается — НФО, Физлица).
 * Подобраны в духе нашего editorial design — desaturated, paper-friendly,
 * устойчиво различимы и в light, и в dark теме.
 *
 * Светлая тема: чуть более насыщенные, чтобы на cream/white фоне различались
 * Тёмная: чуть осветлённые, чтобы не «проваливались» в тёмный фон
 */

interface CategoryColor {
  light: string;
  dark: string;
}

export const CBR_CATEGORY_COLORS: Record<string, CategoryColor> = {
  // === Общие для stocks/ofz/fx ===
  'Нерезиденты':              { light: '#5BA4C9', dark: '#6FB8DC' }, // cyan
  'НФО':                       { light: '#7C8794', dark: '#8A95A2' }, // stone-grey
  'Физические лица':           { light: '#1F4D7A', dark: '#3A6CA5' }, // тёмно-синий

  // === Stocks/OFZ-only ===
  'Прочие Банки':             { light: '#D4A574', dark: '#E0B585' }, // охра
  'СЗКО':                      { light: '#3D4757', dark: '#5A6678' }, // dark grey-blue
  'Доверительное управление':  { light: '#C75B5B', dark: '#D87575' }, // брусничный
  'Нефинансовые организации':  { light: '#D87642', dark: '#E58853' }, // терракот

  // === FX-only ===
  'Клиенты российских кредитных организаций': { light: '#5BA4C9', dark: '#6FB8DC' }, // тот же cyan
  'Российские кредитные организации':         { light: '#1F4D7A', dark: '#3A6CA5' }, // dark blue
  'Банк России':                              { light: '#C75B5B', dark: '#D87575' }, // брусничный — регулятор
};

const FALLBACK_LIGHT = '#7C8794';
const FALLBACK_DARK = '#8A95A2';

/** Возвращает hex-цвет категории для текущей темы. */
export function getCategoryColor(
  category: string,
  theme: 'editorial-light' | 'editorial-dark' | string,
): string {
  const c = CBR_CATEGORY_COLORS[category];
  if (!c) return theme.includes('dark') ? FALLBACK_DARK : FALLBACK_LIGHT;
  return theme.includes('dark') ? c.dark : c.light;
}
