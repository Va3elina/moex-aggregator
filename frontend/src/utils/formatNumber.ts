/**
 * formatNumber — единый форматер чисел для Фрейма.
 *
 * Стандарт сайта: РУССКИЙ разделитель тысяч (тонкий пробел) + АНГЛИЙСКИЙ
 * разделитель дробной части (точка). Пример: "1 234.56".
 *
 *  Почему смешанный формат:
 *    - Пробел между тысячами читается лучше чем американская запятая
 *      ("1,234.56" → "1 234.56"), особенно для крупных значений на графиках.
 *    - Точка для дробной — большинство индикаторов на сайте (Buffett, OI и т.д.)
 *      исторически используют `.toFixed(N)` который пишет именно точку.
 *      Запятая в части индикаторов (СЧА секонд-ось, Heatmap %) раньше создавала
 *      рассинхрон → юзер замечал. Стандартизировали на точку.
 *
 *  Trade-off: формат не соответствует ни 100% русскому (там было бы
 *  "1 234,56"), ни 100% американскому ("1,234.56") — это наш кастомный
 *  hybrid. Но он более единообразен внутри одного UI чем смесь locale'ов.
 *
 *  Использование:
 *    formatNumber(1234.5)         → "1 235"      (без decimals)
 *    formatNumber(1234.5, 2)      → "1 234.50"   (2 знака после)
 *    formatNumber(1234.567, 1)    → "1 234.6"    (округление)
 *    formatNumber(-169238)        → "-169 238"
 *    formatNumber(14.16, 2)       → "14.16"
 */
export function formatNumber(value: number, decimals: number = 0): string {
  // toLocaleString('ru-RU') даёт " " (NBSP, U+00A0) между тысячами и "," для дробной.
  // Заменяем запятую на точку — получаем "1 234.56".
  // U+00A0 (non-breaking space) специально оставляем — он не ломает перенос текста
  // и SVG корректно его рендерит как пробел.
  return value.toLocaleString('ru-RU', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).replace(',', '.');
}

/**
 * Краткий формат для больших чисел: "1.5 млрд", "234 млн".
 * Авто-выбирает unit на основе масштаба value.
 */
export function formatCompact(value: number, decimals: number = 1): string {
  const abs = Math.abs(value);
  if (abs >= 1e12) return `${formatNumber(value / 1e12, decimals)} трлн`;
  if (abs >= 1e9)  return `${formatNumber(value / 1e9, decimals)} млрд`;
  if (abs >= 1e6)  return `${formatNumber(value / 1e6, decimals)} млн`;
  if (abs >= 1e3)  return `${formatNumber(value / 1e3, decimals)} тыс`;
  return formatNumber(value, decimals);
}
