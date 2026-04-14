/**
 * Fear Index — цвета, лейблы, хелперы.
 *
 * Цвета берутся из CHART_COLORS.fear (chartTheme.ts).
 * Используется в FearIndexPage и OverviewPage.
 */

import { CHART_COLORS } from './chartTheme';

// Маппинг API-классификация → цвет (для итерации в легенде)
export const FEAR_COLORS: Record<string, string> = {
  'Extreme Greed': CHART_COLORS.fear.extremeGreed,
  'Greed': CHART_COLORS.fear.greed,
  'Neutral': CHART_COLORS.fear.neutral,
  'Fear': CHART_COLORS.fear.fear,
  'Extreme Fear': CHART_COLORS.fear.extremeFear,
};

// API возвращает англ. ключи — маппим на рус. лейблы
export const FEAR_LABELS_RU: Record<string, string> = {
  'Extreme Greed': 'Экстремальная жадность',
  'Greed': 'Жадность',
  'Neutral': 'Нейтрально',
  'Fear': 'Страх',
  'Extreme Fear': 'Экстремальный страх',
};

// Цвет по числовому score (0–100)
export function getFearColor(score: number): string {
  if (score < 25) return CHART_COLORS.fear.extremeGreed;
  if (score < 45) return CHART_COLORS.fear.greed;
  if (score < 55) return CHART_COLORS.fear.neutral;
  if (score < 75) return CHART_COLORS.fear.fear;
  return CHART_COLORS.fear.extremeFear;
}

// Градиент фон для карточки
export function getFearGradient(score: number): string {
  const color = getFearColor(score);
  return `linear-gradient(135deg, ${color}22 0%, ${color}11 100%)`;
}

// Рус. лейбл по числовому score
export function getFearLabel(score: number): string {
  if (score < 25) return FEAR_LABELS_RU['Extreme Greed'];
  if (score < 45) return FEAR_LABELS_RU['Greed'];
  if (score < 55) return FEAR_LABELS_RU['Neutral'];
  if (score < 75) return FEAR_LABELS_RU['Fear'];
  return FEAR_LABELS_RU['Extreme Fear'];
}
