// Общие константы для графиков — единственный источник правды

export const PERIOD_LABELS: Record<string, string> = {
  '1d':  '1Д',
  '1w':  '1Н',
  '1m':  '1М',
  '3m':  '3М',
  '6m':  '6М',
  '1y':  '1Г',
  '2y':  '2Г',
  '3y':  '3Г',
  'all': 'Всё',
};

export const INTERVAL_LABELS: Record<number, string> = {
  5:  '5м',
  60: '1ч',
  24: '1д',
};

export const CHART_COLORS = {
  primary:  '#6366f1',
  emerald:  '#2EE59D',
  rose:     '#FF4D4D',
  amber:    '#FFB020',
  cyan:     '#22D3EE',
  lime:     '#C8FF2E',
  violet:   '#8B5CF6',
  pink:     '#EC4899',
};
