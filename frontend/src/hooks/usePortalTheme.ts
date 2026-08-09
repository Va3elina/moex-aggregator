/**
 * usePortalTheme — пропсы темы для КОРНЯ портала, уходящего в document.body.
 *
 * Зачем. Тема сайта живёт одним атрибутом на <html>, и портал в body её
 * наследует сам. Но песочница (/sandbox) даёт КАЖДОЙ панели собственную тему
 * через data-theme на .sb-panel — а портал из этого поддерева вылетает и
 * резолвит переменные от корня, то есть от темы ОБОЛОЧКИ. Отсюда светлая
 * модалка на тёмной панели и светлые поля рамки вокруг тёмного снимка.
 *
 * ⚠️ Почему не «всегда вешать data-theme». Эти же модалки (алерты, настройки
 * графика, экспорт, подсказки, пикер фондов) работают на обычных страницах
 * сайта, где всё давно готово и НИЧЕГО меняться не должно (прямое требование
 * Вадима). Поэтому хук отдаёт пустой объект, когда тема поддерева совпадает с
 * корневой: на сайте и в embed-виджетах DOM остаётся байт-в-байт прежним, а
 * атрибут и цвет появляются ровно там, где темы разошлись — то есть только
 * внутри песочницы с переопределённой темой панели.
 *
 * color обязателен вместе с data-theme: атрибут чинит фон и переменные, но
 * цвет текста портал наследует от <body>, то есть от темы оболочки.
 */
import { useTheme } from '../contexts/ThemeContext';
import type { CSSProperties } from 'react';

export interface PortalThemeProps {
  'data-theme'?: string;
  style?: CSSProperties;
}

export function usePortalTheme(): PortalThemeProps {
  const { theme } = useTheme();
  // Читаем корень напрямую: ThemeContext внутри песочницы подменён на тему
  // панели (SandboxThemeScope), а на <html> висит тема оболочки — сравнить
  // их иначе нечем.
  const rootTheme = typeof document !== 'undefined'
    ? document.documentElement.getAttribute('data-theme')
    : null;
  if (!theme || theme === rootTheme) return {};
  return { 'data-theme': theme, style: { color: 'var(--text-primary)' } };
}
