/**
 * useEditorial — хелпер для conditional editorial rendering на страницах.
 *
 * Возвращает { isEditorial, theme } — readonly информация о текущей теме.
 * Используется когда нужны inline-стили (например custom chart colors) которые
 * EditorialChip/EditorialFrame не покрывают.
 */
import { useTheme } from '../../contexts/ThemeContext';

export function useEditorial() {
  const { theme } = useTheme();
  return {
    theme,
    isEditorial: theme.startsWith('editorial'),
  };
}
