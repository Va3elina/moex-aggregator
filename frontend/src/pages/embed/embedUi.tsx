/**
 * embedUi — общие мелкие UI-куски для embed-виджетов (чтобы не дублировать
 * по каждому индикатору). Стили инлайновые с CSS-var + fallback, чтобы корректно
 * работать в любой теме и даже без полного набора токенов.
 */
import type { CSSProperties } from 'react';

/** Вертикальный контейнер виджета — заполняет весь iframe. */
export const embedColumn: CSSProperties = {
  width: '100%',
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  boxSizing: 'border-box',
  padding: 12,
};

/** Шапка виджета: заголовок + контролы. */
export const embedHeader: CSSProperties = {
  flexShrink: 0,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  minHeight: 30,
  marginBottom: 6,
  flexWrap: 'wrap',
};

/** Сегмент-кнопка (переключатель режима/группы). */
export function segBtn(active: boolean): CSSProperties {
  return {
    fontSize: 11,
    fontWeight: 600,
    padding: '3px 8px',
    borderRadius: 4,
    cursor: 'pointer',
    border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
    background: active ? 'var(--accent, #FF5C2B)' : 'transparent',
    color: active ? '#fff' : 'var(--text-secondary)',
    lineHeight: 1.4,
    whiteSpace: 'nowrap',
  };
}

/** Кнопка-триггер выбора инструмента. */
export const pickerBtn: CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: 6,
  cursor: 'pointer',
  padding: '3px 8px',
  borderRadius: 4,
  border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
  background: 'var(--bg-secondary, transparent)',
  color: 'var(--text-primary)',
};

/** Центрированное сообщение (loading / empty / error) поверх области графика. */
export function EmbedMsg({ text }: { text: string }) {
  return (
    <div
      style={{
        position: 'absolute',
        inset: 0,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: 'var(--text-secondary)',
        fontSize: 14,
        textAlign: 'center',
        padding: 16,
      }}
    >
      {text}
    </div>
  );
}
