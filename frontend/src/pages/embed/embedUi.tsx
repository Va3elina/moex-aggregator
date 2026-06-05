/**
 * embedUi — общий мелкий UI для embed-виджетов. После перехода на EmbedShell +
 * drawer настроек (EmbedSettings.tsx) контролы шапки переехали туда; здесь остался
 * только центрированный статус-месседж поверх области графика.
 */

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
