/**
 * ToggleRow — строка-переключатель вкл/выкл («Цена», «Экспирации», «Индекс»).
 *
 * Вынесен из pages/embed/EmbedSettings (там остался re-export для embed-импортов):
 * теперь используется и desktop-модалкой «Слои» (LayersButton), и embed-drawer'ами.
 */
export function ToggleRow({
  label,
  checked,
  onChange,
  hint,
}: {
  label: string;
  checked: boolean;
  onChange: (v: boolean) => void;
  hint?: string;
}) {
  return (
    <button
      type="button"
      onClick={() => onChange(!checked)}
      style={{
        width: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 10,
        padding: '8px 12px',
        borderRadius: 8,
        border: '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
        background: 'transparent',
        color: 'var(--text-primary)',
        cursor: 'pointer',
        textAlign: 'left',
      }}
    >
      <span style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
        <span style={{ fontSize: 12.5, fontWeight: 700 }}>{label}</span>
        {hint && <span style={{ fontSize: 10.5, color: 'var(--text-secondary)' }}>{hint}</span>}
      </span>
      <span
        style={{
          flexShrink: 0,
          width: 38,
          height: 22,
          borderRadius: 999,
          position: 'relative',
          background: checked ? 'var(--accent)' : 'var(--border-color, rgba(128,128,128,0.4))',
          transition: 'background 0.15s',
        }}
      >
        <span
          style={{
            position: 'absolute',
            top: 2,
            left: checked ? 18 : 2,
            width: 18,
            height: 18,
            borderRadius: '50%',
            background: '#fff',
            transition: 'left 0.15s',
            boxShadow: '0 1px 2px rgba(0,0,0,0.3)',
          }}
        />
      </span>
    </button>
  );
}
