/**
 * EmbedSettings — общие примитивы контролов для embed-виджетов.
 *
 * Раньше здесь жил весь «скелет» виджета (EmbedShell + выезжающий drawer +
 * AssetPickerInline). После редизайна под терминал (EmbedToolbar.tsx: тулбар
 * прямо над графиком) скелет и пикер переехали туда; здесь остались только
 * переиспользуемые кусочки, которые кладутся в ⚙-поповер («ещё настройки»):
 * DrawerSection (метка-капс + контрол), SegGroup (сегмент-пилюли), ToggleRow
 * (свитч, re-export) и Checklist (мультивыбор категорий/участников).
 *
 * Всё инлайн-стилями с CSS-var, чтобы работать в любой теме внутри iframe.
 */
import { type CSSProperties, type ReactNode } from 'react';
import { Check } from 'lucide-react';

/** Раздел настроек: метка-капс + контрол. */
export function DrawerSection({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <div
        style={{
          fontSize: 10.5,
          fontWeight: 800,
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          color: 'var(--text-secondary)',
          marginBottom: 8,
        }}
      >
        {label}
      </div>
      {children}
    </div>
  );
}

function segStyle(active: boolean): CSSProperties {
  return {
    fontSize: 12,
    fontWeight: 700,
    padding: '6px 12px',
    borderRadius: 6,
    cursor: 'pointer',
    border: active ? '1.5px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
    background: active ? 'var(--accent)' : 'transparent',
    color: active ? '#fff' : 'var(--text-primary)',
    lineHeight: 1.3,
    whiteSpace: 'nowrap',
    transition: 'background 0.12s, border-color 0.12s',
  };
}

/** Группа сегмент-кнопок (режим / период / таймфрейм). Переносится на узких панелях. */
export function SegGroup<T extends string | number>({
  value,
  options,
  onChange,
}: {
  value: T;
  options: { id: T; label: string }[];
  onChange: (v: T) => void;
}) {
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 7 }}>
      {options.map((o) => (
        <button key={String(o.id)} style={segStyle(o.id === value)} onClick={() => onChange(o.id)}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

// ToggleRow вынесен в components/ToggleRow.tsx (нужен и desktop-модалке «Слои»);
// re-export сохраняет существующие импорты embed-страниц.
export { ToggleRow } from '../../components/ToggleRow';

/** Список чекбоксов (мультивыбор) — категории/участники с цветным маркером. */
export function Checklist({
  items,
}: {
  items: { id: string; label: string; on: boolean; color?: string; desc?: string; disabled?: boolean; onToggle: () => void }[];
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      {items.map((it) => (
        <button
          key={it.id}
          type="button"
          disabled={it.disabled}
          onClick={it.onToggle}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 9,
            padding: '7px 10px',
            borderRadius: 7,
            border: '1px solid var(--border-color, rgba(128,128,128,0.25))',
            background: 'transparent',
            color: 'var(--text-primary)',
            cursor: it.disabled ? 'not-allowed' : 'pointer',
            textAlign: 'left',
            opacity: it.disabled ? 0.55 : 1,
          }}
          title={it.disabled ? 'Нельзя скрыть последнюю видимую' : undefined}
        >
          <span
            style={{
              flexShrink: 0,
              width: 16,
              height: 16,
              borderRadius: 4,
              border: '1.5px solid ' + (it.color || 'var(--text-secondary)'),
              background: it.on ? (it.color || 'var(--accent)') : 'transparent',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            {it.on && <Check size={11} color="#fff" strokeWidth={3} />}
          </span>
          <span style={{ flex: 1, minWidth: 0 }}>
            <span style={{ fontSize: 12.5, fontWeight: 700, display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {it.label}
            </span>
            {it.desc && (
              <span style={{ fontSize: 10.5, color: 'var(--text-secondary)', display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {it.desc}
              </span>
            )}
          </span>
        </button>
      ))}
    </div>
  );
}
