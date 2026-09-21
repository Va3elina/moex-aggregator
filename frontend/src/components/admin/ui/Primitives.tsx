/**
 * Базовые кирпичи админки — по docs/admin-design.md.
 *
 * Собраны по модели Tremor (copy-paste на чистом Tailwind): структура и
 * поведение отсюда, внешний вид — из наших editorial-токенов. Своих цветов
 * компоненты не заводят, только роли из index.css.
 */
import { cn } from '../../../lib/cn';

const MONO = "'IBM Plex Mono', monospace";

/* ── Уровень 1: ответ ─────────────────────────────────────────────────── */

/** Крупное число без рамки — то, ради чего заходят на страницу. */
export function Metric({ label, value, hint, active, onClick, delta }: {
  label: string;
  value: string | number;
  /** Как посчитано — не повтор заголовка. */
  hint?: string;
  active?: boolean;
  onClick?: () => void;
  delta?: { text: string; good: boolean } | null;
}) {
  const body = (
    <>
      <div
        className="text-[11px] uppercase"
        style={{ fontFamily: MONO, letterSpacing: '.08em', color: 'var(--text-muted)' }}
      >
        {label}
      </div>
      <div
        className="mt-1 font-semibold leading-none"
        style={{
          fontFamily: MONO, fontVariantNumeric: 'tabular-nums',
          fontSize: 'clamp(1.35rem, 2.4vw, 1.75rem)',
          color: active ? 'var(--accent)' : 'var(--text-primary)',
        }}
      >
        {typeof value === 'number' ? value.toLocaleString('ru-RU') : value}
      </div>
      {(hint || delta) && (
        <div className="mt-1.5 flex items-baseline gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
          {delta && (
            <span style={{ color: delta.good ? 'var(--success)' : 'var(--danger)', fontWeight: 600 }}>
              {delta.text}
            </span>
          )}
          {hint && <span>{hint}</span>}
        </div>
      )}
    </>
  );
  if (!onClick) return <div className="py-1">{body}</div>;
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={cn('text-left py-1 transition-opacity hover:opacity-70')}
    >
      {body}
    </button>
  );
}

/** Ряд метрик первого уровня: без рамок, разделены тонкими линиями. */
export function MetricRow({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="grid gap-x-6 gap-y-4"
      style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))' }}
    >
      {children}
    </div>
  );
}

/* ── Уровень 2: поддержка ─────────────────────────────────────────────── */

/** Карточка: фон и рамка 1px. 2px — только у активного элемента, не здесь. */
export function Panel({ title, note, action, children, className }: {
  title?: string;
  note?: string;
  action?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section
      className={cn('p-4', className)}
      style={{
        backgroundColor: 'var(--bg-secondary)',
        border: '1px solid color-mix(in srgb, var(--border-color) 35%, transparent)',
      }}
    >
      {(title || action) && (
        <header className="flex items-baseline gap-3 mb-3">
          {title && (
            <h3 className="text-[13px] font-semibold" style={{ color: 'var(--text-primary)' }}>
              {title}
            </h3>
          )}
          {note && <span className="text-xs" style={{ color: 'var(--text-muted)' }}>{note}</span>}
          {action && <div className="ml-auto">{action}</div>}
        </header>
      )}
      {children}
    </section>
  );
}

/* ── Уровень 3: детали ────────────────────────────────────────────────── */

/** Список с долей — вместо диаграммы там, где сравнивают величины. */
export function BarList({ items, onPick, activeKey }: {
  items: { key?: string; label: string; note?: string; value: number; extra?: string }[];
  onPick?: (key: string) => void;
  activeKey?: string | null;
}) {
  const max = Math.max(...items.map(i => i.value), 1);
  if (!items.length) {
    return <p className="py-4 text-center text-sm" style={{ color: 'var(--text-muted)' }}>Пусто</p>;
  }
  return (
    <div className="flex flex-col">
      {items.map((it, i) => {
        const on = !!it.key && it.key === activeKey;
        const inner = (
          <>
            <span
              aria-hidden
              className="absolute inset-y-px left-0 transition-[width] duration-300"
              style={{
                width: `${(it.value / max) * 100}%`,
                backgroundColor: on
                  ? 'color-mix(in srgb, var(--accent) 26%, transparent)'
                  : 'color-mix(in srgb, var(--accent) 12%, transparent)',
              }}
            />
            <span className="relative flex items-baseline gap-2 min-w-0">
              <span className="truncate text-[13px]" style={{ color: 'var(--text-primary)' }}>{it.label}</span>
              {it.note && <span className="text-xs shrink-0" style={{ color: 'var(--text-muted)' }}>{it.note}</span>}
            </span>
            <span
              className="relative text-[13px] font-semibold shrink-0"
              style={{ fontFamily: MONO, fontVariantNumeric: 'tabular-nums' }}
            >
              {it.value.toLocaleString('ru-RU')}
              {it.extra && (
                <em className="not-italic ml-1.5 text-xs font-normal" style={{ color: 'var(--text-muted)' }}>
                  {it.extra}
                </em>
              )}
            </span>
          </>
        );
        const cls = 'relative grid grid-cols-[1fr_auto] items-center gap-3 px-2 py-2 text-left';
        return it.key && onPick ? (
          <button key={i} type="button" className={cn(cls, 'hover:opacity-80')} onClick={() => onPick(it.key!)}>
            {inner}
          </button>
        ) : (
          <div key={i} className={cls}>{inner}</div>
        );
      })}
    </div>
  );
}

/** Доли одной величины одной полосой — когда важно соотношение, а не ряд. */
export function SplitBar({ parts }: { parts: { label: string; value: number; color: string }[] }) {
  const total = Math.max(parts.reduce((a, p) => a + p.value, 0), 1);
  return (
    <>
      <div className="flex h-6 overflow-hidden" style={{ border: '1px solid color-mix(in srgb, var(--border-color) 35%, transparent)' }}>
        {parts.map((p, i) => (
          <div
            key={i}
            className="transition-[width] duration-300"
            style={{ width: `${(p.value / total) * 100}%`, backgroundColor: p.color }}
            title={`${p.label}: ${p.value}`}
          />
        ))}
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
        {parts.map((p, i) => (
          <span key={i} className="inline-flex items-center gap-1.5">
            <i className="inline-block w-2 h-2" style={{ backgroundColor: p.color }} />
            {p.label}
            <b style={{ fontFamily: MONO, color: 'var(--text-primary)' }}>{p.value.toLocaleString('ru-RU')}</b>
          </span>
        ))}
      </div>
    </>
  );
}

/** Пусто и ошибка — разные вещи, поэтому разные тексты. */
export function Empty({ text }: { text: string }) {
  return <p className="py-6 text-center text-sm" style={{ color: 'var(--text-muted)' }}>{text}</p>;
}
export function Failed({ text = 'Не удалось загрузить' }: { text?: string }) {
  return <p className="py-6 text-center text-sm" style={{ color: 'var(--danger)' }}>{text}</p>;
}

/* ── Навигация разделов ───────────────────────────────────────────────── */

/** Разделы страницы. Не «ещё один фильтр»: они не сужают данные, а меняют
 *  вопрос, на который страница отвечает. Поэтому их место — под заголовком,
 *  отдельно от контролов периода. */
export function SectionTabs<T extends string>({ tabs, value, onChange }: {
  tabs: { key: T; label: string }[];
  value: T;
  onChange: (k: T) => void;
}) {
  return (
    <nav
      role="tablist"
      className="flex gap-1 overflow-x-auto"
      style={{ borderBottom: '1px solid color-mix(in srgb, var(--border-color) 35%, transparent)', scrollbarWidth: 'none' }}
    >
      {tabs.map(t => {
        const on = t.key === value;
        return (
          <button
            key={t.key}
            role="tab"
            aria-selected={on}
            onClick={() => onChange(t.key)}
            className="whitespace-nowrap px-3.5 pt-2 pb-2.5 text-[13px] transition-colors"
            style={{
              color: on ? 'var(--text-primary)' : 'var(--text-secondary)',
              fontWeight: on ? 600 : 400,
              borderBottom: `2px solid ${on ? 'var(--accent)' : 'transparent'}`,
              marginBottom: -1,
            }}
          >
            {t.label}
          </button>
        );
      })}
    </nav>
  );
}

/** Строка таблицы кликается целиком — отдельной кнопки «открыть» не нужно. */
export function DataTable({ head, children, minWidth = 720 }: {
  head: { label: string; align?: 'left' | 'right'; hide?: 'md' | 'lg' }[];
  children: React.ReactNode;
  minWidth?: number;
}) {
  return (
    <div className="overflow-x-auto -mx-2">
      <table className="w-full" style={{ minWidth }}>
        <thead>
          <tr>
            {head.map((h, i) => (
              <th
                key={i}
                className={cn(
                  'px-2 pb-2 text-[11px] uppercase font-medium',
                  h.align === 'right' ? 'text-right' : 'text-left',
                  h.hide === 'md' && 'hidden md:table-cell',
                  h.hide === 'lg' && 'hidden lg:table-cell',
                )}
                style={{ fontFamily: MONO, letterSpacing: '.08em', color: 'var(--text-muted)' }}
              >
                {h.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}

/** Состояние данных в строке: бейдж, а не крашеная строка. */
export function Badge({ text, tone = 'muted' }: {
  text: string;
  tone?: 'accent' | 'good' | 'info' | 'muted';
}) {
  const color = {
    accent: 'var(--accent)', good: 'var(--success)',
    info: 'var(--info)', muted: 'var(--text-muted)',
  }[tone];
  return (
    <span
      className="inline-block px-2 py-0.5 rounded-full text-[11px] font-semibold whitespace-nowrap"
      style={{ backgroundColor: `color-mix(in srgb, ${color} 16%, transparent)`, color }}
    >
      {text}
    </span>
  );
}
