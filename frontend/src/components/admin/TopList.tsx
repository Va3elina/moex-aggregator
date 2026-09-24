/**
 * Список «название — цифра» с полоской доли от лидера. Общий для всех
 * разделов /admin/stats: топы страниц, активов, источников, фондов и окон
 * терминала выглядят одинаково.
 */
import Card from '../Card';
import Skeleton from '../Skeleton';
import HelpTooltip from '../HelpTooltip';

export interface TopItem { label: string; note?: string; value: number; value2?: number }

interface TopListProps {
  title: string;
  items: TopItem[] | null;
  loading: boolean;
  emptyText: string;
  hint?: string;
  hintAlign?: 'left' | 'right';
  /** Подписи колонок: главная цифра и (опц.) серая вторая. */
  columns?: [string] | [string, string];
}

export default function TopList({ title, items, loading, emptyText, hint, hintAlign = 'left', columns }: TopListProps) {
  if (loading && !items) return <Skeleton height={240} rounded="lg" />;
  const max = items && items.length > 0 ? Math.max(...items.map(i => i.value)) || 1 : 1;
  return (
    <Card padding="md" className="md:p-5">
      <div className="flex items-center gap-2 mb-3">
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          {title}
        </span>
        {hint && <HelpTooltip icon="help" title={title} content={hint} size={13} align={hintAlign} />}
        {columns && items && items.length > 0 && (
          <span className="ml-auto text-xs" style={{ color: 'var(--text-muted)' }}>
            {columns[0]}{columns[1] ? ` · ${columns[1]}` : ''}
          </span>
        )}
      </div>
      {!items || items.length === 0 ? (
        <p className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
          {emptyText}
        </p>
      ) : (
        <div className="space-y-1.5" style={{ animation: 'fadeIn 0.3s ease-out' }}>
          {items.map((it, i) => (
            <div key={`${it.label}-${i}`} className="relative">
              <div
                className="absolute inset-y-0 left-0 rounded"
                style={{
                  width: `${(it.value / max) * 100}%`,
                  backgroundColor: 'color-mix(in srgb, var(--accent) 14%, transparent)',
                  transition: 'width 0.45s cubic-bezier(0.22, 1, 0.36, 1)',
                }}
              />
              <div className="relative flex items-center justify-between py-1.5 px-2 gap-2">
                <span className="text-sm truncate min-w-0" style={{ color: 'var(--text-primary)' }} title={it.note ? `${it.label} · ${it.note}` : it.label}>
                  {it.label || '—'}
                  {it.note && (
                    <span className="text-xs ml-1.5" style={{ color: 'var(--text-muted)' }}>{it.note}</span>
                  )}
                </span>
                <span
                  className="text-sm flex-shrink-0 whitespace-nowrap"
                  style={{ fontFamily: "'IBM Plex Mono', monospace", fontVariantNumeric: 'tabular-nums' }}
                >
                  <span className="font-semibold" style={{ color: 'var(--text-primary)' }}>
                    {it.value.toLocaleString('ru-RU')}
                  </span>
                  {it.value2 !== undefined && (
                    <span className="text-xs ml-1.5" style={{ color: 'var(--text-muted)' }}>
                      · {it.value2.toLocaleString('ru-RU')}
                    </span>
                  )}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
