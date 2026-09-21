/**
 * Иконки и ключи разделов для админки. Иконки те же, что в терминале, —
 * раздел должен узнаваться глазом в любом месте статистики.
 */
import {
  Layers, Grid3x3, Wallet, ArrowLeftRight, Activity, CalendarDays, Waves, Scale,
  LayoutDashboard, Eye, type LucideIcon,
} from 'lucide-react';

export const INDICATOR_ICONS: Record<string, { Icon: LucideIcon; color: string }> = {
  '/oi': { Icon: Layers, color: 'var(--info)' },
  '/heatmap': { Icon: Grid3x3, color: 'var(--danger)' },
  '/funds-money': { Icon: Wallet, color: 'var(--success)' },
  '/fund-trades': { Icon: ArrowLeftRight, color: 'var(--accent)' },
  '/strength': { Icon: Activity, color: 'var(--info)' },
  '/seasonality': { Icon: CalendarDays, color: 'var(--success)' },
  '/cbr-flows': { Icon: Waves, color: 'var(--info)' },
  '/buffett': { Icon: Scale, color: 'var(--warning)' },
  '/sandbox': { Icon: LayoutDashboard, color: 'var(--accent)' },
};

export function IndicatorGlyph({ path, size = 30 }: { path: string; size?: number }) {
  const ic = INDICATOR_ICONS[path];
  const color = ic?.color || 'var(--accent)';
  const Icon = ic?.Icon || Eye;
  return (
    <span
      className="flex items-center justify-center rounded-md shrink-0"
      style={{
        width: size, height: size,
        backgroundColor: `color-mix(in srgb, ${color} 16%, transparent)`,
        color,
      }}
    >
      <Icon size={Math.round(size * 0.53)} />
    </span>
  );
}

/** '/funds-money' ↔ 'funds-money' — ключ раздела в адресе страницы. */
export const indicatorKey = (path: string) => path.replace(/^\//, '');
export const indicatorPath = (key: string) => `/${key}`;
