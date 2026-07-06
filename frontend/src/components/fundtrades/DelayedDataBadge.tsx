import { Clock } from 'lucide-react';
import { useCurrentTier } from '../../contexts/TierFeaturesContext';

/**
 * DelayedDataBadge — постоянное честное напоминание для Free/гостя, что данные
 * «Покупок фондов» показываются НА ОДИН СНАПШОТ ПОЗАДИ (свежая месячная выборка
 * «что фонды купили» открывается по подписке). Пилот модели delayed-data freemium:
 * видно всё, но не realtime — и об этом прямо сообщаем + CTA на тарифы.
 *
 * Платные тиры (Basic/Pro/admin) видят свежий срез → бейдж скрыт.
 * Задержанные тиры совпадают с snapshot_delay>0 в features.py
 * INDICATOR_FEATURES["fund_trades"] (free/guest).
 *
 * variant='banner' (дефолт) — крупный баннер под шапкой/BETA-боксом (виден на всех табах).
 * variant='compact' — маленькая контекстная плашка у данных конкретного таба
 * («Состав фондов» / «Потоки по компании»), чтобы напоминание было рядом с цифрами.
 *
 * Значок — наш (lucide Clock в accent), НЕ эмодзи.
 */
export default function DelayedDataBadge({
  variant = 'banner',
}: {
  variant?: 'banner' | 'compact';
}) {
  const tier = useCurrentTier();
  const delayed = tier === 'guest' || tier === 'free';
  if (!delayed) return null;

  const compact = variant === 'compact';

  return (
    <div
      role="note"
      className={`flex flex-wrap items-center gap-x-2 gap-y-1 rounded-lg ${compact ? 'px-2.5 py-1.5 mb-3' : 'px-3 py-2 mb-4'}`}
      style={{
        fontSize: compact ? 'var(--fs-xs, 0.8125rem)' : 'var(--fs-sm, 0.875rem)',
        background: 'color-mix(in oklab, var(--accent) 10%, transparent)',
        border: '1px solid color-mix(in oklab, var(--accent) 28%, transparent)',
      }}
    >
      <Clock
        size={compact ? 13 : 15}
        strokeWidth={2.2}
        style={{ color: 'var(--accent)', flexShrink: 0 }}
        aria-hidden="true"
      />
      <span style={{ fontWeight: 600 }}>Данные с задержкой</span>
      <span style={{ opacity: 0.72 }}>
        {compact
          ? '— это не самая свежая выборка.'
          : '— вы видите предыдущую выборку фондов, а не самую свежую.'}
      </span>
      <a
        href="/pricing"
        style={{ fontWeight: 600, color: 'var(--accent)', textDecoration: 'underline' }}
      >
        {compact ? 'Свежий срез →' : 'Открыть свежий срез →'}
      </a>
    </div>
  );
}
