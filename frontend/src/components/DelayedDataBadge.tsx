import { Clock } from 'lucide-react';
import { useCurrentTier } from '../contexts/TierFeaturesContext';

/**
 * DelayedDataBadge — постоянное честное напоминание для Free/гостя, что данные
 * индикатора показываются С ЗАДЕРЖКОЙ, а realtime открывается по подписке.
 * Модель delayed-data freemium: видно всё, но не самое свежее — и об этом прямо
 * сообщаем + CTA на тарифы.
 *
 * Платные тиры (Basic/Pro/admin) видят свежий срез → бейдж скрыт.
 * Тиры с задержкой должны совпадать с features.py INDICATOR_FEATURES:
 *   fund_trades   — snapshot_delay=1 (free/guest)
 *   open_interest — data_delay_hours=24 (free/guest)
 *   cbr_flows     — data_delay_hours=24 (free/guest)
 * Если гейт в features.py снимут — убрать и бейдж, иначе врём пользователю.
 *
 * variant='banner' (дефолт) — крупный баннер под шапкой страницы.
 * variant='compact' — маленькая контекстная плашка рядом с самими цифрами
 * (нужна там, где баннер съел бы высоту: мобильные full-bleed графики, табы).
 *
 * Тексты по умолчанию — «фондовые» (первый раздел, где обкатали модель);
 * остальные индикаторы передают свои через message/compactMessage.
 *
 * Значок — наш (lucide Clock в accent), НЕ эмодзи.
 */
export default function DelayedDataBadge({
  variant = 'banner',
  message = '— вы видите предыдущую выборку фондов, а не самую свежую.',
  compactMessage = '— это не самая свежая выборка.',
  cta = 'Открыть свежий срез →',
  compactCta = 'Свежий срез →',
}: {
  variant?: 'banner' | 'compact';
  message?: string;
  compactMessage?: string;
  cta?: string;
  compactCta?: string;
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
      <span style={{ opacity: 0.72 }}>{compact ? compactMessage : message}</span>
      <a
        href="/pricing"
        style={{ fontWeight: 600, color: 'var(--accent)', textDecoration: 'underline' }}
      >
        {compact ? compactCta : cta}
      </a>
    </div>
  );
}
