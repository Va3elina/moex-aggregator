import { Clock } from 'lucide-react';
import { useTierAccess } from '../contexts/TierFeaturesContext';

/**
 * DelayedDataBadge — постоянное честное напоминание, что данные индикатора
 * показываются С ЗАДЕРЖКОЙ, а realtime открывается по подписке.
 * Модель delayed-data freemium: видно всё, но не самое свежее — и об этом прямо
 * сообщаем + CTA на тарифы.
 *
 * Показ РЕШАЕТ МАТРИЦА, а не тир (правка 2026-08). Раньше бейдж был захардкожен
 * на `tier === 'guest' || 'free'`: когда задержку у cbr_flows сняли, плашка
 * осталась бы висеть и врать. Теперь читаем реальный лимит текущего тира из
 * features.py через useTierAccess(indicator):
 *   data_delay_hours > 0  — open_interest, ...
 *   snapshot_delay   > 0  — fund_trades (задержка меряется снапшотами)
 * Снимают гейт в features.py → бейдж исчезает сам, без правок фронта.
 *
 * Пока матрица/роль не догрузились (isLoading) — бейдж скрыт: лучше не показать
 * правдивую плашку на миг, чем мигнуть ложной платному юзеру.
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
  indicator = 'fund_trades',
  variant = 'banner',
  message = '— вы видите предыдущую выборку фондов, а не самую свежую.',
  compactMessage = '— это не самая свежая выборка.',
  cta = 'Открыть свежий срез →',
  compactCta = 'Свежий срез →',
}: {
  /** Ключ индикатора в INDICATOR_FEATURES — по нему берётся реальная задержка. */
  indicator?: string;
  variant?: 'banner' | 'compact';
  message?: string;
  compactMessage?: string;
  cta?: string;
  compactCta?: string;
}) {
  const { limits, isLoading } = useTierAccess(indicator);
  const delayed =
    !isLoading &&
    ((limits.data_delay_hours ?? 0) > 0 || (limits.snapshot_delay ?? 0) > 0);
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
