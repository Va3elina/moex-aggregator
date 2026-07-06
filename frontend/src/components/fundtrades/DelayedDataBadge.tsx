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
 */
export default function DelayedDataBadge() {
  const tier = useCurrentTier();
  const delayed = tier === 'guest' || tier === 'free';
  if (!delayed) return null;

  return (
    <div
      role="note"
      className="flex flex-wrap items-center gap-x-2 gap-y-1 rounded-lg px-3 py-2 mb-3"
      style={{
        fontSize: 'var(--fs-sm, 0.875rem)',
        background: 'color-mix(in oklab, var(--accent) 10%, transparent)',
        border: '1px solid color-mix(in oklab, var(--accent) 28%, transparent)',
      }}
    >
      <span aria-hidden="true">⏳</span>
      <span style={{ fontWeight: 600 }}>Данные с задержкой</span>
      <span style={{ opacity: 0.72 }}>
        — вы видите предыдущую выборку фондов, а не самую свежую.
      </span>
      <a
        href="/pricing"
        style={{ fontWeight: 600, color: 'var(--accent)', textDecoration: 'underline' }}
      >
        Открыть свежий срез →
      </a>
    </div>
  );
}
