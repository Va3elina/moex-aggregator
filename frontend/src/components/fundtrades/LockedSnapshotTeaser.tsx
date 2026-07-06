import { Lock } from 'lucide-react';
import { useUpgradePrompt } from '../tier/UpgradeModal';

/**
 * LockedSnapshotTeaser — тизер заблокированного (свежего) среза фонда для Free/гостя.
 *
 * ⚠️ Пейволл: НАСТОЯЩИХ цифр тут нет. Скелетон — фейковые плейсхолдер-ряды (только
 * форма, размытая), backend на locked-дату отдаёт маркер БЕЗ холдингов. Свежие
 * данные физически не покидают сервер.
 *
 * Клик → ваш стандартный upgrade-модал (`showUpgrade`), как на OI/сезонности/Баффете.
 * Визуал — токены приложения (--accent/--text-*/--bg-secondary/--fs-*), язык замка
 * как в TierLock (blur+grayscale под оверлеем).
 */
export default function LockedSnapshotTeaser({
  latestDate,
  requiredTier,
}: {
  latestDate?: string | null;
  requiredTier?: string;
}) {
  const { showUpgrade } = useUpgradePrompt();
  const tier = requiredTier === 'pro' ? 'pro' : 'basic';
  const open = () =>
    showUpgrade({ tier, featureName: 'свежий срез фондов', indicator: 'fund_trades' });

  const dateStr = latestDate
    ? new Date(latestDate + 'T00:00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' })
    : null;

  // Фейковые ряды-заглушки (НЕ данные) — только форма для блюра.
  const sections: Array<{ title: string; color: string; rows: number[] }> = [
    { title: 'Нарастили', color: 'var(--mood-green, #34d17a)', rows: [72, 50, 31] },
    { title: 'Сократили', color: 'var(--mood-red, #f2685f)', rows: [40, 18] },
  ];

  return (
    <div style={{ position: 'relative', borderRadius: 12, overflow: 'hidden', border: '1px solid var(--border-color)' }}>
      {/* Скелетон — размытый, серый, некликабельный (как TierLock, + blur) */}
      <div
        aria-hidden="true"
        style={{ filter: 'blur(6px) grayscale(0.8)', opacity: 0.5, pointerEvents: 'none', userSelect: 'none', padding: 16 }}
      >
        {sections.map((s) => (
          <div key={s.title} style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 'var(--fs-xs)', fontWeight: 700, marginBottom: 9, color: 'var(--text-secondary)' }}>{s.title}</div>
            {s.rows.map((w, i) => (
              <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '7px 0', borderTop: i ? '1px solid var(--border-color)' : 'none' }}>
                <div style={{ height: 12, width: `${34 + i * 14}%`, background: 'var(--bg-secondary)', borderRadius: 4 }} />
                <div style={{ marginLeft: 'auto', height: 8, width: `${w}%`, maxWidth: 120, background: s.color, borderRadius: 4 }} />
              </div>
            ))}
          </div>
        ))}
      </div>

      {/* Оверлей-замок + CTA */}
      <div
        style={{
          position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column',
          alignItems: 'center', justifyContent: 'center', gap: 12, textAlign: 'center', padding: 24,
          background: 'color-mix(in oklab, var(--bg-secondary) 42%, transparent)',
        }}
      >
        <div style={{ width: 52, height: 52, borderRadius: '50%', background: 'var(--accent)', display: 'grid', placeItems: 'center', color: 'var(--text-inverse)', boxShadow: '0 6px 22px rgba(255,92,43,.35)' }}>
          <Lock size={22} strokeWidth={2.3} />
        </div>
        <div style={{ fontWeight: 800, fontSize: 'var(--fs-md, 1rem)', color: 'var(--text-primary)' }}>
          Свежий срез — по подписке
        </div>
        <div style={{ fontSize: 'var(--fs-sm, .9rem)', color: 'var(--text-secondary)', maxWidth: 340, lineHeight: 1.5 }}>
          Что фонды купили и продали {dateStr ? `в срезе за ${dateStr}` : 'в свежей выборке'} — доступно на тарифе {tier === 'pro' ? 'Pro' : 'Basic'} и выше.
        </div>
        <button
          onClick={open}
          className="editorial-press"
          style={{ background: 'var(--accent)', color: 'var(--text-inverse)', fontWeight: 800, fontSize: 'var(--fs-sm, .9rem)', border: '2px solid var(--text-primary)', borderRadius: 999, padding: '10px 20px', cursor: 'pointer', boxShadow: '3px 3px 0 var(--text-primary)' }}
        >
          Открыть свежий срез
        </button>
      </div>
    </div>
  );
}
