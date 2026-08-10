/**
 * OiScreenerLocked — тизер заблокированной ленты «Скринер сигналов» для
 * гостя и Free (решение владельца 2026-08-09: скринер открывается с Basic).
 *
 * ⚠️ Пейволл: НАСТОЯЩИХ цифр тут нет и быть не может. Скелетон ниже —
 * захардкоженные плейсхолдер-ряды (только форма ленты, размытая), а бэкенд на
 * locked-тир отдаёт маркер `locked:true` с ПУСТЫМ rows (см. GET
 * /api/oi/screener). Снимать блюр в инспекторе бессмысленно — под ним фейк.
 * Тот же приём, что у LockedSnapshotTeaser в «Сделках фондов».
 *
 * Одна заглушка на ВСЕ три поверхности: сайт (OiScreenerTable), мобилка (у /oi
 * своей мобильной вкладки скринера нет — открывается тот же компонент) и
 * embed/песочница (EmbedScreener) через `compact`.
 *
 * Показываем строго по маркеру бэкенда, а не по матрице тарифов: пока матрица
 * грузится, фронт вообще ничего не решает — блюр не мигнёт у платника
 * (принцип «лучше на миг не запереть, чем запереть платника»).
 */
import { Lock } from 'lucide-react';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { useTierAccess } from '../../contexts/TierFeaturesContext';

// Фейковая лента (НЕ данные): доля имени, положение головы кометы на оси
// −100…+100, ширина пилюли «Силы». Числа подобраны на глаз, чтобы форма была
// правдоподобной; ни одно из них ниоткуда не приходит.
const FAKE_ROWS: { name: number; pos: number; force: number; sharp: boolean }[] = [
  { name: 74, pos: 78, force: 54, sharp: true },
  { name: 52, pos: 22, force: 50, sharp: true },
  { name: 86, pos: 61, force: 46, sharp: true },
  { name: 60, pos: 40, force: 44, sharp: false },
  { name: 68, pos: 88, force: 48, sharp: false },
  { name: 44, pos: 33, force: 42, sharp: false },
  { name: 80, pos: 55, force: 46, sharp: false },
];

export default function OiScreenerLocked({ compact = false }: { compact?: boolean }) {
  const { showUpgrade } = useUpgradePrompt();
  const access = useTierAccess('oi_screener');
  // requiredTierFor вернёт null, пока матрица не приехала — тогда просто Basic
  // (бэкенд в маркере присылает то же самое).
  const tier = access.requiredTierFor({ flag: 'open' }) ?? 'basic';

  const open = () =>
    showUpgrade({ tier, featureName: 'скринер сигналов', indicator: 'oi_screener' });

  const rowH = compact ? 30 : 44;
  const pad = compact ? 10 : 16;

  return (
    <div style={{ position: 'relative', borderRadius: 12, overflow: 'hidden', border: '1px solid var(--border-color)' }}>
      {/* Скелетон — размытый, серый, некликабельный (язык замка как в TierLock) */}
      <div
        aria-hidden="true"
        style={{ filter: 'blur(6px) grayscale(0.8)', opacity: 0.5, pointerEvents: 'none', userSelect: 'none', padding: pad }}
      >
        {FAKE_ROWS.map((r, i) => (
          <div
            key={i}
            style={{
              display: 'grid',
              gridTemplateColumns: compact ? '90px 1fr 44px' : '190px 1fr 90px 120px',
              gap: compact ? 8 : 16,
              alignItems: 'center',
              height: rowH,
              borderTop: i ? '1px solid var(--border-color)' : 'none',
            }}
          >
            {/* Актив: кружок иконки + полоска имени */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
              <div style={{ width: compact ? 18 : 28, height: compact ? 18 : 28, borderRadius: '50%', background: 'var(--bg-secondary)', flexShrink: 0 }} />
              <div style={{ height: 10, width: `${r.name}%`, background: 'var(--bg-secondary)', borderRadius: 4 }} />
            </div>
            {/* Позиция: дорожка кометы с «головой» */}
            <div style={{ position: 'relative', height: 8, background: 'var(--bg-secondary)', borderRadius: 999 }}>
              <div
                style={{
                  position: 'absolute', top: -3, left: `calc(${r.pos}% - 7px)`,
                  width: 14, height: 14, borderRadius: '50%',
                  background: r.sharp ? 'var(--accent)' : 'var(--text-muted)',
                }}
              />
            </div>
            {/* Сила: пилюля-бейдж */}
            <div
              style={{
                height: compact ? 16 : 22, width: `${r.force}px`, borderRadius: 999,
                border: '2px solid var(--text-muted)',
                background: r.sharp ? 'var(--accent)' : 'transparent',
                justifySelf: compact ? 'end' : 'center',
              }}
            />
            {/* Сигнал (в компакте колонки нет — не влезает) */}
            {!compact && (
              <div style={{ height: 10, width: '70%', background: 'var(--bg-secondary)', borderRadius: 4, justifySelf: 'center' }} />
            )}
          </div>
        ))}
      </div>

      {/* Оверлей-замок + CTA */}
      <div
        style={{
          position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column',
          alignItems: 'center', justifyContent: 'center', gap: compact ? 8 : 12,
          textAlign: 'center', padding: compact ? 14 : 24,
          background: 'color-mix(in oklab, var(--bg-secondary) 42%, transparent)',
        }}
      >
        <div
          style={{
            width: compact ? 38 : 52, height: compact ? 38 : 52, borderRadius: '50%',
            background: 'var(--accent)', display: 'grid', placeItems: 'center',
            color: 'var(--text-inverse)', boxShadow: '0 6px 22px rgba(255,92,43,.35)',
          }}
        >
          <Lock size={compact ? 17 : 22} strokeWidth={2.3} />
        </div>
        <div style={{ fontWeight: 800, fontSize: compact ? 'var(--fs-sm, .9rem)' : 'var(--fs-md, 1rem)', color: 'var(--text-primary)' }}>
          Скринер сигналов — по подписке
        </div>
        <div style={{ fontSize: compact ? 'var(--fs-xs, .8rem)' : 'var(--fs-sm, .9rem)', color: 'var(--text-secondary)', maxWidth: 360, lineHeight: 1.5 }}>
          Где сегодня резко сдвинулись позиции физлиц и юрлиц по всему рынку разом —
          доступно на тарифе {tier === 'pro' ? 'Pro' : 'Basic или Pro'}.
        </div>
        <button
          onClick={open}
          className="editorial-press"
          style={{
            background: 'var(--accent)', color: 'var(--text-inverse)', fontWeight: 800,
            fontSize: compact ? 'var(--fs-xs, .8rem)' : 'var(--fs-sm, .9rem)',
            border: '2px solid var(--text-primary)', borderRadius: 999,
            padding: compact ? '7px 14px' : '10px 20px', cursor: 'pointer',
            boxShadow: '3px 3px 0 var(--text-primary)',
          }}
        >
          Открыть скринер
        </button>
      </div>
    </div>
  );
}
