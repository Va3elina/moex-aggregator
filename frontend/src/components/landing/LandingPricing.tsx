/**
 * LandingPricing — тарифная сетка в подвале лендинга (для гостей).
 *
 * Раньше внизу главной был только текстовый CTA-блок «Начни разбираться в
 * рынке» с двумя кнопками. Решение владельца 2026-08-30: гость должен видеть
 * тарифы прямо на главной, не переходя на /pricing. Заголовок-легенда
 * сохранён, под ним — три карточки (Free / Basic / Pro).
 *
 * Цены тянем из публичного GET /api/billing/plans (auth не нужен, см.
 * api/routers/billing.py). Показываем помесячную стоимость при оплате за год —
 * так же, как /pricing по умолчанию. Если запрос упал, карточки остаются, но
 * вместо цены стоит прочерк: сетка тарифов на лендинге не должна пропадать
 * из-за сетевой ошибки.
 */
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Check, Gift } from 'lucide-react';
import { apiFetch } from '../../services/api';

interface PlanVariant {
  plan_id: string;
  amount: number;
  duration_days: number;
  badge: string | null;
}

interface TierCard {
  tier: string;
  title: string;
  description: string;
  monthly: PlanVariant | null;
  yearly: PlanVariant | null;
}

interface PlansResponse {
  tiers: TierCard[];
  trial_enabled?: boolean;
  trial_days?: Record<string, number>;
}

// Короткие списки фич — выжимка из CARD_FEATURES на /pricing. На лендинге
// нужен не полный чек-лист, а 4 строки, по которым видна разница тарифов;
// детальная матрица живёт на /pricing (ссылка под сеткой).
const FEATURES: Record<'free' | 'basic' | 'pro', string[]> = {
  free: [
    'Все 9 индикаторов',
    'Все активы и таймфреймы',
    'Вся история',
    'Без оплаты и без карты',
  ],
  basic: [
    'Открытые позиции без задержки',
    'Юрлица и число трейдеров',
    'Скринер сигналов',
    '20 уведомлений в Telegram',
  ],
  pro: [
    'Всё из Basic',
    'Свой терминал с панелями индикаторов',
    'Индикаторы в терминале Т-Инвестиций',
    'Безлимит уведомлений в Telegram',
  ],
};

export default function LandingPricing() {
  const [data, setData] = useState<PlansResponse | null>(null);

  useEffect(() => {
    let alive = true;
    apiFetch('/api/billing/plans')
      .then((r) => (r.ok ? r.json() : null))
      .then((json) => { if (alive && json) setData(json); })
      .catch(() => { /* сетка тарифов рендерится и без цен */ });
    return () => { alive = false; };
  }, []);

  const tierOf = (t: string) => data?.tiers?.find((x) => x.tier === t) || null;

  /** Помесячная цена при оплате за год (как дефолт на /pricing). */
  const priceOf = (t: string): string | null => {
    const card = tierOf(t);
    const variant = card?.yearly || card?.monthly;
    if (!variant) return null;
    const perMonth = card?.yearly ? Math.round(variant.amount / 12) : variant.amount;
    return `${perMonth.toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽`;
  };

  const trialOf = (t: string): number | null => {
    if (!data?.trial_enabled) return null;
    return data.trial_days?.[t] ?? null;
  };

  return (
    <section
      className="editorial-frame"
      style={{
        backgroundColor: 'var(--accent)',
        borderColor: 'var(--text-primary)',
        padding: 'clamp(32px, 5vw, 64px) clamp(16px, 3vw, 48px)',
        marginTop: 'clamp(24px, 4vw, 48px)',
      }}
    >
      <div className="text-center">
        <p
          className="mb-4 uppercase"
          style={{
            color: 'var(--text-inverse)',
            fontSize: 'var(--fs-2xs)',
            letterSpacing: '0.32em',
            fontWeight: 700,
            opacity: 0.8,
          }}
        >
          FRAME · ТАРИФЫ
        </p>
        <h2
          className="font-bold mb-4 mx-auto"
          style={{
            color: 'var(--text-inverse)',
            fontSize: 'clamp(26px, 4vw + 0.5rem, 56px)',
            letterSpacing: '-0.035em',
            lineHeight: 1.0,
            maxWidth: '14ch',
          }}
        >
          Начни разбираться<br/>
          <span style={{ fontStyle: 'italic' }}>в рынке</span>
        </h2>
        <p
          className="mb-8 max-w-xl mx-auto"
          style={{
            color: 'var(--text-inverse)',
            opacity: 0.85,
            fontSize: 'var(--fs-sm)',
            lineHeight: 1.55,
          }}
        >
          Базовые индикаторы бесплатны: без оплаты и без карты. Basic и Pro нужны,
          когда понадобятся данные без задержки, продвинутые режимы и уведомления.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 md:gap-5 max-w-5xl mx-auto text-left">
        <TierBox
          title="Free"
          price="0 ₽"
          priceNote="навсегда"
          features={FEATURES.free}
          ctaLabel="Начать бесплатно"
          ctaTo="/login"
          primary
        />
        <TierBox
          title="Basic"
          price={priceOf('basic')}
          priceNote="в месяц при оплате за год"
          trialDays={trialOf('basic')}
          features={FEATURES.basic}
          ctaLabel="Выбрать Basic"
          ctaTo="/pricing"
        />
        <TierBox
          title="Pro"
          price={priceOf('pro')}
          priceNote="в месяц при оплате за год"
          trialDays={trialOf('pro')}
          features={FEATURES.pro}
          ctaLabel="Выбрать Pro"
          ctaTo="/pricing"
          primary
        />
      </div>

      <div className="text-center" style={{ marginTop: 'clamp(20px, 3vw, 32px)' }}>
        <Link
          to="/pricing"
          className="uppercase font-bold"
          style={{
            color: 'var(--text-inverse)',
            fontSize: 'var(--fs-xs)',
            letterSpacing: '0.16em',
            textDecoration: 'underline',
            textUnderlineOffset: 4,
          }}
        >
          Сравнить тарифы подробно →
        </Link>
      </div>
    </section>
  );
}

/** Карточка одного тарифа внутри accent-блока: светлый лист на оранжевом. */
function TierBox({
  title, price, priceNote, features, ctaLabel, ctaTo, trialDays, primary,
}: {
  title: string;
  price: string | null;
  priceNote: string;
  features: string[];
  ctaLabel: string;
  ctaTo: string;
  trialDays?: number | null;
  /** Кнопка на тёмном фоне (Free и Pro), иначе контурная. */
  primary?: boolean;
}) {
  return (
    <div
      className="flex flex-col"
      style={{
        backgroundColor: 'var(--bg-primary)',
        border: '1.5px solid var(--text-primary)',
        boxShadow: '4px 4px 0 var(--text-primary)',
        borderRadius: 12,
        padding: 'clamp(18px, 2vw, 26px)',
      }}
    >
      <h3
        className="font-bold uppercase"
        style={{
          color: 'var(--text-primary)',
          fontSize: 'var(--fs-sm)',
          letterSpacing: '0.2em',
          marginBottom: 10,
        }}
      >
        {title}
      </h3>

      <div style={{ marginBottom: 14 }}>
        <div
          className="font-bold leading-none"
          style={{ color: 'var(--text-primary)', fontSize: 'clamp(26px, 2.4vw, 34px)', whiteSpace: 'nowrap' }}
        >
          {price ?? '—'}
        </div>
        <div style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-2xs)', marginTop: 6 }}>
          {priceNote}
        </div>
      </div>

      {trialDays ? (
        <div
          className="inline-flex items-center font-bold"
          style={{
            gap: 6,
            alignSelf: 'flex-start',
            marginBottom: 12,
            padding: '4px 10px',
            borderRadius: 999,
            border: '1.5px solid var(--accent)',
            color: 'var(--accent)',
            fontSize: 'var(--fs-2xs)',
          }}
        >
          <Gift size={13} strokeWidth={2.4} />
          {trialDays} дней бесплатно
        </div>
      ) : null}

      <ul className="flex-1" style={{ marginBottom: 18, display: 'grid', gap: 8, alignContent: 'start' }}>
        {features.map((f) => (
          <li
            key={f}
            className="flex items-start"
            style={{ gap: 8, color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)', lineHeight: 1.45 }}
          >
            <Check size={14} strokeWidth={2.6} style={{ color: 'var(--accent)', flexShrink: 0, marginTop: 3 }} />
            <span>{f}</span>
          </li>
        ))}
      </ul>

      <Link
        to={ctaTo}
        className="editorial-press text-center font-bold uppercase"
        style={{
          padding: '12px 16px',
          borderRadius: 9,
          backgroundColor: primary ? 'var(--text-primary)' : 'transparent',
          color: primary ? 'var(--text-inverse)' : 'var(--text-primary)',
          border: '1.5px solid var(--text-primary)',
          fontSize: 'var(--fs-xs)',
          letterSpacing: '0.14em',
          textDecoration: 'none',
        }}
      >
        {ctaLabel}
      </Link>
    </div>
  );
}
