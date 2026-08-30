/**
 * LandingPricing — тарифная сетка в подвале лендинга (для гостей).
 *
 * Раньше внизу главной был только текстовый CTA-блок «Начни разбираться в
 * рынке» с двумя кнопками. Решение владельца 2026-08-30: гость должен видеть
 * тарифы прямо на главной, не переходя на /pricing. Оранжевый блок-легенда и
 * тарифная сетка — раздельные блоки (класть карточки внутрь accent-фона
 * пробовали и откатили). Карточки — TierPlanCard, та же вёрстка, что на
 * странице тарифов, иначе они разъезжаются.
 *
 * Кнопка карточки запускает обычный цикл оформления, а не ведёт на /pricing:
 * лендинг виден только гостям (App.tsx редиректит залогиненных с «/»), поэтому
 * клик — это всегда первый шаг цикла «нет аккаунта → регистрация»: тот же
 * signupUrlForIntent, что зовёт handleCheckout/handleTrialStart на странице
 * тарифов для гостя. Выбранный тариф едет в ?next= (см. utils/checkoutIntent),
 * и после регистрации /pricing сразу открывает окно согласий на нём.
 *
 * Цены тянем из публичного GET /api/billing/plans (auth не нужен, см.
 * api/routers/billing.py). Если запрос упал, секция схлопывается до
 * заголовка-легенды: пустые карточки без цен хуже, чем их отсутствие.
 */
import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import TierPlanCard, { TIER_META, type PlanVariant } from '../pricing/TierPlanCard';
import { apiFetch } from '../../services/api';
import { signupUrlForIntent, type CheckoutIntent } from '../../utils/checkoutIntent';

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

export default function LandingPricing() {
  const navigate = useNavigate();
  const [data, setData] = useState<PlansResponse | null>(null);
  // Годовой по умолчанию — как на /pricing (выгоднее, и цена сопоставима «в лоб»).
  const [period, setPeriod] = useState<'monthly' | 'yearly'>('yearly');

  useEffect(() => {
    let alive = true;
    apiFetch('/api/billing/plans')
      .then((r) => (r.ok ? r.json() : null))
      .then((json) => { if (alive && json) setData(json); })
      .catch(() => { /* без цен секция остаётся заголовком-легендой */ });
    return () => { alive = false; };
  }, []);

  // Free-карточку фильтруем так же, как /pricing: бесплатный доступ продаёт
  // текст легенды выше, а карточки — платные тарифы.
  const paid = (data?.tiers || []).filter((t) => t.tier !== 'free');

  // Первый шаг цикла оформления для гостя — регистрация с возвратом на
  // страницу тарифов, где уже авторизованным завершается оплата/триал
  // (зеркало guest-ветки handleCheckout/handleTrialStart в PricingPage).
  // Выбранный тариф едет вместе с ним: на /pricing он сразу превращается в
  // окно согласий, а не в просьбу нажать ту же кнопку второй раз.
  const startSignup = (intent: CheckoutIntent | null) => navigate(signupUrlForIntent(intent));

  return (
    <>
      {/* Легенда на accent-фоне — бывший финальный CTA лендинга. */}
      <section
        className="editorial-frame text-center"
        style={{
          backgroundColor: 'var(--accent)',
          borderColor: 'var(--text-primary)',
          padding: 'clamp(32px, 5vw, 64px) clamp(20px, 4vw, 60px)',
          marginTop: 'clamp(24px, 4vw, 48px)',
        }}
      >
        <p
          className="mb-5 uppercase"
          style={{
            color: 'var(--text-inverse)',
            fontSize: 'var(--fs-2xs)',
            letterSpacing: '0.32em',
            fontWeight: 700,
            opacity: 0.8,
          }}
        >
          FRAME · ОТКРЫТЫЙ ДОСТУП
        </p>
        <h2
          className="font-bold mb-4 mx-auto"
          style={{
            color: 'var(--text-inverse)',
            fontSize: 'clamp(26px, 4.5vw + 0.5rem, 64px)',
            letterSpacing: '-0.035em',
            lineHeight: 1.0,
            maxWidth: '14ch',
          }}
        >
          Начни разбираться<br/>
          <span style={{ fontStyle: 'italic' }}>в рынке</span>
        </h2>
        <p
          className="max-w-xl mx-auto"
          style={{
            color: 'var(--text-inverse)',
            opacity: 0.85,
            fontSize: 'var(--fs-sm)',
            lineHeight: 1.55,
          }}
        >
          Бесплатный доступ к базовым индикаторам: без оплаты и без карты.
          Тарифы Basic и Pro пригодятся, когда понадобятся данные в реальном времени, расширенная история и продвинутые режимы.
        </p>
      </section>

      {paid.length > 0 && (
        <section style={{ marginTop: 'clamp(28px, 4vw, 48px)' }}>
          {/* Переключатель Месяц / Год — копия блока с /pricing (компактный,
              размеры в px: root font-size 20px делает rem-классы в 1.25x). */}
          <div className="flex justify-center mb-6">
            <div
              className="inline-flex rounded-full border"
              style={{ borderColor: 'var(--border-color)', backgroundColor: 'var(--bg-secondary)', padding: 2 }}
            >
              <button
                onClick={() => setPeriod('monthly')}
                className="rounded-full font-semibold transition-colors"
                style={{
                  padding: '4px 12px',
                  fontSize: 13,
                  lineHeight: 1.3,
                  backgroundColor: period === 'monthly' ? 'var(--bg-tertiary)' : 'transparent',
                  color: period === 'monthly' ? 'var(--text-primary)' : 'var(--text-secondary)',
                }}
              >
                Месяц
              </button>
              <button
                onClick={() => setPeriod('yearly')}
                className="rounded-full font-semibold transition-colors inline-flex items-center"
                style={{
                  padding: '4px 12px',
                  fontSize: 13,
                  lineHeight: 1.3,
                  gap: 5,
                  backgroundColor: period === 'yearly' ? 'var(--bg-tertiary)' : 'transparent',
                  color: period === 'yearly' ? 'var(--text-primary)' : 'var(--text-secondary)',
                }}
              >
                Год
                <span
                  className="rounded-full font-bold"
                  style={{ background: 'var(--accent)', color: 'var(--bg-primary)', padding: '1px 6px', fontSize: 11 }}
                >
                  −20%
                </span>
              </button>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-5 max-w-3xl mx-auto">
            {paid.map((tier) => {
              const meta = TIER_META[tier.tier] || TIER_META.free;
              const variant = period === 'yearly' ? tier.yearly : tier.monthly;
              const trialDays = data?.trial_enabled ? data.trial_days?.[tier.tier] : null;
              return (
                <TierPlanCard
                  key={tier.tier}
                  tier={tier.tier}
                  title={tier.title}
                  variant={variant}
                  period={period}
                  monthlyAmount={tier.monthly?.amount ?? null}
                  trialDays={trialDays}
                >
                  {/* Текст кнопки — как у гостя на /pricing: триал начинается
                      с регистрации, покупка — тоже. */}
                  <button
                    onClick={() => startSignup(
                      trialDays
                        ? { kind: 'trial', tier: tier.tier, period }
                        : variant
                          ? { kind: 'plan', planId: variant.plan_id }
                          : null,
                    )}
                    className="w-full py-3 rounded-xl text-sm font-semibold transition-colors"
                    style={{ backgroundColor: meta.color, color: 'var(--bg-primary)' }}
                  >
                    {trialDays ? 'Зарегистрироваться и попробовать' : 'Оформить'}
                  </button>
                </TierPlanCard>
              );
            })}
          </div>
        </section>
      )}
    </>
  );
}
