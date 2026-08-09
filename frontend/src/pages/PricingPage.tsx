/**
 * PricingPage — страница тарифов.
 *
 * Загружает /api/billing/plans → показывает 3 тарифа (Free / Basic / Pro).
 * Переключатель "Месяц/Год" внутри каждой платной карточки.
 * Клик "Купить" → POST /api/billing/checkout → редирект на confirmation_url.
 *
 * Provider выбирается на бэке (factory.py):
 *   - tbank → confirmation_url ведёт на https://pay.tbank.ru/...
 *   - stub  → confirmation_url ведёт на /billing/stub (для dev без T-Bank ключей)
 *
 * Premium tier удалён 2026-05-19. Премиум-юзеры мигрированы в pro,
 * Вадим (id 2/4) и Тория (id 3/11) — в admin (полный доступ).
 */
import React, { useEffect, useMemo, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import {
  Check, Zap, Crown, Sparkles, X, Gift, Heart,
  Grid3X3, BarChart3, Wallet, Activity, Scale,
  CalendarDays, Banknote, LayoutGrid, Settings,
  type LucideIcon,
} from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { useAnalytics } from '../contexts/AnalyticsContext';
import { apiFetch } from '../services/api';
import { trackEvent } from '../hooks/useYandexMetrica';
import { API_CSV_ENABLED } from '../config/features';

interface PlanVariant {
  plan_id: string;
  amount: number;
  duration_days: number;
  badge: string | null;
}

interface TierCard {
  tier: string;                  // 'free' / 'basic' / 'pro' / 'premium'
  title: string;
  description: string;
  monthly: PlanVariant | null;
  yearly: PlanVariant | null;
  is_current_default?: boolean;
}

interface PlansResponse {
  provider: 'stub' | 'tbank';
  currency: string;
  tiers: TierCard[];
  /** Только для provider=tbank — публичный terminalKey для SDK init (SpeedPay). */
  terminal_key?: string | null;
  /** Бесплатный пробный период включён (публичный флаг для гостевого CTA). */
  trial_enabled?: boolean;
  /** Длительность триала по tier'ам: {basic:14, pro:7}. */
  trial_days?: Record<string, number>;
}

// Визуал tier'ов: иконка + акцентный цвет.
// Editorial palette: все цвета через CSS-vars (theme-aware: light/dark).
// Pro выделен var(--accent) (pumpkin) — это «звезда» каталога, остальные —
// нейтральные оттенки с возрастающим контрастом снизу вверх.
const TIER_META: Record<string, { icon: React.ReactNode; color: string; accentBg: string }> = {
  free:    { icon: <Check size={24} />,    color: 'var(--text-muted)',     accentBg: 'color-mix(in srgb, var(--text-muted) 12%, transparent)' },
  basic:   { icon: <Zap size={24} />,      color: 'var(--text-secondary)', accentBg: 'color-mix(in srgb, var(--text-secondary) 10%, transparent)' },
  pro:     { icon: <Crown size={24} />,    color: 'var(--accent)',         accentBg: 'color-mix(in srgb, var(--accent) 14%, transparent)' },
  premium: { icon: <Sparkles size={24} />, color: 'var(--text-primary)',   accentBg: 'color-mix(in srgb, var(--text-primary) 12%, transparent)' },
};

// Зеркало api/billing/plans.py::TIER_LEVELS — для сравнения «выше/ниже».
// admin=99 — у админа полный доступ, тарифы скрываются.
const TIER_LEVELS: Record<string, number> = {
  guest: 0,
  free: 1,
  basic: 2,
  pro: 3,
  premium: 4,
  admin: 99,
};

// Тип ответа /api/billing/status — реальное состояние подписки user'а.
// В отличие от user.role (derived state), здесь актуальный plan_id и
// cancelled_at — для определения «продление same tier» vs «текущий план».
interface BillingStatus {
  tier: string;
  is_active: boolean;
  subscription_id: number | null;
  plan_id: string | null;
  cancelled_at: string | null;
  expires_at: string | null;
  trial_eligible?: boolean;   // можно ли предложить бесплатный пробный период
  // Персональный подарочный оффер (whitelist через env). Fallback для баннера:
  // если юзер закрыл FounderOfferBanner, оффер остаётся доступен здесь.
  founder_offer?: { tier: string; period: string; days: number; amount: number | null } | null;
}

export default function PricingPage() {
  const { user, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const { track } = useAnalytics();
  const [data, setData] = useState<PlansResponse | null>(null);
  const [period, setPeriod] = useState<'monthly' | 'yearly'>('yearly'); // годовой по умолчанию (выгоднее)
  const [loading, setLoading] = useState(true);
  const [checkoutLoading, setCheckoutLoading] = useState<string | null>(null);
  // recurrent — hardcoded true в body /api/billing/checkout (см. confirmCheckout).
  // Раньше был toggle в ConsentModal, но создавал избыточный UX (юзер мог снять
  // галку и получить разовый платёж). Сейчас подписка ВСЕГДА с авто-продлением —
  // стандарт SaaS, снижает churn. Отменить юзер может в /profile.
  const [error, setError] = useState<string | null>(null);
  // billing — реальное состояние подписки user'а (через /api/billing/status).
  // Нужно для блокировки кнопок: с active basic нельзя купить тот же basic
  // или ниже. Со cancelled подпиской разрешаем продление того же tier'а.
  // null = ещё грузится или гость.
  const [billing, setBilling] = useState<BillingStatus | null>(null);

  // === Модалка подтверждения согласий ===
  // ФЗ-152 + общая гигиена: перед платежом юзер должен явно подтвердить
  // принятие Соглашения + Политики конфиденциальности (всегда), а если
  // активна опция авто-продления — ещё и Договор о рекуррентных платежах.
  // pendingPlanId — какой plan был кликнут, ждёт подтверждения юзера.
  // null = модалка закрыта.
  const [pendingPlanId, setPendingPlanId] = useState<string | null>(null);
  // Единый consent на все 3 документа сразу (Соглашение + Privacy + Recurrent).
  // Раньше было 2 чекбокса — UX-избыточно для юзеров (один документ-один чекбокс
  // когда они тематически связаны = noise). Теперь юзер одним кликом
  // подтверждает информированное согласие со всем legal-stack'ом, ссылки
  // на каждый документ открываются отдельно в новой вкладке.
  const [agreementConsent, setAgreementConsent] = useState<boolean>(false);
  // Триал ждёт подтверждения согласия (как pendingPlanId, но для /trial/start).
  const [pendingTrial, setPendingTrial] = useState<{ tier: string; period: 'monthly' | 'yearly' } | null>(null);

  // Email теперь собирается и подтверждается ДО оплаты (сразу после регистрации/
  // OAuth или в профиле). В чекауте поля email больше нет — вместо этого
  // handleCheckout не пускает неподтверждённых (см. ниже), а бэкенд дублирует
  // гейт в billing/service.py (anti-bypass).

  // Загружаем план при mount
  useEffect(() => {
    fetch('/api/billing/plans')
      .then(r => r.json())
      .then((d: PlansResponse) => {
        setData(d);
        setLoading(false);
      })
      .catch(e => {
        setError('Не удалось загрузить тарифы');
        setLoading(false);
        console.error(e);
      });
  }, []);

  // Параллельно — реальный статус подписки user'а. Для гостя 401, billing=null.
  // Используется в render-time логике disabled-state кнопок (см. ниже).
  useEffect(() => {
    if (!isAuthenticated) {
      setBilling(null);
      return;
    }
    apiFetch('/api/billing/status')
      .then(r => (r.ok ? r.json() : null))
      .then((b: BillingStatus | null) => setBilling(b))
      .catch(() => setBilling(null));
  }, [isAuthenticated]);

  // Текущий tier пользователя (для подсветки "твоего" тарифа)
  const currentTier = useMemo(() => {
    if (!isAuthenticated || !user) return 'guest';
    const role = (user.role || 'free').toLowerCase();
    return role === 'user' ? 'free' : role;
  }, [user, isAuthenticated]);

  // Клик "Купить" — открываем модалку согласий (НЕ создаём checkout сразу).
  // Гость → редирект на логин с next=/pricing (модалка не нужна).
  const handleCheckout = (planId: string) => {
    if (!isAuthenticated) {
      navigate(`/login?next=/pricing`);
      return;
    }
    // Гейт: оплатить можно только с подтверждённым email. Неподтверждённых ведём
    // подтверждать — synthetic (Telegram/VK без email) сначала на /add-email,
    // остальных на /verify-email (ввод кода). Бэкенд дублирует проверку.
    if (user && !user.is_verified) {
      navigate(user.requires_email_setup ? '/add-email' : '/verify-email');
      return;
    }
    setError(null);
    setAgreementConsent(false);
    setPendingPlanId(planId);
  };

  // Закрыть модалку без покупки
  const closeConsent = () => {
    setPendingPlanId(null);
    setPendingTrial(null);
    setAgreementConsent(false);
  };

  // Старт триала: гость → регистрация; неподтверждённый email → верификация;
  // иначе открываем consent-модалку в trial-режиме (tier+period с карточки).
  const handleTrialStart = (tier: string, trialPeriod: 'monthly' | 'yearly') => {
    if (!isAuthenticated) {
      navigate('/login?next=/pricing');
      return;
    }
    if (user && !user.is_verified) {
      navigate(user.requires_email_setup ? '/add-email' : '/verify-email');
      return;
    }
    setError(null);
    setAgreementConsent(false);
    setPendingPlanId(null);
    setPendingTrial({ tier, period: trialPeriod });
  };

  // Подтверждение триала: создаём триал + привязку карты → редирект на T-Bank.
  const confirmTrial = async () => {
    if (!pendingTrial) return;
    const { tier, period: trialPeriod } = pendingTrial;
    setCheckoutLoading(`trial_${tier}`);
    setError(null);
    track('trial_start', { tier, period: trialPeriod });
    trackEvent('trial_start', { tier, period: trialPeriod });
    try {
      const resp = await apiFetch('/api/billing/trial/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tier, period: trialPeriod, consent: true }),
      });
      const body = await resp.json().catch(() => ({}));
      if (!resp.ok || !body.payment_url) {
        throw new Error(body.detail || body.error?.message || 'Не удалось начать пробный период');
      }
      window.location.href = body.payment_url; // → привязка карты (T-Bank AddCard)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Ошибка');
      setCheckoutLoading(null);
      setPendingTrial(null);
    }
  };

  // Подтверждение: создаём checkout-сессию на бэке и редиректим на pay.tbank.ru
  const confirmCheckout = async (rail: 'card' | 'sbp' = 'card') => {
    if (!pendingPlanId) return;
    const planId = pendingPlanId;
    setCheckoutLoading(planId);
    setError(null);
    track('checkout_start', { plan: planId, rail });
    trackEvent('checkout_start', { plan: planId, rail });
    try {
      const resp = await apiFetch('/api/billing/checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          plan_id: planId,
          return_url: `${window.location.origin}/billing/success`,
          recurrent: true,  // подписка ВСЕГДА с авто-продлением (toggle убран)
          rail,
        }),
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(errData.detail || errData.error?.message || 'Ошибка создания платежа');
      }
      const body = await resp.json();
      if (rail === 'sbp') {
        // СБП: переходим на страницу QR. qr_image — крупный data-URL, поэтому
        // передаём через navigation state (в query не положишь).
        navigate('/billing/sbp', {
          state: {
            payment_id: body.payment_id,
            qr_image: body.qr_image,
            qr_payload: body.qr_payload,
            plan_id: planId,
          },
        });
        return;
      }
      // Карта: full-page redirect на pay.tbank.ru (или /billing/stub).
      window.location.href = body.confirmation_url;
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Ошибка');
      setCheckoutLoading(null);
      setPendingPlanId(null);
    }
  };

  // Можно ли нажать "Подтвердить": единый consent (Соглашение + Privacy +
  // Договор о рекуррентах). Email-валидации тут больше нет — он подтверждён до оплаты.
  const consentReady = agreementConsent;

  if (loading) return (
    <div className="max-w-6xl mx-auto p-8 text-center text-theme-secondary">Загрузка тарифов...</div>
  );
  if (error && !data) return (
    <div className="max-w-6xl mx-auto p-8 text-center text-red-400">{error}</div>
  );
  if (!data) return null;

  return (
    <div className="max-w-6xl mx-auto px-4 md:px-6 py-6 md:py-8">
      {/* Заголовок без описательной подписи — оставляем чистый «Тарифы»,
          подробности по способам оплаты есть в блоке внизу. */}
      <div className="text-center mb-6 md:mb-8">
        <h1 className="text-3xl md:text-4xl font-bold text-theme-primary">Тарифы</h1>
      </div>

      {/* Плашка: тарифный план на пересмотре. Оплата действующих тарифов работает
          как обычно (карта / T-Pay / СБП); пробный период на время пересмотра снят. */}
      <div
        className="mb-6 md:mb-8 mx-auto max-w-3xl text-center"
        style={{
          background: 'color-mix(in srgb, var(--accent) 10%, transparent)',
          border: '1px solid color-mix(in srgb, var(--accent) 40%, transparent)',
          borderRadius: 12,
          padding: '14px 18px',
        }}
      >
        <p style={{ fontWeight: 700, fontSize: 'var(--fs-base, 1rem)', color: 'var(--text-primary)', margin: 0 }}>
          Мы пересматриваем тарифный план
        </p>
        <p style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm, 0.9rem)', margin: '4px 0 0', lineHeight: 1.5 }}>
          Действующие тарифы можно оформить — оплата картой, T&#8209;Pay и СБП.
        </p>
      </div>

      {/* Founder-оффер: персональный подарок (whitelist через env). Виден ТОЛЬКО
          юзеру, кому backend вернул founder_offer. Это fallback для глобального
          баннера: если юзер закрыл баннер, оффер не теряется — он здесь. Клик →
          тот же consent-модал (с раскрытием 30 дней/цены/даты) → /trial/start. */}
      {billing?.founder_offer && (
        <div
          className="mb-8 mx-auto max-w-3xl"
          style={{
            background: 'var(--bg-primary, #FBF7EF)',
            border: '2px solid var(--text-primary, #0A0A0A)',
            borderRadius: 14,
            boxShadow: '4px 4px 0 var(--text-primary, #0A0A0A)',
            padding: '18px 20px',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
            <span style={{
              display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
              width: 32, height: 32, borderRadius: 9, flexShrink: 0,
              background: 'var(--accent, #FF5C2B)',
              border: '2px solid var(--text-primary, #0A0A0A)',
              boxShadow: 'var(--shadow-hard-chip, 2px 2px 0 var(--text-primary, #0A0A0A))',
            }}>
              <Heart size={17} strokeWidth={2.4} color="#fff" fill="#fff" />
            </span>
            <span style={{ fontWeight: 800, fontSize: 'var(--fs-lg, 1.15rem)' }}>
              Спасибо, что были первым — дарим месяц Pro
            </span>
          </div>
          <p style={{ color: 'var(--text-secondary, #555)', fontSize: 'var(--fs-sm, 0.9rem)', lineHeight: 1.55, margin: '0 0 8px' }}>
            Бесплатно {billing.founder_offer.days} дней Pro. Для привязки карты спишется 1&nbsp;₽ и сразу
            вернётся; полное списание — только по окончании периода, если не отмените.
          </p>
          <p style={{ color: 'var(--text-secondary, #666)', fontSize: 'var(--fs-xs, 0.8rem)', lineHeight: 1.5, margin: '0 0 14px' }}>
            Будем рады услышать ваш опыт — пишите{' '}
            <a href="https://t.me/TorSasha" target="_blank" rel="noopener noreferrer"
               style={{ color: 'var(--accent, #FF5C2B)', fontWeight: 700, textDecoration: 'none' }}>
              @TorSasha
            </a>
          </p>
          <button
            type="button"
            onClick={() => handleTrialStart('pro', 'monthly')}
            className="editorial-press"
            style={{
              minHeight: 48, padding: '12px 24px', borderRadius: 9,
              border: '2px solid var(--text-primary, #0A0A0A)',
              background: 'var(--accent, #FF5C2B)', color: '#fff', fontWeight: 800,
              fontSize: 'var(--fs-base, 1rem)', cursor: 'pointer',
              boxShadow: 'var(--shadow-hard-chip, 2px 2px 0 var(--text-primary, #0A0A0A))',
            }}
          >
            Активировать месяц Pro
          </button>
        </div>
      )}


      {/* Баннер STUB-режима — показываем только если провайдер 'stub' */}
      {data.provider === 'stub' && (
        <div className="mb-6 mx-auto max-w-3xl rounded-xl border border-[color-mix(in_srgb,var(--warning)_40%,transparent)] bg-[color-mix(in_srgb,var(--warning)_10%,transparent)] px-4 py-3 text-sm text-[var(--warning)]">
          ⚙️ <strong>Тестовый режим:</strong> эквайринг ещё не подключён, платежи не спишутся.
        </div>
      )}

      {/* Переключатель Месяц / Год — увеличен, более заметный CTA-уровень */}
      <div className="flex justify-center mb-8">
        <div
          className="inline-flex rounded-2xl border p-1.5"
          style={{ borderColor: 'var(--border-color)', backgroundColor: 'var(--bg-secondary)' }}
        >
          <button
            onClick={() => setPeriod('monthly')}
            className="px-6 py-3 rounded-xl text-base font-semibold transition-colors"
            style={{
              backgroundColor: period === 'monthly' ? 'var(--bg-tertiary)' : 'transparent',
              color: period === 'monthly' ? 'var(--text-primary)' : 'var(--text-secondary)',
            }}
          >
            Месяц
          </button>
          <button
            onClick={() => setPeriod('yearly')}
            className="px-6 py-3 rounded-xl text-base font-semibold transition-colors inline-flex items-center gap-2"
            style={{
              backgroundColor: period === 'yearly' ? 'var(--bg-tertiary)' : 'transparent',
              color: period === 'yearly' ? 'var(--text-primary)' : 'var(--text-secondary)',
            }}
          >
            Год
            <span
              className="px-2 py-0.5 rounded-full text-xs font-bold"
              style={{ background: 'var(--accent)', color: 'var(--bg-primary)' }}
            >
              −20%
            </span>
          </button>
        </div>
      </div>

      {/* Карточки тарифов — 3 колонки на десктопе, в стек на мобилке.
          Раньше было grid-cols-4 для 4-х тарифов (Free/Basic/Pro/Premium),
          теперь Premium удалён → 3 равные колонки. */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 md:gap-5 max-w-5xl mx-auto">
        {data.tiers.map((tier) => {
          const meta = TIER_META[tier.tier] || TIER_META.free;
          const variant = tier.tier === 'free' ? null : (period === 'yearly' ? tier.yearly : tier.monthly);

          // === Расширенная логика доступности тарифа ===
          // Источник истины — billing.tier (актуальный server-side state),
          // fallback на currentTier из user.role. Сравниваем уровни через
          // TIER_LEVELS (зеркало api/billing/plans.py).
          const effectiveTier = billing?.is_active ? billing.tier : currentTier;
          const effectiveLevel = TIER_LEVELS[effectiveTier] ?? 0;
          const cardLevel = TIER_LEVELS[tier.tier] ?? 0;

          const isAdmin = effectiveTier === 'admin';
          const isCurrent = effectiveTier === tier.tier;
          // Тот же тариф + тот же plan_id (period) = «текущий план», нельзя купить
          // повторно. Pro yearly + пытается купить Pro yearly → blocked.
          // НО: cancelled подписка → разрешаем продление (восстановление авто-продл).
          const isSamePlan =
            isCurrent &&
            !!variant &&
            billing?.plan_id === variant.plan_id &&
            !billing?.cancelled_at;
          // Tier ниже текущего → blocked (downgrade недоступен пока активная).
          const isLowerTier = !isAdmin && billing?.is_active && cardLevel < effectiveLevel;
          // Tier выше текущего ИЛИ same-tier-other-period → можно покупать.
          // Тарифы скрыты для admin (у него и так full access).
          const isLocked = isSamePlan || isLowerTier || isAdmin;

          // Можно ли начать бесплатный пробный период с этой карточки:
          // фича включена, tier платный, нет активной подписки, и юзер eligible
          // (гость — да, ведём в регистрацию; залогиненный — по trial_eligible).
          const trialDays = data.trial_days?.[tier.tier];
          const canTrial =
            tier.tier !== 'free' &&
            data.trial_enabled === true &&
            !!trialDays &&
            !isLocked &&
            !billing?.is_active &&
            (!isAuthenticated || billing?.trial_eligible === true);

          return (
            <div
              key={tier.tier}
              className="relative rounded-2xl border p-5 flex flex-col"
              style={{
                borderColor: isCurrent ? meta.color : 'var(--border-color)',
                backgroundColor: 'var(--bg-secondary)',
                boxShadow: isCurrent ? `0 0 0 2px ${meta.color}40` : undefined,
              }}
            >
              {/* Badge */}
              {variant?.badge && (
                <div
                  className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 rounded-full text-xs font-semibold whitespace-nowrap"
                  style={{ backgroundColor: meta.color, color: 'var(--bg-primary)' }}
                >
                  {variant.badge}
                </div>
              )}
              {isCurrent && (
                <div
                  className="absolute top-3 right-3 px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider"
                  style={{ backgroundColor: meta.color, color: 'var(--bg-primary)' }}
                >
                  Текущий
                </div>
              )}

              {/* Иконка + заголовок.
                  tier.description с бэка не рендерим: «Базовый доступ к сайту» /
                  «Расширенный доступ на уровне Basic» ничего не добавляют к названию,
                  содержание тарифа раскрывает список фич ниже. */}
              <div className="mb-4">
                <div
                  className="w-12 h-12 rounded-xl flex items-center justify-center mb-3"
                  style={{ backgroundColor: meta.accentBg, color: meta.color }}
                >
                  {meta.icon}
                </div>
                <h3 className="text-xl font-bold text-theme-primary">{tier.title}</h3>
              </div>

              {/* Цена.
                  Годовой тариф показываем в пересчёте на месяц (крупно), а полную
                  периодичность списания и экономию — строками ниже. Так цены месяца
                  и года сравнимы «в лоб»; полная сумма списания раскрывается до
                  оплаты в consent-модалке и на форме T-Bank. */}
              <div className="mb-5">
                {variant ? (
                  <>
                    <div className="flex items-baseline gap-1.5 flex-wrap">
                      <span
                        className="font-bold text-theme-primary leading-none"
                        style={{
                          // Fluid scale: 1.5rem на узких → 2rem на широких карточках.
                          // clamp() предотвращает overflow когда длинная цена (19 990 ₽)
                          // не помещается в узкую карточку на грид 4-в-ряд.
                          fontSize: 'clamp(1.5rem, 2.2vw, 2rem)',
                          whiteSpace: 'nowrap',
                        }}
                      >
                        {(period === 'yearly'
                          ? Math.round(variant.amount / 12)
                          : variant.amount
                        ).toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽
                      </span>
                      <span className="text-sm text-theme-secondary leading-none">
                        /мес
                      </span>
                    </div>
                    {period === 'yearly' && (
                      <div className="text-xs text-theme-muted mt-1">
                        Оплата раз в год
                      </div>
                    )}
                    {period === 'yearly' && tier.monthly && tier.monthly.amount * 12 > variant.amount && (
                      <div className="text-xs mt-0.5" style={{ color: meta.color, fontWeight: 600 }}>
                        Вы экономите {(tier.monthly.amount * 12 - variant.amount).toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽ в год
                      </div>
                    )}
                  </>
                ) : (
                  <div
                    className="font-bold text-theme-primary leading-none"
                    style={{
                      fontSize: 'clamp(1.5rem, 2.2vw, 2rem)',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    Бесплатно
                  </div>
                )}
              </div>

              {/* Бесплатный пробный период — выделенный чип, главная «фишка» карточки */}
              {canTrial && (
                <div
                  className="inline-flex items-center gap-2 mb-4 -mt-1 px-3 py-2 rounded-xl font-extrabold leading-tight"
                  style={{
                    color: meta.color,
                    background: meta.accentBg,
                    border: `1.5px solid ${meta.color}`,
                    fontSize: 'clamp(0.95rem, 1.5vw, 1.1rem)',
                  }}
                >
                  <Gift size={20} strokeWidth={2.4} className="flex-shrink-0" />
                  <span>{trialDays} дней бесплатно</span>
                </div>
              )}

              {/* Features list — пока placeholder, реальные появятся потом */}
              <ul className="flex-1 space-y-2 mb-5 text-sm">
                {getFeaturesList(tier.tier).map((feat, i) => (
                  <li key={i} className="flex items-start gap-2 text-theme-secondary">
                    <Check size={16} className="mt-0.5 flex-shrink-0" style={{ color: meta.color }} />
                    <span>{feat}</span>
                  </li>
                ))}
              </ul>

              {/* Кнопка */}
              {tier.tier === 'free' ? (
                <button
                  disabled
                  className="w-full py-2.5 rounded-xl text-sm font-medium opacity-50 cursor-not-allowed border"
                  style={{ borderColor: 'var(--border-color)', color: 'var(--text-secondary)' }}
                >
                  {isCurrent ? 'Текущий тариф' : 'Доступен всем'}
                </button>
              ) : isLocked ? (
                <button
                  disabled
                  className="w-full py-2.5 rounded-xl text-sm font-medium opacity-50 cursor-not-allowed border"
                  style={{ borderColor: meta.color, color: meta.color }}
                  title={
                    isAdmin
                      ? 'У админа полный доступ ко всем функциям'
                      : isSamePlan
                      ? 'Этот план уже активен. Изменить — после окончания текущего периода.'
                      : `У вас активен более высокий тариф (${effectiveTier.toUpperCase()})`
                  }
                >
                  {isAdmin
                    ? 'Admin-доступ'
                    : isSamePlan
                    ? 'Текущий план'
                    : 'Меньший тариф'}
                </button>
              ) : canTrial ? (
                <button
                  onClick={() => handleTrialStart(tier.tier, period)}
                  disabled={checkoutLoading === `trial_${tier.tier}`}
                  className="w-full py-3 rounded-xl text-sm font-semibold transition-colors disabled:opacity-50"
                  style={{ backgroundColor: meta.color, color: 'var(--bg-primary)' }}
                >
                  {checkoutLoading === `trial_${tier.tier}`
                    ? 'Открываем...'
                    : isAuthenticated
                      ? `Попробовать ${trialDays} дней бесплатно`
                      : 'Зарегистрироваться и попробовать'}
                </button>
              ) : (
                <button
                  onClick={() => variant && handleCheckout(variant.plan_id)}
                  disabled={!variant || checkoutLoading === variant.plan_id}
                  className="w-full py-3 rounded-xl text-sm font-semibold transition-colors disabled:opacity-50"
                  style={{ backgroundColor: meta.color, color: 'var(--bg-primary)' }}
                >
                  {checkoutLoading === variant?.plan_id
                    ? 'Создаём...'
                    : (isCurrent && billing?.cancelled_at)
                      ? 'Продлить'
                      : (isCurrent && cardLevel === effectiveLevel)
                        ? 'Сменить период'
                        : (billing?.is_active && cardLevel > effectiveLevel)
                          ? `Перейти на ${tier.title}`
                          : 'Оформить'}
                </button>
              )}
            </div>
          );
        })}
      </div>

      {/* Ошибка checkout */}
      {error && (
        <div className="mt-6 mx-auto max-w-xl rounded-xl border border-[color-mix(in_srgb,var(--danger)_40%,transparent)] bg-[color-mix(in_srgb,var(--danger)_10%,transparent)] px-4 py-3 text-sm text-[var(--danger)] text-center">
          {error}
        </div>
      )}

      {/* Сравнительная таблица тарифов — детальная матрица по индикаторам */}
      <ComparisonMatrix />

      {/* Способы оплаты — обязательный блок по требованиям эквайринга Т-Банка
          (логотипы принимаемых ПС + логотип Банка-эквайера + URL на tbank.ru). */}
      <PaymentMethods />

      {/* Footer с FAQ */}
      <div className="mt-8 text-center text-sm text-theme-muted">
        <p>
          Есть вопросы? Напиши в{' '}
          <a href="https://t.me/TorSasha" target="_blank" rel="noreferrer" className="text-theme-primary hover:underline">
            Telegram
          </a>
          {' '}или на{' '}
          <a href="mailto:frameinfo@mail.ru" className="text-theme-primary hover:underline">
            frameinfo@mail.ru
          </a>
          . Подробнее об{' '}
          <a href="/refund" className="text-theme-primary hover:underline">
            условиях возврата
          </a>
          .
        </p>
      </div>

      {/* === Модалка подтверждения согласий ===
          Появляется при клике "Оформить картой". Юзер обязан подтвердить
          соглашение + (если выбрано) рекуррент перед редиректом на T-Bank.
          Это требование ФЗ-152 (явное согласие) + общая защита от
          случайных списаний. */}
      {pendingPlanId && (
        <ConsentModal
          agreementConsent={agreementConsent}
          onAgreementChange={setAgreementConsent}
          onConfirm={() => confirmCheckout('card')}
          onClose={closeConsent}
          isLoading={checkoutLoading === pendingPlanId}
          canConfirm={consentReady}
        />
      )}

      {/* Consent для триала: то же окно, но с раскрытием суммы/даты первого
          списания (ЗоЗПП ст.10) — информированное согласие на автосписание. */}
      {pendingTrial && (() => {
        const tcard = data.tiers.find((t) => t.tier === pendingTrial.tier);
        const v = pendingTrial.period === 'yearly' ? tcard?.yearly : tcard?.monthly;
        // Founder получает 30 дней (из founder_offer), остальные — публичные trial_days.
        const days = billing?.founder_offer
          ? billing.founder_offer.days
          : (data.trial_days?.[pendingTrial.tier] ?? 7);
        const chargeDate = new Date(Date.now() + days * 86400000)
          .toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' });
        return (
          <ConsentModal
            agreementConsent={agreementConsent}
            onAgreementChange={setAgreementConsent}
            onConfirm={confirmTrial}
            onClose={closeConsent}
            isLoading={checkoutLoading === `trial_${pendingTrial.tier}`}
            canConfirm={consentReady}
            trialInfo={{
              tierRu: pendingTrial.tier === 'pro' ? 'Pro' : 'Basic',
              days,
              amount: v?.amount ?? 0,
              periodRu: pendingTrial.period === 'yearly' ? 'год' : 'месяц',
              chargeDate,
            }}
          />
        );
      })()}
    </div>
  );
}

/**
 * ConsentModal — модальное окно подтверждения согласия перед оплатой.
 *
 * ОДИН consent-чекбокс покрывает 3 документа:
 *   • Пользовательское соглашение
 *   • Политика конфиденциальности
 *   • Договор о регулярных (рекуррентных) платежах
 *
 * Раньше было 2 чекбокса + toggle auto-renewal — UX-избыточно. Сейчас:
 * подписка ВСЕГДА с авто-продлением, юзер одним кликом даёт информированное
 * согласие со всем legal-stack'ом. Каждый документ открывается по ссылке
 * в новой вкладке для review перед галкой.
 *
 * Email-поля тут больше нет — email подтверждается ДО оплаты (handleCheckout
 * не пускает неподтверждённых). Сюда доходят только verified-юзеры.
 */
function ConsentModal({
  agreementConsent,
  onAgreementChange,
  onConfirm,
  onClose,
  isLoading,
  canConfirm,
  trialInfo,
}: {
  agreementConsent: boolean;
  onAgreementChange: (v: boolean) => void;
  onConfirm: () => void;
  onClose: () => void;
  isLoading: boolean;
  canConfirm: boolean;
  // Если задан — режим подтверждения бесплатного пробного периода (раскрытие
  // суммы/даты первого списания + явное согласие на автосписание).
  trialInfo?: { tierRu: string; days: number; amount: number; periodRu: string; chargeDate: string };
}) {
  const amountStr = trialInfo ? trialInfo.amount.toLocaleString('ru-RU') : '';
  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center px-4 py-6"
      style={{ background: 'rgba(0,0,0,0.65)' }}
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      aria-labelledby="consent-modal-title"
    >
      <div
        className="relative w-full max-w-md rounded-2xl border p-6 overflow-y-auto"
        style={{
          background: 'var(--bg-primary)',
          borderColor: 'var(--border-color)',
          boxShadow: '0 24px 64px rgba(0,0,0,0.4)',
          maxHeight: 'calc(100dvh - 48px)',  // мобилка: не вылезать за экран, кнопка не скрыта
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Close X — 44px touch-target (мобилка) */}
        <button
          onClick={onClose}
          aria-label="Закрыть"
          className="absolute top-2 right-2 w-11 h-11 rounded-full flex items-center justify-center transition-colors"
          style={{
            color: 'var(--text-secondary)',
            background: 'transparent',
          }}
          onMouseEnter={(e) => (e.currentTarget.style.background = 'var(--bg-tertiary)')}
          onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent')}
        >
          <X size={18} />
        </button>

        <h2
          id="consent-modal-title"
          style={{
            fontSize: 'var(--fs-xl, 22px)',
            fontWeight: 800,
            letterSpacing: '-0.02em',
            color: 'var(--text-primary)',
            marginBottom: '0.4rem',
            paddingRight: '2rem',
          }}
        >
          {trialInfo ? 'Бесплатный пробный период' : 'Подтверждение'}
        </h2>
        <p
          style={{
            fontSize: 'var(--fs-sm, 13px)',
            color: 'var(--text-secondary)',
            lineHeight: 1.5,
            marginBottom: trialInfo ? '0.75rem' : '1.25rem',
          }}
        >
          {trialInfo
            ? `Бесплатно ${trialInfo.days} дней, затем автоматическое списание. Подтвердите согласие:`
            : 'Перед оплатой подтвердите согласие со следующими условиями:'}
        </p>

        {trialInfo && (
          <div
            style={{
              background: 'var(--bg-tertiary)',
              border: '1px solid var(--border-color)',
              borderRadius: 10,
              padding: '12px 14px',
              marginBottom: '1rem',
              fontSize: 'var(--fs-sm, 13px)',
              color: 'var(--text-secondary)',
              lineHeight: 1.5,
            }}
          >
            Тариф <b>{trialInfo.tierRu}</b> — бесплатно <b>{trialInfo.days} дней</b>. Для привязки
            карты спишется <b>1&nbsp;₽</b> и сразу вернётся. Первое полное списание{' '}
            <b>{amountStr} ₽</b> за {trialInfo.periodRu} — <b>{trialInfo.chargeDate}</b>, и только
            если до этой даты не отмените. Отменить и отвязать карту можно в любой момент в профиле.
          </div>
        )}

        {/* Единый consent на 3 документа — Соглашение, Privacy, Recurrent.
            Раньше было 2 чекбокса + toggle, сейчас одна галка покрывает весь
            legal-stack. Юзер review'ит документы по ссылкам (открываются
            в новых вкладках), ставит галку = информированное согласие. */}
        <ConsentRow
          checked={agreementConsent}
          onChange={onAgreementChange}
          label={
            <>
              Я согласен с{' '}
              <Link
                to="/agreement"
                target="_blank"
                style={{ color: 'var(--accent)', textDecoration: 'underline' }}
              >
                Пользовательским соглашением
              </Link>
              ,{' '}
              <Link
                to="/privacy"
                target="_blank"
                style={{ color: 'var(--accent)', textDecoration: 'underline' }}
              >
                Политикой конфиденциальности
              </Link>
              {' '}и{' '}
              <Link
                to="/recurring"
                target="_blank"
                style={{ color: 'var(--accent)', textDecoration: 'underline' }}
              >
                Договором о рекуррентных платежах
              </Link>
              .{' '}
              <span style={{ color: 'var(--text-muted)' }}>
                Подписка автоматически продлевается, отменить можно в профиле.
              </span>
            </>
          }
        />


        {/* Buttons */}
        <div className="flex gap-3 mt-6">
          <button
            onClick={onClose}
            disabled={isLoading}
            className="flex-1 py-3 rounded-xl text-sm font-medium border transition-colors disabled:opacity-50"
            style={{
              borderColor: 'var(--border-color)',
              color: 'var(--text-secondary)',
              background: 'transparent',
            }}
          >
            Отменить
          </button>
          <button
            onClick={onConfirm}
            disabled={!canConfirm || isLoading}
            className="flex-1 py-3 rounded-xl text-sm font-bold transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            style={{
              background: 'var(--accent)',
              color: 'var(--bg-primary)',
            }}
          >
            {isLoading
              ? (trialInfo ? 'Открываем…' : 'Создаём…')
              : (trialInfo ? 'Начать бесплатно' : 'Оплатить')}
          </button>
        </div>

        {/* Кнопка «СБП — QR-код» убрана 2026-07-02: T-Bank GetQr(IMAGE) стабильно
            отдавал «Внутренняя ошибка системы» → юзер видел тупиковый экран.
            СБП остаётся доступен внутри хостед-формы T-Bank («Быстрая оплата»).
            Механизм QR-флоу (confirmCheckout('sbp') + /billing/sbp + бэк) сохранён —
            вернуть = снова передать onConfirmSbp в ConsentModal. */}
      </div>
    </div>
  );
}

/**
 * ConsentRow — одна строка с toggle-switch и подписью.
 * Toggle сделан на чистом CSS через label + scaled checkbox для accessibility.
 */
function ConsentRow({
  checked,
  onChange,
  label,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  label: React.ReactNode;
}) {
  return (
    <label
      className="flex items-start gap-3 cursor-pointer select-none mb-3 p-3 rounded-xl border transition-colors"
      style={{
        borderColor: checked ? 'var(--accent)' : 'var(--border-color)',
        background: checked ? 'rgba(255,92,43,0.06)' : 'transparent',
      }}
    >
      <span
        className="relative inline-flex flex-shrink-0 mt-0.5"
        style={{
          width: 36,
          height: 20,
          borderRadius: 999,
          background: checked ? 'var(--accent)' : 'var(--bg-tertiary)',
          border: `1.5px solid ${checked ? 'var(--accent)' : 'var(--border-color)'}`,
          transition: 'background 0.18s ease, border-color 0.18s ease',
        }}
      >
        <span
          style={{
            position: 'absolute',
            top: 1,
            left: checked ? 17 : 1,
            width: 14,
            height: 14,
            borderRadius: '50%',
            background: 'var(--bg-primary)',
            transition: 'left 0.18s ease',
          }}
        />
        <input
          type="checkbox"
          checked={checked}
          onChange={(e) => onChange(e.target.checked)}
          style={{
            position: 'absolute',
            opacity: 0,
            inset: 0,
            cursor: 'pointer',
          }}
        />
      </span>
      <span
        style={{
          fontSize: 'var(--fs-sm, 13px)',
          color: 'var(--text-primary)',
          lineHeight: 1.45,
        }}
      >
        {label}
      </span>
    </label>
  );
}

/**
 * ComparisonMatrix — детальная таблица сравнения тарифов по каждому индикатору.
 * Источник истины — api/billing/features.py. Здесь — hand-crafted, потому что
 * нужно человекочитаемое описание каждой строки, а не raw matrix.
 *
 * Mobile (< 640px): table расщепляется в 3 card-секции (по tier'у) через CSS.
 * Desktop: классическая горизонтальная матрица 3 столбца × N строк.
 */
function ComparisonMatrix() {
  // Структура: [section_title, icon, [row_label, free_val, basic_val, pro_val]]
  // Иконки взяты из methodology-страниц индикаторов — единый visual язык.
  const sections: Array<{
    title: string;
    icon: LucideIcon;
    rows: Array<[string, string, string, string]>;
  }> = [
    {
      title: 'Карта рынка',
      icon: Grid3X3,
      rows: [
        ['Доступные режимы', 'Индекс IMOEX', 'IMOEX + Все акции', 'IMOEX + Все акции'],
      ],
    },
    {
      title: 'Открытые позиции',
      icon: BarChart3,
      rows: [
        ['Активов', 'Все 65', 'Все 65', 'Все 65'],
        ['Категория и режим', 'Базовый вид', 'Все настройки', 'Все настройки'],
        ['Таймфреймы', 'Часовой, дневной', 'Часовой, дневной', '5 мин + часовой + дневной'],
        ['История', '5 лет', '10 лет', 'Вся'],
        ['Задержка данных', '24 часа', 'Real-time', 'Real-time'],
      ],
    },
    {
      title: 'Деньги в фондах',
      icon: Wallet,
      rows: [
        ['Фондов', 'Все', 'Все', 'Все'],
        ['Таймфреймы', 'Все', 'Все', 'Все'],
        ['История', '3 года', 'Вся', 'Вся'],
      ],
    },
    {
      title: 'Сила рынка',
      icon: Activity,
      // Индикатор полностью бесплатен с 2026-08 (features.py: strength) —
      // таблица обязана это отражать, иначе обещаем платное там, где не берём.
      rows: [
        ['Вселенные', 'IMOEX + 100 акций', 'IMOEX + 100 акций', 'IMOEX + 100 акций'],
        ['Долларовый режим', 'Да', 'Да', 'Да'],
        ['История', 'Вся', 'Вся', 'Вся'],
      ],
    },
    {
      title: 'Индикатор Баффетта',
      icon: Scale,
      rows: [
        ['Режимы', 'Кап / ВВП + Кап / M2', 'Кап / ВВП + Кап / M2', 'Кап / ВВП + Кап / M2'],
        ['История', 'Вся', 'Вся', 'Вся'],
        ['Кастомные диапазоны', 'Да', 'Да', 'Да'],
      ],
    },
    {
      title: 'Сезонность',
      icon: CalendarDays,
      rows: [
        ['Активов', '10 крупных акций', 'Все', 'Все'],
        ['Режимы', 'Только годовая', 'Гистограмма + годовая', 'Все + внутри дня'],
        ['История', '10 лет', 'Вся', 'Вся'],
      ],
    },
    {
      title: 'Поток капитала',
      icon: Banknote,
      // Задержку данных сняли 2026-08 (data_delay_hours=0 везде), история и так
      // была открыта. Платным остаётся только СОСТАВ участников и ручные фильтры.
      rows: [
        ['История', 'Вся', 'Вся', 'Вся'],
        ['Участники', 'Розничный срез', 'Все', 'Все'],
        ['Фильтры по категориям', '—', 'Да', 'Да'],
      ],
    },
    {
      title: 'Каталог фондов',
      icon: LayoutGrid,
      rows: [
        ['Доступ', 'Полный', 'Полный', 'Полный'],
      ],
    },
    {
      title: 'Общие фичи',
      icon: Settings,
      rows: [
        ['Водяной знак на экспорте', 'Да', '—', '—'],
        // KILL-SWITCH: CSV/API скрыты до запуска (config/features.ts)
        ...(API_CSV_ENABLED ? ([
          ['Экспорт CSV / Excel', '—', '—', 'Да'],
          ['API-доступ', '—', '—', 'Да'],
        ] as [string, string, string, string][]) : []),
      ],
    },
  ];

  // ── Стили cells ─────────────────────────────────────────────────
  const subRowLabelStyle: React.CSSProperties = {
    padding: '11px 12px 11px 48px',  // 48px indent — выравнивается под текст заголовка (12 + 28 icon + 8 gap)
    fontSize: 'var(--fs-sm)',
    color: 'var(--text-secondary)',
    borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)',
    verticalAlign: 'middle',
    lineHeight: 1.4,
  };

  const valueCellBase: React.CSSProperties = {
    padding: '11px 12px',
    fontSize: 'var(--fs-sm)',
    textAlign: 'center',
    borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)',
    verticalAlign: 'middle',
    lineHeight: 1.4,
  };

  /** Стилизация ячейки значения с учётом «—» (отсутствие). */
  const valueStyle = (
    value: string,
    tier: 'free' | 'basic' | 'pro',
  ): React.CSSProperties => {
    const isAbsent = value === '—';
    if (isAbsent) {
      return { ...valueCellBase, color: 'var(--text-muted)', opacity: 0.55 };
    }
    if (tier === 'free') {
      return { ...valueCellBase, color: 'var(--text-muted)' };
    }
    if (tier === 'basic') {
      return { ...valueCellBase, color: 'var(--text-primary)' };
    }
    // pro — accent + bold для подсвечивания «топового» предложения
    return {
      ...valueCellBase,
      color: 'var(--accent)',
      fontWeight: 700,
    };
  };

  const headerStyle: React.CSSProperties = {
    padding: '12px',
    textAlign: 'center',
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    fontSize: 'var(--fs-2xs)',
    color: 'var(--text-secondary)',
    borderBottom: '2px solid var(--text-primary)',
  };

  return (
    <div className="mt-12 pt-8 border-t" style={{ borderColor: 'var(--border-color)' }}>
      <h3
        className="text-center mb-6 font-bold"
        style={{
          color: 'var(--text-primary)',
          fontSize: 'var(--fs-lg)',
          letterSpacing: '0.01em',
        }}
      >
        Что входит в каждый тариф
      </h3>

      <div className="overflow-x-auto -mx-4 px-4 styled-scrollbar">
        <table
          style={{
            width: '100%',
            borderCollapse: 'collapse',
            tableLayout: 'fixed',
            minWidth: 640,
          }}
        >
          <thead>
            <tr>
              <th style={{ ...headerStyle, textAlign: 'left', width: '38%' }}>Возможность</th>
              <th style={headerStyle}>Free</th>
              <th style={{ ...headerStyle, color: 'var(--accent)' }}>Basic</th>
              <th style={{ ...headerStyle, color: 'var(--accent)' }}>Pro</th>
            </tr>
          </thead>
          <tbody>
            {sections.map((section, si) => {
              const Icon = section.icon;
              return (
                <React.Fragment key={si}>
                  {/* Section header — крупная строка с иконкой, фоном и top-border'ом */}
                  <tr>
                    <td
                      colSpan={4}
                      style={{
                        padding: si === 0 ? '8px 12px' : '20px 12px 8px',
                        background: 'color-mix(in srgb, var(--accent) 6%, var(--bg-secondary))',
                        borderTop:
                          si === 0
                            ? 'none'
                            : '2px solid color-mix(in srgb, var(--text-primary) 25%, transparent)',
                        borderBottom: '1px solid var(--border-color)',
                      }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                        <span
                          style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            width: 28,
                            height: 28,
                            borderRadius: 6,
                            background: 'color-mix(in srgb, var(--accent) 18%, transparent)',
                            color: 'var(--accent)',
                            flexShrink: 0,
                          }}
                        >
                          <Icon size={16} strokeWidth={2} />
                        </span>
                        <span
                          style={{
                            fontSize: 'var(--fs-md)',
                            fontWeight: 800,
                            color: 'var(--text-primary)',
                            letterSpacing: '-0.005em',
                          }}
                        >
                          {section.title}
                        </span>
                      </div>
                    </td>
                  </tr>
                  {/* Sub-rows — indented, label приглушённый, значения с tier-coloring */}
                  {section.rows.map(([label, free, basic, pro], ri) => (
                    <tr key={`${si}-${ri}`}>
                      <td style={subRowLabelStyle}>{label}</td>
                      <td style={valueStyle(free, 'free')}>{free}</td>
                      <td style={valueStyle(basic, 'basic')}>{basic}</td>
                      <td style={valueStyle(pro, 'pro')}>{pro}</td>
                    </tr>
                  ))}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}


/**
 * PaymentMethods — блок «Способы оплаты» внизу страницы тарифов.
 * Обязателен для эквайринга T-Bank: показываем только логотип Банка-эквайера
 * + ссылка на tbank.ru. Логотипы конкретных ПС (МИР/T-Pay/СБП) убраны —
 * юзер увидит все доступные варианты прямо на pay.tbank.ru.
 */
function PaymentMethods() {
  return (
    <div
      className="mt-12 pt-8 border-t"
      style={{ borderColor: 'var(--border-color)' }}
    >
      <p
        className="text-center uppercase mb-5"
        style={{
          color: 'var(--text-muted)',
          fontSize: 'var(--fs-2xs)',
          letterSpacing: '0.32em',
          fontWeight: 700,
        }}
      >
        Способы оплаты
      </p>

      {/* Логотип Банка-эквайера + ссылка на tbank.ru — обязательное требование */}
      <div className="flex flex-col items-center gap-2 text-center">
        <a
          href="https://tbank.ru"
          target="_blank"
          rel="noreferrer noopener"
          className="inline-flex items-center gap-2 transition-opacity hover:opacity-80"
          style={{ textDecoration: 'none' }}
          aria-label="Эквайринг от Т-Банка — перейти на tbank.ru"
        >
          {/* T-Bank жёлтый щит-логотип */}
          <span
            className="inline-flex items-center justify-center font-bold"
            style={{
              width: 28,
              height: 28,
              background: '#FFDD2D',
              color: '#111',
              borderRadius: 4,
              fontSize: 16,
            }}
          >
            Т
          </span>
          <span
            className="font-semibold"
            style={{
              color: 'var(--text-primary)',
              fontSize: 'var(--fs-sm)',
            }}
          >
            Эквайринг от Т-Банка
          </span>
        </a>
        <p
          className="text-xs"
          style={{ color: 'var(--text-muted)' }}
        >
          Платежи защищены банком-эквайером по стандарту PCI DSS.{' '}
          <a
            href="https://tbank.ru"
            target="_blank"
            rel="noreferrer noopener"
            style={{ color: 'var(--accent)' }}
          >
            tbank.ru
          </a>
        </p>
      </div>
    </div>
  );
}

/**
 * Список фичей для каждого tier'а — отражает api/billing/features.py.
 * Premium удалён (упразднён в пользу 3-уровневой схемы).
 */
function getFeaturesList(tier: string): string[] {
  switch (tier) {
    case 'free':
      return [
        'Все 9 индикаторов с базовой функциональностью',
        'Все фьючерсы в Открытых позициях (базовый вид), 10 акций в Сезонности',
        'Только режим IMOEX в Карте рынка',
        'Дневные данные, глубина истории ограничена',
        'Задержка данных 24 часа',
        'Водяной знак на экспорте',
      ];
    case 'basic':
      return [
        'Всё из Free, плюс:',
        'Все категории и режимы в Открытых позициях (физлица и юрлица)',
        '«Все акции» в Карте рынка и Силе рынка',
        'Долларовый режим в Силе рынка',
        'История до 10 лет, без задержки данных',
      ];
    case 'pro':
      return [
        'Всё из Basic, плюс:',
        '5-минутные таймфреймы в Открытых позициях',
        'Внутридневная сезонность',
        'Вся история без ограничений',
        // KILL-SWITCH: CSV/API скрыты до запуска (config/features.ts)
        ...(API_CSV_ENABLED ? ['Экспорт в CSV/Excel', 'API-доступ для автоматизации'] : []),
        'Без водяного знака на экспорте',
      ];
    default:
      return [];
  }
}
