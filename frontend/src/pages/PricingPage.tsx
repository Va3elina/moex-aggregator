/**
 * PricingPage — страница тарифов.
 *
 * Загружает /api/billing/plans → показывает 4 тарифа (Free / Basic / Pro / Premium).
 * Переключатель "Месяц/Год" внутри каждой платной карточки.
 * Клик "Купить" → POST /api/billing/checkout → редирект на confirmation_url.
 *
 * Провайдер выбирается на бэке (factory.py):
 *   - tbank → confirmation_url ведёт на https://pay.tbank.ru/...
 *   - stub  → confirmation_url ведёт на /billing/stub (для dev без T-Bank ключей)
 */
import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Check, Zap, Crown, Sparkles } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { apiFetch } from '../services/api';
import SpeedPayButtons from '../components/billing/SpeedPayButtons';

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
}

// Визуал tier'ов: иконка + акцентный цвет
const TIER_META: Record<string, { icon: React.ReactNode; color: string; accentBg: string }> = {
  free: { icon: <Check size={24} />, color: '#9CA3B8', accentBg: 'rgba(156,163,184,0.1)' },
  basic: { icon: <Zap size={24} />, color: '#60A5FA', accentBg: 'rgba(96,165,250,0.1)' },
  pro: { icon: <Crown size={24} />, color: '#C8FF2E', accentBg: 'rgba(200,255,46,0.1)' },
  premium: { icon: <Sparkles size={24} />, color: '#F97316', accentBg: 'rgba(249,115,22,0.1)' },
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
}

export default function PricingPage() {
  const { user, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const [data, setData] = useState<PlansResponse | null>(null);
  const [period, setPeriod] = useState<'monthly' | 'yearly'>('yearly'); // годовой по умолчанию (выгоднее)
  const [loading, setLoading] = useState(true);
  const [checkoutLoading, setCheckoutLoading] = useState<string | null>(null);
  // recurrent — сохранить карту для авто-продления. Default true (по умолчанию
  // подписочные сервисы обычно с auto-renewal — Netflix, Spotify тоже).
  // Юзер может снять галку чтобы заплатить одноразово.
  const [recurrent, setRecurrent] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  // billing — реальное состояние подписки user'а (через /api/billing/status).
  // Нужно для блокировки кнопок: с active basic нельзя купить тот же basic
  // или ниже. Со cancelled подпиской разрешаем продление того же tier'а.
  // null = ещё грузится или гость.
  const [billing, setBilling] = useState<BillingStatus | null>(null);

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

  // Клик "Купить"
  const handleCheckout = async (planId: string) => {
    if (!isAuthenticated) {
      navigate(`/login?next=/pricing`);
      return;
    }
    setCheckoutLoading(planId);
    setError(null);
    try {
      const resp = await apiFetch('/api/billing/checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          plan_id: planId,
          return_url: `${window.location.origin}/billing/success`,
          recurrent,  // сохранить карту для авто-продления?
        }),
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(errData.detail || 'Ошибка создания платежа');
      }
      const body = await resp.json();
      // Full-page redirect на pay.tbank.ru (или yoomoney.ru / /billing/stub).
      // Раньше мы для T-Bank открывали iframe modal, но из-за X-Frame-Options:DENY
      // на нашем nginx редирект внутри iframe на наш /billing/fail блокировался
      // (ERR_BLOCKED_BY_RESPONSE). T-Bank рекомендует именно redirect-flow,
      // когда параллельно используется SpeedPay SDK.
      window.location.href = body.confirmation_url;
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Ошибка');
      setCheckoutLoading(null);
    }
  };

  if (loading) return (
    <div className="max-w-6xl mx-auto p-8 text-center text-theme-secondary">Загрузка тарифов...</div>
  );
  if (error && !data) return (
    <div className="max-w-6xl mx-auto p-8 text-center text-red-400">{error}</div>
  );
  if (!data) return null;

  return (
    <div className="max-w-6xl mx-auto px-4 md:px-6 py-6 md:py-8">
      {/* Заголовок */}
      <div className="text-center mb-8">
        <h1 className="text-3xl md:text-4xl font-bold text-theme-primary mb-3">Тарифы</h1>
        <p className="text-theme-secondary text-base md:text-lg max-w-2xl mx-auto">
          Выбирай тариф под задачи. Оплата через Т-Банк — карты, СБП, T-Pay.
          Отмена в один клик.
        </p>
      </div>

      {/* Баннер STUB-режима — показываем только если провайдер 'stub' */}
      {data.provider === 'stub' && (
        <div className="mb-6 mx-auto max-w-3xl rounded-xl border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-300">
          ⚙️ <strong>Тестовый режим:</strong> эквайринг ещё не подключён, платежи не спишутся.
          После оплаты будет страница с кнопкой "Симулировать успех" — это для разработки.
        </div>
      )}

      {/* Переключатель Месяц / Год */}
      <div className="flex justify-center mb-4">
        <div className="inline-flex rounded-xl border p-1" style={{ borderColor: 'var(--border-color)', backgroundColor: 'var(--bg-secondary)' }}>
          <button
            onClick={() => setPeriod('monthly')}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${period === 'monthly' ? 'bg-theme-tertiary text-theme-primary' : 'text-theme-secondary'}`}
          >
            Месяц
          </button>
          <button
            onClick={() => setPeriod('yearly')}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${period === 'yearly' ? 'bg-theme-tertiary text-theme-primary' : 'text-theme-secondary'}`}
          >
            Год <span className="text-green-400 text-xs ml-1">−17%</span>
          </button>
        </div>
      </div>

      {/* Авто-продление — сохранить карту для будущих списаний */}
      {data.provider === 'tbank' && (
        <div className="flex justify-center mb-8">
          <label
            className="inline-flex items-center gap-2 cursor-pointer select-none"
            style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)' }}
          >
            <input
              type="checkbox"
              checked={recurrent}
              onChange={(e) => setRecurrent(e.target.checked)}
              className="w-4 h-4 cursor-pointer"
              style={{ accentColor: 'var(--accent)' }}
            />
            <span>
              Авто-продление: сохранить карту для следующих периодов
            </span>
          </label>
        </div>
      )}

      {/* Карточки тарифов */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 md:gap-5">
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

              {/* Иконка + заголовок */}
              <div className="mb-4">
                <div
                  className="w-12 h-12 rounded-xl flex items-center justify-center mb-3"
                  style={{ backgroundColor: meta.accentBg, color: meta.color }}
                >
                  {meta.icon}
                </div>
                <h3 className="text-xl font-bold text-theme-primary">{tier.title}</h3>
                <p className="text-sm text-theme-secondary mt-1">{tier.description}</p>
              </div>

              {/* Цена */}
              <div className="mb-5">
                {variant ? (
                  <>
                    <div className="flex items-baseline gap-2">
                      <span className="text-3xl font-bold text-theme-primary">
                        {variant.amount.toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽
                      </span>
                      <span className="text-sm text-theme-secondary">
                        /{period === 'yearly' ? 'год' : 'мес'}
                      </span>
                    </div>
                    {period === 'yearly' && tier.monthly && (
                      <div className="text-xs text-theme-muted mt-1">
                        ≈ {Math.round(variant.amount / 12).toLocaleString('ru-RU')} ₽/мес
                      </div>
                    )}
                  </>
                ) : (
                  <div className="text-3xl font-bold text-theme-primary">Бесплатно</div>
                )}
              </div>

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
              ) : (
                <>
                  <button
                    onClick={() => variant && handleCheckout(variant.plan_id)}
                    disabled={!variant || checkoutLoading === variant.plan_id}
                    className="w-full py-2.5 rounded-xl text-sm font-medium transition-colors disabled:opacity-50"
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
                            : 'Оформить картой'}
                  </button>

                  {/* SpeedPay — T-Bank виджет с кнопками СБП / T-Pay / BNPL.
                      Появляется только когда provider=tbank и SDK успел загрузиться.
                      Список конкретных кнопок управляется в кабинете T-Bank
                      (Магазины → Прием оплаты → Кнопки быстрой оплаты). */}
                  {data.provider === 'tbank' && data.terminal_key && variant && (
                    <div className="mt-3 pt-3 border-t" style={{ borderColor: 'var(--border-color)' }}>
                      <p
                        className="text-center mb-2 uppercase tracking-wider"
                        style={{
                          color: 'var(--text-muted)',
                          fontSize: 'var(--fs-2xs, 10px)',
                          fontWeight: 600,
                          letterSpacing: '0.16em',
                        }}
                      >
                        Или быстрее
                      </p>
                      <SpeedPayButtons
                        terminalKey={data.terminal_key}
                        planId={variant.plan_id}
                      />
                    </div>
                  )}
                </>
              )}
            </div>
          );
        })}
      </div>

      {/* Ошибка checkout */}
      {error && (
        <div className="mt-6 mx-auto max-w-xl rounded-xl border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-300 text-center">
          {error}
        </div>
      )}

      {/* Способы оплаты — обязательный блок по требованиям эквайринга Т-Банка
          (логотипы принимаемых ПС + логотип Банка-эквайера + URL на tbank.ru). */}
      <PaymentMethods />

      {/* Footer с FAQ */}
      <div className="mt-8 text-center text-sm text-theme-muted">
        <p>
          Есть вопросы? Напиши в{' '}
          <a href="https://t.me/" target="_blank" rel="noreferrer" className="text-theme-primary hover:underline">
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
    </div>
  );
}

/**
 * PaymentMethods — блок «Способы оплаты» внизу страницы тарифов.
 * Обязателен для эквайринга T-Bank (Требования к Интернет-магазину):
 *   - Изображения с логотипами поддерживаемых ПС
 *   - Логотип Банка-эквайера (T-Bank)
 *   - URL ссылка на ресурсы Банка: tbank.ru
 *
 * Visa/MasterCard и СБП временно убраны — Вадим согласовал с T-Bank
 * только МИР + T-Pay. Если T-Bank подключит другие методы — добавим.
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

      <div className="flex flex-wrap items-center justify-center gap-3 mb-6">
        {/* МИР */}
        <Badge background="#0F754E" color="#fff">
          МИР
        </Badge>

        {/* T-Pay */}
        <Badge background="#FFDD2D" color="#111">
          T-Pay
        </Badge>
      </div>

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
 * Badge — простой pill-логотип ПС с brand-colored background.
 */
function Badge({
  children,
  background,
  color,
  italic = false,
}: {
  children: React.ReactNode;
  background: string;
  color: string;
  italic?: boolean;
}) {
  return (
    <span
      className="inline-flex items-center justify-center font-bold"
      style={{
        background,
        color,
        padding: '6px 14px',
        borderRadius: 4,
        fontSize: 14,
        fontStyle: italic ? 'italic' : 'normal',
        letterSpacing: '0.05em',
        minWidth: 60,
        height: 28,
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif',
      }}
    >
      {children}
    </span>
  );
}

/**
 * Текущий список фичей для каждого tier'а.
 * ВРЕМЕННАЯ ЗАГЛУШКА — реальная feature-matrix появится позже
 * в api/billing/features.py и будет приходить из /api/billing/plans.
 */
function getFeaturesList(tier: string): string[] {
  switch (tier) {
    case 'free':
      return [
        'Просмотр основных индикаторов',
        'Дневные таймфреймы',
        'Последний год истории',
      ];
    case 'basic':
      return [
        'Всё из Free',
        'Часовые таймфреймы',
        'Полная история за 5 лет',
        'Все базовые индикаторы',
      ];
    case 'pro':
      return [
        'Всё из Basic',
        '5-минутные таймфреймы',
        'Вся история без ограничений',
        'Индикатор Баффета + ОИ в деталях',
        'Real-time обновление',
        'Экспорт в CSV',
      ];
    case 'premium':
      return [
        'Всё из Pro',
        'Доступ к API для автоматизации',
        'Приоритетная поддержка',
        'Все будущие фичи включены',
      ];
    default:
      return [];
  }
}
