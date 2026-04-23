/**
 * PricingPage — страница тарифов.
 *
 * Загружает /api/billing/plans → показывает 4 тарифа (Free / Basic / Pro / Premium).
 * Переключатель "Месяц/Год" внутри каждой платной карточки.
 * Клик "Купить" → POST /api/billing/checkout → редирект на confirmation_url.
 *
 * При работе в STUB-режиме (нет ключей ЮKassa) — confirmation_url ведёт на
 * /billing/stub, где можно симулировать успешную оплату.
 */
import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Check, Zap, Crown, Sparkles } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { apiFetch } from '../services/api';

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
  provider: 'stub' | 'yookassa';
  currency: string;
  tiers: TierCard[];
}

// Визуал tier'ов: иконка + акцентный цвет
const TIER_META: Record<string, { icon: React.ReactNode; color: string; accentBg: string }> = {
  free: { icon: <Check size={24} />, color: '#9CA3B8', accentBg: 'rgba(156,163,184,0.1)' },
  basic: { icon: <Zap size={24} />, color: '#60A5FA', accentBg: 'rgba(96,165,250,0.1)' },
  pro: { icon: <Crown size={24} />, color: '#C8FF2E', accentBg: 'rgba(200,255,46,0.1)' },
  premium: { icon: <Sparkles size={24} />, color: '#F97316', accentBg: 'rgba(249,115,22,0.1)' },
};

export default function PricingPage() {
  const { user, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const [data, setData] = useState<PlansResponse | null>(null);
  const [period, setPeriod] = useState<'monthly' | 'yearly'>('yearly'); // годовой по умолчанию (выгоднее)
  const [loading, setLoading] = useState(true);
  const [checkoutLoading, setCheckoutLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

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
        }),
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(errData.detail || 'Ошибка создания платежа');
      }
      const body = await resp.json();
      // Редирект на страницу оплаты
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
          Выбирай тариф под задачи. Оплата через ЮKassa — карты, СБП, кошельки.
          Отмена в один клик.
        </p>
      </div>

      {/* Баннер STUB-режима — показываем только если провайдер 'stub' */}
      {data.provider === 'stub' && (
        <div className="mb-6 mx-auto max-w-3xl rounded-xl border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-300">
          ⚙️ <strong>Тестовый режим:</strong> ЮKassa ещё не подключена, платежи не спишутся.
          После оплаты будет страница с кнопкой "Симулировать успех" — это для разработки.
        </div>
      )}

      {/* Переключатель Месяц / Год */}
      <div className="flex justify-center mb-8">
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

      {/* Карточки тарифов */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 md:gap-5">
        {data.tiers.map((tier) => {
          const meta = TIER_META[tier.tier] || TIER_META.free;
          const variant = tier.tier === 'free' ? null : (period === 'yearly' ? tier.yearly : tier.monthly);
          const isCurrent = currentTier === tier.tier;
          const isDowngrade = isCurrent && tier.tier !== 'free';

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
              ) : isDowngrade ? (
                <button
                  disabled
                  className="w-full py-2.5 rounded-xl text-sm font-medium opacity-50 cursor-not-allowed border"
                  style={{ borderColor: meta.color, color: meta.color }}
                >
                  Действует
                </button>
              ) : (
                <button
                  onClick={() => variant && handleCheckout(variant.plan_id)}
                  disabled={!variant || checkoutLoading === variant.plan_id}
                  className="w-full py-2.5 rounded-xl text-sm font-medium transition-colors disabled:opacity-50"
                  style={{ backgroundColor: meta.color, color: 'var(--bg-primary)' }}
                >
                  {checkoutLoading === variant?.plan_id ? 'Создаём...' : 'Оформить'}
                </button>
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

      {/* Footer с FAQ (заготовка) */}
      <div className="mt-12 text-center text-sm text-theme-muted">
        <p>
          Есть вопросы? Напиши в{' '}
          <a href="https://t.me/" target="_blank" rel="noreferrer" className="text-theme-primary hover:underline">
            Telegram
          </a>
          . Возврат — 7 дней по закону ЗПП.
        </p>
      </div>
    </div>
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
