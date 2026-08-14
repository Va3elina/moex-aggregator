import { useState, useEffect } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import {
  LogOut, Lock, Mail, Calendar, Shield, Crown,
  Check, X as XIcon, Eye, EyeOff, Sparkles, ExternalLink, AlertCircle, User,
} from 'lucide-react';
import AdminBillingInvites from '../components/AdminBillingInvites';
import ApiKeysSection from '../components/profile/ApiKeysSection';
import ExtensionTokenSection from '../components/profile/ExtensionTokenSection';
import TelegramAlertsSection from '../components/profile/TelegramAlertsSection';
import { ALERTS_ENABLED } from '../config/alertsConfig';
import { API_CSV_ENABLED, PAYMENT_METHODS_UI_ENABLED } from '../config/features';

interface BillingStatus {
  tier: string;
  is_active: boolean;
  subscription_id: number | null;
  plan_id: string | null;
  started_at: string | null;
  expires_at: string | null;
  cancelled_at: string | null;  // NULL → активна, NOT NULL → отменена (доступ до expires_at)
  retention_eligible?: boolean; // можно предложить retention-скидку 40% вместо отмены
}

interface HistoryItem {
  id: number;
  tier: string;
  plan_id: string | null;
  amount: number | null;
  currency: string | null;
  status: string;              // pending / active / cancelled / expired / failed / refunded
  created_at: string | null;
  started_at: string | null;
  expires_at: string | null;
  cancelled_at: string | null;
  yk_payment_id: string | null;
}

interface PaymentMethod {
  id: number;
  provider: string;            // 'tbank' / 'yookassa'
  method_type: string | null;  // 'card' / 'sbp'
  card_last4: string | null;
  card_brand: string | null;
  display_name: string | null;
  is_default: boolean;
  last_used_at: string | null;
  created_at: string | null;
}

const TIER_LABELS: Record<string, string> = {
  free: 'Бесплатный план',
  basic: 'Basic',
  pro: 'Pro',
};

// Tier-specific features. Каждый next tier включает всё предыдущее (transitively).
// Здесь только incremental — что добавляется к предыдущему уровню.
const TIER_FEATURES: Record<string, { tagline: string; features: string[] }> = {
  basic: {
    tagline: 'Данные в реальном времени и расширенная история (10 лет).',
    features: [
      'Все 65 фьючерсов в Открытых позициях',
      'Режим «Все акции» в Карте рынка и Силе рынка',
      'Долларовый режим в Силе рынка',
      'История до 10 лет, без задержки данных',
      '20 уведомлений в Telegram',
    ],
  },
  pro: {
    tagline: API_CSV_ENABLED
      ? 'Максимум: 5-минутные данные, API и безлимиты.'
      : 'Максимум: 5-минутные данные и безлимиты.',
    features: [
      'Всё из Basic',
      '5-минутные таймфреймы в Открытых позициях',
      // «Внутридневная сезонность» убрана 2026-08: сезонность бесплатна целиком
      // (features.py: seasonality — все режимы на всех тирах).
      'Вся история без ограничений',
      // API + CSV скрыты до официального запуска (config/features.ts),
      // как на PricingPage — вернутся сами при включении флага.
      ...(API_CSV_ENABLED ? ['Экспорт CSV / Excel', 'API-доступ для автоматизации'] : []),
      'Безлимитные уведомления в Telegram',
    ],
  },
};

const ROLE_LABELS: Record<string, string> = {
  user: 'Пользователь',
  pro: 'Pro',
  admin: 'Администратор',
};

const ROLE_COLORS: Record<string, { bg: string; text: string }> = {
  user: { bg: 'color-mix(in srgb, var(--accent) 15%, transparent)', text: 'var(--accent)' },
  pro: { bg: 'color-mix(in srgb, var(--warning) 15%, transparent)', text: 'var(--warning)' },
  admin: { bg: 'color-mix(in srgb, var(--danger) 15%, transparent)', text: 'var(--danger)' },
};

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString('ru-RU', {
    day: 'numeric',
    month: 'long',
    year: 'numeric',
  });
}

export default function ProfilePage() {
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  // Avatar — always show initials

  // Password change
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [showCurrentPw, setShowCurrentPw] = useState(false);
  const [showNewPw, setShowNewPw] = useState(false);
  const [pwLoading, setPwLoading] = useState(false);
  const [pwMsg, setPwMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);

  // Real subscription status — fetch'аем при mount, заменяет hardcoded "Бесплатный план"
  const [billing, setBilling] = useState<BillingStatus | null>(null);
  const [cancelLoading, setCancelLoading] = useState(false);
  const [showRetention, setShowRetention] = useState(false);  // модалка retention-оффера

  // История платежей — последние N подписок любого статуса
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [historyExpanded, setHistoryExpanded] = useState(false);

  // Привязанные способы оплаты (рекуррент-токены). Секция вернулась 2026-07-03:
  // компл-требование ЮKassa (и T-Bank) — юзер обязан уметь отвязать сам,
  // без обращения в поддержку; бэк при отвязке стирает токен из БД.
  const [paymentMethods, setPaymentMethods] = useState<PaymentMethod[]>([]);
  const [pmActionId, setPmActionId] = useState<number | null>(null);

  const fetchBilling = () => {
    fetch('/api/billing/status', {
      headers: { Authorization: `Bearer ${localStorage.getItem('access_token')}` },
    })
      .then(r => r.ok ? r.json() : null)
      .then(setBilling)
      .catch(() => setBilling(null));
  };
  const fetchHistory = () => {
    fetch('/api/billing/history?limit=20', {
      headers: { Authorization: `Bearer ${localStorage.getItem('access_token')}` },
    })
      .then(r => r.ok ? r.json() : { items: [] })
      .then(d => setHistory(d.items || []))
      .catch(() => setHistory([]));
  };
  const fetchPaymentMethods = () => {
    fetch('/api/billing/payment_methods', {
      headers: { Authorization: `Bearer ${localStorage.getItem('access_token')}` },
    })
      .then(r => r.ok ? r.json() : { items: [] })
      .then(d => setPaymentMethods(d.items || []))
      .catch(() => setPaymentMethods([]));
  };
  useEffect(() => {
    fetchBilling();
    fetchHistory();
    if (PAYMENT_METHODS_UI_ENABLED) fetchPaymentMethods();
  }, []);

  // Отвязать способ оплаты. Данные привязки стираются на бэке — повторное
  // списание невозможно; активная подписка доживает до expires_at.
  const handleDeletePaymentMethod = async (pmId: number) => {
    if (!window.confirm(
      'Отвязать способ оплаты?\n\n' +
      'Данные привязки будут удалены, авто-продление подписки перестанет работать. ' +
      'Оплаченный период продолжит действовать до конца. ' +
      'Привязать способ оплаты снова можно при следующей оплате.'
    )) return;
    setPmActionId(pmId);
    try {
      await fetch(`/api/billing/payment_methods/${pmId}`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${localStorage.getItem('access_token')}` },
      });
      fetchPaymentMethods();
    } finally {
      setPmActionId(null);
    }
  };

  // Cancel или Resume — обе ручки идут через те же args, разные endpoints
  const handleSubAction = async (action: 'cancel' | 'resume', opts?: { force?: boolean }) => {
    if (!billing?.subscription_id) return;
    if (action === 'cancel' && !opts?.force) {
      // Интерсепт: если доступна retention-скидка — показываем оффер 40% вместо
      // отмены. «Всё равно отменить» в модалке зовёт с force=true.
      if (billing.retention_eligible) { setShowRetention(true); return; }
      if (!window.confirm(
        'Отменить подписку?\n\nАвто-продление будет отключено, но доступ к Pro-функциям останется до конца оплаченного периода.'
      )) return;
    }
    setCancelLoading(true);
    try {
      await fetch(`/api/billing/${action}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${localStorage.getItem('access_token')}`,
        },
        body: JSON.stringify({ subscription_id: billing.subscription_id }),
      });
      fetchBilling();
    } finally {
      setCancelLoading(false);
      setShowRetention(false);
    }
  };

  // Принять retention-оффер: скидка 40% на следующее продление вместо отмены.
  const acceptRetentionOffer = async () => {
    if (!billing?.subscription_id) return;
    setCancelLoading(true);
    try {
      await fetch('/api/billing/retention/accept', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${localStorage.getItem('access_token')}`,
        },
        body: JSON.stringify({ subscription_id: billing.subscription_id }),
      });
      fetchBilling();
    } finally {
      setCancelLoading(false);
      setShowRetention(false);
    }
  };

  if (!user) {
    navigate('/login');
    return null;
  }

  const displayName = user.display_name || user.email;
  const isOAuthLocal = user.email.endsWith('@oauth.local');
  const roleStyle = ROLE_COLORS[user.role] || ROLE_COLORS.user;

  const handlePasswordChange = async (e: React.FormEvent) => {
    e.preventDefault();
    setPwLoading(true);
    setPwMsg(null);
    try {
      const resp = await fetch('/api/auth/change-password', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${localStorage.getItem('access_token')}`,
        },
        body: JSON.stringify({
          current_password: currentPassword,
          new_password: newPassword,
        }),
      });
      if (!resp.ok) {
        const data = await resp.json();
        throw new Error(data.detail || 'Ошибка');
      }
      setCurrentPassword('');
      setNewPassword('');
      setPwMsg({ type: 'ok', text: 'Пароль успешно изменён' });
    } catch (err) {
      setPwMsg({ type: 'err', text: err instanceof Error ? err.message : 'Ошибка' });
    } finally {
      setPwLoading(false);
    }
  };

  const handleLogout = async () => {
    await logout();
    navigate('/');
  };

  const cardStyle = {
    backgroundColor: 'var(--bg-secondary)',
    borderColor: 'var(--border-color)',
  };

  const inputStyle = {
    backgroundColor: 'var(--bg-primary)',
    borderColor: 'var(--border-color)',
    color: 'var(--text-primary)',
  };

  return (
    <div className="max-w-2xl mx-auto px-4 py-8 space-y-6">
      <h1 className="text-2xl font-bold" style={{ color: 'var(--text-primary)' }}>
        Личный кабинет
      </h1>

      {/* ============ Секция 1: Шапка профиля ============ */}
      <div className="rounded-2xl border p-6" style={cardStyle}>
        <div className="flex items-center gap-4 mb-5">
          {/* Аватар — lucide-иконка User (как на кнопке кабинета в шапке) */}
          <div
            className="w-16 h-16 rounded-full flex items-center justify-center shrink-0"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            <User style={{ width: 40, height: 40 }} strokeWidth={2} />
          </div>
          <div className="text-left">
            <div className="text-lg font-semibold" style={{ color: 'var(--text-primary)' }}>
              {displayName}
            </div>
            <div className="flex items-center gap-2 mt-1">
              <span
                className="px-2.5 py-0.5 text-xs font-medium rounded-full"
                style={{ backgroundColor: roleStyle.bg, color: roleStyle.text }}
              >
                {ROLE_LABELS[user.role] || user.role}
              </span>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-sm">
          {!isOAuthLocal && (
            <div className="flex items-center gap-2" style={{ color: 'var(--text-secondary)' }}>
              <Mail size={16} style={{ color: 'var(--text-muted)' }} />
              <span>{user.email}</span>
            </div>
          )}
          <div className="flex items-center gap-2" style={{ color: 'var(--text-secondary)' }}>
            <Calendar size={16} style={{ color: 'var(--text-muted)' }} />
            <span>Регистрация: {formatDate(user.created_at)}</span>
          </div>
        </div>

        {/* Баннер email: synthetic (Telegram/VK без email) → указать email;
            реальный неподтверждённый → подтвердить кодом. OAuth с реальным email
            (Google/Yandex) → is_verified=true, баннер не видят. */}
        {!user.is_verified && (
          <div
            className="mt-4 flex items-start gap-3 p-3 rounded-xl"
            style={{
              backgroundColor: 'color-mix(in srgb, var(--accent) 12%, transparent)',
              border: '1px solid color-mix(in srgb, var(--accent) 35%, transparent)',
            }}
          >
            <AlertCircle size={18} style={{ color: 'var(--accent)' }} className="mt-0.5 shrink-0" />
            <div className="text-sm" style={{ color: 'var(--text-secondary)' }}>
              {isOAuthLocal ? (
                <>
                  <span style={{ color: 'var(--text-primary)', fontWeight: 600 }}>Укажите email.</span>{' '}
                  Без подтверждённого email нельзя получать чеки и оформить оплату.{' '}
                  <Link to="/add-email" style={{ color: 'var(--accent)', fontWeight: 600 }}>Указать →</Link>
                </>
              ) : (
                <>
                  <span style={{ color: 'var(--text-primary)', fontWeight: 600 }}>Email не подтверждён.</span>{' '}
                  Подтвердите адрес, чтобы получать чеки и оформить оплату.{' '}
                  <Link to="/verify-email" style={{ color: 'var(--accent)', fontWeight: 600 }}>Подтвердить →</Link>
                </>
              )}
            </div>
          </div>
        )}
      </div>

      {/* ============ Секция 2: Подписка ============
          3 ветки: admin → spec, active sub → details, free → CTA */}
      <div className="rounded-2xl border p-6" style={cardStyle}>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
            <Crown size={20} style={{ color: 'var(--accent)' }} />
            {user.role === 'admin' ? 'Доступ' : 'Подписка'}
          </h2>
          {user.role === 'admin' ? (
            <span
              className="px-3 py-1 text-xs font-bold rounded-full"
              style={{
                backgroundColor: 'color-mix(in srgb, var(--danger) 18%, transparent)',
                color: 'var(--danger)',
              }}
            >
              admin
            </span>
          ) : billing && (
            <span
              className="px-3 py-1 text-xs font-medium rounded-full"
              style={{
                backgroundColor: billing.is_active
                  ? 'color-mix(in srgb, var(--success) 18%, transparent)'
                  : 'color-mix(in srgb, var(--accent) 15%, transparent)',
                color: billing.is_active ? 'var(--success)' : 'var(--accent)',
              }}
            >
              {TIER_LABELS[billing.tier] || billing.tier}
            </span>
          )}
        </div>

        {user.role === 'admin' ? (
          // === Admin — полный доступ, без CTA ===
          <>
            <p className="text-sm mb-3" style={{ color: 'var(--text-secondary)' }}>
              Полный административный доступ ко всем функциям платформы — без ограничений по тарифу.
            </p>
            <div className="space-y-2 mb-2">
              {[
                'Все индикаторы и инструменты',
                'Полная история данных по всем тикерам',
                'Все таймфреймы (5мин / 1ч / 1д / 1мес)',
                'Аналитика сайта (/admin/stats)',
                'Управление подписками и invite-ссылками',
              ].map((text, i) => (
                <div key={i} className="flex items-center gap-2.5 text-sm">
                  <Check size={16} style={{ color: 'var(--success)' }} className="shrink-0" />
                  <span style={{ color: 'var(--text-secondary)' }}>{text}</span>
                </div>
              ))}
            </div>
          </>
        ) : billing?.is_active ? (
          // === Active subscription — детали + tier features + change/cancel ===
          <>
            {billing.cancelled_at ? (
              // === Cancelled (но ещё активна до expires_at) ===
              <div
                className="p-3 rounded-xl mb-4 text-sm"
                style={{
                  backgroundColor: 'color-mix(in srgb, var(--warning) 12%, transparent)',
                  border: '1px solid color-mix(in srgb, var(--warning) 35%, transparent)',
                  color: 'var(--text-secondary)',
                }}
              >
                <strong style={{ color: 'var(--warning)' }}>Подписка отменена.</strong>{' '}
                Доступ к Pro-функциям сохранится до{' '}
                <strong style={{ color: 'var(--text-primary)' }}>
                  {billing.expires_at ? formatDate(billing.expires_at) : '—'}
                </strong>
                . После этой даты аккаунт перейдёт на бесплатный план.
              </div>
            ) : (
              <p className="text-sm mb-2" style={{ color: 'var(--text-secondary)' }}>
                Активна до{' '}
                <strong style={{ color: 'var(--text-primary)' }}>
                  {billing.expires_at ? formatDate(billing.expires_at) : '—'}
                </strong>
              </p>
            )}

            {/* Tier features — что включено в текущий tier */}
            {TIER_FEATURES[billing.tier] && (
              <>
                <p className="text-sm mb-3" style={{ color: 'var(--text-secondary)' }}>
                  {TIER_FEATURES[billing.tier].tagline}
                </p>
                <div className="space-y-2 mb-5">
                  {TIER_FEATURES[billing.tier].features.map((text, i) => (
                    <div key={i} className="flex items-center gap-2.5 text-sm">
                      <Check size={16} style={{ color: 'var(--success)' }} className="shrink-0" />
                      <span style={{ color: 'var(--text-secondary)' }}>{text}</span>
                    </div>
                  ))}
                </div>
              </>
            )}

            {/* Action buttons — change tier, cancel/resume */}
            <div className="flex flex-wrap gap-2 items-center">
              <Link
                to="/pricing"
                className="inline-flex items-center gap-2 px-4 py-2 rounded-full text-sm font-medium transition-opacity hover:opacity-90"
                style={{
                  backgroundColor: 'var(--bg-secondary)',
                  color: 'var(--text-primary)',
                  border: '1.5px solid var(--text-primary)',
                }}
              >
                Сменить тариф
                <ExternalLink size={14} />
              </Link>

              {billing.cancelled_at ? (
                <button
                  onClick={() => handleSubAction('resume')}
                  disabled={cancelLoading}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-full text-sm font-medium transition-opacity hover:opacity-90 disabled:opacity-50"
                  style={{
                    backgroundColor: 'var(--accent)',
                    color: '#fff',
                    border: '1.5px solid var(--text-primary)',
                  }}
                >
                  Возобновить
                </button>
              ) : (
                <button
                  onClick={() => handleSubAction('cancel')}
                  disabled={cancelLoading}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-full text-sm font-medium transition-opacity hover:opacity-90 disabled:opacity-50"
                  style={{
                    backgroundColor: 'transparent',
                    color: 'var(--danger)',
                    border: '1.5px solid var(--danger)',
                  }}
                >
                  Отменить подписку
                </button>
              )}
            </div>

            {/* Retention-оффер: скидка 40% вместо отмены автопродления (1 раз на аккаунт) */}
            {showRetention && billing?.subscription_id && (
              <div className="fixed inset-0 z-50 flex items-center justify-center p-4" role="dialog" aria-modal="true">
                <div className="absolute inset-0" style={{ backgroundColor: 'rgba(0,0,0,0.6)' }} onClick={() => !cancelLoading && setShowRetention(false)} />
                <div
                  className="relative w-full max-w-sm rounded-2xl p-6"
                  style={{
                    backgroundColor: 'var(--bg-secondary)',
                    border: '2px solid var(--text-primary)',
                    boxShadow: 'var(--shadow-hard-chip, 6px 6px 0 var(--text-primary))',
                    color: 'var(--text-primary)',
                  }}
                >
                  <div className="text-center">
                    <div className="text-3xl font-extrabold mb-1" style={{ color: 'var(--accent)' }}>−40%</div>
                    <h3 className="text-lg font-bold mb-2">Останьтесь со скидкой</h3>
                    <p className="text-sm mb-5" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
                      Не уходите — следующее продление со&nbsp;скидкой <b style={{ color: 'var(--text-primary)' }}>40%</b>. Подписка
                      останется активной, дальше — по&nbsp;обычной цене. Предложение одноразовое.
                    </p>
                  </div>
                  <div className="flex flex-col gap-2.5">
                    <button
                      onClick={acceptRetentionOffer}
                      disabled={cancelLoading}
                      className="w-full px-4 py-3 rounded-full text-sm font-bold transition-opacity hover:opacity-90 disabled:opacity-50"
                      style={{
                        backgroundColor: 'var(--accent)',
                        color: 'var(--text-inverse)',
                        border: '2px solid var(--text-primary)',
                        boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))',
                      }}
                    >
                      {cancelLoading ? 'Применяем…' : 'Получить скидку 40%'}
                    </button>
                    <button
                      onClick={() => handleSubAction('cancel', { force: true })}
                      disabled={cancelLoading}
                      className="w-full px-4 py-2.5 rounded-full text-sm font-medium transition-opacity hover:opacity-80 disabled:opacity-50"
                      style={{ backgroundColor: 'transparent', color: 'var(--text-muted)' }}
                    >
                      Всё равно отменить
                    </button>
                  </div>
                </div>
              </div>
            )}
          </>
        ) : (
          // === Free user — features list + CTA ===
          <>
            <p className="text-sm mb-4" style={{ color: 'var(--text-muted)' }}>
              Все инструменты доступны с базовыми ограничениями
            </p>
            <div className="space-y-2.5 mb-5">
              {[
                { ok: true, text: 'Все инструменты и индикаторы' },
                { ok: true, text: 'Обзор рынка в реальном времени' },
                { ok: true, text: 'Дневной таймфрейм' },
                { ok: false, text: 'Короткие таймфреймы (5мин, 1ч)' },
                { ok: false, text: 'Полная история данных' },
              ].map((item, i) => (
                <div key={i} className="flex items-center gap-2.5 text-sm">
                  {item.ok ? (
                    <Check size={16} style={{ color: 'var(--success)' }} className="shrink-0" />
                  ) : (
                    <XIcon size={16} className="shrink-0" style={{ color: 'var(--text-muted)' }} />
                  )}
                  <span style={{ color: item.ok ? 'var(--text-secondary)' : 'var(--text-muted)' }}>
                    {item.text}
                    {!item.ok && <span className="ml-1 text-xs opacity-60">— Pro</span>}
                  </span>
                </div>
              ))}
            </div>
            <Link
              to="/pricing"
              className="w-full flex items-center justify-center gap-2 py-3 rounded-xl text-sm font-bold transition-opacity hover:opacity-90"
              style={{
                backgroundColor: 'var(--accent)',
                color: '#fff',
                border: '1.5px solid var(--text-primary)',
              }}
            >
              <Sparkles size={16} />
              Перейти к тарифам
            </Link>
          </>
        )}
      </div>

      {/* ============ Секция 2.3: Способы оплаты ============
          Вернулась 2026-07-03 (была убрана 2026-05-18): компл-требование
          ЮKassa при подключении автоплатежей — юзер должен уметь отвязать
          способ оплаты самостоятельно, из ЛК, без поддержки. Показываем
          только когда есть хотя бы одна активная привязка. При отвязке
          бэк стирает рекуррент-токен из БД (не только скрывает). */}
      {PAYMENT_METHODS_UI_ENABLED && paymentMethods.length > 0 && (
        <div className="rounded-2xl border p-6" style={cardStyle}>
          <h2 className="text-lg font-semibold mb-1 flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
            <Lock size={20} style={{ color: 'var(--text-muted)' }} />
            Способы оплаты
          </h2>
          <p className="text-sm mb-4" style={{ color: 'var(--text-muted)' }}>
            Используются для авто-продления подписки. Отвязать можно в любой момент —
            авто-продление остановится, оплаченный период сохранится.
          </p>
          <div className="space-y-3">
            {paymentMethods.map((pm) => {
              const isSbp = pm.method_type === 'sbp';
              const badge = isSbp ? 'СБП' : (pm.card_brand || 'КАРТА');
              const label = isSbp
                ? (pm.display_name || 'Счёт по СБП')
                : `···· ${pm.card_last4 || '????'}`;
              return (
                <div
                  key={pm.id}
                  className="flex items-center justify-between gap-3 rounded-xl p-3 border"
                  style={{
                    borderColor: 'var(--border-color)',
                    backgroundColor: 'var(--bg-secondary)',
                  }}
                >
                  <div className="flex items-center gap-3 min-w-0">
                    <span
                      className="inline-flex items-center justify-center font-bold rounded shrink-0"
                      style={{
                        minWidth: 44,
                        height: 28,
                        padding: '0 6px',
                        fontSize: 11,
                        letterSpacing: '0.03em',
                        color: 'var(--text-primary)',
                        border: '1.5px solid var(--border-color)',
                        backgroundColor: 'var(--bg-primary)',
                      }}
                    >
                      {badge.toUpperCase()}
                    </span>
                    <span className="text-sm truncate" style={{ color: 'var(--text-secondary)' }}>
                      {label}
                    </span>
                    {pm.is_default && paymentMethods.length > 1 && (
                      <span className="text-xs shrink-0" style={{ color: 'var(--text-muted)' }}>
                        основной
                      </span>
                    )}
                  </div>
                  <button
                    onClick={() => handleDeletePaymentMethod(pm.id)}
                    disabled={pmActionId === pm.id}
                    className="shrink-0 px-3 py-1.5 rounded-full text-sm font-medium transition-opacity hover:opacity-80 disabled:opacity-50"
                    style={{
                      background: 'transparent',
                      border: '1.5px solid var(--border-color)',
                      color: 'var(--text-muted)',
                    }}
                  >
                    {pmActionId === pm.id ? 'Отвязываем…' : 'Отвязать'}
                  </button>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ============ Секция 2.4: История платежей ============
          Показываем когда у юзера есть хотя бы 1 запись в history. Свёрнут по
          умолчанию — кликабельный header «История платежей (N)». При раскрытии
          таблица с datetime, tier, amount, status. Раскрытие — useState toggle,
          без анимации (минималистичный editorial-стиль). */}
      {history.length > 0 && (
        <div className="rounded-2xl border" style={cardStyle}>
          <button
            onClick={() => setHistoryExpanded(v => !v)}
            className="w-full flex items-center justify-between p-6 text-left"
            style={{ background: 'transparent', border: 'none' }}
          >
            <h2 className="text-lg font-semibold flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
              <Calendar size={20} style={{ color: 'var(--text-muted)' }} />
              История платежей
              <span style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-sm)', fontWeight: 400 }}>
                ({history.length})
              </span>
            </h2>
            <span style={{ color: 'var(--text-muted)', fontSize: 18 }}>
              {historyExpanded ? '−' : '+'}
            </span>
          </button>

          {historyExpanded && (
            <div className="px-6 pb-6">
              <div className="space-y-2">
                {history.map(h => {
                  // Status → цветной бейдж
                  const statusColor =
                    h.status === 'active' ? 'var(--success, #22c55e)'
                    : h.status === 'pending' ? 'var(--accent)'
                    : h.status === 'refunded' || h.status === 'cancelled' || h.status === 'expired' ? 'var(--text-muted)'
                    : h.status === 'failed' ? 'var(--danger)'
                    : 'var(--text-muted)';
                  const statusLabel =
                    h.status === 'active' ? 'активна'
                    : h.status === 'pending' ? 'ожидает оплаты'
                    : h.status === 'refunded' ? 'возврат'
                    : h.status === 'cancelled' ? 'отменена'
                    : h.status === 'expired' ? 'истекла'
                    : h.status === 'failed' ? 'не оплачена'
                    : h.status;
                  return (
                    <div
                      key={h.id}
                      className="flex items-center justify-between rounded-xl p-3 border"
                      style={{
                        borderColor: 'var(--border-color)',
                        backgroundColor: 'var(--bg-secondary)',
                      }}
                    >
                      <div className="flex-1 min-w-0">
                        <div className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>
                          {TIER_LABELS[h.tier] || h.tier}
                          {h.plan_id?.endsWith('_yearly') && (
                            <span className="ml-2 text-xs" style={{ color: 'var(--text-muted)' }}>год</span>
                          )}
                          {h.plan_id?.endsWith('_monthly') && (
                            <span className="ml-2 text-xs" style={{ color: 'var(--text-muted)' }}>месяц</span>
                          )}
                        </div>
                        <div className="text-xs mt-0.5" style={{ color: 'var(--text-muted)' }}>
                          {h.created_at ? formatDate(h.created_at) : '—'}
                        </div>
                      </div>
                      <div className="flex items-center gap-3 flex-shrink-0">
                        <div className="text-right">
                          <div className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>
                            {h.amount !== null ? `${h.amount.toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽` : '—'}
                          </div>
                          <div
                            className="text-[10px] uppercase tracking-wider mt-0.5"
                            style={{ color: statusColor, fontWeight: 600 }}
                          >
                            {statusLabel}
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
              <p className="mt-4 text-xs" style={{ color: 'var(--text-muted)' }}>
                Возврат денег — через поддержку{' '}
                <a href="mailto:frameinfo@mail.ru" style={{ color: 'var(--accent)' }} className="hover:underline">
                  frameinfo@mail.ru
                </a>
                . Чеки 54-ФЗ приходят на email сразу после оплаты.
              </p>
            </div>
          )}
        </div>
      )}

      {/* ============ Секция 2.5: Admin — invite-ссылки (только для admin'ов) ============ */}
      {user.role === 'admin' && <AdminBillingInvites />}

      {/* ============ Секция 3: Безопасность ============
          Скрыта для OAuth-юзеров (Telegram/VK/Google), у которых нет
          пароля и менять нечего. Единственный контент секции —
          форма смены пароля. */}
      {user.has_password && (
      <div className="rounded-2xl border p-6" style={cardStyle}>
        {/* marginBottom — ИНЛАЙН: на мобиле `.fm-app h2 { margin:0 }` (mobile.css)
            перебивает Tailwind mb-*, поэтому отступ задаём инлайном (бьёт всё). */}
        <h2 className="text-lg font-semibold flex items-center gap-2" style={{ color: 'var(--text-primary)', marginBottom: '2rem' }}>
          <Shield size={20} style={{ color: 'var(--text-muted)' }} />
          Безопасность
        </h2>

        {/* Форма смены пароля — единственный контент секции, поэтому без
            отдельного подзаголовка: заголовок «Безопасность» + плейсхолдеры
            полей самодостаточны (раньше «Смена пароля» липло к инпуту). */}
        <div className="mb-2">
            <form onSubmit={handlePasswordChange} className="space-y-3">
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4" style={{ color: 'var(--text-muted)' }} />
                <input
                  type={showCurrentPw ? 'text' : 'password'}
                  value={currentPassword}
                  onChange={e => setCurrentPassword(e.target.value)}
                  placeholder="Текущий пароль"
                  required
                  className="w-full pl-10 pr-11 py-2.5 rounded-xl border outline-none transition-all text-sm"
                  style={inputStyle}
                />
                <button
                  type="button"
                  onClick={() => setShowCurrentPw(!showCurrentPw)}
                  className="absolute right-3 top-1/2 -translate-y-1/2"
                  style={{ color: 'var(--text-muted)' }}
                >
                  {showCurrentPw ? <EyeOff size={16} /> : <Eye size={16} />}
                </button>
              </div>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4" style={{ color: 'var(--text-muted)' }} />
                <input
                  type={showNewPw ? 'text' : 'password'}
                  value={newPassword}
                  onChange={e => setNewPassword(e.target.value)}
                  placeholder="Новый пароль (минимум 8 символов)"
                  required
                  minLength={8}
                  className="w-full pl-10 pr-11 py-2.5 rounded-xl border outline-none transition-all text-sm"
                  style={inputStyle}
                />
                <button
                  type="button"
                  onClick={() => setShowNewPw(!showNewPw)}
                  className="absolute right-3 top-1/2 -translate-y-1/2"
                  style={{ color: 'var(--text-muted)' }}
                >
                  {showNewPw ? <EyeOff size={16} /> : <Eye size={16} />}
                </button>
              </div>
              {pwMsg && (
                <div
                  className="p-3 rounded-xl text-sm"
                  style={{
                    backgroundColor: pwMsg.type === 'ok'
                      ? 'color-mix(in srgb, var(--accent) 15%, transparent)'
                      : 'color-mix(in srgb, #ef4444 15%, transparent)',
                    color: pwMsg.type === 'ok' ? 'var(--accent)' : 'var(--danger)',
                  }}
                >
                  {pwMsg.text}
                </div>
              )}
              <button
                type="submit"
                disabled={pwLoading}
                className="flex items-center gap-2 px-5 py-2.5 rounded-xl text-sm font-medium transition-opacity disabled:opacity-50"
                style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
              >
                <Lock size={16} />
                {pwLoading ? 'Сохранение...' : 'Сменить пароль'}
              </button>
            </form>
          </div>

        {/* Секция «Способы входа» удалена: на финансовой платформе для
            обычного юзера это шум. Линкование OAuth-провайдеров остаётся
            доступным через /login flow при необходимости. */}
      </div>
      )}

      {/* ============ Секция: API-ключи ============
          KILL-SWITCH: скрыто до официального запуска (config/features.ts). */}
      {API_CSV_ENABLED && (
        <div className="rounded-2xl border p-6" style={cardStyle}>
          <ApiKeysSection />
        </div>
      )}

      {/* ============ Секция: Telegram-алерты ============ */}
      {/* ВРЕМЕННО скрыто (ALERTS_ENABLED): доставка в Telegram не работает (IPv6 хоста). */}
      {ALERTS_ENABLED && (
        <div className="rounded-2xl border p-6" style={cardStyle}>
          <TelegramAlertsSection />
        </div>
      )}

      {/* ============ Секция: Расширение для терминала ============ */}
      <div className="rounded-2xl border p-6" style={cardStyle}>
        <ExtensionTokenSection />
      </div>

      {/* ============ Секция: Конфиденциальность ============ */}
      <PrivacyOptOutSection />

      {/* ============ Секция 4: Выход ============ */}
      <button
        onClick={handleLogout}
        className="w-full flex items-center justify-center gap-2 py-3 rounded-2xl border text-sm font-medium transition-all hover:opacity-80"
        style={{
          borderColor: 'var(--danger)',
          color: 'var(--danger)',
          backgroundColor: 'color-mix(in srgb, #ef4444 10%, transparent)',
        }}
      >
        <LogOut size={16} />
        Выйти из аккаунта
      </button>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════
// Privacy section — opt-out из аналитики через cookie + localStorage
// ════════════════════════════════════════════════════════════════════════

function PrivacyOptOutSection() {
  // Читаем текущий статус из cookie. Если установлен '1' — opted out.
  const isOptedOut = (() => {
    try {
      return document.cookie.split('; ').some(c => c.startsWith('frame_analytics_optout=1'));
    } catch {
      return false;
    }
  })();

  const [optedOut, setOptedOut] = useState(isOptedOut);

  const toggle = () => {
    const next = !optedOut;
    if (next) {
      // Set cookie на 10 лет
      document.cookie = 'frame_analytics_optout=1; path=/; max-age=315360000; SameSite=Lax';
      // Также reset consent в localStorage чтобы баннер появился снова если user захочет вернуть
      try { localStorage.removeItem('frame_consent_v1'); } catch { /* ignore */ }
    } else {
      // Удаляем cookie
      document.cookie = 'frame_analytics_optout=; path=/; max-age=0; SameSite=Lax';
    }
    setOptedOut(next);
  };

  return (
    <div className="mb-4 p-4 rounded-2xl border" style={{
      borderColor: 'var(--border-color)',
      backgroundColor: 'var(--bg-secondary)',
    }}>
      <h3 className="font-semibold mb-1" style={{ color: 'var(--text-primary)', fontSize: 'var(--fs-base)' }}>
        Аналитика
      </h3>
      <p className="mb-3" style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
        Мы собираем обезличенную статистику посещений (страницы, события, время) для улучшения сервиса.{' '}
        <Link to="/privacy" style={{ color: 'var(--accent)', textDecoration: 'underline' }}>Подробнее</Link>
      </p>
      <label className="flex items-center gap-2 cursor-pointer">
        <input
          type="checkbox"
          checked={optedOut}
          onChange={toggle}
          style={{ width: 16, height: 16, accentColor: 'var(--accent)' }}
        />
        <span style={{ color: 'var(--text-primary)', fontSize: 'var(--fs-sm)' }}>
          Не собирать аналитику обо мне
        </span>
      </label>
    </div>
  );
}
