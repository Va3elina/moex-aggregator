import { useState, useEffect } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import {
  LogOut, Lock, Mail, Calendar, Shield, Crown,
  Check, X as XIcon, Eye, EyeOff, Sparkles, ExternalLink,
} from 'lucide-react';
import AdminBillingInvites from '../components/AdminBillingInvites';

interface BillingStatus {
  tier: string;
  is_active: boolean;
  subscription_id: number | null;
  plan_id: string | null;
  started_at: string | null;
  expires_at: string | null;
  cancelled_at: string | null;  // NULL → активна, NOT NULL → отменена (доступ до expires_at)
}

interface PaymentMethod {
  id: number;
  provider: string;
  card_last4: string | null;
  card_brand: string | null;   // 'VISA' | 'MASTERCARD' | 'MIR' | null
  display_name: string;         // 'VISA ····0333' для UI
  is_default: boolean;
  last_used_at: string | null;
  created_at: string | null;
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

const TIER_LABELS: Record<string, string> = {
  free: 'Бесплатный план',
  basic: 'Basic',
  pro: 'Pro',
  premium: 'Premium',
};

// Tier-specific features. Каждый next tier включает всё предыдущее (transitively).
// Здесь только incremental — что добавляется к предыдущему уровню.
const TIER_FEATURES: Record<string, { tagline: string; features: string[] }> = {
  basic: {
    tagline: 'Расширенный доступ к историческим данным и сезонности.',
    features: [
      'Все индикаторы и инструменты',
      'Полная история данных (без 1y guest-лимита)',
      'Сезонность по дням недели / месяцам / годам',
      'Полная карта рынка с фильтрами',
      'Экспорт PNG-графиков с разметкой',
    ],
  },
  pro: {
    tagline: 'Профессиональный уровень с intraday-данными.',
    features: [
      'Всё из Basic',
      'Часовой таймфрейм (1ч intraday OI и Seasonality)',
      'Все режимы Сезонности (внутри дня + median режим)',
      'Расширенный screener в обзоре',
      'Приоритетная поддержка',
    ],
  },
  premium: {
    tagline: 'Максимум — 5-минутные данные и real-time WebSocket.',
    features: [
      'Всё из Pro',
      '5-минутный таймфрейм (highest resolution)',
      'Real-time WebSocket-обновления (без 5-мин SSE-задержки)',
      'Heavy-weight backtest и screening без лимитов',
      'Эксклюзивный доступ к новым feature\'ам в beta',
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

  // Сохранённые карты для recurring payments (T-Bank RebillId).
  const [paymentMethods, setPaymentMethods] = useState<PaymentMethod[]>([]);
  const [pmActionId, setPmActionId] = useState<number | null>(null);

  // История платежей — последние N подписок любого статуса
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [historyExpanded, setHistoryExpanded] = useState(false);

  const fetchBilling = () => {
    fetch('/api/billing/status', {
      headers: { Authorization: `Bearer ${localStorage.getItem('access_token')}` },
    })
      .then(r => r.ok ? r.json() : null)
      .then(setBilling)
      .catch(() => setBilling(null));
  };
  const fetchPaymentMethods = () => {
    fetch('/api/billing/payment_methods', {
      headers: { Authorization: `Bearer ${localStorage.getItem('access_token')}` },
    })
      .then(r => r.ok ? r.json() : { items: [] })
      .then(d => setPaymentMethods(d.items || []))
      .catch(() => setPaymentMethods([]));
  };
  const fetchHistory = () => {
    fetch('/api/billing/history?limit=20', {
      headers: { Authorization: `Bearer ${localStorage.getItem('access_token')}` },
    })
      .then(r => r.ok ? r.json() : { items: [] })
      .then(d => setHistory(d.items || []))
      .catch(() => setHistory([]));
  };
  useEffect(() => {
    fetchBilling();
    fetchPaymentMethods();
    fetchHistory();
  }, []);

  // Удалить сохранённую карту (soft delete на бэке — auto-renewal на этой
  // карте больше не сработает, но активные подписки продолжают работать
  // до expires_at).
  const handleDeletePaymentMethod = async (pmId: number) => {
    if (!window.confirm(
      'Удалить карту?\n\n' +
      'Авто-продление подписок на этой карте перестанет работать. ' +
      'Текущая подписка продолжит действовать до окончания оплаченного периода. ' +
      'Сама карта удалена не будет — её можно привязать снова при следующей оплате.'
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
  const handleSubAction = async (action: 'cancel' | 'resume') => {
    if (!billing?.subscription_id) return;
    if (action === 'cancel' && !window.confirm(
      'Отменить подписку?\n\nАвто-продление будет отключено, но доступ к Pro-функциям останется до конца оплаченного периода.'
    )) return;
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
    }
  };

  // OAuth connect — редирект на провайдера. Если backend увидит совпадение
  // email с текущим аккаунтом → присоединит OAuth identity. Если другой email →
  // создаст отдельный аккаунт (не идеально, но MVP — UX лучше "Скоро").
  const handleOAuthConnect = async (provider: string) => {
    if (provider === 'telegram') {
      const botId = '8604817597';
      const origin = encodeURIComponent(window.location.origin);
      const returnTo = encodeURIComponent(window.location.origin + '/auth/callback/telegram');
      window.location.href = `https://oauth.telegram.org/auth?bot_id=${botId}&origin=${origin}&embed=0&request_access=write&return_to=${returnTo}`;
      return;
    }
    try {
      const resp = await fetch(`/api/auth/oauth/${provider}/url`);
      const data = await resp.json();
      if (resp.ok && data.url) {
        // localStorage не sessionStorage — VK на iPhone открывает new tab,
        // sessionStorage isolated per tab. См. LoginPage:115 для деталей.
        if (data.code_verifier) localStorage.setItem('vk_code_verifier', data.code_verifier);
        if (data.device_id) localStorage.setItem('vk_device_id', data.device_id);
        window.location.href = data.url;
      }
    } catch {
      /* ignore */
    }
  };

  if (!user) {
    navigate('/login');
    return null;
  }

  const displayName = user.display_name || user.email;
  const isOAuthLocal = user.email.endsWith('@oauth.local');
  const initials = displayName[0].toUpperCase();
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
          {/* Аватар — инициал */}
          <div
            className="w-16 h-16 rounded-full flex items-center justify-center text-2xl font-bold shrink-0"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            {initials}
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

            {billing.plan_id && (
              <p className="text-xs mb-4" style={{ color: 'var(--text-muted)' }}>
                План: <span style={{ fontFamily: "'IBM Plex Mono', monospace" }}>{billing.plan_id}</span>
                {billing.started_at && ` · с ${formatDate(billing.started_at)}`}
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

      {/* ============ Секция 2.3: Сохранённые карты ============
          Показываем когда у user'а есть хотя бы 1 сохранённая карта.
          У каждой — brand + last4 (e.g. «VISA ····0333»), default-индикатор
          и кнопка «Удалить». T-Bank продолжает хранить карту у себя
          (юзер не «отвязывает», а скрывает у нас — auto-renewal перестаёт
          работать). */}
      {paymentMethods.length > 0 && (
        <div className="rounded-2xl border p-6" style={cardStyle}>
          <h2 className="text-lg font-semibold mb-4 flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
            <Lock size={20} style={{ color: 'var(--text-muted)' }} />
            Сохранённые карты
          </h2>
          <div className="space-y-3">
            {paymentMethods.map((pm) => (
              <div
                key={pm.id}
                className="flex items-center justify-between rounded-xl p-3 border"
                style={{
                  borderColor: 'var(--border-color)',
                  backgroundColor: 'var(--bg-secondary)',
                }}
              >
                <div className="flex items-center gap-3">
                  {/* Иконка карты — простой brand badge */}
                  <span
                    className="inline-flex items-center justify-center font-bold rounded"
                    style={{
                      width: 44,
                      height: 28,
                      background:
                        pm.card_brand === 'VISA' ? '#1A1F71'
                        : pm.card_brand === 'MASTERCARD' ? '#EB001B'
                        : pm.card_brand === 'MIR' ? '#0F754E'
                        : '#3C3C3C',
                      color: '#fff',
                      fontSize: 11,
                      letterSpacing: '0.04em',
                    }}
                    aria-label={pm.card_brand || 'Карта'}
                  >
                    {pm.card_brand || 'CARD'}
                  </span>
                  <div>
                    <div className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>
                      ····{pm.card_last4 || '????'}
                      {pm.is_default && (
                        <span
                          className="ml-2 px-1.5 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider"
                          style={{
                            backgroundColor: 'var(--accent)',
                            color: '#fff',
                          }}
                        >
                          основная
                        </span>
                      )}
                    </div>
                    <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
                      {pm.last_used_at
                        ? `Использована ${formatDate(pm.last_used_at)}`
                        : 'Ещё не использовалась'}
                    </div>
                  </div>
                </div>
                <button
                  onClick={() => handleDeletePaymentMethod(pm.id)}
                  disabled={pmActionId === pm.id}
                  className="inline-flex items-center gap-1 px-3 py-1.5 rounded-full text-xs font-medium transition-opacity hover:opacity-90 disabled:opacity-50"
                  style={{
                    backgroundColor: 'transparent',
                    color: 'var(--danger)',
                    border: '1.5px solid var(--danger)',
                  }}
                >
                  <XIcon size={12} />
                  {pmActionId === pm.id ? 'Удаляем…' : 'Удалить'}
                </button>
              </div>
            ))}
          </div>
          <p className="mt-4 text-xs" style={{ color: 'var(--text-muted)' }}>
            При удалении карта остаётся у Т-Банка, но мы её больше не используем.
            Активные подписки продолжат работать до окончания оплаченного периода.
          </p>
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

      {/* ============ Секция 3: Безопасность ============ */}
      <div className="rounded-2xl border p-6" style={cardStyle}>
        <h2 className="text-lg font-semibold mb-4 flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
          <Shield size={20} style={{ color: 'var(--text-muted)' }} />
          Безопасность
        </h2>

        {/* Смена пароля */}
        {user.has_password && (
          <div className="mb-6">
            <h3 className="text-sm font-medium mb-3" style={{ color: 'var(--text-secondary)' }}>
              Смена пароля
            </h3>
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
        )}

        {/* Привязанные провайдеры */}
        <div>
          <h3 className="text-sm font-medium mb-3" style={{ color: 'var(--text-secondary)' }}>
            Способы входа
          </h3>
          <div className="space-y-2">
            {/* Email + пароль */}
            <div
              className="flex items-center justify-between px-4 py-3 rounded-xl border text-sm"
              style={{ borderColor: 'var(--border-color)' }}
            >
              <div className="flex items-center gap-2" style={{ color: 'var(--text-secondary)' }}>
                <Mail size={16} />
                <span>Email + пароль</span>
              </div>
              {user.has_password ? (
                <span className="text-xs text-emerald-400 font-medium">Подключено</span>
              ) : (
                <span className="text-xs" style={{ color: 'var(--text-muted)' }}>Не задан</span>
              )}
            </div>

            {/* OAuth провайдеры — все четыре подключены, можно линковать аккаунт.
                NB: если email текущего user'а отличается от email из OAuth provider —
                backend создаст отдельный аккаунт. Это MVP-trade-off:
                proper "link to existing account" требует backend endpoint
                /api/auth/oauth/link с auth + state-token. */}
            {['Google', 'ВКонтакте', 'Яндекс', 'Telegram'].map(name => {
              const key = name === 'ВКонтакте' ? 'vk' : name === 'Яндекс' ? 'yandex' : name.toLowerCase();
              const connected = user.oauth_providers.includes(key);
              return (
                <div
                  key={key}
                  className="flex items-center justify-between px-4 py-3 rounded-xl border text-sm"
                  style={{ borderColor: 'var(--border-color)' }}
                >
                  <div className="flex items-center gap-2" style={{ color: 'var(--text-secondary)' }}>
                    <Shield size={16} style={{ color: 'var(--text-muted)' }} />
                    <span>{name}</span>
                  </div>
                  {connected ? (
                    <span
                      className="text-xs font-medium px-2 py-1 rounded-full"
                      style={{
                        backgroundColor: 'color-mix(in srgb, var(--success) 18%, transparent)',
                        color: 'var(--success)',
                      }}
                    >
                      Подключено
                    </span>
                  ) : (
                    <button
                      onClick={() => handleOAuthConnect(key)}
                      className="text-xs font-medium px-3 py-1 rounded-full transition-opacity hover:opacity-80"
                      style={{
                        backgroundColor: 'var(--bg-primary)',
                        color: 'var(--accent)',
                        border: '1.5px solid var(--accent)',
                      }}
                    >
                      Подключить
                    </button>
                  )}
                </div>
              );
            })}
          </div>
        </div>
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
