import { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import {
  LogOut, Lock, Mail, Calendar, Shield, Crown,
  Check, X as XIcon, Eye, EyeOff, Sparkles,
} from 'lucide-react';
import AdminBillingInvites from '../components/AdminBillingInvites';

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

      {/* ============ Секция 2: Подписка (заглушка) ============ */}
      <div className="rounded-2xl border p-6" style={cardStyle}>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
            <Crown size={20} style={{ color: 'var(--accent)' }} />
            Подписка
          </h2>
          <span
            className="px-3 py-1 text-xs font-medium rounded-full"
            style={{
              backgroundColor: 'color-mix(in srgb, #2EE59D 15%, transparent)',
              color: 'var(--accent)',
            }}
          >
            Бесплатный план
          </span>
        </div>

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
                <Check size={16} className="text-emerald-400 shrink-0" />
              ) : (
                <XIcon size={16} className="shrink-0" style={{ color: 'var(--text-muted)' }} />
              )}
              <span style={{ color: item.ok ? 'var(--text-secondary)' : 'var(--text-muted)' }}>
                {item.text}
                {!item.ok && <span className="ml-1 text-xs opacity-60">— Plus</span>}
              </span>
            </div>
          ))}
        </div>

        <Link
          to="/pricing"
          className="w-full flex items-center justify-center gap-2 py-3 rounded-xl text-sm font-medium transition-opacity hover:opacity-90"
          style={{
            backgroundColor: 'var(--accent-pink)',
            color: '#fff',
          }}
        >
          <Sparkles size={16} />
          Перейти к тарифам
        </Link>
      </div>

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

            {/* OAuth провайдеры */}
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
                    <span className="text-xs text-emerald-400 font-medium">Подключено</span>
                  ) : (
                    <span className="text-xs" style={{ color: 'var(--text-muted)' }}>Скоро</span>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>

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
