import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { LogOut, Save, Lock, User, Mail, Calendar, Shield } from 'lucide-react';

export default function ProfilePage() {
  const { user, logout, refreshUser } = useAuth();
  const navigate = useNavigate();

  const [username, setUsername] = useState(user?.username || '');
  const [profileLoading, setProfileLoading] = useState(false);
  const [profileMsg, setProfileMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);

  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [pwLoading, setPwLoading] = useState(false);
  const [pwMsg, setPwMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);

  if (!user) {
    navigate('/login');
    return null;
  }

  const initials = (user.username || user.email)[0].toUpperCase();

  const handleProfileSave = async (e: React.FormEvent) => {
    e.preventDefault();
    setProfileLoading(true);
    setProfileMsg(null);
    try {
      const resp = await fetch('/api/auth/profile', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${localStorage.getItem('access_token')}`,
        },
        body: JSON.stringify({ username: username || null }),
      });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || 'Ошибка');
      await refreshUser();
      setProfileMsg({ type: 'ok', text: 'Профиль обновлён' });
    } catch (err: any) {
      setProfileMsg({ type: 'err', text: err.message });
    } finally {
      setProfileLoading(false);
    }
  };

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
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || 'Ошибка');
      setCurrentPassword('');
      setNewPassword('');
      setPwMsg({ type: 'ok', text: 'Пароль изменён' });
    } catch (err: any) {
      setPwMsg({ type: 'err', text: err.message });
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

  return (
    <div className="max-w-2xl mx-auto px-4 py-8 space-y-6">
      <h1 className="text-2xl font-bold" style={{ color: 'var(--text-primary)' }}>
        Личный кабинет
      </h1>

      {/* Секция 1 — Информация */}
      <div className="rounded-2xl border p-6" style={cardStyle}>
        <div className="flex items-center gap-4 mb-6">
          <div
            className="w-16 h-16 rounded-full flex items-center justify-center text-2xl font-bold"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            {initials}
          </div>
          <div>
            <div className="text-lg font-semibold" style={{ color: 'var(--text-primary)' }}>
              {user.username || user.email}
            </div>
            <div className="text-sm" style={{ color: 'var(--text-muted)' }}>
              {user.email}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-sm">
          <div className="flex items-center gap-2" style={{ color: 'var(--text-secondary)' }}>
            <Mail size={16} />
            <span>{user.email}</span>
          </div>
          <div className="flex items-center gap-2" style={{ color: 'var(--text-secondary)' }}>
            <Shield size={16} />
            <span>Роль: {user.role}</span>
          </div>
          <div className="flex items-center gap-2" style={{ color: 'var(--text-secondary)' }}>
            <Calendar size={16} />
            <span>Регистрация: {new Date(user.created_at).toLocaleDateString('ru-RU')}</span>
          </div>
          {user.oauth_providers.length > 0 && (
            <div className="flex items-center gap-2" style={{ color: 'var(--text-secondary)' }}>
              <User size={16} />
              <span>OAuth: {user.oauth_providers.join(', ')}</span>
            </div>
          )}
        </div>
      </div>

      {/* Секция 2 — Редактирование профиля */}
      <div className="rounded-2xl border p-6" style={cardStyle}>
        <h2 className="text-lg font-semibold mb-4" style={{ color: 'var(--text-primary)' }}>
          Редактирование профиля
        </h2>
        <form onSubmit={handleProfileSave} className="space-y-4">
          <div>
            <label className="block text-sm font-medium mb-1.5" style={{ color: 'var(--text-secondary)' }}>
              Имя пользователя
            </label>
            <input
              type="text"
              value={username}
              onChange={e => setUsername(e.target.value)}
              placeholder="Введите имя"
              className="w-full px-4 py-2.5 rounded-xl border outline-none transition-all"
              style={{
                backgroundColor: 'var(--bg-primary)',
                borderColor: 'var(--border-color)',
                color: 'var(--text-primary)',
              }}
            />
          </div>
          {profileMsg && (
            <div
              className="p-3 rounded-xl text-sm"
              style={{
                backgroundColor: profileMsg.type === 'ok' ? 'color-mix(in srgb, var(--accent) 15%, transparent)' : 'color-mix(in srgb, #ef4444 15%, transparent)',
                color: profileMsg.type === 'ok' ? 'var(--accent)' : '#ef4444',
              }}
            >
              {profileMsg.text}
            </div>
          )}
          <button
            type="submit"
            disabled={profileLoading}
            className="flex items-center gap-2 px-5 py-2.5 rounded-xl text-sm font-medium transition-opacity disabled:opacity-50"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            <Save size={16} />
            {profileLoading ? 'Сохранение...' : 'Сохранить'}
          </button>
        </form>
      </div>

      {/* Секция 3 — Смена пароля (только если есть пароль) */}
      {user.has_password && (
        <div className="rounded-2xl border p-6" style={cardStyle}>
          <h2 className="text-lg font-semibold mb-4" style={{ color: 'var(--text-primary)' }}>
            Смена пароля
          </h2>
          <form onSubmit={handlePasswordChange} className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-1.5" style={{ color: 'var(--text-secondary)' }}>
                Текущий пароль
              </label>
              <input
                type="password"
                value={currentPassword}
                onChange={e => setCurrentPassword(e.target.value)}
                required
                className="w-full px-4 py-2.5 rounded-xl border outline-none transition-all"
                style={{
                  backgroundColor: 'var(--bg-primary)',
                  borderColor: 'var(--border-color)',
                  color: 'var(--text-primary)',
                }}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5" style={{ color: 'var(--text-secondary)' }}>
                Новый пароль
              </label>
              <input
                type="password"
                value={newPassword}
                onChange={e => setNewPassword(e.target.value)}
                required
                minLength={8}
                className="w-full px-4 py-2.5 rounded-xl border outline-none transition-all"
                style={{
                  backgroundColor: 'var(--bg-primary)',
                  borderColor: 'var(--border-color)',
                  color: 'var(--text-primary)',
                }}
              />
            </div>
            {pwMsg && (
              <div
                className="p-3 rounded-xl text-sm"
                style={{
                  backgroundColor: pwMsg.type === 'ok' ? 'color-mix(in srgb, var(--accent) 15%, transparent)' : 'color-mix(in srgb, #ef4444 15%, transparent)',
                  color: pwMsg.type === 'ok' ? 'var(--accent)' : '#ef4444',
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

      {/* Кнопка выхода */}
      <button
        onClick={handleLogout}
        className="w-full flex items-center justify-center gap-2 py-3 rounded-2xl border text-sm font-medium transition-all hover:opacity-80"
        style={{
          borderColor: '#ef4444',
          color: '#ef4444',
          backgroundColor: 'color-mix(in srgb, #ef4444 10%, transparent)',
        }}
      >
        <LogOut size={16} />
        Выйти из аккаунта
      </button>
    </div>
  );
}
