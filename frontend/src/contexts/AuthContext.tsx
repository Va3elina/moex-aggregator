import { createContext, useContext, useState, useEffect, useCallback, type ReactNode } from 'react';
import { apiFetch } from '../services/api';
import { hydrateSettingsFromServer } from '../services/settingsSync';

interface User {
  id: number;
  email: string;
  display_name: string | null;
  role: string;
  /** Подтверждён ли email. У email+password юзеров false до ввода кода из письма;
   *  у OAuth-юзеров true (провайдер подтвердил). Управляет баннером в профиле. */
  is_verified: boolean;
  has_password: boolean;
  avatar_url: string | null;
  created_at: string;
  oauth_providers: string[];
  /** True если email — placeholder `*@oauth.local` (Telegram/VK без реального
   *  email). Такого юзера App редиректит на /add-email до тех пор пока не
   *  введёт реальный адрес — иначе T-Bank не выдаст чек по 54-ФЗ. */
  requires_email_setup?: boolean;
}

interface AuthContextType {
  user: User | null;
  loading: boolean;
  isAuthenticated: boolean;
  login: (tokens: { access_token: string; refresh_token: string }) => Promise<void>;
  logout: () => Promise<void>;
  refreshUser: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

// Auth-запросы идут через единый apiFetch (services/api.ts): там мьютекс
// refresh'а (refreshPromise) и 401-retry. Прежняя локальная копия
// tryRefreshToken/fetchWithAuth БЕЗ мьютекса давала гонку: параллельные
// /refresh с одним refresh-токеном → первый ротацией отзывал токен, второй
// получал 401 → ложный logout (аудит 10.06).

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  const clearTokens = useCallback(() => {
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
    setUser(null);
  }, []);

  const loadUser = useCallback(async () => {
    // Достаточно refresh-токена: apiFetch (ensureFreshToken) умеет получить
    // свежий access по нему, даже если access уже удалён/протух.
    if (!localStorage.getItem('access_token') && !localStorage.getItem('refresh_token')) {
      setUser(null);
      setLoading(false);
      return;
    }

    try {
      // apiFetch сам делает проактивный refresh (с мьютексом) и 401-retry.
      const resp = await apiFetch('/api/auth/me');

      if (resp.ok) {
        const data = await resp.json();
        setUser(data);
        // Настройки с сервера — ДО setLoading(false) (finally): страницы ещё
        // не отрендерены, usePersistedState прочитает уже гидрированный
        // localStorage. Никогда не бросает; провал = синк молча выключен.
        await hydrateSettingsFromServer();
        return;
      }

      if (resp.status === 401) {
        // refresh внутри apiFetch уже не помог — сессия мертва.
        clearTokens();
      }
    } catch {
      // Network error — keep tokens, user stays null
    } finally {
      setLoading(false);
    }
  }, [clearTokens]);

  useEffect(() => {
    loadUser();
  }, [loadUser]);

  const login = useCallback(async (tokens: { access_token: string; refresh_token: string }) => {
    localStorage.setItem('access_token', tokens.access_token);
    localStorage.setItem('refresh_token', tokens.refresh_token);
    setLoading(true);
    await loadUser();
  }, [loadUser]);

  const logout = useCallback(async () => {
    try {
      await apiFetch('/api/auth/logout', { method: 'POST' });
    } catch {
      // ignore
    }
    clearTokens();
  }, [clearTokens]);

  const refreshUser = useCallback(async () => {
    await loadUser();
  }, [loadUser]);

  // Не рендерим приложение пока auth не разрешился —
  // иначе страницы инициализируют state с гостевыми значениями,
  // а потом получают 403 при запросе данных.
  // НО: на /auth/callback/* не блокируем — AuthCallback сам покажет свой спиннер.
  const isAuthCallback = typeof window !== 'undefined' && window.location.pathname.startsWith('/auth/callback');
  if (loading && !isAuthCallback) {
    return (
      <div style={{
        height: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: 'var(--color-bg, #0a0a0f)',
        color: 'var(--color-text-secondary, #888)',
      }}>
        Загрузка...
      </div>
    );
  }

  return (
    <AuthContext.Provider value={{
      user,
      loading,
      isAuthenticated: !!user,
      login,
      logout,
      refreshUser,
    }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
