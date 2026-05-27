import { createContext, useContext, useState, useEffect, useCallback, type ReactNode } from 'react';

interface User {
  id: number;
  email: string;
  display_name: string | null;
  role: string;
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

function isTokenExpiringSoon(token: string): boolean {
  try {
    const payload = JSON.parse(atob(token.split('.')[1]));
    const exp = payload.exp * 1000; // ms
    return exp - Date.now() < 60_000; // меньше 1 минуты до истечения
  } catch {
    return true;
  }
}

async function tryRefreshToken(): Promise<string | null> {
  const refreshToken = localStorage.getItem('refresh_token');
  if (!refreshToken) return null;

  try {
    const resp = await fetch('/api/auth/refresh', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });
    if (resp.ok) {
      const tokens = await resp.json();
      localStorage.setItem('access_token', tokens.access_token);
      localStorage.setItem('refresh_token', tokens.refresh_token);
      return tokens.access_token;
    }
  } catch { /* ignore */ }
  return null;
}

async function fetchWithAuth(url: string, options: RequestInit = {}): Promise<Response> {
  let token = localStorage.getItem('access_token');

  // Проактивный refresh если токен скоро протухнет
  if (token && isTokenExpiringSoon(token)) {
    const newToken = await tryRefreshToken();
    if (newToken) token = newToken;
  }

  return fetch(url, {
    ...options,
    headers: {
      ...options.headers,
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  const clearTokens = useCallback(() => {
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
    setUser(null);
  }, []);

  const loadUser = useCallback(async () => {
    const token = localStorage.getItem('access_token');
    if (!token) {
      setUser(null);
      setLoading(false);
      return;
    }

    try {
      const resp = await fetchWithAuth('/api/auth/me');

      if (resp.ok) {
        const data = await resp.json();
        setUser(data);
        return;
      }

      if (resp.status === 401) {
        // Try refresh
        const refreshToken = localStorage.getItem('refresh_token');
        if (!refreshToken) {
          clearTokens();
          return;
        }

        const refreshResp = await fetch('/api/auth/refresh', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ refresh_token: refreshToken }),
        });

        if (refreshResp.ok) {
          const tokens = await refreshResp.json();
          localStorage.setItem('access_token', tokens.access_token);
          localStorage.setItem('refresh_token', tokens.refresh_token);

          const retryResp = await fetchWithAuth('/api/auth/me');
          if (retryResp.ok) {
            const data = await retryResp.json();
            setUser(data);
            return;
          }
        }

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
      await fetchWithAuth('/api/auth/logout', { method: 'POST' });
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
