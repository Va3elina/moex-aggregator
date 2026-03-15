import { useState } from 'react';
import { NavLink, Outlet, useNavigate } from 'react-router-dom';
import { useTheme } from '../contexts/ThemeContext';
import { useAuth } from '../contexts/AuthContext';
import { useSSE } from '../hooks/useSSE';
import { Menu, X, LogIn } from 'lucide-react';

const NAV_ITEMS: { path: string; label: string; disabled?: boolean }[] = [
  { path: '/fear', label: 'Индекс страха' },
  { path: '/heatmap', label: 'Карта рынка' },
  { path: '/oi', label: 'Открытый интерес' },
  { path: '/funds-money', label: 'Деньги в фондах' },
  { path: '/buffett', label: 'Индикатор Баффетта' },
  { path: '/strength', label: 'Сила рынка' },
];

export default function Layout() {
  const { cycleTheme, themeName, themeIcon } = useTheme();
  const { user, isAuthenticated } = useAuth();
  const { connected } = useSSE();
  const navigate = useNavigate();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  return (
    <div className="min-h-screen" style={{ backgroundColor: 'var(--bg-primary)' }}>
      {/* Sticky Header с glass эффектом */}
      <nav className="sticky top-0 z-50 backdrop-blur-xl border-b" style={{
        backgroundColor: 'var(--glass-bg)',
        borderColor: 'var(--border-color)'
      }}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-14 md:h-16">
            {/* Логотип */}
            <NavLink to="/" className="flex items-center gap-2">
              <span className="text-lg md:text-xl font-bold" style={{ color: 'var(--accent)' }}>Фрейм</span>
              <span className="px-2 py-0.5 text-xs font-medium rounded-full hidden sm:inline" style={{
                color: 'var(--text-secondary)',
                backgroundColor: 'var(--border-color)'
              }}>
                Beta
              </span>
              <span
                title={connected ? 'Live: данные обновляются автоматически' : 'Нет соединения с сервером'}
                className={`w-2 h-2 rounded-full hidden sm:inline-block ${connected ? 'bg-emerald-400' : 'bg-gray-500'}`}
              />
            </NavLink>

            {/* Desktop Навигация */}
            <div className="hidden lg:flex items-center gap-1">
              {NAV_ITEMS.map((item) => (
                <NavLink
                  key={item.path}
                  to={item.disabled ? '#' : item.path}
                  onClick={(e) => item.disabled && e.preventDefault()}
                  className={({ isActive }) => `
                    px-3 xl:px-4 py-2 rounded-xl text-sm font-medium 
                    whitespace-nowrap transition-all
                    ${item.disabled
                      ? 'cursor-not-allowed'
                      : isActive
                        ? 'border'
                        : ''
                    }
                  `}
                  style={({ isActive }) => ({
                    color: item.disabled
                      ? 'var(--text-muted)'
                      : isActive
                        ? 'var(--accent)'
                        : 'var(--text-secondary)',
                    backgroundColor: isActive ? 'color-mix(in srgb, var(--accent) 10%, transparent)' : undefined,
                    borderColor: isActive ? 'color-mix(in srgb, var(--accent) 30%, transparent)' : undefined,
                  })}
                >
                  {item.label}
                </NavLink>
              ))}
            </div>

            {/* Right side */}
            <div className="flex items-center gap-2 md:gap-3">
              {/* Theme Toggle */}
              <button
                onClick={cycleTheme}
                className="flex items-center gap-1 md:gap-2 px-2 md:px-3 py-2 rounded-lg transition-all hover:bg-white/10"
                style={{
                  color: 'var(--text-secondary)',
                  borderColor: 'var(--border-color)'
                }}
                title={`Тема: ${themeName}. Нажми для смены`}
              >
                <span className="text-base md:text-lg">{themeIcon}</span>
                <span className="text-xs font-medium hidden md:inline" style={{ color: 'var(--text-muted)' }}>
                  {themeName}
                </span>
              </button>

              {/* Auth button */}
              {isAuthenticated ? (
                <button
                  onClick={() => navigate('/profile')}
                  className="w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold transition-opacity hover:opacity-80"
                  style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
                  title="Личный кабинет"
                >
                  {(user?.email || '?')[0].toUpperCase()}
                </button>
              ) : (
                <button
                  onClick={() => navigate('/login')}
                  className="flex items-center gap-1.5 px-3 py-2 rounded-xl text-sm font-medium transition-all hover:bg-white/10"
                  style={{ color: 'var(--text-secondary)' }}
                >
                  <LogIn size={16} />
                  <span className="hidden md:inline">Войти</span>
                </button>
              )}

              {/* Plus версия - скрыта на мобильных */}
              <button
                className="hidden sm:block px-3 md:px-4 py-2 text-white text-sm font-medium rounded-xl transition-colors"
                style={{ backgroundColor: 'var(--accent-pink)' }}
              >
                Plus
              </button>

              {/* Mobile Menu Button */}
              <button
                onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                className="lg:hidden p-2 rounded-lg transition-all hover:bg-white/10"
                style={{ color: 'var(--text-secondary)' }}
              >
                {mobileMenuOpen ? <X size={24} /> : <Menu size={24} />}
              </button>
            </div>
          </div>
        </div>

        {/* Mobile Menu Dropdown */}
        {mobileMenuOpen && (
          <div
            className="lg:hidden border-t"
            style={{
              backgroundColor: 'var(--bg-secondary)',
              borderColor: 'var(--border-color)'
            }}
          >
            <div className="px-4 py-3 space-y-1">
              {NAV_ITEMS.map((item) => (
                <NavLink
                  key={item.path}
                  to={item.disabled ? '#' : item.path}
                  onClick={(e) => {
                    if (item.disabled) {
                      e.preventDefault();
                    } else {
                      setMobileMenuOpen(false);
                    }
                  }}
                  className={({ isActive }) => `
                    block px-4 py-3 rounded-xl text-sm font-medium transition-all
                    ${item.disabled
                      ? 'cursor-not-allowed'
                      : isActive
                        ? 'border'
                        : ''
                    }
                  `}
                  style={({ isActive }) => ({
                    color: item.disabled
                      ? 'var(--text-muted)'
                      : isActive
                        ? 'var(--accent)'
                        : 'var(--text-secondary)',
                    backgroundColor: isActive ? 'color-mix(in srgb, var(--accent) 10%, transparent)' : undefined,
                    borderColor: isActive ? 'color-mix(in srgb, var(--accent) 30%, transparent)' : undefined,
                  })}
                >
                  {item.label}
                </NavLink>
              ))}

              {/* Auth в мобильном меню */}
              {isAuthenticated ? (
                <button
                  onClick={() => { navigate('/profile'); setMobileMenuOpen(false); }}
                  className="w-full mt-3 flex items-center gap-3 px-4 py-3 rounded-xl text-sm font-medium transition-all"
                  style={{ color: 'var(--text-secondary)', backgroundColor: 'color-mix(in srgb, var(--accent) 10%, transparent)' }}
                >
                  <div
                    className="w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold"
                    style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
                  >
                    {(user?.email || '?')[0].toUpperCase()}
                  </div>
                  Личный кабинет
                </button>
              ) : (
                <button
                  onClick={() => { navigate('/login'); setMobileMenuOpen(false); }}
                  className="w-full mt-3 flex items-center justify-center gap-2 px-4 py-3 rounded-xl text-sm font-medium transition-all"
                  style={{ color: 'var(--accent)', backgroundColor: 'color-mix(in srgb, var(--accent) 10%, transparent)' }}
                >
                  <LogIn size={16} />
                  Войти
                </button>
              )}

              {/* Plus версия в мобильном меню */}
              <button
                className="w-full mt-3 px-4 py-3 text-white text-sm font-medium rounded-xl transition-colors"
                style={{ backgroundColor: 'var(--accent-pink)' }}
              >
                Plus версия
              </button>
            </div>
          </div>
        )}
      </nav>

      {/* Контент страницы */}
      <main className="relative">
          <Outlet />
      </main>
    </div>
  );
}