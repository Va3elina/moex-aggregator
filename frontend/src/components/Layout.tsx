import { useState } from 'react';
import { NavLink, Outlet, useNavigate } from 'react-router-dom';
import { useTheme } from '../contexts/ThemeContext';
import { useAuth } from '../contexts/AuthContext';
import { useSSE } from '../hooks/useSSE';
import { Menu, X, LogIn } from 'lucide-react';
import Logo from './Logo';
import FrameLogo from './FrameLogo';
import ThemeToggle from './ThemeToggle';

const NAV_ITEMS: { path: string; label: string; disabled?: boolean }[] = [
  { path: '/fear', label: 'Индекс страха' },
  { path: '/heatmap', label: 'Карта рынка' },
  { path: '/oi', label: 'Открытый интерес' },
  { path: '/funds-money', label: 'Деньги в фондах' },
  { path: '/funds-catalog', label: 'Состав фондов' },
  { path: '/buffett', label: 'Индикатор Баффетта' },
  { path: '/strength', label: 'Сила рынка' },
  { path: '/seasonality', label: 'Сезонность' },
];

export default function Layout() {
  const { theme } = useTheme();
  const { user, isAuthenticated } = useAuth();
  const { connected } = useSSE();
  const navigate = useNavigate();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const isEditorial = theme.startsWith('editorial');

  return (
    <div className="min-h-screen" style={{ backgroundColor: 'var(--bg-primary)' }}>
      {/* Sticky Header.
          В non-editorial темах — glass-эффект через --header-blur (backdrop-filter
          blur 28px + saturate 180%). В editorial — solid bg + 1.5px solid border-bottom,
          без blur, без shadow. Все значения через CSS-переменные, так что компонент
          theme-aware без ветвления JSX. */}
      <nav
        className="sticky top-0 z-50"
        style={{
          backgroundColor: 'var(--header-bg)',
          backdropFilter: 'var(--header-blur)',
          WebkitBackdropFilter: 'var(--header-blur)',
          boxShadow: 'var(--header-shadow)',
          borderBottom: 'var(--header-border-bottom)',
        }}
      >
        <div className="mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-14 md:h-16">
            {/* Логотип */}
            <NavLink to="/" className="flex items-center gap-2">
              {isEditorial ? (
                <FrameLogo size={22} color="var(--accent)" />
              ) : (
                <>
                  <Logo size={28} />
                  <span className="hidden xl:inline text-lg md:text-xl font-bold" style={{ color: 'var(--accent)' }}>Фрейм</span>
                </>
              )}
              <span
                className={
                  isEditorial
                    ? 'px-2 py-0.5 text-[10px] font-bold tracking-wider hidden 2xl:inline'
                    : 'px-2 py-0.5 text-xs font-medium rounded-full hidden 2xl:inline'
                }
                style={
                  isEditorial
                    ? {
                        color: 'var(--text-primary)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: '4px',
                        textTransform: 'uppercase',
                      }
                    : {
                        color: 'var(--text-secondary)',
                        backgroundColor: 'var(--border-color)',
                      }
                }
              >
                Beta
              </span>
              <span
                title={connected ? 'Live: данные обновляются автоматически' : 'Нет соединения с сервером'}
                className={`w-2 h-2 rounded-full hidden 2xl:inline-block mr-2 ${connected ? 'bg-emerald-400' : 'bg-gray-500'}`}
              />
            </NavLink>

            {/* Desktop Навигация */}
            <div className="hidden lg:flex items-center gap-0.5 xl:gap-1 min-w-0">
              {NAV_ITEMS.map((item) => (
                <NavLink
                  key={item.path}
                  to={item.disabled ? '#' : item.path}
                  onClick={(e) => item.disabled && e.preventDefault()}
                  className={({ isActive }) =>
                    isEditorial
                      ? `relative px-1 lg:px-2 2xl:px-3 py-2 whitespace-nowrap transition-all
                         ${item.disabled ? 'cursor-not-allowed' : ''}
                         ${isActive ? 'font-bold' : 'font-medium'}`
                      : `px-1 lg:px-2 2xl:px-3 py-2 rounded-xl font-medium
                         whitespace-nowrap transition-all border
                         ${item.disabled
                           ? 'cursor-not-allowed border-transparent'
                           : isActive
                             ? ''
                             : 'border-transparent'
                         }`
                  }
                  style={({ isActive }) =>
                    isEditorial
                      ? {
                          fontSize: 'clamp(9px, 0.45vw + 0.3rem, 14px)',
                          letterSpacing: '-0.02em',
                          color: item.disabled
                            ? 'var(--text-muted)'
                            : isActive
                              ? 'var(--text-primary)'
                              : 'var(--text-secondary)',
                        }
                      : {
                          fontSize: 'clamp(9px, 0.45vw + 0.3rem, 14px)',
                          letterSpacing: '-0.02em',
                          color: item.disabled
                            ? 'var(--text-muted)'
                            : isActive
                              ? 'var(--accent)'
                              : 'var(--text-secondary)',
                          backgroundColor: isActive ? 'color-mix(in srgb, var(--accent) 10%, transparent)' : undefined,
                          borderColor: isActive ? 'color-mix(in srgb, var(--accent) 30%, transparent)' : undefined,
                        }
                  }
                >
                  {({ isActive }) => (
                    <>
                      {item.label}
                      {/* Editorial active stripe — 3px accent-линия под текстом */}
                      {isEditorial && isActive && (
                        <span
                          aria-hidden
                          style={{
                            position: 'absolute',
                            left: 12,
                            right: 12,
                            bottom: 4,
                            height: 3,
                            background: 'var(--accent)',
                            borderRadius: 2,
                          }}
                        />
                      )}
                    </>
                  )}
                </NavLink>
              ))}
            </div>

            {/* Right side */}
            <div className="flex items-center gap-1 lg:gap-1.5 xl:gap-3">
              {/* Theme Toggle (sun/moon, animated) */}
              <ThemeToggle />

              {/* Auth button */}
              {isAuthenticated ? (
                <button
                  onClick={() => navigate('/profile')}
                  className="editorial-press w-7 h-7 xl:w-8 xl:h-8 rounded-full flex items-center justify-center text-xs xl:text-sm font-bold"
                  style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)', border: '1.5px solid var(--text-primary)' }}
                  title="Личный кабинет"
                >
                  {(user?.email || '?')[0].toUpperCase()}
                </button>
              ) : (
                <button
                  onClick={() => navigate('/login')}
                  className="flex items-center gap-1.5 px-1.5 py-1.5 xl:px-3 xl:py-2 rounded-xl text-xs xl:text-sm font-medium transition-all hover:bg-white/10"
                  style={{ color: 'var(--text-secondary)' }}
                >
                  <LogIn size={16} />
                  <span className="hidden xl:inline">Войти</span>
                </button>
              )}

              {/* Plus версия - скрыта на мобильных.
                  В editorial — pill с accent + hard-shadow. */}
              <button
                className={
                  isEditorial
                    ? 'editorial-press hidden xl:block px-4 py-2 text-sm font-bold'
                    : 'hidden xl:block px-3 md:px-4 py-2 text-white text-sm font-medium rounded-xl transition-colors'
                }
                style={
                  isEditorial
                    ? {
                        backgroundColor: 'var(--accent)',
                        color: 'var(--text-inverse)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: 999,
                      }
                    : { backgroundColor: 'var(--accent-pink)' }
                }
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