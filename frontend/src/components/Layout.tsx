import { useState } from 'react';
import { NavLink, Outlet, useNavigate } from 'react-router-dom';
import { useTheme, ACCENTS } from '../contexts/ThemeContext';
import { useAuth } from '../contexts/AuthContext';
import { useSSE } from '../hooks/useSSE';
import { Menu, X, LogIn } from 'lucide-react';
import Logo from './Logo';
import FrameLogo from './FrameLogo';

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
  const { cycleTheme, themeName, themeIcon, theme, accent, setAccent } = useTheme();
  const { user, isAuthenticated } = useAuth();
  const { connected } = useSSE();
  const navigate = useNavigate();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const isEditorial = theme.startsWith('editorial');

  // Accent cycler — только в editorial. Бежит default → pumpkin → default.
  const cycleAccent = () => {
    const order = ['default', 'pumpkin'] as const;
    const idx = order.indexOf(accent as typeof order[number]);
    const next = order[(idx + 1) % order.length] || 'default';
    setAccent(next);
  };
  const accentSwatch = ACCENTS.find((a) => a.id === accent)?.swatch || 'transparent';

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
                  <span className="text-lg md:text-xl font-bold" style={{ color: 'var(--accent)' }}>Фрейм</span>
                </>
              )}
              <span
                className={
                  isEditorial
                    ? 'px-2 py-0.5 text-[10px] font-bold tracking-wider hidden sm:inline'
                    : 'px-2 py-0.5 text-xs font-medium rounded-full hidden sm:inline'
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
                className={`w-2 h-2 rounded-full hidden sm:inline-block mr-2 ${connected ? 'bg-emerald-400' : 'bg-gray-500'}`}
              />
            </NavLink>

            {/* Desktop Навигация */}
            <div className="hidden lg:flex items-center gap-1">
              {NAV_ITEMS.map((item) => (
                <NavLink
                  key={item.path}
                  to={item.disabled ? '#' : item.path}
                  onClick={(e) => item.disabled && e.preventDefault()}
                  className={({ isActive }) =>
                    isEditorial
                      ? `relative px-3 xl:px-4 py-2 text-sm whitespace-nowrap transition-all
                         ${item.disabled ? 'cursor-not-allowed' : ''}
                         ${isActive ? 'font-bold' : 'font-medium'}`
                      : `px-3 xl:px-4 py-2 rounded-xl text-sm font-medium
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
                          color: item.disabled
                            ? 'var(--text-muted)'
                            : isActive
                              ? 'var(--text-primary)'
                              : 'var(--text-secondary)',
                        }
                      : {
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
            <div className="flex items-center gap-2 md:gap-3">
              {/* Accent picker — только в editorial (другие темы имеют свой нативный accent) */}
              {isEditorial && (
                <button
                  onClick={cycleAccent}
                  className="flex items-center gap-1.5 px-2 md:px-3 py-2 transition-all hover:opacity-80"
                  style={{
                    color: 'var(--text-secondary)',
                    border: '1.5px solid var(--text-primary)',
                    borderRadius: 999,
                    backgroundColor: 'transparent',
                  }}
                  title={`Accent: ${accent}. Нажми для смены`}
                >
                  <span
                    style={{
                      width: 12,
                      height: 12,
                      borderRadius: '50%',
                      background: accent === 'default' ? 'var(--accent)' : accentSwatch,
                      border: '1px solid var(--text-primary)',
                      display: 'inline-block',
                    }}
                  />
                  <span className="text-xs font-semibold hidden md:inline">
                    {accent === 'default' ? 'Theme' : accent}
                  </span>
                </button>
              )}

              {/* Theme Toggle */}
              <button
                onClick={cycleTheme}
                className={
                  isEditorial
                    ? 'flex items-center gap-1 md:gap-2 px-2 md:px-3 py-2 transition-all hover:opacity-80'
                    : 'flex items-center gap-1 md:gap-2 px-2 md:px-3 py-2 rounded-lg transition-all hover:bg-white/10'
                }
                style={
                  isEditorial
                    ? {
                        color: 'var(--text-secondary)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: 999,
                      }
                    : {
                        color: 'var(--text-secondary)',
                        borderColor: 'var(--border-color)',
                      }
                }
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

              {/* Plus версия - скрыта на мобильных.
                  В editorial — pill с accent + hard-shadow. */}
              <button
                className={
                  isEditorial
                    ? 'hidden sm:block px-4 py-2 text-sm font-bold transition-all hover:translate-x-[-1px] hover:translate-y-[-1px]'
                    : 'hidden sm:block px-3 md:px-4 py-2 text-white text-sm font-medium rounded-xl transition-colors'
                }
                style={
                  isEditorial
                    ? {
                        backgroundColor: 'var(--accent)',
                        color: 'var(--text-inverse)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: 999,
                        boxShadow: 'var(--shadow-hard-chip)',
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