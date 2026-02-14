import { useState } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { useTheme } from '../contexts/ThemeContext';
import { Menu, X } from 'lucide-react';

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
              <span className="text-lg md:text-xl font-bold" style={{ color: 'var(--accent)' }}>TradingLens</span>
              <span className="px-2 py-0.5 text-xs font-medium rounded-full hidden sm:inline" style={{
                color: 'var(--text-secondary)',
                backgroundColor: 'var(--border-color)'
              }}>
                Beta
              </span>
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
                        : 'hover:bg-white/5'
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