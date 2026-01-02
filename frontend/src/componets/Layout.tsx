import { NavLink, Outlet } from 'react-router-dom';

const NAV_ITEMS = [
  { path: '/', label: 'Открытый интерес', icon: '📊' },
  { path: '/heatmap', label: 'Карта рынка', icon: '🗺️' },
  { path: '/fear-index', label: 'Индекс страха', icon: '😱', disabled: true },
  { path: '/fund-flows', label: 'Деньги в фондах', icon: '💰', disabled: true },
  { path: '/buffett', label: 'Индикатор Баффета', icon: '🎯', disabled: true },
  { path: '/market-strength', label: 'Сила рынка', icon: '💪', disabled: true },
];

export default function Layout() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
      {/* Фоновые эффекты */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-0 left-1/4 w-96 h-96 bg-emerald-500/5 rounded-full blur-3xl" />
        <div className="absolute bottom-0 right-1/4 w-96 h-96 bg-indigo-500/5 rounded-full blur-3xl" />
      </div>

      {/* Навигация */}
      <nav className="sticky top-0 z-50 bg-slate-900/80 backdrop-blur-xl border-b border-slate-700/50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            {/* Логотип */}
            <div className="flex items-center gap-3">
              <span className="text-2xl">📈</span>
              <span className="text-xl font-bold text-white">
                MOEX <span className="text-emerald-400">Analytics</span>
              </span>
            </div>

            {/* Навигация */}
            <div className="flex items-center gap-1 overflow-x-auto">
              {NAV_ITEMS.map((item) => (
                <NavLink
                  key={item.path}
                  to={item.disabled ? '#' : item.path}
                  onClick={(e) => item.disabled && e.preventDefault()}
                  className={({ isActive }) => `
                    flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium 
                    whitespace-nowrap transition-all
                    ${item.disabled 
                      ? 'text-slate-600 cursor-not-allowed' 
                      : isActive 
                        ? 'bg-emerald-600 text-white shadow-lg' 
                        : 'text-slate-400 hover:text-white hover:bg-slate-700/50'
                    }
                  `}
                >
                  <span>{item.icon}</span>
                  <span className="hidden md:inline">{item.label}</span>
                </NavLink>
              ))}
            </div>
          </div>
        </div>
      </nav>

      {/* Контент страницы */}
      <main className="relative z-10">
        <Outlet />
      </main>
    </div>
  );
}