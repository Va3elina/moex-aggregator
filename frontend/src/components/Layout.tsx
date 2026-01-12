import { NavLink, Outlet } from 'react-router-dom';

const NAV_ITEMS = [
  { path: '/fear', label: 'Fear Index' },
  { path: '/heatmap', label: 'Market Heatmap' },
  { path: '/oi', label: 'Open Interest' },
  { path: '/funds', label: 'Money in Funds', disabled: true },
  { path: '/buffett', label: 'Buffett Indicator', disabled: true },
  { path: '/strength', label: 'Market Strength', disabled: true },
];

export default function Layout() {
  return (
    <div className="min-h-screen bg-[#0B0D12]">
      {/* Sticky Header с glass эффектом */}
      <nav className="sticky top-0 z-50 bg-[#0B0D12]/70 backdrop-blur-xl border-b border-white/5">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            {/* Логотип */}
            <NavLink to="/" className="flex items-center gap-2">
              <span className="text-xl font-bold text-[#C8FF2E]">TradingLens</span>
              <span className="px-2 py-0.5 text-xs font-medium text-[#A7ADBC] bg-white/10 rounded-full">
                Beta
              </span>
            </NavLink>

            {/* Навигация */}
            <div className="flex items-center gap-1">
              {NAV_ITEMS.map((item) => (
                <NavLink
                  key={item.path}
                  to={item.disabled ? '#' : item.path}
                  onClick={(e) => item.disabled && e.preventDefault()}
                  className={({ isActive }) => `
                    px-4 py-2 rounded-xl text-sm font-medium 
                    whitespace-nowrap transition-all
                    ${item.disabled 
                      ? 'text-[#5E6576] cursor-not-allowed' 
                      : isActive 
                        ? 'bg-[#C8FF2E]/10 text-[#C8FF2E] border border-[#C8FF2E]/30' 
                        : 'text-[#A7ADBC] hover:text-[#F4F6FA] hover:bg-white/5'
                    }
                  `}
                >
                  {item.label}
                </NavLink>
              ))}
            </div>

            {/* Right side */}
            <div className="flex items-center gap-3">
              <button className="p-2 text-[#A7ADBC] hover:text-[#F4F6FA] transition-colors">
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
                </svg>
              </button>
              <button className="px-4 py-2 bg-[#FF4FD8] hover:bg-[#E647C2] text-white text-sm font-medium rounded-xl transition-colors">
                Plus version
              </button>
            </div>
          </div>
        </div>
      </nav>

      {/* Контент страницы */}
      <main className="relative">
        <Outlet />
      </main>
    </div>
  );
}