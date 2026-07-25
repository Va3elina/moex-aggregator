import { useState } from 'react';
import { Link, NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useTheme } from '../contexts/ThemeContext';
import { useAuth } from '../contexts/AuthContext';
import { useSSE } from '../hooks/useSSE';
import { useYandexMetrica } from '../hooks/useYandexMetrica';
import { useViewportWidth } from '../hooks/useViewportWidth';
import { useIsPhone } from '../hooks/useIsPhone';
import { Menu, X, LogIn, BarChart3, Newspaper, Globe2 } from 'lucide-react';
import Logo from './Logo';
import FrameLogo from './FrameLogo';
import ThemeToggle from './ThemeToggle';
import { AnomalyBell } from './anomaly/AnomalyBell';
import PageSEO from './PageSEO';
import MobileFallbackChrome from './mobile/MobileFallbackChrome';

/* Маршруты с собственной мобильной версией (ResponsiveRoute → Mobile*Page
   с MobileLayout): им chrome не нужен — рисуют свой. Остальные маршруты
   на мобиле получают MobileFallbackChrome (top-bar + «Назад» + скролл).
   При добавлении новой mobile-страницы — дополнить список. */
const MOBILE_READY_PATHS = new Set([
  '/', '/oi', '/heatmap', '/funds-money', '/buffett', '/strength',
  '/seasonality', '/cbr-flows', '/profile', '/pricing', '/fund-trades',
]);

// Порядок задан Вадимом (04.07.2026): частые/продуктовые впереди,
// Баффетт в конце. Мобильный rail (MobileBottomRail) — отдельный список.
const NAV_ITEMS: { path: string; label: string; disabled?: boolean; badge?: string }[] = [
  { path: '/heatmap', label: 'Карта рынка' },
  // NEW — внутри появилась вкладка «Скринер сигналов» (04.07.2026).
  { path: '/oi', label: 'Открытые позиции', badge: 'New' },
  { path: '/funds-money', label: 'Деньги в фондах' },
  { path: '/strength', label: 'Сила рынка' },
  // Smart-money tracking — что покупают/продают БПИФ. Pro-only фича.
  { path: '/fund-trades', label: 'Сделки фондов' },
  { path: '/seasonality', label: 'Сезонность' },
  { path: '/cbr-flows', label: 'Поток капитала' },
  { path: '/buffett', label: 'Индикатор Баффетта' },
];

export default function Layout() {
  // ВСЕ хуки до conditional return — React hooks rule.
  const { theme } = useTheme();
  const { user, isAuthenticated } = useAuth();
  const { connected } = useSSE();
  const navigate = useNavigate();
  const location = useLocation();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  // useIsPhone (не useIsMobile) — это routing-гейт (Outlet-only vs
  // MobileFallbackChrome vs десктоп-chrome): телефон в ландшафте должен
  // оставаться на мобильной вёрстке, а не получать десктопный Layout.
  const isMobileViewport = useIsPhone();
  const vw = useViewportWidth();
  // SPA-tracking для Yandex.Metrica — фиксирует переходы /buffett → /oi → ...
  // Первый hit отправляется автоматически через init() в index.html.
  useYandexMetrica();

  const isEditorial = theme.startsWith('editorial');
  // Compact header (laptop 1024-1280): уменьшенные logo + icons чтобы
  // освободить место для nav items, иначе они пересекаются с logo/icons.
  // На ≥1280 (xl) — нормальные размеры.
  const isCompactHeader = vw >= 1024 && vw < 1280;

  // На мобиле страницы с mobile-версией используют MobileLayout
  // (TopBar + Main + BottomRail) — им рендерим только Outlet, страница
  // сама рисует свой layout. Маршруты БЕЗ mobile-версии (login, legal,
  // billing, methodology, …) показывают десктопную вёрстку — заворачиваем
  // её в облегчённый MobileFallbackChrome (top-bar + «Назад»), иначе на
  // них нет никакой навигации (аудит 2026-06-12).
  if (isMobileViewport) {
    const pathname = location.pathname.replace(/\/+$/, '') || '/';
    if (MOBILE_READY_PATHS.has(pathname)) {
      return (
        <>
          <PageSEO />
          <Outlet />
        </>
      );
    }
    return (
      <>
        <PageSEO />
        <MobileFallbackChrome>
          <Outlet />
        </MobileFallbackChrome>
      </>
    );
  }

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
                <FrameLogo size={isCompactHeader ? 12 : 22} color="var(--accent)" />
              ) : (
                <>
                  <Logo size={28} />
                  <span className="hidden xl:inline text-lg md:text-xl font-bold" style={{ color: 'var(--accent)' }}>FRAME</span>
                </>
              )}
              <span
                className={
                  isEditorial
                    ? 'px-2 py-0.5 font-bold tracking-wider hidden lg:inline'
                    : 'px-2 py-0.5 font-medium rounded-full hidden lg:inline'
                }
                style={
                  isEditorial
                    ? {
                        fontSize: 'var(--fs-2xs)',
                        color: 'var(--text-primary)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: '4px',
                        textTransform: 'uppercase',
                      }
                    : {
                        fontSize: 'var(--fs-2xs)',
                        color: 'var(--text-secondary)',
                        backgroundColor: 'var(--border-color)',
                      }
                }
              >
                Beta
              </span>
              <span
                title={connected ? 'Live: данные обновляются автоматически' : 'Нет соединения с сервером'}
                className={`w-2 h-2 rounded-full hidden xl:inline-block mr-2 ${connected ? 'bg-emerald-400' : 'bg-gray-500'}`}
              />
            </NavLink>

            {/* Desktop Навигация — flex-1 min-w-0 overflow-hidden чтобы
                не выталкивать right side когда узко (laptop 1024). */}
            <div className="hidden lg:flex flex-1 min-w-0 items-center justify-center gap-0.5 xl:gap-1 overflow-hidden">
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
                      {item.badge && (
                        <span
                          className="ml-1.5 uppercase font-bold hidden xl:inline-block align-middle"
                          style={{
                            fontSize: '0.62rem',
                            letterSpacing: '0.06em',
                            color: 'var(--accent)',
                            border: '1px solid var(--accent)',
                            borderRadius: '3px',
                            padding: '1px 5px',
                            lineHeight: 1.2,
                            whiteSpace: 'nowrap',
                          }}
                        >
                          {item.badge}
                        </span>
                      )}
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

            {/* Right side — flex-shrink-0 чтобы никогда не сжималось
                под nav overflow. Gap stable 8-12px fluid clamp. */}
            <div
              className="flex flex-shrink-0 items-center"
              style={{ gap: 'clamp(6px, 0.5vw + 0.25rem, 12px)' }}
            >
              {/* Theme Toggle (sun/moon, animated) */}
              <ThemeToggle />

              <AnomalyBell />

              {/* Header-icons унифицированы: 40 → 36 → 32 пропорциональное
                  масштабирование на mobile/md/xl. ThemeToggle и Admin Stats
                  имеют одинаковый outline-стиль (border 1.5px text-primary +
                  transparent bg + accent-tinted icon). Auth — filled accent
                  для visual-priority "это твой login". */}

              {/* Admin Stats link — только для role=admin (📊 → /admin/stats) */}
              {isAuthenticated && user?.role === 'admin' && (
                <button
                  onClick={() => navigate('/admin/stats')}
                  className="editorial-press grid place-items-center rounded-full"
                  style={{
                    color: 'var(--accent)',
                    border: '1.5px solid var(--text-primary)',
                    backgroundColor: 'transparent',
                    width: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
                    height: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
                  }}
                  title="Статистика сайта"
                  aria-label="Статистика сайта (admin)"
                >
                  <BarChart3
                    style={{ width: 'clamp(13px, 1vw + 0.3rem, 17px)', height: 'clamp(13px, 1vw + 0.3rem, 17px)' }}
                    strokeWidth={2}
                  />
                </button>
              )}

              {/* Admin Content News link — только для role=admin (📰 → /admin/content-news) */}
              {isAuthenticated && user?.role === 'admin' && (
                <button
                  onClick={() => navigate('/admin/content-news')}
                  className="editorial-press grid place-items-center rounded-full"
                  style={{
                    color: 'var(--accent)',
                    border: '1.5px solid var(--text-primary)',
                    backgroundColor: 'transparent',
                    width: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
                    height: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
                  }}
                  title="Новости (content-пайплайн)"
                  aria-label="Новости (admin)"
                >
                  <Newspaper
                    style={{ width: 'clamp(13px, 1vw + 0.3rem, 17px)', height: 'clamp(13px, 1vw + 0.3rem, 17px)' }}
                    strokeWidth={2}
                  />
                </button>
              )}

              {/* Admin OI Global link — только для role=admin (🌐 → /admin/oi-global) */}
              {isAuthenticated && user?.role === 'admin' && (
                <button
                  onClick={() => navigate('/admin/oi-global')}
                  className="editorial-press grid place-items-center rounded-full"
                  style={{
                    color: 'var(--accent)',
                    border: '1.5px solid var(--text-primary)',
                    backgroundColor: 'transparent',
                    width: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
                    height: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
                  }}
                  title="Международный Open Interest"
                  aria-label="Международный Open Interest (admin)"
                >
                  <Globe2
                    style={{ width: 'clamp(13px, 1vw + 0.3rem, 17px)', height: 'clamp(13px, 1vw + 0.3rem, 17px)' }}
                    strokeWidth={2}
                  />
                </button>
              )}

              {/* Auth button — единый размер с ThemeToggle/AdminStats.
                  Filled accent с initial — primary visual hierarchy. */}
              {isAuthenticated ? (
                <button
                  onClick={() => navigate('/profile')}
                  className="editorial-press grid place-items-center rounded-full font-bold"
                  style={{
                    backgroundColor: 'var(--accent)',
                    color: '#fff',
                    border: '1.5px solid var(--text-primary)',
                    fontSize: 'clamp(11px, 0.6vw + 0.5rem, 14px)',
                    width: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
                    height: 'clamp(22px, 1.6vw + 0.3rem, 32px)',
                  }}
                  title="Личный кабинет"
                  aria-label="Личный кабинет"
                >
                  {(user?.email || '?')[0].toUpperCase()}
                </button>
              ) : (
                <button
                  onClick={() => navigate('/login')}
                  className="grid place-items-center w-5 h-5 xl:w-auto xl:h-auto xl:flex xl:items-center xl:gap-1.5 xl:px-3 xl:py-2 rounded-xl text-xs xl:text-sm font-medium transition-opacity hover:opacity-70"
                  style={{
                    color: 'var(--text-secondary)',
                  }}
                  aria-label="Войти"
                >
                  <LogIn style={{ width: 'clamp(13px, 1vw + 0.3rem, 17px)', height: 'clamp(13px, 1vw + 0.3rem, 17px)' }} />
                  <span className="hidden xl:inline">Войти</span>
                </button>
              )}

              {/* Plus button — CTA для перехода на /pricing.
                  Скрыта на мобильных (xl:block). Не показывается admin'у
                  (бессмысленно — у него full access). */}
              {user?.role !== 'admin' && (
                <button
                  onClick={() => navigate('/pricing')}
                  className={
                    isEditorial
                      ? 'editorial-press hidden xl:block px-4 py-2 text-sm font-bold'
                      : 'hidden xl:block px-3 md:px-4 py-2 text-white text-sm font-medium rounded-xl transition-colors'
                  }
                  style={
                    isEditorial
                      ? {
                          backgroundColor: 'var(--accent)',
                          color: '#fff',  // hardcoded white: bg=accent (pumpkin) не зависит от темы
                          border: '1.5px solid var(--text-primary)',
                          borderRadius: 999,
                        }
                      : { backgroundColor: 'var(--accent-pink)' }
                  }
                  title="Перейти к тарифам"
                >
                  Plus
                </button>
              )}

              {/* Mobile Menu Button.
                  40×40 — компактно и в размер ThemeToggle/Login. Иконка 22px
                  для визуального баланса (8+22+8 = 38, +2 от border = 40). */}
              <button
                onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                className="lg:hidden grid place-items-center w-10 h-10 rounded-lg transition-opacity hover:opacity-70"
                style={{ color: 'var(--text-secondary)' }}
                aria-label={mobileMenuOpen ? 'Закрыть меню' : 'Открыть меню'}
                aria-expanded={mobileMenuOpen}
                aria-controls="mobile-nav-drawer"
              >
                {mobileMenuOpen ? <X size={22} /> : <Menu size={22} />}
              </button>
            </div>
          </div>
        </div>

        {/* Mobile Menu Dropdown.
            Bg = --bg-primary (paper-консистентный с основным фоном страницы),
            а не --bg-secondary, чтобы в editorial-light (warm cream) не появлялась
            белая панель которая выбивается из палитры. */}
        {mobileMenuOpen && (
          <div
            id="mobile-nav-drawer"
            className="lg:hidden border-t"
            style={{
              backgroundColor: 'var(--bg-primary)',
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
                    block px-4 py-3 rounded-xl text-sm
                    ${item.disabled ? 'cursor-not-allowed' : 'editorial-press'}
                    ${isActive ? 'font-bold' : 'font-medium'}
                  `}
                  style={({ isActive }) => ({
                    // Theme-aware editorial palette: orange accent removed.
                    // Active = inverted (text-primary bg, paper text) — высокий
                    // contrast без рыжего highlight'а. Inactive = paper bg с
                    // outline + editorial-press hover-shadow (см. index.css).
                    // Работает на light и dark editorial — переменные адаптируются.
                    color: item.disabled
                      ? 'var(--text-muted)'
                      : isActive
                        ? 'var(--bg-primary)'
                        : 'var(--text-secondary)',
                    backgroundColor: item.disabled
                      ? undefined
                      : isActive
                        ? 'var(--text-primary)'
                        : 'var(--bg-primary)',
                    border: !item.disabled
                      ? '1.5px solid var(--text-primary)'
                      : undefined,
                  })}
                >
                  {item.label}
                  {item.badge && (
                    <span
                      className="ml-2 uppercase font-bold inline-block align-middle"
                      style={{
                        fontSize: '0.62rem',
                        letterSpacing: '0.06em',
                        color: 'var(--accent)',
                        border: '1px solid var(--accent)',
                        borderRadius: '3px',
                        padding: '1px 5px',
                        lineHeight: 1.2,
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {item.badge}
                    </span>
                  )}
                </NavLink>
              ))}

              {/* Auth в мобильном меню */}
              {isAuthenticated ? (
                <button
                  onClick={() => { navigate('/profile'); setMobileMenuOpen(false); }}
                  className="editorial-press w-full mt-3 flex items-center gap-3 px-4 py-3 rounded-xl text-sm font-medium"
                  style={{
                    color: 'var(--text-primary)',
                    backgroundColor: 'var(--bg-primary)',
                    border: '1.5px solid var(--text-primary)',
                  }}
                >
                  <div
                    className="w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold"
                    style={{ backgroundColor: 'var(--text-primary)', color: 'var(--bg-primary)' }}
                  >
                    {(user?.email || '?')[0].toUpperCase()}
                  </div>
                  Личный кабинет
                </button>
              ) : (
                <button
                  onClick={() => { navigate('/login'); setMobileMenuOpen(false); }}
                  className="editorial-press w-full mt-3 flex items-center justify-center gap-2 px-4 py-3 rounded-xl text-sm font-medium"
                  style={{
                    color: 'var(--text-primary)',
                    backgroundColor: 'var(--bg-primary)',
                    border: '1.5px solid var(--text-primary)',
                  }}
                >
                  <LogIn size={16} />
                  Войти
                </button>
              )}

              {/* Plus версия в мобильном меню.
                  В editorial — accent (pumpkin) + hard-shadow press; в legacy — accent-pink. */}
              <button
                className={
                  isEditorial
                    ? 'editorial-press w-full mt-3 px-4 py-3 text-sm font-bold rounded-xl'
                    : 'w-full mt-3 px-4 py-3 text-white text-sm font-medium rounded-xl transition-colors'
                }
                style={
                  isEditorial
                    ? {
                        backgroundColor: 'var(--accent)',
                        color: 'var(--text-inverse)',
                        border: '1.5px solid var(--text-primary)',
                      }
                    : { backgroundColor: 'var(--accent-pink)' }
                }
              >
                Plus версия
              </button>
            </div>
          </div>
        )}
      </nav>

      {/* SEO: per-page <title>/<meta>/canonical/JSON-LD breadcrumbs.
          Один компонент на весь Layout — резолвит метаданные через
          useLocation(). Все 14+ страниц получают уникальный <head>
          без дублирования <PageSEO /> в каждой. */}
      <PageSEO />

      {/* Контент страницы */}
      <main className="relative">
          <Outlet />
      </main>

      {/* Footer — компактный, со ссылками на обязательные документы.
          Privacy/Contacts/Refund/Delivery — обязательны для эквайринга Т-Банка
          (см. требования к интернет-магазину). Layout: однострочный flex с
          равномерным распределением (justify-between на широких, wrap на узких).
          В конце — иконка Telegram-канала. */}
      <footer
        className="mt-12 md:mt-16 py-4 md:py-6"
        style={{
          borderTop: '1px solid var(--border-color)',
          color: 'var(--text-muted)',
        }}
      >
        <div className="max-w-7xl mx-auto px-4 md:px-6 flex flex-col gap-2">
        {/* Top row — основные ссылки */}
        <div className="flex flex-wrap items-center justify-between gap-x-5 gap-y-2 text-xs">
          <span>© Frame · таймфрейм.рф</span>
          {/* Attribution к индексам MOEX (Strength + FundsMoney используют
              IMOEX/RTSI/RGBI как benchmark). License MOEX требует видимое
              упоминание сайтов отображающих их индексы. Свечи акций (Heatmap,
              Seasonality, OI) — обычная market data, attribution не требуется. */}
          <span style={{ color: 'var(--text-muted)' }}>
            Индексы:{' '}
            <a
              href="https://www.moex.com"
              target="_blank"
              rel="noreferrer noopener"
              className="transition-opacity hover:opacity-80"
              style={{ color: 'var(--text-secondary)' }}
            >
              © ПАО Московская Биржа
            </a>
          </span>
          <Link to="/faq" className="transition-opacity hover:opacity-80" style={{ color: 'var(--text-secondary)' }}>
            FAQ
          </Link>
          <Link to="/glossary" className="transition-opacity hover:opacity-80" style={{ color: 'var(--text-secondary)' }}>
            Глоссарий
          </Link>
          <Link to="/contacts" className="transition-opacity hover:opacity-80" style={{ color: 'var(--text-secondary)' }}>
            Контакты
          </Link>
          <Link to="/refund" className="transition-opacity hover:opacity-80" style={{ color: 'var(--text-secondary)' }}>
            Возврат
          </Link>
          <Link to="/delivery" className="transition-opacity hover:opacity-80" style={{ color: 'var(--text-secondary)' }}>
            Услуга
          </Link>
          <Link to="/privacy" className="transition-opacity hover:opacity-80" style={{ color: 'var(--text-secondary)' }}>
            Политика обработки данных
          </Link>
          <a
            href="https://t.me/+vbt614-Qq1w1YWYy"
            target="_blank"
            rel="noreferrer noopener"
            className="inline-flex items-center gap-1.5 transition-opacity hover:opacity-80"
            style={{ color: 'var(--text-secondary)' }}
            aria-label="Telegram-канал FRAME"
          >
            <svg
              viewBox="0 0 24 24"
              width="16"
              height="16"
              fill="currentColor"
              aria-hidden="true"
            >
              {/* Telegram paper-plane glyph — single path, brand-recognizable */}
              <path d="M9.78 18.65l.28-4.23 7.68-6.92c.34-.31-.07-.46-.52-.19L7.74 13.3 3.64 12c-.88-.25-.89-.86.2-1.3l15.97-6.16c.73-.33 1.43.18 1.15 1.3l-2.72 12.81c-.19.91-.74 1.13-1.5.71L12.6 16.3l-1.99 1.93c-.23.23-.42.42-.83.42z" />
            </svg>
            Telegram
          </a>
        </div>
        {/* Bottom row — ИП-реквизиты для trust signal (Yandex Webmaster
            POSITION_GENERATE_USER_TRUST_BOX). Полный адрес и расчётный счёт
            живут в /agreement, /offer и /contacts — здесь сокращённая версия. */}
        <div
          className="text-[10px] md:text-xs opacity-70"
          style={{ color: 'var(--text-muted)' }}
        >
          ИП Тория А.Р. · ИНН 782627792630 · ОГРНИП 325784700029296
        </div>
        </div>
      </footer>
    </div>
  );
}