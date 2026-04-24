/**
 * LandingPage — главная для незарегистрированных посетителей.
 *
 * Показывается на "/" когда нет auth'а. Задача: за 5 секунд понять что такое
 * Фрейм, увидеть pulse рынка, перейти в логин или тариф.
 *
 * Залогиненные users видят OverviewPage (dashboard). Роутер делает conditional.
 */
import { Link } from 'react-router-dom';
import {
  Gauge,
  Grid3X3,
  Wallet,
  CalendarDays,
  Layers,
  Database,
  Clock,
  Zap,
} from 'lucide-react';
import FeatureCarousel from '../components/landing/FeatureCarousel';
import NoiseToSignal from '../components/landing/NoiseToSignal';
import { SeasonalityPreview, FundsFlowPreview, FearPreview, HeatmapPreview } from '../components/landing/previews';

export default function LandingPage() {
  return (
    <div>

      {/* ═══ HERO — full viewport (100vh) ═══
          Tagline + CTAs + анимация "шум → сигнал" фоном на весь экран.
          Layout: анимация absolute (z-0), текст поверх (z-10).
          min-h-screen гарантирует что секция займёт весь viewport,
          следующая секция появится только при scroll. */}
      <section className="relative min-h-screen flex flex-col items-center justify-center overflow-hidden px-4 md:px-6">
        {/* Анимация фоном — заполняет секцию целиком */}
        <NoiseToSignal />

        {/* Контентный overlay поверх анимации */}
        <div className="relative z-10 text-center max-w-3xl mx-auto">
          <h1
            className="text-4xl md:text-7xl font-bold mb-6 md:mb-8"
            style={{
              color: 'var(--text-primary)',
              letterSpacing: '-0.035em',
              lineHeight: 0.95,
              // Лёгкий text-shadow для читаемости над анимацией
              textShadow: '0 2px 40px rgba(0,0,0,0.5)',
            }}
          >
            Посмотри. Подумай. Решай.
          </h1>
          <p
            className="text-base md:text-xl max-w-2xl mx-auto mb-8 md:mb-10"
            style={{
              color: 'var(--text-secondary)',
              lineHeight: 1.55,
              textShadow: '0 1px 20px rgba(0,0,0,0.6)',
            }}
          >
            Лучшие инвестиционные решения начинаются с качественных данных.
            Российский рынок, индикаторы и аналитика — в одном дашборде.
          </p>
          <div className="flex flex-wrap items-center justify-center gap-3">
            <Link
              to="/login"
              className="px-6 py-3 font-semibold transition-opacity hover:opacity-90"
              style={{
                backgroundColor: 'var(--accent)',
                color: 'var(--text-inverse)',
                borderRadius: 'var(--radius-md)',
              }}
            >
              Попробовать бесплатно
            </Link>
            <Link
              to="/pricing"
              className="px-6 py-3 font-semibold transition-colors border"
              style={{
                color: 'var(--text-primary)',
                borderColor: 'var(--border-color)',
                borderRadius: 'var(--radius-md)',
                // Полупрозрачный фон — чтобы кнопка читалась над анимацией
                backgroundColor: 'color-mix(in srgb, var(--bg-primary) 50%, transparent)',
                backdropFilter: 'blur(4px)',
              }}
            >
              Тарифы
            </Link>
          </div>
          {/* Маленький hint что ниже есть контент */}
          <p
            className="mt-12 md:mt-16 text-xs"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.15em', fontWeight: 500 }}
          >
            ПРОКРУТИТЕ ВНИЗ ↓
          </p>
        </div>
      </section>

      {/* Wrapper для остального контента с обычным layout-padding */}
      <div className="max-w-7xl mx-auto px-4 md:px-6 py-14 md:py-20">

      {/* ═══ "CHARTS THAT MOVE MARKETS"-style section — TV-inspired ═══
          Большой hero-заголовок + 4 small feature-points (как у TV "Up to 16
          charts per screen" grid) → затем основной interactive Carousel. */}
      <section className="mb-14 md:mb-20">
        <div className="text-center mb-10 md:mb-14 max-w-3xl mx-auto">
          <h2
            className="text-3xl md:text-5xl font-bold mb-4"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.03em', lineHeight: 1.05 }}
          >
            Индикаторы, которые<br className="hidden md:inline"/> меняют решения
          </h2>
          <p className="text-sm md:text-lg" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
            Хотите просто посмотреть цену или разобрать структуру фонда по активам —
            всё уже готово.
          </p>
        </div>

        {/* 4 small feature points — эхо TV's "Up to 16 charts per screen" grid */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-6 md:gap-8 mb-10 md:mb-14 max-w-5xl mx-auto">
          <SmallFeature
            icon={<Layers size={28} strokeWidth={1.5} />}
            title="8 индикаторов"
            desc="От просмотра цены до композитных метрик и сезонности"
          />
          <SmallFeature
            icon={<Database size={28} strokeWidth={1.5} />}
            title="95+ акций MOEX"
            desc="Все ключевые бумаги + фьючерсы + индексы + валюты"
          />
          <SmallFeature
            icon={<Clock size={28} strokeWidth={1.5} />}
            title="С 1997 года"
            desc="27 лет исторических данных, включая кризисы 2008/14/20/22"
          />
          <SmallFeature
            icon={<Zap size={28} strokeWidth={1.5} />}
            title="Live-обновление"
            desc="Данные обновляются каждые 5 минут в торговое время"
          />
        </div>

        {/* Interactive carousel — демонстрация каждого индикатора */}
        <FeatureCarousel />
      </section>

      {/* ═══ 4 BIG FEATURE CARDS (2×2 grid) ═══
          Эхо TV's "Technical analysis, done right" / "Bar Replay" / etc.
          Каждая card — hero-title + desc + button + illustration. */}
      <section className="mb-14 md:mb-20">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 md:gap-5">
          <BigFeatureCard
            title="Сезонность за 30 секунд"
            desc="Средняя динамика по дням, месяцам, годам. Паттерны с 1997 года — на одном экране. Сравнивайте любые периоды и находите закономерности."
            ctaLabel="Открыть сезонность"
            ctaIcon={<CalendarDays size={14} />}
            to="/seasonality"
            illustration={<SeasonalityPreview />}
          />
          <BigFeatureCard
            title="Куда идут деньги"
            desc="36 российских ETF: СЧА, притоки-оттоки по категориям, реальный состав фондов. Видите куда идут большие деньги — туда идёт рынок."
            ctaLabel="Открыть фонды"
            ctaIcon={<Wallet size={14} />}
            to="/funds-money"
            illustration={<FundsFlowPreview />}
          />
          <BigFeatureCard
            title="Страх и жадность рынка"
            desc="Композитный индекс по 4 метрикам: волатильность, breadth, ликвидность, momentum. Когда все жадны — осторожно. Когда все в страхе — искать возможности."
            ctaLabel="Открыть индекс страха"
            ctaIcon={<Gauge size={14} />}
            to="/fear"
            illustration={<FearPreview />}
          />
          <BigFeatureCard
            title="Весь рынок за секунду"
            desc="Все акции MOEX на одном экране. Размер — капитализация, цвет — дневная динамика. За взгляд видите кто двигает рынок и где фокус."
            ctaLabel="Открыть карту"
            ctaIcon={<Grid3X3 size={14} />}
            to="/heatmap"
            illustration={<HeatmapPreview />}
          />
        </div>
      </section>

      {/* ═══ FINAL CTA ═══
          Второй шанс конверсии после того как user прокрутил всю landing.
          Текст фокусирует на "no risk to try" — бесплатный базовый доступ. */}
      <section className="text-center">
        <h2
          className="text-2xl md:text-4xl font-bold mb-3"
          style={{ color: 'var(--text-primary)', letterSpacing: '-0.02em', lineHeight: 1.1 }}
        >
          Начни разбираться в рынке
        </h2>
        <p className="mb-7 text-sm md:text-base max-w-xl mx-auto" style={{ color: 'var(--text-secondary)' }}>
          Бесплатный доступ к базовым индикаторам — без оплаты и без карты.
          Подписка Pro или Premium — когда и если понадобятся продвинутые фичи.
        </p>
        <div className="flex flex-wrap items-center justify-center gap-3">
          <Link
            to="/login"
            className="px-6 py-3 font-semibold transition-opacity hover:opacity-90"
            style={{
              backgroundColor: 'var(--accent)',
              color: 'var(--text-inverse)',
              borderRadius: 'var(--radius-md)',
            }}
          >
            Попробовать бесплатно
          </Link>
          <Link
            to="/pricing"
            className="px-6 py-3 font-semibold transition-colors border"
            style={{
              color: 'var(--text-primary)',
              borderColor: 'var(--border-color)',
              borderRadius: 'var(--radius-md)',
              backgroundColor: 'transparent',
            }}
          >
            Тарифы
          </Link>
        </div>
      </section>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════
// SUBCOMPONENTS
// ═══════════════════════════════════════════════════════════

/** Big Feature Card — большая карточка с заголовком, описанием, CTA и illustration.
    Pattern из TV features page (2×2 grid sections).
    Illustration внизу — наш SVG-preview индикатора, акцент на функциональность. */
function BigFeatureCard({
  title,
  desc,
  ctaLabel,
  ctaIcon,
  to,
  illustration,
}: {
  title: string;
  desc: string;
  ctaLabel: string;
  ctaIcon: React.ReactNode;
  to: string;
  illustration: React.ReactNode;
}) {
  return (
    <div
      className="rounded-2xl border p-6 md:p-8 flex flex-col"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
      }}
    >
      <h3
        className="text-2xl md:text-3xl font-bold mb-3"
        style={{ color: 'var(--text-primary)', letterSpacing: '-0.02em', lineHeight: 1.15 }}
      >
        {title}
      </h3>
      <p
        className="text-sm md:text-base mb-5 flex-shrink-0"
        style={{ color: 'var(--text-secondary)', lineHeight: 1.55 }}
      >
        {desc}
      </p>

      {/* CTA — pill-style button */}
      <Link
        to={to}
        className="inline-flex items-center gap-2 px-4 py-2 mb-5 text-sm font-semibold w-fit transition-all"
        style={{
          backgroundColor: 'var(--bg-tertiary)',
          color: 'var(--text-primary)',
          borderRadius: 'var(--radius-md)',
          border: '1px solid var(--border-color)',
        }}
        onMouseEnter={e => (e.currentTarget.style.borderColor = 'color-mix(in srgb, var(--accent) 40%, transparent)')}
        onMouseLeave={e => (e.currentTarget.style.borderColor = 'var(--border-color)')}
      >
        {ctaLabel} {ctaIcon}
      </Link>

      {/* Illustration — SVG preview */}
      <div
        className="rounded-xl overflow-hidden border mt-auto"
        style={{
          backgroundColor: 'var(--bg-primary)',
          borderColor: 'var(--border-color)',
          aspectRatio: '16 / 10',
        }}
      >
        {illustration}
      </div>
    </div>
  );
}

/** Small feature point — иконка + title + описание.
    Pattern из TV features page (icon row под большим хиро-заголовком). */
function SmallFeature({ icon, title, desc }: { icon: React.ReactNode; title: string; desc: string }) {
  return (
    <div className="text-center md:text-left">
      <div className="flex md:justify-start justify-center mb-3" style={{ color: 'var(--accent)' }}>
        {icon}
      </div>
      <h3 className="font-semibold text-base md:text-lg mb-1.5"
        style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}>
        {title}
      </h3>
      <p className="text-xs md:text-sm" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
        {desc}
      </p>
    </div>
  );
}

