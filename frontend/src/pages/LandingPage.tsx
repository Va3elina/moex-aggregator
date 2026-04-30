/**
 * LandingPage — главная для незарегистрированных посетителей.
 *
 * Показывается на "/" когда нет auth'а. Задача: за 5 секунд понять что такое
 * Фрейм, увидеть pulse рынка, перейти в логин или тариф.
 *
 * Структура (после реструктуры):
 *   1. HERO — Tagline + Monte Carlo анимация + CTA
 *   2. 3 ТЕМАТИЧЕСКИХ ГРУППЫ ИНДИКАТОРОВ:
 *        — «Что чувствуют участники» (Индекс страха, Сила рынка, Баффетт)
 *        — «Куда идут реальные деньги» (ОИ, Деньги в фондах, Состав фондов)
 *        — «Закономерности и текущая картина» (Сезонность, Карта рынка)
 *   3. FINAL CTA — «Начни разбираться в рынке»
 *
 * Залогиненные users видят OverviewPage (dashboard). Роутер делает conditional.
 */
import { Link } from 'react-router-dom';
import {
  Gauge,
  Grid3X3,
  Wallet,
  CalendarDays,
  TrendingUp,
  BarChart3,
  Activity,
  PieChart,
  ArrowRight,
  Layers,
  Database,
  Clock,
  Zap,
} from 'lucide-react';
import type { ReactNode } from 'react';
import MarketPulse from '../components/landing/MarketPulse';
import IndicatorGroup, { type Indicator } from '../components/landing/IndicatorGroup';
import MultiChartShowcase from '../components/landing/MultiChartShowcase';
import { FearPreview } from '../components/landing/previews';

// ═══════════════════════════════════════════════════════════
// 8 ИНДИКАТОРОВ → 3 ТЕМАТИЧЕСКИХ ГРУППЫ
// ═══════════════════════════════════════════════════════════

const ICON_SIZE = 20;
const CTA_ICON_SIZE = 14;

// Хелпер: добавляет videoUrl + posterUrl по name (видео в /videos/<name>.{webm,mp4})
const v = (name: string) => ({
  videoUrl: `/videos/${name}.webm`,
  posterUrl: `/videos/${name}-poster.jpg`,
});

// Группа 1: Что чувствуют участники
const SENTIMENT_INDICATORS: Indicator[] = [
  {
    title: 'Индекс страха',
    desc: 'Композитный индекс по 4 метрикам: волатильность, breadth, ликвидность, momentum. Когда все жадны — осторожно.',
    icon: <Gauge size={ICON_SIZE} strokeWidth={2} />,
    ctaIcon: <ArrowRight size={CTA_ICON_SIZE} />,
    href: '/fear',
    illustration: <FearPreview />,  // оставляем SVG-fallback (видео не делаем)
  },
  {
    title: 'Сила рынка',
    desc: 'IMOEX + breadth по 90 акциям. Виден ли рост на широком фронте или только на нескольких тяжеловесах.',
    icon: <Activity size={ICON_SIZE} strokeWidth={2} />,
    ctaIcon: <ArrowRight size={CTA_ICON_SIZE} />,
    href: '/strength',
    ...v('strength'),
  },
  {
    title: 'Индикатор Баффетта',
    desc: 'Капитализация рынка к ВВП и М2. Классическая макро-метрика переоценки/недооценки рынка.',
    icon: <TrendingUp size={ICON_SIZE} strokeWidth={2} />,
    ctaIcon: <ArrowRight size={CTA_ICON_SIZE} />,
    href: '/buffett',
    ...v('buffett'),
  },
];

// Группа 2: Куда идут реальные деньги
const MONEY_FLOW_INDICATORS: Indicator[] = [
  {
    title: 'Открытый интерес',
    desc: 'Позиции участников по фьючерсам с 2007 года. Разбивка физики/юрики/нерезиденты — где деньги на самом деле.',
    icon: <BarChart3 size={ICON_SIZE} strokeWidth={2} />,
    ctaIcon: <ArrowRight size={CTA_ICON_SIZE} />,
    href: '/oi',
    ...v('oi'),
  },
  {
    title: 'Деньги в фондах',
    desc: '36 российских ETF: СЧА, притоки-оттоки по категориям. Куда идут большие деньги — туда идёт рынок.',
    icon: <Wallet size={ICON_SIZE} strokeWidth={2} />,
    ctaIcon: <ArrowRight size={CTA_ICON_SIZE} />,
    href: '/funds-money',
    ...v('funds-money'),
  },
  {
    title: 'Состав фондов',
    desc: 'Какие акции внутри каждого ETF и в какой пропорции. Реальные позиции фондов — без оценок и рейтингов.',
    icon: <PieChart size={ICON_SIZE} strokeWidth={2} />,
    ctaIcon: <ArrowRight size={CTA_ICON_SIZE} />,
    href: '/funds-catalog',
    ...v('funds-catalog'),
  },
];

// Группа 3: Закономерности и текущая картина
const PATTERNS_INDICATORS: Indicator[] = [
  {
    title: 'Сезонность',
    desc: 'Средняя динамика по дням, месяцам, годам. Паттерны с 1997 года — на одном экране. Сравнивайте любые периоды.',
    icon: <CalendarDays size={ICON_SIZE} strokeWidth={2} />,
    ctaIcon: <ArrowRight size={CTA_ICON_SIZE} />,
    href: '/seasonality',
    ...v('seasonality'),
  },
  {
    title: 'Карта рынка',
    desc: 'Все акции MOEX на одном экране. Размер — капитализация, цвет — дневная динамика. За взгляд видите кто двигает рынок.',
    icon: <Grid3X3 size={ICON_SIZE} strokeWidth={2} />,
    ctaIcon: <ArrowRight size={CTA_ICON_SIZE} />,
    href: '/heatmap',
    ...v('heatmap'),
  },
];

export default function LandingPage() {
  return (
    <div>

      {/* ═══ HERO — full viewport (100vh) ═══
          Tagline + CTAs + анимация "шум → сигнал" фоном на весь экран.
          Layout: анимация absolute (z-0), текст поверх (z-10).
          min-h-screen гарантирует что секция займёт весь viewport,
          следующая секция появится только при scroll. */}
      <section className="relative min-h-screen flex flex-col items-center justify-center overflow-hidden px-4 md:px-6">
        {/* Анимация фоном — scrolling candlestick chart */}
        <MarketPulse />

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

        {/* ═══ INTRO TO INDICATORS — заголовок + 4 факта на scattered-фоне ═══
            Section full-bleed: выходит за max-w-7xl до краёв viewport через
            margin trick. Scattered scattered-фон занимает всю ширину экрана. */}
        <section
          className="mb-14 md:mb-20 relative overflow-hidden py-12 md:py-20"
          style={{
            marginLeft: 'calc(50% - 50vw)',
            marginRight: 'calc(50% - 50vw)',
            width: '100vw',
          }}
        >
          {/* Декоративный scattered фон из 12 разбросанных скриншотов */}
          <MultiChartShowcase />

          {/* Контент поверх фона */}
          <div className="relative" style={{ zIndex: 30 }}>
            <div className="text-center mb-10 md:mb-14 max-w-3xl mx-auto">
              <h2
                className="text-3xl md:text-5xl font-bold mb-4"
                style={{ color: 'var(--text-primary)', letterSpacing: '-0.03em', lineHeight: 1.05 }}
              >
                Индикаторы, которые<br className="hidden md:inline"/> меняют решения
              </h2>
              <p className="text-sm md:text-lg" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
                8 инструментов, разбитые на три группы. От настроения участников
                до структуры реальных денежных потоков и исторических паттернов.
              </p>
            </div>

            {/* 4 small feature points — числовые «доказательства» */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-6 md:gap-8 max-w-5xl mx-auto">
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
          </div>
        </section>

        {/* ═══ ГРУППА 1: Настроение рынка ═══ */}
        <IndicatorGroup
          title="Что чувствуют участники"
          subtitle="Композитные индексы и метрики настроения. Жадность, страх, переоценка — численно."
          indicators={SENTIMENT_INDICATORS}
        />

        {/* ═══ ГРУППА 2: Деньги участников ═══ */}
        <IndicatorGroup
          title="Куда идут реальные деньги"
          subtitle="Позиции на фьючерсах, потоки в ETF, структура портфелей фондов. Большие деньги двигают рынок — следите за ними."
          indicators={MONEY_FLOW_INDICATORS}
        />

        {/* ═══ ГРУППА 3: Паттерны и текущая картина ═══ */}
        <IndicatorGroup
          title="Закономерности и текущая картина"
          subtitle="Что повторяется год от года, и что на рынке прямо сейчас. Один взгляд — и всё видно."
          indicators={PATTERNS_INDICATORS}
        />

        {/* ═══ FINAL CTA ═══
            Второй шанс конверсии после того как user прокрутил всю landing.
            Текст фокусирует на "no risk to try" — бесплатный базовый доступ. */}
        <section className="text-center pt-6 md:pt-10">
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

/** Small feature point — иконка + title + описание.
    Pattern из TV features page (icon row под большим хиро-заголовком). */
function SmallFeature({ icon, title, desc }: { icon: ReactNode; title: string; desc: string }) {
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
