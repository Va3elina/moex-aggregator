/**
 * LandingPage — главная для незарегистрированных посетителей.
 *
 * Показывается на "/" когда нет auth'а. Задача: за 5 секунд понять что такое
 * Фрейм, увидеть pulse рынка, перейти в логин или тариф.
 *
 * Структура (после реструктуры):
 *   1. HERO — Tagline + Monte Carlo анимация + CTA
 *   2. 3 ТЕМАТИЧЕСКИХ ГРУППЫ ИНДИКАТОРОВ:
 *        — «Что чувствуют участники» (Сила рынка, Баффетт)
 *        — «Куда идут реальные деньги» (ОИ, Деньги в фондах, Состав фондов)
 *        — «Закономерности и текущая картина» (Сезонность, Карта рынка)
 *   3. FINAL CTA — «Начни разбираться в рынке»
 *
 * Залогиненные users видят OverviewPage (dashboard). Роутер делает conditional.
 */
import { Link } from 'react-router-dom';
import {
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
import ThermalLinesHero from '../components/landing/ThermalLinesHero';
import IndicatorGroup, { type Indicator } from '../components/landing/IndicatorGroup';
import MultiChartShowcase from '../components/landing/MultiChartShowcase';

// ═══════════════════════════════════════════════════════════
// 7 ИНДИКАТОРОВ → 3 ТЕМАТИЧЕСКИХ ГРУППЫ
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
    badge: 'Alfa тест',
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

      {/* ═══ HERO — full viewport с Dune-style thermal lines анимацией ═══
          Фон секции — var(--accent), поверх 3 анимированные "тепловые ленты"
          с переливом цвета сверху-вниз. Анимация в ThermalLinesHero компоненте.
          Текст белый поверх accent — консистентно в обеих editorial темах. */}
      <ThermalLinesHero height="100vh">
        <div className="h-full flex flex-col items-center justify-center text-center px-4 md:px-6">
          <div className="max-w-3xl mx-auto">
            <h1
              className="text-4xl md:text-7xl font-bold mb-6 md:mb-8"
              style={{
                color: '#FFFFFF',
                letterSpacing: '-0.035em',
                lineHeight: 0.95,
                textShadow: '0 2px 24px rgba(0,0,0,0.3)',
              }}
            >
              Посмотри. Подумай. Решай.
            </h1>
            <p
              className="text-base md:text-xl max-w-2xl mx-auto mb-8 md:mb-10"
              style={{
                color: 'rgba(255,255,255,0.85)',
                lineHeight: 1.55,
                textShadow: '0 1px 20px rgba(0,0,0,0.4)',
              }}
            >
              Лучшие инвестиционные решения начинаются с качественных данных.
              Российский рынок, индикаторы и аналитика — в одном дашборде.
            </p>
            {/* Hero buttons — кастомные классы hero-btn-* с ИНВЕРТИРОВАННОЙ
                тенью: чёрная кнопка → кремовая тень (light theme), кремовая
                кнопка → чёрная тень (dark theme). Shadow всегда контрастен с
                цветом кнопки и хорошо видим на оранжевом accent-фоне. */}
            <div className="flex flex-wrap items-center justify-center gap-4">
              <Link to="/login" className="hero-btn-primary">
                Попробовать бесплатно
                <span style={{ fontSize: 18, lineHeight: 1 }}>→</span>
              </Link>
              <Link to="/pricing" className="hero-btn-ghost">
                Тарифы
              </Link>
            </div>
            <p
              className="mt-12 md:mt-16"
              style={{
                color: 'rgba(255,255,255,0.7)',
                fontSize: 'var(--fs-2xs)',
                letterSpacing: '0.32em',
                fontWeight: 700,
                textTransform: 'uppercase',
              }}
            >
              Прокрутите вниз ↓
            </p>
          </div>
        </div>
      </ThermalLinesHero>

      {/* Wrapper для остального контента с обычным layout-padding */}
      <div className="max-w-7xl mx-auto px-4 md:px-6 py-14 md:py-20">

        {/* ═══ INTRO TO INDICATORS — editorial-стиль ═══
            Eyebrow → BIG H2 (Archivo bold, italic accent) → подзаголовок →
            4 KPI с вертикальными dividers (как Manifesto strip в handoff). */}
        <section
          className="mb-14 md:mb-20 relative overflow-hidden py-12 md:py-20"
          style={{
            marginLeft: 'calc(50% - 50vw)',
            marginRight: 'calc(50% - 50vw)',
            width: '100vw',
          }}
        >
          {/* Декоративный scattered фон со скриншотами в editorial-frame'ах */}
          <MultiChartShowcase />

          {/* Контент поверх фона. Editorial-frame backdrop-card даёт чистый
              paper-фон под текстом — "лист бумаги поверх стопки скриншотов".
              Без него текст конкурирует со scattered-photos и плохо читается
              в обеих темах. */}
          <div className="relative max-w-7xl mx-auto px-4 md:px-6" style={{ zIndex: 30 }}>
            <div
              className="editorial-frame max-w-5xl mx-auto"
              style={{
                padding: 'clamp(32px, 5vw, 64px) clamp(20px, 4vw, 48px)',
              }}
            >
              <div className="text-center mb-8 md:mb-12 max-w-3xl mx-auto">
                <h2
                  className="font-bold mb-4"
                  style={{
                    // Mobile: уменьшен min до 22px (был 28) и line-height расширен
                    // до 1.1 (был 0.95) — с tight'ом 0.95 descenders "меняют"
                    // и "решения" пересекались на 375vw. Desktop сохраняет
                    // dramatic editorial tight kerning через clamp max=64px.
                    color: 'var(--text-primary)',
                    fontSize: 'clamp(22px, 4.5vw + 0.5rem, 64px)',
                    letterSpacing: '-0.03em',
                    lineHeight: 1.05,
                  }}
                >
                  Данные, которые<br className="hidden md:inline"/>
                  {' '}
                  <span style={{ fontStyle: 'italic', marginLeft: '0.1em', marginRight: '0.18em' }}>меняют</span>
                  {' '}
                  решения
                </h2>
                <p
                  className="md:text-lg max-w-2xl mx-auto"
                  style={{ color: 'var(--text-secondary)', lineHeight: 1.55, fontSize: 'var(--fs-sm)' }}
                >
                  7 инструментов, разбитые на три группы. От настроения участников
                  до структуры реальных денежных потоков и исторических паттернов.
                </p>
              </div>

              {/* 4 KPI с вертикальными dividers между блоками на md+ */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-y-8 md:gap-y-0">
                <SmallFeature
                  icon={<Layers size={28} strokeWidth={1.5} />}
                  title="7 индикаторов"
                  desc="От просмотра цены до композитных метрик и сезонности"
                />
                <div className="md:border-l" style={{ borderColor: 'var(--border-color)' }}>
                  <SmallFeature
                    icon={<Database size={28} strokeWidth={1.5} />}
                    title="95+ акций MOEX"
                    desc="Все ключевые бумаги + фьючерсы + индексы + валюты"
                  />
                </div>
                <div className="md:border-l" style={{ borderColor: 'var(--border-color)' }}>
                  <SmallFeature
                    icon={<Clock size={28} strokeWidth={1.5} />}
                    title="С 1997 года"
                    desc="27 лет исторических данных, включая кризисы 2008/14/20/22"
                  />
                </div>
                <div className="md:border-l" style={{ borderColor: 'var(--border-color)' }}>
                  <SmallFeature
                    icon={<Zap size={28} strokeWidth={1.5} />}
                    title="Live-обновление"
                    desc="Данные обновляются каждые 5 минут в торговое время"
                  />
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* ═══ ГРУППА 1: Настроение рынка ═══ */}
        <IndicatorGroup
          title="Что чувствуют участники"
          subtitle="Композитные индексы и метрики настроения. Ширина рынка и макро-переоценка — численно."
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

        {/* ═══ FINAL CTA — editorial-frame card ═══
            Карточка на accent-фоне с inverse кнопками — финальный
            призыв в стиле "Final CTA + Footer" из handoff. */}
        <section
          className="editorial-frame text-center"
          style={{
            backgroundColor: 'var(--accent)',
            borderColor: 'var(--text-primary)',
            padding: 'clamp(40px, 6vw, 80px) clamp(20px, 4vw, 60px)',
            marginTop: 'clamp(24px, 4vw, 48px)',
          }}
        >
          <p
            className="mb-5 uppercase"
            style={{
              color: 'var(--text-inverse)',
              fontSize: 'var(--fs-2xs)',
              letterSpacing: '0.32em',
              fontWeight: 700,
              opacity: 0.8,
            }}
          >
            FRAME · ОТКРЫТЫЙ ДОСТУП
          </p>
          <h2
            className="font-bold mb-4 mx-auto"
            style={{
              color: 'var(--text-inverse)',
              fontSize: 'clamp(26px, 4.5vw + 0.5rem, 64px)',
              letterSpacing: '-0.035em',
              lineHeight: 1.0,
              maxWidth: '14ch',
            }}
          >
            Начни разбираться<br/>
            <span style={{ fontStyle: 'italic' }}>в рынке</span>
          </h2>
          <p
            className="mb-8 max-w-xl mx-auto"
            style={{
              color: 'var(--text-inverse)',
              opacity: 0.85,
              fontSize: 'var(--fs-sm)',
              lineHeight: 1.55,
            }}
          >
            Бесплатный доступ к базовым индикаторам — без оплаты и без карты.
            Подписка Pro или Premium — когда и если понадобятся продвинутые фичи.
          </p>
          <div className="flex flex-wrap items-center justify-center gap-3">
            <Link
              to="/login"
              className="editorial-press px-7 py-4 font-bold uppercase"
              style={{
                backgroundColor: 'var(--text-primary)',
                color: 'var(--text-inverse)',
                border: '1.5px solid var(--text-primary)',
                fontSize: 'var(--fs-sm)',
                letterSpacing: '0.16em',
                textDecoration: 'none',
              }}
            >
              Попробовать бесплатно →
            </Link>
            <Link
              to="/pricing"
              className="editorial-press px-7 py-4 font-bold uppercase"
              style={{
                backgroundColor: 'transparent',
                color: 'var(--text-inverse)',
                border: '1.5px solid var(--text-inverse)',
                fontSize: 'var(--fs-sm)',
                letterSpacing: '0.16em',
                textDecoration: 'none',
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

/** SmallFeature — editorial KPI block: иконка + большой title + описание.
    Pattern: accent eyebrow icon → BIG title (Archivo bold) → desc.
    На desktop вертикальные dividers между блоками (border-l). */
function SmallFeature({ icon, title, desc }: { icon: ReactNode; title: string; desc: string }) {
  return (
    <div className="text-center md:text-left md:px-5">
      <div className="flex md:justify-start justify-center mb-4" style={{ color: 'var(--accent)' }}>
        {icon}
      </div>
      <h3
        className="font-bold mb-2"
        style={{
          color: 'var(--text-primary)',
          fontSize: 'clamp(15px, 0.4vw + 0.7rem, 20px)',
          letterSpacing: '-0.015em',
          lineHeight: 1.2,
        }}
      >
        {title}
      </h3>
      <p
        style={{
          color: 'var(--text-secondary)',
          fontSize: 'var(--fs-xs)',
          lineHeight: 1.55,
        }}
      >
        {desc}
      </p>
    </div>
  );
}
