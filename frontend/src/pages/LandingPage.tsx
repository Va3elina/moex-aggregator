/**
 * LandingPage — главная для незарегистрированных посетителей.
 *
 * Показывается на "/" когда нет auth'а. Задача: за 5 секунд понять что такое
 * Фрейм, увидеть pulse рынка, перейти в логин или тариф.
 *
 * Залогиненные users видят OverviewPage (dashboard). Роутер делает conditional.
 */
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ArrowRight,
  Gauge,
  Activity,
  Scale,
  Grid3X3,
  BarChart3,
  Wallet,
  LayoutGrid,
  CalendarDays,
} from 'lucide-react';
import {
  getFearIndex,
  getBreadthCurrent,
  getBuffettCapGdp,
} from '../services/api';

/** Список всех индикаторов с коротким описанием для "card grid" внизу. */
const INDICATORS = [
  { path: '/heatmap', title: 'Карта рынка', desc: 'Акции MOEX по секторам, размер = капитализация', icon: Grid3X3 },
  { path: '/oi', title: 'Открытый интерес', desc: 'Позиции участников по фьючерсам Мосбиржи', icon: BarChart3 },
  { path: '/funds-money', title: 'Деньги в фондах', desc: 'Динамика СЧА и притоки-оттоки фондов', icon: Wallet },
  { path: '/funds-catalog', title: 'Состав фондов', desc: 'Портфели фондов акций и облигаций', icon: LayoutGrid },
  { path: '/strength', title: 'Сила рынка', desc: '% акций выше EMA 50/100/200', icon: Activity },
  { path: '/buffett', title: 'Индикатор Баффетта', desc: 'Капитализация / ВВП + Cap / M2', icon: Scale },
  { path: '/seasonality', title: 'Сезонность', desc: 'Среднее изменение цены по периодам', icon: CalendarDays },
  { path: '/fear', title: 'Индекс страха', desc: 'Настроения инвесторов по 4 метрикам', icon: Gauge },
];

export default function LandingPage() {
  const [fear, setFear] = useState<number | null>(null);
  const [strength, setStrength] = useState<number | null>(null);
  const [buffett, setBuffett] = useState<number | null>(null);
  const [lastUpdate, setLastUpdate] = useState<string>('');

  useEffect(() => {
    // Параллельно грузим 3 метрики — если одна упадёт, остальные покажутся.
    Promise.allSettled([
      getFearIndex().then(r => setFear(r.fear_index)),
      getBreadthCurrent(50, 'imoex').then(r => setStrength(r.percent_above)),
      getBuffettCapGdp('1y').then(r => {
        const last = r.data[r.data.length - 1];
        if (last) setBuffett(last.buffett);
      }),
    ]).then(() => {
      setLastUpdate(
        new Date().toLocaleTimeString('ru-RU', {
          hour: '2-digit',
          minute: '2-digit',
          timeZone: 'Europe/Moscow',
        })
      );
    });
  }, []);

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-8 md:py-12">

      {/* ═══ HERO ═══ */}
      <section className="text-center mb-10 md:mb-14">
        <h1
          className="text-4xl md:text-6xl font-bold mb-4"
          style={{ color: 'var(--text-primary)', letterSpacing: '-0.02em', lineHeight: 1.05 }}
        >
          Аналитика Московской биржи
        </h1>
        <p className="text-base md:text-lg" style={{ color: 'var(--text-secondary)' }}>
          {lastUpdate ? (
            <>8 индикаторов · таймфреймы 5м / 1ч / 1д · обновлено в <span style={{ color: 'var(--text-primary)' }}>{lastUpdate}</span> МСК</>
          ) : (
            '8 индикаторов · таймфреймы 5м / 1ч / 1д · live с Мосбиржи'
          )}
        </p>
      </section>

      {/* ═══ 4 HERO METRICS ═══ */}
      <section className="grid grid-cols-2 md:grid-cols-4 gap-3 md:gap-4 mb-12 md:mb-16">
        <MetricCard
          label="Индекс страха"
          value={fear}
          unit="/ 100"
          color="amber"
          description={fearClassification(fear)}
        />
        <MetricCard
          label="Сила рынка"
          value={strength}
          unit="%"
          color={strength !== null && strength > 50 ? 'green' : 'red'}
          description={strength !== null ? `${strength.toFixed(0)}% акций > EMA50` : ''}
        />
        <MetricCard
          label="Баффетт"
          value={buffett}
          unit="%"
          color="amber"
          description="Cap / GDP"
        />
        <MetricCard
          label="Индикаторов"
          value={8}
          unit=""
          color="green"
          description="в одной платформе"
          decimals={0}
        />
      </section>

      {/* ═══ INDICATOR GRID ═══ */}
      <section className="mb-14">
        <div className="flex items-center gap-3 mb-6 md:mb-8">
          <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
          <p
            className="text-xs uppercase"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.15em', fontWeight: 600 }}
          >
            Что есть на Фрейме
          </p>
          <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3 md:gap-4">
          {INDICATORS.map(ind => (
            <Link
              key={ind.path}
              to={ind.path}
              className="group block p-5 border transition-all"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                borderColor: 'var(--border-color)',
                borderRadius: 'var(--radius-lg)',
              }}
              onMouseEnter={e => (e.currentTarget.style.borderColor = 'color-mix(in srgb, var(--accent) 40%, transparent)')}
              onMouseLeave={e => (e.currentTarget.style.borderColor = 'var(--border-color)')}
            >
              <div className="flex items-start gap-3 mb-2">
                <div
                  className="flex items-center justify-center flex-shrink-0"
                  style={{
                    width: 36,
                    height: 36,
                    borderRadius: 'var(--radius-md)',
                    background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
                    color: 'var(--accent)',
                  }}
                >
                  <ind.icon size={18} strokeWidth={1.8} />
                </div>
                <div className="flex-1">
                  <h3
                    className="font-semibold text-base md:text-lg"
                    style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
                  >
                    {ind.title}
                  </h3>
                </div>
                <ArrowRight
                  size={18}
                  className="opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0 mt-2"
                  style={{ color: 'var(--accent)' }}
                />
              </div>
              <p className="text-sm" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
                {ind.desc}
              </p>
            </Link>
          ))}
        </div>
      </section>

      {/* ═══ VALUE PROP STATS ═══ */}
      <section
        className="py-8 md:py-10 mb-12 md:mb-14 border-y"
        style={{ borderColor: 'var(--border-color)' }}
      >
        <div className="grid grid-cols-3 gap-4 md:gap-8 max-w-3xl mx-auto text-center">
          <Stat value="1,5 млн+" label="свечей в базе" />
          <Stat value="с 1997" label="года истории" />
          <Stat value="95+" label="акций Мосбиржи" />
        </div>
      </section>

      {/* ═══ CTA ═══ */}
      <section className="text-center">
        <h2
          className="text-2xl md:text-3xl font-bold mb-3"
          style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
        >
          Попробуйте бесплатно
        </h2>
        <p className="mb-6 text-sm md:text-base" style={{ color: 'var(--text-secondary)' }}>
          Базовый доступ — без оплаты. Pro и Premium — продвинутые фичи.
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
            Войти
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
            Посмотреть тарифы
          </Link>
        </div>
      </section>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════
// SUBCOMPONENTS
// ═══════════════════════════════════════════════════════════

interface MetricCardProps {
  label: string;
  value: number | null;
  unit: string;
  color: 'green' | 'amber' | 'red';
  description?: string;
  decimals?: number;
}

function MetricCard({ label, value, unit, color, description, decimals = 1 }: MetricCardProps) {
  const colorVar = color === 'amber' ? 'var(--warning)' : color === 'red' ? 'var(--danger)' : 'var(--accent)';
  return (
    <div
      className="p-4 md:p-5 border"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
        borderRadius: 'var(--radius-lg)',
      }}
    >
      <p
        className="text-[10px] md:text-xs uppercase mb-2"
        style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
      >
        {label}
      </p>
      <div className="flex items-baseline gap-1 mb-1">
        <span
          className="text-2xl md:text-4xl font-bold"
          style={{
            color: colorVar,
            fontFamily: "'IBM Plex Mono', 'SF Mono', monospace",
            fontVariantNumeric: 'tabular-nums',
            letterSpacing: '-0.02em',
          }}
        >
          {value !== null ? value.toFixed(decimals) : '—'}
        </span>
        <span className="text-xs md:text-sm" style={{ color: 'var(--text-muted)' }}>
          {unit}
        </span>
      </div>
      {description && (
        <p className="text-[10px] md:text-xs truncate" style={{ color: 'var(--text-secondary)' }}>
          {description}
        </p>
      )}
    </div>
  );
}

function Stat({ value, label }: { value: string; label: string }) {
  return (
    <div>
      <div
        className="text-2xl md:text-5xl font-bold mb-1"
        style={{
          color: 'var(--accent)',
          fontFamily: "'IBM Plex Mono', 'SF Mono', monospace",
          fontVariantNumeric: 'tabular-nums',
          whiteSpace: 'nowrap',
          letterSpacing: '-0.03em',
        }}
      >
        {value}
      </div>
      <p className="text-xs md:text-sm" style={{ color: 'var(--text-secondary)' }}>
        {label}
      </p>
    </div>
  );
}

function fearClassification(fear: number | null): string {
  if (fear === null) return '';
  if (fear <= 20) return 'крайний страх';
  if (fear <= 40) return 'страх';
  if (fear <= 60) return 'нейтрально';
  if (fear <= 80) return 'жадность';
  return 'крайняя жадность';
}
