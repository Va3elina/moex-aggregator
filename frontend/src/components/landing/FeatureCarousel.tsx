/**
 * FeatureCarousel — TradingView-style animated carousel для Landing'а.
 *
 * Архитектура:
 *  - Слева: вертикальный список всех индикаторов, активный — highlighted
 *  - Справа: preview-чарт активного индикатора
 *  - Auto-rotation каждые N секунд (пауза при hover / клике)
 *  - Controls: prev / next / play-pause + counter "3/9"
 *  - Клик по элементу списка → переход к нему вручную
 *
 * Previews — static SVG-компоненты (см. ./previews/).
 * Не fetch'ат реальные данные → страница грузится мгновенно.
 */
import { useEffect, useRef, useState } from 'react';
import { ChevronLeft, ChevronRight, Pause, Play } from 'lucide-react';
import {
  FearPreview,
  HeatmapPreview,
  OpenInterestPreview,
  FundsFlowPreview,
  FundsDonutPreview,
  StrengthPreview,
  BuffettPreview,
  SeasonalityPreview,
} from './previews';

type Category = 'macro' | 'deep' | 'funds' | 'viz';

const CATEGORY_LABELS: Record<Category, string> = {
  macro: 'Настроения рынка',
  deep: 'Глубокий анализ',
  funds: 'Фонды',
  viz: 'Визуализация',
};

interface Feature {
  id: string;
  title: string;
  desc: string;
  category: Category;
  preview: React.ReactNode;
}

// Порядок: группируем по category (macro → deep → funds → viz)
// чтобы при auto-rotation индикаторы одной темы шли подряд.
const FEATURES: Feature[] = [
  // === MACRO — настроения рынка в целом ===
  {
    id: 'fear',
    title: 'Индекс страха',
    desc: 'Композитный индикатор настроений по 4 метрикам. Когда все жадны — время осторожничать, когда все в страхе — искать возможности.',
    category: 'macro',
    preview: <FearPreview />,
  },
  {
    id: 'strength',
    title: 'Сила рынка',
    desc: '% акций выше EMA-50/100/200. Индекс может расти на 2 бумагах — сила рынка покажет реальную ширину движения.',
    category: 'macro',
    preview: <StrengthPreview />,
  },
  {
    id: 'buffett',
    title: 'Индикатор Баффетта',
    desc: 'Капитализация рынка / ВВП + Cap/M2. Любимый индикатор Оракула из Омахи адаптирован под российский рынок.',
    category: 'macro',
    preview: <BuffettPreview />,
  },
  // === DEEP — инструменты глубокого анализа ===
  {
    id: 'seasonality',
    title: 'Сезонность',
    desc: 'Средняя динамика по периодам: внутри дня, дням недели, числам месяца, месяцам. Статистика с 1997 года.',
    category: 'deep',
    preview: <SeasonalityPreview />,
  },
  {
    id: 'oi',
    title: 'Открытый интерес',
    desc: 'Позиции участников по фьючерсам с 2007 года. Разбивка по физикам/юрикам/нерезидентам — видите где деньги на самом деле.',
    category: 'deep',
    preview: <OpenInterestPreview />,
  },
  // === FUNDS — фондовая аналитика ===
  {
    id: 'funds-money',
    title: 'Деньги в фондах',
    desc: 'СЧА и притоки/оттоки по категориям: акции, облигации, золото, денежный рынок. Куда идут деньги — туда идёт рынок.',
    category: 'funds',
    preview: <FundsFlowPreview />,
  },
  {
    id: 'funds-catalog',
    title: 'Состав фондов',
    desc: 'Холдинги 36 российских ETF. Видите из чего на самом деле состоят EQMX, TSOX, SBMX — не по названию, по активам.',
    category: 'funds',
    preview: <FundsDonutPreview />,
  },
  // === VIZ — визуализация ===
  {
    id: 'heatmap',
    title: 'Карта рынка',
    desc: 'Все акции MOEX на одном экране. Размер — по капитализации, цвет — по динамике. За секунду видите кто двигает рынок.',
    category: 'viz',
    preview: <HeatmapPreview />,
  },
];

const AUTO_ROTATE_MS = 6000;

export default function FeatureCarousel() {
  const [index, setIndex] = useState(0);
  const [paused, setPaused] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (paused) return;
    intervalRef.current = setInterval(() => {
      setIndex(i => (i + 1) % FEATURES.length);
    }, AUTO_ROTATE_MS);
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [paused]);

  const goPrev = () => setIndex(i => (i - 1 + FEATURES.length) % FEATURES.length);
  const goNext = () => setIndex(i => (i + 1) % FEATURES.length);
  const togglePause = () => setPaused(p => !p);

  const current = FEATURES[index];

  return (
    <div
      className="rounded-2xl border p-4 md:p-6"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
      }}
    >
      <div className="grid grid-cols-1 lg:grid-cols-[1fr_1.4fr] gap-4 md:gap-8 items-center">

        {/* LEFT: list of features (active highlighted) */}
        <div className="space-y-0.5 md:space-y-1">
          {FEATURES.map((f, i) => {
            const isActive = i === index;
            return (
              <button
                key={f.id}
                onClick={() => setIndex(i)}
                className="w-full text-left py-2 md:py-3 px-3 md:px-4 transition-all"
                style={{
                  borderRadius: 'var(--radius-md, 8px)',
                  backgroundColor: isActive ? 'color-mix(in srgb, var(--accent) 8%, transparent)' : 'transparent',
                  borderLeft: `3px solid ${isActive ? 'var(--accent)' : 'transparent'}`,
                  opacity: isActive ? 1 : 0.5,
                }}
              >
                {isActive && (
                  <p
                    className="text-[9px] md:text-[10px] uppercase mb-1"
                    style={{
                      color: 'var(--accent)',
                      letterSpacing: '0.15em',
                      fontWeight: 700,
                    }}
                  >
                    {CATEGORY_LABELS[f.category]}
                  </p>
                )}
                <h3
                  className="font-semibold text-sm md:text-lg mb-1"
                  style={{
                    color: isActive ? 'var(--text-primary)' : 'var(--text-secondary)',
                    letterSpacing: '-0.01em',
                  }}
                >
                  {f.title}
                </h3>
                {isActive && (
                  <p
                    className="text-xs md:text-sm"
                    style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}
                  >
                    {f.desc}
                  </p>
                )}
              </button>
            );
          })}
        </div>

        {/* RIGHT: preview chart */}
        <div
          key={current.id}
          className="relative rounded-xl overflow-hidden feature-preview-fade"
          style={{
            backgroundColor: 'var(--bg-primary)',
            border: '1px solid var(--border-color)',
            aspectRatio: '16 / 10',
            minHeight: 260,
          }}
        >
          {current.preview}
        </div>
      </div>

      {/* Controls */}
      <div className="flex items-center justify-end gap-1 mt-4 md:mt-6">
        <button
          onClick={goPrev}
          className="p-2 rounded-md transition-colors hover:bg-white/5"
          style={{ color: 'var(--text-secondary)' }}
          aria-label="Предыдущий"
        >
          <ChevronLeft size={16} />
        </button>
        <span
          className="text-xs mx-2 tabular-nums"
          style={{ color: 'var(--text-muted)', fontFamily: "'IBM Plex Mono', monospace" }}
        >
          {index + 1} / {FEATURES.length}
        </span>
        <button
          onClick={goNext}
          className="p-2 rounded-md transition-colors hover:bg-white/5"
          style={{ color: 'var(--text-secondary)' }}
          aria-label="Следующий"
        >
          <ChevronRight size={16} />
        </button>
        <button
          onClick={togglePause}
          className="p-2 rounded-md transition-colors hover:bg-white/5 ml-1"
          style={{ color: 'var(--text-secondary)' }}
          aria-label={paused ? 'Play' : 'Pause'}
        >
          {paused ? <Play size={14} /> : <Pause size={14} />}
        </button>
      </div>
    </div>
  );
}
