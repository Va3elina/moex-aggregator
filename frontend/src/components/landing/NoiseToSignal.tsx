/**
 * NoiseToSignal — Monte Carlo симуляция → signal path.
 *
 * Концепт (10-сек cinematic loop):
 *   [0-3s]  60 Monte Carlo траекторий разворачиваются от общей точки (слева)
 *           по geometric Brownian motion — fan-out эффект, классическая
 *           визуализация вероятностного моделирования (Black-Scholes, VaR).
 *   [3-5s]  Все пути видны → формируют "cone of uncertainty"
 *   [5-6s]  Signal-линия мигает 2x (lock-on эффект) — "среди возможных
 *           сценариев мы находим правильный"
 *   [6-8s]  Шум fade → Signal прорисовывается smooth exponential-кривой
 *           (equity growth trajectory) в верх-правый угол
 *   [8-10s] Hold + reset → loop
 *
 * Математика симуляции:
 *   Box-Muller transform: из 2 uniform random → N(0,1) gaussian
 *   Per step: y += drift + z·volatility
 *   drift уникален для каждой траектории (some up, some down, some sideways)
 *
 * Это не просто анимация — это узнаваемая финансовая концепция.
 */
import { useMemo } from 'react';

// Deterministic seeded random
function seededRandom(seed: number): () => number {
  let state = seed * 2654435761 % 2147483647;
  return () => {
    state = (state * 16807) % 2147483647;
    return state / 2147483647;
  };
}

// Box-Muller: uniform(0,1) → normal(0,1)
function normalRandom(rand: () => number): number {
  const u1 = Math.max(rand(), 0.0001);
  const u2 = rand();
  return Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

/**
 * Генерация Monte Carlo траектории по Geometric Brownian Motion.
 * Все пути начинают в одной точке и расходятся согласно волатильности.
 */
function generateMonteCarloPath(seedId: number): string {
  const rand = seededRandom(seedId + 1);
  const steps = 60;

  // Одинаковая стартовая точка для ВСЕХ путей — это ключевой visual Monte Carlo
  const startX = 0;
  const startY = 32;
  const stepX = 100 / steps;

  // Уникальный drift на каждую траекторию: нормально распределён около 0
  // → большинство путей "никуда особо не идут", меньшинство — сильно рост/падение
  const drift = (rand() - 0.5) * 0.3;
  // Волатильность: 0.8-2.2 — умеренный шум
  const volatility = 0.8 + rand() * 1.4;

  let y = startY;
  const points = [`M ${startX},${startY.toFixed(2)}`];

  for (let i = 1; i <= steps; i++) {
    // GBM step: y += drift + gaussian * volatility
    const z = normalRandom(rand);
    y += drift + z * volatility;

    // Мягкий clamp к видимой области (не обрезаем жёстко, иначе теряется organic look)
    if (y < -10) y = -10 + (y - (-10)) * 0.3;
    if (y > 70) y = 70 + (y - 70) * 0.3;

    points.push(`L ${(i * stepX).toFixed(2)},${y.toFixed(2)}`);
  }

  return points.join(' ');
}

/**
 * Signal path — equity growth от нижнего-левого угла до верхнего-правого.
 * viewBox 100x60: (0, 52) ≈ bottom-left, (100, 3) ≈ top-right.
 * Bezier с ускорением — реалистичный экспоненциальный рост.
 */
function generateSignalPath(): string {
  return 'M 0,52 C 18,48 32,40 48,26 S 78,10 100,3';
}

// Палитра для Monte Carlo — спокойные сине-фиолетовые + тёплые желтые
// → контрастирует с ярко-зелёным signal
const MC_COLORS = [
  '#4DA3FF', '#9D4DFF', '#06B6D4', '#A78BFA',
  '#FCD34D', '#F7931A', '#5FADD9', '#8B5CF6',
  '#F59E0B', '#14B8A6',
];

const PATH_COUNT = 60;

export default function NoiseToSignal() {
  const paths = useMemo(() => {
    return Array.from({ length: PATH_COUNT }, (_, i) => {
      const rand = seededRandom(i * 317 + 11);
      return {
        d: generateMonteCarloPath(i * 7 + 13),
        color: MC_COLORS[i % MC_COLORS.length],
        width: 0.2 + rand() * 0.35,
        opacity: 0.35 + rand() * 0.35,
        delay: rand() * 2.2, // staggered entry 0-2.2s
      };
    });
  }, []);

  const signalPath = useMemo(() => generateSignalPath(), []);

  return (
    <div
      className="absolute inset-0 pointer-events-none overflow-hidden"
      aria-hidden="true"
    >
      <svg
        viewBox="0 0 100 60"
        preserveAspectRatio="xMidYMid slice"
        className="w-full h-full"
      >
        <defs>
          {/* Glow для signal */}
          <filter id="signal-glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="0.6" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          {/* Signal gradient — очень лёгкий fade только на самых концах,
              чтобы линия не торчала "обрубленными" краями за viewport.
              Средняя часть (5-95%) — full opacity → blink показывает ЦЕЛИКОМ,
              а не только правый конец (прежний баг с stopOpacity=0.3 на старте). */}
          <linearGradient id="signal-grad" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%"   stopColor="var(--accent)" stopOpacity="0" />
            <stop offset="5%"   stopColor="var(--accent)" stopOpacity="1" />
            <stop offset="95%"  stopColor="var(--accent)" stopOpacity="1" />
            <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
          </linearGradient>
        </defs>

        {/* 60 Monte Carlo траекторий */}
        {paths.map((p, i) => (
          <path
            key={i}
            d={p.d}
            fill="none"
            stroke={p.color}
            strokeWidth={p.width}
            strokeLinecap="round"
            strokeLinejoin="round"
            style={{
              strokeDasharray: 300,
              strokeDashoffset: 300,
              opacity: 0,
              animation: `noise-line 10s ease-in-out infinite`,
              animationDelay: `${p.delay}s`,
              ['--line-peak-opacity' as string]: String(p.opacity),
            }}
          />
        ))}

        {/* Ghost preview — ПОЛНЫЙ путь виден целиком, dim.
            Цель: показать "вот тут пойдёт сигнал" 2 быстрыми dim-бликами,
            потом фадится к моменту когда real-линия начинает расти. */}
        <path
          d={signalPath}
          fill="none"
          stroke="var(--accent)"
          strokeWidth="1.3"
          strokeLinecap="round"
          strokeLinejoin="round"
          style={{
            opacity: 0,
            animation: 'signal-ghost 10s ease-in-out infinite',
          }}
        />

        {/* Real signal — РАСТЁТ снизу-слева вверх-вправо через stroke-dashoffset.
            pathLength="100" нормализует длину → dashoffset от 100 (скрыт)
            до 0 (весь нарисован). Draw fast (~0.7s). */}
        <path
          d={signalPath}
          pathLength="100"
          fill="none"
          stroke="url(#signal-grad)"
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
          filter="url(#signal-glow)"
          style={{
            strokeDasharray: 100,
            strokeDashoffset: 100,
            opacity: 0,
            animation: 'signal-grow 10s ease-in-out infinite',
          }}
        />
      </svg>
    </div>
  );
}
