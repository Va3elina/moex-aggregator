/**
 * MarketPulse — hero-анимация с прокручивающимися candlestick'ами.
 *
 * Концепт: бесконечно прокручивающийся справа-налево candlestick-чарт
 * с MA-линией и volume-барами. Как живой ticker tape Bloomberg-style.
 * Symbolизирует "real-time market data" — основная тема Фрейм'а.
 *
 * Реализация:
 *   - 80 candles, генерируются deterministic'но (seeded random)
 *   - Дублируются в SVG → 160 candles, plotted side-by-side
 *   - CSS animation translateX移ивает группу на половину её ширины
 *     (TOTAL_W) → seamless loop без видимых "swap"
 *   - MA-линия проходит сверху, volume-bars снизу
 *   - Bottom fade overlay → текст hero читается над анимацией
 */
import { useMemo } from 'react';

const NUM_CANDLES = 80;
const CANDLE_W = 14;
const TOTAL_W = NUM_CANDLES * CANDLE_W;
const VB_W = TOTAL_W * 2;
const VB_H = 540;

const PRICE_AREA = { top: 40, height: 320 };
const VOLUME_AREA = { top: 400, height: 100 };

const C_GREEN = '#2EE59D';
const C_RED = '#FF4D4D';

interface Candle {
  open: number;
  close: number;
  high: number;
  low: number;
  volume: number;
}

function seededRandom(seed: number): () => number {
  let state = (seed * 2654435761) % 2147483647;
  return () => {
    state = (state * 16807) % 2147483647;
    return state / 2147483647;
  };
}

function generateCandles(seed = 42): Candle[] {
  const rand = seededRandom(seed);
  let price = 100;
  let trend = 0;
  const out: Candle[] = [];
  for (let i = 0; i < NUM_CANDLES; i++) {
    const open = price;
    // Slowly evolving trend + per-step noise → realistic walk
    trend = trend * 0.85 + (rand() - 0.5) * 1.2;
    const move = trend + (rand() - 0.5) * 3;
    const close = open + move;
    const wick = 0.5 + rand() * 2;
    const high = Math.max(open, close) + rand() * wick;
    const low = Math.min(open, close) - rand() * wick;
    const baseVol = 60 + rand() * 80;
    const volSpike = Math.abs(move) > 2 ? rand() * 60 : 0;
    out.push({ open, close, high, low, volume: baseVol + volSpike });
    price = close;
  }
  return out;
}

export default function MarketPulse() {
  const candles = useMemo(() => generateCandles(), []);

  // Y-scale
  const priceMin = Math.min(...candles.map(c => c.low));
  const priceMax = Math.max(...candles.map(c => c.high));
  const volumeMax = Math.max(...candles.map(c => c.volume));

  const priceY = (v: number) =>
    PRICE_AREA.top + (1 - (v - priceMin) / (priceMax - priceMin)) * PRICE_AREA.height;
  const volumeH = (v: number) => (v / volumeMax) * VOLUME_AREA.height;

  // EMA-10 для MA-линии
  const ma: number[] = [];
  let emaPrev = candles[0].close;
  const k = 2 / (10 + 1);
  for (const c of candles) {
    emaPrev = c.close * k + emaPrev * (1 - k);
    ma.push(emaPrev);
  }

  const maPath = candles
    .map((_, i) => {
      const x = i * CANDLE_W + CANDLE_W / 2;
      const y = priceY(ma[i]);
      return `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');

  // Дублируем candles для seamless loop scroll
  return (
    <div
      className="absolute inset-0 pointer-events-none overflow-hidden"
      aria-hidden="true"
    >
      <svg
        viewBox={`0 0 ${VB_W} ${VB_H}`}
        preserveAspectRatio="xMidYMid slice"
        className="w-full h-full"
      >
        <defs>
          {/* Glow для MA-линии */}
          <filter id="ma-glow" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur stdDeviation="2.5" result="b" />
            <feMerge>
              <feMergeNode in="b" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          {/* Bottom fade — за hero text */}
          <linearGradient id="bottom-fade" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--bg-primary)" stopOpacity="0" />
            <stop offset="100%" stopColor="var(--bg-primary)" stopOpacity="0.85" />
          </linearGradient>
          {/* Top fade */}
          <linearGradient id="top-fade" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--bg-primary)" stopOpacity="0.7" />
            <stop offset="100%" stopColor="var(--bg-primary)" stopOpacity="0" />
          </linearGradient>
        </defs>

        {/* Background grid (horizontal lines) */}
        {[0.25, 0.5, 0.75].map((t, i) => {
          const y = PRICE_AREA.top + PRICE_AREA.height * t;
          return (
            <line
              key={i}
              x1={0}
              y1={y}
              x2={VB_W}
              y2={y}
              stroke="var(--text-muted)"
              strokeWidth="0.5"
              opacity="0.08"
              strokeDasharray="2 4"
            />
          );
        })}

        {/* Прокручивающаяся группа со свечами */}
        <g
          style={{
            animation: `market-scroll 35s linear infinite`,
          }}
        >
          {[0, 1].map(copyIdx => (
            <g key={copyIdx} transform={`translate(${copyIdx * TOTAL_W}, 0)`}>
              {/* Volume-bars */}
              {candles.map((c, i) => {
                const x = i * CANDLE_W;
                const isUp = c.close >= c.open;
                const color = isUp ? C_GREEN : C_RED;
                const h = volumeH(c.volume);
                return (
                  <rect
                    key={i}
                    x={x + 2}
                    y={VOLUME_AREA.top + VOLUME_AREA.height - h}
                    width={CANDLE_W - 4}
                    height={h}
                    fill={color}
                    opacity="0.35"
                    rx="0.5"
                  />
                );
              })}

              {/* Candles */}
              {candles.map((c, i) => {
                const x = i * CANDLE_W;
                const cx = x + CANDLE_W / 2;
                const isUp = c.close >= c.open;
                const color = isUp ? C_GREEN : C_RED;
                const bodyTop = priceY(Math.max(c.open, c.close));
                const bodyBottom = priceY(Math.min(c.open, c.close));
                const bodyHeight = Math.max(1.5, bodyBottom - bodyTop);
                return (
                  <g key={i}>
                    {/* Wick (high-low) */}
                    <line
                      x1={cx}
                      y1={priceY(c.high)}
                      x2={cx}
                      y2={priceY(c.low)}
                      stroke={color}
                      strokeWidth="1"
                      opacity="0.85"
                    />
                    {/* Body */}
                    <rect
                      x={x + 2}
                      y={bodyTop}
                      width={CANDLE_W - 4}
                      height={bodyHeight}
                      fill={color}
                      opacity="0.9"
                      rx="0.5"
                    />
                  </g>
                );
              })}

              {/* MA-линия — поверх свечей */}
              <path
                d={maPath}
                fill="none"
                stroke="var(--accent)"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                opacity="0.9"
                filter="url(#ma-glow)"
              />
            </g>
          ))}
        </g>

        {/* Top + Bottom fades — растворяет анимацию в фоне Hero */}
        <rect x={0} y={0} width={VB_W} height={80} fill="url(#top-fade)" />
        <rect x={0} y={VB_H - 160} width={VB_W} height={160} fill="url(#bottom-fade)" />
      </svg>
    </div>
  );
}
