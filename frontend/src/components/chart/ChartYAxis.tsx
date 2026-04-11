/**
 * Подписи значений Y-оси (справа или слева от графика).
 * Позиционируется absolute внутри chart-контейнера.
 */

interface YTick {
  value: number;
  /** Позиция в % от высоты viewBox (0 = верх, 100 = низ) */
  pct: number;
}

interface ChartYAxisProps {
  ticks: YTick[];
  /** Сторона: 'left' или 'right' */
  side?: 'left' | 'right';
  /** Функция форматирования значения */
  format?: (value: number) => string;
  /** Цвет текста */
  color?: string;
  /** Верхний padding в px (default 10) */
  padTop?: number;
  /** Нижний padding в px (default 50) */
  padBottom?: number;
}

export default function ChartYAxis({
  ticks, side = 'right', format, color, padTop = 10, padBottom = 50,
}: ChartYAxisProps) {
  const fmt = format ?? ((v: number) => v.toFixed(0));
  const clr = color ?? 'var(--axis-color, #9CA3B8)';

  return (
    <>
      {ticks.map((t, i) => {
        // Вычисляем top в px: padTop + pct% от области графика
        const chartAreaH = `calc(100% - ${padTop + padBottom}px)`;
        return (
          <div
            key={i}
            className="absolute pointer-events-none"
            style={{
              [side]: 4,
              top: `calc(${padTop}px + ${t.pct / 100} * ${chartAreaH})`,
              transform: 'translateY(-50%)',
            }}
          >
            <span
              className="font-semibold"
              style={{ fontSize: 'var(--chart-font-y, 14px)', color: clr }}
            >
              {fmt(t.value)}
            </span>
          </div>
        );
      })}
    </>
  );
}
