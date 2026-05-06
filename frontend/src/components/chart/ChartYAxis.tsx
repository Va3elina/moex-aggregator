/**
 * Подписи значений Y-оси.
 * Позиционируется от КРАЯ ГРАФИКА (не от края контейнера),
 * чтобы расстояние между цифрами и графиком было одинаковым
 * независимо от длины цифр (68 vs 269 778).
 *
 * Референс: SimpleChart (ОИ) — x={chartWidth + 12}, textAnchor="start"
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
  /** Верхний padding контейнера в px (default 10) */
  padTop?: number;
  /** Нижний padding контейнера в px (default 50) */
  padBottom?: number;
  /** Левый padding (граница графика слева) */
  padLeft?: number;
  /** Правый padding (граница графика справа) */
  padRight?: number;
}

export default function ChartYAxis({
  ticks, side = 'right', format, color,
  padTop = 10, padBottom = 50, padLeft = 60, padRight = 80,
}: ChartYAxisProps) {
  const fmt = format ?? ((v: number) => v.toFixed(0));
  const clr = color ?? 'var(--axis-color, #9CA3B8)';

  return (
    <>
      {ticks.map((t, i) => {
        const chartAreaH = `calc(100% - ${padTop + padBottom}px)`;
        // Располагаем метку в padding-зоне ВНЕ графика:
        // - Right Y-axis: левый край метки на 4px правее правого края графика,
        //   ширина = padRight - 4 (заполняет padding-зону), text-align: left.
        // - Left Y-axis: симметрично — правый край на 4px левее графика.
        // Раньше gap=12px → на mobile (padRight=32) колонка = 20px, метка
        // "+25%" (~28px на 9-11px font) обрезалась за правый край card.
        // С gap=4px колонка = 28-91px (mobile-desktop) — метки влезают.
        const posStyle = side === 'right'
          ? { right: 0, width: `${padRight - 4}px`, textAlign: 'left' as const }
          : { left: 0, width: `${padLeft - 4}px`, textAlign: 'right' as const };

        return (
          <div
            key={i}
            className="absolute pointer-events-none"
            style={{
              ...posStyle,
              top: `calc(${padTop}px + ${t.pct / 100} * ${chartAreaH})`,
              transform: 'translateY(-50%)',
            }}
          >
            <span
              className="font-bold"
              style={{ fontSize: 'var(--chart-font-y, 16px)', color: clr }}
            >
              {fmt(t.value)}
            </span>
          </div>
        );
      })}
    </>
  );
}
