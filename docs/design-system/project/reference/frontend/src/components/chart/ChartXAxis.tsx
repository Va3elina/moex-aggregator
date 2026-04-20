/**
 * Подписи X-оси (даты/метки) под графиком.
 * Позиционируется absolute внизу chart-контейнера.
 */

interface ChartXAxisProps {
  /** Метки для отображения */
  labels: string[];
  /** Левый padding (px) */
  padLeft?: number;
  /** Правый padding (px) */
  padRight?: number;
  /** Цвет текста */
  color?: string;
}

export default function ChartXAxis({ labels, padLeft = 60, padRight = 80, color }: ChartXAxisProps) {
  return (
    <div
      className="absolute flex justify-between font-semibold"
      style={{
        left: padLeft,
        right: padRight,
        bottom: 4,
        fontSize: 'var(--chart-font-x, 13px)',
        color: color ?? 'var(--axis-color, #9CA3B8)',
      }}
    >
      {labels.map((label, i) => (
        <span key={i}>{label}</span>
      ))}
    </div>
  );
}
