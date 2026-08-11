/**
 * ChartAxisPill — значение на правой Y-оси «таблеткой» в цвете серии
 * (TradingView-style). Тот же элемент, что в «Сезонности» (renderAxisPill в
 * YearlySeasonalityChart), ОИ (lastValueVisible у lightweight-charts) и
 * SimpleChart: без курсора — последнее значение серии, под курсором —
 * значение наведённой точки.
 *
 * Рендер — SVG (rect + text с dominantBaseline="central"), а не div с
 * padding'ом и transform'ом: html2canvas игнорирует CSS-transform и роняет
 * вертикальную центровку flex/line-height текста, из-за чего в PNG-экспорте
 * таблетка уезжала бы от своей грид-линии.
 *
 * Позиция по вертикали — доля высоты области графика (0..1), считается тем же
 * масштабом, что и подписи тиков. По горизонтали таблетка встаёт так, чтобы её
 * ТЕКСТ стоял в колонке тиков (textLeft), а заливка выходила левее на padX.
 */
import { cssVar } from '../../config/chartTheme';
import { measureText } from './measureText';

interface ChartAxisPillProps {
    /** Готовая строка — форматируется тем же форматтером, что подписи оси. */
    value: string;
    /** Цвет серии (заливка таблетки). */
    color: string;
    /** Позиция центра по вертикали — доля высоты области графика (0..1). */
    topFrac: number;
    /** Левый край текста внутри правого жёлоба, px (колонка тиков). */
    textLeft?: number;
    /** Доп. классы — hover-таблетке передаём chart-hover-ui (снимается в PNG). */
    className?: string;
}

export default function ChartAxisPill({
    value,
    color,
    topFrac,
    textLeft = 12,
    className = '',
}: ChartAxisPillProps) {
    const fs = cssVar('--chart-font-y', 16);
    const padX = 6;
    const padY = 2;
    const h = fs + padY * 2;
    const w = Math.ceil(measureText(value, fs, 700)) + padX * 2;

    return (
        <div
            className={`absolute pointer-events-none ${className}`}
            style={{
                left: textLeft - padX,
                // calc вместо translateY(-50%): transform не переживает экспорт.
                top: `calc(${(topFrac * 100).toFixed(3)}% - ${h / 2}px)`,
                width: w,
                height: h,
                zIndex: 2,
            }}
        >
            <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} style={{ overflow: 'visible' }}>
                <rect x={0} y={0} width={w} height={h} rx={4} ry={4} fill={color} />
                <text
                    x={padX}
                    y={h / 2}
                    textAnchor="start"
                    dominantBaseline="central"
                    fontSize={fs}
                    fontWeight={700}
                    fill="#fff"
                    style={{ fontVariantNumeric: 'tabular-nums', fontFamily: 'inherit' }}
                >
                    {value}
                </text>
            </svg>
        </div>
    );
}
