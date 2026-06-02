import { useMemo } from 'react';
import { DONUT_COLORS } from '../../config/fundConfig';

interface DonutProps {
    holdings: { name: string; weight: number }[];  // weight 0..1
    size?: number;          // px, default 180
    outerRadius?: number;   // в координатах viewBox 200, default 70
    innerRadius?: number;   // default 45
    maxSlices?: number;     // default 10; остальное агрегируется в "Прочее"
    showCenterText?: boolean; // default true: число сегментов + "позиций"
    colors?: string[];      // default DONUT_COLORS из fundConfig
    // Опц. клик по сектору. index — позиция отрисованного слайса (совпадает с
    // массивом holdings, если maxSlices >= holdings.length, т.е. Donut не
    // агрегирует «Прочее» сам). Если включён — у path появляется cursor:pointer.
    onSliceClick?: (index: number) => void;
}

/**
 * Переиспользуемый SVG donut состава фонда. Извлечён из FundCardModal.
 * Легенду НЕ рисует — её рисует вызывающий снаружи (цвета берутся из того же
 * массива `colors` по индексу, поэтому совпадут). Вся slice-геометрия считается
 * в одном useMemo. Цвета центрального текста — CSS-vars (theme-aware).
 *
 * «Прочее» = сумма весов за пределами maxSlices, добавляется только если > 0.
 * Если sum(top) < 1 — искусственное «Прочее» НЕ добавляем (дырка допустима).
 */
export default function Donut({
    holdings,
    size = 180,
    outerRadius = 70,
    innerRadius = 45,
    maxSlices = 10,
    showCenterText = true,
    colors = DONUT_COLORS,
    onSliceClick,
}: DonutProps) {
    const { paths, segmentCount } = useMemo(() => {
        const top = holdings.slice(0, maxSlices);
        const otherWeight = holdings
            .slice(maxSlices)
            .reduce((s, h) => s + h.weight, 0);
        const items = otherWeight > 0 ? [...top, { name: 'Прочее', weight: otherWeight }] : top;
        const total = items.reduce((s, h) => s + h.weight, 0);

        const cx = 100, cy = 100, r = outerRadius, ir = innerRadius;
        let cumAngle = -90;

        const built = items.map((h, i) => {
            const angle = total > 0 ? (h.weight / total) * 360 : 0;
            const startRad = (cumAngle * Math.PI) / 180;
            const endRad = ((cumAngle + angle) * Math.PI) / 180;
            cumAngle += angle;
            const x1 = cx + r * Math.cos(startRad), y1 = cy + r * Math.sin(startRad);
            const x2 = cx + r * Math.cos(endRad), y2 = cy + r * Math.sin(endRad);
            const ix1 = cx + ir * Math.cos(endRad), iy1 = cy + ir * Math.sin(endRad);
            const ix2 = cx + ir * Math.cos(startRad), iy2 = cy + ir * Math.sin(startRad);
            const largeArc = angle > 180 ? 1 : 0;
            const d = `M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2} L ${ix1} ${iy1} A ${ir} ${ir} 0 ${largeArc} 0 ${ix2} ${iy2} Z`;
            return { d, color: colors[i % colors.length] };
        });

        return { paths: built, segmentCount: holdings.length };
    }, [holdings, outerRadius, innerRadius, maxSlices, colors]);

    return (
        <svg viewBox="0 0 200 200" width={size} height={size}>
            {paths.map((p, i) => (
                <path
                    key={i}
                    d={p.d}
                    fill={p.color}
                    stroke="var(--bg-primary)"
                    strokeWidth="1.5"
                    onClick={onSliceClick ? () => onSliceClick(i) : undefined}
                    style={onSliceClick ? { cursor: 'pointer' } : undefined}
                />
            ))}
            {showCenterText && (
                <>
                    <text x="100" y="96" textAnchor="middle" fill="var(--text-primary)" fontSize="14" fontWeight="bold">
                        {segmentCount}
                    </text>
                    <text x="100" y="112" textAnchor="middle" fill="var(--text-muted)" fontSize="10">
                        позиций
                    </text>
                </>
            )}
        </svg>
    );
}
