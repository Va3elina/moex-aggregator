import { useMemo, useState } from 'react';
import { DONUT_COLORS } from '../../config/fundConfig';

interface DonutProps {
    holdings: { name: string; weight: number }[];  // weight в любых единицах — нормируется
    size?: number;          // px, default 180
    outerRadius?: number;   // в координатах viewBox 200, default 70
    innerRadius?: number;   // default 45
    maxSlices?: number;     // default 10; остальное агрегируется в "Прочее"
    showCenterText?: boolean; // default true: число сегментов + "позиций"
    colors?: string[];      // default DONUT_COLORS из fundConfig
    // Опц. клик по сектору. index — позиция отрисованного слайса (= holdings, если
    // maxSlices >= holdings.length). Включает cursor:pointer.
    onSliceClick?: (index: number) => void;
    // Hover-интерактив: сектор «выдвигается» наружу + остальные тускнеют, центр
    // показывает наведённый актив. onHoverChange сообщает индекс наружу (для
    // подсветки строки списка). highlightIndex — внешняя подсветка (из списка → пончик).
    highlightIndex?: number | null;
    onHoverChange?: (index: number | null) => void;
}

/**
 * Переиспользуемый SVG donut состава фонда с hover-explode. Легенду НЕ рисует —
 * её рисует вызывающий (цвета из того же `colors` по индексу → совпадут).
 * «Прочее» = сумма весов за пределами maxSlices (если > 0). Проценты в центре
 * считаются от total (нормировано) — единица входного weight не важна.
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
    highlightIndex = null,
    onHoverChange,
}: DonutProps) {
    const [hover, setHover] = useState<number | null>(null);
    const active = highlightIndex != null ? highlightIndex : hover;
    const interactive = !!onHoverChange || !!onSliceClick;

    const { paths, items, total, segmentCount } = useMemo(() => {
        const top = holdings.slice(0, maxSlices);
        const otherWeight = holdings.slice(maxSlices).reduce((s, h) => s + h.weight, 0);
        const items = otherWeight > 0 ? [...top, { name: 'Прочее', weight: otherWeight }] : top;
        const total = items.reduce((s, h) => s + h.weight, 0);

        const cx = 100, cy = 100, r = outerRadius, ir = innerRadius;
        let cumAngle = -90;

        const built = items.map((h, i) => {
            const angle = total > 0 ? (h.weight / total) * 360 : 0;
            const startRad = (cumAngle * Math.PI) / 180;
            const midRad = ((cumAngle + angle / 2) * Math.PI) / 180;
            const endRad = ((cumAngle + angle) * Math.PI) / 180;
            cumAngle += angle;
            const x1 = cx + r * Math.cos(startRad), y1 = cy + r * Math.sin(startRad);
            const x2 = cx + r * Math.cos(endRad), y2 = cy + r * Math.sin(endRad);
            const ix1 = cx + ir * Math.cos(endRad), iy1 = cy + ir * Math.sin(endRad);
            const ix2 = cx + ir * Math.cos(startRad), iy2 = cy + ir * Math.sin(startRad);
            const largeArc = angle > 180 ? 1 : 0;
            const d = `M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2} L ${ix1} ${iy1} A ${ir} ${ir} 0 ${largeArc} 0 ${ix2} ${iy2} Z`;
            return { d, color: colors[i % colors.length], mx: Math.cos(midRad), my: Math.sin(midRad) };
        });

        return { paths: built, items, total, segmentCount: holdings.length };
    }, [holdings, outerRadius, innerRadius, maxSlices, colors]);

    const setH = (i: number | null) => { setHover(i); onHoverChange?.(i); };
    const activeName = active != null && items[active] ? items[active].name : '';
    const activePct = active != null && items[active] && total > 0
        ? (items[active].weight / total) * 100 : 0;

    return (
        <svg viewBox="0 0 200 200" width={size} height={size} style={{ overflow: 'visible' }}>
            {paths.map((p, i) => {
                const on = active === i;
                const dim = active != null && active !== i;
                const off = on ? 7 : 0; // «выдвижение» наружу по биссектрисе
                return (
                    <path
                        key={i}
                        d={p.d}
                        fill={p.color}
                        stroke="var(--bg-primary)"
                        strokeWidth={on ? 2 : 1.5}
                        transform={off ? `translate(${(p.mx * off).toFixed(2)} ${(p.my * off).toFixed(2)})` : undefined}
                        opacity={dim ? 0.4 : 1}
                        onMouseEnter={interactive ? () => setH(i) : undefined}
                        onMouseLeave={interactive ? () => setH(null) : undefined}
                        onClick={onSliceClick ? () => onSliceClick(i) : undefined}
                        style={{
                            cursor: onSliceClick ? 'pointer' : 'default',
                            transition: 'transform 140ms ease, opacity 140ms ease, stroke-width 140ms ease',
                        }}
                    />
                );
            })}
            {showCenterText && active == null && (
                <>
                    <text x="100" y="96" textAnchor="middle" fill="var(--text-primary)" fontSize="14" fontWeight="bold">
                        {segmentCount}
                    </text>
                    <text x="100" y="112" textAnchor="middle" fill="var(--text-muted)" fontSize="10">
                        позиций
                    </text>
                </>
            )}
            {showCenterText && active != null && items[active] && (
                <>
                    <text x="100" y="97" textAnchor="middle" fill="var(--text-primary)" fontSize="11" fontWeight="bold">
                        {activeName.length > 13 ? activeName.slice(0, 12) + '…' : activeName}
                    </text>
                    <text x="100" y="114" textAnchor="middle" fill="var(--text-secondary)" fontSize="12" fontWeight="bold">
                        {activePct.toFixed(1)}%
                    </text>
                </>
            )}
        </svg>
    );
}
