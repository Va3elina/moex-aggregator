import { useMemo, useState } from 'react';
import { DONUT_COLORS } from '../../config/fundConfig';
import { useGrowReveal } from '../../hooks/useGrowReveal';

interface DonutProps {
    holdings: { name: string; weight: number }[];  // weight в любых единицах — нормируется
    size?: number;          // px, default 180
    outerRadius?: number;   // в координатах viewBox 200, default 70
    innerRadius?: number;   // default 45
    maxSlices?: number;     // default 10; остальное агрегируется в "Прочее"
    showCenterText?: boolean; // default true: число сегментов + "позиций"
    // Реальное число позиций фонда для центра (а не число отрисованных слайсов).
    // Тайл передаёт [топ-10, Прочее] → segmentCount был бы 11; centerCount фиксит это.
    centerCount?: number;
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
    centerCount,
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

    // Entrance-reveal: проявление слайсов при mount/смене состава (НЕ при hover).
    // Ключ зависит ТОЛЬКО от состава → hover/returnPeriod (не меняющие holdings) не
    // перезапускают анимацию.
    const animKey = `${holdings.length}|${holdings[0]?.name ?? ''}|${total.toFixed(1)}`;
    const reveal = useGrowReveal(paths.length, animKey);

    const setH = (i: number | null) => { setHover(i); onHoverChange?.(i); };
    const activeName = active != null && items[active] ? items[active].name : '';
    const activePct = active != null && items[active] && total > 0
        ? (items[active].weight / total) * 100 : 0;
    // Размер центрового текста — в единицах viewBox, но обратно пропорционально size,
    // чтобы РЕНДЕР был постоянным (~px) независимо от размера пончика: на маленьком
    // пончике плитки (144) текст не съёживается.
    const cf = (px: number) => ((px * 200) / size).toFixed(1);

    return (
        <svg viewBox="0 0 200 200" width={size} height={size} style={{ overflow: 'visible' }}>
            <g
                style={{
                    transformBox: 'view-box',
                    transformOrigin: '100px 100px',
                    transform: `scale(${(0.92 + 0.08 * (reveal[reveal.length - 1] ?? 1)).toFixed(3)})`,
                }}
            >
                {paths.map((p, i) => {
                    const on = active === i;
                    const dim = active != null && active !== i;
                    const off = on ? 7 : 0; // «выдвижение» наружу по биссектрисе
                    const p0 = reveal[i] ?? 1; // entrance-progress слайса i
                    return (
                        <path
                            key={i}
                            d={p.d}
                            fill={p.color}
                            stroke="var(--bg-primary)"
                            strokeWidth={on ? 2 : 1.5}
                            transform={off ? `translate(${(p.mx * off).toFixed(2)} ${(p.my * off).toFixed(2)})` : undefined}
                            opacity={(dim ? 0.4 : 1) * p0}
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
            </g>
            {showCenterText && active == null && (
                <>
                    <text x="100" y="95" textAnchor="middle" fill="var(--text-primary)" fontSize={cf(19)} fontWeight="bold">
                        {centerCount ?? segmentCount}
                    </text>
                    <text x="100" y="116" textAnchor="middle" fill="var(--text-muted)" fontSize={cf(11)}>
                        позиций
                    </text>
                </>
            )}
            {showCenterText && active != null && items[active] && (
                <>
                    <text x="100" y="96" textAnchor="middle" fill="var(--text-primary)" fontSize={cf(13)} fontWeight="bold">
                        {activeName.length > 13 ? activeName.slice(0, 12) + '…' : activeName}
                    </text>
                    <text x="100" y="117" textAnchor="middle" fill="var(--text-secondary)" fontSize={cf(15)} fontWeight="bold">
                        {activePct.toFixed(1)}%
                    </text>
                </>
            )}
        </svg>
    );
}
