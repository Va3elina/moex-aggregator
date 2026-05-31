/**
 * FundDonut — интерактивный donut состава фонда (SVG, hover-подсветка + tooltip
 * в центре). Перенесён из бывшей страницы FundsCatalogPage при слиянии раздела
 * «Состав фондов» в /fund-trades.
 *
 * weight — ДОЛЯ (0..1). Если на входе проценты (0..100) — делите на 100.
 */
import { useState } from 'react';
import { DONUT_COLORS } from '../config/fundConfig';

export default function FundDonut({
    holdings,
    size = 160,
}: {
    holdings: { name: string; weight: number }[];
    size?: number;
}) {
    const [hovered, setHovered] = useState<number | null>(null);
    if (!holdings.length) return null;

    const filtered = holdings.filter((h) => h.weight > 0 && !h.name.includes('акция прив'));
    if (!filtered.length) return null;

    const r = size / 2, outerR = r - 2, innerR = r * 0.5;
    let cumAngle = -90;
    const total = filtered.reduce((s, h) => s + h.weight, 0);

    const segments = filtered.map((h, i) => {
        const angle = (h.weight / total) * 360;
        const startRad = (cumAngle * Math.PI) / 180;
        const endRad = ((cumAngle + angle) * Math.PI) / 180;
        cumAngle += angle;
        const oR = hovered === i ? outerR + 4 : outerR;
        const iR = hovered === i ? innerR - 2 : innerR;
        const x1 = r + oR * Math.cos(startRad), y1 = r + oR * Math.sin(startRad);
        const x2 = r + oR * Math.cos(endRad), y2 = r + oR * Math.sin(endRad);
        const ix1 = r + iR * Math.cos(endRad), iy1 = r + iR * Math.sin(endRad);
        const ix2 = r + iR * Math.cos(startRad), iy2 = r + iR * Math.sin(startRad);
        const large = angle > 180 ? 1 : 0;
        const d = `M ${x1} ${y1} A ${oR} ${oR} 0 ${large} 1 ${x2} ${y2} L ${ix1} ${iy1} A ${iR} ${iR} 0 ${large} 0 ${ix2} ${iy2} Z`;
        return { d, color: DONUT_COLORS[i % DONUT_COLORS.length], name: h.name, weight: h.weight, index: i };
    });

    return (
        <div className="relative" style={{ width: size + 8, height: size + 8 }}>
            <svg viewBox={`-4 -4 ${size + 8} ${size + 8}`} width={size + 8} height={size + 8}>
                {segments.map((seg) => (
                    <path
                        key={seg.index}
                        d={seg.d}
                        fill={seg.color}
                        stroke="var(--bg-secondary)"
                        strokeWidth="1.5"
                        opacity={hovered === null || hovered === seg.index ? 1 : 0.4}
                        onMouseEnter={() => setHovered(seg.index)}
                        onMouseLeave={() => setHovered(null)}
                        className="transition-opacity duration-150 cursor-pointer"
                    />
                ))}
            </svg>
            {/* Hover tooltip в центре donut */}
            {hovered !== null && (
                <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
                    <div className="text-center px-2">
                        <div
                            className="text-xs font-semibold truncate max-w-[110px]"
                            style={{ color: 'var(--text-primary)' }}
                        >
                            {segments[hovered].name.replace(', акция об.', '')}
                        </div>
                        <div className="text-sm font-mono font-bold" style={{ color: segments[hovered].color }}>
                            {(segments[hovered].weight * 100).toFixed(1)}%
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
