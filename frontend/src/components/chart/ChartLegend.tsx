/**
 * ChartLegend — SVG-based легенда для графиков.
 *
 * Зачем SVG (vs HTML):
 *   HTML span + flex items-center зависит от font-metrics (Inter cap-vertical-center
 *   ≠ line-box-center) и CSS hacks (transform/padding) которые html2canvas игнорирует
 *   на inline-block. Результат: text выглядит криво и live, и в snapshot.
 *
 *   SVG <text dominantBaseline="middle"> — spec-compliant geometry instruction.
 *   Браузер обязан расположить glyph baseline на y-coord. Все renderer'ы
 *   (Chrome/Firefox/Safari/html2canvas) делают это идентично, пиксель-в-пиксель.
 *
 * Layout:
 *   Каждый item = одна <svg> с <circle> + <text>. Контейнер — HTML flex
 *   (flex layout для items независим от alignment самого dot/text).
 *
 * Text width:
 *   Измеряется через canvas.measureText() при первом mount + при изменении
 *   font-size/items. Без двойного render — sync API.
 */

import { useMemo } from 'react';
import { measureText } from './measureText';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { legendFontSize, legendDotSize } from './chartTypography';

export interface ChartLegendItem {
    /** Цвет dot circle (CSS color: hex / rgb / var(--xxx)) */
    color: string;
    /** Текст label */
    label: string;
    /** Opacity всего item (default 1.0) */
    opacity?: number;
    /** Текст color override (default — currentColor наследуется от parent) */
    textColor?: string;
}

interface Props {
    items: ChartLegendItem[];
    /** Font size в px. Если не задан — берётся discrete responsive default
     *  через chartTypography (320=11, 375=12, 425=13, 768=15, 1024=17, 1440=19). */
    fontSize?: number;
    /** Font weight (default 600 — match существующему font-semibold) */
    fontWeight?: number;
    /** Diameter dot circle в px. Если не задан — discrete responsive
     *  (320=9, 375=10, 425=11, 768=13, 1024=14, 1440=16). */
    dotSize?: number;
    /** Gap между dot и text внутри одного item (default 6) */
    itemGap?: number;
    /** Gap между items в контейнере (default 16; CSS clamp string OK) */
    gap?: number | string;
    /** Justify-content контейнера (default 'center') */
    justify?: 'start' | 'center' | 'end';
    /** Дополнительные style для контейнера */
    style?: React.CSSProperties;
}

export default function ChartLegend({
    items,
    fontSize,
    fontWeight = 600,
    dotSize,
    itemGap = 6,
    gap = 16,
    justify = 'center',
    style,
}: Props) {
    // Discrete responsive defaults через chartTypography.ts. Если caller
    // пробросил fontSize/dotSize — используются они, иначе breakpoint scale.
    const vw = useViewportWidth();
    const effectiveFs = fontSize ?? legendFontSize(vw);
    const effectiveDot = dotSize ?? legendDotSize(vw);

    // Pre-compute SVG dimensions для каждого item один раз на render.
    // useMemo инвалидируется при изменении items/font — стабильная ссылка
    // между ре-рендерами parent.
    const computed = useMemo(() => {
        const totalH = Math.max(effectiveDot, effectiveFs);
        return items.map((it) => {
            const textW = measureText(it.label, effectiveFs, fontWeight);
            // +1px sub-pixel safety — на retina измерение может округляться вниз,
            // glyph рендерится с tiny extra width → clipping. +1 убирает риск.
            const totalW = effectiveDot + itemGap + Math.ceil(textW) + 1;
            return { ...it, totalW, totalH };
        });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [items.map((i) => `${i.color}|${i.label}|${i.opacity ?? 1}|${i.textColor ?? ''}`).join('||'), effectiveFs, fontWeight, effectiveDot, itemGap]);

    const containerStyle: React.CSSProperties = {
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        justifyContent:
            justify === 'start' ? 'flex-start' : justify === 'end' ? 'flex-end' : 'center',
        gap: typeof gap === 'number' ? `${gap}px` : gap,
        ...style,
    };

    return (
        <div style={containerStyle}>
            {computed.map((it, idx) => (
                <svg
                    key={idx}
                    width={it.totalW}
                    height={it.totalH}
                    viewBox={`0 0 ${it.totalW} ${it.totalH}`}
                    overflow="visible"
                    style={{ display: 'block', flexShrink: 0, opacity: it.opacity ?? 1, overflow: 'visible' }}
                    aria-label={it.label}
                >
                    {/* Dot — center y = totalH/2 (geometric center of SVG height) */}
                    <circle
                        cx={effectiveDot / 2}
                        cy={it.totalH / 2}
                        r={effectiveDot / 2}
                        fill={it.color}
                    />
                    {/* Text — dominantBaseline="central" даёт ТОЧНЫЙ pixel center
                        (em-box middle на y), в отличие от "middle" которое полагается
                        на font's middle baseline metric → сдвиг на 1-2px у Inter
                        (asymmetric ascender/descender). "central" = geometric center,
                        spec-compliant, identical в browser и html2canvas.
                        overflow="visible" на parent svg — descender Cyrillic ("р", "у", "ц")
                        не клипается на retina sub-pixel rendering. */}
                    <text
                        x={effectiveDot + itemGap}
                        y={it.totalH / 2}
                        dominantBaseline="central"
                        fill={it.textColor || 'currentColor'}
                        fontSize={effectiveFs}
                        fontWeight={fontWeight}
                        // Inter — primary; fallback chain совпадает с body styles
                        style={{ fontFamily: 'inherit' }}
                    >
                        {it.label}
                    </text>
                </svg>
            ))}
        </div>
    );
}
