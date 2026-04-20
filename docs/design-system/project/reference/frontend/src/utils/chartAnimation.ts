/**
 * Общие утилиты анимации/рендера для SVG-графиков.
 *
 * Используются в SimpleChart, IndexChart, BreadthChart и др.
 * Раньше дублировались в strength/chartUtils.ts и SimpleChart.tsx.
 */

// ─── Типы ───────────────────────────────────────────

export type ChartPadding = { left: number; right: number; top: number; bottom: number };

// ─── Интерполяция ───────────────────────────────────

export const lerp = (a: number, b: number, t: number) => a + (b - a) * t;

export const easeOutCubic = (t: number) => 1 - Math.pow(1 - t, 3);

// ─── Ресемплинг и морфинг ───────────────────────────

export const resamplePts = (pts: { x: number; y: number }[], len: number) => {
    if (pts.length === 0) return [];
    if (pts.length === len) return pts;
    const out: { x: number; y: number }[] = [];
    for (let i = 0; i < len; i++) {
        const t = i / (len - 1);
        const si = t * (pts.length - 1);
        const lo = Math.floor(si);
        const hi = Math.min(lo + 1, pts.length - 1);
        const lt = si - lo;
        out.push({ x: lerp(pts[lo].x, pts[hi].x, lt), y: lerp(pts[lo].y, pts[hi].y, lt) });
    }
    return out;
};

export const morphPts = (from: { x: number; y: number }[], to: { x: number; y: number }[], t: number) => {
    const n = Math.max(from.length, to.length);
    const a = resamplePts(from, n);
    const b = resamplePts(to, n);
    return a.map((p, i) => ({ x: lerp(p.x, b[i].x, t), y: lerp(p.y, b[i].y, t) }));
};

// ─── SVG path генерация ─────────────────────────────

export const ptsToPath = (pts: { x: number; y: number }[]) =>
    pts.length ? pts.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ') : '';

export const ptsToArea = (pts: { x: number; y: number }[], bottom: number) => {
    if (!pts.length) return '';
    return ptsToPath(pts) + ` L ${pts[pts.length - 1].x} ${bottom} L ${pts[0].x} ${bottom} Z`;
};
