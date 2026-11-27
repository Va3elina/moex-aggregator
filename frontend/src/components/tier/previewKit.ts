/**
 * previewKit — общие кирпичи анимированных превью окна апселла: палитры (ровно
 * токены Терминала из pages/sandbox/sandbox.css), выбор светлой/тёмной темы по
 * теме сайта, генератор «рыночных» рядов и построители SVG-путей.
 * Компоненты лежат в previewParts.tsx / PreviewScene.tsx / TerminalPreview.tsx.
 */
import { useContext } from 'react';
import { ThemeContext } from '../../contexts/ThemeContext';

export type PreviewMode = 'light' | 'dark';

/** theme — принудительно (демка); без него берётся тема сайта из ThemeContext. */
export interface PreviewVisualProps { theme?: PreviewMode }

export interface TermPalette {
    body: string; panel: string; chip: string; text: string; muted: string;
    border: string; faint: string; dot: string; bar: string;
}

export const PALETTES: Record<PreviewMode, TermPalette> = {
    dark: {
        body: '#0E0E10', panel: '#201E25', chip: '#2A2830', text: '#F5F1E8', muted: '#9A958C',
        border: 'rgba(245,241,232,0.18)', faint: 'rgba(245,241,232,0.10)', dot: 'rgba(245,241,232,0.28)', bar: '#4A4652',
    },
    light: {
        body: '#F4F1EA', panel: '#FFFFFF', chip: '#E6E1D5', text: '#0A0A0A', muted: '#6B6760',
        border: 'rgba(10,10,10,0.16)', faint: 'rgba(10,10,10,0.08)', dot: 'rgba(10,10,10,0.25)', bar: '#D6D0C3',
    },
};

export const ACCENT = 'var(--accent, #FF5C2B)';
export const UP = '#5BD49C';
export const DOWN = '#EF6F6F';
/** Вторая серия (как --c-price Терминала). */
export const SERIES2 = '#5DA3E9';

/**
 * Тема сайта → тема превью, ровно как SandboxPage.siteTheme(): editorial-dark →
 * тёмный, всё остальное → светлый. Контекст читаем мягко (без useTheme, который
 * кидает вне провайдера) — превью рендерится и в демке.
 */
export function usePreviewPalette(theme?: PreviewMode): TermPalette {
    const site = useContext(ThemeContext)?.theme;
    return PALETTES[theme ?? (site === 'editorial-dark' ? 'dark' : 'light')];
}

/** Детерминированный «рыночный» ряд: тренд + волны + шум, значения 0..1. */
export function series(n: number, seed: number, noise = 0.06): number[] {
    let s = seed * 9301 + 49297;
    const rnd = () => { s = (s * 9301 + 49297) % 233280; return s / 233280; };
    const out: number[] = [];
    for (let i = 0; i < n; i++) {
        const x = i / Math.max(1, n - 1);
        out.push(0.25 + 0.45 * x + 0.12 * Math.sin(x * 9 + seed) + 0.07 * Math.sin(x * 23 + seed * 2) + (rnd() - 0.5) * noise);
    }
    const lo = Math.min(...out), hi = Math.max(...out);
    return out.map((v) => (v - lo) / (hi - lo || 1));
}

/** Ломаная по значениям 0..1 в прямоугольнике (x, y, w, h). */
export function linePath(vals: number[], x: number, y: number, w: number, h: number): string {
    return vals.map((v, i) => {
        const px = x + (w * i) / Math.max(1, vals.length - 1);
        const py = y + h - v * h;
        return `${i ? 'L' : 'M'}${px.toFixed(1)} ${py.toFixed(1)}`;
    }).join(' ');
}

/** Дуга шкалы: от угла a0 до a1 (градусы, 180 = слева). */
export function arc(cx: number, cy: number, r: number, a0: number, a1: number): string {
    const pt = (a: number) => [cx + r * Math.cos((a * Math.PI) / 180), cy - r * Math.sin((a * Math.PI) / 180)];
    const [x0, y0] = pt(a0);
    const [x1, y1] = pt(a1);
    return `M${x0.toFixed(1)} ${y0.toFixed(1)} A${r} ${r} 0 0 1 ${x1.toFixed(1)} ${y1.toFixed(1)}`;
}

/* ── Описание сцены (строит resolvePreview в featurePreviews.ts) ───────── */

export type SceneKind =
    | 'terminal' | 'chart' | 'screener' | 'alerts' | 'csv' | 'extension' | 'fundpick' | 'fundtrades';
/**
 * Как выглядит индикатор в сцене 'chart' и что делает клик по закрытой функции:
 *   line — к линии добавляется вторая серия (юрлица, число трейдеров);
 *   bars — те же столбики пересчитываются (фильтры сезонности).
 */
export type ChartArch = 'line' | 'bars';

export interface SceneSpec {
    kind: SceneKind;
    /** Заголовок окна внутри сцены («Открытые позиции · Si»). */
    header?: string;
    arch?: ChartArch;
    /** 'fundtrades': какой замок показываем (без него — просто экран вкладки). */
    variant?: 'fresh' | 'picker' | 'range';
    /** Чипы над графиком; последний — та самая закрытая функция, по нему кликает курсор. */
    chips?: string[];
    seed?: number;
}

/** Ширина «чипа» под подпись (Inter 7.5px). */
export const chipWidth = (label: string) => Math.round(14 + label.length * 4.6);
