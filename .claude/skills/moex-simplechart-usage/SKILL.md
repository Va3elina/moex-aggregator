---
name: moex-simplechart-usage
description: Use or modify the SimpleChart component in Фрейм. Use when user asks to "добавь график", "поменяй график", "добавь вторую ось", "сделай гистограмму", "add chart", or when working with any chart component in pages/*.tsx. Also use when data formatting, horizontal lines, smoothing, or dual-axis displays are needed.
---

# SimpleChart Component Usage

The main chart component for all indicators. Located at `frontend/src/components/SimpleChart.tsx` (~800 lines). This skill captures correct prop usage patterns from the 8 indicators that already use it.

## ⚠️ Critical Rules

1. **Container needs `minHeight: height + 50`** — otherwise chart clips with legend
2. **Wrap data transformation in `useMemo`** — re-rendering on every state change otherwise
3. **Use `chartTheme.ts` CHART_COLORS** — все theme-aware (`var(--accent)` etc), не хардкод hex
4. **Dual-axis uses `secondaryColor` prop** — not `secondary` (common typo)
5. **Height — через `useFitToViewport(chartAnchorRef, {min, max, bottomBuffer})`** — не хардкод `height={450}`. См. design_system.md
6. **Default colors уже theme-aware** — `primaryColor='var(--accent)'`, `secondaryColor='var(--funds-flow-positive)'`, `thirdColor='var(--funds-flow-negative)'`. Можно не передавать.

## Minimum Working Chart (editorial-style)

```tsx
import { useFitToViewport } from '../hooks/useFitToViewport';

const chartAnchorRef = useRef<HTMLDivElement>(null);
const chartHeight = useFitToViewport(chartAnchorRef, { min: 360, max: 720, bottomBuffer: 96 });

return (
  <div className="editorial-frame">
    <div ref={chartAnchorRef}>
      <SimpleChart
        data={data}
        height={chartHeight}
        formatValue={(v) => `${v.toFixed(2)}%`}
        primaryLabel="My metric"
        loading={loading}
        showNavigator={true}
      />
    </div>
  </div>
);
```

## Complete Props Reference

### Data props (required)
- `data: DataPoint[]` — primary line (or bars). `DataPoint = { time: string, value: number }`
- `secondaryData?: DataPoint[]` — optional second line on right Y-axis
- `thirdData?: DataPoint[]` — third line (rarely needed)

### Display
- `height?: number` — обязательно `chartHeight` от `useFitToViewport`. **НЕ** хардкод 450.
- `primaryColor?: string` — default **`var(--accent)`** (theme-aware pumpkin). Можно не передавать.
- `secondaryColor?: string` — default **`var(--funds-flow-positive)`** (forest green).
- `thirdColor?: string` — default **`var(--funds-flow-negative)`** (clay red).
  Цвета theme-aware → автоматически адаптируются на light/dark, не нужны hex'ы.

### Toggles
- `showSecondary?: boolean` — enable right Y-axis
- `showThird?: boolean`
- `showNavigator?: boolean` — range slider at bottom (for long timeseries)
- `showDownloadButton?: boolean` — CSV export button (default true, usually false)
- `showValueHeader?: boolean` — top-right value box (usually false)
- `hideTime?: boolean` — hide X-axis date labels (default false)

### Formatters
- `formatValue?: (v: number) => string` — primary values (tooltip + axis)
- `formatSecondaryValue?: (v: number) => string` — secondary values in **tooltip**
- `formatSecondaryAxis?: (v: number) => string` — secondary values on the **right Y-axis** (falls back to `formatSecondaryValue` if omitted). Use when axis numbers don't fit — e.g. Buffett shows `68.33 трлн ₽` in tooltip but `68.33` on axis.
- `formatThirdValue?: (v: number) => string`
- `formatTime?: (t: string) => string` — X-axis dates

### Labels
- `primaryLabel?: string` — legend label
- `secondaryLabel?: string`
- `thirdLabel?: string`
- `legendPosition?: 'top' | 'bottom'` — default `bottom`, usually `top`

### Advanced
- `allowHistogram?: boolean` — show line/histogram toggle button
- `defaultHistogram?: boolean` — start in histogram mode (use for discrete quarterly/yearly bars)
- `histogramDisabled?: boolean`
- `horizontalLines?: {value, color, label?, axis?}[]` — threshold lines
- `annotations?: ChartAnnotation[]` — event markers
- `chartPadding?: { left?, right? }` — custom margins
- `loading?: boolean` — show skeleton

## Common Patterns by Indicator Type

### Line chart with dual Y-axis (Buffett, Strength)

```tsx
<SimpleChart
    data={ratioData}                          // left Y: the ratio
    secondaryData={capData}                   // right Y: absolute value
    height={450}
    primaryColor="#C8FF2E"
    secondaryColor="#f59e0b"
    showSecondary={true}
    formatValue={(v) => `${v.toFixed(2)}%`}
    formatSecondaryValue={(v) => `${v.toFixed(2)} трлн ₽`}
    primaryLabel="Капитализация / ВВП"
    secondaryLabel="Капитализация (трлн ₽)"
    legendPosition="top"
    showDownloadButton={false}
    showNavigator={true}
    hideTime={true}
/>
```

### Histogram (bars) for discrete quarterly/yearly data

```tsx
<SimpleChart
    data={yearlyData}
    height={400}
    primaryColor="#f59e0b"
    formatValue={(v) => `${v.toFixed(1)}%`}
    primaryLabel="Годовое изменение"
    allowHistogram={true}
    defaultHistogram={true}  // start as bars, not line
    horizontalLines={[{ value: 0, color: '#6b7280' }]}
/>
```

### Threshold lines (Buffett, Seasonality)

```tsx
horizontalLines={[
    { value: 1, color: '#4ade80', label: 'η̄ = 1' },      // green: target
    { value: 0, color: '#6b7280', label: '' },            // gray: zero
    { value: -1, color: '#ef4444', label: 'Кризис' },     // red: danger
]}
```

### With event markers (Funds Money)

Used in `FlowsHistogram.tsx` — annotations are defined in `config/fundAnnotations.ts`:

```typescript
annotations: [
    { date: '2024-08-06', label: 'Слияние', icon: '🔄' }
]
```

For time-series charts, prefer `annotations` prop over vertical lines.

## Data Smoothing

For noisy quarterly data, use EMA:

```typescript
function ema(values: number[], span: number): number[] {
    if (values.length === 0) return [];
    const alpha = 2 / (span + 1);
    const result = [values[0]];
    for (let i = 1; i < values.length; i++) {
        result.push(alpha * values[i] + (1 - alpha) * result[i - 1]);
    }
    return result;
}

// Use: EMA(4) for quarterly ≈ 1-year smoothing
const smoothed = ema(rawValues, 4);
```

For annual data, usually no smoothing needed — already low-frequency.

## Clamping Outliers

Some metrics can explode near zero (division-based). Clamp before passing to chart:

```typescript
function clampValue(v: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, v));
}

// Example: η̄ can range from -100 to +100 but meaningful values are [-3, 5]
const cleanData = raw.map(d => ({
    time: d.date,
    value: clampValue(d.eta, -3, 5),
}));
```

## Color Palette (from `chartTheme.ts`)

```typescript
import { CHART_COLORS } from '../config/chartTheme';

CHART_COLORS.accent         // '#C8FF2E'  — primary green (Buffett, stocks)
CHART_COLORS.primary        // '#6366f1'  — indigo (default)
CHART_COLORS.positive       // '#2EE59D'  — inflow/growth
CHART_COLORS.negative       // '#FF4D4D'  — outflow/loss
CHART_COLORS.muted          // '#9CA3B8'  — axes, secondary text
CHART_COLORS.warning        // '#f59e0b'  — amber (volume)
```

**When to use what:**
- Financial metric (Cap/GDP, Strength): `#C8FF2E` (accent green) primary
- Economic metric (Buffett, Nigmatulin-like): `#8b5cf6` or `#6366f1` (purple/indigo)
- Flow direction: `CHART_COLORS.positive` / `CHART_COLORS.negative`
- Secondary metric: `#f59e0b` (amber)

## Dual-Y-axis Scale Considerations

When `showSecondary={true}`, SimpleChart auto-scales each axis independently:
- Left Y: min/max of `data[].value`
- Right Y: min/max of `secondaryData[].value`

**Potential issue:** if secondary data has wildly different scale (e.g., ratio 0.5 and market cap 75 trln), both lines look correct individually but eye can't compare them. Use dual-axis only when the two metrics are **related** (same event moves both).

## Animation

Controlled via `chartTheme.ts` `ANIMATION`:
```typescript
waveDuration: 1500,     // bar chart reveal
waveStagger: 700,       // delay between bars
morphDuration: 600,     // line morphing on data change
```

Usually don't override — defaults are tuned for the project's feel.

## Testing Chart Changes

1. Build: `cd frontend && npm run build`
2. Deploy frontend (use `moex-deploy-frontend` skill)
3. Visit page, check:
   - Legend shows both metrics
   - Hover tooltip values are correct
   - Navigator drag works if enabled
   - Horizontal lines at right values
   - Mobile layout (if relevant)

## Layout Tokens (CSS Vars) — Unified Chart Geometry

**All chart spacing is driven by CSS custom properties in `frontend/src/index.css`.**
Never hardcode padding / container heights — they'll drift out of sync with other charts. Breakpoints: desktop `:root`, tablet `@media(max-width: 1023px)`, mobile `@media(max-width: 767px)`.

Canonical tokens (desktop values):

```css
--chart-height: 450px;              /* inner chart area (without legend/navigator) */
--chart-nav-height: 52px;           /* ChartNavigator mini-preview height */
--chart-pad-top: 19px;              /* legend-to-plot gap */
--chart-pad-bottom: 50px;           /* X-axis labels area */
--chart-pad-left: 100px;            /* left Y-axis labels */
--chart-pad-right-single: 95px;     /* right padding when only left axis used */
--chart-pad-right-dual: 95px;       /* right padding when right Y-axis present */
--chart-legend-mb: 16px;            /* space between legend and plot */
--chart-annotation-height: 28px;    /* annotation-row height */
--chart-annotation-offset: -34px;   /* how much annotation row overlaps into the plot */
--chart-nav-mt: 12px;               /* navigator top margin */
--chart-xlabel-bottom: 20px;        /* X-labels distance from plot bottom */
--date-top-legend-top: 38px;        /* FlowsHistogram date tooltip y-position */
```

SimpleChart reads them internally. Custom chart components (FlowsHistogram, seasonality charts) must also read them:

```jsx
<div className="relative chart-reveal"
     style={{ height: 'var(--chart-height, 450px)', display: 'flow-root' }}>
  <div className="absolute" style={{
    top: 'var(--chart-pad-top, 19px)',
    bottom: 'var(--chart-pad-bottom, 50px)',
    left: 'var(--chart-pad-left, 100px)',
    right: 'var(--chart-pad-right-single, 95px)',
  }}>
    {/* plot area */}
  </div>
</div>
```

**Specialist tuning:** `docs/chart-layout-playground.html` and `docs/scha-layout-playground.html` expose the vars as sliders — a non-coder can tune visual balance and copy the final values into index.css.

## Known Chart Pitfalls (expensive to debug)

These bit us in the v105 session. Guard against them in every new chart.

### 1. CSS margin-collapse through padding-less parent
**Symptom:** container is visibly shorter than sibling SimpleChart (~50px) despite identical inline values.
**Cause:** a child row has negative `marginTop` (e.g. annotation row overlapping the plot). If the parent has no `padding`, `border`, or `overflow: hidden/auto`, the negative margin escapes *upward* through the parent and the parent's own height shrinks.
**Fix:** `display: 'flow-root'` on the parent — creates a Block Formatting Context that contains the margins.

### 2. ChartNavigator right-edge "stretching" on mount
**Symptom:** navigator appears narrow for ~2s, then snaps to full width.
**Cause:** SVG path was computed in pixels from measured JS width, which starts at 0 until ResizeObserver fires.
**Fix:** already applied in ChartNavigator. Uses `viewBox="0 0 1000 H"` + `preserveAspectRatio="none"` so the browser stretches the path to container width with no JS. Don't revert to pixel-based geometry.

### 3. One-frame [0, 0] selection flash
**Symptom:** navigator handle snaps from center to edges when data arrives.
**Cause:** `useEffect` for `setNavRange([0, data.length - 1])` runs *after* paint — first frame renders with stale `[0, 0]`.
**Fix:** use `useLayoutEffect` for any state that determines layout. Pattern in FundsMoneyPage:
```jsx
useLayoutEffect(() => {
  if (flowsData?.flows?.length) setFlowNavRange([0, flowsData.flows.length - 1]);
}, [flowsData]);
```

### 4. Refs accessed during render
**Symptom:** React warning "Cannot read refs during render."
**Cause:** IIFE inside JSX that reads `ref.current.getBoundingClientRect()` — render runs before refs are attached.
**Fix:** move the measurement into `useLayoutEffect`, store in state.

### 5. ResizeObserver `width=0` on remount
**Symptom:** viewMode toggle resets navigator to 0-width for one frame.
**Cause:** component unmounts, remounts with fresh `useState(0)`, ResizeObserver fires next frame.
**Fix:** module-level cache pattern (in ChartNavigator):
```js
let lastKnownWidth = 0;  // module scope, persists across mounts
const [width, setWidth] = useState(lastKnownWidth);
// on measure: setWidth(w); lastKnownWidth = w;
```
