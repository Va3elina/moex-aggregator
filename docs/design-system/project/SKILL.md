# Фрейм — design system skill

Use this when designing anything for **Фрейм**, the MOEX retail-investor analytics dashboard at фрейм.рф.

## Start every task by reading
1. `README.md` — brand voice, product context, writing conventions
2. `colors_and_type.css` — all design tokens. Import it in your `<head>`; never redefine colors or fonts inline.
3. `ui_kits/web/index.html` — the live UI kit. Open it and crib patterns before inventing anything.

## The product in one paragraph
Фрейм is a **free, Russian-language market-structure dashboard**. Every screen answers one question — *«Что сейчас делает рынок?»* — through a single primary indicator (Индекс страха, Сила рынка, Карта рынка, Баффет, Открытый интерес, Фонды) visualised as a big number, a scale, or a heatmap. No execution, no portfolio. Tone is precise, unsensational, Russian — «настроения», «широта», «перепроданность».

## Non-negotiables

- **Dark-first.** Background `#0B0D12`, widgets `#13151F` with a subtle gradient. Light theme exists (OKX Day) but design everything in dark and verify light.
- **Accent is green `#00E676` (OKX Green).** Red/green semantic colors are fixed and never themed: `+value` → `#2EE59D`, `-value` → `#FF4D4D`, `0` → `#9CA3B8`.
- **Numbers are the UI.** Big numerics are `font-variant-numeric: tabular-nums`, 72–96px, weight 700, letter-spacing `-0.02em`. Never left-align them — center, with the pill status below.
- **Widget signature: 16px radius, 1px `var(--border)`, `var(--shadow-md)`, hover swaps to `rgba(0,230,118,0.3)` border + `var(--bg-widget-hover)` background.** Use the `Widget` component; don't hand-roll cards.
- **Russian only.** All UI copy is Russian. Numbers use Russian formatting: decimal comma (`+12,34%`), space thousands separator (`1 245 670 ₽`), ruble symbol `₽` after the amount.
- **No emoji** (except the existing theme-toggle 🌿). No icon-as-bullet lists. Icons are lucide-family, 1.75 stroke, rendered in gradient `IconTile` only at the page-hero position.
- **Density matters.** Dashboards are busy on purpose. Don't pad to fill — let widgets breathe with 20–24px grid gaps and 22–24px internal padding, not more.

## Page pattern
Every indicator page follows the same three-block rhythm:

1. **`PageHero`** — gradient `IconTile` + H1 (22px/700) + one-line subtitle + optional `PeriodGroup` on the right.
2. **Hero metric row** — 340px `MetricHero` (big number + pill + caption) on the left, `ChartBox` on the right. A single bold answer, then its history.
3. **Components / breakdown** — grid of sub-metrics or a sector table inside another `ChartBox`.

The overview page is a **3×2 grid of `Widget`s**, one per indicator, each a preview of the detail page it links to.

## Variation

When asked for options, vary these first:
- Scale visualization: 5-step fear bar vs. single gradient fill vs. circular gauge
- Widget density: 3-up vs. 2-up vs. masthead-and-row
- Heatmap palette: finviz-style compressed (default) vs. high-contrast vs. colorblind-safe
- Tooltip shape: rounded `rgba(26,31,46,.95)` panel (default) vs. callout arrow vs. inline delta row

Do **not** vary: the accent green, +/- semantic colors, 16px widget radius, tabular numerics.

## Output filenames
`*.html` files should be descriptive Russian or English titles — `Индекс страха.html`, `Heatmap alternatives.html`. Multiple versions: add ` v2`, ` v3` rather than overwriting.

## What exists in `preview/`
Type, color, spacing, and component atoms — 19 registered cards. Read them before proposing token changes.
