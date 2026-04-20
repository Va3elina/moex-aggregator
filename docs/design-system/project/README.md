# Фрейм — Аналитика Московской биржи

**Brand:** Фрейм (Beta)
**Domain:** фрейм.рф (xn--80aklbnczmv.xn--p1ai)
**Product:** Retail-investor analytics for the Moscow Exchange — a dashboard of 8 market-structure indicators plus a market overview.

## Sources

- **Codebase:** [Va3elina/moex-aggregator](https://github.com/Va3elina/moex-aggregator) — full-stack app (FastAPI + React/Vite). Imported in `reference/frontend/` (React/TS source, tokens, chart library).
- **Live site:** <https://xn--80aklbnczmv.xn--p1ai> — blocked from fetching; all design context is derived from source code.
- **Key token files** (inside the reference clone):
  - `reference/frontend/src/index.css` — 6 CSS-var themes + layout tokens
  - `reference/frontend/src/config/chartTheme.ts` — chart palette, animation, dot/line specs
  - `reference/frontend/src/config/fearConfig.ts` — Fear Index color scale
  - `reference/frontend/src/contexts/ThemeContext.tsx` — theme list
  - `reference/frontend/src/pages/*.tsx` — one page per indicator (production UI)

## Product surfaces

Single marketing-less web app. The nav bar lists:

| Path              | Russian label          | Accent    | What it is |
| ----------------- | ---------------------- | --------- | ---------- |
| `/`               | Обзор рынка            | lime→green gradient | Market overview — all indicators at a glance |
| `/fear`           | Индекс страха          | #F97316   | Fear/greed gauge 0–100 based on fund rotations |
| `/heatmap`        | Карта рынка            | #22C55E   | Finviz-style treemap by sector/cap/volume |
| `/oi`             | Открытый интерес       | #6366F1   | Open interest on MOEX futures, physicals vs legal |
| `/funds-money`    | Деньги в фондах        | #2EE59D   | AUM flows across fund categories |
| `/funds-catalog`  | Состав фондов          | #06B6D4   | Fund portfolio holdings |
| `/buffett`        | Индикатор Баффетта     | #F59E0B   | MarketCap/GDP ratio |
| `/strength`       | Сила рынка             | #8B5CF6   | % of stocks above EMA200 (breadth) |
| `/seasonality`    | Сезонность             | #EC4899   | Monthly/yearly seasonality histograms |

## Design brief (from the owner)

> Хочу немного переработать дизайн — где-то несимметричные графики, где-то шрифты и размеры.
> Focus: унификация визуального стиля между разными индикаторами (8 штук), чистая типографика, consistent spacing.

**Translation:** unify the eight indicator pages — consistent spacing, cleaner typography, symmetric chart layouts. The system in this repo reflects that goal: one type scale, one spacing grid, one widget treatment.

---

## CONTENT FUNDAMENTALS

Copy is **Russian**, direct, and numeric. The product never uses emoji in content (theme icons like `🌿` are UI-only controls). It never markets itself with hype language — it reports.

**Voice & tone.**
- **Informational, not instructional.** Page subtitles describe what the indicator is, not what the user should do. E.g. *"Настроения инвесторов по потокам в фонды"*, not *"Узнай настроения инвесторов!"*.
- **Impersonal.** Addresses neither "я" nor "ты/вы" in body copy. Exception: the theme toggle tooltip uses imperative *"Нажми"* — lowercase, casual.
- **Russian financial terminology preferred over English.** "Капитализация", "Оборот", "Открытый интерес", "Сила рынка". English terms used only when truly technical: *Rotation Ratio*, *Velocity*, *EMA200*, *MM* (money-market).
- **Case:** Sentence case everywhere. No ALL CAPS. No Title Case for headings. Labels are lowercase when they're verbal ("открыть", "войти", "всё") and sentence case when they're nouns ("Индекс страха", "Карта рынка").
- **No exclamation marks.** No em-dashes for emphasis. Hyphens are used for ranges and the iconic brand name treatment (*"Фрейм — Аналитика..."*).

**Numbers.**
- Russian locale: decimal comma, space thousands separator. Percentages: `12,34%` (comma, no space).
- Currency: `1 234 млрд ₽`, always ruble at end.
- Tabular-nums on **every** numeric surface so values don't jitter when they tick.
- Deltas: explicit sign on both sides — `+12,34%` / `−12,34%` (display uses `+` / `−`, not `-`).

**Copy examples (verbatim).**
- Page hero: `Обзор рынка / Аналитика и индикаторы в реальном времени`
- Page hero: `Индекс страха / Настроения инвесторов по потокам в фонды`
- Metric caption: `Шкала: 0 (жадность) → 100 (страх)` — arrow glyph, parentheticals for scale ends.
- Status badge: `Beta` (English word, capital B, small pill).
- Empty state: `Нет данных` (three lowercase words) or `Нет данных для отображения`.
- CTA: `Полный график →`, `Открыть канал`, `Войти`, `Плюс` / `Plus` (paid).
- Real-time tooltip: `Live: данные обновляются автоматически` / `Нет соединения с сервером`.

**Emoji.** Only in the theme switcher as functional iconography for named palettes: 🌿 OKX Green, 🌙 Neon Lime, 💛 Binance Gold, 🌊 Ocean Blue, 🌅 Sunset Orange, ☀️ Light Mode. **Never** in marketing copy, headings, buttons, or data.

**The vibe.** Bloomberg Terminal pared down for retail — dark, green-accented, graph-forward. Treats the user as someone who already knows what "СЧА" and "open interest" mean. No onboarding prose.

---

## VISUAL FOUNDATIONS

The system has **six themes** shipped in production; the canonical one (default) is **OKX Green** — a near-black background with vivid green accents. Everything below describes the OKX Green default; the token file has equivalents for Light. See `reference/frontend/src/index.css` for the other four themes (Neon Lime, Binance Gold, Ocean Blue, Sunset Orange).

**Surfaces.** A 4-step dark ladder, not a gradient ramp.
- `--bg-primary` `#0B0D0F` — page background (near-black, slight warm)
- `--bg-secondary` `#131518` — widget body
- `--bg-tertiary` `#1B1E21` — inner chips, lighter regions inside widgets
- `--bg-elevated` `#232629` — popovers, tooltips

Widgets themselves use a **subtle 135° linear gradient** (`#131518 → #1B1E21`) rather than a flat fill — just enough specular to separate them from the page. On hover, each gradient stop lifts by ~4–6 lightness points.

**Type.** Inter everywhere (400/500/600/700/800). Production uses `'Inter', system-ui, ...` — downloadable webfont is Inter from Google Fonts. `tabular-nums` and `cv11` are ON globally so numbers in tooltips/tickers stay rigid.

**Colors.**
- **Accent (product):** `#00E676` — the "Фрейм" green. Used for logo, primary CTAs, active-state nav link, focus rings.
- **Eight indicator accents:** each page claims a color (see table above). These tint the page icon tile and act as the page's chart primary. Never mix them on the same widget.
- **Data semantics:** `--data-positive #2EE59D`, `--data-negative #FF4D4D`, `--data-neutral #9CA3B8`. These are fixed across themes — a green bar must feel "up" in every palette.
- **Fear scale (5-step):** `#22C55E → #84CC16 → #EAB308 → #F97316 → #EF4444`.
- **Heatmap:** Finviz-style compressed palette. Dark center (`#2A2A2A`) for 0%, saturated but not neon greens/reds at extremes. Deliberately **not** using the bright `#00E676` accent for bull cells.
- **Plus CTA:** `--accent-pink #FF4081` — used exclusively for the "Plus" upsell button.

**Spacing.** 4px base grid. Widgets use `p-6` (24px) inside. Widget gaps in grids: `gap-6` (24px). Page containers: `max-w-7xl` (1280px) with `px-4 md:px-6` padding and `py-6 md:py-8` vertical. Sticky header: 56px mobile / 64px desktop.

**Radii.** The signature is `--radius-xl` (16px) on widgets. Smaller rounding descending: 12px (nav links, pill groups), 8px (inputs), 6px (inner chips), full (indicator dots and avatar).

**Borders.** Always ultra-subtle: `rgba(255,255,255,0.08)`. Hover lifts to the accent tint (`rgba(0,230,118,0.4)`), active to 0.6.

**Shadows.** A 3-tier elevation system — all pure black with opacity, no colored shadows except the "glow" variant.
- `--shadow-sm 0 2px 8px rgba(0,0,0,0.30)` — tooltips, chips
- `--shadow-md 0 4px 24px rgba(0,0,0,0.40)` — default widget (always on)
- `--shadow-lg 0 8px 32px rgba(0,0,0,0.50)` — modal, big hover lift
- `--shadow-glow 0 0 24px rgba(0,230,118,0.25)` — accent buttons only

**Transparency & blur.** Sticky header uses `backdrop-blur-xl` on a `rgba(11,13,15,0.85)` glass fill — the *only* place blur is used in the app. Tooltips add `backdrop-blur-sm` on a 95% fill.

**Cards / widgets.**
```
background: linear-gradient(135deg, #131518, #1B1E21);
border: 1px solid rgba(255,255,255,0.08);
border-radius: 16px;
box-shadow: 0 4px 24px rgba(0,0,0,0.4);
transition: background .3s, border-color .3s, box-shadow .3s;
padding: 24px;
```
On hover for clickable widgets (`<Link>`): border shifts to `rgba(0,230,118,0.3)`, gradient lifts slightly, no lift-Y transform.

**Backgrounds / imagery.** No hero images. No illustrations. No repeating patterns. No gradients on page backgrounds — the app is a data surface. The only "art" is the data itself: charts fill the canvas.

**Animations.**
- **Page enter:** `page-reveal` — 350ms `cubic-bezier(0.22, 1, 0.36, 1)` left-to-right clip-path sweep. Used when routing between pages.
- **Chart reveal:** `chart-reveal` — 1s `cubic-bezier(0.25, 0.46, 0.45, 0.94)` left-to-right clip-path sweep. Each chart enters with this on data-load.
- **Line morph:** 600ms `easeOutCubic` ( `t → 1 − (1−t)³` ) between two sets of points. All line-charts use this same curve.
- **Histogram wave:** 1500ms total with 700ms cascade stagger left→right, easing `cubic-bezier(0.25, 0.46, 0.45, 0.94)`. Bars grow from the zero-line.
- **Hover transitions:** 200ms ease on color/bg/border; 300ms ease on widget transitions. No bounces. No spring physics.

**Interaction states.**
- **Hover:** lighter bg, accent-tinted border. Links add a chevron color change to accent. No opacity dimming of whole elements except `btn-accent:hover → opacity 0.9` + glow shadow.
- **Press:** `transform: scale(0.95)` applied globally to `button`, `[role=button]`, `a.btn-control`, `a.btn-accent` via a 150ms ease. This is the signature press feedback.
- **Active (selected):** accent bg at 10% mix + accent border at 30% mix. Text color = accent.
- **Disabled:** `--fg-muted` color + `cursor: not-allowed`, no opacity change.
- **Focus:** border shifts to `--accent`, no outline ring.

**Layout rules.**
- Sticky top nav, always visible.
- Page hero is always: colored icon tile (3rem square, gradient) + `<h1>` + muted subtitle on one row.
- 3-column widget grid on desktop (`grid-cols-1 lg:grid-cols-3`), stacking on mobile.
- Each widget has a fixed internal rhythm: header row (icon + title + right-arrow) → main block → footer (separator + CTA or stat).
- Chart padding is asymmetric by purpose: 100px left (for y-axis labels), 12px right (single-axis) or 95px right (dual-axis). This is the "несимметричные графики" the brief mentions — intentional, not accidental.

**Fixed elements.** The sticky nav is the only fixed chrome. No floating FABs. No side drawers. No toast notifications.

---

## ICONOGRAPHY

**The library is [Lucide React](https://lucide.dev)** — ships via `lucide-react` in the codebase; in this design system we load from the Lucide CDN. Stroke width is the Lucide default (2). Sizes in use: 14, 16, 18, 20, 24.

**Usage by page / UI:**

| Icon            | Where it's used |
| --------------- | --------------- |
| `Compass`       | `/` Overview page title tile |
| `Gauge`         | `/fear` title tile; `/buffett` |
| `Grid3X3` / `LayoutGrid` | `/heatmap` title; Overview card |
| `BarChart3`     | `/oi` title; Overview "OI Preview" |
| `TrendingUp`    | Strength page; positive deltas |
| `TrendingDown`  | Negative deltas |
| `DollarSign`    | Funds pages |
| `Activity`      | Fear Index widget header on Overview |
| `ArrowRight`    | "Open" affordance, CTA chevrons |
| `ExternalLink`  | Outbound links (e.g. Telegram channel) |
| `MessageCircle` | Telegram widget |
| `Menu` / `X`    | Mobile nav toggle |
| `LogIn`         | Auth button |
| `Lock`          | Gated periods |
| `AlertTriangle` | Error states |
| `Mail`, `Eye`, `EyeOff` | Login form |

**Title-tile pattern.** Every page hero has a 3rem square rounded-xl (12px) tile with a `linear-gradient(135deg, color-1, color-2)` fill and the page's Lucide icon in white or `text-black` (when the accent is pale). Examples: `from-[#C8FF2E] to-[#22c55e]`, `from-[#f97316] to-[#ef4444]`, `from-[#22c55e] to-[#14b8a6]`.

**Emoji.** Only in the theme switcher (🌿 🌙 💛 🌊 🌅 ☀️) as labeled palette icons. **Never** in body content, titles, data, or buttons.

**SVG assets.**
- **Logo (placeholder):** there's no wordmark yet — the product renders the literal word "Фрейм" in Inter Bold 20px, colored `--accent`. `assets/icon-192.svg` and `icon-512.svg` are the PWA icons: a single Cyrillic "Ф" in Inter Bold 110px, drawn with a `#6366f1 → #9D4DFF` gradient fill on a `#13151f → #0B0D12` dark tile. Note: these icons still use the old "Neon Lime" palette — a known inconsistency with the current OKX Green default.
- **OAuth provider marks** (`GoogleIcon`, `VKIcon`, `YandexIcon`, `TelegramIcon`) live inline in `LoginPage.tsx`; they're the only brand marks in the system.
- The app has **no** custom illustration style, no spot art, no decorative line-work. Every vector is either a Lucide icon or a live SVG chart.

---

## Index / manifest

Root of this design system:

- `README.md` — this file.
- `SKILL.md` — agent-skill preamble (loads this system for Claude Code compatibility).
- `colors_and_type.css` — all tokens. Import this first in any new HTML artifact.
- `fonts/` — webfont references (Inter via Google Fonts).
- `assets/` — logos, PWA icons.
- `preview/` — one HTML card per design-system concept (renders in the Design System tab).
- `ui_kits/web/` — React recreations of the core web-app surfaces (Overview, Fear Index, Heatmap). `index.html` is a click-through prototype.
- `reference/frontend/` — the imported production source. Treat as read-only; the design system lives in the root, not here.
