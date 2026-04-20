# UI kit — Фрейм web app

High-fidelity recreation of the production Moscow-Exchange-analytics dashboard. One click-through prototype with the core indicator pages, using tokens from `colors_and_type.css`.

**Screens:**
- `/` — Обзор рынка (market overview, 3×2 widget grid + OI chart)
- `/fear` — Индекс страха (big gauge + components grid + chart + legend)
- `/heatmap` — Карта рынка (treemap by sector)
- `/strength` — Сила рынка (breadth + sector detail)

Open `index.html`.

Files:
- `index.html` — shell with route-based navigation (fake URL state)
- `components.jsx` — shared primitives (Widget, Nav, PageHero, MetricHero, ScaleBar, IconTile, ChartBox, Icon)
- `OverviewPage.jsx` — market overview
- `FearIndexPage.jsx` — fear page
- `HeatmapPage.jsx` — treemap page
- `StrengthPage.jsx` — breadth page
- `data.js` — canned demo data (stocks, sectors, fear points)
