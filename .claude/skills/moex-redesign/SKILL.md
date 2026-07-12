---
name: moex-redesign
description: Editorial-редизайн сайта Frame. Use when user says "редизайн", "редизайнить", "переделать страницу под новый стиль", "editorial", "сделай как в референсе", "адаптируй дизайн", "новый дизайн", или когда правит CSS/визуал editorial-light/editorial-dark тем. Содержит design tokens, прогресс по страницам, паттерны editorial-override через CSS.
---

# Frame Editorial Redesign — Knowledge & Progress

Редизайн всего сайта Frame в editorial-стиле. Этот skill — единая точка правды:
где лежит референс, что уже сделано, и какие паттерны применять.

## ✅ Статус (2026-05-03): редизайн завершён на 100%

Все 8 индикаторных страниц на единой fluid design system:
- `editorial-frame` обнимает controls + chart
- `useFitToViewport` для chart height
- Dropdown'ы вместо segmented controls
- Editorial-press chip-pills с press-effect
- Theme-aware colors (никаких hardcoded hex)
- SW: `frame-v367`

**Главное чтиво** (расположено в проектной памяти):
- ⚠️ Пути ниже — это личная память Claude Code **на Mac Вадима**
  (`~/.claude/projects/-Users-vadim-PyCharmMiscProject-MOEX/memory/`), она не переносится
  между машинами/аккаунтами. На этой машине эквивалентного `design_system.md`/
  `recent_changes.md` нет — если нужны детали tokens/helpers/anti-patterns, спроси Вадима
  напрямую или попроси его экспортировать эти файлы.
- `design_system.md` — все tokens, helpers, anti-patterns
- `recent_changes.md` — что сделано в последней сессии

## Quick reference — что использовать в новом коде

| Нужно | Используй |
|-------|-----------|
| font-size | `var(--fs-*)` (2xs/xs/sm/base/lg/xl/2xl/3xl) или Tailwind `text-*` |
| spacing | `var(--sp-*)` (1..6) или Tailwind `gap-*`/`p-*` |
| icon size | `var(--ico-*)` (xs/sm/md/lg) |
| chart height | `useFitToViewport(ref, {min:360, max:720, bottomBuffer:96})` |
| date pill position | `computeChartTopLineY` + `getDatePillStyle` из `datePillLayout.ts` |
| color: primary line | `var(--accent)` (pumpkin) |
| color: positive | `var(--funds-flow-positive)` (forest) |
| color: negative | `var(--funds-flow-negative)` (clay) |
| chart bg | `var(--bg-primary)` (paper) — НЕ secondary (white) |
| button hover | класс `editorial-press` + `1.5px solid var(--text-primary)` outline |
| dropdown | `<Dropdown options=... value=... onChange=... />` |

Перед деплоем — `проверь верстку` запускает `moex-responsive-auditor` agent.

## ⚠️ Прежде чем начать любую правку

1. **Открыть соответствующий jsx-референс** из папки design_handoff (см. ниже, где лежит на этой машине)
2. **Сверять pixel-by-pixel**: толщина линий, цвета, отступы, типографика
3. **НЕ копипастить код** из референса — это дизайн-референс уровня Figma, а не production код. Цель — воссоздать в нашем стеке (React/Vite, Tailwind + CSS-vars).

## Где лежит референс

⚠️ Ниже — путь на Mac Вадима (`/Users/vadim/Downloads/design_handoff_frame_redesign/`).
Сама папка — не часть git-репозитория, это отдельный набор файлов, который нужно
получить от Вадима отдельно (архивом/облаком) и положить в удобное место на этой
машине (например `C:/MOEX/local_archive/design_handoff_frame_redesign/`) — на новой
машине по умолчанию её нет.

Внутри:

| Файл | Что внутри |
|---|---|
| `README.md` | Design tokens (colors, spacing, typography, shadows), описание 7 страниц |
| `Frame Journal.html` | **Лучший обзорный материал** — журнал на 17 разворотов |
| `Frame Redesign.html` | Все артборды (canvas pan/zoom) |
| `page.jsx` | Открытый интерес — главная страница, шеллa |
| `extra-pages.jsx` | 6 остальных страниц (карта рынка, фонды, Баффетт, сила, сезонность) |
| `chart.jsx` | Line-chart + brush/range selector (navigator) |
| `logo.jsx` | FrameLogo SVG-компонент |

## Design Tokens

### Цвета

**Editorial-light:**
- Фон страницы: `#F4F1EA` (тёплая бумага) — `--bg-primary`
- Панели/widget: `#FFFFFF` — `--bg-secondary`
- Текст: `#0A0A0A` — `--text-primary`
- Border: `#0A0A0A` (1.5px solid, не alpha) — `--border-color`

**Editorial-dark:**
- Фон страницы: `#0E0E10` — `--bg-primary`
- Текст: `#FFFFFF` — `--text-primary`
- Border: `#FFFFFF` (1.5px) — `--border-color`

**Accent (по умолчанию + 2 вариации):**
- Pumpkin (default): `#FF5C2B` (наш) — референс показывает `#F46A5C`, но мы используем уже существующий
- Indigo: `#5B5BD6`
- _Убраны: violet, forest, lemon (по решению пользователя — оставили только default + pumpkin)_

**OI палитра (теперь одинаково в обеих editorial-темах):**
```
--oi-amber: orange  → ОИ
--oi-green: green   → Покупки/Long
--oi-red:   #B91C5C → Продажи/Short (deep magenta)
--oi-cyan:  #FF5C2B → Чистая позиция (фирменный pumpkin)
```

### Тени (Hard offset shadows — главная фишка editorial)

```css
--shadow-sm:   2px 2px 0 #0A0A0A;
--shadow-card: 4px 4px 0 currentColor;   /* idle */
--shadow-hover: 6px 6px 0 currentColor;  /* hover */
```

В editorial-dark тени **cream-цвета** (`#F5F1E8`), не чёрного — чтобы их было видно на тёмном фоне.

### Типографика

- Display: Archivo 800, 96–120px, line-height 1.0, tracking -0.04em
- H1: Archivo 800, 64px
- H2: Archivo 800 italic, 48px
- Eyebrow: Archivo 700, 11px, uppercase, tracking 0.16–0.32em
- Body: Archivo/Inter 400, 17px, line-height 1.55
- Mono: Archivo 600, 13px, tabular-nums

### Spacing

`8 · 16 · 24 · 40 · 64 · 96` px

### Border radius

`0` для всех карточек/кнопок (квадратные)
`12` — CTA-кнопки
`999` — pill-кнопки в header
`8` — navigator container (исключение)

## Архитектура CSS-overrides

Главный паттерн: **editorial-overrides через `[data-theme^="editorial"]`** в `frontend/src/index.css`.

Это значит:
- Базовые стили в Tailwind/inline остаются "OKX-friendly"
- Editorial темы переопределяют через CSS-attr селектор без изменения JSX
- Это позволяет один и тот же компонент работать во всех темах

### Пример (Navigator — уже готов):

```css
[data-theme^="editorial"] .chart-navigator {
  border: 1.5px solid var(--text-primary);
  border-radius: 8px;
  overflow: hidden;
  background: var(--bg-primary);
}
[data-theme^="editorial"] .chart-navigator path.nav-mini-area {
  fill: color-mix(in srgb, var(--text-primary) 22%, transparent) !important;
}
[data-theme^="editorial"] .chart-navigator .nav-handle {
  height: 100% !important;
  width: 14px !important;
  border: none !important;
  border-radius: 0 !important;
}
[data-theme^="editorial"] .chart-navigator .nav-handle-left {
  border-right: 2px solid var(--text-primary) !important;
}
```

**Когда нужны className** (для targeting в CSS-override): добавляем чистые
имена-классы (`nav-handle`, `nav-mini-area` и т.д.) на JSX элементы, не
зависящие от Tailwind.

## Прогресс по страницам

### ✅ Открытый интерес — готово (~85%)

Что сделано:
- Фон графика = `var(--bg-primary)` (paper, был secondary/white)
- SimpleChart wrapper: `bg-theme-primary` (был secondary)
- Убрана внешняя обёртка-дубль с border (была двойная рамка)
- Цвета OI-линий через CSS-vars (`--oi-amber/green/red/cyan`)
- Толщина линий +10%: primary 3.3px, secondary 2.75px (mobile 2.2px)
- Crosshair tooltip: `var(--text-primary)` opacity 0.55 (был --accent → сливался с pumpkin линиями)
- Затемнение области после курсора: 8% text-primary (было 50% almost-black)
- Navigator: переделан под референс из `chart.jsx`
- PageHeader: editorial-style иконки (filled square + hard shadow)
- Hard shadows в editorial-dark через cream (#F5F1E8) вместо чёрного
- Chart shadow visible во всём контейнере (а не только наружный)

Что осталось:
- Проверка KPI-блока сверху (если есть)
- Footer-rule с timestamp/source/ticker chips
- Side-rail с фильтрами

### 🔄 Не делали (6 страниц)

| Страница | Файл-референс | Что предполагает |
|---|---|---|
| **Карта рынка** | `extra-pages.jsx → MarketMapPage` | Treemap, размер=кап, цвет=изменение |
| **Деньги в фондах** | `FundsFlowPage` | Горизонтальные бары притоков/оттоков |
| **Состав фондов** | `FundsCompositionPage` | Сетка donut-чартов |
| **Индикатор Баффетта** | `BuffettPage` | Двойной line + KPI-карточки |
| **Сила рынка** | `MarketStrengthPage` | Двухпанельный график MA50/MA200 |
| **Сезонность** | `SeasonalityPage` | Heatmap месяц × год |

## Workflow при правке страницы

1. **Открыть jsx-референс** соответствующей страницы из handoff
2. **Сравнить с текущим** на проде (https://таймфрейм.рф)
3. **Список изменений**: цвета, отступы, толщины линий, типографика
4. **Применить через editorial-override** в `index.css` где возможно (минимум JSX-правок)
5. **Bump SW + build + deploy** (см. moex-deploy-frontend skill)
6. **WIP-коммит** с пометкой `wip(redesign):` если правки не закончены — safety net

## Критичные паттерны (lessons learned)

### 1. SVG inline-style побеждает класс родителя

`<svg style={{ backgroundColor: 'var(--bg-secondary)' }}>` — даже если parent
имеет `bg-theme-primary`, SVG не наследует. **Менять inline-style напрямую**.

### 2. Hardcoded цвета — главный враг

Найти и заменить все `#0B0D12`, `rgba(0,0,0,0.X)`, `#FFFFFF` хардкоды на
CSS-vars (`var(--text-primary)`, `var(--bg-primary)` etc.).

### 3. Hard shadow в dark

В editorial-dark тени **не чёрные** — это бессмысленно на тёмном фоне.
Используем cream (`#F5F1E8`) для visibility.

### 4. Двойные рамки

Если SimpleChart уже имеет `border + rounded`, не оборачивай его в ещё один
`<div className="border rounded-2xl">`. Иначе двойная рамка.

### 5. Adaptive translateX для handles

Когда нужно держать элемент **внутри** контейнера (с overflow:hidden), но
позиционировать через CSS %:
```jsx
transform: `translateX(-${frac * 100}%) translateY(-50%)`
```
0% → translateX(0), 100% → translateX(-100%) — handle всегда внутри.

### 6. !important в editorial-override

Так как inline-style побеждает обычные CSS-rules, **в editorial-overrides
часто нужен `!important`** чтобы перебить inline-стили JSX. Не злоупотреблять,
но это нормально для theme-override слоя.

### 7. Editorial press-effect — единый паттерн "живых" нажатий

Все интерактивные кнопки/chip'ы в editorial должны иметь press-эффект:
hover → translate(-1px,-1px) + 4px×4px hard shadow; active → translate(+1px,+1px) + 1×1×0.

**Реализация в `index.css`** — generic rule на все типичные селекторы:
```css
[data-theme^="editorial"] .editorial-press,
[data-theme^="editorial"] .frame-dropdown-trigger,
[data-theme^="editorial"] .instrument-modal-chip,
[data-theme^="editorial"] button.widget-flat,
[data-theme^="editorial"] .btn-control {
  transition: transform 0.12s ease, box-shadow 0.12s ease !important;
}
[data-theme^="editorial"] *.editorial-press:hover:not(:disabled), ... {
  transform: translate(-1px, -1px);
  box-shadow: 4px 4px 0 var(--text-primary) !important;
}
```

**Когда применять**:
- Любой новый chip/pill/кнопка в editorial-стиле — добавь class `editorial-press`
- Не нужно для list-items, ссылок текста, иконок без border
- Не работает на disabled (через `:not(:disabled)`)

Эффект автоматически активен на: Dropdown trigger, asset selector
(widget-flat), category chips модалки, btn-control. Любой новый element
с этим визуальным языком — просто добавляешь `editorial-press` className.

## Связанные skills

- `moex-deploy-frontend` — после правок дизайна
- `moex-simplechart-usage` — если правишь SimpleChart
- `moex-git-workflow` — для wip-коммитов

## Текущая SW версия

См. `frontend/public/sw.js` — обновлять при каждом деплое.
Последняя на момент создания скилла: `v271`.
