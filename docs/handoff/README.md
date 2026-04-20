# Handoff — графики Фрейм

Файлы для фронтенд-разработчика. Задача: унифицировать визуал графиков + завершить частичный рефакторинг.

## 📦 Что в папке

### 🔵 СЧА (эталон, работает — 1286 строк inline)

| Файл | Что это |
|---|---|
| **`SimpleChart.tsx`** | Основной компонент. Используется для СЧА (AUM), Buffett, Strength, и частично Seasonality | 
| **`ChartNavigator.tsx`** | Нижний мини-график с ручками (использует SimpleChart) |

### 🟠 Притоки-Оттоки

| Файл | Состояние |
|---|---|
| **`FlowsHistogram.ORIGINAL.tsx`** | До моих правок (git HEAD) |
| **`FlowsHistogram.CURRENT.tsx`** | Сейчас на проде (v90, с правками симметрии padding'а) |
| `FundsMoneyPage.ORIGINAL.tsx` / `.CURRENT.tsx` | Страница рендерит и SimpleChart (AUM) и FlowsHistogram |

### 🟣 Сезонность (уже отрефакторена)

| Файл | Что это |
|---|---|
| **`SeasonalityPage.CURRENT.tsx`** | Страница «Сезонность» |
| **`seasonality-components/SeasonalityHistogram.tsx`** | Гистограмма — **использует `chart-primitives`** |
| **`seasonality-components/YearlySeasonalityChart.tsx`** | Годовая — **использует `chart-primitives`** |
| **`seasonality-components/SeasonalityPriceChart.tsx`** | Цена — **использует `chart-primitives`** |
| **`seasonality-components/MtdChart.tsx`** | Month-to-date — **использует `chart-primitives`** |

### 🧱 Chart primitives (готовые кирпичики)

| Файл | Что это |
|---|---|
| **`chart-primitives/`** | 8 компонентов (ChartGrid, ChartCrosshair, ChartDot, ChartTooltip, ChartDateLabel, ChartYAxis, ChartXAxis, ChartMarker) — **используются в Seasonality**, НО НЕ в SimpleChart / FlowsHistogram. 420 строк суммарно. |

### 🔧 Общее (зависимости)

| Файл | Что это |
|---|---|
| `chartTheme.ts` | Константы: цвета, паттерны тултипов, анимации, padding |
| `index.css` | CSS-переменные (`--chart-pad-*`, `--chart-font-*`, темы) |

---

## 🎯 Задача специалисту

### Статус рефакторинга

**Частично применён:**
- ✅ Сезонность (4 компонента) — использует `chart-primitives`
- ❌ `SimpleChart.tsx` (1286 строк inline-кода) — не использует
- ❌ `FlowsHistogram.tsx` (750 строк inline) — не использует

**Логичное завершение:** переписать `SimpleChart` и `FlowsHistogram` на `chart-primitives`. Это:
1. Сократит код ~40%
2. Унифицирует визуал (бары, оси, тултипы одинаковые через общие примитивы)
3. Решит проблему "Притоки-Оттоки отличается от СЧА"

### Почему частичный рефакторинг получился

Первый этап (Сезонность) был сделан коммитом `9fb4af1 feat: shared chart components + UI fixes`. Далее должен был быть коммит "refactor: SimpleChart uses chart-primitives", но работу отложили. Остался неприменённый патч в корне проекта (я его удалил).

### Унификация Притоки-Оттоки с СЧА

**Рекомендация (легче):** использовать `SimpleChart` напрямую для Притоков в режиме гистограммы — он уже поддерживает `allowHistogram=true, defaultHistogram=true`.

**Рекомендация (чище):** переписать `FlowsHistogram` на `chart-primitives` (как сделано в сезонности) — получится компактный компонент, который точно повторяет paddings и визуал SimpleChart.

---

## 🔗 Контекст

- Репо: `Va3elina/moex-aggregator`
- Папка: `frontend/src/components/` (SimpleChart.tsx, ChartNavigator.tsx, chart/, funds/FlowsHistogram.tsx, seasonality/*)
- Деплой: `root@103.88.243.232` (Timeweb Cloud, 4GB RAM)
- Стек: React 18 + Vite + TypeScript + Tailwind

## 📐 Ключевые CSS переменные

```css
--chart-pad-left: 100px;
--chart-pad-right-dual: 95px;     /* когда есть правая ось */
--chart-pad-right-single: 95px;   /* когда только левая */
--chart-pad-top: 19px;
--chart-pad-bottom: 50px;
--chart-font-y: 16px;
--chart-font-x: 14px;
--date-top-legend-top: 38px;
--crosshair-color: #C8FF2E;
```
