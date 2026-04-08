# Responsive Design Reference — Фрейм (таймфрейм.рф)

## Эталонное разрешение (референс)

Разработка ведётся на **Mac Chrome**, разрешение ~2560x1440 (Retina). Все размеры ниже — эталон. На других экранах сайт должен **адаптивно подстраиваться**, сохраняя пропорции и читаемость.

---

## Текущее состояние (аудит 2026-04-08)

### Критические проблемы

| Проблема | Где | Приоритет |
|----------|-----|-----------|
| Высота графиков захардкожена (450px, 420px, 350px) | SimpleChart, Seasonality, Funds, Strength | Высокий |
| Шрифты в SVG фиксированные (11-16px) | Все графики | Высокий |
| SVG viewBox фиксированный 1000x500 | SeasonalityPage | Средний |
| Тултипы на fixed/absolute с px | Все страницы | Высокий |
| Навигатор: стрелки Unicode → emoji | ChartNavigator | Исправлено (SVG path) |
| Нет breakpoints в графиках | Charts | Средний |

### Страницы по уровню адаптивности

| Страница | Уровень | Ключевые проблемы |
|----------|---------|-------------------|
| OverviewPage | Отлично | — |
| FearIndexPage | Отлично | — |
| ProfilePage | Отлично | — |
| LoginPage | Хорошо | 90vh max-height |
| BuffettPage | Хорошо | 450px chart height |
| OpenInterestPage | Хорошо | 160/200px min-width |
| FundsCatalogPage | Хорошо | 100px max-width |
| HeatmapPage | Частично | Magic thresholds для текста |
| FundsMoneyPage | Плохо | 450px, 420px, 60px offsets |
| SeasonalityPage | Плохо | 450px, 420px, 350px, viewBox 1000x500 |
| StrengthPage | Плохо | 500px, 400px, 174/474px |

---

## Эталонные размеры (Design Tokens)

### Высота графиков

| Контекст | Эталон | Мобильный (< 768px) | Правило |
|----------|--------|---------------------|---------|
| Основной график (SimpleChart) | 450px | 300px (max 350) | `h-[450px] md:h-[450px] sm:h-[300px]` или CSS var |
| Гистограмма (Funds flows) | 450px | 300px | aspect-ratio 16/9 предпочтительнее фиксированной высоты |
| Мини-график (навигатор) | 52px | 40px | Проп `height` |
| Сезонность (histogram) | 450px | 300px | |
| Сезонность (price chart) | 420px | 280px | |

### Отступы (padding)

| Контекст | Эталон | Правило |
|----------|--------|---------|
| Контейнер страницы | `px-4 md:px-6 py-6 md:py-8` | Стандарт для всех страниц |
| График: Y-ось справа | 80px (desktop), 50px (mobile) | |
| График: Y-ось слева | 60px (desktop), 40px (mobile) | |
| SVG gap между rect | 2px | Фиксированный |

### Шрифты

| Контекст | Эталон | Правило |
|----------|--------|---------|
| Заголовок страницы | `text-2xl font-bold` | Tailwind standard |
| Подзаголовок | `text-sm text-theme-secondary` | |
| Ось Y (числа) | 14px semibold | SVG `fontSize` |
| Ось X (даты) | 13px semibold | SVG/HTML |
| Тултип: заголовок | `text-[11px] text-theme-secondary` | |
| Тултип: значение | `text-xs font-semibold` (12px) | |
| Кнопки управления | `text-sm font-medium` (14px) | |
| Легенда графика | `text-sm font-medium` (14px) | |

### Детальная анатомия SimpleChart (внутри контейнера графика)

```
┌─────────────────────────────────────────────────────────┐
│ Легенда (legendPosition="top")                          │  ← mb-2, text-sm
│   ● Цена    ● Открытый интерес                          │     кружки w-3 h-3, gap-5
├─────────────────────────────────────────────────────────┤
│ Плавающая дата (при hover)                              │  ← top: 38px от верха контейнера
│   [11 дек. 2025 г.]                                     │     text-[11px], bg-theme-tertiary/90
├─────────────────────────────────────────────────────────┤
│        │                                           │    │
│        │  padding.top = 19px                       │    │
│        │                                           │    │
│  Y-ось │  ┌─── Область графика (SVG) ───────┐     │Y-ось│
│  левая │  │                                  │     │прав.│
│  100px │  │  Линии, сетка, данные            │     │95px │
│        │  │                                  │     │     │
│ #9CA3B8│  │  chartWidth = width - 100 - 95   │     │sec. │
│ 16px   │  │  chartHeight = 450 - 19 - 50     │     │color│
│ 600wt  │  │  = 381px                         │     │16px │
│        │  │                                  │     │600wt│
│  x=-12 │  └──────────────────────────────────┘     │x=+12│
│        │                                           │    │
│        │  padding.bottom = 50px                    │    │
│        │                                           │    │
│        │  X-ось: даты                              │    │
│        │  #9CA3B8, 14px, 600wt                     │    │
│        │  y = chartHeight + 30                     │    │
│        │  1-я: textAnchor="start"                  │    │
│        │  последняя: textAnchor="end"              │    │
│        │  остальные: textAnchor="middle"           │    │
├─────────────────────────────────────────────────────────┤
│ Аннотации (кружки экспираций/событий)                   │
│   top = padding.top + chartHeight - 14                  │
│   w-7 h-7 rounded-full                                  │
│   bg: #3a3f4f, text: #9CA3B8, 11px, 600wt               │
│   opacity: 0.5 → 1.0 on hover                           │
│   тултип: text-[13px] font-semibold, py-2.5 px-4        │
├─────────────────────────────────────────────────────────┤
│ Навигатор (ChartNavigator)                              │
│   mt-3 (12px), height = 52px + 4px padding = 56px       │
│   Ручки: HANDLE_W = 14px, height * 0.7 = 36px          │
│   Стрелки: SVG path (шевроны), stroke #fff, 2px         │
│   Окно: fill rgba(56,98,251,0.08), stroke 0.45          │
│   Маска: rgba(0,0,0,0.5)                                │
├─────────────────────────────────────────────────────────┤
│ Легенда (legendPosition="bottom") — если внизу          │
│   mt-4, flex justify-center                             │
└─────────────────────────────────────────────────────────┘
```

**Padding (Desktop / Mobile):**

| Параметр | Desktop | Mobile (< 768px) |
|----------|---------|------------------|
| padding.top | 19px | 16px |
| padding.bottom | 50px | 40px |
| padding.left | 100px (с Y-осью) | 45px |
| padding.right | 95px (с вторичной Y) или 12px (без) | 20px |

**Сетка (grid lines):**

| Элемент | Цвет | Толщина |
|---------|------|---------|
| Горизонтальные линии Y | rgba(255,255,255,0.08) | 1px |
| Вертикальные линии X | rgba(255,255,255,0.08) | 1px |
| Hover crosshair (вертикальная) | primaryColor | 1px dashed (4,3) opacity 0.5 |
| Hover crosshair (горизонтальная) | #9CA3B8 | 1px dashed (4,3) opacity 0.3 |

**Оси (axis labels):**

| Ось | Шрифт | Цвет | Позиция |
|-----|-------|------|---------|
| Y левая (primary) | 16px, weight 600 | #9CA3B8 | x = -12 от left edge |
| Y правая (secondary) | 16px, weight 600, opacity 0.9 | secondaryColor | x = chartWidth + 12 |
| X (даты) | 14px, weight 600 | #9CA3B8 | y = chartHeight + 30 |

**Линии графика:**

| Линия | Толщина | Стиль |
|-------|---------|-------|
| Primary | 2.5px | solid, round caps/joins |
| Secondary | 2px | solid, round caps/joins |
| Third | 2px | solid, round caps/joins |
| Forecast (dashed) | 2px | dasharray 8,6, dashoffset animation |

**Hover элементы:**

| Элемент | Размер | Стиль |
|---------|--------|-------|
| Точка на primary линии | r=5 | fill=primaryColor, без stroke |
| Точка на secondary линии | r=4 | fill=secondaryColor, без stroke |
| Тултип значения | min-w-[140px] | bg-theme-tertiary/95, backdrop-blur, py-1.5 px-3 |
| Тултип: label | text-[11px] | text-theme-secondary, truncate |
| Тултип: value | text-xs (12px) | font-semibold, ml-auto |

### Тултипы

| Правило | Значение |
|---------|----------|
| Позиционирование | `absolute` внутри контейнера графика |
| Z-index | 30 (обычный), 50 (модалки) |
| Верхняя граница | `Math.max(y, 4)` — не выходит за верх |
| Боковая граница | Переключение left/right при > 50% ширины |
| Стиль | `bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme` |

### Кнопки управления

| Правило | Значение |
|---------|----------|
| Контейнер группы | `flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1` |
| Кнопка | `px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200` |
| Активная | `btn-control active` |
| Неактивная | `text-theme-secondary hover:text-theme-primary` |
| Toggle (вкл) | `bg-[color]/20 text-[color] ring-1 ring-[color]/50` |

---

### Кастомные графики (не SimpleChart)

#### SeasonalityPage — Histogram

```
Контейнер: height: 450px, cursor-crosshair
SVG viewBox: 0 0 1000 500 (preserveAspectRatio="none")
Midline Y: 250 (50%)
Bar range: halfH = 500 * 0.38 = 190px (от центра вверх и вниз)
Bar width: slotW = 1000 / bars.length
  > 12 bars: barW = slotW * 0.6
  ≤ 12 bars: barW = slotW * 0.5
Цвета: green #2EE59D, red #FF4D4D
Y подписи справа: text-[16px] font-semibold #9CA3B8, right: 4px
X подписи: text-[14px] font-semibold #9CA3B8, absolute bottom, px-2
```

#### SeasonalityPage — Price Chart

```
Контейнер: height: 420px
SVG область: left: 60px, right: 80px, top: 10px, bottom: 60px
SVG viewBox: 0 0 1000 500 (preserveAspectRatio="none")
Линия цены: stroke #C8FF2E, 2px
Линия adjusted: stroke #22c55e, 2px, dasharray 6,3
Y подписи: text-[14px] font-semibold #9CA3B8, right: 4px
X подписи: text-[13px] font-semibold #9CA3B8, bottom: 4px
Кружки дивидендов: w-7 h-7, bg #3a3f4f, #9CA3B8, 11px/600
Плавающая дата: height: 22px, text-[11px], между легендой и графиком
```

#### StrengthPage — Price Chart (верхний)

```
Контейнер: min-h-[500px], chart внутри ~400px
Padding: { top: 20, bottom: 50, left: 100, right: padding для 2-й оси }
Y подписи: 16px, #9CA3B8
Crosshair circle: r=5, fill без stroke
```

#### StrengthPage — Breadth Chart (нижний)

```
Контейнер: minHeight 174px (с ценой) или 474px (без)
Reference levels: 10%, 50%, 90% горизонтальные пунктиры
```

#### FundsMoneyPage — Flows Histogram

```
Контейнер: aspectRatio 16/9, cursor-crosshair
SVG: width 100%, height 100%
Bar area: right offset 60px (для Y подписей)
Y подписи: text-[13px] font-medium #9CA3B8, right: 4px
X подписи: text-[12px] #9CA3B8, bottom: 8px
Кружки событий: w-7 h-7, bg #3a3f4f, 11px/600, opacity 0.5→1.0
Навигатор: height 56px, HANDLE_W 14px
```

#### HeatmapPage — Treemap

```
Контейнер: height = max(600, window.innerHeight - 180)
SVG: dynamic viewBox = container dimensions
Sector header: 18px height, bg-secondary
Gap между rect: 2px
Border radius: 6px
Тикер шрифт: Math.min(width/4.5, height*0.35, 48), weight 800, белый
Процент шрифт: 70% от тикера, weight 700
Min для тикера: rect > 18x14
Min для процента: rect > 30x25
Тултип: py-3 px-5, text-[15px]/text-[13px], font-semibold, rounded-xl
```

## План адаптации (приоритеты)

### Фаза 1: Design Tokens (CSS Variables)

Создать CSS переменные для ключевых размеров:

```css
:root {
  --chart-height: 450px;
  --chart-height-mobile: 300px;
  --chart-padding-left: 60px;
  --chart-padding-right: 80px;
  --chart-padding-left-mobile: 40px;
  --chart-padding-right-mobile: 50px;
  --nav-height: 52px;
  --font-axis: 14px;
  --font-tooltip: 11px;
}

@media (max-width: 768px) {
  :root {
    --chart-height: 300px;
    --chart-padding-left: 40px;
    --chart-padding-right: 50px;
    --nav-height: 40px;
    --font-axis: 12px;
  }
}
```

### Фаза 2: SimpleChart (ключевой компонент)

SimpleChart используется на 5 страницах — исправление здесь даёт максимальный эффект:
- Заменить `height = 450` на CSS variable
- Адаптивные padding (left/right) через breakpoints
- SVG font-size через CSS переменные
- Тултипы: проверка границ viewport

### Фаза 3: Страницы с кастомными SVG

- **SeasonalityPage**: заменить viewBox 1000x500 на динамический
- **StrengthPage**: заменить hardcoded heights на CSS variables  
- **FundsMoneyPage**: заменить 60px offsets на CSS variables

### Фаза 4: Тестирование

- Chrome (Mac, Windows)
- Edge (Windows)
- Yandex Browser (Windows)
- Safari (Mac)
- Mobile Safari / Chrome (iOS / Android)
- Разрешения: 1366x768, 1920x1080, 2560x1440, 375x812 (iPhone)

---

## Breakpoints (Tailwind)

| Prefix | Width | Типичные устройства |
|--------|-------|---------------------|
| (none) | < 640px | Мобильные |
| `sm:` | >= 640px | Большие мобильные, маленькие планшеты |
| `md:` | >= 768px | Планшеты |
| `lg:` | >= 1024px | Ноутбуки |
| `xl:` | >= 1280px | Десктоп |
| `2xl:` | >= 1536px | Большие мониторы |
