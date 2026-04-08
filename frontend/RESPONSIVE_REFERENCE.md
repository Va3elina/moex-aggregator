# Responsive Design Reference — Фрейм (таймфрейм.рф)

> Эталонный документ. Все размеры — референс. При адаптации сайт должен
> подстраиваться под экран, сохраняя пропорции и эти значения как базовые.
> Разработка ведётся на Mac Chrome Retina ~2560x1440.

---

## 1. SimpleChart (используется на 5 страницах)

### 1.1 Контейнер

| Параметр | Desktop | Mobile (<768px) | Источник |
|----------|---------|-----------------|----------|
| Высота | 450px (проп) | max 350px | SimpleChart:277 |
| Ширина | 100% контейнера | 100% | ResizeObserver |
| Фон | var(--bg-secondary) | то же | :823 |
| Border | 1px solid var(--border) | то же | |
| Border radius | 16px (rounded-2xl) | то же | :756 |
| Padding контейнера | 20px (p-5) | то же | :756 |

### 1.2 Padding SVG-области (от контейнера до осей)

| Сторона | Desktop | Mobile | Условие | Источник |
|---------|---------|--------|---------|----------|
| top | 19px | 16px | — | :266, :264 |
| bottom | 50px | 40px | — | :268, :264 |
| left | 100px | 45px | — | :269, :264 |
| right | 95px | 20px | showSecondary=true | :267, :264 |
| right | 12px | 20px | showSecondary=false | :267 |

### 1.3 Y-ось левая (primary)

| Параметр | Значение | Источник |
|----------|----------|----------|
| fontSize | 16px | :859 |
| fontWeight | 600 | :860 |
| fill | #9CA3B8 | :858 |
| x-позиция | -12px от левого края SVG | :854 |
| textAnchor | end | :856 |
| dominantBaseline | middle | :857 |
| Количество тиков | 5 | :356 |

### 1.4 Y-ось правая (secondary)

| Параметр | Значение | Источник |
|----------|----------|----------|
| fontSize | 16px | :876 |
| fontWeight | 600 | :877 |
| fill | secondaryColor | :875 |
| opacity | 0.9 | :878 |
| x-позиция | chartWidth + 12px | :871 |
| textAnchor | start | :873 |
| Скрыта на мобиле | да | :868 |

### 1.5 X-ось (даты)

| Параметр | Значение | Источник |
|----------|----------|----------|
| fontSize | 14px | :905 |
| fontWeight | 600 | :906 |
| fill | #9CA3B8 | :904 |
| y-позиция | chartHeight + 30px | :902 |
| textAnchor первая | start | :903 |
| textAnchor последняя | end | :903 |
| textAnchor остальные | middle | :903 |
| Количество тиков | min(7, data.length) | :371 |

### 1.6 Сетка

| Элемент | stroke | strokeWidth | dasharray | opacity | Источник |
|---------|--------|-------------|-----------|---------|----------|
| Горизонтальные Y | rgba(255,255,255,0.08) | 1px | — | 1 | :850-851 |
| Вертикальные X | rgba(255,255,255,0.08) | 1px | — | 1 | :896-897 |
| Первая/последняя X | не рисуются | — | — | — | :890 |

### 1.7 Линии данных

| Линия | strokeWidth | strokeLinecap | strokeLinejoin | Источник |
|-------|-------------|---------------|----------------|----------|
| Primary | 3px | round | round | :935-937 |
| Secondary | 2.5px | round | round | :1024-1026 |
| Third | 2.5px | round | round | :1059 |
| Forecast (dashed) | 2.5px | — | — | :964 |
| Forecast dasharray | 6 4 | — | — | :965 |
| Forecast opacity | 0.7 | — | — | :965 |

### 1.8 Hover crosshair

| Параметр | Значение | Источник |
|----------|----------|----------|
| Вертикальная линия stroke | #C8FF2E | :1089 |
| strokeWidth | 1px | :1090 |
| strokeDasharray | 4,4 | :1091 |
| opacity | 0.5 | :1092 |
| Затемнение справа fill | #0B0D12 | :1074 |
| Затемнение opacity | 0.5 | :1075 |

### 1.9 Hover точки (dots)

| Точка | radius | fill | stroke | strokeWidth | Источник |
|-------|--------|------|--------|-------------|----------|
| Primary | 6px | primaryColor | #0B0D12 | 2px | :1098-1101 |
| Secondary | 5px | secondaryColor | #0B0D12 | 2px | :1107-1112 |
| Third | 5px | thirdColor | #0B0D12 | 2px | :1119-1123 |

### 1.10 Тултип (карточка значений)

| Параметр | Desktop | Mobile | Источник |
|----------|---------|--------|----------|
| Ширина | 200px | 150px | :1132 |
| Позиция left (правая половина) | tooltip.x - cardWidth - 8 | то же | :1134 |
| Позиция left (левая половина) | tooltip.x + 8 | то же | :1136 |
| Позиция top min | padding.top | то же | :1160 |
| Позиция top max | padding.top + chartHeight - cardHeight | то же | :1160 |
| Фон | bg-theme-tertiary/95 backdrop-blur-sm | то же | :1164 |
| Border | 1px solid var(--border) | то же | :1164 |
| Border radius | 8px (rounded-lg) | то же | :1164 |
| Padding | py-1.5 px-3 (6px/12px) | то же | :1164 |
| Shadow | shadow-xl | то же | :1164 |
| Высота строки | 26px | то же | :1151 |
| Высота карточки | 16 + (lines × 26)px | то же | :1151 |
| Label fontSize | 11px | то же | :1169 |
| Label цвет | text-theme-secondary | то же | :1169 |
| Value fontSize | 12px (text-xs) | то же | :1171 |
| Value fontWeight | semibold | то же | :1171 |
| Dot | w-2 h-2 (8px) | то же | :1168 |

### 1.11 Плавающая дата

| Параметр | Значение | Источник |
|----------|----------|----------|
| top (legend сверху) | 38px от верха контейнера | :1259 |
| top (legend снизу) | 8px от верха контейнера | :1259 |
| left | tooltip.x + 20 | :1259 |
| transform | translateX(-50%) | :1261 |
| fontSize | 11px | :1264 |
| color | text-theme-secondary | :1264 |
| background | bg-theme-tertiary/90 backdrop-blur-sm | :1264 |
| padding | px-2 py-0.5 (8px/2px) | :1264 |
| border-radius | 4px (rounded) | :1264 |
| border | 1px solid var(--border) | :1264 |
| Расстояние от верхнего края SVG | 38 - 19 = 19px | top - padding.top |
| Расстояние от кружка аннотации | ~348px | annotation.top - date.top |

### 1.12 Аннотации (кружки экспираций/событий)

| Параметр | Значение | Источник |
|----------|----------|----------|
| Позиция top | padding.top + chartHeight - 14 | :1211 |
| transform | translate(-50%, 0) | :1210 |
| Размер кружка | w-7 h-7 (28px) | :1216 |
| Border radius | full (50%) | :1216 |
| Background | #3a3f4f | :1217 |
| Text color | #9CA3B8 | :1217 |
| fontSize | 11px | :1217 |
| fontWeight | 600 | :1217 |
| opacity нормальная | 0.5 | :1216 |
| opacity hover | 1.0 | :1216 |
| Тултип padding | py-2.5 px-4 (10px/16px) | :1223 |
| Тултип border-radius | 12px (rounded-xl) | :1223 |
| Тултип fontSize | 13px | :1224 |
| Тултип fontWeight | 600 (semibold) | :1224 |

### 1.13 Плавающая дата аннотации

| Параметр | Значение | Источник |
|----------|----------|----------|
| top | 38px (legend top) / 8px (legend bottom) | :1282 |
| left | x + 20 | :1282 |
| Стиль | идентичен обычной плавающей дате (1.11) | :1289 |

### 1.14 Легенда

| Параметр | Значение | Источник |
|----------|----------|----------|
| Gap между элементами | 5 (20px) | :735 |
| fontSize | sm (14px) | :735 |
| Dot | w-3 h-3 (12px), rounded-full | :737 |
| Label fontWeight | medium | :738 |
| Позиция top | mb-2 от графика | :795 |
| Позиция bottom | mt-4 от графика | :1278 |

### 1.15 Навигатор (ChartNavigator)

| Параметр | Значение | Источник |
|----------|----------|----------|
| margin-top | 12px (mt-3) | :180 |
| Высота SVG | 52px (проп default) | :22 |
| Высота контейнера | 56px (52 + 4px padding) | :180 |
| Ручка ширина | 14px (HANDLE_W) | :15 |
| Ручка высота | height × 0.7 = 36px | :223 |
| Ручка border-radius | 3px | :225 |
| Ручка fill | rgba(56,98,251,0.9) | :225 |
| Стрелки | SVG path, stroke #fff, 2px | :229 |
| Окно fill | rgba(56,98,251,0.08) | :209 |
| Окно stroke | rgba(56,98,251,0.45) | :210 |
| Маска (неактивная часть) | rgba(0,0,0,0.5) | :200 |
| Мини-график gradient top | color, opacity 0.35 | :184 |
| Мини-график gradient bottom | color, opacity 0.03 | :186 |

### 1.16 Кнопки управления (Download, Histogram toggle)

| Параметр | Значение | Источник |
|----------|----------|----------|
| Размер | w-9 h-9 (36px) | :776, :761 |
| Background | bg-theme-tertiary/90 backdrop-blur-sm | :776 |
| Border | 1px solid var(--border) | :776 |
| Border radius | 8px (rounded-lg) | :776 |
| Icon size | 18px | :779 |

### 1.17 Value Header

| Параметр | Значение | Источник |
|----------|----------|----------|
| fontSize | 36px (text-4xl) | :802 |
| fontWeight | bold | :802 |
| Change badge fontSize | 16px (text-base) | :805 |
| Change badge fontWeight | medium | :805 |
| Change badge padding | px-2 py-0.5 (8px/2px) | :805 |

---

## 2. SeasonalityPage — Histogram

| Параметр | Значение | Источник |
|----------|----------|----------|
| Контейнер height | 450px | :406 |
| SVG viewBox | 0 0 1000 500 | :421 |
| preserveAspectRatio | none | :421 |
| Midline Y | 250 (50%) | :430 |
| Bar halfH | 500 × 0.38 = 190 | :430 |
| Bar width (>12 bars) | slotW × 0.6 | :427 |
| Bar width (≤12 bars) | slotW × 0.5 | :427 |
| Bar цвет green | #2EE59D | :438 |
| Bar цвет red | #FF4D4D | :441 |
| Bar border-radius | rx=3 | :438 |
| Bar min height | 500 × 0.005 = 2.5 | :432 |
| Y подписи right | 4px от правого края | :513 |
| Y подписи fontSize | 16px | :515 |
| Y подписи fontWeight | semibold | :515 |
| Y подписи color | #9CA3B8 | :515 |
| X подписи | text-[14px] font-semibold #9CA3B8 | :521 |
| X подписи position | absolute bottom, right: 80px | :521 |
| Hover opacity неактивных | 0.35 | :435 |
| Hover transition | duration-150 | :436 |
| Crosshair color | #C8FF2E | :468 |
| Crosshair dasharray | 4 3 | :468 |
| Crosshair opacity | 0.5 | :469 |
| Анимация первая загрузка | 1200ms с stagger 600ms | :167-168 |
| Анимация обновление | 600ms без stagger | :167-168 |
| Easing | easeOutCubic | :147 |

## 3. SeasonalityPage — Price Chart

| Параметр | Значение | Источник |
|----------|----------|----------|
| Контейнер height | 420px | :585 |
| SVG left | 60px (PL) | :532 |
| SVG right | 80px (PR) | :532 |
| SVG top | 10px (PT) | :532 |
| SVG bottom | 60px (PB) | :532 |
| SVG viewBox | 0 0 1000 500 | :606 |
| Линия цены stroke | #C8FF2E | :612 |
| Линия цены width | 2px | :612 |
| Линия adjusted stroke | #22c55e | :618 |
| Линия adjusted dasharray | 6,3 | :619 |
| Y подписи fontSize | 14px | :643 |
| X подписи fontSize | 13px | :648 |
| X подписи bottom | 4px | :648 |
| Плавающая дата height | 22px | :574 |
| Плавающая дата left | tooltip.x | :576 |
| Дивиденды top | PT + chartAreaH - 14 | :667 |
| Дивиденды кружок | w-7 h-7, #3a3f4f, 11px/600 | :669 |
| Дивиденды пунктир height | chartAreaH - 28 | :672 |
| Дивиденды пунктир border | 1px dashed rgba(156,163,184,0.4) | :673 |
| Навигатор color | #C8FF2E | :688 |
| Тултип карточка left (правая) | tooltip.x - 150 | :695 |
| Тултип карточка left (левая) | tooltip.x + 12 | :695 |

## 4. StrengthPage

| Параметр | Значение | Источник |
|----------|----------|----------|
| Chart padding | { left: 0, right: 70, top: 10, bottom: 30 } | :50 |
| Контейнер верх min-h | 500px | :372 |
| Loading placeholder h | 400px | :378 |
| Breadth minHeight (с ценой) | 174px | :494 |
| Breadth minHeight (без цены) | 474px | :494 |
| Hover circle radius | 5px | :803 |
| Hover circle fill | #6366f1 (верхний) / var(--accent) (нижний) | :803, :1181 |
| Hover circle stroke | нет (убрана) | |
| Tooltip card height | 60px (с ценой) / 34px (без) | :422-423 |
| Reference levels | 10%, 50%, 90% | breadth chart |
| Reference stroke | 1px dashed | |

## 5. FundsMoneyPage — Flows Histogram

| Параметр | Значение | Источник |
|----------|----------|----------|
| Контейнер | aspectRatio 16/9 | :585 |
| SVG right offset | 60px | :590 |
| Y подписи fontSize | 13px | |
| Y подписи fontWeight | medium | |
| Y подписи color | #9CA3B8 | |
| Y подписи right | 4px | |
| X подписи fontSize | 12px | |
| X подписи color | #9CA3B8 | |
| X подписи bottom | 8px | |
| Crosshair stroke | #C8FF2E | :663 |
| Crosshair dasharray | 4 3 | :665 |
| Crosshair opacity | 0.5 | :666 |
| Аннотации height reserve | 28px | :771 |
| Аннотации marginTop | -34px | :771 |
| Навигатор height | 56px | :875 |
| Навигатор handle width | 14px | |
| Тултип label fontSize | 11px | :706 |
| Тултип value fontSize | 12px (text-xs) | :708 |
| Тултип positioning left | hoverX - 188 (right) / hoverX + 8 (left) | :282 |
| Тултип top clamp | min(max(y-20, 4), 330) | :284 |

## 6. HeatmapPage — Treemap

| Параметр | Значение | Источник |
|----------|----------|----------|
| Container height | max(600, window.innerHeight - 180) | :153 |
| Default size | 1200 × 700 | :129 |
| Gap между rect | 2px | :324 |
| Rect border-radius | 6px | :325 |
| Rect transition | fill 0.6s, x/y/width/height 0.5s | :350 |
| Ticker fontSize | Math.min(w/4.5, h×0.35, 48) | :242 |
| Ticker fontWeight | 800 | :361 |
| Ticker shadow | 0 2px 8px rgba(0,0,0,0.8) | :363 |
| Ticker letterSpacing | -0.02em | :364 |
| Percent fontSize | 70% от ticker | :245 |
| Percent fontWeight | 700 | :379 |
| Min для тикера | rect > 18×14 | :322 |
| Min для процента | rect > 30×25 | :323 |
| Sector header height | 18px | :284 |
| Sector header fontSize | 11px | :496 |
| Sector header fontWeight | 500 | :496 |
| Sector header color | rgba(255,255,255,0.7) | :497 |
| Тултип border-radius | 12px (rounded-xl) | :516 |
| Тултип padding | py-3 px-5 (12px/20px) | :516 |
| Тултип ticker fontSize | 15px | :524 |
| Тултип name fontSize | 13px | :525 |
| Тултип price fontSize | 13px | :526 |
| Тултип changes fontSize | 13px | :528 |
| Тултип position min left | 180px | :518 |
| Тултип flip threshold | y < 200 | :519 |
| Тултип offset below | +25px | :519 |
| Тултип offset above | -10px | :519 |

## 7. Пропсы SimpleChart по страницам

| Страница | height | primaryColor | secondaryColor | showSecondary | navigator | legendPos |
|----------|--------|-------------|----------------|---------------|-----------|-----------|
| Buffett (cap-gdp) | 450 | #C8FF2E | #f59e0b | true | true | top |
| Buffett (mcftr-m2) | 450 | #C8FF2E | #f59e0b | true | true | top |
| Buffett (cap-m2) | 450 | #C8FF2E | #f59e0b | true | true | top |
| FearIndex | 400 | #6366f1 | #C8FF2E | true | true | top |
| OpenInterest | 450 | #6366f1 | varies | true | true | top |
| FundsMoneyPage (СЧА) | 450 | #6366f1 | INDEX_COLOR | true | true | top |
| OverviewPage | varies | #6366f1 | — | false | false | — |

---

## 8. Цветовая палитра

| Назначение | Цвет | Где используется |
|------------|------|------------------|
| Primary accent | #C8FF2E | Crosshair, Buffett primary, акценты |
| Default chart | #6366f1 | OI, Funds, Fear primary |
| Secondary amber | #f59e0b | Buffett secondary, OI |
| Success green | #2EE59D | Long, приток, seasonality green |
| Error red | #FF4D4D | Short, отток, seasonality red |
| Adjusted green | #22c55e | Seasonality adjusted price |
| Cyan | #22D3EE | Чистая позиция OI |
| Purple | #A855F7 | Покупки+Продажи OI |
| Dark background | #0B0D12 | Chart bg, hover dot stroke |
| Grid | rgba(255,255,255,0.08) | Все графики |
| Axis text | #9CA3B8 | Все оси, тултипы |
| Annotation circle | #3a3f4f | Экспирации, события |

---

## 9. Кнопки управления (общий стандарт)

| Элемент | Стиль |
|---------|-------|
| Контейнер группы | `flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1` |
| Кнопка | `px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200` |
| Активная | `btn-control active` |
| Неактивная | `text-theme-secondary hover:text-theme-primary` |
| Toggle вкл | `bg-[color]/20 text-[color] ring-1 ring-[color]/50` |
| Toggle выкл | `bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary` |

---

## 10. Breakpoints (Tailwind)

| Prefix | Width | Устройства |
|--------|-------|------------|
| (none) | < 640px | Мобильные |
| `sm:` | >= 640px | Большие мобильные |
| `md:` | >= 768px | Планшеты |
| `lg:` | >= 1024px | Ноутбуки |
| `xl:` | >= 1280px | Десктоп |
| `2xl:` | >= 1536px | Большие мониторы |

---

## 11. План адаптации (4 фазы)

### Фаза 1: CSS Variables
Перенести все hardcoded значения в CSS переменные с @media breakpoints.

### Фаза 2: SimpleChart responsive
Ключевой компонент — исправление здесь даёт эффект на 5 страницах.

### Фаза 3: Кастомные SVG
SeasonalityPage, StrengthPage, FundsMoneyPage — переход на CSS variables.

### Фаза 4: Тестирование
Chrome, Edge, Yandex, Safari × 4 разрешения (375px, 768px, 1366px, 2560px).
