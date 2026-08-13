/**
 * record-indicator.mjs — записывает видео-демо индикатора для лендинга.
 *
 * Использует Playwright для:
 *   1. Открытия страницы индикатора (на проде или локально)
 *   2. Inject'а SVG-курсора (визуальный overlay через CSS background-image)
 *   3. Сценарного движения курсора + click'ов
 *   4. Записи всей сессии в .webm
 *
 * Затем ffmpeg:
 *   - Сжимает .webm (VP9, target 200-400KB)
 *   - Конвертирует в .mp4 (H.264) для Safari fallback
 *   - Извлекает poster (1й кадр)
 *
 * Использование:
 *   RECORD_TEST_EMAIL=... RECORD_TEST_PASSWORD=... node record-indicator.mjs seasonality
 *   RECORD_TEST_EMAIL=... RECORD_TEST_PASSWORD=... node record-indicator.mjs all
 *
 * RECORD_TEST_EMAIL/RECORD_TEST_PASSWORD — тестовый Pro-аккаунт для записи БЕЗ
 * tier-гейтов (без него пишет как гость — сценарии могут упереться в апсейл-попапы).
 *
 * Output: ../../frontend/public/videos/<name>.{webm,mp4,-poster.jpg}
 */
import { chromium } from 'playwright';
import { execFileSync } from 'node:child_process';
import { mkdirSync, existsSync, rmSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_DIR = join(__dirname, '../../frontend/public/videos');
const TEMP_DIR = join(__dirname, '.tmp-recordings');

// ═══════════════════════════════════════════════════════════
// Конфиг
// ═══════════════════════════════════════════════════════════
const BASE_URL = process.env.RECORD_BASE_URL || 'https://xn--80aklbnczmv.xn--p1ai';
// Тестовый Pro-аккаунт — снимаем БЕЗ tier-гейтов (гость упирается в апсейл-попапы
// на части сценариев, см. buffett ниже). Креды — только через env, не хардкодить
// (секреты в репо запрещены, см. .claude/skills/moex-billing/SKILL.md).
const TEST_EMAIL = process.env.RECORD_TEST_EMAIL;
const TEST_PASSWORD = process.env.RECORD_TEST_PASSWORD;
// LOGICAL viewport — координаты сценариев пишутся в этом размере (1280×800).
// PHYSICAL viewport — реальный размер которым работает Playwright, в 2× больше
// для hi-res capture. Хелперы moveCursor/clickAt домножают input на COORD_SCALE.
//
// Почему 2× viewport, а не deviceScaleFactor=2:
// Playwright recordVideo capture'ит в CSS-пикселях viewport (proven empirically).
// DPR=2 даёт sharp render на screen, но в видео он выбрасывается. Единственный
// способ получить 2× видео — буквально удвоить viewport.
// Viewport 1280×900 — выше стандартных 800 чтобы вместить chart + navigator
// для тяжёлых индикаторов (OI: chart 500 + nav 52 + toolbars + paddings ≈ 850px).
// На 800-viewport navigator ушёл бы за нижний край → приходилось ужимать chart
// через onAfterLoad-хак, который ломал layout. 900 решает проблему естественно.
// Viewport 1280×800 — gives generous vertical buffer.
// Strength's actual rendered content (with our CSS-vars override) ends up
// taller than expected, navigator handle was at y=715 in viewport 720 —
// 5px from edge, visually клипалось. 800 даёт 80px дополнительного
// пространства снизу, плюс с PADDING_TOP=40 сверху общий буфер ~120px.
// Aspect 1280:800 = 16:10 — IndicatorGroup tile aspect обновлён.
const LOGICAL_VIEWPORT = { width: 1280, height: 800 };
const COORD_SCALE = 1;
const VIEWPORT = LOGICAL_VIEWPORT;
const FINAL_WIDTH = 1280;
const FINAL_HEIGHT = 800;
const VIDEO_FPS = 30;
// CRF — оптимизировано под размер для лендинга (карточки 820×513 css-px).
// CRF 28 = web-standard (YouTube/Vimeo дефолт). На маленьких тайлах
// разница с CRF 8 невидна, а файлы меньше в 5-10×: 1.5 MB → 250 KB.
// Это критично для скорости загрузки лендинга — 7 видео × 250 KB = 1.7 MB total
// вместо 11 MB. HTTP/2 connection limit перестаёт быть bottleneck.
const VP9_CRF = 30;
const H264_CRF = 26;
const VP9_CPU_USED = 0;  // 0=best/slowest. Offline encode — берём максимум.
// Обрезаем стартовые секунды — там видна шапка сайта (до hideSiteChrome).
// Подобрано эмпирически под последовательность: navigate(1.5s) + waitForSelector + chartReveal(0.8s) + hideChrome(0.1s).
const TRIM_START_SEC = 3.5;

// SVG-курсор как data URL — используется как CSS background-image, без innerHTML.
// Простой arrow-cursor: белая стрелка с чёрной обводкой.
const CURSOR_SVG_DATA = 'data:image/svg+xml;utf8,' + encodeURIComponent(
  '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24">' +
  '<path d="M5 3L19 12L12 13L8.5 20L5 3Z" fill="white" stroke="black" stroke-width="1.5" stroke-linejoin="round"/>' +
  '</svg>'
);

/**
 * Скрывает глобальный nav (sticky top header сайта) и возможно другие
 * декорации — даём максимум места под chart card в видео.
 *
 * Селектор `nav.sticky` достаточно специфичен: в Layout.tsx это
 * <nav className="sticky top-0 z-50 border-b"> — единственный sticky nav.
 */
async function hideSiteChrome(page, cssExtra = '') {
  // Стратегия для edge-to-edge видео:
  //   1. Удаляем nav (sticky header сайта)
  //   2. Скрываем ВСЁ что ВЫШЕ chart-card (.editorial-frame): PageHeader,
  //      eyebrow, subtitle — через previousElementSibling chain
  //   3. Скрываем всё что НИЖЕ chart-card: SectorDetail, описания, stat-cards
  //   4. Cвязка с addInitScript (spoof innerHeight=760 → useFitToViewport
  //      отдаёт высоту графика ближе к max, но с запасом под навигатор)
  //      гарантирует что chart card заполняет кадр и помещается в viewport
  //      без crop'а навигатора.
  await page.evaluate((extraCss) => {
    document.querySelectorAll('nav').forEach(el => el.remove());
    document.querySelectorAll('header.sticky, .site-header').forEach(el => el.remove());
    // Footer — sibling <main>, а НЕ sibling .editorial-frame/.tabbed-card
    // (см. Layout.tsx: <main><Outlet/></main> затем <footer> на уровне корня
    // Layout) — общий CSS-селектор `.editorial-frame ~ *` физически не может
    // его достать (разные родители). Раньше footer «случайно» не попадал в
    // кадр только у высоких карточек (Strength/Buffett); у коротких (OI,
    // Seasonality) — попадал. Убираем явно, независимо от геометрии.
    document.querySelectorAll('footer').forEach(el => el.remove());

    // Скрыть ВСЁ что выше карточки в её parent контейнере. На страницах с
    // вкладками (OI, funds-money — см. OpenInterestPage.tsx:734-752) вкладки
    // <ChartTabs> рендерятся ВНУТРИ `.tabbed-card` как previousElementSibling
    // самого `.editorial-frame` — если якорить на editorial-frame, вкладки тоже
    // попадут под скрытие и сценарий не сможет их кликнуть. Поэтому якорим на
    // `.tabbed-card`, когда он есть, и только на editorial-frame иначе.
    const frame = document.querySelector('.tabbed-card') || document.querySelector('.editorial-frame');
    if (frame) {
      let prev = frame.previousElementSibling;
      while (prev) {
        prev.style.display = 'none';
        prev = prev.previousElementSibling;
      }
    }

    const style = document.createElement('style');
    style.textContent = `
      ::-webkit-scrollbar { display: none !important; }
      body { margin: 0 !important; padding-top: 0 !important; }

      /* Скрываем всё что идёт ПОСЛЕ chart card (editorial-frame):
         - Strength: SectorDetail таблица "Детализация по акциям"
         - Buffett: stat-cards с описанием
         - OI/funds-money: блоки описания методологии */
      .editorial-frame ~ * { display: none !important; }

      ${extraCss}
    `;
    document.head.appendChild(style);
  }, cssExtra);
}

/** Scroll так чтобы chart card был на y=PADDING_TOP в viewport (не на y=0).
 *  Это даёт фиксированный padding сверху → frame не "обрезан" к верхнему
 *  краю кадра. Используем 40px — заметный отступ, но не съедает много места. */
async function scrollChartCardToTop(page) {
  await page.evaluate(() => {
    // .tabbed-card первым — иначе на страницах с вкладками (OI, funds-money)
    // scroll ставит editorial-frame к самому верху, а вкладки ChartTabs (выше
    // frame внутри tabbed-card) оказываются обрезаны за верхним краем кадра.
    const card =
      document.querySelector('.tabbed-card') ||
      document.querySelector('main .editorial-frame, .editorial-frame') ||
      document.querySelector('[class*="rounded-2xl"]:not(button)');
    if (!card) return;
    const PADDING_TOP = 40;
    const cardTopInDoc = card.getBoundingClientRect().top + window.scrollY;
    window.scrollTo(0, Math.max(0, cardTopInDoc - PADDING_TOP));
  });
}

/**
 * Измеряет bbox chart-card (.tabbed-card/.editorial-frame) в ЛОГИЧЕСКИХ
 * viewport-координатах (CSS px, 1280×800) — вызывается в самом конце, после
 * сценария, когда layout уже settled (drag navigator, открытые дропдауны и т.п.
 * могли слегка сдвинуть высоту).
 */
async function measureCardBBox(page) {
  return page.evaluate(() => {
    const card = document.querySelector('.tabbed-card') || document.querySelector('.editorial-frame');
    if (!card) return null;
    const r = card.getBoundingClientRect();
    return { top: r.top, bottom: r.bottom, height: r.height };
  });
}

/**
 * Считает ffmpeg crop-фильтр, который тесно обрамляет card ТОЛЬКО по вертикали
 * (сверху/снизу) — убирает лишний чёрный фон и заодно footer, если тот попал
 * в кадр из-за короткой карточки. Ширину НЕ трогаем: некоторые страницы
 * (напр. скринер сигналов на OI) — это таблица край-в-край без «воздуха» по
 * бокам, обрезка по ширине резала бы реальный контент (колонки), а не фон.
 *
 * Итоговый аспект видео поэтому у разных индикаторов будет разным (высота
 * card отличается: OI/Seasonality искусственно занижены под viewport-спуф и
 * CSS-клиппинг, Strength/Buffett — нет) — это ОК: плитка на лендинге
 * (IndicatorGroup: aspectRatio 16/10 + object-cover) сама аккуратно докропит
 * до 16:10 без искажений, это её штатная задача для любого исходного аспекта.
 */
function computeCropFilter(bbox, scaleFactor) {
  if (!bbox) return null;
  const PAD = 24; // логический px симметричного отступа сверху/снизу
  const topLogical = Math.max(0, bbox.top - PAD);
  const bottomLogical = Math.min(LOGICAL_VIEWPORT.height, bbox.bottom + PAD);
  const heightLogical = bottomLogical - topLogical;
  if (heightLogical < 40) return null; // bbox выглядит некорректным — не рискуем

  const w = Math.round(LOGICAL_VIEWPORT.width * scaleFactor);
  const h = Math.round(heightLogical * scaleFactor);
  const y = Math.round(topLogical * scaleFactor);
  return `crop=${w}:${h}:0:${y}`;
}

/** Универсальный CSS для скрытия description/list блоков после chart card.
 *  Применяется на 4 страницах (buffett, oi, funds-money — где есть descriptions
 *  снизу). НЕ применяется на funds-catalog/seasonality (там нет такого паттерна). */
const HIDE_DESCRIPTIONS = `
  /* Описания/методология ниже chart card.
     - .mt-6.bg-theme-secondary  ← OI, funds-money
     - div.mt-6 > div.bg-theme-secondary  ← buffett (grid из 2-х blocks с mt-6 на parent) */
  .mt-6.bg-theme-secondary { display: none !important; }
  div.mt-6.grid { display: none !important; }
  div.mt-6 > div.bg-theme-secondary { display: none !important; }
`;

/**
 * Drag-helper: эмулирует drag-and-drop (mouse down → move → up).
 * Координаты в logical (1280×800), scale применяется внутри moveCursor/clickAt.
 */
async function dragCursor(page, fromX, fromY, toX, toY, duration = 1000) {
  await moveCursor(page, fromX, fromY, 600);
  await page.waitForTimeout(150);
  await page.mouse.down();
  await page.waitForTimeout(100);
  await moveCursor(page, toX, toY, duration);
  await page.mouse.up();
  await page.waitForTimeout(300);
}

/**
 * Drag по PHYSICAL координатам (для bbox-based drag через page.mouse).
 * Параллельно перемещает визуальный курсор и реальный mouse.
 */
async function dragPhysical(page, fromX, fromY, toX, toY, duration = 1000) {
  // Перемещаем курсор к старту в physical координатах
  const moveTo = async (x, y, dur) => {
    const steps = Math.max(8, Math.round(dur / 33));
    const stepMs = dur / steps;
    const cur = await page.evaluate(() => {
      const m = window.__cursor.style.transform.match(/translate\(([\d.]+)px,\s*([\d.]+)px\)/);
      return m ? { x: parseFloat(m[1]), y: parseFloat(m[2]) } : { x: 50, y: 50 };
    });
    for (let i = 1; i <= steps; i++) {
      const t = i / steps;
      const eased = t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
      const cx = cur.x + (x - cur.x) * eased;
      const cy = cur.y + (y - cur.y) * eased;
      await page.evaluate(([cx, cy]) => {
        window.__cursor.style.transform = `translate(${cx}px, ${cy}px)`;
      }, [cx, cy]);
      await page.mouse.move(cx, cy);
      await page.waitForTimeout(stepMs);
    }
  };
  await moveTo(fromX, fromY, 600);
  await page.waitForTimeout(150);
  await page.mouse.down();
  await page.waitForTimeout(100);
  await moveTo(toX, toY, duration);
  await page.mouse.up();
  await page.waitForTimeout(300);
}

/**
 * Находит ЛЕВУЮ ручку (handle) ChartNavigator и тянет её к центру.
 * Видимый эффект: левый край видимого окна сдвигается → window сжимается.
 *
 * Handle определяется через inline-style `cursor: ew-resize` (см. ChartNavigator.tsx:241-271).
 * На странице только два таких элемента — handle-left и handle-right.
 *
 * @param toXRatio  конечная X-позиция handle по ширине navigator (0-1)
 */
async function dragNavigatorHandle(page, toXRatio = 0.45, duration = 1500) {
  const findBox = () => page.evaluate(() => {
    // Ручки имеют inline-style "cursor: ew-resize" — устойчивый признак
    const handles = Array.from(document.querySelectorAll('[style*="ew-resize"]'));
    if (!handles.length) return null;
    // Левая = с минимальным X
    const handlesWithRect = handles.map(h => {
      const r = h.getBoundingClientRect();
      return { x: r.left, y: r.top, w: r.width, h: r.height, cx: r.left + r.width / 2 };
    }).sort((a, b) => a.cx - b.cx);
    const left = handlesWithRect[0];
    if (left.w === 0 || left.h === 0) return null;  // ещё не отрисован (chart reveal в процессе)
    // Найти контейнер navigator'а для расчёта target X. Раньше искали SVG с
    // preserveAspectRatio="none", но тот SVG рендерится ТОЛЬКО в histogram-режиме
    // превью (см. ChartNavigator.tsx:262 — `{previewMode === 'histogram' && (...)}`).
    // В line-режиме (strength, buffett) SVG вообще не рендерится → drag всегда
    // молча пропускался. `.nav-inner` — обёртка с рельсом/ручками, есть в обоих режимах.
    const nav = document.querySelector('.nav-inner');
    if (!nav) return null;
    const navR = nav.getBoundingClientRect();
    return {
      handleX: left.cx,
      handleY: left.y + left.h / 2,
      navX: navR.left,
      navW: navR.width,
    };
  });

  // Ретраи: под логином (лишние сетевые запросы: профиль, тир-гейтинг) chart
  // reveal-анимация иногда не успевает дорисоваться к первой попытке.
  let box = null;
  for (let attempt = 0; attempt < 5 && !box; attempt++) {
    if (attempt > 0) await page.waitForTimeout(400);
    box = await findBox();
  }
  if (!box) {
    console.warn('   ⚠️  ChartNavigator handle не найден — пропускаю drag');
    return false;
  }
  const targetX = box.navX + box.navW * toXRatio;
  console.log(`   ↔️  drag handle: y=${box.handleY.toFixed(0)}, x=${box.handleX.toFixed(0)}→${targetX.toFixed(0)}`);
  await dragPhysical(page, box.handleX, box.handleY, targetX, box.handleY, duration);
  return true;
}

async function injectCursor(page) {
  await page.evaluate(({ url, scale }) => {
    const wrap = document.createElement('div');
    wrap.id = '__demo_cursor';
    // Курсор увеличиваем в COORD_SCALE раз чтобы оставаться видимым на 2x viewport
    const size = 24 * scale;
    Object.assign(wrap.style, {
      position: 'fixed',
      top: '0', left: '0',
      width: `${size}px`,
      height: `${size}px`,
      backgroundImage: `url("${url}")`,
      backgroundSize: 'contain',
      backgroundRepeat: 'no-repeat',
      pointerEvents: 'none',
      zIndex: '99999',
      transform: `translate(${50 * scale}px, ${50 * scale}px)`,
      transition: 'transform 0ms linear',
      filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.4))',
    });
    document.body.appendChild(wrap);
    window.__cursor = wrap;
  }, { url: CURSOR_SVG_DATA, scale: COORD_SCALE });
}

/**
 * Плавно перемещает курсор от текущей позиции к (x, y) за `duration` ms.
 * Параллельно делает реальный page.mouse.move для триггера hover events.
 *
 * x, y — координаты В LOGICAL system (1280×800). Internally scaled by COORD_SCALE.
 */
async function moveCursor(page, x, y, duration = 800) {
  const tx = x * COORD_SCALE;
  const ty = y * COORD_SCALE;
  const steps = Math.max(8, Math.round(duration / 33)); // ~30fps
  const stepMs = duration / steps;

  const cur = await page.evaluate(() => {
    const m = window.__cursor.style.transform.match(/translate\(([\d.]+)px,\s*([\d.]+)px\)/);
    return m ? { x: parseFloat(m[1]), y: parseFloat(m[2]) } : { x: 50, y: 50 };
  });

  for (let i = 1; i <= steps; i++) {
    const t = i / steps;
    const eased = t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
    const cx = cur.x + (tx - cur.x) * eased;
    const cy = cur.y + (ty - cur.y) * eased;
    await page.evaluate(([cx, cy]) => {
      window.__cursor.style.transform = `translate(${cx}px, ${cy}px)`;
    }, [cx, cy]);
    await page.mouse.move(cx, cy);
    await page.waitForTimeout(stepMs);
  }
}

async function clickAt(page, x, y) {
  await page.evaluate(() => {
    const c = window.__cursor;
    const orig = c.style.transform;
    c.style.transition = 'transform 100ms ease-out';
    c.style.transform = orig + ' scale(0.85)';
    setTimeout(() => {
      c.style.transform = orig;
      c.style.transition = 'transform 0ms linear';
    }, 100);
  });
  await page.mouse.click(x * COORD_SCALE, y * COORD_SCALE);
  await page.waitForTimeout(300);
}

/**
 * Находит центр первого видимого элемента по CSS-селектору (nth-й, если
 * их несколько). Видимость — по bounding box, а не offsetParent (position:fixed
 * попапы вроде PeriodSettingsPopover дают offsetParent=null, но реально видимы).
 */
async function boxOfSelector(page, selector, nth = 0) {
  return page.evaluate(({ selector, nth }) => {
    const visible = (el) => {
      const r = el.getBoundingClientRect();
      const cs = getComputedStyle(el);
      return r.width > 0 && r.height > 0 && cs.visibility !== 'hidden' && cs.display !== 'none';
    };
    const all = Array.from(document.querySelectorAll(selector));
    const els = all.filter(visible);
    const el = els[nth];
    // Диагностика вместо немого null: отличаем «нет в DOM» от «есть, но
    // невидим» и от «видим, но индекс nth за пределами» — без этого
    // «⚠️ элемент не найден» приходится расследовать вслепую.
    if (!el) {
      const why = all.slice(0, 3).map((e) => {
        const r = e.getBoundingClientRect();
        const cs = getComputedStyle(e);
        return `${Math.round(r.width)}x${Math.round(r.height)} display=${cs.display} vis=${cs.visibility}`;
      });
      return { __miss: { selector, nth, inDom: all.length, visible: els.length, why } };
    }
    const r = el.getBoundingClientRect();
    return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
  }, { selector, nth });
}

/** Как boxOfSelector, но матчит по точному/частичному тексту внутри селектора. */
async function boxOfText(page, text, selector = 'button') {
  return page.evaluate(({ text, selector }) => {
    const visible = (el) => {
      const r = el.getBoundingClientRect();
      const cs = getComputedStyle(el);
      return r.width > 0 && r.height > 0 && cs.visibility !== 'hidden' && cs.display !== 'none';
    };
    const els = Array.from(document.querySelectorAll(selector)).filter(visible);
    const el = els.find((e) => (e.textContent || '').trim().includes(text));
    if (!el) return null;
    const r = el.getBoundingClientRect();
    return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
  }, { text, selector });
}

/** Перемещает курсор к box'у и кликает. Возвращает false если box=null (лог warning). */
async function clickBox(page, box, duration = 800) {
  if (!box || box.__miss) {
    const m = box?.__miss;
    console.warn(
      m
        ? `   ⚠️  clickBox: не найден «${m.selector}» (nth=${m.nth}): в DOM ${m.inDom}, видимых ${m.visible}` +
          (m.why.length ? ` — ${m.why.join(' | ')}` : '')
        : '   ⚠️  clickBox: элемент не найден — шаг сценария пропущен'
    );
    return false;
  }
  await moveCursor(page, box.x, box.y, duration);
  await page.waitForTimeout(150);
  await clickAt(page, box.x, box.y);
  return true;
}

// ═══════════════════════════════════════════════════════════
// Сценарии для каждого индикатора
// ═══════════════════════════════════════════════════════════
// Координаты для сценариев рассчитаны под viewport 1280×800 БЕЗ шапки сайта
// (после hideSiteChrome весь контент сдвигается вверх на ~64px).
//
// Когда меняем layout — пересмотри координаты через посткриптумные кадры:
//   ffmpeg -i video.mp4 -vf "select='eq(n\,30)'" -frames:v 1 -update 1 frame.jpg
// Стартовая позиция курсора по умолчанию (справа за кадром)
const startCursorOffscreen = async (page) => {
  await page.evaluate(() => {
    window.__cursor.style.transform = 'translate(1100px, 100px)';
  });
  await page.waitForTimeout(400);
};

// Координаты toolbar buttons — разведаны через debug-toolbars.mjs
// (после удаления nav + page header). Y обычно = 55-130 для toolbar rows.
//
// Структура сценария: hover главный chart → click toggle → hover новый режим.
// Для OI и Силы рынка дополнительно: drag navigator (slider под графиком).
// SCENARIOS под viewport 1280×800 + spoofed innerHeight=760.
// Chart card edge-to-edge: chart=min~320 (auto-fit) или fixed (Strength).
// Hover y-coords в chart area: 200-450. scrollOffset=0 везде.
const SCENARIOS = {
  seasonality: {
    url: '/seasonality',
    waitFor: '.chart-reveal',
    scrollOffset: 0,
    // Seasonality использует aspect-ratio chart → может вырастать. Клиппим
    // frame и chart heights чтобы content помещался в viewport с запасом.
    cssExtra: `
      :root {
        --seasonality-max-height: 460px !important;
        --seasonality-min-height: 320px !important;
        --seasonality-hist-max-height: 420px !important;
        --seasonality-hist-min-height: 280px !important;
      }
      .editorial-frame {
        max-height: calc(100vh - 80px) !important;
        overflow: hidden !important;
      }
    `,
    // Реальный сценарий использования (не просто hover): сменить актив →
    // переключить на «Годовая» → добавить ещё один период → включить
    // «Без дивидендных гэпов» на первом периоде. Селекторы — по data-tour /
    // стабильным классам (frame-dropdown-*, instrument-*), не по пикселям,
    // так переживают правки layout лучше чем голые координаты.
    actions: async (page) => {
      await startCursorOffscreen(page);

      // 1. Открыть селектор актива и выбрать другую бумагу через поиск
      await clickBox(page, await boxOfSelector(page, '[data-tour="seasonality-instrument"] button'), 900);
      await page.waitForTimeout(500);
      const searchBox = await boxOfSelector(page, '.instrument-modal-search');
      await clickBox(page, searchBox, 600);
      await page.type('.instrument-modal-search', 'Газпром', { delay: 55 });
      await page.waitForTimeout(600);
      await clickBox(page, await boxOfSelector(page, '.instrument-item', 0), 700);
      await page.waitForTimeout(800);

      // 2. Переключить режим на «Годовая»
      await clickBox(page, await boxOfText(page, 'Годовая', '[data-tour="seasonality-mode"] button'), 800);
      await page.waitForTimeout(800);

      // 3. Добавить ещё один период сравнения через «+ Период с» — берём НЕ
      // первый год в списке (index 1 совпал бы с уже выбранным периодом и
      // дал бы дубль), а один из середины списка для визуально другого периода.
      await clickBox(page, await boxOfSelector(page, '.frame-dropdown-trigger'), 700);
      await page.waitForTimeout(400);
      await clickBox(page, await boxOfSelector(page, '.frame-dropdown-item', 4), 600);
      await page.waitForTimeout(800);

      // 4. Открыть настройки первого периода и включить «Без дивидендных гэпов»
      await clickBox(page, await boxOfSelector(page, '[data-tour="seasonality-filters"] button', 0), 700);
      await page.waitForTimeout(400);
      await clickBox(page, await boxOfText(page, 'Без дивидендных гэпов'), 600);
      await page.waitForTimeout(900);
    },
  },

  // Реальные сценарии использования (не hover) — выбраны ТОЛЬКО опции без
  // `locked:` для гостя (иначе клик триггерит upgrade-модалку вместо действия).
  strength: {
    url: '/strength',
    waitFor: '.chart-reveal, svg',
    scrollOffset: 0,
    // С viewport 800 нативный Strength (300+150=450 + chrome) помещается
    // с большим буфером — override CSS-vars не нужен.
    actions: async (page) => {
      await startCursorOffscreen(page);

      // 1. Сменить EMA-период (dropdown, всегда доступен гостю)
      await clickBox(page, await boxOfSelector(page, '[data-tour="strength-controls"] .frame-dropdown-trigger'), 800);
      await page.waitForTimeout(400);
      await clickBox(page, await boxOfText(page, 'EMA 50', '.frame-dropdown-item'), 600);
      await page.waitForTimeout(700);

      // 2. Слои графика → включить «Гистограмма», закрыть модалку.
      // Закрытие — через Escape, а не клик по «Закрыть»: модалка рендерится
      // порталом в document.body с фуллскрин-бэкдропом (zIndex 9999, см.
      // CenteredModalShell.tsx) — клик по координатам крестика ненадёжен
      // (промах уводит клик на бэкдроп, тот молча закрывает модалку раньше
      // времени и следующий клик по «20Л» попадает мимо, тоже в бэкдроп).
      await clickBox(page, await boxOfSelector(page, 'button[aria-label="Действия с графиком"]'), 700);
      await page.waitForTimeout(400);
      await clickBox(page, await boxOfSelector(page, 'button[aria-label="Слои графика"]'), 600);
      await page.waitForTimeout(500);
      await clickBox(page, await boxOfText(page, 'Гистограмма'), 600);
      await page.waitForTimeout(500);
      await page.keyboard.press('Escape');
      await page.waitForTimeout(400);

      // 3. Период → «20Л» (новая фича, Pro-only — см. StrengthControls.tsx,
      // добавлена вместе с 10Л в 2026-07-22; под гостем эта кнопка `locked`
      // и клик открыл бы upgrade-модалку вместо смены периода).
      await clickBox(page, await boxOfText(page, '20Л', '[data-tour="strength-controls"] button'), 800);
      await page.waitForTimeout(800);

      // 4. Zoom через drag navigator-ручки
      await dragNavigatorHandle(page, 0.45, 1500);
      await page.waitForTimeout(500);
    },
  },

  buffett: {
    url: '/buffett',
    waitFor: '.chart-reveal, svg',
    scrollOffset: 0,
    actions: async (page) => {
      await startCursorOffscreen(page);

      // Записываем под тестовым Pro-аккаунтом — тарифных замков нет, поэтому
      // дефолтный период «10Л» открывается без апсейл-попапа (в отличие от
      // гостевого сценария, где это было отдельным багом, task_2a27d623).

      // 1. Таймфрейм → «1Н» (доступен в обоих режимах, без tier-замка)
      await clickBox(page, await boxOfText(page, '1Н', '[data-tour="buffett-timeframe"] button'), 800);
      await page.waitForTimeout(700);

      // 2. Режим → «Капитализация / M2» и обратно.
      //    Раньше здесь был дропдаун прогноза, но прогноз ОТКЛЮЧЁН на сайте
      //    флагом SHOW_FORECAST (BuffettPage.tsx:352, Вадим 04.08.2026) —
      //    селектор [data-tour="buffett-forecast"] больше не существует в DOM,
      //    шаг молча пропускался и оставлял в кадре мёртвую паузу.
      //    Замена показывает вторую метрику индикатора — под Pro-аккаунтом
      //    режим cap-m2 без замка.
      await clickBox(page, await boxOfText(page, 'Капитализация / M2', '[data-tour="buffett-view-mode"] button'), 800);
      await page.waitForTimeout(1100);
      await clickBox(page, await boxOfText(page, 'Капитализация / ВВП', '[data-tour="buffett-view-mode"] button'), 700);
      await page.waitForTimeout(800);

      // 3. Zoom через drag navigator-ручки
      await dragNavigatorHandle(page, 0.45, 1500);
      await page.waitForTimeout(500);
    },
  },

  oi: {
    url: '/oi',
    waitFor: '.chart-reveal, svg',
    scrollOffset: 0,
    actions: async (page) => {
      await startCursorOffscreen(page);

      // 1. Вкладки: показать «Скринер сигналов», вернуться на «Открытые позиции»
      await clickBox(page, await boxOfText(page, 'Скринер сигналов', '[data-tour="screener-tabs"] .chart-tab'), 800);
      await page.waitForTimeout(900);
      await clickBox(page, await boxOfText(page, 'Открытые позиции', '[data-tour="screener-tabs"] .chart-tab'), 800);
      await page.waitForTimeout(700);

      // 2. Вариант → «Чистая позиция» (единственный вариант без tier-замка для гостя)
      await clickBox(page, await boxOfSelector(page, '[data-tour="oi-variant"] .frame-dropdown-trigger'), 700);
      await page.waitForTimeout(400);
      await clickBox(page, await boxOfText(page, 'Чистая позиция', '.frame-dropdown-item'), 600);
      await page.waitForTimeout(800);

      // 3. Zoom через drag navigator-ручки
      await dragNavigatorHandle(page, 0.45, 1500);
      await page.waitForTimeout(400);
    },
  },

  'funds-money': {
    url: '/funds-money',
    waitFor: '.chart-reveal, svg',
    scrollOffset: 0,
    actions: async (page) => {
      await startCursorOffscreen(page);

      // 1. Категория активов → «Облигации» (новые вкладки классов активов —
      // ChartTabs tourId="funds-categories", см. FundsMoneyPage.tsx:641-653;
      // раньше страница показывала только денежный рынок без переключателя).
      await clickBox(page, await boxOfText(page, 'Облигации', '[data-tour="funds-categories"] .chart-tab'), 800);
      await page.waitForTimeout(900);

      // 2. Открыть селектор фондов (показать список), закрыть
      await clickBox(page, await boxOfSelector(page, '[data-tour="funds-table"] button'), 800);
      await page.waitForTimeout(900);
      await clickBox(page, await boxOfSelector(page, 'button[aria-label="Закрыть"]'), 600);
      await page.waitForTimeout(500);

      // 3. Режим → «СЧА» (без tier-замка, в отличие от «Притоки-Оттоки»)
      await clickBox(page, await boxOfText(page, 'СЧА', '[data-tour="funds-view-mode"] button'), 700);
      await page.waitForTimeout(800);

      // 4. Zoom через drag navigator-ручки
      await dragNavigatorHandle(page, 0.45, 1500);
      await page.waitForTimeout(400);
    },
  },

  'funds-catalog': {
    url: '/funds-catalog',
    waitFor: '[class*="rounded"]',
    scrollOffset: 0,
    // MutationObserver в addInitScript ловит nav при mount React.
    // scrollIntoView block:'start' даёт top frame'а в видимости + padding сверху.
    actions: async (page) => {
      await startCursorOffscreen(page);
      await moveCursor(page, 350, 250, 1100);
      await page.waitForTimeout(700);
      await moveCursor(page, 750, 280, 800);
      await page.waitForTimeout(500);
      await moveCursor(page, 350, 420, 800);
      await page.waitForTimeout(500);
      await moveCursor(page, 750, 470, 700);
      await page.waitForTimeout(500);
    },
  },

  heatmap: {
    url: '/heatmap',
    waitFor: 'svg',
    scrollOffset: 0,
    // Treemap без time-series navigator — zoom/drag тут неприменим, вместо
    // этого демонстрируем группировку/масштаб/период + hover по плиткам
    // (drill-down по клику отсутствует, только tooltip).
    actions: async (page) => {
      await startCursorOffscreen(page);

      // 1. Группировка → «По секторам» (dropdown, без tier-замка)
      await clickBox(page, await boxOfSelector(page, '[data-tour="heatmap-size"] .frame-dropdown-trigger'), 800);
      await page.waitForTimeout(400);
      await clickBox(page, await boxOfText(page, 'По секторам', '.frame-dropdown-item'), 600);
      await page.waitForTimeout(700);

      // 2. Размер плитки → «Оборот»
      await clickBox(page, await boxOfText(page, 'Оборот', '[data-tour="heatmap-size"] button'), 700);
      await page.waitForTimeout(700);

      // 3. Период → «1Н»
      await clickBox(page, await boxOfText(page, '1Н', '[data-tour="heatmap-period"] button'), 700);
      await page.waitForTimeout(700);

      // 4. Hover по паре плиток — показать tooltip
      await moveCursor(page, 400, 350, 700);
      await page.waitForTimeout(500);
      await moveCursor(page, 700, 400, 600);
      await page.waitForTimeout(500);
    },
  },
};

// ═══════════════════════════════════════════════════════════
// Helpers — execFileSync (массив аргументов, без shell injection)
// ═══════════════════════════════════════════════════════════
function ffmpeg(args) {
  execFileSync('ffmpeg', args, { stdio: 'pipe' });
}

/**
 * Логинится под тестовым аккаунтом через API и возвращает токены — их кладём
 * в localStorage через addInitScript ДО первого рендера React (AuthContext
 * читает access_token/refresh_token из localStorage при монтировании,
 * см. frontend/src/contexts/AuthContext.tsx). access_token живёт 15 минут —
 * этого с запасом хватает на одну запись.
 */
async function loginTestAccount() {
  if (!TEST_EMAIL || !TEST_PASSWORD) return null;
  const res = await fetch(`${BASE_URL}/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: TEST_EMAIL, password: TEST_PASSWORD }),
  });
  if (!res.ok) {
    console.warn(`   ⚠️  логин тестового аккаунта не удался (${res.status}) — записываю как гость`);
    return null;
  }
  const { access_token, refresh_token } = await res.json();
  return { access_token, refresh_token };
}

// ═══════════════════════════════════════════════════════════
// Запись одного видео
// ═══════════════════════════════════════════════════════════
/**
 * @param {string} name    ключ сценария
 * @param {'dark'|'light'} theme  тема сайта на время записи. 'dark' пишет в
 *        `<name>.*` (исторический набор), 'light' — в `<name>-light.*`.
 *        Соответствие темы и суффикса продублировано во фронте:
 *        frontend/src/components/landing/IndicatorGroup.tsx (themedVideo).
 */
async function recordOne(name, theme = 'dark') {
  const scenario = SCENARIOS[name];
  if (!scenario) {
    console.error(`❌ Сценарий "${name}" не найден. Доступны: ${Object.keys(SCENARIOS).join(', ')}`);
    process.exit(1);
  }
  const themeId = theme === 'light' ? 'editorial-light' : 'editorial-dark';
  const suffix = theme === 'light' ? '-light' : '';

  console.log(`\n🎬 Запись: ${name} (тема ${themeId})`);
  console.log(`   URL: ${BASE_URL}${scenario.url}`);

  const tmpDir = join(TEMP_DIR, name);
  if (existsSync(tmpDir)) rmSync(tmpDir, { recursive: true });
  mkdirSync(tmpDir, { recursive: true });

  console.log('   → логин тестового Pro-аккаунта...');
  const authTokens = await loginTestAccount();
  console.log(authTokens ? '   ✅ залогинены (Pro)' : '   ℹ️  без логина — записываю как гость');

  const browser = await chromium.launch({
    headless: true,
    args: ['--disable-blink-features=AutomationControlled'],
  });

  const context = await browser.newContext({
    viewport: VIEWPORT,
    deviceScaleFactor: 2,
    recordVideo: {
      dir: tmpDir,
      size: VIEWPORT,
    },
    bypassCSP: true,
  });

  // КРИТИЧНО: pre-page setup (запускается ДО любого скрипта страницы):
  //   1. Spoof window.innerHeight=760 → useFitToViewport (OI/funds-money:
  //      min:360,max:720,bottomBuffer:64) отдаёт бОльшую высоту графика, ближе
  //      к max — карточка заполняет кадр 1280×800 почти впритык, без чёрного
  //      фона (было 500 → chart=min=360, оставляло много пустоты + давало
  //      место, куда утекал footer — см. computeCropFilter ниже и hideSiteChrome).
  //      760, а не 800: должно остаться место под навигатор+паддинги ПОСЛЕ
  //      scrollChartCardToTop (PADDING_TOP=40) — иначе навигатор с
  //      drag-хендлами уезжает за нижний край записываемого кадра.
  //   2. CSS hide nav/header — nav невидим С ПЕРВОГО фрейма
  //   3. scenario.cssExtra тоже здесь — чтобы CSS-vars (--strength-chart-*,
  //      --seasonality-* и т.д.) применились ДО mount React. Иначе React
  //      читает дефолтные значения и наш override не действует.
  //   4. MutationObserver как backup для удаления nav из DOM
  const sceneCss = scenario.cssExtra || '';
  await context.addInitScript(({ cssExtra, indicatorKey, tokens, themeId }) => {
    // Подавляем cookie-баннер, onboarding-тур и anomaly-тосты — иначе они
    // перекрывают chart card в первых кадрах записи.
    // Ключи см. frontend/src/contexts/AnalyticsContext.tsx (STORAGE_CONSENT_KEY),
    // frontend/src/hooks/useFirstVisit.ts (STORAGE_PREFIX, версия бампается),
    // frontend/src/contexts/AnomalyContext.tsx (LS_TOASTS).
    try {
      // Тема — ДО первого рендера React: ThemeProvider читает localStorage
      // в ленивом инициализаторе useState (frontend/src/contexts/ThemeContext.tsx),
      // так что data-theme встаёт с первого кадра, без вспышки чужой темы.
      localStorage.setItem('theme', themeId);
      localStorage.setItem('frame_consent_v1', 'minimal');
      localStorage.setItem(`frame_tour_seen_v4:${indicatorKey}`, '1');
      // OI имеет отдельный под-тур для вкладки «Скринер сигналов»
      // (useOnboardingTour('oi-screener'), см. OpenInterestPage.tsx:264) —
      // свой ключ, независимый от главного тура индикатора. Гасим и его,
      // иначе клик по вкладке триггерит 7-шаговый onboarding поверх записи.
      localStorage.setItem(`frame_tour_seen_v4:${indicatorKey}-screener`, '1');
      // Анонс «Новое: Терминал» живёт в Layout (frontend/src/components/Layout.tsx),
      // а не на странице индикатора — гасится СВОИМ ключом, отдельно от тура
      // страницы. Без него полноэкранный оверлей перехватывает все клики
      // сценария: боксы находятся, но elementFromPoint отдаёт DIV.fixed.
      // Любой НОВЫЙ общесайтовый анонс придётся добавлять сюда же.
      localStorage.setItem('frame_tour_seen_v4:terminal-intro', '1');
      localStorage.setItem('anomaly_toasts_enabled', '0');
      // Токены тестового Pro-аккаунта — до монтирования React, чтобы
      // AuthContext сразу поднял сессию без мигания гостевого состояния.
      if (tokens) {
        localStorage.setItem('access_token', tokens.access_token);
        localStorage.setItem('refresh_token', tokens.refresh_token);
      }
    } catch {
      /* localStorage недоступен — тур/баннер/тост могут остаться в кадре */
    }

    Object.defineProperty(window, 'innerHeight', {
      get: () => 760,
      configurable: true,
    });
    Object.defineProperty(document.documentElement, 'clientHeight', {
      get: () => 760,
      configurable: true,
    });

    const styleHide = document.createElement('style');
    styleHide.id = '__rec_hide_chrome';
    styleHide.textContent = `
      nav, header.sticky, .site-header { display: none !important; }
      ${cssExtra}
    `;
    (document.head || document.documentElement).appendChild(styleHide);

    const removeChrome = () => {
      document.querySelectorAll('nav, header.sticky, .site-header, footer').forEach(el => el.remove());
    };
    const setupObserver = () => {
      removeChrome();
      const obs = new MutationObserver(removeChrome);
      obs.observe(document.body, { childList: true, subtree: true });
    };
    if (document.body) setupObserver();
    else document.addEventListener('DOMContentLoaded', setupObserver);
  }, { cssExtra: sceneCss, indicatorKey: name, tokens: authTokens, themeId });

  // Стратегия: recording стартует с newContext, но первые ~3 секунды
  // (loading + render React) шапка видна. Решение — обрезаем стартовые
  // кадры через ffmpeg (TRIM_START_SEC) после записи. Сценарий начинает
  // действия после hideSiteChrome → курсор и интерактив попадают в обрезанную часть.

  const page = await context.newPage();

  console.log('   → переход на страницу...');
  await page.goto(`${BASE_URL}${scenario.url}`, { waitUntil: 'domcontentloaded', timeout: 30000 });

  if (scenario.waitFor) {
    try {
      await page.waitForSelector(scenario.waitFor, { timeout: 15000 });
    } catch {
      console.warn(`   ⚠️  selector "${scenario.waitFor}" не найден — продолжаем без ожидания`);
    }
  }
  await page.waitForTimeout(800);

  console.log('   → скрываем шапку сайта + per-page CSS...');
  await hideSiteChrome(page, scenario.cssExtra || '');

  // Per-scenario hook для DOM-мутаций после React mount (например ужатие
  // chart container'а который использует inline style и не реагирует на CSS-vars)
  if (scenario.onAfterLoad) {
    console.log('   → onAfterLoad (DOM mutate)...');
    await scenario.onAfterLoad(page);
    await page.waitForTimeout(300);
  }

  // Scroll chart card к top viewport. Page wrapper `py-6 md:py-8` даёт
  // ~32px естественного padding сверху → не "обрезано".
  console.log('   → scroll chart card → top viewport...');
  await scrollChartCardToTop(page);
  await page.waitForTimeout(400);

  // Если сценарий требует custom scroll — делаем
  if (scenario.scrollTo) {
    await page.evaluate((selector) => {
      const el = document.querySelector(selector);
      if (el) el.scrollIntoView({ behavior: 'instant', block: 'start' });
    }, scenario.scrollTo);
    await page.waitForTimeout(400);
  }

  console.log('   → inject курсора...');
  await injectCursor(page);

  console.log('   → сценарий...');
  await scenario.actions(page);

  await page.waitForTimeout(200);

  console.log('   → измеряем bbox chart-card...');
  const cardBBox = await measureCardBBox(page);

  console.log('   → закрытие, сохранение видео...');
  const videoPath = await page.video()?.path();
  await context.close();
  await browser.close();

  if (!videoPath || !existsSync(videoPath)) {
    console.error(`❌ Видео не записалось`);
    return null;
  }

  if (!existsSync(OUTPUT_DIR)) mkdirSync(OUTPUT_DIR, { recursive: true });
  const base = `${name}${suffix}`;
  const outWebm = join(OUTPUT_DIR, `${base}.webm`);
  const outMp4 = join(OUTPUT_DIR, `${base}.mp4`);
  const outPoster = join(OUTPUT_DIR, `${base}-poster.jpg`);
  const outHlsDir = join(OUTPUT_DIR, base);  // HLS сегменты в папку <base>/
  if (existsSync(outHlsDir)) rmSync(outHlsDir, { recursive: true });
  mkdirSync(outHlsDir, { recursive: true });

  // Raw-запись Playwright — 1280×800 (в CSS-пикселях viewport'а; DPR=2 влияет
  // на резкость рендера в браузере, но НЕ на разрешение самого video-файла —
  // подтверждено эмпирически через ffprobe). Lanczos-downscale ниже — это
  // сглаживание/масштаб к финальному размеру плитки, не honest retina-downsample.
  //
  // Перед scale — динамический crop по замеренному bbox card (см.
  // computeCropFilter): убирает лишний чёрный фон вокруг разновысоких
  // карточек и заодно footer, если тот оказался в кадре. scaleFactor=1, т.к.
  // bbox измерен в тех же CSS-px, что и raw-видео.
  const cropFilter = computeCropFilter(cardBBox, 1);
  // scale только по ширине (высота -2 → авто, кратно 2 для кодека) — высота
  // после crop уже плотно облегает card и своя у каждого индикатора; насильно
  // тянуть её к FINAL_HEIGHT растягивало/сжимало бы контент по вертикали.
  const scaleTarget = cropFilter ? `${FINAL_WIDTH}:-2` : `${FINAL_WIDTH}:${FINAL_HEIGHT}`;
  const fpsFilter = `${cropFilter ? cropFilter + ',' : ''}scale=${scaleTarget}:flags=lanczos,fps=${VIDEO_FPS}`;
  if (!cropFilter) console.warn('   ⚠️  bbox карточки не измерен — записываю без crop (полный кадр 1280×800)');
  const trim = ['-ss', String(TRIM_START_SEC)];

  console.log(`   → ffmpeg → .webm (VP9 CRF ${VP9_CRF}, cpu-used ${VP9_CPU_USED}, обрезка ${TRIM_START_SEC}s)...`);
  ffmpeg([
    '-y', ...trim, '-i', videoPath,
    '-c:v', 'libvpx-vp9',
    '-b:v', '0',
    '-crf', String(VP9_CRF),
    '-cpu-used', String(VP9_CPU_USED),
    '-deadline', 'good',
    '-row-mt', '1', '-an',
    '-vf', fpsFilter,
    outWebm,
  ]);

  console.log(`   → ffmpeg → .mp4 (H.264 CRF ${H264_CRF}, Safari fallback)...`);
  ffmpeg([
    '-y', ...trim, '-i', videoPath,
    '-c:v', 'libx264', '-preset', 'slow', '-crf', String(H264_CRF), '-an', '-movflags', '+faststart',
    '-vf', fpsFilter,
    outMp4,
  ]);

  console.log('   → ffmpeg → poster (1й кадр)...');
  ffmpeg([
    '-y', ...trim, '-i', videoPath,
    '-vframes', '1', '-q:v', '2',
    '-vf', fpsFilter,
    outPoster,
  ]);

  console.log('   → ffmpeg → HLS (2s сегменты для быстрого старта)...');
  // HLS: H264 baseline (max compat), 2s сегменты, faststart-ish для мгновенного play.
  // Стартовый сегмент ~150-300 KB → видео начинает играть с первой секунды,
  // остальные сегменты подкачиваются параллельно во время воспроизведения.
  ffmpeg([
    '-y', ...trim, '-i', videoPath,
    '-c:v', 'libx264', '-preset', 'medium', '-crf', String(H264_CRF),
    '-profile:v', 'baseline', '-level', '3.0',
    '-an',
    '-vf', fpsFilter,
    '-hls_time', '2',
    '-hls_list_size', '0',
    '-hls_segment_filename', join(outHlsDir, 'seg%03d.ts'),
    '-f', 'hls',
    join(outHlsDir, 'index.m3u8'),
  ]);

  rmSync(tmpDir, { recursive: true });

  const sizeKB = (p) => existsSync(p) ? Math.round(statSync(p).size / 1024) : 0;
  console.log(`   ✅ ${base}.webm: ${sizeKB(outWebm)} KB`);
  console.log(`   ✅ ${base}.mp4:  ${sizeKB(outMp4)} KB`);
  console.log(`   ✅ ${base}-poster.jpg: ${sizeKB(outPoster)} KB`);
  // HLS сегменты — суммарный размер
  try {
    const segs = execFileSync('ls', [outHlsDir]).toString().trim().split('\n');
    const tsSegs = segs.filter(s => s.endsWith('.ts'));
    const totalKB = tsSegs.reduce((sum, s) => sum + sizeKB(join(outHlsDir, s)), 0);
    const firstKB = sizeKB(join(outHlsDir, tsSegs[0] || ''));
    console.log(`   ✅ ${base}/ HLS: ${tsSegs.length} сегментов, total ${totalKB} KB, first segment ${firstKB} KB (instant start)`);
  } catch {}

  return { webm: outWebm, mp4: outMp4, poster: outPoster };
}

// ═══════════════════════════════════════════════════════════
// CLI
// ═══════════════════════════════════════════════════════════
const args = process.argv.slice(2);
const arg = args.find(a => !a.startsWith('--'));
// --theme=dark|light|both. Дефолт dark — исторический набор `<name>.*`,
// светлый пишется в `<name>-light.*` (см. recordOne).
const themeArg = (args.find(a => a.startsWith('--theme='))?.split('=')[1] || 'dark');
const THEMES_TO_RECORD = themeArg === 'both' ? ['dark', 'light'] : [themeArg];
if (!arg || !['dark', 'light', 'both'].includes(themeArg)) {
  console.log('Usage:');
  console.log('  node record-indicator.mjs <name>                 # один индикатор (тёмная тема)');
  console.log('  node record-indicator.mjs all                    # все');
  console.log('  node record-indicator.mjs all --theme=light      # светлый набор → <name>-light.*');
  console.log('  node record-indicator.mjs all --theme=both       # оба набора подряд');
  console.log('Доступны:', Object.keys(SCENARIOS).join(', '));
  process.exit(0);
}

(async () => {
  for (const theme of THEMES_TO_RECORD) {
    const names = arg === 'all' ? Object.keys(SCENARIOS) : [arg];
    for (const name of names) {
      await recordOne(name, theme);
    }
  }
  console.log('\n🎉 Готово.');
})().catch(e => { console.error(e); process.exit(1); });
