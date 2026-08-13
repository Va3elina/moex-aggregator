/**
 * make-showcase-shots.mjs — снимает HQ screenshots индикаторов для
 * MultiChartShowcase секции на лендинге.
 *
 * В отличие от recordVideo, page.screenshot() В Playwright корректно работает
 * с deviceScaleFactor=2 — захватывает в physical pixels (2560×1600 при viewport
 * 1280×800 + DPR=2). Это даёт настоящий HiDPI quality для статичных тайлов.
 *
 * Использование:
 *   node make-showcase-shots.mjs
 *
 * Output: ../../frontend/public/showcase/<name>.webp
 */
import { chromium } from 'playwright';
import { execFileSync } from 'node:child_process';
import { mkdirSync, existsSync, rmSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_DIR = join(__dirname, '../../frontend/public/showcase');
const TEMP_DIR = join(__dirname, '.tmp-shots');

const BASE_URL = process.env.RECORD_BASE_URL || 'https://xn--80aklbnczmv.xn--p1ai';
// Viewport 1280×950 — выше стандартных 800 чтобы вместить toolbar + chart + navigator
// для тяжёлых страниц (OI, Buffett с descriptions). DPR=2 → 2560×1900 phys px capture.
const VIEWPORT = { width: 1280, height: 950 };

// HIDE_DESCRIPTIONS — общий css для индикаторов с описанием снизу,
// убирает блоки описания методологии чтобы chart card занял весь viewport.
const HIDE_DESCRIPTIONS = `
  .mt-6.bg-theme-secondary { display: none !important; }
  div.mt-6.grid { display: none !important; }
  div.mt-6 > div.bg-theme-secondary { display: none !important; }
`;

/**
 * Скрывает глобальный nav и per-page header — даём максимум места под chart.
 */
async function hideSiteChrome(page, cssExtra = '') {
  await page.evaluate((extraCss) => {
    document.querySelectorAll('nav').forEach(el => el.remove());
    document.querySelectorAll('header.sticky, .site-header').forEach(el => el.remove());
    document.querySelectorAll('h1.text-2xl.font-bold, h1.text-3xl.font-bold').forEach(h1 => {
      const wrap = h1.closest('.flex.items-start');
      if (wrap) wrap.remove();
      else h1.parentElement?.remove();
    });
    const style = document.createElement('style');
    style.textContent = `
      html, body { overflow: hidden !important; }
      ::-webkit-scrollbar { display: none !important; }
      body { padding-top: 0 !important; margin-top: 0 !important; }
      ${extraCss}
    `;
    document.head.appendChild(style);
  }, cssExtra);
}

// ═══════════════════════════════════════════════════════════
// 8 тайлов для showcase
// ═══════════════════════════════════════════════════════════
// Названия: для индикаторов с режимами добавляем "-mode1"/"-mode2" суффикс.
// `setup` (опц.) — async actions перед screenshot (клик по кнопкам режимов).
const SHOTS = [
  { name: 'heatmap', url: '/heatmap' },
  { name: 'seasonality-week', url: '/seasonality' },  // default = по дням недели
  { name: 'seasonality-month', url: '/seasonality',
    setup: async (page) => {
      // Click на "По месяцам" toolbar button (x≈936 logical после hideSiteChrome)
      await page.locator('button:has-text("По месяцам")').first().click({ timeout: 5000 }).catch(() => {});
      await page.waitForTimeout(1200);  // re-render
    },
  },
  { name: 'oi', url: '/oi', cssExtra: HIDE_DESCRIPTIONS },
  { name: 'buffett', url: '/buffett', cssExtra: HIDE_DESCRIPTIONS },
  { name: 'strength', url: '/strength' },
  { name: 'funds-money-aum', url: '/funds-money', cssExtra: HIDE_DESCRIPTIONS },  // default = СЧА
  { name: 'funds-money-flows', url: '/funds-money', cssExtra: HIDE_DESCRIPTIONS,
    setup: async (page) => {
      await page.locator('button:has-text("Притоки")').first().click({ timeout: 5000 }).catch(() => {});
      await page.waitForTimeout(1200);
    },
  },
];

async function shootOne(page, shot) {
  console.log(`\n📸 ${shot.name}: ${shot.url}`);
  await page.goto(`${BASE_URL}${shot.url}`, { waitUntil: 'domcontentloaded', timeout: 30000 });
  // Ждём загрузки chart (svg или chart-reveal)
  try {
    await page.waitForSelector('.chart-reveal, svg.cursor-crosshair, svg', { timeout: 15000 });
  } catch {
    console.warn(`   ⚠️  chart selector не найден — продолжаю`);
  }
  await page.waitForTimeout(1500);  // settle animations

  await hideSiteChrome(page, shot.cssExtra || '');
  await page.waitForTimeout(300);

  if (shot.setup) {
    console.log(`   → setup actions...`);
    await shot.setup(page);
  }

  const tmpPath = join(TEMP_DIR, `${shot.name}.png`);
  // Снимаем ВЕСЬ viewport (1280×950 logical, 2560×1900 physical с DPR=2).
  // Захватывает toolbar + chart + navigator — как живая страница.
  // Это близко к тому что пользователь видит, открыв индикатор.
  await page.screenshot({ path: tmpPath, fullPage: false });
  console.log(`   → screenshot saved → downscale + jpg HQ...`);

  const outPath = join(OUTPUT_DIR, `${shot.name}.jpg`);
  // КЛЮЧЕВОЙ INSIGHT: source resolution должен быть БЛИЗКИМ к display size,
  // НЕ значительно больше. Browser downscale из 1920→550 phys (на retina ratio 3.5×)
  // даёт smoothing на мелком тексте (axis-labels SBER 0.66%, ROSN, и т.д.).
  // Уменьшаем source до 1280-wide → ratio 2× = sharp без потери детализации.
  // Lanczos один раз через ffmpeg даёт лучший результат чем bilinear browser многократно.
  execFileSync('ffmpeg', [
    '-y', '-i', tmpPath,
    '-vf', "scale='min(1280,iw)':-2:flags=lanczos",
    '-q:v', '2',
    outPath,
  ], { stdio: 'pipe' });

  const sizeKB = Math.round(statSync(outPath).size / 1024);
  console.log(`   ✅ ${shot.name}.webp: ${sizeKB} KB`);
}

(async () => {
  if (!existsSync(OUTPUT_DIR)) mkdirSync(OUTPUT_DIR, { recursive: true });
  if (existsSync(TEMP_DIR)) rmSync(TEMP_DIR, { recursive: true });
  mkdirSync(TEMP_DIR, { recursive: true });

  const browser = await chromium.launch({
    headless: true,
    args: ['--disable-blink-features=AutomationControlled'],
  });
  const context = await browser.newContext({
    viewport: VIEWPORT,
    deviceScaleFactor: 2,  // HiDPI capture для screenshot
    bypassCSP: true,
  });
  const page = await context.newPage();

  for (const shot of SHOTS) {
    try {
      await shootOne(page, shot);
    } catch (e) {
      console.error(`   ❌ ${shot.name}: ${e.message}`);
    }
  }

  await browser.close();
  rmSync(TEMP_DIR, { recursive: true });
  console.log('\n🎉 Готово.');
})().catch(e => { console.error(e); process.exit(1); });
