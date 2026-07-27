/**
 * prerender-meta.ts — postbuild step, генерит per-route HTML с правильными
 * meta tags для каждой публичной страницы.
 *
 * Проблема которую решает: SPA отдаёт одинаковый dist/index.html на все URL.
 * Yandex/Google видят на /buffett, /heatmap, /cbr-flows и пр. одинаковый
 * <title> и <meta description> — это duplicate-content penalty и плохие
 * сниппеты в выдаче.
 *
 * Что делает: для каждого route из SEO_META генерирует dist/<route>/index.html
 * с правильными <title>, <meta description>, <meta keywords>, <link canonical>,
 * <meta og:*>, <meta twitter:*>, <meta robots>, и добавляет BreadcrumbList
 * JSON-LD для не-главных страниц.
 *
 * Body содержимое остаётся SPA shell — после первой загрузки клиент-JS
 * (React + @unhead) рендерит контент и подтверждает тот же набор meta tags
 * (dedupe). Yandex дорендерит body через свой JS-исполнитель.
 *
 * Запускается через `tsx` (postbuild hook в package.json). Не требует
 * Puppeteer/Chromium — чистая string substitution за ~50ms.
 *
 * FastAPI catch-all (api/main.py::serve_spa) проверяет наличие
 * dist/<path>/index.html и отдаёт его, иначе fallback на dist/index.html.
 */

import { readFileSync, writeFileSync, mkdirSync, readdirSync, existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { SEO_META, CANONICAL_HOST } from '../src/config/seoMeta';
import type { SeoMeta } from '../src/config/seoMeta';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DIST = resolve(__dirname, '../dist');
const TEMPLATE_PATH = resolve(DIST, 'index.html');

const template = readFileSync(TEMPLATE_PATH, 'utf-8');

function escapeAttr(s: string): string {
    return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;');
}
function escapeText(s: string): string {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;');
}

function replaceMetaByName(html: string, name: string, content: string): string {
    const pattern = new RegExp(`<meta\\s+name="${name}"[^>]*>`);
    const replacement = `<meta name="${name}" content="${escapeAttr(content)}" />`;
    return pattern.test(html)
        ? html.replace(pattern, replacement)
        : html.replace('</head>', `  ${replacement}\n</head>`);
}
function replaceMetaByProperty(html: string, prop: string, content: string): string {
    const pattern = new RegExp(`<meta\\s+property="${prop}"[^>]*>`);
    const replacement = `<meta property="${prop}" content="${escapeAttr(content)}" />`;
    return pattern.test(html)
        ? html.replace(pattern, replacement)
        : html.replace('</head>', `  ${replacement}\n</head>`);
}
function replaceCanonical(html: string, url: string): string {
    const pattern = /<link\s+rel="canonical"[^>]*>/;
    return html.replace(pattern, `<link rel="canonical" href="${escapeAttr(url)}" />`);
}

function buildBreadcrumb(path: string, meta: SeoMeta, canonical: string): object {
    const items: Array<{ name: string; url: string }> = [
        { name: 'Главная', url: CANONICAL_HOST + '/' },
    ];
    const pageName = meta.title.split(' | ')[0].split(' — ')[0];
    if (meta.breadcrumb && path !== '/') {
        items.push({ name: meta.breadcrumb, url: canonical });
        items.push({ name: pageName, url: canonical });
    } else if (path !== '/') {
        items.push({ name: pageName, url: canonical });
    }
    return {
        '@context': 'https://schema.org',
        '@type': 'BreadcrumbList',
        itemListElement: items.map((b, i) => ({
            '@type': 'ListItem',
            position: i + 1,
            name: b.name,
            item: b.url,
        })),
    };
}

// Статический блок внутренних ссылок на главные индикаторы. Краулер (особенно
// до выполнения JS) видел в #root только текст, но НИ ОДНОЙ <a href> — вся
// навигация рисуется React'ом. Без реальных ссылок слабее перелинковка и почти
// нет шанса на «быстрые ссылки»/sitelinks (движок берёт кандидатов из заметных
// внутренних ссылок). Добавляем настоящие <a> на 7 индикаторов; React заменяет
// #root при гидрации, так что для пользователя блок невидим. Источник истины —
// SEO_META (breadcrumb==='Индикаторы'), новый индикатор подхватится сам.
const NAV_LINKS = Object.entries(SEO_META)
    .filter(([, m]) => m.breadcrumb === 'Индикаторы' && !m.noindex)
    .map(([p, m]) => {
        const label = escapeText(m.title.split(' | ')[0].split(' — ')[0]);
        return (
            '<li style="margin:0">' +
            `<a href="${p}" style="color:#F5F1E8;text-decoration:underline;` +
            'text-underline-offset:3px;display:inline-block;padding:4px 0">' +
            `${label}</a></li>`
        );
    })
    .join('');
const NAV_BLOCK =
    '<nav aria-label="Индикаторы Фрейма" style="margin-top:32px">' +
    '<ul style="list-style:none;padding:0;margin:0;display:flex;flex-wrap:wrap;' +
    'gap:8px 24px;font-size:1rem;line-height:1.5">' +
    NAV_LINKS +
    '</ul></nav>';

function renderRoute(path: string, meta: SeoMeta): string {
    const canonical = `${CANONICAL_HOST}${path}`;
    let html = template;

    html = html.replace(/<title>[^<]*<\/title>/, `<title>${escapeText(meta.title)}</title>`);
    if (meta.description) {
        html = replaceMetaByName(html, 'description', meta.description);
        html = replaceMetaByProperty(html, 'og:description', meta.description);
        html = replaceMetaByName(html, 'twitter:description', meta.description);
    }
    if (meta.keywords) {
        html = replaceMetaByName(html, 'keywords', meta.keywords);
    }
    const robotsContent = meta.noindex
        ? 'noindex, nofollow'
        : 'index, follow, max-image-preview:large';
    html = replaceMetaByName(html, 'robots', robotsContent);
    html = replaceCanonical(html, canonical);
    html = replaceMetaByProperty(html, 'og:url', canonical);
    html = replaceMetaByProperty(html, 'og:title', meta.title);
    html = replaceMetaByName(html, 'twitter:title', meta.title);

    // BreadcrumbList JSON-LD — даёт хлебные крошки в выдаче Google для не-главных
    // публичных страниц. Для noindex (login/profile/billing/*) breadcrumbs не нужны —
    // эти URL не должны попадать в выдачу вообще.
    if (path !== '/' && !meta.noindex) {
        const bc = buildBreadcrumb(path, meta, canonical);
        const ld = `<script type="application/ld+json">${JSON.stringify(bc)}</script>`;
        html = html.replace('</head>', `  ${ld}\n</head>`);
    }

    // Article schema для /methodology/* — длинный educational контент,
    // даёт rich snippet с датой в выдаче Yandex/Google. Дата = время билда.
    if (path.startsWith('/methodology/') && !meta.noindex) {
        const article = {
            '@context': 'https://schema.org',
            '@type': 'Article',
            headline: meta.title.split(' | ')[0],
            description: meta.description,
            author: {
                '@type': 'Organization',
                name: 'Фрейм',
                url: CANONICAL_HOST + '/',
            },
            publisher: {
                '@type': 'Organization',
                name: 'Фрейм',
                logo: {
                    '@type': 'ImageObject',
                    url: CANONICAL_HOST + '/logo.svg',
                },
            },
            datePublished: new Date().toISOString().slice(0, 10),
            dateModified: new Date().toISOString().slice(0, 10),
            mainEntityOfPage: canonical,
            inLanguage: 'ru-RU',
        };
        const ld = `<script type="application/ld+json">${JSON.stringify(article)}</script>`;
        html = html.replace('</head>', `  ${ld}\n</head>`);
    }

    // Product schema для /pricing — Yandex показывает «от 0₽» snippet
    // в выдаче по запросам типа «аналитика MOEX цена». AggregateOffer
    // даёт диапазон без commit'а на точные тарифы (они меняются через API).
    if (path === '/pricing') {
        const product = {
            '@context': 'https://schema.org',
            '@type': 'Product',
            name: 'Подписка Фрейм',
            description: meta.description,
            brand: { '@type': 'Brand', name: 'Фрейм' },
            url: canonical,
            offers: {
                '@type': 'AggregateOffer',
                offerCount: 3,
                lowPrice: '0',
                priceCurrency: 'RUB',
                availability: 'https://schema.org/InStock',
                url: canonical,
            },
        };
        const ld = `<script type="application/ld+json">${JSON.stringify(product)}</script>`;
        html = html.replace('</head>', `  ${ld}\n</head>`);
    }

    // ── Статический контент в #root (для краулеров, особенно Yandex) ──────────
    // SPA отдавал пустой <div id="root"></div> — Yandex почти не исполняет JS, то
    // есть видел meta, но НЕ видел текста страницы → ранжировать нечего. Вставляем
    // настоящий <h1> + вводный абзац (intro, иначе description) прямо в #root.
    // main.tsx монтирует через createRoot → React заменяет это содержимое при
    // загрузке (для пользователя — кратковременный «server-rendered» текст вместо
    // пустоты, что даже быстрее по ощущению). Только для публичных (index) страниц.
    if (!meta.noindex) {
        const h1 = escapeText(meta.title.split(' | ')[0]);
        const intro = escapeText(meta.intro || meta.description || '');
        // Блок стилизован под тёмную editorial-тему (до React страница всегда
        // тёмная — background-color:#0B0D12 в инлайне <html>). CSS-скрытие
        // (sr-only/clip) не годится: YandexMobileBot почти не исполняет JS, но
        // ЛЮБОЙ рендерер применяет CSS без JS — clip-хак оставлял боту ПУСТУЮ
        // страницу, Вебмастер выдал NOT_MOBILE_FRIENDLY (PR #666). Вместо этого
        // прячем синхронным inline <script> сразу после блока: он выполняется
        // (и скрывает блок) только в JS-способных клиентах — то есть у реальных
        // пользователей, почти мгновенно, до того как человек успевает увидеть
        // текст. YandexMobileBot, который скрипт не исполняет, по-прежнему видит
        // видимый блок → критерий мобилопригодности выполнен без FOUC для людей.
        const block =
            '<div id="seo-prerender" style="max-width:720px;margin:0 auto;' +
            'padding:clamp(28px,7vw,72px) 20px;color:#F5F1E8;' +
            'font-family:Inter,system-ui,-apple-system,sans-serif">' +
            '<h1 style="font-family:Archivo,Inter,system-ui,sans-serif;' +
            'font-size:clamp(1.45rem,1.1rem+1.8vw,2.1rem);line-height:1.25;' +
            `font-weight:800;letter-spacing:-0.01em;margin:0 0 16px">${h1}</h1>` +
            (intro
                ? '<p style="font-size:clamp(1rem,0.95rem+0.3vw,1.125rem);' +
                  `line-height:1.65;color:#9A9A9A;margin:0">${intro}</p>`
                : '') +
            NAV_BLOCK +
            '</div>' +
            '<script>document.getElementById("seo-prerender").style.display="none"</script>';
        html = html.replace('<div id="root"></div>', `<div id="root">${block}</div>`);
    }

    return html;
}

// Генерируем HTML для ВСЕХ routes из SEO_META, включая noindex. Для приватных
// routes (login/profile/billing/*) HTML содержит <meta name="robots" content="noindex, nofollow">
// — defense in depth даже если crawler как-то добрался до URL мимо robots.txt.
const routes = Object.entries(SEO_META);
const publicCount = routes.filter(([, m]) => !m.noindex).length;
console.log(`prerender-meta: ${routes.length} routes (${publicCount} публичных, ${routes.length - publicCount} noindex)`);

for (const [path, meta] of routes) {
    const html = renderRoute(path, meta);
    if (path === '/') {
        writeFileSync(TEMPLATE_PATH, html);
        console.log(`  ✓ / → dist/index.html`);
    } else {
        const dir = resolve(DIST, '.' + path);
        mkdirSync(dir, { recursive: true });
        writeFileSync(resolve(dir, 'index.html'), html);
        console.log(`  ✓ ${path} → dist${path}/index.html`);
    }
}
// ── Авто-версия Service Worker ───────────────────────────────────────────
// CACHE_NAME раньше бампился руками (frame-vNNN, ~366 коммитов истории). Теперь —
// хэш от имён файлов dist/assets (Vite content-hash'ит их → имена меняются только
// при реальной правке фронта). Бампится автоматически и ровно когда нужно.
try {
    const swPath = resolve(DIST, 'sw.js');
    const assetsDir = resolve(DIST, 'assets');
    if (existsSync(swPath) && existsSync(assetsDir)) {
        const names = readdirSync(assetsDir).sort().join('|');
        const version = `frame-${createHash('sha1').update(names).digest('hex').slice(0, 8)}`;
        const sw = readFileSync(swPath, 'utf-8');
        if (sw.includes('__SW_VERSION__')) {
            writeFileSync(swPath, sw.replace(/__SW_VERSION__/g, version));
            console.log(`prerender-meta: SW CACHE_NAME → ${version}`);
        } else {
            console.warn('prerender-meta: ⚠ __SW_VERSION__ не найден в dist/sw.js — SW не пере-версионирован');
        }
    }
} catch (e) {
    console.warn('prerender-meta: ⚠ SW versioning skipped:', e);
}

console.log(`prerender-meta: готово`);
