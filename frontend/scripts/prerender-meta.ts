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

import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
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
console.log(`prerender-meta: готово`);
