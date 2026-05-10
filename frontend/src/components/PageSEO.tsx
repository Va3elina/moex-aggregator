/**
 * PageSEO — устанавливает per-page meta tags через @unhead/react.
 *
 * Использование:
 *   <PageSEO />          // авто-резолв из useLocation()
 *   <PageSEO path="/buffett" />  // явный override (для редких случаев)
 *
 * Что делает:
 *   - <title> и <meta description/keywords>
 *   - <link rel="canonical"> — указатель на каноническую версию URL
 *   - Open Graph + Twitter Card meta для соцсетей
 *   - JSON-LD BreadcrumbList — для появления хлебных крошек в выдаче Google
 *   - <meta name="robots"> noindex для приватных страниц
 *
 * Base meta (общие на всех страницах) живёт в index.html — здесь только
 * per-page переопределения. Unhead умеет dedupe meta по name/property,
 * поэтому конфликтов не возникает.
 */

import { useLocation } from 'react-router-dom'
import { useHead } from '@unhead/react'
import { CANONICAL_HOST, getSeoMeta } from '../config/seoMeta'

interface Props {
    /** Override pathname; по умолчанию берётся из react-router useLocation */
    path?: string
}

export default function PageSEO({ path }: Props) {
    const location = useLocation()
    const pathname = path ?? location.pathname
    const meta = getSeoMeta(pathname)
    const canonical = `${CANONICAL_HOST}${pathname === '/' ? '/' : pathname}`

    // BreadcrumbList JSON-LD — Google рисует «Главная > Индикаторы > Баффет»
    // прямо в результатах поиска. Появляется без BreadcrumbList — никогда.
    const breadcrumbItems: Array<{ name: string; url: string }> = [
        { name: 'Главная', url: CANONICAL_HOST + '/' },
    ]
    if (meta.breadcrumb && pathname !== '/') {
        // Категория без своей страницы — просто текстовая опора
        breadcrumbItems.push({ name: meta.breadcrumb, url: canonical })
        breadcrumbItems.push({ name: meta.title.split(' | ')[0].split(' — ')[0], url: canonical })
    } else if (pathname !== '/') {
        breadcrumbItems.push({ name: meta.title.split(' | ')[0].split(' — ')[0], url: canonical })
    }

    const breadcrumbJsonLd = {
        '@context': 'https://schema.org',
        '@type': 'BreadcrumbList',
        itemListElement: breadcrumbItems.map((b, i) => ({
            '@type': 'ListItem',
            position: i + 1,
            name: b.name,
            item: b.url,
        })),
    }

    useHead({
        title: meta.title,
        meta: [
            { name: 'description', content: meta.description },
            ...(meta.keywords ? [{ name: 'keywords', content: meta.keywords }] : []),
            // robots: noindex для приватных страниц, index/follow для публичных
            {
                name: 'robots',
                content: meta.noindex ? 'noindex, nofollow' : 'index, follow, max-image-preview:large',
            },
            // Open Graph
            { property: 'og:title', content: meta.title },
            { property: 'og:description', content: meta.description },
            { property: 'og:url', content: canonical },
            { property: 'og:type', content: 'website' },
            // Twitter Card
            { name: 'twitter:title', content: meta.title },
            { name: 'twitter:description', content: meta.description },
        ],
        link: [{ rel: 'canonical', href: canonical }],
        // BreadcrumbList показывается только для не-noindex и не-главной
        ...(!meta.noindex && pathname !== '/'
            ? {
                  script: [
                      {
                          type: 'application/ld+json',
                          // Unhead принимает innerHTML/children — строкой
                          innerHTML: JSON.stringify(breadcrumbJsonLd),
                      },
                  ],
              }
            : {}),
    })

    return null
}
