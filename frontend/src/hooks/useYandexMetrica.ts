/**
 * useYandexMetrica — SPA-tracking хук для Yandex.Metrica.
 *
 * Yandex.Metrica при initial load делает первый hit автоматически (через
 * `ym(id, 'init', {...})` в index.html). Но в SPA переходы между роутами
 * НЕ перезагружают страницу, и Metrica их НЕ видит — без этого хука вся
 * статистика бы крутилась вокруг главной.
 *
 * Хук подписывается на изменения `location.pathname` через react-router
 * useLocation() и вызывает `ym(id, 'hit', url, { title, referer })` при
 * каждом переходе. Это эквивалент того что делает обычный <a href> переход.
 *
 * Counter ID: 109137033 (захардкожен в index.html и здесь — должны совпадать).
 *
 * Также экспортируется helper `trackEvent(name, params)` для бизнес-событий
 * (клик «Plus», экспорт графика, активация промо). Используй явно из
 * компонентов: trackEvent('export_chart', { indicator: 'buffett' }).
 */

import { useEffect, useRef } from 'react'
import { useLocation } from 'react-router-dom'

const YM_COUNTER_ID = 109137033

declare global {
    interface Window {
        ym?: (counterId: number, action: string, ...args: unknown[]) => void
    }
}

/**
 * Вызывать ОДИН раз в Layout. На каждый change pathname отправляет hit в Metrica.
 *
 * Не делает первый hit при mount — он уже отправлен в `init` callback из
 * index.html. Skip предотвращает дублирование первого визита.
 */
export function useYandexMetrica() {
    const location = useLocation()
    const isFirstHitRef = useRef(true)

    useEffect(() => {
        // Скип первого хита — он отправлен через ym(...,'init',...) в index.html
        if (isFirstHitRef.current) {
            isFirstHitRef.current = false
            return
        }
        if (typeof window === 'undefined' || !window.ym) return

        const url = window.location.href
        // 'hit' — стандартное событие Metrica для SPA. Передаём title и referer
        // чтобы статистика выглядела как при обычной HTML-навигации.
        window.ym(YM_COUNTER_ID, 'hit', url, {
            title: document.title,
            referer: document.referrer,
        })
    }, [location.pathname, location.search])
}

/**
 * Передаёт в Метрику внутренний номер аккаунта (без email и имени), чтобы
 * записи Вебвизора и отчёты можно было отфильтровать по конкретному
 * пользователю: Вебвизор → фильтр «Параметры посетителей» → UserID.
 * Раскрыто в политике обработки данных (раздел «Кому передаём»).
 * Вызывать один раз в Layout. Гостям ничего не шлёт.
 */
export function useYandexMetricaUser(userId: number | null | undefined) {
    const sentRef = useRef<number | null>(null)
    useEffect(() => {
        if (!userId || sentRef.current === userId) return
        if (typeof window === 'undefined' || !window.ym) return
        sentRef.current = userId
        window.ym(YM_COUNTER_ID, 'setUserID', String(userId))
        window.ym(YM_COUNTER_ID, 'userParams', { UserID: userId })
    }, [userId])
}

/**
 * Бизнес-событие. Цели в Metrica настраиваются через интерфейс:
 *   Цели → Добавить → JavaScript-событие → имя совпадает с `name`.
 *
 * Примеры использования:
 *   trackEvent('plus_click')                                — клик на кнопку Plus в header
 *   trackEvent('export_chart', { indicator: 'buffett' })   — успешный экспорт графика
 *   trackEvent('promo_activated', { tier: 'plus' })        — активация инвайт-кода
 */
export function trackEvent(name: string, params?: Record<string, unknown>) {
    if (typeof window === 'undefined' || !window.ym) return
    if (params) {
        window.ym(YM_COUNTER_ID, 'reachGoal', name, params)
    } else {
        window.ym(YM_COUNTER_ID, 'reachGoal', name)
    }
}
