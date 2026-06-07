/**
 * IndicatorShell — презентационная обвязка («рамка») для desktop-страниц
 * индикаторов. Выносит повторяющийся каркас, который сейчас дублируется
 * на 7 страницах (Buffett/Strength/Seasonality/Heatmap/OpenInterest/
 * FundsMoney/CbrFlows):
 *
 *   <div container>
 *     <PageHeader … />
 *     <div className="editorial-frame">{controls}{chart}</div>
 *     {footer: блоки-описания + OnboardingTour}
 *   </div>
 *
 * ВАЖНО — Shell владеет ТОЛЬКО хромом (контейнер + editorial-frame + размещение
 * footer). Он НЕ инкапсулирует data-логику страниц: loadData, useMemo чарт-данных,
 * usePersistedState, а также измерительные chartAnchorRef/useFitToViewport
 * остаются в самих страницах. Контролы/график/хедер передаются как непрозрачные
 * слоты (ReactNode) — Shell не знает про Dropdown, tier-lock или props SimpleChart.
 *
 * Это standalone-компонент (шаг 1 рефактора). Страницы НЕ переведены на него —
 * пилот (BuffettPage) и раскатка идут отдельными коммитами после хук-рефактора.
 *
 * Mobile (pages/mobile/*) и embed (pages/embed/*) — ДРУГОЙ UX, сюда не подключаются.
 */
import type { ReactNode, Ref } from 'react';

interface IndicatorShellProps {
    /** Шапка страницы — обычно <PageHeader … />. Передаётся как слот целиком,
     *  чтобы Shell не зависел от формы props PageHeader. */
    header: ReactNode;
    /** Ряд контролов (dropdown'ы, переключатели, CSV/Camera через ml-auto).
     *  Рендерится первым внутри editorial-frame. */
    controls: ReactNode;
    /** График. Обычно <div ref={chartAnchorRef}><SimpleChart … /></div> —
     *  измерительный ref остаётся в странице. */
    chart: ReactNode;
    /** Опциональный «подвал»: блоки-описания + <OnboardingTour …/>.
     *  Рендерится ПОСЛЕ editorial-frame (вне рамки). */
    footer?: ReactNode;
    /** Переопределение класса внешнего контейнера (напр. Seasonality использует
     *  более широкий max-width). По умолчанию — стандартный 1408px-контейнер. */
    containerClassName?: string;
    /** Переопределение класса рамки (по умолчанию 'editorial-frame'). */
    frameClassName?: string;
    /** Ref на обёртку хедера — нужен OI для измерения высоты sticky-шапки
     *  (watchRefs). Если задан, header оборачивается в <div ref={headerRef}>. */
    headerRef?: Ref<HTMLDivElement>;
}

const DEFAULT_CONTAINER =
    'max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen';

export default function IndicatorShell({
    header,
    controls,
    chart,
    footer,
    containerClassName,
    frameClassName = 'editorial-frame',
    headerRef,
}: IndicatorShellProps) {
    return (
        <div className={containerClassName ?? DEFAULT_CONTAINER}>
            {headerRef ? <div ref={headerRef}>{header}</div> : header}

            {/* Editorial frame — обнимает controls + chart в один контейнер */}
            <div className={frameClassName}>
                {controls}
                {chart}
            </div>

            {footer}
        </div>
    );
}
