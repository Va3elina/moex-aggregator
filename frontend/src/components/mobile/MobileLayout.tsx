/**
 * MobileLayout — единый шаблон всех мобильных страниц-индикаторов.
 *
 * Структура:
 *   .fm-app
 *     ├── MobileTopBar         (sticky top)
 *     ├── .fm-main             (scrollable content, flex:1)
 *     │     └── {children}
 *     ├── .fm-page-actions     (FIXED bar — до 4 кнопок:
 *     │                          ★ Актив, 🕐 Время, ⚙️ Опции, ⛶ Экран)
 *     └── MobileBottomRail     (FIXED bottom — навигация по 8 индикаторам)
 *
 * Контракт по 4 слотам:
 *   - ★ АКТИВ      — выбор инструмента (тикер). Если нет — кнопка
 *                    не рендерится (Strength, Buffett, …).
 *   - 🕐 ВРЕМЯ     — периоды/таймфреймы/интервалы.
 *   - ⚙️ ОПЦИИ     — всё остальное (варианты, фильтры, режимы).
 *   - ⛶ ЭКРАН      — переключение в полноэкранный режим. Логика
 *                    управляется самим layout'ом (state isFullscreen).
 *
 * Sheet'ы остаются под управлением страницы — layout только триггерит
 * open. Это минимизирует blast radius и позволяет странице держать
 * собственные множественные sheet'ы.
 *
 * Полноэкранный режим: скрывает TopBar, PageHeader, page-actions
 * и BottomRail. Chart занимает весь viewport. Floating exit-кнопка
 * в углу выводит обратно.
 */
import { useState } from 'react';
import type { ReactNode } from 'react';
import { Clock, Settings, Star, Maximize2, Minimize2, RefreshCw } from 'lucide-react';
import MobileTopBar from './MobileTopBar';
import MobileBottomRail from './MobileBottomRail';
import MobilePWABanner from './MobilePWABanner';
import { usePullToRefresh } from '../../hooks/usePullToRefresh';
import '../../styles/mobile.css';

interface MobileLayoutProps {
  children: ReactNode;

  // ──────── ★ Актив ────────
  /** Клик по кнопке Актив (открывает asset search sheet). */
  onAssetClick?: () => void;
  /** Имя актива («Сбербанк»). */
  assetLabel?: string;
  /** Тикер актива («SR»). */
  assetTicker?: string;
  /** data-tour ID для Asset-кнопки. */
  assetTourId?: string;

  // ──────── 🕐 Время ────────
  /** Клик по кнопке Время (период / таймфрейм). */
  onTimeClick?: () => void;
  /** Короткая подпись: «6м · 1ч». */
  timeSummary?: string;
  timeTourId?: string;

  // ──────── ⚙️ Опции ────────
  /** Клик по кнопке Опции (фильтры, варианты, режимы). */
  onSettingsClick?: () => void;
  /** Короткая подпись: «Чистая позиция · Юр». */
  settingsSummary?: string;
  settingsTourId?: string;

  // ──────── ⛶ Экран ────────
  /** Включить полноэкранный режим (кнопка Maximize). По умолчанию false. */
  enableFullscreen?: boolean;
  fullscreenTourId?: string;

  // ──────── Pull-to-refresh ────────
  /** Callback при pull-down жесте сверху страницы. Если не передан —
   *  жест отключен. Обычно — функция перезагрузки данных страницы. */
  onRefresh?: () => Promise<void> | void;

  // ──────── Loading indicator ────────
  /** Если true — показывает тонкий accent-progress-bar сверху страницы
   *  (под TopBar). Используется для индикации загрузки данных индикатора. */
  loading?: boolean;

  // ──────── Back navigation ────────
  /** Если задан — заменяет логотип в TopBar на ← кнопку «Назад».
   *  Используется для secondary screens (Profile, Pricing, Methodology). */
  onBack?: () => void;
}

export default function MobileLayout({
  children,
  onAssetClick,
  assetLabel,
  assetTicker,
  assetTourId,
  onTimeClick,
  timeSummary,
  timeTourId,
  onSettingsClick,
  settingsSummary,
  settingsTourId,
  enableFullscreen = true,
  fullscreenTourId,
  onRefresh,
  loading = false,
  onBack,
}: MobileLayoutProps) {
  const [isFullscreen, setIsFullscreen] = useState(false);
  const hasActions =
    !!onAssetClick || !!onTimeClick || !!onSettingsClick || enableFullscreen;

  // Pull-to-refresh — активен только если передан onRefresh и не fullscreen
  const { pullDistance, isRefreshing, trigger } = usePullToRefresh(
    !isFullscreen ? onRefresh : undefined,
  );
  const isTriggered = pullDistance >= trigger;
  const indicatorVisible = pullDistance > 0 || isRefreshing;

  return (
    <div className={`fm-app ${isFullscreen ? 'fullscreen' : ''}`}>
      {!isFullscreen && <MobileTopBar onBack={onBack} />}

      {/* Top loading bar — тонкая полоска под TopBar когда страница refresh'ится.
          Indeterminate animation (как у Safari address bar при загрузке). */}
      {loading && !isFullscreen && <div className="fm-loading-bar" aria-hidden />}

      {/* Pull-to-refresh indicator — плавающий spinner сверху */}
      {indicatorVisible && (
        <div
          className="fm-pull-indicator"
          style={{
            transform: `translate(-50%, ${Math.min(pullDistance, 90)}px)`,
            opacity: Math.min(pullDistance / 40, 1),
          }}
        >
          <RefreshCw
            size={18}
            strokeWidth={2.4}
            className={
              isRefreshing
                ? 'fm-pull-spin'
                : isTriggered
                  ? 'fm-pull-ready'
                  : ''
            }
            style={{
              transform: !isRefreshing
                ? `rotate(${Math.min(pullDistance * 4, 360)}deg)`
                : undefined,
            }}
          />
        </div>
      )}

      <main className={`fm-main ${hasActions && !isFullscreen ? '' : 'no-actions'}`}>
        {children}
      </main>

      {hasActions && !isFullscreen && (
        <div className="fm-page-actions">
          {onAssetClick && (
            <button
              type="button"
              className="fm-page-action"
              onClick={onAssetClick}
              data-tour={assetTourId}
              aria-label={
                assetLabel ? `Актив: ${assetLabel}${assetTicker ? ` (${assetTicker})` : ''}` : 'Выбор актива'
              }
              title={assetLabel && assetTicker ? `${assetLabel} · ${assetTicker}` : assetLabel}
            >
              <span className="fm-rail-ico">
                <Star size={16} fill="var(--accent)" strokeWidth={0} />
              </span>
              <span>Актив</span>
            </button>
          )}

          {onTimeClick && (
            <button
              type="button"
              className="fm-page-action"
              onClick={onTimeClick}
              data-tour={timeTourId}
              aria-label={timeSummary ? `Время · ${timeSummary}` : 'Время / период'}
              title={timeSummary}
            >
              <span className="fm-rail-ico">
                <Clock size={16} strokeWidth={2.2} />
              </span>
              <span>Время</span>
            </button>
          )}

          {onSettingsClick && (
            <button
              type="button"
              className="fm-page-action"
              onClick={onSettingsClick}
              data-tour={settingsTourId}
              aria-label={settingsSummary ? `Опции · ${settingsSummary}` : 'Опции'}
              title={settingsSummary}
            >
              <span className="fm-rail-ico">
                <Settings size={16} strokeWidth={2.2} />
              </span>
              <span>Опции</span>
            </button>
          )}

          {enableFullscreen && (
            <button
              type="button"
              className="fm-page-action"
              onClick={() => setIsFullscreen(true)}
              data-tour={fullscreenTourId}
              aria-label="Полный экран"
            >
              <span className="fm-rail-ico">
                <Maximize2 size={16} strokeWidth={2.2} />
              </span>
              <span>Экран</span>
            </button>
          )}
        </div>
      )}

      {!isFullscreen && <MobileBottomRail />}

      {/* Floating exit-button когда в полноэкранном режиме */}
      {isFullscreen && (
        <button
          type="button"
          className="fm-fullscreen-exit"
          onClick={() => setIsFullscreen(false)}
          aria-label="Выйти из полного экрана"
        >
          <Minimize2 size={18} strokeWidth={2.4} />
        </button>
      )}

      {/* PWA install banner — показывается автоматически после 5 сек,
          если приложение можно установить и юзер не dismiss'ил */}
      {!isFullscreen && <MobilePWABanner />}
    </div>
  );
}
