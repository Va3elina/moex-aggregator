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
 *
 * Поворотный режим (4c): на телефоне (coarse pointer) поворот в ландшафт
 * АВТО-входит в fullscreen (TradingView-паттерн), поворот обратно — выходит.
 * State machine: manualFs (кнопка ⛶) + autoFs (ориентация) + optOutRef
 * (юзер вышел, оставаясь в ландшафте — не форсим обратно до следующей
 * смены ориентации). В fullscreen рендерятся overlay-контролы: контекст-
 * пилюля (актив · период · опции) слева-сверху и кнопки Время/Опции
 * справа-сверху — юзер не «заперт» без управления.
 */
import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { Clock, Settings, Star, Maximize2, Minimize2, RefreshCw } from 'lucide-react';
import MobileTopBar from './MobileTopBar';
import MobileBottomRail from './MobileBottomRail';
import MobilePWABanner from './MobilePWABanner';
import { usePullToRefresh } from '../../hooks/usePullToRefresh';
import { useIsLandscape } from '../../hooks/useIsLandscape';
import { isCoarsePointer } from '../../hooks/useIsPhone';
import InstrumentIcon from '../InstrumentIcon';
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
  /** Sectype для лого компании в кнопке (вместо звезды). Если задан — кнопка
   *  рисует InstrumentIcon + тикер/фьючерс (как на ПК); иначе ★ + «Актив». */
  assetSectype?: string;
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
  assetSectype,
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
  // ── Fullscreen state machine ──
  // manualFs — юзер нажал ⛶ (переживает поворот туда-обратно);
  // autoFs   — авто-вход по повороту в ландшафт (только телефон);
  // optOutRef — юзер вышел из fullscreen, ОСТАВАЯСЬ в ландшафте: не форсим
  //             авто-вход обратно до следующей смены ориентации (ref, не
  //             state — не должен вызывать re-render).
  const [manualFs, setManualFs] = useState(false);
  const [autoFs, setAutoFs] = useState(false);
  const optOutRef = useRef(false);
  const isLandscape = useIsLandscape();
  const isFullscreen = manualFs || autoFs;

  useEffect(() => {
    if (isLandscape) {
      // Авто-вход: только телефон (coarse pointer — иначе низкое окно на
      // десктопе свалилось бы в fullscreen), только страницы с графиком
      // (enableFullscreen), не во время онбординг-тура (fullscreen спрятал
      // бы data-tour якоря) и не после явного выхода юзера (optOut).
      if (
        isCoarsePointer() &&
        enableFullscreen &&
        !optOutRef.current &&
        !document.documentElement.hasAttribute('data-tour-active')
      ) {
        setAutoFs(true);
      }
    } else {
      // Поворот обратно в портрет: авто-режим снимаем, opt-out сбрасываем.
      // manualFs НЕ трогаем — ручной портретный fullscreen переживает
      // поворот туда-обратно.
      setAutoFs(false);
      optOutRef.current = false;
    }
  }, [isLandscape, enableFullscreen]);

  const exitFullscreen = () => {
    setManualFs(false);
    setAutoFs(false);
    // Выход в ландшафте = осознанный отказ от авто-режима до след. поворота.
    if (isLandscape) optOutRef.current = true;
  };

  const hasActions =
    !!onAssetClick || !!onTimeClick || !!onSettingsClick || enableFullscreen;

  // Контекст-пилюля в fullscreen: собираем из тех же props, что кормят
  // кнопки action-bar'а — юзер видит ЧТО он смотрит без выхода из fullscreen.
  const fsPillText = [assetTicker || assetLabel, timeSummary, settingsSummary]
    .filter(Boolean)
    .join(' · ');

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
              {assetSectype ? (
                <>
                  {/* Лого компании + последний фьючерс/тикер — один в один как на ПК */}
                  <InstrumentIcon sectype={assetSectype} size={20} rounded="full" />
                  <span style={{ maxWidth: '100%', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {assetTicker || assetLabel || 'Актив'}
                  </span>
                </>
              ) : (
                <>
                  <span className="fm-rail-ico">
                    <Star size={16} fill="var(--accent)" strokeWidth={0} />
                  </span>
                  <span>Актив</span>
                </>
              )}
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
                <Clock size={18} strokeWidth={2.2} />
              </span>
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
                <Settings size={18} strokeWidth={2.2} />
              </span>
            </button>
          )}

          {enableFullscreen && (
            <button
              type="button"
              className="fm-page-action"
              onClick={() => setManualFs(true)}
              data-tour={fullscreenTourId}
              aria-label="Полный экран"
            >
              <span className="fm-rail-ico">
                <Maximize2 size={18} strokeWidth={2.2} />
              </span>
            </button>
          )}
        </div>
      )}

      {!isFullscreen && <MobileBottomRail />}

      {/* Fullscreen overlay-контролы: контекст-пилюля слева-сверху,
          Время/Опции справа-сверху, exit справа-снизу. Всё z-index 50
          (< sheet 100) — sheets открываются поверх. Полупрозрачные в idle,
          как exit-кнопка — не мешают чтению графика. */}
      {isFullscreen && (
        <>
          {fsPillText &&
            (onAssetClick ? (
              <button
                type="button"
                className="fm-fs-pill"
                onClick={onAssetClick}
                aria-label={`Актив: ${fsPillText}`}
              >
                {assetSectype && <InstrumentIcon sectype={assetSectype} size={16} rounded="full" />}
                <span className="fm-fs-pill-text">{fsPillText}</span>
              </button>
            ) : (
              <div className="fm-fs-pill" role="status">
                {assetSectype && <InstrumentIcon sectype={assetSectype} size={16} rounded="full" />}
                <span className="fm-fs-pill-text">{fsPillText}</span>
              </div>
            ))}

          {(onTimeClick || onSettingsClick) && (
            <div className="fm-fs-controls">
              {onTimeClick && (
                <button
                  type="button"
                  className="fm-fs-btn"
                  onClick={onTimeClick}
                  aria-label={timeSummary ? `Время · ${timeSummary}` : 'Время / период'}
                >
                  <Clock size={18} strokeWidth={2.2} />
                </button>
              )}
              {onSettingsClick && (
                <button
                  type="button"
                  className="fm-fs-btn"
                  onClick={onSettingsClick}
                  aria-label={settingsSummary ? `Опции · ${settingsSummary}` : 'Опции'}
                >
                  <Settings size={18} strokeWidth={2.2} />
                </button>
              )}
            </div>
          )}

          <button
            type="button"
            className="fm-fullscreen-exit"
            onClick={exitFullscreen}
            aria-label="Выйти из полного экрана"
          >
            <Minimize2 size={18} strokeWidth={2.4} />
          </button>
        </>
      )}

      {/* PWA install banner — показывается последним (после cookie-consent и
          онбординг-тура), если приложение можно установить и юзер не dismiss'ил */}
      {!isFullscreen && <MobilePWABanner />}
    </div>
  );
}
