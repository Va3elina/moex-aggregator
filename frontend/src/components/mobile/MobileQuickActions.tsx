/**
 * MobileQuickActions — компактная строка контролов под графиком.
 *
 * Иконочные кнопки заменяют десктопный ряд из 5-7 dropdown'ов:
 *   - Asset chip (звезда + логотип + название + тикер + chevron) — главный
 *   - Time icon (период + интервал) — открывает sheet
 *   - Options icon (3 lines) — открывает sheet с режимом / вариантом
 *   - Fullscreen icon — full-bleed chart
 *   - Export icon — PNG экспорт
 *
 * Каждая иконка может быть скрыта через showXxx prop, чтобы не показывать
 * irrelevant действия (например, на Heatmap нет fullscreen).
 */
import { Star, Clock, Settings, Maximize2, Download, ChevronDown } from 'lucide-react';
import type { ReactNode } from 'react';

interface AssetInfo {
  name: string;
  ticker: string;
  /** Логотип-иконка слева (опционально — буква, картинка, эмодзи). */
  logo?: ReactNode;
}

interface MobileQuickActionsProps {
  asset?: AssetInfo;
  /** Подпись на иконке времени (показывается в title tooltip). */
  timeLabel?: string;
  /** Подпись опций (показывается в title tooltip). */
  optionsLabel?: string;
  onAsset?: () => void;
  onTime?: () => void;
  onOptions?: () => void;
  onFullscreen?: () => void;
  onExport?: () => void;
  /** Видимость иконок (по умолчанию все видны). */
  showAsset?: boolean;
  showTime?: boolean;
  showOptions?: boolean;
  showFullscreen?: boolean;
  showExport?: boolean;
}

export default function MobileQuickActions({
  asset,
  timeLabel,
  optionsLabel,
  onAsset,
  onTime,
  onOptions,
  onFullscreen,
  onExport,
  showAsset = true,
  showTime = true,
  showOptions = true,
  showFullscreen = true,
  showExport = true,
}: MobileQuickActionsProps) {
  return (
    <div className="fm-quickactions">
      {showAsset && asset && (
        <button className="fm-qa asset" onClick={onAsset} aria-label={`Актив: ${asset.name}`}>
          <span className="fm-qa-star">
            {asset.logo ?? <Star size={14} fill="currentColor" strokeWidth={0} />}
          </span>
          <span className="fm-qa-label">
            <span className="fm-qa-name">{asset.name}</span>
            <span className="fm-qa-ticker">{asset.ticker}</span>
          </span>
          <ChevronDown size={12} strokeWidth={2.6} style={{ marginLeft: 'auto', flexShrink: 0 }} />
        </button>
      )}
      {showTime && (
        <button
          className="fm-qa icon-only"
          onClick={onTime}
          aria-label={timeLabel ? `Время · ${timeLabel}` : 'Время'}
          title={timeLabel}
        >
          <Clock size={18} strokeWidth={2.2} />
        </button>
      )}
      {showOptions && (
        <button
          className="fm-qa icon-only"
          onClick={onOptions}
          aria-label={optionsLabel ? `Опции · ${optionsLabel}` : 'Опции'}
          title={optionsLabel}
        >
          <Settings size={18} strokeWidth={2.2} />
        </button>
      )}
      {showFullscreen && (
        <button
          className="fm-qa icon-only"
          onClick={onFullscreen}
          aria-label="Полный экран"
        >
          <Maximize2 size={18} strokeWidth={2.2} />
        </button>
      )}
      {showExport && (
        <button
          className="fm-qa icon-only"
          onClick={onExport}
          aria-label="Экспорт графика"
        >
          <Download size={18} strokeWidth={2.2} />
        </button>
      )}
    </div>
  );
}
