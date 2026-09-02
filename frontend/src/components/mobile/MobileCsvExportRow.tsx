/**
 * MobileCsvExportRow — строка «Скачать данные» в конце шита «Опции» мобильных
 * страниц индикаторов. Мобильный аналог CsvExportButton (ПК): в мобильной
 * панели действий 4 фиксированных слота (Актив/Время/Опции/Экран), пятого
 * под экспорт нет — поэтому вход в экспорт живёт в шите «Опции».
 *
 * Клик: сначала закрываем «Опции» (onClose), затем
 *   - Free/Basic → UpgradeModal (pre-gate, без сетевого запроса);
 *   - Pro/admin  → onOpen() — страница открывает MobileCsvExportSheet.
 *
 * Сам шит экспорта рендерит СТРАНИЦА, а не эта строка: MobileSheet
 * размонтирует детей при закрытии, вложенный шит исчез бы вместе с «Опциями».
 *
 * Лежит СНАРУЖИ padded-контейнера шита (между ним и </MobileSheet>) —
 * отступы свои, см. .fm-export-row в mobile.css.
 */
import { ChevronRight, Download, Lock } from 'lucide-react';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { CSV_EXPORT_ENABLED } from '../../config/features';

interface Props {
  /** Indicator ID для UpgradeModal context. */
  indicator: string;
  /** Закрыть шит «Опции» — вызывается при любом клике. */
  onClose: () => void;
  /** Открыть шит экспорта — только когда экспорт доступен по тарифу. */
  onOpen: () => void;
}

export default function MobileCsvExportRow({ indicator, onClose, onOpen }: Props) {
  const common = useCommonFeatures();
  const { showUpgrade } = useUpgradePrompt();

  // KILL-SWITCH экспорта (config/features.ts) — тот же, что у CsvExportButton.
  if (!CSV_EXPORT_ENABLED) return null;

  const locked = !common.csv_export;

  const handleClick = () => {
    onClose();
    if (locked) {
      showUpgrade({ tier: 'pro', featureName: 'экспорт в CSV', indicator });
      return;
    }
    onOpen();
  };

  return (
    <button
      type="button"
      className="fm-export-row"
      onClick={handleClick}
      aria-label={locked ? 'Скачать данные — доступно на тарифе Pro' : 'Скачать данные'}
    >
      <span className="fm-export-row-icon">
        <Download size={16} strokeWidth={2.2} />
      </span>
      <span className="fm-export-row-text">
        <span className="fm-export-row-title">
          Скачать данные
          {locked && <Lock size={12} strokeWidth={2.2} />}
        </span>
        <span className="fm-export-row-sub">
          {locked ? 'CSV или Excel · тариф Pro' : 'CSV или Excel'}
        </span>
      </span>
      <ChevronRight size={16} strokeWidth={2.2} className="fm-export-row-chevron" />
    </button>
  );
}
