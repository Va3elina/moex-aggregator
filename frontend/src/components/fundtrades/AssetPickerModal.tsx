import { useState, useEffect, useRef } from 'react';
import { Search, X } from 'lucide-react';
import TickerLogo from '../TickerLogo';
import { assetTicker, assetColor } from '../../config/fundConfig';

// Один актив в списке выбора. `key` — стабильный идентификатор строки
// (обычно isin || asset_name), `funds_count` — сколько фондов держат бумагу.
export interface AssetPickerAsset {
  key: string;
  asset_name: string;
  isin: string | null;
  funds_count: number;
}

export interface AssetPickerModalProps {
  assets: AssetPickerAsset[];
  onSelect: (a: AssetPickerAsset) => void;
  onClose: () => void;
}

/**
 * AssetPickerModal — окно выбора бумаги для «Покупок фондов».
 *
 * Стиль и структура зеркалят InstrumentSearchModal (editorial overlay + окно
 * с поиском и списком), но без tier-lock / сортировок / избранного / категорий
 * и без загрузки из API — активы приходят через props.
 *
 * Лого: assetTicker(asset_name) → <TickerLogo> (sprite), иначе цветная точка
 * по assetColor(asset_name). Справа — «N фондов». Длинные имена обрезаются
 * (ellipsis + title). Клик по строке → onSelect(asset) + onClose().
 */
export default function AssetPickerModal({ assets, onSelect, onClose }: AssetPickerModalProps) {
  const [searchQuery, setSearchQuery] = useState('');

  // Autofocus — только на desktop (mouse). На мобиле не дёргаем клавиатуру,
  // чтобы пользователь сначала увидел список (как в InstrumentSearchModal).
  const inputRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    const isTouch = window.matchMedia('(hover: none)').matches;
    if (!isTouch && inputRef.current) {
      inputRef.current.focus();
    }
  }, []);

  // Esc закрывает окно.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  // Фильтрация по имени (поиск нечувствителен к регистру).
  const q = searchQuery.trim().toLowerCase();
  const filtered = q
    ? assets.filter((a) => a.asset_name.toLowerCase().includes(q))
    : assets;

  const renderItem = (asset: AssetPickerAsset) => {
    const ticker = assetTicker(asset.asset_name);
    const color = assetColor(asset.asset_name);

    return (
      <div
        key={asset.key}
        onClick={() => {
          onSelect(asset);
          onClose();
        }}
        className="instrument-item flex items-center gap-3.5 px-3 py-2.5 rounded-lg transition-colors"
        style={{ color: 'var(--text-primary)', cursor: 'pointer' }}
      >
        {/* Лого: спрайт по тикеру, иначе цветная точка по фирменному цвету бумаги */}
        {ticker ? (
          <TickerLogo ticker={ticker} size={24} rounded="full" />
        ) : (
          <span
            className="flex-shrink-0 rounded-full"
            style={{
              width: 24,
              height: 24,
              backgroundColor: color || 'var(--text-muted)',
            }}
            aria-hidden="true"
          />
        )}

        {/* Имя бумаги — обрезается ellipsis, полное имя в title */}
        <span
          className="truncate flex-1 font-semibold"
          style={{ fontSize: 'var(--fs-sm)' }}
          title={asset.asset_name}
        >
          {asset.asset_name}
        </span>

        {/* «N фондов» — справа */}
        <span
          className="flex-shrink-0"
          style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)' }}
        >
          {asset.funds_count} фондов
        </span>
      </div>
    );
  };

  return (
    <div className="instrument-modal-root fixed inset-0 z-50 flex items-start justify-center p-4 pt-20">
      {/* Backdrop — solid dim без backdrop-blur (editorial: no glass effects). */}
      <div
        className="absolute inset-0"
        style={{ backgroundColor: 'rgba(0,0,0,0.6)' }}
        onClick={onClose}
      />

      {/* Окно — bg-secondary + 2px border text-primary + hard shadow. */}
      <div
        className="instrument-modal relative w-full rounded-2xl max-h-[80vh] overflow-hidden"
        style={{
          maxWidth: 560,
          backgroundColor: 'var(--bg-secondary)',
          border: '2px solid var(--text-primary)',
          boxShadow: 'var(--shadow-hard-chip, 6px 6px 0 var(--text-primary))',
          color: 'var(--text-primary)',
        }}
      >
        {/* Header */}
        <div className="px-6 pt-6 pb-4">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-2xl font-bold" style={{ color: 'var(--text-primary)' }}>
              Выбор бумаги
            </h2>
            <button
              onClick={onClose}
              className="instrument-modal-close p-2 rounded-lg transition-colors"
              style={{ color: 'var(--text-secondary)' }}
              aria-label="Закрыть"
            >
              <X size={24} />
            </button>
          </div>

          {/* Search */}
          <div className="relative">
            <Search
              size={20}
              className="absolute left-4 top-1/2 -translate-y-1/2"
              style={{ color: 'var(--text-secondary)' }}
            />
            <input
              ref={inputRef}
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Поиск бумаги"
              className="instrument-modal-search w-full pl-12 pr-4 py-4 text-base rounded-xl focus:outline-none transition-colors"
              style={{
                backgroundColor: 'var(--bg-primary)',
                color: 'var(--text-primary)',
                border: '2px solid var(--text-primary)',
              }}
            />
          </div>
        </div>

        {/* Results */}
        <div className="overflow-y-auto max-h-[calc(80vh-180px)] px-6 pb-6 styled-scrollbar">
          {filtered.length === 0 ? (
            <div className="py-12 text-center" style={{ color: 'var(--text-secondary)' }}>
              Ничего не найдено
            </div>
          ) : (
            <div className="instrument-list">{filtered.map(renderItem)}</div>
          )}
        </div>
      </div>
    </div>
  );
}
