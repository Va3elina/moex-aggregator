import { useState, useEffect, useRef } from 'react';
import { Search, X, Star, ChevronUp, ChevronDown, ChevronsUpDown } from 'lucide-react';
import TickerLogo from '../TickerLogo';
import { assetTicker, assetColor } from '../../config/fundConfig';
import { formatCompact } from '../../utils/formatNumber';
import { useViewportWidth } from '../../hooks/useViewportWidth';

// Один актив в списке выбора. `key` — стабильный идентификатор строки
// (обычно isin || asset_name), `funds_count` — сколько фондов держат бумагу.
// `last_amount_rub` — суммарный объём в портфелях фондов (₽), `avg_weight_pct` —
// средний вес бумаги в портфелях (%). Поле avg_weight_pct опционально: бэкенд
// дополняется им параллельно, до его появления родитель может прислать объект без него.
export interface AssetPickerAsset {
  key: string;
  asset_name: string;
  isin: string | null;
  funds_count: number;
  last_amount_rub: number | null;
  avg_weight_pct?: number | null;
}

export interface AssetPickerModalProps {
  assets: AssetPickerAsset[];
  onSelect: (a: AssetPickerAsset) => void;
  onClose: () => void;
}

// Сортировка списка: активная колонка (объём / вес / число фондов) + направление.
type SortCol = 'volume' | 'weight' | 'funds';
type SortDir = 'asc' | 'desc';

// Ширины числовых колонок (px) — единый источник для сорт-заголовков И значений,
// чтобы они гарантированно стояли друг под другом, выровненные по правому краю.
const COL: Record<SortCol, number> = { volume: 110, weight: 56, funds: 66 };

/**
 * AssetPickerModal — окно выбора бумаги для «Покупок фондов».
 *
 * Стиль и структура зеркалят InstrumentSearchModal (editorial overlay + окно
 * с поиском, сорт-заголовками, избранным и списком). Активы приходят через
 * props (без загрузки из API и без tier-lock).
 *
 * Лого: assetTicker(asset_name) → <TickerLogo> (sprite), иначе цветная точка
 * по assetColor(asset_name). Справа — три числовые колонки: объём (₽),
 * средний вес (%) и «N фондов». Длинные имена обрезаются (ellipsis + title).
 * Клик по строке → onSelect(asset) + onClose(). Клик по звезде — toggle
 * избранного (localStorage 'favoriteFundTradeAssets', id = asset.key).
 */
export default function AssetPickerModal({ assets, onSelect, onClose }: AssetPickerModalProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [sortCol, setSortCol] = useState<SortCol>('volume');
  const [sortDir, setSortDir] = useState<SortDir>('desc');

  // Избранные из localStorage. Ключ — отдельное пространство имён для бумаг
  // фондов (id = asset.key = ISIN/имя), НЕ пересекается с favoriteInstruments
  // (тикеры фьючерсов на ОИ/Сезонности).
  const [favorites, setFavorites] = useState<string[]>(() => {
    const saved = localStorage.getItem('favoriteFundTradeAssets');
    return saved ? JSON.parse(saved) : [];
  });

  // Autofocus — только на desktop (mouse). На мобиле не дёргаем клавиатуру,
  // чтобы пользователь сначала увидел список (как в InstrumentSearchModal).
  // На мобиле ✕ — заметная кнопка с обводкой (как панель УК). Десктоп прежний.
  const isMobile = useViewportWidth() < 768;
  const inputRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    // Надёжная детекция тача (см. FundPicker): одна '(hover: none)' ненадёжна.
    const isTouch = window.matchMedia('(hover: none), (pointer: coarse)').matches
      || (navigator.maxTouchPoints ?? 0) > 0;
    if (!isTouch && inputRef.current) {
      inputRef.current.focus();
    }
  }, []);

  // Сохранение избранных
  useEffect(() => {
    localStorage.setItem('favoriteFundTradeAssets', JSON.stringify(favorites));
  }, [favorites]);

  // Esc закрывает окно.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const toggleFavorite = (key: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (favorites.includes(key)) {
      setFavorites(favorites.filter(k => k !== key));
    } else {
      setFavorites([...favorites, key]);
    }
  };

  // Фильтрация по имени (поиск нечувствителен к регистру).
  const q = searchQuery.trim().toLowerCase();
  const filtered = (q
    ? assets.filter((a) => a.asset_name.toLowerCase().includes(q))
    : assets
  ).slice().sort((a, b) => {
    const pick = (x: AssetPickerAsset) =>
      sortCol === 'volume' ? x.last_amount_rub
      : sortCol === 'weight' ? (x.avg_weight_pct ?? null)
      : x.funds_count;
    const av = pick(a);
    const bv = pick(b);
    // Активы без значения — всегда в конце, в обе стороны сортировки.
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    return sortDir === 'desc' ? bv - av : av - bv;
  });

  // При поиске — все бумаги в одном списке (избранные не прячутся).
  const favoriteAssets = q ? [] : filtered.filter((a) => favorites.includes(a.key));
  const regularAssets = q ? filtered : filtered.filter((a) => !favorites.includes(a.key));

  // Кликабельный заголовок-сортировки. Клик: если колонка активна — меняет
  // направление; иначе делает её активной (по убыванию). Иконка-индикатор есть
  // ВСЕГДА (⇅ для неактивных = «можно сортировать», ↑/↓ для активной) + hover —
  // чтобы было очевидно, что заголовки кликабельны.
  const renderSortHeader = (col: SortCol, label: string, hint: string) => {
    const active = sortCol === col;
    return (
      <button
        type="button"
        title={hint}
        onClick={() => {
          if (active) setSortDir((d) => (d === 'desc' ? 'asc' : 'desc'));
          else { setSortCol(col); setSortDir('desc'); }
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.background = 'color-mix(in srgb, var(--text-primary) 8%, transparent)';
          if (!active) e.currentTarget.style.color = 'var(--text-primary)';
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.background = 'transparent';
          e.currentTarget.style.color = active ? 'var(--accent)' : 'var(--text-secondary)';
        }}
        className="flex items-center justify-end uppercase font-bold whitespace-nowrap transition-colors"
        style={{
          gap: 3,
          width: COL[col],
          flexShrink: 0,
          padding: '5px 0',
          borderRadius: 6,
          fontSize: 'var(--fs-xs)',
          letterSpacing: '0.04em',
          color: active ? 'var(--accent)' : 'var(--text-secondary)',
          cursor: 'pointer',
        }}
      >
        {/* Иконка СЛЕВА от текста: правый край текста = правый край чисел ниже. */}
        {active
          ? (sortDir === 'desc'
              ? <ChevronDown size={13} strokeWidth={2.5} />
              : <ChevronUp size={13} strokeWidth={2.5} />)
          : <ChevronsUpDown size={13} style={{ opacity: 0.5 }} />}
        {label}
      </button>
    );
  };

  const renderItem = (asset: AssetPickerAsset) => {
    const ticker = assetTicker(asset.asset_name);
    const color = assetColor(asset.asset_name);
    const isFavorite = favorites.includes(asset.key);

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

        {/* Числовые колонки справа (ширины COL — под сорт-заголовками, по правому
            краю). Активная колонка сортировки — ярче (text-primary, bold). */}
        {([
          ['volume', asset.last_amount_rub != null ? `${formatCompact(asset.last_amount_rub)} ₽` : '—'],
          ['weight', asset.avg_weight_pct != null ? `${asset.avg_weight_pct.toFixed(1)}%` : '—'],
          ['funds', String(asset.funds_count)],
        ] as [SortCol, string][]).map(([c, text]) => (
          <span
            key={c}
            className="flex-shrink-0 text-right"
            style={{
              width: COL[c],
              fontSize: 'var(--fs-sm)',
              fontVariantNumeric: 'tabular-nums',
              fontWeight: sortCol === c ? 700 : 600,
              color: sortCol === c ? 'var(--text-primary)' : 'var(--text-secondary)',
            }}
          >
            {text}
          </span>
        ))}

        {/* Star — toggle избранного, stopPropagation чтобы не выбрать актив */}
        <button
          onClick={(e) => toggleFavorite(asset.key, e)}
          className="p-2 transition-colors flex-shrink-0"
          style={{ color: isFavorite ? 'var(--accent)' : 'var(--text-muted)' }}
          aria-label={isFavorite ? 'Убрать из избранных' : 'Добавить в избранные'}
        >
          <Star size={20} fill={isFavorite ? 'currentColor' : 'transparent'} />
        </button>
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
              className="instrument-modal-close transition-colors"
              style={isMobile ? {
                display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                width: 34, height: 34, flexShrink: 0, borderRadius: 8,
                border: '1.5px solid var(--text-primary)',
                background: 'var(--bg-primary)', color: 'var(--text-primary)',
              } : { color: 'var(--text-secondary)', padding: 8, borderRadius: 8 }}
              aria-label="Закрыть"
            >
              <X size={isMobile ? 18 : 24} strokeWidth={isMobile ? 2.4 : 2} />
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

        {/* Results — со sticky-шапкой колонок ВНУТРИ скролл-контейнера: общий
            скроллбар (scrollbar-gutter stable) + одинаковые с строками отступы/
            gap/ширины → заголовки и значения гарантированно в одной сетке. */}
        <div
          className="overflow-y-auto max-h-[calc(80vh-240px)] px-6 pb-6 styled-scrollbar"
          style={{ scrollbarGutter: 'stable' }}
        >
          {/* Sticky-шапка колонок — кликабельная сортировка, зеркалит строку списка
              ([лого-спейсер 24] · gap · [Бумага flex-1] · cols · [звезда-спейсер 36]) */}
          <div
            className="sticky top-0 z-10 flex items-center gap-3.5 px-3 pt-1 pb-2.5 mb-2"
            style={{ backgroundColor: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-color)' }}
          >
            <span
              className="flex-1 uppercase font-bold"
              style={{ fontSize: 'var(--fs-xs)', letterSpacing: '0.04em', color: 'var(--text-secondary)' }}
            >
              Бумага
            </span>
            {renderSortHeader('volume', 'Объём', 'Суммарный объём бумаги в портфелях фондов, ₽')}
            {renderSortHeader('weight', 'Вес', 'Средний вес бумаги в портфелях фондов, %')}
            {renderSortHeader('funds', 'Фонды', 'Сколько фондов держат бумагу')}
            <span style={{ width: 36, flexShrink: 0 }} aria-hidden="true" />
          </div>

          {favoriteAssets.length === 0 && regularAssets.length === 0 ? (
            <div className="py-12 text-center" style={{ color: 'var(--text-secondary)' }}>
              Ничего не найдено
            </div>
          ) : (
            <>
              {/* Favorites */}
              {favoriteAssets.length > 0 && !q && (
                <div className="mb-6">
                  <h3
                    className="text-xs font-semibold uppercase tracking-wider mb-4 pl-3"
                    style={{ color: 'var(--text-secondary)' }}
                  >
                    Избранные
                  </h3>
                  <div className="instrument-list">
                    {favoriteAssets.map(renderItem)}
                  </div>
                </div>
              )}

              {/* Divider */}
              {!q && favoriteAssets.length > 0 && regularAssets.length > 0 && (
                <div className="h-px mb-6" style={{ backgroundColor: 'var(--text-primary)', opacity: 0.15 }} />
              )}

              {/* Regular */}
              {regularAssets.length > 0 && (
                <div className="instrument-list">
                  {regularAssets.map(renderItem)}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
