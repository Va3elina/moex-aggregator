import { useState, useEffect, useRef, useMemo } from 'react';
import { Search, X, Star, ChevronUp, ChevronDown, ChevronsUpDown } from 'lucide-react';
import InstrumentIcon from '../InstrumentIcon';
import { resolveFundTicker, fundAssetName, fundAssetColor } from '../../config/fundConfig';
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
  sec_type?: string | null;
  // Крупная корзина для табов: share|bond|fund|currency|commodity|index|other.
  // Опционально: бэкенд добавляет его, до появления — строка попадёт в 'other'.
  category?: string | null;
  funds_count: number;
  last_amount_rub: number | null;
  avg_weight_pct?: number | null;
}

export interface AssetPickerModalProps {
  assets: AssetPickerAsset[];
  onSelect: (a: AssetPickerAsset) => void;
  onClose: () => void;
}

// Сортировка списка. По образцу Сезонности оставлена ОДНА колонка — объём (₽);
// вес и число фондов убраны из таблицы (объём — основной интересующий показатель).
type SortCol = 'volume';
type SortDir = 'asc' | 'desc';

// Ширина колонки объёма (px) — единый источник для сорт-заголовка И значений,
// чтобы они гарантированно стояли друг под другом, выровненные по правому краю.
const COL: Record<SortCol, number> = { volume: 96 };

// Корзина-категория бумаги (приходит с бэкенда) → подпись таба. Порядок = порядок
// табов слева направо; показываем ТОЛЬКО непустые корзины + «Все» всегда последней.
// 'share' — дефолтный таб: «Потоки по компании» в первую очередь про акции, а
// облигации/ОФЗ/фонды засоряли список (см. securities_ref.sec_type).
const CAT_LABELS: Record<string, string> = {
  share: 'Акции',
  bond: 'Облигации',
  fund: 'Фонды',
  currency: 'Валюта',
  commodity: 'Сырьё',
  index: 'Индексы',
  other: 'Прочее',
};
const CAT_ORDER = ['share', 'bond', 'fund', 'currency', 'commodity', 'index', 'other'];
const normCat = (a: AssetPickerAsset) => a.category ?? 'other';

/**
 * AssetPickerModal — окно выбора бумаги для «Покупок фондов».
 *
 * Стиль и структура зеркалят InstrumentSearchModal (editorial overlay + окно
 * с поиском, сорт-заголовками, избранным и списком). Активы приходят через
 * props (без загрузки из API и без tier-lock).
 *
 * Лого: resolveFundTicker(asset_name, isin) → <InstrumentIcon> (как в Сезонности/
 * ОИ), иначе цветная точка по fundAssetColor. Справа — одна колонка объёма (₽)
 * (по образцу Сезонности; вес/число фондов убраны). Имена обрезаются (ellipsis + title).
 * Клик по строке → onSelect(asset) + onClose(). Клик по звезде — toggle
 * избранного (localStorage 'favoriteFundTradeAssets', id = asset.key).
 */
export default function AssetPickerModal({ assets, onSelect, onClose }: AssetPickerModalProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [sortCol, setSortCol] = useState<SortCol>('volume');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  // Активный таб-категория. Дефолт 'share' (Акции) — чистый список без облигаций/
  // ОФЗ/фондов. 'all' = показать все корзины.
  const [activeCat, setActiveCat] = useState<string>('share');

  // Сколько бумаг в каждой корзине (по полному списку, не зависит от поиска) +
  // какие корзины присутствуют → набор табов. Табы строим только для непустых.
  const { catCounts, presentCats } = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const a of assets) counts[normCat(a)] = (counts[normCat(a)] ?? 0) + 1;
    return { catCounts: counts, presentCats: CAT_ORDER.filter((c) => counts[c] > 0) };
  }, [assets]);

  // Эффективная категория: если дефолтный 'share' пуст (или выбранный таб исчез
  // после смены данных) — откатываемся на 'all', чтобы пикер не оказался пустым.
  const effectiveCat =
    activeCat === 'all' || (catCounts[activeCat] ?? 0) > 0 ? activeCat : 'all';

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

  // Сначала корзина-категория (таб), затем поиск по имени (нечувствителен к регистру).
  const base = effectiveCat === 'all' ? assets : assets.filter((a) => normCat(a) === effectiveCat);
  const q = searchQuery.trim().toLowerCase();
  const filtered = (q
    ? base.filter((a) =>
        a.asset_name.toLowerCase().includes(q) ||
        fundAssetName(a.asset_name, a.isin).toLowerCase().includes(q))
    : base
  ).slice().sort((a, b) => {
    const av = a.last_amount_rub;
    const bv = b.last_amount_rub;
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
          e.currentTarget.style.color = 'var(--text-primary)';
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.background = 'transparent';
          e.currentTarget.style.color = 'var(--text-secondary)';
        }}
        className="flex items-center justify-end uppercase font-bold whitespace-nowrap transition-colors"
        style={{
          gap: 3,
          // Правый край шапки = правый край чисел ниже (значения — width COL[col]).
          // minWidth, а не width: длинная подпись «Суммарный объём в фондах»
          // расширяет кнопку ВЛЕВО (за счёт «Бумага» flex-1), не ломая выравнивание.
          minWidth: COL[col],
          flexShrink: 0,
          padding: '5px 0',
          borderRadius: 6,
          fontSize: 'var(--fs-xs)',
          letterSpacing: '0.04em',
          // Сортировка тут единственная (по объёму) → колонка всегда «активна».
          // Не подсвечиваем её акцентом (оранжевым): нейтральный text-secondary,
          // иначе большая подпись постоянно горит ярким цветом без смысла.
          color: 'var(--text-secondary)',
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
    // Унификация с Сезонностью: резолвим бумагу → каноничный тикер (по ISIN),
    // каноничное имя и фирменный цвет. Лого — через InstrumentIcon (тот же путь,
    // что в ОИ/Сезонности: STOCK_LOGO_OVERRIDE → стикерпак → /logos/<тикер>.png).
    const ticker = resolveFundTicker(asset.asset_name, asset.isin);
    const color = fundAssetColor(asset.asset_name, asset.isin);
    const displayName = fundAssetName(asset.asset_name, asset.isin);
    const isFavorite = favorites.includes(asset.key);

    return (
      <div
        key={asset.key}
        onClick={() => {
          onSelect(asset);
          onClose();
        }}
        className="instrument-item flex items-center gap-3 px-3 py-1 rounded-lg transition-colors"
        style={{ color: 'var(--text-primary)', cursor: 'pointer' }}
      >
        {/* Лого: InstrumentIcon по резолвнутому тикеру (акция), иначе — цветная точка
            по фирменному цвету бумаги (облигации/ОФЗ/денежный рынок без тикера). */}
        {ticker ? (
          <InstrumentIcon sectype={ticker} size={28} rounded="full" />
        ) : (
          <span
            className="flex-shrink-0 rounded-full"
            style={{
              width: 28,
              height: 28,
              backgroundColor: color || 'var(--text-muted)',
            }}
            aria-hidden="true"
          />
        )}

        {/* Имя + тикер — как в Сезонности: имя жирное (fs-sm), резолвнутый тикер
            (SBER/GAZP…) серый рядом (fs-xs). У бумаг без тикера — только имя. */}
        <div className="flex items-baseline gap-1.5 flex-1 min-w-0">
          <span className="font-bold truncate" style={{ fontSize: 'var(--fs-sm)' }} title={displayName}>
            {displayName}
          </span>
          {ticker && (
            <span className="flex-shrink-0" style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)' }}>
              {ticker}
            </span>
          )}
        </div>

        {/* Колонка объёма (₽) — единственная, под сорт-заголовком, по правому краю
            (вес/число фондов убраны по образцу Сезонности). */}
        <span
          className="flex-shrink-0 text-right"
          style={{
            width: COL.volume,
            fontSize: 'var(--fs-sm)',
            fontVariantNumeric: 'tabular-nums',
            fontWeight: 700,
            color: 'var(--text-primary)',
          }}
        >
          {asset.last_amount_rub != null ? `${formatCompact(asset.last_amount_rub)} ₽` : '—'}
        </span>

        {/* Star — toggle избранного, stopPropagation чтобы не выбрать актив */}
        <button
          onClick={(e) => toggleFavorite(asset.key, e)}
          className="p-1.5 transition-colors flex-shrink-0"
          style={{ color: isFavorite ? 'var(--accent)' : 'var(--text-muted)' }}
          aria-label={isFavorite ? 'Убрать из избранных' : 'Добавить в избранные'}
        >
          <Star size={16} fill={isFavorite ? 'currentColor' : 'transparent'} />
        </button>
      </div>
    );
  };

  return (
    <div className="instrument-modal-root fixed inset-0 z-50 flex items-start justify-center p-4 pt-8 sm:pt-10">
      {/* Backdrop — solid dim без backdrop-blur (editorial: no glass effects). */}
      <div
        className="absolute inset-0"
        style={{ backgroundColor: 'rgba(0,0,0,0.6)' }}
        onClick={onClose}
      />

      {/* Окно — bg-secondary + 2px border text-primary + hard shadow. Flex-колонка
          + max-h-[90vh] (как в InstrumentSearchModal): список тянется на всю
          доступную высоту, а не упирается в фикс. calc → окно не «укороченное». */}
      <div
        className="instrument-modal relative w-full max-w-xl rounded-2xl max-h-[90vh] overflow-hidden flex flex-col"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '2px solid var(--text-primary)',
          boxShadow: 'var(--shadow-hard-chip, 6px 6px 0 var(--text-primary))',
          color: 'var(--text-primary)',
        }}
      >
        {/* Header — заголовок «Выбор бумаги» убран (поиск самоочевиден). Поиск
            (flex-1) + «×» в одном ряду — как в Сезонности (InstrumentSearchModal). */}
        <div className="px-6 pt-6 pb-3 flex-shrink-0">
          <div className="flex items-center gap-3">
            {/* Search — outline 2px text-primary, компактный (py-2.5) как в Сезонности */}
            <div className="relative flex-1 min-w-0">
              <Search
                size={18}
                className="absolute left-3.5 top-1/2 -translate-y-1/2"
                style={{ color: 'var(--text-secondary)' }}
              />
              <input
                ref={inputRef}
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Поиск актива"
                className="instrument-modal-search w-full pl-11 pr-4 py-2.5 text-sm rounded-xl focus:outline-none transition-colors"
                style={{
                  backgroundColor: 'var(--bg-primary)',
                  color: 'var(--text-primary)',
                  border: '2px solid var(--text-primary)',
                }}
              />
            </div>
            <button
              onClick={onClose}
              className="instrument-modal-close transition-colors flex-shrink-0"
              style={isMobile ? {
                display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                width: 34, height: 34, borderRadius: 8,
                border: '1.5px solid var(--text-primary)',
                background: 'var(--bg-primary)', color: 'var(--text-primary)',
              } : { color: 'var(--text-secondary)', padding: 8, borderRadius: 8, marginRight: -8 }}
              aria-label="Закрыть"
            >
              <X size={isMobile ? 18 : 22} strokeWidth={isMobile ? 2.4 : 2} />
            </button>
          </div>

          {/* Табы-категории (показываем только если корзин > 1). Дефолт «Акции» —
              чистый список; облигации/ОФЗ/фонды доступны по клику. «Все» — последняя. */}
          {presentCats.length > 1 && (
            <div className="flex flex-wrap gap-2 mt-3">
              {[...presentCats, 'all'].map((cat) => {
                const isActive = effectiveCat === cat;
                const label = cat === 'all' ? 'Все' : (CAT_LABELS[cat] ?? cat);
                return (
                  <button
                    key={cat}
                    type="button"
                    onClick={() => setActiveCat(cat)}
                    className="instrument-modal-chip px-4 py-2 text-sm font-semibold rounded-full transition-colors"
                    style={{
                      backgroundColor: isActive ? 'var(--accent)' : 'var(--bg-secondary)',
                      color: isActive ? 'var(--text-inverse)' : 'var(--text-primary)',
                      border: `2px solid ${isActive ? 'var(--accent)' : 'var(--text-primary)'}`,
                      boxShadow: isActive ? 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))' : undefined,
                    }}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {/* Results — со sticky-шапкой колонок ВНУТРИ скролл-контейнера: общий
            скроллбар (scrollbar-gutter stable) + одинаковые с строками отступы/
            gap/ширины → заголовки и значения гарантированно в одной сетке. */}
        <div
          className="flex-1 min-h-0 overflow-y-auto px-6 pb-6 styled-scrollbar"
          style={{ scrollbarGutter: 'stable' }}
        >
          {/* Sticky-шапка колонок — кликабельная сортировка, зеркалит строку списка
              ([лого-спейсер 24] · gap · [Бумага flex-1] · cols · [звезда-спейсер 36]) */}
          <div
            className="sticky top-0 z-10 flex items-center gap-3 px-3 pt-1 pb-2.5 mb-2"
            style={{ backgroundColor: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-color)' }}
          >
            <span
              className="flex-1 uppercase font-bold"
              style={{ fontSize: 'var(--fs-xs)', letterSpacing: '0.04em', color: 'var(--text-secondary)' }}
            >
              Бумага
            </span>
            {renderSortHeader('volume', 'Суммарный объём в фондах', 'Суммарный объём бумаги в портфелях фондов, ₽')}
            <span style={{ width: 28, flexShrink: 0 }} aria-hidden="true" />
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
