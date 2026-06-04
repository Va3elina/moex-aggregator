import { useState, useEffect, useRef } from 'react';
import { Search, X, Star, Lock, ChevronUp, ChevronDown, ChevronsUpDown } from 'lucide-react';
import InstrumentIcon from './InstrumentIcon';
import { formatCompact } from '../utils/formatNumber';
import { useAnalytics } from '../contexts/AnalyticsContext';
import { useTierAccess } from '../contexts/TierFeaturesContext';
import { useUpgradePrompt } from './tier/UpgradeModal';

interface Instrument {
  sec_id: string;
  sectype: string;
  name: string;
  type: string;
  group?: string;
  daily_volume?: number;
  day_change_pct?: number | null;
}

const CATEGORY_FILTERS = [
  { key: 'all', label: 'Все' },
  { key: 'Акции', label: 'Акции' },
  { key: 'Индексы', label: 'Индексы' },
  { key: 'Валюта', label: 'Валюта' },
  { key: 'Сырьё', label: 'Сырьё' },
];

// Сортировка списка: активная колонка (изменение / объём) + направление.
type SortCol = 'change' | 'volume';
type SortDir = 'asc' | 'desc';

// Ширины числовых колонок (px) — единый источник для сорт-заголовков И значений,
// чтобы они стояли строго друг под другом (выровнены по правому краю).
const COL: Record<SortCol, number> = { change: 76, volume: 74 };

interface InstrumentSearchModalProps {
  onSelect: (sectype: string, name: string) => void;
  onClose: () => void;
  filterType?: 'stock' | 'futures';
  excludeType?: string;
  onlyGroups?: string[];
  /** Если задан — для каждого инструмента проверяем доступность по tier'у.
   *  Заблокированные затемняются + lock icon + клик открывает UpgradeModal. */
  indicator?: string;
}


// InstrumentIcon + INSTRUMENT_ICONS + FUT_TO_STOCK перенесены в
// отдельный модуль ./InstrumentIcon.tsx, общий для всех страниц.

export default function InstrumentSearchModal({ onSelect, onClose, filterType, excludeType, onlyGroups, indicator }: InstrumentSearchModalProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [instruments, setInstruments] = useState<Instrument[]>([]);
  const [loading, setLoading] = useState(true);
  const [categoryFilter, setCategoryFilter] = useState('all');
  const [sortCol, setSortCol] = useState<SortCol>('volume');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const { track } = useAnalytics();

  // Tier-gating: если передан indicator — проверяем доступ для каждого актива.
  // Если нет — useTierAccess всё равно вызывается (rules of hooks), но не используется.
  const tierAccess = useTierAccess(indicator || '');
  const { showUpgrade } = useUpgradePrompt();

  // Track wrapper — отдельная функция чтобы не дублировать в renderItem.
  const handleSelect = (sectype: string, name: string) => {
    track('instrument_select', {
      secid: sectype,
      from: typeof window !== 'undefined' ? window.location.pathname : null,
    });
    onSelect(sectype, name);
  };

  // Избранные из localStorage
  const [favorites, setFavorites] = useState<string[]>(() => {
    const saved = localStorage.getItem('favoriteInstruments');
    return saved ? JSON.parse(saved) : ['SR', 'GZ', 'MX'];
  });

  // Autofocus — только на desktop (mouse), чтобы на мобиле сразу не вылетала
  // клавиатура и пользователь мог сначала просмотреть категории/избранные.
  const inputRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    const isTouch = window.matchMedia('(hover: none)').matches;
    if (!isTouch && inputRef.current) {
      inputRef.current.focus();
    }
  }, []);

  // Сохранение избранных
  useEffect(() => {
    localStorage.setItem('favoriteInstruments', JSON.stringify(favorites));
  }, [favorites]);

  // Загрузка инструментов из API
  useEffect(() => {
    async function load() {
      try {
        const url = filterType ? `/api/instruments?type=${filterType}` : '/api/instruments';
        const resp = await fetch(url);
        const data = await resp.json();
        setInstruments(data.instruments || []);
      } catch (err) {
        console.error('Ошибка загрузки:', err);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  // Фильтрация по поиску и категории
  const filteredInstruments = instruments.filter(inst => {
    if (excludeType && inst.type === excludeType) return false;
    if (onlyGroups && !onlyGroups.includes(inst.group || '')) return false;
    const matchesSearch = !searchQuery ||
      inst.sectype.toLowerCase().includes(searchQuery.toLowerCase()) ||
      inst.name.toLowerCase().includes(searchQuery.toLowerCase());
    // matchesCategory: основной путь — group (backend проставляет
    // 'Акции'/'Валюта'/'Индексы'/'Сырьё'). Fallback для категории «Акции» —
    // type='stock', на случай если у инструмента группа не заполнена
    // (видели 2 такие записи на проде → попадали в "не найдено").
    const matchesCategory =
      categoryFilter === 'all' ||
      inst.group === categoryFilter ||
      (categoryFilter === 'Акции' && inst.type === 'stock');
    return matchesSearch && matchesCategory;
  });

  // Уникальные по sectype (убираем дубликаты контрактов)
  const uniqueInstruments = filteredInstruments.reduce((acc, inst) => {
    const existing = acc.find(i => i.sectype === inst.sectype);
    if (!existing) {
      acc.push(inst);
    } else if ((inst.daily_volume || 0) > (existing.daily_volume || 0)) {
      // Берём запись с бОльшим объёмом (актуальный контракт)
      acc[acc.indexOf(existing)] = inst;
    }
    return acc;
  }, [] as Instrument[])
  .sort((a, b) => {
    const av = sortCol === 'change' ? a.day_change_pct : a.daily_volume;
    const bv = sortCol === 'change' ? b.day_change_pct : b.daily_volume;
    // Активы без значения — всегда в конце, в обе стороны сортировки.
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    return sortDir === 'desc' ? bv - av : av - bv;
  });

  // При поиске — все инструменты в одном списке (избранные не прячутся)
  const favoriteInstruments = searchQuery ? [] : uniqueInstruments.filter(inst => favorites.includes(inst.sectype));
  const regularInstruments = searchQuery ? uniqueInstruments : uniqueInstruments.filter(inst => !favorites.includes(inst.sectype));

  const toggleFavorite = (sectype: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (favorites.includes(sectype)) {
      setFavorites(favorites.filter(t => t !== sectype));
    } else {
      setFavorites([...favorites, sectype]);
    }
  };

  // Кликабельный заголовок-сортировки. Иконка-индикатор ВСЕГДА (⇅ для неактивных —
  // «можно сортировать», ↑/↓ для активной) + hover-подсветка — очевидно, что
  // заголовки кликабельны. Ширина = COL[col] → значения стоят строго под ними.
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
        {/* Иконка СЛЕВА от текста: текст прижат к правому краю колонки → его
            правый край совпадает с правым краем чисел под заголовком. */}
        {active
          ? (sortDir === 'desc'
              ? <ChevronDown size={13} strokeWidth={2.5} />
              : <ChevronUp size={13} strokeWidth={2.5} />)
          : <ChevronsUpDown size={13} style={{ opacity: 0.5 }} />}
        {label}
      </button>
    );
  };

  // Один render для items — переиспользуется в favorites и regular списках.
  const renderItem = (inst: Instrument) => {
    const isFavorite = favorites.includes(inst.sectype);

    // Tier-gating: проверяем доступ к активу. Если indicator не задан или
    // матрица ещё грузится → считаем accessible (graceful fallback).
    const accessible = !indicator || tierAccess.isLoading
      ? true
      : tierAccess.canAccessAsset(inst.sectype);
    const requiredTier = !accessible ? tierAccess.requiredTierFor({ asset: inst.sectype }) : null;

    const handleClick = () => {
      if (accessible) {
        handleSelect(inst.sectype, inst.name);
      } else if (requiredTier && indicator) {
        // Закрываем instrument-модалку и открываем UpgradeModal — иначе
        // получается две full-screen модалки наложенных друг на друга.
        onClose();
        showUpgrade({
          tier: requiredTier,
          featureName: `актив ${inst.name} (${inst.sectype})`,
          indicator,
        });
      }
    };

    return (
      <div
        key={inst.sectype}
        onClick={handleClick}
        className="instrument-item flex items-center gap-3.5 px-3 py-2.5 rounded-lg transition-colors"
        style={{
          color: 'var(--text-primary)',
          cursor: accessible ? 'pointer' : 'not-allowed',
        }}
        title={!accessible && requiredTier
          ? `Доступно на тарифе ${requiredTier === 'basic' ? 'Basic' : 'Pro'}`
          : undefined}
      >
        {/* Иконка (32) — отдельный flex-child, зеркалит спейсер в шапке */}
        <span
          style={{ flexShrink: 0, lineHeight: 0, opacity: accessible ? 1 : 0.45, filter: accessible ? undefined : 'grayscale(0.5)' }}
        >
          <InstrumentIcon sectype={inst.sectype} size={32} />
        </span>

        {/* Актив: тикер + название (flex-1) */}
        <div
          className="flex items-baseline gap-1.5 flex-1 min-w-0"
          style={{ opacity: accessible ? 1 : 0.45 }}
        >
          <span className="font-bold flex-shrink-0" style={{ fontSize: 'var(--fs-sm)' }}>{inst.sectype}</span>
          <span className="truncate" style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)' }}>{inst.name}</span>
        </div>

        {/* Изм. % — семантический цвет (зелёный/красный). Активная колонка bold. */}
        <span
          className="flex-shrink-0 text-right"
          style={{
            width: COL.change,
            fontSize: 'var(--fs-sm)',
            fontVariantNumeric: 'tabular-nums',
            fontWeight: sortCol === 'change' ? 700 : 600,
            opacity: accessible ? 1 : 0.45,
            color: inst.day_change_pct == null
              ? 'var(--text-muted)'
              : inst.day_change_pct >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)',
          }}
        >
          {inst.day_change_pct != null ? `${inst.day_change_pct >= 0 ? '+' : ''}${inst.day_change_pct.toFixed(2)}%` : '—'}
        </span>

        {/* Объём — нейтральный; активная колонка ярче (text-primary bold) */}
        <span
          className="flex-shrink-0 text-right"
          style={{
            width: COL.volume,
            fontSize: 'var(--fs-sm)',
            fontVariantNumeric: 'tabular-nums',
            fontWeight: sortCol === 'volume' ? 700 : 600,
            opacity: accessible ? 1 : 0.45,
            color: inst.daily_volume
              ? (sortCol === 'volume' ? 'var(--text-primary)' : 'var(--text-secondary)')
              : 'var(--text-muted)',
          }}
        >
          {inst.daily_volume ? formatCompact(inst.daily_volume) : '—'}
        </span>

        {/* Lock-слот (фикс. 18px — звезда не смещается между заблок./доступными) */}
        <span style={{ width: 18, flexShrink: 0, display: 'flex', justifyContent: 'center' }}>
          {!accessible && (
            <Lock size={15} strokeWidth={2.2} style={{ color: 'var(--text-muted)' }} aria-label="Доступно на повышенном тарифе" />
          )}
        </span>

        {/* Star — всегда кликабельна (можно добавить в избранное даже заблок. актив) */}
        <button
          onClick={(e) => toggleFavorite(inst.sectype, e)}
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

      {/* Modal — editorial pill в светлой / glass в dark, через CSS-overrides.
          Базово: bg-secondary + 2px border + hard shadow. */}
      <div
        className="instrument-modal relative w-full max-w-xl rounded-2xl max-h-[78vh] overflow-hidden"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '2px solid var(--text-primary)',
          boxShadow: 'var(--shadow-hard-chip, 6px 6px 0 var(--text-primary))',
          color: 'var(--text-primary)',
        }}
      >
        {/* Header */}
        <div className="px-6 pt-6 pb-4">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-2xl font-bold" style={{ color: 'var(--text-primary)' }}>Выбор актива</h2>
            <button
              onClick={onClose}
              className="instrument-modal-close p-2 rounded-lg transition-colors"
              style={{ color: 'var(--text-secondary)' }}
              aria-label="Закрыть"
            >
              <X size={24} />
            </button>
          </div>

          {/* Search — outline 2px text-primary в editorial / accent в dark */}
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
              placeholder="Поиск актива"
              className="instrument-modal-search w-full pl-12 pr-4 py-4 text-base rounded-xl focus:outline-none transition-colors"
              style={{
                backgroundColor: 'var(--bg-primary)',
                color: 'var(--text-primary)',
                border: '2px solid var(--text-primary)',
              }}
            />
          </div>

          {/* Категории — chip pills */}
          {!onlyGroups && (
          <div className="flex gap-2 mt-5 flex-wrap">
            {CATEGORY_FILTERS.map(cat => {
              const active = categoryFilter === cat.key;
              return (
                <button
                  key={cat.key}
                  onClick={() => setCategoryFilter(cat.key)}
                  className="instrument-modal-chip px-4 py-2 text-sm font-semibold rounded-full transition-colors"
                  style={{
                    backgroundColor: active ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                    border: '2px solid var(--text-primary)',
                    boxShadow: active ? 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))' : undefined,
                  }}
                >
                  {cat.label}
                </button>
              );
            })}
          </div>
          )}
        </div>

        {/* Results — sticky-шапка колонок ВНУТРИ скролла: общий скроллбар
            (scrollbar-gutter stable) + одинаковые с строками отступы/gap/ширины
            → заголовки и значения гарантированно в одной сетке. */}
        <div
          className="overflow-y-auto max-h-[calc(78vh-220px)] px-6 pb-6 styled-scrollbar"
          style={{ scrollbarGutter: 'stable' }}
        >
          {/* Sticky-шапка — кликабельная сортировка, зеркалит строку списка
              ([иконка 32]·gap·[Актив flex-1]·[Изм.]·[Объём]·[lock 18]·[звезда 36]) */}
          {!loading && (
            <div
              className="sticky top-0 z-10 flex items-center gap-3.5 px-3 pt-1 pb-2.5 mb-2"
              style={{ backgroundColor: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-color)' }}
            >
              <span
                className="flex-1 uppercase font-bold"
                style={{ fontSize: 'var(--fs-xs)', letterSpacing: '0.04em', color: 'var(--text-secondary)' }}
              >
                Актив
              </span>
              {renderSortHeader('change', 'Изм. %', 'Изменение цены за торговый день, %')}
              {renderSortHeader('volume', 'Объём', 'Объём торгов за день, ₽')}
              <span style={{ width: 18, flexShrink: 0 }} aria-hidden="true" />
              <span style={{ width: 36, flexShrink: 0 }} aria-hidden="true" />
            </div>
          )}
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <div
                className="w-8 h-8 border-2 rounded-full animate-spin"
                style={{
                  borderColor: 'var(--accent)',
                  borderTopColor: 'transparent',
                }}
              />
            </div>
          ) : (
            <>
              {/* Favorites */}
              {favoriteInstruments.length > 0 && searchQuery === '' && (
                <div className="mb-6">
                  <h3
                    className="text-xs font-semibold uppercase tracking-wider mb-4 pl-3"
                    style={{ color: 'var(--text-secondary)' }}
                  >
                    Избранные
                  </h3>
                  <div className="instrument-list">
                    {favoriteInstruments.map(renderItem)}
                  </div>
                </div>
              )}

              {/* Divider */}
              {searchQuery === '' && favoriteInstruments.length > 0 && regularInstruments.length > 0 && (
                <div className="h-px mb-6" style={{ backgroundColor: 'var(--text-primary)', opacity: 0.15 }} />
              )}

              {/* Regular */}
              {regularInstruments.length === 0 && favoriteInstruments.length === 0 ? (
                <div className="py-12 text-center" style={{ color: 'var(--text-secondary)' }}>
                  Ничего не найдено
                </div>
              ) : (
                <div className="instrument-list">
                  {regularInstruments.map(renderItem)}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
