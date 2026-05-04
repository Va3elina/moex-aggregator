import { useState, useEffect, useRef } from 'react';
import { Search, X, Star } from 'lucide-react';
import InstrumentIcon from './InstrumentIcon';

interface Instrument {
  sec_id: string;
  sectype: string;
  name: string;
  type: string;
  group?: string;
  daily_volume?: number;
}

const CATEGORY_FILTERS = [
  { key: 'all', label: 'Все' },
  { key: 'Акции', label: 'Акции' },
  { key: 'Индексы', label: 'Индексы' },
  { key: 'Валюта', label: 'Валюта' },
  { key: 'Сырьё', label: 'Сырьё' },
];

interface InstrumentSearchModalProps {
  onSelect: (sectype: string, name: string) => void;
  onClose: () => void;
  filterType?: 'stock' | 'futures';
  excludeType?: string;
  onlyGroups?: string[];
}


// InstrumentIcon + INSTRUMENT_ICONS + FUT_TO_STOCK перенесены в
// отдельный модуль ./InstrumentIcon.tsx, общий для всех страниц.

export default function InstrumentSearchModal({ onSelect, onClose, filterType, excludeType, onlyGroups }: InstrumentSearchModalProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [instruments, setInstruments] = useState<Instrument[]>([]);
  const [loading, setLoading] = useState(true);
  const [categoryFilter, setCategoryFilter] = useState('all');

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
    const matchesCategory = categoryFilter === 'all' || inst.group === categoryFilter;
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
  .sort((a, b) => (b.daily_volume || 0) - (a.daily_volume || 0));

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

  // Один render для items — переиспользуется в favorites и regular списках.
  const renderItem = (inst: Instrument) => {
    const isFavorite = favorites.includes(inst.sectype);
    return (
      <div
        key={inst.sectype}
        onClick={() => onSelect(inst.sectype, inst.name)}
        className="instrument-item flex items-center gap-3.5 px-3 py-2.5 rounded-lg cursor-pointer transition-colors"
        style={{ color: 'var(--text-primary)' }}
      >
        <InstrumentIcon sectype={inst.sectype} size={32} />
        <span className="font-bold flex-shrink-0 mr-1.5" style={{ fontSize: 'var(--fs-sm)' }}>{inst.sectype}</span>
        <span className="truncate flex-1" style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)' }}>{inst.name}</span>
        <button
          onClick={(e) => toggleFavorite(inst.sectype, e)}
          className="p-2 transition-colors"
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

        {/* Results */}
        <div className="overflow-y-auto max-h-[calc(78vh-220px)] px-6 pb-6 styled-scrollbar">
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
                    className="text-xs font-semibold uppercase tracking-wider mb-4"
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
