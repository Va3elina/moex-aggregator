import { useState, useEffect } from 'react';
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

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center p-4 pt-20">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/80 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal — glassmorphism */}
      <div className="relative w-full max-w-xl bg-white/[0.08] backdrop-blur-xl rounded-2xl border border-white/15 shadow-2xl max-h-[78vh] overflow-hidden" style={{ boxShadow: '0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.1)' }}>
        {/* Header */}
        <div className="px-6 pt-6 pb-4">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-2xl font-bold text-[#F4F6FA]">Выбор актива</h2>
            <button
              onClick={onClose}
              className="p-2 text-[#A7ADBC] hover:text-[#F4F6FA] hover:bg-white/5 rounded-lg transition-colors"
            >
              <X size={24} />
            </button>
          </div>

          {/* Search */}
          <div className="relative">
            <Search size={20} className="absolute left-4 top-1/2 -translate-y-1/2 text-[#A7ADBC]" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Поиск актива"
              className="w-full pl-12 pr-4 py-4 bg-transparent text-[#F4F6FA] text-base rounded-xl border border-[#C8FF2E] focus:outline-none transition-colors placeholder:text-[#A7ADBC]/50"
              autoFocus
            />
          </div>

          {/* Категории — крупнее для удобства касания на mobile + читаемости */}
          {!onlyGroups && (
          <div className="flex gap-2 mt-5 flex-wrap">
            {CATEGORY_FILTERS.map(cat => (
              <button
                key={cat.key}
                onClick={() => setCategoryFilter(cat.key)}
                className={`px-4 py-2 text-sm font-semibold rounded-full transition-colors ${
                  categoryFilter === cat.key
                    ? 'bg-[#C8FF2E] text-[#0B0D12]'
                    : 'bg-white/[0.06] text-[#A7ADBC] hover:bg-white/10 hover:text-[#F4F6FA]'
                }`}
              >
                {cat.label}
              </button>
            ))}
          </div>
          )}
        </div>

        {/* Results */}
        <div className="overflow-y-auto max-h-[calc(78vh-220px)] px-6 pb-6 styled-scrollbar">
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <div className="w-8 h-8 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
            </div>
          ) : (
            <>
              {/* Favorites */}
              {favoriteInstruments.length > 0 && searchQuery === '' && (
                <div className="mb-6">
                  <h3 className="text-xs font-semibold text-[#A7ADBC] uppercase tracking-wider mb-4">
                    Избранные
                  </h3>
                  <div className="space-y-0.5">
                    {favoriteInstruments.map((inst) => (
                      <div
                        key={inst.sectype}
                        onClick={() => onSelect(inst.sectype, inst.name)}
                        className="flex items-center gap-3.5 px-3 py-2.5 rounded-lg hover:bg-white/5 transition-colors cursor-pointer"
                      >
                        <InstrumentIcon sectype={inst.sectype} size={36} />
                        <span className="font-bold text-[15px] text-[#F4F6FA] flex-shrink-0 mr-1.5">{inst.sectype}</span>
                        <span className="text-[13px] text-[#7A8194] truncate flex-1">{inst.name}</span>
                        <button
                          onClick={(e) => toggleFavorite(inst.sectype, e)}
                          className="p-2 text-[#FFB020]"
                        >
                          <Star size={20} fill="#FFB020" />
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Divider */}
              {searchQuery === '' && favoriteInstruments.length > 0 && regularInstruments.length > 0 && (
                <div className="h-px bg-white/10 mb-6" />
              )}

              {/* Regular */}
              {regularInstruments.length === 0 && favoriteInstruments.length === 0 ? (
                <div className="py-12 text-center text-[#A7ADBC]">
                  Ничего не найдено
                </div>
              ) : (
                <div className="space-y-0.5">
                  {regularInstruments.map((inst) => (
                    <div
                      key={inst.sectype}
                      onClick={() => onSelect(inst.sectype, inst.name)}
                      className="flex items-center gap-3.5 px-3 py-2.5 rounded-lg hover:bg-white/5 transition-colors cursor-pointer"
                    >
                      <InstrumentIcon sectype={inst.sectype} size={36} />
                      <span className="font-bold text-[15px] text-[#F4F6FA] flex-shrink-0 mr-1.5">{inst.sectype}</span>
                      <span className="text-[13px] text-[#7A8194] truncate flex-1">{inst.name}</span>
                      <button
                        onClick={(e) => toggleFavorite(inst.sectype, e)}
                        className="p-2 text-[#5E6576] hover:text-[#FFB020] transition-colors"
                      >
                        <Star size={20} />
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}