import { useState, useEffect } from 'react';
import { Search, X, Star } from 'lucide-react';

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

// Генерация цвета из строки
const stringToColor = (str: string) => {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  const colors = [
    '#2EE59D', '#4DA3FF', '#9D4DFF', '#FF4D4D', '#FFB020',
    '#00D9FF', '#FF6B9D', '#FCD34D', '#14B8A6', '#F97316',
    '#06B6D4', '#3B82F6', '#F59E0B', '#8B5CF6', '#EC4899',
    '#84CC16', '#6366F1', '#A855F7', '#22C55E', '#EF4444'
  ];
  return colors[Math.abs(hash) % colors.length];
};

const INSTRUMENT_ICONS: Record<string, { icon: string; bg: string; color: string }> = {
  'Si': { icon: '$/₽', bg: '#2563EB', color: '#fff' },
  'Eu': { icon: '€/₽', bg: '#1D4ED8', color: '#fff' },
  'CR': { icon: '¥/₽', bg: '#DC2626', color: '#fff' },
  'CNYRUBF': { icon: '¥/₽', bg: '#DC2626', color: '#fff' },
  'ED': { icon: '€/$', bg: '#7C3AED', color: '#fff' },
  'USDRUBF': { icon: '$/₽', bg: '#2563EB', color: '#fff' },
  'EURRUBF': { icon: '€/₽', bg: '#1D4ED8', color: '#fff' },
  'BR': { icon: '🛢', bg: '#92400E', color: '#fff' },
  'BM': { icon: '🛢', bg: '#92400E', color: '#fff' },
  'NG': { icon: '🔥', bg: '#EA580C', color: '#fff' },
  'GZ': { icon: 'ГП', bg: '#0EA5E9', color: '#fff' },
  'GD': { icon: 'Au', bg: '#D97706', color: '#fff' },
  'GL': { icon: 'Au', bg: '#D97706', color: '#fff' },
  'SV': { icon: 'Ag', bg: '#6B7280', color: '#fff' },
  'PT': { icon: 'Pt', bg: '#9CA3AF', color: '#000' },
  'PD': { icon: 'Pd', bg: '#A3A3A3', color: '#000' },
  'AL': { icon: '💎', bg: '#2DD4BF', color: '#000' },
  'CE': { icon: 'Cu', bg: '#B45309', color: '#fff' },
  'RI': { icon: 'РТС', bg: '#6366F1', color: '#fff' },
  'MX': { icon: 'МБ', bg: '#6366F1', color: '#fff' },
  'SR': { icon: 'Сб', bg: '#21A038', color: '#fff' },
  'GK': { icon: 'ЛК', bg: '#E11D48', color: '#fff' },
  'YD': { icon: 'Я', bg: '#FC3F1D', color: '#fff' },
  'TT': { icon: 'Т', bg: '#FFDD2D', color: '#000' },
  'VB': { icon: 'ВТБ', bg: '#009FDF', color: '#fff' },
  'NK': { icon: 'НН', bg: '#F59E0B', color: '#000' },
  'RN': { icon: 'РН', bg: '#FFD700', color: '#000' },
  'TN': { icon: 'ТН', bg: '#16A34A', color: '#fff' },
  'NA': { icon: 'НВ', bg: '#0284C7', color: '#fff' },
  'SF': { icon: 'СН', bg: '#1E3A5F', color: '#fff' },
  'AF': { icon: 'АФ', bg: '#0369A1', color: '#fff' },
  'SE': { icon: 'СС', bg: '#475569', color: '#fff' },
  // Индексы
  'IMOEX': { icon: 'МБ', bg: '#6366F1', color: '#fff' },
  'RTSI': { icon: 'РТС', bg: '#8B5CF6', color: '#fff' },
  'MCFTR': { icon: 'МП', bg: '#6366F1', color: '#fff' },
  'RGBITR': { icon: 'ОФЗ', bg: '#0EA5E9', color: '#fff' },
  'RGBI': { icon: 'ОФЗ', bg: '#0284C7', color: '#fff' },
  'RVI': { icon: 'σ', bg: '#EF4444', color: '#fff' },
  'RUSFAR3M': { icon: '%', bg: '#14B8A6', color: '#fff' },
  // Валюта
  'USD000UTSTOM': { icon: '$/₽', bg: '#2563EB', color: '#fff' },
  'EUR_RUB__TOM': { icon: '€/₽', bg: '#1D4ED8', color: '#fff' },
  'CNYRUB_TOM': { icon: '¥/₽', bg: '#DC2626', color: '#fff' },
  'GLDRUB_TOM': { icon: 'Au₽', bg: '#D97706', color: '#fff' },
};

const InstrumentIcon = ({ sectype }: { sectype: string }) => {
  const icon = INSTRUMENT_ICONS[sectype];
  if (icon) {
    return (
      <div className="w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 font-bold text-[10px]"
        style={{ backgroundColor: icon.bg, color: icon.color }}>
        {icon.icon}
      </div>
    );
  }
  return (
    <div className="w-7 h-7 rounded-full flex-shrink-0 flex items-center justify-center text-[10px] font-bold text-white/70"
      style={{ backgroundColor: stringToColor(sectype) }}>
      {sectype.slice(0, 2)}
    </div>
  );
};

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
      <div className="relative w-full max-w-lg bg-white/[0.08] backdrop-blur-xl rounded-2xl border border-white/15 shadow-2xl max-h-[70vh] overflow-hidden" style={{ boxShadow: '0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.1)' }}>
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

          {/* Категории */}
          {!onlyGroups && (
          <div className="flex gap-1.5 mt-4 flex-wrap">
            {CATEGORY_FILTERS.map(cat => (
              <button
                key={cat.key}
                onClick={() => setCategoryFilter(cat.key)}
                className={`px-3 py-1 text-[12px] font-medium rounded-full transition-colors ${
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
        <div className="overflow-y-auto max-h-[calc(70vh-220px)] px-6 pb-6 styled-scrollbar">
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
                        className="flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-white/5 transition-colors cursor-pointer"
                      >
                        <InstrumentIcon sectype={inst.sectype} />
                        <span className="font-bold text-[13px] text-[#F4F6FA] flex-shrink-0 mr-2">{inst.sectype}</span>
                        <span className="text-[12px] text-[#7A8194] truncate flex-1">{inst.name}</span>
                        <button
                          onClick={(e) => toggleFavorite(inst.sectype, e)}
                          className="p-1.5 text-[#FFB020]"
                        >
                          <Star size={16} fill="#FFB020" />
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
                      className="flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-white/5 transition-colors cursor-pointer"
                    >
                      <InstrumentIcon sectype={inst.sectype} />
                      <span className="font-bold text-[13px] text-[#F4F6FA] flex-shrink-0 mr-2">{inst.sectype}</span>
                      <span className="text-[12px] text-[#7A8194] truncate flex-1">{inst.name}</span>
                      <button
                        onClick={(e) => toggleFavorite(inst.sectype, e)}
                        className="p-1.5 text-[#5E6576] hover:text-[#FFB020] transition-colors"
                      >
                        <Star size={16} />
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