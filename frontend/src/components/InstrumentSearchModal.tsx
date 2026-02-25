import { useState, useEffect } from 'react';
import { Search, X, Star } from 'lucide-react';

interface Instrument {
  sec_id: string;
  sectype: string;
  name: string;
  type: string;
}

interface InstrumentSearchModalProps {
  onSelect: (sectype: string, name: string) => void;
  onClose: () => void;
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

export default function InstrumentSearchModal({ onSelect, onClose }: InstrumentSearchModalProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [instruments, setInstruments] = useState<Instrument[]>([]);
  const [loading, setLoading] = useState(true);

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
        const resp = await fetch('/api/instruments?type=futures');
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

  // Фильтрация по поиску
  const filteredInstruments = instruments.filter(inst =>
    inst.sectype.toLowerCase().includes(searchQuery.toLowerCase()) ||
    inst.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  // Уникальные по sectype (убираем дубликаты контрактов)
  const uniqueInstruments = filteredInstruments.reduce((acc, inst) => {
    if (!acc.find(i => i.sectype === inst.sectype)) {
      acc.push(inst);
    }
    return acc;
  }, [] as Instrument[]);

  const favoriteInstruments = uniqueInstruments.filter(inst => favorites.includes(inst.sectype));
  const regularInstruments = uniqueInstruments.filter(inst => !favorites.includes(inst.sectype));

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

      {/* Modal */}
      <div className="relative w-full max-w-lg bg-[#1A1F2E] rounded-2xl shadow-2xl max-h-[70vh] overflow-hidden">
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
        </div>

        {/* Results */}
        <div className="overflow-y-auto max-h-[calc(70vh-180px)] px-6 pb-6">
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
                  <div className="space-y-1">
                    {favoriteInstruments.map((inst) => (
                      <div
                        key={inst.sectype}
                        onClick={() => onSelect(inst.sectype, inst.name)}
                        className="flex items-center gap-4 p-3 rounded-xl hover:bg-white/5 transition-colors cursor-pointer"
                      >
                        <div
                          className="w-10 h-10 rounded-full flex-shrink-0"
                          style={{ backgroundColor: stringToColor(inst.sectype) }}
                        />
                        <div className="flex-1">
                          <div className="font-semibold text-[#F4F6FA]">{inst.name}</div>
                          <div className="text-sm text-[#A7ADBC]">{inst.sectype}</div>
                        </div>
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
                <div className="space-y-1">
                  {regularInstruments.map((inst) => (
                    <div
                      key={inst.sectype}
                      onClick={() => onSelect(inst.sectype, inst.name)}
                      className="flex items-center gap-4 p-3 rounded-xl hover:bg-white/5 transition-colors cursor-pointer"
                    >
                      <div
                        className="w-10 h-10 rounded-full flex-shrink-0"
                        style={{ backgroundColor: stringToColor(inst.sectype) }}
                      />
                      <div className="flex-1">
                        <div className="font-semibold text-[#F4F6FA]">{inst.name}</div>
                        <div className="text-sm text-[#A7ADBC]">{inst.sectype}</div>
                      </div>
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