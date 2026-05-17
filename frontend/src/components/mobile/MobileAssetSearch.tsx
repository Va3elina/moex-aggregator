/**
 * MobileAssetSearch — мобильный sheet для выбора инструмента.
 *
 * Заменяет десктопный InstrumentSearchModal: вместо центрированной
 * полноэкранной модалки — slide-up sheet с поиском, категориями
 * и группами (Избранные / Недавно / Все).
 *
 * Тач-оптимизирован:
 *   - Большие touch-targets (44+ px)
 *   - НЕ автофокус на поиск (чтобы клавиатура не вылетела сразу)
 *   - Свайп вниз / тап по backdrop / Esc закрывают
 *
 * API совпадает с InstrumentSearchModal — onSelect(sectype, name).
 */
import { useEffect, useState } from 'react';
import { Search, Star } from 'lucide-react';
import MobileSheet from './MobileSheet';
import InstrumentIcon from '../InstrumentIcon';
import type { Instrument } from '../../types';

interface MobileAssetSearchProps {
  open: boolean;
  onClose: () => void;
  onSelect: (sectype: string, name: string) => void;
  filterType?: 'stock' | 'futures';
  excludeType?: string;
}

const FAVORITES_KEY = 'favoriteInstruments';

const CATEGORY_CHIPS = [
  { key: 'all', label: 'Все' },
  { key: 'stock', label: 'Акции' },
  { key: 'futures', label: 'Фьючерсы' },
  { key: 'currency', label: 'Валюты' },
  { key: 'index', label: 'Индексы' },
];

export default function MobileAssetSearch({
  open,
  onClose,
  onSelect,
  filterType,
  excludeType,
}: MobileAssetSearchProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [instruments, setInstruments] = useState<Instrument[]>([]);
  const [loading, setLoading] = useState(true);
  const [categoryFilter, setCategoryFilter] = useState('all');
  const [favorites, setFavorites] = useState<string[]>(() => {
    const saved = localStorage.getItem(FAVORITES_KEY);
    return saved ? JSON.parse(saved) : ['SR', 'GZ', 'MX'];
  });

  // Загрузка списка
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        const url = filterType ? `/api/instruments?type=${filterType}` : '/api/instruments';
        const resp = await fetch(url);
        const data = await resp.json();
        if (!cancelled) setInstruments(data.instruments || []);
      } catch (err) {
        console.error('Ошибка загрузки инструментов:', err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [open, filterType]);

  // Сохранение избранных
  useEffect(() => {
    localStorage.setItem(FAVORITES_KEY, JSON.stringify(favorites));
  }, [favorites]);

  const toggleFavorite = (sectype: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setFavorites((prev) =>
      prev.includes(sectype) ? prev.filter((t) => t !== sectype) : [...prev, sectype],
    );
  };

  // Фильтрация (та же логика что в десктопном InstrumentSearchModal)
  const filtered = instruments.filter((inst) => {
    if (excludeType && inst.type === excludeType) return false;
    const matchesSearch =
      !searchQuery ||
      inst.sectype.toLowerCase().includes(searchQuery.toLowerCase()) ||
      inst.name.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesCategory = categoryFilter === 'all' || inst.group === categoryFilter || inst.type === categoryFilter;
    return matchesSearch && matchesCategory;
  });

  // Уникальные по sectype — берём первый встретившийся.
  // Сортировка по объёму делается на бэкенде, тут просто preserve order.
  const unique = filtered.reduce((acc, inst) => {
    if (!acc.find((i) => i.sectype === inst.sectype)) acc.push(inst);
    return acc;
  }, [] as Instrument[]);

  const favoriteItems = searchQuery ? [] : unique.filter((i) => favorites.includes(i.sectype));
  const regularItems = searchQuery ? unique : unique.filter((i) => !favorites.includes(i.sectype));

  // Render одного элемента списка
  const renderItem = (inst: Instrument) => {
    const isFav = favorites.includes(inst.sectype);
    return (
      <button
        key={inst.sectype}
        onClick={() => {
          onSelect(inst.sectype, inst.name);
          onClose();
        }}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 14,
          padding: '11px 18px',
          borderTop: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
          width: '100%',
          background: 'transparent',
          cursor: 'pointer',
          color: 'var(--text-primary)',
          textAlign: 'left',
        }}
      >
        <InstrumentIcon sectype={inst.sectype} size={36} />
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 14, fontWeight: 700, lineHeight: 1.15 }}>{inst.name}</div>
          <div
            className="mono"
            style={{
              fontSize: 10.5,
              color: 'var(--text-secondary)',
              letterSpacing: '0.04em',
              marginTop: 2,
            }}
          >
            {inst.sectype}
          </div>
        </div>
        <span
          role="button"
          onClick={(e) => toggleFavorite(inst.sectype, e)}
          style={{
            padding: 8,
            color: isFav ? 'var(--accent)' : 'var(--text-muted)',
            display: 'inline-grid',
            placeItems: 'center',
          }}
          aria-label={isFav ? 'Убрать из избранных' : 'Добавить в избранные'}
        >
          <Star size={18} fill={isFav ? 'currentColor' : 'transparent'} strokeWidth={2} />
        </span>
      </button>
    );
  };

  return (
    <MobileSheet open={open} onClose={onClose}>
      {/* Search input */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          padding: '12px 16px',
          borderBottom: '1.5px solid var(--text-primary)',
          flexShrink: 0,
        }}
      >
        <div style={{ position: 'relative', flex: 1 }}>
          <Search
            size={15}
            strokeWidth={2.4}
            style={{
              position: 'absolute',
              left: 14,
              top: '50%',
              transform: 'translateY(-50%)',
              color: 'var(--text-secondary)',
            }}
          />
          <input
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Тикер или название"
            style={{
              width: '100%',
              padding: '10px 12px 10px 38px',
              border: '1.5px solid var(--text-primary)',
              borderRadius: 10,
              background: 'var(--bg-secondary)',
              color: 'var(--text-primary)',
              fontSize: 14,
              fontWeight: 500,
              outline: 'none',
            }}
            // НЕ автофокус — иначе клавиатура сразу вылетает и закрывает контент
          />
        </div>
        <button
          onClick={onClose}
          style={{
            fontWeight: 700,
            fontSize: 14,
            background: 'none',
            border: 'none',
            color: 'var(--text-primary)',
            cursor: 'pointer',
            padding: 4,
          }}
        >
          Отмена
        </button>
      </div>

      {/* Category chips */}
      <div
        style={{
          display: 'flex',
          gap: 7,
          padding: '12px 14px 8px',
          overflowX: 'auto',
          scrollbarWidth: 'none',
          flexShrink: 0,
        }}
      >
        {CATEGORY_CHIPS.map((c) => (
          <button
            key={c.key}
            className={`fm-chip ${categoryFilter === c.key ? 'active' : 'dim'}`}
            onClick={() => setCategoryFilter(c.key)}
          >
            {c.label}
          </button>
        ))}
      </div>

      {/* Список */}
      <div style={{ flex: 1, overflowY: 'auto', minHeight: 0, WebkitOverflowScrolling: 'touch' }}>
        {loading ? (
          <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-secondary)' }}>
            Загрузка...
          </div>
        ) : (
          <>
            {favoriteItems.length > 0 && (
              <>
                <div
                  style={{
                    padding: '14px 18px 6px',
                    display: 'flex',
                    alignItems: 'center',
                    gap: 5,
                    fontSize: 11,
                    fontWeight: 700,
                    letterSpacing: '0.06em',
                    textTransform: 'uppercase',
                    color: 'var(--accent)',
                  }}
                >
                  <Star size={11} fill="currentColor" strokeWidth={0} />
                  Избранные
                </div>
                {favoriteItems.map(renderItem)}
              </>
            )}
            {regularItems.length > 0 && (
              <>
                {favoriteItems.length > 0 && (
                  <div
                    style={{
                      padding: '14px 18px 6px',
                      fontSize: 11,
                      fontWeight: 700,
                      letterSpacing: '0.06em',
                      textTransform: 'uppercase',
                      color: 'var(--text-muted)',
                    }}
                  >
                    Все инструменты
                  </div>
                )}
                {regularItems.map(renderItem)}
              </>
            )}
            {!loading && favoriteItems.length === 0 && regularItems.length === 0 && (
              <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
                Не найдено
              </div>
            )}
          </>
        )}
      </div>
    </MobileSheet>
  );
}
