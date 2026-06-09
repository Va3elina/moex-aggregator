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
import { Search, Star, Lock } from 'lucide-react';
import MobileSheet from './MobileSheet';
import InstrumentIcon from '../InstrumentIcon';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import type { Instrument } from '../../types';

interface MobileAssetSearchProps {
  open: boolean;
  onClose: () => void;
  onSelect: (sectype: string, name: string) => void;
  filterType?: 'stock' | 'futures' | 'no-futures';
  excludeType?: string;
  /** Если задан — для каждого инструмента проверяем доступность по tier'у.
   *  Заблокированные затемняются + lock icon + клик открывает UpgradeModal. */
  indicator?: string;
}

const FAVORITES_KEY = 'favoriteInstruments';

// Категории-чипы. Зависят от filterType:
//   - 'no-futures' (Seasonality) → Акции/Валюта/Индексы/Сырьё (без Фьючерсов)
//   - undefined (OI, default)    → Фьючерсы/Валюта/Индексы/Сырьё (как было)
//   - 'stock' / 'futures'        → чипы не показываем (один тип уже)
// Key матчит inst.type ('futures', 'shares') или inst.group ('Валюта' и т.д.).
const CATEGORY_CHIPS_FUTURES_FIRST = [
  { key: 'all', label: 'Все' },
  { key: 'futures', label: 'Фьючерсы' },
  { key: 'Валюта', label: 'Валюта' },
  { key: 'Индексы', label: 'Индексы' },
  { key: 'Сырьё', label: 'Сырьё' },
  { key: 'Крипто', label: 'Крипто' },
];
const CATEGORY_CHIPS_NO_FUTURES = [
  { key: 'all', label: 'Все' },
  { key: 'Акции', label: 'Акции' },
  { key: 'Валюта', label: 'Валюта' },
  { key: 'Индексы', label: 'Индексы' },
  { key: 'Сырьё', label: 'Сырьё' },
];

export default function MobileAssetSearch({
  open,
  onClose,
  onSelect,
  filterType,
  excludeType,
  indicator,
}: MobileAssetSearchProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [instruments, setInstruments] = useState<Instrument[]>([]);
  const [loading, setLoading] = useState(true);
  const [categoryFilter, setCategoryFilter] = useState('all');
  const [favorites, setFavorites] = useState<string[]>(() => {
    const saved = localStorage.getItem(FAVORITES_KEY);
    return saved ? JSON.parse(saved) : ['SR', 'GZ', 'MX'];
  });

  // Tier-gating: проверяем доступ для каждого актива если задан indicator.
  const tierAccess = useTierAccess(indicator || '');
  const { showUpgrade } = useUpgradePrompt();

  // Загрузка списка. Futures лежат в отдельной таблице (требует
  // ?type=futures), валюта/индексы/сырьё/акции в общей.
  // filterType:
  //   - 'stock' / 'futures' → один endpoint
  //   - 'no-futures' → только /api/instruments (без futures)
  //   - undefined → оба endpoint'а параллельно (futures + general)
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        if (filterType === 'stock' || filterType === 'futures') {
          const resp = await fetch(`/api/instruments?type=${filterType}`);
          const data = await resp.json();
          if (!cancelled) setInstruments(data.instruments || []);
        } else if (filterType === 'no-futures') {
          const resp = await fetch('/api/instruments');
          const data = await resp.json();
          if (!cancelled) setInstruments(data.instruments || []);
        } else {
          const [generalResp, futuresResp] = await Promise.all([
            fetch('/api/instruments'),
            fetch('/api/instruments?type=futures'),
          ]);
          const [general, futures] = await Promise.all([
            generalResp.json(),
            futuresResp.json(),
          ]);
          if (!cancelled) {
            const merged = [
              ...(futures.instruments || []),
              ...(general.instruments || []),
            ];
            setInstruments(merged);
          }
        }
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
    // filterType='no-futures' грузит общий список — фьючерсы туда тоже
    // приходят, поэтому отсекаем их здесь: Сезонность работает по
    // спот-валютам / индексам / акциям / сырью, но не по фьючерсам.
    if (filterType === 'no-futures' && inst.type === 'futures') return false;
    const matchesSearch =
      !searchQuery ||
      inst.sectype.toLowerCase().includes(searchQuery.toLowerCase()) ||
      inst.name.toLowerCase().includes(searchQuery.toLowerCase());
    // matchesCategory: совпадение по group (основной путь — backend проставляет
    // 'Акции'/'Валюта'/'Индексы'/'Сырьё') ИЛИ по type (fallback). Для категории
    // «Акции» есть особый случай: если backend не проставил group у какой-то
    // акции (видели 2 такие записи на проде), но type='stock' — она всё равно
    // должна попадать в категорию. Без этого fallback юзер искал «акции»
    // через chip и не видел эти 2 инструмента.
    const matchesCategory =
      categoryFilter === 'all' ||
      inst.group === categoryFilter ||
      inst.type === categoryFilter ||
      (categoryFilter === 'Акции' && inst.type === 'stock');
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

    // Tier-check. Graceful fallback если indicator не задан или матрица грузится.
    const accessible = !indicator || tierAccess.isLoading
      ? true
      : tierAccess.canAccessAsset(inst.sectype);
    const requiredTier = !accessible ? tierAccess.requiredTierFor({ asset: inst.sectype }) : null;

    return (
      <button
        key={inst.sectype}
        onClick={() => {
          if (accessible) {
            onSelect(inst.sectype, inst.name);
            onClose();
          } else if (requiredTier && indicator) {
            // Закрываем mobile sheet и сразу открываем UpgradeModal сверху —
            // иначе bottom-sheet не даст увидеть upgrade dialog.
            onClose();
            showUpgrade({
              tier: requiredTier,
              featureName: `актив ${inst.name} (${inst.sectype})`,
              indicator,
            });
          }
        }}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 14,
          padding: '11px 18px',
          borderTop: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
          width: '100%',
          background: 'transparent',
          cursor: accessible ? 'pointer' : 'not-allowed',
          color: 'var(--text-primary)',
          textAlign: 'left',
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 14,
            flex: 1,
            minWidth: 0,
            opacity: accessible ? 1 : 0.45,
            filter: accessible ? undefined : 'grayscale(0.5)',
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
        </div>

        {/* Lock icon для заблокированных */}
        {!accessible && (
          <Lock
            size={16}
            strokeWidth={2.2}
            style={{ color: 'var(--text-muted)', flexShrink: 0 }}
            aria-label="Доступно на повышенном тарифе"
          />
        )}

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

      {/* Category chips — gap ужат, чтобы все 5 категорий помещались
          в строку на 320px viewport без скролла. */}
      <div
        style={{
          display: 'flex',
          gap: 4,
          padding: '12px 10px 8px',
          overflowX: 'auto',
          scrollbarWidth: 'none',
          flexShrink: 0,
          justifyContent: 'center',
        }}
      >
        {(filterType === 'no-futures' ? CATEGORY_CHIPS_NO_FUTURES : CATEGORY_CHIPS_FUTURES_FIRST).map((c) => (
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
