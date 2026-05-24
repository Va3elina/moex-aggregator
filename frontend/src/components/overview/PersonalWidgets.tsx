/**
 * PersonalWidgets — 3 виджета для главной залогиненного юзера:
 *   - WatchlistWidget: личный список тикеров с быстрыми метриками
 *   - RecentWidget: последние посещённые страницы (localStorage)
 *   - ScreenersWidget: preset-фильтры с результатами
 *
 * Все виджеты доступны всем тарифам бесплатно. Click-through на актив
 * или индикатор может вызвать UpgradeModal на конечной странице — гейтинг
 * не дублируется здесь.
 *
 * Все компоненты используют editorial-style оформление (paper bg + outline
 * + 1.5px border) совместимо с остальной OverviewPage.
 */
import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { Star, X, Plus, Clock, ArrowRight, Search } from 'lucide-react';
import {
  getWatchlist,
  addToWatchlist,
  removeFromWatchlist,
  getScreenerPresets,
  runScreener,
  type WatchlistItem,
  type ScreenerPreset,
  type ScreenerItem,
} from '../../services/api';
import { useRecent, clearRecent } from '../../hooks/useRecent';
import { useAuth } from '../../contexts/AuthContext';
import InstrumentSearchModal from '../InstrumentSearchModal';

// ════════════════════════════════════════════════════════════════════
// WatchlistWidget
// ════════════════════════════════════════════════════════════════════

export function WatchlistWidget() {
  const { isAuthenticated } = useAuth();
  const [items, setItems] = useState<WatchlistItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);

  // Загружаем только если залогинен — у гостя watchlist всегда пустой.
  useEffect(() => {
    if (!isAuthenticated) return;
    setLoading(true);
    getWatchlist()
      .then(setItems)
      .catch(() => setItems([]))
      .finally(() => setLoading(false));
  }, [isAuthenticated]);

  const handleAdd = async (sectype: string) => {
    try {
      const item = await addToWatchlist(sectype);
      setItems((prev) => {
        // Replace if exists (idempotent) или добавить в конец.
        const filtered = prev.filter((p) => p.ticker !== item.ticker);
        return [...filtered, item];
      });
    } catch (e) {
      // eslint-disable-next-line no-alert
      alert((e as Error).message);
    }
  };

  const handleRemove = async (ticker: string) => {
    setItems((prev) => prev.filter((p) => p.ticker !== ticker));  // Optimistic.
    try {
      await removeFromWatchlist(ticker);
    } catch {
      // Rollback (refetch).
      getWatchlist().then(setItems).catch(() => undefined);
    }
  };

  // Гость не видит watchlist (только при логине).
  if (!isAuthenticated) {
    return (
      <div className="editorial-frame p-6 text-center" style={{ minHeight: 200 }}>
        <Star size={20} className="mx-auto mb-3" style={{ color: 'var(--text-muted)' }} />
        <p className="text-sm mb-2" style={{ color: 'var(--text-secondary)' }}>
          Избранные тикеры на главной
        </p>
        <p className="text-xs mb-4" style={{ color: 'var(--text-muted)' }}>
          Залогиньтесь чтобы добавить акции в watchlist
        </p>
        <Link
          to="/login"
          className="editorial-press inline-flex items-center font-semibold rounded-full"
          style={{
            background: 'var(--bg-secondary)',
            color: 'var(--text-primary)',
            border: '1.5px solid var(--text-primary)',
            fontSize: 'var(--fs-sm)',
            padding: 'var(--sp-2) var(--sp-3)',
          }}
        >
          Войти
        </Link>
      </div>
    );
  }

  return (
    <div className="editorial-frame" style={{ padding: 'var(--sp-4)' }}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Star size={14} fill="currentColor" style={{ color: 'var(--accent)' }} />
          <p
            className="text-xs uppercase"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
          >
            Избранное
          </p>
          {items.length > 0 && (
            <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
              {items.length}
            </span>
          )}
        </div>
        <button
          onClick={() => setSearchOpen(true)}
          className="editorial-press inline-flex items-center gap-1 font-semibold rounded-full whitespace-nowrap"
          style={{
            background: 'var(--bg-secondary)',
            color: 'var(--text-primary)',
            border: '1.5px solid var(--text-primary)',
            fontSize: 'var(--fs-xs)',
            padding: 'var(--sp-1) var(--sp-2)',
          }}
          title="Добавить тикер"
        >
          <Plus size={12} strokeWidth={2.5} />
          Добавить
        </button>
      </div>

      {loading ? (
        <p className="text-xs" style={{ color: 'var(--text-muted)' }}>Загружаем…</p>
      ) : items.length === 0 ? (
        <p className="text-xs py-4 text-center" style={{ color: 'var(--text-muted)' }}>
          Watchlist пуст. Добавьте свои тикеры через «Добавить».
        </p>
      ) : (
        <div className="flex flex-wrap gap-2">
          {items.map((item) => (
            <div
              key={item.ticker}
              className="editorial-press inline-flex items-center font-semibold rounded-full whitespace-nowrap"
              style={{
                background: 'var(--bg-secondary)',
                color: 'var(--text-primary)',
                border: '1.5px solid var(--text-primary)',
                fontSize: 'var(--fs-sm)',
                padding: 'var(--sp-2) var(--sp-3)',
                gap: 'var(--sp-2)',
              }}
            >
              {/* Deep-link на heatmap с focus. Гейтинг тарифа произойдёт там
                  через useTierAccess, если актив недоступен на текущем тарифе. */}
              <Link
                to={`/heatmap?focus=${encodeURIComponent(item.ticker)}`}
                className="transition-opacity hover:opacity-80"
                style={{ color: 'inherit' }}
                title={item.name ?? item.ticker}
              >
                {item.ticker}
              </Link>
              <button
                onClick={() => handleRemove(item.ticker)}
                className="opacity-60 hover:opacity-100 transition-opacity"
                title="Убрать из избранного"
                aria-label={`Убрать ${item.ticker}`}
              >
                <X size={12} strokeWidth={2.5} />
              </button>
            </div>
          ))}
        </div>
      )}

      {searchOpen && (
        <InstrumentSearchModal
          onSelect={(sectype) => {
            void handleAdd(sectype);
            setSearchOpen(false);
          }}
          onClose={() => setSearchOpen(false)}
          excludeType="futures"
        />
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════
// RecentWidget — последние просмотры из localStorage
// ════════════════════════════════════════════════════════════════════

const INDICATOR_LABEL: Record<string, string> = {
  heatmap: 'Карта',
  strength: 'Сила',
  buffett: 'Баффет',
  seasonality: 'Сезонность',
  'funds-money': 'Фонды',
  'funds-catalog': 'Состав',
  oi: 'ОИ',
  'cbr-flows': 'Потоки',
};

export function RecentWidget() {
  const entries = useRecent();

  return (
    <div className="editorial-frame" style={{ padding: 'var(--sp-4)' }}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Clock size={14} style={{ color: 'var(--text-secondary)' }} />
          <p
            className="text-xs uppercase"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
          >
            Недавнее
          </p>
        </div>
        {entries.length > 0 && (
          <button
            onClick={() => clearRecent()}
            className="text-xs opacity-60 hover:opacity-100 transition-opacity"
            style={{ color: 'var(--text-muted)' }}
            title="Очистить историю"
          >
            Очистить
          </button>
        )}
      </div>

      {entries.length === 0 ? (
        <p className="text-xs py-4 text-center" style={{ color: 'var(--text-muted)' }}>
          История появится по мере просмотра индикаторов и активов.
        </p>
      ) : (
        <div className="flex flex-col gap-1">
          {entries.slice(0, 6).map((e) => {
            const indLabel = e.indicatorId ? INDICATOR_LABEL[e.indicatorId] : null;
            return (
              <Link
                key={e.path}
                to={e.path}
                className="flex items-center justify-between gap-2 py-1.5 transition-opacity hover:opacity-70"
                style={{ borderBottom: '1px dashed color-mix(in srgb, var(--text-primary) 10%, transparent)' }}
              >
                <span
                  className="truncate"
                  style={{ color: 'var(--text-primary)', fontSize: 'var(--fs-sm)' }}
                >
                  {e.label}
                </span>
                <span className="flex items-center gap-1 flex-shrink-0">
                  {indLabel && (
                    <span
                      className="text-xs px-1.5 rounded"
                      style={{
                        background: 'var(--bg-secondary)',
                        color: 'var(--text-secondary)',
                        fontSize: 'var(--fs-2xs)',
                      }}
                    >
                      {indLabel}
                    </span>
                  )}
                  <ArrowRight size={11} style={{ color: 'var(--text-muted)' }} />
                </span>
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════
// ScreenersWidget — chip-row presets + результат top-10
// ════════════════════════════════════════════════════════════════════

export function ScreenersWidget() {
  const [presets, setPresets] = useState<ScreenerPreset[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [items, setItems] = useState<ScreenerItem[]>([]);
  const [loading, setLoading] = useState(false);

  // Lazy load preset-meta при mount.
  useEffect(() => {
    getScreenerPresets()
      .then((list) => {
        setPresets(list);
        // По умолчанию открываем первый preset — даём юзеру что посмотреть сразу.
        if (list.length > 0) setActiveId(list[0].id);
      })
      .catch(() => undefined);
  }, []);

  // При смене activeId — query результата.
  useEffect(() => {
    if (!activeId) return;
    setLoading(true);
    runScreener(activeId)
      .then(setItems)
      .catch(() => setItems([]))
      .finally(() => setLoading(false));
  }, [activeId]);

  const activePreset = useMemo(
    () => presets.find((p) => p.id === activeId) ?? null,
    [presets, activeId],
  );

  return (
    <section className="mb-10 md:mb-12">
      <div className="flex items-center gap-3 mb-5 md:mb-6">
        <p
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
        >
          Скриннеры
        </p>
        <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
        {activePreset && (
          <span className="text-xs hidden md:inline" style={{ color: 'var(--text-muted)' }}>
            {activePreset.description}
          </span>
        )}
      </div>

      {/* Chip-row presets — горизонтальный скролл на узких экранах. */}
      <div className="flex gap-2 mb-4 overflow-x-auto pb-1" style={{ scrollbarWidth: 'thin' }}>
        {presets.map((p) => {
          const active = p.id === activeId;
          return (
            <button
              key={p.id}
              onClick={() => setActiveId(p.id)}
              className="editorial-press inline-flex items-center gap-1 font-semibold rounded-full whitespace-nowrap"
              style={{
                background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                border: '1.5px solid var(--text-primary)',
                boxShadow: active ? 'var(--shadow-hard-chip)' : undefined,
                fontSize: 'var(--fs-sm)',
                padding: 'var(--sp-2) var(--sp-3)',
                flexShrink: 0,
              }}
            >
              <span>{p.emoji}</span>
              {p.label}
            </button>
          );
        })}
      </div>

      {/* Result — таблица top-10. */}
      <div className="editorial-frame" style={{ padding: 'var(--sp-4)' }}>
        {loading ? (
          <p className="text-xs" style={{ color: 'var(--text-muted)' }}>Загружаем…</p>
        ) : items.length === 0 ? (
          <p
            className="text-xs py-6 text-center"
            style={{ color: 'var(--text-muted)' }}
          >
            {activePreset
              ? 'Сейчас нет тикеров под этот фильтр.'
              : 'Выберите фильтр выше.'}
          </p>
        ) : (
          <table className="w-full text-sm">
            <tbody>
              {items.map((item) => (
                <tr
                  key={item.ticker}
                  style={{
                    borderBottom: '1px dashed color-mix(in srgb, var(--text-primary) 10%, transparent)',
                  }}
                >
                  <td className="py-2 pr-3">
                    <Link
                      to={`/heatmap?focus=${encodeURIComponent(item.ticker)}`}
                      className="transition-opacity hover:opacity-70"
                      style={{ color: 'var(--text-primary)', fontWeight: 700 }}
                    >
                      {item.ticker}
                    </Link>
                  </td>
                  <td
                    className="py-2 pr-3"
                    style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)' }}
                  >
                    {item.name ?? ''}
                  </td>
                  <td
                    className="py-2 text-right tabular-nums font-semibold"
                    style={{
                      color:
                        (item.value ?? 0) >= 0
                          ? 'var(--funds-flow-positive, #5BD49C)'
                          : 'var(--funds-flow-negative, #FF7A5C)',
                      fontSize: 'var(--fs-sm)',
                    }}
                  >
                    {item.value_label ?? '—'}
                  </td>
                  <td className="py-2 pl-3 text-right">
                    <Link
                      to={`/seasonality?ticker=${encodeURIComponent(item.ticker)}`}
                      className="text-xs transition-opacity hover:opacity-70 inline-flex items-center gap-1"
                      style={{ color: 'var(--text-muted)' }}
                      title="Открыть сезонность"
                    >
                      <Search size={11} />
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}
