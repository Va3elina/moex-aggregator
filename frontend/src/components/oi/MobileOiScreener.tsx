/**
 * MobileOiScreener — вкладка «Скринер сигналов» на мобильной /oi.
 *
 * Та же лента, что в десктопной таблице (OiScreenerTable): состав, порядок и
 * тексты сигналов считаются общим кодом (screenerShared), настройки лежат в
 * тех же ключах localStorage — скринер открывается в одном виде на телефоне и
 * на компьютере. Отличается только вёрстка.
 *
 * Почему не сама таблица: она рассчитана на ширину от 1000px (шесть колонок),
 * на телефоне её пришлось бы листать вбок. Здесь строка одна (утверждённый
 * вариант B, холст 2026-11): лого · имя · комета · доля · сила · ★. Колонка
 * имени узкая — длинное имя растворяется у края, как на ПК, и всё свободное
 * место отдано комете. Текста сигнала в строке нет (он в aria-label строки).
 * Ленту не обводим рамкой: на телефоне на такой акцент нет места.
 *
 * Фильтры — в sheet'е «Опции» (кнопка ⚙ нижней панели), как на остальных
 * мобильных страницах. Сортировка — тапом по «Доле» / «Силе» в шапке.
 */
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import type { CSSProperties, MouseEvent, MutableRefObject } from 'react';
import { Lock, Star } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { getLang } from '../../i18n';
import InstrumentIcon from '../InstrumentIcon';
import MobileSheet from '../mobile/MobileSheet';
import PositionComet from './PositionComet';
import OiScreenerLocked from './OiScreenerLocked';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { getOiScreener, type OiScreenerRow, type OiScreenerHorizon } from '../../services/api';
import { usePersistedState } from '../../hooks/usePersistedState';
import { useSnapshotTracking } from '../../hooks/useSnapshotTracking';
import { useAnalytics } from '../../contexts/AnalyticsContext';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import { noteFavoriteAdded } from '../../utils/favoritesSignupNudge';
import {
  GROUP_OPTIONS, FAVORITES_KEY,
  fmtRatio, pluralAssets, sideShare, isStrengthAccent, readFavorites,
  filterSortScreenerRows, sharpRatioRange, screenerSignalLabel,
  type ScreenerClgroup, type ScreenerSortKey, type ScreenerSortDir,
} from './screenerShared';

/** Имя актива: растворяется у правого края, только если реально обрезано
 *  (замер по факту, пересчёт на ресайз) — тот же приём, что FadedAssetName на ПК. */
function FadedName({ name }: { name: string }) {
  const ref = useRef<HTMLSpanElement>(null);
  const [clipped, setClipped] = useState(false);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const check = () => setClipped(el.scrollWidth > el.clientWidth + 1);
    check();
    const ro = new ResizeObserver(check);
    ro.observe(el);
    return () => ro.disconnect();
  }, [name]);
  return <span ref={ref} title={name} className={`fm-screener-name${clipped ? ' faded' : ''}`}>{name}</span>;
}

interface Props {
  /** Тап по строке — открыть график этого актива. */
  onSelect: (sectype: string) => void;
  /** Sheet «Опции» открывает кнопка ⚙ нижней панели — состоянием владеет страница. */
  optionsOpen: boolean;
  onOptionsClose: () => void;
  /** Краткая сводка фильтров («Физлица · Дни · Все категории») — в подзаголовок страницы. */
  onSummary: (summary: string) => void;
  /** Идёт загрузка ленты — для полоски загрузки под шапкой. */
  onLoading: (loading: boolean) => void;
  /** Страница кладёт сюда «обновить ленту» для жеста pull-to-refresh. */
  reloadRef: MutableRefObject<(() => Promise<void>) | null>;
}

const SHEET_LABEL: CSSProperties = {
  fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-secondary)',
  textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8,
};
const SHEET_CHIP: CSSProperties = {
  flex: 1, justifyContent: 'center', cursor: 'pointer',
  display: 'inline-flex', alignItems: 'center', gap: 4,
};

/** «20 ноября» / «20 November» — дата, за которую посчитана лента. */
function fmtDataDate(d: string): string {
  return new Date(d + 'T00:00:00').toLocaleDateString(getLang() === 'en' ? 'en-GB' : 'ru-RU', { day: 'numeric', month: 'long' });
}

export default function MobileOiScreener({
  onSelect, optionsOpen, onOptionsClose, onSummary, onLoading, reloadRef,
}: Props) {
  const { t } = useTranslation();
  const { track } = useAnalytics();
  // Ответ хранится вместе с ключом среза (группа + горизонт), за который он
  // получен. Сменился срез — прежние строки сразу перестают подходить, и лента
  // показывает «Загрузка…», а не старые строки под новыми подписями (у
  // недельной ленты свои пороги и слова «за 2 недели»).
  const [feed, setFeed] = useState<{
    key: string;
    rows: OiScreenerRow[];
    signalDate: string | null;
    // Пейволл: бэкенд отдал маркер без строк. Читаем ответ, а не матрицу
    // тарифов — пока матрица грузится, заглушка не мигнёт у платника.
    locked: boolean;
  } | null>(null);
  const [errorKey, setErrorKey] = useState<string | null>(null);

  // Ключи настроек — те же, что у десктопной таблицы: значения совместимы.
  const [rawClgroup, setClgroup] = usePersistedState<ScreenerClgroup>('frame:oi-screener:clgroup', 'FIZ');
  const [horizon, setHorizon] = usePersistedState<OiScreenerHorizon>('frame:oi-screener:horizon', 'short');
  const [group, setGroup] = usePersistedState<string>('frame:oi-screener:group', 'all');
  const [onlyFav, setOnlyFav] = usePersistedState<boolean>('frame:oi-screener:onlyfav', false);
  const [sortKey, setSortKey] = usePersistedState<ScreenerSortKey>('frame:oi-screener:sortkey', 'ratio');
  const [sortDir, setSortDir] = usePersistedState<ScreenerSortDir>('frame:oi-screener:sortdir', -1);
  const [favorites, setFavorites] = useState<string[]>(readFavorites);

  // «Юрлица» — тот же замок, что на вкладке графика (open_interest.clgroup_yur,
  // с Basic). Сохранённый выбор не трогаем — после апгрейда вернётся сам. Пока
  // тариф грузится, считаем залоченным, чтобы не мигнуть чужим срезом.
  const oiAccess = useTierAccess('open_interest');
  const { showUpgrade } = useUpgradePrompt();
  const canUseYur = !oiAccess.isLoading && oiAccess.canUseFlag('clgroup_yur');
  const yurLocked = !oiAccess.isLoading && !oiAccess.canUseFlag('clgroup_yur');
  const clgroup: ScreenerClgroup = canUseYur ? rawClgroup : 'FIZ';

  const feedKey = `${clgroup}:${horizon}`;
  const current = feed?.key === feedKey ? feed : null;
  const rows = current?.rows ?? null;
  const locked = !!current?.locked;
  const signalDate = current?.signalDate ?? null;
  const error = !current && errorKey === feedKey;

  // Загрузка. silent — фоновое обновление (онлайн-события, pull-to-refresh):
  // при его сбое прежняя лента остаётся на экране, а не меняется на ошибку.
  // Номер запроса отсекает ответ, который обогнали (быстрая смена среза).
  const reqRef = useRef(0);
  const load = useCallback((silent: boolean): Promise<void> => {
    const id = ++reqRef.current;
    return getOiScreener(clgroup, horizon).then(
      (r) => {
        if (id !== reqRef.current) return;
        setFeed({ key: feedKey, rows: r.rows, signalDate: r.signal_date, locked: !!r.locked });
        setErrorKey(null);
      },
      () => { if (id === reqRef.current && !silent) setErrorKey(feedKey); },
    );
  }, [clgroup, horizon, feedKey]);

  useEffect(() => { void load(false); }, [load]);
  // Лента считается по тем же 5-минутным срезам ОИ, что и график.
  useRealtimeData(['5min', 'hourly'], () => { void load(true); });
  useEffect(() => {
    reloadRef.current = () => load(true);
    return () => { reloadRef.current = null; };
  }, [load, reloadRef]);

  const loading = rows === null && !error;
  useEffect(() => {
    onLoading(loading);
    return () => onLoading(false);
  }, [loading, onLoading]);

  const visible = useMemo(
    () => (rows ? filterSortScreenerRows(rows, { group, onlyFav, favorites, sortKey, sortDir }) : []),
    [rows, favorites, group, sortKey, sortDir, onlyFav],
  );
  const stats = useMemo(() => sharpRatioRange(visible), [visible]);

  const isMed = horizon === 'medium';
  const groupLabel = clgroup === 'FIZ' ? t('Физлица') : t('Юрлица');
  const groupWord = clgroup === 'FIZ' ? t('физлица') : t('юрлица');
  const horizonLabel = isMed ? t('Недели') : t('Дни');
  const groupFilterLabel = t(GROUP_OPTIONS.find((g) => g.key === group)?.label ?? 'Все категории');
  const overWord = isMed ? t('за 2 недели') : t('за день');
  const prevWord = isMed ? t('2 недели назад') : t('вчера');

  const summary = [groupLabel, horizonLabel, groupFilterLabel, onlyFav ? `★ ${t('Избранные')}` : null]
    .filter(Boolean).join(' · ');
  useEffect(() => { onSummary(summary); }, [summary, onSummary]);

  // Аналитика — те же события, что у десктопной таблицы: снимок настроек при
  // открытии и после смены, разовые действия отдельно.
  useSnapshotTracking('screener_view', rows === null && !locked ? null : {
    clgroup, horizon, group,
    only_fav: onlyFav || undefined,
    sort: sortKey === 'skew' || sortDir === 1 ? `${sortKey}_${sortDir === 1 ? 'asc' : 'desc'}` : undefined,
    locked: locked || undefined,
  });

  const pick = (sectype: string) => {
    const rank = visible.findIndex((r) => r.sectype === sectype);
    track('screener_pick', { secid: sectype, clgroup, horizon, rank: rank >= 0 ? rank + 1 : undefined });
    onSelect(sectype);
  };

  const toggleFavorite = (sectype: string, e: MouseEvent) => {
    e.stopPropagation();
    const adding = !favorites.includes(sectype);
    track('screener_fav', { secid: sectype, on: adding });
    // Гостю на второй звёздочке — предложение сохранить избранное в аккаунт
    // (как в таблице на ПК, где нет блока алертов).
    if (adding) noteFavoriteAdded();
    setFavorites((prev) => {
      const next = prev.includes(sectype) ? prev.filter((s) => s !== sectype) : [...prev, sectype];
      localStorage.setItem(FAVORITES_KEY, JSON.stringify(next));
      return next;
    });
  };

  // Тап по активной колонке переворачивает порядок, по другой — переключает
  // на неё со стрелкой ▼ (сильные / самые лонговые сверху).
  const clickSort = (key: ScreenerSortKey) => {
    if (key === sortKey) setSortDir((d) => (d === -1 ? 1 : -1));
    else { setSortKey(key); setSortDir(-1); }
  };
  const sortArrow = (key: ScreenerSortKey) => (sortKey === key ? (sortDir === -1 ? ' ▼' : ' ▲') : '');

  const hasEodOnly = visible.some((r) => !r.has_intraday);

  const sheet = (
    <MobileSheet open={optionsOpen} onClose={onOptionsClose} title={t('Опции')}>
      <div style={{ padding: 16 }}>
        <div style={SHEET_LABEL}>{t('Категория участников')}</div>
        <div style={{ display: 'flex', gap: 6, marginBottom: 20 }}>
          {(['FIZ', 'YUR'] as const).map((g) => {
            const isLocked = yurLocked && g === 'YUR';
            return (
              <button
                key={g}
                type="button"
                className={`fm-chip ${clgroup === g ? 'active' : ''}`}
                onClick={() => {
                  if (isLocked) {
                    showUpgrade({
                      tier: oiAccess.requiredTierFor({ flag: 'clgroup_yur' }) ?? 'basic',
                      featureName: t('данные по юрлицам'),
                      indicator: 'open_interest',
                    });
                    return;
                  }
                  setClgroup(g);
                  onOptionsClose();
                }}
                style={SHEET_CHIP}
                aria-disabled={isLocked}
              >
                {g === 'FIZ' ? t('Физлица') : t('Юрлица')}
                {isLocked && <Lock size={12} strokeWidth={2.2} />}
              </button>
            );
          })}
        </div>

        <div style={SHEET_LABEL}>{t('Горизонт')}</div>
        <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
          {(['short', 'medium'] as const).map((h) => (
            <button
              key={h}
              type="button"
              className={`fm-chip ${horizon === h ? 'active' : ''}`}
              onClick={() => { setHorizon(h); onOptionsClose(); }}
              style={SHEET_CHIP}
            >
              {h === 'short' ? t('Дни') : t('Недели')}
            </button>
          ))}
        </div>
        <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', lineHeight: 1.45, margin: '0 0 20px' }}>
          {isMed
            ? t('Сдвиг позиции за 14 торговых дней (около двух недель) против того, сколько актив обычно проходит за такой же срок. Показывает то, что копилось неделями и в дневной ленте незаметно.')
            : t('Движение позиции за один торговый день против обычного дневного размаха актива (ATR-14). Ловит резкие однодневные сдвиги. Это те же сигналы, что уходят в уведомления в Telegram.')}
        </p>

        <div style={SHEET_LABEL}>{t('Категория активов')}</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginBottom: 20 }}>
          {GROUP_OPTIONS.map((g) => (
            <button
              key={g.key}
              type="button"
              className={`fm-chip ${group === g.key ? 'active' : ''}`}
              onClick={() => { setGroup(g.key); onOptionsClose(); }}
              style={{ cursor: 'pointer' }}
            >
              {t(g.label)}
            </button>
          ))}
        </div>

        <div style={SHEET_LABEL}>{t('Избранное')}</div>
        <div style={{ display: 'flex' }}>
          <button
            type="button"
            className={`fm-chip ${onlyFav ? 'active' : ''}`}
            onClick={() => { setOnlyFav((v) => !v); onOptionsClose(); }}
            aria-pressed={onlyFav}
            style={{ cursor: 'pointer', gap: 6 }}
          >
            <Star size={13} fill={onlyFav ? 'currentColor' : 'none'} strokeWidth={2.2} />
            {t('Только избранные')}
            {favorites.length > 0 && <span className="mono" style={{ opacity: 0.75 }}>{favorites.length}</span>}
          </button>
        </div>
      </div>
    </MobileSheet>
  );

  // Пейволл. Заглушка вместо ленты (сейчас лента открыта всем тирам, ветка —
  // на случай, если oi_screener.open снова закроют в матрице).
  if (locked) {
    return (
      <>
        <div className="fm-ft-scroll"><OiScreenerLocked compact /></div>
        {sheet}
      </>
    );
  }

  return (
    <>
      <div className="fm-screener">
        {/* Шапка колонок. «Доля» и «Сила» — кнопки сортировки; над кометой —
            подписи краёв шкалы. */}
        <div className="fm-screener-grid fm-screener-head">
          <span style={{ gridColumn: '1 / 3' }}>{t('Актив')}</span>
          <span className="fm-screener-scale" aria-hidden><span>{t('шорт')}</span><span>{t('лонг')}</span></span>
          <button type="button" className="fm-screener-th" onClick={() => clickSort('skew')}>
            {t('Доля')}{sortArrow('skew')}
          </button>
          <button type="button" className="fm-screener-th" onClick={() => clickSort('ratio')}>
            {t('Сила')}{sortArrow('ratio')}
          </button>
          <span />
        </div>

        <div className="fm-screener-list">
          {error && (
            <div className="fm-screener-state">
              {t('Не удалось загрузить данные — попробуйте обновить страницу')}
            </div>
          )}
          {loading && <div className="fm-screener-state">{t('Загрузка…')}</div>}
          {!error && rows !== null && visible.length === 0 && (
            <div className="fm-screener-state">
              <div style={{ color: 'var(--text-primary)', fontWeight: 700, fontSize: 'var(--fs-base)' }}>
                {onlyFav ? t('Среди избранных активов пусто') : t('Нет активов по фильтру')}
              </div>
              <div style={{ marginTop: 4 }}>{t('Попробуйте снять фильтры')}</div>
            </div>
          )}

          {!error && visible.map((r) => {
            const isFav = favorites.includes(r.sectype);
            const long = (r.net_pct ?? 0) >= 0;
            return (
              <div
                key={r.sectype}
                role="button"
                tabIndex={0}
                onClick={() => pick(r.sectype)}
                onKeyDown={(e) => { if (e.key === 'Enter') pick(r.sectype); }}
                aria-label={`${r.name}: ${screenerSignalLabel(r, isMed).text}`}
                className="fm-screener-grid fm-screener-row"
                style={{ opacity: r.status === 'illiquid' || r.status === 'nodata' ? 0.55 : 1 }}
              >
                <InstrumentIcon sectype={r.sectype} size={18} />

                {/* Актив: узкая колонка, длинное имя растворяется у края. */}
                <div className="fm-screener-asset">
                  <div className="fm-screener-name-line">
                    <FadedName name={r.name} />
                    {/* «!» — позиции только на конец дня, сигнал T+1 (пояснение в подвале). */}
                    {!r.has_intraday && <span className="fm-screener-eod" aria-hidden>!</span>}
                  </div>
                  <div className="mono fm-screener-ticker">{r.front_secid || r.sectype}</div>
                </div>

                {/* Комета позиции — забирает всё свободное место строки. */}
                <div style={{ minWidth: 0 }}>
                  <PositionComet
                    netPct={r.net_pct}
                    netPctPrev={r.net_pct_prev}
                    ratio={r.ratio}
                    ratioLo={stats.ratioLo}
                    ratioHi={stats.ratioHi}
                    prevLabel={prevWord}
                    deltaLabel={overWord}
                    scaleOverride={0.7}
                    ringColor="var(--bg-primary)"
                  />
                </div>

                {/* Доля стороны перевеса: сторону говорят цвет и место точки на шкале. */}
                <div className="mono fm-screener-num">
                  {r.net_pct == null ? (
                    <span style={{ color: 'var(--text-muted)' }}>—</span>
                  ) : (
                    <span style={{ color: long ? 'var(--oi-green)' : 'var(--oi-short)' }}>{sideShare(r.net_pct)}%</span>
                  )}
                </div>

                {/* Сила — тем же кеглем, что доля; от 2× чёрным, слабее серым. */}
                <div className="mono fm-screener-num">
                  {r.ratio == null ? (
                    <span style={{ color: 'var(--text-muted)' }}>—</span>
                  ) : (
                    <span style={{ color: isStrengthAccent(r.ratio) ? 'var(--text-primary)' : 'var(--text-muted)' }}>
                      {fmtRatio(r.ratio)}
                    </span>
                  )}
                </div>

                <button
                  type="button"
                  onClick={(e) => toggleFavorite(r.sectype, e)}
                  aria-label={isFav ? t('Убрать из избранного') : t('В избранное')}
                  className="fm-screener-fav"
                  style={{ color: isFav ? 'var(--accent)' : 'var(--text-secondary)' }}
                >
                  <Star size={15} fill={isFav ? 'var(--accent)' : 'none'} strokeWidth={2} />
                </button>
              </div>
            );
          })}

          {/* Подвал: как читать строку. На десктопе это живёт в подсказках. */}
          {!error && rows !== null && visible.length > 0 && (
            <div className="fm-screener-foot">
              <p>{t('Нажмите на строку, чтобы открыть график актива.')}</p>
              <p>
                {t('Полоса — шкала долей: слева 100% шорт, по центру поровну, справа 100% лонг. Точка — где {{group}} стоят сейчас, хвост — сдвиг {{over}}.', { group: groupWord, over: overWord })}
              </p>
              <p>
                {isMed
                  ? t('Сила 4,3× — за 2 недели позиция сдвинулась в 4,3 раза сильнее, чем этот актив обычно проходит за такой срок.')
                  : t('Сила 4,3× — за день позиция сдвинулась в 4,3 раза сильнее, чем этот актив двигается обычно.')}
              </p>
              {hasEodOnly && (
                <p>{t('«!» — позиции по активу обновляются только на конец дня, сигнал появляется на следующий день.')}</p>
              )}
              <p className="mono">
                {pluralAssets(visible.length)} · {groupWord} · {isMed ? t('2 недели') : t('день')}
                {signalDate ? ` · ${fmtDataDate(signalDate)}` : ''}
              </p>
            </div>
          )}
        </div>
      </div>
      {sheet}
    </>
  );
}
