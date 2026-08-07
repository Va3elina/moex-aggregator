/**
 * EmbedScreener — «Скринер сигналов ОИ» для плавающей панели/embed-роута.
 *
 * Сайтовая ИНФОГРАФИКА (комета позиции, пилюли «Силы», рекорды, избранное) в
 * КОМПАКТНОЙ вёрстке под панель: embed-контролы (PillGroup — «кнопки меньше»),
 * фиксированные px вместо fluid-токенов, сетка влезает в дефолтную ширину
 * панели без горизонтального скролла и растягивания. Прямое переиспользование
 * OiScreenerTable пробовали (2026-08-07) — сайтовые SegmentedControl/Dropdown
 * и minWidth 1000 требовали растягивать панель на весь экран, откатили в тот
 * же день.
 *
 * Процентов лонг/шорт в строках НЕТ (как на сайте): точный перекос, дельта и
 * сила — в title-подсказке кометы.
 *
 * Логика ленты (сортировка по силе, пороги «резко» по горизонту, приоритет
 * рекордов над глаголом, калибровка головы кометы по видимой выборке) —
 * зеркалит OiScreenerTable; при изменении методологии там — синхронизировать.
 * Избранное — общий ключ localStorage с сайтом (favoriteInstruments).
 */
import { useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { User, Building2, Star, Clock, Columns3 } from 'lucide-react';
import InstrumentIcon from '../../components/InstrumentIcon';
import PositionComet from '../../components/oi/PositionComet';
import { getOiScreener, type OiScreenerRow, type OiScreenerHorizon } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, SegGroup } from './EmbedSettings';
import { EmbedFrame, PillGroup, ToolbarButton } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import { useToolbarCompact } from './useToolbarCompact';

type Clgroup = 'FIZ' | 'YUR';
type LoadStatus = 'loading' | 'ok' | 'error';

const CLGROUP_OPTS: { id: Clgroup; label: string; icon: ReactNode }[] = [
  { id: 'FIZ', label: 'Физ', icon: <User size={14} /> },
  { id: 'YUR', label: 'Юр', icon: <Building2 size={14} /> },
];

// Подписи как на сайте («Дни»/«Недели», не «1 день»): дневной срез приходит
// T+1, точная цифра на кнопке обещала бы свежесть, которой у ленты нет.
// Иконки — та же пара, что у шага столбца в «Деньгах в фондах» (Clock=день,
// Columns3=неделя): на дефолтной ширине панели 440 полная подпись «Недели»
// не влезала, compact-режим сворачивает пилюли до иконок.
const HORIZON_OPTS: { id: OiScreenerHorizon; label: string; title: string; icon: ReactNode }[] = [
  { id: 'short', label: 'Дни', title: 'Движение позиции за день против обычного дневного размаха (ATR-14)', icon: <Clock size={14} /> },
  { id: 'medium', label: 'Недели', title: 'Сдвиг за 14 торговых дней против нормы за такой же срок', icon: <Columns3 size={14} /> },
];

const GROUP_OPTS: { id: string; label: string }[] = [
  { id: 'all', label: 'Все категории' },
  { id: 'Индексы', label: 'Индексы' },
  { id: 'Валюта', label: 'Валюта' },
  { id: 'Сырьё', label: 'Сырьё' },
  { id: 'Акции', label: 'Акции' },
  { id: 'Крипто', label: 'Крипто' },
];

const FAVORITES_KEY = 'favoriteInstruments'; // общий ключ с сайтом

const PERIOD_WORD: Record<string, string> = {
  all: 'за всё время', '5y': 'за 5 лет', '4y': 'за 4 года',
  '3y': 'за 3 года', '2y': 'за 2 года', '1y': 'за год', '6m': 'за полгода',
  '3m': 'за квартал',
};

function fmtRatio(r: number): string {
  return r.toLocaleString('ru-RU', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '×';
}
function fmtDate(d: string): string {
  return new Date(d + 'T00:00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' });
}

const MONO = { fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontVariantNumeric: 'tabular-nums' as const };

// Актив · комета (тянется) · сила · сигнал · ★. Минимум сетки ~415px —
// звезда и сигнал видны даже на MINW панели; сумма minmax-минимумов + gap +
// падинги строк ОБЯЗАНА оставаться меньше стартовой ширины панели
// (SIZES.screener в SandboxPage), иначе правая колонка уезжает за край.
const GRID: CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'minmax(106px, 150px) minmax(110px, 1fr) 50px minmax(86px, 118px) 22px',
  gap: 6,
  alignItems: 'center',
};

/** `onPick` — клик по строке сигнала (§6.10 модели песочницы: как лента аномалий,
 *  открывает панель ОИ на этом активе). Не задан → строки не кликабельны (embed-роут). */
export default function EmbedScreener({ onPick }: { onPick?: (r: { sectype: string }) => void } = {}) {
  const { rd, wr } = useEmbedPersist();
  const [clgroup, setClgroup] = useState<Clgroup>(() => rd('frame:embed:screener:clgroup', 'FIZ') as Clgroup);
  const [horizon, setHorizon] = useState<OiScreenerHorizon>(() => rd('frame:embed:screener:horizon', 'short') as OiScreenerHorizon);
  const [group, setGroup] = useState<string>(() => rd('frame:embed:screener:group', 'all'));
  const [onlyFav, setOnlyFav] = useState<boolean>(() => rd('frame:embed:screener:onlyfav', '0') === '1');
  const [favorites, setFavorites] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem(FAVORITES_KEY);
      return saved ? JSON.parse(saved) : [];
    } catch { return []; }
  });

  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();

  const [rows, setRows] = useState<OiScreenerRow[] | null>(null);
  const [minPart, setMinPart] = useState<number>(50);
  const [signalDate, setSignalDate] = useState<string | null>(null);
  const [status, setStatus] = useState<LoadStatus>('loading');

  useEffect(() => { wr('frame:embed:screener:clgroup', clgroup); }, [clgroup]);
  useEffect(() => { wr('frame:embed:screener:horizon', horizon); }, [horizon]);
  useEffect(() => { wr('frame:embed:screener:group', group); }, [group]);
  useEffect(() => { wr('frame:embed:screener:onlyfav', onlyFav ? '1' : '0'); }, [onlyFav]);

  // Реалтайм: дневной пересчёт (SSE 'daily') → тихий рефетч, лента не гаснет.
  const [refreshTick, setRefreshTick] = useState(0);
  const silentRef = useRef(false);
  useRealtimeData(['daily'], () => { silentRef.current = true; setRefreshTick((t) => t + 1); });

  useEffect(() => {
    let cancelled = false;
    if (!silentRef.current) { setStatus('loading'); setRows(null); }
    silentRef.current = false;
    getOiScreener(clgroup, horizon)
      .then((r) => {
        if (cancelled) return;
        setRows(r.rows);
        setMinPart(r.min_part);
        setSignalDate(r.signal_date);
        setStatus('ok');
      })
      .catch(() => { if (!cancelled) setStatus('error'); });
    return () => { cancelled = true; };
  }, [clgroup, horizon, refreshTick]);

  const toggleFavorite = (sectype: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setFavorites((prev) => {
      const next = prev.includes(sectype) ? prev.filter((s) => s !== sectype) : [...prev, sectype];
      localStorage.setItem(FAVORITES_KEY, JSON.stringify(next));
      return next;
    });
  };

  // Порядок сквозной по силе, строки без силы — в конце (зеркалит сайт).
  const visible = useMemo(() => {
    if (!rows) return [];
    const favSet = new Set(favorites);
    return rows
      .filter((r) =>
        (group === 'all' || r.group === group) &&
        (!onlyFav || favSet.has(r.sectype)),
      )
      .sort((a, b) => {
        if (a.ratio == null || b.ratio == null) {
          if (a.ratio == null && b.ratio == null) return 0;
          return a.ratio == null ? 1 : -1;
        }
        return b.ratio - a.ratio;
      });
  }, [rows, favorites, group, onlyFav]);

  // Калибровка головы кометы — по видимой выборке (как на сайте).
  const stats = useMemo(() => {
    const sharp = visible.filter((r) => r.status === 'sharp' && r.ratio != null).map((r) => r.ratio!);
    return {
      ratioLo: sharp.length ? Math.min(...sharp) : null,
      ratioHi: sharp.length ? Math.max(...sharp) : null,
    };
  }, [visible]);

  const isMed = horizon === 'medium';
  const groupWord = clgroup === 'FIZ' ? 'физлица' : 'юрлица';
  const groupGen = clgroup === 'FIZ' ? 'физлиц' : 'юрлиц';
  const W = {
    over: isMed ? 'за 2 недели' : 'за день',
    prev: isMed ? '2 недели назад' : 'вчера',
    usual: isMed ? 'обычного движения за 2 недели' : 'обычного дневного движения',
    thr: isMed ? '3×' : '2×',
  };
  const sharpThr = isMed ? 3 : 2;

  // Пилюля «Силы» — ступени как на сайте: ×1 контур, ×1,5 контур accent,
  // ×2 (или лидер выборки от ×1,5) — заливка accent.
  const ratioCell = (r: OiScreenerRow) => {
    if (r.ratio == null) return <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>—</span>;
    const pill: CSSProperties = {
      ...MONO, fontSize: 11.5, fontWeight: 800, letterSpacing: '0.01em',
      whiteSpace: 'nowrap', display: 'inline-block',
      padding: '2px 8px', borderRadius: 999, border: '1.5px solid',
    };
    if (r.status !== 'sharp') {
      return (
        <span
          title={`Изменение позиции ${groupGen} ${W.over} в ${fmtRatio(r.ratio)} от ${W.usual} — ниже порога «резко» (${W.thr})`}
          style={{ ...pill, borderColor: 'var(--text-muted)', color: 'var(--text-muted)' }}
        >
          {fmtRatio(r.ratio)}
        </span>
      );
    }
    const leader = stats.ratioHi != null && r.ratio >= stats.ratioHi - 1e-9;
    const strong = r.ratio >= sharpThr * 2 || (leader && r.ratio >= sharpThr * 1.5);
    const mid = r.ratio >= sharpThr * 1.5;
    return (
      <span
        title={`Изменение позиции ${groupGen} ${W.over} в ${fmtRatio(r.ratio)} сильнее ${W.usual}${strong ? ' — особо сильное движение' : mid ? ' — сильное движение' : ''}`}
        style={{
          ...pill,
          borderColor: mid ? 'var(--accent)' : 'var(--text-primary)',
          background: strong ? 'var(--accent)' : 'transparent',
          color: strong ? '#fff' : mid ? 'var(--accent)' : 'var(--text-primary)',
        }}
      >
        {fmtRatio(r.ratio)}
      </span>
    );
  };

  // Сигнал: рекорд (перебивает всё) → глагол резкого движения → пометка.
  const signalCell = (r: OiScreenerRow) => {
    if (r.net_record) {
      const high = r.net_record.kind === 'high';
      return (
        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--accent)', whiteSpace: 'nowrap', cursor: 'help' }}
          title={`Чистая позиция ${groupGen} (в контрактах) — исторический ${high ? 'максимум' : 'минимум'} за всё время наблюдений.`}>
          {high ? 'Ист. макс' : 'Ист. мин'}
        </span>
      );
    }
    if (r.record) {
      const high = r.record.kind === 'high';
      return (
        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--accent)', whiteSpace: 'nowrap', cursor: 'help' }}
          title={`Перекос ${groupGen} пробил ${high ? 'максимум (рекордный лонг)' : 'минимум (рекордный шорт)'} ${PERIOD_WORD[r.record.period]}.`}>
          {high ? 'Макс' : 'Мин'} {PERIOD_WORD[r.record.period]}
        </span>
      );
    }
    if (r.status === 'sharp' && r.ratio != null && r.direction) {
      const netLong = r.net >= 0;
      const grew = netLong === (r.direction === 'up');
      const verb = netLong
        ? (grew ? 'Набрали лонг' : 'Сократили лонг')
        : (grew ? 'Нарастили шорт' : 'Сократили шорт');
      return (
        <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-primary)', whiteSpace: 'nowrap' }}
          title={`${groupWord[0].toUpperCase() + groupWord.slice(1)} резко ${grew ? 'нарастили' : 'сократили'} ${netLong ? 'длинную' : 'короткую'} позицию по «${r.name}»: изменение ${W.over} в ${fmtRatio(r.ratio)} сильнее ${W.usual}.`}>
          {verb}
        </span>
      );
    }
    const note = r.status === 'normal'
      ? (isMed ? 'обычные 2 недели' : 'обычный день')
      : r.status === 'illiquid' ? 'мало участников' : 'мало истории';
    const noteTitle = r.status === 'illiquid'
      ? `На стороне ${groupWord}: ${r.npart} участников — ниже порога ликвидности ${minPart}.`
      : r.status === 'nodata'
        ? 'Мало истории для расчёта нормы движения.'
        : `Сдвиг ${W.over} в пределах обычного — ниже порога «резко» (${W.thr}).`;
    return <span style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)', whiteSpace: 'nowrap' }} title={noteTitle}>{note}</span>;
  };

  const headCell: CSSProperties = {
    ...MONO, fontSize: 9.5, fontWeight: 800, letterSpacing: '0.06em',
    textTransform: 'uppercase', color: 'var(--text-secondary)', whiteSpace: 'nowrap',
  };

  return (
    <EmbedFrame
      lead={
        <span style={{ fontWeight: 800, fontSize: 13, letterSpacing: '-0.01em', paddingLeft: 2, flexShrink: 0 }}>
          Скринер сигналов
        </span>
      }
      toolbarUnified
      toolbar={
        <div ref={toolbarWrapRef} style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {/* Невидимый измеритель — см. useToolbarCompact.ts: всегда полные лейблы. */}
          <div ref={toolbarMeasureRef} aria-hidden style={{ position: 'absolute', top: 0, left: 0, visibility: 'hidden', pointerEvents: 'none', display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}>
            <PillGroup<Clgroup> value={clgroup} options={CLGROUP_OPTS} onChange={setClgroup} />
            <PillGroup<OiScreenerHorizon> value={horizon} options={HORIZON_OPTS} onChange={setHorizon} />
            <ToolbarButton label="Избранные" icon={<Star size={14} />} onClick={() => {}} />
          </div>
          <PillGroup<Clgroup> value={clgroup} options={CLGROUP_OPTS} onChange={setClgroup} compact={toolbarCompact} />
          <PillGroup<OiScreenerHorizon> value={horizon} options={HORIZON_OPTS} onChange={setHorizon} compact={toolbarCompact} />
          <ToolbarButton
            label="Избранные"
            icon={<Star size={14} fill={onlyFav ? 'currentColor' : 'none'} />}
            title="Показать только избранные активы"
            active={onlyFav}
            compact={toolbarCompact}
            onClick={() => setOnlyFav((v) => !v)}
          />
        </div>
      }
      more={
        <>
          <DrawerSection label="Категория">
            <SegGroup value={group} options={GROUP_OPTS} onChange={setGroup} />
          </DrawerSection>
          <div style={{ fontSize: 11, color: 'var(--text-secondary)', lineHeight: 1.5 }}>
            <b style={{ color: 'var(--text-primary)' }}>Комета</b> — где {groupWord} стоят на оси −100…+100 (слева шорт,
            справа лонг), хвост — сдвиг {W.over}, размер головы — сила движения.{' '}
            <b style={{ color: 'var(--text-primary)' }}>Сила</b> — во сколько раз изменение позиции {W.over} резче
            обычного; «резко» — от {W.thr}. Точные проценты — при наведении на комету. Данные дневные, T+1.
          </div>
        </>
      }
    >
      <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column' }}>
        <div className="styled-scrollbar" style={{ flex: 1, minHeight: 0, overflowY: 'auto', overflowX: 'hidden' }}>
          {status === 'loading' && <EmbedMsg text="Загрузка…" />}
          {status === 'error' && <EmbedMsg text="Не удалось загрузить сигналы" />}
          {status === 'ok' && visible.length === 0 && (
            <EmbedMsg text={onlyFav ? 'Среди избранных активов пусто' : 'Нет активов по фильтру'} />
          )}
          {status === 'ok' && visible.length > 0 && (
            <>
              {/* Шапка — прилипает при скролле ленты */}
              <div style={{ ...GRID, position: 'sticky', top: 0, zIndex: 2, background: 'var(--bg-secondary)', padding: '6px 8px 6px 12px', borderBottom: '1.5px solid var(--text-primary)' }}>
                <span style={headCell}>Актив</span>
                <span style={headCell} title={`Ось перекоса −100…+100: слева полный шорт, по центру ноль, справа полный лонг. Голова = где ${groupWord} сейчас, хвост = сдвиг ${W.over}.`}>
                  Позиция {groupGen}
                </span>
                <span style={{ ...headCell, justifySelf: 'center' }}>Сила</span>
                <span style={{ ...headCell, justifySelf: 'center' }}>Сигнал</span>
                <span />
              </div>
              {visible.map((r) => {
                const isFav = favorites.includes(r.sectype);
                const dim = r.status === 'illiquid' || r.status === 'nodata';
                return (
                  <div
                    key={r.sectype}
                    onClick={onPick ? () => onPick({ sectype: r.sectype }) : undefined}
                    title={onPick ? `Открыть открытые позиции — ${r.name}` : undefined}
                    style={{
                      ...GRID,
                      padding: '6px 8px 6px 9px',
                      borderBottom: '1px solid var(--border-color, rgba(128,128,128,0.18))',
                      borderLeft: `3px solid ${r.record || r.net_record ? 'var(--accent)' : 'transparent'}`,
                      opacity: dim ? 0.55 : 1,
                      cursor: onPick ? 'pointer' : 'default',
                    }}
                  >
                    {/* Актив */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
                      <InstrumentIcon sectype={r.sectype} size={24} />
                      <div style={{ minWidth: 0 }}>
                        <div style={{ fontWeight: 700, fontSize: 12, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }} title={r.name}>
                          {r.name}
                        </div>
                        <div style={{ ...MONO, fontSize: 9.5, color: 'var(--text-secondary)', letterSpacing: '0.04em' }}>
                          {r.front_secid || r.sectype}
                        </div>
                      </div>
                    </div>
                    {/* Комета — масштаб 1 (вьюпортный ×1,5 раздувал бы строку панели) */}
                    <div style={{ minWidth: 0 }}>
                      <PositionComet
                        netPct={r.net_pct}
                        netPctPrev={r.net_pct_prev}
                        ratio={r.ratio}
                        ratioLo={stats.ratioLo}
                        ratioHi={stats.ratioHi}
                        prevLabel={W.prev}
                        deltaLabel={W.over}
                        scaleOverride={1}
                      />
                    </div>
                    {/* Сила */}
                    <div style={{ justifySelf: 'center' }}>{ratioCell(r)}</div>
                    {/* Сигнал */}
                    <div style={{ justifySelf: 'center', minWidth: 0, overflow: 'hidden' }}>{signalCell(r)}</div>
                    {/* ★ */}
                    <button
                      type="button"
                      onClick={(e) => toggleFavorite(r.sectype, e)}
                      aria-label={isFav ? 'Убрать из избранного' : 'В избранное'}
                      style={{
                        background: 'none', border: 'none', cursor: 'pointer', padding: 2,
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        color: isFav ? 'var(--accent)' : 'var(--text-secondary)', lineHeight: 0,
                      }}
                    >
                      <Star size={16} fill={isFav ? 'var(--accent)' : 'none'} strokeWidth={2} />
                    </button>
                  </div>
                );
              })}
            </>
          )}
        </div>
        {status === 'ok' && signalDate && (
          <div style={{ ...MONO, flexShrink: 0, padding: '5px 12px', fontSize: 10, color: 'var(--text-secondary)', borderTop: '1px solid var(--border-color, rgba(128,128,128,0.18))', textAlign: 'right' }}>
            {visible.length} · {groupWord} · {isMed ? '2 недели' : 'день'} · {fmtDate(signalDate)}
          </div>
        )}
      </div>
    </EmbedFrame>
  );
}
