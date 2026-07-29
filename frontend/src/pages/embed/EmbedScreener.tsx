/**
 * EmbedScreener — компактный «скринер сигналов ОИ» для плавающей панели.
 *
 * Лента как в Telegram-алертах: активы, у которых дневной сдвиг чистой позиции
 * группы резче обычного (ATR14-кратность ×N), + перекос позиции (лонг/шорт %).
 * Данные — /api/oi/screener (дневные T+1). Это ужатая версия десктопной
 * OiScreenerTable: тут только фид (иконка · имя · перекос · ×N), без избранного,
 * алертов, сортировок и кометы — панель узкая, а колесо тянет высоту.
 *
 * Контролы в тулбаре: группа (Физ/Юр) · порог (Все/≥2×/≥3×/≥5×); категория и
 * пояснение — за ⚙. Периода нет → Shift+колесо не действует; обычное колесо
 * растит высоту (больше строк видно сразу).
 */
import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { User, Building2 } from 'lucide-react';
import InstrumentIcon from '../../components/InstrumentIcon';
import { getOiScreener, type OiScreenerRow } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, SegGroup } from './EmbedSettings';
import { EmbedFrame, PillGroup } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';

type Clgroup = 'FIZ' | 'YUR';
type ThresholdKey = '0' | '2' | '3' | '5';
type LoadStatus = 'loading' | 'ok' | 'error';

const THRESHOLD_OPTS: { id: ThresholdKey; label: string; title: string }[] = [
  { id: '0', label: 'Все', title: 'Все активы, включая без сигнала' },
  { id: '2', label: '≥2×', title: 'Движения минимум в 2× резче обычного' },
  { id: '3', label: '≥3×', title: 'Движения минимум в 3× резче обычного' },
  { id: '5', label: '≥5×', title: 'Движения минимум в 5× резче обычного' },
];

const CLGROUP_OPTS: { id: Clgroup; label: string; icon: ReactNode }[] = [
  { id: 'FIZ', label: 'Физ', icon: <User size={14} /> },
  { id: 'YUR', label: 'Юр', icon: <Building2 size={14} /> },
];

const GROUP_OPTS: { id: string; label: string }[] = [
  { id: 'all', label: 'Все категории' },
  { id: 'Индексы', label: 'Индексы' },
  { id: 'Валюта', label: 'Валюта' },
  { id: 'Сырьё', label: 'Сырьё' },
  { id: 'Акции', label: 'Акции' },
  { id: 'Крипто', label: 'Крипто' },
];

// Сила строки для порядка и фильтра. Строки без силы (мало участников / мало
// истории) — в конец: значения нет, ставить их в общий ряд не по чему.
const strengthOf = (r: OiScreenerRow): number => r.ratio ?? -1;

function fmtRatio(r: number): string {
  return r.toLocaleString('ru-RU', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '×';
}
function fmtDate(d: string): string {
  return new Date(d + 'T00:00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' });
}

const MONO = { fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontVariantNumeric: 'tabular-nums' as const };

/** `onPick` — клик по строке сигнала (§6.10 модели песочницы: как лента аномалий,
 *  открывает панель ОИ на этом активе). Не задан → строки не кликабельны (сайт/расширение). */
export default function EmbedScreener({ onPick }: { onPick?: (r: OiScreenerRow) => void } = {}) {
  const { rd, wr } = useEmbedPersist();
  const [clgroup, setClgroup] = useState<Clgroup>(() => rd('frame:embed:screener:clgroup', 'FIZ') as Clgroup);
  const [threshold, setThreshold] = useState<ThresholdKey>(() => rd('frame:embed:screener:threshold', '2') as ThresholdKey);
  const [group, setGroup] = useState<string>(() => rd('frame:embed:screener:group', 'all'));

  // Compact-режим тулбара (узкая панель sandbox — см. useToolbarCompact.ts).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();

  const [rows, setRows] = useState<OiScreenerRow[] | null>(null);
  const [signalDate, setSignalDate] = useState<string | null>(null);
  const [intradayDate, setIntradayDate] = useState<string | null>(null);
  const [status, setStatus] = useState<LoadStatus>('loading');

  useEffect(() => { wr('frame:embed:screener:clgroup', clgroup); }, [clgroup]);
  useEffect(() => { wr('frame:embed:screener:threshold', threshold); }, [threshold]);
  useEffect(() => { wr('frame:embed:screener:group', group); }, [group]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    setRows(null);
    getOiScreener(clgroup)
      .then((r) => {
        if (cancelled) return;
        setRows(r.rows);
        setSignalDate(r.signal_date);
        setIntradayDate(r.intraday_date);
        setStatus('ok');
      })
      .catch(() => { if (!cancelled) setStatus('error'); });
    return () => { cancelled = true; };
  }, [clgroup]);

  const visible = useMemo(() => {
    if (!rows) return [];
    const thr = Number(threshold);
    return rows
      // Фильтр силы — ПО САМОЙ СИЛЕ, а не по статусу. Раньше он требовал ещё и
      // status === 'sharp', и строка с силой 3,4×, не прошедшая гард
      // материальности, пропадала из выборки «≥2×» — фильтр выглядел сломанным.
      .filter((r) =>
        (group === 'all' || r.group === group) &&
        (thr === 0 || strengthOf(r) >= thr),
      )
      // Порядок сквозной по силе, без полос по статусу: полосы ставили тихую
      // sharp-строку выше громкой обычной, и лента противоречила колонке «Сила».
      .sort((a, b) => strengthOf(b) - strengthOf(a));
  }, [rows, group, threshold]);

  const groupWord = clgroup === 'FIZ' ? 'физлица' : 'юрлица';
  const dateLabel = signalDate
    ? (intradayDate && intradayDate > signalDate
        ? `дневные за ${fmtDate(signalDate)} · интрадей за ${fmtDate(intradayDate)}`
        : `по данным за ${fmtDate(signalDate)}`)
    : null;

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
            <PillGroup<ThresholdKey> value={threshold} options={THRESHOLD_OPTS} onChange={setThreshold} />
          </div>
          <PillGroup<Clgroup> value={clgroup} options={CLGROUP_OPTS} onChange={setClgroup} compact={toolbarCompact} />
          <PillGroup<ThresholdKey> value={threshold} options={THRESHOLD_OPTS} onChange={setThreshold} />
        </div>
      }
      more={
        <>
          <DrawerSection label="Категория">
            <SegGroup value={group} options={GROUP_OPTS} onChange={setGroup} />
          </DrawerSection>
          <div style={{ fontSize: 11, color: 'var(--text-secondary)', lineHeight: 1.5 }}>
            <b style={{ color: 'var(--text-primary)' }}>×N</b> — во сколько раз дневной сдвиг чистой позиции {groupWord} резче
            обычного (ATR-14). <b style={{ color: 'var(--text-primary)' }}>Перекос</b> — чистая позиция ÷ вся экспозиция группы
            (лонг/шорт). Данные дневные, T+1.
          </div>
        </>
      }
    >
      <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column' }}>
        <div className="styled-scrollbar" style={{ flex: 1, minHeight: 0, overflowY: 'auto' }}>
          {status === 'loading' && <EmbedMsg text="Загрузка…" />}
          {status === 'error' && <EmbedMsg text="Не удалось загрузить сигналы" />}
          {status === 'ok' && visible.length === 0 && (
            <EmbedMsg text={threshold !== '0' ? 'Тихий день — резких движений нет' : 'Нет активов по фильтру'} />
          )}
          {status === 'ok' && visible.map((r) => <ScreenerRow key={r.sectype} r={r} onClick={onPick ? () => onPick(r) : undefined} />)}
        </div>
        {status === 'ok' && dateLabel && (
          <div style={{ ...MONO, flexShrink: 0, padding: '6px 12px', fontSize: 10.5, color: 'var(--text-secondary)', borderTop: '1px solid var(--border-color, rgba(128,128,128,0.18))', textAlign: 'right' }}>
            {visible.length} · {groupWord} · {dateLabel}
          </div>
        )}
      </div>
    </EmbedFrame>
  );
}

function ScreenerRow({ r, onClick }: { r: OiScreenerRow; onClick?: () => void }) {
  const dim = r.status === 'illiquid' || r.status === 'nodata';
  const long = (r.net_pct ?? 0) >= 0;
  const perekosColor = long ? 'var(--oi-green)' : 'var(--oi-short)';
  const sharp = r.status === 'sharp' && r.ratio != null;
  const strong = (r.ratio ?? 0) >= 5;
  const mid = (r.ratio ?? 0) >= 3;

  return (
    <div
      onClick={onClick}
      title={onClick ? `Открыть открытые позиции — ${r.name}` : undefined}
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 10,
        padding: '8px 12px',
        borderBottom: '0.5px solid var(--border-color, rgba(128,128,128,0.22))',
        opacity: dim ? 0.55 : 1,
        cursor: onClick ? 'pointer' : 'default',
      }}
    >
      <span style={{ flexShrink: 0, lineHeight: 0 }}>
        <InstrumentIcon sectype={r.sectype} size={26} />
      </span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontWeight: 700, fontSize: 12.5, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {r.name}
        </div>
        <div style={{ ...MONO, fontSize: 10.5, color: 'var(--text-secondary)', letterSpacing: '0.03em' }}>
          {r.front_secid || r.sectype}
        </div>
      </div>

      {r.net_pct != null && (
        <div style={{ ...MONO, textAlign: 'right', flexShrink: 0, lineHeight: 1.1, color: perekosColor, opacity: sharp ? 1 : 0.62 }}>
          <div style={{ fontWeight: 800, fontSize: 12 }}>{Math.abs(r.net_pct).toFixed(1).replace('.', ',')}%</div>
          <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: '0.05em', textTransform: 'uppercase' }}>{long ? 'лонг' : 'шорт'}</div>
        </div>
      )}

      <span
        style={{
          ...MONO,
          flexShrink: 0,
          minWidth: 46,
          textAlign: 'center',
          padding: '3px 8px',
          borderRadius: 999,
          fontSize: 12,
          fontWeight: 800,
          border: `1.5px solid ${sharp ? (strong || mid ? 'var(--accent)' : 'var(--text-primary)') : 'var(--border-color, rgba(128,128,128,0.35))'}`,
          background: sharp && strong ? 'var(--accent)' : 'transparent',
          color: sharp ? (strong ? 'var(--text-inverse)' : mid ? 'var(--accent)' : 'var(--text-primary)') : 'var(--text-muted)',
        }}
      >
        {r.ratio != null ? fmtRatio(r.ratio) : '—'}
      </span>
    </div>
  );
}
