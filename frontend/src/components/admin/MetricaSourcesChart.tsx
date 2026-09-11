/**
 * «По источникам трафика» — график блока Метрики в /admin/stats, как на
 * сводке самой Метрики: выбранный показатель по дням (на длинных периодах
 * по неделям), линия «Всего» и по линии на каждый источник. Легенда включает
 * и выключает линии и показывает итог за период, есть вид таблицей.
 *
 * Цвет закреплён за источником (SOURCE_SLOT), а не за местом в списке:
 * выключенная линия не перекрашивает остальные. Палитра --viz-1…8 задана в
 * index.css для обеих editorial-тем и проверена на различимость при
 * дальтонизме на фоне карточки.
 */
import { useMemo, useRef, useState } from 'react';
import HelpTooltip from '../HelpTooltip';
import { useElementWidth } from '../../hooks/useElementWidth';
import type { MetricaBySource, MetricaMetric } from '../../services/api';

// Порядок слотов — часть проверки палитры на дальтонизм, не переставлять.
const SOURCE_SLOT: Record<string, number> = {
  direct: 1, organic: 2, referral: 3, social: 4,
  internal: 5, messenger: 6, recommend: 7, ad: 8,
};
// Тема без --viz-* (не editorial) получает светлые значения той же палитры.
const FALLBACK = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948'];

const SHORT_NAME: Record<string, string> = {
  direct: 'Прямые заходы', organic: 'Поиск', referral: 'Ссылки на сайтах', social: 'Соцсети',
  internal: 'Внутренние переходы', messenger: 'Мессенджеры', recommend: 'Рекомендации', ad: 'Реклама',
};

const TOTAL = '__total';
const PLOT_H = 240;
const AXIS_H = 26;
const PAD_TOP = 10;
const PAD_LEFT = 52;
const PAD_RIGHT = 12;

function sourceColor(id: string): string {
  const slot = SOURCE_SLOT[id];
  return slot ? `var(--viz-${slot}, ${FALLBACK[slot - 1]})` : 'var(--text-muted)';
}

function parseDay(s: string): Date {
  const [y, m, d] = s.split('-').map(Number);
  return new Date(y, m - 1, d);
}

const fmtDay = (s: string) => parseDay(s).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
const fmtDayYear = (s: string) => parseDay(s).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
const fmtDayLong = (s: string) => parseDay(s).toLocaleDateString('ru-RU', { weekday: 'short', day: 'numeric', month: 'long' });
const fmtDayMonth = (s: string) => parseDay(s).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' });

function niceStep(raw: number): number {
  const exp = Math.pow(10, Math.floor(Math.log10(raw)));
  const f = raw / exp;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10) * exp;
}

/** Деления оси от нуля: 3–5 круглых шагов. Время — целыми минутами, счётчики — целыми. */
function axisTicks(max: number, kind: 'int' | 'float' | 'time'): number[] {
  if (!(max > 0)) return [0, 1];
  const unit = kind === 'time' && max > 120 ? 60 : 1;
  let step = niceStep(max / unit / 4) * unit;
  if (kind !== 'float') step = Math.max(1, step);
  const count = Math.ceil(max / step);
  return Array.from({ length: count + 1 }, (_, i) => +(i * step).toFixed(6));
}

interface Props {
  data: MetricaBySource;
  metric: MetricaMetric;
  /** Название показателя — для подписи и описания графика. */
  label: string;
  hint: string;
  /** Итог за период для «Всего» — из сводки, а не сумма дней. */
  totalValue: number;
  format: (v: number) => string;
  formatAxis?: (v: number) => string;
}

interface Line { id: string; name: string; full: string; color: string; values: number[]; period: number }

export default function MetricaSourcesChart({ data, metric, label, hint, totalValue, format, formatAxis = format }: Props) {
  const rootRef = useRef<HTMLDivElement>(null);
  const width = useElementWidth(rootRef);
  const [hidden, setHidden] = useState<ReadonlySet<string>>(() => new Set());
  const [hover, setHover] = useState<number | null>(null);
  const [asTable, setAsTable] = useState(false);

  const lines = useMemo<Line[]>(() => [
    { id: TOTAL, name: 'Всего', full: 'Всего по сайту', color: 'var(--text-primary)', values: data.total[metric] ?? [], period: totalValue },
    ...data.series.map((s) => ({
      id: s.id,
      name: SHORT_NAME[s.id] ?? s.name,
      full: s.name,
      color: sourceColor(s.id),
      values: s.values[metric] ?? [],
      period: s.period[metric] ?? 0,
    })),
  ], [data, metric, totalValue]);

  const shown = lines.filter((l) => !hidden.has(l.id));
  const n = data.dates.length;
  const week = data.group === 'week';
  const kind = metric === 'avg_visit_sec' ? 'time' : metric === 'page_depth' || metric === 'bounce_pct' ? 'float' : 'int';
  const ticks = axisTicks(shown.reduce((m, l) => Math.max(m, ...l.values), 0), kind);
  const top = ticks[ticks.length - 1] || 1;

  const plotW = Math.max(0, width - PAD_LEFT - PAD_RIGHT);
  const step = n > 1 ? plotW / (n - 1) : plotW;
  const x = (i: number) => PAD_LEFT + (n > 1 ? i * step : plotW / 2);
  const y = (v: number) => PAD_TOP + PLOT_H - (v / top) * PLOT_H;
  const h = hover !== null && hover < n ? hover : null;

  const weekends = week ? [] : data.dates.flatMap((d, i) => {
    const wd = parseDay(d).getDay();
    return wd === 0 || wd === 6 ? [i] : [];
  });
  const every = Math.max(1, Math.ceil(n / Math.max(2, Math.floor(plotW / (week ? 72 : 56)))));
  const anchor = (i: number): 'start' | 'middle' | 'end' =>
    n > 1 && i === 0 ? 'start' : x(i) > PAD_LEFT + plotW - 28 ? 'end' : 'middle';
  const path = (vals: number[]) => vals.map((v, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join('');
  const dateLabel = (i: number) => (week
    ? `${fmtDayMonth(data.dates[i])} – ${fmtDayMonth(data.ends[i] ?? data.dates[i])}`
    : fmtDayLong(data.dates[i]));

  const pick = (clientX: number, el: Element) => {
    const i = n > 1 ? Math.round((clientX - el.getBoundingClientRect().left - PAD_LEFT) / step) : 0;
    setHover(Math.min(n - 1, Math.max(0, i)));
  };
  const onKey = (e: React.KeyboardEvent) => {
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    e.preventDefault();
    setHover(Math.min(n - 1, Math.max(0, (h ?? n - 1) + (e.key === 'ArrowRight' ? 1 : -1))));
  };
  const toggle = (id: string) => setHidden((prev) => {
    const next = new Set(prev);
    if (next.has(id)) next.delete(id); else next.add(id);
    return next;
  });

  const thStyle = (align: 'left' | 'right'): React.CSSProperties => ({
    position: 'sticky', top: 0, background: 'var(--bg-secondary)', color: 'var(--text-muted)',
    fontWeight: 500, textAlign: align, padding: align === 'left' ? '4px 8px 6px 0' : '4px 0 6px 12px', whiteSpace: 'nowrap',
  });

  return (
    <div ref={rootRef}>
      <div className="flex items-center justify-between gap-2 mb-2">
        <div className="flex items-center gap-1.5 text-xs" style={{ color: 'var(--text-muted)' }}>
          <span>{label} · по {week ? 'неделям' : 'дням'}</span>
          <HelpTooltip icon="help" title="По источникам трафика" content={hint} size={12} />
        </div>
        <button
          type="button"
          onClick={() => setAsTable(!asTable)}
          className="editorial-press rounded-full text-xs"
          style={{ padding: 'var(--sp-1) var(--sp-3)' }}
        >
          {asTable ? 'Графиком' : 'Таблицей'}
        </button>
      </div>

      {asTable ? (
        <div style={{ maxHeight: PAD_TOP + PLOT_H + AXIS_H, overflow: 'auto', scrollbarGutter: 'stable', paddingRight: 6 }}>
          <table className="w-full text-xs" style={{ borderCollapse: 'collapse', fontVariantNumeric: 'tabular-nums' }}>
            <thead>
              <tr>
                <th style={thStyle('left')}>{week ? 'Неделя' : 'День'}</th>
                {shown.map((l) => <th key={l.id} style={thStyle('right')} title={l.full}>{l.name}</th>)}
              </tr>
            </thead>
            <tbody>
              {data.dates.map((d, i) => ({ d, i })).reverse().map(({ d, i }) => (
                <tr key={d} style={{ borderTop: '1px solid var(--chart-grid)' }}>
                  <td style={{ padding: '4px 8px 4px 0', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>{dateLabel(i)}</td>
                  {shown.map((l) => (
                    <td key={l.id} style={{ padding: '4px 0 4px 12px', textAlign: 'right', color: 'var(--text-primary)' }}>
                      {format(l.values[i] ?? 0)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div
          className="relative"
          tabIndex={0}
          onKeyDown={onKey}
          onFocus={() => setHover((v) => v ?? n - 1)}
          onBlur={() => setHover(null)}
          aria-label={`${label} по ${week ? 'неделям' : 'дням'}. Стрелки влево и вправо показывают значения по датам.`}
        >
          {width > 0 && (
            <svg
              width={width}
              height={PAD_TOP + PLOT_H + AXIS_H}
              role="img"
              aria-label={`${label} за период: ${shown.map((l) => `${l.name} ${format(l.period)}`).join(', ')}`}
              style={{ display: 'block', fontVariantNumeric: 'tabular-nums' }}
            >
              {weekends.map((i) => {
                const x0 = Math.max(PAD_LEFT, x(i) - step / 2);
                const x1 = Math.min(PAD_LEFT + plotW, x(i) + step / 2);
                return <rect key={i} x={x0} y={PAD_TOP} width={Math.max(0, x1 - x0)} height={PLOT_H} fill="var(--chart-area)" />;
              })}
              {ticks.map((t) => (
                <g key={t}>
                  <line x1={PAD_LEFT} x2={PAD_LEFT + plotW} y1={y(t)} y2={y(t)} stroke="var(--chart-grid)" strokeWidth={1} shapeRendering="crispEdges" />
                  <text x={PAD_LEFT - 8} y={y(t)} dy="0.32em" textAnchor="end" fontSize={11} fill="var(--text-muted)">{formatAxis(t)}</text>
                </g>
              ))}
              {data.dates.map((d, i) => (i % every === 0 ? (
                <text key={d} x={x(i)} y={PAD_TOP + PLOT_H + 17} textAnchor={anchor(i)} fontSize={11} fill="var(--text-muted)">
                  {week ? fmtDayYear(d) : fmtDay(d)}
                </text>
              ) : null))}
              {shown.map((l) => (n > 1 ? (
                <path key={l.id} d={path(l.values)} fill="none" stroke={l.color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
              ) : (
                <circle key={l.id} cx={x(0)} cy={y(l.values[0] ?? 0)} r={4} fill={l.color} />
              )))}
              {h !== null && (
                <g pointerEvents="none">
                  <line x1={x(h)} x2={x(h)} y1={PAD_TOP} y2={PAD_TOP + PLOT_H} stroke="var(--chart-crosshair)" strokeWidth={1} />
                  {shown.map((l) => (
                    <circle key={l.id} cx={x(h)} cy={y(l.values[h] ?? 0)} r={4} fill={l.color} stroke="var(--bg-secondary)" strokeWidth={2} />
                  ))}
                </g>
              )}
              <rect
                x={PAD_LEFT} y={PAD_TOP} width={plotW} height={PLOT_H} fill="transparent"
                onPointerMove={(e) => pick(e.clientX, e.currentTarget.ownerSVGElement ?? e.currentTarget)}
                onPointerLeave={() => setHover(null)}
              />
            </svg>
          )}
          {h !== null && shown.length > 0 && (
            <div
              className="absolute pointer-events-none text-xs"
              style={{
                top: PAD_TOP,
                left: x(h),
                transform: x(h) > width * 0.6 ? 'translateX(calc(-100% - 12px))' : 'translateX(12px)',
                background: 'var(--bg-elevated)',
                border: '1px solid var(--border-color)',
                borderRadius: 8,
                padding: '8px 10px',
                whiteSpace: 'nowrap',
                zIndex: 5,
              }}
            >
              <div className="mb-1" style={{ color: 'var(--text-muted)' }}>{dateLabel(h)}</div>
              {shown.map((l) => (
                <div key={l.id} className="flex items-center gap-2" style={{ lineHeight: 1.6 }}>
                  <span aria-hidden style={{ width: 12, height: 2, borderRadius: 1, background: l.color, flexShrink: 0 }} />
                  <span style={{ color: 'var(--text-primary)', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
                    {format(l.values[h] ?? 0)}
                  </span>
                  <span style={{ color: 'var(--text-secondary)' }}>{l.name}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      <div className="flex flex-wrap gap-2 mt-3">
        {lines.map((l) => {
          const off = hidden.has(l.id);
          return (
            <button
              key={l.id}
              type="button"
              aria-pressed={!off}
              title={`${l.full}. Нажмите, чтобы ${off ? 'показать' : 'скрыть'} линию`}
              onClick={() => toggle(l.id)}
              className="editorial-press rounded-full inline-flex items-center text-xs"
              style={{ padding: 'var(--sp-1) var(--sp-3)', gap: 8, opacity: off ? 0.5 : 1 }}
            >
              <span
                aria-hidden
                style={{ width: 14, height: 3, borderRadius: 2, flexShrink: 0, background: off ? 'transparent' : l.color, outline: off ? `1px solid ${l.color}` : 'none' }}
              />
              <span style={{ color: 'var(--text-secondary)', textDecoration: off ? 'line-through' : 'none' }}>{l.name}</span>
              <span style={{ color: 'var(--text-primary)', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>{format(l.period)}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}
