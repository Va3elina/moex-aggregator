/**
 * «Подписчики Telegram-канала» в /admin/stats: вступления по дням столбиками,
 * раскрашенными по инвайт-ссылке, и отписки вниз от нуля. Легенда включает и
 * выключает ссылки и показывает итог за период: вступило и сколько из них
 * ещё в канале.
 *
 * Цвет закреплён за ссылкой по её месту в итоге за период: самая
 * результативная всегда первого цвета. Палитра --viz-1…8 та же, что у графика
 * источников Метрики.
 */
import { useMemo, useRef, useState } from 'react';
import { useElementWidth } from '../../hooks/useElementWidth';
import type { TgJoinsReport } from '../../services/api';

const FALLBACK = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948'];
const PLOT_H = 240;
const AXIS_H = 26;
const PAD_TOP = 10;
const PAD_LEFT = 44;
const PAD_RIGHT = 12;

function linkColor(i: number): string {
  const slot = (i % 8) + 1;
  return `var(--viz-${slot}, ${FALLBACK[slot - 1]})`;
}

function parseDay(s: string): Date {
  const [y, m, d] = s.split('-').map(Number);
  return new Date(y, m - 1, d);
}

const fmtDay = (s: string) => parseDay(s).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
const fmtDayLong = (s: string) => parseDay(s).toLocaleDateString('ru-RU', { weekday: 'short', day: 'numeric', month: 'long' });

function niceStep(raw: number): number {
  const exp = Math.pow(10, Math.floor(Math.log10(raw)));
  const f = raw / exp;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10) * exp;
}

/** Целые деления от нуля до круглого верха, 2–5 шагов. */
function axisTicks(max: number, steps = 4): number[] {
  if (!(max > 0)) return [0];
  const step = Math.max(1, niceStep(max / steps));
  const count = Math.ceil(max / step);
  return Array.from({ length: count + 1 }, (_, i) => i * step);
}

export default function TgJoinsChart({ data }: { data: TgJoinsReport }) {
  const rootRef = useRef<HTMLDivElement>(null);
  const width = useElementWidth(rootRef);
  const [hidden, setHidden] = useState<ReadonlySet<string>>(() => new Set());
  const [hover, setHover] = useState<number | null>(null);

  const n = data.dates.length;
  const series = useMemo(
    () => data.series.map((s, i) => ({ ...s, color: linkColor(i) })),
    [data.series],
  );
  const shown = series.filter((s) => !hidden.has(s.name));
  // Столбик дня — сумма показанных ссылок; отписки от ссылки не зависят.
  const dayJoins = data.dates.map((_, i) => shown.reduce((acc, s) => acc + (s.values[i] ?? 0), 0));
  const topTicks = axisTicks(Math.max(0, ...dayJoins));
  const botTicks = axisTicks(Math.max(0, ...data.leaves), 2);
  const top = topTicks[topTicks.length - 1];
  const bot = botTicks[botTicks.length - 1];
  // Ноль делит плот пропорционально размаху вверх и вниз; без отписок вся высота вверх.
  const unit = PLOT_H / Math.max(1, top + bot);
  const y0 = PAD_TOP + top * unit;

  const plotW = Math.max(0, width - PAD_LEFT - PAD_RIGHT);
  const slot = n > 0 ? plotW / n : plotW;
  const barW = Math.max(2, Math.min(28, slot * 0.68));
  const xc = (i: number) => PAD_LEFT + slot * (i + 0.5);
  const h = hover !== null && hover < n ? hover : null;

  const weekends = data.dates.flatMap((d, i) => {
    const wd = parseDay(d).getDay();
    return wd === 0 || wd === 6 ? [i] : [];
  });
  const every = Math.max(1, Math.ceil(n / Math.max(2, Math.floor(plotW / 56))));

  const pick = (clientX: number, el: Element) => {
    const i = Math.floor((clientX - el.getBoundingClientRect().left - PAD_LEFT) / slot);
    setHover(Math.min(n - 1, Math.max(0, i)));
  };
  const onKey = (ev: React.KeyboardEvent) => {
    if (ev.key !== 'ArrowLeft' && ev.key !== 'ArrowRight') return;
    ev.preventDefault();
    setHover(Math.min(n - 1, Math.max(0, (h ?? n - 1) + (ev.key === 'ArrowRight' ? 1 : -1))));
  };
  const toggle = (name: string) => setHidden((prev) => {
    const next = new Set(prev);
    if (next.has(name)) next.delete(name); else next.add(name);
    return next;
  });

  const hoverRows = h === null ? [] : shown.filter((s) => (s.values[h] ?? 0) > 0);

  return (
    <div ref={rootRef}>
      <div
        className="relative"
        tabIndex={0}
        onKeyDown={onKey}
        onFocus={() => setHover((v) => v ?? n - 1)}
        onBlur={() => setHover(null)}
        aria-label="Вступления и отписки по дням. Стрелки влево и вправо показывают значения по датам."
      >
        {width > 0 && (
          <svg
            width={width}
            height={PAD_TOP + PLOT_H + AXIS_H}
            role="img"
            aria-label={`За период вступило ${data.totals.joins}, отписалось ${data.totals.leaves}`}
            style={{ display: 'block', fontVariantNumeric: 'tabular-nums' }}
          >
            {weekends.map((i) => (
              <rect key={i} x={PAD_LEFT + slot * i} y={PAD_TOP} width={slot} height={PLOT_H} fill="var(--chart-area)" />
            ))}
            {topTicks.map((t) => (
              <g key={`t${t}`}>
                <line x1={PAD_LEFT} x2={PAD_LEFT + plotW} y1={y0 - t * unit} y2={y0 - t * unit} stroke="var(--chart-grid)" strokeWidth={1} shapeRendering="crispEdges" />
                <text x={PAD_LEFT - 8} y={y0 - t * unit} dy="0.32em" textAnchor="end" fontSize={11} fill="var(--text-muted)">{t}</text>
              </g>
            ))}
            {botTicks.filter((t) => t > 0).map((t) => (
              <g key={`b${t}`}>
                <line x1={PAD_LEFT} x2={PAD_LEFT + plotW} y1={y0 + t * unit} y2={y0 + t * unit} stroke="var(--chart-grid)" strokeWidth={1} shapeRendering="crispEdges" />
                <text x={PAD_LEFT - 8} y={y0 + t * unit} dy="0.32em" textAnchor="end" fontSize={11} fill="var(--text-muted)">−{t}</text>
              </g>
            ))}
            <line x1={PAD_LEFT} x2={PAD_LEFT + plotW} y1={y0} y2={y0} stroke="var(--text-muted)" strokeOpacity={0.5} strokeWidth={1} shapeRendering="crispEdges" />
            {data.dates.map((d, i) => (i % every === 0 ? (
              <text key={d} x={xc(i)} y={PAD_TOP + PLOT_H + 17} textAnchor="middle" fontSize={11} fill="var(--text-muted)">
                {fmtDay(d)}
              </text>
            ) : null))}
            {data.dates.map((d, i) => {
              let acc = 0;
              const dim = h !== null && h !== i;
              return (
                <g key={d} opacity={dim ? 0.55 : 1} style={{ transition: 'opacity 0.15s ease' }}>
                  {shown.map((s) => {
                    const v = s.values[i] ?? 0;
                    if (v <= 0) return null;
                    const yTop = y0 - (acc + v) * unit;
                    acc += v;
                    return <rect key={s.name} x={xc(i) - barW / 2} y={yTop} width={barW} height={v * unit} fill={s.color} rx={1} />;
                  })}
                  {(data.leaves[i] ?? 0) > 0 && (
                    <rect x={xc(i) - barW / 2} y={y0} width={barW} height={(data.leaves[i] ?? 0) * unit} fill="var(--danger)" opacity={0.75} rx={1} />
                  )}
                </g>
              );
            })}
            <rect
              x={PAD_LEFT} y={PAD_TOP} width={plotW} height={PLOT_H} fill="transparent"
              onPointerMove={(ev) => pick(ev.clientX, ev.currentTarget.ownerSVGElement ?? ev.currentTarget)}
              onPointerLeave={() => setHover(null)}
            />
          </svg>
        )}
        {h !== null && (
          <div
            className="absolute pointer-events-none text-xs"
            style={{
              top: PAD_TOP,
              left: xc(h),
              transform: xc(h) > width * 0.6 ? 'translateX(calc(-100% - 12px))' : 'translateX(12px)',
              background: 'var(--bg-elevated)',
              border: '1px solid var(--border-color)',
              borderRadius: 8,
              padding: '8px 10px',
              whiteSpace: 'nowrap',
              zIndex: 5,
            }}
          >
            <div className="mb-1" style={{ color: 'var(--text-muted)' }}>{fmtDayLong(data.dates[h])}</div>
            <div className="flex items-center gap-2" style={{ lineHeight: 1.6 }}>
              <span style={{ color: 'var(--text-primary)', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>+{dayJoins[h]}</span>
              <span style={{ color: 'var(--text-secondary)' }}>вступило</span>
            </div>
            {hoverRows.map((s) => (
              <div key={s.name} className="flex items-center gap-2" style={{ lineHeight: 1.6, paddingLeft: 8 }}>
                <span aria-hidden style={{ width: 8, height: 8, borderRadius: 2, background: s.color, flexShrink: 0 }} />
                <span style={{ color: 'var(--text-primary)', fontVariantNumeric: 'tabular-nums' }}>{s.values[h]}</span>
                <span style={{ color: 'var(--text-secondary)' }}>{s.name}</span>
              </div>
            ))}
            <div className="flex items-center gap-2" style={{ lineHeight: 1.6 }}>
              <span style={{ color: 'var(--danger)', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>−{data.leaves[h] ?? 0}</span>
              <span style={{ color: 'var(--text-secondary)' }}>отписалось</span>
            </div>
            {data.members[h] !== null && (
              <div className="flex items-center gap-2" style={{ lineHeight: 1.6 }}>
                <span style={{ color: 'var(--text-primary)', fontVariantNumeric: 'tabular-nums' }}>{data.members[h]!.toLocaleString('ru-RU')}</span>
                <span style={{ color: 'var(--text-secondary)' }}>подписчиков</span>
              </div>
            )}
          </div>
        )}
      </div>

      <div className="flex flex-wrap gap-2 mt-3">
        {series.map((s) => {
          const off = hidden.has(s.name);
          return (
            <button
              key={s.name}
              type="button"
              aria-pressed={!off}
              title={`${s.name}: вступило ${s.period}, ещё в канале ${s.stayed}. Нажмите, чтобы ${off ? 'показать' : 'скрыть'}`}
              onClick={() => toggle(s.name)}
              className="editorial-press rounded-full inline-flex items-center text-xs"
              style={{ padding: 'var(--sp-1) var(--sp-3)', gap: 8, opacity: off ? 0.5 : 1, transition: 'opacity 0.2s ease' }}
            >
              <span
                aria-hidden
                style={{ width: 10, height: 10, borderRadius: 2, flexShrink: 0, background: off ? 'transparent' : s.color, outline: off ? `1px solid ${s.color}` : 'none' }}
              />
              <span style={{ color: 'var(--text-secondary)', textDecoration: off ? 'line-through' : 'none' }}>{s.name}</span>
              <span style={{ color: 'var(--text-primary)', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>{s.period}</span>
              {s.period > 0 && (
                <span style={{ color: 'var(--text-muted)', fontVariantNumeric: 'tabular-nums' }}>
                  остались {Math.round((s.stayed / s.period) * 100)}%
                </span>
              )}
            </button>
          );
        })}
        <span
          className="inline-flex items-center text-xs"
          style={{ padding: 'var(--sp-1) var(--sp-3)', gap: 8, color: 'var(--text-secondary)' }}
        >
          <span aria-hidden style={{ width: 10, height: 10, borderRadius: 2, background: 'var(--danger)', opacity: 0.75 }} />
          отписки
          <span style={{ color: 'var(--text-primary)', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>{data.totals.leaves}</span>
        </span>
      </div>
    </div>
  );
}
