/**
 * Сегмент по индикаторам: кто смотрит одни разделы и не смотрит другие.
 *
 * Каждый индикатор — чип с тремя состояниями по клику:
 *   нейтральный → «смотрит» → «не смотрит» → нейтральный.
 * Три состояния в одном ряду вместо двух отдельных списков: выбор «смотрит X,
 * не смотрит Y» читается одной строкой, а не сверкой двух наборов.
 */
import { Check, X } from 'lucide-react';
import Card from '../Card';
import HelpTooltip from '../HelpTooltip';
import Dropdown from '../Dropdown';

export interface SegmentState {
  seen: string[];
  notSeen: string[];
  /** all — смотрит каждый из выбранных, any — хотя бы один. */
  mode: 'all' | 'any';
}

export const EMPTY_SEGMENT: SegmentState = { seen: [], notSeen: [], mode: 'all' };

export function isSegmentActive(s: SegmentState): boolean {
  return s.seen.length > 0 || s.notSeen.length > 0;
}

export const SEGMENT_HINT =
  'Отбирает людей по тому, какими разделами они пользуются. Клик по индикатору переключает его: ' +
  'галочка — смотрит, крестик — не смотрит, пусто — неважно. Учитываются просмотры за выбранный ' +
  'период, поэтому «не смотрит» значит «не смотрел за период», а не «никогда». Фильтр действует ' +
  'сразу на гостей и на зарегистрированных.';

type Tri = 'off' | 'seen' | 'not';

function triOf(path: string, s: SegmentState): Tri {
  if (s.seen.includes(path)) return 'seen';
  if (s.notSeen.includes(path)) return 'not';
  return 'off';
}

function nextState(path: string, s: SegmentState): SegmentState {
  const cur = triOf(path, s);
  const seen = s.seen.filter(p => p !== path);
  const notSeen = s.notSeen.filter(p => p !== path);
  if (cur === 'off') return { ...s, seen: [...seen, path], notSeen };
  if (cur === 'seen') return { ...s, seen, notSeen: [...notSeen, path] };
  return { ...s, seen, notSeen };
}

export function IndicatorSegment({ indicators, value, onChange, summary, loading }: {
  indicators: { path: string; name: string }[];
  value: SegmentState;
  onChange: (s: SegmentState) => void;
  /** Сводка по сегменту; null — ещё не посчитана. */
  summary: {
    people: number; registered: number; guests: number; paying: number;
    indicators: { path: string; name: string; people: number }[];
  } | null;
  loading: boolean;
}) {
  const active = isSegmentActive(value);
  const cover = new Map((summary?.indicators ?? []).map(i => [i.path, i.people]));

  return (
    <Card padding="md" className="md:p-5">
      <div className="flex flex-wrap items-center gap-2 mb-3">
        <p className="text-xs uppercase" style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}>
          Кто чем пользуется
        </p>
        <HelpTooltip icon="help" title="Сегмент по индикаторам" content={SEGMENT_HINT} size={13} />
        {value.seen.length > 1 && (
          <Dropdown<string>
            options={[
              { key: 'all', label: 'Смотрит все отмеченные' },
              { key: 'any', label: 'Смотрит хотя бы один' },
            ]}
            value={value.mode}
            onChange={m => onChange({ ...value, mode: m === 'any' ? 'any' : 'all' })}
            menuMaxWidth={280}
          />
        )}
        {active && (
          <button
            type="button"
            onClick={() => onChange(EMPTY_SEGMENT)}
            className="text-xs ml-auto transition-opacity hover:opacity-70"
            style={{ color: 'var(--accent)', fontWeight: 600 }}
          >
            Сбросить
          </button>
        )}
      </div>

      <div className="flex flex-wrap gap-2">
        {indicators.map(ind => {
          const tri = triOf(ind.path, value);
          const people = cover.get(ind.path);
          return (
            <button
              key={ind.path}
              type="button"
              onClick={() => onChange(nextState(ind.path, value))}
              className="inline-flex items-center text-sm transition-colors"
              style={{
                gap: 6,
                padding: 'var(--sp-2) var(--sp-3)',
                borderRadius: 9999,
                border: `1.5px solid ${tri === 'off' ? 'var(--border-color)' : 'var(--text-primary)'}`,
                backgroundColor:
                  tri === 'seen' ? 'color-mix(in srgb, var(--accent) 18%, transparent)'
                  : tri === 'not' ? 'color-mix(in srgb, var(--text-muted) 18%, transparent)'
                  : 'transparent',
                color: tri === 'off' ? 'var(--text-secondary)' : 'var(--text-primary)',
                textDecoration: tri === 'not' ? 'line-through' : 'none',
                fontWeight: tri === 'off' ? 400 : 600,
              }}
            >
              {tri === 'seen' && <Check size={12} style={{ color: 'var(--accent)' }} />}
              {tri === 'not' && <X size={12} style={{ color: 'var(--text-muted)' }} />}
              {ind.name}
              {people !== undefined && (
                <span className="text-xs" style={{ color: 'var(--text-muted)', fontWeight: 400 }}>
                  {people}
                </span>
              )}
            </button>
          );
        })}
      </div>

      <div className="flex flex-wrap items-baseline gap-x-5 gap-y-1 mt-4 text-sm" style={{ color: 'var(--text-secondary)' }}>
        {loading && !summary ? (
          <span style={{ color: 'var(--text-muted)' }}>Считаем…</span>
        ) : summary ? (
          <>
            <span>
              <b style={{ color: 'var(--text-primary)', fontFamily: "'IBM Plex Mono', monospace" }}>
                {summary.people.toLocaleString('ru-RU')}
              </b>{' '}
              {active ? 'человек в сегменте' : 'человек за период'}
            </span>
            <span style={{ color: 'var(--text-muted)' }}>
              гостей {summary.guests} · зарегистрированных {summary.registered}
              {summary.paying > 0 && ` · платящих ${summary.paying}`}
            </span>
          </>
        ) : null}
      </div>

      {/* Цифры у чипов — охват внутри сегмента, поэтому у отмеченных «смотрит»
          он равен размеру сегмента: это не ошибка, а проверка отбора. */}
      <p className="text-xs mt-2" style={{ color: 'var(--text-muted)' }}>
        Цифра у индикатора — сколько людей {active ? 'из сегмента' : 'за период'} его открывали.
      </p>
    </Card>
  );
}
