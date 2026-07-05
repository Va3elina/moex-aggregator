/**
 * OiScreenerTable — вкладка «Скринер сигналов» на /oi (десктоп, v1).
 *
 * Лента сигналов по открытому интересу «как Telegram-алерты»: чистая позиция
 * группы (контракты + % перекоса) и ATR14-кратность дневного движения
 * («в N× резче обычного»). Данные дневные (T+1).
 *
 * Контролы — ТЕ ЖЕ компоненты, что на вкладке графика (SegmentedControl:
 * editorial-пилюли, 2px рамки, accent-актив, hover-поднажим) — паритет
 * размеров/жирности/анимаций с «Открытыми позициями» по построению.
 * Тумблер Физлица/Юрлица: кратность ×N у групп зеркально идентична, но
 * знаки, проценты и ликвидность (npart) — свои, бэк считает честно.
 * Тон текстов — «резкое движение/аномалия», НЕ «где заработать».
 */
import { useEffect, useMemo, useState } from 'react';
import type { CSSProperties, MouseEvent } from 'react';
import { ArrowDown, ArrowUp, Star } from 'lucide-react';
import InstrumentIcon from '../InstrumentIcon';
import SegmentedControl from '../SegmentedControl';
import { getOiScreener, type OiScreenerRow } from '../../services/api';
import { formatNumber } from '../../utils/formatNumber';
import { usePersistedState } from '../../hooks/usePersistedState';

type Clgroup = 'FIZ' | 'YUR';
type ThresholdKey = '0' | '2' | '3' | '5';

const THRESHOLD_OPTIONS: Array<{ key: ThresholdKey; label: string; title: string }> = [
  { key: '0', label: 'Все', title: 'Все активы, включая без сигнала' },
  { key: '2', label: '≥2×', title: 'Движения минимум в 2 раза резче обычного' },
  { key: '3', label: '≥3×', title: 'Движения минимум в 3 раза резче обычного' },
  { key: '5', label: '≥5×', title: 'Движения минимум в 5 раз резче обычного' },
];

// Реальные значения instruments.group (как в пикере активов).
const GROUP_OPTIONS = [
  { key: 'all', label: 'Все' },
  { key: 'Индексы', label: 'Индексы' },
  { key: 'Валюта', label: 'Валюта' },
  { key: 'Сырьё', label: 'Сырьё' },
  { key: 'Акции', label: 'Акции' },
  { key: 'Крипто', label: 'Крипто' },
] as const;

const FAVORITES_KEY = 'favoriteInstruments'; // общий ключ с пикером активов

// U+2212 — типографский минус; знак всегда явный.
const MINUS = '−';
function fmtSigned(n: number): string {
  return (n >= 0 ? '+' : MINUS) + formatNumber(Math.abs(n));
}
function fmtSignedPct(p: number): string {
  return (p >= 0 ? '+' : MINUS) + Math.abs(p).toLocaleString('ru-RU', { maximumFractionDigits: 1 }) + '%';
}
function fmtRatio(r: number): string {
  return r.toLocaleString('ru-RU', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '×';
}
/** «1 актив / 2 актива / 5 активов» */
function pluralAssets(n: number): string {
  const mod10 = n % 10, mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return `${n} актив`;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return `${n} актива`;
  return `${n} активов`;
}

const STATUS_RANK: Record<OiScreenerRow['status'], number> = {
  sharp: 0, normal: 1, illiquid: 2, nodata: 3,
};

const MONO: CSSProperties = {
  fontFamily: 'var(--font-mono, ui-monospace, monospace)',
  fontVariantNumeric: 'tabular-nums',
};

interface Props {
  /** Клик по строке — открыть график этого актива (вкладка «Открытые позиции»). */
  onSelect: (sectype: string) => void;
}

export default function OiScreenerTable({ onSelect }: Props) {
  const [rows, setRows] = useState<OiScreenerRow[] | null>(null);
  const [signalDate, setSignalDate] = useState<string | null>(null);
  const [error, setError] = useState(false);
  const [clgroup, setClgroup] = usePersistedState<Clgroup>('frame:oi-screener:clgroup', 'FIZ');
  const [threshold, setThreshold] = useState<ThresholdKey>('2');
  const [group, setGroup] = useState<string>('all');
  const [sortKey, setSortKey] = useState<'ratio' | 'delta'>('ratio');
  const [sortDir, setSortDir] = useState<1 | -1>(-1);
  const [favorites, setFavorites] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem(FAVORITES_KEY);
      return saved ? JSON.parse(saved) : [];
    } catch { return []; }
  });

  useEffect(() => {
    let cancelled = false;
    setRows(null);
    setError(false);
    getOiScreener(clgroup)
      .then((r) => { if (!cancelled) { setRows(r.rows); setSignalDate(r.signal_date); } })
      .catch(() => { if (!cancelled) setError(true); });
    return () => { cancelled = true; };
  }, [clgroup]);

  const toggleFavorite = (sectype: string, e: MouseEvent) => {
    e.stopPropagation();
    setFavorites((prev) => {
      const next = prev.includes(sectype) ? prev.filter((s) => s !== sectype) : [...prev, sectype];
      localStorage.setItem(FAVORITES_KEY, JSON.stringify(next));
      return next;
    });
  };

  const visible = useMemo(() => {
    if (!rows) return [];
    const thr = Number(threshold);
    const favSet = new Set(favorites);
    let out = rows.filter((r) =>
      (group === 'all' || r.group === group) &&
      (thr === 0 || (r.status === 'sharp' && (r.ratio ?? 0) >= thr)),
    );
    // sortDir = −1 → по убыванию (дефолт: сильные сверху), 1 → по возрастанию.
    if (sortKey === 'delta') {
      // Плоская сортировка по дневному сдвигу (null — в конец).
      out = [...out].sort((a, b) => {
        const av = a.delta_net, bv = b.delta_net;
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        return sortDir * (av - bv);
      });
    } else {
      // Дефолт: полосы по статусу (sharp → normal → illiquid → nodata),
      // избранные наверх внутри полосы, внутри — по кратности.
      out = [...out].sort((a, b) => {
        const sr = STATUS_RANK[a.status] - STATUS_RANK[b.status];
        if (sr !== 0) return sr;
        const fav = Number(favSet.has(b.sectype)) - Number(favSet.has(a.sectype));
        if (fav !== 0) return fav;
        return sortDir * ((a.ratio ?? -1) - (b.ratio ?? -1));
      });
    }
    return out;
  }, [rows, favorites, group, threshold, sortKey, sortDir]);

  const clickSort = (key: 'ratio' | 'delta') => {
    if (sortKey === key) setSortDir((d) => (d === -1 ? 1 : -1));
    else { setSortKey(key); setSortDir(-1); }
  };

  const dateLabel = signalDate
    ? new Date(signalDate + 'T00:00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' })
    : null;

  const groupWord = clgroup === 'FIZ' ? 'физлица' : 'юрлица';
  const mirrorWord = clgroup === 'FIZ' ? 'юрлица' : 'физлица';

  // ── ×N бейдж: сила по градации 2×/3×/5× ──
  const ratioBadge = (r: OiScreenerRow) => {
    if (r.status !== 'sharp' || r.ratio == null) return null;
    const strong = r.ratio >= 5;
    const mid = r.ratio >= 3;
    return (
      <span style={{
        ...MONO,
        padding: '3px 10px',
        borderRadius: 999,
        fontSize: 'var(--fs-base)',
        fontWeight: 800,
        flexShrink: 0,
        border: `2px solid ${strong || mid ? 'var(--accent)' : 'var(--text-primary)'}`,
        background: strong ? 'var(--accent)' : 'transparent',
        color: strong ? 'var(--text-inverse)' : mid ? 'var(--accent)' : 'var(--text-primary)',
      }}>
        {fmtRatio(r.ratio)}
      </span>
    );
  };

  const signalCell = (r: OiScreenerRow) => {
    const netColor = r.net >= 0 ? 'var(--oi-green)' : 'var(--oi-red)';
    const numbers = (
      <span style={{ ...MONO, color: netColor, fontWeight: 700, fontSize: 'var(--fs-base)', whiteSpace: 'nowrap' }}>
        {r.net_pct != null ? fmtSignedPct(r.net_pct) : ''} ({fmtSigned(r.net)})
      </span>
    );
    if (r.status === 'sharp' && r.ratio != null && r.direction) {
      // Глагол — по МОДУЛЮ чистой позиции: |net| вырос → «нарастили»,
      // уменьшился → «сократили». Нельзя брать знак Δnet напрямую: у шортовой
      // стороны рост net (−5,3М → −4,7М) — это СОКРАЩЕНИЕ шорта, а не рост.
      // Нога — по знаку net (длинная/короткая). Так физики и юрики на CNYRUBF
      // корректно оба «сократили» (свой лонг / свой шорт), а не «нарастили».
      const netLong = r.net >= 0;
      const grewExposure = netLong === (r.direction === 'up'); // |net| вырос
      const legWord = netLong ? 'длинную позицию' : 'короткую позицию';
      const verb = grewExposure ? 'нарастили' : 'сократили';
      const Arrow = grewExposure ? ArrowUp : ArrowDown;
      const legColor = netLong ? 'var(--oi-green)' : 'var(--oi-red)';
      const cap = (s: string) => s[0].toUpperCase() + s.slice(1);
      const full = `${cap(groupWord)} резко ${verb} ${legWord} по «${r.name}»: дневное изменение чистой позиции в ${fmtRatio(r.ratio)} резче обычного (ATR-14; «резко» — от 2×). Обратная сторона (${mirrorWord}) держит зеркальную позицию.`;
      return (
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }} title={full}>
          {numbers}
          <Arrow size={18} strokeWidth={2.6} style={{ color: legColor, flexShrink: 0 }} />
          {ratioBadge(r)}
          <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-primary)', minWidth: 0 }}>
            резко {verb} {legWord} — в {fmtRatio(r.ratio)} резче обычного
          </span>
        </div>
      );
    }
    const note =
      r.status === 'normal'
        ? `в пределах обычного${r.ratio != null ? ` (${fmtRatio(r.ratio)})` : ''}`
        : r.status === 'illiquid'
          ? 'низкая ликвидность'
          : 'недостаточно данных';
    return (
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
        {numbers}
        <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 500, color: 'var(--text-secondary)' }}>{note}</span>
      </div>
    );
  };

  const sortArrow = (key: 'ratio' | 'delta') =>
    sortKey === key
      ? (sortDir === -1 ? ' ▼' : ' ▲')
      : ' ⇅';

  // Заголовки таблицы — как в прототипе: mono uppercase, ЖИРНЫЕ, цвет primary
  // (не блеклые), сортируемые подсвечены на hover через cursor.
  const headCell: CSSProperties = {
    ...MONO,
    fontSize: 'var(--fs-xs)', fontWeight: 800, letterSpacing: '0.06em',
    textTransform: 'uppercase', color: 'var(--text-primary)',
    textAlign: 'left', whiteSpace: 'nowrap',
  };

  const gridCols = 'minmax(200px, 270px) minmax(320px, 1fr) 150px 130px 48px';

  return (
    <div>
      {/* Тулбар — те же SegmentedControl, что контролы графика ОИ */}
      <div className="flex flex-wrap items-center mb-4 md:mb-6 gap-2 md:gap-3">
        <SegmentedControl<Clgroup>
          options={[
            { key: 'FIZ', label: 'Физлица' },
            { key: 'YUR', label: 'Юрлица', title: 'Сигналы юрлиц зеркальны физлицам по кратности, но проценты и ликвидность — свои' },
          ]}
          value={clgroup}
          onChange={setClgroup}
        />
        <SegmentedControl<ThresholdKey>
          options={THRESHOLD_OPTIONS}
          value={threshold}
          onChange={setThreshold}
        />
        <SegmentedControl<string>
          options={GROUP_OPTIONS.map((g) => ({ key: g.key, label: g.label }))}
          value={group}
          onChange={setGroup}
        />
      </div>

      {/* Таблица */}
      <div style={{ overflowX: 'auto' }}>
        <div style={{ minWidth: 960 }}>
          {/* Заголовки */}
          <div style={{ display: 'grid', gridTemplateColumns: gridCols, gap: 16, padding: '12px 18px', borderBottom: '2px solid var(--text-primary)' }}>
            <span style={headCell}>Актив</span>
            <button type="button" style={{ ...headCell, cursor: 'pointer', background: 'none', border: 'none', padding: 0 }} onClick={() => clickSort('ratio')}>
              Чистая поз. ({groupWord}) · Сигнал{sortArrow('ratio')}
            </button>
            <span style={{ ...headCell, textAlign: 'right' }}>ОИ, контракты</span>
            <button type="button" style={{ ...headCell, cursor: 'pointer', background: 'none', border: 'none', padding: 0, textAlign: 'right' }} onClick={() => clickSort('delta')} title="Изменение чистой позиции за торговый день">
              Δ за день{sortArrow('delta')}
            </button>
            <span />
          </div>

          {/* Состояния */}
          {error && (
            <div style={{ padding: 48, textAlign: 'center', color: 'var(--text-secondary)', fontSize: 'var(--fs-base)' }}>
              Не удалось загрузить данные — попробуйте обновить страницу
            </div>
          )}
          {!error && rows === null && (
            <div style={{ padding: 48, textAlign: 'center', color: 'var(--text-secondary)', fontSize: 'var(--fs-base)' }}>Загрузка…</div>
          )}
          {!error && rows !== null && visible.length === 0 && (
            <div style={{ padding: 48, textAlign: 'center' }}>
              <div style={{ marginBottom: 16, color: 'var(--text-primary)', fontSize: 'var(--fs-base)', fontWeight: 600 }}>
                Сегодня резких движений нет
              </div>
              <button
                type="button"
                className="editorial-press rounded-full font-semibold"
                style={{
                  fontSize: 'var(--fs-sm)',
                  padding: 'var(--sp-2) var(--sp-4)',
                  background: 'var(--bg-secondary)',
                  border: '2px solid var(--text-primary)',
                  color: 'var(--text-primary)',
                  cursor: 'pointer',
                }}
                onClick={() => setThreshold('0')}
              >
                Показать все активы
              </button>
            </div>
          )}

          {/* Строки */}
          {!error && visible.map((r) => {
            const isFav = favorites.includes(r.sectype);
            return (
              <div
                key={r.sectype}
                role="button"
                tabIndex={0}
                onClick={() => onSelect(r.sectype)}
                onKeyDown={(e) => { if (e.key === 'Enter') onSelect(r.sectype); }}
                className="oi-screener-row"
                style={{
                  display: 'grid', gridTemplateColumns: gridCols, gap: 16,
                  alignItems: 'center', padding: '12px 18px',
                  borderBottom: '0.5px solid var(--border-color, rgba(128,128,128,0.25))',
                  cursor: 'pointer',
                  opacity: r.status === 'illiquid' || r.status === 'nodata' ? 0.55 : 1,
                }}
              >
                {/* Актив */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 12, minWidth: 0 }}>
                  <InstrumentIcon sectype={r.sectype} size={32} />
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontWeight: 700, fontSize: 'var(--fs-base)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {r.name}
                    </div>
                    <div style={{ ...MONO, fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', letterSpacing: '0.04em' }}>
                      {r.front_secid || r.sectype}
                    </div>
                  </div>
                </div>

                {/* Сигнал */}
                {signalCell(r)}

                {/* ОИ */}
                <div style={{ textAlign: 'right' }}>
                  <div style={{ ...MONO, fontSize: 'var(--fs-base)', fontWeight: 600 }}>{formatNumber(r.oi)}</div>
                  {r.oi_delta_pct != null && (
                    <div style={{ ...MONO, fontSize: 'var(--fs-xs)', color: r.oi_delta_pct >= 0 ? 'var(--oi-green)' : 'var(--oi-red)' }}>
                      {fmtSignedPct(r.oi_delta_pct)}
                    </div>
                  )}
                </div>

                {/* Δ за день */}
                <div style={{ textAlign: 'right' }}>
                  {r.delta_net != null ? (
                    <span style={{
                      ...MONO,
                      display: 'inline-block', padding: '3px 12px', borderRadius: 999,
                      fontSize: 'var(--fs-sm)', fontWeight: 700,
                      border: `2px solid ${r.delta_net >= 0 ? 'var(--oi-green)' : 'var(--oi-red)'}`,
                      color: r.delta_net >= 0 ? 'var(--oi-green)' : 'var(--oi-red)',
                    }}>
                      {fmtSigned(r.delta_net)}
                    </span>
                  ) : (
                    <span style={{ color: 'var(--text-secondary)' }}>{MINUS}</span>
                  )}
                </div>

                {/* ⭐ */}
                <button
                  type="button"
                  onClick={(e) => toggleFavorite(r.sectype, e)}
                  aria-label={isFav ? 'Убрать из избранного' : 'В избранное'}
                  style={{
                    background: 'none', border: 'none', cursor: 'pointer', padding: 6,
                    color: isFav ? 'var(--accent)' : 'var(--text-secondary)', lineHeight: 0,
                  }}
                >
                  <Star size={18} fill={isFav ? 'var(--accent)' : 'none'} strokeWidth={2} />
                </button>
              </div>
            );
          })}
        </div>
      </div>

      {/* Футер */}
      {!error && rows !== null && (
        <div className="flex items-center justify-between flex-wrap" style={{ ...MONO, gap: 8, padding: '14px 4px 0', fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
          <span>{pluralAssets(visible.length)} · {groupWord}</span>
          <span>
            Данные: Московская биржа{dateLabel && <> · <em>по данным за {dateLabel}</em></>}
          </span>
        </div>
      )}
    </div>
  );
}
