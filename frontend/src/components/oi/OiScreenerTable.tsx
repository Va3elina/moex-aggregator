/**
 * OiScreenerTable — вкладка «Скринер сигналов» на /oi (десктоп, v1).
 *
 * Лента сигналов по открытому интересу «как Telegram-алерты»: чистая позиция
 * физлиц (контракты + % перекоса) и ATR14-кратность дневного движения
 * («в N× резче обычного»). Данные дневные (T+1), только физлица — сигналы
 * юрлиц зеркальны тождественно (net(FIZ) ≡ −net(YUR)) и задвоили бы список.
 *
 * Тулбар (по срезу Вадима — минимум): чипы порога Все/≥2×/≥3×/≥5× (дефолт ≥2×)
 * + чипы типа актива. Без поиска, без показателей, без физ/юр, без пресетов.
 * Тон текстов — «резкое движение/аномалия», НЕ «где заработать».
 */
import { useEffect, useMemo, useState } from 'react';
import type { CSSProperties, MouseEvent } from 'react';
import { ArrowDown, ArrowUp, Star } from 'lucide-react';
import InstrumentIcon from '../InstrumentIcon';
import { getOiScreener, type OiScreenerRow } from '../../services/api';
import { formatNumber } from '../../utils/formatNumber';

const THRESHOLDS: Array<{ key: number; label: string }> = [
  { key: 0, label: 'Все' },
  { key: 2, label: '≥2×' },
  { key: 3, label: '≥3×' },
  { key: 5, label: '≥5×' },
];

// Реальные значения instruments.group (как в пикере активов).
const GROUPS: Array<{ key: string; label: string }> = [
  { key: 'all', label: 'Все' },
  { key: 'Индексы', label: 'Индексы' },
  { key: 'Валюта', label: 'Валюта' },
  { key: 'Сырьё', label: 'Сырьё' },
  { key: 'Акции', label: 'Акции' },
  { key: 'Крипто', label: 'Крипто' },
];

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
  const [threshold, setThreshold] = useState(2);
  const [group, setGroup] = useState('all');
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
    getOiScreener()
      .then((r) => { if (!cancelled) { setRows(r.rows); setSignalDate(r.signal_date); } })
      .catch(() => { if (!cancelled) setError(true); });
    return () => { cancelled = true; };
  }, []);

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
    const favSet = new Set(favorites);
    let out = rows.filter((r) =>
      (group === 'all' || r.group === group) &&
      (threshold === 0 || (r.status === 'sharp' && (r.ratio ?? 0) >= threshold)),
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

  // ── чипы ──
  const chip = (active: boolean): CSSProperties => ({
    padding: '4px 12px',
    borderRadius: 999,
    border: `1.5px solid ${active ? 'var(--accent)' : 'var(--border-color, var(--text-secondary))'}`,
    background: active ? 'var(--accent)' : 'transparent',
    color: active ? 'var(--text-inverse)' : 'var(--text-secondary)',
    fontSize: 'var(--fs-xs)',
    fontWeight: 700,
    cursor: 'pointer',
    whiteSpace: 'nowrap',
    transition: 'all 150ms ease',
  });

  const label = (text: string) => (
    <span style={{
      ...MONO,
      fontSize: 10, fontWeight: 700, letterSpacing: '0.08em',
      textTransform: 'uppercase', color: 'var(--text-muted)',
    }}>{text}</span>
  );

  // ── ×N бейдж: сила по градации 2×/3×/5× ──
  const ratioBadge = (r: OiScreenerRow) => {
    if (r.status !== 'sharp' || r.ratio == null) return null;
    const strong = r.ratio >= 5;
    const mid = r.ratio >= 3;
    return (
      <span style={{
        ...MONO,
        padding: '2px 8px',
        borderRadius: 999,
        fontSize: 'var(--fs-sm)',
        fontWeight: 800,
        flexShrink: 0,
        border: `1.5px solid ${strong || mid ? 'var(--accent)' : 'var(--text-primary)'}`,
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
      <span style={{ ...MONO, color: netColor, fontWeight: 700, fontSize: 'var(--fs-sm)', whiteSpace: 'nowrap' }}>
        {r.net_pct != null ? fmtSignedPct(r.net_pct) : ''} ({fmtSigned(r.net)})
      </span>
    );
    if (r.status === 'sharp' && r.ratio != null && r.direction) {
      const grew = r.direction === 'up';
      const Arrow = grew ? ArrowUp : ArrowDown;
      const full = `Физлица резко ${grew ? 'нарастили' : 'сократили'} позицию — в ${fmtRatio(r.ratio)} резче обычного (порог 2×). Юрлица — зеркально ${grew ? 'сократили' : 'нарастили'}.`;
      return (
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }} title={full}>
          {numbers}
          <Arrow size={16} strokeWidth={2.6} style={{ color: grew ? 'var(--oi-green)' : 'var(--oi-red)', flexShrink: 0 }} />
          {ratioBadge(r)}
          <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-primary)', minWidth: 0 }}>
            резко {grew ? 'нарастили' : 'сократили'} позицию — в {fmtRatio(r.ratio)} резче обычного
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
        <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-muted)' }}>{note}</span>
      </div>
    );
  };

  const sortArrow = (key: 'ratio' | 'delta') =>
    sortKey === key
      ? (sortDir === -1 ? ' ▼' : ' ▲')
      : ' ⇅';

  const headCell: CSSProperties = {
    ...MONO,
    fontSize: 11, fontWeight: 700, letterSpacing: '0.06em',
    textTransform: 'uppercase', color: 'var(--text-secondary)',
    textAlign: 'left', whiteSpace: 'nowrap',
  };

  const gridCols = 'minmax(190px, 260px) minmax(300px, 1fr) 150px 120px 44px';

  return (
    <div>
      {/* Тулбар: порог + тип актива */}
      <div className="flex flex-wrap items-center mb-4" style={{ gap: 'var(--sp-3)' }}>
        <div className="flex items-center" style={{ gap: 8 }}>
          {label('Порог')}
          {THRESHOLDS.map((t) => (
            <button key={t.key} type="button" style={chip(threshold === t.key)} onClick={() => setThreshold(t.key)}>
              {t.label}
            </button>
          ))}
        </div>
        <div style={{ width: 2, height: 22, background: 'var(--border-color, var(--text-muted))', opacity: 0.5 }} />
        <div className="flex items-center flex-wrap" style={{ gap: 8 }}>
          {label('Тип актива')}
          {GROUPS.map((g) => (
            <button key={g.key} type="button" style={chip(group === g.key)} onClick={() => setGroup(g.key)}>
              {g.label}
            </button>
          ))}
        </div>
      </div>

      {/* Таблица */}
      <div style={{ overflowX: 'auto' }}>
        <div style={{ minWidth: 920 }}>
          {/* Заголовки */}
          <div style={{ display: 'grid', gridTemplateColumns: gridCols, gap: 16, padding: '10px 18px', borderBottom: '1.5px solid var(--border-color, var(--text-muted))' }}>
            <span style={headCell}>Актив</span>
            <button type="button" style={{ ...headCell, cursor: 'pointer', background: 'none', border: 'none', padding: 0 }} onClick={() => clickSort('ratio')}>
              Чистая поз. (физлица) · Сигнал{sortArrow('ratio')}
            </button>
            <span style={{ ...headCell, textAlign: 'right' }}>ОИ, контракты</span>
            <button type="button" style={{ ...headCell, cursor: 'pointer', background: 'none', border: 'none', padding: 0, textAlign: 'right' }} onClick={() => clickSort('delta')}>
              Δ 1д{sortArrow('delta')}
            </button>
            <span />
          </div>

          {/* Состояния */}
          {error && (
            <div style={{ padding: 48, textAlign: 'center', color: 'var(--text-muted)' }}>
              Не удалось загрузить данные — попробуйте обновить страницу
            </div>
          )}
          {!error && rows === null && (
            <div style={{ padding: 48, textAlign: 'center', color: 'var(--text-muted)' }}>Загрузка…</div>
          )}
          {!error && rows !== null && visible.length === 0 && (
            <div style={{ padding: 48, textAlign: 'center', color: 'var(--text-muted)' }}>
              <div style={{ marginBottom: 12 }}>Сегодня резких движений нет</div>
              <button
                type="button"
                style={{ ...chip(false), color: 'var(--text-primary)', borderColor: 'var(--text-primary)' }}
                onClick={() => setThreshold(0)}
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
                  alignItems: 'center', padding: '11px 18px',
                  borderBottom: '0.5px solid var(--border-color, rgba(128,128,128,0.25))',
                  cursor: 'pointer',
                  opacity: r.status === 'illiquid' || r.status === 'nodata' ? 0.55 : 1,
                }}
              >
                {/* Актив */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 12, minWidth: 0 }}>
                  <InstrumentIcon sectype={r.sectype} size={30} />
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontWeight: 700, fontSize: 'var(--fs-sm)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {r.name}
                    </div>
                    <div style={{ ...MONO, fontSize: 10.5, color: 'var(--text-muted)', letterSpacing: '0.04em' }}>
                      {r.front_secid || r.sectype}
                    </div>
                  </div>
                </div>

                {/* Сигнал */}
                {signalCell(r)}

                {/* ОИ */}
                <div style={{ textAlign: 'right' }}>
                  <div style={{ ...MONO, fontSize: 'var(--fs-sm)', fontWeight: 600 }}>{formatNumber(r.oi)}</div>
                  {r.oi_delta_pct != null && (
                    <div style={{ ...MONO, fontSize: 11, color: r.oi_delta_pct >= 0 ? 'var(--oi-green)' : 'var(--oi-red)' }}>
                      {fmtSignedPct(r.oi_delta_pct)}
                    </div>
                  )}
                </div>

                {/* Δ 1д */}
                <div style={{ textAlign: 'right' }}>
                  {r.delta_net != null ? (
                    <span style={{
                      ...MONO,
                      display: 'inline-block', padding: '2px 10px', borderRadius: 999,
                      fontSize: 'var(--fs-xs)', fontWeight: 700,
                      border: `1.5px solid ${r.delta_net >= 0 ? 'var(--oi-green)' : 'var(--oi-red)'}`,
                      color: r.delta_net >= 0 ? 'var(--oi-green)' : 'var(--oi-red)',
                    }}>
                      {fmtSigned(r.delta_net)}
                    </span>
                  ) : (
                    <span style={{ color: 'var(--text-muted)' }}>{MINUS}</span>
                  )}
                </div>

                {/* ⭐ */}
                <button
                  type="button"
                  onClick={(e) => toggleFavorite(r.sectype, e)}
                  aria-label={isFav ? 'Убрать из избранного' : 'В избранное'}
                  style={{
                    background: 'none', border: 'none', cursor: 'pointer', padding: 6,
                    color: isFav ? 'var(--accent)' : 'var(--text-muted)', lineHeight: 0,
                  }}
                >
                  <Star size={16} fill={isFav ? 'var(--accent)' : 'none'} strokeWidth={2} />
                </button>
              </div>
            );
          })}
        </div>
      </div>

      {/* Футер */}
      {!error && rows !== null && (
        <div className="flex items-center justify-between flex-wrap" style={{ ...MONO, gap: 8, padding: '12px 4px 0', fontSize: 11, color: 'var(--text-muted)' }}>
          <span>{pluralAssets(visible.length)} · физлица</span>
          <span>
            Данные: Московская биржа{dateLabel && <> · <em>по данным за {dateLabel}</em></>}
          </span>
        </div>
      )}
    </div>
  );
}
