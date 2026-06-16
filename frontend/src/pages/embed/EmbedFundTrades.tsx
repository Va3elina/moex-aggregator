/**
 * EmbedFundTrades — виджет «Сделки фондов / Что покупают фонды» для терминала
 * Т-Инвестиций. Компактный 4-таб экран (полностью PRO — embed под PRO-токеном):
 *   - movers   «Покупки» — консенсус-движения: две колонки Покупают/Продают с
 *               барами по Δвеса/Δобъёму. Контролы (месяц/метрика/фонды) — в drawer'е.
 *   - snapshots «Снапшот» — per-fund помесячный diff (докупил/продал/новые/вышел).
 *   - funds    «Состав» — карточки фондов (УК + тикер + донат + топ-холдинги + доходность).
 *   - company  «Потоки» — потоки по компании (переиспользует CompanyFlowsTab).
 *
 * Локальный pill-таб-бар живёт ВНУТРИ этого файла (не в EmbedSettings). Активный
 * таб + параметры контролов персистятся в namespace frame:embed:fundtrades:*.
 *
 * Всё инлайн-стилями с CSS-var, чтобы работать в любой теме внутри iframe.
 */
import { useEffect, useMemo, useState, type CSSProperties, type ReactNode } from 'react';
import {
  listFundsWithHistory,
  getFundTradesMovers,
  getFundSnapshots,
  getFundSnapshotReview,
  type FundTradesMover,
  type FundTradesPeriod,
  type FundWithHistory,
  type FundSnapshotsList,
  type FundSnapshotReview,
  type FundDiffRow,
} from '../../services/api';
import { EmbedMsg } from './embedUi';
import { useEmbedSettings, EmbedShell, DrawerSection, SegGroup } from './EmbedSettings';
import FundPicker, { type FundPickerFund } from '../../components/fundtrades/FundPicker';
import UkMultiSelect, { type UkOption } from '../../components/fundtrades/UkMultiSelect';
import CompanyFlowsTab from '../../components/fundtrades/CompanyFlowsTab';
import Donut from '../../components/funds/Donut';
import {
  formatRubShort,
  formatShares,
  formatReturnPct,
  returnColor,
} from '../../components/funds/FundDetailModal';
import { UK_LOGOS, DONUT_COLORS, assetColor, resolveFundLogo, stripUkName } from '../../config/fundConfig';

type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type EmbedTab = 'movers' | 'snapshots' | 'funds' | 'company';

const POS = 'var(--funds-flow-positive, #4A9268)';
const NEG = 'var(--funds-flow-negative, #C0504D)';

const PERIODS: { id: FundTradesPeriod; label: string }[] = [
  { id: '1m', label: 'Месяц' },
  { id: '1y', label: 'Год' },
];
const P_SHORT: Record<FundTradesPeriod, string> = { '1m': 'за месяц', '1y': 'за год' };

const TABS: { id: EmbedTab; label: string }[] = [
  { id: 'movers', label: 'Покупки' },
  { id: 'snapshots', label: 'Снапшот' },
  { id: 'funds', label: 'Состав' },
  { id: 'company', label: 'Потоки' },
];

const SUBTITLE: Record<EmbedTab, string> = {
  movers: 'консенсус движений · Δвеса',
  snapshots: 'обзор снапшота по фонду',
  funds: 'состав портфелей фондов',
  company: 'потоки по компании',
};

// ─────────────────────────────── helpers ───────────────────────────────

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}
function writeLS(key: string, value: string): void {
  try { localStorage.setItem(key, value); } catch { /* quota */ }
}

// "2026-04-30" → "Апрель 2026" — для month-picker день не показываем.
const MONTHS_RU = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
  'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
function formatMonthYear(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return `${MONTHS_RU[d.getMonth()]} ${d.getFullYear()}`;
}

// Стабильный ключ УК: uk_id, иначе имя.
function ukKey(f: { uk_id?: number | string | null; uk?: string | null }): string {
  if (f.uk_id != null && f.uk_id !== '') return String(f.uk_id);
  return f.uk || '';
}

// ─────────────────────────────── shared bits ───────────────────────────────

// Локальный таб-бар (pills) ВНУТРИ виджета. Узкий, переносится на узких панелях.
function TabBar({ tab, onChange }: { tab: EmbedTab; onChange: (t: EmbedTab) => void }) {
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, flexShrink: 0, marginBottom: 8 }}>
      {TABS.map((t) => {
        const on = t.id === tab;
        return (
          <button
            key={t.id}
            type="button"
            onClick={() => onChange(t.id)}
            style={{
              padding: '5px 12px',
              fontSize: 12,
              fontWeight: on ? 800 : 600,
              borderRadius: 999,
              cursor: 'pointer',
              border: on ? '1.5px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
              background: on ? 'var(--accent)' : 'transparent',
              color: on ? '#fff' : 'var(--text-primary)',
              whiteSpace: 'nowrap',
              lineHeight: 1.3,
              transition: 'background 0.12s, border-color 0.12s',
            }}
          >
            {t.label}
          </button>
        );
      })}
    </div>
  );
}

// Кнопки-сегменты компактные (для метрики внутри тела таба).
function MetricToggle<T extends string>({
  value,
  options,
  onChange,
}: {
  value: T;
  options: [T, string][];
  onChange: (v: T) => void;
}) {
  return (
    <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
      {options.map(([key, lbl]) => {
        const on = key === value;
        return (
          <button
            key={key}
            type="button"
            onClick={() => onChange(key)}
            style={{
              fontSize: 11.5,
              fontWeight: on ? 700 : 600,
              padding: '4px 10px',
              borderRadius: 6,
              cursor: 'pointer',
              border: on ? '1.5px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
              background: on ? 'var(--accent)' : 'transparent',
              color: on ? '#fff' : 'var(--text-primary)',
              whiteSpace: 'nowrap',
            }}
          >
            {lbl}
          </button>
        );
      })}
    </div>
  );
}

// Горизонтальный бар (editorial-стиль) для снапшота: имя + bar + значение.
function EmbedBar({
  label,
  subLabel,
  amount,
  maxAbs,
  isPositive,
  formatValue,
}: {
  label: string;
  subLabel?: string;
  amount: number;
  maxAbs: number;
  isPositive: boolean;
  formatValue: (absValue: number) => string;
}) {
  const widthPct = maxAbs > 0 ? Math.max(2, (Math.abs(amount) / maxAbs) * 100) : 2;
  const color = isPositive ? POS : NEG;
  return (
    <div style={{ padding: '6px 0', borderBottom: '1px solid color-mix(in srgb, var(--text-primary) 8%, transparent)' }}>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
        <span style={{ flex: 1, minWidth: 0, fontSize: 12, color: 'var(--text-primary)', fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {label}
        </span>
        <span style={{ flexShrink: 0, fontSize: 12, fontWeight: 700, color, fontVariantNumeric: 'tabular-nums' }}>
          {isPositive ? '+' : '−'}{formatValue(Math.abs(amount))}
        </span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 4 }}>
        <div style={{ flex: 1, height: 5, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 3, overflow: 'hidden' }}>
          <div style={{ width: `${widthPct}%`, height: '100%', background: color, borderRadius: 3 }} />
        </div>
        {subLabel && (
          <span style={{ flexShrink: 0, fontSize: 10.5, color: 'var(--text-muted)', fontVariantNumeric: 'tabular-nums', maxWidth: 130, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {subLabel}
          </span>
        )}
      </div>
    </div>
  );
}

function EmbedSection({
  title,
  count,
  total,
  isPositive,
  formatValue,
  children,
}: {
  title: string;
  count: number;
  total: number;
  isPositive: boolean;
  formatValue: (absValue: number) => string;
  children: ReactNode;
}) {
  return (
    <div style={{ marginBottom: 18 }}>
      <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', borderBottom: '1.5px solid var(--text-primary)', paddingBottom: 5, marginBottom: 8 }}>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
          <span style={{ fontSize: 11, fontWeight: 800, letterSpacing: '0.06em', color: 'var(--text-primary)' }}>{title}</span>
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{count}</span>
        </div>
        <span style={{ fontSize: 12, fontWeight: 700, color: isPositive ? POS : NEG, fontVariantNumeric: 'tabular-nums' }}>
          {isPositive ? '+' : '−'}{formatValue(Math.abs(total))}
        </span>
      </div>
      {children}
    </div>
  );
}

// ─────────────────────────────── root ───────────────────────────────

export default function EmbedFundTrades() {
  const settings = useEmbedSettings();
  const [tab, setTab] = useState<EmbedTab>(() => {
    const v = readLS('frame:embed:fundtrades:tab', 'movers');
    return (['movers', 'snapshots', 'funds', 'company'] as const).includes(v as EmbedTab) ? (v as EmbedTab) : 'movers';
  });
  useEffect(() => { writeLS('frame:embed:fundtrades:tab', tab); }, [tab]);

  // Список фондов — загружается один раз, шарится между movers/snapshots/funds.
  const [funds, setFunds] = useState<FundWithHistory[]>([]);
  useEffect(() => {
    let cancelled = false;
    listFundsWithHistory()
      .then((r) => { if (!cancelled) setFunds(r.funds); })
      .catch((err) => { if (!cancelled) console.error('embed/fund-trades funds load failed:', err); });
    return () => { cancelled = true; };
  }, []);

  // ── movers state ──
  const [period, setPeriod] = useState<FundTradesPeriod>(() => readLS('frame:embed:fundtrades:period', '1m') as FundTradesPeriod);
  const [moverMetric, setMoverMetric] = useState<'weight' | 'amount'>(() => readLS('frame:embed:fundtrades:metric', 'weight') as 'weight' | 'amount');
  const [asOf, setAsOf] = useState<string | undefined>(undefined);
  const [selectedMoverFunds, setSelectedMoverFunds] = useState<Set<string>>(new Set());
  const [acc, setAcc] = useState<FundTradesMover[]>([]);
  const [red, setRed] = useState<FundTradesMover[]>([]);
  const [availableMonths, setAvailableMonths] = useState<string[]>([]);
  const [resolvedMonth, setResolvedMonth] = useState<string | null>(null);
  const [moversStatus, setMoversStatus] = useState<LoadStatus>('idle');

  useEffect(() => { writeLS('frame:embed:fundtrades:period', period); }, [period]);
  useEffect(() => { writeLS('frame:embed:fundtrades:metric', moverMetric); }, [moverMetric]);

  const fundsParam = useMemo(() => Array.from(selectedMoverFunds).join(','), [selectedMoverFunds]);

  useEffect(() => {
    if (tab !== 'movers') return;
    let cancelled = false;
    setMoversStatus('loading');
    getFundTradesMovers(period, { asOf, funds: fundsParam || undefined, sort: moverMetric, limit: 8 })
      .then((res) => {
        if (cancelled) return;
        const a = res?.top_accumulated ?? [];
        const r = res?.top_reduced ?? [];
        setAcc(a);
        setRed(r);
        setAvailableMonths(res?.available_months ?? []);
        setResolvedMonth(res?.resolved_month ?? null);
        setMoversStatus(a.length || r.length ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/fund-trades movers load failed:', err);
        setMoversStatus('error');
      });
    return () => { cancelled = true; };
  }, [tab, period, asOf, fundsParam, moverMetric]);

  const moverPickerFunds = useMemo<FundPickerFund[]>(
    () => funds.map((f) => ({ ticker: f.ticker, name: f.name, uk: f.uk, uk_id: f.uk_id })),
    [funds],
  );

  // ── funds tab state ──
  const [fundSort, setFundSort] = useState<'return' | 'volume' | 'name'>(() => readLS('frame:embed:fundtrades:fundSort', 'return') as 'return' | 'volume' | 'name');
  const [selectedUks, setSelectedUks] = useState<Set<string>>(new Set());
  useEffect(() => { writeLS('frame:embed:fundtrades:fundSort', fundSort); }, [fundSort]);

  const ukOptions = useMemo<UkOption[]>(() => {
    const map = new Map<string, UkOption>();
    for (const f of funds) {
      const key = ukKey(f);
      if (!key || map.has(key)) continue;
      map.set(key, { key, name: UK_LOGOS[key]?.name || f.uk || key, uk_id: f.uk_id ?? key });
    }
    return Array.from(map.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [funds]);

  const sortedFunds = useMemo(() => {
    const filtered = selectedUks.size > 0 ? funds.filter((f) => selectedUks.has(ukKey(f))) : funds;
    const cmp = (a: FundWithHistory, b: FundWithHistory): number => {
      if (fundSort === 'name') return a.ticker.localeCompare(b.ticker);
      const av = fundSort === 'return' ? (a.returns?.y1 ?? null) : a.nav_rub;
      const bv = fundSort === 'return' ? (b.returns?.y1 ?? null) : b.nav_rub;
      if (av == null && bv == null) return a.ticker.localeCompare(b.ticker);
      if (av == null) return 1;
      if (bv == null) return -1;
      return bv - av;
    };
    return [...filtered].sort(cmp);
  }, [funds, fundSort, selectedUks]);

  // ── drawer per tab ──
  const drawer: ReactNode =
    tab === 'movers' ? (
      <>
        <DrawerSection label="Окно периода">
          <SegGroup value={period} options={PERIODS} onChange={setPeriod} />
        </DrawerSection>
        <DrawerSection label="Метрика">
          <SegGroup<'weight' | 'amount'>
            value={moverMetric}
            options={[{ id: 'weight', label: '% веса' }, { id: 'amount', label: 'Объём, руб' }]}
            onChange={setMoverMetric}
          />
        </DrawerSection>
        {availableMonths.length > 0 && (
          <DrawerSection label="Месяц снапшота">
            <SegGroup<string>
              value={asOf ?? resolvedMonth ?? availableMonths[0]}
              options={availableMonths.map((m) => ({ id: m, label: formatMonthYear(m) }))}
              onChange={setAsOf}
            />
          </DrawerSection>
        )}
        {moverPickerFunds.length > 1 && (
          <DrawerSection label="Фонды">
            <FundPicker funds={moverPickerFunds} mode="multi" selected={selectedMoverFunds} onChange={setSelectedMoverFunds} />
          </DrawerSection>
        )}
      </>
    ) : tab === 'funds' ? (
      <>
        <DrawerSection label="Сортировка">
          <SegGroup<'return' | 'volume' | 'name'>
            value={fundSort}
            options={[{ id: 'return', label: 'Доходность' }, { id: 'volume', label: 'Объём' }, { id: 'name', label: 'Имя' }]}
            onChange={setFundSort}
          />
        </DrawerSection>
        {ukOptions.length > 1 && (
          <DrawerSection label="Управляющая компания">
            <UkMultiSelect options={ukOptions} selected={selectedUks} onChange={setSelectedUks} size="md" />
          </DrawerSection>
        )}
      </>
    ) : tab === 'snapshots' ? (
      <DrawerSection label="Снапшот">
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5 }}>
          Выбор фонда, месяца и метрики — кнопками в самом виджете.
        </div>
      </DrawerSection>
    ) : (
      <DrawerSection label="Потоки по компании">
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5 }}>
          Выбор бумаги и фондов — кнопками в самом виджете.
        </div>
      </DrawerSection>
    );

  return (
    <EmbedShell settings={settings} title="Сделки фондов" subtitle={tab === 'movers' ? `консенсус ${P_SHORT[period]} · Δвеса` : SUBTITLE[tab]} drawer={drawer}>
      <div style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
        <TabBar tab={tab} onChange={setTab} />
        <div style={{ flex: 1, minHeight: 0, overflow: 'auto', position: 'relative', display: 'flex', flexDirection: 'column' }}>
          {tab === 'movers' && (
            <div style={{ flex: 1, minHeight: 0, display: 'flex', gap: 12, position: 'relative' }}>
              {moversStatus === 'ok' ? (
                <>
                  <MoverCol title="Покупают" color={POS} items={acc} metric={moverMetric} />
                  <MoverCol title="Продают" color={NEG} items={red} metric={moverMetric} />
                </>
              ) : (
                <>
                  {moversStatus === 'loading' && <EmbedMsg text="Загрузка…" />}
                  {moversStatus === 'empty' && <EmbedMsg text="Нет данных" />}
                  {moversStatus === 'error' && <EmbedMsg text="Ошибка загрузки" />}
                  {moversStatus === 'idle' && <EmbedMsg text="Загрузка…" />}
                </>
              )}
            </div>
          )}

          {tab === 'snapshots' && <SnapshotsTab funds={funds} />}

          {tab === 'funds' && <FundsTab funds={sortedFunds} hasFunds={funds.length > 0} />}

          {tab === 'company' && (
            <div style={{ flex: 1, minHeight: 0 }}>
              <CompanyFlowsTab />
            </div>
          )}
        </div>
      </div>
    </EmbedShell>
  );
}

// ─────────────────────────────── movers column ───────────────────────────────

function MoverCol({ title, color, items, metric }: { title: string; color: string; items: FundTradesMover[]; metric: 'weight' | 'amount' }) {
  const valOf = (m: FundTradesMover) => (metric === 'amount' ? m.total_delta_amount : m.total_delta_weight);
  const fmtVal = (v: number) => (metric === 'amount'
    ? `${v > 0 ? '+' : '−'}${formatRubShort(Math.abs(v))}`
    : `${v > 0 ? '+' : ''}${v.toFixed(2)}%`);
  const maxAbs = Math.max(...items.map((m) => Math.abs(valOf(m))), 0.0001);
  return (
    <div style={{ flex: 1, minWidth: 0 }}>
      <div style={{ fontSize: 11, fontWeight: 800, color, borderBottom: `1px solid ${color}`, paddingBottom: 4, marginBottom: 6 }}>
        {title}
      </div>
      {items.length === 0 && (
        <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>—</div>
      )}
      {items.slice(0, 8).map((m) => {
        const v = valOf(m);
        const pct = Math.max(3, (Math.abs(v) / maxAbs) * 100);
        return (
          <div key={m.akey} style={{ marginBottom: 6 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: 6, fontSize: 11 }}>
              <span style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', color: 'var(--text-primary)' }}>
                {m.asset_name}
              </span>
              <span style={{ color, fontWeight: 600, flexShrink: 0, fontVariantNumeric: 'tabular-nums' }}>
                {fmtVal(v)}
              </span>
            </div>
            <div style={{ height: 4, background: 'var(--border-color, rgba(128,128,128,0.18))', borderRadius: 2, marginTop: 2 }}>
              <div style={{ height: '100%', width: `${pct}%`, background: color, borderRadius: 2 }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ─────────────────────────────── snapshots tab ───────────────────────────────

function SnapshotsTab({ funds }: { funds: FundWithHistory[] }) {
  const [ticker, setTicker] = useState<string>('EQMX');
  const [metric, setMetric] = useState<'amount' | 'weight'>(() => readLS('frame:embed:fundtrades:snapMetric', 'amount') as 'amount' | 'weight');
  const [snapshotsList, setSnapshotsList] = useState<FundSnapshotsList | null>(null);
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [review, setReview] = useState<FundSnapshotReview | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { writeLS('frame:embed:fundtrades:snapMetric', metric); }, [metric]);

  // Фонды с историей снапшотов (для пикера). funds приходят из root.
  const pickerFunds = useMemo<FundPickerFund[]>(
    () => funds.filter((f) => (f.snapshot_count || 0) > 0).map((f) => ({ ticker: f.ticker, name: f.name, uk: f.uk, uk_id: f.uk_id })),
    [funds],
  );
  const pickerSelected = useMemo(() => new Set([ticker]), [ticker]);

  // Список снапшотов при смене фонда.
  useEffect(() => {
    let cancel = false;
    setStatus('loading');
    getFundSnapshots(ticker)
      .then((data) => {
        if (cancel) return;
        setSnapshotsList(data);
        if (data.snapshots.length > 0) {
          setSelectedDate(data.snapshots[0].snapshot_date);
        } else {
          setSelectedDate(null);
          setReview(null);
          setStatus('empty');
        }
      })
      .catch((err) => { if (!cancel) { console.error('embed/snapshots list failed:', err); setStatus('error'); } });
    return () => { cancel = true; };
  }, [ticker]);

  // Обзор при смене даты.
  useEffect(() => {
    if (!selectedDate) return;
    let cancel = false;
    setStatus('loading');
    getFundSnapshotReview(ticker, selectedDate)
      .then((data) => { if (!cancel) { setReview(data); setStatus('ok'); } })
      .catch((err) => { if (!cancel) { console.error('embed/snapshot review failed:', err); setStatus('error'); } });
    return () => { cancel = true; };
  }, [ticker, selectedDate]);

  const isW = metric === 'weight';
  const wDelta = (r: FundDiffRow) => (r.curr_weight ?? 0) - (r.prev_weight ?? 0);
  const aAdded = (r: FundDiffRow) => r.delta_amount_rub ?? 0;
  const aNew = (r: FundDiffRow) => r.curr_amount_rub ?? 0;
  const aSold = (r: FundDiffRow) => -(r.prev_amount_rub ?? 0);
  const wNew = (r: FundDiffRow) => r.curr_weight ?? 0;
  const wSold = (r: FundDiffRow) => -(r.prev_weight ?? 0);

  const maxAbs = useMemo(() => {
    if (!review) return 1;
    if (isW) {
      return Math.max(
        0.01,
        ...review.added.map((r) => Math.abs(wDelta(r))),
        ...review.reduced.map((r) => Math.abs(wDelta(r))),
        ...review.new.map((r) => r.curr_weight ?? 0),
        ...review.sold_out.map((r) => r.prev_weight ?? 0),
      );
    }
    return Math.max(
      1,
      ...review.added.map((r) => Math.abs(r.delta_amount_rub ?? 0)),
      ...review.reduced.map((r) => Math.abs(r.delta_amount_rub ?? 0)),
      ...review.new.map((r) => r.curr_amount_rub ?? 0),
      ...review.sold_out.map((r) => r.prev_amount_rub ?? 0),
    );
  }, [review, isW]);

  const fmtVal = isW ? (v: number) => `${v.toFixed(2)}%` : (v: number) => formatRubShort(v);
  const sortByAbs = (items: FundDiffRow[], get: (r: FundDiffRow) => number) =>
    [...items].sort((a, b) => Math.abs(get(b)) - Math.abs(get(a)));
  const sumBy = (items: FundDiffRow[], get: (r: FundDiffRow) => number) => items.reduce((s, r) => s + get(r), 0);

  // В режиме «% веса» докупил/продал бакетим по знаку Δдоли.
  const addedItems = review
    ? (isW ? [...review.added, ...review.reduced].filter((r) => wDelta(r) > 0) : review.added)
    : [];
  const reducedItems = review
    ? (isW ? [...review.added, ...review.reduced].filter((r) => wDelta(r) < 0) : review.reduced)
    : [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10, minHeight: 0 }}>
      {/* Контролы: фонд (FundPicker single) + метрика */}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center', flexShrink: 0 }}>
        {pickerFunds.length > 0 && (
          <FundPicker
            funds={pickerFunds}
            mode="single"
            selected={pickerSelected}
            onChange={(next) => {
              const t = next.values().next().value as string | undefined;
              if (t) setTicker(t);
            }}
            minWidth={200}
          />
        )}
        <div style={{ marginLeft: 'auto' }}>
          <MetricToggle<'amount' | 'weight'>
            value={metric}
            options={[['amount', 'Объём, руб'], ['weight', '% веса']]}
            onChange={setMetric}
          />
        </div>
      </div>

      {/* Лента месяцев */}
      {snapshotsList && snapshotsList.snapshots.length > 0 && (
        <div style={{ display: 'flex', gap: 6, overflowX: 'auto', paddingBottom: 4, flexShrink: 0 }}>
          {snapshotsList.snapshots.map((s) => {
            const on = s.snapshot_date === selectedDate;
            return (
              <button
                key={s.snapshot_date}
                type="button"
                onClick={() => setSelectedDate(s.snapshot_date)}
                title={`${s.snapshot_date} · ${s.asset_count} активов`}
                style={{
                  padding: '4px 11px',
                  fontSize: 11,
                  fontWeight: on ? 700 : 600,
                  borderRadius: 999,
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                  flexShrink: 0,
                  fontVariantNumeric: 'tabular-nums',
                  border: on ? '1.5px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
                  background: on ? 'var(--accent)' : 'transparent',
                  color: on ? '#fff' : 'var(--text-secondary)',
                }}
              >
                {formatMonthYear(s.snapshot_date)}
              </button>
            );
          })}
        </div>
      )}

      {/* Тело */}
      <div style={{ flex: 1, minHeight: 0, overflow: 'auto', position: 'relative' }}>
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
        {status === 'empty' && <EmbedMsg text={`У ${ticker} пока нет снапшотов`} />}

        {status === 'ok' && review && (
          <div>
            {/* Заголовок */}
            <div style={{ marginBottom: 12 }}>
              <div style={{ fontSize: 14, fontWeight: 800, color: 'var(--text-primary)', lineHeight: 1.25 }}>{review.fund.name}</div>
              <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginTop: 2 }}>
                {formatMonthYear(review.current_snapshot_date)}
                {review.previous_snapshot_date && <> · с {formatMonthYear(review.previous_snapshot_date)}</>}
                {' · '}{review.totals.current_assets} активов
              </div>
            </div>

            {/* Нет предыдущего — показываем состав */}
            {!review.previous_snapshot_date && (
              <div>
                <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginBottom: 8 }}>
                  Самый ранний снапшот — состав фонда на эту дату:
                </div>
                {review.current_holdings.map((h) => (
                  <div key={h.isin || h.asset_name} style={{ display: 'flex', justifyContent: 'space-between', gap: 10, padding: '5px 0', borderBottom: '1px solid color-mix(in srgb, var(--text-primary) 8%, transparent)' }}>
                    <span style={{ fontSize: 12, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{h.asset_name}</span>
                    <span style={{ fontSize: 12, color: 'var(--text-secondary)', flexShrink: 0, fontVariantNumeric: 'tabular-nums' }}>
                      {h.weight != null ? `${h.weight.toFixed(2)}%` : '—'}
                    </span>
                  </div>
                ))}
              </div>
            )}

            {/* ДОКУПИЛ */}
            {addedItems.length > 0 && (
              <EmbedSection
                title="ДОКУПИЛ"
                count={addedItems.length}
                total={isW ? sumBy(addedItems, wDelta) : review.totals.total_added_rub}
                isPositive
                formatValue={fmtVal}
              >
                {sortByAbs(addedItems, isW ? wDelta : aAdded).slice(0, 8).map((r) => (
                  <EmbedBar
                    key={`${r.asset_name}-${r.isin || ''}`}
                    label={r.asset_name}
                    subLabel={`+${formatShares(r.delta_positions || 0)} шт${r.curr_weight != null ? ` · ${r.curr_weight.toFixed(2)}%` : ''}`}
                    amount={isW ? wDelta(r) : aAdded(r)}
                    maxAbs={maxAbs}
                    isPositive
                    formatValue={fmtVal}
                  />
                ))}
              </EmbedSection>
            )}

            {/* ПРОДАЛ */}
            {reducedItems.length > 0 && (
              <EmbedSection
                title="ПРОДАЛ"
                count={reducedItems.length}
                total={isW ? Math.abs(sumBy(reducedItems, wDelta)) : Math.abs(review.totals.total_reduced_rub)}
                isPositive={false}
                formatValue={fmtVal}
              >
                {sortByAbs(reducedItems, isW ? wDelta : aAdded).slice(0, 8).map((r) => (
                  <EmbedBar
                    key={`${r.asset_name}-${r.isin || ''}`}
                    label={r.asset_name}
                    subLabel={`${formatShares(r.delta_positions || 0)} шт${r.curr_weight != null ? ` · ${r.curr_weight.toFixed(2)}%` : ''}`}
                    amount={isW ? wDelta(r) : aAdded(r)}
                    maxAbs={maxAbs}
                    isPositive={false}
                    formatValue={fmtVal}
                  />
                ))}
              </EmbedSection>
            )}

            {/* НОВЫЕ ПОЗИЦИИ */}
            {review.new.length > 0 && (
              <EmbedSection
                title="НОВЫЕ ПОЗИЦИИ"
                count={review.new.length}
                total={isW ? sumBy(review.new, wNew) : review.totals.total_new_rub}
                isPositive
                formatValue={fmtVal}
              >
                {sortByAbs(review.new, isW ? wNew : aNew).slice(0, 8).map((r) => (
                  <EmbedBar
                    key={`${r.asset_name}-${r.isin || ''}`}
                    label={r.asset_name}
                    subLabel={`${formatShares(r.curr_positions)} шт${r.curr_weight != null ? ` · ${r.curr_weight.toFixed(2)}%` : ''}`}
                    amount={isW ? wNew(r) : aNew(r)}
                    maxAbs={maxAbs}
                    isPositive
                    formatValue={fmtVal}
                  />
                ))}
              </EmbedSection>
            )}

            {/* ПОЛНОСТЬЮ ВЫШЕЛ */}
            {review.sold_out.length > 0 && (
              <EmbedSection
                title="ПОЛНОСТЬЮ ВЫШЕЛ"
                count={review.sold_out.length}
                total={isW ? Math.abs(sumBy(review.sold_out, wSold)) : review.totals.total_sold_out_rub}
                isPositive={false}
                formatValue={fmtVal}
              >
                {sortByAbs(review.sold_out, isW ? wSold : aSold).slice(0, 8).map((r) => (
                  <EmbedBar
                    key={`${r.asset_name}-${r.isin || ''}`}
                    label={r.asset_name}
                    subLabel={`было ${formatShares(r.prev_positions)} шт`}
                    amount={isW ? wSold(r) : aSold(r)}
                    maxAbs={maxAbs}
                    isPositive={false}
                    formatValue={fmtVal}
                  />
                ))}
              </EmbedSection>
            )}

            {/* Без изменений */}
            {review.previous_snapshot_date
              && review.added.length === 0
              && review.reduced.length === 0
              && review.new.length === 0
              && review.sold_out.length === 0 && (
              <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>
                Состав не изменился между снапшотами.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ─────────────────────────────── funds tab ───────────────────────────────

function FundsTab({ funds, hasFunds }: { funds: FundWithHistory[]; hasFunds: boolean }) {
  if (!hasFunds) {
    return <EmbedMsg text="Загрузка…" />;
  }
  if (funds.length === 0) {
    return <EmbedMsg text="Фонды не найдены" />;
  }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {funds.map((f) => (
        <FundCard key={f.fund_id} fund={f} />
      ))}
    </div>
  );
}

function FundCard({ fund: f }: { fund: FundWithHistory }) {
  const uk = resolveFundLogo(f.ticker, f.uk_id);
  const top = f.top_holdings ?? [];
  const sum = top.reduce((s, h) => s + (h.weight || 0), 0);
  const other = 100 - sum;
  const donutHoldings = other > 1 ? [...top, { name: 'Прочее', weight: other }] : top;
  const donutColors = donutHoldings.map((h, i) =>
    h.name === 'Прочее' ? 'var(--text-muted)' : (assetColor(h.name) ?? DONUT_COLORS[i % DONUT_COLORS.length]));
  const ret = f.returns?.y1 ?? f.returns?.all ?? null;

  const avatarStyle: CSSProperties = {
    width: 34,
    height: 34,
    borderRadius: '50%',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    flexShrink: 0,
    fontWeight: 900,
    fontSize: 14,
    overflow: 'hidden',
    backgroundColor: uk && uk.img ? undefined : uk?.bg,
    color: uk?.color,
  };

  return (
    <div
      style={{
        padding: 12,
        background: 'var(--bg-secondary, transparent)',
        border: '1.5px solid var(--border-color, rgba(128,128,128,0.3))',
        borderRadius: 10,
        display: 'flex',
        flexDirection: 'column',
        gap: 10,
      }}
    >
      {/* Header: аватар + имя + тикер */}
      <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
        {uk && (
          <div title={uk.name} style={avatarStyle}>
            {uk.img
              ? <img src={uk.img} alt={uk.name} style={{ width: '100%', height: '100%', objectFit: 'cover' }} />
              : uk.letter}
          </div>
        )}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 12.5, fontWeight: 800, color: 'var(--text-primary)', lineHeight: 1.2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={f.name}>
            {stripUkName(f.name, f.uk_id)}
          </div>
          <div style={{ fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace', fontSize: 11, color: 'var(--text-secondary)', lineHeight: 1.3 }}>
            {f.ticker}
          </div>
        </div>
        <div style={{ textAlign: 'right', flexShrink: 0 }}>
          <div style={{ fontSize: 9.5, color: 'var(--text-muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em' }}>Доходн.</div>
          <div style={{ fontSize: 14, fontWeight: 800, color: returnColor(ret), fontVariantNumeric: 'tabular-nums', lineHeight: 1.1 }}>
            {formatReturnPct(ret)}
          </div>
        </div>
      </div>

      {/* Body: донат + топ-холдинги */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
        {top.length > 0 ? (
          <div style={{ flexShrink: 0, lineHeight: 0 }}>
            <Donut
              holdings={donutHoldings}
              colors={donutColors}
              size={96}
              outerRadius={92}
              innerRadius={62}
              maxSlices={donutHoldings.length}
              centerCount={f.holdings_count}
              showCenterText
            />
          </div>
        ) : (
          <div style={{ width: 96, height: 96, flexShrink: 0, borderRadius: '50%', border: '1.5px dashed var(--border-color)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)', fontSize: 22 }}>
            —
          </div>
        )}
        <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 3 }}>
          {top.slice(0, 5).map((h, i) => (
            <div key={h.name + i} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11 }}>
              <span style={{ width: 7, height: 7, borderRadius: '50%', flexShrink: 0, backgroundColor: assetColor(h.name) ?? DONUT_COLORS[i % DONUT_COLORS.length] }} />
              <span style={{ flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-secondary)', fontWeight: 600 }}>
                {h.name}
              </span>
              <span style={{ flexShrink: 0, fontVariantNumeric: 'tabular-nums', fontWeight: 600, color: 'var(--text-primary)' }}>
                {h.weight.toFixed(1)}%
              </span>
            </div>
          ))}
          {top.length === 0 && (
            <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Состав не публикуется</span>
          )}
          <div style={{ marginTop: 4, paddingTop: 6, borderTop: '1px solid color-mix(in srgb, var(--text-primary) 8%, transparent)', display: 'flex', justifyContent: 'space-between', fontSize: 11 }}>
            <span style={{ color: 'var(--text-muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em' }}>СЧА</span>
            <span style={{ color: 'var(--text-primary)', fontWeight: 700, fontVariantNumeric: 'tabular-nums' }}>
              {f.nav_rub != null ? formatRubShort(f.nav_rub) : '—'}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
