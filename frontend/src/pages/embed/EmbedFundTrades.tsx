/**
 * EmbedFundTrades — виджет «Сделки / Что покупают фонды» для терминала
 * Т-Инвестиций. Компактный 4-таб экран (полностью PRO — embed под PRO-токеном):
 *   - movers   «Сделки» — консенсус-движения, тот же `PortfolioMoversPanel`
 *               («Чистые покупки/продажи»), что и на сайте во вкладке «Общий
 *               портфель» (июль 2026 редизайн, #493/#571) — раньше тут была своя
 *               параллельная вёрстка, разъехавшаяся с сайтом после переноса
 *               «Покупок фондов» внутрь «Сделок фондов». Период 1М/6М/1Г —
 *               в шапке самой панели (её проп), фильтр фондов — в ⚙.
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
  type FundTradesMovers,
  type FundWithHistory,
  type FundSnapshotsList,
  type FundSnapshotReview,
  type FundDiffRow,
} from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, SegGroup } from './EmbedSettings';
import { EmbedFrame } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import FundPicker, { type FundPickerFund } from '../../components/fundtrades/FundPicker';
import UkMultiSelect, { type UkOption } from '../../components/fundtrades/UkMultiSelect';
import CompanyFlowsTab from '../../components/fundtrades/CompanyFlowsTab';
import PortfolioMoversPanel, { type MoversPeriod } from '../../components/fundtrades/PortfolioMoversPanel';
import Donut from '../../components/funds/Donut';
import {
  formatRubShort,
  formatShares,
  formatReturnPct,
  returnColor,
} from '../../components/funds/FundDetailModal';
import { UK_LOGOS, DONUT_COLORS, fundAssetName, fundAssetColor, resolveFundLogo, stripUkName } from '../../config/fundConfig';

type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type EmbedTab = 'movers' | 'snapshots' | 'funds' | 'company';

const POS = 'var(--funds-flow-positive, #4A9268)';
const NEG = 'var(--funds-flow-negative, #C0504D)';

const TABS: { id: EmbedTab; label: string }[] = [
  { id: 'movers', label: 'Сделки' },
  { id: 'snapshots', label: 'Снапшот' },
  { id: 'funds', label: 'Состав' },
  { id: 'company', label: 'Потоки' },
];

// ─────────────────────────────── helpers ───────────────────────────────

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

// Локальный таб-бар (pills) ВНУТРИ виджета. Живёт в тулбаре EmbedFrame — там
// у родителя overflow:hidden в ОДНУ строку (§4.1), поэтому flexWrap тут не
// сработал бы (лишние строки просто обрежутся, а ⚙ наедет на обрезанную
// вкладку — реальный баг на узкой панели у MINW=300). Вместо wrap — горизонтальный
// скролл в один ряд: узко — скроллим тач/колесом, не теряем доступ к вкладкам.
function TabBar({ tab, onChange }: { tab: EmbedTab; onChange: (t: EmbedTab) => void }) {
  return (
    <div className="styled-scrollbar" style={{ display: 'flex', flexWrap: 'nowrap', overflowX: 'auto', gap: 6, flexShrink: 1, minWidth: 0 }}>
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

export default function EmbedFundTrades({ lockTab }: { lockTab?: EmbedTab } = {}) {
  const { rd, wr } = useEmbedPersist();
  // lockTab — отдельный индикатор «Сделки»: фиксируем вкладку movers,
  // таб-бар прячем, а название и первичные контролы (период/метрика) — в тулбар.
  const [tab, setTab] = useState<EmbedTab>(() => {
    if (lockTab) return lockTab;
    const v = rd('frame:embed:fundtrades:tab', 'movers');
    return (['movers', 'snapshots', 'funds', 'company'] as const).includes(v as EmbedTab) ? (v as EmbedTab) : 'movers';
  });
  useEffect(() => { if (!lockTab) wr('frame:embed:fundtrades:tab', tab); }, [tab, lockTab]);

  // Список фондов — загружается один раз, шарится между movers/snapshots/funds.
  const [funds, setFunds] = useState<FundWithHistory[]>([]);
  useEffect(() => {
    let cancelled = false;
    listFundsWithHistory()
      .then((r) => { if (!cancelled) setFunds(r.funds); })
      .catch((err) => { if (!cancelled) console.error('embed/fund-trades funds load failed:', err); });
    return () => { cancelled = true; };
  }, []);

  // ── movers state (= сайтовый PortfolioMoversPanel, вкладка «Общий портфель») ──
  // '3y' убран из пресетов (2026-07) — у кого он был персистнут в embed, откатываем на 1Г.
  const [moversPeriod, setMoversPeriod] = useState<MoversPeriod>(() => {
    const saved = rd('frame:embed:fundtrades:period', '1m') as string;
    return saved === '3y' ? '1y' : (saved as MoversPeriod);
  });
  const [selectedMoverFunds, setSelectedMoverFunds] = useState<Set<string>>(new Set());
  const [moversData, setMoversData] = useState<FundTradesMovers | null>(null);
  const [moversStatus, setMoversStatus] = useState<LoadStatus>('idle');

  useEffect(() => { wr('frame:embed:fundtrades:period', moversPeriod); }, [moversPeriod]);

  const fundsParam = useMemo(() => Array.from(selectedMoverFunds).join(','), [selectedMoverFunds]);

  useEffect(() => {
    if (tab !== 'movers') return;
    let cancelled = false;
    setMoversStatus('loading');
    // sort:'amount' — как на сайте (вкладка «Общий портфель»); отдельного
    // тумблера «% веса» больше нет, редизайн #493/#571 его убрал.
    getFundTradesMovers(moversPeriod, { funds: fundsParam || undefined, sort: 'amount' })
      .then((res) => {
        if (cancelled) return;
        setMoversData(res);
        setMoversStatus((res?.top_accumulated?.length || res?.top_reduced?.length) ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/fund-trades movers load failed:', err);
        setMoversStatus('error');
      });
    return () => { cancelled = true; };
  }, [tab, moversPeriod, fundsParam]);

  const moverPickerFunds = useMemo<FundPickerFund[]>(
    () => funds.map((f) => ({ ticker: f.ticker, name: f.name, uk: f.uk, uk_id: f.uk_id })),
    [funds],
  );

  // ── funds tab state ──
  const [fundSort, setFundSort] = useState<'return' | 'volume' | 'name'>(() => rd('frame:embed:fundtrades:fundSort', 'return') as 'return' | 'volume' | 'name');
  const [selectedUks, setSelectedUks] = useState<Set<string>>(new Set());
  useEffect(() => { wr('frame:embed:fundtrades:fundSort', fundSort); }, [fundSort]);

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

  // ── «ещё» (⚙) для movers: период 1М/6М/1Г теперь в шапке самой панели
  // (её onPeriodChange) — как на сайте; в ⚙ остаётся только фильтр фондов. ──
  const moversMore: ReactNode = moverPickerFunds.length > 1 ? (
    <DrawerSection label="Фонды">
      <FundPicker funds={moverPickerFunds} mode="multi" selected={selectedMoverFunds} onChange={setSelectedMoverFunds} />
    </DrawerSection>
  ) : undefined;
  const fundsMore: ReactNode = (
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
  );
  const more: ReactNode =
    tab === 'movers' ? moversMore
    : tab === 'funds' ? fundsMore
    : (
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5 }}>
          {tab === 'snapshots'
            ? 'Выбор фонда, месяца и метрики — кнопками в самом виджете.'
            : 'Выбор бумаги и фондов — кнопками в самом виджете.'}
        </div>
      );

  // Тулбар: заперт на movers → пусто (заголовок и период 1М/6М/1Г уже в
  // шапке самой PortfolioMoversPanel, дублировать их в тулбаре не нужно).
  const toolbar: ReactNode = lockTab === 'movers' ? undefined : (
    <TabBar tab={tab} onChange={setTab} />
  );

  return (
    <EmbedFrame
      toolbar={toolbar}
      more={more}
    >
      <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column' }}>
        <div className="styled-scrollbar" style={{ flex: 1, minHeight: 0, overflow: 'auto', position: 'relative', display: 'flex', flexDirection: 'column', padding: 10 }}>
          {tab === 'movers' && (
            moversStatus === 'ok' || moversStatus === 'empty' ? (
              <PortfolioMoversPanel
                movers={moversData}
                loading={false}
                period={moversPeriod}
                variant="embedded"
                onPeriodChange={setMoversPeriod}
              />
            ) : (
              <div style={{ flex: 1, minHeight: 0, position: 'relative' }}>
                {moversStatus === 'loading' && <EmbedMsg text="Загрузка…" />}
                {moversStatus === 'error' && <EmbedMsg text="Ошибка загрузки" />}
                {moversStatus === 'idle' && <EmbedMsg text="Загрузка…" />}
              </div>
            )
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
    </EmbedFrame>
  );
}

// ─────────────────────────────── snapshots tab ───────────────────────────────

function SnapshotsTab({ funds }: { funds: FundWithHistory[] }) {
  const { rd, wr } = useEmbedPersist();
  const [ticker, setTicker] = useState<string>('EQMX');
  const [metric, setMetric] = useState<'amount' | 'weight'>(() => rd('frame:embed:fundtrades:snapMetric', 'amount') as 'amount' | 'weight');
  const [snapshotsList, setSnapshotsList] = useState<FundSnapshotsList | null>(null);
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [review, setReview] = useState<FundSnapshotReview | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { wr('frame:embed:fundtrades:snapMetric', metric); }, [metric]);

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
        <div className="styled-scrollbar" style={{ display: 'flex', gap: 6, overflowX: 'auto', paddingBottom: 4, flexShrink: 0 }}>
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
      <div className="styled-scrollbar" style={{ flex: 1, minHeight: 0, overflow: 'auto', position: 'relative' }}>
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
        {status === 'empty' && <EmbedMsg text={`У ${ticker} пока нет снапшотов`} />}

        {status === 'ok' && review && review.totals && (
          <div>
            {/* Заголовок */}
            <div style={{ marginBottom: 12 }}>
              <div style={{ fontSize: 14, fontWeight: 800, color: 'var(--text-primary)', lineHeight: 1.25 }}>{review.fund.name}</div>
              <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginTop: 2 }}>
                {formatMonthYear(review.current_snapshot_date)}
                {review.previous_snapshot_date && <> · с {formatMonthYear(review.previous_snapshot_date)}</>}
                {' · '}{review.totals!.current_assets} активов
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
                    <span style={{ fontSize: 12, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{fundAssetName(h.asset_name, h.isin)}</span>
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
                total={isW ? sumBy(addedItems, wDelta) : review.totals!.total_added_rub}
                isPositive
                formatValue={fmtVal}
              >
                {sortByAbs(addedItems, isW ? wDelta : aAdded).slice(0, 8).map((r) => (
                  <EmbedBar
                    key={`${r.asset_name}-${r.isin || ''}`}
                    label={fundAssetName(r.asset_name, r.isin)}
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
                total={isW ? Math.abs(sumBy(reducedItems, wDelta)) : Math.abs(review.totals!.total_reduced_rub)}
                isPositive={false}
                formatValue={fmtVal}
              >
                {sortByAbs(reducedItems, isW ? wDelta : aAdded).slice(0, 8).map((r) => (
                  <EmbedBar
                    key={`${r.asset_name}-${r.isin || ''}`}
                    label={fundAssetName(r.asset_name, r.isin)}
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
                total={isW ? sumBy(review.new, wNew) : review.totals!.total_new_rub}
                isPositive
                formatValue={fmtVal}
              >
                {sortByAbs(review.new, isW ? wNew : aNew).slice(0, 8).map((r) => (
                  <EmbedBar
                    key={`${r.asset_name}-${r.isin || ''}`}
                    label={fundAssetName(r.asset_name, r.isin)}
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
                total={isW ? Math.abs(sumBy(review.sold_out, wSold)) : review.totals!.total_sold_out_rub}
                isPositive={false}
                formatValue={fmtVal}
              >
                {sortByAbs(review.sold_out, isW ? wSold : aSold).slice(0, 8).map((r) => (
                  <EmbedBar
                    key={`${r.asset_name}-${r.isin || ''}`}
                    label={fundAssetName(r.asset_name, r.isin)}
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
        {status === 'ok' && review && !review.totals && (
          <EmbedMsg text="Свежий срез — по подписке · таймфрейм.рф" />
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
  // Донат подсвечивает только те 5 бумаг, что показаны в списке; остальное — серый сектор «Прочее».
  const listed = top.slice(0, 5);
  const sum = listed.reduce((s, h) => s + (h.weight || 0), 0);
  const other = 100 - sum;
  const donutHoldings = other > 1 ? [...listed, { name: 'Прочее', isin: null, weight: other }] : listed;
  const donutColors = donutHoldings.map((h, i) =>
    h.name === 'Прочее' ? 'var(--text-muted)' : (fundAssetColor(h.name, h.isin) ?? DONUT_COLORS[i % DONUT_COLORS.length]));
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
              <span style={{ width: 7, height: 7, borderRadius: '50%', flexShrink: 0, backgroundColor: fundAssetColor(h.name, h.isin) ?? DONUT_COLORS[i % DONUT_COLORS.length] }} />
              <span style={{ flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-secondary)', fontWeight: 600 }}>
                {fundAssetName(h.name, h.isin)}
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
