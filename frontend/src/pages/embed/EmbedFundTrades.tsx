/**
 * EmbedFundTrades — «Сделки фондов» для панели терминала (песочница и окно
 * расширения). Вкладки и их наполнение — 1-в-1 с сайтовой FundTradesPage:
 *
 *   - portfolio «Общий портфель» — движения фондов (PortfolioMoversPanel) +
 *      состав портфеля (CombinedPortfolioView). На сайте это два столбца, в
 *      окне ширины нет → колонкой с общим скроллом. Набор фондов один на оба
 *      блока, пикер живёт в тулбаре (на сайте — рядом над карточкой).
 *   - company  «По бумаге» — CompanyFlowsTab в панельном режиме: его контролы
 *      уезжают ПОРТАЛОМ в тулбар окна (см. controlsTarget).
 *   - funds    «Витрина» — карточки фондов; клик открывает FundDetailModal.
 *
 * Вкладки «Сделки» и «Снапшот» убраны вслед за сайтом (там их скрыли): их id
 * остаются в типе только чтобы старые сохранённые панели открывались на
 * «Общем портфеле», а не падали.
 *
 * Активная вкладка и настройки персистятся в namespace frame:embed:fundtrades:*.
 * Всё инлайн-стилями с CSS-var, чтобы работать в любой теме внутри iframe.
 */
import { useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import {
  listFundsWithHistory,
  getFundTradesMovers,
  getFundTradesDetail,
  getFundPortfolio,
  type FundPortfolio,
  type FundTradesMovers,
  type FundWithHistory,
} from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, SegGroup } from './EmbedSettings';
import { Dropdown, EmbedFrame } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import CompanyFlowsTab from '../../components/fundtrades/CompanyFlowsTab';
import PortfolioMoversPanel, { type MoversPeriod } from '../../components/fundtrades/PortfolioMoversPanel';
import PortfolioFundPicker, { defaultPortfolioTickers } from '../../components/fundtrades/PortfolioFundPicker';
import CombinedPortfolioView from '../../components/fundtrades/CombinedPortfolioView';
import Donut from '../../components/funds/Donut';
import FundDetailModal, {
  formatRubShort,
  formatReturnPct,
  returnColor,
} from '../../components/funds/FundDetailModal';
import { DONUT_COLORS, fundAssetName, fundAssetColor, resolveFundLogo, stripUkName } from '../../config/fundConfig';

type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
// Состав и порядок вкладок 1-в-1 с сайтом (FundTradesPage.ChartTabs): «Сделки»
// и «Снапшот» оттуда убраны, «Состав» называется «Витрина», «Потоки» — «По
// бумаге». `movers`/`snapshots` остаются как ID для уже сохранённых панелей
// и роута /embed/fund-movers — они мапятся на «Общий портфель».
type EmbedTab = 'portfolio' | 'company' | 'funds' | 'movers' | 'snapshots';


const TABS: { id: EmbedTab; label: string }[] = [
  { id: 'portfolio', label: 'Общий портфель' },
  { id: 'company', label: 'По бумаге' },
  { id: 'funds', label: 'Витрина' },
];

// ─────────────────────────────── helpers ───────────────────────────────


// ─────────────────────────────── shared bits ───────────────────────────────


// ─────────────────────────────── root ───────────────────────────────

export default function EmbedFundTrades({ lockTab }: { lockTab?: EmbedTab } = {}) {
  const { rd, wr } = useEmbedPersist();
  // lockTab — отдельный индикатор «Сделки фондов»: фиксируем вкладку movers,
  // таб-бар прячем, а название и первичные контролы (период/метрика) — в тулбар.
  const [tab, setTabRaw] = useState<EmbedTab>(() => {
    const v = lockTab ?? rd('frame:embed:fundtrades:tab', 'portfolio');
    // Старые сохранения (и lockTab='movers' у /embed/fund-movers) → «Общий
    // портфель»: движения фондов теперь живут там, как на сайте.
    if (v === 'movers' || v === 'snapshots') return 'portfolio';
    return TABS.some((t) => t.id === v) ? (v as EmbedTab) : 'portfolio';
  });
  const setTab = setTabRaw;
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

  // Реалтайм: ингест раскрытий фондов (SSE 'funds'/'daily') → тихий рефетч
  // движителей. Вкладки снапшотов/состава грузятся по действию пользователя.
  const [refreshTick, setRefreshTick] = useState(0);
  const silentRef = useRef(false);
  useRealtimeData(['funds', 'daily'], () => { silentRef.current = true; setRefreshTick((t) => t + 1); });

  useEffect(() => {
    if (tab !== 'portfolio') return;
    let cancelled = false;
    if (!silentRef.current) setMoversStatus('loading');
    silentRef.current = false;
    // sort:'amount' — как на сайте (вкладка «Общий портфель»); отдельного
    // тумблера «% веса» больше нет, редизайн #493/#571 его убрал.
    // scope:'portfolio' — как на сайте на этой вкладке (своя, нулевая задержка
    // в тарифной матрице), иначе Free видел бы другой срез, чем на сайте.
    getFundTradesMovers(moversPeriod, { funds: fundsParam || undefined, sort: 'amount', scope: 'portfolio' })
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
  }, [tab, moversPeriod, fundsParam, refreshTick]);

  // ── funds tab state ──
  const [fundSort, setFundSort] = useState<'return' | 'volume' | 'name'>(() => rd('frame:embed:fundtrades:fundSort', 'return') as 'return' | 'volume' | 'name');
  useEffect(() => { wr('frame:embed:fundtrades:fundSort', fundSort); }, [fundSort]);

  const sortedFunds = useMemo(() => {
    const cmp = (a: FundWithHistory, b: FundWithHistory): number => {
      if (fundSort === 'name') return a.ticker.localeCompare(b.ticker);
      const av = fundSort === 'return' ? (a.returns?.y1 ?? null) : a.nav_rub;
      const bv = fundSort === 'return' ? (b.returns?.y1 ?? null) : b.nav_rub;
      if (av == null && bv == null) return a.ticker.localeCompare(b.ticker);
      if (av == null) return 1;
      if (bv == null) return -1;
      return bv - av;
    };
    return [...funds].sort(cmp);
  }, [funds, fundSort]);

  // ── состав портфеля (правая половина сайтовой вкладки «Общий портфель») ──
  const [portfolio, setPortfolio] = useState<FundPortfolio | null>(null);
  const [portfolioLoading, setPortfolioLoading] = useState(false);
  const [portfolioMode, setPortfolioMode] = useState<'rub' | 'share'>(
    () => (rd('frame:embed:fundtrades:pMode', 'rub') as 'rub' | 'share'));
  const [portfolioAsOf, setPortfolioAsOf] = useState<string | undefined>(
    () => rd('frame:embed:fundtrades:pAsOf', '') || undefined);
  useEffect(() => { wr('frame:embed:fundtrades:pMode', portfolioMode); }, [portfolioMode]);
  useEffect(() => { wr('frame:embed:fundtrades:pAsOf', portfolioAsOf ?? ''); }, [portfolioAsOf]);

  useEffect(() => {
    if (tab !== 'portfolio' || funds.length === 0) return;
    let cancelled = false;
    setPortfolioLoading(true);
    getFundPortfolio({ funds: fundsParam || undefined, as_of: portfolioAsOf })
      .then((r) => { if (!cancelled) setPortfolio(r); })
      .catch((err) => { if (!cancelled) console.error('embed/fund-trades portfolio load failed:', err); })
      .finally(() => { if (!cancelled) setPortfolioLoading(false); });
    return () => { cancelled = true; };
  }, [tab, fundsParam, portfolioAsOf, funds.length, refreshTick]);

  // Дефолт набора фондов — как на сайте: всё, кроме индексных (их сделки это
  // ребалансировка вслед за индексом, в консенсусе они забивают управляющих).
  const defaultsApplied = useRef(false);
  useEffect(() => {
    if (funds.length === 0 || defaultsApplied.current) return;
    defaultsApplied.current = true;
    if (selectedMoverFunds.size === 0) setSelectedMoverFunds(new Set(defaultPortfolioTickers(funds)));
  }, [funds, selectedMoverFunds.size]);

  // Узел-слот тулбара для контролов вкладки «Потоки» (портал из CompanyFlowsTab).
  // Через useState, а не ref: портал должен перерисоваться, когда узел появится.
  const [companySlot, setCompanySlot] = useState<HTMLDivElement | null>(null);
  // Бумага, выбранная кликом в «Общем портфеле» → открыть её во вкладке «По бумаге».
  const [companyPreset, setCompanyPreset] = useState<{ asset_name: string; isin: string | null } | null>(null);

  const fundsMore: ReactNode = (
    <>
      <DrawerSection label="Сортировка">
        <SegGroup<'return' | 'volume' | 'name'>
          value={fundSort}
          options={[{ id: 'return', label: 'Доходность' }, { id: 'volume', label: 'Объём' }, { id: 'name', label: 'Имя' }]}
          onChange={setFundSort}
        />
      </DrawerSection>
    </>
  );
  // ⚙ рисуем ТОЛЬКО когда за ней есть настройки. У «Потоков» и «Снапшота» все
  // контролы вынесены в тулбар, и шестерёнка показывала одну лишь подсказку —
  // кнопка-пустышка (фидбек Вадима: «если не нужна — убираем»).
  const more: ReactNode | undefined = tab === 'funds' ? fundsMore : undefined;

  // Тулбар: заперт на movers → пусто (заголовок и период 1М/6М/1Г уже в
  // шапке самой PortfolioMoversPanel, дублировать их в тулбаре не нужно).
  // Рядом с табами — слот, в который вкладка «Потоки» порталом кладёт СВОИ
  // контролы (бумага/режим/период/фонды): состояние живёт в CompanyFlowsTab,
  // поэтому поднять их пропами нельзя, а второй реализации быть не должно.
  // Вкладка — Dropdown в LEAD, то есть ЗА пределами прокручиваемой полосы
  // контролов. Раньше это был ряд пилюль внутри неё: у «Потоков» свои четыре
  // контрола, полоса уходила в скролл и таб-бар уезжал из виду — переключить
  // вкладку становилось нечем (проверено вживую на панели 660px).
  const lead: ReactNode = lockTab === 'movers' ? undefined : (
    <Dropdown<EmbedTab>
      value={tab}
      options={TABS.map((t) => ({ id: t.id, label: t.label }))}
      onChange={setTab}
      title="Вкладка"
    />
  );
  const toolbar: ReactNode = (
    <>
      {tab === 'portfolio' && funds.length > 1 && (
        <PortfolioFundPicker
          funds={funds}
          selected={selectedMoverFunds}
          onChange={setSelectedMoverFunds}
          compact
          resetWhenLocked
        />
      )}
      {/* Слот вкладки «По бумаге»: её контролы приезжают порталом. */}
      <div ref={setCompanySlot} style={{ display: 'contents' }} />
    </>
  );

  return (
    <EmbedFrame
      lead={lead}
      toolbar={toolbar}
      more={more}
    >
      <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column' }}>
        {/* «Потоки» — чарт-вкладка: edge-to-edge и без прокрутки, как у остальных
            графических панелей (её контролы уехали в тулбар). Остальные вкладки —
            списки/карточки: им нужны отступы и скролл. */}
        <div
          className="styled-scrollbar"
          style={tab === 'company'
            ? { flex: 1, minHeight: 0, overflow: 'hidden', position: 'relative', display: 'flex', flexDirection: 'column' }
            : { flex: 1, minHeight: 0, overflow: 'auto', position: 'relative', display: 'flex', flexDirection: 'column', padding: 10 }}
        >
          {/* «Общий портфель» = сайтовая вкладка целиком: сверху движения фондов
              (что докупили / из чего вышли), снизу — состав портфеля. На сайте
              они стоят в два столбца; в окне терминала места по ширине нет,
              поэтому колонкой, с общим скроллом. Набор фондов — один на оба
              блока (пикер в тулбаре), как и на сайте. */}
          {tab === 'portfolio' && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
              {moversStatus === 'ok' || moversStatus === 'empty' ? (
                <PortfolioMoversPanel
                  movers={moversData}
                  loading={false}
                  period={moversPeriod}
                  variant="embedded"
                  onPeriodChange={setMoversPeriod}
                  onAssetClick={(m) => { setCompanyPreset({ asset_name: m.asset_name, isin: null }); setTab('company'); }}
                />
              ) : (
                <div style={{ minHeight: 120, position: 'relative' }}>
                  {moversStatus === 'error' ? <EmbedMsg text="Ошибка загрузки" /> : <EmbedMsg text="Загрузка…" />}
                </div>
              )}
              <div style={{ borderTop: '1.5px solid var(--border-color, rgba(128,128,128,0.25))', paddingTop: 12 }}>
                <CombinedPortfolioView
                  portfolio={portfolio}
                  loading={portfolioLoading}
                  mode={portfolioMode}
                  onModeChange={setPortfolioMode}
                  variant="embedded"
                  availableMonths={portfolio?.available_months}
                  asOf={portfolioAsOf}
                  onAsOfChange={setPortfolioAsOf}
                  onAssetClick={(h) => { setCompanyPreset({ asset_name: h.asset_name, isin: h.isin }); setTab('company'); }}
                />
              </div>
            </div>
          )}

          {tab === 'funds' && <FundsTab funds={sortedFunds} hasFunds={funds.length > 0} />}

          {tab === 'company' && (
            <div style={{ flex: 1, minHeight: 0 }}>
              {/* presetAsset — клик по бумаге в «Общем портфеле» открывает её
                  здесь, как переход между вкладками на сайте. */}
              <CompanyFlowsTab
                embedded
                controlsTarget={companySlot}
                presetAsset={companyPreset}
                onPresetConsumed={() => setCompanyPreset(null)}
              />
            </div>
          )}
        </div>
      </div>
    </EmbedFrame>
  );
}

// ─────────────────────────────── funds tab ───────────────────────────────

function FundsTab({ funds, hasFunds }: { funds: FundWithHistory[]; hasFunds: boolean }) {
  // Карточка фонда кликабельна, как на сайте: открывается FundDetailModal
  // (СЧА, цена пая, состав с пончиком, изменения) — в панели этого не хватало.
  const [ticker, setTicker] = useState<string | null>(null);
  const selected = ticker ? funds.find((f) => f.ticker === ticker) ?? null : null;

  if (!hasFunds) {
    return <EmbedMsg text="Загрузка…" />;
  }
  if (funds.length === 0) {
    return <EmbedMsg text="Фонды не найдены" />;
  }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {funds.map((f) => (
        <FundCard key={f.fund_id} fund={f} onOpen={() => setTicker(f.ticker)} />
      ))}
      {ticker && (
        <FundDetailModal
          ticker={ticker}
          loadDetail={(period, range) => getFundTradesDetail(ticker, period, range ? { from: range.from, to: range.to } : {})}
          navRub={selected?.nav_rub}
          returns={selected?.returns}
          hasDistributions={selected?.has_distributions}
          enableDrilldown
          onClose={() => setTicker(null)}
        />
      )}
    </div>
  );
}

function FundCard({ fund: f, onOpen }: { fund: FundWithHistory; onOpen: () => void }) {
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
      role="button"
      tabIndex={0}
      onClick={onOpen}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpen(); } }}
      title="Открыть карточку фонда"
      style={{
        padding: 12,
        background: 'var(--bg-secondary, transparent)',
        border: '1.5px solid var(--border-color, rgba(128,128,128,0.3))',
        borderRadius: 10,
        display: 'flex',
        flexDirection: 'column',
        gap: 10,
        cursor: 'pointer',
        textAlign: 'left',
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
