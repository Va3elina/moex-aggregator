/**
 * MobileFundTradesPage — мобильная версия «Что покупают фонды» (desktop
 * FundTradesPage). Реплицирует ВСЕ 4 режима десктопа 1:1, но контролы вынесены
 * не в табы наверху, а в action-rail ПОД контентом (MobileLayout):
 *
 *   ⭐ Актив      — выбор фонда (funds/snapshots) или бумаги (company).
 *   🕐 Время      — период доходности (funds) / месяц снапшота movers (asOf) /
 *                   таймлайн снапшотов (snapshots).
 *   ⚙️ Опции      — переключатель РЕЖИМОВ (Состав/Движения/Снапшот/Потоки) +
 *                   метрика (вес%/объём₽) + сортировка фондов + выбор УК +
 *                   мультивыбор фондов movers. Всё, что не время и не актив.
 *
 * Открыт для всех тиров (флаг useCommonFeatures().fund_trades_access; LockedView-
 * ветка оставлена на случай ре-гейтинга).
 *
 * Переиспользует весь data-слой (services/api), FundDetailModal, Donut,
 * FundPicker, UkMultiSelect, CompanyFlowsTab, usePersistedState с ТЕМИ ЖЕ
 * ключами frame:fundtrades:* (синхрон с десктопом), useGrowReveal, onboarding.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  Wallet,
  Lock,
  Sparkles,
  TrendingUp,
  TrendingDown,
  Activity,
  ArrowLeftRight,
  Briefcase,
} from 'lucide-react';
import {
  listFundsWithHistory,
  getFundTradesDetail,
  getFundTradesMovers,
  getFundSnapshots,
  getFundSnapshotReview,
  getFundPortfolio,
  type FundTradesPeriod,
  type FundPortfolio,
  type FundWithHistory,
  type FundTradesMovers,
  type FundSnapshotsList,
  type FundSnapshotReview,
  type FundDiffRow,
} from '../../services/api';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useAuth } from '../../contexts/AuthContext';
import { useUpgradePrompt } from '../../components/tier/UpgradeModal';
import { usePersistedState, usePersistedSet } from '../../hooks/usePersistedState';
import { useGrowReveal } from '../../hooks/useGrowReveal';
import { UK_LOGOS, DONUT_COLORS, fundAssetName, fundAssetColor, resolveFundLogo, stripUkName } from '../../config/fundConfig';
import Donut from '../../components/funds/Donut';
import FundDetailModal, {
  AssetHistoryModal,
  formatRubShort,
  formatReturnPct,
  returnColor,
  formatShares,
} from '../../components/funds/FundDetailModal';
import FundPicker, { type FundPickerFund } from '../../components/fundtrades/FundPicker';
import UkMultiSelect, { type UkOption } from '../../components/fundtrades/UkMultiSelect';
import CombinedPortfolioView from '../../components/fundtrades/CombinedPortfolioView';
import CompanyFlowsTab from '../../components/fundtrades/CompanyFlowsTab';
import DelayedDataBadge from '../../components/fundtrades/DelayedDataBadge';
import LockedSnapshotTeaser from '../../components/fundtrades/LockedSnapshotTeaser';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileSheet from '../../components/mobile/MobileSheet';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour, { type TourStep } from '../../components/onboarding/OnboardingTour';

type Tab = 'funds' | 'portfolio' | 'movers' | 'snapshots' | 'company';

const TAB_LABEL: Record<Tab, string> = {
  funds: 'Состав фондов',
  portfolio: 'Общий портфель',
  movers: 'Движения',
  snapshots: 'Снапшот',
  company: 'Потоки по компании',
};

const CATEGORY_LABEL: Record<string, string> = {
  stocks: 'Акции',
  Авторские: 'Авторские',
  bonds: 'Облигации',
  money_market: 'Денежный рынок',
  gold: 'Золото',
};

// ── helpers (зеркало десктопа) ───────────────────────────────────────────────
type FundSortKey = 'return' | 'volume' | 'name';
type ReturnPeriodKey = 'm1' | 'm3' | 'm6' | 'y1';
const RETURN_PERIOD_LABEL: Record<ReturnPeriodKey, string> = {
  m1: '1 мес',
  m3: '3 мес',
  m6: '6 мес',
  y1: '1 год',
};

function returnForPeriod(
  r: { m1: number | null; m3: number | null; m6: number | null; y1: number | null } | null | undefined,
  period: ReturnPeriodKey,
): number | null {
  if (!r) return null;
  return r[period] ?? null;
}

function bestReturn(
  r?: { m1: number | null; m3: number | null; m6: number | null; y1: number | null } | null,
): { v: number; period: string } | null {
  if (!r) return null;
  if (r.y1 != null) return { v: r.y1, period: '1 год' };
  if (r.m6 != null) return { v: r.m6, period: '6 мес' };
  if (r.m3 != null) return { v: r.m3, period: '3 мес' };
  if (r.m1 != null) return { v: r.m1, period: '1 мес' };
  return null;
}

function displayReturn(
  r: { m1: number | null; m3: number | null; m6: number | null; y1: number | null } | null | undefined,
  period: ReturnPeriodKey,
): { v: number; period: string } | null {
  const v = returnForPeriod(r, period);
  if (v != null) return { v, period: RETURN_PERIOD_LABEL[period] };
  return bestReturn(r);
}

function ukKey(f: { uk_id?: number | string | null; uk?: string | null }): string {
  if (f.uk_id != null && f.uk_id !== '') return String(f.uk_id);
  return f.uk || '';
}

function isIsin(s: string | null | undefined): s is string {
  return !!s && /^[A-Z]{2}[A-Z0-9]{10}$/.test(s);
}

function formatMonthYear(iso: string): string {
  const d = new Date(iso);
  const months = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
    'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
  return `${months[d.getMonth()]} ${d.getFullYear()}`;
}

// Заголовок секции опций (uppercase, muted).
const SHEET_SECTION_LABEL = {
  fontSize: 'var(--fs-xs)',
  fontWeight: 800,
  color: 'var(--text-secondary)',
  textTransform: 'uppercase' as const,
  letterSpacing: '0.06em',
  marginBottom: 8,
};

// ════════════════════════════════════════════════════════════════════
// Lock screen (Pro-only) — мобильная версия десктопного LockedView.
// ════════════════════════════════════════════════════════════════════
function LockedView() {
  const { isAuthenticated } = useAuth();
  const { showUpgrade } = useUpgradePrompt();
  return (
    // enableFullscreen=false: на lock-скрине нет графика — поворот телефона
    // не должен уводить в пустой fullscreen (авто-вход гейтится этим пропом).
    <MobileLayout enableFullscreen={false}>
      <div style={{ padding: 16 }}>
        <div
          style={{
            padding: 24,
            textAlign: 'center',
            background: 'var(--bg-secondary)',
            border: '2px solid color-mix(in srgb, var(--accent) 30%, transparent)',
            borderRadius: 16,
          }}
        >
          <div
            style={{
              width: 56,
              height: 56,
              margin: '0 auto 16px',
              borderRadius: 14,
              background: 'color-mix(in srgb, var(--accent) 18%, transparent)',
              color: 'var(--accent)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <Lock size={26} strokeWidth={2.2} />
          </div>
          <h1
            style={{
              fontSize: 'var(--fs-xl)',
              fontWeight: 800,
              color: 'var(--text-primary)',
              marginBottom: 8,
              letterSpacing: '-0.01em',
            }}
          >
            Что покупают фонды
          </h1>
          <p
            style={{
              fontSize: 'var(--fs-sm)',
              color: 'var(--text-secondary)',
              lineHeight: 1.55,
              margin: '0 auto 16px',
            }}
          >
            Отслеживайте куда направляются деньги крупных фондов акций: какие
            акции управляющие компании накапливают, а что распродают.
          </p>
          <ul
            style={{
              listStyle: 'none',
              padding: 0,
              margin: '0 auto 20px',
              maxWidth: 360,
              textAlign: 'left',
            }}
          >
            {[
              'Изменения портфеля по каждому БПИФ за период',
              'Топ-аккумуляция / распродажа across всех фондов',
              'История портфельных движений по месяцам',
            ].map((t) => (
              <li
                key={t}
                style={{
                  display: 'flex',
                  gap: 10,
                  padding: '7px 0',
                  fontSize: 'var(--fs-sm)',
                  color: 'var(--text-secondary)',
                }}
              >
                <span style={{ color: 'var(--accent)' }}>✓</span>
                <span>{t}</span>
              </li>
            ))}
          </ul>
          {isAuthenticated ? (
            <button
              onClick={() =>
                showUpgrade({
                  tier: 'pro',
                  featureName: 'Что покупают фонды',
                  indicator: 'fund_trades',
                })
              }
              className="editorial-press"
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 6,
                padding: '12px 24px',
                background: 'var(--accent)',
                color: 'var(--text-inverse)',
                border: '1.5px solid var(--text-primary)',
                borderRadius: 999,
                fontSize: 'var(--fs-sm)',
                fontWeight: 700,
                cursor: 'pointer',
                boxShadow: 'var(--shadow-hard-chip)',
              }}
            >
              <Sparkles size={14} />
              Перейти на Pro
            </button>
          ) : (
            <Link
              to="/login"
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 6,
                padding: '12px 24px',
                background: 'var(--accent)',
                color: 'var(--text-inverse)',
                textDecoration: 'none',
                border: '1.5px solid var(--text-primary)',
                borderRadius: 999,
                fontSize: 'var(--fs-sm)',
                fontWeight: 700,
              }}
            >
              Войти
            </Link>
          )}
        </div>
      </div>
    </MobileLayout>
  );
}

// ════════════════════════════════════════════════════════════════════
// Main page
// ════════════════════════════════════════════════════════════════════
export default function MobileFundTradesPage() {
  const common = useCommonFeatures();
  const { showUpgrade } = useUpgradePrompt(); // пейволл на locked-месяце movers (Free/гость)

  // ── State (ключи усиленно совпадают с десктопом для синхрона настроек) ──
  const [tab, setTab] = usePersistedState<Tab>('frame:fundtrades:tab', 'funds');
  const [period] = useState<FundTradesPeriod>('1m'); // шаг данных — месяц vs предыдущий
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null);
  // Состав фондов: период доходности + сортировка + мультиселект УК.
  const [returnPeriod, setReturnPeriod] = usePersistedState<ReturnPeriodKey>('frame:fundtrades:returnPeriod', 'y1');
  const [fundSort, setFundSort] = usePersistedState<FundSortKey>('frame:fundtrades:fundSort', 'return');
  const [selectedUks, setSelectedUks] = useState<Set<string>>(new Set());
  // Hover-связь пончик↔список на плитке (по fund_id + индекс слайса).
  const [tileHover, setTileHover] = useState<{ fund: number; idx: number } | null>(null);

  const [funds, setFunds] = useState<FundWithHistory[]>([]);
  const [movers, setMovers] = useState<FundTradesMovers | null>(null);
  // Раздельные спиннеры: funds-список и movers-агрегаты грузятся независимыми
  // эффектами — общий loading давал ложный спиннер/гашение чужого запроса.
  const [loadingFunds, setLoadingFunds] = useState(false);
  const [loadingMovers, setLoadingMovers] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // reqId stale-guard на каждый загрузчик: out-of-order-ответ не пишет state.
  const fundsReqRef = useRef(0);
  const moversReqRef = useRef(0);

  // Движения: месяц снапшота + конкретные фонды + метрика.
  const [asOf, setAsOf] = useState<string | undefined>(undefined);
  const [selectedMoverFunds, setSelectedMoverFunds] = useState<Set<string>>(new Set());
  const [metric, setMetric] = usePersistedState<'weight' | 'amount'>('frame:fundtrades:metric', 'weight');
  // cross-tab: предвыбор бумаги для перехода movers → Потоки по компании.
  const [companyPreset, setCompanyPreset] = useState<{ asset_name: string; isin: string | null } | null>(null);

  // Общий портфель — агрегированный состав выбранных фондов акций как один портфель.
  const [portfolioUks, setPortfolioUks] = usePersistedSet<string>('frame:fundtrades:portfolioUks');
  const [portfolioMode, setPortfolioMode] = usePersistedState<'rub' | 'share'>('frame:fundtrades:portfolioMode', 'rub');
  const [portfolioPeriod, setPortfolioPeriod] = usePersistedState<ReturnPeriodKey>('frame:fundtrades:portfolioPeriod', 'y1');
  const [portfolio, setPortfolio] = useState<FundPortfolio | null>(null);
  const [loadingPortfolio, setLoadingPortfolio] = useState(false);
  const portfolioReqRef = useRef(0);

  // ── Sheets ──
  const [assetSheetOpen, setAssetSheetOpen] = useState(false);
  const [timeSheetOpen, setTimeSheetOpen] = useState(false);
  const [optionsSheetOpen, setOptionsSheetOpen] = useState(false);

  // ── Load funds list (один раз, при Pro-доступе) ──
  useEffect(() => {
    if (!common.fund_trades_access) return;
    const reqId = ++fundsReqRef.current;
    const isStale = () => reqId !== fundsReqRef.current;
    setLoadingFunds(true);
    listFundsWithHistory()
      .then((r) => { if (isStale()) return; setFunds(r.funds); })
      .catch((e: Error) => { if (isStale()) return; setError(e.message); })
      .finally(() => { if (!isStale()) setLoadingFunds(false); });
  }, [common.fund_trades_access]);

  // ── Load movers (tab=movers, либо смена месяца/фондов/метрики) ──
  const fundsParam = useMemo(
    () => Array.from(selectedMoverFunds).join(','),
    [selectedMoverFunds],
  );
  useEffect(() => {
    if (!common.fund_trades_access) return;
    if (tab !== 'movers') return;
    const reqId = ++moversReqRef.current;
    const isStale = () => reqId !== moversReqRef.current;
    setLoadingMovers(true);
    getFundTradesMovers(period, { asOf, funds: fundsParam || undefined, sort: metric })
      .then((m) => { if (isStale()) return; setMovers(m); })
      .catch((e: Error) => { if (isStale()) return; setError(e.message); })
      .finally(() => { if (!isStale()) setLoadingMovers(false); });
  }, [tab, period, asOf, fundsParam, metric, common.fund_trades_access]);

  // Общий портфель: выбранные УК → тикеры фондов (пусто = все whitelist-акции).
  const portfolioFundsParam = useMemo(
    () => (portfolioUks.size === 0
      ? ''
      : funds.filter((f) => portfolioUks.has(ukKey(f))).map((f) => f.ticker).join(',')),
    [funds, portfolioUks],
  );
  useEffect(() => {
    if (!common.fund_trades_access) return;
    if (tab !== 'portfolio') return;
    if (funds.length === 0) return;
    const reqId = ++portfolioReqRef.current;
    const isStale = () => reqId !== portfolioReqRef.current;
    setLoadingPortfolio(true);
    getFundPortfolio({ funds: portfolioFundsParam || undefined })
      .then((p) => { if (isStale()) return; setPortfolio(p); })
      .catch((e: Error) => { if (isStale()) return; setError(e.message); })
      .finally(() => { if (!isStale()) setLoadingPortfolio(false); });
  }, [tab, portfolioFundsParam, funds.length, common.fund_trades_access]);

  // Уникальные УК для UkMultiSelect (Состав фондов).
  const ukOptions = useMemo<UkOption[]>(() => {
    const map = new Map<string, UkOption>();
    for (const f of funds) {
      const key = ukKey(f);
      if (!key || map.has(key)) continue;
      map.set(key, {
        key,
        name: UK_LOGOS[key]?.name || f.uk || key,
        uk_id: f.uk_id ?? key,
      });
    }
    return Array.from(map.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [funds]);

  // Все фонды для FundPicker (movers multi + актив-sheet single).
  const moverPickerFunds = useMemo<FundPickerFund[]>(
    () => funds.map((f) => ({ ticker: f.ticker, name: f.name, uk: f.uk, uk_id: f.uk_id })),
    [funds],
  );
  // Выбранный фонд для актив-sheet (FundPicker single). Хук — до early return.
  const assetSheetSelected = useMemo(
    () => new Set(selectedTicker ? [selectedTicker] : []),
    [selectedTicker],
  );

  // Группировка + сортировка карточек (Состав фондов).
  const fundsByCategory = useMemo(() => {
    const filtered = selectedUks.size > 0
      ? funds.filter((f) => selectedUks.has(ukKey(f)))
      : funds;
    const groups: Record<string, FundWithHistory[]> = {};
    for (const f of filtered) {
      const key = f.subcategory === 'Авторские' ? 'Авторские' : (f.category || 'other');
      if (!groups[key]) groups[key] = [];
      groups[key].push(f);
    }
    const cmp = (a: FundWithHistory, b: FundWithHistory): number => {
      if (fundSort === 'name') return a.ticker.localeCompare(b.ticker);
      const av = fundSort === 'return' ? returnForPeriod(a.returns, returnPeriod) : a.nav_rub;
      const bv = fundSort === 'return' ? returnForPeriod(b.returns, returnPeriod) : b.nav_rub;
      if (av === null && bv === null) return a.ticker.localeCompare(b.ticker);
      if (av === null) return 1;
      if (bv === null) return -1;
      return bv - av;
    };
    for (const k of Object.keys(groups)) groups[k] = [...groups[k]].sort(cmp);
    return groups;
  }, [funds, fundSort, returnPeriod, selectedUks]);

  // movers: клик по активу → Потоки по компании с предвыбором.
  const openCompanyFlows = (m: FundTradesMovers['top_accumulated'][number]) => {
    const isin = isIsin(m.akey) ? m.akey : null;
    setCompanyPreset({ asset_name: m.asset_name, isin });
    setTab('company');
    setAssetSheetOpen(false);
    setTimeSheetOpen(false);
    setOptionsSheetOpen(false);
  };

  // ── Onboarding tour (shared key с десктопом fund-trades) ──
  const tour = useOnboardingTour('fund-trades');
  const tourSteps: TourStep[] = [
    {
      selector: null,
      title: 'Что покупают фонды',
      body: (
        <>
          <p style={{ marginBottom: 8 }}>
            Состав портфелей крупных БПИФ акций: что управляющие компании
            накапливают, а что распродают. Данные — из официальных Справок о
            СЧА (форма ЦБ № 0420502).
          </p>
          <p>Разберём управление и режимы.</p>
        </>
      ),
      onEnter: () => { setAssetSheetOpen(false); setTimeSheetOpen(false); setOptionsSheetOpen(false); },
    },
    {
      selector: '.fm-page-actions',
      title: 'Кнопки управления',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>Снизу — 2 кнопки:</p>
          <p style={{ marginBottom: 4 }}>
            <strong>Время</strong> — период доходности, месяц снапшота
          </p>
          <p>
            <strong>Опции</strong> — режим (Состав / Движения / Снапшот /
            Потоки), метрика, сортировка, выбор УК. Карточка фонда —
            тап по плитке в списке.
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
    },
    {
      selector: null,
      title: '4 режима',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 4 }}>
            Открыл кнопку <strong>«Опции»</strong>. Вверху — переключатель режимов:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Состав фондов</strong> — пончик портфеля каждого БПИФ
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Движения</strong> — что докупили/продали across фондов
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Снапшот</strong> — детальный обзор изменений одного фонда
          </p>
          <p>
            <strong>Потоки по компании</strong> — притоки/оттоки по одной бумаге
          </p>
        </>
      ),
      onEnter: () => { setOptionsSheetOpen(true); setTimeSheetOpen(false); setAssetSheetOpen(false); },
    },
    {
      selector: null,
      title: 'Готово!',
      body: (
        <p>
          Нажми <strong>?</strong> рядом с заголовком — методология и
          источники данных по фондам.
        </p>
      ),
      onEnter: () => { setOptionsSheetOpen(false); setTimeSheetOpen(false); setAssetSheetOpen(false); },
    },
  ];

  if (!common.fund_trades_access) {
    return <LockedView />;
  }

  // ── action-rail summaries ──
  const selectedFund = funds.find((f) => f.ticker === selectedTicker) ?? null;
  // Какие кнопки рейла активны. ⭐ Актив НЕ используется ни в одном режиме
  // фонд-трейда: «Состав» — список фондов (карточка = tap по плитке, фильтр
  // УК + сортировка живут в ⚙️ Опции, как на десктопе); в Снапшоте/Потоках
  // выбор фонда и бумаги — в теле раздела. Поэтому рейл = 🕐 Время + ⚙️ Опции.
  // - funds:     🕐(период доходности) ⚙️(сортировка + УК-фильтр)
  // - movers:    🕐(месяц) ⚙️(метрика + выбор фондов)
  // - snapshots: ⚙️(режим); фонд/дата/метрика — в теле
  // - company:   ⚙️(режим); бумага/фонды — в теле CompanyFlowsTab
  const timeSummary = (() => {
    if (tab === 'funds') return `Доходность · ${RETURN_PERIOD_LABEL[returnPeriod]}`;
    if (tab === 'portfolio') return `Доходность · ${RETURN_PERIOD_LABEL[portfolioPeriod]}`;
    if (tab === 'movers') return asOf ? formatMonthYear(asOf) : (movers?.resolved_month ? formatMonthYear(movers.resolved_month) : (movers?.available_months[0] ? formatMonthYear(movers.available_months[0]) : 'Месяц'));
    return undefined;
  })();
  const optionsSummary = (() => {
    const base = TAB_LABEL[tab];
    if (tab === 'movers') return `${base} · ${metric === 'weight' ? '% веса' : '₽'}`;
    if (tab === 'portfolio') return `Портфель · ${portfolioMode === 'rub' ? '₽' : 'ср. доля'}`;
    if (tab === 'funds') {
      const s = fundSort === 'return' ? 'доходность' : fundSort === 'volume' ? 'объём' : 'имя';
      return `${base} · ${s}`;
    }
    return base;
  })();
  return (
    <MobileLayout
      onTimeClick={timeSummary ? () => setTimeSheetOpen(true) : undefined}
      timeSummary={timeSummary}
      timeTourId="ft-time"
      onSettingsClick={() => setOptionsSheetOpen(true)}
      settingsSummary={optionsSummary}
      settingsTourId="ft-options"
      enableFullscreen={false}
      onRefresh={async () => {
        if (tab === 'portfolio') {
          const reqId = ++portfolioReqRef.current;
          const p = await getFundPortfolio({ funds: portfolioFundsParam || undefined }).catch(() => null);
          if (reqId !== portfolioReqRef.current) return;
          if (p) setPortfolio(p);
        } else if (tab === 'movers') {
          const reqId = ++moversReqRef.current;
          const m = await getFundTradesMovers(period, { asOf, funds: fundsParam || undefined, sort: metric }).catch(() => null);
          if (reqId !== moversReqRef.current) return;
          if (m) setMovers(m);
        } else {
          const reqId = ++fundsReqRef.current;
          const r = await listFundsWithHistory().catch(() => null);
          if (reqId !== fundsReqRef.current) return;
          if (r) setFunds(r.funds);
        }
      }}
      loading={loadingFunds || loadingMovers || loadingPortfolio}
    >
      <MobilePageHeader
        Icon={Wallet}
        title="Что покупают фонды"
        helpLink="/methodology/funds-catalog"
        sourceNote="Справки о СЧА (форма ЦБ № 0420502) · УК Первая, Т-Капитал, ВИМ, Альфа"
      />

      {error && (
        <div
          style={{
            margin: '0 0 12px',
            padding: 12,
            background: 'color-mix(in srgb, var(--danger, #ef4444) 10%, transparent)',
            border: '1px solid var(--danger, #ef4444)',
            borderRadius: 8,
            color: 'var(--danger, #ef4444)',
            fontSize: 'var(--fs-sm)',
          }}
        >
          {error}
        </div>
      )}

      {/* ── Tab content — СОБСТВЕННЫЙ вертикальный скролл-контейнер. ──
          .fm-main залочен (overflow:hidden, под чарт-страницы, где чарт
          фиксирован в viewport). Фонд-трейд — это списки/несколько графиков,
          им нужен свой скролл: иначе контент режется (было видно «только 2
          фонда», обрезанный снапшот/потоки) и затекает в щель над нижним
          рейлом. */}
      <div className="fm-ft-scroll">
      <DelayedDataBadge />
      {tab === 'funds' && (
        <FundsTab
          fundsByCategory={fundsByCategory}
          loading={loadingFunds && funds.length === 0}
          empty={!loadingFunds && funds.length === 0 && !error}
          returnPeriod={returnPeriod}
          tileHover={tileHover}
          setTileHover={setTileHover}
          onSelect={(ticker) => setSelectedTicker(ticker)}
        />
      )}

      {tab === 'movers' && (
        <MoversTab
          movers={movers}
          loading={loadingMovers && !movers}
          metric={metric}
          onAssetClick={openCompanyFlows}
        />
      )}

      {tab === 'company' && (
        <div style={{ padding: '4px 2px 16px' }}>
          <CompanyFlowsTab
            presetAsset={companyPreset}
            onPresetConsumed={() => setCompanyPreset(null)}
          />
        </div>
      )}

      {tab === 'portfolio' && (
        <div style={{ paddingBottom: 12 }}>
          <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', margin: '4px 2px 12px', lineHeight: 1.5 }}>
            Все выбранные фонды акций собраны в один портфель, как будто ими управляет
            один управляющий. Режим веса и выбор УК находятся в кнопке Опции.
          </p>
          <CombinedPortfolioView
            portfolio={portfolio}
            loading={loadingPortfolio}
            mode={portfolioMode}
            period={portfolioPeriod}
            variant="mobile"
          />
        </div>
      )}

      {tab === 'snapshots' && <SnapshotReviewTab />}
      </div>

      {/* ── ⭐ Актив sheet (только funds): FundPicker single → выбирает фонд ──
          В режиме funds выбор фонда = открыть детальную карточку. FundPicker
          (single) сам рисует модал поверх; selecting закрывает sheet и
          открывает FundDetailModal. */}
      <MobileSheet open={assetSheetOpen} onClose={() => setAssetSheetOpen(false)} title="Выбор фонда">
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 12 }}>
          <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', margin: 0, lineHeight: 1.5 }}>
            Выберите фонд — откроется детальная карточка: СЧА, доходность пая и
            состав портфеля с переходом в историю по бумаге.
          </p>
          <FundPicker
            funds={moverPickerFunds}
            mode="single"
            selected={assetSheetSelected}
            onChange={(next) => {
              const t = next.values().next().value as string | undefined;
              if (t) {
                setSelectedTicker(t);
                setAssetSheetOpen(false);
              }
            }}
            minWidth={260}
          />
        </div>
      </MobileSheet>

      {/* ── 🕐 Время sheet ── */}
      <MobileSheet open={timeSheetOpen} onClose={() => setTimeSheetOpen(false)} title="Время">
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 20 }}>
          {tab === 'portfolio' && (
            <div>
              <div style={SHEET_SECTION_LABEL}>Период доходности</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {(['m1', 'm3', 'm6', 'y1'] as ReturnPeriodKey[]).map((k) => (
                  <button
                    key={k}
                    className={`fm-chip ${portfolioPeriod === k ? 'active' : ''}`}
                    onClick={() => { setPortfolioPeriod(k); setTimeSheetOpen(false); }}
                    style={{ flex: 1, justifyContent: 'center', minWidth: 'calc(50% - 4px)' }}
                  >
                    {RETURN_PERIOD_LABEL[k]}
                  </button>
                ))}
              </div>
              <p style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', marginTop: 8, lineHeight: 1.5 }}>
                Доходность всего портфеля, взвешенная по СЧА фондов.
              </p>
            </div>
          )}
          {tab === 'funds' && (
            <div>
              <div style={SHEET_SECTION_LABEL}>Период доходности</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {(['m1', 'm3', 'm6', 'y1'] as ReturnPeriodKey[]).map((k) => (
                  <button
                    key={k}
                    className={`fm-chip ${returnPeriod === k ? 'active' : ''}`}
                    onClick={() => { setReturnPeriod(k); setTimeSheetOpen(false); }}
                    style={{ flex: 1, justifyContent: 'center', minWidth: 'calc(50% - 4px)' }}
                  >
                    {RETURN_PERIOD_LABEL[k]}
                  </button>
                ))}
              </div>
              <p style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', marginTop: 8, lineHeight: 1.5 }}>
                Показывается на плитках и используется при сортировке «по доходности».
              </p>
            </div>
          )}
          {tab === 'movers' && (
            <div>
              <div style={SHEET_SECTION_LABEL}>Месяц снапшота</div>
              {movers && movers.available_months.length > 0 ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {(() => {
                    const cutoff = movers.snapshot_cutoff ?? null;
                    const isLocked = (m: string) => cutoff != null && m > cutoff;
                    // дефолт = последний ДОСТУПНЫЙ (не locked) месяц — сразу актуальный срез
                    const def = movers.available_months.find((m) => !isLocked(m)) ?? movers.available_months[0];
                    return movers.available_months.map((m) => {
                      const locked = isLocked(m);
                      const active = (asOf ?? def) === m;
                      return (
                        <button
                          key={m}
                          className={`fm-chip ${active ? 'active' : ''}`}
                          onClick={() => {
                            if (locked) {
                              showUpgrade({ tier: 'basic', featureName: 'свежий срез фондов', indicator: 'fund_trades' });
                              setTimeSheetOpen(false);
                              return;
                            }
                            setAsOf(m);
                            setTimeSheetOpen(false);
                          }}
                          style={{ justifyContent: 'flex-start', padding: '14px 16px', gap: 8, opacity: locked ? 0.6 : 1 }}
                        >
                          {formatMonthYear(m)}
                          {locked && <Lock size={12} strokeWidth={2.4} style={{ marginLeft: 'auto' }} />}
                        </button>
                      );
                    });
                  })()}
                </div>
              ) : (
                <p style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-muted)', margin: 0 }}>
                  Загружаем месяцы…
                </p>
              )}
              <p style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', marginTop: 8, lineHeight: 1.5 }}>
                Сравнение всегда «выбранный месяц vs предыдущий».
              </p>
            </div>
          )}
        </div>
      </MobileSheet>

      {/* ── ⚙️ Опции sheet — режимы + всё прочее ── */}
      <MobileSheet open={optionsSheetOpen} onClose={() => setOptionsSheetOpen(false)} title="Опции">
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 20 }}>
          {/* Переключатель режимов */}
          <div>
            <div style={SHEET_SECTION_LABEL}>Режим</div>
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
              {([
                { id: 'funds' as const, label: 'Состав фондов', icon: Wallet },
                { id: 'portfolio' as const, label: 'Портфель', icon: Briefcase },
                { id: 'movers' as const, label: 'Движения', icon: TrendingUp },
                { id: 'snapshots' as const, label: 'Снапшот', icon: Activity },
                { id: 'company' as const, label: 'Потоки', icon: ArrowLeftRight },
              ]).map((t) => {
                const Icon = t.icon;
                return (
                  <button
                    key={t.id}
                    className={`fm-chip ${tab === t.id ? 'active' : ''}`}
                    onClick={() => { setTab(t.id); setOptionsSheetOpen(false); }}
                    style={{ flex: 1, minWidth: 'calc(50% - 4px)', justifyContent: 'center', gap: 6 }}
                  >
                    <Icon size={14} />
                    {t.label}
                  </button>
                );
              })}
            </div>
          </div>

          {/* Состав фондов: сортировка + УК */}
          {tab === 'funds' && (
            <>
              <div>
                <div style={SHEET_SECTION_LABEL}>Сортировка</div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {([
                    { id: 'return' as const, label: 'Доходность' },
                    { id: 'volume' as const, label: 'Объём, руб' },
                    { id: 'name' as const, label: 'Имя' },
                  ]).map((s) => (
                    <button
                      key={s.id}
                      className={`fm-chip ${fundSort === s.id ? 'active' : ''}`}
                      onClick={() => setFundSort(s.id)}
                      style={{ flex: 1, justifyContent: 'center' }}
                    >
                      {s.label}
                    </button>
                  ))}
                </div>
                {fundSort === 'return' && (
                  <p style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', marginTop: 8, lineHeight: 1.5 }}>
                    Период доходности ({RETURN_PERIOD_LABEL[returnPeriod]}) меняется в кнопке «Время».
                  </p>
                )}
              </div>
              {ukOptions.length > 1 && (
                <div>
                  <div style={SHEET_SECTION_LABEL}>Управляющая компания</div>
                  <UkMultiSelect
                    options={ukOptions}
                    selected={selectedUks}
                    onChange={setSelectedUks}
                    size="md"
                  />
                </div>
              )}
            </>
          )}

          {/* Движения: метрика + выбор конкретных фондов */}
          {tab === 'movers' && (
            <>
              <div>
                <div style={SHEET_SECTION_LABEL}>Метрика</div>
                <div style={{ display: 'flex', gap: 6 }}>
                  {([['weight', '% веса'], ['amount', 'Объём, руб']] as const).map(([key, lbl]) => (
                    <button
                      key={key}
                      className={`fm-chip ${metric === key ? 'active' : ''}`}
                      onClick={() => setMetric(key)}
                      style={{ flex: 1, justifyContent: 'center' }}
                    >
                      {lbl}
                    </button>
                  ))}
                </div>
              </div>
              {moverPickerFunds.length > 1 && (
                <div>
                  <div style={SHEET_SECTION_LABEL}>Фонды</div>
                  <FundPicker
                    funds={moverPickerFunds}
                    mode="multi"
                    selected={selectedMoverFunds}
                    onChange={setSelectedMoverFunds}
                  />
                </div>
              )}
            </>
          )}

          {/* Общий портфель: режим веса + выбор УК */}
          {tab === 'portfolio' && (
            <>
              <div>
                <div style={SHEET_SECTION_LABEL}>Режим веса</div>
                <div style={{ display: 'flex', gap: 6 }}>
                  {([['rub', 'По рублям'], ['share', 'Средняя доля']] as const).map(([key, lbl]) => (
                    <button
                      key={key}
                      className={`fm-chip ${portfolioMode === key ? 'active' : ''}`}
                      onClick={() => setPortfolioMode(key)}
                      style={{ flex: 1, justifyContent: 'center' }}
                    >
                      {lbl}
                    </button>
                  ))}
                </div>
              </div>
              {ukOptions.length > 1 && (
                <div>
                  <div style={SHEET_SECTION_LABEL}>Управляющая компания</div>
                  <UkMultiSelect
                    options={ukOptions}
                    selected={portfolioUks}
                    onChange={setPortfolioUks}
                    size="md"
                  />
                </div>
              )}
            </>
          )}

          {/* Снапшот: метрика живёт внутри тела (frame:fundtrades:snapMetric);
              Потоки: контролы (бумага + фонды) внутри CompanyFlowsTab. */}
          {tab === 'snapshots' && (
            <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', margin: 0, lineHeight: 1.5 }}>
              Выбор фонда, дата снапшота и метрика (вес % / объём ₽) — в самом
              разделе «Снапшот» выше.
            </p>
          )}
          {tab === 'company' && (
            <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', margin: 0, lineHeight: 1.5 }}>
              Выбор бумаги и конкретных фондов — в самом разделе «Потоки по
              компании» выше.
            </p>
          )}
        </div>
      </MobileSheet>

      {/* ── Детальная карточка фонда (Состав фондов: tap по плитке) ── */}
      {selectedTicker && (
        <FundDetailModal
          ticker={selectedTicker}
          loadDetail={() => getFundTradesDetail(selectedTicker, period)}
          navRub={selectedFund?.nav_rub}
          returns={selectedFund?.returns}
          hasDistributions={selectedFund?.has_distributions}
          enableDrilldown
          onClose={() => setSelectedTicker(null)}
        />
      )}

      <OnboardingTour steps={tourSteps} open={tour.open} onClose={tour.close} />
    </MobileLayout>
  );
}

// ════════════════════════════════════════════════════════════════════
// Tab 1 — Состав фондов: пончик + топ-5 в плитке, tap → FundDetailModal.
// ════════════════════════════════════════════════════════════════════
function FundsTab({
  fundsByCategory,
  loading,
  empty,
  returnPeriod,
  tileHover,
  setTileHover,
  onSelect,
}: {
  fundsByCategory: Record<string, FundWithHistory[]>;
  loading: boolean;
  empty: boolean;
  returnPeriod: ReturnPeriodKey;
  tileHover: { fund: number; idx: number } | null;
  setTileHover: (v: { fund: number; idx: number } | null) => void;
  onSelect: (ticker: string) => void;
}) {
  if (loading) {
    return <div style={{ padding: '24px 4px', color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>Загружаем фонды…</div>;
  }
  if (empty) {
    return <EmptyState message="Фонды не найдены." />;
  }
  return (
    <div style={{ paddingBottom: 12 }}>
      {Object.entries(fundsByCategory).map(([cat, list]) => (
        <div key={cat} style={{ marginBottom: 24 }}>
          <h2
            style={{
              fontSize: 'var(--fs-sm)',
              fontWeight: 700,
              color: 'var(--text-primary)',
              margin: '0 0 10px',
              textTransform: 'uppercase',
              letterSpacing: '0.04em',
            }}
          >
            {CATEGORY_LABEL[cat] || cat}{' '}
            <span style={{ color: 'var(--text-muted)', fontWeight: 500 }}>· {list.length}</span>
          </h2>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {list.map((f) => (
              <FundTile
                key={f.fund_id}
                fund={f}
                returnPeriod={returnPeriod}
                tileHover={tileHover}
                setTileHover={setTileHover}
                onSelect={() => onSelect(f.ticker)}
              />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function FundTile({
  fund: f,
  returnPeriod,
  tileHover,
  setTileHover,
  onSelect,
}: {
  fund: FundWithHistory;
  returnPeriod: ReturnPeriodKey;
  tileHover: { fund: number; idx: number } | null;
  setTileHover: (v: { fund: number; idx: number } | null) => void;
  onSelect: () => void;
}) {
  const uk = resolveFundLogo(f.ticker, f.uk_id);
  const top = f.top_holdings ?? [];
  const sum = top.reduce((s, h) => s + (h.weight || 0), 0);
  const other = 100 - sum;
  const donutHoldings = other > 1 ? [...top, { name: 'Прочее', isin: null, weight: other }] : top;
  const donutColors = donutHoldings.map((h, i) =>
    h.name === 'Прочее'
      ? 'var(--text-muted)'
      : (fundAssetColor(h.name, h.isin) ?? DONUT_COLORS[i % DONUT_COLORS.length]),
  );
  const ret = displayReturn(f.returns, returnPeriod);

  return (
    <button
      onClick={onSelect}
      className="editorial-press"
      style={{
        padding: 14,
        background: 'var(--bg-secondary)',
        border: '1.5px solid var(--border-color)',
        borderRadius: 12,
        textAlign: 'left',
        cursor: 'pointer',
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
      }}
    >
      {/* Header: УК-аватар + имя + тикер */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
        {uk && (
          <div
            style={{
              width: 40,
              height: 40,
              borderRadius: '50%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              flexShrink: 0,
              fontWeight: 900,
              fontSize: 'var(--fs-md)',
              overflow: 'hidden',
              backgroundColor: uk.img ? undefined : uk.bg,
              color: uk.color,
            }}
          >
            {uk.img
              ? <img src={uk.img} alt={uk.name} style={{ width: '100%', height: '100%', objectFit: 'cover' }} />
              : uk.letter}
          </div>
        )}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div
            style={{
              fontSize: 'var(--fs-sm)',
              fontWeight: 800,
              color: 'var(--text-primary)',
              lineHeight: 1.2,
              marginBottom: 2,
              display: '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
              overflow: 'hidden',
            }}
            title={f.name}
          >
            {stripUkName(f.name, f.uk_id)}
          </div>
          <div
            style={{
              fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
              fontSize: 'var(--fs-2xs)',
              color: 'var(--text-secondary)',
              lineHeight: 1.3,
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
            }}
          >
            {f.ticker}
          </div>
        </div>
      </div>

      {/* Body: пончик + топ-5 */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginTop: 12 }}>
        {top.length > 0 ? (
          <div style={{ flexShrink: 0, lineHeight: 0 }}>
            <Donut
              holdings={donutHoldings}
              colors={donutColors}
              size={120}
              outerRadius={92}
              innerRadius={62}
              maxSlices={donutHoldings.length}
              centerCount={f.holdings_count}
              showCenterText
              highlightIndex={tileHover?.fund === f.fund_id ? tileHover.idx : null}
              onHoverChange={(i) => setTileHover(i == null ? null : { fund: f.fund_id, idx: i })}
            />
          </div>
        ) : (
          <div
            style={{
              width: 120,
              height: 120,
              flexShrink: 0,
              borderRadius: '50%',
              border: '1.5px dashed var(--border-color)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: 'var(--text-muted)',
              fontSize: 'var(--fs-xl)',
            }}
          >
            —
          </div>
        )}
        <div
          style={{
            flex: 1,
            minWidth: 0,
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            gap: 4,
            minHeight: 100,
          }}
        >
          {top.slice(0, 5).map((h, i) => (
            <div
              key={h.name + i}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                minHeight: 18,
                fontSize: 'var(--fs-2xs)',
                borderRadius: 5,
                padding: '0 4px',
                margin: '0 -4px',
                background: tileHover?.fund === f.fund_id && tileHover.idx === i ? 'var(--bg-primary)' : 'transparent',
              }}
            >
              <span
                style={{
                  width: 8,
                  height: 8,
                  borderRadius: '50%',
                  flexShrink: 0,
                  backgroundColor: fundAssetColor(h.name, h.isin) ?? DONUT_COLORS[i % DONUT_COLORS.length],
                }}
              />
              <span
                style={{
                  flex: 1,
                  minWidth: 0,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                  color: 'var(--text-secondary)',
                  fontWeight: 600,
                }}
              >
                {fundAssetName(h.name, h.isin)}
              </span>
              <span
                style={{
                  flexShrink: 0,
                  fontVariantNumeric: 'tabular-nums',
                  fontWeight: 600,
                  color: 'var(--text-primary)',
                }}
              >
                {h.weight.toFixed(1)}%
              </span>
            </div>
          ))}
          {top.length === 0 && (
            <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}>
              Состав не публикуется
            </span>
          )}
        </div>
      </div>

      {/* Footer: доходность + СЧА */}
      <div
        style={{
          marginTop: 12,
          paddingTop: 10,
          borderTop: '1px solid var(--border-color)',
          display: 'flex',
          alignItems: 'baseline',
          justifyContent: 'space-between',
          gap: 8,
        }}
      >
        <span style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
          <span style={{ fontSize: 'var(--fs-3xs, 9px)', color: 'var(--text-secondary)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
            Доходн. · {ret?.period ?? RETURN_PERIOD_LABEL[returnPeriod]}
          </span>
          <span style={{ fontSize: 'var(--fs-lg)', fontWeight: 800, fontVariantNumeric: 'tabular-nums', color: returnColor(ret?.v), lineHeight: 1.1 }}>
            {formatReturnPct(ret?.v)}
          </span>
        </span>
        <span style={{ display: 'flex', flexDirection: 'column', gap: 1, alignItems: 'flex-end' }}>
          <span style={{ fontSize: 'var(--fs-3xs, 9px)', color: 'var(--text-secondary)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
            СЧА
          </span>
          <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, fontVariantNumeric: 'tabular-nums', color: 'var(--text-primary)' }}>
            {f.nav_rub != null ? formatRubShort(f.nav_rub) : '—'}
          </span>
        </span>
      </div>
    </button>
  );
}

// ════════════════════════════════════════════════════════════════════
// Tab 2 — Движения: топ-аккумуляция / распродажа (две колонки → стек).
// ════════════════════════════════════════════════════════════════════
function MoversTab({
  movers,
  loading,
  metric,
  onAssetClick,
}: {
  movers: FundTradesMovers | null;
  loading: boolean;
  metric: 'weight' | 'amount';
  onAssetClick: (m: FundTradesMovers['top_accumulated'][number]) => void;
}) {
  if (loading) {
    return <div style={{ padding: '24px 4px', color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>Загружаем агрегаты…</div>;
  }
  if (!movers) return null;
  const noData = movers.top_accumulated.length === 0 && movers.top_reduced.length === 0;
  if (noData) {
    return <EmptyState message={
      movers.funds_in_month === 0
        ? `Для выбранных фондов нет данных за ${formatMonthYear(movers.resolved_month ?? '')}. Снапшот за этот месяц ещё не загружен.`
        : 'Нет заметных движений между этими месяцами.'
    } />;
  }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, paddingBottom: 12 }}>
      <MoversColumn
        title="Топ-аккумуляция"
        icon={TrendingUp}
        color="var(--success, #2dd478)"
        items={movers.top_accumulated.slice(0, 5)}
        empty="Накоплений нет"
        metric={metric}
        onAssetClick={onAssetClick}
      />
      <MoversColumn
        title="Топ-распродажа"
        icon={TrendingDown}
        color="var(--danger, #ef4444)"
        items={movers.top_reduced.slice(0, 5)}
        empty="Распродаж нет"
        negative
        metric={metric}
        onAssetClick={onAssetClick}
      />
    </div>
  );
}

function MoversColumn({
  title,
  icon: Icon,
  color,
  items,
  empty,
  negative,
  metric,
  onAssetClick,
}: {
  title: string;
  icon: typeof TrendingUp;
  color: string;
  items: FundTradesMovers['top_accumulated'];
  empty: string;
  negative?: boolean;
  metric: 'weight' | 'amount';
  onAssetClick: (m: FundTradesMovers['top_accumulated'][number]) => void;
}) {
  const valOf = (m: FundTradesMovers['top_accumulated'][number]) =>
    metric === 'amount' ? m.total_delta_amount : m.total_delta_weight;
  const fmtVal = (v: number) => metric === 'amount'
    ? `${v > 0 ? '+' : '−'}${formatRubShort(Math.abs(v))}`
    : `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
  const maxAbs = Math.max(...items.map((m) => Math.abs(valOf(m))), 0.0001);
  const reveal = useGrowReveal(
    items.length,
    `${title}|${metric}|${items.length}|${items[0]?.akey ?? ''}|${maxAbs.toFixed(0)}`,
  );
  return (
    <div
      style={{
        background: 'var(--bg-secondary)',
        border: '1.5px solid var(--border-color)',
        borderRadius: 12,
        padding: 14,
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          marginBottom: 10,
          paddingBottom: 8,
          borderBottom: `1px solid ${color}`,
        }}
      >
        <Icon size={16} color={color} strokeWidth={2.2} />
        <h3 style={{ fontSize: 'var(--fs-sm)', fontWeight: 800, color, margin: 0 }}>{title}</h3>
      </div>
      {items.length === 0 ? (
        <p style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>{empty}</p>
      ) : (
        <div>
          {items.map((m, i) => {
            const val = valOf(m);
            const pct = Math.max(2, (Math.abs(val) / maxAbs) * 100);
            const mName = fundAssetName(m.asset_name, isIsin(m.akey) ? m.akey : null);
            return (
              <div
                key={m.akey}
                onClick={() => onAssetClick(m)}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onAssetClick(m); } }}
                title={`Потоки по компании: ${mName}`}
                style={{
                  padding: '9px 8px',
                  margin: '0 -8px',
                  borderRadius: 8,
                  cursor: 'pointer',
                  borderBottom: i === items.length - 1
                    ? 'none'
                    : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                  <span style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', fontFamily: 'ui-monospace, monospace', flexShrink: 0 }}>
                    {i + 1}.
                  </span>
                  <span
                    style={{
                      flex: 1,
                      minWidth: 0,
                      fontSize: 'var(--fs-sm)',
                      color: 'var(--text-primary)',
                      fontWeight: 600,
                      lineHeight: 1.3,
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {mName}
                  </span>
                  <span style={{ fontFamily: 'ui-monospace, "SF Mono", monospace', fontSize: 'var(--fs-sm)', fontWeight: 800, color, flexShrink: 0 }}>
                    {fmtVal(val)}
                  </span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 5 }}>
                  <div style={{ flex: 1, height: 6, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 3, overflow: 'hidden' }}>
                    <div style={{ width: `${pct * (reveal[i] ?? 1)}%`, height: '100%', background: color, borderRadius: 3 }} />
                  </div>
                  <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', flexShrink: 0, minWidth: 76, textAlign: 'right' }}>
                    {negative ? `${m.funds_selling} продают` : `${m.funds_buying} покупают`}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════
// Tab 3 — Снапшот: состав фонда на дату + докупил/продал/новые/вышел.
// (FundPicker single + таймлайн снапшотов + метрика — всё в теле раздела.)
// ════════════════════════════════════════════════════════════════════
function SnapshotReviewTab() {
  const [availableFunds, setAvailableFunds] = useState<FundWithHistory[]>([]);
  const [ticker, setTicker] = usePersistedState<string>('frame:fundtrades:snapTicker', 'EQMX');
  const [snapshotsList, setSnapshotsList] = useState<FundSnapshotsList | null>(null);
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [review, setReview] = useState<FundSnapshotReview | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [drillDown, setDrillDown] = useState<{ asset_name: string; isin: string | null } | null>(null);

  useEffect(() => {
    listFundsWithHistory()
      .then((data) => setAvailableFunds(data.funds.filter((f) => (f.snapshot_count || 0) > 0)))
      .catch(() => {});
  }, []);

  useEffect(() => {
    let cancel = false;
    setLoading(true);
    setError(null);
    setReview(null); // очистить прошлый фонд при смене тикера
    getFundSnapshots(ticker)
      .then((data) => {
        if (cancel) return;
        setSnapshotsList(data);
        // Дефолт = последний ДОСТУПНЫЙ тиру срез (не locked) → сразу данные, не замок.
        if (data.snapshots.length > 0) {
          const firstAvailable = data.snapshots.find((s) => !s.locked) ?? data.snapshots[0];
          setSelectedDate(firstAvailable.snapshot_date);
        } else { setSelectedDate(null); setReview(null); }
      })
      .catch((e) => !cancel && setError(e.message))
      .finally(() => !cancel && setLoading(false));
    return () => { cancel = true; };
  }, [ticker]);

  useEffect(() => {
    if (!selectedDate) return;
    let cancel = false;
    setLoading(true);
    setError(null);
    getFundSnapshotReview(ticker, selectedDate)
      .then((data) => !cancel && setReview(data))
      .catch((e) => !cancel && setError(e.message))
      .finally(() => !cancel && setLoading(false));
    return () => { cancel = true; };
  }, [ticker, selectedDate]);

  const maxAbsAmount = useMemo(() => {
    if (!review) return 0;
    const allBars = [
      ...review.added.map((r) => r.delta_amount_rub || 0),
      ...review.reduced.map((r) => Math.abs(r.delta_amount_rub || 0)),
      ...review.new.map((r) => r.curr_amount_rub || 0),
      ...review.sold_out.map((r) => r.prev_amount_rub || 0),
    ];
    return Math.max(...allBars, 1);
  }, [review]);

  const pickerFunds = useMemo<FundPickerFund[]>(
    () => availableFunds.map((f) => ({ ticker: f.ticker, name: f.name, uk: f.uk, uk_id: f.uk_id })),
    [availableFunds],
  );
  const pickerSelected = useMemo(() => new Set([ticker]), [ticker]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 18, paddingBottom: 12 }}>
      {/* Ticker selector — FundPicker single (УК → фонд) */}
      <FundPicker
        funds={pickerFunds}
        mode="single"
        selected={pickerSelected}
        onChange={(next) => {
          const t = next.values().next().value as string | undefined;
          if (t) setTicker(t);
        }}
        minWidth={260}
      />

      {/* Snapshots timeline */}
      {snapshotsList && snapshotsList.snapshots.length > 0 && (
        <div
          style={{
            display: 'flex',
            gap: 8,
            overflowX: 'auto',
            padding: '10px 2px 12px',
            borderTop: '1px solid var(--border-color)',
            borderBottom: '1px solid var(--border-color)',
            WebkitOverflowScrolling: 'touch',
          }}
        >
          {snapshotsList.snapshots.map((s) => {
            const active = s.snapshot_date === selectedDate;
            return (
              <button
                key={s.snapshot_date}
                onClick={() => setSelectedDate(s.snapshot_date)}
                title={s.locked ? 'свежий срез — по подписке' : undefined}
                className={`fm-chip ${active ? 'active' : ''}`}
                style={{ flexShrink: 0, display: 'inline-flex', alignItems: 'center', gap: 4, opacity: s.locked && !active ? 0.7 : 1 }}
              >
                {formatMonthYear(s.snapshot_date)}
                {s.locked && <Lock size={11} strokeWidth={2.4} />}
              </button>
            );
          })}
        </div>
      )}

      {loading && (
        <div style={{ textAlign: 'center', padding: 32, color: 'var(--text-tertiary)' }}>Загрузка...</div>
      )}
      {error && (
        <div style={{ padding: 14, background: 'var(--bg-secondary)', color: 'var(--mood-red)', borderRadius: 8 }}>{error}</div>
      )}

      {!loading && review && (
        review.locked
          ? <LockedSnapshotTeaser latestDate={review.latest_snapshot_date} requiredTier={review.required_tier} />
          : <SnapshotReviewBody
              review={review}
              maxAbsAmount={maxAbsAmount}
              onRowClick={(r) => setDrillDown({ asset_name: r.asset_name, isin: r.isin })}
            />
      )}

      {!loading && !error && snapshotsList && snapshotsList.snapshots.length === 0 && (
        <div style={{ padding: 32, textAlign: 'center', color: 'var(--text-tertiary)', fontSize: 'var(--fs-sm)' }}>
          У {ticker} пока нет исторических снапшотов SCHA. Данные накапливаются с каждым месяцем.
        </div>
      )}

      {drillDown && (
        <AssetHistoryModal
          ticker={ticker}
          asset_name={drillDown.asset_name}
          isin={drillDown.isin}
          onClose={() => setDrillDown(null)}
        />
      )}
    </div>
  );
}

function SnapshotReviewBody({
  review,
  maxAbsAmount,
  onRowClick,
}: {
  review: FundSnapshotReview;
  maxAbsAmount: number;
  onRowClick: (r: FundDiffRow) => void;
}) {
  const [metric, setMetric] = usePersistedState<'amount' | 'weight'>('frame:fundtrades:snapMetric', 'amount');
  const isW = metric === 'weight';
  const wDelta = (r: FundDiffRow) => (r.curr_weight ?? 0) - (r.prev_weight ?? 0);
  const aAdded = (r: FundDiffRow) => r.delta_amount_rub ?? 0;
  const aNew = (r: FundDiffRow) => r.curr_amount_rub ?? 0;
  const aSold = (r: FundDiffRow) => -(r.prev_amount_rub ?? 0);
  const wNew = (r: FundDiffRow) => r.curr_weight ?? 0;
  const wSold = (r: FundDiffRow) => -(r.prev_weight ?? 0);
  const maxAbsWeight = useMemo(() => Math.max(
    0.01,
    ...review.added.map((r) => Math.abs(wDelta(r))),
    ...review.reduced.map((r) => Math.abs(wDelta(r))),
    ...review.new.map((r) => r.curr_weight ?? 0),
    ...review.sold_out.map((r) => r.prev_weight ?? 0),
  ), [review]);
  const maxAbs = isW ? maxAbsWeight : maxAbsAmount;
  const fmtVal = isW ? (v: number) => `${v.toFixed(2)}%` : (v: number) => formatRubShort(v);
  const sortByAbs = (items: FundDiffRow[], get: (r: FundDiffRow) => number) =>
    [...items].sort((a, b) => Math.abs(get(b)) - Math.abs(get(a)));
  const sumBy = (items: FundDiffRow[], get: (r: FundDiffRow) => number) =>
    items.reduce((s, r) => s + get(r), 0);
  const addedItems = isW
    ? [...review.added, ...review.reduced].filter((r) => wDelta(r) > 0)
    : review.added;
  const reducedItems = isW
    ? [...review.added, ...review.reduced].filter((r) => wDelta(r) < 0)
    : review.reduced;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      {/* Header: fund + dates */}
      <div>
        <h3
          style={{
            fontFamily: 'var(--font-serif, Georgia, serif)',
            fontSize: 'var(--fs-xl)',
            margin: '0 0 6px',
            color: 'var(--text-primary)',
          }}
        >
          {review.fund.name}
        </h3>
        <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-tertiary)', lineHeight: 1.5 }}>
          Снапшот <strong style={{ color: 'var(--text-primary)' }}>{formatMonthYear(review.current_snapshot_date)}</strong>
          {review.previous_snapshot_date && (
            <> · сравниваем с {formatMonthYear(review.previous_snapshot_date)}</>
          )}
          {' · '}{review.totals!.current_assets} активов
        </div>
      </div>

      {/* Нет предыдущего снапшота → состав на дату */}
      {!review.previous_snapshot_date && (
        <div>
          <div style={{ padding: 12, marginBottom: 12, background: 'var(--bg-secondary)', borderRadius: 8, fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', lineHeight: 1.5 }}>
            Самый ранний снапшот — сравнивать не с чем. Состав фонда на эту дату:
          </div>
          {review.current_holdings.map((h) => (
            <div
              key={h.isin || h.asset_name}
              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 12, padding: '7px 0', borderBottom: '1px solid var(--border-soft, rgba(0,0,0,0.06))' }}
            >
              <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>{fundAssetName(h.asset_name, h.isin)}</span>
              <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', fontVariantNumeric: 'tabular-nums', flexShrink: 0 }}>
                {h.weight != null ? `${h.weight.toFixed(2)}%` : '—'}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Метрика (вес % / объём ₽) */}
      {review.previous_snapshot_date && (
        <div style={{ display: 'flex', gap: 6 }}>
          {([['amount', 'Объём, руб'], ['weight', '% веса']] as const).map(([key, lbl]) => (
            <button
              key={key}
              className={`fm-chip ${metric === key ? 'active' : ''}`}
              onClick={() => setMetric(key)}
              style={{ flex: 1, justifyContent: 'center' }}
            >
              {lbl}
            </button>
          ))}
        </div>
      )}

      {addedItems.length > 0 && (
        <SnapshotSection
          title="ДОКУПИЛ"
          count={addedItems.length}
          total={isW ? sumBy(addedItems, wDelta) : review.totals!.total_added_rub}
          items={sortByAbs(addedItems, isW ? wDelta : aAdded)}
          maxAbs={maxAbs}
          isPositive
          valueGetter={isW ? wDelta : aAdded}
          formatValue={fmtVal}
          subLabelGetter={(r) =>
            `+${formatShares(r.delta_positions || 0)} шт` +
            (r.curr_weight !== null ? ` · ${r.curr_weight.toFixed(2)}%` : '')}
          onItemClick={onRowClick}
        />
      )}

      {reducedItems.length > 0 && (
        <SnapshotSection
          title="ПРОДАЛ"
          count={reducedItems.length}
          total={isW ? Math.abs(sumBy(reducedItems, wDelta)) : Math.abs(review.totals!.total_reduced_rub)}
          items={sortByAbs(reducedItems, isW ? wDelta : aAdded)}
          maxAbs={maxAbs}
          isPositive={false}
          valueGetter={isW ? wDelta : aAdded}
          formatValue={fmtVal}
          subLabelGetter={(r) =>
            `${formatShares(r.delta_positions || 0)} шт` +
            (r.curr_weight !== null ? ` · ${r.curr_weight.toFixed(2)}%` : '')}
          onItemClick={onRowClick}
        />
      )}

      {review.new.length > 0 && (
        <SnapshotSection
          title="НОВЫЕ ПОЗИЦИИ"
          count={review.new.length}
          total={isW ? sumBy(review.new, wNew) : review.totals!.total_new_rub}
          items={sortByAbs(review.new, isW ? wNew : aNew)}
          maxAbs={maxAbs}
          isPositive
          valueGetter={isW ? wNew : aNew}
          formatValue={fmtVal}
          subLabelGetter={(r) =>
            `${formatShares(r.curr_positions)} шт` +
            (r.curr_weight !== null ? ` · ${r.curr_weight.toFixed(2)}%` : '')}
          onItemClick={onRowClick}
        />
      )}

      {review.sold_out.length > 0 && (
        <SnapshotSection
          title="ПОЛНОСТЬЮ ВЫШЕЛ"
          count={review.sold_out.length}
          total={isW ? Math.abs(sumBy(review.sold_out, wSold)) : review.totals!.total_sold_out_rub}
          items={sortByAbs(review.sold_out, isW ? wSold : aSold)}
          maxAbs={maxAbs}
          isPositive={false}
          valueGetter={isW ? wSold : aSold}
          formatValue={fmtVal}
          subLabelGetter={(r) => `было ${formatShares(r.prev_positions)} шт`}
          onItemClick={onRowClick}
        />
      )}

      {review.previous_snapshot_date
        && review.added.length === 0
        && review.reduced.length === 0
        && review.new.length === 0
        && review.sold_out.length === 0 && (
        <div style={{ padding: 32, textAlign: 'center', color: 'var(--text-tertiary)', fontSize: 'var(--fs-sm)' }}>
          Состав не изменился между снапшотами.
        </div>
      )}
    </div>
  );
}

function SnapshotSection({
  title,
  count,
  total,
  items,
  maxAbs,
  isPositive,
  valueGetter,
  subLabelGetter,
  onItemClick,
  formatValue,
}: {
  title: string;
  count: number;
  total: number;
  items: FundDiffRow[];
  maxAbs: number;
  isPositive: boolean;
  valueGetter: (r: FundDiffRow) => number;
  subLabelGetter: (r: FundDiffRow) => string;
  onItemClick?: (r: FundDiffRow) => void;
  formatValue?: (absValue: number) => string;
}) {
  const fmt = formatValue ?? ((v: number) => formatRubShort(v));
  const [expanded, setExpanded] = useState(false);
  const displayed = expanded ? items : items.slice(0, 3);
  const reveal = useGrowReveal(
    displayed.length,
    `${title}|${displayed.length}|${items[0]?.asset_name ?? ''}|${maxAbs.toFixed(0)}`,
  );
  return (
    <div>
      <div
        style={{
          display: 'flex',
          alignItems: 'baseline',
          justifyContent: 'space-between',
          marginBottom: 10,
          borderBottom: '1.5px solid var(--text-primary)',
          paddingBottom: 6,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 10, minWidth: 0 }}>
          <span style={{ fontSize: 'var(--fs-xs)', fontWeight: 700, letterSpacing: '0.06em', color: 'var(--text-primary)', flexShrink: 0 }}>
            {title}
          </span>
          <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-tertiary)' }}>
            {count} {count === 1 ? 'позиция' : count < 5 ? 'позиции' : 'позиций'}
          </span>
        </div>
        <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: isPositive ? 'var(--mood-green)' : 'var(--mood-red)', fontVariantNumeric: 'tabular-nums', flexShrink: 0 }}>
          {isPositive ? '+' : '−'}{fmt(Math.abs(total))}
        </span>
      </div>
      <div>
        {displayed.map((r, i) => (
          <SnapshotBar
            key={`${r.asset_name}-${r.isin || ''}`}
            label={fundAssetName(r.asset_name, r.isin)}
            subLabel={subLabelGetter(r)}
            amount={valueGetter(r)}
            maxAbs={maxAbs}
            isPositive={isPositive}
            onClick={onItemClick ? () => onItemClick(r) : undefined}
            formatValue={formatValue}
            progress={reveal[i] ?? 1}
          />
        ))}
        {items.length > 3 && (
          <button
            onClick={() => setExpanded((e) => !e)}
            className="editorial-press"
            style={{
              marginTop: 10,
              padding: '6px 14px',
              background: 'var(--bg-secondary)',
              border: '1.5px solid var(--border-color)',
              borderRadius: 999,
              fontSize: 'var(--fs-xs)',
              fontWeight: 600,
              color: 'var(--text-secondary)',
              cursor: 'pointer',
            }}
          >
            {expanded ? '↑ Свернуть' : `Показать все · ${items.length} ↓`}
          </button>
        )}
      </div>
    </div>
  );
}

// Горизонтальный бар (растёт от левого края) — мобильная компоновка строки
// снапшота: имя+sublabel сверху, бар+значение снизу (компактнее десктоп-grid).
function SnapshotBar({
  label,
  subLabel,
  amount,
  maxAbs,
  isPositive,
  onClick,
  formatValue,
  progress = 1,
}: {
  label: string;
  subLabel?: string;
  amount: number;
  maxAbs: number;
  isPositive: boolean;
  onClick?: () => void;
  formatValue?: (absValue: number) => string;
  progress?: number;
}) {
  const fmt = formatValue ?? ((v: number) => formatRubShort(v));
  const widthPct = maxAbs > 0 ? Math.max(2, (Math.abs(amount) / maxAbs) * 100) : 2;
  const color = isPositive ? 'var(--mood-green)' : 'var(--mood-red)';
  return (
    <div
      onClick={onClick}
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
      onKeyDown={onClick ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onClick(); } } : undefined}
      style={{
        padding: '8px 8px',
        margin: '0 -8px',
        borderRadius: 6,
        cursor: onClick ? 'pointer' : 'default',
        borderBottom: '1px solid var(--border-soft, rgba(0,0,0,0.06))',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 8 }}>
        <span
          style={{
            flex: 1,
            minWidth: 0,
            fontSize: 'var(--fs-sm)',
            color: 'var(--text-primary)',
            fontWeight: 500,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {label}
        </span>
        <span style={{ fontSize: 'var(--fs-sm)', textAlign: 'right', color, fontWeight: 700, fontVariantNumeric: 'tabular-nums', flexShrink: 0 }}>
          {isPositive ? '+' : '−'}{fmt(Math.abs(amount))}
        </span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 5 }}>
        <div style={{ flex: 1, height: 6, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 3, overflow: 'hidden' }}>
          <div style={{ width: `${widthPct * progress}%`, height: '100%', background: color, borderRadius: 3 }} />
        </div>
        {subLabel && (
          <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-tertiary)', flexShrink: 0, whiteSpace: 'nowrap' }}>
            {subLabel}
          </span>
        )}
      </div>
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div
      style={{
        padding: 28,
        textAlign: 'center',
        background: 'var(--bg-secondary)',
        border: '1.5px dashed var(--border-color)',
        borderRadius: 12,
        margin: '8px 0',
      }}
    >
      <Activity size={26} style={{ color: 'var(--text-muted)', margin: '0 auto 10px' }} />
      <p style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', margin: 0, lineHeight: 1.5 }}>
        {message}
      </p>
    </div>
  );
}
