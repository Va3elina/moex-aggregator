
import type {
  Instrument,
  ChartResponse,
  InstrumentGroup,
  StatsResponse,
  TopInstrumentsResponse
} from '../types';

// Для Cloudflare Tunnel и локалки используем относительный /api через Vite proxy.
// При необходимости можно переопределить через VITE_API_BASE.
const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? '';

/**
 * Проверяет, истекает ли JWT через < 60 сек.
 */
function isTokenExpiringSoon(token: string): boolean {
  try {
    const payload = JSON.parse(atob(token.split('.')[1]));
    return payload.exp * 1000 - Date.now() < 60_000;
  } catch { return true; }
}

/**
 * Mutex для refresh — только один refresh за раз.
 * Параллельные запросы ждут тот же промис.
 */
let refreshPromise: Promise<string | null> | null = null;

async function ensureFreshToken(): Promise<string | null> {
  const token = localStorage.getItem('access_token');
  if (!token) return null;
  if (!isTokenExpiringSoon(token)) return token;

  // Если refresh уже идёт — ждём его
  if (refreshPromise) return refreshPromise;

  refreshPromise = (async () => {
    const refreshToken = localStorage.getItem('refresh_token');
    if (!refreshToken) return null;
    try {
      const resp = await fetch('/api/auth/refresh', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: refreshToken }),
      });
      if (resp.ok) {
        const tokens = await resp.json();
        localStorage.setItem('access_token', tokens.access_token);
        localStorage.setItem('refresh_token', tokens.refresh_token);
        return tokens.access_token as string;
      }
    } catch { /* ignore */ }
    return null;
  })().finally(() => { refreshPromise = null; });

  return refreshPromise;
}

/**
 * Обёртка над fetch с авторизацией и проактивным refresh.
 */
export async function apiFetch(url: string, init?: RequestInit): Promise<Response> {
  const token = await ensureFreshToken() || localStorage.getItem('access_token');
  const headers: Record<string, string> = {};
  if (token) headers['Authorization'] = `Bearer ${token}`;
  if (init?.headers) Object.assign(headers, init.headers);

  let response = await fetch(url, { ...init, headers });

  // Если всё равно 401 — пробуем refresh и retry
  if (response.status === 401 && token) {
    localStorage.removeItem('access_token'); // форсируем refresh
    const newToken = await ensureFreshToken();
    if (newToken) {
      const retryHeaders: Record<string, string> = { Authorization: `Bearer ${newToken}` };
      if (init?.headers) Object.assign(retryHeaders, init.headers);
      response = await fetch(url, { ...init, headers: retryHeaders });
    }
  }

  if (response.status === 403) {
    const data = await response.json().catch(() => ({ detail: 'Доступ ограничен' }));
    throw new Error(data.detail || 'Для доступа необходима авторизация');
  }

  return response;
}

// ==================== ИНСТРУМЕНТЫ ====================

export async function getInstruments(
  type?: string,
  group?: string
): Promise<{ instruments: Instrument[] }> {
  let url = `${API_BASE}/api/instruments`;
  const params = new URLSearchParams();

  if (type) params.append('type', type);
  if (group) params.append('group', group);

  if (params.toString()) {
    url += `?${params.toString()}`;
  }

  const response = await fetch(url);
  if (!response.ok) throw new Error('Failed to fetch instruments');

  const data = await response.json();

  if (Array.isArray(data)) {
    return { instruments: data };
  }
  return data;
}

export async function getGroups(): Promise<{ groups: string[] }> {
  const response = await fetch(`${API_BASE}/api/instruments/groups`);
  if (!response.ok) throw new Error('Failed to fetch groups');

  const data = await response.json();

  if (Array.isArray(data)) {
    const groups = [...new Set(data.map((g: InstrumentGroup) => g.name || g.sectype))];
    return { groups };
  }
  return data;
}

export async function getInstrumentGroups(): Promise<InstrumentGroup[]> {
  const response = await fetch(`${API_BASE}/api/instruments/groups`);
  if (!response.ok) throw new Error('Failed to fetch instrument groups');
  return response.json();
}

// ==================== ГРАФИК ====================

export async function getChartData(
  secId: string,
  sectype: string,
  instType: string,
  interval: number,
  clgroup: string,
  showOi: boolean = true,
  period: string = '6m'
): Promise<ChartResponse> {
  const params = new URLSearchParams({
    sectype,
    inst_type: instType,
    interval: interval.toString(),
    clgroup,
    show_oi: showOi.toString(),
    period
  });

  const response = await apiFetch(`${API_BASE}/api/chart/${secId}?${params}`);
  if (!response.ok) throw new Error('Failed to fetch chart data');
  return response.json();
}

// ==================== СТАТИСТИКА ====================

export async function getStats(
  period: string = '1w',
  clgroup: string = 'FIZ'
): Promise<StatsResponse> {
  const params = new URLSearchParams({
    period,
    clgroup,
  });

  const response = await fetch(`${API_BASE}/api/stats?${params}`);
  if (!response.ok) throw new Error('Failed to fetch stats');
  return response.json();
}

export async function getTopInstruments(
  period: string = '1w',
  clgroup: string = 'FIZ',
  limit: number = 10,
  sortBy: string = 'oi'
): Promise<TopInstrumentsResponse> {
  const params = new URLSearchParams({
    period,
    clgroup,
    limit: limit.toString(),
    sort_by: sortBy,
  });

  const response = await fetch(`${API_BASE}/api/stats/top?${params}`);
  if (!response.ok) throw new Error('Failed to fetch top instruments');
  return response.json();
}

// ==================== КАРТА РЫНКА ====================

export interface HeatmapStock {
  secId: string;
  name: string;
  sector: string;
  price: number;
  change_1d: number;
  change_1w: number;
  change_1m: number;
  change_1y: number;
  volume_1d: number;
  volume_1w: number;
  volume_1m: number;
  value_1d: number;
  value_1w: number;
  value_1m: number;
  market_cap: number;
  weight?: number;
}

export interface HeatmapSector {
  name: string;
  stocks: HeatmapStock[];
  totalValue: number;
}

export interface HeatmapResponse {
  stocks: HeatmapStock[];
  sectors: HeatmapSector[];
  params: {
    size_by: string;
    color_by: string;
    group_by: string;
  };
}

export async function getHeatmapData(
  sizeBy: string = 'value_1d',
  colorBy: string = 'change_1d',
  groupBy: string = 'sector'
): Promise<HeatmapResponse> {
  const params = new URLSearchParams({
    size_by: sizeBy,
    color_by: colorBy,
    group_by: groupBy
  });
  const response = await fetch(`${API_BASE}/api/heatmap/stocks?${params}`);
  if (!response.ok) throw new Error('Failed to fetch heatmap data');
  return response.json();
}

export async function getHeatmapImoex(
  colorBy: string = 'change_1w',
  groupBy: string = 'sector'
): Promise<HeatmapResponse> {
  const params = new URLSearchParams({
    color_by: colorBy,
    group_by: groupBy
  });
  const response = await fetch(`${API_BASE}/api/heatmap/imoex?${params}`);
  if (!response.ok) throw new Error('Failed to fetch IMOEX heatmap');
  return response.json();
}

// ==================== ФОНДЫ ====================

export interface FundDataPoint {
  date: string;
  nav: number | null;
}

export interface FundInfo {
  fund_id: number;
  ticker: string;
  name: string;
  subcategory?: string | null;
  uk_id?: string | null;
  data: FundDataPoint[];
}

export interface FundHolding {
  name: string;
  weight: number;
}

export interface FundHoldingsResponse {
  fund_id: number;
  holdings: FundHolding[];
}

export interface IndexDataPoint {
  date: string;
  close: number | null;
}

export interface FundsChartResponse {
  category: string;
  category_name: string;
  period: string;
  funds: FundInfo[];
  total_nav: FundDataPoint[];
  index: {
    secid: string;
    data: IndexDataPoint[];
  };
}

export interface FundsSummaryCategory {
  category: string;
  name: string;
  index: string;
  funds_count: number;
  total_nav: number;
  total_nav_formatted: string;
  change_pct: number;
  last_date: string | null;
}

export interface FundsSummaryResponse {
  categories: FundsSummaryCategory[];
}

export type FundCategory = 'money_market' | 'stocks' | 'bonds' | 'gold';
export type FundPeriod = '1w' | '1m' | '3m' | '6m' | '1y' | '2y' | '3y' | 'all';

export async function getFundsChartData(
  category: FundCategory,
  period: FundPeriod = '6m'
): Promise<FundsChartResponse> {
  const params = new URLSearchParams({
    category,
    period
  });
  const response = await apiFetch(`${API_BASE}/api/funds/chart?${params}`);
  if (!response.ok) throw new Error('Failed to fetch funds chart data');
  return response.json();
}

export async function getFundsSummary(): Promise<FundsSummaryResponse> {
  const response = await fetch(`${API_BASE}/api/funds/summary`);
  if (!response.ok) throw new Error('Failed to fetch funds summary');
  return response.json();
}

export async function getFundsCategories() {
  const response = await fetch(`${API_BASE}/api/funds/categories`);
  if (!response.ok) throw new Error('Failed to fetch funds categories');
  return response.json();
}

// Flows (притоки/оттоки)
export type FlowTimeframe = '1d' | '1w' | '1m' | '3m' | '1y';

export interface FlowDataPoint {
  period_start: string;
  period_end: string;
  flow: number;      // В млрд рублей
  flow_pct: number;  // В процентах
}

export interface FundsFlowsResponse {
  category: string;
  timeframe: FlowTimeframe;
  period: string;
  flows: FlowDataPoint[];
}

export async function getFundsFlows(
  category: FundCategory,
  timeframe: FlowTimeframe = '1w',
  period: FundPeriod = '1y',
  fundIds?: number[]
): Promise<FundsFlowsResponse> {
  const params = new URLSearchParams({ category, timeframe, period });
  if (fundIds && fundIds.length > 0) {
    params.set('fund_ids', fundIds.join(','));
  }
  const response = await apiFetch(`${API_BASE}/api/funds/flows?${params}`);
  if (!response.ok) throw new Error('Failed to fetch funds flows');
  return response.json();
}

// ==================== FUND HOLDINGS ====================

export async function getFundHoldings(fundId: number): Promise<FundHoldingsResponse> {
  const response = await apiFetch(`${API_BASE}/api/funds/holdings/${fundId}`);
  if (!response.ok) throw new Error('Failed to fetch fund holdings');
  return response.json();
}

// ==================== FUND CATALOG ====================

export interface CatalogFund {
  fund_id: number;
  ticker: string;
  name: string;
  category: string;
  subcategory: string | null;
  uk_id: string | null;
  last_nav: number | null;
  last_pay: number | null;
  last_date: string | null;
  return_1m: number | null;
  return_3m: number | null;
  return_6m: number | null;
  return_1y: number | null;
  holdings_count: number;
  top_holdings: FundHolding[];
}

export interface FundsCatalogResponse {
  funds: CatalogFund[];
  total: number;
}

export async function getFundsCatalog(): Promise<FundsCatalogResponse> {
  const response = await apiFetch(`${API_BASE}/api/funds/catalog`);
  if (!response.ok) throw new Error('Failed to fetch funds catalog');
  return response.json();
}

// ==================== FEAR INDEX ====================

export interface FearIndexComponents {
  rotation_ratio: number;
  money_market_flow: number;
  stocks_flow: number;
  velocity: number;
}

export interface FearIndexRawValues {
  rotation_ratio: number;
  mm_flow_pct: number;
  stocks_flow_pct: number;
  velocity: number;
}

export interface FearIndexTotals {
  money_market_nav: number;
  stocks_nav: number;
}

export interface FearIndexResponse {
  date: string;
  fear_index: number;
  classification: string;
  classification_ru: string;
  components: FearIndexComponents;
  raw_values: FearIndexRawValues;
  totals: FearIndexTotals;
  error?: string;
}

export interface FearIndexHistoryPoint {
  date: string;
  fear_index: number;
  rotation_ratio: number;
  mm_nav: number;
  stocks_nav: number;
}

export interface FearIndexHistoryResponse {
  period: string;
  count: number;
  history: FearIndexHistoryPoint[];
  error?: string;
}

export type FearIndexPeriod = '1m' | '3m' | '6m' | '1y' | 'all';

export async function getFearIndex(): Promise<FearIndexResponse> {
  const response = await fetch(`${API_BASE}/api/funds/fear-index`);
  if (!response.ok) throw new Error('Failed to fetch fear index');
  return response.json();
}

export async function getFearIndexHistory(
  period: FearIndexPeriod = '3m'
): Promise<FearIndexHistoryResponse> {
  const params = new URLSearchParams({ period });
  const response = await apiFetch(`${API_BASE}/api/funds/fear-index/history?${params}`);
  if (!response.ok) throw new Error('Failed to fetch fear index history');
  return response.json();
}

// ==================== MARKET BREADTH ====================

export interface BreadthStock {
  ticker: string;
  sector: string;
  price: number;
  ema: number;
  is_above: boolean;
  diff_percent: number;
}

export interface BreadthCurrentResponse {
  percent_above: number;
  count_above: number;
  count_total: number;
  ema_period: number;
  classification: 'overbought' | 'bullish' | 'neutral' | 'oversold';
  stocks: BreadthStock[];
}

export interface BreadthHistoryPoint {
  date: string;
  percent_above: number;
  count_above: number;
  count_total: number;
  imoex?: number;  // IMOEX close price for this date
}

export interface BreadthHistoryResponse {
  ema_period: number;
  data: BreadthHistoryPoint[];
  imoex: { date: string; close: number }[];
}

export type BreadthUniverse = 'all' | 'imoex';

export async function getBreadthCurrent(
  emaPeriod: number = 200,
  universe: BreadthUniverse = 'all',
): Promise<BreadthCurrentResponse> {
  const params = new URLSearchParams({ ema_period: emaPeriod.toString(), universe });
  const response = await fetch(`${API_BASE}/api/breadth/current?${params}`);
  if (!response.ok) throw new Error('Failed to fetch market breadth');
  return response.json();
}

export async function getBreadthHistory(
  emaPeriod: number = 200,
  days: number = 365,
  universe: BreadthUniverse = 'all',
): Promise<BreadthHistoryResponse> {
  const params = new URLSearchParams({
    ema_period: emaPeriod.toString(),
    days: days.toString(),
    universe,
  });
  const response = await apiFetch(`${API_BASE}/api/breadth/history?${params}`);
  if (!response.ok) throw new Error('Failed to fetch breadth history');
  return response.json();
}

// ==================== BUFFETT INDICATOR ====================

export interface BuffettCapGdpPoint {
  date: string;
  buffett: number;
  buffett_raw: number;
  cap: number;
  gdp_ttm: number;
}

export interface BuffettCapGdpResponse {
  data: BuffettCapGdpPoint[];
  period: string;
}

export interface BuffettMcftrM2Point {
  date: string;
  ratio: number;
  mcftr: number;
  m2: number;
}

export interface BuffettMcftrM2Response {
  data: BuffettMcftrM2Point[];
  period: string;
}

export type BuffettPeriod = '1m' | '1y' | '2y' | '3y' | '5y' | '10y' | '20y' | 'all';

export async function getBuffettCapGdp(
  period: BuffettPeriod = '3y',
  smooth: boolean = true,
  timeframe: string = '1m'
): Promise<BuffettCapGdpResponse> {
  const params = new URLSearchParams({
    period,
    smooth: smooth.toString(),
    timeframe
  });
  const response = await apiFetch(`${API_BASE}/api/buffett/cap-gdp?${params}`);
  if (!response.ok) throw new Error('Failed to fetch Buffett cap/gdp');
  return response.json();
}

export async function getBuffettMcftrM2(
  period: BuffettPeriod = '3y',
  smooth: boolean = true
): Promise<BuffettMcftrM2Response> {
  const params = new URLSearchParams({ period, smooth: smooth.toString() });
  const response = await apiFetch(`${API_BASE}/api/buffett/mcftr-m2?${params}`);
  if (!response.ok) throw new Error('Failed to fetch Buffett mcftr/m2');
  return response.json();
}

// ==================== СЕЗОННОСТЬ ====================

export interface SeasonalityBar {
  label: string;
  key: number;
  avg_change: number;
  count: number;
}

export interface SeasonalityResponse {
  secid: string;
  mode: string;
  iterations: number;
  exclude_dividends: boolean;
  ex_dates_count: number;
  bars: SeasonalityBar[];
}

export type SeasonalityMode = 'intraday' | 'weekday' | 'monthday' | 'monthly';

export async function getSeasonality(
  secid: string,
  mode: SeasonalityMode = 'weekday',
  iterations: number = 90,
  excludeDividends: boolean = false,
): Promise<SeasonalityResponse> {
  const params = new URLSearchParams({
    secid,
    mode,
    iterations: iterations.toString(),
    exclude_dividends: excludeDividends.toString(),
  });
  const response = await apiFetch(`${API_BASE}/api/seasonality?${params}`);
  if (!response.ok) throw new Error('Failed to fetch seasonality');
  return response.json();
}

export interface PricePoint {
  date: string;
  close: number;
  adjusted: number;
}

export interface PriceChartResponse {
  secid: string;
  days: number;
  ex_dates_count: number;
  data: PricePoint[];
}

export async function getSeasonalityPrice(
  secid: string,
  days: number = 365,
): Promise<PriceChartResponse> {
  const params = new URLSearchParams({ secid, days: days.toString() });
  const response = await apiFetch(`${API_BASE}/api/seasonality/price?${params}`);
  if (!response.ok) throw new Error('Failed to fetch price chart');
  return response.json();
}