
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
    // 403 — это либо tier-restriction («Доступно на тарифе Basic...»),
    // либо отсутствие auth-token'а. Backend всегда возвращает detail
    // с конкретным текстом — пробрасываем его как есть.
    const data = await response.json().catch(() => ({ detail: 'Доступ ограничен' }));
    throw new Error(data.detail || 'Доступ ограничен');
  }

  return response;
}

// ==================== AUTH ====================

/**
 * Кастомный error с HTTP status code — нужен AddEmailPage чтобы отличить
 * 409 (email занят другим аккаунтом → подсказать войти через другой провайдер)
 * от 400/500 (показать generic сообщение).
 */
export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = 'ApiError';
  }
}

/**
 * Парсит error-ответ backend'а с учётом кастомной middleware-обёртки
 * `{"success":false,"error":{"code":N,"message":"..."}}` (api/middleware.py).
 * FastAPI default `{detail:"..."}` НЕ возвращается — все HTTPException
 * проходят через http_exception_handler middleware.
 */
async function parseApiError(response: Response, fallback: string): Promise<string> {
  try {
    const data = await response.json();
    if (data?.error?.message && typeof data.error.message === 'string') return data.error.message;
    if (typeof data?.detail === 'string') return data.detail;
  } catch {
    // Response body не JSON
  }
  return fallback;
}

/**
 * Привязка реального email к OAuth-аккаунту (для юзеров с synthetic email
 * вроде telegram_123@oauth.local). После успеха AuthContext'у нужно сделать
 * refreshUser() чтобы подтянуть свежий /me — requires_email_setup станет false.
 */
export async function addEmail(email: string): Promise<void> {
  const response = await apiFetch(`${API_BASE}/api/auth/add-email`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email }),
  });
  if (!response.ok) {
    const message = await parseApiError(response, 'Не удалось привязать email');
    throw new ApiError(response.status, message);
  }
}

/**
 * Подтверждение email 6-значным кодом из письма. После успеха AuthContext'у
 * нужно refreshUser() — is_verified станет true, баннер в профиле исчезнет.
 */
export async function verifyEmail(code: string): Promise<void> {
  const response = await apiFetch(`${API_BASE}/api/auth/verify-email`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code }),
  });
  if (!response.ok) {
    const message = await parseApiError(response, 'Не удалось подтвердить email');
    throw new ApiError(response.status, message);
  }
}

/** Повторная отправка кода подтверждения. Бэкенд держит кулдаун 60с → 429. */
export async function resendVerification(): Promise<void> {
  const response = await apiFetch(`${API_BASE}/api/auth/resend-verification`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  });
  if (!response.ok) {
    const message = await parseApiError(response, 'Не удалось отправить код');
    throw new ApiError(response.status, message);
  }
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

/**
 * Получить один инструмент по sec_id (для резолва имени по тикеру
 * когда он пришёл из URL-параметра, например `?instrument=IMOEXF`).
 */
export async function getInstrument(secId: string): Promise<Instrument | null> {
  try {
    const response = await fetch(`${API_BASE}/api/instruments/${encodeURIComponent(secId)}`);
    if (!response.ok) return null;
    return await response.json();
  } catch {
    return null;
  }
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
  prev_close: number;
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
  updated_at?: string;
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
  // apiFetch (не голый fetch) — endpoint /stocks tier-gated (Basic+).
  // Без Authorization-заголовка backend видит guest → 403 даже у admin'а.
  const response = await apiFetch(`${API_BASE}/api/heatmap/stocks?${params}`);
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
  // apiFetch для единообразия — /imoex бесплатный, но токен не помешает.
  const response = await apiFetch(`${API_BASE}/api/heatmap/imoex?${params}`);
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
  /** Tier-gated: фонд недоступен в текущем тарифе (Free показывает только
   *  whitelist'овые с данными, остальные приходят с tier_locked=true и
   *  пустым data). Frontend рендерит их затемнёнными в FundsTable. */
  tier_locked?: boolean;
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

export type FundCategory = 'money_market' | 'stocks' | 'bonds' | 'gold' | 'yuan';
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
  flow: number;       // Нетто (млрд ₽)
  gross_in: number;   // Суммарный приток (>= 0, млрд ₽)
  gross_out: number;  // Суммарный отток (<= 0, млрд ₽)
  flow_pct: number;   // Нетто в %
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

export type BreadthUniverse = 'all' | 'imoex' | 'all_usd' | 'imoex_usd';

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

export interface BuffettRatioPoint {
  date: string;
  ratio: number;
  m2: number;
  cap?: number;
}

export interface BuffettRatioResponse {
  data: BuffettRatioPoint[];
  period: string;
}

export type BuffettPeriod = '1m' | '1y' | '2y' | '3y' | '5y' | '10y' | '20y' | 'all';

/** Извлекает человекочитаемый текст ошибки из 4xx-ответа buffett-эндпоинтов.
 *  Backend оборачивает ошибки в envelope {success:false,error:{code,message}};
 *  legacy-роуты — в {detail}. Парсим оба. Без этого период «Всё» на free-
 *  тарифе показывал «Ошибка загрузки данных» вместо upgrade-модалки —
 *  tier-403 сообщение терялось и фронт не отличал его от реального сбоя. */
async function buffettErrorDetail(response: Response, fallback: string): Promise<string> {
  try {
    const body = await response.json();
    // Legacy FastAPI-формат: {detail: "..."}
    if (body && typeof body.detail === 'string' && body.detail) return body.detail;
    // Текущий envelope ошибок: {success:false, error:{code, message}}
    if (body?.error && typeof body.error.message === 'string' && body.error.message) {
      return body.error.message;
    }
  } catch { /* не JSON — отдаём fallback */ }
  return `Failed to fetch Buffett ${fallback}`;
}

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
  if (!response.ok) throw new Error(await buffettErrorDetail(response, 'cap/gdp'));
  return response.json();
}

export async function getBuffettCapM2(
  period: BuffettPeriod = '3y',
  smooth: boolean = true,
  timeframe: string = '1m'
): Promise<BuffettRatioResponse> {
  const params = new URLSearchParams({ period, smooth: smooth.toString(), timeframe });
  const response = await apiFetch(`${API_BASE}/api/buffett/cap-m2?${params}`);
  if (!response.ok) throw new Error(await buffettErrorDetail(response, 'cap/m2'));
  return response.json();
}

// ==================== СЕЗОННОСТЬ ====================

export interface SeasonalityBar {
  label: string;
  key: number;
  avg_change: number;
  /** Стандартное отклонение по годам. Присутствует в monthly режиме. */
  std_change?: number;
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

export interface SeasonalityFilters {
  /** Учитывать годы ≥ значения (для «с 2015 г.») */
  sinceYear?: number;
  /** Исключить конкретные годы (legacy — оставлен для обратной совместимости) */
  excludeYears?: number[];
  /** Тип агрегации: 'avg' (по умолчанию) или 'median'.
   *  Кнопка «Без выбросов» теперь = aggType='median' (устойчива к выбросам). */
  aggType?: 'avg' | 'median';
}

export async function getSeasonality(
  secid: string,
  mode: SeasonalityMode = 'weekday',
  iterations: number = 90,
  excludeDividends: boolean = false,
  filters?: SeasonalityFilters,
): Promise<SeasonalityResponse> {
  const params = new URLSearchParams({
    secid,
    mode,
    iterations: iterations.toString(),
    exclude_dividends: excludeDividends.toString(),
  });
  if (filters?.sinceYear) params.set('since_year', filters.sinceYear.toString());
  if (filters?.excludeYears?.length) params.set('exclude_years', filters.excludeYears.join(','));
  if (filters?.aggType) params.set('agg_type', filters.aggType);
  const response = await apiFetch(`${API_BASE}/api/seasonality?${params}`);
  if (!response.ok) throw new Error('Failed to fetch seasonality');
  return response.json();
}

export interface PricePoint {
  date: string;
  close: number;
  adjusted: number;
}

export interface DividendPoint {
  date: string;
  value: number;
}

export interface PriceChartResponse {
  secid: string;
  days: number;
  ex_dates_count: number;
  dividends: DividendPoint[];
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

// --- Yearly seasonality ---

export interface YearlySeasonalityPoint {
  td: number;  // bucket index (0..251, calendar-aligned)
  month: number;
  avg_pct: number;
  /** Стандартное отклонение по годам в этом bucket'е (для ±1σ полосы) */
  std_pct?: number;
  years_count: number;
}

export interface YearlyCurrentPoint {
  td: number;
  month: number;
  pct: number;
  date: string;
}

export interface YearlySeasonalityResponse {
  secid: string;
  exclude_dividends: boolean;
  since_year?: number | null;
  excluded_years?: number[];
  average: YearlySeasonalityPoint[];
  current: YearlyCurrentPoint[];
  years_range: string;
  current_year: number;
  max_trading_days: number;
}

export interface AvailableYearsResponse {
  secid: string;
  min_year: number;
  max_year: number;
  years: number[];
}

export async function getSeasonalityYears(secid: string): Promise<AvailableYearsResponse> {
  const response = await apiFetch(`${API_BASE}/api/seasonality/available-years?secid=${encodeURIComponent(secid)}`);
  if (!response.ok) throw new Error('Failed to fetch available years');
  return response.json();
}

// MTD (Month-To-Date / "Зум на месяц") endpoint и types удалены —
// режим убран из UI Сезонности. Если потребуется вернуть, см. git history.

export async function getSeasonalityYearly(
  secid: string,
  excludeDividends: boolean = false,
  filters?: SeasonalityFilters,
): Promise<YearlySeasonalityResponse> {
  const params = new URLSearchParams({
    secid,
    exclude_dividends: excludeDividends.toString(),
  });
  if (filters?.sinceYear) params.set('since_year', filters.sinceYear.toString());
  if (filters?.excludeYears?.length) params.set('exclude_years', filters.excludeYears.join(','));
  if (filters?.aggType) params.set('agg_type', filters.aggType);
  const response = await apiFetch(`${API_BASE}/api/seasonality/yearly?${params}`);
  if (!response.ok) throw new Error('Failed to fetch yearly seasonality');
  return response.json();
}

// ═══════════════════════════════════════════════════════════════════
// Analytics — admin-only stats endpoint
// ═══════════════════════════════════════════════════════════════════

export interface AnalyticsStatsSummary {
  dau: number;
  sessions: number;
  events: number;
  avg_session_sec: number;
  delta_dau: number | null;
  delta_sessions_pct: number | null;
  delta_events_pct: number | null;
  delta_avg_session_sec: number | null;
}

export interface AnalyticsStats {
  period_days: number;
  segment: string;
  device: string;
  summary: AnalyticsStatsSummary;
  trends: { date: string; dau: number; sessions: number }[];
  top_pages: { path: string; views: number }[];
  top_instruments: { secid: string; selects: number }[];
  top_exports: { indicator: string; count: number }[];
  mode_distribution: { mode: string; count: number }[];
}

export async function getAnalyticsStats(opts: {
  days?: number;
  segment?: string;
  device?: string;
}): Promise<AnalyticsStats> {
  const params = new URLSearchParams();
  if (opts.days !== undefined) params.set('days', String(opts.days));
  if (opts.segment) params.set('segment', opts.segment);
  if (opts.device) params.set('device', opts.device);
  const response = await apiFetch(`${API_BASE}/api/analytics/stats?${params}`);
  if (!response.ok) {
    if (response.status === 403) throw new Error('Доступ только для администратора');
    throw new Error('Failed to fetch analytics stats');
  }
  return response.json();
}

// ═══════════════════════════════════════════════════════════════════
// Funnel / Cohort / Realtime / A/B
// ═══════════════════════════════════════════════════════════════════

export interface FunnelResponse {
  period_days: number;
  steps: { step: number; label: string; sessions: number; conversion_pct: number }[];
}

export async function getAnalyticsFunnel(opts: {
  steps: string;
  days?: number;
}): Promise<FunnelResponse> {
  const params = new URLSearchParams({ steps: opts.steps });
  if (opts.days !== undefined) params.set('days', String(opts.days));
  const response = await apiFetch(`${API_BASE}/api/analytics/funnel?${params}`);
  if (!response.ok) {
    if (response.status === 403) throw new Error('Доступ только для администратора');
    throw new Error('Failed to fetch funnel');
  }
  return response.json();
}

export interface CohortRow {
  cohort_week: string;
  cohort_size: number;
  d1: number; d7: number; d30: number;
  d1_pct: number; d7_pct: number; d30_pct: number;
}

export interface CohortResponse {
  weeks: number;
  cohorts: CohortRow[];
}

export async function getAnalyticsCohort(weeks: number = 8): Promise<CohortResponse> {
  const response = await apiFetch(`${API_BASE}/api/analytics/cohort?weeks=${weeks}`);
  if (!response.ok) {
    if (response.status === 403) throw new Error('Доступ только для администратора');
    throw new Error('Failed to fetch cohort');
  }
  return response.json();
}

export interface RealtimeEvent {
  event_type: string;
  event_path: string | null;
  payload: Record<string, unknown> | null;
  server_ts: string;
  country: string | null;
  device: string | null;
}

export interface RealtimeResponse {
  minutes: number;
  active_sessions: number;
  active_pages: { path: string; sessions: number }[];
  recent_events: RealtimeEvent[];
  devices: { device: string; sessions: number }[];
}

export async function getAnalyticsRealtime(minutes: number = 5): Promise<RealtimeResponse> {
  const response = await apiFetch(`${API_BASE}/api/analytics/realtime?minutes=${minutes}`);
  if (!response.ok) {
    if (response.status === 403) throw new Error('Доступ только для администратора');
    throw new Error('Failed to fetch realtime');
  }
  return response.json();
}

export interface ABExperiment {
  name: string;
  description: string | null;
  variant_a: string;
  variant_b: string;
  traffic_split: number;
  active: boolean;
  created_at: string;
  total_assignments: number;
}

export async function listABExperiments(): Promise<{ experiments: ABExperiment[] }> {
  const response = await apiFetch(`${API_BASE}/api/analytics/experiments`);
  if (!response.ok) throw new Error('Failed to fetch experiments');
  return response.json();
}

export async function createABExperiment(exp: {
  name: string; description?: string;
  variant_a: string; variant_b: string;
  traffic_split: number; active: boolean;
}): Promise<{ status: string; name: string }> {
  const response = await apiFetch(`${API_BASE}/api/analytics/experiments`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(exp),
  });
  if (!response.ok) {
    if (response.status === 409) throw new Error('Эксперимент с таким именем уже существует');
    throw new Error('Failed to create experiment');
  }
  return response.json();
}

export async function deleteABExperiment(name: string): Promise<void> {
  const response = await apiFetch(`${API_BASE}/api/analytics/experiments/${encodeURIComponent(name)}`, {
    method: 'DELETE',
  });
  if (!response.ok && response.status !== 204) {
    throw new Error('Failed to delete experiment');
  }
}

export interface AssignResponse {
  experiment: string;
  variant: 'a' | 'b';
  label: string;
  active: boolean;
}

export async function getABAssignment(name: string): Promise<AssignResponse> {
  // Pass session_id as header — backend читает X-Session-Id для гостей
  const sessionId = (() => {
    try { return sessionStorage.getItem('frame_session_id') || ''; } catch { return ''; }
  })();
  const response = await apiFetch(`${API_BASE}/api/analytics/experiments/${encodeURIComponent(name)}/assign`, {
    headers: sessionId ? { 'X-Session-Id': sessionId } : {},
  });
  if (!response.ok) throw new Error('Failed to fetch assignment');
  return response.json();
}

// ═══════════════════════════════════════════════════════════════════
// Admin: Users panel
// ═══════════════════════════════════════════════════════════════════

export interface AdminUser {
  id: number;
  email: string;
  display_name: string | null;
  username: string | null;
  role: string;
  is_active: boolean;
  is_verified: boolean;
  created_at: string | null;
  last_login_at: string | null;
  oauth_provider: string | null;
  avatar_url: string | null;
  plan: string | null;
  sessions_count: number;
  events_count: number;
  last_active_ts: string | null;
}

export async function listAdminUsers(opts: {
  days?: number;
  sort?: string;
  search?: string;
}): Promise<{ period_days: number; users: AdminUser[] }> {
  const params = new URLSearchParams();
  if (opts.days !== undefined) params.set('days', String(opts.days));
  if (opts.sort) params.set('sort', opts.sort);
  if (opts.search) params.set('search', opts.search);
  const response = await apiFetch(`${API_BASE}/api/analytics/users?${params}`);
  if (!response.ok) {
    if (response.status === 403) throw new Error('Доступ только для администратора');
    throw new Error('Failed to fetch users');
  }
  return response.json();
}

export interface UserSubscription {
  id: number;
  tier: string;
  period: string;
  plan_id: string;
  amount: number;
  currency: string;
  status: string;
  started_at: string | null;
  expires_at: string | null;
  cancelled_at: string | null;
  created_at: string | null;
}

export interface UserTimelineEvent {
  event_type: string;
  event_path: string | null;
  payload: Record<string, unknown> | null;
  server_ts: string;
  country: string | null;
  device: string | null;
  session_id: string;
}

export interface UserDetailResponse {
  user: AdminUser & {
    last_login_ip?: string | null;
    oauth_id?: string | null;
    updated_at?: string | null;
  };
  subscriptions: UserSubscription[];
  summary: {
    period_days: number;
    events: number;
    sessions: number;
    first_active_ts: string | null;
    last_active_ts: string | null;
    avg_session_sec: number;
    total_time_sec: number;
  };
  timeline: UserTimelineEvent[];
  top_pages: { path: string; views: number }[];
  top_instruments: { secid: string; selects: number }[];
  top_exports: { indicator: string; count: number }[];
  devices: { device: string; sessions: number }[];
  countries: { country: string; sessions: number }[];
}

export async function getAdminUserDetail(userId: number, days: number = 30): Promise<UserDetailResponse> {
  const response = await apiFetch(`${API_BASE}/api/analytics/users/${userId}?days=${days}`);
  if (!response.ok) {
    if (response.status === 404) throw new Error('Пользователь не найден');
    if (response.status === 403) throw new Error('Доступ только для администратора');
    throw new Error('Failed to fetch user detail');
  }
  return response.json();
}

// ────────────────────────────────────────────────────────────────────────────
// CBR Flows — ОРФР Банка России (потоки участников биржевых торгов)
// ────────────────────────────────────────────────────────────────────────────

export type CbrInstrumentType = 'stocks' | 'ofz' | 'fx';

export interface CbrFlowsPeriod {
  year: number;
  label: string;          // 'I квартал', 'Апрель', ...
  kind: 'quarter' | 'month';
  end_date: string;       // ISO 'YYYY-MM-DD'
  values: Record<string, number>;  // {category: value_in_billion_rub}
}

export interface CbrFlowsResponse {
  instrument_type: CbrInstrumentType;
  instrument_label: string;
  categories: string[];   // в порядке для legend/stack
  periods: CbrFlowsPeriod[];
  source: string | null;  // 'ORFR_2026-4'
  updated_at: string | null;
  unit: string;           // 'млрд руб.'
  note: string;
}

/**
 * Потоки участников биржевых торгов по данным ЦБ (ОРФР).
 * type: 'stocks' (акции) | 'ofz' (ОФЗ) | 'fx' (валюты)
 */
export async function getCbrFlows(type: CbrInstrumentType): Promise<CbrFlowsResponse> {
  const response = await apiFetch(`${API_BASE}/api/cbr-flows?type=${type}`);
  if (!response.ok) {
    if (response.status === 404) throw new Error('Данные ЦБ ещё не загружены');
    throw new Error('Не удалось получить данные');
  }
  return response.json();
}

// ════════════════════════════════════════════════════════════════════════════
// API Keys (Pro tier) — personal programmatic access tokens
// ════════════════════════════════════════════════════════════════════════════

export interface ApiKeyInfo {
    id: number;
    name: string | null;
    key_prefix: string;
    mode: 'live' | 'test';
    created_at: string;
    last_used_at: string | null;
    is_revoked: boolean;
}

export interface ApiKeyCreated extends ApiKeyInfo {
    plain_key: string;  // показывается ОДИН раз
}

export async function listApiKeys(): Promise<ApiKeyInfo[]> {
    const resp = await apiFetch(`${API_BASE}/api/keys`);
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (!resp.ok) throw new Error('Failed to list API keys');
    return resp.json();
}

export async function createApiKey(
    name?: string,
    mode: 'live' | 'test' = 'live',
): Promise<ApiKeyCreated> {
    const resp = await apiFetch(`${API_BASE}/api/keys`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name || null, mode }),
    });
    if (resp.status === 403) {
        const data = await resp.json().catch(() => ({}));
        // Прокидываем backend-сообщение если есть, иначе generic.
        throw new Error(
            data?.error?.message
            || data?.detail
            || 'Доступно на тарифе Pro',
        );
    }
    if (resp.status === 400) {
        const data = await resp.json().catch(() => ({}));
        throw new Error(data?.error?.message || data?.detail || 'Не удалось создать ключ');
    }
    if (!resp.ok) throw new Error('Не удалось создать ключ');
    return resp.json();
}

export async function revokeApiKey(id: number): Promise<void> {
    const resp = await apiFetch(`${API_BASE}/api/keys/${id}`, { method: 'DELETE' });
    if (!resp.ok && resp.status !== 204) throw new Error('Не удалось отозвать ключ');
}

export interface ApiKeyUsageStats {
    days: number;
    total: number;
    by_day: { date: string; count: number }[];
}

export async function getApiKeyUsage(days = 30): Promise<ApiKeyUsageStats> {
    const resp = await apiFetch(`${API_BASE}/api/keys/usage?days=${days}`);
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (!resp.ok) throw new Error('Не удалось загрузить статистику');
    return resp.json();
}

// ════════════════════════════════════════════════════════════════════════════
// Fund Trades — отслеживание покупок/продаж в БПИФах (Pro tier)
// ════════════════════════════════════════════════════════════════════════════

export type FundTradesPeriod = '1m' | '3m' | '6m' | '1y';

export interface FundWithHistory {
    fund_id: number;
    ticker: string;
    name: string;
    uk: string | null;
    category: string;
    subcategory: string | null;
    last_snapshot_date: string | null;
    snapshot_count: number;
    snapshots_count?: number; // backward compat
    uk_id?: number | string | null;
}

export interface FundTradesHolding {
    asset_name: string;
    weight: number | null;
    positions: number | null;
    amount_rub: number | null;
}

export type FundTradeChangeType = 'new' | 'sold_out' | 'accumulated' | 'reduced' | 'unchanged';

export interface FundTradeDiff {
    asset_name: string;
    isin?: string | null;
    change_type: FundTradeChangeType;
    delta_weight: number | null;
    current_weight: number | null;
    previous_weight: number | null;
    current_positions: number | null;
    previous_positions: number | null;
}

export interface FundTradesDetail {
    fund: {
        fund_id: number;
        ticker: string;
        name: string;
        category: string;
        subcategory: string | null;
    };
    period: FundTradesPeriod;
    current_snapshot_date: string | null;
    previous_snapshot_date: string | null;
    current_holdings: FundTradesHolding[];
    diff: FundTradeDiff[];
    summary: {
        new: number;
        sold_out: number;
        accumulated: number;
        reduced: number;
    };
}

export interface FundTradesMover {
    akey: string;
    asset_name: string;
    total_delta_weight: number;
    total_delta_amount: number;
    funds_buying: number;
    funds_selling: number;
}

export interface FundTradesMovers {
    period: FundTradesPeriod;
    category: string | null;
    as_of: string | null;
    manager: string | null;
    sort: 'weight' | 'amount';
    available_months: string[];
    top_accumulated: FundTradesMover[];
    top_reduced: FundTradesMover[];
}

export async function listFundsWithHistory(): Promise<{ funds: FundWithHistory[]; count: number }> {
    const resp = await apiFetch(`${API_BASE}/api/fund-trades/funds`);
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (!resp.ok) throw new Error('Не удалось загрузить фонды');
    return resp.json();
}

export async function getFundTradesDetail(
    ticker: string,
    period: FundTradesPeriod = '3m',
): Promise<FundTradesDetail> {
    const resp = await apiFetch(
        `${API_BASE}/api/fund-trades/fund/${encodeURIComponent(ticker)}?period=${period}`,
    );
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (resp.status === 404) throw new Error(`Фонд ${ticker} не найден`);
    if (!resp.ok) throw new Error('Не удалось загрузить детали фонда');
    return resp.json();
}

export async function getFundTradesMovers(
    period: FundTradesPeriod = '1m',
    opts: { asOf?: string; manager?: string; sort?: 'weight' | 'amount'; limit?: number } = {},
): Promise<FundTradesMovers> {
    const { asOf, manager, sort = 'weight', limit = 20 } = opts;
    const params = new URLSearchParams({ period, sort, limit: String(limit) });
    if (asOf) params.set('as_of', asOf);
    if (manager) params.set('manager', manager);
    const resp = await apiFetch(`${API_BASE}/api/fund-trades/movers?${params}`);
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (!resp.ok) throw new Error('Не удалось загрузить топ движений');
    return resp.json();
}

export interface FundIntradayEvent {
    timestamp: string;
    asset_name: string;
    change_type: 'new' | 'sold_out' | 'accumulated' | 'reduced';
    delta_positions: number;
    current_positions: number | null;
    previous_positions: number | null;
    current_weight: number | null;
}

export interface FundIntradayDetail {
    fund: {
        fund_id: number;
        ticker: string;
        name: string;
        category: string;
        subcategory: string | null;
    };
    days: number;
    snapshots_count: number;
    events: FundIntradayEvent[];
    latest_timestamp: string | null;
    earliest_timestamp?: string;
}

export async function getFundIntraday(
    ticker: string,
    days = 7,
): Promise<FundIntradayDetail> {
    const resp = await apiFetch(
        `${API_BASE}/api/fund-trades/intraday/${encodeURIComponent(ticker)}?days=${days}`,
    );
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (resp.status === 404) throw new Error(`Фонд ${ticker} не поддерживает intraday`);
    if (!resp.ok) throw new Error('Не удалось загрузить intraday');
    return resp.json();
}

// ─── Snapshot review API ────────────────────────────────────────

export interface FundSnapshotItem {
    snapshot_date: string;
    asset_count: number;
}

export interface FundSnapshotsList {
    ticker: string;
    fund_name: string;
    snapshots: FundSnapshotItem[];
}

export interface FundHoldingRow {
    asset_name: string;
    isin: string | null;
    positions: number | null;
    amount_rub: number | null;
    weight: number | null;
}

export interface FundDiffRow {
    asset_name: string;
    isin: string | null;
    curr_positions: number | null;
    prev_positions: number | null;
    curr_amount_rub: number | null;
    prev_amount_rub: number | null;
    curr_weight: number | null;
    prev_weight: number | null;
    delta_positions: number | null;
    delta_amount_rub: number | null;
}

export interface FundSnapshotReview {
    fund: { ticker: string; name: string; category: string | null };
    current_snapshot_date: string;
    previous_snapshot_date: string | null;
    current_holdings: FundHoldingRow[];
    added: FundDiffRow[];
    reduced: FundDiffRow[];
    new: FundDiffRow[];
    sold_out: FundDiffRow[];
    totals: {
        current_assets: number;
        previous_assets: number;
        total_added_rub: number;
        total_reduced_rub: number;
        total_new_rub: number;
        total_sold_out_rub: number;
    };
}

export async function getFundSnapshots(ticker: string): Promise<FundSnapshotsList> {
    const resp = await apiFetch(
        `${API_BASE}/api/fund-trades/snapshots/${encodeURIComponent(ticker)}`,
    );
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (resp.status === 404) throw new Error(`Фонд ${ticker} не найден`);
    if (!resp.ok) throw new Error('Не удалось загрузить список снапшотов');
    return resp.json();
}

export async function getFundSnapshotReview(
    ticker: string,
    date?: string,
): Promise<FundSnapshotReview> {
    const params = date ? `?date=${date}` : '';
    const resp = await apiFetch(
        `${API_BASE}/api/fund-trades/snapshot/${encodeURIComponent(ticker)}${params}`,
    );
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (resp.status === 404) throw new Error('Снапшот не найден');
    if (!resp.ok) throw new Error('Не удалось загрузить обзор снапшота');
    return resp.json();
}

// ─── Asset history (drill-down) ─────────────────────────────

export interface AssetHistoryPoint {
    snapshot_date: string;
    positions: number | null;
    amount_rub: number | null;
    weight: number | null;
    price_rub: number | null;
    delta_positions: number | null;
    delta_amount_rub: number | null;
}

export interface AssetHistory {
    fund: { ticker: string; name: string };
    asset_name: string;
    isin: string | null;
    snapshots_count: number;
    first_seen: string;
    last_seen: string;
    timeline: AssetHistoryPoint[];
}

export async function getAssetHistory(
    ticker: string,
    opts: { assetName?: string; isin?: string },
): Promise<AssetHistory> {
    const params = new URLSearchParams();
    if (opts.isin) params.set('isin', opts.isin);
    else if (opts.assetName) params.set('asset_name', opts.assetName);
    else throw new Error('asset_name or isin required');

    const resp = await apiFetch(
        `${API_BASE}/api/fund-trades/asset-history/${encodeURIComponent(ticker)}?${params}`,
    );
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (resp.status === 404) throw new Error('История по позиции не найдена');
    if (!resp.ok) throw new Error('Не удалось загрузить историю');
    return resp.json();
}
