
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
  // НЕ выходим при отсутствии access_token: 401-retry в apiFetch сначала
  // удаляет протухший access и зовёт нас за новым — ранний return null делал
  // retry мёртвым кодом (401 не восстанавливался). Если есть refresh_token —
  // рефрешим по нему; access не нужен.
  if (token && !isTokenExpiringSoon(token)) return token;
  if (!token && !localStorage.getItem('refresh_token')) return null;

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

/** Дефолтный таймаут запроса (мс). Тяжёлые холодные эндпоинты (OI-чарт редкого
 * фьючерса, heatmap) укладываются в пару секунд после фиксов; 20с — потолок,
 * за которым висящий запрос точно стоит оборвать, чтобы не крутить спиннер
 * вечно. nginx upstream timeout — 60с, мы обрываем раньше и показываем ошибку. */
const DEFAULT_TIMEOUT_MS = 20000;

/** Ошибка таймаута — отличима от AbortError (отмена устаревшего запроса
 * stale-guard'ом) и от сетевых ошибок, чтобы UI показал «превышено время». */
export class TimeoutError extends Error {
  constructor(msg = 'Превышено время ожидания ответа сервера') {
    super(msg);
    this.name = 'TimeoutError';
  }
}

/**
 * fetch с таймаутом через AbortController. Пробрасывает (форвардит) внешний
 * signal вызывающего (stale-guard / отмена при переключении актива): аборт
 * любого из двух обрывает запрос. Таймаут-аборт превращается в TimeoutError,
 * пользовательский аборт — остаётся AbortError.
 */
function fetchWithTimeout(url: string, init?: RequestInit, timeoutMs = DEFAULT_TIMEOUT_MS): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const callerSignal = init?.signal;
  if (callerSignal) {
    if (callerSignal.aborted) controller.abort();
    else callerSignal.addEventListener('abort', () => controller.abort(), { once: true });
  }
  return fetch(url, { ...init, signal: controller.signal })
    .catch((err) => {
      // Оборвал таймер, а не вызывающий → это таймаут, а не отмена.
      if (controller.signal.aborted && !callerSignal?.aborted) {
        throw new TimeoutError();
      }
      throw err;
    })
    .finally(() => clearTimeout(timer));
}

/**
 * Обёртка над fetch с авторизацией и проактивным refresh.
 */
export async function apiFetch(url: string, init?: RequestInit): Promise<Response> {
  const token = await ensureFreshToken() || localStorage.getItem('access_token');
  const headers: Record<string, string> = {};
  if (token) headers['Authorization'] = `Bearer ${token}`;
  if (init?.headers) Object.assign(headers, init.headers);

  let response = await fetchWithTimeout(url, { ...init, headers });

  // Если всё равно 401 — пробуем refresh и retry
  if (response.status === 401 && token) {
    localStorage.removeItem('access_token'); // форсируем refresh
    const newToken = await ensureFreshToken();
    if (newToken) {
      const retryHeaders: Record<string, string> = { Authorization: `Bearer ${newToken}` };
      if (init?.headers) Object.assign(retryHeaders, init.headers);
      response = await fetchWithTimeout(url, { ...init, headers: retryHeaders });
    }
  }

  if (response.status === 403) {
    // 403 — это либо tier-restriction («… недоступен на тарифе …»), либо
    // отсутствие auth-token'а. Backend шлёт текст в ДВУХ форматах: envelope
    // {success:false,error:{message}} (текущий) ИЛИ legacy {detail}. Читаем ОБА —
    // иначе tier-текст терялся, throw был 'Доступ ограничен' (без слов «тарифе»/
    // «недоступ»), и handleTierError на странице не распознавал tier-403 →
    // показывал «Ошибка загрузки» вместо upgrade-промпта. Это корень для ВСЕХ
    // индикаторов через apiFetch (funds/OI/seasonality/…), не точечный.
    const data = await response.json().catch(() => ({} as Record<string, unknown>));
    const msg = (data as { detail?: string })?.detail
      || (data as { error?: { message?: string } })?.error?.message
      || 'Доступ ограничен';
    throw new Error(msg);
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

// ==================== СКРИНЕР СИГНАЛОВ ОИ ====================

/** Строка скринера сигналов ОИ (дневные данные физлиц, T+1). */
export interface OiScreenerRow {
  sectype: string;
  name: string;
  group: string | null;           // Индексы / Валюта / Сырьё / Акции / Крипто
  front_secid: string | null;     // датированный фронт-контракт ('BRQ6')
  has_intraday: boolean;          // есть свежие 5-мин данные → сигнал считается интрадей
  oi: number;                     // валовый ОИ, контракты
  oi_delta_pct: number | null;    // Δ ОИ за день, %
  net: number;                    // чистая позиция физлиц, контракты
  net_pct: number | null;         // net / (лонги+шорты), −100…+100
  net_pct_prev: number | null;    // перекос вчера (для «следа кометы»)
  delta_net: number | null;       // Δ чистой позиции за день, контракты
  npart: number;                  // число участников группы (для подсказки illiquid)
  ratio: number | null;           // ATR14-кратность |Δnet| («в N× резче обычного»)
  direction: 'up' | 'down' | null;
  status: 'sharp' | 'normal' | 'illiquid' | 'nodata';
  // Новый рекорд перекоса сегодня (сильнейший период), null если нет.
  record: { kind: 'high' | 'low'; period: 'all' | '5y' | '4y' | '3y' | '2y' | '1y' } | null;
  // Истор. экстремум СЫРОЙ чистой позиции (контракты, за всё время + интрадей), null если нет.
  net_record: { kind: 'high' | 'low' } | null;
  signal_date: string;            // дата последних данных (T+1)
}

export interface OiScreenerResponse {
  signal_date: string | null;
  intraday_date: string | null;   // свежайший интрадей-бар (5-мин), если новее дневной; для честной подписи
  clgroup: 'FIZ' | 'YUR';
  min_part: number;               // порог ликвидности группы (физ 50 / юр 15)
  rows: OiScreenerRow[];
}

export async function getOiScreener(clgroup: 'FIZ' | 'YUR' = 'FIZ'): Promise<OiScreenerResponse> {
  const response = await fetch(`${API_BASE}/api/oi/screener?clgroup=${clgroup}`);
  if (!response.ok) throw new Error('Failed to fetch OI screener');
  return response.json();
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
  period: string = '1y'
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
  data_date?: string;   // ISO дата данных карты (последний торговый день, если торгов сегодня нет)
  is_live?: boolean;    // идёт ли сейчас торговая сессия
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
  /** Доходность фонда (total-return по pay = СЧА на пай + выплаты). Все периоды,
   *  чтобы молодые фонды (<1 года) показывали лучший доступный (m6/m3/m1) вместо «—».
   *  null-период = истории не хватает; весь returns null = битые данные источника. */
  returns?: { m1: number | null; m3: number | null; m6: number | null; y1: number | null; y5?: number | null } | null;
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
export type FundPeriod = '1w' | '1m' | '1y' | '3y' | 'all';

export async function getFundsChartData(
  category: FundCategory,
  period: FundPeriod = '1y'
): Promise<FundsChartResponse> {
  const params = new URLSearchParams({
    category,
    period
  });
  const response = await apiFetch(`${API_BASE}/api/funds/chart?${params}`);
  // tier-403 уже обрабатывает apiFetch (бросает текст «… недоступен на тарифе …»).
  // Здесь — только НЕ-403 4xx/5xx: пробрасываем detail/error.message, чтобы текст
  // ошибки не терялся (а не глоталось в генерик 'Failed to fetch').
  if (!response.ok) throw new Error(await apiErrorDetail(response, 'Failed to fetch funds chart data'));
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
  // tier-403 ловит apiFetch (см. выше). Здесь — только НЕ-403 ошибки: detail/
  // error.message вместо генерик-текста.
  if (!response.ok) throw new Error(await apiErrorDetail(response, 'Failed to fetch funds flows'));
  return response.json();
}

// ==================== FUND HOLDINGS ====================

export async function getFundHoldings(fundId: number): Promise<FundHoldingsResponse> {
  const response = await apiFetch(`${API_BASE}/api/funds/holdings/${fundId}`);
  if (!response.ok) throw new Error('Failed to fetch fund holdings');
  return response.json();
}

// Публичная карточка фонда для «Деньги в фондах» (все категории). Форма ответа
// совпадает с FundTradesDetail → переиспользуем тот же модал. diff/summary тут
// всегда пустые; current_holdings заполнен только для stocks/bonds.
export async function getFundsDetail(fundId: number): Promise<FundTradesDetail> {
  const response = await apiFetch(`${API_BASE}/api/funds/detail/${fundId}`);
  if (response.status === 404) throw new Error('Фонд не найден');
  if (!response.ok) throw new Error('Не удалось загрузить детали фонда');
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
  /** USD-режим: долларовый ряд отстаёт от рублёвого (РТС и курс не торгуются
   *  на выходных и в нерабочие дни). Драйвит маркер «доллар не обновляется». */
  dollar_stale?: boolean;
  /** Дата последней доступной точки долларового ряда (для текста маркера). */
  data_date?: string | null;
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

/** Извлекает человекочитаемый текст ошибки из 4xx-ответа API.
 *  Backend оборачивает ошибки в envelope {success:false,error:{code,message}};
 *  legacy-роуты (FastAPI HTTPException) — в {detail}. Парсим оба. Без этого
 *  tier-403 (текст «… на тарифе …»/«… недоступ…») терялся → индикатор показывал
 *  «Ошибка загрузки данных» вместо upgrade-модалки (handleTierError матчит по
 *  тексту). Универсален — используется buffett и funds (chart/flows). */
async function apiErrorDetail(response: Response, fallback: string): Promise<string> {
  try {
    const body = await response.json();
    // Legacy FastAPI-формат: {detail: "..."}
    if (body && typeof body.detail === 'string' && body.detail) return body.detail;
    // Текущий envelope ошибок: {success:false, error:{code, message}}
    if (body?.error && typeof body.error.message === 'string' && body.error.message) {
      return body.error.message;
    }
  } catch { /* не JSON — отдаём fallback */ }
  return fallback;
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
  if (!response.ok) throw new Error(await apiErrorDetail(response, 'Failed to fetch Buffett cap/gdp'));
  return response.json();
}

export async function getBuffettCapM2(
  period: BuffettPeriod = '3y',
  smooth: boolean = true,
  timeframe: string = '1m'
): Promise<BuffettRatioResponse> {
  const params = new URLSearchParams({ period, smooth: smooth.toString(), timeframe });
  const response = await apiFetch(`${API_BASE}/api/buffett/cap-m2?${params}`);
  if (!response.ok) throw new Error(await apiErrorDetail(response, 'Failed to fetch Buffett cap/m2'));
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
  // uniques — уникальные посетители ЗА ВЕСЬ период (НЕ дневной DAU).
  uniques: number;
  sessions: number;
  events: number;
  avg_session_sec: number;
  delta_uniques: number | null;
  delta_sessions_pct: number | null;
  delta_events_pct: number | null;
  delta_avg_session_sec: number | null;
}

export interface AnalyticsStats {
  period_days: number;
  segment: string;
  device: string;
  summary: AnalyticsStatsSummary;
  trends: { date: string; uniques: number; sessions: number }[];
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
// Admin: Users panel
// (Funnel удалён 2026-07-16 вместе с бэкенд-эндпоинтом /api/analytics/funnel —
//  воронка не давала пользы и была самым дорогим запросом admin-stats.
//  Cohort / Realtime / A/B удалены ещё 2026-06-16.)
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
  // Опциональные: отдаёт только list-эндпоинт /users (не /users/{id})
  plan_expires_at?: string | null;
  is_paid?: boolean;
  sessions_count: number;
  events_count: number;
  last_active_ts: string | null;
}

export async function listAdminUsers(opts: {
  days?: number;
  sort?: string;
  search?: string;
  /** all / paid / free / admin */
  filter?: string;
}): Promise<{ period_days: number; total_count: number; paid_count: number; users: AdminUser[] }> {
  const params = new URLSearchParams();
  if (opts.days !== undefined) params.set('days', String(opts.days));
  if (opts.sort) params.set('sort', opts.sort);
  if (opts.search) params.set('search', opts.search);
  if (opts.filter && opts.filter !== 'all') params.set('filter', opts.filter);
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
  categories: string[];   // в порядке для legend/stack (locked входят — для пикера)
  locked_categories?: string[];  // категории под замком на текущем тарифе (значения вырезаны)
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

// ─── Extension tokens (расширение для терминала Т-Инвестиций) ───────────────
export interface ExtensionTokenInfo {
    id: number;
    name: string | null;
    token_prefix: string;
    created_at: string;
    last_used_at: string | null;
}
export interface ExtensionTokenCreated {
    id: number;
    token: string;        // показывается ОДИН раз
    token_prefix: string;
}
export async function listExtensionTokens(): Promise<ExtensionTokenInfo[]> {
    const resp = await apiFetch(`${API_BASE}/api/extension/token`);
    if (!resp.ok) throw new Error('Не удалось загрузить токены');
    return resp.json();
}
export async function createExtensionToken(): Promise<ExtensionTokenCreated> {
    const resp = await apiFetch(`${API_BASE}/api/extension/token`, { method: 'POST' });
    if (resp.status === 403) {
        const data = await resp.json().catch(() => ({}));
        throw new Error(data?.detail || data?.error?.message || 'Токен доступен только на тарифе Pro');
    }
    if (!resp.ok) throw new Error('Не удалось сгенерировать токен');
    return resp.json();
}
export async function revokeExtensionToken(id: number): Promise<void> {
    const resp = await apiFetch(`${API_BASE}/api/extension/token/${id}`, { method: 'DELETE' });
    if (!resp.ok && resp.status !== 204) throw new Error('Не удалось отозвать токен');
}

export interface ExtensionExchange {
    access_token: string;
    token_type: string;
    expires_in: number;
    role: string;
}
/** Обмен ext-токена на короткий JWT (для embed внутри расширения).
 *  Публичный эндпоинт — без apiFetch (валидирует токен сам). */
export async function exchangeExtensionToken(token: string): Promise<ExtensionExchange> {
    const resp = await fetch(`${API_BASE}/api/extension/exchange`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token }),
    });
    if (!resp.ok) {
        const data = await resp.json().catch(() => ({}));
        // Пробрасываем HTTP-статус, чтобы embed отличил 403 (подписка кончилась)
        // от 401 (нет/невалиден/отозван токен) и показал правильный экран.
        const err = new Error(
            data?.detail || (resp.status === 403 ? 'Токен требует активную подписку PRO' : 'Недействительный токен'),
        ) as Error & { status?: number };
        err.status = resp.status;
        throw err;
    }
    return resp.json();
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

export type FundTradesPeriod = '1m' | '3m' | '6m' | '1y' | '3y';

export interface FundReturns {
    m1: number | null;
    m3: number | null;
    m6: number | null;
    y1: number | null;
    all?: number | null;  // доходность за всё время (с первого дня данных) — есть всегда, даже у молодых фондов
}

export interface FundTopHolding {
    name: string;
    isin?: string | null;
    weight: number;
}

export interface FundWithHistory {
    fund_id: number;
    ticker: string;
    name: string;
    uk: string | null;
    category: string;
    subcategory: string | null;
    last_snapshot_date: string | null;
    snapshot_count: number;
    holdings_count: number;
    snapshots_count?: number; // backward compat
    uk_id?: number | string | null;
    nav_rub: number | null;
    returns: FundReturns;
    top_holdings: FundTopHolding[];
    has_distributions?: boolean; // фонд платит доход → returns = полная доходность (с выплатами)
}

export interface FundTradesHolding {
    asset_name: string;
    isin: string | null;
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

export interface FundPerformancePoint {
    date: string;
    pay: number;
    nav?: number | null; // СЧА (AUM) на дату — заполняется в /api/funds/detail
}

export interface FundPerformance {
    timeline: FundPerformancePoint[];
    returns: FundReturns;
}

export interface FundTradesDetail {
    fund: {
        fund_id: number;
        ticker: string;
        name: string;
        category: string;
        subcategory: string | null;
        nav_rub?: number | null; // СЧА (AUM) — заполняется в /api/funds/detail
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
    performance?: FundPerformance;
    has_distributions?: boolean; // фонд платит доход → returns = полная доходность
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
    // resolved_month — фактический месяц консенсуса (выбранный as_of или последний
    // доступный для набора). funds_in_month — сколько выбранных фондов реально имеют
    // снапшот этого месяца (0 → данных за месяц нет, показываем явное сообщение).
    resolved_month: string | null;
    funds_in_month: number;
    manager: string | null;
    sort: 'weight' | 'amount';
    available_months: string[];
    /** Дата-отсечка свежести (Free/гость): месяцы с датой > cutoff заблокированы. null = без задержки. */
    snapshot_cutoff?: string | null;
    top_accumulated: FundTradesMover[];
    top_reduced: FundTradesMover[];
}

// ─── Общий портфель (агрегированный состав всех выбранных фондов акций) ───

export interface FundPortfolioHolding {
    akey: string;
    asset_name: string;
    isin: string | null;
    /** Суммарная рублёвая стоимость позиции по всем выбранным фондам. */
    value_rub: number;
    /** Доля в общем портфеле по деньгам (value-weighted, суммируется к 100). */
    weight_rub: number;
    /** Средняя доля по выбранным фондам (equal-weight, отсутствие = 0%). */
    weight_avg: number;
    /** Сколько выбранных фондов держат бумагу. */
    funds_holding: number;
}

export interface FundPortfolioFund {
    ticker: string;
    name: string;
    uk: string | null;
    uk_id: number | string | null;
    nav_rub: number | null;
    snapshot_date: string | null;
}

export interface FundPortfolio {
    num_funds: number;
    num_assets: number;
    /** Суммарная стоимость акций в портфеле (Σ рублёвых позиций). */
    total_value_rub: number;
    /** Суммарная полная СЧА выбранных фондов (включая кэш/прочее). */
    total_nav_rub: number;
    /** nav-взвешенная доходность набора по периодам. */
    returns: FundReturns;
    /** Выбранный месяц-срез (echo параметра as_of), null = последний. */
    as_of?: string | null;
    /** Фактический месяц среза набора (max snapshot выбранных фондов <= bound). */
    resolved_month?: string | null;
    /** Календарные месяцы с данными (для month-picker), ISO, DESC. */
    available_months?: string[];
    /** Дата-отсечка свежести (Free/гость): null = без задержки. */
    snapshot_cutoff?: string | null;
    funds: FundPortfolioFund[];
    holdings: FundPortfolioHolding[];
}

export async function getFundPortfolio(
    // `funds` — comma-separated ТИКЕРЫ фондов (фронт резолвит выбранные УК в тикеры).
    //   Приоритет над manager. Пусто = все фонды акций из whitelist.
    // `manager` — comma-separated uk_id (на случай API-клиента; фронт шлёт funds).
    // `as_of` — YYYY-MM-DD целевой месяц-срез (пусто = последний).
    opts: { funds?: string; manager?: string; as_of?: string } = {},
): Promise<FundPortfolio> {
    const { funds, manager, as_of } = opts;
    const params = new URLSearchParams();
    if (funds) params.set('funds', funds);
    if (manager) params.set('manager', manager);
    if (as_of) params.set('as_of', as_of);
    const qs = params.toString();
    const resp = await apiFetch(`${API_BASE}/api/fund-trades/portfolio${qs ? `?${qs}` : ''}`);
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (!resp.ok) throw new Error('Не удалось загрузить общий портфель');
    return resp.json();
}

export async function listFundsWithHistory(): Promise<{ funds: FundWithHistory[]; count: number }> {
    const resp = await apiFetch(`${API_BASE}/api/fund-trades/funds`);
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (!resp.ok) throw new Error('Не удалось загрузить фонды');
    return resp.json();
}

export async function getFundTradesDetail(
    ticker: string,
    period: FundTradesPeriod = '1m',
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
    // `funds` — comma-separated ТИКЕРЫ конкретных фондов (напр. "TMOS,SBMX").
    //   Приоритет над manager на бэкенде. Пусто = все фонды.
    // `managers` — comma-separated uk_id (мультиселект УК), напр. "34,5,3597".
    // `manager` оставлен для обратной совместимости (single uk_id / имя). Пусто = все УК.
    opts: { asOf?: string; funds?: string; manager?: string; managers?: string; sort?: 'weight' | 'amount'; limit?: number } = {},
): Promise<FundTradesMovers> {
    const { asOf, funds, manager, managers, sort = 'weight', limit = 20 } = opts;
    const params = new URLSearchParams({ period, sort, limit: String(limit) });
    if (asOf) params.set('as_of', asOf);
    // Бэкенд: `funds` (тикеры фондов) имеет приоритет над `manager` (comma-sep uk_id).
    if (funds) params.set('funds', funds);
    const managerParam = managers ?? manager;
    if (managerParam) params.set('manager', managerParam);
    const resp = await apiFetch(`${API_BASE}/api/fund-trades/movers?${params}`);
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (!resp.ok) throw new Error('Не удалось загрузить топ движений');
    return resp.json();
}

// ─── Snapshot review API ────────────────────────────────────────

export interface FundSnapshotItem {
    snapshot_date: string;
    asset_count: number;
    locked?: boolean;   // Free/гость: свежий срез — под замком (по подписке)
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
    // Free/гость: свежий срез (> cutoff) заблокирован — бэк вернул маркер БЕЗ холдингов.
    locked?: boolean;
    latest_snapshot_date?: string | null;   // реальная свежая дата (для метки «по подписке»)
    required_tier?: string;
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
    } | null;
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

// ─── Asset selector + company flows ─────────────────────────

export interface FundTradeAsset {
    key: string;
    asset_name: string;
    isin: string | null;
    sec_type?: string | null;   // сырой тип ISS (common_share / ofz_bond / ...)
    category?: string | null;   // крупная корзина: share|bond|fund|currency|commodity|index|other
    funds_count: number;
    last_amount_rub: number | null;
    avg_weight_pct: number | null;
}

export interface FundTradeAssetsResponse {
    assets: FundTradeAsset[];
}

export interface CompanyFlowFund {
    ticker: string;
    fund_name: string;
    uk_id: string | number | null;
    values: (number | null)[];
}

export interface CompanyFlowsResponse {
    asset_name: string;
    isin: string | null;
    metric: string;
    funds_count: number;
    months: string[];
    funds: CompanyFlowFund[];
    total: (number | null)[];
}

export async function listFundTradeAssets(): Promise<FundTradeAssetsResponse> {
    const resp = await apiFetch(`${API_BASE}/api/fund-trades/assets`);
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (!resp.ok) throw new Error('Не удалось загрузить список бумаг');
    return resp.json();
}

export async function getCompanyFlows(
    opts: { isin?: string; assetName?: string; metric?: 'amount' | 'weight' },
): Promise<CompanyFlowsResponse> {
    const params = new URLSearchParams();
    if (opts.isin) params.set('isin', opts.isin);
    else if (opts.assetName) params.set('asset_name', opts.assetName);
    else throw new Error('isin or asset_name required');
    if (opts.metric) params.set('metric', opts.metric);

    const resp = await apiFetch(`${API_BASE}/api/fund-trades/company-flows?${params}`);
    if (resp.status === 403) throw new Error('Доступно на тарифе Pro');
    if (!resp.ok) throw new Error('Не удалось загрузить потоки по компании');
    return resp.json();
}

// ════════════════════════════════════════════════════════════════════════════
// Telegram alert-bot (привязка + алерты). Бэкенд: /api/alerts/*
// ════════════════════════════════════════════════════════════════════════════
export interface TelegramLinkInfo { deep_link: string; token: string; expires_at: string; }
export interface TelegramStatusInfo { linked: boolean; username: string | null; }
export interface AlertInfo {
    id: number; indicator: string; asset: string; asset_name: string | null;
    metric: string; clgroup: string | null; op: string; threshold: number;
    mode: string; status: string; timeframe?: string;
    // source — разделитель кабинета ('oi'|'funds'); sector — instruments.group
    // джойном по asset (null если инструмент не найден); fires_count — число
    // срабатываний за всю жизнь алерта (COUNT по alert_fires).
    source: string; sector: string | null; fires_count: number;
    // Каналы доставки ('telegram'|'site'|'email'). Старый бэк мог не отдавать →
    // опционально (фронт трактует пусто как ['telegram']).
    channels?: string[];
    last_fired_at: string | null; created_at: string | null;
    // Фонд-алерты: заморожённый набор fund_id'ов через запятую (asset='custom').
    // null для asset='all'/<категория> (динамический набор). Опционально — старый
    // бэк может не отдавать поле. Метка выбора для кабинета — в asset_name.
    fund_ids?: string | null;
}
// Страница алертов: items + общее число (из заголовка X-Total-Count).
export interface AlertsPage { items: AlertInfo[]; total: number; }
// Одно срабатывание алерта (история).
export interface AlertFire {
    id: number; fired_at: string | null;
    value: number | null; message_text: string | null;
}
// Admin-статистика алертов (GET /api/analytics/alerts-stats).
export interface AlertsStats {
    period_days: number;
    created: number; deleted: number; paused: number; resumed: number;
    active_now: number; with_fires: number;
    by_source: { source: string; active: number }[];
    top_assets?: { asset: string; count: number }[];
}
export interface AlertCreatePayload {
    indicator: string; asset: string; asset_name?: string; metric: string;
    clgroup?: string | null; op: string; threshold: number;
    mode?: string; cooldown_hours?: number;
    // Таймфрейм раннего срабатывания дневного сигнала: '5m' | '1h' | '1d'
    // (по умолчанию '1d'). Внутридневные доступны только у ликвидных активов.
    timeframe?: string;
    // Фонд-алерты (indicator='funds_flow'): заморожённый набор fund_id'ов.
    // Шлём только при asset='custom' (точечный выбор фондов); для asset='all'
    // (все фонды) или asset=<категория> (вся категория) НЕ шлём — набор резолвится
    // динамически на бэке. См. контракт fund-алерта в CreateFundAlertModal.
    fund_ids?: number[];
    // Каналы доставки: ['telegram','site','email']. Не шлём → бэк берёт дефолт юзера.
    channels?: string[];
}

// Настройки доставки (GET/PUT /api/alerts/settings).
export interface NotifySettings {
    default_channels: string[];
    telegram_linked: boolean;
    email_available: boolean;
    email: string | null;
}

// Срабатывание для центра уведомлений (тост + вкладка «Мои алерты»).
export interface RecentFire {
    id: number; alert_id: number; fired_at: string | null;
    value: number | null; message_text: string | null;
    indicator: string; asset: string; asset_name: string | null; source: string;
}

export interface AlertContext {
    price: {
        value: number | null;
        ts: string | null;
        interval: number | null;
        intraday: boolean;
    };
}

export async function getTelegramStatus(): Promise<TelegramStatusInfo> {
    const resp = await apiFetch(`${API_BASE}/api/alerts/telegram/status`);
    if (!resp.ok) throw new Error('Не удалось получить статус Telegram');
    return resp.json();
}
export async function getAlertContext(
    indicator: string, asset: string, clgroup = 'FIZ',
): Promise<AlertContext> {
    const params = new URLSearchParams({ indicator, asset, clgroup });
    const resp = await apiFetch(`${API_BASE}/api/alerts/context?${params}`);
    if (!resp.ok) throw new Error('Не удалось получить контекст алерта');
    return resp.json();
}
export async function getNotifySettings(): Promise<NotifySettings> {
    const resp = await apiFetch(`${API_BASE}/api/alerts/settings`);
    if (!resp.ok) throw new Error('Не удалось получить настройки доставки');
    return resp.json();
}
export async function updateNotifySettings(default_channels: string[]): Promise<NotifySettings> {
    const resp = await apiFetch(`${API_BASE}/api/alerts/settings`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ default_channels }),
    });
    if (!resp.ok) throw new Error('Не удалось сохранить настройки доставки');
    return resp.json();
}
export async function getRecentFires(since = 0, limit = 20): Promise<RecentFire[]> {
    const params = new URLSearchParams({ since: String(since), limit: String(limit) });
    const resp = await apiFetch(`${API_BASE}/api/alerts/recent-fires?${params}`);
    if (!resp.ok) throw new Error('Не удалось получить срабатывания');
    return resp.json();
}
export async function createTelegramLink(): Promise<TelegramLinkInfo> {
    const resp = await apiFetch(`${API_BASE}/api/alerts/telegram/link`, { method: 'POST' });
    if (!resp.ok) throw new Error('Не удалось создать ссылку привязки');
    return resp.json();
}
export async function unlinkTelegram(): Promise<void> {
    const resp = await apiFetch(`${API_BASE}/api/alerts/telegram`, { method: 'DELETE' });
    if (!resp.ok && resp.status !== 204) throw new Error('Не удалось отвязать Telegram');
}
// Список алертов пользователя — плоский массив AlertOut + общее число в
// заголовке X-Total-Count (для пагинации в кабинете). ?limit (деф 200) & ?offset.
export async function listAlerts(opts?: { limit?: number; offset?: number }): Promise<AlertsPage> {
    const params = new URLSearchParams();
    if (opts?.limit != null) params.set('limit', String(opts.limit));
    if (opts?.offset != null) params.set('offset', String(opts.offset));
    const qs = params.toString();
    const resp = await apiFetch(`${API_BASE}/api/alerts${qs ? `?${qs}` : ''}`);
    if (!resp.ok) throw new Error('Не удалось загрузить алерты');
    const items: AlertInfo[] = await resp.json();
    // X-Total-Count может отсутствовать (старый бэк / прокси режет заголовок) —
    // фолбэк на длину текущей страницы, чтобы UI не падал.
    const totalHeader = resp.headers.get('X-Total-Count');
    const total = totalHeader != null ? Number(totalHeader) : items.length;
    return { items, total: Number.isFinite(total) ? total : items.length };
}
// История срабатываний одного алерта (alert_fires), новые сверху. Доступ:
// владелец|admin. total — из X-Total-Count. ?limit (деф 50) & ?offset.
export async function getAlertFires(
    alertId: number,
    opts?: { limit?: number; offset?: number },
): Promise<{ items: AlertFire[]; total: number }> {
    const params = new URLSearchParams();
    if (opts?.limit != null) params.set('limit', String(opts.limit));
    if (opts?.offset != null) params.set('offset', String(opts.offset));
    const qs = params.toString();
    const resp = await apiFetch(`${API_BASE}/api/alerts/${alertId}/fires${qs ? `?${qs}` : ''}`);
    if (!resp.ok) throw new Error('Не удалось загрузить историю срабатываний');
    const items: AlertFire[] = await resp.json();
    const totalHeader = resp.headers.get('X-Total-Count');
    const total = totalHeader != null ? Number(totalHeader) : items.length;
    return { items, total: Number.isFinite(total) ? total : items.length };
}
// Admin-статистика алертов за период (created/deleted/paused/resumed) + снимок
// «сейчас» (active_now/with_fires/by_source/top_assets). GET /api/analytics/alerts-stats?days=
export async function getAlertsStats(days?: number): Promise<AlertsStats> {
    const qs = days != null ? `?days=${days}` : '';
    const resp = await apiFetch(`${API_BASE}/api/analytics/alerts-stats${qs}`);
    if (!resp.ok) throw new Error('Не удалось загрузить статистику алертов');
    return resp.json();
}
export async function createAlert(payload: AlertCreatePayload): Promise<AlertInfo> {
    const resp = await apiFetch(`${API_BASE}/api/alerts`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    if (resp.status === 403) {
        const d = await resp.json().catch(() => ({}));
        throw new Error(d?.detail || d?.error?.message || 'Алерты доступны на тарифе Basic и Pro');
    }
    if (!resp.ok) throw new Error('Не удалось создать алерт');
    return resp.json();
}
export interface AlertBatchResult { created: number; skipped: number; errors: string[]; }
// Пакетное создание (группа активов) — ОДИН запрос вместо N, иначе N
// параллельных POST'ов пробивают nginx rate-limit (burst=20) → часть 503.
export async function createAlertsBatch(alerts: AlertCreatePayload[]): Promise<AlertBatchResult> {
    const resp = await apiFetch(`${API_BASE}/api/alerts/batch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ alerts }),
    });
    if (resp.status === 403) {
        const d = await resp.json().catch(() => ({}));
        throw new Error(d?.detail || d?.error?.message || 'Алерты доступны на тарифе Basic и Pro');
    }
    if (!resp.ok) throw new Error('Не удалось создать алерты');
    return resp.json();
}
export async function deleteAlert(id: number): Promise<void> {
    const resp = await apiFetch(`${API_BASE}/api/alerts/${id}`, { method: 'DELETE' });
    if (!resp.ok && resp.status !== 204) throw new Error('Не удалось удалить алерт');
}
// Массовое удаление всех алертов пользователя (если случайно создал группу из 100).
export async function deleteAllAlerts(): Promise<{ deleted: number }> {
    const resp = await apiFetch(`${API_BASE}/api/alerts`, { method: 'DELETE' });
    if (!resp.ok) throw new Error('Не удалось удалить алерты');
    return resp.json();
}
export async function setAlertStatus(id: number, status: 'active' | 'paused'): Promise<AlertInfo> {
    const resp = await apiFetch(`${API_BASE}/api/alerts/${id}?status=${status}`, { method: 'PATCH' });
    if (!resp.ok) throw new Error('Не удалось изменить алерт');
    return resp.json();
}
// Таймфрейм источника сигнала ('5m'|'1h'|'1d') — только для oi_move/oi_participants/
// oi_level (см. TIMEFRAME_INDICATORS в api/routers/alerts.py); для остальных бэк вернёт 400.
export async function setAlertTimeframe(id: number, timeframe: '5m' | '1h' | '1d'): Promise<AlertInfo> {
    const resp = await apiFetch(`${API_BASE}/api/alerts/${id}?timeframe=${timeframe}`, { method: 'PATCH' });
    if (!resp.ok) throw new Error('Не удалось изменить таймфрейм');
    return resp.json();
}

// ════════════════════════════════════════════════════════════════════════════
// Лента аномалий — всплывающие тосты + колокол. Бэкенд: /api/anomalies/*
// ════════════════════════════════════════════════════════════════════════════
export interface AnomalyDeepLink {
    route: string;            // '/oi' | '/funds-money'
    secid?: string;
    category?: string;
    clgroup?: string;
    interval?: number;
    mode?: string;
    variant?: string;
    period?: string;
}
export interface AnomalyItem {
    id: number;
    type: string;             // 'oi_move' | 'funds_flow' | 'oi_zscore' | 'oi_participants'
    asset_id: string;
    asset_name: string | null;
    clgroup: string | null;
    direction: string | null; // 'up' | 'down'
    headline: string;
    context: string | null;
    severity_value: number | null;
    signal_date: string | null;
    created_at: string | null;
    deep_link: AnomalyDeepLink;
    mine: boolean;                              // личная (твой сигнал сработал)
    link_required_tier: 'basic' | 'pro' | null; // gated-цель → апселл + дефолт
}
export interface ChannelPost {
    id: number;
    channel: string;             // username канала (FrameTool)
    channel_name: string | null; // человекочитаемо
    text: string | null;
    photo_url: string | null;
    link: string;                // t.me/<channel>/<post_id> — открывать внешней вкладкой
    posted_at: string | null;
}
export interface AnomalyFeed {
    items: AnomalyItem[];
    last_seen_id: number | null;  // серверный для залогиненных; null для гостя
    toasts_enabled: boolean;      // тумблер показа (гость → true, решает localStorage)
    channel_posts: ChannelPost[]; // новости каналов — секция в колоколе
}

// Лента аномалий. apiFetch работает и для гостя (без токена бэкенд видит guest).
export async function getAnomalyFeed(
    opts?: { since?: number; limit?: number; maxAgeHours?: number },
): Promise<AnomalyFeed> {
    const params = new URLSearchParams();
    if (opts?.since != null) params.set('since', String(opts.since));
    if (opts?.limit != null) params.set('limit', String(opts.limit));
    if (opts?.maxAgeHours != null) params.set('max_age_hours', String(opts.maxAgeHours));
    const qs = params.toString();
    const resp = await apiFetch(`${API_BASE}/api/anomalies/feed${qs ? `?${qs}` : ''}`);
    if (!resp.ok) throw new Error('Не удалось загрузить ленту аномалий');
    return resp.json();
}
// Сдвинуть серверный маркер «просмотрено» (только залогиненные; гость — localStorage).
export async function markAnomaliesSeen(lastId: number): Promise<void> {
    const resp = await apiFetch(`${API_BASE}/api/anomalies/seen`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ last_id: lastId }),
    });
    if (!resp.ok) throw new Error('Не удалось пометить просмотренным');
}
// Вкл/выкл всплывающие тосты (залогиненные; гость — localStorage).
export async function setAnomalyToasts(enabled: boolean): Promise<void> {
    const resp = await apiFetch(`${API_BASE}/api/anomalies/toggle`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled }),
    });
    if (!resp.ok) throw new Error('Не удалось изменить настройку');
}
// Создать личный сигнал из аномалии (кнопка «🔔 Получать»). 403 → текст с «тарифе»
// для handleTierError (апселл-модалка).
export async function subscribeAnomaly(id: number): Promise<{ ok: boolean; alert_id: number }> {
    const resp = await apiFetch(`${API_BASE}/api/anomalies/${id}/subscribe`, { method: 'POST' });
    if (resp.status === 403) {
        const d = await resp.json().catch(() => ({}));
        throw new Error(d?.detail || d?.error?.message || 'Сигналы доступны на тарифе Basic и Pro');
    }
    if (!resp.ok) throw new Error('Не удалось создать сигнал');
    return resp.json();
}
// Последние посты каналов для виджета «Новости каналов» на главной. Публичный
// эндпоинт (без auth) → голый fetch; при ошибке — пустой массив (виджет скрыт).
export async function getChannelPosts(limit = 12): Promise<ChannelPost[]> {
    try {
        const resp = await fetch(`${API_BASE}/api/anomalies/channel-posts?limit=${limit}`);
        if (!resp.ok) return [];
        return await resp.json();
    } catch {
        return [];
    }
}

// Список sectype, у которых есть свежие внутридневные данные позиций
// (open_interest interval=5). Используется в InstrumentSearchModal для бейджа
// «intraday». При любой ошибке — пустой массив (бейджи просто не показываются).
export async function getIntradayAssets(): Promise<string[]> {
    try {
        const resp = await fetch(`${API_BASE}/api/oi/intraday-assets`);
        if (!resp.ok) return [];
        const data = await resp.json();
        return data.sectypes ?? [];
    } catch {
        return [];
    }
}

// Список sectype фьючерсов с низкой активностью физлиц (медиана числа
// трейдеров за 5 торговых дней ниже порога релевантности). Пикер ОИ прячет их
// из дефолтного списка, но раскрывает поиском/избранным; при росте активности
// актив возвращается сам. При любой ошибке — пустой список (ничего не прячем).
export async function getLowActivityAssets(): Promise<string[]> {
    try {
        const resp = await fetch(`${API_BASE}/api/oi/low-activity-assets`);
        if (!resp.ok) return [];
        const data = await resp.json();
        return data.sectypes ?? [];
    } catch {
        return [];
    }
}

// ═══════════════════════════════════════════════════════════════════
// Admin: Content news Kanban (/admin/content-news)
// ═══════════════════════════════════════════════════════════════════

export type ContentCandidateStatus =
  | 'candidate' | 'discarded' | 'pending' | 'draft_ready'
  | 'no_data' | 'in_review' | 'published' | 'rejected';

export interface ContentCandidate {
  id: number;
  status: ContentCandidateStatus;
  source: string | null;
  headline: string;
  tickers: string[];
  futures_ticker: string | null;
  event_type: string | null;
  importance_1_5: number | null;
  reasoning: string | null;
  matched_anomaly_id: number | null;
  thread_key: string | null;
  parent_candidate_id: number | null;
  forwards_count: number | null;
  created_at: string | null;
  updated_at: string | null;
  pending_expires_at: string | null;
  published_at: string | null;
  reviewer_action: string | null;
}

export interface ContentCandidateDetail extends ContentCandidate {
  raw_text: string | null;
  draft_text: string | null;
  synth_declined_reason: string | null;
  reviewer_id: number | null;
  thread: ContentCandidate[];
}

export interface ContentCandidateList {
  items: ContentCandidate[];
  total: number;
  limit: number;
  offset: number;
}

export async function listContentCandidates(
  status: ContentCandidateStatus,
  opts: { limit?: number; offset?: number; source?: string } = {},
): Promise<ContentCandidateList> {
  const params = new URLSearchParams({ status });
  if (opts.limit !== undefined) params.set('limit', String(opts.limit));
  if (opts.offset !== undefined) params.set('offset', String(opts.offset));
  if (opts.source) params.set('source', opts.source);
  const response = await apiFetch(`${API_BASE}/api/admin/content-candidates?${params}`);
  if (!response.ok) {
    if (response.status === 403) throw new Error('Доступ только для администратора');
    throw new Error('Failed to fetch content candidates');
  }
  return response.json();
}

export interface ContentCandidateSourceStats {
  source: string;
  total: number;
  by_status: Record<string, number>;
}

export async function getContentCandidateStatsBySource(): Promise<ContentCandidateSourceStats[]> {
  const response = await apiFetch(`${API_BASE}/api/admin/content-candidates/stats/by-source`);
  if (!response.ok) {
    if (response.status === 403) throw new Error('Доступ только для администратора');
    throw new Error('Failed to fetch content candidate source stats');
  }
  return response.json();
}

export async function getContentCandidateDetail(id: number): Promise<ContentCandidateDetail> {
  const response = await apiFetch(`${API_BASE}/api/admin/content-candidates/${id}`);
  if (!response.ok) {
    if (response.status === 403) throw new Error('Доступ только для администратора');
    if (response.status === 404) throw new Error('Кандидат не найден');
    throw new Error('Failed to fetch content candidate detail');
  }
  return response.json();
}

export async function updateContentCandidateStatus(
  id: number,
  status: ContentCandidateStatus,
): Promise<ContentCandidate> {
  const response = await apiFetch(`${API_BASE}/api/admin/content-candidates/${id}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }),
  });
  if (!response.ok) {
    if (response.status === 403) throw new Error('Доступ только для администратора');
    if (response.status === 404) throw new Error('Кандидат не найден');
    if (response.status === 422) {
      const body = await response.json().catch(() => null);
      throw new Error(body?.detail || 'Недопустимый переход статуса');
    }
    throw new Error('Failed to update content candidate status');
  }
  return response.json();
}
