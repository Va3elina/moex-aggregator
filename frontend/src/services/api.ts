
import type {
  Instrument,
  ChartResponse,
  InstrumentGroup,
  StatsResponse,
  TopInstrumentsResponse
} from '../types';

// В продакшене - относительные пути (тот же сервер)
// В dev - localhost:8000
const API_BASE = import.meta.env.DEV ? 'http://localhost:8000' : '';

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

  const response = await fetch(`${API_BASE}/api/chart/${secId}?${params}`);
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
