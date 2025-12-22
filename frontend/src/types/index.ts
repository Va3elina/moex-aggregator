// ==================== ИНСТРУМЕНТЫ ====================

export interface Instrument {
  sec_id: string;
  sectype: string;
  name: string;
  type: string | null;
  group: string | null;
  iss_code: string | null;
}

export interface InstrumentGroup {
  sectype: string;
  name: string;
  type: string;
  contracts: string[];
  contracts_count: number;
}

// ==================== СВЕЧИ ====================

export interface Candle {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

// ==================== ОТКРЫТЫЙ ИНТЕРЕС ====================

export interface OpenInterest {
  time: string;
  pos: number | null;
  pos_long: number | null;
  pos_short: number | null;
  pos_long_num: number | null;
  pos_short_num: number | null;
}

// ==================== ГРАФИК ====================

export interface ChartResponse {
  sec_id: string;
  sectype: string;
  interval: number;
  clgroup: string;
  candles_count: number;
  oi_count: number;
  candles: Candle[];
  open_interest: OpenInterest[];
  oi_start_date: string | null;
  oi_end_date: string | null;
  candles_start_date: string | null;
  candles_end_date: string | null;
  has_oi_data: boolean;
  contracts: string[];
  mode: string;
  period: string;
  data_start: string | null;
  data_end: string | null;
  available_intervals: number[];
}

// ==================== СТАТИСТИКА ====================

export interface StatsData {
  total_oi: number;
  oi_change: number;
  total_long: number;
  long_change: number;
  total_short: number;
  short_change: number;
  participants: number;
  participants_change: number;
  instruments_count: number;
}

export interface ChartDataPoint {
  date: string;
  total_oi: number;
  total_long: number;
  total_short: number;
  net_position: number;
  total_long_num: number;
  total_short_num: number;
}

export interface StatsResponse {
  period: string;
  clgroup: string;
  stats: StatsData;
  chart_data: ChartDataPoint[];
  data_points: number;
}

export interface TopInstrument {
  sectype: string;
  current_oi: number;
  current_long: number;
  current_short: number;
  net_position: number;
  oi_change: number;
}

export interface TopInstrumentsResponse {
  period: string;
  clgroup: string;
  sort_by: string;
  instruments: TopInstrument[];
}