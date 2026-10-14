import { useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { getInstrument } from '../services/api';

const TICKER_RE = /^[A-Za-z0-9_]{1,24}$/;

/**
 * Диплинк на /seasonality из Telegram-уведомлений (signals/season_notify.py): ?ticker=SBER&chart=yearly.
 * Применяется один раз на маунте и стирается из адреса: дальше выбор живёт в localStorage, как обычно,
 * и перезагрузка после смены актива не возвращает к активу из ссылки. Имя резолвится через
 * /api/instruments/{ticker}; пока ответа нет, в шапке стоит сам тикер.
 */
export function useSeasonalityDeepLink(apply: (ticker: string, name: string, chart: 'yearly' | null) => void) {
  const [searchParams, setSearchParams] = useSearchParams();
  useEffect(() => {
    const ticker = searchParams.get('ticker');
    if (!ticker || !TICKER_RE.test(ticker)) return;
    const chart = searchParams.get('chart') === 'yearly' ? 'yearly' : null;
    apply(ticker, ticker, chart);
    getInstrument(ticker).then((inst) => {
      if (inst?.name) apply(ticker, inst.name, chart);
    });
    const next = new URLSearchParams(searchParams);
    next.delete('ticker');
    next.delete('chart');
    setSearchParams(next, { replace: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
}
