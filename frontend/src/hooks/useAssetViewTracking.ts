import { useEffect } from 'react';
import { useAnalytics } from '../contexts/AnalyticsContext';

/**
 * Пишет asset_view при каждом показе актива на индикаторе — откуда бы он ни
 * пришёл: поиск, ссылка, сохранённый с прошлого раза выбор, мобильный пикер.
 * На этом событии держится «Топ активов» в /admin/stats. instrument_select
 * ловит только выбор в поиске и сильно занижает популярные активы, которые
 * открываются по умолчанию.
 *
 * indicator — короткий ключ страницы (oi / seasonality / repo).
 */
export function useAssetViewTracking(indicator: string, secid: string | null | undefined) {
  const { track } = useAnalytics();
  useEffect(() => {
    if (!secid) return;
    track('asset_view', { secid, indicator });
    // track стабилен в пределах согласия; повторный показ того же актива
    // после смены согласия — не беда.
  }, [indicator, secid, track]);
}
