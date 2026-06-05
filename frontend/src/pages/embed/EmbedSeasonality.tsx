/**
 * EmbedSeasonality — виджет сезонности (per-ticker). Актив (акция) и разрез
 * (дни недели / дни месяца / месяцы) выбираются в drawer'е настроек.
 * Режимы yearly/price вынесены на полный сайт — для компактного виджета не нужны.
 */
import { useMemo, useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SeasonalityHistogram from '../../components/seasonality/SeasonalityHistogram';
import { getSeasonality, getInstrument, type SeasonalityResponse } from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { EmbedMsg } from './embedUi';
import {
  useEmbedSettings,
  EmbedShell,
  DrawerSection,
  SegGroup,
  AssetPickerInline,
} from './EmbedSettings';

type Mode = 'weekday' | 'monthday' | 'monthly';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Tip = { x: number; y: number; bar?: SeasonalityResponse['bars'][0] } | null;

const MODES: { id: Mode; label: string }[] = [
  { id: 'weekday', label: 'Дни недели' },
  { id: 'monthday', label: 'Дни месяца' },
  { id: 'monthly', label: 'Месяцы' },
];

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}

export default function EmbedSeasonality() {
  const [params] = useSearchParams();
  const settings = useEmbedSettings();

  const [stock, setStock] = useState<string>(() => params.get('instrument') || readLS('frame:embed:seasonality:stock', 'SBER'));
  const [stockName, setStockName] = useState<string>(params.get('name') || '');
  const [mode, setMode] = useState<Mode>(() => readLS('frame:embed:seasonality:mode', 'weekday') as Mode);
  const [data, setData] = useState<SeasonalityResponse | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');
  const [tooltip, setTooltip] = useState<Tip>(null);

  useEffect(() => { try { localStorage.setItem('frame:embed:seasonality:stock', stock); } catch { /* quota */ } }, [stock]);
  useEffect(() => { try { localStorage.setItem('frame:embed:seasonality:mode', mode); } catch { /* quota */ } }, [mode]);

  useEffect(() => {
    if (stockName) return;
    let cancelled = false;
    getInstrument(stock)
      .then((i) => { if (!cancelled && i?.name) setStockName(i.name); })
      .catch(() => { /* имя не критично */ });
    return () => { cancelled = true; };
  }, [stock, stockName]);

  useEffect(() => {
    if (!stock) { setStatus('empty'); return; }
    let cancelled = false;
    setStatus('loading');
    getSeasonality(stock, mode, 9999, false)
      .then((res) => {
        if (cancelled) return;
        setData(res);
        setStatus((res?.bars?.length ?? 0) > 0 ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/seasonality load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [stock, mode]);

  const bars = data?.bars ?? [];
  const maxAbs = useMemo(
    () => (bars.length ? Math.max(...bars.map((b) => Math.abs(b.avg_change))) : 1),
    [bars],
  );

  const displayName = stockName || displayTicker(stock);

  return (
    <EmbedShell
      settings={settings}
      title={displayName}
      subtitle="Сезонность"
      drawer={
        <>
          <DrawerSection label="Актив">
            <AssetPickerInline
              filterType="stock"
              current={stock}
              active={settings.open}
              onSelect={(secid, name) => { setStock(secid); setStockName(name); }}
            />
          </DrawerSection>
          <DrawerSection label="Разрез">
            <SegGroup value={mode} options={MODES} onChange={(v) => setMode(v)} />
          </DrawerSection>
        </>
      }
    >
      <div style={{ flex: 1, position: 'relative', minHeight: 0, overflow: 'hidden' }}>
        {status === 'ok' && bars.length > 0 && (
          <SeasonalityHistogram bars={bars} maxAbs={maxAbs} tooltip={tooltip} setTooltip={setTooltip} />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text={stock ? 'Нет данных' : 'Акция не выбрана'} />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedShell>
  );
}
