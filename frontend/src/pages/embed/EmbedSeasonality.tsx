/**
 * EmbedSeasonality — виджет сезонности (per-ticker). Инструмент (акция) выбирается
 * внутри через InstrumentSearchModal. Headline — гистограмма (weekday/monthday/monthly).
 * Режимы yearly/price вынесены на полный сайт (для компактного виджета не нужны).
 */
import { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SeasonalityHistogram from '../../components/seasonality/SeasonalityHistogram';
import InstrumentSearchModal from '../../components/InstrumentSearchModal';
import { getSeasonality, getInstrument, type SeasonalityResponse } from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { EmbedMsg, embedColumn, embedHeader, pickerBtn, segBtn } from './embedUi';

type Mode = 'weekday' | 'monthday' | 'monthly';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Tip = { x: number; y: number; bar?: SeasonalityResponse['bars'][0] } | null;

const MODES: { id: Mode; label: string }[] = [
  { id: 'weekday', label: 'Дни недели' },
  { id: 'monthday', label: 'Дни месяца' },
  { id: 'monthly', label: 'Месяцы' },
];

function initStock(p: string | null): string {
  if (p) return p;
  try {
    return localStorage.getItem('frame:seasonality:stock') || 'SBER';
  } catch {
    return 'SBER';
  }
}

function initMode(): Mode {
  try {
    return (localStorage.getItem('frame:seasonality:mode') as Mode) || 'weekday';
  } catch {
    return 'weekday';
  }
}

export default function EmbedSeasonality() {
  const [params] = useSearchParams();
  const [stock, setStock] = useState<string>(() => initStock(params.get('instrument')));
  const [stockName, setStockName] = useState<string>(params.get('name') || '');
  const [mode, setMode] = useState<Mode>(initMode);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [data, setData] = useState<SeasonalityResponse | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');
  const [tooltip, setTooltip] = useState<Tip>(null);

  useEffect(() => {
    try { localStorage.setItem('frame:seasonality:stock', stock); } catch { /* quota */ }
  }, [stock]);
  useEffect(() => {
    try { localStorage.setItem('frame:seasonality:mode', mode); } catch { /* quota */ }
  }, [mode]);

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
    <div style={embedColumn}>
      <div style={embedHeader}>
        <button style={pickerBtn} onClick={() => setPickerOpen(true)} title="Выбрать акцию">
          <span style={{ fontWeight: 700, fontSize: 14 }}>{displayName}</span>
          <span style={{ opacity: 0.5, fontSize: 11 }}>▾</span>
        </button>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>Сезонность</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 4 }}>
          {MODES.map((m) => (
            <button key={m.id} style={segBtn(mode === m.id)} onClick={() => setMode(m.id)}>
              {m.label}
            </button>
          ))}
        </div>
      </div>
      <div style={{ flex: 1, position: 'relative', minHeight: 0, overflow: 'hidden' }}>
        {status === 'ok' && bars.length > 0 && (
          <SeasonalityHistogram bars={bars} maxAbs={maxAbs} tooltip={tooltip} setTooltip={setTooltip} />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text={stock ? 'Нет данных' : 'Акция не выбрана'} />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>

      {pickerOpen && (
        <InstrumentSearchModal
          filterType="stock"
          indicator="seasonality"
          onSelect={(secid, name) => {
            setStock(secid);
            setStockName(name);
            setPickerOpen(false);
          }}
          onClose={() => setPickerOpen(false)}
        />
      )}
    </div>
  );
}
