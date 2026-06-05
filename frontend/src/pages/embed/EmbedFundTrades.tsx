/**
 * EmbedFundTrades — виджет «Сделки фондов» (рыночный). Headline — консенсус-движения
 * за период: две колонки «Покупают» (top_accumulated) / «Продают» (top_reduced) с
 * горизонтальными барами по Δвеса. Окно периода — в drawer'е настроек.
 * Полный 4-таб экран — на сайте.
 */
import { useEffect, useState } from 'react';
import { getFundTradesMovers, type FundTradesMover, type FundTradesPeriod } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { useEmbedSettings, EmbedShell, DrawerSection, SegGroup } from './EmbedSettings';

type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';

const POS = 'var(--funds-flow-positive, #4A9268)';
const NEG = 'var(--funds-flow-negative, #C0504D)';

const PERIODS: { id: FundTradesPeriod; label: string }[] = [
  { id: '1m', label: 'Месяц' },
  { id: '3m', label: '3 месяца' },
  { id: '6m', label: 'Полгода' },
  { id: '1y', label: 'Год' },
];
const P_SHORT: Record<FundTradesPeriod, string> = { '1m': 'за месяц', '3m': 'за 3 мес', '6m': 'за полгода', '1y': 'за год' };

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}

export default function EmbedFundTrades() {
  const settings = useEmbedSettings();
  const [period, setPeriod] = useState<FundTradesPeriod>(() => readLS('frame:embed:fundtrades:period', '1m') as FundTradesPeriod);
  const [acc, setAcc] = useState<FundTradesMover[]>([]);
  const [red, setRed] = useState<FundTradesMover[]>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { try { localStorage.setItem('frame:embed:fundtrades:period', period); } catch { /* quota */ } }, [period]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getFundTradesMovers(period, { sort: 'weight', limit: 8 })
      .then((res) => {
        if (cancelled) return;
        const a = res?.top_accumulated ?? [];
        const r = res?.top_reduced ?? [];
        setAcc(a);
        setRed(r);
        setStatus(a.length || r.length ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/fund-trades load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [period]);

  return (
    <EmbedShell
      settings={settings}
      title="Сделки фондов"
      subtitle={`консенсус ${P_SHORT[period]} · Δвеса`}
      drawer={
        <DrawerSection label="Окно периода">
          <SegGroup value={period} options={PERIODS} onChange={(v) => setPeriod(v)} />
        </DrawerSection>
      }
    >
      <div style={{ flex: 1, minHeight: 0, overflow: 'auto', display: 'flex', gap: 12, position: 'relative' }}>
        {status === 'ok' ? (
          <>
            <MoverCol title="Покупают" color={POS} items={acc} />
            <MoverCol title="Продают" color={NEG} items={red} />
          </>
        ) : (
          <>
            {status === 'loading' && <EmbedMsg text="Загрузка…" />}
            {status === 'empty' && <EmbedMsg text="Нет данных" />}
            {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
          </>
        )}
      </div>
    </EmbedShell>
  );
}

function MoverCol({ title, color, items }: { title: string; color: string; items: FundTradesMover[] }) {
  const maxAbs = Math.max(...items.map((m) => Math.abs(m.total_delta_weight)), 0.0001);
  return (
    <div style={{ flex: 1, minWidth: 0 }}>
      <div
        style={{
          fontSize: 11,
          fontWeight: 800,
          color,
          borderBottom: `1px solid ${color}`,
          paddingBottom: 4,
          marginBottom: 6,
        }}
      >
        {title}
      </div>
      {items.slice(0, 8).map((m) => {
        const v = m.total_delta_weight;
        const pct = Math.max(3, (Math.abs(v) / maxAbs) * 100);
        return (
          <div key={m.akey} style={{ marginBottom: 6 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: 6, fontSize: 11 }}>
              <span style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', color: 'var(--text-primary)' }}>
                {m.asset_name}
              </span>
              <span style={{ color, fontWeight: 600, flexShrink: 0 }}>
                {(v > 0 ? '+' : '') + v.toFixed(2)}%
              </span>
            </div>
            <div style={{ height: 4, background: 'var(--border-color, rgba(128,128,128,0.18))', borderRadius: 2, marginTop: 2 }}>
              <div style={{ height: '100%', width: `${pct}%`, background: color, borderRadius: 2 }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}
