/**
 * EmbedFundTrades — виджет «Сделки фондов» (рыночный). Headline — консенсус-движения
 * за месяц: две компактные колонки «Покупают» (top_accumulated) / «Продают»
 * (top_reduced) с горизонтальными барами по Δвеса. Полный 4-таб экран — на сайте.
 */
import { useEffect, useState } from 'react';
import { getFundTradesMovers, type FundTradesMover } from '../../services/api';
import { EmbedMsg, embedColumn, embedHeader } from './embedUi';

type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';

const POS = 'var(--funds-flow-positive, #4A9268)';
const NEG = 'var(--funds-flow-negative, #C0504D)';

export default function EmbedFundTrades() {
  const [acc, setAcc] = useState<FundTradesMover[]>([]);
  const [red, setRed] = useState<FundTradesMover[]>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getFundTradesMovers('1m', { sort: 'weight', limit: 8 })
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
  }, []);

  return (
    <div style={embedColumn}>
      <div style={embedHeader}>
        <span style={{ fontWeight: 700, fontSize: 14 }}>Сделки фондов</span>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>консенсус за месяц · Δвеса</span>
      </div>
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
    </div>
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
              <span
                style={{
                  whiteSpace: 'nowrap',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  color: 'var(--text-primary)',
                }}
              >
                {m.asset_name}
              </span>
              <span style={{ color, fontWeight: 600, flexShrink: 0 }}>
                {(v > 0 ? '+' : '') + v.toFixed(2)}%
              </span>
            </div>
            <div
              style={{
                height: 4,
                background: 'var(--border-color, rgba(128,128,128,0.18))',
                borderRadius: 2,
                marginTop: 2,
              }}
            >
              <div style={{ height: '100%', width: `${pct}%`, background: color, borderRadius: 2 }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}
