/**
 * EmbedSignals — «центр сигналов» для плавающей панели расширения (роут /embed/signals).
 *
 * Лента аномалий/автосигналов (getAnomalyFeed): резкие сдвиги открытого интереса
 * (z-score), притоки/оттоки фондов, и — для залогиненного PRO — личные сработавшие
 * алерты (mine). Каждый сигнал несёт deep_link (route + secid + настройки). Клик →
 * postMessage наверх в widget.js расширения, который открывает компактную панель
 * нужного индикатора на этом активе. В standalone pop-out → открывает страницу сайта.
 *
 * Фаза 1 «центра сигналов» (см. .claude/TERMINAL_EXTENSION_PLAN.md). Бэкенд
 * /api/anomalies/* уже на проде; авторизация — тем же токеном, что и другие embed'ы
 * (apiFetch читает access_token, выставленный EmbedPage после обмена ext-токена).
 * Поллинг 90с + рефетч на возврат фокуса вкладки.
 */
import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { Sparkles, BarChart3, Wallet, Star } from 'lucide-react';
import InstrumentIcon from '../../components/InstrumentIcon';
import { getAnomalyFeed, type AnomalyDeepLink, type AnomalyItem } from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { EmbedMsg } from './embedUi';
import { EmbedFrame, PillGroup } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';
import { useRealtimeData } from '../../hooks/useRealtimeData';

const SITE = 'https://framedata.ru';
const POLL_MS = 90_000;

type SourceKey = 'all' | 'oi' | 'funds' | 'mine';
type LoadStatus = 'loading' | 'ok' | 'error';

// Иконки нужны для compact-режима: на дефолтной ширине панели 380 полный ряд
// «Все·ОИ·Фонды·Мои» не влезал — последняя пилюля обрезалась тулбаром.
const SOURCE_OPTS: { id: SourceKey; label: string; title: string; icon: ReactNode }[] = [
  { id: 'all', label: 'Все', title: 'Все сигналы', icon: <Sparkles size={14} /> },
  { id: 'oi', label: 'ОИ', title: 'Резкие сдвиги открытого интереса', icon: <BarChart3 size={14} /> },
  { id: 'funds', label: 'Фонды', title: 'Притоки/оттоки фондов', icon: <Wallet size={14} /> },
  { id: 'mine', label: 'Мои', title: 'Мои сработавшие уведомления', icon: <Star size={14} /> },
];

const MONO = { fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontVariantNumeric: 'tabular-nums' as const };

// Относительное время «только что / 5 мин / 2 ч / 3 дн».
function ago(iso: string | null): string {
  if (!iso) return '';
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return '';
  const s = Math.max(0, (Date.now() - t) / 1000);
  if (s < 90) return 'только что';
  const m = s / 60;
  if (m < 60) return `${Math.round(m)} мин`;
  const h = m / 60;
  if (h < 24) return `${Math.round(h)} ч`;
  return `${Math.round(h / 24)} дн`;
}

/**
 * `onPick` — режим ПЕСОЧНИЦЫ: клик по сигналу отдаём наверх (SandboxPage спавнит
 * панель нужного индикатора на активе сигнала), а не шлём postMessage. Не задан →
 * прежнее поведение (расширение / standalone pop-out).
 */
export default function EmbedSignals({ onPick }: { onPick?: (dl: AnomalyDeepLink) => void }) {
  const { rd, wr } = useEmbedPersist();
  const [source, setSource] = useState<SourceKey>(() => rd('frame:embed:signals:source', 'all') as SourceKey);
  // Compact-режим тулбара (узкая панель sandbox — см. useToolbarCompact.ts).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();
  const [items, setItems] = useState<AnomalyItem[] | null>(null);
  const [status, setStatus] = useState<LoadStatus>('loading');

  useEffect(() => { wr('frame:embed:signals:source', source); }, [source]);

  // Реалтайм: новая аномалия (SSE 'anomaly') → немедленный рефетч ленты, не
  // дожидаясь 90с поллинга. Тик в депсах перезапускает эффект (и его таймер —
  // безвредно). Ошибку статусом не показываем — load и так тихий.
  const [refreshTick, setRefreshTick] = useState(0);
  useRealtimeData(['anomaly'], () => setRefreshTick((t) => t + 1));

  // Поллинг ленты: старт + каждые 90с + на возврат фокуса. Транзиентную ошибку
  // поллинга не показываем, если данные уже есть (оставляем последнюю ленту).
  useEffect(() => {
    let alive = true;
    const load = () => {
      getAnomalyFeed({ limit: 60, maxAgeHours: 168 })
        .then((feed) => { if (alive) { setItems(feed.items); setStatus('ok'); } })
        .catch(() => { if (alive) setStatus((prev) => (prev === 'ok' ? 'ok' : 'error')); });
    };
    load();
    const timer = window.setInterval(load, POLL_MS);
    const onVis = () => { if (document.visibilityState === 'visible') load(); };
    document.addEventListener('visibilitychange', onVis);
    return () => { alive = false; window.clearInterval(timer); document.removeEventListener('visibilitychange', onVis); };
  }, [refreshTick]);

  const visible = useMemo(() => {
    if (!items) return [];
    return items.filter((it) => {
      switch (source) {
        case 'oi': return it.type.startsWith('oi');
        case 'funds': return it.type === 'funds_flow';
        case 'mine': return it.mine;
        default: return true;
      }
    });
  }, [items, source]);

  const openSignal = (it: AnomalyItem) => {
    const dl = it.deep_link;
    if (onPick) { onPick(dl); return; }
    if (window.parent !== window) {
      // В терминале: widget.js расширения откроет панель индикатора на этом активе.
      window.parent.postMessage({ source: 'frame-embed', type: 'open-signal', deepLink: dl }, '*');
    } else if (dl?.route) {
      // Standalone pop-out (вне iframe): открыть страницу сайта.
      const u = `${SITE}${dl.route}${dl.secid ? `?instrument=${encodeURIComponent(dl.secid)}` : ''}`;
      window.open(u, '_blank', 'noopener');
    }
  };

  return (
    <EmbedFrame
      lead={
        <span style={{ fontWeight: 800, fontSize: 13, letterSpacing: '-0.01em', paddingLeft: 2, flexShrink: 0 }}>
          Сигналы
        </span>
      }
      toolbar={
        <div ref={toolbarWrapRef} style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {/* Невидимый измеритель — см. useToolbarCompact.ts: всегда полные лейблы. */}
          <div ref={toolbarMeasureRef} aria-hidden style={{ position: 'absolute', top: 0, left: 0, visibility: 'hidden', pointerEvents: 'none', display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}>
            <PillGroup<SourceKey> value={source} options={SOURCE_OPTS} onChange={setSource} />
          </div>
          <PillGroup<SourceKey> value={source} options={SOURCE_OPTS} onChange={setSource} compact={toolbarCompact} />
        </div>
      }
      more={
        <div style={{ fontSize: 11, color: 'var(--text-secondary)', lineHeight: 1.5 }}>
          Лента резких движений: сдвиги <b style={{ color: 'var(--text-primary)' }}>открытого интереса</b> (z-score),
          притоки/оттоки <b style={{ color: 'var(--text-primary)' }}>фондов</b> и твои сработавшие уведомления
          (<b style={{ color: 'var(--text-primary)' }}>Мои</b>). Клик по сигналу открывает панель нужного
          индикатора на этом активе.
        </div>
      }
    >
      <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column' }}>
        <div className="styled-scrollbar" style={{ flex: 1, minHeight: 0, overflowY: 'auto' }}>
          {status === 'loading' && !items && <EmbedMsg text="Загрузка…" />}
          {status === 'error' && !items && <EmbedMsg text="Не удалось загрузить сигналы" />}
          {items && visible.length === 0 && (
            <EmbedMsg text={source === 'mine' ? 'Нет сработавших уведомлений' : 'Пока тихо, сигналов нет'} />
          )}
          {visible.map((it) => <SignalRow key={it.id} it={it} onOpen={openSignal} />)}
        </div>
      </div>
    </EmbedFrame>
  );
}

function SignalRow({ it, onOpen }: { it: AnomalyItem; onOpen: (it: AnomalyItem) => void }) {
  const up = it.direction === 'up';
  const down = it.direction === 'down';
  const dirColor = up ? 'var(--oi-green)' : down ? 'var(--oi-red)' : 'var(--text-secondary)';
  const ticker = it.asset_id ? displayTicker(it.asset_id) : '';

  return (
    <button
      type="button"
      onClick={() => onOpen(it)}
      title="Открыть индикатор по этому сигналу"
      style={{
        display: 'flex', alignItems: 'center', gap: 10, width: '100%', textAlign: 'left',
        padding: '9px 12px', border: 'none',
        borderBottom: '0.5px solid var(--border-color, rgba(128,128,128,0.22))',
        // Личные (сработавшие алерты юзера) — мягкая акцентная подсветка.
        background: it.mine ? 'color-mix(in srgb, var(--accent) 8%, transparent)' : 'transparent',
        color: 'var(--text-primary)', cursor: 'pointer',
      }}
    >
      <span style={{ flexShrink: 0, lineHeight: 0 }}>
        <InstrumentIcon sectype={it.asset_id} size={26} />
      </span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontWeight: 700, fontSize: 12.5, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {it.headline}
        </div>
        <div style={{ ...MONO, fontSize: 10.5, color: 'var(--text-secondary)', letterSpacing: '0.03em', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {ticker}{it.context ? ` · ${it.context}` : ''}
        </div>
      </div>
      {it.direction && (
        <span style={{ flexShrink: 0, color: dirColor, fontSize: 15, fontWeight: 800, lineHeight: 1 }}>
          {up ? '↑' : '↓'}
        </span>
      )}
      <span style={{ ...MONO, flexShrink: 0, fontSize: 10.5, color: 'var(--text-muted)', minWidth: 44, textAlign: 'right' }}>
        {ago(it.created_at || it.signal_date)}
      </span>
    </button>
  );
}
