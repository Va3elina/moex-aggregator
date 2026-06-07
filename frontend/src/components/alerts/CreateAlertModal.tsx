/**
 * CreateAlertModal — создание алерта по текущему активу + привязка Telegram.
 * Если Telegram не привязан → шаг «Подключить» (deep-link t.me/<bot>?start=,
 * poll статуса). Если привязан → форма (метрика/условие/порог) → POST /api/alerts.
 * Inline-styles + CSS-vars (как UpgradeModal — переживает portal/тему).
 */
import { useEffect, useState, type CSSProperties } from 'react';
import { Bell, X, Check, ExternalLink, Info } from 'lucide-react';
import {
    getTelegramStatus, createTelegramLink, createAlert, getAlertContext,
    type AlertCreatePayload, type AlertContext,
} from '../../services/api';
import MessengerChoice from './MessengerChoice';

export interface AlertMetricOption {
    key: string;
    label: string;
    indicator: string;   // 'price' | 'oi_zscore'
    metric: string;      // 'close' | 'zscore'
    clgroup?: string;    // OI: 'FIZ' | 'YUR'
    ops: { value: string; label: string }[];
    unit?: string;       // '₽' | 'σ'
    defaultThreshold?: number;
    hint?: string;
}

interface Props {
    indicator: string;
    asset: string;
    assetName?: string;
    metrics: AlertMetricOption[];
    onClose: () => void;
}

const overlay: CSSProperties = {
    position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.5)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 'var(--sp-4)',
};
const card: CSSProperties = {
    background: 'var(--bg-primary)', border: '2px solid var(--text-primary)',
    boxShadow: '5px 5px 0 0 var(--text-primary)', borderRadius: 16, padding: 24,
    width: '100%', maxWidth: 420, maxHeight: '90vh', overflowY: 'auto',
};
const field: CSSProperties = {
    width: '100%', padding: '10px 12px', borderRadius: 10,
    border: '2px solid var(--text-primary)', background: 'var(--bg-secondary)',
    color: 'var(--text-primary)', fontSize: 'var(--fs-sm)',
};
const primaryBtn: CSSProperties = {
    ...field, background: 'var(--accent)', color: 'var(--text-inverse)',
    cursor: 'pointer', fontWeight: 600, textAlign: 'center',
};

// Форматирование рублёвой цены для шапки «Сейчас: …» (1 234,5).
const fmtRub = (v: number): string =>
    v.toLocaleString('ru-RU', { maximumFractionDigits: 2 });

export default function CreateAlertModal({ indicator, asset, assetName, metrics, onClose }: Props) {
    const [linked, setLinked] = useState<boolean | null>(null);  // null = загрузка
    const [linkUrl, setLinkUrl] = useState<string | null>(null);
    const [metricKey, setMetricKey] = useState(metrics[0]?.key ?? '');
    const metric = metrics.find((m) => m.key === metricKey) ?? metrics[0];
    const [op, setOp] = useState(metric?.ops[0]?.value ?? 'cross_up');
    const [threshold, setThreshold] = useState<string>(
        metric?.defaultThreshold != null ? String(metric.defaultThreshold) : '',
    );
    const [mode, setMode] = useState<'once' | 'repeat'>('once');
    const [context, setContext] = useState<AlertContext | null>(null);
    const [busy, setBusy] = useState(false);
    const [msg, setMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);
    const [created, setCreated] = useState(false);

    useEffect(() => {
        getTelegramStatus().then((s) => setLinked(s.linked)).catch(() => setLinked(false));
    }, []);

    // Контекст (свежая цена + intraday-доступность) — грузим когда форма доступна
    // (Telegram привязан). clgroup нужен для oi-метрик; берём из выбранной метрики,
    // иначе дефолт FIZ. Перезапрашиваем при смене актива/clgroup.
    const ctxClgroup = metric?.clgroup ?? 'FIZ';
    useEffect(() => {
        if (linked !== true) return;
        let cancelled = false;
        getAlertContext(indicator, asset, ctxClgroup)
            .then((c) => { if (!cancelled) setContext(c); })
            .catch(() => { if (!cancelled) setContext(null); });
        return () => { cancelled = true; };
    }, [linked, indicator, asset, ctxClgroup]);

    const price = context?.price;
    const hasPrice = price?.value != null;

    // смена метрики → сброс условия + дефолтного порога
    useEffect(() => {
        if (!metric) return;
        setOp(metric.ops[0]?.value ?? 'cross_up');
        setThreshold(metric.defaultThreshold != null ? String(metric.defaultThreshold) : '');
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [metricKey]);

    // poll статуса после генерации ссылки (юзер жмёт Start в боте)
    useEffect(() => {
        if (!linkUrl || linked) return;
        const t = setInterval(async () => {
            try {
                const s = await getTelegramStatus();
                if (s.linked) { setLinked(true); clearInterval(t); }
            } catch { /* ignore */ }
        }, 3000);
        return () => clearInterval(t);
    }, [linkUrl, linked]);

    const handleConnect = async () => {
        setBusy(true); setMsg(null);
        try {
            const { deep_link } = await createTelegramLink();
            setLinkUrl(deep_link);
            window.open(deep_link, '_blank');
        } catch (e) {
            setMsg({ type: 'err', text: (e as Error).message });
        } finally { setBusy(false); }
    };

    const handleCreate = async () => {
        if (!metric) return;
        const th = parseFloat(threshold.replace(',', '.'));
        if (Number.isNaN(th)) { setMsg({ type: 'err', text: 'Введите числовой порог' }); return; }
        setBusy(true); setMsg(null);
        try {
            const payload: AlertCreatePayload = {
                indicator: metric.indicator, asset, asset_name: assetName,
                metric: metric.metric, clgroup: metric.clgroup ?? null,
                op, threshold: th, mode,
                ...(mode === 'repeat' ? { cooldown_hours: 24 } : {}),
            };
            await createAlert(payload);
            setCreated(true);
        } catch (e) {
            setMsg({ type: 'err', text: (e as Error).message });
        } finally { setBusy(false); }
    };

    return (
        <div style={overlay} onClick={onClose}>
            <div style={card} onClick={(e) => e.stopPropagation()}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
                    <span style={{ display: 'flex', alignItems: 'center', gap: 8, fontWeight: 700, fontSize: 'var(--fs-lg)' }}>
                        <Bell size={20} style={{ color: 'var(--accent)' }} /> Новый алерт
                    </span>
                    <button onClick={onClose} aria-label="Закрыть" className="editorial-press" style={{ color: 'var(--text-secondary)', width: 36, height: 36, borderRadius: 8, display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}><X size={20} /></button>
                </div>

                {created ? (
                    <div style={{ textAlign: 'center', padding: '12px 0' }}>
                        <Check size={40} style={{ color: 'var(--accent)', margin: '0 auto 8px', display: 'block' }} />
                        <div style={{ fontWeight: 600, marginBottom: 4 }}>Алерт создан</div>
                        <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)', marginBottom: 16 }}>
                            Уведомление придёт в Telegram (@framesignalbot).
                        </div>
                        <button onClick={onClose} className="editorial-press" style={{ ...primaryBtn, width: 'auto', padding: '10px 24px', margin: '0 auto', display: 'inline-block' }}>Готово</button>
                    </div>
                ) : linked === null ? (
                    <div style={{ color: 'var(--text-secondary)', textAlign: 'center', padding: '20px 0' }}>Загрузка…</div>
                ) : !linked ? (
                    <div>
                        {linkUrl ? (
                            <>
                                <p style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)', marginBottom: 12, lineHeight: 1.5 }}>
                                    Откройте бота, нажмите <b>Start</b> — и вернитесь сюда. Статус обновится сам.
                                </p>
                                <a href={linkUrl} target="_blank" rel="noreferrer" className="editorial-press"
                                    style={{ ...primaryBtn, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8, textDecoration: 'none', marginBottom: 8 }}>
                                    <ExternalLink size={16} /> Открыть @framesignalbot
                                </a>
                                <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)', textAlign: 'center' }}>Ждём подключения…</div>
                            </>
                        ) : (
                            <>
                                <p style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)', marginBottom: 16, lineHeight: 1.5 }}>
                                    Чтобы получать алерты, подключите мессенджер. После этого все ваши алерты будут приходить туда.
                                </p>
                                <MessengerChoice onTelegram={handleConnect} busy={busy} />
                            </>
                        )}
                    </div>
                ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                        <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                            Актив: <b style={{ color: 'var(--text-primary)' }}>{assetName || asset}</b>
                            {hasPrice && (
                                <>
                                    {' · '}Сейчас: <b style={{ color: 'var(--text-primary)' }}>{fmtRub(price!.value!)} руб</b>
                                </>
                            )}
                        </div>
                        <label style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Метрика
                            <select value={metricKey} onChange={(e) => setMetricKey(e.target.value)} style={{ ...field, marginTop: 4 }}>
                                {metrics.map((m) => <option key={m.key} value={m.key}>{m.label}</option>)}
                            </select>
                        </label>
                        <label style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Условие
                            <select value={op} onChange={(e) => setOp(e.target.value)} style={{ ...field, marginTop: 4 }}>
                                {metric?.ops.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                            </select>
                        </label>
                        <label style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                            Порог {metric?.unit ? `(${metric.unit})` : ''}
                            <input type="text" inputMode="decimal" value={threshold}
                                onChange={(e) => setThreshold(e.target.value)}
                                placeholder={
                                    metric?.indicator === 'price' && hasPrice ? fmtRub(price!.value!)
                                        : metric?.unit === 'σ' ? '2.5' : '0'
                                } style={{ ...field, marginTop: 4 }} />
                        </label>

                        {/* Режим срабатывания */}
                        <label style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Режим
                            <select value={mode} onChange={(e) => setMode(e.target.value as 'once' | 'repeat')} style={{ ...field, marginTop: 4 }}>
                                <option value="once">Уведомить один раз</option>
                                <option value="repeat">Каждый раз</option>
                            </select>
                        </label>

                        {metric?.hint && (
                            <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', lineHeight: 1.4 }}>{metric.hint}</div>
                        )}

                        {/* Цена без внутридневных данных — проверка раз в день */}
                        {metric?.indicator === 'price' && context && price?.intraday === false && (
                            <div style={{
                                display: 'flex', alignItems: 'flex-start', gap: 8,
                                fontSize: 'var(--fs-xs)', lineHeight: 1.4,
                                color: 'var(--text-primary)', background: 'var(--bg-secondary)',
                                border: '2px solid var(--text-primary)', borderRadius: 10,
                                padding: '8px 10px',
                            }}>
                                <Info size={16} style={{ flex: '0 0 auto', marginTop: 1, color: 'var(--accent)' }} />
                                <span>У этого актива нет внутридневных данных — проверка раз в день после закрытия.</span>
                            </div>
                        )}

                        {/* OI-аномалия обновляется раз в день */}
                        {metric?.indicator === 'oi_zscore' && (
                            <div style={{
                                display: 'flex', alignItems: 'flex-start', gap: 8,
                                fontSize: 'var(--fs-xs)', lineHeight: 1.4,
                                color: 'var(--text-secondary)',
                            }}>
                                <Info size={16} style={{ flex: '0 0 auto', marginTop: 1, color: 'var(--accent)' }} />
                                <span>Аномалия OI обновляется раз в день после публикации позиций МосБиржи.</span>
                            </div>
                        )}

                        <button disabled={busy} onClick={handleCreate} className="editorial-press" style={primaryBtn}>
                            {busy ? 'Создаём…' : 'Создать алерт'}
                        </button>
                    </div>
                )}

                {msg && (
                    <div style={{ marginTop: 12, fontSize: 'var(--fs-sm)', color: msg.type === 'ok' ? 'var(--accent)' : 'var(--funds-flow-negative, #FF7A5C)' }}>
                        {msg.text}
                    </div>
                )}
            </div>
        </div>
    );
}
