/**
 * CreateAlertModal — конструктор алертов + привязка Telegram.
 * Если Telegram не привязан → шаг «Подключить» (deep-link t.me/<bot>?start=,
 * poll статуса). Если привязан → конструктор:
 *   • для oi_move — мульти-выбор активов + уровень сигнала (ступени) → N алертов;
 *   • для price — один текущий актив + условие + порог в ₽ → 1 алерт.
 * Inline-styles + CSS-vars (как UpgradeModal — переживает portal/тему).
 */
import { useEffect, useMemo, useState, type CSSProperties } from 'react';
import { Bell, X, Check, ExternalLink, Info, Plus } from 'lucide-react';
import {
    getTelegramStatus, createTelegramLink, createAlert, getAlertContext,
    getInstruments,
    type AlertCreatePayload, type AlertContext,
} from '../../services/api';
import MessengerChoice from './MessengerChoice';
import InstrumentSearchModal from '../InstrumentSearchModal';

export interface AlertMetricOption {
    key: string;
    label: string;
    indicator: string;   // 'price' | 'oi_move'
    metric: string;      // 'close' | 'atr'
    clgroup?: string;    // OI: 'FIZ' | 'YUR'
    ops: { value: string; label: string }[];
    unit?: string;       // '₽' | '×'
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

// Русское склонение: 1 алерт / 2 алерта / 5 алертов; 1 актив / 2 актива / 5 активов.
const plural = (n: number, one: string, few: string, many: string): string => {
    const m10 = n % 10, m100 = n % 100;
    if (m10 === 1 && m100 !== 11) return one;
    if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return few;
    return many;
};
const alertsWord = (n: number) => plural(n, 'алерт', 'алерта', 'алертов');
const assetsWord = (n: number) => plural(n, 'актив', 'актива', 'активов');

// Уровни сигнала для oi_move — ступени множителя ATR (×). «Сильное» по умолчанию.
// freq — калибровка по частоте на ликвидных бумагах (см. блок-формулу ниже).
type SignalLevel = { key: string; label: string; mult: number; freq: string };
const SIGNAL_LEVELS: SignalLevel[] = [
    { key: 'notable', label: 'Заметное', mult: 2, freq: 'примерно раз в неделю' },
    { key: 'strong', label: 'Сильное', mult: 3, freq: 'раз в 2–3 недели' },
    { key: 'extreme', label: 'Экстремальное', mult: 5, freq: 'раз в 2 месяца' },
];
const PRESETS: { key: string; label: string; group?: string }[] = [
    { key: 'fav', label: 'Избранное' },
    { key: 'Акции', label: 'Акции', group: 'Акции' },
    { key: 'Сырьё', label: 'Сырьё', group: 'Сырьё' },
    { key: 'Валюта', label: 'Валюта', group: 'Валюта' },
    { key: 'Индексы', label: 'Индексы', group: 'Индексы' },
];

// Чип выбранного актива (тикер + крестик) и пресет-кнопка — общий editorial-стиль.
const chipStyle: CSSProperties = {
    display: 'inline-flex', alignItems: 'center', gap: 6,
    padding: '5px 8px 5px 12px', borderRadius: 999,
    border: '2px solid var(--text-primary)', background: 'var(--bg-secondary)',
    color: 'var(--text-primary)', fontSize: 'var(--fs-xs)', fontWeight: 600,
};
const ghostBtn: CSSProperties = {
    display: 'inline-flex', alignItems: 'center', gap: 4,
    padding: '6px 12px', borderRadius: 999,
    border: '2px dashed var(--text-secondary)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)', fontWeight: 600,
    cursor: 'pointer',
};

export default function CreateAlertModal({ indicator, asset, assetName, metrics, onClose }: Props) {
    const [linked, setLinked] = useState<boolean | null>(null);  // null = загрузка
    const [linkUrl, setLinkUrl] = useState<string | null>(null);
    const [metricKey, setMetricKey] = useState(metrics[0]?.key ?? '');
    const metric = metrics.find((m) => m.key === metricKey) ?? metrics[0];
    const isOiMove = metric?.indicator === 'oi_move';

    const [op, setOp] = useState(metric?.ops[0]?.value ?? 'cross_up');
    // price → числовой порог в ₽; oi_move → не используется (порог = множитель ступени).
    const [threshold, setThreshold] = useState<string>(
        metric?.defaultThreshold != null ? String(metric.defaultThreshold) : '',
    );
    const [mode, setMode] = useState<'once' | 'repeat'>('once');
    const [context, setContext] = useState<AlertContext | null>(null);
    const [busy, setBusy] = useState(false);
    const [msg, setMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);
    const [created, setCreated] = useState(false);
    const [createdCount, setCreatedCount] = useState(0);

    // ── Мульти-выбор активов (только oi_move) ───────────────────────────────
    // Выбранные активы как map sectype→name; по умолчанию — текущий актив.
    const [selected, setSelected] = useState<Record<string, string>>(
        () => ({ [asset]: assetName || asset }),
    );
    const [pickerOpen, setPickerOpen] = useState(false);
    // Справочник фьючерсов (имена + группы) для пресетов «Акции/Сырьё/…».
    const [futures, setFutures] = useState<{ sectype: string; name: string; group: string | null }[]>([]);

    // ── Уровень сигнала (только oi_move) ────────────────────────────────────
    const [levelKey, setLevelKey] = useState('strong');       // default «Сильное»
    const [customMult, setCustomMult] = useState('');          // ввод для «Своё значение»
    const isCustomLevel = levelKey === 'custom';
    const activeLevel = SIGNAL_LEVELS.find((l) => l.key === levelKey);

    useEffect(() => {
        getTelegramStatus().then((s) => setLinked(s.linked)).catch(() => setLinked(false));
    }, []);

    // Справочник фьючерсов — грузим один раз (имена/секторы для чипов и пресетов).
    useEffect(() => {
        let cancelled = false;
        getInstruments('futures')
            .then((r) => {
                if (cancelled) return;
                setFutures(r.instruments.map((i) => ({ sectype: i.sectype, name: i.name, group: i.group })));
            })
            .catch(() => { if (!cancelled) setFutures([]); });
        return () => { cancelled = true; };
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

    // ── Выбор активов (oi_move) ─────────────────────────────────────────────
    const selectedList = useMemo(
        () => Object.entries(selected).map(([sectype, name]) => ({ sectype, name })),
        [selected],
    );
    const selectedSectypes = useMemo(() => Object.keys(selected), [selected]);

    const toggleAsset = (sectype: string, name: string) => {
        setSelected((prev) => {
            if (prev[sectype]) {
                const next = { ...prev };
                delete next[sectype];
                return next;
            }
            return { ...prev, [sectype]: name };
        });
    };
    const removeAsset = (sectype: string) => {
        setSelected((prev) => {
            const next = { ...prev };
            delete next[sectype];
            return next;
        });
    };
    // Пресет добавляет НАБОР в текущий выбор (не заменяет). «Избранное» — из
    // localStorage; группы — фильтр справочника фьючерсов по group.
    const addPreset = (preset: typeof PRESETS[number]) => {
        let add: { sectype: string; name: string }[];
        if (preset.key === 'fav') {
            let favs: string[] = [];
            try { favs = JSON.parse(localStorage.getItem('favoriteInstruments') || '[]'); } catch { favs = []; }
            const favSet = new Set(favs);
            add = futures.filter((f) => favSet.has(f.sectype)).map((f) => ({ sectype: f.sectype, name: f.name }));
        } else {
            add = futures.filter((f) => f.group === preset.group).map((f) => ({ sectype: f.sectype, name: f.name }));
        }
        if (add.length === 0) return;
        setSelected((prev) => {
            const next = { ...prev };
            for (const a of add) next[a.sectype] = a.name;
            return next;
        });
    };

    // Множитель ступени для oi_move (числовое значение порога).
    const resolvedMult = (): number | null => {
        if (isCustomLevel) {
            const v = parseFloat(customMult.replace(',', '.'));
            return Number.isNaN(v) ? null : v;
        }
        return activeLevel?.mult ?? null;
    };

    const handleCreate = async () => {
        if (!metric) return;

        // ── price: один актив, числовой порог в ₽ (старое поведение) ─────────
        if (!isOiMove) {
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
                setCreatedCount(1);
                setCreated(true);
            } catch (e) {
                setMsg({ type: 'err', text: (e as Error).message });
            } finally { setBusy(false); }
            return;
        }

        // ── oi_move: N активов × ступень → N алертов ─────────────────────────
        const mult = resolvedMult();
        if (mult == null || mult <= 0) { setMsg({ type: 'err', text: 'Укажите множитель уровня' }); return; }
        if (selectedList.length === 0) { setMsg({ type: 'err', text: 'Выберите хотя бы один актив' }); return; }
        const oiOp = metric.ops[0]?.value ?? 'gt';
        setBusy(true); setMsg(null);
        const errors: string[] = [];
        let ok = 0;
        // Параллельно (по одному запросу на актив), но ошибки ловим per-promise,
        // чтобы один отказ не валил всю пачку. setBusy(false) — в finally, иначе
        // неожиданное исключение навсегда заблокирует кнопку.
        try {
            const results = await Promise.all(selectedList.map(async (a) => {
                try {
                    const payload: AlertCreatePayload = {
                        indicator: metric.indicator, asset: a.sectype, asset_name: a.name,
                        metric: metric.metric, clgroup: metric.clgroup ?? null,
                        op: oiOp, threshold: mult, mode,
                        ...(mode === 'repeat' ? { cooldown_hours: 24 } : {}),
                    };
                    await createAlert(payload);
                    return true;
                } catch (e) {
                    errors.push(`${a.sectype}: ${(e as Error).message}`);
                    return false;
                }
            }));
            ok = results.filter(Boolean).length;
        } finally {
            setBusy(false);
        }
        if (ok > 0) {
            setCreatedCount(ok);
            setCreated(true);
            if (errors.length) {
                setMsg({ type: 'err', text: `Не удалось создать ${errors.length}: ${errors[0]}` });
            }
        } else {
            setMsg({ type: 'err', text: errors[0] || 'Не удалось создать алерты' });
        }
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
                        <div style={{ fontWeight: 600, marginBottom: 4 }}>
                            {createdCount > 1
                                ? `${createdCount} ${alertsWord(createdCount)} создано`
                                : 'Алерт создан'}
                        </div>
                        <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)', marginBottom: 16 }}>
                            {createdCount > 1 ? 'Уведомления придут' : 'Уведомление придёт'} в Telegram (@framesignalbot).
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
                        {/* ── ВЫБОР АКТИВОВ ──────────────────────────────────── */}
                        {isOiMove ? (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Активы</div>
                                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                                    {selectedList.map((a) => (
                                        <span key={a.sectype} style={chipStyle}>
                                            {a.sectype}
                                            <button
                                                onClick={() => removeAsset(a.sectype)}
                                                aria-label={`Убрать ${a.sectype}`}
                                                className="editorial-press"
                                                style={{
                                                    display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                                                    width: 18, height: 18, borderRadius: 999,
                                                    color: 'var(--text-secondary)',
                                                }}
                                            >
                                                <X size={13} />
                                            </button>
                                        </span>
                                    ))}
                                    <button onClick={() => setPickerOpen(true)} className="editorial-press" style={ghostBtn}>
                                        <Plus size={13} /> добавить
                                    </button>
                                </div>
                                {/* Пресеты — добавляют набор в выбор */}
                                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                                    {PRESETS.map((p) => (
                                        <button
                                            key={p.key}
                                            onClick={() => addPreset(p)}
                                            className="editorial-press"
                                            style={{
                                                padding: '4px 10px', borderRadius: 999,
                                                border: '2px solid var(--text-primary)', background: 'var(--bg-primary)',
                                                color: 'var(--text-primary)', fontSize: 'var(--fs-xs)', fontWeight: 600,
                                                cursor: 'pointer',
                                            }}
                                        >
                                            {p.label}
                                        </button>
                                    ))}
                                </div>
                            </div>
                        ) : (
                            <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                                Актив: <b style={{ color: 'var(--text-primary)' }}>{assetName || asset}</b>
                                {hasPrice && (
                                    <>
                                        {' · '}Сейчас: <b style={{ color: 'var(--text-primary)' }}>{fmtRub(price!.value!)} руб</b>
                                    </>
                                )}
                            </div>
                        )}

                        {/* ── МЕТРИКА (применяется ко всем выбранным активам) ── */}
                        <label style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Метрика
                            <select value={metricKey} onChange={(e) => setMetricKey(e.target.value)} style={{ ...field, marginTop: 4 }}>
                                {metrics.map((m) => <option key={m.key} value={m.key}>{m.label}</option>)}
                            </select>
                        </label>

                        {/* ── УРОВЕНЬ СИГНАЛА (oi_move) / УСЛОВИЕ+ПОРОГ (price) ─ */}
                        {isOiMove ? (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Уровень сигнала</div>
                                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                                    {SIGNAL_LEVELS.map((l) => {
                                        const checked = levelKey === l.key;
                                        return (
                                            <label key={l.key} style={{
                                                display: 'flex', alignItems: 'center', gap: 10,
                                                padding: '8px 10px', borderRadius: 10, cursor: 'pointer',
                                                border: '2px solid var(--text-primary)',
                                                background: checked ? 'var(--bg-secondary)' : 'var(--bg-primary)',
                                                boxShadow: checked ? '3px 3px 0 0 var(--text-primary)' : undefined,
                                            }}>
                                                <input
                                                    type="radio" name="oi-level" value={l.key}
                                                    checked={checked} onChange={() => setLevelKey(l.key)}
                                                    style={{ accentColor: 'var(--accent)', width: 16, height: 16, flex: '0 0 auto' }}
                                                />
                                                <span style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
                                                    <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-primary)' }}>
                                                        {l.label}
                                                        <span style={{ color: 'var(--text-secondary)', fontWeight: 600 }}> · {l.mult}×</span>
                                                    </span>
                                                    <span style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>{l.freq}</span>
                                                </span>
                                            </label>
                                        );
                                    })}
                                    {/* Своё значение */}
                                    <label style={{
                                        display: 'flex', alignItems: 'center', gap: 10,
                                        padding: '8px 10px', borderRadius: 10, cursor: 'pointer',
                                        border: '2px solid var(--text-primary)',
                                        background: isCustomLevel ? 'var(--bg-secondary)' : 'var(--bg-primary)',
                                        boxShadow: isCustomLevel ? '3px 3px 0 0 var(--text-primary)' : undefined,
                                    }}>
                                        <input
                                            type="radio" name="oi-level" value="custom"
                                            checked={isCustomLevel} onChange={() => setLevelKey('custom')}
                                            style={{ accentColor: 'var(--accent)', width: 16, height: 16, flex: '0 0 auto' }}
                                        />
                                        <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-primary)' }}>Своё значение</span>
                                        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, marginLeft: 'auto' }}>
                                            <input
                                                type="text" inputMode="decimal" value={customMult}
                                                onChange={(e) => { setCustomMult(e.target.value); setLevelKey('custom'); }}
                                                onFocus={() => setLevelKey('custom')}
                                                placeholder="4"
                                                style={{ ...field, width: 64, padding: '6px 8px', textAlign: 'right' }}
                                            />
                                            <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)' }}>×</span>
                                        </span>
                                    </label>
                                </div>
                            </div>
                        ) : (
                            <>
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
                                                : metric?.unit === 'σ' ? '2.5' : metric?.unit === '×' ? '3' : '0'
                                        } style={{ ...field, marginTop: 4 }} />
                                </label>
                            </>
                        )}

                        {/* Режим срабатывания */}
                        <label style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Режим
                            <select value={mode} onChange={(e) => setMode(e.target.value as 'once' | 'repeat')} style={{ ...field, marginTop: 4 }}>
                                <option value="once">Уведомить один раз</option>
                                <option value="repeat">Каждый раз</option>
                            </select>
                        </label>

                        {/* ── ТЕКСТ ФОРМУЛЫ (oi_move) ─────────────────────────── */}
                        {isOiMove && (
                            <div style={{
                                borderTop: '1px solid var(--border-color)', paddingTop: 12,
                                fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', lineHeight: 1.5,
                            }}>
                                <div style={{ fontWeight: 700, color: 'var(--text-primary)', marginBottom: 4 }}>
                                    Как считается резкость движения
                                </div>
                                Берётся чистая позиция выбранной группы (физические или юридические лица)
                                и её изменение за день. Это изменение делится на средний дневной шаг позиции
                                за последние 14 торговых дней. Получается «во сколько раз сегодняшнее движение
                                больше обычного»: 1 — обычный день, 3 — втрое сильнее обычного. Уровни
                                откалиброваны по частоте на ликвидных бумагах: «Заметное» случается примерно
                                раз в неделю, «Сильное» — раз в две-три недели, «Экстремальное» — раз в два месяца.
                                Алерт описывает движение позиций, а не прогноз цены.
                            </div>
                        )}

                        {metric?.hint && !isOiMove && (
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
                        {isOiMove && (
                            <div style={{
                                display: 'flex', alignItems: 'flex-start', gap: 8,
                                fontSize: 'var(--fs-xs)', lineHeight: 1.4,
                                color: 'var(--text-secondary)',
                            }}>
                                <Info size={16} style={{ flex: '0 0 auto', marginTop: 1, color: 'var(--accent)' }} />
                                <span>Позиции МосБиржи обновляются раз в день. Это описание движения, а не прогноз цены.</span>
                            </div>
                        )}

                        {/* Сводка для oi_move: N актив(ов) × <уровень> = N алертов */}
                        {isOiMove && (
                            <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                                <b style={{ color: 'var(--text-primary)' }}>{selectedList.length}</b> {assetsWord(selectedList.length)}
                                {' × '}
                                <b style={{ color: 'var(--text-primary)' }}>
                                    {isCustomLevel ? `${customMult || '—'}×` : activeLevel?.label}
                                </b>
                                {' = '}
                                <b style={{ color: 'var(--text-primary)' }}>{selectedList.length}</b> {alertsWord(selectedList.length)}
                            </div>
                        )}

                        <button
                            disabled={busy || (isOiMove && selectedList.length === 0)}
                            onClick={handleCreate}
                            className="editorial-press"
                            style={primaryBtn}
                        >
                            {busy
                                ? 'Создаём…'
                                : isOiMove
                                    ? `Создать ${selectedList.length} ${alertsWord(selectedList.length)}`
                                    : 'Создать алерт'}
                        </button>
                    </div>
                )}

                {msg && (
                    <div style={{ marginTop: 12, fontSize: 'var(--fs-sm)', color: msg.type === 'ok' ? 'var(--accent)' : 'var(--funds-flow-negative, #FF7A5C)' }}>
                        {msg.text}
                    </div>
                )}
            </div>

            {/* Мульти-выбор активов поверх конструктора. InstrumentSearchModal —
                fixed inset-0 z-50; оборачиваем в слой с z выше нашего overlay
                (9999), чтобы пикер был сверху. stopPropagation — клик внутри не
                закрывает конструктор. */}
            {pickerOpen && (
                <div
                    style={{ position: 'fixed', inset: 0, zIndex: 10000 }}
                    onClick={(e) => e.stopPropagation()}
                >
                    <InstrumentSearchModal
                        filterType="futures"
                        multiSelect
                        selectedSectypes={selectedSectypes}
                        onToggleSelect={toggleAsset}
                        onDone={() => setPickerOpen(false)}
                        onSelect={() => { /* multiSelect: не используется */ }}
                        onClose={() => setPickerOpen(false)}
                    />
                </div>
            )}
        </div>
    );
}
