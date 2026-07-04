/**
 * CreateAlertModal — конструктор алертов + привязка Telegram.
 * Если Telegram не привязан → шаг «Подключить» (deep-link t.me/<bot>?start=,
 * poll статуса). Если привязан → конструктор:
 *   • для oi_move — мульти-выбор активов + уровень сигнала (ступени) → N алертов;
 *   • для price — один текущий актив + условие + порог в ₽ → 1 алерт.
 * Inline-styles + CSS-vars (как UpgradeModal — переживает portal/тему).
 */
import { useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { createPortal } from 'react-dom';
import { AlarmClock, X, Check, ExternalLink, Info, Search } from 'lucide-react';
import {
    getTelegramStatus, createTelegramLink, createAlert, createAlertsBatch, getAlertContext,
    getIntradayAssets, getNotifySettings,
    type AlertCreatePayload, type AlertContext, type NotifySettings,
} from '../../services/api';
import MessengerChoice from './MessengerChoice';
import InstrumentSearchModal from '../InstrumentSearchModal';

// Каналы доставки. E-mail пока за флагом (SMTP noreply настраивается — см. план):
// пилюля рендерится, но выбрать нельзя, пока EMAIL_CHANNEL_ENABLED=false.
type Channel = 'telegram' | 'site' | 'email';
const EMAIL_CHANNEL_ENABLED = false;

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
    /** Префилл при открытии из «+» на графике: метрика, порог и текущее значение
     *  серии (для «Сейчас: …»). */
    prefill?: { metricKey?: string; threshold?: number; currentLabel?: string };
}

const overlay: CSSProperties = {
    position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.5)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 'var(--sp-4)',
};
const card: CSSProperties = {
    background: 'var(--bg-primary)', border: '2px solid var(--text-primary)',
    boxShadow: '5px 5px 0 0 var(--text-primary)', borderRadius: 16, padding: 24,
    width: '100%', maxWidth: 480, maxHeight: '88vh', overflowY: 'auto',
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

// Таймфрейм раннего срабатывания дневного сигнала. Один и тот же дневной сигнал,
// но net_сейчас берётся из последнего бара выбранного интервала: 5м / 1ч ловят
// дневной сдвиг раньше (доступны у ликвидных активов), 1д — публикация раз в день.
type TfOption = { key: string; label: string };
const TIMEFRAMES: TfOption[] = [
    { key: '5m', label: '5 мин' },
    { key: '1h', label: '1 час' },
    { key: '1d', label: '1 день' },
];

// Сегмент-пилл переключателя (уровень сигнала / режим) — editorial-стиль.
// active = целиком оранжевый (accent fill + inverse text), иначе бордер.
const pill = (active: boolean): CSSProperties => ({
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '8px 12px', borderRadius: 10, cursor: 'pointer',
    border: '2px solid var(--text-primary)',
    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
    // Выбранное «приподнимается» фирменной жёсткой тенью (как карточка 5px 5px),
    // невыбранное — заподлицо. Это и есть «тень выбора».
    boxShadow: active ? '3px 3px 0 0 var(--text-primary)' : 'none',
    transition: 'box-shadow 0.12s, background 0.12s',
    textAlign: 'left', width: '100%',
});

export default function CreateAlertModal({ indicator, asset, assetName, metrics, onClose, prefill }: Props) {
    const [linked, setLinked] = useState<boolean | null>(null);  // null = загрузка
    const [linkUrl, setLinkUrl] = useState<string | null>(null);
    const [metricKey, setMetricKey] = useState(
        (prefill?.metricKey && metrics.some((m) => m.key === prefill.metricKey))
            ? prefill.metricKey
            : (metrics[0]?.key ?? ''),
    );
    const metric = metrics.find((m) => m.key === metricKey) ?? metrics[0];

    // ── Каналы доставки ─────────────────────────────────────────────────────
    // Дефолт из настроек юзера; до их загрузки — site (бесплатный, без привязки).
    const [channels, setChannels] = useState<Set<Channel>>(() => new Set<Channel>(['site']));
    const [settings, setSettings] = useState<NotifySettings | null>(null);
    const [settingsLoaded, setSettingsLoaded] = useState(false);
    const toggleChannel = (c: Channel) => setChannels((prev) => {
        const next = new Set(prev);
        if (next.has(c)) next.delete(c); else next.add(c);
        return next;
    });
    // Тир-UI (ступени множителя ATR / мульти-выбор активов / формула резкости)
    // включается для всех «во сколько раз больше обычного» метрик: движение
    // позиции (oi_move) И изменение числа участников (oi_participants). Признак —
    // unit «×». Цена (₽) остаётся на старой числовой-порог ветке.
    const isTierMetric =
        metric?.unit === '×' ||
        metric?.indicator === 'oi_move' ||
        metric?.indicator === 'oi_participants';

    const [op, setOp] = useState(metric?.ops[0]?.value ?? 'cross_up');
    // price → числовой порог в ₽; oi_move → не используется (порог = множитель ступени).
    const [threshold, setThreshold] = useState<string>(
        prefill?.threshold != null
            ? String(Math.round(prefill.threshold * 100) / 100)
            : (metric?.defaultThreshold != null ? String(metric.defaultThreshold) : ''),
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

    // ── Уровень сигнала (только oi_move) ────────────────────────────────────
    const [levelKey, setLevelKey] = useState('strong');       // default «Сильное»
    const [customMult, setCustomMult] = useState('');          // ввод для «Своё значение»
    const isCustomLevel = levelKey === 'custom';
    const activeLevel = SIGNAL_LEVELS.find((l) => l.key === levelKey);

    // ── Таймфрейм раннего срабатывания (тир-метрика) ────────────────────────
    // Дефолт '1d'. Внутридневные ('5m'/'1h') доступны только у ликвидных активов
    // (есть в intradaySet). Набор грузим один раз — как в InstrumentSearchModal.
    const [timeframe, setTimeframe] = useState('1d');
    const [intradaySet, setIntradaySet] = useState<Set<string>>(new Set());
    useEffect(() => {
        let cancelled = false;
        getIntradayAssets()
            .then((list) => { if (!cancelled) setIntradaySet(new Set(list)); })
            .catch(() => { /* пустой набор — внутридневные пиллы недоступны */ });
        return () => { cancelled = true; };
    }, []);

    useEffect(() => {
        getTelegramStatus().then((s) => setLinked(s.linked)).catch(() => setLinked(false));
    }, []);

    // Настройки доставки: дефолт-каналы юзера + доступность e-mail. Задаём стартовый
    // набор каналов один раз (после загрузки), если пользователь ещё не трогал.
    useEffect(() => {
        let cancelled = false;
        getNotifySettings()
            .then((s) => {
                if (cancelled) return;
                setSettings(s);
                const init = (s.default_channels || []).filter(
                    (c): c is Channel => c === 'telegram' || c === 'site'
                        || (c === 'email' && EMAIL_CHANNEL_ENABLED && s.email_available),
                );
                setChannels(new Set<Channel>(init.length ? init : ['site']));
                setSettingsLoaded(true);
            })
            .catch(() => { if (!cancelled) setSettingsLoaded(true); });
        return () => { cancelled = true; };
    }, []);

    // Контекст (свежая цена + intraday-доступность) — грузим когда форма доступна
    // (Telegram привязан). clgroup нужен для oi-метрик; берём из выбранной метрики,
    // иначе дефолт FIZ. Перезапрашиваем при смене актива/clgroup.
    const ctxClgroup = metric?.clgroup ?? 'FIZ';
    useEffect(() => {
        let cancelled = false;
        getAlertContext(indicator, asset, ctxClgroup)
            .then((c) => { if (!cancelled) setContext(c); })
            .catch(() => { if (!cancelled) setContext(null); });
        return () => { cancelled = true; };
    }, [linked, indicator, asset, ctxClgroup]);

    const price = context?.price;
    const hasPrice = price?.value != null;

    // смена метрики → сброс условия + дефолтного порога. НО на маунте НЕ трогаем:
    // иначе стёрли бы префилл-уровень из «+» на графике (баг «значение не доехало»).
    const metricInitDone = useRef(false);
    useEffect(() => {
        if (!metric) return;
        if (!metricInitDone.current) { metricInitDone.current = true; return; }
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

    // Есть ли среди выбранных хотя бы один ликвидный (внутридневной) актив —
    // от этого зависит, доступны ли пиллы «5 мин»/«1 час».
    const hasIntradaySelected = useMemo(
        () => selectedSectypes.some((s) => intradaySet.has(s)),
        [selectedSectypes, intradaySet],
    );

    // Если внутридневные стали недоступны (все выбранные — дневные-only), а был
    // выбран 5м/1ч — откатываем на 1д, чтобы не отправить недоступный таймфрейм.
    useEffect(() => {
        if (!hasIntradaySelected && timeframe !== '1d') setTimeframe('1d');
    }, [hasIntradaySelected, timeframe]);

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

    // Множитель ступени для oi_move (числовое значение порога).
    const resolvedMult = (): number | null => {
        if (isCustomLevel) {
            const v = parseFloat(customMult.replace(',', '.'));
            return Number.isNaN(v) ? null : v;
        }
        return activeLevel?.mult ?? null;
    };

    const channelsArr = useMemo(() => Array.from(channels), [channels]);
    // Telegram выбран, но не привязан → создать нельзя (доставить некуда по этому
    // каналу). Site/email покрывают — тогда Telegram не обязателен.
    const needsTelegramLink = channels.has('telegram') && linked === false;

    const handleCreate = async () => {
        if (!metric) return;
        if (channelsArr.length === 0) { setMsg({ type: 'err', text: 'Выберите хотя бы один канал доставки' }); return; }
        if (needsTelegramLink) { setMsg({ type: 'err', text: 'Подключите Telegram или уберите этот канал' }); return; }

        // ── price: один актив, числовой порог в ₽ (старое поведение) ─────────
        if (!isTierMetric) {
            const th = parseFloat(threshold.replace(',', '.'));
            if (Number.isNaN(th)) { setMsg({ type: 'err', text: 'Введите числовой порог' }); return; }
            setBusy(true); setMsg(null);
            try {
                const payload: AlertCreatePayload = {
                    indicator: metric.indicator, asset, asset_name: assetName,
                    metric: metric.metric, clgroup: metric.clgroup ?? null,
                    op, threshold: th, mode, channels: channelsArr,
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

        // ── тир-метрика: N активов × ступень → N алертов ─────────────────────
        // clgroup уходит как есть (ALL/FIZ/YUR) — payload строится из полей метрики.
        const mult = resolvedMult();
        if (mult == null || mult <= 0) { setMsg({ type: 'err', text: 'Укажите множитель уровня' }); return; }
        if (selectedList.length === 0) { setMsg({ type: 'err', text: 'Выберите хотя бы один актив' }); return; }
        const oiOp = metric.ops[0]?.value ?? 'gt';
        setBusy(true); setMsg(null);
        // ОДИН batch-запрос на всю группу — иначе N параллельных POST'ов бьются
        // о nginx rate-limit (burst=20) и часть падает с 503. Квота на бэке
        // проверяется один раз, ошибки по активам возвращаются списком.
        try {
            const payloads: AlertCreatePayload[] = selectedList.map((a) => ({
                indicator: metric.indicator, asset: a.sectype, asset_name: a.name,
                metric: metric.metric, clgroup: metric.clgroup ?? null,
                op: oiOp, threshold: mult, mode, channels: channelsArr,
                // Внутридневной таймфрейм — только ликвидным активам; дневным-only
                // всегда '1d', даже если в селекторе выбран 5м/1ч.
                timeframe: (intradaySet.has(a.sectype) && timeframe !== '1d') ? timeframe : '1d',
                ...(mode === 'repeat' ? { cooldown_hours: 24 } : {}),
            }));
            const res = await createAlertsBatch(payloads);
            if (res.created > 0) {
                setCreatedCount(res.created);
                setCreated(true);
                const notes: string[] = [];
                if (res.skipped > 0) notes.push(`${res.skipped} уже были`);
                if (res.errors.length) notes.push(`не удалось ${res.errors.length}: ${res.errors[0]}`);
                if (notes.length) setMsg({ type: 'err', text: notes.join('; ') });
            } else if (res.skipped > 0) {
                setMsg({ type: 'ok', text: 'Эти алерты уже созданы' });
            } else {
                setMsg({ type: 'err', text: res.errors[0] || 'Не удалось создать алерты' });
            }
        } catch (e) {
            setMsg({ type: 'err', text: (e as Error).message });
        } finally {
            setBusy(false);
        }
    };

    // Портал в document.body: иначе fixed-overlay (z 9999) проваливается под
    // sticky-шапку (z 50) из-за stacking-контекста родителя (transform/анимация
    // на странице) → верхний край модалки прятался под шапкой. Портал выносит
    // оверлей из контекста, и z честно перекрывает шапку.
    return createPortal(
        <div style={overlay} onClick={onClose}>
            <div style={card} onClick={(e) => e.stopPropagation()}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
                    <span style={{ display: 'flex', alignItems: 'center', gap: 8, fontWeight: 700, fontSize: 'var(--fs-lg)' }}>
                        <AlarmClock size={20} style={{ color: 'var(--accent)' }} /> Новый алерт
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
                            {createdCount > 1 ? 'Уведомления придут' : 'Уведомление придёт'}{' '}
                            {[
                                channels.has('telegram') && 'в Telegram',
                                channels.has('site') && 'на сайт',
                                channels.has('email') && 'на e-mail',
                            ].filter(Boolean).join(' и ') || 'на сайт'}.
                        </div>
                        <button onClick={onClose} className="editorial-press" style={{ ...primaryBtn, width: 'auto', padding: '10px 24px', margin: '0 auto', display: 'inline-block' }}>Готово</button>
                    </div>
                ) : linked === null || !settingsLoaded ? (
                    <div style={{ color: 'var(--text-secondary)', textAlign: 'center', padding: '20px 0' }}>Загрузка…</div>
                ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                        {/* ── ВЫБОР АКТИВОВ (тир-метрика) — всегда первым в форме ── */}
                        {isTierMetric ? (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Активы</div>
                                {/* Заметная full-width кнопка пикера (поиск + секторы + избранное).
                                    Тикеры выбранных НЕ перечисляем — только счётчик ниже. */}
                                <button
                                    onClick={() => setPickerOpen(true)}
                                    className="editorial-press"
                                    style={{
                                        display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8,
                                        width: '100%', padding: '12px 14px', borderRadius: 10,
                                        border: '2px solid var(--text-primary)', background: 'var(--bg-secondary)',
                                        color: 'var(--text-primary)', fontSize: 'var(--fs-sm)', fontWeight: 700,
                                        cursor: 'pointer',
                                    }}
                                >
                                    <Search size={18} /> Выбрать активы
                                </button>
                                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                                    Выбрано:{' '}
                                    <b style={{ color: 'var(--text-primary)' }}>
                                        {selectedList.length} {assetsWord(selectedList.length)}
                                    </b>
                                </div>
                            </div>
                        ) : (
                            <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                                Актив: <b style={{ color: 'var(--text-primary)' }}>{assetName || asset}</b>
                                {/* «Сейчас»: из «+» приходит currentLabel (цена ₽ / ОИ контракты);
                                    иначе для price-метрики — из контекста свежей цены. */}
                                {prefill?.currentLabel ? (
                                    <>
                                        {' · '}Сейчас: <b style={{ color: 'var(--text-primary)' }}>{prefill.currentLabel}</b>
                                    </>
                                ) : hasPrice && metric?.indicator === 'price' && (
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

                        {/* ── УРОВЕНЬ СИГНАЛА (тир-метрика) / УСЛОВИЕ+ПОРОГ (price) ─ */}
                        {isTierMetric ? (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Уровень сигнала</div>
                                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                                    {SIGNAL_LEVELS.map((l) => {
                                        const checked = levelKey === l.key;
                                        return (
                                            <button
                                                key={l.key}
                                                type="button"
                                                onClick={() => setLevelKey(l.key)}
                                                className="editorial-press"
                                                style={pill(checked)}
                                            >
                                                <span style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
                                                    <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700 }}>
                                                        {l.label}
                                                        <span style={{ fontWeight: 600, opacity: 0.85 }}> · {l.mult}×</span>
                                                    </span>
                                                    <span style={{ fontSize: 'var(--fs-xs)', opacity: checked ? 0.85 : 1, color: checked ? 'var(--text-inverse)' : 'var(--text-secondary)' }}>{l.freq}</span>
                                                </span>
                                            </button>
                                        );
                                    })}
                                    {/* Своё значение — пилл + числовой ввод × раскрывается при выборе */}
                                    <button
                                        type="button"
                                        onClick={() => setLevelKey('custom')}
                                        className="editorial-press"
                                        style={pill(isCustomLevel)}
                                    >
                                        <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700 }}>Своё значение</span>
                                        {isCustomLevel && (
                                            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, marginLeft: 'auto' }}>
                                                <input
                                                    type="text" inputMode="decimal" value={customMult}
                                                    onChange={(e) => { setCustomMult(e.target.value); setLevelKey('custom'); }}
                                                    onClick={(e) => { e.stopPropagation(); setLevelKey('custom'); }}
                                                    placeholder="4"
                                                    style={{ ...field, width: 64, padding: '6px 8px', textAlign: 'right' }}
                                                />
                                                <span style={{ fontSize: 'var(--fs-sm)' }}>×</span>
                                            </span>
                                        )}
                                    </button>
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

                        {/* ── ТАЙМФРЕЙМ (тир-метрика) — когда проверять сигнал ── */}
                        {/* Тот же дневной сигнал; 5м/1ч ловят дневной сдвиг раньше по
                            внутридневным данным (только ликвидные активы). Дефолт 1д. */}
                        {isTierMetric && (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Когда проверять</div>
                                <div style={{ display: 'flex', gap: 6 }}>
                                    {TIMEFRAMES.map((tf) => {
                                        const intraday = tf.key !== '1d';
                                        const disabled = intraday && !hasIntradaySelected;
                                        const checked = timeframe === tf.key;
                                        return (
                                            <button
                                                key={tf.key}
                                                type="button"
                                                disabled={disabled}
                                                onClick={() => { if (!disabled) setTimeframe(tf.key); }}
                                                className="editorial-press"
                                                style={{
                                                    ...pill(checked),
                                                    justifyContent: 'center',
                                                    fontSize: 'var(--fs-sm)', fontWeight: 700,
                                                    ...(disabled ? { opacity: 0.5, cursor: 'not-allowed' } : {}),
                                                }}
                                            >
                                                {tf.label}
                                            </button>
                                        );
                                    })}
                                </div>
                                <div style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)', lineHeight: 1.4 }}>
                                    5 мин и 1 час ловят дневной сдвиг раньше — доступны у ликвидных активов
                                    (с бейджем). У остальных всегда раз в день.
                                </div>
                            </div>
                        )}

                        {/* Режим срабатывания — переключатель (активный = оранжевый) */}
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Режим</div>
                            <div style={{ display: 'flex', gap: 6 }}>
                                {([
                                    { value: 'once', label: 'Один раз' },
                                    { value: 'repeat', label: 'Каждый раз' },
                                ] as const).map((m) => {
                                    const checked = mode === m.value;
                                    return (
                                        <button
                                            key={m.value}
                                            type="button"
                                            onClick={() => setMode(m.value)}
                                            className="editorial-press"
                                            style={{ ...pill(checked), justifyContent: 'center', fontSize: 'var(--fs-sm)', fontWeight: 700 }}
                                        >
                                            {m.label}
                                        </button>
                                    );
                                })}
                            </div>
                        </div>

                        {/* ── КАНАЛЫ ДОСТАВКИ ─────────────────────────────────── */}
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Куда присылать</div>
                            <div style={{ display: 'flex', gap: 6 }}>
                                {([
                                    { key: 'telegram' as Channel, label: 'Telegram', disabled: false, soon: false },
                                    { key: 'site' as Channel, label: 'На сайте', disabled: false, soon: false },
                                    { key: 'email' as Channel, label: 'E-mail', disabled: !(EMAIL_CHANNEL_ENABLED && settings?.email_available), soon: !EMAIL_CHANNEL_ENABLED },
                                ]).map((c) => {
                                    const active = channels.has(c.key);
                                    return (
                                        <button key={c.key} type="button" disabled={c.disabled}
                                            onClick={() => { if (!c.disabled) toggleChannel(c.key); }}
                                            className="editorial-press"
                                            style={{
                                                ...pill(active), justifyContent: 'center',
                                                fontSize: 'var(--fs-sm)', fontWeight: 700,
                                                opacity: c.disabled ? 0.5 : 1,
                                                cursor: c.disabled ? 'default' : 'pointer',
                                            }}
                                        >
                                            {c.label}{c.soon && <span style={{ fontWeight: 600, opacity: 0.8 }}> · скоро</span>}
                                        </button>
                                    );
                                })}
                            </div>
                            {/* Telegram выбран, но не привязан → инлайн-подключение */}
                            {channels.has('telegram') && linked === false && (
                                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                    {linkUrl ? (
                                        <>
                                            <a href={linkUrl} target="_blank" rel="noreferrer" className="editorial-press"
                                                style={{ ...primaryBtn, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8, textDecoration: 'none' }}>
                                                <ExternalLink size={16} /> Открыть @framesignalbot
                                            </a>
                                            <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)', textAlign: 'center' }}>Нажмите Start в боте — статус обновится сам</div>
                                        </>
                                    ) : (
                                        <MessengerChoice onTelegram={handleConnect} busy={busy} />
                                    )}
                                </div>
                            )}
                        </div>

                        {/* ── ТЕКСТ ФОРМУЛЫ (тир-метрика) ─────────────────────── */}
                        {isTierMetric && (
                            <div style={{
                                borderTop: '1px solid var(--border-color)', paddingTop: 12,
                                fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', lineHeight: 1.5,
                            }}>
                                <div style={{ fontWeight: 700, color: 'var(--text-primary)', marginBottom: 4 }}>
                                    Как считается резкость
                                </div>
                                Берётся отслеживаемая величина и её изменение за день. Это изменение делится
                                на средний дневной шаг за последние 14 торговых дней. Получается «во сколько
                                раз сегодняшнее движение больше обычного»: 1 — обычный день, 3 — втрое
                                сильнее обычного. Уровни откалиброваны по частоте на ликвидных бумагах:
                                «Заметное» случается примерно раз в неделю, «Сильное» — раз в две-три недели,
                                «Экстремальное» — раз в два месяца. Алерт описывает само движение, а не прогноз цены.
                            </div>
                        )}

                        {metric?.hint && !isTierMetric && (
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

                        {/* Таймфреймы алертов — честно про intraday vs дневной */}
                        {isTierMetric && (
                            <div style={{
                                display: 'flex', alignItems: 'flex-start', gap: 8,
                                fontSize: 'var(--fs-xs)', lineHeight: 1.4,
                                color: 'var(--text-secondary)',
                            }}>
                                <Info size={16} style={{ flex: '0 0 auto', marginTop: 1, color: 'var(--accent)' }} />
                                <span>
                                    Для ликвидных активов можно ловить дневной сдвиг раньше — по часовым
                                    или 5-минутным данным (таймфрейм выше). У остальных — раз в день.
                                    Это описание движения, а не прогноз цены.
                                </span>
                            </div>
                        )}

                        {/* Сводка для тир-метрики: N актив(ов) × <уровень> = N алертов */}
                        {isTierMetric && (
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
                            disabled={busy || channels.size === 0 || needsTelegramLink || (isTierMetric && selectedList.length === 0)}
                            onClick={handleCreate}
                            className="editorial-press"
                            style={{ ...primaryBtn, opacity: (channels.size === 0 || needsTelegramLink) ? 0.6 : 1 }}
                        >
                            {busy
                                ? 'Создаём…'
                                : isTierMetric
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
                        indicator="open_interest"
                        filterType="futures"
                        multiSelect
                        selectedSectypes={selectedSectypes}
                        onToggleSelect={toggleAsset}
                        onClearAll={() => setSelected({})}
                        onDone={() => setPickerOpen(false)}
                        onSelect={() => { /* multiSelect: не используется */ }}
                        onClose={() => setPickerOpen(false)}
                    />
                </div>
            )}
        </div>,
        document.body,
    );
}
