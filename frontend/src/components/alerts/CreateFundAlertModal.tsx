/**
 * CreateFundAlertModal — конструктор фонд-алертов «аномальный поток ×N».
 * Упрощённый брат CreateAlertModal: метрика — «аномальный поток» (ATR-кратность
 * дневного net_flow), но вместо одного актива — ПИКЕР ФОНДОВ (чипы категорий +
 * мультивыбор отдельных фондов по УК). По умолчанию выбраны ВСЕ фонды.
 *
 * Контракт fund-алерта (резолв выбора → payload):
 *   • выбраны ЦЕЛЫЕ категории (одна или несколько, вкл. «Все категории») →
 *     ОТДЕЛЬНЫЙ сигнал на КАЖДУЮ категорию (asset=<category>), чтобы спайк одной
 *     категории не тонул в сумме других. Одна категория → createAlert; несколько
 *     → createAlertsBatch (один сигнал на каждую). Это и есть «при выборе всех
 *     категорий сигнал приходит конкретно по категории» (денежный рынок / золото / …).
 *   • точечный набор фондов (не целые категории) → asset='custom',
 *     fund_ids=[выбранные fund_id] — одна осознанно собранная «корзина» = один сигнал.
 *   asset_name = человеко-метка («Денежный рынок» / «N фондов») — текст сигнала + кабинет.
 *   indicator='funds_flow', metric='net_flow', op='gt', threshold=×N,
 *   timeframe='1d'; source='funds' проставляет бэкенд. Требует привязки Telegram.
 *
 * Inline-styles + CSS-vars (как CreateAlertModal/UpgradeModal — переживает
 * portal/тему).
 */
import { useEffect, useMemo, useState, type CSSProperties } from 'react';
import { createPortal } from 'react-dom';
import { Bell, X, Check, ExternalLink } from 'lucide-react';
import {
    getTelegramStatus, createTelegramLink, createAlert, createAlertsBatch, getFundsCatalog,
    type AlertCreatePayload, type CatalogFund,
} from '../../services/api';
import FundPicker, { type FundPickerFund } from '../fundtrades/FundPicker';
import { CATEGORY_LABELS } from '../../config/fundConfig';
import MessengerChoice from './MessengerChoice';

interface Props {
    onClose: () => void;
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

// Сегмент-пилл (уровень сигнала / режим) — editorial-стиль. active = целиком
// оранжевый (accent fill + inverse text), иначе бордер + «тень выбора».
const pill = (active: boolean): CSSProperties => ({
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '8px 12px', borderRadius: 10, cursor: 'pointer',
    border: '2px solid var(--text-primary)',
    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
    boxShadow: active ? '3px 3px 0 0 var(--text-primary)' : 'none',
    transition: 'box-shadow 0.12s, background 0.12s',
    textAlign: 'left', width: '100%',
});

// Чип категории (мультивыбор) — pill-форма, как фильтр-пилюли в FundTrades.
const catChip = (active: boolean): CSSProperties => ({
    padding: '6px 14px',
    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
    border: '2px solid var(--text-primary)',
    borderRadius: 999,
    fontSize: 'var(--fs-xs)',
    fontWeight: active ? 700 : 600,
    cursor: 'pointer',
    boxShadow: active ? '3px 3px 0 var(--text-primary)' : 'none',
    whiteSpace: 'nowrap',
});

// Категории фонд-алерта. Порядок — как в разделах «Деньги в фондах». Юань
// включён (2026-06-19): раздел на сайте рабочий, поток считается так же.
const CATEGORY_KEYS = ['money_market', 'stocks', 'bonds', 'gold', 'yuan'] as const;
type CategoryKey = (typeof CATEGORY_KEYS)[number];

// Уровни сигнала — ступени множителя ATR (×), как у oi_move. «Сильное» по умолчанию.
type SignalLevel = { key: string; label: string; mult: number; freq: string };
const SIGNAL_LEVELS: SignalLevel[] = [
    { key: 'notable', label: 'Заметное', mult: 2, freq: 'случается чаще' },
    { key: 'strong', label: 'Сильное', mult: 3, freq: 'оптимальный баланс' },
    { key: 'extreme', label: 'Экстремальное', mult: 5, freq: 'только редкие всплески' },
];

// Русское склонение «N фондов».
function fundsWord(n: number): string {
    const m10 = n % 10, m100 = n % 100;
    if (m10 === 1 && m100 !== 11) return 'фонд';
    if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return 'фонда';
    return 'фондов';
}

export default function CreateFundAlertModal({ onClose }: Props) {
    const [linked, setLinked] = useState<boolean | null>(null);  // null = загрузка
    const [linkUrl, setLinkUrl] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    const [msg, setMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);
    const [created, setCreated] = useState(false);

    // ── Каталог фондов (все категории, включая Юань) ────────────────────────
    const [catalog, setCatalog] = useState<CatalogFund[]>([]);
    // selectedTickers — выбор фондов в конвенции FundPicker: ПУСТО = ВСЕ фонды
    // (так FundPicker рендерит «Все фонды» активной и его внутренняя кнопка
    // «Все фонды» = onChange(new Set())). Дефолт — пусто = все фонды.
    const [selectedTickers, setSelectedTickers] = useState<Set<string>>(new Set());
    const [catalogReady, setCatalogReady] = useState(false);

    // Уровень сигнала (множитель ATR) + ввод «Своё значение».
    const [levelKey, setLevelKey] = useState('strong');   // default «Сильное»
    const [customMult, setCustomMult] = useState('');
    const isCustomLevel = levelKey === 'custom';
    const activeLevel = SIGNAL_LEVELS.find((l) => l.key === levelKey);

    // Режим срабатывания: один раз / каждый раз.
    const [mode, setMode] = useState<'once' | 'repeat'>('once');

    useEffect(() => {
        getTelegramStatus().then((s) => setLinked(s.linked)).catch(() => setLinked(false));
    }, []);

    // Каталог фондов — публичный эндпоинт (как FundsMoney). Все категории, вкл. юань.
    // selectedTickers оставляем пустым (= все фонды по дефолту).
    useEffect(() => {
        let cancelled = false;
        getFundsCatalog()
            .then((r) => {
                if (cancelled) return;
                setCatalog(r.funds);
                setCatalogReady(true);
            })
            .catch(() => { if (!cancelled) setCatalogReady(true); });
        return () => { cancelled = true; };
    }, []);

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

    // ── Производные от каталога ──────────────────────────────────────────────
    // Фонды для FundPicker (группировка по УК — внутри пикера).
    const pickerFunds = useMemo<FundPickerFund[]>(
        () => catalog.map((f) => ({ ticker: f.ticker, name: f.name, uk_id: f.uk_id })),
        [catalog],
    );
    // ticker → fund_id (для резолва payload) и ticker → category (для чипов).
    const fundIdByTicker = useMemo(() => {
        const m = new Map<string, number>();
        for (const f of catalog) m.set(f.ticker, f.fund_id);
        return m;
    }, [catalog]);
    // Тикеры по категории (присутствующие в каталоге).
    const tickersByCategory = useMemo(() => {
        const m = new Map<CategoryKey, string[]>();
        for (const k of CATEGORY_KEYS) m.set(k, []);
        for (const f of catalog) {
            if ((CATEGORY_KEYS as readonly string[]).includes(f.category)) {
                m.get(f.category as CategoryKey)!.push(f.ticker);
            }
        }
        return m;
    }, [catalog]);
    // Категории, реально присутствующие в каталоге (для рендера чипов).
    const presentCategories = useMemo<CategoryKey[]>(
        () => CATEGORY_KEYS.filter((k) => (tickersByCategory.get(k)?.length ?? 0) > 0),
        [tickersByCategory],
    );

    const totalFunds = catalog.length;
    // Конвенция FundPicker: ПУСТО = ВСЕ. allSelected — пусто (или физически все).
    const allSelected = totalFunds > 0 && (selectedTickers.size === 0 || selectedTickers.size === totalFunds);
    // Эффективный явный набор: при пустом selectedTickers подставляем все тикеры
    // (нужно для подсветки чипов категорий и резолва payload).
    const effectiveTickers = useMemo<Set<string>>(
        () => (selectedTickers.size === 0 ? new Set(catalog.map((f) => f.ticker)) : selectedTickers),
        [selectedTickers, catalog],
    );
    const selectedCount = effectiveTickers.size;

    // Категория «целиком выбрана» = все её тикеры в эффективном наборе.
    const isCategoryFullySelected = (k: CategoryKey): boolean => {
        const ts = tickersByCategory.get(k) ?? [];
        return ts.length > 0 && ts.every((t) => effectiveTickers.has(t));
    };

    // Тоггл чипа категории: если категория уже выбрана целиком — снимаем её
    // фонды; иначе — добавляем все её фонды к выбору. Работаем от эффективного
    // набора (раскрываем «пусто=все» в явный), чтобы снятие категории из дефолта
    // «все» дало корректное подмножество.
    const toggleCategory = (k: CategoryKey) => {
        const ts = tickersByCategory.get(k) ?? [];
        const full = ts.every((t) => effectiveTickers.has(t));
        const next = new Set(effectiveTickers);
        if (full) ts.forEach((t) => next.delete(t));
        else ts.forEach((t) => next.add(t));
        // Нормализация: явный «все» → пусто (канон дефолта = все фонды).
        setSelectedTickers(next.size === totalFunds ? new Set() : next);
    };

    // «Все фонды» / «Все категории» — сброс к дефолту (пусто = все фонды).
    // Создание требует ≥1 фонда, поэтому отдельного «снять всё в ноль» нет:
    // тонкая настройка делается чипами категорий и тогглами в FundPicker.
    const selectAll = () => setSelectedTickers(new Set());

    // ── Метка выбора (asset_name) + резолв payload ───────────────────────────
    // «Все фонды» | «<категории через запятую>» | «N фондов».
    const selectionLabel = useMemo<string>(() => {
        if (allSelected) return 'Все фонды';
        if (selectedCount === 0) return 'Фонды не выбраны';
        // Если выбор совпадает с объединением целых категорий — перечисляем их.
        const fullCats = presentCategories.filter((k) => {
            const ts = tickersByCategory.get(k) ?? [];
            return ts.length > 0 && ts.every((t) => effectiveTickers.has(t));
        });
        const coveredByCats = fullCats.reduce(
            (acc, k) => acc + (tickersByCategory.get(k)?.length ?? 0), 0,
        );
        if (fullCats.length > 0 && coveredByCats === selectedCount) {
            return fullCats.map((k) => CATEGORY_LABELS[k] ?? k).join(', ');
        }
        return `${selectedCount} ${fundsWord(selectedCount)}`;
    }, [allSelected, selectedCount, presentCategories, tickersByCategory, effectiveTickers]);

    // Множитель ступени (числовое значение порога).
    const resolvedMult = (): number | null => {
        if (isCustomLevel) {
            const v = parseFloat(customMult.replace(',', '.'));
            return Number.isNaN(v) ? null : v;
        }
        return activeLevel?.mult ?? null;
    };

    // Целиком выбранные категории, ТОЧНО покрывающие выбор (ничего вне них).
    // Если так — это «подписка на категории», и каждую шлём отдельным сигналом.
    const wholeCategorySelection = (): CategoryKey[] | null => {
        const fullCats = presentCategories.filter((k) => isCategoryFullySelected(k));
        const coveredByCats = fullCats.reduce(
            (acc, k) => acc + (tickersByCategory.get(k)?.length ?? 0), 0,
        );
        return fullCats.length >= 1 && coveredByCats === selectedCount ? fullCats : null;
    };

    const handleCreate = async () => {
        const mult = resolvedMult();
        if (mult == null || mult <= 0) { setMsg({ type: 'err', text: 'Укажите множитель уровня' }); return; }
        if (selectedCount === 0) { setMsg({ type: 'err', text: 'Выберите хотя бы один фонд' }); return; }
        setBusy(true); setMsg(null);
        // Общие поля контракта: indicator='funds_flow', metric='net_flow', op='gt',
        // threshold=×N, timeframe всегда '1d' (потоки дневные); source='funds' ставит бэк.
        const common = {
            indicator: 'funds_flow', metric: 'net_flow', op: 'gt',
            threshold: mult, mode, timeframe: '1d',
            ...(mode === 'repeat' ? { cooldown_hours: 24 } : {}),
        } as const;
        try {
            const cats = wholeCategorySelection();
            if (cats) {
                // Подписка на целые категории → ОТДЕЛЬНЫЙ сигнал на каждую, чтобы
                // спайк одной (золото 5.6×) не размывался суммой остальных. Одна
                // категория → createAlert; несколько → один batch-запрос.
                const payloads: AlertCreatePayload[] = cats.map((k) => ({
                    ...common,
                    asset: k,
                    asset_name: CATEGORY_LABELS[k] ?? k,
                }));
                if (payloads.length === 1) {
                    await createAlert(payloads[0]);
                    setCreated(true);
                } else {
                    const res = await createAlertsBatch(payloads);
                    if (res.created > 0) setCreated(true);
                    else setMsg({ type: 'err', text: res.errors[0] || 'Не удалось создать сигналы' });
                }
            } else {
                // Точечный набор фондов (не целые категории) → один «custom»-сигнал
                // по осознанно собранной корзине.
                const ids = Array.from(effectiveTickers)
                    .map((t) => fundIdByTicker.get(t))
                    .filter((v): v is number => v != null);
                await createAlert({ ...common, asset: 'custom', asset_name: selectionLabel, fund_ids: ids });
                setCreated(true);
            }
        } catch (e) {
            setMsg({ type: 'err', text: (e as Error).message });
        } finally { setBusy(false); }
    };

    // Портал в document.body (как CreateAlertModal): иначе fixed-overlay (z 9999)
    // проваливается под sticky-шапку из-за stacking-контекста родителя.
    return createPortal(
        <div style={overlay} onClick={onClose}>
            <div style={card} onClick={(e) => e.stopPropagation()}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
                    <span style={{ display: 'flex', alignItems: 'center', gap: 8, fontWeight: 700, fontSize: 'var(--fs-lg)' }}>
                        <Bell size={20} style={{ color: 'var(--accent)' }} /> Сигнал по фондам
                    </span>
                    <button onClick={onClose} aria-label="Закрыть" className="editorial-press" style={{ color: 'var(--text-secondary)', width: 36, height: 36, borderRadius: 8, display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}><X size={20} /></button>
                </div>

                {created ? (
                    <div style={{ textAlign: 'center', padding: '12px 0' }}>
                        <Check size={40} style={{ color: 'var(--accent)', margin: '0 auto 8px', display: 'block' }} />
                        <div style={{ fontWeight: 600, marginBottom: 4 }}>Сигнал создан</div>
                        <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)', marginBottom: 16 }}>
                            Следим за потоком: <b style={{ color: 'var(--text-primary)' }}>{selectionLabel}</b>. Когда дневной приток или отток станет аномальным, придёт сигнал в Telegram (@framesignalbot) — по конкретной категории.
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
                                    Чтобы получать сигналы, подключите мессенджер. После этого все ваши алерты будут приходить туда.
                                </p>
                                <MessengerChoice onTelegram={handleConnect} busy={busy} />
                            </>
                        )}
                    </div>
                ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                        {/* ── ВЫБОР ФОНДОВ — чипы категорий + пикер по УК ── */}
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Фонды</div>
                            {!catalogReady ? (
                                <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>Загружаем список фондов…</div>
                            ) : totalFunds === 0 ? (
                                <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>Фонды не найдены.</div>
                            ) : (
                                <>
                                    {/* Чипы категорий (мультивыбор) + «Все категории» */}
                                    <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                                        <button
                                            type="button"
                                            onClick={selectAll}
                                            className="editorial-press"
                                            style={catChip(allSelected)}
                                        >
                                            Все категории
                                        </button>
                                        {presentCategories.map((k) => (
                                            <button
                                                key={k}
                                                type="button"
                                                onClick={() => toggleCategory(k)}
                                                className="editorial-press"
                                                style={catChip(!allSelected && isCategoryFullySelected(k))}
                                            >
                                                {CATEGORY_LABELS[k] ?? k}
                                            </button>
                                        ))}
                                    </div>

                                    {/* Пикер отдельных фондов (по УК; тоггл отдельных + «Все
                                        фонды»/выбор-всего-УК внутри). Рядом — быстрый сброс
                                        к «все фонды», когда выбрано подмножество. */}
                                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                                        <FundPicker
                                            funds={pickerFunds}
                                            mode="multi"
                                            selected={selectedTickers}
                                            onChange={setSelectedTickers}
                                            buttonLabel={() => (allSelected ? 'Все фонды' : `${selectedCount} ${fundsWord(selectedCount)}`)}
                                        />
                                        {!allSelected && (
                                            <button
                                                type="button"
                                                onClick={selectAll}
                                                className="editorial-press"
                                                style={{
                                                    background: 'transparent', border: 'none', cursor: 'pointer',
                                                    color: 'var(--accent)', fontSize: 'var(--fs-xs)', fontWeight: 700,
                                                    padding: '4px 2px',
                                                }}
                                            >
                                                Выбрать все
                                            </button>
                                        )}
                                    </div>

                                    <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                                        Выбрано: <b style={{ color: 'var(--text-primary)' }}>{selectionLabel}</b>
                                    </div>
                                </>
                            )}
                        </div>

                        {/* ── УРОВЕНЬ СИГНАЛА — ступени множителя ATR ── */}
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

                        {/* ── РЕЖИМ — один раз / каждый раз ── */}
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

                        {/* ── ТЕКСТ ФОРМУЛЫ ── */}
                        <div style={{
                            borderTop: '1px solid var(--border-color)', paddingTop: 12,
                            fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', lineHeight: 1.5,
                        }}>
                            <div style={{ fontWeight: 700, color: 'var(--text-primary)', marginBottom: 4 }}>
                                Как считается аномальный поток
                            </div>
                            Берётся чистый дневной приток-отток денег в выбранные фонды и делится на
                            средний дневной шаг за последние 14 дней. Получается «во сколько раз
                            сегодняшний поток больше обычного»: 1 — обычный день, 3 — втрое сильнее
                            обычного. Сигнал придёт, когда |поток| превысит выбранную кратность —
                            и в тексте укажет направление (приток или отток).
                        </div>

                        {/* Сводка */}
                        <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                            Сигнал: <b style={{ color: 'var(--text-primary)' }}>{selectionLabel}</b>
                            {' · поток ≥ '}
                            <b style={{ color: 'var(--text-primary)' }}>
                                {isCustomLevel ? `${customMult || '—'}×` : `${activeLevel?.mult}×`}
                            </b>
                        </div>

                        <button
                            disabled={busy || selectedCount === 0}
                            onClick={handleCreate}
                            className="editorial-press"
                            style={primaryBtn}
                        >
                            {busy ? 'Создаём…' : 'Создать сигнал'}
                        </button>
                    </div>
                )}

                {msg && (
                    <div style={{ marginTop: 12, fontSize: 'var(--fs-sm)', color: msg.type === 'ok' ? 'var(--accent)' : 'var(--funds-flow-negative, #FF7A5C)' }}>
                        {msg.text}
                    </div>
                )}
            </div>
        </div>,
        document.body,
    );
}
