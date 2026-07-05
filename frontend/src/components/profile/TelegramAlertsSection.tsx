/**
 * TelegramAlertsSection — центр управления Telegram-алертами в ЛК.
 * Закрывает «синхронизацию аккаунта с Telegram» + edge-кейсы:
 *  - статус (подключён @X / не подключён) + Подключить / Отвязать / Переподключить;
 *  - предупреждение «N алертов не придут, пока не подключён»;
 *  - список алертов (актив · условие · статус) + пауза/возобновить/удалить;
 *  - tier-aware: Free (quota 0) → upgrade на Basic.
 * Рендерится внутри карточки ProfilePage (как ExtensionTokenSection).
 */
import { useCallback, useEffect, useMemo, useState, type CSSProperties } from 'react';
import {
    AlarmClock, ExternalLink, Send, Trash2, Pause, Play, AlertTriangle,
    ChevronDown, ChevronRight, Search, Zap, Clock, Wallet,
} from 'lucide-react';
import {
    getTelegramStatus, createTelegramLink, unlinkTelegram,
    listAlerts, deleteAlert, deleteAllAlerts, setAlertStatus,
    type AlertInfo,
} from '../../services/api';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import MessengerChoice from '../alerts/MessengerChoice';
import AlertFiresList from './AlertFiresList';

/** Inline-глиф колокольчика — повторяет кнопку-колокол с индикаторов (Bell в обведённом
 *  кружке), чтобы текст «кнопкой …» указывал на реальный контрол, а не на эмодзи. */
const BellGlyph = () => (
    <span style={{
        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        width: 18, height: 18, borderRadius: '50%', margin: '0 2px',
        border: '1.5px solid var(--text-primary)', background: 'var(--bg-secondary)',
        color: 'var(--text-primary)', verticalAlign: '-4px',
    }} aria-label="колокол алертов">
        <AlarmClock size={11} strokeWidth={2.2} />
    </span>
);

// Editorial-чип для текстовых действий (Переподключить/Отвязать/назад) — даёт
// press-анимацию (translate+hard-shadow) и ≥36px тач-таргет, как везде в проекте.
const chipBtn: CSSProperties = {
    padding: '7px 12px', borderRadius: 8, border: '1.5px solid var(--text-primary)',
    background: 'var(--bg-primary)', fontSize: 'var(--fs-sm)', lineHeight: 1.2,
};
// Квадратная icon-кнопка 36×36 для pause/resume/delete в списке алертов.
const iconBtn: CSSProperties = {
    width: 36, height: 36, borderRadius: 8, border: '1.5px solid var(--text-primary)',
    background: 'var(--bg-primary)', display: 'inline-flex',
    alignItems: 'center', justifyContent: 'center',
};

const OP_LABEL: Record<string, string> = {
    gt: 'выше', lt: 'ниже', cross_up: '↑ пересечёт', cross_down: '↓ пересечёт',
};
const METRIC_LABEL: Record<string, string> = {
    price: 'Цена', oi_zscore: 'OI z-score', oi_level: 'Открытый интерес',
    oi_move: 'Движение позиции', oi_participants: 'Число участников',
    oi_extreme: 'Экстремум позиции', funds_flow: 'Аномальный поток',
};

// Человеческое условие алерта. oi_extreme особый: op = направление рекорда,
// threshold = период (дни 30/90/0=всё).
function condLabel(a: AlertInfo): string {
    if (a.indicator === 'oi_extreme') {
        const clg = (a.clgroup || 'FIZ') === 'FIZ' ? 'физлица' : 'юрлица';
        const kind = a.op === 'new_low' ? 'новый минимум' : 'новый максимум';
        const per = { 180: 'за 6 месяцев', 365: 'за год' }[Number(a.threshold)] || 'за всё время';
        return `чистая позиция (${clg}) — ${kind} перекоса ${per}`;
    }
    return `${METRIC_LABEL[a.indicator] || a.indicator} ${OP_LABEL[a.op] || a.op} ${a.threshold}${unitFor(a)}`;
}
const STATUS_LABEL: Record<string, string> = {
    active: 'Активен', paused: 'Пауза', fired: 'Сработал',
};

// Имена категорий фондов — фолбэк для старых фонд-алертов, у которых asset=ключ
// категории, а asset_name мог быть пуст. Новые алерты несут метку выбора в
// asset_name («Все фонды» / «Акции, Облигации» / «N фондов»).
const FUNDS_CATEGORY_NAME: Record<string, string> = {
    money_market: 'Денежный рынок', stocks: 'Акции',
    bonds: 'Облигации', gold: 'Золото', yuan: 'Юань',
    all: 'Все фонды',
};

function unitFor(a: AlertInfo): string {
    if (a.indicator === 'price') return ' ₽';
    if (a.indicator === 'oi_zscore') return 'σ';
    // oi_move/oi_participants/funds_flow — кратность ×N (ATR-множитель).
    if (a.indicator === 'oi_move' || a.indicator === 'oi_participants' || a.indicator === 'funds_flow') return '×';
    return '';
}

// Отображаемое имя актива. Для фонд-алерта показываем метку выбора (asset_name:
// «Все фонды» / «Акции, Облигации» / «N фондов»); фолбэк — карта категорий по
// asset (старые алерты), затем сам asset.
function assetLabel(a: AlertInfo): string {
    if (a.indicator === 'funds_flow') return a.asset_name || FUNDS_CATEGORY_NAME[a.asset] || a.asset;
    return a.asset_name || a.asset;
}

// Сколько алертов грузим за один запрос (X-Total-Count в шапке даёт общее число).
// 200 — дефолт бэкенда; хватает на типичный кабинет, «показать ещё» догружает остаток.
const PAGE_LIMIT = 200;
// Заголовок секции без сектора — у фьючерсов sector обычно null.
const NO_SECTOR = 'Без сектора';
type SortKey = 'date' | 'fires';

// «N сигналов» с правильным склонением.
function firesLabel(n: number): string {
    const mod10 = n % 10, mod100 = n % 100;
    if (mod10 === 1 && mod100 !== 11) return `${n} сигнал`;
    if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return `${n} сигнала`;
    return `${n} сигналов`;
}

export default function TelegramAlertsSection() {
    const quota = useCommonFeatures().telegram_alerts_quota; // 0 / число / null(∞)
    const { showUpgrade } = useUpgradePrompt();
    const [linked, setLinked] = useState<boolean | null>(null);
    const [username, setUsername] = useState<string | null>(null);
    const [alerts, setAlerts] = useState<AlertInfo[]>([]);
    const [total, setTotal] = useState(0);            // X-Total-Count — сколько алертов всего
    const [loadingMore, setLoadingMore] = useState(false);
    const [linkUrl, setLinkUrl] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    const [msg, setMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);

    // ── UI-стейт нового списка ──
    const [query, setQuery] = useState('');            // локальный поиск по тикеру/имени
    const [sortKey, setSortKey] = useState<SortKey>('date');
    const [openSectors, setOpenSectors] = useState<Set<string>>(new Set());  // раскрытые секторы
    const [openFires, setOpenFires] = useState<number | null>(null);          // alert.id с раскрытой историей

    // Грузим первую страницу. items/total из контракта Agent A (listAlerts→{items,total}).
    const loadAlerts = useCallback(async (offset: number) => {
        const page = await listAlerts({ limit: PAGE_LIMIT, offset });
        setTotal(page.total);
        setAlerts((prev) => (offset === 0 ? page.items : [...prev, ...page.items]));
    }, []);

    const refresh = useCallback(async () => {
        try {
            const s = await getTelegramStatus();
            setLinked(s.linked); setUsername(s.username);
        } catch { setLinked(false); }
        try { await loadAlerts(0); } catch { /* free / no access */ }
    }, [loadAlerts]);

    useEffect(() => { refresh(); }, [refresh]);

    const loadMore = async () => {
        setLoadingMore(true);
        try { await loadAlerts(alerts.length); }
        catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
        finally { setLoadingMore(false); }
    };

    // poll статуса после генерации ссылки (юзер жмёт Start в боте)
    useEffect(() => {
        if (!linkUrl || linked) return;
        const t = setInterval(async () => {
            try {
                const s = await getTelegramStatus();
                if (s.linked) { setLinked(true); setUsername(s.username); setLinkUrl(null); clearInterval(t); }
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
        } catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
        finally { setBusy(false); }
    };
    const handleUnlink = async () => {
        if (!window.confirm('Отвязать Telegram? Алерты перестанут приходить.')) return;
        setBusy(true); setMsg(null);
        try { await unlinkTelegram(); setLinked(false); setUsername(null); setLinkUrl(null); }
        catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
        finally { setBusy(false); }
    };
    const toggle = async (a: AlertInfo) => {
        try { await setAlertStatus(a.id, a.status === 'active' ? 'paused' : 'active'); refresh(); }
        catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
    };
    const remove = async (a: AlertInfo) => {
        if (!window.confirm('Удалить алерт?')) return;
        try { await deleteAlert(a.id); refresh(); }
        catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
    };
    // Массовое удаление — на случай, если случайно создал группу из десятков алертов.
    const removeAll = async () => {
        if (!window.confirm(`Удалить ВСЕ ${alerts.length} алертов? Это необратимо.`)) return;
        try { const r = await deleteAllAlerts(); refresh(); setMsg({ type: 'ok', text: `Удалено ${r.deleted}` }); }
        catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
    };

    const activeCount = alerts.filter((a) => a.status === 'active').length;
    const link = 'var(--accent)';
    const sub = 'var(--text-secondary)';

    const toggleSector = (name: string) => setOpenSectors((prev) => {
        const next = new Set(prev);
        if (next.has(name)) next.delete(name); else next.add(name);
        return next;
    });

    // ── Алерты «Открытые позиции» (source='oi'), отфильтрованные поиском ──
    // Поиск локальный (по тикеру/имени) — TODO: когда Agent A отдаст useInstrumentFilter,
    // переключить на общий хук (по секторам/группам), чтобы совпадать с конструктором.
    const oiAlerts = useMemo(() => {
        const q = query.trim().toLowerCase();
        return alerts.filter((a) => {
            const src = a.source || 'oi';
            if (src !== 'oi') return false;
            if (!q) return true;
            return (a.asset || '').toLowerCase().includes(q)
                || (a.asset_name || '').toLowerCase().includes(q);
        });
    }, [alerts, query]);

    // ── Алерты «Фонды» (source='funds'), отфильтрованные тем же поиском ──
    // У фондов нет сектора → плоский список (без аккордеонов), сортировка как
    // у OI (дата / число сигналов).
    const fundsAlerts = useMemo(() => {
        const q = query.trim().toLowerCase();
        const byCmp = (a: AlertInfo, b: AlertInfo) => {
            if (sortKey === 'fires') {
                const d = (b.fires_count ?? 0) - (a.fires_count ?? 0);
                if (d !== 0) return d;
            }
            return (b.created_at || '').localeCompare(a.created_at || '');
        };
        return alerts
            .filter((a) => {
                if ((a.source || 'oi') !== 'funds') return false;
                if (!q) return true;
                return assetLabel(a).toLowerCase().includes(q)
                    || (a.asset || '').toLowerCase().includes(q);
            })
            .sort(byCmp);
    }, [alerts, query, sortKey]);

    // Группировка по сектору + сортировка внутри секции.
    const sectorGroups = useMemo(() => {
        const byCmp = (a: AlertInfo, b: AlertInfo) => {
            if (sortKey === 'fires') {
                const d = (b.fires_count ?? 0) - (a.fires_count ?? 0);
                if (d !== 0) return d;
            }
            // дефолт / тай-брейк — по дате создания (новые сверху)
            return (b.created_at || '').localeCompare(a.created_at || '');
        };
        const map = new Map<string, AlertInfo[]>();
        for (const a of oiAlerts) {
            const key = a.sector || NO_SECTOR;
            const arr = map.get(key);
            if (arr) arr.push(a); else map.set(key, [a]);
        }
        const groups = Array.from(map.entries()).map(([sector, list]) => ({
            sector,
            list: [...list].sort(byCmp),
            fires: list.reduce((s, a) => s + (a.fires_count ?? 0), 0),
        }));
        // секции сортируем: по сумме сигналов (если sort=fires) либо по размеру
        groups.sort((x, y) => sortKey === 'fires'
            ? y.fires - x.fires
            : y.list.length - x.list.length);
        return groups;
    }, [oiAlerts, sortKey]);

    return (
        <div>
            <h2 className="text-lg font-bold mb-1" style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)' }}>
                <AlarmClock size={18} style={{ color: link }} /> Алерты в мессенджере
            </h2>
            <p style={{ color: sub, fontSize: 'var(--fs-sm)', marginBottom: 16 }}>
                Уведомления при достижении уровней (цена, аномалии OI). Создаются с индикаторов кнопкой<BellGlyph />.
                Сейчас доступен Telegram, мессенджер&nbsp;МАКС — в&nbsp;разработке.
            </p>

            {quota === 0 ? (
                <button onClick={() => showUpgrade({ tier: 'basic', featureName: 'Алерты в мессенджере', indicator: 'alerts' })}
                    className="editorial-press" style={{ padding: '10px 16px', borderRadius: 10, border: '2px solid var(--text-primary)', background: 'var(--accent)', color: 'var(--text-inverse)', fontWeight: 600 }}>
                    Доступно на Basic и Pro — улучшить тариф
                </button>
            ) : (
                <>
                    {/* ── Статус Telegram ── */}
                    <div className="rounded-xl p-4 mb-4" style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}>
                        {linked === null ? (
                            <span style={{ color: sub }}>Загрузка…</span>
                        ) : linked ? (
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
                                <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                    <Send size={16} style={{ color: link }} />
                                    <span>Подключён: <b>Telegram</b>{username ? ` · @${username}` : ''}</span>
                                </span>
                                <span style={{ display: 'flex', gap: 'var(--sp-2)' }}>
                                    <button onClick={handleConnect} disabled={busy} title="Сменить чат или мессенджер" className="editorial-press" style={{ ...chipBtn, color: link }}>Переподключить</button>
                                    <button onClick={handleUnlink} disabled={busy} className="editorial-press" style={{ ...chipBtn, color: sub }}>Отвязать</button>
                                </span>
                            </div>
                        ) : linkUrl ? (
                            <div>
                                <a href={linkUrl} target="_blank" rel="noreferrer" className="editorial-press"
                                    style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '8px 14px', borderRadius: 10, border: '2px solid var(--text-primary)', background: 'var(--accent)', color: 'var(--text-inverse)', textDecoration: 'none', fontWeight: 600 }}>
                                    <ExternalLink size={15} /> Открыть @framesignalbot
                                </a>
                                <div style={{ color: sub, fontSize: 'var(--fs-xs)', marginTop: 6 }}>Нажмите Start в боте — статус обновится сам.</div>
                                <button onClick={() => setLinkUrl(null)} className="editorial-press" style={{ ...chipBtn, color: sub, fontSize: 'var(--fs-xs)', marginTop: 'var(--sp-2)' }}>← выбрать другой мессенджер</button>
                            </div>
                        ) : (
                            <div>
                                <div style={{ color: sub, marginBottom: 12 }}>Мессенджер не подключён.</div>
                                <MessengerChoice onTelegram={handleConnect} busy={busy} title={null} />
                            </div>
                        )}
                        {linked === false && activeCount > 0 && (
                            <div style={{ display: 'flex', alignItems: 'center', gap: 6, color: 'var(--funds-flow-negative, #FF7A5C)', fontSize: 'var(--fs-xs)', marginTop: 8 }}>
                                <AlertTriangle size={14} /> {activeCount} активных алертов не придут, пока не подключите мессенджер.
                            </div>
                        )}
                    </div>

                    {/* ── Список алертов ── */}
                    {alerts.length === 0 ? (
                        <div style={{ color: sub, fontSize: 'var(--fs-sm)' }}>
                            Пока нет алертов. Создайте кнопкой<BellGlyph /> на индикаторе («Открытые позиции» или «Деньги в фондах»).
                        </div>
                    ) : (
                        <>
                        {/* ── Секция «Открытые позиции» (source='oi') ── */}
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 'var(--sp-2)' }}>
                            <Zap size={16} style={{ color: link }} />
                            <h3 style={{ fontWeight: 700, fontSize: 'var(--fs-base)' }}>Открытые позиции</h3>
                            <span style={{ color: sub, fontSize: 'var(--fs-xs)' }}>{oiAlerts.length}</span>
                        </div>

                        {/* Контролы: поиск + сортировка */}
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 'var(--sp-2)', alignItems: 'center', marginBottom: 12 }}>
                            <div style={{ position: 'relative', flex: '1 1 180px', minWidth: 0 }}>
                                <Search size={14} style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', color: sub, pointerEvents: 'none' }} />
                                <input
                                    value={query}
                                    onChange={(e) => setQuery(e.target.value)}
                                    placeholder="Поиск по тикеру…"
                                    style={{
                                        width: '100%', minHeight: 36, padding: '7px 10px 7px 30px', borderRadius: 8,
                                        border: '1.5px solid var(--border-color)', background: 'var(--bg-primary)',
                                        fontSize: 'var(--fs-sm)', color: 'var(--text-primary)',
                                    }}
                                />
                            </div>
                            <div style={{ display: 'flex', gap: 6, flexShrink: 0 }}>
                                <button
                                    onClick={() => setSortKey('date')}
                                    className="editorial-press"
                                    aria-pressed={sortKey === 'date'}
                                    style={{ ...chipBtn, fontSize: 'var(--fs-xs)', padding: '6px 10px', minHeight: 36, display: 'inline-flex', alignItems: 'center', gap: 4, background: sortKey === 'date' ? 'var(--bg-secondary)' : 'var(--bg-primary)', color: sortKey === 'date' ? 'var(--text-primary)' : sub }}
                                >
                                    <Clock size={13} /> По дате
                                </button>
                                <button
                                    onClick={() => setSortKey('fires')}
                                    className="editorial-press"
                                    aria-pressed={sortKey === 'fires'}
                                    style={{ ...chipBtn, fontSize: 'var(--fs-xs)', padding: '6px 10px', minHeight: 36, display: 'inline-flex', alignItems: 'center', gap: 4, background: sortKey === 'fires' ? 'var(--bg-secondary)' : 'var(--bg-primary)', color: sortKey === 'fires' ? 'var(--text-primary)' : sub }}
                                >
                                    <Zap size={13} /> По сигналам
                                </button>
                            </div>
                        </div>

                        {/* Аккордеоны по секторам (схлопнуты по умолчанию — не вываливаем весь список) */}
                        {oiAlerts.length === 0 ? (
                            <div style={{ color: sub, fontSize: 'var(--fs-sm)', marginBottom: 16 }}>
                                {query ? 'Ничего не найдено по запросу.' : 'Нет алертов по открытым позициям.'}
                            </div>
                        ) : (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginBottom: 16 }}>
                                {sectorGroups.map(({ sector, list, fires }) => {
                                    const open = openSectors.has(sector);
                                    return (
                                        <div key={sector} className="rounded-xl" style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-color)', overflow: 'hidden' }}>
                                            <button
                                                onClick={() => toggleSector(sector)}
                                                className="editorial-press"
                                                aria-expanded={open}
                                                style={{
                                                    width: '100%', minHeight: 44, padding: '10px 12px', background: 'transparent',
                                                    border: 'none', display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer',
                                                    textAlign: 'left', color: 'var(--text-primary)',
                                                }}
                                            >
                                                {open ? <ChevronDown size={16} style={{ flexShrink: 0 }} /> : <ChevronRight size={16} style={{ flexShrink: 0 }} />}
                                                <span style={{ fontWeight: 600, fontSize: 'var(--fs-sm)', flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{sector}</span>
                                                <span style={{ color: sub, fontSize: 'var(--fs-xs)', flexShrink: 0 }}>{list.length}</span>
                                                {fires > 0 && (
                                                    <span style={{ color: link, fontSize: 'var(--fs-xs)', flexShrink: 0, display: 'inline-flex', alignItems: 'center', gap: 3 }}>
                                                        <Zap size={11} /> {fires}
                                                    </span>
                                                )}
                                            </button>
                                            {open && (
                                                <ul style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: '0 12px 12px' }}>
                                                    {list.map((a) => {
                                                        const firesN = a.fires_count ?? 0;
                                                        const firesOpen = openFires === a.id;
                                                        return (
                                                            <li key={a.id} className="rounded-xl p-3" style={{ background: 'var(--bg-primary)', border: '1px solid var(--border-color)' }}>
                                                                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 'var(--sp-2)' }}>
                                                                    <div style={{ minWidth: 0 }}>
                                                                        <div style={{ fontWeight: 600, fontSize: 'var(--fs-sm)' }}>{a.asset_name || a.asset}</div>
                                                                        <div style={{ color: sub, fontSize: 'var(--fs-xs)' }}>
                                                                            {condLabel(a)}
                                                                            {' · '}<span style={{ color: a.status === 'active' ? link : sub }}>{STATUS_LABEL[a.status] || a.status}</span>
                                                                        </div>
                                                                        <button
                                                                            onClick={() => setOpenFires(firesOpen ? null : a.id)}
                                                                            disabled={firesN === 0}
                                                                            className="editorial-press"
                                                                            aria-expanded={firesOpen}
                                                                            style={{
                                                                                marginTop: 6, padding: 0, background: 'transparent', border: 'none',
                                                                                color: firesN === 0 ? sub : link, fontSize: 'var(--fs-xs)',
                                                                                cursor: firesN === 0 ? 'default' : 'pointer', opacity: firesN === 0 ? 0.6 : 1,
                                                                                display: 'inline-flex', alignItems: 'center', gap: 4,
                                                                            }}
                                                                        >
                                                                            <Zap size={12} /> {firesLabel(firesN)}
                                                                            {firesN > 0 && (firesOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />)}
                                                                        </button>
                                                                    </div>
                                                                    <div style={{ display: 'flex', gap: 'var(--sp-2)', flexShrink: 0 }}>
                                                                        {a.status !== 'fired' && (
                                                                            <button onClick={() => toggle(a)} title={a.status === 'active' ? 'Пауза' : 'Возобновить'} aria-label={a.status === 'active' ? 'Пауза' : 'Возобновить'} className="editorial-press" style={{ ...iconBtn, color: sub }}>
                                                                                {a.status === 'active' ? <Pause size={16} /> : <Play size={16} />}
                                                                            </button>
                                                                        )}
                                                                        <button onClick={() => remove(a)} title="Удалить" aria-label="Удалить алерт" className="editorial-press" style={{ ...iconBtn, color: 'var(--funds-flow-negative, #FF7A5C)' }}><Trash2 size={16} /></button>
                                                                    </div>
                                                                </div>
                                                                {firesOpen && firesN > 0 && (
                                                                    <AlertFiresList alertId={a.id} unit={unitFor(a)} />
                                                                )}
                                                            </li>
                                                        );
                                                    })}
                                                </ul>
                                            )}
                                        </div>
                                    );
                                })}
                            </div>
                        )}

                        {/* Пагинация: догрузка остатка по total (X-Total-Count) */}
                        {alerts.length < total && (
                            <button onClick={loadMore} disabled={loadingMore} className="editorial-press" style={{ ...chipBtn, fontSize: 'var(--fs-sm)', marginBottom: 12, color: link }}>
                                {loadingMore ? 'Загрузка…' : `Показать ещё (${total - alerts.length})`}
                            </button>
                        )}

                        {/* ── Секция «Фонды» (source='funds') ── */}
                        {fundsAlerts.length > 0 ? (
                            <>
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 'var(--sp-2)' }}>
                                    <Wallet size={16} style={{ color: link }} />
                                    <h3 style={{ fontWeight: 700, fontSize: 'var(--fs-base)' }}>Сигналы по фондам</h3>
                                    <span style={{ color: sub, fontSize: 'var(--fs-xs)' }}>{fundsAlerts.length}</span>
                                </div>
                                <ul style={{ display: 'flex', flexDirection: 'column', gap: 8, marginBottom: 16 }}>
                                    {fundsAlerts.map((a) => {
                                        const firesN = a.fires_count ?? 0;
                                        const firesOpen = openFires === a.id;
                                        return (
                                            <li key={a.id} className="rounded-xl p-3" style={{ background: 'var(--bg-primary)', border: '1px solid var(--border-color)' }}>
                                                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 'var(--sp-2)' }}>
                                                    <div style={{ minWidth: 0 }}>
                                                        <div style={{ fontWeight: 600, fontSize: 'var(--fs-sm)' }}>{assetLabel(a)}</div>
                                                        <div style={{ color: sub, fontSize: 'var(--fs-xs)' }}>
                                                            {METRIC_LABEL[a.indicator] || a.indicator} ≥ {a.threshold}{unitFor(a)}
                                                            {' · '}<span style={{ color: a.status === 'active' ? link : sub }}>{STATUS_LABEL[a.status] || a.status}</span>
                                                        </div>
                                                        <button
                                                            onClick={() => setOpenFires(firesOpen ? null : a.id)}
                                                            disabled={firesN === 0}
                                                            className="editorial-press"
                                                            aria-expanded={firesOpen}
                                                            style={{
                                                                marginTop: 6, padding: 0, background: 'transparent', border: 'none',
                                                                color: firesN === 0 ? sub : link, fontSize: 'var(--fs-xs)',
                                                                cursor: firesN === 0 ? 'default' : 'pointer', opacity: firesN === 0 ? 0.6 : 1,
                                                                display: 'inline-flex', alignItems: 'center', gap: 4,
                                                            }}
                                                        >
                                                            <Zap size={12} /> {firesLabel(firesN)}
                                                            {firesN > 0 && (firesOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />)}
                                                        </button>
                                                    </div>
                                                    <div style={{ display: 'flex', gap: 'var(--sp-2)', flexShrink: 0 }}>
                                                        {a.status !== 'fired' && (
                                                            <button onClick={() => toggle(a)} title={a.status === 'active' ? 'Пауза' : 'Возобновить'} aria-label={a.status === 'active' ? 'Пауза' : 'Возобновить'} className="editorial-press" style={{ ...iconBtn, color: sub }}>
                                                                {a.status === 'active' ? <Pause size={16} /> : <Play size={16} />}
                                                            </button>
                                                        )}
                                                        <button onClick={() => remove(a)} title="Удалить" aria-label="Удалить алерт" className="editorial-press" style={{ ...iconBtn, color: 'var(--funds-flow-negative, #FF7A5C)' }}><Trash2 size={16} /></button>
                                                    </div>
                                                </div>
                                                {firesOpen && firesN > 0 && (
                                                    <AlertFiresList alertId={a.id} unit={unitFor(a)} />
                                                )}
                                            </li>
                                        );
                                    })}
                                </ul>
                            </>
                        ) : (
                            /* Нет фонд-алертов — заглушка «Скоро»: создаются с индикатора «Деньги в фондах». */
                            <div className="rounded-xl p-4" style={{ background: 'var(--bg-secondary)', border: '1px dashed var(--border-color)', marginBottom: 16 }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                    <Wallet size={16} style={{ color: sub }} />
                                    <h3 style={{ fontWeight: 700, fontSize: 'var(--fs-base)', color: sub }}>Сигналы по фондам</h3>
                                </div>
                                <p style={{ color: sub, fontSize: 'var(--fs-xs)', marginTop: 6, lineHeight: 1.4 }}>
                                    Создайте сигнал по аномальному притоку-оттоку на индикаторе «Деньги в фондах» (режим Притоки-Оттоки, кнопка<BellGlyph />).
                                </p>
                            </div>
                        )}

                        {alerts.length > 1 && (
                            <button onClick={removeAll} className="editorial-press" style={{ alignSelf: 'flex-start', background: 'transparent', border: 'none', color: 'var(--funds-flow-negative, #FF7A5C)', fontSize: 'var(--fs-xs)', cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 6, padding: 0 }}>
                                <Trash2 size={14} /> Удалить все ({total || alerts.length})
                            </button>
                        )}
                        </>
                    )}
                </>
            )}

            {msg && <div style={{ marginTop: 12, fontSize: 'var(--fs-sm)', color: msg.type === 'ok' ? link : 'var(--funds-flow-negative, #FF7A5C)' }}>{msg.text}</div>}
        </div>
    );
}
