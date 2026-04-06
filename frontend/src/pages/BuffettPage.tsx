import { useEffect, useState, useMemo, useCallback } from 'react';
import { Scale, Lock } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import {
    getBuffettCapGdp,
    getBuffettMcftrM2,
    type BuffettCapGdpResponse,
    type BuffettMcftrM2Response,
    type BuffettPeriod,
} from '../services/api';
import SimpleChart from '../components/SimpleChart';
import { useAuth } from '../contexts/AuthContext';
import { isPeriodAllowed } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';

type ViewMode = 'cap-gdp' | 'mcftr-m2';

const PERIOD_LABELS: Partial<Record<BuffettPeriod, string>> = {
    '10y': '10Г',
    '20y': '20Г',
    'all': 'Всё',
};

export default function BuffettPage() {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();
    const [viewMode, setViewMode] = useState<ViewMode>('cap-gdp');
    const [period, setPeriod] = useState<BuffettPeriod>('10y');
    const smooth = false;
    const [timeframe, setTimeframe] = useState<'1d' | '1w' | '1m'>('1m');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [capGdpData, setCapGdpData] = useState<BuffettCapGdpResponse | null>(null);
    const [mcftrM2Data, setMcftrM2Data] = useState<BuffettMcftrM2Response | null>(null);

    // Загрузка данных
    const loadData = useCallback(async () => {
        try {
            setLoading(true);
            setError(null);
            if (viewMode === 'cap-gdp') {
                const result = await getBuffettCapGdp(period, smooth, timeframe);
                setCapGdpData(result);
            } else {
                const result = await getBuffettMcftrM2(period, smooth);
                setMcftrM2Data(result);
            }
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : '';
            // При 403 (гость или протухший токен) — фолбэк на 1y
            if (msg.includes('авторизац') && period !== '1y') {
                setPeriod('1y');
                return;
            }
            setError('Ошибка загрузки данных');
            console.error(err);
        } finally {
            setLoading(false);
        }
    }, [period, smooth, viewMode, timeframe]);

    useEffect(() => { loadData(); }, [loadData]);

    // SSE: автоматическое обновление
    useRealtimeData(['daily', 'buffett'], loadData);

    // Подготовка данных для SimpleChart: Cap/GDP
    const capGdpChartData = useMemo(() => {
        if (!capGdpData?.data?.length) return { primary: [], secondary: [] };
        return {
            primary: capGdpData.data.map(d => ({
                time: d.date,
                value: d.buffett,
            })),
            secondary: capGdpData.data.map(d => ({
                time: d.date,
                value: d.cap,
            })),
        };
    }, [capGdpData]);

    // Подготовка данных для SimpleChart: MCFTR/M2
    const mcftrM2ChartData = useMemo(() => {
        if (!mcftrM2Data?.data?.length) return { primary: [], secondary: [] };
        return {
            primary: mcftrM2Data.data.map(d => ({
                time: d.date,
                value: d.ratio,
            })),
            secondary: mcftrM2Data.data.map(d => ({
                time: d.date,
                value: d.mcftr,
            })),
        };
    }, [mcftrM2Data]);

    return (
        <div className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
            {/* Заголовок */}
            <div className="flex items-center gap-3 mb-6">
                <div className="p-3 bg-gradient-to-br from-[#f59e0b] to-[#ef4444] rounded-xl">
                    <Scale className="w-6 h-6 text-white" />
                </div>
                <div>
                    <h1 className="text-2xl font-bold text-theme-primary">Индикатор Баффетта</h1>
                    <p className="text-theme-secondary text-sm">Оценка рынка относительно экономики</p>
                </div>
            </div>

            {/* Контролы */}
            <div className="flex items-center gap-4 mb-6 flex-wrap">
                {/* Переключатель режимов */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => setViewMode('cap-gdp')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200 ${viewMode === 'cap-gdp'
                            ? 'btn-control active'
                            : 'text-theme-secondary hover:text-theme-primary'
                            }`}
                    >
                        Капитализация / ВВП
                    </button>
                    <button
                        onClick={() => setViewMode('mcftr-m2')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200 ${viewMode === 'mcftr-m2'
                            ? 'btn-control active'
                            : 'text-theme-secondary hover:text-theme-primary'
                            }`}
                    >
                        MCFTR / M2
                    </button>
                </div>

                {/* Периоды */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    {(Object.keys(PERIOD_LABELS) as BuffettPeriod[]).map((p) => {
                        const allowed = isPeriodAllowed(p, isAuthenticated);
                        return (
                            <button
                                key={p}
                                onClick={() => {
                                    if (!allowed) { navigate('/login'); return; }
                                    setPeriod(p);
                                }}
                                title={!allowed ? 'Войдите для доступа' : undefined}
                                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200 ${
                                    !allowed
                                        ? 'text-theme-muted cursor-not-allowed opacity-50'
                                        : period === p
                                            ? 'btn-control active'
                                            : 'text-theme-secondary hover:text-theme-primary'
                                }`}
                            >
                                {PERIOD_LABELS[p]}
                                {!allowed && <Lock className="inline-block ml-0.5 w-3 h-3" />}
                            </button>
                        );
                    })}
                </div>

                {/* Таймфрейм — только для cap-gdp */}
                {viewMode === 'cap-gdp' && (
                    <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                        {(['1d', '1w', '1m'] as const).map((tf) => (
                            <button
                                key={tf}
                                onClick={() => setTimeframe(tf)}
                                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200 ${timeframe === tf ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'}`}
                            >
                                {tf === '1d' ? '1Д' : tf === '1w' ? '1Н' : '1М'}
                            </button>
                        ))}
                    </div>
                )}

            </div>

            {/* График */}
            {error ? (
                <div className="flex items-center justify-center h-[450px] bg-theme-secondary rounded-2xl border border-theme">
                    <div className="text-[#FF4D4D] text-center">
                        <p className="text-lg font-medium">{error}</p>
                        <p className="text-sm text-theme-secondary mt-2">Попробуйте обновить страницу</p>
                    </div>
                </div>
            ) : (
            <div className="bg-theme-secondary rounded-2xl border border-theme overflow-hidden" style={{ minHeight: 500 }}>
            {viewMode === 'cap-gdp' ? (
                <SimpleChart
                    data={capGdpChartData.primary}
                    secondaryData={capGdpChartData.secondary}
                    height={450}
                    primaryColor="#f59e0b"
                    secondaryColor="#C8FF2E"
                    showSecondary={true}
                    formatValue={(v) => `${v.toFixed(1)}%`}
                    formatSecondaryValue={(v) => `${v.toFixed(1)} трлн ₽`}
                    primaryLabel="Капитализация / ВВП"
                    secondaryLabel="Капитализация"
                    loading={loading}
                    showValueHeader={false}
                    legendPosition="top"
                    showDownloadButton={false}
                    showNavigator={true}
                    hideTime={true}
                />
            ) : (
                <SimpleChart
                    data={mcftrM2ChartData.primary}
                    secondaryData={mcftrM2ChartData.secondary}
                    height={450}
                    primaryColor="#f59e0b"
                    secondaryColor="#C8FF2E"
                    showSecondary={true}
                    formatValue={(v) => v.toFixed(4)}
                    formatSecondaryValue={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
                    primaryLabel="MCFTR / M2"
                    secondaryLabel="MCFTR"
                    loading={loading}
                    showValueHeader={false}
                    legendPosition="top"
                    showDownloadButton={false}
                    showNavigator={true}
                    hideTime={true}
                />
            )}
            </div>
            )}

            {/* Описание */}
            <div className="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="bg-theme-secondary rounded-2xl p-5 border border-theme">
                    <h3 className="font-semibold mb-2 text-[#f59e0b]">Капитализация / ВВП</h3>
                    <p className="text-sm text-theme-secondary leading-relaxed">
                        Классический индикатор Баффетта: отношение рыночной капитализации к ВВП.
                        Значения ниже 40% указывают на недооценённость рынка,
                        40-70% — норма, выше 70% — возможная переоценка.
                    </p>
                </div>
                <div className="bg-theme-secondary rounded-2xl p-5 border border-theme">
                    <h3 className="font-semibold mb-2 text-[#f59e0b]">MCFTR / M2</h3>
                    <p className="text-sm text-theme-secondary leading-relaxed">
                        Отношение индекса полной доходности (MCFTR) к денежной массе M2.
                        Показывает, насколько фондовый рынок растёт относительно объёма денег в экономике.
                        Снижение может указывать на отток капитала из акций.
                    </p>
                </div>
            </div>
        </div>
    );
}
