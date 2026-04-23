import { useEffect, useState, useMemo, useCallback } from 'react';
import { Scale, Lock } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import PageHeader from '../components/PageHeader';
import {
    getBuffettCapGdp,
    getBuffettCapM2,
    type BuffettCapGdpResponse,
    type BuffettRatioResponse,
    type BuffettPeriod,
} from '../services/api';
import SimpleChart from '../components/SimpleChart';
import { useAuth } from '../contexts/AuthContext';
import { isPeriodAllowed } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';

type ViewMode = 'cap-gdp' | 'cap-m2';

const PERIOD_LABELS: Partial<Record<BuffettPeriod, string>> = {
    '5y': '5Л',
    '10y': '10Л',
    '20y': '20Л',
    'all': 'Всё',
};

export default function BuffettPage() {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();
    const [viewMode, setViewMode] = useState<ViewMode>('cap-gdp');
    const [period, setPeriod] = useState<BuffettPeriod>('10y');
    const smooth = false;
    const [timeframe, setTimeframe] = useState<'1d' | '1w' | '1m'>('1m');
    const [forecastTarget, setForecastTarget] = useState<number | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [capGdpData, setCapGdpData] = useState<BuffettCapGdpResponse | null>(null);
    const [capM2Data, setCapM2Data] = useState<BuffettRatioResponse | null>(null);

    // Загрузка данных
    const loadData = useCallback(async () => {
        try {
            setLoading(true);
            setError(null);
            if (viewMode === 'cap-gdp') {
                const result = await getBuffettCapGdp(period, smooth, timeframe);
                setCapGdpData(result);
            } else {
                const result = await getBuffettCapM2(period, smooth, timeframe);
                setCapM2Data(result);
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

    // Подготовка данных для SimpleChart: Cap/GDP (с прогнозом)
    const capGdpChartData = useMemo(() => {
        if (!capGdpData?.data?.length) return { primary: [], secondary: [] };

        const primary = capGdpData.data.map(d => ({ time: d.date, value: d.buffett }));
        const secondary = capGdpData.data.map(d => ({ time: d.date, value: d.cap }));

        if (forecastTarget !== null) {
            const last = capGdpData.data[capGdpData.data.length - 1];
            if (last.gdp_ttm > 0) {
                const targetCap = last.gdp_ttm * forecastTarget / 100;
                const currentRatio = last.buffett;
                const currentCap = last.cap;
                const lastDate = new Date(last.date);
                // 12 промежуточных точек за 12 месяцев с хаотичным шумом
                const steps = 12;
                const ratioDiff = forecastTarget - currentRatio;
                const capDiff = targetCap - currentCap;
                // Разный шум для ratio и cap (разные seed)
                const seedR = forecastTarget * 7 + 13;
                const seedC = forecastTarget * 11 + 37;
                for (let i = 1; i <= steps; i++) {
                    const t = i / steps;
                    const noiseR = Math.sin(seedR * i * 0.7) * 0.2 * (1 - t * 0.5);
                    const noiseC = Math.sin(seedC * i * 0.9 + 2) * 0.18 * (1 - t * 0.5);
                    const stepDate = new Date(lastDate);
                    stepDate.setMonth(stepDate.getMonth() + i);
                    const stepDateStr = stepDate.toISOString().slice(0, 10);
                    primary.push({ time: stepDateStr, value: currentRatio + ratioDiff * Math.max(0, t + noiseR) });
                    secondary.push({ time: stepDateStr, value: currentCap + capDiff * Math.max(0, t + noiseC) });
                }
            }
        }

        return { primary, secondary };
    }, [capGdpData, forecastTarget]);

    // Подготовка данных для SimpleChart: Cap/M2
    const capM2ChartData = useMemo(() => {
        if (!capM2Data?.data?.length) return { primary: [], secondary: [] };
        return {
            primary: capM2Data.data.map(d => ({
                time: d.date,
                value: d.ratio,
            })),
            secondary: capM2Data.data.map(d => ({
                time: d.date,
                value: d.cap ?? 0,
            })),
        };
    }, [capM2Data]);

    return (
        <div className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
            <PageHeader
                icon={Scale}
                title="Индикатор Баффетта"
                subtitle="Оценка рынка относительно экономики"
            />

            {/* Контролы */}
            <div className="flex items-center gap-4 mb-6 flex-wrap">
                {/* Переключатель режимов */}
                <div className="btn-group-scroll gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => setViewMode('cap-gdp')}
                        className={`px-2 md:px-3 py-1 md:py-1.5 text-xs md:text-sm font-medium rounded-lg transition-colors duration-200 ${viewMode === 'cap-gdp'
                            ? 'btn-control active'
                            : 'text-theme-secondary hover:text-theme-primary'
                            }`}
                    >
                        Капитализация / ВВП
                    </button>
                    <button
                        onClick={() => setViewMode('cap-m2')}
                        className={`px-2 md:px-3 py-1 md:py-1.5 text-xs md:text-sm font-medium rounded-lg transition-colors duration-200 ${viewMode === 'cap-m2'
                            ? 'btn-control active'
                            : 'text-theme-secondary hover:text-theme-primary'
                            }`}
                    >
                        Капитализация / M2
                    </button>
                </div>

                {/* Периоды */}
                <div className="btn-group-scroll gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
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
                                className={`px-2 md:px-3 py-1 md:py-1.5 text-xs md:text-sm font-medium rounded-lg transition-colors duration-200 ${
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

                {/* Таймфрейм — для cap-gdp и cap-m2 */}
                {(viewMode === 'cap-gdp' || viewMode === 'cap-m2') && (
                    <div className="btn-group-scroll gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                        {(['1d', '1w', '1m'] as const).map((tf) => (
                            <button
                                key={tf}
                                onClick={() => setTimeframe(tf)}
                                className={`px-2 md:px-3 py-1 md:py-1.5 text-xs md:text-sm font-medium rounded-lg transition-colors duration-200 ${timeframe === tf ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'}`}
                            >
                                {tf === '1d' ? '1Д' : tf === '1w' ? '1Н' : '1М'}
                            </button>
                        ))}
                    </div>
                )}

                {/* Прогноз — только для cap-gdp */}
                {viewMode === 'cap-gdp' && (
                    <div className="flex items-center gap-2">
                        <span className="text-sm text-theme-secondary">Прогноз:</span>
                        <select
                            value={forecastTarget ?? ''}
                            onChange={(e) => setForecastTarget(e.target.value ? Number(e.target.value) : null)}
                            className="bg-theme-secondary border border-theme rounded-lg px-3 py-1.5 text-sm text-theme-primary focus:outline-none focus:border-[#C8FF2E]"
                        >
                            <option value="">Выкл</option>
                            {[10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110].map(v => (
                                <option key={v} value={v}>{v}%</option>
                            ))}
                        </select>
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
                    primaryColor="#C8FF2E"
                    secondaryColor="#f59e0b"
                    showSecondary={true}
                    formatValue={(v) => `${v.toFixed(2)}%`}
                    formatSecondaryValue={(v) => `${v.toFixed(2)} трлн ₽`}
                    formatSecondaryAxis={(v) => v.toFixed(2)}
                    primaryLabel="Капитализация / ВВП"
                    secondaryLabel="Капитализация (трлн ₽)"
                    loading={loading}
                    forecastCount={forecastTarget !== null ? 12 : 0}
                    showValueHeader={false}
                    legendPosition="top"
                    showDownloadButton={false}
                    showNavigator={true}
                    hideTime={true}
                />
            ) : (
                <SimpleChart
                    data={capM2ChartData.primary}
                    secondaryData={capM2ChartData.secondary}
                    height={450}
                    primaryColor="#C8FF2E"
                    secondaryColor="#f59e0b"
                    showSecondary={true}
                    formatValue={(v) => `${(v * 100).toFixed(2)}%`}
                    formatSecondaryValue={(v) => `${v.toFixed(2)} трлн ₽`}
                    formatSecondaryAxis={(v) => v.toFixed(2)}
                    primaryLabel="Капитализация / M2"
                    secondaryLabel="Капитализация (трлн ₽)"
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
                    <h3 className="font-semibold mb-2 text-[#f59e0b]">Капитализация / ВВП — оценка vs экономика</h3>
                    <p className="text-sm text-theme-secondary leading-relaxed">
                        Классический индикатор Баффетта: рыночная капитализация к ВВП.
                        Отвечает на вопрос «дорог ли рынок относительно того, что реально производит экономика».
                        Для России: ниже 40% — недооценка, 40–70% — норма, выше 70% — возможная переоценка.
                        Знаменатель (ВВП) движется медленно, поэтому индикатор отражает долгосрочную картину.
                    </p>
                </div>
                <div className="bg-theme-secondary rounded-2xl p-5 border border-theme">
                    <h3 className="font-semibold mb-2 text-[#f59e0b]">Капитализация / M2 — оценка vs ликвидность</h3>
                    <p className="text-sm text-theme-secondary leading-relaxed">
                        Рыночная капитализация к денежной массе M2 (наличные + депозиты).
                        Отвечает на другой вопрос: «сколько в стране денег относительно рынка акций».
                        Низкие значения = денег много, но они не идут в акции (сидят в депозитах/ОФЗ).
                        Знаменатель чувствителен к действиям ЦБ и бюджета — индикатор быстрее реагирует
                        на монетарные условия, чем Cap/ВВП.
                    </p>
                </div>
            </div>
        </div>
    );
}
