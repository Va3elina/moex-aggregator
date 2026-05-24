/**
 * ApiKeysSection — Profile-страница: management API-ключей (Pro tier).
 *
 * UX:
 *   - List active + revoked keys с metadata (prefix, name, last_used).
 *   - "Создать ключ" → modal с name input.
 *   - После create — показывается plain text **один раз** с copy-button +
 *     красным warning "сохраните, больше не покажем".
 *   - Каждый active key — кнопка "Отозвать" с confirm.
 *
 * Tier-gating: показывает CTA на upgrade для не-Pro, скрывает UI.
 */
import { useEffect, useState } from 'react';
import { Key, Plus, Copy, Trash2, AlertCircle, ExternalLink, Activity } from 'lucide-react';
import { Link } from 'react-router-dom';
import {
    listApiKeys,
    createApiKey,
    revokeApiKey,
    getApiKeyUsage,
    type ApiKeyInfo,
    type ApiKeyCreated,
    type ApiKeyUsageStats,
} from '../../services/api';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';

export default function ApiKeysSection() {
    const common = useCommonFeatures();
    const { showUpgrade } = useUpgradePrompt();
    const [keys, setKeys] = useState<ApiKeyInfo[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Usage stats — за последние 30 дней.
    const [usage, setUsage] = useState<ApiKeyUsageStats | null>(null);

    // Create flow
    const [creating, setCreating] = useState(false);
    const [newName, setNewName] = useState('');
    const [newMode, setNewMode] = useState<'live' | 'test'>('live');
    const [createdKey, setCreatedKey] = useState<ApiKeyCreated | null>(null);
    const [creatingInFlight, setCreatingInFlight] = useState(false);

    const load = async () => {
        // Грузим всегда — даже для не-Pro юзеров, потому что они могут
        // создавать и управлять test-ключами.
        try {
            setLoading(true);
            setError(null);
            const [list, usageData] = await Promise.allSettled([
                listApiKeys(),
                getApiKeyUsage(30),
            ]);
            if (list.status === 'fulfilled') setKeys(list.value);
            else throw list.reason;
            if (usageData.status === 'fulfilled') setUsage(usageData.value);
        } catch (e) {
            setError((e as Error).message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void load();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [common.api_access]);

    const handleCreate = async () => {
        // Live-ключ требует Pro. Test-ключ — любой залогиненный юзер.
        if (newMode === 'live' && !common.api_access) {
            showUpgrade({ tier: 'pro', featureName: 'API-доступ', indicator: 'api_access' });
            return;
        }
        setCreatingInFlight(true);
        try {
            const created = await createApiKey(newName.trim() || undefined, newMode);
            setCreatedKey(created);
            setKeys((prev) => [created, ...prev]);
            setNewName('');
            setNewMode('live');  // reset to default
            setCreating(false);
        } catch (e) {
            // eslint-disable-next-line no-alert
            alert((e as Error).message);
        } finally {
            setCreatingInFlight(false);
        }
    };

    const handleRevoke = async (key: ApiKeyInfo) => {
        if (!confirm(`Отозвать ключ ${key.key_prefix}…? Восстановить нельзя.`)) return;
        try {
            await revokeApiKey(key.id);
            setKeys((prev) => prev.map((k) => (k.id === key.id ? { ...k, is_revoked: true } : k)));
        } catch (e) {
            // eslint-disable-next-line no-alert
            alert((e as Error).message);
        }
    };

    const copyToClipboard = (text: string) => {
        navigator.clipboard?.writeText(text);
    };

    return (
        <section style={{ marginTop: 32 }}>
            <SectionHeader />

            {/* Non-Pro CTA баннер — компактный, не блокирующий. Test-ключи всё
                равно доступны для разработки. */}
            {!common.api_access && (
                <div
                    style={{
                        marginTop: 12,
                        padding: 14,
                        background: 'color-mix(in srgb, var(--accent) 6%, var(--bg-secondary))',
                        border: '1.5px solid color-mix(in srgb, var(--accent) 30%, transparent)',
                        borderRadius: 10,
                        display: 'flex',
                        alignItems: 'center',
                        gap: 12,
                        flexWrap: 'wrap',
                    }}
                >
                    <div style={{ flex: 1, minWidth: 200 }}>
                        <div
                            style={{
                                fontSize: 'var(--fs-sm)',
                                fontWeight: 700,
                                color: 'var(--text-primary)',
                                marginBottom: 2,
                            }}
                        >
                            Live-ключи — на тарифе Pro
                        </div>
                        <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', lineHeight: 1.45 }}>
                            Сейчас вы можете создать <strong>test-ключ</strong> для разработки —
                            бесплатно, с теми же эндпоинтами и rate-limit'ом.
                        </div>
                    </div>
                    <button
                        onClick={() =>
                            showUpgrade({ tier: 'pro', featureName: 'API-доступ', indicator: 'api_access' })
                        }
                        style={{
                            padding: '6px 14px',
                            background: 'var(--accent)',
                            color: 'var(--text-inverse)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 999,
                            fontSize: 'var(--fs-xs)',
                            fontWeight: 700,
                            cursor: 'pointer',
                            flexShrink: 0,
                        }}
                    >
                        Перейти на Pro
                    </button>
                </div>
            )}

            {/* После создания — показать plain key один раз */}
            {createdKey && (
                <div
                    style={{
                        marginTop: 12,
                        padding: 16,
                        background: 'color-mix(in srgb, var(--accent) 8%, var(--bg-secondary))',
                        border: '2px solid var(--accent)',
                        borderRadius: 12,
                    }}
                >
                    <div
                        style={{
                            display: 'flex',
                            alignItems: 'flex-start',
                            gap: 10,
                            marginBottom: 12,
                        }}
                    >
                        <AlertCircle
                            size={20}
                            style={{ color: 'var(--accent)', flexShrink: 0, marginTop: 2 }}
                        />
                        <div style={{ flex: 1 }}>
                            <p
                                style={{
                                    fontWeight: 700,
                                    fontSize: 'var(--fs-sm)',
                                    marginBottom: 4,
                                    color: 'var(--text-primary)',
                                }}
                            >
                                Сохраните ключ — больше его не покажем
                            </p>
                            <p
                                style={{
                                    fontSize: 'var(--fs-xs)',
                                    color: 'var(--text-secondary)',
                                    lineHeight: 1.4,
                                }}
                            >
                                Это ваш единственный шанс скопировать ключ. Мы храним только хеш
                                для проверок. Если потеряете — создайте новый.
                            </p>
                        </div>
                    </div>
                    <div
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 8,
                            padding: '10px 14px',
                            background: 'var(--bg-primary)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 8,
                            fontFamily: 'monospace',
                            fontSize: 'var(--fs-sm)',
                            wordBreak: 'break-all',
                        }}
                    >
                        <code style={{ flex: 1 }}>{createdKey.plain_key}</code>
                        <button
                            onClick={() => copyToClipboard(createdKey.plain_key)}
                            title="Копировать"
                            style={{
                                background: 'var(--bg-secondary)',
                                border: '1.5px solid var(--text-primary)',
                                borderRadius: 6,
                                padding: 6,
                                cursor: 'pointer',
                                color: 'var(--text-primary)',
                                flexShrink: 0,
                            }}
                        >
                            <Copy size={14} />
                        </button>
                    </div>
                    <button
                        onClick={() => setCreatedKey(null)}
                        style={{
                            marginTop: 10,
                            background: 'transparent',
                            border: 'none',
                            color: 'var(--text-secondary)',
                            cursor: 'pointer',
                            fontSize: 'var(--fs-xs)',
                            textDecoration: 'underline',
                        }}
                    >
                        Я сохранил, скрыть
                    </button>
                </div>
            )}

            {/* Create button / inline form */}
            <div style={{ marginTop: 12 }}>
                {!creating ? (
                    <button
                        onClick={() => setCreating(true)}
                        style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: 6,
                            padding: '8px 16px',
                            background: 'var(--bg-secondary)',
                            color: 'var(--text-primary)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 999,
                            fontSize: 'var(--fs-sm)',
                            fontWeight: 700,
                            cursor: 'pointer',
                        }}
                    >
                        <Plus size={14} />
                        Создать ключ
                    </button>
                ) : (
                    <div
                        style={{
                            display: 'flex',
                            flexDirection: 'column',
                            gap: 10,
                            padding: 12,
                            background: 'var(--bg-secondary)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 10,
                        }}
                    >
                        {/* Mode toggle — Live vs Test */}
                        <div style={{ display: 'flex', gap: 8 }}>
                            {(['live', 'test'] as const).map((m) => {
                                const isActive = newMode === m;
                                const isLiveLocked = m === 'live' && !common.api_access;
                                return (
                                    <button
                                        key={m}
                                        onClick={() => setNewMode(m)}
                                        style={{
                                            flex: 1,
                                            padding: '8px 12px',
                                            background: isActive
                                                ? 'var(--accent)'
                                                : 'var(--bg-primary)',
                                            color: isActive
                                                ? 'var(--text-inverse)'
                                                : 'var(--text-primary)',
                                            border: '1.5px solid var(--text-primary)',
                                            borderRadius: 8,
                                            fontSize: 'var(--fs-xs)',
                                            fontWeight: isActive ? 700 : 600,
                                            cursor: 'pointer',
                                            display: 'flex',
                                            flexDirection: 'column',
                                            alignItems: 'flex-start',
                                            gap: 2,
                                            opacity: isLiveLocked ? 0.85 : 1,
                                        }}
                                    >
                                        <span style={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: 6,
                                            fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                                            fontSize: 'var(--fs-sm)',
                                        }}>
                                            {m === 'live' ? 'pk_live_' : 'pk_test_'}
                                            {isLiveLocked && '🔒'}
                                        </span>
                                        <span style={{ fontSize: 'var(--fs-2xs)', opacity: 0.8 }}>
                                            {m === 'live'
                                                ? 'Продакшн (Pro)'
                                                : 'Разработка (бесплатно)'}
                                        </span>
                                    </button>
                                );
                            })}
                        </div>
                        {/* Name + Create + Cancel */}
                        <div style={{ display: 'flex', gap: 8 }}>
                            <input
                                value={newName}
                                onChange={(e) => setNewName(e.target.value)}
                                placeholder="Имя ключа (e.g. prod-bot)"
                                maxLength={100}
                                autoFocus
                                style={{
                                    flex: 1,
                                    padding: '6px 12px',
                                    background: 'var(--bg-primary)',
                                    border: '1.5px solid var(--text-primary)',
                                    borderRadius: 8,
                                    color: 'var(--text-primary)',
                                    fontSize: 'var(--fs-sm)',
                                }}
                            />
                            <button
                                onClick={handleCreate}
                                disabled={creatingInFlight}
                                style={{
                                    padding: '6px 14px',
                                    background: 'var(--accent)',
                                    color: 'var(--text-inverse)',
                                    border: '1.5px solid var(--text-primary)',
                                    borderRadius: 999,
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 700,
                                    cursor: creatingInFlight ? 'wait' : 'pointer',
                                }}
                            >
                                {creatingInFlight ? 'Создаём…' : 'Создать'}
                            </button>
                            <button
                                onClick={() => {
                                    setCreating(false);
                                    setNewName('');
                                    setNewMode('live');
                                }}
                                style={{
                                    padding: '6px 14px',
                                    background: 'transparent',
                                    color: 'var(--text-primary)',
                                    border: '1.5px solid var(--text-primary)',
                                    borderRadius: 999,
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 600,
                                    cursor: 'pointer',
                                }}
                            >
                                Отмена
                            </button>
                        </div>
                    </div>
                )}
            </div>

            {/* List */}
            <div style={{ marginTop: 16 }}>
                {loading && <p style={{ color: 'var(--text-muted)' }}>Загружаем…</p>}
                {error && <p style={{ color: 'var(--funds-flow-negative)' }}>{error}</p>}
                {!loading && keys.length === 0 && (
                    <p style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>
                        Ключей пока нет. Создайте первый чтобы начать использовать API.
                    </p>
                )}
                {keys.map((k) => (
                    <div
                        key={k.id}
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 12,
                            padding: '10px 14px',
                            borderBottom: '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                            opacity: k.is_revoked ? 0.5 : 1,
                        }}
                    >
                        <div style={{ flex: 1, minWidth: 0 }}>
                            <div
                                style={{
                                    fontWeight: 700,
                                    fontSize: 'var(--fs-sm)',
                                    color: 'var(--text-primary)',
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: 6,
                                    flexWrap: 'wrap',
                                }}
                            >
                                {k.name || '(без имени)'}
                                {/* Mode badge — отличает live/test визуально */}
                                <span
                                    style={{
                                        fontSize: 'var(--fs-2xs)',
                                        fontWeight: 700,
                                        padding: '1px 6px',
                                        borderRadius: 4,
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.04em',
                                        background: k.mode === 'test'
                                            ? 'color-mix(in srgb, var(--warning, #f59e0b) 18%, transparent)'
                                            : 'color-mix(in srgb, var(--accent) 18%, transparent)',
                                        color: k.mode === 'test'
                                            ? 'var(--warning, #f59e0b)'
                                            : 'var(--accent)',
                                    }}
                                >
                                    {k.mode === 'test' ? 'TEST' : 'LIVE'}
                                </span>
                                {k.is_revoked && (
                                    <span
                                        style={{
                                            fontSize: 'var(--fs-2xs)',
                                            color: 'var(--funds-flow-negative)',
                                            fontWeight: 600,
                                        }}
                                    >
                                        ОТОЗВАН
                                    </span>
                                )}
                            </div>
                            <div
                                style={{
                                    fontFamily: 'monospace',
                                    fontSize: 'var(--fs-xs)',
                                    color: 'var(--text-secondary)',
                                    marginTop: 2,
                                }}
                            >
                                {k.key_prefix}…
                            </div>
                            <div
                                style={{
                                    fontSize: 'var(--fs-2xs)',
                                    color: 'var(--text-muted)',
                                    marginTop: 2,
                                }}
                            >
                                Создан {new Date(k.created_at).toLocaleDateString('ru-RU')}
                                {k.last_used_at && (
                                    <>
                                        {' · '}
                                        Использован {new Date(k.last_used_at).toLocaleDateString('ru-RU')}
                                    </>
                                )}
                            </div>
                        </div>
                        {!k.is_revoked && (
                            <button
                                onClick={() => handleRevoke(k)}
                                title="Отозвать"
                                style={{
                                    background: 'transparent',
                                    border: '1.5px solid var(--text-primary)',
                                    borderRadius: 8,
                                    padding: '6px 10px',
                                    cursor: 'pointer',
                                    color: 'var(--text-primary)',
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    gap: 4,
                                    fontSize: 'var(--fs-xs)',
                                }}
                            >
                                <Trash2 size={12} />
                                Отозвать
                            </button>
                        )}
                    </div>
                ))}
            </div>

            {/* Usage chart — 30-day bar chart */}
            {usage && usage.total > 0 && <UsageChart usage={usage} />}

            {/* Docs link */}
            <div style={{ marginTop: 16 }}>
                <Link
                    to="/api-docs"
                    style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 4,
                        fontSize: 'var(--fs-sm)',
                        color: 'var(--accent)',
                        textDecoration: 'underline',
                    }}
                >
                    Документация API <ExternalLink size={12} />
                </Link>
            </div>
        </section>
    );
}

/**
 * UsageChart — bar chart запросов за последние 30 дней.
 *
 * Pure CSS — без external charting library, чтобы не таскать recharts/d3
 * на /profile (страница лёгкая, не хочется +200KB ради простого графика).
 * Каждый столбец — высота пропорциональна max-значению в окне.
 */
function UsageChart({ usage }: { usage: ApiKeyUsageStats }) {
    const maxCount = Math.max(...usage.by_day.map((d) => d.count), 1);
    return (
        <div
            style={{
                marginTop: 24,
                paddingTop: 20,
                borderTop: '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
            }}
        >
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                <Activity size={14} style={{ color: 'var(--text-secondary)' }} />
                <span
                    style={{
                        fontSize: 'var(--fs-sm)',
                        fontWeight: 700,
                        color: 'var(--text-primary)',
                    }}
                >
                    Использование API
                </span>
                <span
                    style={{
                        marginLeft: 'auto',
                        fontSize: 'var(--fs-xs)',
                        color: 'var(--text-muted)',
                    }}
                >
                    {usage.total.toLocaleString('ru-RU')} запросов · {usage.days} дней
                </span>
            </div>
            <div
                style={{
                    display: 'flex',
                    alignItems: 'flex-end',
                    gap: 3,
                    height: 64,
                    padding: '0 2px',
                }}
                title={`Максимум за день: ${maxCount}`}
            >
                {usage.by_day.map((d) => {
                    const heightPct = (d.count / maxCount) * 100;
                    const isToday = d.date === new Date().toISOString().slice(0, 10);
                    return (
                        <div
                            key={d.date}
                            title={`${d.date}: ${d.count}`}
                            style={{
                                flex: 1,
                                minWidth: 4,
                                height: '100%',
                                display: 'flex',
                                alignItems: 'flex-end',
                            }}
                        >
                            <div
                                style={{
                                    width: '100%',
                                    height: `${Math.max(heightPct, d.count > 0 ? 4 : 0)}%`,
                                    background: isToday ? 'var(--accent)' : 'color-mix(in srgb, var(--accent) 50%, var(--text-muted))',
                                    borderRadius: '2px 2px 0 0',
                                    minHeight: d.count > 0 ? 2 : 0,
                                    transition: 'background 120ms',
                                }}
                            />
                        </div>
                    );
                })}
            </div>
            <div
                style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    marginTop: 6,
                    fontSize: 'var(--fs-2xs)',
                    color: 'var(--text-muted)',
                }}
            >
                <span>{usage.by_day[0]?.date}</span>
                <span>сегодня</span>
            </div>
        </div>
    );
}

function SectionHeader() {
    return (
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Key size={18} style={{ color: 'var(--text-secondary)' }} />
            <h2
                style={{
                    fontSize: 'var(--fs-lg)',
                    fontWeight: 700,
                    color: 'var(--text-primary)',
                    margin: 0,
                }}
            >
                API-ключи
            </h2>
        </div>
    );
}
