/**
 * MessengerChoice — выбор мессенджера для алертов: Telegram (работает) + МАКС (в разработке).
 * Две иконки-карточки. Telegram → onTelegram(); МАКС задизейблен с бейджем «Скоро».
 * Чистый presentational-компонент: state привязки + polling владеет родитель
 * (CreateAlertModal / TelegramAlertsSection). Inline-styles + CSS-vars — переживает
 * portal/тему (как UpgradeModal).
 */
import { type CSSProperties } from 'react';
import { Send, MessageCircle } from 'lucide-react';
import { t } from '../../i18n';

interface Props {
    onTelegram: () => void;
    busy?: boolean;
    /** Заголовок над выбором. null — без заголовка (когда родитель уже пояснил). */
    title?: string | null;
}

const grid: CSSProperties = {
    display: 'flex', gap: 'var(--sp-3)', flexWrap: 'wrap',
};
const cardBase: CSSProperties = {
    flex: '1 1 130px', minWidth: 130, borderRadius: 12, padding: 'var(--sp-4) var(--sp-3)',
    border: '2px solid var(--text-primary)', textAlign: 'center',
    display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 'var(--sp-2)',
};
const badge: CSSProperties = {
    fontSize: 'var(--fs-xs)', fontWeight: 700, padding: '1px 8px', borderRadius: 999,
    border: '1.5px solid currentColor', lineHeight: 1.4,
};

export default function MessengerChoice({ onTelegram, busy, title }: Props) {
    if (title === undefined) title = t('Выберите мессенджер');
    return (
        <div>
            {title && (
                <div style={{ fontSize: 'var(--fs-sm)', fontWeight: 600, marginBottom: 10 }}>{title}</div>
            )}
            <div style={grid}>
                {/* ── Telegram — работает ── */}
                <button
                    type="button"
                    onClick={onTelegram}
                    disabled={busy}
                    className="editorial-press"
                    style={{
                        ...cardBase,
                        background: 'var(--accent)', color: 'var(--text-inverse)',
                        cursor: busy ? 'default' : 'pointer', opacity: busy ? 0.7 : 1,
                    }}
                    aria-label={t('Подключить Telegram')}
                >
                    <Send size={26} />
                    <span style={{ fontWeight: 700, fontSize: 'var(--fs-sm)' }}>Telegram</span>
                    <span style={{ ...badge, opacity: 0.9 }}>{busy ? '…' : t('Подключить')}</span>
                </button>

                {/* ── МАКС — в разработке ── */}
                <div
                    style={{
                        ...cardBase,
                        background: 'var(--bg-secondary)', color: 'var(--text-secondary)',
                        cursor: 'not-allowed', opacity: 0.65, filter: 'grayscale(0.4)',
                    }}
                    title={t('Мессенджер МАКС — в разработке, добавим позже')}
                    aria-disabled="true"
                >
                    <MessageCircle size={26} />
                    <span style={{ fontWeight: 700, fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>МАКС</span>
                    <span style={badge}>{t('Скоро')}</span>
                </div>
            </div>
            <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', marginTop: 10, lineHeight: 1.5 }}>
                {t('Сейчас уведомления доступны в Telegram. Мессенджер')}&nbsp;<b>{t('МАКС')}</b>&nbsp;{t('в разработке, подключим позже. Мессенджер можно сменить в любой момент в профиле.')}
            </p>
        </div>
    );
}
