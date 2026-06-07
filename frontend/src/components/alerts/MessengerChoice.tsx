/**
 * MessengerChoice — выбор мессенджера для алертов: Telegram (работает) + МАКС (в разработке).
 * Две иконки-карточки. Telegram → onTelegram(); МАКС задизейблен с бейджем «Скоро».
 * Чистый presentational-компонент: state привязки + polling владеет родитель
 * (CreateAlertModal / TelegramAlertsSection). Inline-styles + CSS-vars — переживает
 * portal/тему (как UpgradeModal).
 */
import { type CSSProperties } from 'react';
import { Send, MessageCircle } from 'lucide-react';

interface Props {
    onTelegram: () => void;
    busy?: boolean;
    /** Заголовок над выбором. null — без заголовка (когда родитель уже пояснил). */
    title?: string | null;
}

const grid: CSSProperties = {
    display: 'flex', gap: 10, flexWrap: 'wrap',
};
const cardBase: CSSProperties = {
    flex: '1 1 130px', minWidth: 130, borderRadius: 12, padding: '14px 12px',
    border: '2px solid var(--text-primary)', textAlign: 'center',
    display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6,
};
const badge: CSSProperties = {
    fontSize: 'var(--fs-xs)', fontWeight: 700, padding: '1px 8px', borderRadius: 999,
    border: '1.5px solid currentColor', lineHeight: 1.4,
};

export default function MessengerChoice({ onTelegram, busy, title = 'Выберите мессенджер' }: Props) {
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
                    aria-label="Подключить Telegram"
                >
                    <Send size={26} />
                    <span style={{ fontWeight: 700, fontSize: 'var(--fs-sm)' }}>Telegram</span>
                    <span style={{ ...badge, opacity: 0.9 }}>{busy ? '…' : 'Подключить'}</span>
                </button>

                {/* ── МАКС — в разработке ── */}
                <div
                    style={{
                        ...cardBase,
                        background: 'var(--bg-secondary)', color: 'var(--text-secondary)',
                        cursor: 'not-allowed', opacity: 0.65, filter: 'grayscale(0.4)',
                    }}
                    title="Мессенджер МАКС — в разработке, добавим позже"
                    aria-disabled="true"
                >
                    <MessageCircle size={26} />
                    <span style={{ fontWeight: 700, fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>МАКС</span>
                    <span style={badge}>Скоро</span>
                </div>
            </div>
            <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', marginTop: 10, lineHeight: 1.5 }}>
                Сейчас алерты доступны в&nbsp;Telegram. Мессенджер&nbsp;<b>МАКС</b> — в&nbsp;разработке,
                подключим позже. Мессенджер можно сменить в&nbsp;любой момент в&nbsp;профиле.
            </p>
        </div>
    );
}
