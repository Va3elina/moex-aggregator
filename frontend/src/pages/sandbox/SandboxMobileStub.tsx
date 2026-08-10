/**
 * SandboxMobileStub — что показываем вместо терминала на узком экране.
 *
 * Рабочий стол с плавающими окнами на 375px не живёт: тулбар обрезается
 * (кнопка «Выстроить» уезжает за край и недоступна — горизонтального скролла
 * у оболочки нет), окно панели шире экрана, график срезан справа. Вместо
 * сломанного продукта показываем честный экран.
 *
 * Штатной ссылки на /sandbox с телефона нет — кнопки терминала в мобильном
 * интерфейсе не существует. Сюда попадают, открыв на телефоне ссылку,
 * сохранённую с десктопа; на этот сценарий заглушка и рассчитана.
 *
 * Стилизуем сайтовыми editorial-токенами, а не токенами песочницы: это
 * страница-сообщение, а не часть терминала, и человек уходит с неё на сайт.
 */
import { Link } from 'react-router-dom';
import { Monitor, ArrowLeft } from 'lucide-react';
import FrameLogo from '../../components/FrameLogo';

export default function SandboxMobileStub() {
    return (
        <div
            style={{
                minHeight: '100dvh',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                gap: 'var(--sp-4)',
                padding: 'var(--sp-6)',
                textAlign: 'center',
                background: 'var(--bg-primary)',
                color: 'var(--text-primary)',
            }}
        >
            <FrameLogo size={28} color="var(--accent)" />

            <div
                aria-hidden
                style={{
                    display: 'grid',
                    placeItems: 'center',
                    width: 72,
                    height: 72,
                    borderRadius: '50%',
                    border: '1.5px solid var(--text-primary)',
                    color: 'var(--accent)',
                }}
            >
                <Monitor size={32} strokeWidth={1.75} />
            </div>

            <h1 style={{ fontSize: 'var(--fs-xl)', fontWeight: 800, letterSpacing: '-0.02em', margin: 0 }}>
                Терминал работает на компьютере
            </h1>

            <p style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', maxWidth: 380, margin: 0 }}>
                Это рабочий стол: индикаторы открываются плавающими окнами, их двигают
                и раскладывают по листам. Для телефона такой экран слишком тесный —
                откройте ссылку на компьютере.
            </p>

            <Link
                to="/"
                className="editorial-press"
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 8,
                    marginTop: 'var(--sp-2)',
                    padding: '12px 20px',
                    borderRadius: 999,
                    background: 'var(--accent)',
                    color: '#fff',
                    border: '1.5px solid var(--text-primary)',
                    fontWeight: 700,
                    fontSize: 'var(--fs-sm)',
                    textDecoration: 'none',
                }}
            >
                <ArrowLeft size={16} />
                Вернуться на сайт
            </Link>
        </div>
    );
}
