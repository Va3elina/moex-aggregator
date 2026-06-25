/**
 * TrialFailPage — привязка карты для пробного периода не удалась.
 * Терминальный Fail Add Card URL должен вести сюда (а не на общий /billing/fail),
 * чтобы текст был про триал, а не про «оплату».
 */
import { Link } from 'react-router-dom';
import { XCircle } from 'lucide-react';

export default function TrialFailPage() {
  return (
    <div style={{
      maxWidth: 520, margin: '0 auto', padding: '48px 20px',
      textAlign: 'center', color: 'var(--text-primary, #1a1a1a)',
    }}>
      <XCircle size={56} color="var(--danger, #d9534f)" style={{ display: 'block', margin: '0 auto 16px' }} />
      <h1 style={{ fontSize: 'var(--fs-2xl)', marginBottom: 12 }}>Не удалось активировать пробный период</h1>
      <p style={{ color: 'var(--text-secondary, #666)', marginBottom: 8, lineHeight: 1.6 }}>
        Карту не удалось привязать — банк отклонил операцию или вы отменили её.
        <b> Деньги не списаны.</b> Пробный период не активирован.
      </p>
      <p style={{ color: 'var(--text-secondary, #666)', marginBottom: 20, lineHeight: 1.6 }}>
        Можно попробовать ещё раз — при привязке карта проверяется на 0&nbsp;₽,
        списание происходит только по окончании пробного периода.
      </p>
      <div style={{ display: 'flex', gap: 12, justifyContent: 'center', flexWrap: 'wrap' }}>
        <Link to="/pricing" className="editorial-press" style={{
          display: 'inline-block', padding: '12px 24px', borderRadius: 8,
          background: 'var(--accent, #FF5C2B)', color: '#fff', textDecoration: 'none', fontWeight: 600,
        }}>
          Попробовать ещё раз
        </Link>
        <Link to="/" className="editorial-press" style={{
          display: 'inline-block', padding: '12px 24px', borderRadius: 8,
          border: '1px solid var(--border-color, #ddd)', color: 'var(--text-primary, #1a1a1a)',
          textDecoration: 'none', fontWeight: 600,
        }}>
          На главную
        </Link>
      </div>
      <div style={{ marginTop: 20, fontSize: 'var(--fs-sm)', color: 'var(--text-muted, #999)' }}>
        Вопросы? <a href="mailto:frameinfo@mail.ru" style={{ color: 'var(--accent, #FF5C2B)' }}>frameinfo@mail.ru</a>
      </div>
    </div>
  );
}
