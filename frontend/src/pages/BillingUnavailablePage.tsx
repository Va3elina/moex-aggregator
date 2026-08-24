/**
 * BillingUnavailablePage — куда возвращается новый пользователь после ввода
 * карты на демо-терминале T-Bank.
 *
 * Приём платежей ещё не запущен: чекаут новых юзеров (env BILLING_DEMO_SINCE)
 * роутится на демо-терминал, где деньги не списываются, а подписка не
 * активируется никогда (api/billing/service.py — _verify_with_provider,
 * activate_from_webhook, sync_pending_for_user).
 *
 * Старые платящие сюда не попадают: их checkout и продления идут на боевом
 * терминале.
 */
import { Link } from 'react-router-dom';
import { Clock } from 'lucide-react';

export default function BillingUnavailablePage() {
  return (
    <div className="max-w-xl mx-auto px-6 py-12 text-center">
      <Clock
        className="w-16 h-16 mx-auto mb-4"
        style={{ color: 'var(--text-muted)' }}
      />
      <h1
        className="text-2xl font-bold mb-2"
        style={{ color: 'var(--text-primary)' }}
      >
        Оплата временно недоступна
      </h1>
      <p
        className="mb-2"
        style={{ color: 'var(--text-secondary)' }}
      >
        Приём платежей ещё не запущен, поэтому подписка не оформлена.
      </p>
      <p
        className="mb-6 text-sm"
        style={{ color: 'var(--text-muted)' }}
      >
        Деньги не списаны, карта не сохранена. О запуске оплаты объявим в
        Телеграм-канале.
      </p>
      <div className="flex flex-wrap gap-3 justify-center">
        <a
          href="https://t.me/+vbt614-Qq1w1YWYy"
          target="_blank"
          rel="noopener noreferrer"
          className="px-5 py-2 rounded-xl text-sm font-medium transition-opacity hover:opacity-90"
          style={{
            backgroundColor: 'var(--accent)',
            color: '#fff',
          }}
        >
          Подписаться на канал
        </a>
        <Link
          to="/"
          className="px-5 py-2 rounded-xl text-sm font-medium border transition-colors"
          style={{
            borderColor: 'var(--border-color)',
            color: 'var(--text-primary)',
          }}
        >
          На главную
        </Link>
      </div>
      <p
        className="mt-8 text-xs"
        style={{ color: 'var(--text-muted)' }}
      >
        Возникли вопросы? Напиши на{' '}
        <a
          href="mailto:frameinfo@mail.ru"
          style={{ color: 'var(--accent)' }}
          className="hover:underline"
        >
          frameinfo@mail.ru
        </a>
      </p>
    </div>
  );
}
