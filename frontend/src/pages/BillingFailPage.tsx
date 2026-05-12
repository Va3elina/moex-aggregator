/**
 * BillingFailPage — возвращаемся сюда если оплата провалилась.
 *
 * T-Bank редиректит на FailURL при:
 *  - пользователь отменил оплату (нажал «Назад»/закрыл форму)
 *  - неверный CVV, недостаточно средств, лимиты карты, банк отказал
 *  - 3DS не пройден
 *  - истёк срок действия платёжной сессии
 *
 * Что показываем: понятное сообщение + кнопку «Попробовать ещё раз»
 * (ведёт обратно на /pricing) + ссылку на поддержку.
 *
 * НЕ polling статус — на этой странице мы точно знаем что оплата не прошла.
 * Если по какой-то причине webhook всё же придёт позднее с CONFIRMED — пользователь
 * увидит активный тариф в /profile.
 */
import { Link } from 'react-router-dom';
import { XCircle } from 'lucide-react';

export default function BillingFailPage() {
  return (
    <div className="max-w-xl mx-auto px-6 py-12 text-center">
      <XCircle
        className="w-16 h-16 mx-auto mb-4"
        style={{ color: 'var(--funds-flow-negative, #EF4444)' }}
      />
      <h1
        className="text-2xl font-bold mb-2"
        style={{ color: 'var(--text-primary)' }}
      >
        Оплата не прошла
      </h1>
      <p
        className="mb-2"
        style={{ color: 'var(--text-secondary)' }}
      >
        Платёж был отменён или отклонён банком.
      </p>
      <p
        className="mb-6 text-sm"
        style={{ color: 'var(--text-muted)' }}
      >
        Деньги не списаны. Можно попробовать ещё раз с другой картой,
        либо вернуться позже.
      </p>
      <div className="flex flex-wrap gap-3 justify-center">
        <Link
          to="/pricing"
          className="px-5 py-2 rounded-xl text-sm font-medium transition-opacity hover:opacity-90"
          style={{
            backgroundColor: 'var(--accent)',
            color: '#fff',
          }}
        >
          Попробовать ещё раз
        </Link>
        <Link
          to="/profile"
          className="px-5 py-2 rounded-xl text-sm font-medium border transition-colors"
          style={{
            borderColor: 'var(--border-color)',
            color: 'var(--text-primary)',
          }}
        >
          В профиль
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
