/**
 * BillingStubPage — заглушка вместо страницы оплаты ЮKassa.
 *
 * Показывается когда в backend работает StubPaymentProvider (нет ключей ЮKassa).
 * confirmation_url от stub'а ведёт сюда с ?payment_id=X&amount=Y&return_url=Z.
 * Пользователь может "Симулировать успех" → дёргает POST /api/billing/stub/simulate
 * → webhook handler активирует подписку → редирект на return_url.
 *
 * Нужно только для разработки — после подключения ЮKassa эта страница
 * никогда не открывается (confirmation_url ведёт на yookassa.ru).
 */
import { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Info, XCircle } from 'lucide-react';

export default function BillingStubPage() {
  const [params] = useSearchParams();
  const paymentId = params.get('payment_id') || '';
  const amount = params.get('amount') || '0';
  const returnUrl = params.get('return_url') || '/billing/success';
  const [loading, setLoading] = useState<'succeed' | 'cancel' | null>(null);
  const [error, setError] = useState<string | null>(null);

  const simulate = async (status: 'succeeded' | 'canceled') => {
    setLoading(status === 'succeeded' ? 'succeed' : 'cancel');
    setError(null);
    try {
      const r = await fetch('/api/billing/stub/simulate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ payment_id: paymentId, status }),
      });
      if (!r.ok) throw new Error('Ошибка симуляции');
      // Возвращаемся на return_url
      window.location.href = returnUrl;
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Ошибка');
      setLoading(null);
    }
  };

  return (
    <div className="max-w-xl mx-auto px-6 py-12">
      <div className="rounded-2xl border p-6" style={{ borderColor: 'var(--border-color)', backgroundColor: 'var(--bg-secondary)' }}>
        <div className="flex items-center gap-3 mb-4">
          <Info className="text-amber-400" size={28} />
          <h1 className="text-xl font-bold text-theme-primary">Тестовый режим оплаты</h1>
        </div>
        <p className="text-theme-secondary text-sm mb-4">
          ЮKassa ещё не подключена. Это заглушка для разработки — здесь можно имитировать
          результат оплаты и проверить что всё работает на уровне БД/логики.
        </p>
        <div className="rounded-xl p-4 mb-6 text-sm" style={{ backgroundColor: 'var(--bg-tertiary)' }}>
          <div className="flex justify-between mb-2">
            <span className="text-theme-secondary">Payment ID:</span>
            <code className="text-xs text-theme-primary">{paymentId}</code>
          </div>
          <div className="flex justify-between">
            <span className="text-theme-secondary">Сумма:</span>
            <span className="text-theme-primary font-semibold">{amount} ₽</span>
          </div>
        </div>

        <div className="flex gap-3">
          <button
            onClick={() => simulate('succeeded')}
            disabled={loading !== null}
            className="flex-1 py-2.5 rounded-xl font-medium disabled:opacity-50"
            style={{ backgroundColor: '#22c55e', color: 'white' }}
          >
            {loading === 'succeed' ? 'Обработка...' : '✓ Симулировать успех'}
          </button>
          <button
            onClick={() => simulate('canceled')}
            disabled={loading !== null}
            className="flex-1 py-2.5 rounded-xl font-medium border disabled:opacity-50"
            style={{ borderColor: 'var(--border-color)', color: 'var(--text-secondary)' }}
          >
            <XCircle size={16} className="inline mr-1" />
            Отменить
          </button>
        </div>

        {error && (
          <div className="mt-4 text-sm text-red-400 text-center">{error}</div>
        )}
      </div>
    </div>
  );
}
