/**
 * SpeedPayButtons — встраивает T-Bank виджет быстрых кнопок оплаты.
 *
 * T-Bank рендерит свои стандартизованные кнопки (СБП, T-Pay, BNPL, MirPay…)
 * прямо на нашей странице. Какие кнопки реально появятся — управляется в
 * кабинете терминала T-Bank: Магазины → Прием оплаты → Кнопки быстрой оплаты.
 *
 * При клике пользователя:
 *   1. SDK вызывает наш `paymentStartCallback`
 *   2. Callback POST'ит наш backend `/api/billing/checkout` с widget_mode=true
 *   3. Backend возвращает PaymentURL от T-Bank
 *   4. SDK сам редиректит на PaymentURL (или открывает QR overlay для СБП)
 *
 * Flow возврата:
 *   - SuccessURL/FailURL T-Bank возвращают пользователя на /billing/success
 *     или /billing/fail соответственно — так же как для iframe-modal flow
 *   - Webhook на сервере обработает activate_from_webhook независимо от того,
 *     виджетом был платёж или iframe-формой
 *
 * Дока T-Bank:
 *  - https://developer.tbank.ru/eacq/intro/developer/setup_js/setup_speedpay/
 */
import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { useTbankIntegration } from '../../hooks/useTbankIntegration';
import { apiFetch } from '../../services/api';
import { signupUrlForIntent } from '../../utils/checkoutIntent';
import type { TBankPaymentsApi } from '../../types/tbank';

interface Props {
  /** terminalKey от backend через /api/billing/plans (только для tbank-провайдера) */
  terminalKey: string | null | undefined;
  /** plan_id для checkout. Если null — кнопки скрыты. */
  planId: string | null;
  /** Опционально: callback для аналитики/UX. */
  onStart?: () => void;
}

export default function SpeedPayButtons({ terminalKey, planId, onStart }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const widgetRef = useRef<TBankPaymentsApi | null>(null);
  const planIdRef = useRef<string | null>(planId);
  const onStartRef = useRef<typeof onStart>(onStart);
  const navigate = useNavigate();
  const { isAuthenticated } = useAuth();
  const { integration, ready, error } = useTbankIntegration(terminalKey);
  const [mountError, setMountError] = useState<string | null>(null);

  // Держим актуальные значения для callback — без re-mount'а виджета
  useEffect(() => {
    planIdRef.current = planId;
  }, [planId]);
  useEffect(() => {
    onStartRef.current = onStart;
  }, [onStart]);

  // Создаём + mount'им виджет когда SDK готов и контейнер в DOM.
  useEffect(() => {
    if (!ready || !integration || !containerRef.current) return;

    let cancelled = false;
    let widget: TBankPaymentsApi | null = null;

    try {
      widget = integration.payments.create();
      widgetRef.current = widget;

      const callback = async () => {
        // Гость не может купить — отправляем на login и кидаем error
        // чтобы SDK не пытался редиректить дальше.
        if (!isAuthenticated) {
          const pending = planIdRef.current;
          navigate(signupUrlForIntent(pending ? { kind: 'plan', planId: pending } : null));
          throw new Error('Auth required');
        }
        const pid = planIdRef.current;
        if (!pid) {
          throw new Error('No plan selected');
        }
        onStartRef.current?.();
        const resp = await apiFetch('/api/billing/checkout', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            plan_id: pid,
            return_url: `${window.location.origin}/billing/success`,
            widget_mode: true,
            recurrent: true, // подписка ВСЕГДА с авто-продлением — паритет с картой
          }),
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          throw new Error(err.detail || 'Checkout failed');
        }
        const data = await resp.json();
        // SDK ждёт `{ paymentUrl: '...' }`. Наш backend возвращает confirmation_url.
        return { paymentUrl: data.confirmation_url as string };
      };

      // setPaymentStartCallback может быть sync или async — оборачиваем безопасно
      const r = widget.setPaymentStartCallback(callback);
      if (r && typeof (r as Promise<unknown>).then === 'function') {
        (r as Promise<unknown>).catch((e: unknown) => {
          if (!cancelled) {
            setMountError(e instanceof Error ? e.message : String(e));
          }
        });
      }

      widget.mount(containerRef.current);
    } catch (e) {
      setMountError(e instanceof Error ? e.message : String(e));
    }

    return () => {
      cancelled = true;
      try {
        widget?.unmount?.();
        widget?.destroy?.();
      } catch {
        // ignore — некоторые версии SDK без unmount/destroy
      }
      widgetRef.current = null;
    };
  }, [ready, integration, isAuthenticated, navigate]);

  // Если терминал не передан (не-tbank провайдер) или SDK не загрузился —
  // ничего не рендерим. Это позволяет компоненту быть «опциональным» на странице.
  if (!terminalKey) return null;

  if (error) {
    // Лёгкая деградация: ошибку SDK тихо логируем — у пользователя всё ещё
    // есть основная кнопка «Оформить картой» рядом.
    if (import.meta.env.DEV) {
      console.warn('T-Bank SDK load error:', error);
    }
    return null;
  }

  return (
    <div className="w-full">
      <div
        ref={containerRef}
        className="w-full flex flex-wrap justify-center gap-2 min-h-[44px]"
        // T-Bank виджет вставляет свои кнопки внутрь. Не задаём
        // background — кнопки приходят со своим брендингом.
      />
      {mountError && import.meta.env.DEV && (
        <p className="text-xs text-red-400 mt-1">
          SpeedPay error: {mountError}
        </p>
      )}
    </div>
  );
}
