/**
 * TBankCheckoutModal — модальное окно с встроенной формой оплаты Т-Банка.
 *
 * Flow:
 *   1. Backend POST /api/billing/checkout вернул confirmation_url
 *      (это PaymentURL вида https://securepay.tinkoff.ru/Cz5GZAOd).
 *   2. Этот компонент открывает PaymentURL в <iframe> поверх страницы.
 *   3. Параллельно polling-им /api/billing/status каждые 3 сек.
 *   4. Когда подписка стала is_active=true → переходим на /billing/success.
 *   5. Кнопка «X» отменяет (закрывает modal без активации).
 *
 * Fallback: если iframe не загрузился за 5 секунд (X-Frame-Options, расширения,
 * adblock) — показываем кнопку «Открыть в новой вкладке» — старый flow redirect.
 *
 * Документация Т-Банка по iframe-форме:
 *   https://developer.tbank.ru/eacq/intro/developer/setup_js/setup_iframe/
 */
import { useEffect, useRef, useState } from 'react';
import { X, ExternalLink, Loader2, XCircle } from 'lucide-react';
import { apiFetch } from '../../services/api';

interface Props {
  /** PaymentURL от Т-Банка (например https://securepay.tinkoff.ru/Cz5GZAOd) */
  paymentUrl: string;
  /** Закрытие пользователем без оплаты (клик X / Escape / клик по backdrop) */
  onCancel: () => void;
  /** Опционально — куда редиректить после успешной оплаты. Default '/billing/success' */
  successRedirectTo?: string;
  /** Опционально — куда редиректить если оплата провалилась. Default '/billing/fail' */
  failRedirectTo?: string;
}

interface BillingStatus {
  tier: string;
  is_active: boolean;
}

const POLL_INTERVAL_MS = 3000;
const IFRAME_LOAD_TIMEOUT_MS = 5000;

export default function TBankCheckoutModal({
  paymentUrl,
  onCancel,
  successRedirectTo = '/billing/success',
  failRedirectTo = '/billing/fail',
}: Props) {
  const [iframeLoaded, setIframeLoaded] = useState(false);
  const [iframeBlocked, setIframeBlocked] = useState(false);
  const [paymentDone, setPaymentDone] = useState(false);
  const [paymentFailed, setPaymentFailed] = useState(false);
  const iframeRef = useRef<HTMLIFrameElement | null>(null);
  const pollTimerRef = useRef<number | null>(null);
  const loadTimerRef = useRef<number | null>(null);

  // Esc → cancel
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !paymentDone && !paymentFailed) onCancel();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onCancel, paymentDone, paymentFailed]);

  // Polling /api/billing/status → если активна, success
  useEffect(() => {
    let cancelled = false;
    let attempts = 0;
    const maxAttempts = 60; // 60 × 3s = 3 минуты максимум

    const tick = async () => {
      if (cancelled || paymentFailed) return;
      attempts += 1;
      try {
        const r = await apiFetch('/api/billing/status');
        if (!r.ok) throw new Error('status fetch failed');
        const s: BillingStatus = await r.json();
        if (s.is_active) {
          setPaymentDone(true);
          // Небольшая пауза чтобы пользователь увидел checkmark
          setTimeout(() => {
            window.location.href = successRedirectTo;
          }, 800);
          return;
        }
      } catch {
        // тихо игнорируем — следующий tick попробует снова
      }
      if (attempts < maxAttempts) {
        pollTimerRef.current = window.setTimeout(tick, POLL_INTERVAL_MS);
      }
    };

    pollTimerRef.current = window.setTimeout(tick, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      if (pollTimerRef.current) clearTimeout(pollTimerRef.current);
    };
  }, [successRedirectTo, paymentFailed]);

  // Iframe load timeout — если не загрузился за 5 сек, показываем fallback
  useEffect(() => {
    loadTimerRef.current = window.setTimeout(() => {
      if (!iframeLoaded) setIframeBlocked(true);
    }, IFRAME_LOAD_TIMEOUT_MS);

    return () => {
      if (loadTimerRef.current) clearTimeout(loadTimerRef.current);
    };
  }, [iframeLoaded]);

  /**
   * Каждый раз когда iframe загружает новый URL — пытаемся прочитать его
   * location. Cross-origin (securepay.tinkoff.ru) кидает DOMException → ловим.
   * Same-origin (наш домен после редиректа на FailURL/SuccessURL) — читается.
   *
   * Если iframe ушёл на /billing/fail — показываем экран неуспеха внутри
   * модалки и закрываем по кнопке (без редиректа всей страницы).
   *
   * Если iframe ушёл на /billing/success — polling и так найдёт is_active,
   * но можно ускорить переход.
   */
  const handleIframeLoad = () => {
    setIframeLoaded(true);
    setIframeBlocked(false);
    if (loadTimerRef.current) clearTimeout(loadTimerRef.current);

    try {
      const path = iframeRef.current?.contentWindow?.location?.pathname;
      if (!path) return;
      if (path.includes('/billing/fail')) {
        setPaymentFailed(true);
      } else if (path.includes('/billing/success')) {
        // редкий случай: T-Bank редиректит весь iframe на success, мы успеваем
        // прочитать pathname. Polling всё равно подхватит is_active.
        setPaymentDone(true);
        setTimeout(() => {
          window.location.href = successRedirectTo;
        }, 800);
      }
    } catch {
      // Cross-origin — iframe ещё на стороне T-Bank, нормально.
    }
  };

  // Кнопка "Закрыть" на failed-state — ведёт на /billing/fail (или /pricing)
  const handleFailClose = () => {
    window.location.href = failRedirectTo;
  };

  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center px-4"
      style={{ backgroundColor: 'rgba(0, 0, 0, 0.7)' }}
      onClick={(e) => {
        // Клик по backdrop — закрываем (но не если payment в процессе финализации)
        if (e.target === e.currentTarget && !paymentDone) onCancel();
      }}
    >
      <div
        className="relative w-full max-w-2xl rounded-2xl overflow-hidden flex flex-col"
        style={{
          backgroundColor: 'var(--bg-primary)',
          border: '1.5px solid var(--border-color)',
          maxHeight: '92vh',
          height: '700px',
        }}
      >
        {/* Header */}
        <div
          className="flex items-center justify-between px-5 py-3 border-b flex-shrink-0"
          style={{ borderColor: 'var(--border-color)' }}
        >
          <div className="flex items-center gap-3">
            <span
              className="inline-flex items-center justify-center font-bold"
              style={{
                width: 28,
                height: 28,
                background: '#FFDD2D',
                color: '#111',
                borderRadius: 4,
                fontSize: 16,
              }}
            >
              Т
            </span>
            <div>
              <div
                className="font-semibold"
                style={{ color: 'var(--text-primary)', fontSize: 'var(--fs-sm)' }}
              >
                Оплата через Т-Банк
              </div>
              <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
                Защищённая форма банка-эквайера
              </div>
            </div>
          </div>
          <button
            onClick={onCancel}
            disabled={paymentDone}
            className="p-1.5 rounded-lg transition-all hover:bg-white/10 disabled:opacity-30"
            style={{ color: 'var(--text-muted)' }}
            aria-label="Закрыть"
          >
            <X size={20} />
          </button>
        </div>

        {/* Body — iframe или fallback */}
        <div className="relative flex-1 overflow-hidden">
          {/* Спиннер пока iframe грузится */}
          {!iframeLoaded && !iframeBlocked && (
            <div
              className="absolute inset-0 flex flex-col items-center justify-center gap-3"
              style={{ backgroundColor: 'var(--bg-primary)' }}
            >
              <Loader2
                size={28}
                className="animate-spin"
                style={{ color: 'var(--accent)' }}
              />
              <p className="text-sm" style={{ color: 'var(--text-muted)' }}>
                Загрузка платёжной формы…
              </p>
            </div>
          )}

          {/* Iframe — основная форма Т-Банка */}
          {!iframeBlocked && (
            <iframe
              ref={iframeRef}
              src={paymentUrl}
              title="Форма оплаты Т-Банка"
              onLoad={handleIframeLoad}
              className="w-full h-full"
              style={{ border: 'none', display: iframeLoaded ? 'block' : 'none' }}
              // sandbox-разрешения нужны для 3DS внутри iframe и для скриптов формы
              sandbox="allow-scripts allow-forms allow-same-origin allow-top-navigation allow-popups"
              allow="payment"
            />
          )}

          {/* Fallback — если iframe не загрузился (X-Frame-Options или adblock) */}
          {iframeBlocked && (
            <div className="absolute inset-0 flex flex-col items-center justify-center gap-4 px-6 text-center">
              <p
                className="text-base font-semibold"
                style={{ color: 'var(--text-primary)' }}
              >
                Не удалось загрузить форму оплаты на странице
              </p>
              <p
                className="text-sm max-w-md"
                style={{ color: 'var(--text-secondary)' }}
              >
                Возможно, расширение блокирует iframe.
                Откройте платёжную форму в новой вкладке — там оплата
                пройдёт штатно, а после возврата на сайт подписка
                активируется автоматически.
              </p>
              <a
                href={paymentUrl}
                target="_blank"
                rel="noreferrer noopener"
                className="inline-flex items-center gap-2 px-5 py-2.5 rounded-xl font-bold text-sm transition-opacity hover:opacity-90"
                style={{
                  backgroundColor: 'var(--accent)',
                  color: 'var(--text-inverse, #fff)',
                }}
              >
                Открыть форму оплаты
                <ExternalLink size={16} />
              </a>
            </div>
          )}

          {/* Success overlay — когда polling нашёл is_active */}
          {paymentDone && (
            <div
              className="absolute inset-0 flex flex-col items-center justify-center gap-3"
              style={{
                backgroundColor: 'var(--bg-primary)',
                zIndex: 10,
              }}
            >
              <div
                className="flex items-center justify-center"
                style={{
                  width: 64,
                  height: 64,
                  borderRadius: '50%',
                  background: 'color-mix(in srgb, var(--accent) 18%, transparent)',
                  color: 'var(--accent)',
                  fontSize: 32,
                  fontWeight: 700,
                }}
              >
                ✓
              </div>
              <p
                className="text-base font-semibold"
                style={{ color: 'var(--text-primary)' }}
              >
                Оплата прошла!
              </p>
              <p className="text-sm" style={{ color: 'var(--text-muted)' }}>
                Перенаправляем в личный кабинет…
              </p>
            </div>
          )}

          {/* Failed overlay — iframe внутри ушёл на /billing/fail */}
          {paymentFailed && !paymentDone && (
            <div
              className="absolute inset-0 flex flex-col items-center justify-center gap-3 px-6 text-center"
              style={{
                backgroundColor: 'var(--bg-primary)',
                zIndex: 10,
              }}
            >
              <XCircle
                size={56}
                style={{ color: 'var(--funds-flow-negative, #EF4444)' }}
              />
              <p
                className="text-base font-semibold"
                style={{ color: 'var(--text-primary)' }}
              >
                Оплата не прошла
              </p>
              <p
                className="text-sm max-w-sm"
                style={{ color: 'var(--text-secondary)' }}
              >
                Платёж был отменён или банк отказал. Деньги не списаны —
                можно попробовать ещё раз.
              </p>
              <button
                onClick={handleFailClose}
                className="mt-2 px-5 py-2 rounded-xl text-sm font-medium transition-opacity hover:opacity-90"
                style={{
                  backgroundColor: 'var(--accent)',
                  color: '#fff',
                }}
              >
                Понятно
              </button>
            </div>
          )}
        </div>

        {/* Footer — статус + ссылка на политику */}
        <div
          className="px-5 py-2 text-xs flex items-center justify-between flex-shrink-0 border-t"
          style={{
            borderColor: 'var(--border-color)',
            color: 'var(--text-muted)',
          }}
        >
          <span>
            🔒 SSL/TLS · PCI DSS · карта не сохраняется на нашем сервере
          </span>
          <a
            href="https://tbank.ru"
            target="_blank"
            rel="noreferrer noopener"
            style={{ color: 'var(--text-muted)' }}
            className="hover:underline"
          >
            tbank.ru
          </a>
        </div>
      </div>
    </div>
  );
}
