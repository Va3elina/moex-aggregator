/**
 * BillingSbpPage — оплата подписки через СБП по QR-коду.
 *
 * Приходим сюда из ConsentModal (PricingPage) после выбора «СБП — QR-код».
 * payment_id / qr_image / qr_payload передаются через location.state — qr_image
 * это крупный data-URL (картинка QR), в query его не положишь.
 *
 * Серверный QR обходит исходную проблему серта: оплата завершается в приложении
 * банка по СБП напрямую, браузер не ходит на недоверенную платёжку T-Bank.
 *
 * Поллинг статуса — как в BillingSuccessPage:
 *  1. POST /api/billing/sync (GetState у T-Bank) — страховка от задержки webhook.
 *  2. polling /api/billing/status каждые 2с (СБП руками дольше карты → до 2 мин).
 *  3. при is_active → /billing/success (там общий success-UI + воронка).
 *  4. таймаут → кнопка «Проверить статус».
 */
import { useCallback, useEffect, useState } from 'react';
import { Link, useNavigate, useLocation } from 'react-router-dom';
import { Clock, AlertCircle, RotateCw, Smartphone } from 'lucide-react';
import { apiFetch } from '../services/api';

interface SbpNavState {
  payment_id?: string;
  qr_image?: string | null;
  qr_payload?: string | null;
  plan_id?: string;
}

interface Status {
  tier: string;
  is_active: boolean;
}

export default function BillingSbpPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const sbp = (location.state || {}) as SbpNavState;
  const [state, setState] = useState<'await' | 'timeout'>('await');
  const [syncing, setSyncing] = useState(false);

  /** Принудительный sync через GetState у провайдера. */
  const runSync = useCallback(async (): Promise<Status | null> => {
    try {
      const r = await apiFetch('/api/billing/sync', { method: 'POST' });
      if (!r.ok) return null;
      return (await r.json()) as Status;
    } catch {
      return null;
    }
  }, []);

  /** Лёгкий polling статуса. */
  const fetchStatus = useCallback(async (): Promise<Status | null> => {
    try {
      const r = await apiFetch('/api/billing/status');
      if (!r.ok) return null;
      return (await r.json()) as Status;
    } catch {
      return null;
    }
  }, []);

  useEffect(() => {
    if (!sbp.qr_image) return; // нет QR (refresh/прямой заход) — поллить нечего
    let cancelled = false;
    let attempts = 0;
    const maxAttempts = 60; // СБП-оплата вручную дольше карты: 60 × 2с = 2 мин

    const done = () => {
      if (!cancelled) navigate('/billing/success');
    };

    (async () => {
      const synced = await runSync();
      if (cancelled) return;
      if (synced?.is_active) return done();

      const tick = async () => {
        if (cancelled) return;
        const s = await fetchStatus();
        if (s?.is_active) return done();
        attempts++;
        if (attempts >= maxAttempts) setState('timeout');
        else setTimeout(tick, 2000);
      };
      setTimeout(tick, 2000);
    })();

    return () => {
      cancelled = true;
    };
  }, [sbp.qr_image, runSync, fetchStatus, navigate]);

  const handleManualSync = async () => {
    setSyncing(true);
    const s = await runSync();
    setSyncing(false);
    if (s?.is_active) navigate('/billing/success');
    else setState('await'); // продолжаем ждать
  };

  // QR живёт только в navigation state → при обновлении страницы он теряется.
  if (!sbp.qr_image) {
    return (
      <div className="max-w-xl mx-auto px-6 py-12 text-center">
        <AlertCircle className="w-16 h-16 mx-auto mb-4 text-amber-400" />
        <h1 className="text-2xl font-bold text-theme-primary mb-2">QR-код недоступен</h1>
        <p className="text-theme-secondary mb-6">
          Похоже, страница была обновлена. Вернись к тарифам и сгенерируй новый QR.
        </p>
        <Link
          to="/pricing"
          className="px-5 py-3 rounded-xl text-sm font-medium"
          style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
        >
          К тарифам
        </Link>
      </div>
    );
  }

  return (
    <div className="max-w-xl mx-auto px-6 py-12 text-center">
      <h1 className="text-2xl font-bold text-theme-primary mb-2">Оплата через СБП</h1>
      <p className="text-theme-secondary mb-6">
        Отсканируй QR-код камерой или приложением банка. На телефоне — нажми кнопку ниже.
      </p>

      {/* QR-код (T-Bank GetQr DataType=IMAGE → data-URL). Фон белый для контраста. */}
      <div className="inline-block p-4 rounded-2xl bg-white mb-4">
        <img
          src={sbp.qr_image}
          alt="СБП QR-код для оплаты"
          width={240}
          height={240}
          style={{ display: 'block', width: 240, height: 240 }}
        />
      </div>

      {/* Мобильный диплинк — СБП-ссылка (qr.nspk.ru/...) открывает приложение банка. */}
      {sbp.qr_payload && (
        <div className="mb-6">
          <a
            href={sbp.qr_payload}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-5 py-3 rounded-xl text-sm font-medium"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            <Smartphone size={16} /> Открыть приложение банка
          </a>
        </div>
      )}

      {/* Статус ожидания / таймаут */}
      {state === 'await' ? (
        <p className="inline-flex items-center gap-2 text-theme-secondary text-sm">
          <Clock size={16} className="animate-pulse" /> Ждём подтверждение оплаты…
        </p>
      ) : (
        <div>
          <p className="text-theme-secondary text-sm mb-3">
            Если ты уже оплатил, но статус не обновился — проверь вручную.
          </p>
          <button
            onClick={handleManualSync}
            disabled={syncing}
            className="inline-flex items-center gap-2 px-5 py-3 rounded-xl text-sm font-medium disabled:opacity-50"
            style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
          >
            <RotateCw size={16} className={syncing ? 'animate-spin' : ''} />
            {syncing ? 'Проверяем…' : 'Проверить статус'}
          </button>
        </div>
      )}

      <p className="mt-8 text-xs" style={{ color: 'var(--text-muted)' }}>
        Подписка продлевается автоматически по СБП. Отменить можно в профиле.
      </p>
    </div>
  );
}
