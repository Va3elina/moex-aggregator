/**
 * BillingSbpPage — привязка счёта по СБП и первое списание.
 *
 * Порядок перевёрнут относительно старого QR-флоу: сначала юзер подтверждает
 * в банке ПРИВЯЗКУ СЧЁТА (денег не двигается), и только потом мы сами делаем
 * первое списание через ChargeQr. Так привязка становится главным действием
 * экрана, а не необязательным постскриптумом после оплаты — по правилам НСПК
 * привязать счёт молча, по ходу платежа, нельзя.
 *
 * Приходим сюда из ConsentModal (PricingPage) с subscription_id + payload
 * (ссылка sub.nspk.ru) в location.state.
 *
 * Состояния экрана:
 *   waiting  — ждём подтверждения в банке (поллим GET /sbp/bind/{id})
 *   charging — привязка получена, идёт первое списание
 *   failed   — привязали, но списать не смогли (чаще всего нет денег на счёте)
 *   expired  — заявка протухла или банк отклонил привязку
 * Успех → /billing/success (общий success-UI).
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { Link, useNavigate, useLocation } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { AlertCircle, Clock, Loader2, Smartphone } from 'lucide-react';
import { apiFetch } from '../services/api';

interface SbpNavState {
  subscription_id?: number;
  // payload — либо ссылка sub.nspk.ru (data_type='PAYLOAD', телефон), либо
  // base64-картинка QR от банка (data_type='IMAGE', десктоп).
  payload?: string | null;
  data_type?: 'PAYLOAD' | 'IMAGE';
  plan_id?: string;
  amount?: number;
}

interface BindState {
  status: 'waiting' | 'charged' | 'failed' | 'expired' | string;
  message?: string;
  failure_kind?: string;
}

export default function BillingSbpPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const location = useLocation();
  const nav = (location.state || {}) as SbpNavState;

  const [phase, setPhase] = useState<'waiting' | 'charging' | 'failed' | 'expired'>('waiting');
  const [message, setMessage] = useState<string | null>(null);
  // Гварда от повторного входа в поллинг при ре-рендере (ручка не идемпотентна:
  // именно она проводит первое списание).
  const pollingRef = useRef(false);

  // Картинку QR рисует банк (data_type='IMAGE'), мы её только показываем.
  // ⚠️ AddAccountQr отдаёт НЕ base64, а готовую SVG-разметку («<svg …»), в
  // отличие от GetQr, который присылает base64-PNG. Оборачиваем в data-URL
  // через encodeURIComponent: btoa на кириллицу в Description падает.
  const isImage = nav.data_type === 'IMAGE';
  const qrSrc = (() => {
    const raw = nav.payload || '';
    if (!isImage || !raw) return null;
    if (raw.startsWith('data:')) return raw;
    if (raw.trimStart().startsWith('<svg')) {
      return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(raw)}`;
    }
    return `data:image/png;base64,${raw}`;
  })();

  const poll = useCallback(async (): Promise<BindState | null> => {
    try {
      const r = await apiFetch(`/api/billing/sbp/bind/${nav.subscription_id}`);
      if (!r.ok) return null;
      return (await r.json()) as BindState;
    } catch {
      return null;
    }
  }, [nav.subscription_id]);

  useEffect(() => {
    if (!nav.subscription_id || pollingRef.current) return;
    pollingRef.current = true;
    let cancelled = false;
    let attempts = 0;
    const maxAttempts = 90; // 90 × 2с = 3 мин: подтверждение в банке руками

    const tick = async () => {
      if (cancelled) return;
      const s = await poll();
      if (cancelled) return;

      if (s?.status === 'charged') {
        navigate('/billing/success');
        return;
      }
      if (s?.status === 'failed') {
        setMessage(s.message || null);
        setPhase('failed');
        return;
      }
      if (s?.status === 'expired') {
        setPhase('expired');
        return;
      }

      attempts++;
      if (attempts >= maxAttempts) {
        setPhase('expired');
        return;
      }
      setTimeout(tick, 2000);
    };
    setTimeout(tick, 2000);

    return () => {
      cancelled = true;
    };
  }, [nav.subscription_id, poll, navigate]);

  // Заявка живёт только в navigation state → при обновлении страницы теряется.
  if (!nav.subscription_id || !nav.payload) {
    return (
      <div className="max-w-xl mx-auto px-6 py-12 text-center">
        <AlertCircle className="w-16 h-16 mx-auto mb-4 text-amber-400" />
        <h1 className="text-2xl font-bold text-theme-primary mb-2">
          {t('Привязка недоступна')}
        </h1>
        <p className="text-theme-secondary mb-6">
          {t('Похоже, страница была обновлена. Вернись к тарифам и начни заново.')}
        </p>
        <Link
          to="/pricing"
          className="px-5 py-3 rounded-xl text-sm font-medium"
          style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
        >
          {t('К тарифам')}
        </Link>
      </div>
    );
  }

  return (
    <div className="max-w-xl mx-auto px-6 py-12 text-center">
      <h1 className="text-2xl font-bold text-theme-primary mb-2">
        {t('Оформление подписки')}
      </h1>
      <p className="text-theme-secondary mb-6">
        {isImage
          ? t('Отсканируйте код камерой телефона и нажмите «Привязать» для оформления подписки.')
          : t('Откройте приложение банка и нажмите «Привязать» для оформления подписки.')}
      </p>

      {/* Оборачивающий flex: раньше QR был inline-block и вставал в строку
          со статусом ожидания — код уезжал влево, статус прилипал справа. */}
      {qrSrc && (
        <div className="flex justify-center mb-4">
          <div className="inline-block p-4 rounded-2xl bg-white">
          <img
            src={qrSrc}
            alt={t('QR-код для привязки счёта')}
            width={240}
            height={240}
            style={{ display: 'block', width: 240, height: 240 }}
            />
          </div>
        </div>
      )}

      {!isImage && nav.payload && (
        <div className="mb-6">
          <a
            href={nav.payload}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-5 py-3 rounded-xl text-sm font-medium"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            <Smartphone size={16} /> {t('Открыть приложение банка')}
          </a>
        </div>
      )}

      {phase === 'waiting' && (
        <p className="flex items-center justify-center gap-2 text-theme-secondary text-sm">
          <Clock size={16} className="animate-pulse" /> {t('Ждём подтверждение в банке…')}
        </p>
      )}

      {phase === 'charging' && (
        <p className="flex items-center justify-center gap-2 text-theme-secondary text-sm">
          <Loader2 size={16} className="animate-spin" /> {t('Счёт привязан, списываем оплату…')}
        </p>
      )}

      {phase === 'failed' && (
        <div>
          <p className="text-theme-secondary text-sm mb-3">
            {message || t('Счёт привязан, но списание не прошло.')}
          </p>
          <Link
            to="/pricing"
            className="inline-flex items-center gap-2 px-5 py-3 rounded-xl text-sm font-medium"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            {t('Попробовать ещё раз')}
          </Link>
        </div>
      )}

      {phase === 'expired' && (
        <div>
          <p className="text-theme-secondary text-sm mb-3">
            {t('Привязка не подтверждена. Начните заново — ссылка действует ограниченное время.')}
          </p>
          <Link
            to="/pricing"
            className="inline-flex items-center gap-2 px-5 py-3 rounded-xl text-sm font-medium"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            {t('К тарифам')}
          </Link>
        </div>
      )}

      {/* Ссылка на договор. Акцепт берётся раньше — галкой в модалке согласия,
          но привязка счёта это момент, когда человек даёт согласие на будущие
          списания, и условия должны быть под рукой именно здесь. */}
      <p className="mt-8 text-xs" style={{ color: 'var(--text-muted)' }}>
        {t('После привязки подписка продлевается автоматически по СБП. Отвязать счёт можно в профиле.')}
        {' '}
        <Link
          to="/recurring"
          target="_blank"
          style={{ color: 'var(--accent)', textDecoration: 'underline' }}
        >
          {t('Договор о рекуррентных платежах')}
        </Link>
      </p>
    </div>
  );
}
