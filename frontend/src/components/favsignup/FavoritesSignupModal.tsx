/**
 * FavoritesSignupModal — «Не потеряйте своё избранное».
 *
 * Глобальное окно (смонтировано в App): ждёт событие FAV_SIGNUP_EVENT —
 * гость упёрся в лимит избранного (см. utils/favoritesSignupNudge) — и
 * предлагает бесплатную регистрацию. Десктоп — окно по центру,
 * телефон (<768px) — нижняя шторка. Сверху — зацикленная анимация
 * добавления актива в избранное (по образцу onboarding-окон TradingView),
 * с паузой. Закрытие: крестик, ESC, клик по фону.
 *
 * После регистрации/входа избранное из localStorage уезжает в аккаунт через
 * settingsSync (при входе в существующий аккаунт списки объединяются).
 * Цели Метрики: favsignup_shown / favsignup_register / favsignup_login /
 * favsignup_close.
 */
import { useCallback, useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useAuth } from '../../contexts/AuthContext';
import { useIsMobile } from '../../hooks/useIsMobile';
import { trackEvent } from '../../hooks/useYandexMetrica';
import { MODAL_LAYER_Z } from '../../utils/modalHost';
import { FAV_SIGNUP_EVENT } from '../../utils/favoritesSignupNudge';
import InstrumentIcon from '../InstrumentIcon';
import './favoritesSignup.css';

const STAR_POINTS = '12 2 15 9 22 9.3 16.5 14 18.5 21 12 17 5.5 21 7.5 14 2 9.3 9 9';

function Star({ filled, className }: { filled: boolean; className?: string }) {
  return (
    <svg
      className={className}
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill={filled ? '#FF5C2B' : 'none'}
      stroke={filled ? '#FF5C2B' : '#8C877D'}
      strokeWidth={filled ? 1.5 : 1.8}
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <polygon points={STAR_POINTS} />
    </svg>
  );
}

/** Настоящий логотип эмитента — тот же, что в поиске актива на сайте. */
function Logo({ ticker, size = 18 }: { ticker: string; size?: number }) {
  return <span className="fsm-logo"><InstrumentIcon sectype={ticker} size={size} rounded="full" /></span>;
}

function Row({ ticker, name, starred, last }: {
  ticker: string; name: string; starred: boolean; last?: boolean;
}) {
  return (
    <div className={`fsm-row${last ? ' fsm-row--last' : ''}`}>
      <Logo ticker={ticker} />
      <span className="fsm-ticker">{ticker}</span>
      <span className="fsm-name">{name}</span>
      <span className="fsm-star"><Star filled={starred} /></span>
    </div>
  );
}

function CheckIcon({ size }: { size: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="#1A7A43" strokeWidth="3"
      strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M4 12.5l5 5L20 6.5" />
    </svg>
  );
}

/** Зацикленная сцена: звёздочка у YDEX → «Избранные» → сохранено в аккаунте. */
function Preview({ sheet }: { sheet: boolean }) {
  const { t } = useTranslation();
  return (
    <div className="fsm-scene fsm-anim" aria-hidden="true">
      <div className="fsm-card">
        <div className="fsm-label">{t('ИЗБРАННЫЕ')}</div>
        <Row ticker="SBER" name={t('Сбербанк')} starred />
        <div className="fsm-row-in fsm-anim">
          <Row ticker="YDEX" name={t('Яндекс')} starred />
        </div>
        <div className="fsm-label fsm-label--second">{t('ОСТАЛЬНЫЕ')}</div>
        <Row ticker="GAZP" name={t('Газпром')} starred={false} />
        <div className="fsm-row-out fsm-anim">
          <div className="fsm-row">
            <Logo ticker="YDEX" />
            <span className="fsm-ticker">YDEX</span>
            <span className="fsm-name">{t('Яндекс')}</span>
            <span className="fsm-star">
              <Star filled={false} />
              <Star filled className="fsm-star-fill fsm-anim" />
            </span>
          </div>
        </div>
        <Row ticker="LKOH" name={t('Лукойл')} starred={false} last />

        <div className="fsm-ring fsm-anim" />
        {sheet ? (
          <div className="fsm-tap fsm-anim" />
        ) : (
          <svg className="fsm-cursor fsm-anim" width="20" height="24" viewBox="0 0 20 24">
            <path d="M3 2 L3 19 L7.5 15 L10.5 22 L13.5 20.7 L10.6 13.8 L16.5 13.6 Z" fill="#0A0A0A"
              stroke="#FFFFFF" strokeWidth="1.6" strokeLinejoin="round" />
          </svg>
        )}
      </div>

      {sheet ? (
        <div className="fsm-chip-wrap">
          <span className="fsm-saved fsm-anim"><CheckIcon size={12} />{t('Сохранено в аккаунте')}</span>
        </div>
      ) : (
        <div className="fsm-phone fsm-anim">
          <div className="fsm-phone-notch" />
          <div className="fsm-phone-title">{t('Избранное')}</div>
          <div className="fsm-row">
            <Logo ticker="SBER" size={16} />
            <span className="fsm-ticker">SBER</span>
          </div>
          <div className="fsm-phone-new fsm-anim">
            <div className="fsm-row">
              <Logo ticker="YDEX" size={16} />
              <span className="fsm-ticker">YDEX</span>
            </div>
          </div>
          <span className="fsm-saved fsm-anim"><CheckIcon size={11} />{t('Сохранено в аккаунте')}</span>
        </div>
      )}
    </div>
  );
}

export default function FavoritesSignupModal() {
  const { t } = useTranslation();
  const { isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const sheet = useIsMobile();
  // Страница, на которой окно открыто (null — закрыто). Ушёл на другую
  // страницу или вошёл в аккаунт — окно гаснет само, без эффектов-сбросов.
  const [openPath, setOpenPath] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const open = openPath !== null && openPath === location.pathname && !isAuthenticated;

  useEffect(() => {
    const onNudge = () => {
      if (isAuthenticated) return;
      setPaused(false);
      setOpenPath(window.location.pathname);
      trackEvent('favsignup_shown', { device: window.innerWidth < 768 ? 'mobile' : 'desktop' });
    };
    window.addEventListener(FAV_SIGNUP_EVENT, onNudge);
    return () => window.removeEventListener(FAV_SIGNUP_EVENT, onNudge);
  }, [isAuthenticated]);

  const close = useCallback(() => {
    setOpenPath(null);
    trackEvent('favsignup_close');
  }, []);

  useEffect(() => {
    if (!open) return;
    // Логотипы в сцене — lazy <img> (TickerLogo), часть строк в начале цикла
    // свёрнута в 0 высоты и без прогрева грузилась бы рывком посреди анимации.
    for (const t of ['SBER', 'YDEX', 'GAZP', 'LKOH']) new Image().src = `/logos/sm/${t}.png`;
  }, [open]);

  useEffect(() => {
    if (!open) return;
    // Окно обычно открыто ПОВЕРХ поиска актива: ESC ловим на фазе захвата и
    // гасим дальше, чтобы закрылось только оно, а не оба окна сразу.
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      e.stopPropagation();
      close();
    };
    window.addEventListener('keydown', onKey, true);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      window.removeEventListener('keydown', onKey, true);
      document.body.style.overflow = prevOverflow;
    };
  }, [open, close]);

  if (!open) return null;

  const next = encodeURIComponent(location.pathname + location.search);
  const goRegister = () => {
    trackEvent('favsignup_register');
    setOpenPath(null);
    navigate(`/login?mode=register&next=${next}`);
  };
  const goLogin = () => {
    trackEvent('favsignup_login');
    setOpenPath(null);
    navigate(`/login?next=${next}`);
  };

  return createPortal(
    <div
      className={`fsm-backdrop${sheet ? ' fsm-backdrop--sheet' : ''}`}
      style={{ zIndex: MODAL_LAYER_Z + 10 }}
      onClick={close}
    >
      <div
        className="fsm-panel"
        role="dialog"
        aria-modal="true"
        aria-labelledby="fsm-title"
        onClick={(e) => e.stopPropagation()}
      >
        {sheet && <div className="fsm-handle" />}
        <div className={`fsm-stage${paused ? ' is-paused' : ''}`}>
          <Preview sheet={sheet} />
          <button type="button" className="fsm-icon-btn fsm-close" onClick={close} aria-label={t('Закрыть')}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.8"
              strokeLinecap="round" aria-hidden="true"><path d="M5 5l14 14M19 5L5 19" /></svg>
          </button>
          <button
            type="button"
            className="fsm-icon-btn fsm-pause"
            onClick={() => setPaused((p) => !p)}
            aria-label={paused ? t('Воспроизвести анимацию') : t('Остановить анимацию')}
          >
            {paused ? (
              <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M7 4.5v15l12-7.5z" /></svg>
            ) : (
              <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                <rect x="5" y="4" width="4.5" height="16" rx="1" /><rect x="14.5" y="4" width="4.5" height="16" rx="1" />
              </svg>
            )}
          </button>
        </div>

        <div className="fsm-body">
          <h2 id="fsm-title" className="fsm-title">{t('Не потеряйте своё избранное')}</h2>
          <p className="fsm-text">
            {sheet
              ? t('Без аккаунта в избранное можно добавить только 2 актива, и список хранится лишь в этом браузере — он пропадёт, если очистить данные сайта или сменить телефон. Зарегистрируйтесь: избранное станет без ограничений и сохранится в вашем аккаунте.')
              : t('Без аккаунта в избранное можно добавить только 2 актива, и список хранится лишь в этом браузере — он пропадёт, если очистить данные сайта или зайти с другого устройства. Зарегистрируйтесь: избранное станет без ограничений и сохранится в вашем аккаунте.')}
          </p>
        </div>
        <div className="fsm-actions">
          <button type="button" className="fsm-cta" onClick={goRegister}>{t('Зарегистрироваться бесплатно')}</button>
          <span className="fsm-login">
            {t('Уже есть аккаунт?')}{' '}
            <button type="button" onClick={goLogin}>{t('Войти')}</button>
          </span>
        </div>
      </div>
    </div>,
    document.body,
  );
}
