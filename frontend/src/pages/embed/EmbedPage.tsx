/**
 * EmbedPage — chromeless обёртка для встраивания индикаторов FRAME в iframe.
 *
 * Назначение: расширение для терминала Т-Инвестиций вставляет
 *   <iframe src="таймфрейм.рф/embed/oi?instrument=SR&theme=editorial-dark&token=ext_…">
 *
 * Гейтинг (только PRO): ?token=ext_… обменивается на короткий JWT через
 * /api/extension/exchange (PRO проверяется вживую). JWT кладётся в localStorage,
 * дальше все embed-компоненты работают штатным apiFetch. Нет/невалиден токен → замок.
 * Таймер переобмена держит JWT свежим (refresh у embed нет).
 *
 * - Без Layout/nav/footer — заполняет весь iframe (100vw×100vh), резиновый.
 * - Тема форсится из ?theme=.
 *
 * НЕ в навигации сайта. План: .claude/TERMINAL_EXTENSION_PLAN.md
 */
import { useEffect, useState } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import { useTheme, type ThemeId } from '../../contexts/ThemeContext';
import { exchangeExtensionToken } from '../../services/api';
import EmbedOpenInterest from './EmbedOpenInterest';
import EmbedBuffett from './EmbedBuffett';
import EmbedCbrFlows from './EmbedCbrFlows';
import EmbedSeasonality from './EmbedSeasonality';
import EmbedFundsMoney from './EmbedFundsMoney';
import EmbedStrength from './EmbedStrength';
import EmbedFundTrades from './EmbedFundTrades';

const DEFAULT_THEME = 'editorial-dark';
const SITE = 'https://xn--80aklbnczmv.xn--p1ai'; // таймфрейм.рф

type AuthState = 'loading' | 'ok' | 'locked';

export default function EmbedPage() {
  const { indicator } = useParams<{ indicator: string }>();
  const [params] = useSearchParams();
  const theme = params.get('theme') || DEFAULT_THEME;
  const token = params.get('token');
  const { setTheme } = useTheme();
  const [auth, setAuth] = useState<AuthState>('loading');

  // Синхронизируем тему контекста (bespoke-компоненты читают её через useTheme()).
  useEffect(() => {
    if (theme === 'editorial-light' || theme === 'editorial-dark') {
      setTheme(theme as ThemeId);
    }
  }, [theme, setTheme]);

  // Гейт: обмен ext-токена на JWT. Нет/невалиден → замок.
  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;
    if (!token) {
      setAuth('locked');
      return;
    }
    setAuth('loading');
    const run = async (t: string) => {
      try {
        const res = await exchangeExtensionToken(t);
        if (cancelled) return;
        try { localStorage.setItem('access_token', res.access_token); } catch { /* partitioned/quota */ }
        setAuth('ok');
        // Переобмен за 2 мин до истечения — держим JWT свежим без refresh.
        timer = window.setTimeout(() => run(t), Math.max(60, res.expires_in - 120) * 1000);
      } catch {
        if (cancelled) return;
        try { localStorage.removeItem('access_token'); } catch { /* ignore */ }
        setAuth('locked');
      }
    };
    run(token);
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [token]);

  return (
    <div
      data-embed-root={indicator || 'unknown'}
      data-embed-auth={auth}
      style={{
        width: '100vw',
        height: '100vh',
        background: 'var(--bg-base)',
        color: 'var(--text-primary)',
        overflow: 'hidden',
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Inter, system-ui, sans-serif',
        boxSizing: 'border-box',
      }}
    >
      {auth === 'loading' && <EmbedCenter text="Загрузка…" />}
      {auth === 'locked' && <EmbedLocked />}
      {auth === 'ok' && renderIndicator(indicator)}
    </div>
  );
}

function renderIndicator(indicator: string | undefined) {
  switch (indicator) {
    case 'oi':
      return <EmbedOpenInterest />;
    case 'buffett':
      return <EmbedBuffett />;
    case 'cbr-flows':
      return <EmbedCbrFlows />;
    case 'seasonality':
      return <EmbedSeasonality />;
    case 'funds-money':
      return <EmbedFundsMoney />;
    case 'strength':
      return <EmbedStrength />;
    case 'fund-trades':
      return <EmbedFundTrades />;
    default:
      return <EmbedPlaceholder name={indicator} />;
  }
}

function EmbedCenter({ text }: { text: string }) {
  return (
    <div
      style={{
        width: '100%',
        height: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: 'var(--text-secondary)',
        fontSize: 14,
      }}
    >
      {text}
    </div>
  );
}

function EmbedLocked() {
  return (
    <div
      style={{
        width: '100%',
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: 10,
        textAlign: 'center',
        padding: 24,
        boxSizing: 'border-box',
      }}
    >
      <div style={{ fontSize: 30, lineHeight: 1 }}>🔒</div>
      <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--text-primary)' }}>
        Индикаторы под замком
      </div>
      <div style={{ fontSize: 12.5, color: 'var(--text-secondary)', maxWidth: 300, lineHeight: 1.5 }}>
        Вставьте PRO-токен в popup расширения «Фрейм». Сгенерировать токен можно в личном
        кабинете на таймфрейм.рф — нужен тариф&nbsp;Pro.
      </div>
      <a
        href={`${SITE}/profile`}
        target="_blank"
        rel="noopener noreferrer"
        style={{
          marginTop: 4,
          padding: '7px 14px',
          background: 'var(--accent, #FF5C2B)',
          color: '#fff',
          fontWeight: 700,
          fontSize: 12.5,
          borderRadius: 6,
          textDecoration: 'none',
        }}
      >
        Открыть личный кабинет
      </a>
    </div>
  );
}

function EmbedPlaceholder({ name }: { name?: string }) {
  return (
    <div
      style={{
        width: '100%',
        height: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexDirection: 'column',
        gap: 6,
        color: 'var(--text-secondary)',
        textAlign: 'center',
        padding: 24,
      }}
    >
      <div style={{ fontSize: 15, fontWeight: 600 }}>Индикатор {name ? `«${name}»` : ''} скоро</div>
      <div style={{ fontSize: 12, opacity: 0.7 }}>embed-компонент в разработке</div>
    </div>
  );
}
