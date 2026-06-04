/**
 * EmbedPage — chromeless обёртка для встраивания индикаторов FRAME в iframe.
 *
 * Назначение: расширение для терминала Т-Инвестиций вставляет
 *   <iframe src="таймфрейм.рф/embed/oi?instrument=SR&theme=editorial-dark&token=...">
 * Работает и как самостоятельная shareable-ссылка на «голый» график.
 *
 * URL: /embed/:indicator?instrument=SR&theme=editorial-dark&token=...&clgroup=FIZ&period=6m
 *
 * - Без Layout/nav/footer — заполняет весь iframe (100vw×100vh), резиновый.
 * - Тема форсится из ?theme= (как /signal-export). Не восстанавливаем — embed
 *   живёт в своём iframe-документе, чужой темы тут нет.
 * - ?token= — короткоживущий embed-токен (валидация в Phase auth; сейчас стаб).
 * - :indicator выбирает конкретный embed-компонент.
 *
 * НЕ в навигации сайта. План: .claude/TERMINAL_EXTENSION_PLAN.md
 */
import { useEffect } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import { useTheme, type ThemeId } from '../../contexts/ThemeContext';
import EmbedOpenInterest from './EmbedOpenInterest';
import EmbedBuffett from './EmbedBuffett';
import EmbedCbrFlows from './EmbedCbrFlows';
import EmbedSeasonality from './EmbedSeasonality';
import EmbedFundsMoney from './EmbedFundsMoney';
import EmbedStrength from './EmbedStrength';
import EmbedFundTrades from './EmbedFundTrades';

const DEFAULT_THEME = 'editorial-dark';

export default function EmbedPage() {
  const { indicator } = useParams<{ indicator: string }>();
  const [params] = useSearchParams();
  const theme = params.get('theme') || DEFAULT_THEME;
  const { setTheme } = useTheme();

  // Синхронизируем тему контекста (не только data-theme) — bespoke-компоненты
  // читают тему через useTheme() (напр. getCategoryColor в Потоках ЦБ).
  useEffect(() => {
    if (theme === 'editorial-light' || theme === 'editorial-dark') {
      setTheme(theme as ThemeId);
    }
  }, [theme, setTheme]);

  // TODO(auth): валидировать params.get('token') — embed только для платных
  // подписчиков FRAME. Пока стаб: рендерим без проверки. Подключим в Phase auth
  // вместе с popup-логином расширения.

  return (
    <div
      data-embed-root={indicator || 'unknown'}
      style={{
        width: '100vw',
        height: '100vh',
        background: 'var(--bg-base)',
        color: 'var(--text-primary)',
        overflow: 'hidden',
        fontFamily:
          '-apple-system, BlinkMacSystemFont, "Segoe UI", Inter, system-ui, sans-serif',
        boxSizing: 'border-box',
      }}
    >
      {renderIndicator(indicator)}
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
    // Остальные подключаются по мере готовности embed-компонентов.
    default:
      return <EmbedPlaceholder name={indicator} />;
  }
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
      <div style={{ fontSize: 15, fontWeight: 600 }}>
        Индикатор {name ? `«${name}»` : ''} скоро
      </div>
      <div style={{ fontSize: 12, opacity: 0.7 }}>
        embed-компонент в разработке
      </div>
    </div>
  );
}
