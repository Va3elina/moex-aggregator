import { Component, type ReactNode } from 'react';
import { isChunkLoadError, reloadOnceForChunk } from '../utils/chunkReload';

interface Props {
  children: ReactNode;
  /** При смене этого ключа ErrorBoundary сбросится в чистое состояние.
      Передавай location.pathname → на каждой смене URL boundary само-восстановится,
      даже если предыдущая страница вылетела с ошибкой. */
  resetKey?: string;
}

interface State {
  hasError: boolean;
  error: Error | null;
  /** Сбой загрузки code-split чанка (устаревший бандл после деплоя). Такой случай
      лечим тихой перезагрузкой, а не экраном «Что-то пошло не так». */
  isChunkError: boolean;
}

export default class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null, isChunkError: false };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error, isChunkError: isChunkLoadError(error) };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error('ErrorBoundary caught:', error, info.componentStack);
    // Устаревший чанк после деплоя — пробуем разово перезагрузиться на свежий
    // билд вместо показа ошибки. Если guard не дал (недавно уже перезагружались,
    // а чанк всё равно битый → деплой реально сломан) — снимаем флаг, чтобы
    // отрисовать обычный экран ошибки, а не висеть на пустом placeholder'е.
    if (this.state.isChunkError && !reloadOnceForChunk()) {
      this.setState({ isChunkError: false });
    }
  }

  componentDidUpdate(prevProps: Props) {
    // Сбрасываем error state когда меняется resetKey (обычно — pathname).
    // Без этого: страница вылетает → ErrorBoundary показывает fallback навсегда,
    // даже если пользователь кликает другие nav-ссылки. URL меняется, но boundary
    // продолжает отрисовывать "Что-то пошло не так".
    if (this.state.hasError && prevProps.resetKey !== this.props.resetKey) {
      this.setState({ hasError: false, error: null, isChunkError: false });
    }
  }

  render() {
    if (this.state.hasError) {
      // Сбой загрузки чанка → тихий placeholder (как Suspense fallback): сейчас
      // инициируется перезагрузка на свежий билд, незачем мигать экраном ошибки.
      if (this.state.isChunkError) {
        return <div style={{ minHeight: '100vh', background: 'var(--bg-primary)' }} />;
      }
      return (
        <div style={{
          display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
          minHeight: '60vh', padding: '2rem', color: '#9CA3B8',
        }}>
          <div style={{ fontSize: '3rem', marginBottom: '1rem' }}>&#9888;&#65039;</div>
          <h2 style={{ color: '#E2E8F0', marginBottom: '0.5rem', fontSize: '1.25rem' }}>
            Что-то пошло не так
          </h2>
          <p style={{ marginBottom: '1.5rem', textAlign: 'center', maxWidth: '400px', fontSize: '0.875rem' }}>
            Произошла ошибка при отображении страницы. Попробуйте перезагрузить.
          </p>
          <button
            onClick={() => window.location.reload()}
            style={{
              padding: '0.5rem 1.5rem', borderRadius: '0.75rem',
              backgroundColor: '#2EE59D', color: '#131722',
              border: 'none', cursor: 'pointer', fontWeight: 600, fontSize: '0.875rem',
            }}
          >
            Перезагрузить
          </button>
          {this.state.error && (
            <details style={{ marginTop: '1.5rem', fontSize: '0.75rem', color: '#6B7280', maxWidth: '600px' }}>
              <summary style={{ cursor: 'pointer' }}>Детали ошибки</summary>
              <pre style={{ marginTop: '0.5rem', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                {this.state.error.message}
              </pre>
            </details>
          )}
        </div>
      );
    }

    return this.props.children;
  }
}
