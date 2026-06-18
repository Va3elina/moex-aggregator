import { Component, type ReactNode } from 'react';

// Сообщения сбоя динамического импорта различаются по браузерам:
//   Chrome/Vite — "Failed to fetch dynamically imported module: <url>"
//   Firefox     — "error loading dynamically imported module: <url>"
//   Safari      — "Importing a module script failed."
// Любое из них = устаревшая вкладка после деплоя: hashed-чанк, который помнит
// старый бандл, удалён новым билдом и отдаёт 404.
const CHUNK_ERROR_RE =
  /failed to fetch dynamically imported module|error loading dynamically imported module|importing a module script failed/i;

function isChunkLoadError(error: unknown): boolean {
  return error instanceof Error && CHUNK_ERROR_RE.test(error.message);
}

// Guard одноразового жёсткого восстановления. ОТДЕЛЬНЫЙ от 'frame:chunk-reload'
// в main.tsx: тот guard расходуется обычным reload по vite:preloadError, и если
// бы мы проверяли его же — наша более глубокая чистка кэшей никогда бы не
// запустилась (main.tsx уже «съел» бы попытку). Свой ключ → ErrorBoundary
// получает ровно одну собственную попытку восстановления.
const RECOVER_GUARD = 'frame:chunk-recover';

function recoverGuardSet(): boolean {
  try { return !!sessionStorage.getItem(RECOVER_GUARD); } catch { return false; /* storage отключён */ }
}
function setRecoverGuard() {
  try { sessionStorage.setItem(RECOVER_GUARD, '1'); } catch { /* private mode / storage отключён */ }
}
function clearRecoverGuard() {
  try { sessionStorage.removeItem(RECOVER_GUARD); } catch { /* storage отключён */ }
}

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
  /** Распознали stale-chunk и идём на жёсткое восстановление — показываем
      нейтральный «обновляем», а не «что-то пошло не так», т.к. сейчас перезагрузимся. */
  recovering: boolean;
}

export default class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null, recovering: false };
  }

  static getDerivedStateFromError(error: Error): State {
    // recovering выводим здесь (чистая деривация по сообщению + чтение guard),
    // чтобы НЕ мелькнул экран «что-то пошло не так» перед reload. Сам reload и
    // чистка кэшей — побочные эффекты — живут в componentDidCatch.
    const recovering = isChunkLoadError(error) && !recoverGuardSet();
    return { hasError: true, error, recovering };
  }

  componentDidMount() {
    // Приложение загрузилось без ошибки → прошлое восстановление (если было)
    // удалось. Снимаем guard, чтобы СЛЕДУЮЩИЙ деплой в этой же долгоживущей
    // вкладке тоже мог разово восстановиться. Зеркалит 10-сек guard в main.tsx.
    if (!this.state.hasError) {
      setTimeout(clearRecoverGuard, 10_000);
    }
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error('ErrorBoundary caught:', error, info.componentStack);

    // Последняя линия обороны для stale-chunk. Обработчик vite:preloadError в
    // main.tsx одноразовый (10-сек guard) и иногда не успевает выстрелить до
    // того, как React.lazy пробрасывает reject в рендер — тогда ошибка долетает
    // сюда. До этого фикса fallback был тупиком: обычный reload крутил ту же
    // ошибку (SW мог отдавать закэшированный index.html/asset). Теперь разово
    // сносим кэши SW и перезагружаемся со свежим манифестом чанков.
    if (isChunkLoadError(error) && !recoverGuardSet()) {
      setRecoverGuard();
      this.hardRecover();
    }
  }

  componentDidUpdate(prevProps: Props) {
    // Сбрасываем error state когда меняется resetKey (обычно — pathname).
    // Без этого: страница вылетает → ErrorBoundary показывает fallback навсегда,
    // даже если пользователь кликает другие nav-ссылки. URL меняется, но boundary
    // продолжает отрисовывать "Что-то пошло не так".
    if (this.state.hasError && prevProps.resetKey !== this.props.resetKey) {
      this.setState({ hasError: false, error: null, recovering: false });
    }
  }

  // Чистим кэши SW (он мог отдать закэшированный index.html или отравленный
  // asset) и перезагружаемся. Reload вызываем всегда — даже если caches API
  // упал/недоступен, иначе зависли бы на «обновляем».
  private hardRecover = async () => {
    try {
      if ('caches' in window) {
        const keys = await caches.keys();
        await Promise.all(keys.map((k) => caches.delete(k)));
      }
    } catch { /* всё равно перезагружаемся */ }
    window.location.reload();
  };

  render() {
    if (this.state.recovering) {
      return (
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          minHeight: '60vh', padding: '2rem', color: '#9CA3B8', fontSize: '0.875rem',
        }}>
          Обновляем приложение...
        </div>
      );
    }

    if (this.state.hasError) {
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
            onClick={this.hardRecover}
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
