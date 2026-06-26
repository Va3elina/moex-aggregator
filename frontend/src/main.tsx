import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { UnheadProvider, createHead } from '@unhead/react/client'
import './index.css'
import App from './App.tsx'
import { reloadOnceForChunk } from './utils/chunkReload'

// После деплоя hashed-имена code-split чанков меняются. Долго открытая вкладка
// держит старый бандл и ссылается на чанк, которого новый билд уже не содержит,
// → dynamic import падает с 404 («Failed to fetch dynamically imported module»).
// Vite кидает vite:preloadError на такие сбои — разово перезагружаем вкладку,
// чтобы подтянуть свежий index.html и манифест чанков. Если reloadOnceForChunk
// вернул false (недавно уже перезагружались, а чанк всё равно не грузится —
// деплой реально сломан), НЕ глушим событие: пусть всплывёт в ErrorBoundary.
// Тот же guard переиспользует ErrorBoundary (utils/chunkReload).
window.addEventListener('vite:preloadError', (e) => {
  if (reloadOnceForChunk()) e.preventDefault()
})

// Unhead SSR-friendly head manager. Заменяет document.title/meta вручную;
// per-page meta через useHead() — необходимо для SEO (каждая страница получает
// свой <title>/<meta description>/canonical/JSON-LD вместо одинаковых из index.html).
const head = createHead()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <UnheadProvider head={head}>
      <App />
    </UnheadProvider>
  </StrictMode>,
)
