import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { UnheadProvider, createHead } from '@unhead/react/client'
import './index.css'
import App from './App.tsx'

// После деплоя hashed-имена code-split чанков меняются. Долго открытая вкладка
// держит старый бандл и ссылается на чанк, которого новый билд уже не содержит,
// → dynamic import падает с 404 («Failed to fetch dynamically imported module»),
// и ErrorBoundary показывает «Что-то пошло не так». Vite кидает vite:preloadError
// на такие сбои — разово перезагружаем вкладку (sessionStorage-guard от цикла),
// чтобы подтянуть свежий index.html и манифест чанков.
window.addEventListener('vite:preloadError', (e) => {
  if (sessionStorage.getItem('frame:chunk-reload')) return // уже пробовали — пусть всплывёт
  e.preventDefault()
  sessionStorage.setItem('frame:chunk-reload', '1')
  window.location.reload()
})
// Через 10 c снимаем guard: если вкладка живёт долго, будущий деплой (новый
// сбой чанка) тоже должен мочь разово перезагрузиться.
setTimeout(() => sessionStorage.removeItem('frame:chunk-reload'), 10_000)

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
