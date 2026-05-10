import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { UnheadProvider, createHead } from '@unhead/react/client'
import './index.css'
import App from './App.tsx'

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
