import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    rollupOptions: {
      output: {
        // Стабильный вендорный чанк: react/react-dom/react-router-dom меняются
        // редко → браузер кэширует их между деплоями (раньше любая правка кода
        // меняла хэш всего entry-бандла ~1.6MB, юзер перекачивал его целиком).
        manualChunks: {
          'react-vendor': ['react', 'react-dom', 'react-router-dom'],
        },
      },
    },
  },
  server: {
    host: '127.0.0.1',
    port: 5173,
    // Нужен для cloudflared quick tunnel (динамический хост *.trycloudflare.com)
    allowedHosts: true,
    proxy: {
      '/api': {
        // `VITE_API_TARGET=prod npm run dev` — фронт локально, данные боевые.
        // Нужно, когда правишь вёрстку/графики: поднимать ради этого Postgres и
        // Redis бессмысленно, а локальная БД — старый частичный снимок, на
        // котором «не воспроизводится» ничего не доказывает. По умолчанию —
        // прежний локальный бэкенд. ⚠️ Только чтение: писать (алерты, биллинг,
        // регистрация) через этот прокси в прод — нельзя.
        target: process.env.VITE_API_TARGET === 'prod'
          ? 'https://framedata.ru'
          : 'http://127.0.0.1:8000',
        changeOrigin: true,
      }
    }
  }
})