# Движок картинок сигналов (Вадим 18.11.2026): тот же компонент графика, что на сайте (SimpleChart), рисуется без
# браузера. Сборка: код сайта как есть + render.tsx → один файл Node. Запуск — разово на картинку:
#   docker run --rm -v /tmp/x:/work frame-chart-render /work/card.json /work/out.png
FROM node:20-bookworm-slim AS build
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
WORKDIR /app/frontend/chart-render
RUN npm install --no-audit --no-fund --omit=dev \
 && cd .. && npx vite build --config chart-render/vite.config.mjs \
 && cp chart-render/theme-*.json chart-render/dist/ && cp -r chart-render/fonts chart-render/dist/fonts

FROM node:20-bookworm-slim
WORKDIR /render
COPY --from=build /app/frontend/chart-render/node_modules ./node_modules
COPY --from=build /app/frontend/chart-render/dist ./dist
COPY --from=build /app/frontend/chart-render/package.json ./package.json
ENV NODE_ENV=production
ENTRYPOINT ["node", "dist/render.mjs"]
