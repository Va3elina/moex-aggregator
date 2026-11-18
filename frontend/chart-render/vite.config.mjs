// Сборка движка картинок сигналов: код сайта (frontend/src) собирается Vite в режиме SSR в один файл для Node.
// Корень — frontend/, чтобы работали те же пути и особенности сборщика, что у сайта. Нативные модули — снаружи.
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const ext = ['@napi-rs/canvas', '@resvg/resvg-js', 'jsdom'];

export default {
  root: path.resolve(here, '..'),
  logLevel: 'warn',
  ssr: { noExternal: true, external: ext },
  build: {
    ssr: true, outDir: path.resolve(here, 'dist'), emptyOutDir: true, minify: false, target: 'node20',
    rollupOptions: { external: ext, input: path.resolve(here, 'render.tsx'),
      output: { format: 'esm', entryFileNames: '[name].mjs', inlineDynamicImports: true } },
  },
};
