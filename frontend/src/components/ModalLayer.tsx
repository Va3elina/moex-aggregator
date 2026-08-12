/**
 * ModalLayer — портал для полноэкранных модалок сайта.
 *
 * Зачем: панель терминала (`/sandbox`, окно расширения) — absolute-блок со
 * своим z-index, то есть стекинг-контекст. Модалка, отрендеренная внутри неё,
 * считает z-index относительно панели и уезжает ПОД топбар терминала: верх
 * срезан, рамки и скруглений не видно. Портал выносит её из панели, а хост
 * (см. modalHost) сохраняет тему терминала.
 *
 * На обычных страницах сайта хост = body, поведение не меняется.
 */
import type { ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { modalHost } from '../utils/modalHost';

export default function ModalLayer({ children }: { children: ReactNode }) {
  const host = modalHost();
  if (!host) return <>{children}</>;
  return createPortal(<>{children}</>, host);
}
