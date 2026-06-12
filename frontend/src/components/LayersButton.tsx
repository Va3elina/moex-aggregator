/**
 * LayersButton — иконка «Слои» (44×44, стиль 1:1 с ChartCaptureButton/AlertBell)
 * + модалка поверх экрана с тумблерами видимости слоёв графика.
 *
 * Низкочастотные toggle'ы вида (Цена, Экспирации, События, Индекс...) не живут
 * в горячем ряду контролов — решение 2026-06-12: прячем за маленькой кнопкой
 * рядом с камерой и колоколом, без бейджа состояния. Паттерн общий для всех
 * индикаторов со слоями.
 */
import { useState } from 'react';
import { Layers } from 'lucide-react';
import CenteredModalShell from './CenteredModalShell';
import { ToggleRow } from './ToggleRow';

export interface LayerToggle {
  key: string;
  label: string;
  hint?: string;
  checked: boolean;
  onChange: (v: boolean) => void;
}

interface LayersButtonProps {
  layers: LayerToggle[];
  /** data-tour id для онбординг-тура страницы */
  tourId?: string;
  className?: string;
}

export default function LayersButton({ layers, tourId, className = '' }: LayersButtonProps) {
  const [open, setOpen] = useState(false);

  return (
    <>
      <button
        type="button"
        data-tour={tourId}
        data-export-ignore="true"
        onClick={() => setOpen(true)}
        className={`editorial-press rounded-full inline-flex items-center justify-center ${className}`}
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '2px solid var(--text-primary)',
          color: 'var(--text-primary)',
          width: 44,
          height: 44,
        }}
        aria-label="Слои графика"
        title="Слои графика"
      >
        <Layers size={22} />
      </button>
      {/* Модалка не закрывается на каждый тумблер — юзер может щёлкнуть оба сразу */}
      <CenteredModalShell open={open} onClose={() => setOpen(false)} title="Слои графика">
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {layers.map((l) => (
            <ToggleRow key={l.key} label={l.label} hint={l.hint} checked={l.checked} onChange={l.onChange} />
          ))}
        </div>
      </CenteredModalShell>
    </>
  );
}
