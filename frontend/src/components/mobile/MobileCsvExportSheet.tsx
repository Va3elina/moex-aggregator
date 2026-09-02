/**
 * MobileCsvExportSheet — мобильная «модалка» экспорта данных: тот же
 * конфигуратор, что и в десктопной CsvExportModal (CsvExportForm — слои,
 * параметры, формат, кнопка), но внутри slide-up MobileSheet. Логика и
 * селекторы общие, различается только обёртка и раскладка кнопки
 * (layout="sheet": одна широкая, липнет к низу прокрутки).
 *
 * config — функция, а не объект: вызывается при открытии, чтобы дефолты
 * селекторов (тикер/период/режим) снимались с ТЕКУЩЕГО состояния страницы.
 *
 * Пока идёт загрузка файла, шит не закрывается тапом по фону/крестику —
 * иначе форма размонтируется посреди запроса.
 */
import { lazy, Suspense, useState } from 'react';
import MobileSheet from './MobileSheet';
import type { CsvExportConfig } from '../export/CsvExportModal';

// Lazy — форма с селекторами живёт в chunk'е десктопной модалки.
const CsvExportForm = lazy(() =>
  import('../export/CsvExportModal').then((m) => ({ default: m.CsvExportForm })),
);

interface Props {
  open: boolean;
  onClose: () => void;
  config: () => CsvExportConfig;
}

export default function MobileCsvExportSheet({ open, onClose, config }: Props) {
  const [busy, setBusy] = useState(false);

  if (!open) return null;

  const cfg = config();
  // Заголовок шита — «Скачать данные», а конфиг-title («Экспорт: …») идёт
  // подстрокой без префикса: что именно выгружаем.
  const subtitle = cfg.title.replace(/^Экспорт:\s*/, '');

  return (
    <MobileSheet
      open
      onClose={() => { if (!busy) onClose(); }}
      title="Скачать данные"
      className="fm-sheet--export"
    >
      <div
        style={{
          padding: '12px 16px 0',
          fontSize: 'var(--fs-xs)',
          fontWeight: 600,
          color: 'var(--text-secondary)',
        }}
      >
        {subtitle}
      </div>
      <Suspense
        fallback={
          <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
            Загрузка…
          </div>
        }
      >
        <CsvExportForm
          config={cfg}
          onClose={onClose}
          layout="sheet"
          onBusyChange={setBusy}
        />
      </Suspense>
    </MobileSheet>
  );
}
