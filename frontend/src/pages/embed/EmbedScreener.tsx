/**
 * EmbedScreener — «Скринер сигналов ОИ» для плавающей панели/embed-роута.
 *
 * Прям сайтовый скринер, один в один: рендерим ТОТ ЖЕ OiScreenerTable, что на
 * /oi (комета позиции, пилюли «Силы», рекорды, избранное), а не ужатый фид.
 * Ужатая версия (иконка · имя · перекос % · ×N) показывала проценты
 * лонг/шорт прямо в строках и выглядела чужой рядом с сайтом — выпилена по
 * решению 2026-08-07. Проценты перекоса на сайте живут только в подсказке
 * кометы — здесь автоматически так же.
 *
 * Состояние тулбара (группа, горизонт, категория, избранные) — те же
 * localStorage-ключи, что на сайте: скринер в панели открывается в том виде,
 * в котором его оставили на /oi, и наоборот.
 *
 * Панель узкая, таблица шире (minWidth 1000) → горизонтальный скролл внутри
 * панели; вертикальный скролл — свой (панель фиксированной высоты).
 */
import OiScreenerTable from '../../components/oi/OiScreenerTable';
import { EmbedFrame } from './EmbedToolbar';

/** `onPick` — клик по строке сигнала (§6.10 модели песочницы: как лента аномалий,
 *  открывает панель ОИ на этом активе). Не задан → клик ничего не делает
 *  (embed-роут сайта/расширения). */
export default function EmbedScreener({ onPick }: { onPick?: (r: { sectype: string }) => void } = {}) {
  return (
    <EmbedFrame
      lead={
        <span style={{ fontWeight: 800, fontSize: 13, letterSpacing: '-0.01em', paddingLeft: 2, flexShrink: 0 }}>
          Скринер сигналов
        </span>
      }
    >
      <div style={{ position: 'absolute', inset: 0 }}>
        <div className="styled-scrollbar" style={{ height: '100%', overflow: 'auto', padding: '10px 12px' }}>
          <OiScreenerTable onSelect={(sectype) => onPick?.({ sectype })} />
        </div>
      </div>
    </EmbedFrame>
  );
}
