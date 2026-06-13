/**
 * AdaptiveSegmented — переключатель, адаптивный к ширине тулбара.
 *
 * Когда места по горизонтали хватает (collapsed=false) — рисуем
 * SegmentedControl: переключение в один клик, активный сегмент виден сразу.
 * Когда тулбар узкий (collapsed=true) — тот же выбор сворачивается в Dropdown,
 * чтобы контролы не переносились на вторую строку и не лезли друг на друга.
 *
 * ВАЖНО: решение collapsed принимает РОДИТЕЛЬ (меряет ширину тулбара одним
 * ResizeObserver и сравнивает с общим порогом), а не сам компонент. Так все
 * адаптивные контролы в одном ряду переключаются синхронно — без рассинхрона
 * (часть сегментами, часть выпадашками) и без мигания. Ширина тулбара не
 * зависит от того, свёрнуты ли его дети, поэтому петли «свернул → стало уже →
 * развернул» не возникает.
 *
 * Сегменты с disabled (нет данных в инструменте) в свёрнутом виде не
 * поддерживаются — у адаптивных контролов (размер, период) таких опций нет.
 * Тарифный locked прокидывается в Dropdown как есть.
 */
import SegmentedControl, { type SegmentOption } from './SegmentedControl';
import Dropdown, { type DropdownOption } from './Dropdown';

interface AdaptiveSegmentedProps<T extends string> {
  options: SegmentOption<T>[];
  value: T;
  onChange: (key: T) => void;
  /** Клик по locked-сегменту/опции (показ UpgradeModal / редирект на /login) */
  onLockedClick?: (key: T) => void;
  /** true → рисуем Dropdown вместо горизонтальных сегментов */
  collapsed: boolean;
  className?: string;
}

export default function AdaptiveSegmented<T extends string>({
  options,
  value,
  onChange,
  onLockedClick,
  collapsed,
  className = '',
}: AdaptiveSegmentedProps<T>) {
  if (collapsed) {
    const dropdownOptions: DropdownOption<T>[] = options.map((o) => ({
      key: o.key,
      label: o.label,
      locked: o.locked,
    }));
    return (
      <Dropdown<T>
        options={dropdownOptions}
        value={value}
        onChange={onChange}
        onLockedClick={onLockedClick}
        className={className}
      />
    );
  }

  return (
    <SegmentedControl<T>
      options={options}
      value={value}
      onChange={onChange}
      onLockedClick={onLockedClick}
      className={className}
    />
  );
}
