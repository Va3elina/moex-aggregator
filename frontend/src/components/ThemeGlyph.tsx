/**
 * ThemeGlyph — анимированная пара Sun/Moon для любых кнопок смены темы.
 *
 * ⚠️ Это ОСОЗНАННАЯ КОПИЯ глифа из ThemeToggle (шапка сайта), а не вынесенный
 * из него общий кусок: сайт трогать нельзя (требование Вадима), поэтому в
 * ThemeToggle те же два SVG так и остались инлайном. Пользуются копией только
 * места песочницы — шапка оболочки (SandboxPage) и кнопка «Тема панели» в
 * тулбаре окна (EmbedToolbar). Меняешь иконку темы на сайте — поменяй и здесь,
 * иначе песочница разъедется с шапкой.
 *
 * Кроссфейд: обе иконки лежат друг на друге (absolute внутри relative-обёртки),
 * прячущаяся уезжает в rotate(∓90deg) scale(0.4) + opacity 0. Transition —
 * Tailwind-классами (`transition-all duration-500 ease-out`): Tailwind v4
 * подключён глобально (`@import "tailwindcss"` в index.css, единый SPA-entry),
 * так что утилиты доступны и в поддеревьях песочницы/embed'а.
 *
 * Цвет заливки — var(--accent), обводка лучей — currentColor (наследуется от
 * кнопки: на сайте --text-primary, в песочнице --muted).
 */
export default function ThemeGlyph({
  dark,
  size,
}: {
  /** true — показываем луну (тёмная тема), false — солнце. */
  dark: boolean;
  /** Сторона глифа. Число = px; строку принимаем ради clamp()-размера в шапке сайта. */
  size: number | string;
}) {
  // На маленьких размерах «полный» рисунок солнца превращается в кашу: при 13px
  // луч из 24-виюбокса даёт ~1.1px длины при ~0.95px обводки, а восемь лучей
  // (4 диагональных вплотную к 4 прямым) сливаются в серый ореол вокруг круга.
  // Поэтому < 16px рисуем упрощённый глиф: только 4 прямых луча, они длиннее,
  // толще и дальше отодвинуты от ядра — на 13px читается как солнце, а не как
  // клякса. Строковый (clamp) размер считаем «большим» — вид сайта не меняется.
  const compact = typeof size === 'number' && size < 16;

  const iconStyle = { width: size, height: size } as const;

  return (
    <span
      aria-hidden
      style={{
        position: 'relative',
        display: 'inline-grid',
        placeItems: 'center',
        width: size,
        height: size,
        flexShrink: 0,
      }}
    >
      {/* Sun — видна в light. Лучи разлетаются + центр пульсирует. */}
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth={compact ? 2.6 : 1.75}
        strokeLinecap="round"
        strokeLinejoin="round"
        className="absolute transition-all duration-500 ease-out"
        style={{
          ...iconStyle,
          opacity: dark ? 0 : 1,
          transform: dark ? 'rotate(-90deg) scale(0.4)' : 'rotate(0) scale(1)',
        }}
      >
        {compact ? (
          <>
            <circle cx="12" cy="12" r="5" fill="var(--accent)" stroke="var(--accent)" />
            <path d="M12 1.5v3" />
            <path d="M12 19.5v3" />
            <path d="M1.5 12h3" />
            <path d="M19.5 12h3" />
          </>
        ) : (
          <>
            <circle cx="12" cy="12" r="4" fill="var(--accent)" stroke="var(--accent)" />
            <path d="M12 2v2" />
            <path d="M12 20v2" />
            <path d="M4.93 4.93l1.41 1.41" />
            <path d="M17.66 17.66l1.41 1.41" />
            <path d="M2 12h2" />
            <path d="M20 12h2" />
            <path d="M4.93 19.07l1.41-1.41" />
            <path d="M17.66 6.34l1.41-1.41" />
          </>
        )}
      </svg>

      {/* Moon — видна в dark. Acccent-fill, плавно влетает. */}
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.75"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="absolute transition-all duration-500 ease-out"
        style={{
          ...iconStyle,
          opacity: dark ? 1 : 0,
          transform: dark ? 'rotate(0) scale(1)' : 'rotate(90deg) scale(0.4)',
        }}
      >
        <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" fill="var(--accent)" stroke="var(--accent)" />
      </svg>
    </span>
  );
}
