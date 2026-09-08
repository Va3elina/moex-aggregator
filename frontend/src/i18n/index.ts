/**
 * i18n — английская версия интерфейса поверх русского.
 *
 * Принцип: РУССКИЙ ТЕКСТ = КЛЮЧ. В коде пишем `t('Открытые позиции')`, словарь
 * en/*.json отображает русскую строку на английскую. Если перевода нет —
 * i18next возвращает ключ, т.е. русский текст, и ничего не ломается.
 * Отдельного ru.json нет и не нужно.
 *
 * Почему так, а не «нормальные» ключи вида `nav.oi`:
 *   - 9 000+ строк уже лежат инлайном в JSX; переименовывать их в ключи —
 *     недели работы без пользы для продукта;
 *   - fallback = русский текст → перевод можно вводить постранично, сайт
 *     для русских пользователей не меняется вообще (см. [[feedback_site_sandbox_separation]]).
 *
 * Словари: src/i18n/en/<domain>.json — по одному файлу на страницу/домен,
 * чтобы параллельные правки не конфликтовали в одном файле. Все файлы
 * склеиваются в один namespace на старте (import.meta.glob, eager).
 *
 * Выбор языка: localStorage `frame:lang` → navigator.language (не ru* → en).
 * Ключ с префиксом `frame:` синкается между устройствами (services/settingsSync).
 *
 * Использование:
 *   React:      const { t } = useTranslation();  t('Сезонность')
 *   вне React:  import { t } from '../i18n';      t('Сезонность')
 *   плюрал/подстановка: t('Осталось {{n}} дней', { n })  — в en.json:
 *                       "Осталось {{n}} дней": "{{n}} days left"
 *
 * ВАЖНО: keySeparator/nsSeparator выключены — точки и двоеточия внутри
 * русских фраз не должны трактоваться как разделители ключей.
 */
import i18next from 'i18next';
import { initReactI18next } from 'react-i18next';

export type Lang = 'ru' | 'en';

export const LANG_STORAGE_KEY = 'frame:lang';

const enModules = import.meta.glob<{ default: Record<string, string> }>('./en/*.json', {
  eager: true,
});

const enResources: Record<string, string> = {};
for (const path of Object.keys(enModules)) {
  const dict = enModules[path].default;
  for (const key of Object.keys(dict)) {
    if (import.meta.env.DEV && key in enResources && enResources[key] !== dict[key]) {
      console.warn(`[i18n] дубликат ключа с разным переводом: "${key}" в ${path}`);
    }
    enResources[key] = dict[key];
  }
}

function readStoredLang(): Lang | null {
  try {
    const v = localStorage.getItem(LANG_STORAGE_KEY);
    return v === 'en' || v === 'ru' ? v : null;
  } catch {
    return null;
  }
}

export function detectLang(): Lang {
  const stored = readStoredLang();
  if (stored) return stored;
  const nav = (typeof navigator !== 'undefined' && navigator.language) || 'ru';
  return nav.toLowerCase().startsWith('ru') ? 'ru' : 'en';
}

function applyHtmlLang(lang: Lang) {
  if (typeof document !== 'undefined') document.documentElement.lang = lang;
}

const initial = detectLang();

i18next.use(initReactI18next).init({
  lng: initial,
  fallbackLng: false,
  resources: { en: { translation: enResources } },
  // ru — пустой namespace: t() вернёт ключ, т.е. исходный русский текст.
  keySeparator: false,
  nsSeparator: false,
  returnNull: false,
  returnEmptyString: false,
  interpolation: { escapeValue: false },
  react: { useSuspense: false },
});
applyHtmlLang(initial);

export function getLang(): Lang {
  return i18next.language === 'en' ? 'en' : 'ru';
}

export function setLang(lang: Lang) {
  try {
    localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch {
    /* private mode */
  }
  applyHtmlLang(lang);
  void i18next.changeLanguage(lang);
}

/** Для кода вне React (чарты, форматтеры, сервисы). */
export const t = (key: string, opts?: Record<string, unknown>): string =>
  String(i18next.t(key, opts as never));

/** BCP-47 локаль для toLocaleDateString / Intl.* с учётом выбранного языка. */
export function dateLocale(): string {
  return getLang() === 'en' ? 'en-GB' : 'ru-RU';
}

const MONTHS_SHORT_RU = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
const MONTHS_SHORT_EN = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const MONTHS_GENITIVE_RU = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'];
const MONTHS_FULL_EN = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

/** Короткие месяцы, 0-based. Ру: «янв», en: «Jan». */
export function monthShort(i: number): string {
  return (getLang() === 'en' ? MONTHS_SHORT_EN : MONTHS_SHORT_RU)[i] ?? '';
}

/** Месяц для даты «5 января» / «5 January», 0-based. */
export function monthGenitive(i: number): string {
  return (getLang() === 'en' ? MONTHS_FULL_EN : MONTHS_GENITIVE_RU)[i] ?? '';
}

export default i18next;
