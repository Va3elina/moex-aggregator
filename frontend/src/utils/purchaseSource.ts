/**
 * Откуда человек пришёл к оплате — чтобы считать, сколько подписок приносят
 * окно «Доступно на тарифе», плашка задержки данных и другие призывы, а
 * сколько прямой заход на тарифы.
 *
 * Каждый призыв ведёт на /pricing?src=<источник>&feature=<функция>. PricingPage
 * запоминает метку в localStorage, а запросы оплаты, СБП и пробного периода
 * отправляют её на бэк, где она ложится в subscriptions.source.
 *
 * localStorage, а не только query-параметр: между кликом по призыву и оплатой
 * бывают регистрация, OAuth и подтверждение почты (иногда в новой вкладке), и
 * ?next= несёт только намерение купить. Метка живёт сутки, новая перебивает
 * старую. Заход на /pricing без метки ничего не стирает: человек, закрывший
 * окно и вернувшийся на тарифы сам, всё равно пришёл с призыва.
 */

export type PurchaseSource =
  | 'paywall'       // окно «Доступно на тарифе X» (UpgradeModal)
  | 'delay_banner'  // плашка «Данные с задержкой»
  | 'header'        // кнопка Plus в шапке
  | 'landing'       // главная страница
  | 'profile';      // профиль: «Сменить тариф» и блок тарифа

const SOURCES: readonly PurchaseSource[] = ['paywall', 'delay_banner', 'header', 'landing', 'profile'];
const KEY = 'frame_purchase_src';
const TTL_MS = 24 * 3600 * 1000;

/** Ссылка на тарифы с меткой источника. */
export function pricingUrlFrom(src: PurchaseSource, feature?: string | null): string {
  const p = new URLSearchParams({ src });
  if (feature) p.set('feature', feature.slice(0, 64));
  return `/pricing?${p}`;
}

export function rememberPurchaseSource(src: PurchaseSource, feature?: string | null): void {
  try {
    localStorage.setItem(KEY, JSON.stringify({ src, feature: feature?.slice(0, 64) || null, ts: Date.now() }));
  } catch { /* приватный режим — метка просто не доедет */ }
}

/** Метка из адреса /pricing. true — была и запомнена. */
export function capturePurchaseSource(params: URLSearchParams): boolean {
  const src = params.get('src') as PurchaseSource | null;
  if (!src || !SOURCES.includes(src)) return false;
  rememberPurchaseSource(src, params.get('feature'));
  return true;
}

/** Поля для тела запроса оплаты. Без свежей метки — прямой заход на тарифы. */
export function purchaseSourcePayload(): { source: string; source_feature: string | null } {
  try {
    const raw = localStorage.getItem(KEY);
    if (raw) {
      const v = JSON.parse(raw);
      if (SOURCES.includes(v?.src) && Date.now() - Number(v.ts) < TTL_MS) {
        return { source: v.src, source_feature: v.feature || null };
      }
    }
  } catch { /* битое значение — считаем прямым заходом */ }
  return { source: 'pricing', source_feature: null };
}
