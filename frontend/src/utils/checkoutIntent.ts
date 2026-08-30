/**
 * Намерение оформить подписку, пережившее регистрацию.
 *
 * Гость жмёт «Оформить» на лендинге или в тарифах — аккаунта у него нет, и
 * раньше выбор на этом месте терялся: после регистрации человек возвращался на
 * /pricing и жал ту же кнопку заново, а следом уходил подтверждать почту и
 * терял страницу совсем. Три клика «купить» на одну покупку. Теперь выбранный
 * тариф едет в ?next= через регистрацию и подтверждение почты, а /pricing сам
 * открывает окно согласий на нужном плане.
 *
 * Формат — обычные query-параметры /pricing, а не localStorage: их переносит
 * тот же ?next=, они переживают OAuth round-trip (см. postLoginRedirect) и
 * видны в адресной строке, так что ссылку можно собрать руками.
 *   /pricing?plan=basic_yearly          — оформление конкретного плана
 *   /pricing?trial=pro&period=monthly   — старт бесплатного периода
 */

export type CheckoutIntent =
  | { kind: 'plan'; planId: string }
  | { kind: 'trial'; tier: string; period: 'monthly' | 'yearly' };

/** Имена параметров намерения — вычищаем из URL, когда оно отработало. */
export const INTENT_PARAM_NAMES = ['plan', 'trial', 'period'] as const;

/** Намерение из query-строки /pricing. null — обычный заход на тарифы. */
export function readCheckoutIntent(params: URLSearchParams): CheckoutIntent | null {
  const planId = params.get('plan');
  if (planId) return { kind: 'plan', planId };
  const tier = params.get('trial');
  // Тариф проверяем по белому списку: параметр приходит из URL, а дальше
  // уходит в POST /trial/start — мусор туда пускать незачем.
  if (tier === 'basic' || tier === 'pro') {
    return { kind: 'trial', tier, period: params.get('period') === 'monthly' ? 'monthly' : 'yearly' };
  }
  return null;
}

/** Страница тарифов со вшитым намерением — куда возвращать юзера. */
export function pricingUrlWithIntent(intent: CheckoutIntent | null): string {
  if (!intent) return '/pricing';
  if (intent.kind === 'plan') return `/pricing?plan=${encodeURIComponent(intent.planId)}`;
  return `/pricing?trial=${intent.tier}&period=${intent.period}`;
}

/** Первый шаг цикла для гостя: регистрация, после которой намерение доигрывается. */
export function signupUrlForIntent(intent: CheckoutIntent | null): string {
  return `/login?mode=register&next=${encodeURIComponent(pricingUrlWithIntent(intent))}`;
}

/** Шаг подтверждения почты с возвратом к намерению — без next оно теряется. */
export function emailStepUrl(path: '/verify-email' | '/add-email', next: string): string {
  return `${path}?next=${encodeURIComponent(next)}`;
}
