/**
 * Типы T-Bank JS SDK (Integration.js / SpeedPay).
 *
 * Скрипт загружается с https://integrationjs.t-static.ru/integration.js и
 * добавляет глобальный объект `window.PaymentIntegration`.
 *
 * Документация:
 *  - https://developer.tbank.ru/eacq/intro/developer/setup_js/
 *  - https://developer.tbank.ru/eacq/intro/developer/setup_js/setup_speedpay/
 *
 * Типы здесь — упрощённые: T-Bank не публикует .d.ts, поэтому покрываем
 * только те API которые реально используем (init, payments.create, mount,
 * setPaymentStartCallback). Если понадобятся дополнительные методы, см.
 * раздел "Reference" в их доке.
 */

/** Виды виджетов которые SpeedPay рисует на странице мерчанта. */
export type TBankWidgetType =
  | 'sbp'      // СБП (QR-код)
  | 'tpay'     // T-Pay (один клик для клиентов T-Bank)
  | 'bnpl'     // Long Pay / Рассрочка
  | 'sberpay'  // SberPay (не отображается в WebView Android)
  | 'mirpay';  // Mir Pay (только Android)

/** Объект, который возвращает setPaymentStartCallback в Promise. */
export interface TBankPaymentStartResult {
  /** PaymentURL от Init — на него SDK редиректит / открывает в виджете. */
  paymentUrl: string;
}

/** Тип callback'а который мы регистрируем через setPaymentStartCallback. */
export type TBankPaymentStartCallback = () => Promise<TBankPaymentStartResult>;

/** Контейнер для виджетов в DOM (SpeedPay монтируется сюда). */
export interface TBankPaymentsApi {
  setPaymentStartCallback: (cb: TBankPaymentStartCallback) => Promise<void> | void;
  mount: (container: HTMLElement | string) => void;
  unmount?: () => void;
  destroy?: () => void;
}

/** Объект интеграции, возвращаемый init(). */
export interface TBankIntegration {
  payments: {
    /** Создаёт интеграцию виджетов оплаты (SpeedPay). */
    create: (options?: { widgetTypes?: TBankWidgetType[] }) => TBankPaymentsApi;
  };
}

/** Конфиг инициализации SDK. */
export interface TBankIntegrationInitConfig {
  /** terminalKey терминала. Публичный идентификатор (не секрет). */
  terminalKey: string;
  /** Продукт. Для эквайринга всегда 'eacq'. */
  product: 'eacq';
  /** Включаемые модули. */
  features: {
    payment?: Record<string, unknown>;
    iframe?: Record<string, unknown>;
    addcardIframe?: Record<string, unknown>;
  };
}

/** Глобальный объект, который добавляет integration.js. */
export interface TBankPaymentIntegrationGlobal {
  init: (config: TBankIntegrationInitConfig) => Promise<TBankIntegration>;
  /** Helper для HTTP-запросов внутри callback'ов (см. документацию). */
  Helpers?: new () => {
    request: <T = unknown>(url: string, method: string, body?: unknown) => Promise<T>;
  };
}

declare global {
  interface Window {
    PaymentIntegration?: TBankPaymentIntegrationGlobal;
  }
}

export {};
