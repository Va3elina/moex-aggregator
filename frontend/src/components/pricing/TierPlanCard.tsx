/**
 * TierPlanCard — карточка тарифа. Один и тот же блок на /pricing и в тарифной
 * сетке внизу лендинга.
 *
 * Вынесено из PricingPage 2026-08-30: сетка на лендинге должна выглядеть ровно
 * так же, как на странице тарифов, а две копии вёрстки неизбежно разъезжаются.
 * Здесь только представление (шапка, цена, чип триала, список фич); вся логика
 * доступности тарифа и кнопка остаются снаружи — кнопка приходит children'ом.
 *
 * TIER_META и CARD_FEATURES живут здесь же: это визуальный контракт карточки,
 * а не состояние страницы.
 */
import type { ReactNode } from 'react';
import { Check, X, Clock, Gift } from 'lucide-react';
import { CSV_EXPORT_ENABLED, PUBLIC_API_ENABLED } from '../../config/features';
import { FEATURE_HINTS, FeatureHintRow } from './featureHints';

export interface PlanVariant {
  plan_id: string;
  amount: number;
  duration_days: number;
  badge: string | null;
}

// Визуал tier'ов: акцентный цвет карточки.
// Editorial palette: все цвета через CSS-vars (theme-aware: light/dark).
// Pro выделен var(--accent) (pumpkin) — это «звезда» каталога, остальные —
// нейтральные оттенки с возрастающим контрастом снизу вверх.
export const TIER_META: Record<string, { color: string; accentBg: string }> = {
  free:    { color: 'var(--text-muted)',     accentBg: 'color-mix(in srgb, var(--text-muted) 12%, transparent)' },
  basic:   { color: 'var(--text-secondary)', accentBg: 'color-mix(in srgb, var(--text-secondary) 10%, transparent)' },
  pro:     { color: 'var(--accent)',         accentBg: 'color-mix(in srgb, var(--accent) 14%, transparent)' },
  premium: { color: 'var(--text-primary)',   accentBg: 'color-mix(in srgb, var(--text-primary) 12%, transparent)' },
};

/**
 * CARD_FEATURES — единый список фич для карточек тарифов (TradingView-формат).
 *
 * Одна строка = одна фича во всех карточках, в одинаковом порядке.
 * Значение per-tier: true = включено (галка), false = недоступно (серый крест),
 * строка = включено с кастомной формулировкой (20 уведомлений / Безлимит).
 * soon = фича заявлена, но ещё не работает: вместо галки часы, текст приглушён.
 *
 * Строки, где pro отличается от basic, карточка Pro подсвечивает сама (см.
 * isUpgrade в рендере) — отдельного флага в данных для этого не нужно.
 *
 * Источник истины — api/billing/features.py: сюда попадают только реально
 * работающие гейты. Не обещать здесь то, что матрица не энфорсит; исключение —
 * строки с soon, которые прямо помечены как ещё не запущенные.
 */
export const CARD_FEATURES: Array<{
  /** Ключ подсказки в FEATURE_HINTS (components/pricing/featureHints.tsx). */
  key: string;
  label: string;
  free: boolean | string;
  basic: boolean | string;
  pro: boolean | string;
  soon?: boolean;
}> = [
  { key: 'indicators', label: 'Все 9 индикаторов',            free: true,  basic: true, pro: true },
  { key: 'assets',     label: 'Все активы и таймфреймы',      free: true,  basic: true, pro: true },
  { key: 'history',    label: 'Вся история',                  free: true,  basic: true, pro: true },
  // Задержка 24 ч на free есть только в Открытых позициях (features.py).
  { key: 'delay',      label: 'Открытые позиции без задержки', free: false, basic: true, pro: true },
  // Два флага матрицы (clgroup_yur + metric_traders) одной строкой — оба
  // открываются вместе на Basic, разделять их в карточке незачем.
  { key: 'yur',        label: 'Юрлица и число трейдеров',     free: false, basic: true, pro: true },
  { key: 'screener',   label: 'Скринер сигналов',             free: false, basic: true, pro: true },
  { key: 'seasonality', label: 'Фильтры сезонности',          free: false, basic: true, pro: true },
  { key: 'funds',      label: 'Свой набор фондов',            free: false, basic: true, pro: true },
  { key: 'range',      label: 'Свой период сравнения',        free: false, basic: true, pro: true },
  { key: 'alerts',     label: 'Уведомления в Telegram',       free: false, basic: '20 уведомлений в Telegram', pro: 'Безлимит уведомлений в Telegram' },
  { key: 'terminal',   label: 'Свой терминал с панелями индикаторов', free: false, basic: false, pro: true },
  // Расширение для браузера (гейт require_pro, см. ExtensionTokenSection): до
  // 2026-08-30 фича жила только в личном кабинете, то есть про неё узнавали
  // уже после оплаты. Отдельной строкой, а не вместе с нашим терминалом:
  // это разные сценарии (свой рабочий стол против работы поверх чужого),
  // и Pro нужен видимый вес отличий от Basic.
  { key: 'extension',  label: 'Индикаторы в терминале Т-Инвестиций', free: false, basic: false, pro: true },
  // KILL-SWITCH (config/features.ts) — раздельный: экспорт включён 01.09.2026,
  // API ещё нет. Выключенная функция не должна выглядеть рабочей, поэтому
  // строка появляется только когда её флаг поднят.
  ...(CSV_EXPORT_ENABLED
    ? [{ key: 'download', label: 'Экспорт CSV / Excel', free: false, basic: false, pro: true }]
    : []),
  ...(PUBLIC_API_ENABLED
    ? [{ key: 'download', label: 'API-доступ', free: false, basic: false, pro: true }]
    : []),
];

interface Props {
  /** 'free' / 'basic' / 'pro' — ключ TIER_META и колонка в CARD_FEATURES. */
  tier: string;
  /** Название тарифа с бэка (Free / Basic / Pro). */
  title: string;
  /** Выбранный вариант оплаты (месяц/год). null у free. */
  variant: PlanVariant | null;
  period: 'monthly' | 'yearly';
  /** Месячная цена того же тарифа — для строки «Вы экономите ... в год». */
  monthlyAmount?: number | null;
  /** Дней бесплатного пробного периода; falsy — чип не показываем. */
  trialDays?: number | null;
  /** Тариф юзера — рамка в цвет тарифа + плашка «Текущий». */
  isCurrent?: boolean;
  /** Кнопка карточки (у страницы тарифов и лендинга она разная). */
  children?: ReactNode;
}

export default function TierPlanCard({
  tier, title, variant, period, monthlyAmount, trialDays, isCurrent, children,
}: Props) {
  const meta = TIER_META[tier] || TIER_META.free;

  return (
    <div
      className="relative rounded-2xl border p-5 flex flex-col"
      style={{
        borderColor: isCurrent ? meta.color : 'var(--border-color)',
        backgroundColor: 'var(--bg-secondary)',
        boxShadow: isCurrent ? `0 0 0 2px ${meta.color}40` : undefined,
      }}
    >
      {/* Badge */}
      {variant?.badge && (
        <div
          className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 rounded-full text-xs font-semibold whitespace-nowrap"
          style={{ backgroundColor: meta.color, color: 'var(--bg-primary)' }}
        >
          {variant.badge}
        </div>
      )}
      {isCurrent && (
        <div
          className="absolute top-3 right-3 px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider"
          style={{ backgroundColor: meta.color, color: 'var(--bg-primary)' }}
        >
          Текущий
        </div>
      )}

      {/* Заголовок без иконки (решение владельца 2026-08-29): плашка с
          пиктограммой не несла смысла и утяжеляла шапку карточки.
          tier.description с бэка не рендерим: «Базовый доступ к сайту» /
          «Расширенный доступ на уровне Basic» ничего не добавляют к названию,
          содержание тарифа раскрывает список фич ниже. */}
      <h3 className="text-lg font-bold text-theme-primary mb-1.5">{title}</h3>

      {/* Цена.
          Годовой тариф показываем в пересчёте на месяц (крупно), а полную
          периодичность списания и экономию — строками ниже. Так цены месяца
          и года сравнимы «в лоб»; полная сумма списания раскрывается до
          оплаты в consent-модалке и на форме T-Bank. */}
      <div className="mb-5">
        {variant ? (
          <>
            <div className="flex items-baseline gap-1.5 flex-wrap">
              <span
                className="font-bold text-theme-primary leading-none"
                style={{
                  // Fluid scale: 1.5rem на узких → 2rem на широких карточках.
                  // clamp() предотвращает overflow когда длинная цена (19 990 ₽)
                  // не помещается в узкую карточку на грид 4-в-ряд.
                  fontSize: 'clamp(1.5rem, 2.2vw, 2rem)',
                  whiteSpace: 'nowrap',
                }}
              >
                {(period === 'yearly'
                  ? Math.round(variant.amount / 12)
                  : variant.amount
                ).toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽
              </span>
              <span className="text-sm text-theme-secondary leading-none">
                /мес
              </span>
            </div>
            {period === 'yearly' && (
              <div className="mt-1.5 space-y-0.5">
                <div className="text-xs text-theme-muted">
                  Оплата раз в год
                </div>
                {!!monthlyAmount && monthlyAmount * 12 > variant.amount && (
                  <div className="text-xs" style={{ color: meta.color, fontWeight: 600 }}>
                    Вы экономите {(monthlyAmount * 12 - variant.amount).toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽ в год
                  </div>
                )}
              </div>
            )}
          </>
        ) : (
          <div
            className="font-bold text-theme-primary leading-none"
            style={{
              fontSize: 'clamp(1.5rem, 2.2vw, 2rem)',
              whiteSpace: 'nowrap',
            }}
          >
            Бесплатно
          </div>
        )}
      </div>

      {/* Бесплатный пробный период — выделенный чип, главная «фишка» карточки */}
      {!!trialDays && (
        <div
          className="inline-flex items-center gap-2 mb-4 -mt-1 px-3 py-2 rounded-xl font-extrabold leading-tight"
          style={{
            color: meta.color,
            background: meta.accentBg,
            border: `1.5px solid ${meta.color}`,
            fontSize: 'clamp(0.95rem, 1.5vw, 1.1rem)',
          }}
        >
          <Gift size={20} strokeWidth={2.4} className="flex-shrink-0" />
          <span>{trialDays} дней бесплатно</span>
        </div>
      )}

      {/* Features list — TradingView-формат: ЕДИНЫЙ список фич в платных
          карточках (Pro повторяет пункты Basic), включённое — с галкой,
          недоступное — приглушённый крест. Источник — CARD_FEATURES.
          У Free списка НЕТ намеренно (решение владельца 2026-08-24):
          карточка не раскрывает, что входит в бесплатный тариф, —
          содержимое Free видно только в сравнительной таблице ниже. */}
      {tier === 'free' && <div className="flex-1 mb-5" />}
      {tier !== 'free' && (
        <ul className="flex-1 space-y-2 mb-5 text-sm">
          {CARD_FEATURES.map((f, i) => {
            const v = f[(tier === 'basic' || tier === 'pro' ? tier : 'free')];
            const included = v !== false;
            const label = typeof v === 'string' ? v : f.label;
            // soon = фича в этом тарифе заявлена, но ещё не работает:
            // часы вместо галки и приглушённый текст, чтобы её не
            // приняли за уже доступную.
            const soon = included && f.soon === true;
            // Строка-апгрейд: в Pro значение отличается от Basic. Общий
            // список фич в обеих карточках сохраняем (цены сравнимы «в
            // лоб»), но в Pro эти строки красим в цвет тарифа и делаем
            // полужирными — иначе три реальных отличия тонут среди
            // десятка одинаковых галок и Pro читается как «то же самое».
            const isUpgrade = tier === 'pro' && !soon && f.pro !== f.basic;
            return (
              <FeatureHintRow
                key={i}
                hint={FEATURE_HINTS[f.key]}
                style={included && !soon
                  ? { color: isUpgrade ? meta.color : 'var(--text-secondary)', fontWeight: isUpgrade ? 600 : undefined }
                  : { color: 'var(--text-muted)', opacity: soon ? 0.8 : 0.55 }}
              >
                {soon ? (
                  <Clock size={16} className="mt-0.5 flex-shrink-0" style={{ color: 'var(--text-muted)' }} />
                ) : included ? (
                  <Check size={16} className="mt-0.5 flex-shrink-0" style={{ color: meta.color }} />
                ) : (
                  <X size={16} className="mt-0.5 flex-shrink-0" style={{ color: 'var(--text-muted)' }} />
                )}
                <span>{label}</span>
              </FeatureHintRow>
            );
          })}
        </ul>
      )}

      {children}
    </div>
  );
}
