/**
 * Общие действия аномалии — переиспользуют ToastHost и AnomalyBell, чтобы логика
 * «открыть график» (с tier-вердиктом) и «получать сигнал» (с квотой) не дублировалась.
 * Плоские функции с прокинутыми зависимостями (navigate/showUpgrade) — без хуков,
 * чтобы звать из любого контекста.
 */
import type { NavigateFunction } from 'react-router-dom';
import { handleTierError, type UpgradeTier } from '../../utils/tierError';
import { subscribeAnomaly as apiSubscribe, type AnomalyItem, type AnomalyDeepLink } from '../../services/api';

type ShowUpgrade = (p: { tier: UpgradeTier; featureName?: string; indicator?: string }) => void;

export function buildAnomalyUrl(dl: AnomalyDeepLink): string {
  if (dl.route === '/oi') {
    const p = new URLSearchParams();
    if (dl.secid) p.set('instrument', dl.secid);
    if (dl.clgroup) p.set('clgroup', dl.clgroup);
    if (dl.interval != null) p.set('interval', String(dl.interval));
    if (dl.mode) p.set('mode', dl.mode);
    if (dl.variant) p.set('variant', dl.variant);
    // Аномалии — дневной OI-сигнал; открываем на 1 году (контекст движения, лёгкая
    // дневная серия), переопределяя сохранённый период пользователя. Решение Вадима.
    p.set('period', '1y');
    return `/oi?${p.toString()}`;
  }
  if (dl.route === '/funds-money') {
    return dl.category ? `/funds-money?category=${dl.category}` : '/funds-money';
  }
  return dl.route || '/';
}

/** Торговый день аномалии (signal_date='YYYY-MM-DD') в родительном падеже: «26 июня».
 *  Форматируем в UTC, чтобы день не «съезжал» в часовых поясах западнее UTC. Пустая
 *  строка, если даты нет (промо/посты канала). Зеркалит TG-строку «по данным за N». */
export function tradeDateLabel(signalDate?: string | null): string {
  if (!signalDate) return '';
  const d = new Date(`${signalDate}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleDateString('ru-RU', { day: 'numeric', month: 'long', timeZone: 'UTC' });
}

/** Открыть график аномалии. Если цель закрыта тарифом (link_required_tier) —
 *  апселл + переход на ДЕФОЛТ страницы (доступное по умолчанию); иначе диплинк. */
export function openAnomaly(item: AnomalyItem, navigate: NavigateFunction, showUpgrade: ShowUpgrade): void {
  // Внешняя ссылка (промо канала / пост) — открываем в новой вкладке, без навигации.
  const route = item.deep_link?.route || '';
  if (/^https?:\/\//.test(route)) {
    window.open(route, '_blank', 'noopener,noreferrer');
    return;
  }
  if (item.link_required_tier) {
    const indicator = item.type === 'funds_flow' ? 'funds_money' : 'open_interest';
    showUpgrade({ tier: item.link_required_tier, featureName: item.asset_name || item.asset_id, indicator });
    navigate(item.deep_link.route || '/');
  } else {
    navigate(buildAnomalyUrl(item.deep_link));
  }
}

/** Создать личный сигнал из аномалии. Гость → апселл (нужен аккаунт+тариф);
 *  квота-403 → handleTierError (апселл Pro/Basic). Возвращает исход для UI. */
export async function subscribeAnomalyAction(
  item: AnomalyItem, isAuthenticated: boolean, showUpgrade: ShowUpgrade,
): Promise<'ok' | 'guest' | 'error'> {
  if (!isAuthenticated) {
    showUpgrade({ tier: 'basic', featureName: 'сигналы', indicator: 'anomaly' });
    return 'guest';
  }
  try {
    await apiSubscribe(item.id);
    return 'ok';
  } catch (e) {
    handleTierError(e, { showUpgrade, indicator: 'anomaly', featureName: 'сигналы' });
    return 'error';
  }
}
