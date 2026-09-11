import { useAuth } from '../contexts/AuthContext';
import { isViewAsUser } from '../services/viewMode';

/**
 * Показывает ли сайт этому зрителю незамедленную цену (прежнюю версию сайта).
 *
 * Только роль admin и только вне «вида пользователя». Зеркало бэкенда
 * (api/services/session_close.is_live_viewer): данные бэкенд режет сам, фронт
 * по этому флагу лишь прячет то, что без живой цены не работает — алерты по
 * цене, режим «Внутри дня», живое обновление карты рынка.
 */
export function useLivePrices(): boolean {
  const { user } = useAuth();
  return user?.role === 'admin' && !isViewAsUser();
}

/**
 * То же + готовность: пока AuthContext грузит пользователя, роль неизвестна.
 * Нужно там, где по флагу что-то ПЕРЕКЛЮЧАЮТ (сохранённый режим «Внутри дня»):
 * иначе админский выбор сбросился бы, не дождавшись /me.
 */
export function useLivePricesState(): { live: boolean; ready: boolean } {
  const { user, loading } = useAuth();
  const hasToken = typeof window !== 'undefined' && !!localStorage.getItem('access_token');
  return {
    live: user?.role === 'admin' && !isViewAsUser(),
    ready: !loading && !(hasToken && !user),
  };
}
