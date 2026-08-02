/**
 * PendingRedeemApplier — применяет отложенный invite-токен сразу, как только
 * гость становится авторизованным.
 *
 * Зачем: гость по инвайт-ссылке уходит регистрироваться, и раньше возврат на
 * /billing/redeem жил только в `?next=`, который терялся на пути
 * регистрация → /verify-email → главная (и в OAuth-ветке через /add-email).
 * Человек регистрировался и оставался без подписки, пока сам не открывал
 * ссылку повторно.
 *
 * Теперь применение не зависит от маршрута: висим глобально, ждём
 * isAuthenticated и делаем redeem. Эндпоинт идемпотентен и НЕ требует
 * подтверждённой почты, так что повтор безопасен, а подписка выдаётся до
 * верификации email.
 *
 * После успеха уводим на /billing/redeem — показать «Подписка активирована».
 * Сам /billing/redeem обрабатывает токен собственной логикой, поэтому там мы
 * не вмешиваемся.
 */
import { useEffect, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { apiFetch } from '../services/api';
import { clearPendingRedeemToken, getPendingRedeemToken } from '../utils/pendingRedeem';

export default function PendingRedeemApplier() {
  const { isAuthenticated, refreshUser } = useAuth();
  const navigate = useNavigate();
  const { pathname } = useLocation();
  // Один заход на монтирование сессии: без гварда эффект перезапустился бы
  // после refreshUser() (меняется user) и ушёл бы во второй POST.
  const inFlight = useRef(false);

  useEffect(() => {
    if (!isAuthenticated) return;
    if (pathname.startsWith('/billing/redeem')) return;  // страница сделает всё сама
    if (inFlight.current) return;

    const token = getPendingRedeemToken();
    if (!token) return;

    inFlight.current = true;
    let cancelled = false;

    apiFetch('/api/billing/redeem', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }),
    })
      .then(async (r) => {
        if (cancelled) return;
        if (!r.ok) {
          // Токен протух / отозван / исчерпан — второй попытки не будет,
          // иначе висел бы в хранилище и дёргал бы API на каждой навигации.
          // 5xx оставляем: сеть или бэк могли моргнуть, повторим позже.
          if (r.status < 500) clearPendingRedeemToken();
          return;
        }
        clearPendingRedeemToken();
        await refreshUser();          // роль/тариф в шапке обновятся сразу
        if (!cancelled) navigate('/billing/redeem', { replace: true });
      })
      .catch(() => { /* сеть моргнула — токен остаётся, применим на следующей навигации */ })
      .finally(() => { inFlight.current = false; });

    return () => { cancelled = true; };
  }, [isAuthenticated, pathname, navigate, refreshUser]);

  return null;
}
