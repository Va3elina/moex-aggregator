// Подтверждение входа в кабинет бота (/tg/approve?c=код) — открывается в ОБЫЧНОМ браузере из окна Telegram.
// Человек здесь уже вошёл на сайт (или входит с сохранёнными паролями) и разрешает окну бота войти под собой:
// api/services/browser_login.py. После подтверждения возвращаем его в Telegram.
import { useEffect, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { apiFetch } from '../../services/api';
import './tg.css';

const BOT = 'framesignalbot';
type Stage = 'check' | 'ask' | 'stale' | 'done';

export default function TgApprovePage() {
  const [params] = useSearchParams();
  const code = params.get('c') ?? '';
  const navigate = useNavigate();
  const { user, loading, isAuthenticated } = useAuth();
  const [stage, setStage] = useState<Stage>('check');
  const [who, setWho] = useState<{ username: string | null; name: string | null } | null>(null);
  const [pin, setPin] = useState('');
  const [busy, setBusy] = useState(false);

  // Не вошёл — на обычную страницу входа сайта, после входа вернёмся сюда же
  useEffect(() => {
    if (!loading && !isAuthenticated) navigate(`/login?next=${encodeURIComponent(`/tg/approve?c=${code}`)}`, { replace: true });
  }, [loading, isAuthenticated, code, navigate]);

  useEffect(() => {
    if (!isAuthenticated) return;
    let dead = false;
    (async () => {
      try {
        const r = await apiFetch(`/api/tg-login/info?code=${encodeURIComponent(code)}`);
        const j = r.ok ? await r.json() : null;
        if (dead) return;
        setWho(j?.who ?? null);
        setStage(j ? 'ask' : 'stale');
      } catch { if (!dead) setStage('stale'); }
    })();
    return () => { dead = true; };
  }, [isAuthenticated, code]);

  const approve = async () => {
    setBusy(true);
    try {
      const r = await apiFetch('/api/tg-login/approve', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ code }) });
      const j = r.ok ? await r.json() : null;
      setPin(j?.pin ?? '');      // код видит только подтвердивший; его вводят в окне бота
      setStage(j?.pin ? 'done' : 'stale');
    } catch { setStage('stale'); } finally { setBusy(false); }
  };

  const mark = (
    <svg width="72" height="72" viewBox="0 0 32 32" aria-hidden="true"><rect width="32" height="32" rx="8" fill="var(--accent)" /><path d="M7 7h6.5v2.6H9.6v3.9H7zM25 7h-6.5v2.6h3.9v3.9H25zM7 25h6.5v-2.6H9.6v-3.9H7zM25 25h-6.5v-2.6h3.9v-3.9H25z" fill="#fff" /></svg>
  );

  return (
    <div className="tgs">
      <div className="tgs-col">
        <div className="tgs-pg">
          {stage === 'ask' && user && (
            <div className="tgs-welcome">
              {mark}
              <h1>Войти в кабинет в Telegram?</h1>
              <p>Аккаунт <b>{user.requires_email_setup ? (user.display_name || 'Фрейм') : user.email}</b>{who?.username ? <> откроется в Telegram у <b>@{who.username}</b>.</> : who?.name ? <> откроется в Telegram у пользователя «{who.name}» (ник не задан).</> : <> откроется в окне бота Фрейма в Telegram.</>}</p>
              <button type="button" className="tgs-pr" disabled={busy} onClick={approve}>Подтвердить</button>
              <button type="button" className="tgs-alt" onClick={() => navigate('/', { replace: true })}>Отмена</button>
              <div className="tgs-f" style={{ textAlign: 'center', padding: 0 }}>Подтверждайте, только если вы сами только что нажали «Войти» в боте. Ссылку прислал кто-то другой — нажмите «Отмена».</div>
            </div>
          )}
          {stage === 'done' && (
            <div className="tgs-welcome">
              {mark}
              <h1>Введите код в окне бота</h1>
              <p>Вернитесь в Telegram и введите эти четыре цифры. Никому их не сообщайте.</p>
              <div className="tgs-pin"><span>Код</span><b>{pin}</b></div>
              <a className="tgs-pr tgs-btn-link" href={`https://t.me/${BOT}`}>Вернуться в Telegram</a>
            </div>
          )}
          {stage === 'stale' && (
            <div className="tgs-welcome">
              {mark}
              <h1>Ссылка устарела</h1>
              <p>Вернитесь в бота и нажмите «Войти» ещё раз.</p>
              <a className="tgs-pr tgs-btn-link" href={`https://t.me/${BOT}`}>Открыть бота</a>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
