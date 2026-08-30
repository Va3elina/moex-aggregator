/**
 * AnomalyToast — одна карточка всплывающей аномалии (правый-нижний угол десктоп /
 * над нижней навигацией мобилка). Чисто презентационный: вся логика очереди/
 * троттлинга/таймера — в ToastHost. Стиль — семантические токены темы
 * (bg, text, success, danger), темо-адаптивно (editorial light/dark).
 *
 * Тип различаем ЦВЕТОМ точки (на сайте можно, в отличие от одноцветных TG-эмодзи):
 * оранжевый = открытый интерес, циан = фонды. Сторона — стрелка + цвет (зелёный
 * вверх / красный вниз). Замок на «Открыть график» = цель закрыта тарифом (клик
 * даст апселл + дефолт — решает сервер через link_required_tier).
 */
import { TrendingUp, TrendingDown, X, Lock, Bell, ArrowRight, Check, ExternalLink } from 'lucide-react';
import type { AnomalyItem } from '../../services/api';
import { tradeDateLabel } from './anomalyActions';

const TYPE_META: Record<string, { label: string; dot: string }> = {
  oi_move:         { label: 'Открытый интерес', dot: 'var(--accent-orange, #FF9100)' },
  oi_zscore:       { label: 'Открытый интерес', dot: 'var(--accent-orange, #FF9100)' },
  oi_participants: { label: 'Открытый интерес', dot: 'var(--accent-orange, #FF9100)' },
  funds_flow:      { label: 'Деньги в фондах',  dot: 'var(--accent-cyan, #00BCD4)' },
  promo:           { label: 'Канал Фрейма',     dot: 'var(--accent, #00E676)' },
  data_release:    { label: 'Новые данные',     dot: 'var(--accent, #00E676)' },
};

interface Props {
  item: AnomalyItem;
  progress: number;          // 0..1 — заполнение полоски автоскрытия
  subscribing?: boolean;
  subscribed?: boolean;
  onOpen: () => void;
  onSubscribe: () => void;
  onClose: () => void;
  onPause: () => void;       // ховер — пауза таймера
  onResume: () => void;
}

export function AnomalyToast({
  item, progress, subscribing, subscribed,
  onOpen, onSubscribe, onClose, onPause, onResume,
}: Props) {
  const meta = TYPE_META[item.type] ?? { label: 'Сигнал', dot: 'var(--text-muted)' };
  const isPromo = item.type === 'promo';
  // «Инфо»-типы без направления/подписки (промо канала, релиз данных): вместо
  // строки «стрелка + ×N» показываем текст context, кнопку «Получать» прячем.
  const isInfo = isPromo || item.type === 'data_release';
  const up = item.direction === 'up';
  const dirColor = isInfo ? 'var(--accent, #00E676)'
    : (up ? 'var(--success, #00E676)' : 'var(--danger, #FF5252)');
  const DirIcon = up ? TrendingUp : TrendingDown;
  const sev = item.severity_value != null ? `×${item.severity_value.toFixed(1)}` : '';
  // context = «×5.2 к обычному дневному шагу» → отрезаем ведущий ×N (он рендерится
  // отдельно цветом), оставляем хвост «к обычному дневному шагу».
  const tail = (item.context || '').replace(/^×[\d.,]+\s*/, '');
  const assetLine = item.type === 'funds_flow'
    ? (item.asset_name || item.asset_id)
    : [item.asset_id, item.asset_name].filter(Boolean).join(' · ');
  // Торговый день движения («за 26 июня») — данные позиций/СЧА выходят с лагом ~T+1,
  // поэтому время обнаружения тоста ≠ дата самих торгов. Показываем явно.
  const dateLbl = tradeDateLabel(item.signal_date);
  const metaLine = [assetLine, dateLbl ? `за ${dateLbl}` : ''].filter(Boolean).join(' · ');
  const locked = !!item.link_required_tier;

  return (
    <div
      role="status"
      onMouseEnter={onPause}
      onMouseLeave={onResume}
      style={{
        position: 'relative', overflow: 'hidden', width: '100%',
        background: 'var(--bg-secondary, #15181C)',
        border: '0.5px solid var(--border-color, rgba(255,255,255,0.10))',
        borderRadius: 12, padding: '13px 14px 13px',
        boxShadow: '0 8px 28px rgba(0,0,0,0.40)',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 7 }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 6, color: 'var(--text-secondary)', fontSize: 12 }}>
          <span style={{ width: 6, height: 6, borderRadius: '50%', background: meta.dot }} />
          {meta.label}
        </span>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {item.mine && (
            <span style={{
              background: 'color-mix(in srgb, var(--success, #00E676) 14%, transparent)',
              color: 'var(--success, #00E676)', fontSize: 11, padding: '2px 7px', borderRadius: 6,
            }}>
              ваш сигнал
            </span>
          )}
          <button onClick={onClose} aria-label="Закрыть" style={iconBtn}>
            <X size={16} color="var(--text-muted)" />
          </button>
        </div>
      </div>

      <div style={{ color: 'var(--text-primary)', fontSize: 15, fontWeight: 500, lineHeight: 1.35, marginBottom: 5 }}>
        {item.headline}
      </div>

      {isInfo ? (
        item.context && (
          <div style={{ color: 'var(--text-secondary)', fontSize: 13, lineHeight: 1.4, marginBottom: 12 }}>
            {item.context}
          </div>
        )
      ) : (
        <>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
            <DirIcon size={16} color={dirColor} />
            {sev && <span style={{ color: dirColor, fontSize: 13, fontWeight: 500 }}>{sev}</span>}
            {tail && <span style={{ color: 'var(--text-secondary)', fontSize: 13 }}>{tail}</span>}
          </div>
          {metaLine && (
            <div style={{ color: 'var(--text-muted)', fontSize: 12, marginBottom: 12 }}>{metaLine}</div>
          )}
        </>
      )}

      <div style={{ display: 'flex', gap: 8 }}>
        <button
          onClick={onOpen}
          style={{
            flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 5,
            background: locked ? 'transparent' : 'color-mix(in srgb, var(--success, #00E676) 8%, transparent)',
            border: `0.5px solid ${locked ? 'rgba(255,145,0,0.45)' : 'color-mix(in srgb, var(--success, #00E676) 40%, transparent)'}`,
            color: locked ? 'var(--accent-orange, #FFB454)' : 'var(--text-primary)',
            fontSize: 13, fontWeight: 500, padding: '7px 10px', borderRadius: 8, cursor: 'pointer',
          }}
        >
          {locked && !isInfo && <Lock size={14} />}
          {isPromo ? 'Открыть канал' : isInfo ? 'Открыть' : 'Открыть график'}
          {isPromo ? <ExternalLink size={15} /> : (!locked && <ArrowRight size={15} />)}
        </button>
        {!isInfo && (
        <button
          onClick={onSubscribe}
          disabled={subscribing || subscribed}
          aria-label="Получать сигнал"
          style={{
            display: 'flex', alignItems: 'center', gap: 5, background: 'transparent',
            border: '0.5px solid var(--border-color, rgba(255,255,255,0.14))',
            color: subscribed ? 'var(--success, #00E676)' : 'var(--text-secondary)',
            fontSize: 13, padding: '7px 11px', borderRadius: 8,
            cursor: subscribing || subscribed ? 'default' : 'pointer', opacity: subscribing ? 0.6 : 1,
          }}
        >
          {subscribed ? <Check size={15} /> : <Bell size={15} />}
          {subscribed ? 'Готово' : 'Получать'}
        </button>
        )}
      </div>

      <div style={{
        position: 'absolute', left: 0, bottom: 0, height: 2,
        width: `${Math.round(progress * 100)}%`, background: dirColor, opacity: 0.7,
        transition: 'width 0.1s linear',
      }} />
    </div>
  );
}

const iconBtn: React.CSSProperties = {
  display: 'flex', alignItems: 'center', justifyContent: 'center',
  background: 'transparent', border: 'none', padding: 0, cursor: 'pointer',
};
