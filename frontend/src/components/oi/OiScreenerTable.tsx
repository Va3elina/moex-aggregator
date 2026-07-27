/**
 * OiScreenerTable — вкладка «Скринер сигналов» на /oi (десктоп, v2 по макету
 * Вадима 2026-07, «Скринер сигналов standalone»).
 *
 * Лента сигналов по открытому интересу «как Telegram-алерты»: комета позиции
 * группы на оси −100…+100 и ATR14-кратность дневного движения. Данные дневные
 * (T+1). Числа перекоса из строк убраны — точные значения в подсказке кометы;
 * «Сила» (ATR-кратность) — крупное число ×4,3 с градиентом «теплоты» по
 * размаху дня. Заголовок «×N» заменён на «Сила»: аббревиатура ничего не
 * сообщала, а само число уже читается как кратность.
 *
 * Тулбар: Физлица/Юрлица (SegmentedControl — паритет с вкладкой графика),
 * Категории — Dropdown, ★ Избранные со счётчиком, справа — свежесть данных.
 * Порога ≥2×/3×/5× нет: лента всегда отсортирована по силе (резкие сверху,
 * тихие приглушены). Сортировки по перекосу тоже нет — «где стоят» это не
 * повод для ранжирования ленты СОБЫТИЙ, для этого есть вкладка графика.
 *
 * Рекорды перекоса — оранжевая метка периода у сигнала («↑3мес»: пробили
 * максимум за 3 месяца) + акцентная левая грань строки. Исторический экстремум
 * сырой позиции (контракты) — залитая метка «всё» (сильнейший сигнал).
 * Тон текстов — «резкое движение/аномалия», НЕ «где заработать».
 */
import { useEffect, useMemo, useState } from 'react';
import type { CSSProperties, MouseEvent } from 'react';
import { Star } from 'lucide-react';
import InstrumentIcon from '../InstrumentIcon';
import SegmentedControl from '../SegmentedControl';
import Dropdown from '../Dropdown';
import PositionComet, { ratioHeat } from './PositionComet';
import { getOiScreener, type OiScreenerRow } from '../../services/api';
import { usePersistedState } from '../../hooks/usePersistedState';

type Clgroup = 'FIZ' | 'YUR';

// Реальные значения instruments.group (как в пикере активов).
const GROUP_OPTIONS = [
  { key: 'all', label: 'Все категории' },
  { key: 'Индексы', label: 'Индексы' },
  { key: 'Валюта', label: 'Валюта' },
  { key: 'Сырьё', label: 'Сырьё' },
  { key: 'Акции', label: 'Акции' },
  { key: 'Крипто', label: 'Крипто' },
] as const;

const FAVORITES_KEY = 'favoriteInstruments'; // общий ключ с пикером активов

function fmtRatio(r: number): string {
  return r.toLocaleString('ru-RU', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '×';
}
/** «1 актив / 2 актива / 5 активов» */
function pluralAssets(n: number): string {
  const mod10 = n % 10, mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return `${n} актив`;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return `${n} актива`;
  return `${n} активов`;
}

const STATUS_RANK: Record<OiScreenerRow['status'], number> = {
  sharp: 0, normal: 1, illiquid: 2, nodata: 3,
};

const MONO: CSSProperties = {
  fontFamily: 'var(--font-mono, ui-monospace, monospace)',
  fontVariantNumeric: 'tabular-nums',
};

interface Props {
  /** Клик по строке — открыть график этого актива с настройками скринера
   *  (та же группа физ/юр, дневной ТФ, период 1 год). */
  onSelect: (sectype: string, clgroup: Clgroup) => void;
  /** Открыть модалку алерта по активу (из баннера после добавления в
   *  избранное). Не передан → блок алертов выключен, баннер не предлагаем. */
  onRequestAlert?: (sectype: string, name: string, clgroup: Clgroup) => void;
}

export default function OiScreenerTable({ onSelect, onRequestAlert }: Props) {
  const [rows, setRows] = useState<OiScreenerRow[] | null>(null);
  const [signalDate, setSignalDate] = useState<string | null>(null);
  const [intradayDate, setIntradayDate] = useState<string | null>(null);  // свежайший интрадей-бар
  const [minPart, setMinPart] = useState<number>(50);   // порог ликвидности группы (из ответа)
  const [error, setError] = useState(false);
  // Все режимы тулбара запоминаются в localStorage — скринер открывается в том
  // же виде, что оставил юзер (группа, категория, избранные, направление).
  const [clgroup, setClgroup] = usePersistedState<Clgroup>('frame:oi-screener:clgroup', 'FIZ');
  const [group, setGroup] = usePersistedState<string>('frame:oi-screener:group', 'all');
  const [onlyFav, setOnlyFav] = usePersistedState<boolean>('frame:oi-screener:onlyfav', false);
  // Баннер «★ добавлено — поставить алерт?» после добавления в избранное.
  const [alertPrompt, setAlertPrompt] = useState<{ sectype: string; name: string } | null>(null);
  // Единственная сортировка — по силе; тумблер меняет лишь направление.
  const [sortDir, setSortDir] = usePersistedState<1 | -1>('frame:oi-screener:sortdir', -1);
  const [favorites, setFavorites] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem(FAVORITES_KEY);
      return saved ? JSON.parse(saved) : [];
    } catch { return []; }
  });

  useEffect(() => {
    let cancelled = false;
    setRows(null);
    setError(false);
    getOiScreener(clgroup)
      .then((r) => { if (!cancelled) { setRows(r.rows); setSignalDate(r.signal_date); setIntradayDate(r.intraday_date); setMinPart(r.min_part); } })
      .catch(() => { if (!cancelled) setError(true); });
    return () => { cancelled = true; };
  }, [clgroup]);

  const toggleFavorite = (sectype: string, e: MouseEvent) => {
    e.stopPropagation();
    const adding = !favorites.includes(sectype);
    setFavorites((prev) => {
      const next = prev.includes(sectype) ? prev.filter((s) => s !== sectype) : [...prev, sectype];
      localStorage.setItem(FAVORITES_KEY, JSON.stringify(next));
      return next;
    });
    // При добавлении в избранное — предложить алерт (если блок алертов включён).
    if (adding && onRequestAlert) {
      const row = rows?.find((r) => r.sectype === sectype);
      setAlertPrompt({ sectype, name: row?.name || sectype });
    } else if (!adding) {
      setAlertPrompt((p) => (p?.sectype === sectype ? null : p));
    }
  };

  const visible = useMemo(() => {
    if (!rows) return [];
    const favSet = new Set(favorites);
    let out = rows.filter((r) =>
      (group === 'all' || r.group === group) &&
      (!onlyFav || favSet.has(r.sectype)),
    );
    // Полосы по статусу (sharp → normal → illiquid → nodata), избранные наверх
    // внутри полосы, внутри — по силе. sortDir = −1 → сильные сверху (дефолт).
    out = [...out].sort((a, b) => {
      const sr = STATUS_RANK[a.status] - STATUS_RANK[b.status];
      if (sr !== 0) return sr;
      const fav = Number(favSet.has(b.sectype)) - Number(favSet.has(a.sectype));
      if (fav !== 0) return fav;
      return sortDir * ((a.ratio ?? -1) - (b.ratio ?? -1));
    });
    return out;
  }, [rows, favorites, group, sortDir, onlyFav]);

  // Калибровка визуальных кодировок «по дню»: размах ×N sharp-строк (размер
  // головы кометы + градиент цвета числа) и максимум |Δ п.п.| (длина хвоста).
  // Считается по ВИДИМОЙ выборке — фильтр категории меняет и калибровку,
  // строки сравниваются с соседями по экрану.
  const stats = useMemo(() => {
    const sharpRatios = visible.filter((r) => r.status === 'sharp' && r.ratio != null).map((r) => r.ratio!);
    const deltas = visible
      .filter((r) => r.net_pct != null && r.net_pct_prev != null)
      .map((r) => Math.abs(r.net_pct! - r.net_pct_prev!));
    return {
      ratioLo: sharpRatios.length ? Math.min(...sharpRatios) : null,
      ratioHi: sharpRatios.length ? Math.max(...sharpRatios) : null,
      maxAbsDelta: deltas.length ? Math.max(...deltas) : null,
    };
  }, [visible]);

  const _fmtD = (d: string) => new Date(d + 'T00:00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' });
  // Честная свежесть: дневные данные T+1 (signal_date) + свежий интрадей-бар
  // (intraday_date, если новее дневной свечи). Раньше метка = только дневная
  // («3 июля»), а значения — интрадей (6-е) → это вводило в заблуждение.
  const dateLabel = signalDate
    ? (intradayDate && intradayDate > signalDate
        ? `дневные за ${_fmtD(signalDate)} · интрадей за ${_fmtD(intradayDate)}`
        : `по данным за ${_fmtD(signalDate)}`)
    : null;

  // Рекорд перекоса → компактная оранжевая метка периода у сигнала: «↑3мес» =
  // пробили максимум за 3 месяца. Редкость кодируется ДЛИНОЙ периода в метке.
  const PERIOD_WORD: Record<string, string> = {
    all: 'за всё время', '5y': 'за 5 лет', '4y': 'за 4 года',
    '3y': 'за 3 года', '2y': 'за 2 года', '1y': 'за год',
    '6m': 'за полгода', '3m': 'за 3 месяца', '1m': 'за месяц',
  };
  const PERIOD_CHIP: Record<string, string> = {
    all: 'всё', '5y': '5лет', '4y': '4года', '3y': '3года', '2y': '2года',
    '1y': '1год', '6m': '6мес', '3m': '3мес', '1m': '1мес',
  };
  // net_record (истор. экстремум сырой позиции, контракты) сильнее рекорда
  // перекоса и покрывает его → показываем одну метку. Заливка = all-time.
  const recordChip = (r: OiScreenerRow) => {
    const rec = r.net_record
      ? { high: r.net_record.kind === 'high', period: 'all', solid: true,
          title: `Чистая позиция (в контрактах) — исторический ${r.net_record.kind === 'high' ? 'максимум' : 'минимум'} за всё время наблюдений.` }
      : r.record
        ? { high: r.record.kind === 'high', period: r.record.period, solid: r.record.period === 'all',
            title: `Перекос пробил ${r.record.kind === 'high' ? 'максимум (рекордный лонг)' : 'минимум (рекордный шорт)'} ${PERIOD_WORD[r.record.period]}.` }
        : null;
    if (!rec) return null;
    return (
      <span
        title={rec.title}
        style={{
          ...MONO, display: 'inline-flex', alignItems: 'baseline', gap: 3,
          flexShrink: 0, fontSize: 'var(--fs-xs)', fontWeight: 700,
          whiteSpace: 'nowrap', cursor: 'help',
          ...(rec.solid
            // Исторический экстремум — сильнейший сигнал: залитая пилюля.
            ? { background: 'var(--accent)', color: 'var(--text-inverse)',
                padding: '1px 7px', borderRadius: 999 }
            // Периодный рекорд — тихая метка: accent-текст с пунктиром.
            : { color: 'var(--accent)', paddingBottom: 1,
                borderBottom: '1px dashed color-mix(in srgb, var(--accent) 55%, transparent)' }),
        }}
      >
        <span>{rec.high ? '↑' : '↓'}</span>
        <span>{PERIOD_CHIP[rec.period]}</span>
      </span>
    );
  };

  const groupWord = clgroup === 'FIZ' ? 'физлица' : 'юрлица';
  const mirrorWord = clgroup === 'FIZ' ? 'юрлица' : 'физлица';
  // Родительный для заголовка колонки: «Позиция физлиц», не «физлица».
  const groupGen = clgroup === 'FIZ' ? 'физлиц' : 'юрлиц';

  // ── Сила: крупное mono-число «×4,3», цвет — градиент «теплоты» по размаху
  // дня (слабейший видимый sharp → приглушённый, сильнейший → чистый accent).
  // Абсолютные пороги 3×/5× не различали реальный разброс дня 2,9…4,3.
  const ratioCell = (r: OiScreenerRow) => {
    if (r.ratio == null) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
    if (r.status !== 'sharp') {
      return (
        <span style={{ ...MONO, fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-muted)' }}>
          {'×' + r.ratio.toFixed(1).replace('.', ',')}
        </span>
      );
    }
    const heat = stats.ratioLo != null && stats.ratioHi != null
      ? ratioHeat(r.ratio, stats.ratioLo, stats.ratioHi)
      : 1;
    return (
      <span
        title={`Дневное изменение позиции ${groupGen} в ${fmtRatio(r.ratio)} сильнее их обычного дневного изменения за последние 14 дней (ATR-14)`}
        style={{
          ...MONO, fontSize: 'var(--fs-lg)', fontWeight: 800, letterSpacing: '-0.02em',
          whiteSpace: 'nowrap',
          color: `color-mix(in oklab, var(--accent) ${Math.round(30 + heat * 70)}%, var(--text-secondary))`,
        }}
      >
        {'×' + r.ratio.toFixed(1).replace('.', ',')}
      </span>
    );
  };

  // Текст сигнала: только глагол + нога («Набрали лонг»). Кратность — колонка
  // ×N, точные проценты — подсказка кометы; здесь полная трактовка в title.
  const signalText = (r: OiScreenerRow) => {
    if (r.status === 'sharp' && r.ratio != null && r.direction) {
      // Глагол — по МОДУЛЮ чистой позиции: |net| вырос → «набрали/нарастили»,
      // уменьшился → «сократили» (у шорт-стороны рост net = сокращение шорта).
      // Нога — по знаку net. Так физики и юрики на CNYRUBF оба «сократили».
      const netLong = r.net >= 0;
      const grewExposure = netLong === (r.direction === 'up');
      const verb = netLong
        ? (grewExposure ? 'Набрали лонг' : 'Сократили лонг')
        : (grewExposure ? 'Нарастили шорт' : 'Сократили шорт');
      const cap = (s: string) => s[0].toUpperCase() + s.slice(1);
      const full = `${cap(groupWord)} резко ${grewExposure ? 'нарастили' : 'сократили'} ${netLong ? 'длинную' : 'короткую'} позицию по «${r.name}»: дневное изменение чистой позиции в ${fmtRatio(r.ratio)} сильнее обычного (ATR-14; «резко» — от 2×). Обратная сторона (${mirrorWord}) держит зеркальную позицию.`;
      return (
        <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-primary)', whiteSpace: 'nowrap' }} title={full}>
          {verb}
        </span>
      );
    }
    const note =
      r.status === 'normal'
        ? 'в пределах обычного'
        : r.status === 'illiquid'
          ? `низкая ликвидность (${r.npart})`
          : 'недостаточно данных';
    const noteTitle =
      r.status === 'illiquid'
        ? `На стороне ${groupWord}: ${r.npart} участников — ниже порога ликвидности ${minPart}. Движение по такому контракту считаем шумом, а не сигналом (у юрлиц участников структурно меньше, поэтому порог свой).`
        : r.status === 'nodata'
          ? 'Мало истории для расчёта ATR-14.'
          : 'Дневной сдвиг чистой позиции в пределах обычного (ниже порога «резко» 2×).';
    return (
      <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 500, color: 'var(--text-secondary)' }} title={noteTitle}>{note}</span>
    );
  };

  // Заголовки таблицы — mono uppercase, ЖИРНЫЕ, цвет primary (не блеклые).
  // Строго ОДНА строка на колонку: вторые строки-приписки («−100 шорт · 0 ·
  // лонг +100» под кометой, «размах дня 2,0…3,1×» под ×N) убраны — они
  // не влезали в свои колонки и налезали на соседей, а объясняли то, что
  // и так видно (красная зона слева, зелёная справа) или вовсе служебное
  // (диапазон нормировки). Смысл шкалы остался в подсказках и в туре.
  const headCell: CSSProperties = {
    ...MONO,
    fontSize: 'var(--fs-xs)', fontWeight: 800, letterSpacing: '0.06em',
    textTransform: 'uppercase', color: 'var(--text-primary)',
    textAlign: 'left', whiteSpace: 'nowrap',
  };

  // Актив · Позиция (комета, забирает свободное место) · Сила · Сигнал · ★.
  const gridCols = 'minmax(180px, 222px) minmax(360px, 1fr) 92px minmax(190px, 244px) 44px';

  return (
    <div>
      {/* Баннер-предложение алерта после добавления в избранное (п.3) */}
      {alertPrompt && (
        <div
          style={{
            display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap',
            padding: 'var(--sp-3) var(--sp-4)', marginBottom: 'var(--sp-4)',
            background: 'var(--bg-secondary)', border: '2px solid var(--accent)',
            borderRadius: 12,
          }}
        >
          <Star size={18} fill="var(--accent)" strokeWidth={0} style={{ flexShrink: 0 }} />
          <span style={{ fontSize: 'var(--fs-sm)', minWidth: 0 }}>
            <strong>{alertPrompt.name}</strong> в избранном. Поставьте алерт, чтобы о
            резком движении сообщили сами — не следить вручную.
          </span>
          <div style={{ display: 'flex', gap: 8, marginLeft: 'auto' }}>
            <button
              type="button"
              className="editorial-press rounded-full font-semibold"
              style={{
                fontSize: 'var(--fs-sm)', padding: 'var(--sp-1) var(--sp-4)',
                background: 'var(--accent)', border: '2px solid var(--text-primary)',
                color: 'var(--text-inverse)', cursor: 'pointer', whiteSpace: 'nowrap',
              }}
              onClick={() => { onRequestAlert?.(alertPrompt.sectype, alertPrompt.name, clgroup); setAlertPrompt(null); }}
            >
              Создать алерт
            </button>
            <button
              type="button"
              aria-label="Закрыть"
              onClick={() => setAlertPrompt(null)}
              style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-secondary)', fontSize: 'var(--fs-base)', lineHeight: 1, padding: 4 }}
            >
              ✕
            </button>
          </div>
        </div>
      )}

      {/* Тулбар: группа · Категории ▾ · ★ Избранные, справа — свежесть данных
          (перенесена из футера: это первое, что нужно знать про ленту).
          Порога ≥N× и выбора сортировки нет — лента всегда по силе. */}
      <div data-tour="screener-toolbar" className="flex flex-wrap items-center mb-4 md:mb-6 gap-2 md:gap-3">
        <SegmentedControl<Clgroup>
          options={[
            { key: 'FIZ', label: 'Физлица' },
            { key: 'YUR', label: 'Юрлица', title: 'Сигналы юрлиц зеркальны физлицам по кратности, но проценты и ликвидность — свои' },
          ]}
          value={clgroup}
          onChange={setClgroup}
        />
        <Dropdown<string>
          options={GROUP_OPTIONS.map((g) => ({ key: g.key, label: g.label }))}
          value={group}
          onChange={setGroup}
        />
        {/* Фильтр по избранным активам (+ счётчик) */}
        <button
          type="button"
          data-tour="screener-favorites"
          onClick={() => setOnlyFav((v) => !v)}
          aria-pressed={onlyFav}
          title="Показать только избранные активы"
          className="editorial-press rounded-full font-semibold inline-flex items-center"
          style={{
            gap: 6,
            fontSize: 'var(--fs-sm)',
            padding: 'var(--sp-2) var(--sp-3)',
            border: '2px solid var(--text-primary)',
            background: onlyFav ? 'var(--accent)' : 'var(--bg-secondary)',
            color: onlyFav ? 'var(--text-inverse)' : 'var(--text-primary)',
            cursor: 'pointer',
          }}
        >
          <Star size={15} fill={onlyFav ? 'var(--text-inverse)' : 'none'} strokeWidth={2.2} />
          Избранные
          {favorites.length > 0 && (
            <span style={{ ...MONO, fontSize: 'var(--fs-xs)', fontWeight: 700, opacity: 0.75 }}>
              {favorites.length}
            </span>
          )}
        </button>
        {/* Свежесть данных — честная метка (дневные T+1 + интрадей, если новее) */}
        {dateLabel && (
          <span className="ml-auto inline-flex items-center" style={{ gap: 7, fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>
            <span style={{ width: 7, height: 7, borderRadius: 999, background: 'var(--oi-green)', flexShrink: 0 }} />
            {dateLabel}
          </span>
        )}
      </div>

      {/* Таблица */}
      <div data-tour="screener-table" style={{ overflowX: 'auto' }}>
        <div style={{ minWidth: 1000 }}>
          {/* Заголовки — одна строка на колонку, без вторых строк-приписок */}
          <div style={{ display: 'grid', gridTemplateColumns: gridCols, gap: 16, padding: '12px 18px', borderBottom: '2px solid var(--text-primary)', alignItems: 'center' }}>
            <span style={headCell}>Актив</span>
            <span style={headCell} title={`Комета на оси перекоса −100…+100: слева полный шорт, по центру ноль (поровну), справа полный лонг. Голова = где ${groupWord} стоят сейчас, хвост = сдвиг за день, размер головы = сила движения. Точные проценты — при наведении на строку.`}>
              Позиция {groupGen}
            </span>
            <button
              type="button"
              style={{ ...headCell, cursor: 'pointer', background: 'none', border: 'none', padding: 0 }}
              onClick={() => setSortDir((d) => (d === -1 ? 1 : -1))}
              title="Во сколько раз дневное изменение позиции сильнее обычного для этого актива (ATR-14). Клик — перевернуть порядок."
            >
              Сила{sortDir === -1 ? ' ▼' : ' ▲'}
            </button>
            <span style={headCell}>Сигнал</span>
            <span />
          </div>

          {/* Состояния */}
          {error && (
            <div style={{ padding: 48, textAlign: 'center', color: 'var(--text-secondary)', fontSize: 'var(--fs-base)' }}>
              Не удалось загрузить данные — попробуйте обновить страницу
            </div>
          )}
          {!error && rows === null && (
            <div style={{ padding: 48, textAlign: 'center', color: 'var(--text-secondary)', fontSize: 'var(--fs-base)' }}>Загрузка…</div>
          )}
          {!error && rows !== null && visible.length === 0 && (
            <div style={{ padding: '40px 18px', textAlign: 'center' }}>
              <div style={{ color: 'var(--text-primary)', fontSize: 'var(--fs-lg)', fontWeight: 700 }}>
                {onlyFav ? 'Среди избранных активов пусто' : 'Нет активов по фильтру'}
              </div>
              <div style={{ marginTop: 6, color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                Попробуйте снять фильтры{dateLabel ? ` · ${dateLabel}` : ''}
              </div>
            </div>
          )}

          {/* Строки */}
          {!error && visible.map((r) => {
            const isFav = favorites.includes(r.sectype);
            return (
              <div
                key={r.sectype}
                role="button"
                tabIndex={0}
                onClick={() => onSelect(r.sectype, clgroup)}
                onKeyDown={(e) => { if (e.key === 'Enter') onSelect(r.sectype, clgroup); }}
                className="oi-screener-row"
                style={{
                  display: 'grid', gridTemplateColumns: gridCols, gap: 16,
                  alignItems: 'center', padding: '12px 18px 12px 15px',
                  borderBottom: '0.5px solid var(--border-color, rgba(128,128,128,0.25))',
                  // Акцентная левая грань = в строке рекорд (перекоса или позиции).
                  borderLeft: `3px solid ${r.record || r.net_record ? 'var(--accent)' : 'transparent'}`,
                  cursor: 'pointer',
                  opacity: r.status === 'illiquid' || r.status === 'nodata' ? 0.55 : 1,
                }}
              >
                {/* Актив */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 12, minWidth: 0 }}>
                  <InstrumentIcon sectype={r.sectype} size={32} />
                  <div style={{ minWidth: 0 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, minWidth: 0 }}>
                      <span style={{ fontWeight: 700, fontSize: 'var(--fs-base)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {r.name}
                      </span>
                      {/* «!» на EOD-only активах (как в пикере): данные только на
                          конец дня, внутридневных (5м/1ч) нет → сигнал T+1. */}
                      {!r.has_intraday && (
                        <span
                          title="Данные позиций обновляются только на конец дня — внутридневных (5м/1ч) нет. Сигнал по этому активу — на следующий день (T+1)."
                          style={{
                            flexShrink: 0, width: 16, height: 16, borderRadius: 999,
                            border: '1px solid var(--text-muted)', color: 'var(--text-secondary)',
                            display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                            fontSize: 11, fontWeight: 700, lineHeight: 1, cursor: 'help',
                          }}
                        >
                          !
                        </span>
                      )}
                    </div>
                    <div style={{ ...MONO, fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', letterSpacing: '0.04em' }}>
                      {r.front_secid || r.sectype}
                    </div>
                  </div>
                </div>

                {/* Позиция — комета (точные проценты в её подсказке) */}
                <PositionComet
                  netPct={r.net_pct}
                  netPctPrev={r.net_pct_prev}
                  ratio={r.ratio}
                  status={r.status}
                  ratioLo={stats.ratioLo}
                  ratioHi={stats.ratioHi}
                  maxAbsDelta={stats.maxAbsDelta}
                />

                {/* Сила — крупное число ×4,3, яркость по размаху дня */}
                <div>{ratioCell(r)}</div>

                {/* Сигнал: глагол + метка рекорда («↑3мес») */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 9, minWidth: 0 }}>
                  {signalText(r)}
                  {recordChip(r)}
                </div>

                {/* ⭐ */}
                <button
                  type="button"
                  onClick={(e) => toggleFavorite(r.sectype, e)}
                  aria-label={isFav ? 'Убрать из избранного' : 'В избранное'}
                  style={{
                    background: 'none', border: 'none', cursor: 'pointer', padding: 6,
                    color: isFav ? 'var(--accent)' : 'var(--text-secondary)', lineHeight: 0,
                  }}
                >
                  <Star size={18} fill={isFav ? 'var(--accent)' : 'none'} strokeWidth={2} />
                </button>
              </div>
            );
          })}
        </div>
      </div>

      {/* Футер: как читать силу и где точные числа (дата уехала в тулбар) */}
      {!error && rows !== null && (
        <div className="flex items-center justify-between flex-wrap" style={{ gap: 8, padding: '14px 4px 0', fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
          <span>
            Сила ×4,3 значит: за день позиция сдвинулась в 4,3 раза сильнее, чем
            этот актив двигается обычно (среднее за 14 дней). Точный перекос и
            дневная дельта — при наведении на комету.
          </span>
          <span style={MONO}>{pluralAssets(visible.length)} · {groupWord}</span>
        </div>
      )}
    </div>
  );
}
