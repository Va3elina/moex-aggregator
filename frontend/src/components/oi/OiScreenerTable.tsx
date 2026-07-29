/**
 * OiScreenerTable — вкладка «Скринер сигналов» на /oi (десктоп, v2 по макету
 * Вадима 2026-07, «Скринер сигналов standalone»).
 *
 * Лента сигналов по открытому интересу «как Telegram-алерты»: комета позиции
 * группы на оси −100…+100 и кратность движения к норме. Данные дневные (T+1).
 *
 * Горизонт (тумблер «День» / «2 недели») переключает ЛЕНТУ ЦЕЛИКОМ, а не
 * добавляет колонку: на среднесроке и сила, и хвост кометы, и все дельты
 * считаны за 14 торговых дней, поэтому тексты («за день» → «за 2 недели»,
 * «вчера» → «2 недели назад») и порог «резко» (2× → 3×) едут вместе с ним. Числа перекоса из строк убраны — точные значения в подсказке кометы;
 * «Сила» (ATR-кратность) — пилюля-бейдж «4,3×» с тремя абсолютными ступенями от
 * порога «резко»: ×1 контур цветом текста, ×1,5 контур accent, ×2,5 заливка
 * accent с кольцом. Относительный градиент «теплоты» пробовали и убрали: он
 * красил лидера тихого дня как настоящий выброс.
 * Заголовок колонки — «Сила»: аббревиатура «×N» ничего не
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
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import type { CSSProperties, MouseEvent } from 'react';
import { Star } from 'lucide-react';
import InstrumentIcon from '../InstrumentIcon';
import SegmentedControl from '../SegmentedControl';
import Dropdown from '../Dropdown';
import PositionComet from './PositionComet';
import { getOiScreener, type OiScreenerRow, type OiScreenerHorizon } from '../../services/api';
import { usePersistedState } from '../../hooks/usePersistedState';

type Clgroup = 'FIZ' | 'YUR';

// Горизонт ленты. Это ДВА РАЗНЫХ СПИСКА, а не две колонки одного: тумблер
// переключает ленту целиком, вместе со всеми дельтами и хвостами комет.
// Подписи конкретные («День» / «2 недели»), а не «кратко/средне»: юзеру важно
// не как это называется, а за какой срок посчитано движение.
const HORIZON_OPTIONS = [
  { key: 'short' as const, label: 'День', title: 'Краткосрочные сигналы: движение позиции за один день против её обычного дневного размаха (ATR-14). Это те же сигналы, что уходят в Telegram-алерты.' },
  { key: 'medium' as const, label: '2 недели', title: 'Среднесрочные сигналы: сдвиг позиции за 14 торговых дней против того, сколько актив обычно проходит за такой срок. Ловит то, что копилось неделями и в дневной ленте не видно.' },
];

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

const MONO: CSSProperties = {
  fontFamily: 'var(--font-mono, ui-monospace, monospace)',
  fontVariantNumeric: 'tabular-nums',
};

// Fade-out длинных имён вместо троеточия — тот же приём, что в таблице фондов
// (FundsTable.FadedName): текст плавно растворяется у правого края, полное имя
// в подсказке. Маску вешаем ТОЛЬКО когда текст реально обрезан, иначе она
// замазывает хвост и коротких имён (маска считается от бокса, а бокс сжат до
// содержимого). Обрезанность меряем по факту и пересчитываем на ресайз.
const NAME_FADE = 'linear-gradient(to right, #000 calc(100% - 26px), transparent 100%)';

function FadedAssetName({ name }: { name: string }) {
  const ref = useRef<HTMLSpanElement>(null);
  const [clipped, setClipped] = useState(false);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const check = () => setClipped(el.scrollWidth > el.clientWidth + 1);
    check();
    const ro = new ResizeObserver(check);
    ro.observe(el);
    return () => ro.disconnect();
  }, [name]);
  return (
    <span
      ref={ref}
      title={name}
      style={{
        flex: '0 1 auto', minWidth: 0,
        fontWeight: 700, fontSize: 'var(--fs-base)',
        whiteSpace: 'nowrap', overflow: 'hidden',
        ...(clipped ? { maskImage: NAME_FADE, WebkitMaskImage: NAME_FADE } : {}),
      }}
    >
      {name}
    </span>
  );
}

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
  const [horizon, setHorizon] = usePersistedState<OiScreenerHorizon>('frame:oi-screener:horizon', 'short');
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
    getOiScreener(clgroup, horizon)
      .then((r) => { if (!cancelled) { setRows(r.rows); setSignalDate(r.signal_date); setIntradayDate(r.intraday_date); setMinPart(r.min_part); } })
      .catch(() => { if (!cancelled) setError(true); });
    return () => { cancelled = true; };
  }, [clgroup, horizon]);

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
    // Порядок СКВОЗНОЙ по силе. Ни полос по статусу, ни подъёма избранных:
    //  - полосы (sharp → normal → …) ставили тихую sharp-строку выше громкой
    //    обычной, и лента противоречила колонке «Сила»; переворот порядка не
    //    выносил наверх слабейшие — сверху оставалась та же полоса, из-за чего
    //    сортировка по силе читалась как сломанная;
    //  - избранные наверху разрывали ряд по силе в произвольных местах. Для
    //    «только избранные» есть отдельный фильтр ★, он и решает эту задачу.
    // sortDir = −1 → сильные сверху (дефолт).
    out = [...out].sort((a, b) => {
      // Строки без силы держим в конце ОБЕИХ сортировок вручную: через общий
      // ключ (ratio ?? −1) по возрастанию они всплыли бы наверх и первая
      // страница состояла бы из «мало участников».
      if (a.ratio == null || b.ratio == null) {
        if (a.ratio == null && b.ratio == null) return 0;
        return a.ratio == null ? 1 : -1;
      }
      return sortDir * (a.ratio - b.ratio);
    });
    return out;
  }, [rows, favorites, group, sortDir, onlyFav]);

  // Калибровка размера головы кометы: размах ×N sharp-строк по видимой
  // выборке. Длина хвоста не нормируется — она
  // геометрическая, расстояние по шкале от «вчера» к сегодня.
  // Считается по ВИДИМОЙ выборке — фильтр категории меняет и калибровку,
  // строки сравниваются с соседями по экрану.
  const stats = useMemo(() => {
    const sharpRatios = visible.filter((r) => r.status === 'sharp' && r.ratio != null).map((r) => r.ratio!);
    return {
      ratioLo: sharpRatios.length ? Math.min(...sharpRatios) : null,
      ratioHi: sharpRatios.length ? Math.max(...sharpRatios) : null,
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

  const groupWord = clgroup === 'FIZ' ? 'физлица' : 'юрлица';
  const mirrorWord = clgroup === 'FIZ' ? 'юрлица' : 'физлица';
  // Родительный для заголовка колонки: «Позиция физлиц», не «физлица».
  const groupGen = clgroup === 'FIZ' ? 'физлиц' : 'юрлиц';

  // Формулировки горизонта. Лента переключается целиком, поэтому тексты
  // меняются вместе с ней: в среднесрочной нельзя говорить «за день» и
  // «вчера» — там и сигнал, и хвост кометы, и дельты считаны за 2 недели.
  // Порог «резко» тоже свой (3× против 2×, откалиброван по плотности сигналов
  // на проде — см. MED_SHARP_RATIO в api/services/oi_screener.py).
  const isMed = horizon === 'medium';
  const W = {
    over: isMed ? 'за 2 недели' : 'за день',
    prev: isMed ? '2 недели назад' : 'вчера',
    move: isMed ? 'Изменение позиции за 2 недели' : 'Дневное изменение позиции',
    usual: isMed ? 'обычного движения этого актива за 2 недели' : 'обычного дневного движения этого актива',
    thr: isMed ? '3×' : '2×',
    note: isMed ? 'обычные 2 недели' : 'обычный день',
  };
  // Порог «резко» этого горизонта — база ступеней бейджа «Силы» (×1,5 —
  // сильное, ×2,5 — особо сильное). Совпадает с sharp_ratio из ответа бэка.
  const sharpThr = isMed ? 3 : 2;

  // Рекорд перекоса. Окна: квартал, полгода, 1..5 лет, всё время (месячного
  // на бэке нет — слишком часто). Метка достаётся сильнейшему пробитому окну.
  const PERIOD_WORD: Record<string, string> = {
    all: 'за всё время', '5y': 'за 5 лет', '4y': 'за 4 года',
    '3y': 'за 3 года', '2y': 'за 2 года', '1y': 'за год', '6m': 'за полгода',
    '3m': 'за квартал',
  };
  // Рекорд ВМЕСТО обычного сигнала, а не рядом с ним. Раньше метка висела
  // справа от текста, и строка читалась противоречиво: «обычный день · ист
  // макс». Рекорд важнее дневной кратности (позиция может ползти неделями и
  // поставить экстремум без единого резкого дня), поэтому он и занимает место
  // сигнала. net_record (истор. экстремум сырой позиции в контрактах) сильнее
  // рекорда перекоса и перекрывает его.
  const recordSignal = (r: OiScreenerRow) => {
    if (r.net_record) {
      const high = r.net_record.kind === 'high';
      return {
        text: high ? 'Ист. макс' : 'Ист. мин',
        title: `Чистая позиция ${groupGen} (в контрактах) — исторический ${high ? 'максимум' : 'минимум'} за всё время наблюдений. Это сильнее дневной кратности: позиция могла прийти сюда без единого резкого дня.`,
      };
    }
    if (r.record) {
      const high = r.record.kind === 'high';
      return {
        text: `${high ? 'Макс' : 'Мин'} ${PERIOD_WORD[r.record.period]}`,
        title: `Перекос ${groupGen} пробил ${high ? 'максимум (рекордный лонг)' : 'минимум (рекордный шорт)'} ${PERIOD_WORD[r.record.period]}.`,
      };
    }
    return null;
  };

  // ── Сила: пилюля-бейдж «4,3×» с тремя ступенями (как в первом дизайне
  // скринера). Ступени абсолютные и привязаны к порогу «резко» этого горизонта
  // (2× за день / 3× за 2 недели): x1 — обычная резкость (контур цветом
  // текста), x1,5 — сильная (контур accent), x2,5 — особо сильная (заливка
  // accent + кольцо). Относительный градиент «теплоты» убран: он красил
  // лидера тихого дня так же ярко, как настоящий выброс, и одинаковое число в
  // разные дни выглядело по-разному.
  const ratioCell = (r: OiScreenerRow) => {
    if (r.ratio == null) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
    const pill: CSSProperties = {
      ...MONO, fontSize: 'var(--fs-base)', fontWeight: 800, letterSpacing: '0.01em',
      whiteSpace: 'nowrap', display: 'inline-block',
      padding: '3px 11px', borderRadius: 999, border: '2px solid',
    };
    // Ниже порога «резко» — та же пилюля, но приглушённая: колонка должна
    // читаться как одна колонка бейджей, а не прыгать между формами.
    if (r.status !== 'sharp') {
      return (
        <span
          title={`${W.move} ${groupGen} в ${fmtRatio(r.ratio)} от ${W.usual} — ниже порога «резко» (${W.thr})`}
          style={{ ...pill, borderColor: 'var(--text-muted)', color: 'var(--text-muted)' }}
        >
          {fmtRatio(r.ratio)}
        </span>
      );
    }
    const strong = r.ratio >= sharpThr * 2.5;
    const mid = r.ratio >= sharpThr * 1.5;
    const tierNote = strong
      ? ' — особо сильное движение'
      : mid ? ' — сильное движение' : '';
    return (
      <span
        title={`${W.move} ${groupGen} в ${fmtRatio(r.ratio)} сильнее ${W.usual}${tierNote}`}
        style={{
          ...pill,
          borderColor: mid ? 'var(--accent)' : 'var(--text-primary)',
          background: strong ? 'var(--accent)' : 'transparent',
          color: strong ? 'var(--text-inverse)' : mid ? 'var(--accent)' : 'var(--text-primary)',
          boxShadow: strong ? '0 0 0 4px color-mix(in oklab, var(--accent) 22%, transparent)' : undefined,
        }}
      >
        {fmtRatio(r.ratio)}
      </span>
    );
  };

  // Текст сигнала. Приоритет: рекорд → глагол резкого движения → служебная
  // пометка. Кратность живёт в колонке «Сила», точные проценты — в подсказке
  // кометы; здесь полная трактовка в title.
  const signalText = (r: OiScreenerRow) => {
    // Рекорд перебивает всё: «обычный день» рядом с историческим минимумом
    // читался как противоречие, хотя по важности выигрывает второе.
    const rec = recordSignal(r);
    if (rec) {
      return (
        <span
          style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--accent)', whiteSpace: 'nowrap', cursor: 'help' }}
          title={rec.title}
        >
          {rec.text}
        </span>
      );
    }
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
      const full = `${cap(groupWord)} резко ${grewExposure ? 'нарастили' : 'сократили'} ${netLong ? 'длинную' : 'короткую'} позицию по «${r.name}»: изменение чистой позиции ${W.over} в ${fmtRatio(r.ratio)} сильнее ${W.usual} («резко» — от ${W.thr}). Обратная сторона (${mirrorWord}) держит зеркальную позицию.`;
      return (
        <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-primary)', whiteSpace: 'nowrap' }} title={full}>
          {verb}
        </span>
      );
    }
    // Служебные пометки держим короче любого глагола: иначе они (а не
    // сигналы) задают ширину колонки, и у обычных строк справа зияет дыра.
    // Подробности — в подсказке, она у каждой пометки своя.
    const note =
      r.status === 'normal'
        ? W.note
        : r.status === 'illiquid'
          ? 'мало участников'
          : 'мало истории';
    const noteTitle =
      r.status === 'illiquid'
        ? `На стороне ${groupWord}: ${r.npart} участников — ниже порога ликвидности ${minPart}. Движение по такому контракту считаем шумом, а не сигналом (у юрлиц участников структурно меньше, поэтому порог свой).`
        : r.status === 'nodata'
          ? (isMed
              ? 'Мало истории: для среднесрочного сигнала нужна норма минимум за 30 торговых дней до окна движения.'
              : 'Мало истории для расчёта ATR-14.')
          : `Сдвиг чистой позиции ${W.over} в пределах обычного${r.ratio != null ? ` (${fmtRatio(r.ratio)})` : ''} — ниже порога «резко» (${W.thr}).`;
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
  //
  // Ширины подогнаны под ФАКТИЧЕСКИЙ контент (замеры на проде): число силы
  // ≤ 52px («×12,4»). Раньше колонки были 92 и 244 при контенте 42 и 112 —
  // между объектами зияло 16 / 66 / 155px, отступы читались как случайные.
  //
  // «Сигнал» — 140px: самый длинный текст это «Макс за всё время». Колонка
  // центрированная, поэтому запас распределяется по обе стороны текста, и чем
  // она уже, тем меньше это поле «подмешивается» в зазор у соседней «Силы».
  // Только фиксированное число: шапка и строки — РАЗНЫЕ grid-контейнеры, и
  // max-content посчитал бы их независимо (заголовок «СИГНАЛ» узкий, строки
  // широкие) — колонки разъехались бы. При смене формулировок пересчитать.
  // «Сила» — 96px, а не 64: пилюля-бейдж с рамкой и падингами шире голого
  // числа, и на трёхзначных днях («15,2×») она не должна лезть на соседей.
  const gridCols = 'minmax(180px, 222px) minmax(360px, 1fr) 96px 140px 72px';

  // Оптическая компенсация для «Силы»: слева от числа стоит дорожка кометы
  // впритык (зазор = gap), а справа к gap прибавляется половина свободного
  // поля центрированного «Сигнала» (~20px). Из-за этого число выглядело
  // прижатым к комете. Отступ дорожки справа уравнивает зазоры на глаз.
  const COMET_RIGHT_INSET = 12;

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
        {/* Горизонт: краткосрочная лента (день) / среднесрочная (2 недели).
            Стоит сразу за группой — это второй «срез» тех же данных, а не
            фильтр: он меняет сам расчёт, а не состав строк. */}
        <SegmentedControl<OiScreenerHorizon>
          options={HORIZON_OPTIONS}
          value={horizon}
          onChange={setHorizon}
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
          <div style={{ display: 'grid', gridTemplateColumns: gridCols, gap: 16, padding: '12px 8px 12px 18px', borderBottom: '2px solid var(--text-primary)', alignItems: 'center' }}>
            <span style={headCell}>Актив</span>
            <span style={headCell} title={`Комета на оси перекоса −100…+100: слева полный шорт, по центру ноль (поровну), справа полный лонг. Голова = где ${groupWord} стоят сейчас, хвост = сдвиг ${W.over}, размер головы = сила движения. Точные проценты — при наведении на строку.`}>
              Позиция {groupGen}
            </span>
            {/* Сила — числовая колонка: заголовок и число по центру, чтобы
                отступы слева (комета) и справа (сигнал) были симметричны. */}
            <button
              type="button"
              style={{ ...headCell, cursor: 'pointer', background: 'none', border: 'none', padding: 0, justifySelf: 'center' }}
              onClick={() => setSortDir((d) => (d === -1 ? 1 : -1))}
              title={`Во сколько раз изменение позиции ${W.over} сильнее ${W.usual}. Клик — перевернуть порядок.`}
            >
              Сила{sortDir === -1 ? ' ▼' : ' ▲'}
            </button>
            <span style={{ ...headCell, justifySelf: 'center' }}>Сигнал</span>
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
                  // Правый падинг 8, а не 18: мишень звезды доходит почти до
                  // края карточки, промахнуться «мимо вправо» уже некуда.
                  alignItems: 'center', padding: '12px 8px 12px 15px',
                  // 1px, а не 0.5: на дисплеях с DPR=1 полупиксельная линия
                  // рендерилась рвано — часть строк выглядела без разделителя.
                  borderBottom: '1px solid var(--border-color, rgba(128,128,128,0.18))',
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
                      <FadedAssetName name={r.name} />
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
                <div style={{ marginRight: COMET_RIGHT_INSET, minWidth: 0 }}>
                  <PositionComet
                    netPct={r.net_pct}
                    netPctPrev={r.net_pct_prev}
                    ratio={r.ratio}
                    ratioLo={stats.ratioLo}
                    ratioHi={stats.ratioHi}
                    prevLabel={W.prev}
                    deltaLabel={W.over}
                  />
                </div>

                {/* Сила — пилюля-бейдж 4,3× со ступенями по порогу «резко» */}
                <div style={{ justifySelf: 'center' }}>{ratioCell(r)}</div>

                {/* Сигнал: рекорд, глагол резкого движения или пометка */}
                <div style={{ minWidth: 0, justifySelf: 'center' }}>{signalText(r)}</div>

                {/* ⭐ */}
                <button
                  type="button"
                  onClick={(e) => toggleFavorite(r.sectype, e)}
                  aria-label={isFav ? 'Убрать из избранного' : 'В избранное'}
                  className="oi-screener-fav"
                  style={{
                    // Зона клика на всю высоту строки и ширину колонки: звезда
                    // 18px с падингом 6 была слишком мелкой мишенью — промах
                    // попадал по строке и открывал график вместо избранного.
                    background: 'none', border: 'none', cursor: 'pointer', padding: 0,
                    alignSelf: 'stretch', width: '100%', minHeight: 48,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    borderRadius: 8,
                    color: isFav ? 'var(--accent)' : 'var(--text-secondary)', lineHeight: 0,
                  }}
                >
                  <Star size={26} fill={isFav ? 'var(--accent)' : 'none'} strokeWidth={2} />
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
            {isMed
              ? `Сила 4,3× значит: за 2 недели позиция сдвинулась в 4,3 раза сильнее, чем этот актив обычно проходит за такой срок. Медленное движение, которое копилось неделями, — в дневной ленте его не видно. Бейдж с цветной рамкой — сильное движение (от ${fmtRatio(sharpThr * 1.5)}), залитый — особо сильное (от ${fmtRatio(sharpThr * 2.5)}). Точный перекос и дельта — при наведении на комету.`
              : `Сила 4,3× значит: за день позиция сдвинулась в 4,3 раза сильнее, чем этот актив двигается обычно (среднее за 14 дней). Бейдж с цветной рамкой — сильное движение (от ${fmtRatio(sharpThr * 1.5)}), залитый — особо сильное (от ${fmtRatio(sharpThr * 2.5)}). Точный перекос и дневная дельта — при наведении на комету.`}
          </span>
          <span style={MONO}>{pluralAssets(visible.length)} · {groupWord} · {isMed ? '2 недели' : 'день'}</span>
        </div>
      )}
    </div>
  );
}
