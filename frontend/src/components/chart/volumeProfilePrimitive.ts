/**
 * Профиль объёма как ПРИМИТИВ lightweight-charts, а не как серия и не как
 * DOM-оверлей.
 *
 * Почему не серия: у серии одна точка на момент времени, а профиль — гистограмма
 * по цене (см. utils/volumeProfile.ts). Ни один встроенный тип серий такого не
 * рисует.
 *
 * Почему не DOM-слой, как метки экспираций: тех штук десяток и они статичны, а
 * здесь несколько десятков прямоугольников, которые пересчитываются на КАЖДЫЙ
 * кадр пана и зума. Перекладывать это на вёрстку — гарантированный джанк.
 * Примитив рисует на том же канвасе, что и серии: попадает в html2canvas-экспорт
 * вместе с графиком и не требует ручной перепроекции координат — библиотека сама
 * вызывает draw() тогда, когда перерисовывает пейн.
 *
 * ⚠️ Профиль ПРИВЯЗАН К СЕРИИ, на которую его повесили: priceToCoordinate берётся
 * у неё, то есть примитив обязан висеть на ценовой серии. На серии открытого
 * интереса он нарисовал бы уровни по шкале ОИ — формально работающая чепуха.
 */
import type {
  IChartApi, ISeriesApi, ISeriesPrimitive, IPrimitivePaneRenderer, IPrimitivePaneView,
  PrimitivePaneViewZOrder, SeriesAttachedParameter, SeriesType, Time,
} from 'lightweight-charts';
import { volumeProfile, type VpCandle, type VpProfile } from '../../utils/volumeProfile';

/** Тип канвас-таргета берём из самой библиотеки: fancy-canvas у нас транзитивная
 *  зависимость, импортировать её напрямую — значит зависеть от плоского
 *  node_modules. */
type RenderTarget = Parameters<IPrimitivePaneRenderer['draw']>[0];
type AnySeries = ISeriesApi<SeriesType, Time>;

export interface VolumeProfileOptions {
  /** Свечи ровно того же ряда и в том же порядке, что у серии-носителя: видимый
   *  диапазон приходит логическими ИНДЕКСАМИ, и рассинхрон сдвинет профиль. */
  candles: VpCandle[];
  /** Число ценовых уровней. */
  rows: number;
  /** Куда прижат профиль. */
  side: 'left' | 'right';
  /** Доля ширины пейна под самый длинный уровень. */
  widthPct: number;
  /** Доля объёма в зоне стоимости (0.7 — канон). */
  valueAreaPct: number;
  upColor: string;
  downColor: string;
  /** Цвет POC: линия через пейн и подсветка самого уровня. */
  pocColor: string;
}

/** Дефолты профиля: 24 уровня и четверть ширины — компромисс, при котором
 *  профиль читается и не закрывает график (TradingView по умолчанию примерно
 *  так же). */
export const VP_DEFAULTS = { rows: 24, side: 'right' as const, widthPct: 0.2, valueAreaPct: 0.7 };

/** Профиль полупрозрачен НАМЕРЕННО. Он занимает пятую часть графика ровно там,
 *  где самые свежие бары, и сплошная заливка (проверено на живом SR) читается
 *  как основной слой, а цена — как помеха на нём. Уровни вне зоны стоимости
 *  глушим ещё вдвое: это хвосты, а не то, ради чего профиль включают. */
const OUTSIDE_ALPHA = 0.24;
const INSIDE_ALPHA = 0.55;

export class VolumeProfilePrimitive implements ISeriesPrimitive<Time> {
  private _opts: VolumeProfileOptions;
  private _chart: IChartApi | null = null;
  private _series: AnySeries | null = null;
  private _requestUpdate: (() => void) | null = null;
  private _profile: VpProfile | null = null;
  private _cacheKey = '';
  private readonly _views: IPrimitivePaneView[];

  constructor(opts: VolumeProfileOptions) {
    this._opts = opts;
    this._views = [new VpPaneView(this)];
  }

  attached(p: SeriesAttachedParameter<Time>): void {
    this._chart = p.chart as unknown as IChartApi;
    this._series = p.series as AnySeries;
    this._requestUpdate = p.requestUpdate;
  }

  detached(): void {
    this._chart = null;
    this._series = null;
    this._requestUpdate = null;
  }

  paneViews(): readonly IPrimitivePaneView[] {
    return this._views;
  }

  applyOptions(o: Partial<VolumeProfileOptions>): void {
    this._opts = { ...this._opts, ...o };
    this._cacheKey = '';
    this._requestUpdate?.();
  }

  get options(): VolumeProfileOptions { return this._opts; }
  get series(): AnySeries | null { return this._series; }

  /**
   * Профиль по текущему видимому диапазону. Кэш — по границам диапазона в БАРАХ
   * и длине ряда: движок зовёт отрисовку на каждый кадр, а пан внутри одного бара
   * и любой вертикальный зум профиль не меняют (уровни живут в ценах, не в
   * пикселях). Без этого пересчёт крутился бы на 60 Гц впустую.
   */
  profile(): VpProfile | null {
    const chart = this._chart;
    if (!chart) return null;
    const range = chart.timeScale().getVisibleLogicalRange();
    if (!range) return null;
    const o = this._opts;
    const from = Math.floor(range.from);
    const to = Math.ceil(range.to);
    const key = `${from}|${to}|${o.rows}|${o.valueAreaPct}|${o.candles.length}`;
    if (key !== this._cacheKey) {
      this._cacheKey = key;
      this._profile = volumeProfile(o.candles, from, to, o.rows, o.valueAreaPct);
    }
    return this._profile;
  }
}

class VpPaneView implements IPrimitivePaneView {
  // Поля объявлены явно, а не параметрами конструктора: в проекте включён
  // erasableSyntaxOnly (TS-код должен стираться в JS без трансформаций).
  private readonly _src: VolumeProfilePrimitive;
  constructor(src: VolumeProfilePrimitive) { this._src = src; }

  /** Под сериями: профиль — фон, цена должна читаться поверх него. */
  zOrder(): PrimitivePaneViewZOrder { return 'bottom'; }

  renderer(): IPrimitivePaneRenderer | null {
    const profile = this._src.profile();
    const series = this._src.series;
    if (!profile || !series) return null;
    return new VpRenderer(profile, series, this._src.options);
  }
}

class VpRenderer implements IPrimitivePaneRenderer {
  private readonly _p: VpProfile;
  private readonly _series: AnySeries;
  private readonly _o: VolumeProfileOptions;
  constructor(p: VpProfile, series: AnySeries, o: VolumeProfileOptions) {
    this._p = p; this._series = series; this._o = o;
  }

  draw(target: RenderTarget): void {
    target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
      const { buckets, maxTotal, pocIndex, vaFrom, vaTo } = this._p;
      const o = this._o;
      const paneW = mediaSize.width;
      // Нижний предел — иначе на узкой панели песочницы профиль вырождается в
      // несколько пикселей и читается как грязь у края.
      const maxW = Math.max(28, paneW * o.widthPct);
      const rightSide = o.side !== 'left';

      ctx.save();
      for (let k = 0; k < buckets.length; k++) {
        const b = buckets[k];
        if (b.total <= 0) continue;
        const yHi = this._series.priceToCoordinate(b.hi);
        const yLo = this._series.priceToCoordinate(b.lo);
        if (yHi == null || yLo == null) continue;
        const y = Math.min(yHi, yLo);
        // -1 — зазор между уровнями; max(1) — чтобы на плотной сетке уровень не исчез.
        const h = Math.max(1, Math.abs(yLo - yHi) - 1);
        if (y + h < 0 || y > mediaSize.height) continue;

        const w = (b.total / maxTotal) * maxW;
        const wUp = b.total > 0 ? (b.up / b.total) * w : 0;
        const isPoc = k === pocIndex;
        ctx.globalAlpha = k >= vaFrom && k <= vaTo ? INSIDE_ALPHA : OUTSIDE_ALPHA;

        // Растущий объём — от прижатого края наружу, падающий следом: так видно
        // соотношение покупок и продаж на уровне, а не только его толщину.
        if (isPoc) {
          ctx.fillStyle = o.pocColor;
          ctx.fillRect(rightSide ? paneW - w : 0, y, w, h);
        } else {
          if (wUp > 0) {
            ctx.fillStyle = o.upColor;
            ctx.fillRect(rightSide ? paneW - wUp : 0, y, wUp, h);
          }
          const wDown = w - wUp;
          if (wDown > 0) {
            ctx.fillStyle = o.downColor;
            ctx.fillRect(rightSide ? paneW - w : wUp, y, wDown, h);
          }
        }
      }

      // Уровень POC через весь пейн — главная цифра профиля, у края её легко
      // потерять среди соседних полос.
      const poc = buckets[pocIndex];
      const yPoc = this._series.priceToCoordinate((poc.lo + poc.hi) / 2);
      if (yPoc != null) {
        ctx.globalAlpha = 0.75;
        ctx.strokeStyle = o.pocColor;
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        // +0.5 — линия толщиной 1 на целой координате размазывается на два пикселя.
        ctx.moveTo(0, Math.round(yPoc) + 0.5);
        ctx.lineTo(paneW, Math.round(yPoc) + 0.5);
        ctx.stroke();
      }
      ctx.restore();
    });
  }
}
