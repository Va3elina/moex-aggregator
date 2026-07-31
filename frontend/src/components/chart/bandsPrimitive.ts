/**
 * Зоны на шкале индикатора — примитив (как профиль объёма, см. соседний файл).
 *
 * Для RSI это канонические 30/70: полоса между ними — «нормальный» диапазон, всё
 * выше — перекупленность, ниже — перепроданность. Смысл индикатора не в самом
 * числе, а в том, в какой зоне оно оказалось, и без разметки читать его нельзя.
 *
 * Почему примитив, а не три createPriceLine: линии дают только уровни, а нужна
 * ЗАЛИВКА зон — по ней глаз ловит выход за границу мгновенно, не сверяя число со
 * шкалой. Линии при этом рисуются здесь же, так что уровень и его зона всегда
 * одного цвета и не расходятся при смене темы.
 *
 * Заливка «между линией индикатора и уровнем», как в TradingView, СОЗНАТЕЛЬНО не
 * делается: она требует геометрии каждого бара на каждый кадр, а читаемости
 * добавляет мало — зона уже видна.
 */
import type {
  IPrimitivePaneRenderer, IPrimitivePaneView, ISeriesApi, ISeriesPrimitive,
  PrimitivePaneViewZOrder, SeriesAttachedParameter, SeriesType, Time,
} from 'lightweight-charts';

type RenderTarget = Parameters<IPrimitivePaneRenderer['draw']>[0];
type AnySeries = ISeriesApi<SeriesType, Time>;

export interface BandsOptions {
  /** Верхняя граница (перекупленность выше неё). */
  upper: number;
  /** Нижняя граница (перепроданность ниже неё). */
  lower: number;
  /** Средняя линия. null — не рисовать. */
  middle: number | null;
  /** Цвета линий уровней — по одному на границу. */
  upperColor: string;
  middleColor: string;
  lowerColor: string;
  /** Заливка полосы между границами. */
  bandColor: string;
  /** Заливка зоны перекупленности (выше upper). */
  overColor: string;
  /** Заливка зоны перепроданности (ниже lower). */
  underColor: string;
}

export class BandsPrimitive implements ISeriesPrimitive<Time> {
  private _opts: BandsOptions;
  private _series: AnySeries | null = null;
  private _requestUpdate: (() => void) | null = null;
  private readonly _views: IPrimitivePaneView[];

  constructor(opts: BandsOptions) {
    this._opts = opts;
    this._views = [new BandsPaneView(this)];
  }

  attached(p: SeriesAttachedParameter<Time>): void {
    this._series = p.series as AnySeries;
    this._requestUpdate = p.requestUpdate;
  }

  detached(): void {
    this._series = null;
    this._requestUpdate = null;
  }

  paneViews(): readonly IPrimitivePaneView[] { return this._views; }

  applyOptions(o: Partial<BandsOptions>): void {
    this._opts = { ...this._opts, ...o };
    this._requestUpdate?.();
  }

  /**
   * Расширяем автомасштаб до границ зон. Без этого при спокойном рынке RSI
   * держится в 40..60, шкала сжимается по данным, и линии 30/70 уезжают за край
   * — разметка исчезает ровно тогда, когда по ней и надо смотреть, далеко ли до
   * зоны.
   */
  autoscaleInfo() {
    const { upper, lower } = this._opts;
    return { priceRange: { minValue: Math.min(lower, upper), maxValue: Math.max(lower, upper) } };
  }

  get options(): BandsOptions { return this._opts; }
  get series(): AnySeries | null { return this._series; }
}

class BandsPaneView implements IPrimitivePaneView {
  private readonly _src: BandsPrimitive;
  constructor(src: BandsPrimitive) { this._src = src; }

  /** Под сериями: это фон, линия индикатора должна читаться поверх. */
  zOrder(): PrimitivePaneViewZOrder { return 'bottom'; }

  renderer(): IPrimitivePaneRenderer | null {
    const series = this._src.series;
    if (!series) return null;
    return new BandsRenderer(series, this._src.options);
  }
}

class BandsRenderer implements IPrimitivePaneRenderer {
  private readonly _series: AnySeries;
  private readonly _o: BandsOptions;
  constructor(series: AnySeries, o: BandsOptions) { this._series = series; this._o = o; }

  draw(target: RenderTarget): void {
    target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
      const o = this._o;
      const yUp = this._series.priceToCoordinate(o.upper);
      const yLo = this._series.priceToCoordinate(o.lower);
      if (yUp == null || yLo == null) return;
      const w = mediaSize.width, h = mediaSize.height;
      // Ось цены растёт вверх, поэтому координата верхней границы МЕНЬШЕ нижней.
      const top = Math.min(yUp, yLo), bottom = Math.max(yUp, yLo);

      ctx.save();
      ctx.fillStyle = o.overColor;
      ctx.fillRect(0, 0, w, Math.max(0, top));
      ctx.fillStyle = o.underColor;
      ctx.fillRect(0, bottom, w, Math.max(0, h - bottom));
      ctx.fillStyle = o.bandColor;
      ctx.fillRect(0, top, w, Math.max(0, bottom - top));

      ctx.lineWidth = 1;
      ctx.setLineDash([4, 4]);
      const line = (y: number, color: string) => {
        ctx.strokeStyle = color;
        ctx.beginPath();
        // +0.5 — иначе линия толщиной 1 размазывается на два пикселя.
        ctx.moveTo(0, Math.round(y) + 0.5);
        ctx.lineTo(w, Math.round(y) + 0.5);
        ctx.stroke();
      };
      line(yUp, o.upperColor);
      line(yLo, o.lowerColor);
      if (o.middle != null) {
        const yMid = this._series.priceToCoordinate(o.middle);
        if (yMid != null) line(yMid, o.middleColor);
      }
      ctx.restore();
    });
  }
}
