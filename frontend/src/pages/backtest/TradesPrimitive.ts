// Стенд: слой сделок на графике — подсветка окна сигнала и линия «вход → выход». Стрелки и подписи рисуют штатные
// маркеры серии (у них своя раскладка без наложений), здесь — только то, чего маркеры не умеют.
import type {
  IChartApi, ISeriesApi, ISeriesPrimitive, IPrimitivePaneRenderer, IPrimitivePaneView, PrimitivePaneViewZOrder,
  SeriesAttachedParameter, SeriesType, Time,
} from 'lightweight-charts';

type RenderTarget = Parameters<IPrimitivePaneRenderer['draw']>[0];
type AnySeries = ISeriesApi<SeriesType, Time>;
export interface TradeShape {
  tIn: number; pIn: number; tOut: number | null; pOut: number | null; side: number; good: boolean; muted: boolean;
  winFrom: number | null; winTo: number | null; robot?: boolean;
}
export interface TradesLayerOptions { trades: TradeShape[]; lines: boolean; window: boolean }

const UP = '38,166,154', DOWN = '239,83,80', MUTED = '120,130,145', ROBOT = '76,141,255';

export class TradesPrimitive implements ISeriesPrimitive<Time> {
  private _o: TradesLayerOptions = { trades: [], lines: true, window: true };
  private _chart: IChartApi | null = null;
  private _series: AnySeries | null = null;
  private _req: (() => void) | null = null;
  private readonly _views: IPrimitivePaneView[] = [new View(this, 'bottom'), new View(this, 'top')];

  attached(p: SeriesAttachedParameter<Time>) { this._chart = p.chart as unknown as IChartApi; this._series = p.series as AnySeries; this._req = p.requestUpdate; }
  detached() { this._chart = null; this._series = null; this._req = null; }
  paneViews() { return this._views; }
  set(o: TradesLayerOptions) { this._o = o; this._req?.(); }
  get o() { return this._o; }
  get chart() { return this._chart; }
  get series() { return this._series; }
}

class View implements IPrimitivePaneView {
  private readonly src: TradesPrimitive; private readonly z: 'bottom' | 'top';
  constructor(src: TradesPrimitive, z: 'bottom' | 'top') { this.src = src; this.z = z; }
  zOrder(): PrimitivePaneViewZOrder { return this.z; }
  renderer(): IPrimitivePaneRenderer | null {
    const { chart, series, o } = this.src;
    if (!chart || !series || !o.trades.length) return null;
    const z = this.z;
    return {
      draw(target: RenderTarget) {
        target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
          const tsc = chart.timeScale();
          ctx.save();
          for (const t of o.trades) {
            if (z === 'bottom') {
              if (!o.window || t.winFrom == null || t.winTo == null || t.robot) continue;
              const x1 = tsc.timeToCoordinate(t.winFrom as Time), x2 = tsc.timeToCoordinate(t.winTo as Time);
              if (x1 == null || x2 == null || x2 < 0 || x1 > mediaSize.width) continue;
              ctx.fillStyle = `rgba(${t.muted ? MUTED : t.side > 0 ? UP : DOWN},0.07)`;
              ctx.fillRect(x1, 0, Math.max(2, x2 - x1), mediaSize.height);
              continue;
            }
            if (!o.lines || t.tOut == null || t.pOut == null) continue;
            const x1 = tsc.timeToCoordinate(t.tIn as Time), x2 = tsc.timeToCoordinate(t.tOut as Time);
            const y1 = series.priceToCoordinate(t.pIn), y2 = series.priceToCoordinate(t.pOut);
            if (x1 == null || x2 == null || y1 == null || y2 == null || x2 < 0 || x1 > mediaSize.width) continue;
            const rgb = t.robot ? ROBOT : t.muted ? MUTED : t.good ? UP : DOWN;
            ctx.strokeStyle = `rgba(${rgb},0.95)`; ctx.lineWidth = t.robot ? 2 : 1.5; ctx.setLineDash(t.robot ? [] : [5, 4]);
            ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2); ctx.stroke();
            ctx.setLineDash([]); ctx.fillStyle = `rgba(${rgb},1)`;
            for (const [x, y] of [[x1, y1], [x2, y2]]) { ctx.beginPath(); ctx.arc(x, y, 3, 0, 2 * Math.PI); ctx.fill(); }
          }
          ctx.restore();
        });
      },
    };
  }
}
