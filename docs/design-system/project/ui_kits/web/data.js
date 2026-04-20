// Demo data for the UI kit. No network; everything is static & shaped to
// match the production API responses (see reference/frontend/src/services/api.ts).

window.UI_DEMO = (function () {
  // Deterministic PRNG so reloads are stable
  let seed = 0xF4E43;
  const rand = () => {
    seed = (seed * 9301 + 49297) % 233280;
    return seed / 233280;
  };

  // ── Fear Index history — 90 daily points, drift 30..90
  const fearHistory = [];
  {
    let v = 48;
    const days = 90;
    const today = new Date('2026-04-19');
    for (let i = days - 1; i >= 0; i--) {
      const d = new Date(today); d.setDate(today.getDate() - i);
      v = Math.max(10, Math.min(95, v + (rand() - 0.5) * 8));
      fearHistory.push({
        date: d.toISOString().slice(0, 10),
        fear_index: Math.round(v),
        rotation_ratio: 1.8 + (v - 50) * 0.02 + (rand() - 0.5) * 0.15,
      });
    }
  }
  const fearCurrent = fearHistory[fearHistory.length - 1];

  // ── Heatmap stocks (MOEX blue chips)
  const SECTORS = {
    'Финансы':          ['SBER','VTBR','TCSG','MOEX','BSPB'],
    'Нефть и газ':      ['LKOH','GAZP','ROSN','NVTK','TATN','SNGS'],
    'Металлы':          ['GMKN','CHMF','NLMK','MAGN','ALRS','PLZL','POLY'],
    'Потребитель':      ['MGNT','FIVE','DSKY','BELU','OZON'],
    'Технологии':       ['YNDX','VKCO','POSI','QIWI'],
    'Энергетика':       ['IRAO','HYDR','FEES','RSTI'],
    'Недвижимость':     ['PIKK','LSRG','SMLT'],
    'Транспорт':        ['AFLT','FLOT','NMTP'],
  };
  const stocks = [];
  const sectors = [];
  Object.entries(SECTORS).forEach(([name, list]) => {
    const sectorStocks = list.map((id) => {
      const cap = 50 + rand() * 2500;
      const c1d = (rand() - 0.5) * 6;
      const c1w = (rand() - 0.5) * 14;
      const c1m = (rand() - 0.45) * 30;
      const c1y = (rand() - 0.4) * 80;
      return {
        secId: id, name: id, sector: name,
        price: +(100 + rand() * 500).toFixed(2),
        prev_close: 100,
        market_cap: cap * 1e9,
        change_1d: +c1d.toFixed(2),
        change_1w: +c1w.toFixed(2),
        change_1m: +c1m.toFixed(2),
        change_1y: +c1y.toFixed(2),
        value_1d: cap * 1e7, value_1w: cap * 5e7, value_1m: cap * 2e8,
      };
    });
    stocks.push(...sectorStocks);
    sectors.push({ name, stocks: sectorStocks });
  });

  // ── Breadth (Сила рынка)
  const breadth = {
    count_above: 142, count_total: 210,
    percent_above: Math.round(142/210*100),
  };

  // ── Buffett
  const buffett = { ratio: 68 };

  // ── Funds summary
  const funds = {
    categories: [
      { name: 'Акции',      funds_count: 47, total_nav: 1_845_000_000_000, change_pct: -1.24 },
      { name: 'Облигации',  funds_count: 82, total_nav: 3_210_000_000_000, change_pct: 0.42 },
      { name: 'Денежный рынок', funds_count: 24, total_nav: 2_680_000_000_000, change_pct: 0.18 },
    ],
  };

  // ── Sector breadth detail
  const sectorBreadth = Object.entries(SECTORS).map(([name, list]) => ({
    name,
    above: Math.max(0, Math.round(list.length * (0.4 + rand() * 0.55))),
    total: list.length,
  }));

  // ── Overview OI chart (price + buys + sells)
  const oi = (() => {
    const N = 120;
    const price = []; const buys = []; const sells = [];
    let p = 300; let b = 180_000; let s = 160_000;
    for (let i = 0; i < N; i++) {
      p += (rand() - 0.48) * 3;
      b += (rand() - 0.5) * 4000;
      s += (rand() - 0.5) * 4000;
      const t = `2026-03-${String(Math.max(1, Math.floor(i/4))).padStart(2,'0')}`;
      price.push({ time: t, value: +p.toFixed(2) });
      buys.push({ time: t, value: Math.round(b) });
      sells.push({ time: t, value: Math.round(s) });
    }
    return { price, buys, sells };
  })();

  return { fearHistory, fearCurrent, stocks, sectors, breadth, buffett, funds, sectorBreadth, oi };
})();
