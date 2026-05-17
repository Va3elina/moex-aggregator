/**
 * squarify — алгоритм построения treemap'а с минимизацией aspect ratio.
 *
 * Изначально реализован в HeatmapPage.tsx, вынесен сюда чтобы переиспользовать
 * на MobileHeatmapPage. Алгоритм Bruls/Huijbregts/van Wijk (2000) —
 * стандарт для market-cap-карт типа Finviz.
 *
 * Принцип:
 *   - Сортируем items по убыванию value
 *   - Кладём по строкам/столбцам так, чтобы worst-ratio минимизировался
 *   - Каждый item получает (x, y, width, height) пропорциональный его value
 */

export interface SquarifyItem<T> {
  id: string;
  value: number;
  data: T;
}

export interface SquarifyRect<T> {
  id: string;
  x: number;
  y: number;
  width: number;
  height: number;
  data: T;
}

export function squarify<T>(
  items: SquarifyItem<T>[],
  x: number,
  y: number,
  width: number,
  height: number,
): SquarifyRect<T>[] {
  if (items.length === 0 || width <= 0 || height <= 0) return [];

  const total = items.reduce((sum, item) => sum + item.value, 0);
  if (total === 0) return [];

  const sortedItems = [...items].sort((a, b) => b.value - a.value);
  const result: SquarifyRect<T>[] = [];

  let currentX = x;
  let currentY = y;
  let remainingWidth = width;
  let remainingHeight = height;
  let remainingItems = [...sortedItems];
  let remainingTotal = total;

  while (remainingItems.length > 0) {
    const isHorizontal = remainingWidth >= remainingHeight;
    const side = isHorizontal ? remainingHeight : remainingWidth;

    let row: SquarifyItem<T>[] = [];
    let rowValue = 0;
    let bestRatio = Infinity;

    for (let i = 0; i < remainingItems.length; i++) {
      const testRow = remainingItems.slice(0, i + 1);
      const testValue = testRow.reduce((s, item) => s + item.value, 0);
      const rowLength = (testValue / remainingTotal) * (isHorizontal ? remainingWidth : remainingHeight);

      let worstRatio = 0;
      for (const item of testRow) {
        const itemSize = (item.value / testValue) * side;
        const ratio = Math.max(rowLength / itemSize, itemSize / rowLength);
        worstRatio = Math.max(worstRatio, ratio);
      }

      if (worstRatio <= bestRatio) {
        bestRatio = worstRatio;
        row = testRow;
        rowValue = testValue;
      } else {
        break;
      }
    }

    if (row.length === 0) break;

    const rowSize = (rowValue / remainingTotal) * (isHorizontal ? remainingWidth : remainingHeight);
    let offset = 0;

    for (const item of row) {
      const itemSize = (item.value / rowValue) * side;

      if (isHorizontal) {
        result.push({
          id: item.id,
          x: currentX,
          y: currentY + offset,
          width: rowSize,
          height: itemSize,
          data: item.data,
        });
      } else {
        result.push({
          id: item.id,
          x: currentX + offset,
          y: currentY,
          width: itemSize,
          height: rowSize,
          data: item.data,
        });
      }
      offset += itemSize;
    }

    if (isHorizontal) {
      currentX += rowSize;
      remainingWidth -= rowSize;
    } else {
      currentY += rowSize;
      remainingHeight -= rowSize;
    }

    remainingItems = remainingItems.slice(row.length);
    remainingTotal -= rowValue;
  }

  return result;
}
