import { useLayoutEffect, useRef, useState } from 'react';

/**
 * Compact-режим тулбара embed-панели в sandbox: узкая панель — лейблы
 * контролов не помещаются рядом с lead/ассет-кнопкой → контролы схлопываются
 * в иконки, вместо горизонтального скролла (тот выглядел как отдельный «блок»
 * под ассет-кнопкой, см. EmbedFrame toolbarUnified).
 *
 * wrapRef — реальный видимый контейнер (его clientWidth = сколько места
 * реально дал flex:1 родитель в EmbedFrame); measureRef — невидимый клон с
 * полными лейблами (always full, position:absolute), его scrollWidth —
 * сколько места НУЖНО в полном виде. Сравнение стабильно (без осцилляции
 * full↔compact), т.к. измеритель никогда не меняет форму — если сравнивать
 * видимый ряд сам с собой, после схлопывания в иконки он сам себя же и
 * «не переполняет» → compact откатывается обратно, полные лейблы уже
 * переполняют → снова compact, бесконечное мерцание.
 *
 * Caller обязан отрендерить оба ref'а ровно как в EmbedOpenInterest: wrapRef
 * на видимый контейнер (position:relative, overflow:hidden — см. его же
 * комментарий про наезд иконок друг на друга), measureRef на невидимый клон
 * ВСЕГДА с полными лейблами (position:absolute, visibility:hidden) внутри него.
 */
export function useToolbarCompact() {
  const wrapRef = useRef<HTMLDivElement>(null);
  const measureRef = useRef<HTMLDivElement>(null);
  const [compact, setCompact] = useState(false);
  useLayoutEffect(() => {
    const wrap = wrapRef.current;
    const measure = measureRef.current;
    if (!wrap || !measure) return;
    const check = () => setCompact(measure.scrollWidth > wrap.clientWidth);
    check();
    const ro = new ResizeObserver(check);
    ro.observe(wrap);
    return () => ro.disconnect();
  }, []);
  return { wrapRef, measureRef, compact };
}
