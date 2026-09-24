/** Кружок с лого УК (или фото автора фонда); нет лого — ничего. */
import { resolveFundLogo } from '../../config/fundConfig';

export default function FundLogoChip({ ticker, ukId, size = 24 }: {
  ticker: string; ukId?: string | number | null; size?: number;
}) {
  const logo = resolveFundLogo(ticker, ukId ?? null);
  if (!logo) return null;
  return (
    <span
      className="flex items-center justify-center rounded-full overflow-hidden text-[10px] font-bold shrink-0"
      style={{ width: size, height: size, backgroundColor: logo.bg, color: logo.color }}
      title={logo.name}
    >
      {logo.img ? <img src={logo.img} alt="" className="w-full h-full object-cover" /> : logo.letter}
    </span>
  );
}
