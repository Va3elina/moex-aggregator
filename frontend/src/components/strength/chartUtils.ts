// Re-export shared animation utilities from central module
export { lerp, easeOutCubic, resamplePts, morphPts, ptsToPath, ptsToArea } from '../../utils/chartAnimation';
export type { ChartPadding } from '../../utils/chartAnimation';

// Strength-specific types
export type SyncedDataPoint = { time: string; breadth: number; imoex: number };
