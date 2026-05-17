/**
 * MobileOpenInterestPage — мобильная версия индикатора «Открытый интерес».
 *
 * PHASE 0 STUB — пока только shell (TopBar + PageHeader + BottomRail).
 * Phase 2 добавит: реальные данные через useOIData, MobileChart, QuickActions.
 */
import { BarChart3 } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';

export default function MobileOpenInterestPage() {
  return (
    <MobileLayout>
      <MobilePageHeader
        Icon={BarChart3}
        title="Открытый интерес"
        subtitle="Позиции участников · Фьючерсы MOEX"
        helpLink="/methodology/oi"
      />

      <div className="fm-frame">
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, lineHeight: 1.5 }}>
          Mobile view — Phase 0 (foundation). Здесь появится:
        </p>
        <ul style={{ paddingLeft: 18, fontSize: 13, color: 'var(--text-secondary)' }}>
          <li>График с edge-to-edge axis pills (Phase 2)</li>
          <li>QuickActions row: актив · период · опции · полный экран · экспорт</li>
          <li>Sheet'ы для выбора инструмента, периода, режима</li>
          <li>Long-press tooltip + swipe жесты по осям</li>
        </ul>
      </div>
    </MobileLayout>
  );
}
