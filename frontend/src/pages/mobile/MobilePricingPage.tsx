/**
 * MobilePricingPage — мобильная обёртка для страницы тарифов.
 */
import { useNavigate } from 'react-router-dom';
import MobileLayout from '../../components/mobile/MobileLayout';
import PricingPage from '../PricingPage';

export default function MobilePricingPage() {
  const navigate = useNavigate();
  return (
    <MobileLayout
      enableFullscreen={false}
      onBack={() => navigate(-1)}
    >
      <div style={{ flex: 1, overflow: 'auto', minHeight: 0, width: '100%' }}>
        <PricingPage />
      </div>
    </MobileLayout>
  );
}
