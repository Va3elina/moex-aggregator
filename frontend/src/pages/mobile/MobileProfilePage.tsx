/**
 * MobileProfilePage — мобильная обёртка для личного кабинета.
 *
 * Wrap desktop ProfilePage в MobileLayout с back-кнопкой. Юзер может
 * вернуться на предыдущую страницу через ← в TopBar. BottomRail и
 * остальной mobile chrome работают как обычно.
 */
import { useNavigate } from 'react-router-dom';
import MobileLayout from '../../components/mobile/MobileLayout';
import ProfilePage from '../ProfilePage';

export default function MobileProfilePage() {
  const navigate = useNavigate();
  return (
    <MobileLayout
      enableFullscreen={false}
      onBack={() => navigate(-1)}
    >
      <div style={{ flex: 1, overflow: 'auto', minHeight: 0, width: '100%' }}>
        <ProfilePage />
      </div>
    </MobileLayout>
  );
}
