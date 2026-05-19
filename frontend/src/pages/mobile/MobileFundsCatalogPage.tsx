/**
 * MobileFundsCatalogPage — мобильная обёртка для каталога фондов.
 *
 * Pragmatic подход: рендерит desktop FundsCatalogPage внутри MobileLayout.
 * Это даёт юзеру mobile chrome (TopBar + BottomRail) для навигации, а
 * desktop tabular content скроллится внутри main area. Полная mobile-
 * адаптация UI каталога — отдельная задача (большая таблица фондов с
 * фильтрами, поиском, сортировкой требует переосмысления под touch).
 */
import MobileLayout from '../../components/mobile/MobileLayout';
import FundsCatalogPage from '../FundsCatalogPage';

export default function MobileFundsCatalogPage() {
  return (
    <MobileLayout enableFullscreen={false}>
      {/* Desktop FundsCatalogPage сам имеет PageHeader + tabular content.
          Wrap'ит в scrollable area чтобы tablicy умещались на mobile.
          .fm-main глобально имеет overflow:hidden — здесь нужно auto.
          Fullscreen кнопка убрана: каталог — это списковый UI, нет смысла
          скрывать nav для tabular данных. */}
      <div style={{ flex: 1, overflow: 'auto', minHeight: 0, width: '100%' }}>
        <FundsCatalogPage />
      </div>
    </MobileLayout>
  );
}
