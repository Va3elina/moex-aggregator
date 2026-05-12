/**
 * DeliveryPage — условия и сроки предоставления услуги.
 *
 * Обязательная страница по требованиям эквайринга Т-Банка
 * («Условия, порядок и сроки доставки, а также возможные регионы/
 * страны доставки/предоставления Товара»).
 *
 * У нас цифровая услуга (подписка), поэтому «доставка» = открытие
 * доступа сразу после успешной оплаты. Страница описывает именно это.
 */
import { Link } from 'react-router-dom';
import { ArrowLeft, Zap } from 'lucide-react';

const SUPPORT_EMAIL = 'frameinfo@mail.ru';

export default function DeliveryPage() {
  return (
    <div className="max-w-3xl mx-auto px-4 md:px-6 py-8 md:py-12">
      <div className="flex items-center gap-3 mb-6">
        <Link
          to="/"
          className="flex items-center gap-1 text-sm transition-opacity hover:opacity-80"
          style={{ color: 'var(--text-secondary)' }}
        >
          <ArrowLeft size={14} />
          На главную
        </Link>
      </div>

      <div className="flex items-start gap-3 mb-8">
        <div
          className="flex items-center justify-center flex-shrink-0"
          style={{
            width: 48,
            height: 48,
            borderRadius: 'var(--radius-md, 8px)',
            background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
            color: 'var(--accent)',
          }}
        >
          <Zap size={24} strokeWidth={1.8} />
        </div>
        <div>
          <h1
            className="text-2xl md:text-3xl font-semibold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
          >
            Предоставление услуги
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            Действует с 12 мая 2026 г.
          </p>
        </div>
      </div>

      <Section title="Что предоставляется">
        <p>
          Сервис «Frame» предоставляет <strong>цифровую услугу</strong> —
          доступ к аналитическим инструментам российского фондового рынка
          (индикаторам, графикам, историческим данным) по подписке. Физическая
          доставка товаров не производится.
        </p>
      </Section>

      <Section title="Сроки активации">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            <strong>Платная подписка</strong> активируется автоматически в
            течение нескольких секунд после успешного списания средств банком.
            Уведомление приходит на email, привязанный к аккаунту, и в
            интерфейсе сервиса (раздел «Профиль» → «Подписка»).
          </li>
          <li>
            <strong>Бесплатный доступ</strong> к базовым индикаторам открывается
            сразу после регистрации, без оплаты и без банковской карты.
          </li>
          <li>
            <strong>Подарочные ссылки и инвайт-токены</strong> активируются
            мгновенно после перехода по ссылке и подтверждения в личном кабинете.
          </li>
        </ul>
      </Section>

      <Section title="Регионы предоставления">
        <p>
          Услуга доступна на территории Российской Федерации через интернет.
          Технических ограничений по регионам внутри РФ нет — сервис работает
          из любой точки, где есть стабильное интернет-соединение.
        </p>
        <p className="mt-3">
          Для пользователей из других стран доступ возможен, но не
          гарантируется — некоторые источники данных (Московская биржа, ЦБ РФ,
          Росстат) могут ограничивать запросы с зарубежных IP. Возврат средств
          из-за региональных ограничений источников данных не производится.
        </p>
      </Section>

      <Section title="Технические требования">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            Современный браузер (Chrome, Safari, Firefox, Edge) обновлённый
            до последней версии за последние 12 месяцев.
          </li>
          <li>
            Стабильное интернет-соединение (рекомендуется от 5 Мбит/с для
            корректной работы графиков).
          </li>
          <li>
            Cookies и JavaScript должны быть включены в браузере.
          </li>
          <li>
            Поддерживаются десктопные и мобильные устройства (iOS 15+,
            Android 10+).
          </li>
        </ul>
      </Section>

      <Section title="Если что-то пошло не так">
        <p>
          Если после успешной оплаты доступ не открылся в течение 10 минут:
        </p>
        <ol className="list-decimal pl-6 space-y-2 mt-2">
          <li>
            Обновите страницу личного кабинета (Ctrl+F5 / Cmd+Shift+R).
          </li>
          <li>
            Проверьте email, на который пришёл чек от банка — иногда подписка
            привязывается к другому аккаунту.
          </li>
          <li>
            Напишите на{' '}
            <a href={`mailto:${SUPPORT_EMAIL}`} style={{ color: 'var(--accent)' }}>
              {SUPPORT_EMAIL}
            </a>{' '}
            — приложите id транзакции из банка. Активируем доступ вручную в
            течение нескольких часов.
          </li>
        </ol>
      </Section>

      <Section title="Связанные документы">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            <Link to="/refund" style={{ color: 'var(--accent)' }}>
              Условия возврата и отмены
            </Link>
          </li>
          <li>
            <Link to="/contacts" style={{ color: 'var(--accent)' }}>
              Контакты службы поддержки
            </Link>
          </li>
        </ul>
      </Section>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="mb-8">
      <h2
        className="text-lg md:text-xl font-semibold mb-3"
        style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
      >
        {title}
      </h2>
      <div
        className="leading-relaxed"
        style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}
      >
        {children}
      </div>
    </section>
  );
}
