/**
 * PrivacyPage — политика обработки данных (152-ФЗ совместимый шаблон).
 *
 * Текст подготовлен как rough draft для MVP. Перед production-публикацией
 * рекомендуется консультация с юристом — особенно если планируется
 * подача уведомления в Роскомнадзор как оператора ПДн.
 *
 * Placeholders:
 *  - Оператор: «Администрация Frame» (можно заменить на ФИО / ИП / ООО)
 *  - Контактный email: «privacy@framedata.ru»  (можно заменить)
 */
import { Link } from 'react-router-dom';
import { ArrowLeft, Shield } from 'lucide-react';

export default function PrivacyPage() {
  return (
    <div className="max-w-3xl mx-auto px-4 md:px-6 py-8 md:py-12">
      {/* Header */}
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
          <Shield size={24} strokeWidth={1.8} />
        </div>
        <div>
          <h1
            className="text-2xl md:text-3xl font-semibold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
          >
            Политика обработки данных
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            Действует с 8 мая 2026 г.
          </p>
        </div>
      </div>

      <Section title="Кто оператор">
        <p>
          Сервис «Frame» (домен <strong>framedata.ru</strong>) — аналитическая
          платформа по российскому фондовому рынку. Оператор обработки данных:{' '}
          <strong>ИП Тория Александр Роландович</strong> (ИНН 782627792630,
          ОГРНИП 325784700029296). Контакт для вопросов и запросов:{' '}
          <a href="mailto:frameinfo@mail.ru" style={{ color: 'var(--accent)' }}>
            frameinfo@mail.ru
          </a>
          .
        </p>
      </Section>

      <Section title="Что мы собираем">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            <strong>Данные регистрации</strong> — email и (для OAuth) публичный
            профиль провайдера (имя, аватарка). Используются только для входа
            в аккаунт.
          </li>
          <li>
            <strong>Обезличенная статистика посещений</strong> — какие страницы
            смотрите, сколько времени проводите на сайте, какие индикаторы и
            тикеры открываете, в какое время. Привязывается к вашему аккаунту
            если вы залогинены, к session-id (умирает на закрытие вкладки)
            если нет.
          </li>
          <li>
            <strong>Технические данные</strong> — тип устройства (мобильный
            /планшет/десктоп), страна (по IP, через CDN-заголовок). Точный
            IP-адрес <strong>не сохраняется</strong>.
          </li>
          <li>
            <strong>Cookies</strong> — только session-cookies для авторизации
            и localStorage для сохранения вашего выбора темы (light/dark) и
            согласия на сбор статистики. Третьим лицам не передаются.
          </li>
        </ul>
      </Section>

      <Section title="Чего мы НЕ собираем">
        <ul className="list-disc pl-6 space-y-2">
          <li>Полный IP-адрес</li>
          <li>Геолокация точнее страны</li>
          <li>Cодержимое экрана / запись действий (Webvisor и подобное)</li>
          <li>Платёжные данные (для подписки Stripe/ЮKassa используют свои защищённые формы)</li>
          <li>Фингерпринт устройства, реклама, ретаргетинг</li>
        </ul>
      </Section>

      <Section title="Зачем мы это собираем">
        <ul className="list-disc pl-6 space-y-2">
          <li>Улучшать сервис — видеть какие индикаторы популярные, что неудобно</li>
          <li>Чинить баги — анализировать на каких страницах пользователи уходят</li>
          <li>Планировать развитие — добавлять новые тикеры/инструменты по реальным запросам</li>
          <li>Защита от ботов — ограничивать злоупотребления и DDoS</li>
        </ul>
      </Section>

      <Section title="Кому передаём">
        <p>
          Никому, кроме обязательных по закону случаев (запрос правоохранителей).
          Аналитика хранится только у нас на собственном сервере, без передачи
          третьим лицам — ни Yandex Metrica, ни Google Analytics, ни Facebook
          Pixel мы не используем.
        </p>
      </Section>

      <Section title="Сколько храним">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            <strong>Данные аккаунта</strong> — пока аккаунт активен. После
            удаления аккаунта стираются в течение 30 дней.
          </li>
          <li>
            <strong>Статистика посещений</strong> — 180 дней, затем
            автоматически удаляется.
          </li>
          <li>
            <strong>Логи входа</strong> — 90 дней (для расследования инцидентов
            безопасности).
          </li>
        </ul>
      </Section>

      <Section title="Ваши права">
        <p>В любое время вы можете:</p>
        <ul className="list-disc pl-6 space-y-2 mt-2">
          <li>
            <strong>Отключить сбор статистики</strong> — в{' '}
            <Link to="/profile" style={{ color: 'var(--accent)' }}>
              профиле
            </Link>{' '}
            установить галочку «Не собирать аналитику», или нажать «Только
            необходимые» в баннере при первом посещении.
          </li>
          <li>
            <strong>Запросить выгрузку данных</strong> — пишите на{' '}
            <a href="mailto:frameinfo@mail.ru" style={{ color: 'var(--accent)' }}>
              frameinfo@mail.ru
            </a>
            , отправим JSON со всеми вашими данными в течение 30 дней.
          </li>
          <li>
            <strong>Удалить аккаунт</strong> — отправьте запрос на{' '}
            <a href="mailto:frameinfo@mail.ru" style={{ color: 'var(--accent)' }}>
              frameinfo@mail.ru
            </a>
            . Все ваши данные стираются в течение 30 дней.
          </li>
        </ul>
      </Section>

      <Section title="Браузерное расширение «Фрейм»">
        <p>
          Расширение для браузера показывает индикаторы Фрейм плавающими окнами
          поверх веб-терминала Т-Инвестиций. Оно обрабатывает данные так:
        </p>
        <ul className="list-disc pl-6 space-y-2 mt-2">
          <li>
            <strong>PRO-токен и раскладка панелей</strong> хранятся{' '}
            <strong>локально в браузере</strong> (chrome.storage), не передаются
            третьим лицам. Токен нужен только для загрузки индикаторов с
            framedata.ru.
          </li>
          <li>
            <strong>Расширение НЕ читает и НЕ сохраняет</strong> ваши сделки,
            позиции, баланс или любые другие данные торгового терминала. Скрипт
            обращается к странице терминала только чтобы вставить кнопку запуска и
            «прилипание» панелей к виджетам — эти данные никуда не отправляются.
          </li>
          <li>
            Расширение снимает заголовок Content-Security-Policy{' '}
            <strong>только на страницах терминала</strong> Т-Инвестиций — это
            нужно, чтобы внутри терминала отрисовался iframe с индикатором.
          </li>
          <li>
            Никакого трекинга, рекламы, фингерпринта или передачи данных третьим
            лицам расширение не выполняет.
          </li>
        </ul>
      </Section>

      <Section title="Изменения в политике">
        <p>
          Если что-то существенное поменяется (новый тип данных, новый
          получатель, изменение срока хранения) — уведомим вас по email,
          зарегистрированному в аккаунте, не менее чем за 14 дней до
          вступления изменений в силу.
        </p>
      </Section>

      <Section title="Применимое право">
        <p>
          Обработка данных производится в соответствии с законодательством
          Российской Федерации, в том числе Федеральным законом № 152-ФЗ
          «О персональных данных».
        </p>
      </Section>

      <div
        className="mt-12 pt-6 text-xs"
        style={{
          borderTop: '1px solid var(--border-color)',
          color: 'var(--text-muted)',
        }}
      >
        Если у вас есть вопросы или замечания по этой политике — напишите на{' '}
        <a href="mailto:frameinfo@mail.ru" style={{ color: 'var(--accent)' }}>
          frameinfo@mail.ru
        </a>
        .
      </div>
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
