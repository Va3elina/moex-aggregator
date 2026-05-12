/**
 * SecurityPage — политика информационной безопасности.
 *
 * Обязательная страница по требованиям эквайринга Т-Банка
 * («Разъяснение о политике информационной безопасности» +
 * «Разъяснение о процедуре безопасной передачи по каналам связи
 * конфиденциальной информации Плательщиков»).
 *
 * Описывает: TLS, не-хранение платёжных данных, PCI DSS со стороны
 * банка-эквайера, защита аккаунтов и инфраструктуры.
 */
import { Link } from 'react-router-dom';
import { ArrowLeft, Lock } from 'lucide-react';

const SUPPORT_EMAIL = 'frameinfo@mail.ru';

export default function SecurityPage() {
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
          <Lock size={24} strokeWidth={1.8} />
        </div>
        <div>
          <h1
            className="text-2xl md:text-3xl font-semibold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
          >
            Информационная безопасность
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            Действует с 12 мая 2026 г.
          </p>
        </div>
      </div>

      <Section title="Передача данных">
        <p>
          Весь обмен данными между браузером пользователя и сервером сервиса
          «Frame» защищён по протоколу <strong>HTTPS</strong> (TLS 1.2/1.3) с
          валидным сертификатом, выпущенным доверенным удостоверяющим центром.
          Перехват трафика в публичных сетях (Wi-Fi в кафе, точки доступа и т.п.)
          для расшифровки данных невозможен.
        </p>
        <p className="mt-3">
          Соединение по незащищённому HTTP автоматически перенаправляется на
          HTTPS (HSTS включён).
        </p>
      </Section>

      <Section title="Платёжные данные">
        <p>
          Сервис «Frame» <strong>не получает и не хранит</strong> данные
          банковских карт пользователей. При оплате подписки:
        </p>
        <ul className="list-disc pl-6 space-y-2 mt-2">
          <li>
            Пользователь перенаправляется на защищённую платёжную форму
            банка-эквайера. Все поля карты (номер, срок, CVV) вводятся напрямую
            в систему банка.
          </li>
          <li>
            Банк-эквайер сертифицирован по стандарту <strong>PCI DSS</strong>{' '}
            (Payment Card Industry Data Security Standard) — международному
            стандарту защиты данных платёжных карт.
          </li>
          <li>
            Сервис получает только обезличенный идентификатор транзакции и
            статус (успех/ошибка). Полный номер карты, CVV и срок действия — не
            передаются и не хранятся.
          </li>
        </ul>
      </Section>

      <Section title="Хранение паролей">
        <p>
          Пароли пользователей хранятся только в виде <strong>криптографических хешей</strong>{' '}
          (bcrypt с уникальной солью для каждого аккаунта). Восстановление
          оригинального пароля из хеша невозможно даже для администрации
          сервиса.
        </p>
        <p className="mt-3">
          При входе через OAuth-провайдеров (Яндекс, ВКонтакте, Telegram) пароль
          вообще не запрашивается — авторизация проходит на стороне провайдера
          с использованием стандартов OAuth 2.0 / OpenID Connect.
        </p>
      </Section>

      <Section title="Защита аккаунта">
        <p>
          Сервис применяет следующие механизмы защиты аккаунтов:
        </p>
        <ul className="list-disc pl-6 space-y-2 mt-2">
          <li>
            <strong>JWT-токены</strong> с ограниченным сроком жизни (access ~15 минут,
            refresh ~30 дней). При потере токена злоумышленник теряет доступ
            в течение этого окна.
          </li>
          <li>
            <strong>Rate-limiting</strong> на чувствительные endpoint'ы
            (вход, регистрация, восстановление пароля) — защита от brute-force.
          </li>
          <li>
            <strong>Защита от подбора паролей</strong> через автоматическую
            временную блокировку после нескольких неудачных попыток входа.
          </li>
          <li>
            <strong>Логирование событий</strong> входа и подозрительной
            активности — для расследования инцидентов.
          </li>
        </ul>
      </Section>

      <Section title="Серверная инфраструктура">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            Сервер размещён в дата-центре на территории Российской Федерации.
          </li>
          <li>
            Доступ к серверу по SSH разрешён только по криптографическим
            ключам (парольная аутентификация отключена). Включена защита
            <strong> fail2ban</strong> — автоматическая блокировка IP при
            попытках подбора.
          </li>
          <li>
            Сетевой доступ к базе данных — только из внутренней сети докер-кластера,
            наружу СУБД не выставлена.
          </li>
          <li>
            Регулярные резервные копии базы данных хранятся в зашифрованном
            виде с ограниченным сроком (30 дней).
          </li>
        </ul>
      </Section>

      <Section title="Реакция на инциденты">
        <p>
          В случае обнаружения инцидента информационной безопасности (утечка
          данных, несанкционированный доступ к аккаунту, фишинг от имени сервиса):
        </p>
        <ol className="list-decimal pl-6 space-y-2 mt-2">
          <li>
            Сообщите как можно быстрее на{' '}
            <a href={`mailto:${SUPPORT_EMAIL}`} style={{ color: 'var(--accent)' }}>
              {SUPPORT_EMAIL}
            </a>{' '}
            с темой «Инцидент безопасности».
          </li>
          <li>
            Если возможно, приложите детали (скриншоты подозрительного письма,
            точное время события, какие действия выполнялись).
          </li>
          <li>
            Если есть подозрение на компрометацию аккаунта — немедленно
            смените пароль или отзовите OAuth-доступ у провайдера.
          </li>
        </ol>
        <p className="mt-3">
          Подтверждённые инциденты, затрагивающие персональные данные,
          расследуются в соответствии с требованиями 152-ФЗ, пользователи
          уведомляются по email в установленные законом сроки.
        </p>
      </Section>

      <Section title="Связанные документы">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            <Link to="/privacy" style={{ color: 'var(--accent)' }}>
              Политика обработки персональных данных
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
