/**
 * RefundPage — условия возврата и отмены подписки.
 *
 * Обязательная страница по требованиям эквайринга Т-Банка
 * («Условия и порядок возврата и отмены Товара»).
 *
 * Услуга цифровая (подписка на аналитический сервис) — стандартная
 * практика: возврат пропорциональный неиспользованному сроку при
 * запросе через техподдержку. Финальный текст должен быть согласован
 * с юристом и оферте.
 */
import { Link } from 'react-router-dom';
import { ArrowLeft, RotateCcw } from 'lucide-react';

const SUPPORT_EMAIL = 'frameinfo@mail.ru';

export default function RefundPage() {
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
          <RotateCcw size={24} strokeWidth={1.8} />
        </div>
        <div>
          <h1
            className="text-2xl md:text-3xl font-semibold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
          >
            Возврат и отмена
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            Действует с 12 мая 2026 г.
          </p>
        </div>
      </div>

      <Section title="Что покупается">
        <p>
          На сервисе «Frame» (домен <strong>framedata.ru</strong>) приобретается
          доступ к аналитическим инструментам российского фондового рынка
          (далее — «Услуга») по подписке. Услуга является{' '}
          <strong>цифровой</strong> и предоставляется удалённо через интернет.
        </p>
      </Section>

      <Section title="Отмена подписки">
        <p>
          Подписку можно отменить в любой момент в{' '}
          <Link to="/profile" style={{ color: 'var(--accent)' }}>
            личном кабинете
          </Link>{' '}
          (раздел «Подписка» → «Отменить»). После отмены:
        </p>
        <ul className="list-disc pl-6 space-y-2 mt-2">
          <li>
            Доступ к платным функциям сохраняется до конца оплаченного периода —
            повторно списания не будет.
          </li>
          <li>
            По истечении оплаченного периода подписка автоматически переходит в
            бесплатный план; данные аккаунта при этом сохраняются.
          </li>
          <li>
            Возобновить подписку можно в любой момент в личном кабинете —
            без потери настроек и истории.
          </li>
        </ul>
      </Section>

      <Section title="Возврат денежных средств">
        <p>
          Возврат денежных средств производится в следующих случаях:
        </p>
        <ul className="list-disc pl-6 space-y-2 mt-2">
          <li>
            <strong>В течение 7 дней с момента оплаты</strong> — по запросу
            пользователя. Возврат производится в полном объёме на ту же
            карту, с которой была совершена оплата.
          </li>
          <li>
            <strong>При технической невозможности предоставить Услугу</strong>{' '}
            по вине Предприятия в течение более 24 часов подряд — возврат
            пропорциональный сроку недоступности.
          </li>
          <li>
            <strong>При обнаружении ошибочного двойного списания</strong> —
            возврат лишних средств в полном объёме.
          </li>
          <li>
            <strong>По истечении 7 дней</strong> — вы в любой момент можете
            отказаться от дальнейшего оказания услуг. В этом случае возвращается
            часть оплаты, приходящаяся на неиспользованный остаток подписки, за
            вычетом фактически понесённых расходов (п. 4.4.2 оферты).
          </li>
        </ul>
        <p className="mt-3">
          Возврат не производится:
        </p>
        <ul className="list-disc pl-6 space-y-2 mt-2">
          <li>
            За уже использованную часть подписки — она считается оказанной
            услугой.
          </li>
          <li>
            Если пользователь нарушил условия оферты (использование сервиса для
            автоматического скрейпинга, перепродажа доступа третьим лицам и т.п.).
          </li>
        </ul>
      </Section>

      <Section title="Как запросить возврат">
        <ol className="list-decimal pl-6 space-y-2">
          <li>
            Отправьте письмо на{' '}
            <a href={`mailto:${SUPPORT_EMAIL}`} style={{ color: 'var(--accent)' }}>
              {SUPPORT_EMAIL}
            </a>{' '}
            с темой «Возврат подписки».
          </li>
          <li>
            В письме укажите: email аккаунта, дата и сумма платежа, причина
            возврата (свободная форма).
          </li>
          <li>
            Заявка рассматривается в течение 3 рабочих дней. О решении
            сообщается ответным письмом.
          </li>
          <li>
            При положительном решении средства возвращаются на ту же карту, с
            которой производилась оплата, в течение 10 рабочих дней (срок
            определяется банком-эмитентом).
          </li>
        </ol>
      </Section>

      <Section title="Связанные документы">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            <Link to="/contacts" style={{ color: 'var(--accent)' }}>
              Контакты службы поддержки
            </Link>
          </li>
          <li>
            <Link to="/delivery" style={{ color: 'var(--accent)' }}>
              Условия и сроки предоставления услуги
            </Link>
          </li>
          <li>
            <Link to="/privacy" style={{ color: 'var(--accent)' }}>
              Политика обработки персональных данных
            </Link>
          </li>
        </ul>
      </Section>

      <div
        className="mt-12 pt-6 text-xs"
        style={{
          borderTop: '1px solid var(--border-color)',
          color: 'var(--text-muted)',
        }}
      >
        По любым вопросам, связанным с подпиской и возвратами, пишите на{' '}
        <a href={`mailto:${SUPPORT_EMAIL}`} style={{ color: 'var(--accent)' }}>
          {SUPPORT_EMAIL}
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
