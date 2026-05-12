/**
 * ContactsPage — контакты предприятия.
 *
 * Обязательная страница по требованиям эквайринга Т-Банка
 * (Требования к Интернет-магазину, п.1: «Контактная информация
 * службы поддержки клиентов Предприятия, включая адрес электронной
 * почты и номер телефона», «Адрес местонахождения Предприятия»).
 *
 * Placeholders для заполнения перед подписанием договора с банком:
 *   - PHONE_PLACEHOLDER — рабочий номер телефона
 *   - LEGAL_NAME_PLACEHOLDER — ИП или ООО (полное наименование)
 *   - LEGAL_ADDRESS_PLACEHOLDER — юр. адрес как в ЕГРЮЛ/ЕГРИП
 *   - INN_PLACEHOLDER — ИНН
 *   - OGRN_PLACEHOLDER — ОГРНИП / ОГРН
 */
import { Link } from 'react-router-dom';
import { ArrowLeft, Mail, Phone, MapPin, Building2 } from 'lucide-react';

const SUPPORT_EMAIL = 'frameinfo@mail.ru';
const SUPPORT_PHONE = '+7 (960) 263-31-22';
const LEGAL_NAME = 'ИП Тория Александр Роландович';
const LEGAL_ADDRESS = '190020, г. Санкт-Петербург, ул. 8-я Красноармейская, д. 21А, кв. 5';
const INN = '782627792630';
const OGRNIP = '325784700029296';

export default function ContactsPage() {
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
          <Mail size={24} strokeWidth={1.8} />
        </div>
        <div>
          <h1
            className="text-2xl md:text-3xl font-semibold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
          >
            Контакты
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            Связь со службой поддержки и реквизиты
          </p>
        </div>
      </div>

      <Section title="Служба поддержки">
        <div className="space-y-4">
          <ContactRow
            icon={<Mail size={18} />}
            label="Электронная почта"
            value={
              <a href={`mailto:${SUPPORT_EMAIL}`} style={{ color: 'var(--accent)' }}>
                {SUPPORT_EMAIL}
              </a>
            }
            hint="Среднее время ответа — в течение 1 рабочего дня."
          />
          <ContactRow
            icon={<Phone size={18} />}
            label="Телефон"
            value={
              <a
                href={`tel:${SUPPORT_PHONE.replace(/[^+\d]/g, '')}`}
                style={{ color: 'var(--accent)' }}
              >
                {SUPPORT_PHONE}
              </a>
            }
            hint="Режим работы: понедельник–пятница, 10:00–19:00 МСК."
          />
        </div>
      </Section>

      <Section title="Реквизиты предприятия">
        <div className="space-y-4">
          <ContactRow
            icon={<Building2 size={18} />}
            label="Наименование"
            value={<span>{LEGAL_NAME}</span>}
          />
          <ContactRow
            icon={<MapPin size={18} />}
            label="Адрес местонахождения"
            value={<span>{LEGAL_ADDRESS}</span>}
          />
          <ContactRow
            icon={<span className="font-mono text-xs">№</span>}
            label="ИНН"
            value={<span className="font-mono">{INN}</span>}
          />
          <ContactRow
            icon={<span className="font-mono text-xs">№</span>}
            label="ОГРНИП"
            value={<span className="font-mono">{OGRNIP}</span>}
          />
        </div>
      </Section>

      <Section title="Связанные документы">
        <ul className="list-disc pl-6 space-y-2">
          <li>
            <Link to="/privacy" style={{ color: 'var(--accent)' }}>
              Политика обработки персональных данных
            </Link>
          </li>
          <li>
            <Link to="/refund" style={{ color: 'var(--accent)' }}>
              Условия возврата и отмены
            </Link>
          </li>
          <li>
            <Link to="/delivery" style={{ color: 'var(--accent)' }}>
              Условия и сроки предоставления услуги
            </Link>
          </li>
          <li>
            <Link to="/security" style={{ color: 'var(--accent)' }}>
              Политика информационной безопасности
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
        Сервис «Frame» (домен <strong>таймфрейм.рф</strong>) — аналитическая
        платформа по российскому фондовому рынку. По вопросам сотрудничества и
        технической поддержки используйте контакты выше.
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

function ContactRow({
  icon,
  label,
  value,
  hint,
}: {
  icon: React.ReactNode;
  label: string;
  value: React.ReactNode;
  hint?: string;
}) {
  return (
    <div className="flex items-start gap-3">
      <div
        className="flex-shrink-0 flex items-center justify-center"
        style={{
          width: 32,
          height: 32,
          borderRadius: 6,
          background: 'color-mix(in srgb, var(--text-primary) 5%, transparent)',
          color: 'var(--text-muted)',
        }}
      >
        {icon}
      </div>
      <div className="flex-1 min-w-0">
        <div
          className="text-xs uppercase mb-0.5"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.06em' }}
        >
          {label}
        </div>
        <div style={{ color: 'var(--text-primary)' }}>{value}</div>
        {hint && (
          <div className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>
            {hint}
          </div>
        )}
      </div>
    </div>
  );
}
