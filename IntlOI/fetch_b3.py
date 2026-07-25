#!/usr/bin/env python3
"""
Международный OI — B3 "Contratos em aberto por Tipo de Participante" (Бразилия).

СТАБ, статус на 2026-07-24 (проверено вживую через браузер):
Старый отчёт "por tipo de participante" (PF/PJF/PJNF/II/IE) B3 ОТМЕНИЛА
07.04.2025 — на странице прямо написано:

  "A partir de 07/04/2025, será possível consultar os contratos em aberto
  na página Boletim Diário do Mercado - Capítulo: Cotações Resumo
  Estatístico. E consultar a quantidade de participação por Tipo de
  Investidores no Volume Total da B3 no Boletim Diário do Mercado -
  Capítulo: Indicadores e Informativos - Tabela: Participação dos
  Investidores."

Новое место — https://www.b3.com.br/.../consultas/boletim-diario/ —
"Boletim Diário do Mercado" с датапикером и вкладками Download/Renda
fixa/Renda variável/Derivativos/Outros dados. Судя по формату ("Boletim" =
бюллетень), результат похоже на PDF-отчёт, а не CSV/JSON API — то есть
это отдельная, более крупная задача (найти реальный файл + распарсить PDF
таблицы), не быстрый фикс уровня CFTC/NSE. Не успел довести до конца в
рамках этой сессии — CSP/challenge Cloudflare на b3.com.br дополнительно
осложняет автоматизацию.

Таблица open_interest_intl уже готова принять exchange='B3', category ∈
{PF, PJF, PJNF, II, IE, TOTAL} — писать сюда, когда пайплайн найдётся.

Следующий шаг: открыть Boletim Diário → Download, руками скачать файл за
любую дату, посмотреть что реально приходит (PDF/XLSX/CSV) и решить,
стоит ли овчинка выделки (RSS/API может не быть вообще, только ручной
веб-интерфейс).
"""
import sys


def main():
    print(
        "fetch_b3.py: НЕ РЕАЛИЗОВАНО — публичный эндпоинт B3 не найден.\n"
        "См. docstring этого файла: нужен ручной DevTools → Network разбор.",
        file=sys.stderr,
    )
    raise NotImplementedError("B3 fetcher: публичный JSON-эндпоинт не идентифицирован, см. docstring")


if __name__ == "__main__":
    main()
