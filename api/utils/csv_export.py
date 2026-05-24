"""
CSV export helper — общий компонент для всех индикатор-export-endpoint'ов.

Архитектура:
  - StreamingResponse — не держим весь dataset в памяти (некоторые export'ы
    могут быть до 100k строк).
  - UTF-8 BOM (﻿) перед заголовком — Excel правильно opens кириллицу
    без манипуляций с кодировкой.
  - Content-Disposition: attachment — браузер сразу триггерит download.
"""
import csv
import io
from typing import Iterable, Iterator

from fastapi.responses import StreamingResponse


def _bom() -> str:
    """UTF-8 BOM — Excel-friendly кириллица."""
    return "﻿"


def csv_streaming_response(
    rows: Iterable[dict],
    fieldnames: list[str],
    filename: str,
) -> StreamingResponse:
    """
    Стримит CSV-файл клиенту.

    Args:
        rows: iterable of dicts. Каждый dict должен содержать ключи из fieldnames.
              Отсутствующие — пустая ячейка.
        fieldnames: порядок колонок и список заголовков.
        filename: имя файла для browser download (без пути).

    Returns:
        FastAPI StreamingResponse с правильными headers.
    """
    def generator() -> Iterator[str]:
        # Header line + UTF-8 BOM (один раз, в начале).
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        yield _bom() + buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        # Data rows — каждый flushим отдельно.
        for row in rows:
            writer.writerow(row)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    return StreamingResponse(
        generator(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            # Не кэшируем — данные актуальные на момент запроса.
            "Cache-Control": "no-store",
        },
    )
