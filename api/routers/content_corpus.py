"""Внутренний эндпоинт: примеры постов канала для Routine-агента Шага В.

Routine-агенты живут в облаке Anthropic и дотягиваются до нас ровно одним
способом — curl на framedata.ru/api/internal с X-Internal-Token. Значит любая
способность обязана быть HTTP-эндпоинтом. Паттерн скопирован с
api/routers/mandate_scan.py: там уже обкатан эндпоинт, который ОТДАЁТ данные
агенту (GET /known, frame-mandate-scan дёргает его первым шагом каждого прогона).

Зачем это нужно: вместо четырёх статичных образцов в промпте Шага В (все на 2-3
абзаца, один из них — наш же черновик 744) агент получает примеры, подобранные
под конкретную новость, плюс распределение структуры канала. Подробности выбора
без векторной модели — в api/services/corpus_examples.py.
"""
import os

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from api.services import corpus_examples

internal_router = APIRouter(prefix="/api/internal/content-corpus",
                            tags=["internal-content-corpus"])


def _require_internal_token(x_internal_token: str = Header(default="")) -> None:
    """Тот же shared secret, что у content_news и mandate_scan — один токен на всю
    внутреннюю поверхность, отдельный на каждый эндпоинт не нужен и только
    размножал бы секреты в .env."""
    expected = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    if not expected or x_internal_token != expected:
        raise HTTPException(status_code=403, detail="Неверный или отсутствующий internal token")


class SimilarRequest(BaseModel):
    headline: str = Field(default="", description="заголовок новости")
    raw_text: str = Field(default="", description="полный текст новости, если есть")
    tickers: list[str] = Field(default_factory=list,
                                description="тикеры кандидата — нужны только для "
                                            "решения, добирать ли пример из личного канала")
    k: int = Field(default=3, ge=1, le=5)


@internal_router.post("/similar", dependencies=[Depends(_require_internal_token)])
def similar(body: SimilarRequest):
    """Примеры постов канала под конкретную новость + распределение структуры.

    Запрос строится из headline и raw_text вместе: заголовок несёт тему, полный
    текст — детали, по которым лексический поиск и попадает в нужную бумагу."""
    query = " ".join(x for x in (body.headline, body.raw_text) if x).strip()
    if not query:
        raise HTTPException(status_code=422,
                            detail="нужен headline или raw_text — по пустому запросу "
                                    "подбирать примеры нечем")
    return corpus_examples.find_examples(query, tickers=body.tickers, k=body.k)
