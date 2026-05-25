from fastapi import APIRouter
from api.services import views_service

router = APIRouter(prefix="/views", tags=["Views"])


@router.get("/filmes-ranking")
def filmes_ranking():
    return views_service.filmes_ranking()


@router.get("/faturamento-mensal")
def faturamento_mensal():
    return views_service.faturamento_mensal()   