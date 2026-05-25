from fastapi import APIRouter
from typing import Optional

from api.services import dashboard_service

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


@router.get("/resumo")
def dashboard_resumo():
    return dashboard_service.dashboard_resumo()


@router.get("/faturamento")
def faturamento_por_periodo(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    return dashboard_service.faturamento_por_periodo(
        data_ini=data_ini,
        data_fim=data_fim,
    )


@router.get("/faturamento-serie")
def faturamento_serie(
    data_ini: Optional[str] = None,
    data_fim: Optional[str] = None,
    granularidade: str = "dia",
):
    return dashboard_service.faturamento_serie(
        data_ini=data_ini,
        data_fim=data_fim,
        granularidade=granularidade,
    )


@router.get("/faturamento-por-genero")
def faturamento_por_genero(
    data_ini: Optional[str] = None,
    data_fim: Optional[str] = None,
):
    return dashboard_service.faturamento_por_genero(
        data_ini=data_ini,
        data_fim=data_fim,
    )