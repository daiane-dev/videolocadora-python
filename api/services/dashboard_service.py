from fastapi import HTTPException
from typing import Optional, Tuple
from datetime import date, datetime
from api.repositories import dashboard_repository


def parse_periodo(data_ini: Optional[str], data_fim: Optional[str]) -> Tuple[Optional[date], Optional[date]]:
    """
    Valida e converte data_ini/data_fim (YYYY-MM-DD).
    Regras:
    - ou manda as duas, ou não manda nenhuma
    - data_ini <= data_fim
    """
    if (data_ini and not data_fim) or (data_fim and not data_ini):
        raise HTTPException(
            status_code=400,
            detail="Envie data_ini e data_fim juntos (YYYY-MM-DD), ou não envie nenhum."
        )

    if not data_ini and not data_fim:
        return None, None

    try:
        dt_ini = datetime.strptime(data_ini, "%Y-%m-%d").date()
        dt_fim = datetime.strptime(data_fim, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Datas inválidas. Use o formato YYYY-MM-DD.")

    if dt_ini > dt_fim:
        raise HTTPException(status_code=400, detail="data_ini não pode ser maior que data_fim.")

    return dt_ini, dt_fim


def dashboard_resumo():
    hoje = date.today()
    return dashboard_repository.obter_resumo_dashboard(hoje)    



def faturamento_por_periodo(data_ini=None, data_fim=None):
    dt_ini, dt_fim = parse_periodo(data_ini, data_fim)
    hoje = date.today()

    resumo = dashboard_repository.obter_faturamento_por_periodo(
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        hoje=hoje,
    )

    return {
        "periodo": {
            "data_ini": str(dt_ini) if dt_ini else None,
            "data_fim": str(dt_fim) if dt_fim else None,
        },
        **resumo,
    }


def faturamento_serie(data_ini=None, data_fim=None, granularidade="dia"):
    if granularidade not in ("dia", "mes"):
        raise HTTPException(status_code=400, detail="granularidade deve ser 'dia' ou 'mes'.")

    dt_ini, dt_fim = parse_periodo(data_ini, data_fim)

    rows = dashboard_repository.obter_faturamento_serie(
        dt_ini=dt_ini,
        dt_fim=dt_fim,
        granularidade=granularidade,
    )

    for r in rows:
        r["total_locacoes"] = int(r["total_locacoes"])
        r["faturamento_total"] = float(r["faturamento_total"])

    return rows


def faturamento_por_genero(data_ini=None, data_fim=None):
    dt_ini, dt_fim = parse_periodo(data_ini, data_fim)

    rows = dashboard_repository.obter_faturamento_por_genero(
        dt_ini=dt_ini,
        dt_fim=dt_fim,
    )

    for r in rows:
        r["total_locacoes"] = int(r["total_locacoes"])
        r["faturamento_total"] = float(r["faturamento_total"])

    return rows            