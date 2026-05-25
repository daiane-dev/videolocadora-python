from api.repositories import locacoes_repository
from api.services import locacoes_service
from datetime import date
from api.repositories import locacoes_repository
from api.repositories import usuarios_repository
from fastapi import HTTPException


def listar_locacoes_abertas(limit=50, offset=0):
    return locacoes_repository.listar_locacoes_abertas(
        limit=limit,
        offset=offset,
    )


def listar_locacoes_atrasadas(limit=50, offset=0):
    rows = locacoes_repository.listar_locacoes_atrasadas(
        limit=limit,
        offset=offset,
    )

    hoje = date.today()
    for r in rows:
        prevista = r["data_prevista_locacao"]
        r["dias_atraso"] = (hoje - prevista).days if prevista else None

    return rows


def listar_locacoes_por_usuario(id_usuario, limit=50, offset=0):
    usuario = usuarios_repository.buscar_usuario_por_id(id_usuario)

    if usuario is None:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    return locacoes_repository.listar_locacoes_por_usuario(
        id_usuario=id_usuario,
        limit=limit,
        offset=offset,
    )