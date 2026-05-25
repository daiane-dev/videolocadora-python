from fastapi import APIRouter, HTTPException
from typing import Optional
from api.schemas import FilmeCreate, FilmeUpdate
from api.services import filmes_service

router = APIRouter()


@router.get("/filmes")
def listar_filmes(limit: int = 50, offset: int = 0):
    return filmes_service.listar_filmes(limit=limit, offset=offset)


@router.post("/filmes")
def criar_filme(filme: FilmeCreate):
    return filmes_service.criar_filme(filme)


@router.get("/filmes/mais-locados")
def filmes_mais_locados(limit: int = 10, incluir_inativos: bool = False):
    return filmes_service.filmes_mais_locados(
        limit=limit,
        incluir_inativos=incluir_inativos,
    )


@router.get("/filmes/inativos")
def listar_filmes_inativos(limit: int = 50, offset: int = 0):
    return filmes_service.listar_filmes_inativos(
        limit=limit,
        offset=offset,
    )


@router.delete("/filmes/{id_filme}")
def deletar_filme(id_filme: int):
    return filmes_service.deletar_filme(id_filme)


@router.get("/filmes/busca")
def buscar_filmes(
    nome: Optional[str] = None,
    genero: Optional[str] = None,
    ano_min: Optional[int] = None,
    ano_max: Optional[int] = None,
    incluir_inativos: bool = False,
    limit: int = 50,
    offset: int = 0,
):
    return filmes_service.buscar_filmes(
        nome=nome,
        genero=genero,
        ano_min=ano_min,
        ano_max=ano_max,
        incluir_inativos=incluir_inativos,
        limit=limit,
        offset=offset,
    )


@router.get("/filmes/{id_filme}/disponibilidade")
def disponibilidade_filme(id_filme: int):
    return filmes_service.disponibilidade_filme(id_filme)


@router.get("/filmes/{id_filme}")
def obter_filme(id_filme: int):
    return filmes_service.obter_filme(id_filme)


@router.patch("/filmes/{id_filme}")
def atualizar_filme(id_filme: int, filme: FilmeUpdate):
    return filmes_service.atualizar_filme(id_filme, filme)


@router.patch("/filmes/{id_filme}/ativar")
def ativar_filme(id_filme: int):
    return filmes_service.ativar_filme(id_filme)


